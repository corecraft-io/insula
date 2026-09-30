# AGENTS.md

Operating notes for agents (human or AI) working in this repository. Read this
before your first change.

## Build and verify

The full gate — run all of it before saying a change is done:

```sh
gofmt -l . && go vet ./... && go build ./... && go test -race -count=1 ./...
```

Plus, when the change touches anything on a per-tenant path:

```sh
GOGC=off go test -run '^$' -bench . -benchmem -benchtime 1x -count 5 -cpu 4 ./tenant/
go test -run '^$' -bench . -benchmem ./caps/
go run ./cmd/insula demo
```

`demo` is the integration smoke test. It assembles the platform on real loopback
listeners and prints each step; if it exits non-zero, something in the assembly,
routing, isolation, or shutdown path is broken.

## Where the decisions come from

This repository has seven recorded architecture decisions in
[`docs/adr/`](docs/adr/README.md). Read them before proposing a change that
touches any of the boundaries below — most of the "why not the obvious
alternative" answers are already written down there, along with the cost each
decision accepted. If you find yourself wanting to reverse one, that is
legitimate, but it means writing a new ADR, not editing the old one.

## Dependencies

`cordis` is an ordinary published dependency, not a sibling checkout:

```
require github.com/corecraft-io/cordis v0.2.0
```

It is the only one, and adding a second needs a conversation first — the point of
the layout is a dependency chain with nothing else in it.

### Developing against a local cordis

Do **not** add a `replace` directive to `go.mod`. `replace` is honoured only in
the *main* module, so one committed here would (a) be silently ignored by every
downstream consumer — misleading, since it looks like it does something — and
(b) break `go install github.com/corecraft-io/insula/...@latest`, which applies the
main module's `replace` to a relative path inside the module cache.

Use a Go workspace instead. It lives **above both checkouts**, outside either
repository, so neither repo has to gitignore it:

```
// metaRobin/go.work
go 1.22

use (
    ./cordis
    ./insula
)
```

With that file in place, `go build` / `go test` inside `insula/` compile against
the `../cordis` working tree, while the committed `go.mod` keeps describing the
published graph.

To check the **published** graph — the one a downstream user gets, where the
version in `require` is what actually loads — disable the workspace:

```sh
GOWORK=off go build ./...
```

Before the first tag exists this fails (correctly) with `missing go.sum entry for
module providing package github.com/corecraft-io/cordis`; once the tag is on the
proxy, `go mod tidy` writes `go.sum` and it passes. Re-run it after any cordis
version bump: it is the cheapest way to catch a `go.mod`/`go.sum`/tag disagreement,
because it resolves through the proxy instead of the local tree.

## The invariants you must not break

These are the ones that fail *silently* if you break them, which is why they are
listed rather than left to review.

### Short IDs are unique across the whole tree

`EntryTree.store` indexes entries by short ID in a flat map, but addresses them
by `group:child` path. Two entries with the same short ID in different groups are
therefore illegal. Both `reconcile` and `Create` reject them (skip + log), and
that rejection is load-bearing.

### Structural tree changes must detach synchronously

Fiber effect disposal is asynchronous — `Dispose` only enqueues a pump task.
Every path where a *successor* with the same ID can be created before the
predecessor is torn down (context rebuilds, cross-group moves) must call
`Entry.detachSubgroup()` **synchronously first**. Skip it and you get short-ID
index collisions plus leaked subtrees.

### `track` and `untrack` come in pairs

`Reflect.index` is an inverted index. Every `track` needs a matching `untrack`
on **every** exit path, including the failure paths. The registry-unregister run
and the "registration failed" path each need their own rollback.

### The index key must be isomorphic to the store key

`Reflect.index` is bucketed by isolate realm (`map[isolateKey][]*Fiber`), because
the question "who needs notifying" must be answerable with the same key that
resolution will hit. Bucketing it by service *name* makes provisioning O(N²) —
that was a real defect, measured at 82 s and 4.5 GB for 10 000 tenants, 90% of
the profile inside one `candidates` call. Do not "simplify" the key back.

Corollary: `untrack` recomputes the bucket key from the fiber rather than
storing it. That is safe only because `Fiber.ctx` and `Fiber.inject` are
effectively immutable after construction. If you ever make them mutable,
`track` must start recording the key on the `Fiber` instead.

### Platform-level quotas are global

The admission controller is constructed **once**, in `insula.Open`, and handed to
every shard. One per shard silently turns "the platform total" into N times the
total. The same reasoning applies to the model pool, memory store, metrics
registry, and audit sink: process-level singletons, thin per-shard handles.

### A reservation that never reached the upstream must be refunded

Quota and breaker-probe credit are both **reserve-then-use**: `Account.Reserve`
claims a call slot, `Breaker.Allow` claims the single half-open probe. Every
early return between claiming and actually issuing the request must hand them
back — `Account.Release`, `Breaker.ReleaseProbe`. `Pool.Complete` routes all of
those paths through **one** `defer`, deliberately: per-branch hand-written
refunds get missed, and a missed one reads as "one rejection, one unit of quota
burned" — invisible in review, and it only surfaces after the upstream recovers
and the tenant still cannot call.

Two consequences worth spelling out:

- The operational kill-switch (`Pool.SetEnabled(false)`) and the breaker gate are
  **reads**, so they run before any reservation. Put them after and a burst of
  rejected calls can transiently exhaust a quota the tenant never spent. That
  window is only observable under concurrency, and it misreports the cause as
  `ErrQuotaExceeded` instead of `ErrUnavailable` — the tenant sees "out of
  budget" when the real answer is "the pool is down".
- A failure that never reached the upstream must **not** be reported to the
  breaker (`Breaker.Failure`), but the probe still has to come back. That is what
  `ReleaseProbe` is for: `Reset` is wrong (it closes the breaker without having
  verified the upstream) and `Failure` is wrong (it re-opens the pool over a
  local config error). Skip it and the breaker wedges in HalfOpen forever —
  strictly worse than being open, because open at least heals itself.

### Enforcement is structural, not conventional

Where the type system can make a bad thing impossible, it must. `Provision` is
absent from the `Workspace` interface the edge layer holds, so "an HTTP handler
that deprovisions a tenant" is a compile error rather than a code-review item.
Prefer widening an interface over adding a runtime check.

This does **not** mean "no runtime checks". Some things the type system cannot
reach: `cordis` accepts `EntryOptions.Isolate` as `map[string]any`, so a shared
realm label (`"@shared"`) is expressible even though the platform never emits
one — and `cordis` honours it silently. The answer there is a **sentinel**, not
a guarantee: `shard.Pool.SelfCheck` is the designated place where construction
breakage is caught (see SAFETY.md R3), and a sentinel must report what it
scanned — `SelfCheckReport.PairsChecked` and `.Declarations` — so that "ran and
was clean" cannot be confused with "did not run".

## Testing discipline

- **A fix for a semantic bug needs a regression test, and you must verify the
  test fails without the fix.** Temporarily revert the fix, watch the test go
  red, restore. A test that was never red proves nothing.
- **Security and isolation assertions need mutation verification.** Break the
  implementation deliberately, confirm the test turns red, restore. When a
  mutation does *not* turn anything red, distinguish "the test doesn't cover it"
  from "the test isn't aimed at it" — the second is more common and more
  insidious.
- **Scope each mutation to the test you meant to break.** Run it with
  `-run '^TestName$'`, not the whole package. A mutation that reddens half a
  package cannot distinguish "this test guards the invariant" from "this test was
  collateral damage".
- **The expected-red set is an empirical result; write it narrow and let the run
  correct you.** A mutation often breaks less than you predicted. Observed: making
  `Account.Release` a no-op reddened only the breaker-path test, not the
  kill-switch test — because the kill-switch now returns *before* reserving
  anything, so it never calls `Release` at all. A prediction that over-claims is
  how you end up believing a test covers something it does not.
- **Scale tests assert output shape, never elapsed time.** Assert a length or a
  count. A timing assertion is flaky on CI and will be disabled within a month.
  Add an anti-self-deception guard: before asserting a count is small, assert the
  fixture actually built the large thing (e.g. `indexTotal == realms`), otherwise
  a fixture that silently built nothing passes the test.
- **Benchmarks: run the package alone, and keep the collector out of the timed
  window.** `go test ./...` runs each package's test binary in parallel, and
  neighbour load lands directly in your numbers. Then: at `GOMAXPROCS=1` the GC
  has no second `P` to mark on, so every mark assist triggered inside the timed
  window is paid by the benchmark goroutine, and the bill grows with `N` — the
  exact axis these gates measure. One `N=1000` teardown point reads 25 149
  ns/tenant at `-cpu 1` with GC on and 7 704 with `GOGC=off`. Pick either knob
  and write it next to the number; the tables use `GOGC=off -cpu 4`.
- **Cross-scale comparisons need identical `-benchtime` and `-count` on both
  sides.** `Wait()` scan cost read 56.41 ns/fiber under `-benchtime 3x` and
  9.5–10.1 ns/fiber under `-benchtime 5x`, on the same build. If a number
  "got slower", the first move is a same-parameter A/B against the previous
  revision — not a code change.
- **A figure that cannot be reproduced from this repository's own tables is a
  bug in the document.** Three published figures were wrong on 2026-09-29, and
  each was checkable against another line of the same file with one division.

See [BENCHMARK.md](BENCHMARK.md) for the gate benchmarks and current numbers.

## Style

- Comments are written in Chinese; identifiers, commit messages, and this file
  are in English.
- Commit messages use conventional prefixes (`feat:` / `fix:` / `docs:` /
  `chore:` / `ci:`).
- Exported symbols carry a doc comment explaining *why the thing exists*, not
  what the signature already says. The existing packages are the reference.
- Prefer a comment that records a failure mode over one that restates the code.
  The best comments in this repository explain what happens when the obvious
  alternative is chosen instead.
