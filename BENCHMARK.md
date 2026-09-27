# Benchmarking

English | [中文](BENCHMARK.zh.md)

This file exists because insula has one class of defect that unit tests cannot
catch: **an O(N) operation placed on a per-tenant path**. Such a defect is
invisible in a test suite, invisible in a development deployment of a few
hundred tenants, and only appears once the platform is large — by which time the
cause is buried under a year of growth.

So the curves in this file are a gate, not a report. If you change anything on a
per-tenant path, run them and compare.

## Environment

All numbers below: **Apple M5 Pro / darwin arm64**, Go 1.26.3,
`module github.com/metaRobin/insula` declaring `go 1.22`, measured with the local
`go.work` active — so `cordis` resolved to the `../cordis` working tree and not to
a published version. They are single-run medians, not
statistically significant samples — treat the *shape* of each curve as the
finding and any single point as approximate.

## How to measure

```sh
# Tenant-scale curves. Run the package alone.
go test -run '^$' -bench . -benchmem -benchtime 3x -count 3 -cpu 1 ./tenant/

# Event fanout. Snapshot-style internally, so -benchtime controls repetition
# count rather than measurement duration; a small value keeps it fast.
go test -run '^$' -bench BenchmarkEventFanout -benchtime 3x -count 3 -cpu 1 ./caps/

# Isolation correctness, not performance — but it belongs in the same gate.
go test -race -count=1 -run 'Ident|Isolat|Breach|SelfCheck|Forced' ./...
```

Four rules, each learned by getting it wrong:

1. **Run the package alone.** `go test ./...` runs each package's test binary in
   parallel. Neighbour load lands directly in the numbers, and the `in_realm`
   `subscribers=1` row can differ by a factor of two.
2. **`-cpu 1`.** These are single-threaded control-plane operations; letting the
   framework vary `GOMAXPROCS` adds variance without adding information.
3. **Identical `-benchtime` and `-count` on both sides of a comparison.** A
   `Wait()` scan cost read **56.41 ns/fiber** under `-benchtime 3x` and
   **9.5–10.1 ns/fiber** under `-benchtime 5x`, on the *same build*. Extrapolating
   from a different sampling configuration produced a fake 5.2× regression that
   took a same-parameter A/B to disprove.
4. **Then, and only then, suspect the code.** The first reaction to "this got
   slower" is a same-parameter A/B against the previous revision, not an edit.

`BenchmarkWaitScan` and `BenchmarkEventFanout` both use snapshot-style
measurement: they pin `b.N = 1`, do their own internal repeat count, and report
via `ReportMetric`. The metric definition is a per-snapshot average, so
`-benchtime` does not change *what* is measured — but it does control how many
times the framework re-runs the whole snapshot, which is pure wall-clock cost.
Use `-benchtime 3x`. Do not try to raise `b.N`; the benchmark ignores it.

## What CI does and does not do

`.github/workflows/ci.yml` has a separate `bench` job that runs

```sh
go test -run '^$' -bench . -benchmem -benchtime 3x -count 1 -timeout 30m ./...
```

Its purpose is **compilability plus gross regression**: that the gates still
build, that the scale assertions inside them still hold, and that nothing has
gone quadratic by orders of magnitude. It takes about 17 s locally.

It is **not** a measurement. A GitHub runner is shared and jittery, and `./...`
runs packages in parallel — the exact two things rule 1 and the variance notes
above tell you to avoid. Do not read a number off a CI run and call it a
regression. Bring it back to a local machine and follow the four rules.

CI checks out this repository only. `cordis` is an ordinary published dependency
(`require github.com/metaRobin/cordis v0.1.0`), so the runner resolves it through
the module proxy exactly as any other consumer would. The workflow asserts that no
`replace` directive reappears in `go.mod`, because such a directive is honoured
only in the *main* module: it would be silently ignored by every downstream
consumer and would break `go install …@latest`. To hack on both repositories at
once, use a `go.work` above the two checkouts — see `AGENTS.md` § Dependencies.

One consequence matters for benchmarking. If a cordis change is *the point* of
your work, run the gates with the workspace active (the default when you are
inside `metaRobin/`); otherwise you are measuring the published `v0.1.0` and not
your edit. `GOWORK` is the switch: `go env GOWORK` prints the file when it is
loaded, and `GOWORK=off` puts you back on the published graph.

## Gate 1 — provisioning must be flat

`BenchmarkTenantProvision` measures one batch round-trip for N tenants
(`Manager.Provision` does exactly one `DoSync` + one `App.Wait()` for the whole
batch).

| N | ns/tenant | B/op per tenant | allocs per tenant |
| --- | --- | --- | --- |
| 100 | 29 378 | 40.5 KB | 628.6 |
| 1 000 | 30 821 | 40.1 KB | 626.5 |
| 10 000 | 47 986 | 40.7 KB | 626.3 |

The gate is `ns/tenant`, and it must not grow with N. Here it grows **1.63×
across a 100× scale increase** — the residual is cache working-set growth
(120 000 fibers ≈ 300 MB), not an algorithm. `allocs/tenant` is flat at ~627,
which is the cleaner signal.

Fitted over the whole range, total time scales as ≈ N^1.09.

### What this used to be

Before 2026-09-27, provisioning was **quadratic**:

| N | then | now | improvement |
| --- | --- | --- | --- |
| 100 | 85 116 | 29 378 | 2.9× |
| 1 000 | 285 324 | 30 821 | 9.3× |
| 10 000 | 8 197 214 (82 s, 4.5 GB) | 47 986 | **171×** |

The cause was `cordis.Reflect.index` being bucketed by service *name* instead of
by isolate *realm*, so "who needs notifying" cost one pass over every realm's
subscribers. At N=10000 that single call (`candidates`) was **90.36%** of the
profile. Fixed in cordis by making the index key isomorphic to the store key; see
`AGENTS.md` for the invariant. The measured spread is now 1.63× across 100×
scale instead of 96×.

## Gate 2 — teardown must not go quadratic

`BenchmarkTenantTeardownAll` deprovisions the whole batch. It is deliberately
*not* the mirror of provisioning: removing a tenant tears down a whole subtree
(fibers, effect chains, session runtimes, capability watchers), and effect
recovery is LIFO and asynchronous, so the measurement includes waiting for
convergence.

| N | ns/tenant (median of 5) | observed range | total (median) |
| --- | --- | --- | --- |
| 100 | 5 644 | 5 609 – 6 524 | 564 µs |
| 1 000 | 13 090 | 8 560 – 25 189 | 13.1 ms |
| 10 000 | 27 662 | 24 910 – 28 359 | 277 ms |

The design's gate is phrased as: the N=1000 → N=10000 ratio should be ~10, not
~100. Measured **15.4×**. Fitting an intermediate-scale curve (N = 100, 300,
1000, 3000, 10000, medians of 3) gives an exponent of **≈1.31** over the full
100× range. For comparison, provisioning over the same range fits ≈1.09.

So: super-linear, but nowhere near quadratic, and teardown is ~82% of the
provision+teardown combined cost at N=10000. "Teardown is slower than
provisioning" is itself a conclusion rather than a defect — it buys a single
`Remove` that takes the whole subtree, instead of per-entry cleanup that would
actually leak.

The residual super-linearity is consistent with the design's defect #2
(`Runtime.remove` / `Registry.deleteRuntime` removing from slices by splice, so
each removal is O(len)). That term has not been isolated by measurement, and at
N=10000 it is a minority of the total. It should be re-measured before anyone
optimises it — see the caveat at the end of this file.

**Run-to-run variance is high here** (n=1000 spanned 8.6–25.2 µs/tenant across 5
runs). Compare medians, and if a change looks like ±30%, get more samples before
believing it.

## Gate 3 — `Wait()` scan cost

`BenchmarkWaitScan` isolates the global scan inside `App.Wait()`. This matters
because `Wait` is the platform's *only* synchronisation point — every `Loader`
mutation, `Provision`, `Deprovision`, and the pre-close assembly assertion calls
it — and `settled()` walks **all** fibers in the root registry, not the pending
task count. It is O(total fibers), not O(queue length).

| N | fibers | ns/fiber |
| --- | --- | --- |
| 100 | 1 200 | 2.4 |
| 1 000 | 12 000 | 2.7 – 4.2 |
| 10 000 | 120 000 | 11.0 – 11.3 |

At 24 000 fibers this is ~84 µs — acceptable. The jump to ~11 ns/fiber at
N=10000 is a cache effect (300 MB working set), not an algorithmic change.

This curve is the quantitative basis for a platform rule: **batch what can be
batched.** N tenants processed one at a time means N full scans, i.e. O(N²). One
batch with at most one `Wait` per batch means one scan. `Manager.Provision` and
`Manager.Deprovision` are written to the second shape, and this benchmark is the
guard that keeps them there.

## Gate 4 — event fanout

`BenchmarkEventFanout` has two families, and they must be read side by side
because they measure different things.

### In-realm fanout (insula's own mechanism)

K subscribers in *one* realm, one service becoming available:

| K | ns/fanout | ns/subscriber |
| --- | --- | --- |
| 1 | 5 216 | 5 216 |
| 10 | 13 281 | 1 328 |
| 1 000 | 797 704 | **797.7** |

At K=1000 this is very stable (three runs: 790.8 / 797.7 / 800.6 ns/subscriber,
1.2% spread). The per-subscriber cost *falls* as K grows — fixed overhead
amortised — so there is no super-linear term here, which is the point of the
family.

The absolute values at K=1 and K=10 are less stable across invocation styles
(2.4–5.2 µs and 7.0–13.3 µs depending on how many snapshot repeats the framework
chose). Read the K=1000 row when comparing, not the small ones.

Note that insula uses none of cordis's `Emit`/`On`; this fanout *is* the
dependency mechanism. A capability's availability **is** its `Inject`
dependency's liveness, so this fanout is the platform's availability signal path,
and its constant factor is the price of every service change.

### Cross-realm interference (cordis's notify index)

R realms each with one pending watcher; the service is provided only in the
**last** realm. If notification cost is independent of R, the curve is flat.

Medians of three runs, `-cpu 1 -benchtime 3x`:

| R | ns/notify | then (pre-fix) | improvement | ns/per-realm |
| --- | --- | --- | --- | --- |
| 1 | 2 931 | 4 200 | 1.4× | 2 931 |
| 100 | 3 777 | 8 900 | 2.4× | 37.8 |
| 1 000 | 8 148 | 116 000 | 14.2× | 8.1 |
| 10 000 | 47 816 | 1 975 000 | **41.3×** | 4.8 |

Still not flat — but the remaining slope is **not the index**. `fanoutLastRealm`
times `DoSync` + `App.Wait()`, and `Wait` scans every fiber in the app. With R
realms each holding a watcher, the fiber count is proportional to R, so what is
left is the same O(N) as Gate 3, at roughly 5–11 ns/fiber. Per-realm cost falls
toward ~5 ns.

The practical reading: this benchmark no longer measures defect #3 (the index);
it now measures defect #1 (`Wait`). Keep them separate, or you will keep looking
for a slope that has already been removed.

Run-to-run variance here is real: R=10000 measured anywhere between 43 and 72 µs
across this session's runs. Compare medians.

The benchmark also asserts, functionally, that no other realm's watcher wakes up
— "notification filtered by name rather than by realm" is the failure mode that
would make this curve *fast* while being wrong.

## Capacity

`BenchmarkFiberFootprint` (snapshot, `b.N = 1`, `runtime.ReadMemStats`):

| N | fibers | bytes | bytes/tenant |
| --- | --- | --- | --- |
| 1 | 12 | 31 592 | 31 592 |
| 100 | 1 200 | 3 070 304 | 30 703 |
| 1 000 | 12 000 | 30 377 424 | 30 377 |

**A tenant is exactly 12 fibers and ~30 KB resident**, flat across 1000× scale.
That number is the denominator for shard sizing: 2 000 tenants ≈ 6 MB per shard
of pure fiber footprint, before session state, histories, and connection pools.

Derived rules:

| Quantity | Value | Basis |
| --- | --- | --- |
| Fibers per tenant | 12 | measured, flat |
| Resident bytes per tenant | ~30 KB | measured, flat |
| Fibers per shard | ≤ 2 000 tenants = 24 000 | `Wait()` at 24 000 fibers ≈ 84 µs |
| Shard count | 4 – 8 | bounds blast radius and keeps `Load` `Resolve` scans short. The scheduler is single-threaded but its work is µs-scale, so sharding is not about CPU |
| 10 000 active tenants | 120 000 fibers, ~300 MB | 5 shards → 24 000 fibers each, right at the comfort limit |

Shard count and hot/cold tenant policy are the two tuning parameters here.
Everything else in this file describes invariants.

## The five O(N) degradations, and where each stands

The design listed five. Current status, measured:

| # | Location | Status |
| --- | --- | --- |
| 1 | `app.go` `settled()` — `Wait()` scans all fibers; `Loader.Load` calls `Wait` | **Open.** Measured in Gate 3: 2.4 ns/fiber up to N=1000, ~11 ns/fiber at N=10000. Contained by the batch rule, not fixed |
| 2 | `registry.go` `Runtime.remove` / `deleteRuntime` — slice splice | **Open.** The likely source of Gate 2's ≈1.31 exponent; not isolated by measurement, and a minority term at N=10000 |
| 3 | `reflect.go` `untrack` — slice splice in the inverted index | **Structurally bounded.** The realm bucketing means a tenant-private service's bucket holds only that tenant's subscribers. A platform-wide service in the default realm still has one large bucket. Fixed 2026-09-27 along with the key change |
| 4 | `events.go` `hooksOf` / `EmitFiltered` — copy all listeners, then filter | **Open.** Separate path from `Reflect`; insula does not use it, so it is not on the platform's hot path. A platform that does use cordis events at tenant scale must namespace event names |
| 5 | `loader.go` `Resolve` — linear scan of `g.children` | **Open, mitigated by sharding.** 2 000 children per shard ≈ 2 µs; at 100 000 it would be 100 000 string comparisons per address lookup |

## A caveat about this file

Everything here is a median of a handful of runs on one machine. Two things
follow:

- **Do not optimise from these numbers without re-measuring the specific path.**
  The one time this was ignored — treating a `-benchtime 3x` reading as a
  regression against a `-benchtime 5x` baseline — it produced a 5.2× phantom
  regression that cost a stash/restore cycle to disprove.
- **Do not add a timing assertion to a test.** Scale regressions are guarded by
  asserting an output *length* or *count* (see
  `TestReflectCandidatesDoNotScaleWithUnrelatedRealms` in cordis), never elapsed
  time. A timing assertion is flaky on CI and gets disabled within a month,
  which is the same as not having it.
