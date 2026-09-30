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
`module github.com/corecraft-io/insula` declaring `go 1.22`, measured with the local
`go.work` active — so `cordis` resolved to the `../cordis` working tree and not to
a published version. They are medians of a handful of runs, not
statistically significant samples — treat the *shape* of each curve as the
finding and any single point as approximate.

**The tenant-scale tables were re-taken on 2026-09-29** with
`GOGC=off … -benchtime 1x -count 5 -cpu 4`. The earlier reads were taken with
`-cpu 1`, which biases every number in the direction of the scale axis — see
rule 2 under "How to measure". Where a figure below differs from what this file
said before, the new one is the measured one, and the size of the discrepancy is
itself the finding.

## How to measure

```sh
# Tenant-scale curves. Run the package alone, and pin GC.
#
# -benchtime 1x with -count 5 gives five independent *single-batch* readings.
# That is what the gates below are about: one batch of N, not N of anything.
# GOGC=off keeps the collector out of the timed window entirely (rule 2), which
# is also what makes the readings comparable to each other. It does that by never
# triggering a collection, so the heap is unbounded: keep -count small.
GOGC=off go test -run '^$' -bench . -benchmem -benchtime 1x -count 5 -cpu 4 ./tenant/

# Event fanout. Snapshot-style internally, so -benchtime controls repetition
# count rather than measurement duration; a small value keeps it fast.
go test -run '^$' -bench BenchmarkEventFanout -benchtime 3x -count 3 -cpu 4 ./caps/

# Isolation correctness, not performance — but it belongs in the same gate.
go test -race -count=1 -run 'Ident|Isolat|Breach|SelfCheck|Forced' ./...
```

Five rules, each learned by getting it wrong:

1. **Run the package alone.** `go test ./...` runs each package's test binary in
   parallel. Neighbour load lands directly in the numbers, and the `in_realm`
   `subscribers=1` row can differ by a factor of two.
2. **Keep the collector out of the timed window — and know that `-cpu 1` is what
   puts it there.** This file used to say, simply, "`-cpu 1`", on the theory that
   these are single-threaded control-plane operations and varying `GOMAXPROCS`
   only adds variance. That is half right, and the wrong half is expensive: at
   `GOMAXPROCS=1` the collector has no second `P` to mark on, so **every GC mark
   assist triggered inside the timed window is paid by the benchmark goroutine, in
   that window.** The assist bill is proportional to the live heap, the live heap
   is proportional to `N`, and so the bias *grows along the very axis these gates
   exist to measure*. The four cells below are one benchmark, one `N=1000`, one
   `-benchtime 50x`; only the two knobs move (ns/tenant):

   | | GC on | `GOGC=off` |
   | --- | --- | --- |
   | `-cpu 1` | 25 149 | 7 704 |
   | `-cpu 4` | 7 837 | 7 704 |

   One cell is the outlier, and it is exactly the combination this file used to
   prescribe. Either knob fixes it: `GOGC=off` is the more fundamental, because it
   removes the coupling rather than relocating it, while `-cpu 4` is the more
   practical, because GC still runs and reclaims — which matters across many
   iterations at `N=10000`. The tables below use `GOGC=off` with `-cpu 4`, and
   **whichever you use, write it next to the number.** The historic `-cpu 1`,
   GC-on figures in Gates 1, 2 and 4 are the top-left cell.
3. **Identical `-benchtime` and `-count` on both sides of a comparison.** The
   warning is old; rule 2 is its mechanism. A `Wait()` scan cost read
   **56.41 ns/fiber** under `-benchtime 3x` and **9.5–10.1 ns/fiber** under
   `-benchtime 5x`, on the *same build* — and at `-cpu 1` with GC on, the same
   `N=1000` teardown point reads 7 380 ns/tenant at `3x` and 22 696 at `50x`, with
   **identical `allocs/op`** (122 945 vs 122 084). Identical work, different
   answer: that swing is the instrument.
4. **Then, and only then, suspect the code.** The first reaction to "this got
   slower" is a same-parameter A/B against the previous revision, not an edit.
5. **`b.StopTimer()` hides work from `ns/op`, not from the profile.** The
   teardown benchmark provisions inside a `StopTimer()` window, so provisioning
   does not affect its `ns/tenant` — but it is still *in the CPU profile*, and at
   `N=10000` it was 26% of all samples (`Fiber.doLoad`). Always read a profile by
   **caller** before attributing anything to the benchmark you think you ran.

Rule 2 predicts its own effect size, which is the best evidence that it is the
right explanation. The bigger the timed region's allocation, the more the old
readings were inflated — and the one gate that allocates nothing in its timed
region is the one whose numbers did not move:

| gate | alloc inside the timed window | then (`-cpu 1`, GC on) | now (`GOGC=off`) | inflation |
| --- | --- | --- | --- | --- |
| 1 provisioning | 40.7 KB/tenant | 47 986 | 27 818 | 1.72× |
| 2 teardown | 3.3 KB/tenant | 27 662 | 18 003 | 1.54× |
| 3 `Wait()` scan | ~nothing | 11.0–11.3 | 11.1–11.5 | ~1.0× |

Gate 3 is the control: it builds the same `N` tenants, so it sits on the same
live heap, but it allocates nothing inside the timed window, so no collection is
triggered there and no assist is charged. It is the only gate whose old numbers
survive.

`BenchmarkWaitScan` and `BenchmarkEventFanout` both use snapshot-style
measurement: they pin `b.N = 1`, do their own internal repeat count, and report
via `ReportMetric`. The metric definition is a per-snapshot average, so
`-benchtime` does not change *what* is measured — but it does control how many
times the framework re-runs the whole snapshot, which is pure wall-clock cost.
Use `-benchtime 3x`. Do not try to raise `b.N`; the benchmark ignores it.

### Comparing profiles

A CPU profile answers "what is expensive". It does not answer "what got more
expensive as scale grew". For that, run the same benchmark at two scales with
`-benchtime` chosen so `-benchtime × N` is equal — 1 000 × 50 and 10 000 × 5 both
come to 50 000 teardowns — and compare each frame's **share** of the profile:

- a **constant per-tenant** cost keeps its share;
- a **per-batch super-linear** cost grows its share.

That is how Gate 2's residual was separated from its constant part. And because
rule 5 applies, every growing frame then has to be read by **caller**: in the
first pass one frame grew 8× and looked like the answer — and belonged to
provisioning, not to the teardown under measurement. It had to be dropped from
the attribution entirely. A profile comparison produces suspects, not verdicts.

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
regression. Bring it back to a local machine and follow the five rules.

CI checks out this repository only. `cordis` is an ordinary published dependency
(`require github.com/corecraft-io/cordis v0.2.0`), so the runner resolves it through
the module proxy exactly as any other consumer would. The workflow asserts that no
`replace` directive reappears in `go.mod`, because such a directive is honoured
only in the *main* module: it would be silently ignored by every downstream
consumer and would break `go install …@latest`. To hack on both repositories at
once, use a `go.work` above the two checkouts — see `AGENTS.md` § Dependencies.

One consequence matters for benchmarking. If a cordis change is *the point* of
your work, run the gates with the workspace active (the default when you are
inside `metaRobin/`); otherwise you are measuring the published `v0.2.0` and not
your edit. `GOWORK` is the switch: `go env GOWORK` prints the file when it is
loaded, and `GOWORK=off` puts you back on the published graph.

## Gate 1 — provisioning must be flat

`BenchmarkTenantProvision` measures one batch round-trip for N tenants
(`Manager.Provision` does exactly one `DoSync` + one `App.Wait()` for the whole
batch).

| N | ns/tenant | B/op per tenant | allocs per tenant |
| --- | --- | --- | --- |
| 100 | 27 209 | 40.6 KB | 628.8 |
| 1 000 | 17 620 | 40.1 KB | 626.5 |
| 10 000 | 27 818 | 40.7 KB | 626.3 |

The gate is `ns/tenant`, and it must not grow with N. From N=1000 to N=10000 it
grows **1.58×** across a 10× scale increase, which fits ≈ N^1.20 in total time.
`allocs/tenant` is flat at ~627 and `B/op` flat at ~40.7 KB — the cleaner
signals, and both **unchanged** from the pre-2026-09-29 reads. That is what makes
the comparison legitimate: the work never moved, only the time.

N=100 reads *higher* per tenant than N=1000 (27.2 µs vs 17.6 µs) and is the
noisiest row in this file (24.2–38.3 µs, 58% spread). That is fixed per-batch
cost — one `DoSync` plus one `Wait()` for the whole batch — amortised over only
100 tenants. Read this gate as "does it grow with N", and use the upper two rows
to answer it.

### The residual is insula's own call site

One profile at `N=10000` names it. `cordis.(*EntryTree).Resolve` was **5.22%** of
all samples, 57.89% of that inside `runtime.memequal`, and **100% of its callers
were `tenant.(*Manager).verifyAssembly`**. At `N=1000`, at matched total work, the
same frame does not reach a single sample.

The mechanism is one line of insula code:

```go
// tenant.go:508 — once per tenant, inside Provision
e, err := m.d.Loader.Tree().Resolve(t.EntryID)
```

`Resolve` walks the path segment by segment and at each level scans that group's
children linearly (`loader.go:538-562`). A tenant is a **direct child of the
root**, so resolving one tenant scans all N of them. `Provision` resolves every
tenant it just created ⇒ **O(N²) per batch, on the provisioning path.** At the
design's own shard size of 2 000 tenants that is ~2 µs/tenant of a ~9 µs/tenant
operation.

This is the design's degradation **#5**, and note that this file used to file #5
under Gate 2 — where a *different* mechanism with a different caller is at work.
Same label, two code paths.

The fix needs no cordis change, and it is not a micro-optimisation: `createEntries`
already discards the `*Entry` that `Create` returns (`tenant.go:489`), and every
accessor the check needs is public:

```go
byID := make(map[string]*cordis.Entry, len(created))
for _, e := range m.d.Loader.Tree().Root().Children() { // one O(N) pass
    byID[e.ID()] = e
}
```

One pass per batch instead of one scan per tenant, with `ok == false` standing in
for the current "not resolvable" branch. What is *not* free is the decision: a
held `*Entry` is a claim about the tree as it was when it was created, so the
"someone removed it behind our back" case has to be re-argued rather than
inherited. That belongs in a test, not in a benchmark.

### What this used to be

Before 2026-09-27, provisioning was **quadratic**:

| N | then | now | improvement |
| --- | --- | --- | --- |
| 100 | 85 116 | 27 209 | 3.1× |
| 1 000 | 285 324 | 17 620 | 16× |
| 10 000 | 8 197 214 (82 s, 4.5 GB) | 27 818 | **295×** |

The cause was `cordis.Reflect.index` being bucketed by service *name* instead of
by isolate *realm*, so "who needs notifying" cost one pass over every realm's
subscribers. At N=10000 that single call (`candidates`) was **90.36%** of the
profile. Fixed in cordis by making the index key isomorphic to the store key; see
`AGENTS.md` for the invariant. The measured spread is now 1.58× across the upper
10× of scale instead of 96×.

## Gate 2 — teardown must not go quadratic

`BenchmarkTenantTeardownAll` deprovisions the whole batch. It is deliberately
*not* the mirror of provisioning: removing a tenant tears down a whole subtree
(fibers, effect chains, session runtimes, capability watchers), and effect
recovery is LIFO and asynchronous, so the measurement includes waiting for
convergence.

| N | ns/tenant (median of 5) | observed range | total (median) |
| --- | --- | --- | --- |
| 100 | 8 458 | 8 240 – 10 061 | 846 µs |
| 1 000 | 7 689 | 7 499 – 8 836 | 7.7 ms |
| 10 000 | 18 003 | 17 636 – 18 175 | 180 ms |

The N=100 row is the unreliable one here as well: at `-cpu 1`, same `GOGC=off`,
the same point reads 5 474 (range 5 257–6 025). Fixed per-batch cost amortised
over the fewest tenants, and the smallest batch has the least to amortise over.
The two upper rows agree across both CPUs to within 3% (7 797 / 18 434 at
`-cpu 1`), which is the part of the curve the gate is actually about.

The design's gate is phrased as: the N=1000 → N=10000 ratio should be ~10, not
~100. Measured **23.4×** — an exponent of **≈1.37** in total time, against ≈1.20
for provisioning over the same interval.

So: super-linear, but nowhere near quadratic. And "teardown is slower than
provisioning" is a conclusion rather than a defect — it buys a single `Remove`
that takes the whole subtree, instead of per-entry cleanup that would actually
leak. At N=10000 the two are in fact comparable (18.0 µs/tenant teardown against
27.8 µs/tenant provisioning, so teardown is ~39% of their sum). This file used to
say ~82% there; that figure cannot be reconstructed from any table in this file,
which is the tell that it was never measured.

### The residual has a name

One profile pair, matched at 50 000 teardowns (N=1000 × `-benchtime 50x` and
N=10000 × `-benchtime 5x`), isolates it:

| frame | N=1000 | N=10000 | ratio of sampled time |
| --- | --- | --- | --- |
| `cordis.(*Runtime).remove` | 0.01 s (0.5%) | 0.27 s (7.42%) | ~27× |

`-peek` gives the mechanism outright: **92.59% of that frame is
`runtime.typedslicecopy`**, reached from
`Registry.PluginInject.func1.(*Runtime).add.3`. Note the caller — the removal runs
through the *closure that `add` returns*, i.e. during disposal, inside the timed
window. That is what makes this one attributable to Gate 2 and not to
provisioning, and it is the check rule 5 demands.

The structural fact that makes it quadratic is in `registry.go:5`:

```go
// Runtime 同一 Plugin 的全部运行时实例集合。
type Runtime struct {
    plugin *Plugin
    fibers []*Fiber // every instance of this plugin, in the whole App
}
```

A `Runtime` is keyed by **`*Plugin`**, not by tenant. When a plugin is
instantiated once per tenant, that single slice holds one entry per tenant, and
`remove` is both a linear scan and a tail splice over it:

```go
func (rt *Runtime) remove(f *Fiber) {
    for i, cur := range rt.fibers {
        if cur == f {
            rt.fibers = append(rt.fibers[:i], rt.fibers[i+1:]...)
            return
        }
    }
}
```

Removing all N tenants is therefore O(N²) per batch — the design's defect **#2**,
confirmed rather than suspected.

Two claims this file used to carry are withdrawn here. It said the term was "not
isolated by measurement" and "a minority of the total" at N=10000. Both were read
off a `-cpu 1` profile, where the GC assist described in rule 2 *grows with N* and
swamps the share of every other frame. Under the corrected instrument the frame is
plainly visible, and one `-peek` identifies it.

### Fitting a line to the residual

The single-batch readings collapse to a two-term model:

    teardown per tenant ≈ 6.5 µs + 1.15 ns × N

The constant is the subtree teardown itself. The linear term is the slice work
above, and it is the part that matters for capacity: at the design's 2 000-tenant
shard it is ~2.3 µs, about a quarter of the operation, and at 10 000 it is ~64%.

**A warning about this instrument.** These are the most `-benchtime`-sensitive
numbers in the file. On one build at `-cpu 1`, N=1000 reads 7 380 ns/tenant under
`-benchtime 3x` and 22 696 under `-benchtime 50x` — rule 3, in full. What has
improved is the spread: the old table reports an n=1000 range of 8.6–25.2
µs/tenant (194%), the corrected one 7.5–8.8 µs/tenant (18%). Compare medians of
the *same configuration*, or you are comparing instruments, not code.

## Gate 3 — `Wait()` scan cost

`BenchmarkWaitScan` isolates the global scan inside `App.Wait()`. This matters
because `Wait` is the platform's *only* synchronisation point — every `Loader`
mutation, `Provision`, `Deprovision`, and the pre-close assembly assertion calls
it — and `settled()` walks **all** fibers in the root registry, not the pending
task count. It is O(total fibers), not O(queue length).

| N | fibers | ns/fiber (`-cpu 1`) | ns/fiber (`-cpu 4`) |
| --- | --- | --- | --- |
| 100 | 1 200 | 3.6 | 4.8 |
| 1 000 | 12 000 | 2.8 | 3.0 |
| 10 000 | 120 000 | 11.5 | 11.1 |

At 24 000 fibers this is ~84 µs — acceptable. The jump to ~11 ns/fiber at
N=10000 is a cache effect (300 MB working set), not an algorithmic change. Read
medians: a snapshot repeat occasionally lands on a 21 or a 55 ns/fiber outlier, so
the *minimum* over a handful of runs is meaningless here.

This gate is also the **control** for rule 2. It builds the same `N` tenants, so
it sits on the same live heap as Gate 2 — but it allocates nothing inside its
timed window, so no collection is triggered there and no mark assist is charged to
it. That is why its numbers are the same on both CPUs, and the same as they always
were, while Gates 1, 2 and 4 all moved. It is the confirmation that the correction
is about *allocation inside the timed window*, not about `-cpu` as a setting.

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
| 1 | 7 720 | 7 720 |
| 10 | 17 616 | 1 762 |
| 1 000 | 600 718 | **600.7** |

At K=1000 this is very stable (three runs: 594.5 / 614.0 / 600.7 ns/subscriber,
3.2% spread). The per-subscriber cost *falls* as K grows — fixed overhead
amortised — so there is no super-linear term here, which is the point of the
family.

The K=1000 figure is **25% below** the 797.7 this file used to carry. That row
was read at `-cpu 1`, and this benchmark allocates inside its timed window, so it
was paying the same mark assist as Gates 1 and 2. The *shape* — a falling
per-subscriber cost — is unchanged, and that is what the family is for.

The absolute values at K=1 and K=10 are less stable across invocation styles
(5.7–8.5 µs and 16.2–19.1 µs depending on how many snapshot repeats the framework
chose). Read the K=1000 row when comparing, not the small ones.

Note that insula uses none of cordis's `Emit`/`On`; this fanout *is* the
dependency mechanism. A capability's availability **is** its `Inject`
dependency's liveness, so this fanout is the platform's availability signal path,
and its constant factor is the price of every service change.

### Cross-realm interference (cordis's notify index)

R realms each with one pending watcher; the service is provided only in the
**last** realm. If notification cost is independent of R, the curve is flat.

Medians of three runs, `-cpu 4 -benchtime 3x`:

| R | ns/notify | then (pre-fix) | improvement | ns/per-realm |
| --- | --- | --- | --- | --- |
| 1 | 3 449 | 4 200 | 1.2× | 3 449 |
| 100 | 4 216 | 8 900 | 2.1× | 42.2 |
| 1 000 | 9 591 | 116 000 | 12.1× | 9.6 |
| 10 000 | 53 318 | 1 975 000 | **37.1×** | 5.3 |

Still not flat — but the remaining slope is **not the index**. `fanoutLastRealm`
times `DoSync` + `App.Wait()`, and `Wait` scans every fiber in the app. With R
realms each holding a watcher, the fiber count is proportional to R, so what is
left is the same O(N) as Gate 3, at roughly 5–11 ns/fiber. Per-realm cost falls
toward ~5 ns.

The practical reading: this benchmark no longer measures defect #3 (the index);
it now measures defect #1 (`Wait`). Keep them separate, or you will keep looking
for a slope that has already been removed.

Run-to-run variance here is real: R=10000 measured anywhere between 46 and 64 µs
across the corrected runs. Compare medians.

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
That number is the denominator for shard sizing: 2 000 tenants ≈ **60 MB** per
shard of pure fiber footprint, before session state, histories, and connection
pools.

This file previously printed ≈ 6 MB there. That is off by **10×**, and it
disagreed with both of its own neighbouring tables — 30 377 bytes/tenant eleven
lines above, and "10 000 tenants ≈ 300 MB" eleven lines below. It was also the
single highest-impact number in the file, because it is the one a capacity
planner copies out. Arithmetic that can be checked against the file's own tables
is worth checking.

Derived rules:

| Quantity | Value | Basis |
| --- | --- | --- |
| Fibers per tenant | 12 | measured, flat |
| Resident bytes per tenant | ~30 KB | measured, flat |
| Fibers per shard | ≤ 2 000 tenants = 24 000 | `Wait()` at 24 000 fibers ≈ 84 µs |
| Resident bytes per shard | ≤ 2 000 tenants ≈ 60 MB | 30.4 KB/tenant measured, flat |
| Teardown per tenant | ≈ 6.5 µs + 1.15 ns × N | Gate 2; at 2 000 tenants the second term is ~26% of the operation |
| Shard count | 4 – 8 | bounds blast radius and keeps `Load` `Resolve` scans short. The scheduler is single-threaded but its work is µs-scale, so sharding is not about CPU |
| 10 000 active tenants | 120 000 fibers, ~300 MB | 5 shards → 24 000 fibers each, right at the comfort limit |

Shard count and hot/cold tenant policy are the two tuning parameters here.
Everything else in this file describes invariants.

## The five O(N) degradations, and where each stands

The design listed five. Current status, measured:

| # | Location | Status |
| --- | --- | --- |
| 1 | `app.go` `settled()` — `Wait()` scans all fibers; `Loader.Load` calls `Wait` | **Open, linear and cheap.** Measured in Gate 3: ~3 ns/fiber to N=1000, ~11 ns/fiber at N=10000 (cache, not algorithm). The design predicted "tens to hundreds of ms" at 2 M fibers; the measured law gives ~22 ms. Contained by the batch rule, not fixed |
| 2 | `registry.go` `Runtime.remove` / `deleteRuntime` — slice splice | **Confirmed, and it is Gate 2's exponent.** `Runtime` is keyed by `*Plugin`, so its `fibers` slice holds *every instance of that plugin in the whole App*; `remove` scans it and splices the tail, 92.59% of the frame being `runtime.typedslicecopy`. Share of a profile at matched total work: 0.5% at N=1000, 7.42% at N=10000. Not fixed |
| 3 | `reflect.go` `untrack` — slice splice in the inverted index | **Structurally bounded.** The realm bucketing means a tenant-private service's bucket holds only that tenant's subscribers. A platform-wide service in the default realm still has one large bucket. Fixed 2026-09-27 along with the key change. Note that the pre-fix quadratic was *not* `untrack`'s splice but `candidates` scanning every realm's bucket — same index, different function, and the design named the wrong one |
| 4 | `events.go` `hooksOf` / `EmitFiltered` — copy all listeners, then filter | **Open.** Separate path from `Reflect`; insula does not use it, so it is not on the platform's hot path. A platform that does use cordis events at tenant scale must namespace event names |
| 5 | `loader.go` `Resolve` — linear scan of `g.children` | **Open, and measured on insula's own provisioning path.** `Manager.verifyAssembly` calls `Resolve` once per tenant, and tenants are direct children of the root, so each call scans all N. 5.22% of one profile at N=10000, below one sample at N=1000. This is Gate 1's ≈1.20 exponent, and the fix is insula-side — see Gate 1 |

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
- **Check a number against the file before you quote it.** Three figures here
  were wrong in the same way on 2026-09-29: the per-shard footprint (10× too
  small), the teardown share of provision+teardown (printed as ~82%, and
  reconstructible from no table in the file — the real figure is ~39%), and every
  `-cpu 1` timing in Gates 1, 2 and 4 (biased along the scale axis). All three
  were *self*-inconsistent: each was checkable against another line of this same
  file with one division. A gate document is only worth its reproduction
  commands, so when a figure here cannot be produced from them, treat the figure
  as the bug.
