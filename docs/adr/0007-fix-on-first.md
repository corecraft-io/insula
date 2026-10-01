# ADR-0007 先修 O(N)，再谈规模

- **状态**：Proposed（实现部分落地，见下方「更新」）
- **记录日期**：2026-09-18
- **落实于**：`BENCHMARK.md`、`tenant`、`caps`、`cmd/insula`，以及 **cordis 侧**的索引改造

## 背景

`BENCHMARK.md` 列出五处 O(N) 退化。其中：

- **第 1 处**（`Wait` 的全量扫描，经 `Loader.Load` 暴露）藏在「看起来无害」的 `Load()` 里。一个很自然的写法——每次开通租户调一次 `Load`——会让每次开通都扫描全部 fiber。**这个成本随平台成长而增长，在几百租户的压测环境里完全测不出来，上线半年后才爆。**
- **第 2、3 处**（slice splice 导致的 O(N²) 注销）同样必然暴露，同样在小规模压测中看不出来。

它们的共同性质是：**失败是沉默的，且延迟到规模之后**。

## 决定

在任何规模承诺之前，先做这四件事：

1. **平台侧禁用每租户 `Load` / `Wait`，改为批量提交。** 规则：租户开通 / 变更 / 注销一律走 `EntryTree.Create`/`Update`/`Remove`，在一个 `DoSync` 批次里处理一批租户，`Wait` **最多每批一次**。
2. **为 `Runtime.fibers` 与 `Reflect.index` 引入墓碑压缩或改 map。**
3. **事件名租户命名空间化**（`t:<hash>/session.created`），不让人数众多的租户挂同一个 cordis 事件名。
4. **补上五个体量级基准并进 CI 作为门禁。**

## 后果

**更容易：**

- 性能特征从「随规模暗中劣化」变成「有回归测试守住的线性」。
- 规模上限变成可计算量（见 `BENCHMARK.md` 的容量一节）。

**更困难：**

- ②③ 需要改 **cordis 本体**，会触及已通过的测试与语义约定（如 `Registry.Runtimes()` 的返回顺序、依赖通知顺序），必须逐条回归。
- ③ 在一个不使用 cordis 事件的平台上不适用——但那条纪律仍然要在文档里留着，因为下一个使用者可能会用。

## 落实位置

| 项 | 位置 |
| --- | --- |
| ① 批量提交 | `tenant.Manager.Provision` / `Deprovision`（整批一次 `DoSync` + 一次 `Wait`）；`BENCHMARK.md` 的 `BenchmarkWaitScan` 是这条纪律的**回归守卫** |
| ② 索引 | cordis `reflect.go`：`index` 改为 `map[isolateKey][]*Fiber` |
| ④ 基准 | `tenant/tenant_bench_test.go`（`BenchmarkTenantProvision` / `TenantTeardownAll` / `WaitScan` / `FiberFootprint`）+ `caps/caps_bench_test.go`（`BenchmarkEventFanout`，两族：`in_realm` / `cross_realm`） |
| ④ CI | `.github/workflows/ci.yml` |
| 显式报告 | `cmd/insula demo` 第 6、7 步把自检覆盖与指标计数打出来，让「门禁有没有真的跑」可见 |

## 更新

### 2026-09-27

四项的逐条现状：

| 项 | 现状 |
| --- | --- |
| ① 批量提交 | **已落地。** `Manager.Provision` / `Deprovision` 对整批只做一次 `DoSync` + 一次 `Wait`。`BenchmarkWaitScan` 单独量那次扫描（n=10000 时约 11 ns/fiber） |
| ② 索引 | **部分落地。** `Reflect.index` 改为按隔离域分桶（键与 `store` 同构）。**`Runtime.fibers` 仍是 slice splice，未改** |
| ③ 事件命名空间化 | **在 insula 上不适用。** insula 一行 cordis 的 `Emit`/`On` 都没用（可 grep 验证），因此这条通路的 O(N) 不在平台热路径上。纪律保留在 `BENCHMARK.md` 的第 4 处退化里 |
| ④ 基准进 CI | **已落地。** 六个基准 + `demo` 冒烟进 `ci.yml`；独立的 `bench` job 便于单跑 |

### ② 为什么不只是「改成 map」

原计划是「墓碑压缩或改 map」。实际做法是**换桶键**，因为实测剖面显示问题不在容器的常数，而在**键的语义**：

`index` 原本按服务**名**分桶，于是「谁需要被通知」必须先取全量再逐个用隔离域过滤。这让一次域内服务变更的代价正比于**全部域**的订阅者总数。

修法的判据是一句话：**索引的键必须与 `store` 同构**——问「该通知谁」的键，必须与解析会命中的键是同一个。改成 `map[isolateKey][]*Fiber` 之后：

| N | 修复前 ns/租户 | 修复后 | 改善 |
| --- | --- | --- | --- |
| 100 | 85 116 | 29 378 | 2.9× |
| 1 000 | 285 324 | 30 821 | 9.3× |
| 10 000 | 8 197 214（82 秒、4.5 GB） | 47 986 | **171×** |

跨域通知：R=10000 时 1 975 µs → 72 µs（27.4×）。

**这条改动同时修掉了一个潜伏 bug**：`internal/service` 事件的载荷原本用 `getImpl(provider, name)` 取，没有校验「解析到的实现是否真由该 provider 注册」。组件在派生上下文里 `Isolate` 之后再 `Provide` 时，注册键 ≠ 自身上下文的键，于是监听器收到 `nil`。改成查注册记录 `r.store[key]`。

### 为什么状态仍是 Proposed

`Runtime.fibers` 的 slice splice（第 2 处）还在，而它正是 `BenchmarkTenantTeardownAll` 那条 ≈N^1.31 曲线的可疑来源。这条 ADR 的验收条件是「五处退化都有界或被修」，现在只完成了其中最有杀伤力的一处。

**建议：把 ② 的 `Runtime.fibers` 一项做完，再转 Accepted。** 在那之前，分片（[ADR-0002](0002-app-shard-pool.md)）是这些 O(N) 的**缓解**手段，不是修复。

### 2026-09-29 —— ② 与门禁二的实测归因

第 ② 项在本条 ADR 里一直挂着「可疑来源」四个字，没有被测量隔离出来。现在隔离出来了，
结论与原文的猜测**在机制上一致、在优先级上相反**。

**先修仪器。** 门禁二此前的读数取自 `-cpu 1`。`GOMAXPROCS=1` 时回收器没有第二个 `P` 可以
挂标记，于是计时窗口内触发的每一次标记协助都由被测 goroutine 自付，而账单正比于活跃堆、
即正比于 `N`——偏差恰好沿规模轴增长。同一个 N=1000 的注销点在 `-cpu 1`+GC 开下读作
25 149 ns/租户，`GOGC=off` 下读作 7 704；同一构建、同一 `N`、`alloc/op` 完全相同。
按 §「原文不动，行数会漂移」的惯例，本 ADR 不修改上方 09-27 表里已记录的读数，
只在此声明其口径，并给出按修正配置重取的一组：

| N | 修复前 | 09-27 读数（`-cpu 1`、`3x`、3 次） | 09-29 重取（`GOGC=off`、`1x`、5 次） |
| --- | --- | --- | --- |
| 100 | 85 116 | 29 378 | 27 209 |
| 1 000 | 285 324 | 30 821 | 17 620 |
| 10 000 | 8 197 214（82 秒、4.5 GB） | 47 986 | 27 818 |

**机制被确认。** 配平总工作量的一对剖面（两边各 50 000 次注销：N=1000 × `50x` 与
N=10000 × `5x`）里，`cordis.(*Runtime).remove` 在 N=1000 时占 0.5%，N=10000 时占 7.42%；
`-peek` 显示其中 **92.59% 是 `runtime.typedslicecopy`**，调用者是
`Registry.PluginInject.func1.(*Runtime).add.3`——也就是**回收期**，在计时窗口之内。
**第 2 处的 splice 就是门禁二的成因**，其指数按重取数据为 **≈1.37**（09-27 记录为 ≈1.31，
形状一致）。

原文有两句判断在此撤回：「没有被测量隔离出来」、「在 N=10000 处只占总量的一小部分」。
两句都读自一个 `-cpu 1` 剖面，而那里 GC 协助随 `N` 增长，把其它每一帧的份额都压了下去。

成立的结构前提是 `Runtime` 的**键**：

```go
// Runtime 同一 Plugin 的全部运行时实例集合。
type Runtime struct {
    plugin *Plugin
    fibers []*Fiber // 整个 App 里该插件的全部实例
}
```

`Runtime` 按 `*Plugin` 键，**不按租户**。当某插件每租户实例化一份时，这一个 slice 里就是
N 条，于是 `remove` 对它既线性扫描、又整尾拼接。此前本 ADR 判断「这个机制不成立」——
那个判断是错的。

**但优先级要调。** 同一次测量还给出另一项。`cordis.(*EntryTree).Resolve` 在 N=10000 的
剖面里占 5.22%，在 N=1000 时不到一个采样。按调用者读，它 100% 来自
`tenant.(*Manager).verifyAssembly`，而那里位于 **`Provision` 内部**——是被 `StopTimer`
排除在计时之外、但仍在剖面之内的开通阶段工作：

| 退化 | 落在哪道门禁 | 调用者 | N=10000 剖面份额 |
| --- | --- | --- | --- |
| 第 2 处 `Runtime.remove` | 门禁二（注销） | `(*Runtime).add` 返回的闭包 | 7.42% |
| 第 5 处 `EntryTree.Resolve` | 门禁一（开通） | `Manager.verifyAssembly` | 5.22% |

第 2 处必须改 **cordis 本体**；而第 5 处的修法**完全在 insula 侧**，量级却与第 2 处相当。
`verifyAssembly` 对每个租户解析一次路径，而租户是根的直接子节点，所以每次解析都扫过全部
N 个。`createEntries` 本来就把 `Create` 返回的 `*Entry` 丢掉了，而 `Root().Children()`
与 `Entry.ID()` 都是公开 API，一趟 O(N) 建成 ID→Entry 映射即可（不免费的部分是语义：
攥住的 `*Entry` 是「创建那一刻的树」的断言，「有人背后把它删了」那一路要重新论证）。

**状态仍是 Proposed，但下一步换了。** 原文的建议是「把 ② 的 `Runtime.fibers` 一项做完，
再转 Accepted」。按上面的份额，**先做第 5 处更划算**：不动 cordis、不需要新 ADR、量级相同。
第 2 处仍要修——它是 cordis 的公共缺陷，别的使用者会撞上——但它不再是转 Accepted 的唯一
前置。

验收条件不变：五处退化都有界或被修。现在其中两处有实测份额、有调用者、有已确认的机制。

### 2026-09-30 —— 第 5 处已修

09-29 那次调序（先做第 5 处而不是第 2 处）已经执行完毕。**第 5 处退化的修复落在
insula 侧**，位置 `tenant.(*Manager).verifyAssembly`：原先每租户调一次
`Tree().Resolve`，现在改为每批建一张 `Root().Children()` → `*Entry` 映射后查表。

与 09-29 段末那句建议有一处不同，写下来以免下次读的人以为忘了：

> 那句建议的原话是「`createEntries` 本来就把 `Create` 返回的 `*Entry` 丢掉了……
> 攥住它即可」。实际没有攥那个返回值，而是从 `Root().Children()` 建表。

理由就是这条修法里唯一不免费的那部分：**攥住的 `*Entry` 是「创建那一刻的树」
的断言**。若有人在我们背后摘掉这棵子树（并发 `Ensure` / `Deprovision` 撞在同一短 ID 上），
那个指针照样答得出 `Subgroup() != nil`，于是装配校验**放行**，一个已不存在的租户会被登记
为可用。改用当前根子节点的快照，则「查不到」精确继承旧 `Resolve` 报 `ErrEntryNotFound`
的语义——依然是响亮失败的 `ErrShortAssembly`，不用重新论证。

实测（`GOGC=off`、`1x`、`-count 5`、`-cpu 4`，5 次中位，完整口径见 `BENCHMARK.md` 门禁一）：

| N | 修复前 | 修复后 | 改善 |
| --- | --- | --- | --- |
| 100 | 29 854 | 28 731 | 1.04× |
| 1 000 | 17 143 | 17 088 | 1.00× |
| 10 000 | 27 549 | 19 297 | **1.43×** |

1000 → 10000 的比值 **1.61× → 1.13×**，即总耗时指数 **≈N^1.21 → ≈N^1.05**。配平工作量的
一对剖面（`-benchtime 5x`、N=10000）里，`EntryTree.Resolve` 与 `verifyAssembly` 双双跌到
不足一个采样（修复前 cum 8.61% / 9.36%），腾出的份额被清理路径的 `EntryTree.Remove` 吸收。

有三样东西为这次替换兜底：

1. `TestAssemblyCheckIsEquivalentToResolve`（新建）钉住「快照里的 `*Entry` 就是 `Resolve`
   会给的那个」三个支点：同一指针、单段直接子节点、重复 ID 取配置序第一个。**这条测试守的
   是 cordis 的语义承诺，不是这次改动本身**——它要让 cordis 侧的静默变更先炸在测试里。
2. 既有的 `TestProvisionDetectsSilentlySkippedChildEntry` 继续守住那条唯一的失败路径：
   装配不完整 ⇒ `ErrShortAssembly` ⇒ 回滚。变异验证做过了：把映射的键改坏，tenant 包立刻
   有 4 条以上测试变红，且 warn 明确指向新分支。
3. `allocs/租户`（~628）与 `B/租户`（~40.9 KB）基本未动 —— 前后读数可比的前提成立：
   工作量没变，只有时间变了。

**状态：仍是 Proposed。** 验收条件是「五处退化都有界或被修」，现在第 1、3 处有界，第 5 处
已修，剩下第 2 处（`Runtime.remove` 的 slice splice，注销指数 ≈1.37）与第 4 处
（cordis 事件 `hooksOf`，insula 不用）未修。第 2 处要改 cordis 本体。

下一步：按 09-29 段的排序，第 2 处是剩下的唯一 insula 之外的阻塞项。它现在是**注销**这条
路径上唯一的超线性来源，也是本 ADR 转 Accepted 的前置——建议在 cordis 侧开工时连带把
「`Runtime` 是否应该继续按 `*Plugin` 键」这个问题一并论证。

## 相关

- [ADR-0001](0001-cordis-is-the-control-plane.md) —— 本条的代价来源。
- [ADR-0002](0002-app-shard-pool.md) —— 缓解手段。
- `BENCHMARK.md` —— 四道门禁、五处退化的现状、以及取数纪律（含一次把噪声误判成回归的教训）。
- `AGENTS.md` —— 「索引的键必须与 store 同构」这条不变量的操作说明。
