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

## 相关

- [ADR-0001](0001-cordis-is-the-control-plane.md) —— 本条的代价来源。
- [ADR-0002](0002-app-shard-pool.md) —— 缓解手段。
- `BENCHMARK.md` —— 四道门禁、五处退化的现状、以及取数纪律（含一次把噪声误判成回归的教训）。
- `AGENTS.md` —— 「索引的键必须与 store 同构」这条不变量的操作说明。
