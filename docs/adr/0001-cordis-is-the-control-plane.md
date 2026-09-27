# ADR-0001 cordis 只做控制面，数据面跑在 Go 原生 goroutine 上

- **状态**：Accepted
- **记录日期**：2026-09-18
- **落实于**：`cmd/insula`、`agent`、`caps`、`gateway`、`edge`

## 背景

`cordis.App` 只有唯一一个调度 goroutine，且用户回调（`Plugin.Apply`、`Dispose`、事件监听器）**天然运行在其中**——见 `App` 的文档注释：「所有 Context/Fiber 上的 API 必须在调度器 goroutine 内调用……外部 goroutine 一律通过 `Do` / `DoSync` 进入」。

这条性质是 cordis 复刻 JS 事件循环语义的前提，也是它代价的来源：**任何 I/O 阻塞都会冻结整个分片的装配、卸载与服务发布**。而 agent 平台的数据面天然是 I/O：模型调用是长尾的（秒级），工具执行可能是任意长，流式输出是持续的。

把模型调用写进 `Apply` 里，等于一个租户的慢上游把同片所有租户的控制面一起冻住。

## 决定

cordis 负责**装配、依赖解析、生命周期、效果回收**。LLM 调用、工具执行、检索、流式输出全部在**独立 goroutine** 上执行，由租户级信号量与超时约束。

能力句柄在每次 run 开始时经**一次** `DoSync` 一次取走（实测单次控制面往返 3.79 µs），此后整个 run 不再回调 cordis。

## 后果

**更容易：**

- 慢租户不再影响他人。分片的控制面延迟可预测（µs 级），不随上游抖动。
- 分片 CPU 需求极低，容量瓶颈落在真正的 I/O 侧，而不是调度器上。

**更困难：**

- 需要一层薄薄的「能力句柄」抽象把 cordis 与执行面解耦。这层抽象就是 `caps` 包存在的理由，它必须一次性交出全部所需能力，而不是按需回调。
- 跨请求的状态更新必须经 `App.Do` 异步投递，因此带来**最终一致性**：平台要接受「摘要落库略有延迟」这类语义，并在文档里说清，而不是让调用方自己发现。

## 落实位置

| 位置 | 体现 |
| --- | --- |
| `agent/` | 主循环跑在调用方的 goroutine 上，`Deps` 里全是已经取到手的句柄 |
| `caps/` | 句柄类型 `Handle` / `Snapshot`：可用性由 `Inject` 依赖的存活状态表达，取用是一次性的 |
| `cmd/insula/demo.go` | `runOnce` 用带超时的 `context.Context` 驱动真实 HTTP 请求，验证数据面不占调度器 |
| 容量数据 | `App.Wait()` 的 O(全部 fiber) 扫描是本条决定的**成本**：见 `BENCHMARK.md` 门禁三 |

## 相关

- [ADR-0004](0004-request-scope-stays-in-go-context.md) —— 请求级隔离走 Go `context.Context`，是本条在「请求」粒度上的推论。
- [ADR-0002](0002-app-shard-pool.md) —— 分片限制的是本条决定的故障半径。
- [ADR-0007](0007-fix-on-first.md) —— 本条的代价（`Wait` 的全量扫描）就是那个要「先修」的 O(N)。
