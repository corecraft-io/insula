# ADR-0004 请求级隔离走 Go `context.Context`，不进 cordis

- **状态**：Accepted
- **记录日期**：2026-09-18
- **落实于**：`agent`、`edge`、`guard`、`session`

## 背景

cordis 的每次装配都要走 `effectStep` 并登记 `disposableList`，加上 `DoSync` 往返是 µs 级。这个量级对于「租户开通」是廉价的（`BENCHMARK.md` 门禁一：约 30 µs/租户），但对于「每次请求、每轮工具调用」就是致命的：调度器会变成吞吐瓶颈，而它的单线程性质意味着这个瓶颈是**全局**的。

同时，请求级状态（工具调用栈、trace、token 预算、取消信号）在语义上根本不需要 cordis：它们不因为「某个服务上下线」而需要自动装配或拆卸。把它们塞进 fiber，是给一个静态生命周期的东西付了动态生命周期的代价。

## 决定

**判断标准（这一条是本 ADR 的全部价值）：该状态是否需要因「某个服务上下线」而自动装配 / 拆卸？**

- 需要 → 进 cordis。租户级资源（模型网关、记忆存储、工具注册表、守卫策略、能力快照）与会话级资源（历史窗口、临时资源）属于这一类。
- 不需要 → 走原生 `context.Context` value 与一个 run 结构体。请求级状态属于这一类。

## 后果

**更容易：**

- 调度器只处理真正需要反应性的变更，吞吐下限有保障。
- 请求级的取消传播直接用 Go 原生机制，不需要在 cordis 里另造一套。

**更困难：**

- 平台内存在**两套「上下文」概念**（cordis `Context` 与 Go `context.Context`），必须在代码规范里写清边界，否则新人会混用。最典型的混淆是以为 `ctx.Isolate` 会影响 `context.Context` 的取用，或者反过来。
- 请求级状态无法被 cordis 的效果回收自动清理，因此每一处都必须走 `defer`。

## 落实位置

| 位置 | 体现 |
| --- | --- |
| `agent.Run(ctx, Deps, Request)` | 第一个参数是 Go 的 `context.Context`；`Deps` 里全是从 cordis 一次取来的能力句柄 |
| `agent.Request` / `agent.Result` | run 的输入输出结构体。取消、超时、trace 都走 `ctx`，不进结构体 |
| `ident.Run` | run 标识（`NewRun()`），与 `ident.Tenant` / `ident.Session` 并列但**不**进 cordis |
| `guard.Guard` / `guard.Usage` | 步数 / token 预算按 run 计数，随 run 结束即弃 |
| `session` | 会话状态**在** cordis（它有真实的装配语义：软上限触发压缩、`KeepRecent` 保留窗口）；会话内的单次请求不在 |
| `cmd/insula/demo.go` | `ctxWithTimeout()` 统一构造带超时的请求上下文，是这条纪律的示范 |

## 两套上下文的边界，一句话版本

> **cordis 的 `Context` 描述「这台机器上有哪些零件」；Go 的 `context.Context` 描述「这一次调用正在做什么」。**

前者生命周期以天计，变动由服务上下线驱动；后者生命周期以毫秒到秒计，变动由一次 HTTP 请求驱动。两者唯一合法的交集是：在请求开始时，用**一次** `DoSync` 从前者取出后者的全部所需。

## 一条可 grep 的判据

`context.Context` 在本仓库里只承担**取消与超时**，不承担数据传递：

```sh
grep -rn 'context.WithValue\|ctx.Value(' --include='*.go' . | grep -v _test.go
```

在非测试代码里**零命中**。这条 grep 是本 ADR 的守卫：一旦有人开始往 ctx 里塞东西（尤其是身份），它就变红，而那时正是要回来重读本 ADR 的时候。

请求级的数据走结构体（`agent.Request` / `agent.Result` / `caps.Invocation`），ctx 只负责「还能不能继续」。这个分工让「这个值从哪来」在签名里就看得见，而不是藏在一个运行期才成立的约定里。

## 相关

- [ADR-0001](0001-cordis-is-the-control-plane.md) —— 本条是它在请求粒度上的推论。
- [ADR-0005](0005-tenant-identity-is-an-explicit-parameter.md) —— 身份**不**走 `context.Context` value，而是显式参数。这是刻意的，理由见下一条。
