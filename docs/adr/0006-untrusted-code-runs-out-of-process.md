# ADR-0006 不可信代码必须进程外执行

- **状态**：Accepted
- **记录日期**：2026-09-18
- **落实于**：`sandbox`、`guard`、`tools`

## 背景

cordis 是进程内组件库。隔离域解决的是**「服务名撞车」**，不解决**「恶意代码越权」**——同进程的 Go 代码可以经反射、`unsafe`、全局单例、共享的 `*http.Client` 或共享日志器绕过一切域校验。Go 语言层面无法阻止这一点。

把这两件事混为一谈，是自建 agent 平台最常见的一处自我欺骗：以为「有隔离域所以安全」，于是在同一进程里跑用户上传的工具代码。

## 决定

三级信任模型，边界与手段一一对应：

| 等级 | 典型来源 | 隔离手段 |
| --- | --- | --- |
| 一级 · 可信 | 平台自研插件、核心 agent 循环、模型网关适配器 | 进程内 + 私有隔离域 + 代码评审 |
| 二级 · 半可信 | 第三方工具适配器、企业内部团队贡献的插件 | 进程内 + 域隔离 + **能力最小化**（`Inject` 只声明真正需要的服务）+ 出口审计 |
| 三级 · 不可信 | 用户上传的工具代码、任意代码执行类工具、外部 MCP 服务 | **必须进程外**（gRPC / WASM），无凭证、无默认网络出口，身份由宿主侧 `tool-proxy` 注入 |

三级这条规则**不是政策开关**：它对应的是「进程内无法阻止恶意 Go 代码」这个语言事实。

## 后果

**更容易：**

- 安全边界与信任等级一一对应，可审计、可向客户解释。
- 二级的评审有抓手：问「这个插件声明了哪些 `Inject`？它拿到了 `models` 或凭证句柄吗？」就够了。

**更困难：**

- 进程外调用引入序列化开销与运维复杂度：沙箱生命周期、资源配额、崩溃隔离。
- 工具接口需要重新设计成 RPC 形态。

## 落实位置

| 位置 | 体现 |
| --- | --- |
| `sandbox.Tier` | `TierTrusted` / `TierSemi` / `TierUntrusted`，`String()` 便于日志与配置 |
| `sandbox.Tier.AllowsInProcess` | 三级一律 `false`，且**不提供覆盖选项**。注释写明理由 |
| `sandbox.Manager` / `NewManager(inProcess, outProcess)` | 按等级分派执行通道；`UntrustedTier` 是可调的阈值 |
| `sandbox.Closed` | **`outProcess` 为 nil 时的默认落点。** 未配置进程外通道时，不受信的调用**必须失败**，而不是退回进程内 |
| `sandbox.SetOutOfProcess` | 装配期注入真实沙箱（gRPC / WASM 适配器由使用方提供）；传 nil 也落到 `Closed` |
| `sandbox.Executor` / `Runner` / `Direct` | 进程内通道与它的可测替身；`Direct` 有 `DefaultMaxOutputBytes`（256 KiB）截断 |
| `sandbox.Manager.Describe` | 各通道的描述，供启动日志与**能力清单评审** |

## 这条决定里最容易被做反的一处

「没有配置沙箱」时的默认行为。直觉写法是 `if outProcess == nil { use inProcess }`——一个"降级"、看起来更健壮的选择。

那正是最坏的选择：**它把「忘记配置沙箱」变成一个静默的安全降级**，而且只在不受信代码真正到来的那一刻生效，测试环境里永远不会触发。所以 `NewManager` 的默认是 `Closed`，`SetOutOfProcess(nil)` 也是 `Closed`，错误里把原因写清楚：

> `no out-of-process sandbox configured; untrusted tools are disabled by default`

失败要响亮、要在装配期就能被 `Describe()` 看见，而不是在第一次恶意调用时才发现。

## 本仓库不做的部分

`sandbox` 定义的是**接口、等级分派、输出截断与默认失败**，没有沙箱运行时。进程外执行器（gRPC / WASM）由使用方提供。这一点在 `SAFETY.md` 的「本仓库不做什么」里也列了——它是这条 ADR 的边界，不是遗漏。

## 更新

- **`Tier.String()` 的实际用途收窄为「只便于日志」。** 落实位置表里写的是「便于日志与配置」，但本仓库没有任何一处把字符串解析回 `Tier` 的路径：`sandbox.Config` / `NewManager` 收的是 `Tier` 值本身，没有 `ParseTier` 一类的入口。原文里的「与配置」是当时的一句顺带预期，现在明确撤掉——留着一个不存在的用法描述，下一个人会去找那个解析函数，找不到之后要么写一个、要么以为配置这条路坏掉了。（符号是 `Tier.String`，不是行号；行号会漂移，符号不会。）
- 由此补了两条**对外词汇**测试：`TestTierVocabularyIsStable`（同 ADR 的 `gateway.State.String` 另有一条）。这类字符串落在启动日志、能力清单评审与告警文本里，是事实上的对外接口，却因为「测试通过时没人格式化它」而**覆盖率天然是 0** —— 改错了不会有任何东西变红。测试同时钉住「不同等级不得显示成同一个词」，那才是运维真正会踩的坑。

## 相关

- [ADR-0003](0003-tenant-is-an-entry-subtree.md) —— 域隔离的分工边界在哪里。
- [ADR-0005](0005-tenant-identity-is-an-explicit-parameter.md) —— 「身份由宿主侧代理注入」与本条的二级/三级配套。
- `SAFETY.md` —— 三级信任模型与硬规则 R3 / R4 的规范表述。
