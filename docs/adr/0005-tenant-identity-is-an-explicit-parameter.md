# ADR-0005 租户身份是必传参数，不是从上下文推导的隐式值

- **状态**：Accepted
- **记录日期**：2026-09-18
- **落实于**：`memory`、`session`、`tools`、`creds`、`agent`、`edge`

## 背景

记忆、向量库、文件、审计**全部在 cordis 之外**。cordis 的隔离域管不到它们：域隔离保证的是「你不小心拿到了别人的服务实例」，而持久层的问题是「你拿着自己的实例查了别人的行」。

一旦接口设计成「从上下文里取租户」，任何一次上下文丢失或伪造都会变成跨租户数据泄漏，而且这类 bug 是**静默的、无法测试穷尽**的——它不在类型系统里，只在运行期的某条调用链上。

这正是 `context.Context` value 传递身份最诱人的时刻，也是最危险的地方（见 [ADR-0004](0004-request-scope-stays-in-go-context.md)：本仓库的非测试代码里，`context.WithValue` / `ctx.Value` **零命中**）。

## 决定

1. **所有持久层接口的签名强制包含租户标识。** 不是可选的、不是从 ctx 里 fallback 出来的，而是位置参数。
2. **存储层启用行级安全**（PostgreSQL RLS，或每租户独立 collection）作为**第二道**防线。
3. **工具参数中的身份字段一律丢弃，并由运行时覆盖。**
4. **凭证走 KMS 短期句柄**，不落盘、不进日志、不进 prompt。

## 后果

**更容易：**

- 越权在类型层面就写不出来：`Turns(t, s)` 少了 `t` 根本不编译。
- 存储层兜底让「忘了加过滤」从数据泄漏变成查询报错。

**更困难：**

- 接口签名更长、样板代码更多。
- 每租户独立 collection 会带来元数据管理的额外复杂度。

## 落实位置

| 位置 | 体现 |
| --- | --- |
| `memory.Store` | 每一个方法都首参 `t ident.Tenant`。接口注释直接写明了理由：「没有任何『按会话 ID 全局查找』的接口，因为那正是跨租户泄漏最常见的入口形态」 |
| `memory.Store.Search` | 向量检索在**本租户命名空间内部**做，不是「先检索再过滤」 |
| `memory.Store.DropTenant` | 注销是存储层的一等操作，不是删一堆 key |
| `caps.Invocation` | 工具调用的四元属性 `Tenant` / `Session` / `Run` / `Name` + `Args`，全是显式字段 |
| `agent.Request` | `Tenant` / `Session` / `Run` 是必填字段；缺失由 `agent.ErrBadIdentity` 拒绝 |
| `tools.IdentityStripper` | 键集来自 `tools.DefaultIdentityKeys()`，比较前经 `tools.NormalizeKey` 归一（`tenant_id` / `tenantId` / `TENANT-ID` 同键），在 `Registry.Call` 内部**递归**剥离 |
| `creds.StaticProvider` | 生产替换为 KMS / Secrets Manager 适配器，接口不变；`Handle` 里没有明文字段 |
| `edge` | 请求体夹带身份字段会被检测并审计（`Server.ForgedIdentityAttempts`） |

## R1 与「检测」之间的那条减法

`tools.DefaultIdentityKeys()` 之所以**导出**，是为了让接入层能做**减法**而不是重新抄一份：

接入层要检测「请求体里夹带了身份字段」，但请求体自己有 `session` 这类**合法**字段。若那边手抄一份键集，两份清单迟早会分叉，而分叉的表现是「一处挡住、另一处放过」——恰好是最难查的一类漏洞。所以键集只能有一份定义，差异必须以**显式的减法**表达：

```
RunRequest 的字段名（由 json 标签反射推导）  →  从 DefaultIdentityKeys() 里减掉
```

减数由反射推导而不是手写，是为了让「给 `RunRequest` 加字段」不会悄悄让检测失准。

## 相关

- [ADR-0003](0003-tenant-is-an-entry-subtree.md) —— 域隔离是第一道防线，本条是第二道。
- [ADR-0006](0006-untrusted-code-runs-out-of-process.md) —— 解释了为什么光有本条还不够。
- `SAFETY.md` 的硬规则 R1 与 R2 —— 本 ADR 的规范性表述。
