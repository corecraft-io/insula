# ADR-0002 App 分片池，而不是单 App 多隔离域

- **状态**：Accepted
- **记录日期**：2026-09-18
- **落实于**：`shard`、`insula.Open`、`realm.Hasher`

## 背景

单个 `cordis.App` 加 `Isolate` 隔离域，在**语义上**已经能隔离租户——这正是 cordis README 的推荐用法，也是本项目公理「每个租户都是一个域」的直接来源。

但它有两个无法回避的问题：

1. **所有租户共用一个调度 goroutine。** 一个租户的插件死循环或 OOM 会拖垮全平台。域隔离挡的是「拿到别人的实例」，挡不住「占满唯一那个线程」。
2. **cordis 的几个全局结构都在 App 内是单份的**：`Reflect.store`、`Reflect.index`、`Registry.order`、`Events.hooks`。十万租户共用一个 map，而且 `BENCHMARK.md` 里那几处 O(N) 扫描没有上界——`Wait()` 要遍历全部 fiber，`Loader.Resolve` 要线性扫 `g.children`。

第 2 点尤其重要：它在小规模压测里完全看不出来，而它的成本随平台成长而增长。

## 决定

N 个 `cordis.App`（建议 4–8），租户按 `hash(tenantID) % N` **粘性**路由，每片承载约 2000 个活跃租户。

跨分片的全局资源（模型池、向量库连接池、准入器、指标、审计）用**进程内共享单例**，在 cordis 里只注册瘦句柄。

## 后果

**更容易：**

- 故障爆炸半径变成 1/N。
- `Wait()` 与 `Resolve()` 的 O(N) 被压到可接受量级——分片不是为了让 CPU 更快（调度器单线程但工作量是 µs 级），而是为了给这些扫描一个上界。
- 装配吞吐提升 N 倍。

**更困难：**

- 分片数变更需要租户迁移，会话上下文会丢，必须能持久化重建。
- 跨租户的全局操作（比如「给所有租户推配置」）变成 N 次广播。
- 粘性路由成了必须维护的组件，而且要维护得**诚实**：数据面用 `Pool.For`（哈希就是粘性的定义，离位租户响亮地报 `ErrNoTenant`），管理面用 `Pool.Find`（注销是破坏性操作，必须作用在租户真正所在的分片）。给数据面加 `Find` 兜底会把错误的手工装配永久掩盖。

**可逆性：** 这条是**可逆**的。租户规模小时先用 1 片（即单 App）跑；分片是横向扩展的开关，不是前提。

## 落实位置

| 位置 | 体现 |
| --- | --- |
| `shard/shard.go` | `New(Config)` 建池；`Pool.For` / `Find` / `Stats` / `SelfCheck`；粘性哈希 |
| `realm/realm.go` | `NewHasher(secret).Sum(id)` —— 哈希带密钥，租户 ID 到分片号不可被外部枚举 |
| `insula.Open` | 按 `Config.Shards` 构造 N 个 App；进程级单例只构造一份并交给每个分片 |
| `cmd/insula` | `serve -shards` / `demo -shards`（默认 4 / 2） |

### 自检的两个已知限制

`Pool.SelfCheck` 是**抽检**，不是穷尽比对。片内穷尽比较是 O(N²·S)：2000 个租户就是 200 万对，乘上服务数就是两千万次 `Get`。启动自检不能这么做，而「每隔一会儿跑一次」的更慢版本也没必要——**平台级隔离不靠自检保证，靠构造保证**（租户入口子树在私有域；跨分片根本是不同的 `Reflect` 实例）。自检要抓的是「构造被破坏」这一类回归，而这类破坏一旦发生，抽检几乎必然命中。

想穷尽就把 `Config.SelfCheckPairs` 调大，报告里的 `Exhaustive` 会告诉你这次覆没覆盖全。两个如实记录的限制：

1. **`PairsChecked == 0` 是歧义的。** 分片数多于租户数时没有哪片里有两个租户，于是没有任何可比对的对；但「自检压根没跑」也报 0。`Pool.Checked()` 累计的是**比较过的对数**，不是调用次数，因此分辨不了这两种情况。要分辨得另加一个调用计数器。（*该歧义已于 2026-10-01 解决，见下方「更新」——`SelfCheckReport` 新增 `Calls` 字段，`Pool` 新增累计 `calls` 计数器与 `Calls()` 访问器。*）
2. **跨分片那部分是哨兵比对**，各取一个租户。它对片内破坏不敏感，只抓「两片意外共用了同一个 App」这类装配错误。

详见 `SAFETY.md` 的「本仓库不做什么」。

## 更新

**2026-09-27** —— 背景里的第 2 点已部分消解，但本决定不变：

- `Reflect.index` 改为按隔离域分桶后，单次服务通知的代价不再正比于「全部域的订阅者」。实测见 `BENCHMARK.md` 门禁四：R=10000 时从 1 975 µs 降到 72 µs。
- 分片仍然必要：`App.Wait()` 的 O(全部 fiber) 扫描、`Loader.Resolve` 的 `g.children` 线性扫描、`Runtime.remove` 的 slice splice 都还在（`BENCHMARK.md` 里第 1、2、5 处退化）。
- 分片数的**理由**因此变得更清晰了：它不是为了掩盖索引的键设计错误，而是为了让「每片的总 fiber 数」和「每片的 root children 数」有上界。

**2026-10-01** —— 限制 #1（`PairsChecked == 0` 歧义）已解决：

- `SelfCheckReport` 新增 `Calls int` 字段：每次 `SelfCheck` 进入正常执行路径即记 1（池已关闭等早退路径记 0）。它与 `PairsChecked`（累计比较过的对数）分开计。
- `Pool` 新增累计 `calls atomic.Int64` 字段与 `Calls() int64` 访问器，与既有的 `checked`/`Checked()`（累计对数）平行。
- 现在 `PairsChecked == 0` 不再歧义：`Calls == 0` 表示「没跑」，`Calls > 0` 表示「跑了但无对可比」（分片数 > 同片租户数时的正常状态）。`cmd/insula/demo` 第 7 步据此把三种状态分开打印，不再依赖跨查 metrics。
- 测试新增 `TestSelfCheckCallsDisambiguatesNoPairsFromNotRun` 锁死该区分；`TestSelfCheckWithTooFewTenants` / `TestSelfCheckPassesForProvisionedPool` 补了 `Calls` 断言。

## 相关

- [ADR-0003](0003-tenant-is-an-entry-subtree.md) —— 分片之内，租户的表示形式。
- [ADR-0007](0007-fix-on-first.md) —— 本条是那些 O(N) 的**缓解**手段，不是修复。
