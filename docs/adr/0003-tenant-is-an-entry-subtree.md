# ADR-0003 租户 = 入口子树 + 强制私有域 + 命名空间化 ID

- **状态**：Accepted
- **记录日期**：2026-09-18
- **落实于**：`realm`、`tenant`、`shard.SelfCheck`

## 背景

两条来自 cordis 的、都会**静默失败**的性质，在这个决定里同时被处理：

1. **`EntryOptions.ID` 全树唯一，而 `reconcile` 遇到重复 ID 只记日志然后跳过。** 同时 `EntryTree.store` 以短 ID 为扁平索引键，寻址却用 `group:child` 路径。于是「两个租户各自有一个叫 `db` 的子入口」这种看似自然的写法，会构成一条**跨租户误路由**路径——不报错，只是解析到别人。
2. **忘记声明 `Isolate` 会让服务落到默认域**，而默认域等于全平台共享。这是一个「漏写一行就静默变成跨租户共享」的默认值。

两者都是「写错了不会报错」的形态，靠代码评审记是记不住的。

## 决定

租户在 cordis 中的表示固定为：

1. **一棵 `EntryGroup` 子树。**
2. **`Isolate` 只声明在租户根入口上**，全子树经上下文父链继承（子入口的 ctx 由分组 fiber 的 ctx 派生）。
3. **所有入口 ID 强制租户命名空间前缀**：`<scope>-<hash(tenant)>[-<seq>]`。
4. **租户开通走 `EntryTree.Create`（重复即报错），不走 `reconcile`（静默跳过）。**
5. **平台侧禁用共享域标签**：`Isolate` 的值只允许 `true`。

## 后果

**更容易：**

- 一条 `Isolate` 声明覆盖整棵子树，不会漏。
- ID 冲突从「静默误路由」变成「快速失败」。

**更困难：**

- ID 不再人类可读，运维排查需要一张 tenant → hash 的映射表。
- 必须为负向隔离写回归测试并进 CI——这条决定的价值恰恰体现在「没出事的那些情况」，所以它只能靠断言守住，不能靠观察发现。

## 落实位置

| 位置 | 体现 |
| --- | --- |
| `realm.Tenant()` / `realm.Session()` | 全平台**仅有**的两处 `Isolate` 字面量，都硬编码 `true` |
| `realm.Services()` / `SessionServices()` | 租户级 5 个服务（`models`/`memory`/`tools`/`guard`/`caps`）+ 会话级 2 个（`history`/`scratch`）的完整清单，供断言与日志 |
| `realm.Hasher.TenantEntry(id)` / `Child` / `SessionEntry` | ID 的命名空间化生成 |
| `realm.CheckTenantEntry` / `CheckChildEntry` | 生成之外的**校验**：验证一个既有 ID 是否合规 |
| `realm.AssertIsolated(a, b, name)` / `SameInstance` | 负向断言：两个上下文同名服务必须**不是**同一个实例 |
| `realm.CheckIsolateDeclarations(entry)` | 声明层断言：遍历一棵入口子树，**任何**不是字面 `true` 的 `Isolate` 值都被拒（见下方「更新」） |
| `shard.Pool.SelfCheck` | 抽样哨兵，抓「构造被破坏」这一类回归（限制见 [ADR-0002](0002-app-shard-pool.md)）。第 5 条决定的执行点是它的声明层 |
| `tenant.Manager.Provision` | 建子树 → 声明域 → 注册 |

守住这条决定的既有断言（隔离类断言一律配**变异验证**：临时破坏实现，确认变红，还原）：

| 测试 | 断言什么 |
| --- | --- |
| `realm.TestTenantIsolateCoversEveryService` | `realm.Services()` 里**每一个**服务都被 `Tenant()` 声明为私有域。漏一个就变红——这正是「漏写一行」的抓取点 |
| `realm.TestSessionIsolateIsNarrowerThanTenant` | 会话域比租户域更细一层，且**不覆盖**租户级服务 |
| `realm.TestEntryIDsCarryTenantNamespace` | ID 带命名空间前缀 |
| `realm.TestCheckRejectsForeignAndMalformedIDs` | 别家租户的 ID 与畸形 ID 都被拒 |
| `shard.TestEachShardHasItsOwnApp` | 两片不共用 `App`（对应「跨分片靠不同 `Reflect` 实例」） |
| `shard.TestCapabilitiesAreShardLocal` / `TestShardRuntimeIsolatesSessions` | 能力句柄与会话都是片内、租户内独立的 |
| `shard.TestSelfCheckReportsSamplingHonestly` | 自检如实报告抽样覆盖（配合上面的第 1 条限制） |
| `realm.TestSharedRealmLabelDetectsStrings` | 共享域谓词本身有判别力（此前它只被喂过 `true`，判别分支从未执行） |
| `realm.TestCheckIsolateDeclarations{AcceptsPrivateRealms,RejectsEveryNonPrivateValue,WalksSubgroups}` | 声明扫描的正向、负向（字符串 / `false` / `nil` / 错误类型）、以及递归进子入口 |
| `tenant.TestProvisionedSubtreeDeclaresOnlyPrivateRealms` | **真实**租户子树里每一条声明都是字面 `true`，且租户根逐字挂上 `realm.Tenant()` |
| `tenant.TestOnlyTenantRootsAndSessionsDeclareIsolate` | 除租户根外，只有会话入口可声明 `Isolate`，且只能是会话级服务 |
| `tenant.TestSessionsGroupCarriesNoIsolate` | 会话分组**不带**声明（写在分组上会让全部会话塌进同一个域） |
| `shard.TestSelfCheckFailsOnSharedRealmLabel` | 手工塞一条共享域声明 → 自检失败、指名入口、计入 `IsolationBreaches` |

## 为什么 ID 用哈希而不是租户名

用租户名做 ID 看起来更可读，但会把租户标识泄漏进入口 ID，而入口 ID 会出现在日志、错误消息、树 dump 里——那是一条不必要的泄漏面。哈希是带密钥的（`realm.NewHasher(secret)`），因此外部无法从 ID 反推租户，也无法枚举租户的 ID 空间。

代价是运维要多一张映射表。这个代价是明码标价的，不是意外。

## 更新

### 2026-09-29 · 第 5 条决定补上运行时执行点

第 5 条决定（禁用共享域标签）此前只有一半守住：`realm.Tenant()` /
`realm.Session()` 两张映射的内容被断言了，但**入口实际声明了什么**没有任何断言。
于是下面每一条都能在全绿的情况下溜过去：

- 某个子入口的 `EntryOptions.Isolate` 写了 `"@shared"`（cordis 的 `realmKey`
  会老实地建成共享域，两个租户静默拿到同一个实例）；
- 某处写了 `false`（把自己挪出外层域，效果同样是共享）；
- 租户根忘了挂 `realm.Tenant()`（整棵子树落回默认域）。

同时 `realm.SharedRealmLabel` 被 SAFETY 写成了"自检用它断言子树里没有字符串声明"，
但它**没有任何生产调用方**，且唯一调用它的测试先把值断言成了 `true`——
把它改成恒返回 `false`，303 条测试全绿。

现在补上两层：

1. `realm.CheckIsolateDeclarations` 遍历子树，拒绝任何不是字面 `true` 的值
   （比"是不是字符串"更严，因为 `false` 与其它类型同样会共享）。
2. `shard.Pool.SelfCheck` 对每个租户跑一遍，条数报成
   `SelfCheckReport.Declarations`。没有这个计数，「跑了且干净」与「没跑」
   在报告里同形——与 `PairsChecked` 是同一条纪律。

这与「构造保证优先于运行时检查」不冲突：构造仍是**主**保证（唯一字面量来源 +
覆盖断言），声明扫描是**哨兵**，与 `SelfCheck` 整体处于同一位置。

## 相关

- [ADR-0005](0005-tenant-identity-is-an-explicit-parameter.md) —— 持久层的身份必传参数，与本条配合构成「域隔离 + 存储层兜底」两道防线。
- [ADR-0006](0006-untrusted-code-runs-out-of-process.md) —— 说明了域隔离**不是**安全边界，因此本条后面还需要别的防线。
