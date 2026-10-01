# 架构决策记录（ADR）

English | [中文](README.zh.md)

This directory holds Insula's architecture decision records. An ADR is a decision
that was **non-obvious**, that was made **once**, and that will otherwise be
re-argued or silently reversed. The test is not "is this important" — it is "will
someone later wonder why it is this way, or undo it without noticing".

## Format

`NNNN-<slug>.md`, four digits, never reused, never renumbered. Each record uses
the same five sections:

```markdown
# ADR-NNNN <title>

- 状态：Accepted | Proposed | Superseded by ADR-NNNN | Deprecated
- 记录日期：YYYY-MM-DD
- 落实于：<packages>

## 背景
Why a decision was needed. What property of the world (or of cordis) forced it.

## 决定
The decision, stated as an instruction, not as a discussion.

## 后果
What gets easier. What gets harder. The "harder" half is mandatory — an ADR with
no stated cost is a sales document.

## 落实位置
Where in the code this decision is visible. Symbol names, not line numbers:
line numbers drift, symbols do not.

## 更新
Optional, dated. What changed since the decision. Never rewrite the original
text — the value of an ADR is partly that it records what was believed at the
time.
```

**Line numbers drift; symbols do not.** Cite functions and types, not
`file.go:123`.

## Index

| # | Decision | Status |
| --- | --- | --- |
| [0001](0001-cordis-is-the-control-plane.md) | cordis 只做控制面，数据面跑在 Go 原生 goroutine 上 | Accepted |
| [0002](0002-app-shard-pool.md) | App 分片池，而不是单 App 多隔离域 | Accepted |
| [0003](0003-tenant-is-an-entry-subtree.md) | 租户 = 入口子树 + 强制私有域 + 命名空间化 ID | Accepted |
| [0004](0004-request-scope-stays-in-go-context.md) | 请求级隔离走 Go `context.Context`，不进 cordis | Accepted |
| [0005](0005-tenant-identity-is-an-explicit-parameter.md) | 租户身份是必传参数，不是从上下文推导的隐式值 | Accepted |
| [0006](0006-untrusted-code-runs-out-of-process.md) | 不可信代码必须进程外执行 | Accepted |
| [0007](0007-fix-on-first.md) | 先修 O(N)，再谈规模 | Accepted |

## How the seven relate

They are not independent. Three pairs of hand-offs are worth knowing before
reading them individually:

- **0001 → 0002 → 0007.** 0001 puts the data plane off the scheduler, which makes
  the control plane's O(N) scans its only performance liability. 0002 bounds that
  liability by sharding. 0007 is the acknowledgement that bounding is not fixing,
  plus the list of what to fix.
- **0003 → 0005 → 0006.** 0003 gives each tenant a private realm. 0005 adds the
  second line of defence at the storage layer, because realms are a **correctness**
  guarantee, not a security boundary. 0006 is the honest conclusion of that
  observation: for untrusted code, in-process isolation is not a boundary at all,
  so it must leave the process.
- **0004 ↔ 0005.** They pull in opposite directions and that is deliberate. 0004
  says request-scope state lives in Go `context.Context`; 0005 says identity does
  **not**. The split is: `ctx` carries "may I continue", structs carry "who am I".

## Terminology

This repository is written in two languages and that is not incidental. ADR
records and code comments are in Chinese, because the reasoning they record was
done in Chinese and translating it would let the vocabulary drift from the code
it describes. `README`, `SAFETY`, `BENCHMARK` and `AGENTS` are paired
(`X.md` + `X.zh.md`) because they address two different audiences.

Symbol names, commit messages, and this index are English.

## Related documents

- [`SAFETY.md`](../../SAFETY.md) — the normative statement of the four hard rules
  that ADR-0003, 0005 and 0006 implement.
- [`BENCHMARK.md`](../../BENCHMARK.md) — the gates and numbers ADR-0007 refers to.
- [`AGENTS.md`](../../AGENTS.md) — the invariants a contributor must not break,
  which are the code-level shadows of these decisions.
