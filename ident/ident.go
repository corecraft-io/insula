// Package ident 定义平台的租户/会话/运行标识类型。
//
// # 为什么单独成包
//
// 这是纯类型叶子包，零依赖。原因是一个真实的导入环：memory（存储）与
// caps（能力接口）都需要在签名里携带租户身份，而 tenant（租户子系统）
// 又需要引用 caps 与 memory；身份类型若定义在 tenant 里，就会形成
// tenant → caps → memory → tenant 的环。
//
// 把身份类型提升为叶子包后，依赖方向变成单向：
//
//	ident ← memory ← caps ← tenant
//	ident ← guard / metrics / audit / creds / admit / sandbox
//
// 命名上对齐 deepseek-harness 的 identity 包（同为「平台主体标识」之意）。
//
// # 为什么身份是强类型
//
// ADR-005 要求「租户身份是必传参数，不是从上下文推导的隐式值」。
// 强类型让这条规则在编译期生效：任何试图省略租户标识的存储/工具/网关
// 调用都写不出来，而不是等到某次上下文丢失时才变成跨租户泄漏。
// 类型本身无法阻止空值，因此各包另有 fail-closed 校验（见 memory.Store）。
package ident

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"sync/atomic"
	"time"
)

// Tenant 一个租户的稳定标识（对应平台的 tenant / workspace / organization）。
type Tenant string

// Session 一个会话的稳定标识，作用域限于所属租户。
type Session string

// Run 一次 agent 运行（一轮用户请求触发的完整循环）的标识，用于追踪与审计。
type Run string

// String 返回租户标识的字符串形式。
func (t Tenant) String() string { return string(t) }

// String 返回会话标识的字符串形式。
func (s Session) String() string { return string(s) }

// String 返回运行标识的字符串形式。
func (r Run) String() string { return string(r) }

// Valid 报告租户标识是否非空。
//
// 空值必须被视为编程错误而不是「无租户」：多数存储实现无法表达
// 「无租户」这一状态，静默接受空值只会把它变成全局命名空间。
func (t Tenant) Valid() bool { return t != "" }

// Valid 报告会话标识是否非空。
func (s Session) Valid() bool { return s != "" }

// Valid 报告运行标识是否非空。
//
// 接入层必须在入口处拒绝空的 run 标识：它是审计里唯一能把"哪一次请求"
// 串起来的键，允许空值等于允许一条无法追溯的记录。
func (r Run) Valid() bool { return r != "" }

// runSeq 是降级路径上的进程内序号。
var runSeq atomic.Uint64

// NewRun 生成一个新的运行标识。
//
// 用 crypto/rand 而不是计数器或时间戳：run 标识会出现在日志与审计里，
// 可预测的标识让"伪造一条属于别人的审计记录"变成可能，而审计一旦
// 被污染就没法事后分辨哪条是真的。
//
// 平台自己生成、**绝不接受客户端指定**——这与"租户身份由凭据决定"
// 是同一条规则：任何"调用方说自己是谁"的字段都是越权入口。
func NewRun() Run {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 环境级故障，极罕见（新版本 Go 的 crypto/rand 已不会返回错误）。
		// 退回"时间戳 + 进程内序号"：仍然唯一、仍然够用，但**可预测**，
		// 所以名字里明写着 fallback，不要在日志里把它当成正常情况。
		n := runSeq.Add(1)
		return Run("r-fallback-" +
			strconv.FormatInt(time.Now().UnixNano(), 36) + "-" +
			strconv.FormatUint(n, 36))
	}
	return Run("r-" + hex.EncodeToString(b[:]))
}
