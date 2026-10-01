package gateway_test

import (
	"testing"
	"time"

	"github.com/corecraft-io/insula/gateway"
	"github.com/corecraft-io/insula/ident"
)

// TestQuotaExceededErrorVocabulary 钉住额度拒绝的对外文案。
//
// 这条消息是租户与运维唯一能看到的东西（它会被回给调用方、写进审计）。
// 它与 Tier.String / State.String 属同一类：落在日志与告警文本里，是
// 事实上的对外接口，却因为"测试通过时没人格式化它"而覆盖率天然是 0 ——
// 改错了不会有任何东西变红。
//
// 单独钉一条是因为两种超额（token / 调用次数）共用同一个类型，
// 消息里的 Reason 是运维分辨它们的唯一依据。
func TestQuotaExceededErrorVocabulary(t *testing.T) {
	err := &gateway.ErrQuotaExceeded{
		Tenant: ident.Tenant("t-a"),
		Quota:  gateway.Quota{MaxTokens: 1000, MaxCalls: 10},
		Used:   gateway.Usage{TokensIn: 600, TokensOut: 400, Calls: 7},
		Reason: "call limit reached",
	}

	const want = "insula/gateway: tenant t-a exceeded model quota: call limit reached (used 1000 tokens / 7 calls)"
	if got := err.Error(); got != want {
		t.Fatalf("ErrQuotaExceeded.Error()\n got = %q\nwant = %q", got, want)
	}

	// token 数必须是**输入+输出之和**：只报其中一半会让对账对不上
	// ——租户看到"用了 600"，而账单上是 1000。
	other := *err
	other.Used = gateway.Usage{TokensIn: 600, TokensOut: 0, Calls: 7}
	if other.Error() == err.Error() {
		t.Error("TokensOut 没有进消息：token 总数必须是输入+输出之和")
	}

	other = *err
	other.Reason = "token limit reached"
	if other.Error() == err.Error() {
		t.Error("换一个 Reason 必须是另一条消息：运维靠它分辨是哪种超额")
	}
}

// TestAccountExhaustedShortCircuitsOnTokenQuota 守住 token 维度的快速短路。
//
// Exhausted 是调用前的短路闸门（gateway.go 的 Complete 里第一件事）。
// MaxCalls 那半在有别的测试守着，MaxTokens 这半此前**只被读过一半**：
// 覆盖剖面显示它的 `return true` 从未执行过。漏掉它的后果是"调用次数
// 没用完但 token 早就超了的租户"会一直被放行到上游。
func TestAccountExhaustedShortCircuitsOnTokenQuota(t *testing.T) {
	clk := newClock()
	// 只限 token，不限调用次数——正是被漏掉的那条路径。
	acct := gateway.NewAccount(gateway.Quota{MaxTokens: 100, Window: time.Minute}, clk.Now)

	if acct.Exhausted() {
		t.Fatal("一次都没用过就不该触顶")
	}

	acct.Commit(60, 39) // 合计 99
	if acct.Exhausted() {
		t.Fatal("99 < 100 不该触顶")
	}

	acct.Commit(1, 0) // 合计 100，正好触顶
	if !acct.Exhausted() {
		t.Fatal("达到 MaxTokens 必须触顶（边界含等号）")
	}

	// 窗口滚过之后必须放开：额度是**窗口内**的量。
	clk.Advance(2 * time.Minute)
	if acct.Exhausted() {
		t.Fatal("窗口滚过后用量已清零，不该再触顶")
	}
}

// TestZeroQuotaMeansUnlimited 守住 `<=0` 这个约定在两处是一致的。
//
// Quota 的文档写着 "<=0 表示不限"，而 Exhausted 里写的是 `q.MaxTokens > 0`。
// 若哪天有人把它改成 `>= 0`，零额度会从"不限"翻转成"全拒"——而那个
// 默认值恰恰是没配额度时的取值，也就是所有租户一起停摆。
func TestZeroQuotaMeansUnlimited(t *testing.T) {
	acct := gateway.NewAccount(gateway.Quota{}, newClock().Now)
	acct.Commit(1<<40, 1<<40)
	acct.Reserve()
	acct.Reserve()

	if acct.Exhausted() {
		t.Fatal("MaxTokens / MaxCalls 为 0 表示不限，不该触顶")
	}
}

// TestNewAccountDefaultsClock 守住零时钟的兜底。
//
// clock 为 nil 时不兜底会在第一次 Reserve 时以空指针 panic，而那条
// 路径只在"调用方没给时钟"时走到——生产装配给了，测试夹具忘了给。
func TestNewAccountDefaultsClock(t *testing.T) {
	acct := gateway.NewAccount(gateway.Quota{MaxCalls: 1}, nil)

	if err := acct.Reserve(); err != nil {
		t.Fatalf("首次预占应当成功: %v", err)
	}
	if err := acct.Reserve(); err == nil {
		t.Fatal("超过 MaxCalls 必须被拒")
	}
	if u := acct.Usage(); u.WindowEnd.IsZero() {
		t.Error("WindowEnd 为零值说明时钟没有兜底")
	}
}
