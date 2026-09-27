package gateway

import (
	"fmt"
	"sync"
	"time"

	"github.com/metaRobin/insula/ident"
)

// Quota 是单租户在一个滚动窗口内的模型用量上限。
//
// 限的是 **token 与调用次数**，不是请求速率——速率由接入层的令牌桶管
// （见 admit 包）。两处限的是不同的东西：令牌桶挡的是「打得太快」，
// 额度挡的是「用得太多」。轮次全在毫秒级但单次请求 200 万 token 的租户，
// 令牌桶一点都拦不住。
type Quota struct {
	// MaxTokens 窗口内输入+输出 token 之和的上限（<=0 表示不限）。
	MaxTokens int64
	// MaxCalls 窗口内调用次数上限（<=0 表示不限）。
	MaxCalls int64
	// Window 滚动窗口长度（<=0 取默认 1 分钟）。
	Window time.Duration
}

func (q Quota) window() time.Duration {
	if q.Window <= 0 {
		return time.Minute
	}
	return q.Window
}

// Usage 一次窗口内的用量读数。
type Usage struct {
	TokensIn  int64
	TokensOut int64
	Calls     int64
	// WindowEnd 当前窗口的结束时刻。
	WindowEnd time.Time
}

// Total 返回输入+输出 token 之和。
func (u Usage) Total() int64 { return u.TokensIn + u.TokensOut }

// ErrQuotaExceeded 租户的模型额度已用尽。
//
// 这是**拒绝**而不是排队：额度用尽的租户继续排队只会把分片调度器
// 变成一个大缓冲池，最终所有租户一起变慢（见设计 §8「拒绝优于排队」）。
type ErrQuotaExceeded struct {
	Tenant ident.Tenant
	Quota  Quota
	Used   Usage
	Reason string
}

func (e *ErrQuotaExceeded) Error() string {
	return fmt.Sprintf("insula/gateway: tenant %s exceeded model quota: %s (used %d tokens / %d calls)",
		e.Tenant, e.Reason, e.Used.Total(), e.Used.Calls)
}

// Account 是单租户的额度账户。
type Account struct {
	mu          sync.Mutex
	quota       Quota
	windowStart time.Time
	used        Usage
	clock       func() time.Time
}

// NewAccount 构造租户账户。
func NewAccount(q Quota, clock func() time.Time) *Account {
	if clock == nil {
		clock = time.Now
	}
	return &Account{quota: q, clock: clock}
}

// Quota 返回账户当前额度。
func (a *Account) Quota() Quota {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.quota
}

// SetQuota 调整额度。已在窗口内用掉的量不清零——提额立即生效，
// 降额也立即生效（下一次 reserve 就会失败），不做「窗口结束才生效」的缓冲。
func (a *Account) SetQuota(q Quota) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.quota = q
}

// Usage 返回当前窗口的用量读数。
func (a *Account) Usage() Usage {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rollLocked()
	u := a.used
	u.WindowEnd = a.windowStart.Add(a.quota.window())
	return u
}

// rollLocked 在窗口到期时重置计数。调用方必须持锁。
func (a *Account) rollLocked() {
	now := a.clock()
	w := a.quota.window()
	if a.windowStart.IsZero() {
		a.windowStart = now
		return
	}
	if now.Sub(a.windowStart) >= w {
		// 整窗重置。不做滑动的精度补偿：滑窗的收益在额度场景下
		// 抵不上它带来的实现复杂度与解释成本。
		a.windowStart = now
		a.used = Usage{}
	}
}

// Reserve 预占一次调用的额度。
//
// 调用次数在调用前就能确定，因此在这里扣；token 数只有拿到响应才知道，
// 所以走 Commit。两段式是必要的：如果等响应回来才发现超额度，
// 这次调用的钱已经花了。
func (a *Account) Reserve() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rollLocked()
	if q := a.quota.MaxCalls; q > 0 && a.used.Calls >= q {
		return &ErrQuotaExceeded{Quota: a.quota, Used: a.used, Reason: "call limit reached"}
	}
	a.used.Calls++
	return nil
}

// Commit 记入一次已完成的调用的 token 用量。
//
// 它**不返回错误**：token 已经消耗掉了，此时拒绝只是把「欠账」
// 藏起来。超额通过下一条 Reserve 的失败来体现，同时这里把用量
// 如实记上，让它出现在指标与审计里。
func (a *Account) Commit(tokensIn, tokensOut int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rollLocked()
	a.used.TokensIn += tokensIn
	a.used.TokensOut += tokensOut
}

// Exhausted 报告当前窗口是否已触顶 token 额度（供调用前快速短路）。
func (a *Account) Exhausted() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rollLocked()
	q := a.quota
	if q.MaxCalls > 0 && a.used.Calls >= q.MaxCalls {
		return true
	}
	if q.MaxTokens > 0 && a.used.Total() >= q.MaxTokens {
		return true
	}
	return false
}
