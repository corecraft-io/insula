// Package admit 是平台的第一道闸门：租户级速率、租户级并发、平台级总量。
//
// # 设计取向：拒绝优于排队
//
// 超限立刻返回类型化错误（接入层映射为 429），请求不进入任何队列。
// 排队会把一次瞬时超载转成持续的延迟劣化——队列里的请求大多注定超时，
// 而它们仍然占着连接与内存。
//
// # 为什么必须有两层限额
//
// 租户级配额防止单租户作恶；平台级总量防止「每个租户都合规，但一起打满」。
// 只有前者时，1000 个守规矩的租户同时发起请求照样会抽干共享的模型池与
// 数据库连接池。两层都要有，且都是非阻塞获取。
//
// # 与 cordis 的关系
//
// 本包不接触 cordis。cordis 的任务队列是无界 slice（为对齐 JS 事件循环
// 语义、避免单任务内投递自死锁），它**没有背压**；把无界队列当成缓冲池
// 会先 OOM 再停摆。背压必须由本包在进入调度器之前提供。
package admit

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/metaRobin/insula/ident"
)

// Reason 说明拒绝的原因，供接入层映射状态码与指标标签。
type Reason string

const (
	// ReasonRate 租户级速率超限。
	ReasonRate Reason = "tenant_rate"
	// ReasonConcurrency 租户级并发超限。
	ReasonConcurrency Reason = "tenant_concurrency"
	// ReasonPlatformRate 平台级速率超限。
	ReasonPlatformRate Reason = "platform_rate"
	// ReasonPlatformConcurrency 平台级并发总量超限。
	ReasonPlatformConcurrency Reason = "platform_concurrency"
)

// LimitError 准入拒绝。用 errors.As 取出后可按 Reason 分类。
type LimitError struct {
	Reason Reason
	Tenant ident.Tenant
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("insula/admit: %s limit exceeded for tenant %q", e.Reason, e.Tenant)
}

// ErrClosed 准入器已关闭（分片正在停机）。
var ErrClosed = errors.New("insula/admit: closed")

// AsLimit 提取 LimitError。第二个返回值为 false 表示错误并非准入拒绝。
func AsLimit(err error) (*LimitError, bool) {
	var le *LimitError
	if errors.As(err, &le) {
		return le, true
	}
	return nil, false
}

// Limits 单个租户的配额。
type Limits struct {
	// RatePerSecond 每秒补充的令牌数（0 表示不限速）。
	RatePerSecond float64
	// Burst 令牌桶容量，决定瞬时突发上限（RatePerSecond 为 0 时忽略）。
	Burst int
	// MaxConcurrent 同时进行的 run 数上限（0 表示不限）。
	MaxConcurrent int
}

// Config 准入器配置。
type Config struct {
	// Default 新租户的默认配额。
	Default Limits
	// PlatformMaxConcurrent 平台级并发 run 总量（0 表示不限）。
	PlatformMaxConcurrent int
	// PlatformRatePerSecond 平台级速率（0 表示不限）。
	PlatformRatePerSecond float64
	// PlatformBurst 平台级令牌桶容量。
	PlatformBurst int
	// TenantOverrides 按租户覆盖默认配额（如企业版租户）。
	TenantOverrides map[ident.Tenant]Limits
	// Clock 可注入时钟，便于测试；nil 表示 time.Now。
	Clock func() time.Time
}

// Platform 是准入器。零值不可用，必须经 New 构造。
type Platform struct {
	defaults  Limits
	overrides map[ident.Tenant]Limits

	mu       sync.Mutex
	buckets  map[ident.Tenant]*bucket
	inflight map[ident.Tenant]int
	platform *bucket
	platConc int // 当前平台级并发数
	platMax  int
	closed   bool

	clock func() time.Time
}

// New 构造准入器。
func New(cfg Config) *Platform {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	p := &Platform{
		defaults:  cfg.Default,
		overrides: cfg.TenantOverrides,
		buckets:   make(map[ident.Tenant]*bucket),
		inflight:  make(map[ident.Tenant]int),
		platMax:   cfg.PlatformMaxConcurrent,
		clock:     clock,
	}
	if cfg.PlatformRatePerSecond > 0 {
		burst := cfg.PlatformBurst
		if burst <= 0 {
			burst = 1
		}
		p.platform = newBucket(cfg.PlatformRatePerSecond, float64(burst), clock)
	}
	return p
}

// Permit 一次成功准入的凭据，必须被释放。重复释放无副作用。
type Permit struct {
	p       *Platform
	tenant  ident.Tenant
	release sync.Once
}

// Release 归还并发额度。必须调用，通常 defer 在请求处理的最外层。
func (pm *Permit) Release() {
	if pm == nil || pm.p == nil {
		return
	}
	pm.release.Do(func() { pm.p.release(pm.tenant) })
}

// Acquire 非阻塞地申请一次运行许可。
//
// 获取顺序：平台速率 → 租户速率 → 平台并发 → 租户并发。任一步失败
// 都会回滚此前已占用的额度，因此被拒绝的请求不会泄漏任何配额——
// 这一点有专门的测试守着（拒绝路径泄漏会让额度随错误率单调流失）。
//
// 回滚这条规则对**速率令牌**同样成立，而它比并发计数更容易被漏掉：
// 令牌被消耗后看上去"用掉了就用掉了"，但它是一条会单调流失的额度。
// 具体规则是一句话——**因并发上限被拒的请求不消耗速率令牌**：
//
//   - 一个卡在并发上限的客户端会在每次重试时被拒。若这些拒绝也扣令牌，
//     它几毫秒内就会把桶烧空，于是下一句收到的拒绝原因从真话（并发满）
//     变成假话（速率超限），并被 Retry-After 按秒级拖住——而它实际上
//     只要等 10ms 就会有一个槽位空出来。两个限额互相干扰，报出来的
//     恰好是错的那个。
//   - 平台级令牌还多一层后果：一个租户的重试会收紧**全局**速率，
//     把别的租户一起限住，而它自己一个请求都没跑成。
func (p *Platform) Acquire(t ident.Tenant) (*Permit, error) {
	if !t.Valid() {
		return nil, fmt.Errorf("insula/admit: empty tenant identity")
	}
	now := p.clock()
	lim := p.limitsFor(t)

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrClosed
	}

	var (
		platSpent bool
		tb        *bucket
	)
	if p.platform != nil {
		if !p.platform.allow(now) {
			return nil, &LimitError{Reason: ReasonPlatformRate, Tenant: t}
		}
		platSpent = true
	}

	tb = p.bucketFor(t, lim, now)
	if tb != nil && !tb.allow(now) {
		// 速率本身就是这次拒绝的原因：令牌是被这次请求正当消耗掉的，
		// 退回去的话重试将永远不被限速。平台令牌则要退——它记的是
		// "平台实际扛下的吞吐"，而这次请求并没有跑到那一步。
		if platSpent {
			p.platform.refund()
		}
		return nil, &LimitError{Reason: ReasonRate, Tenant: t}
	}

	// 从这里往下都是并发判定。它们不消耗速率令牌，所以在拒绝时把
	// 上面已经消耗的退回去（规则见函数注释）。
	refund := func() {
		if platSpent {
			p.platform.refund()
		}
		if tb != nil {
			tb.refund()
		}
	}

	if p.platMax > 0 && p.platConc >= p.platMax {
		refund()
		return nil, &LimitError{Reason: ReasonPlatformConcurrency, Tenant: t}
	}
	if lim.MaxConcurrent > 0 && p.inflight[t] >= lim.MaxConcurrent {
		refund()
		return nil, &LimitError{Reason: ReasonConcurrency, Tenant: t}
	}

	p.platConc++
	p.inflight[t]++
	return &Permit{p: p, tenant: t}, nil
}

func (p *Platform) release(t ident.Tenant) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := p.inflight[t]; n > 1 {
		p.inflight[t] = n - 1
	} else {
		delete(p.inflight, t)
	}
	if p.platConc > 0 {
		p.platConc--
	}
}

// Close 关闭准入器：此后所有 Acquire 返回 ErrClosed。
// 已在途的 Permit 不受影响，其 Release 仍然有效。
func (p *Platform) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
}

// Stats 返回可用于指标的快照。
type Stats struct {
	TenantsTracked int
	PlatformInUse  int
	PlatformMax    int
	InflightPerTen map[ident.Tenant]int
}

// Stats 返回准入器的当前状态。
func (p *Platform) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	per := make(map[ident.Tenant]int, len(p.inflight))
	for t, n := range p.inflight {
		per[t] = n
	}
	return Stats{
		TenantsTracked: len(p.buckets),
		PlatformInUse:  p.platConc,
		PlatformMax:    p.platMax,
		InflightPerTen: per,
	}
}

// SetLimits 覆盖某租户的配额（套餐变更）。已占用的并发额度不受影响。
func (p *Platform) SetLimits(t ident.Tenant, lim Limits) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.overrides == nil {
		p.overrides = make(map[ident.Tenant]Limits)
	}
	p.overrides[t] = lim
	delete(p.buckets, t) // 让新速率立即生效，而不是等旧桶自然消耗
}

func (p *Platform) limitsFor(t ident.Tenant) Limits {
	if lim, ok := p.overrides[t]; ok {
		return lim
	}
	return p.defaults
}

// Forget 丢弃某租户的全部准入状态（配额覆盖与令牌桶）。
//
// 与 gateway.Pool.Forget、memory.MemStore.DropTenant 是同一条纪律：
// 任何"按租户建索引"的进程级结构，如果只加不减，就是一条无界增长路径。
// 这里的增长速度取决于平台形态——按租户 ID 累积，租户一生一次，
// 在一个以"开通/注销"为核心操作的平台上，那正好等于"永不回收"。
//
// 只删桶，不动 inflight：桶是纯粹的速率状态，删掉只影响下一次请求
// 的起算点；而 inflight 是**并发额度的账**。若连它一起删，一个正在
// 注销的租户只要还有请求在途，就会被放行超过 MaxConcurrent 个新请求
// ——把一次清理变成一次限额绕过。inflight 归零时自己会删。
//
// 调用时机应当是租户注销的**最后一步**（注销动作本身已完成、不再有
// 新请求需要为它限速）。本仓库里由 shard 在注销时调用。
func (p *Platform) Forget(t ident.Tenant) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.buckets, t)
	delete(p.overrides, t)
}

func (p *Platform) bucketFor(t ident.Tenant, lim Limits, now time.Time) *bucket {
	if lim.RatePerSecond <= 0 {
		return nil
	}
	b, ok := p.buckets[t]
	if !ok {
		burst := lim.Burst
		if burst <= 0 {
			burst = 1
		}
		b = newBucket(lim.RatePerSecond, float64(burst), p.clock)
		// 新桶以满容量开始，避免首次请求被限流。
		b.tokens = float64(burst)
		b.last = now
		p.buckets[t] = b
	}
	return b
}

// bucket 是经典的令牌桶。所有访问都在 Platform.mu 下，
// 因此自身不加锁（这也是它能保持极低开销的原因）。
type bucket struct {
	tokens float64
	last   time.Time
	rate   float64
	burst  float64
	clock  func() time.Time
}

func newBucket(rate, burst float64, clock func() time.Time) *bucket {
	return &bucket{tokens: burst, last: clock(), rate: rate, burst: burst, clock: clock}
}

func (b *bucket) allow(now time.Time) bool {
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// refund 归还一个令牌：把一次"因别的原因被拒"的请求从速率账上摘掉。
//
// 上限是 burst——归还不能凭空创造额度。归的还是刚借走的那一个，
// 因此正常情况下不会真的撞到上限。
func (b *bucket) refund() {
	b.tokens++
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}
