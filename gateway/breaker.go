package gateway

import (
	"sync"
	"time"
)

// State 熔断器状态。
type State int

const (
	// StateClosed 正常放行。累计失败达阈值后转 Open。
	StateClosed State = iota
	// StateOpen 直接拒绝，不再打到上游。冷却期结束后转 HalfOpen。
	StateOpen
	// StateHalfOpen 放行**单个**探测请求：成功则回 Closed，失败则重回 Open。
	// 只放一个是有意的——放一批就等于把恢复期的压力又打回去。
	StateHalfOpen
)

// String 便于日志与断言。
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Breaker 是模型池共用的熔断器。
//
// 它是**池级**而不是租户级的：上游模型服务挂了是对所有租户一起挂的，
// 按租户各熔一次只会让第一个撞墙的租户承担探测成本，其余租户继续
// 把请求打进一个确定失败的上游。反过来说，配额必须是租户级的——
// 这两件事的粒度不同，不能合并成一个。
type Breaker struct {
	threshold  int
	cooldown   time.Duration
	clock      func() time.Time
	onChange   func(from, to State)
	mu         sync.Mutex
	failures   int
	openedAt   time.Time
	halfFlight bool // HalfOpen 期间是否已有探测在飞
}

// BreakerConfig 熔断器参数。
type BreakerConfig struct {
	// Threshold 连续失败阈值（<=0 取默认 5）。
	Threshold int
	// Cooldown 打开后的冷却时长（<=0 取默认 10s）。
	Cooldown time.Duration
	// Clock 注入时钟。
	Clock func() time.Time
	// OnChange 状态迁移回调，用于打点与审计。它在持锁状态下被调用，
	// 因此不得回调进 Breaker，也不得做耗时操作。
	OnChange func(from, to State)
}

// NewBreaker 构造熔断器。
func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.Threshold <= 0 {
		cfg.Threshold = 5
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 10 * time.Second
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &Breaker{
		threshold: cfg.Threshold,
		cooldown:  cfg.Cooldown,
		clock:     cfg.Clock,
		onChange:  cfg.OnChange,
	}
}

// State 返回当前状态（顺带完成冷却到期后的 Open → HalfOpen 迁移）。
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stateLocked(b.clock())
}

func (b *Breaker) stateLocked(now time.Time) State {
	if b.openedAt.IsZero() {
		return StateClosed
	}
	if now.Sub(b.openedAt) >= b.cooldown {
		return StateHalfOpen
	}
	return StateOpen
}

// Allow 报告当前是否放行一次调用，并**占用**放行额度。
//
// 之所以把「判断」和「占用」合成一个原子操作：HalfOpen 下只允许一个
// 探测请求，分成两步就会有两个 goroutine 同时看到「可以放行」。
// 调用方在失败路径上必须调用 Failure 归还探测额度，否则熔断器会
// 卡在 HalfOpen 且再也放不出探测请求。
//
// 注意这里**没有**「冷却已过就顺手推进状态」的分支：stateLocked 已经
// 把冷却到期的 Open 报成 HalfOpen 了，在它返回 StateOpen 的分支里
// 再判一次冷却只可能是永不成立的死代码。Open → HalfOpen 的迁移
// 在下面 HalfOpen 分支里首次放行探测时报出。
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.stateLocked(b.clock()) {
	case StateOpen:
		return false
	case StateHalfOpen:
		if b.halfFlight {
			return false
		}
		b.halfFlight = true
		// 把「已从 Open 走出来」这件事报出去，供审计与指标使用。
		b.transition(StateOpen, StateHalfOpen)
		return true
	default:
		return true
	}
}

// Success 上报一次成功。HalfOpen 下探测成功即闭合；Closed 下清零连续失败。
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	from := b.stateLocked(b.clock())
	b.halfFlight = false
	b.failures = 0
	b.openedAt = time.Time{}
	if from != StateClosed {
		b.transition(from, StateClosed)
	}
}

// Failure 上报一次失败。达阈值则打开；HalfOpen 下探测失败立即重回 Open
// 并重新计时（而不是立刻再给一次探测机会，否则恢复期会持续放行）。
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock()
	from := b.stateLocked(now)
	b.halfFlight = false
	b.failures++
	if from == StateHalfOpen || b.failures >= b.threshold {
		b.openedAt = now
		if from != StateOpen {
			b.transition(from, StateOpen)
		}
	}
}

// transition 记录一次状态迁移。调用方必须持锁。
func (b *Breaker) transition(from, to State) {
	if from == to || b.onChange == nil {
		return
	}
	b.onChange(from, to)
}

// Failures 返回当前连续失败计数，供指标上报。
func (b *Breaker) Failures() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failures
}

// Reset 强制回到 Closed（供运维手工恢复）。
func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	from := b.stateLocked(b.clock())
	b.failures = 0
	b.openedAt = time.Time{}
	b.halfFlight = false
	b.transition(from, StateClosed)
}
