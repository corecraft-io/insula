// Package guard 是单次 run 的资源预算：步数、工具调用数、token 总量、墙钟。
//
// # 为什么这四样都要限
//
// agent 循环的失控有四种典型形态，它们触发的限制各不相同：
//
//   - 模型在两个工具之间来回振荡 → 步数
//   - 模型反复调用同一个工具（比如一直重试一个失败的命令） → 工具调用数
//   - 上下文越滚越大，每步都在重复喂入全部历史 → token 总量
//   - 单次工具调用挂死不返回 → 墙钟
//
// 只限其中任何一个都会留下另外几条路。最容易被忽略的是**工具调用数**：
// 步数限制看起来已经够了，但一步里可以有多次工具调用。
//
// # 它只报告，不中断
//
// Guard 不做抢占——Go 里没有办法安全地杀死一个 goroutine。它的作用是让
// 循环在**每一个可中断点**上主动检查并优雅退出（返回已经拿到的部分结果），
// 而不是等整次 run 超时。真正的强制中断由接入层用 context 截止时间实现
// （Deadline 就是为此提供的）。
//
// 两者是互补的：Guard 负责"体面地停"，context 负责"无论如何都要停"。
package guard

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/metaRobin/cordis"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/realm"
)

// Dimension 是被触发的限制维度。
type Dimension string

const (
	DimensionSteps     Dimension = "steps"
	DimensionToolCalls Dimension = "tool_calls"
	DimensionTokens    Dimension = "tokens"
	DimensionWallClock Dimension = "wall_clock"
)

// ErrBudgetExhausted 预算用尽。
//
// 调用方应当把它当作**正常终止条件**而不是故障：模型可以基于已经
// 获得的部分结果给出一个诚实的答复（"我在完成前用尽了预算"），
// 这比返回一个内部错误对用户有用得多。
var ErrBudgetExhausted = errors.New("insula/guard: budget exhausted")

// Exceeded 描述一次预算越界。
type Exceeded struct {
	Tenant    ident.Tenant
	Session   ident.Session
	Run       ident.Run
	Dimension Dimension
	Limit     int64
	Used      int64
}

func (e *Exceeded) Error() string {
	return fmt.Sprintf("insula/guard: run %s exceeded %s budget: used %d of %d",
		e.Run, e.Dimension, e.Used, e.Limit)
}

// Is 让 errors.Is(err, ErrBudgetExhausted) 对 *Exceeded 成立。
func (e *Exceeded) Is(target error) bool { return target == ErrBudgetExhausted }

// Budget 一次 run 的预算。
//
// 每个字段取 0 表示该维度**不限**。这看起来像是个危险默认，但它是必要的：
// 平台不知道租户的合理上限，硬编码一个默认值只会让"忘配"变成"配错"。
// 真正的防线是 New 的调用方必须显式给出预算——装配层用 Default 提供
// 一组保守值，测试用零值表示"我不关心这一维"。
type Budget struct {
	MaxSteps     int
	MaxToolCalls int
	MaxTokens    int64
	WallClock    time.Duration
}

// Default 返回一组保守的默认预算，供装配层使用。
func Default() Budget {
	return Budget{
		MaxSteps:     24,
		MaxToolCalls: 64,
		MaxTokens:    128 * 1024,
		WallClock:    5 * time.Minute,
	}
}

// Usage 当前消耗读数。
type Usage struct {
	Steps     int
	ToolCalls int
	Tokens    int64
	Elapsed   time.Duration
}

// Guard 跟踪一次 run 的预算消耗。它不是并发安全的抽象，
// 但内部加了锁：工具调用可能被并行发起，读数必须是干净的。
type Guard struct {
	tenant  ident.Tenant
	session ident.Session
	run     ident.Run

	budget Budget
	start  time.Time
	clock  func() time.Time

	mu        sync.Mutex
	steps     int
	toolCalls int
	tokens    int64
}

// New 构造预算跟踪器。
func New(t ident.Tenant, s ident.Session, r ident.Run, b Budget, clock func() time.Time) *Guard {
	if clock == nil {
		clock = time.Now
	}
	return &Guard{tenant: t, session: s, run: r, budget: b, start: clock(), clock: clock}
}

// Identity 返回该 run 的身份（用于审计与指标）。
func (g *Guard) Identity() (ident.Tenant, ident.Session, ident.Run) {
	return g.tenant, g.session, g.run
}

// Budget 返回生效的预算。
func (g *Guard) Budget() Budget { return g.budget }

// Deadline 返回墙钟预算对应的绝对截止时刻。
//
// 接入层应当用它派生 context.WithDeadline：Guard 负责优雅收敛，
// deadline 负责让一个卡在 I/O 上的调用无论如何都能返回。
func (g *Guard) Deadline() time.Time {
	if g.budget.WallClock <= 0 {
		return time.Time{}
	}
	return g.start.Add(g.budget.WallClock)
}

// Usage 返回当前消耗读数。
func (g *Guard) Usage() Usage {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Usage{
		Steps:     g.steps,
		ToolCalls: g.toolCalls,
		Tokens:    g.tokens,
		Elapsed:   g.clock().Sub(g.start),
	}
}

// BeginStep 记入一步并检查预算。应在每一步开始时调用。
//
// 墙钟在每一步都会重新检查——把墙钟只留给 context 是不够的：一个
// 每步都很快但步数极多的循环不会触发任何单次超时，却会拖着整个
// 连接不放。
func (g *Guard) BeginStep() error {
	g.mu.Lock()
	g.steps++
	steps := g.steps
	g.mu.Unlock()

	if g.budget.MaxSteps > 0 && steps > g.budget.MaxSteps {
		return g.exceeded(DimensionSteps, int64(g.budget.MaxSteps), int64(steps))
	}
	return g.Check()
}

// ToolCall 记入一次工具调用并检查预算。
func (g *Guard) ToolCall() error {
	g.mu.Lock()
	g.toolCalls++
	calls := g.toolCalls
	g.mu.Unlock()

	if g.budget.MaxToolCalls > 0 && calls > g.budget.MaxToolCalls {
		return g.exceeded(DimensionToolCalls, int64(g.budget.MaxToolCalls), int64(calls))
	}
	return g.Check()
}

// AddTokens 记入 token 用量并检查预算。
//
// token 是**事后**结算的：模型返回之后才知道用了多少。因此这里必然
// 存在一次超额的越界，能做到的是让它在下一步就终止，而不是提前拦截。
// 这与网关的额度分账是同一套语义。
func (g *Guard) AddTokens(n int) error {
	if n <= 0 {
		return g.Check()
	}
	g.mu.Lock()
	g.tokens += int64(n)
	tokens := g.tokens
	g.mu.Unlock()

	if g.budget.MaxTokens > 0 && tokens > g.budget.MaxTokens {
		return g.exceeded(DimensionTokens, g.budget.MaxTokens, tokens)
	}
	return g.Check()
}

// Check 检查非累加型预算（当前只有墙钟）。
func (g *Guard) Check() error {
	if g.budget.WallClock <= 0 {
		return nil
	}
	elapsed := g.clock().Sub(g.start)
	if elapsed >= g.budget.WallClock {
		return g.exceeded(DimensionWallClock,
			int64(g.budget.WallClock/time.Millisecond), int64(elapsed/time.Millisecond))
	}
	return nil
}

// Remaining 返回各维度的剩余额度。与该维度「不限」时一致地保持 0，
// 调用方不要用 0 来区分「用完了」和「没限制」——那要看 Budget 原值。
func (g *Guard) Remaining() Budget {
	g.mu.Lock()
	defer g.mu.Unlock()
	steps, calls, tokens := g.steps, g.toolCalls, g.tokens
	elapsed := g.clock().Sub(g.start)

	out := g.budget
	if out.MaxSteps > 0 {
		out.MaxSteps = max(0, out.MaxSteps-steps)
	}
	if out.MaxToolCalls > 0 {
		out.MaxToolCalls = max(0, out.MaxToolCalls-calls)
	}
	if out.MaxTokens > 0 {
		out.MaxTokens = max(0, out.MaxTokens-tokens)
	}
	if out.WallClock > 0 {
		out.WallClock = max(0, out.WallClock-elapsed)
	}
	return out
}

// Exhausted 报告是否已触顶任意维度，供「开始新工作之前」做快速预检。
//
// 它**不等于** Check()：Check 只看非累加型的墙钟，而步数 / 工具调用 /
// token 三个累加维度得各自比对用量。把两者混为一谈会让预检形同虚设——
// 一个在 10 步预算里烧掉 10000 步的 run 也会被判成「还有额度」。
//
// 累加维度的判据是 used >= limit，不是 used > limit。因为 BeginStep 那一族
// 是「先记后判」：用量正好等于上限时这一次仍然放行、**下一次**必被拒，
// 此刻预算事实上已经用尽，预检就该说用尽。
func (g *Guard) Exhausted() bool {
	if g.Check() != nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.budget.MaxSteps > 0 && g.steps >= g.budget.MaxSteps {
		return true
	}
	if g.budget.MaxToolCalls > 0 && g.toolCalls >= g.budget.MaxToolCalls {
		return true
	}
	if g.budget.MaxTokens > 0 && g.tokens >= g.budget.MaxTokens {
		return true
	}
	return false
}

func (g *Guard) exceeded(d Dimension, limit, used int64) error {
	return &Exceeded{
		Tenant: g.tenant, Session: g.session, Run: g.run,
		Dimension: d, Limit: limit, Used: used,
	}
}

// ---------------------------------------------------------------------------
// 租户策略服务
// ---------------------------------------------------------------------------

// Policy 是暴露给某个租户隔离域的预算策略。
//
// 它是**每租户一份的指针**，而不是 Budget 值：值类型在服务表里比对
// 身份时会退化成字段比较，两个预算恰好相同的租户会被黑盒隔离断言
// 误判为「解析到了同一个实例」。指针身份才是这里要表达的东西。
type Policy struct {
	budget Budget
	clock  func() time.Time
}

// NewPolicy 构造策略。clock 为 nil 时用 time.Now。
func NewPolicy(b Budget, clock func() time.Time) *Policy {
	return &Policy{budget: b, clock: clock}
}

// Budget 返回策略里的预算。
func (p *Policy) Budget() Budget { return p.budget }

// New 为一次 run 创建预算跟踪器。
//
// 身份是必传参数（ADR-005）：预算读数与越界事件都要能归属到具体的
// 租户/会话/run，否则审计里只会留下一条无主的"某次 run 超预算了"。
func (p *Policy) New(t ident.Tenant, s ident.Session, r ident.Run) *Guard {
	return New(t, s, r, p.budget, p.clock)
}

// Plugin 返回预算策略插件**定义**（一个进程内一份）。
//
// 租户差异经 Config 传入（`Config: guard.NewPolicy(budget, clock)`）。
func Plugin() *cordis.Plugin {
	return &cordis.Plugin{
		Name: realm.PluginLoopGuard,
		Validate: func(cfg any) (any, error) {
			p, ok := cfg.(*Policy)
			if !ok || p == nil {
				return nil, fmt.Errorf("insula/guard: loop-guard config must be a non-nil *guard.Policy, got %T", cfg)
			}
			return p, nil
		},
		Apply: func(ctx *cordis.Context, cfg any) error {
			_, err := ctx.Provide(realm.ServiceGuard, cfg.(*Policy), nil)
			return err
		},
	}
}
