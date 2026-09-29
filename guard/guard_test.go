package guard_test

import (
	"errors"
	"testing"
	"time"

	"github.com/metaRobin/insula/guard"
	"github.com/metaRobin/insula/ident"
)

type fakeClock struct {
	t time.Time
}

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func newGuard(b guard.Budget, clk *fakeClock) *guard.Guard {
	return guard.New(ident.Tenant("t-a"), ident.Session("s-1"), ident.Run("run-1"), b, clk.Now)
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)}
}

// 四个维度各自独立生效。只限其中任何一个都会留下另外几条失控路径，
// 尤其是工具调用数——步数限制看起来已经够了，但一步里可以有多次工具调用。
func TestEachDimensionTripsIndependently(t *testing.T) {
	cases := []struct {
		name      string
		budget    guard.Budget
		trip      func(g *guard.Guard) error
		dimension guard.Dimension
	}{
		{
			name:   "steps",
			budget: guard.Budget{MaxSteps: 2},
			trip: func(g *guard.Guard) error {
				for i := 0; i < 3; i++ {
					if err := g.BeginStep(); err != nil {
						return err
					}
				}
				return nil
			},
			dimension: guard.DimensionSteps,
		},
		{
			name:   "tool_calls",
			budget: guard.Budget{MaxToolCalls: 2},
			trip: func(g *guard.Guard) error {
				for i := 0; i < 3; i++ {
					if err := g.ToolCall(); err != nil {
						return err
					}
				}
				return nil
			},
			dimension: guard.DimensionToolCalls,
		},
		{
			name:   "tokens",
			budget: guard.Budget{MaxTokens: 100},
			trip: func(g *guard.Guard) error {
				for i := 0; i < 3; i++ {
					if err := g.AddTokens(50); err != nil {
						return err
					}
				}
				return nil
			},
			dimension: guard.DimensionTokens,
		},
		{
			name:   "wall_clock",
			budget: guard.Budget{WallClock: time.Minute},
			trip: func(g *guard.Guard) error {
				g.Check()
				return g.Check()
			},
			dimension: guard.DimensionWallClock,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := newClock()
			g := newGuard(tc.budget, clk)
			if tc.dimension == guard.DimensionWallClock {
				clk.Advance(2 * time.Minute) // 时间先走，再触发检查
			}
			err := tc.trip(g)
			if err == nil {
				t.Fatalf("%s budget should have tripped", tc.name)
			}
			var ex *guard.Exceeded
			if !errors.As(err, &ex) {
				t.Fatalf("want *guard.Exceeded, got %T: %v", err, err)
			}
			if ex.Dimension != tc.dimension {
				t.Fatalf("dimension = %s, want %s", ex.Dimension, tc.dimension)
			}
			// 循环代码应当能识别它并优雅收尾，而不是当成故障。
			if !errors.Is(err, guard.ErrBudgetExhausted) {
				t.Fatal("Exceeded must satisfy errors.Is(err, ErrBudgetExhausted)")
			}
			// 审计需要知道是哪个 run 越的界。
			if ex.Run != "run-1" || ex.Tenant != "t-a" {
				t.Fatalf("Exceeded must carry the run identity: %+v", ex)
			}
		})
	}
}

// 零值预算表示该维度不限——这是"测试里我不关心这一维"的表达方式，
// 平台装配层应当显式给出预算（见 guard.Default）。
func TestZeroBudgetMeansUnlimited(t *testing.T) {
	clk := newClock()
	g := newGuard(guard.Budget{}, clk)
	for i := 0; i < 1000; i++ {
		if err := g.BeginStep(); err != nil {
			t.Fatalf("unlimited budget tripped at step %d: %v", i, err)
		}
		if err := g.ToolCall(); err != nil {
			t.Fatalf("unlimited budget tripped at tool call %d: %v", i, err)
		}
		if err := g.AddTokens(10000); err != nil {
			t.Fatalf("unlimited budget tripped on tokens: %v", err)
		}
	}
	clk.Advance(24 * time.Hour)
	if err := g.Check(); err != nil {
		t.Fatalf("unlimited wall clock tripped: %v", err)
	}
}

// 默认预算必须给全四个维度，否则新租户上线就等于没有限制。
func TestDefaultBudgetCoversEveryDimension(t *testing.T) {
	b := guard.Default()
	if b.MaxSteps <= 0 || b.MaxToolCalls <= 0 || b.MaxTokens <= 0 || b.WallClock <= 0 {
		t.Fatalf("Default() must bound every dimension: %+v", b)
	}
}

// 墙钟必须在每一步都重新检查：一个每步都很快但步数极多的循环
// 不会触发任何单次超时，却会拖着整个连接不放。
func TestWallClockIsRecheckedOnEveryStep(t *testing.T) {
	clk := newClock()
	g := newGuard(guard.Budget{WallClock: time.Minute}, clk)

	if err := g.BeginStep(); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)
	err := g.BeginStep()
	if !errors.Is(err, guard.ErrBudgetExhausted) {
		t.Fatalf("a step after the wall clock expired must trip, got %v", err)
	}
}

// Deadline 供接入层派生 context.WithDeadline：Guard 负责体面地停，
// deadline 负责让卡在 I/O 上的调用无论如何都能停。
func TestDeadline(t *testing.T) {
	clk := newClock()
	start := clk.Now()

	g := newGuard(guard.Budget{WallClock: 5 * time.Minute}, clk)
	if got := g.Deadline(); !got.Equal(start.Add(5 * time.Minute)) {
		t.Fatalf("deadline = %v, want %v", got, start.Add(5*time.Minute))
	}

	if got := newGuard(guard.Budget{}, clk).Deadline(); !got.IsZero() {
		t.Fatalf("unbounded wall clock must yield a zero deadline, got %v", got)
	}
}

func TestUsageAndRemaining(t *testing.T) {
	clk := newClock()
	g := newGuard(guard.Budget{MaxSteps: 10, MaxToolCalls: 4, MaxTokens: 1000, WallClock: time.Minute}, clk)

	_ = g.BeginStep()
	_ = g.BeginStep()
	_ = g.ToolCall()
	_ = g.AddTokens(300)
	clk.Advance(10 * time.Second)

	u := g.Usage()
	if u.Steps != 2 || u.ToolCalls != 1 || u.Tokens != 300 || u.Elapsed != 10*time.Second {
		t.Fatalf("usage = %+v", u)
	}

	r := g.Remaining()
	if r.MaxSteps != 8 || r.MaxToolCalls != 3 || r.MaxTokens != 700 || r.WallClock != 50*time.Second {
		t.Fatalf("remaining = %+v", r)
	}
}

// 步数统计的是"已开始"的步数：越界的那一步也被计入，
// 否则调用方看到的读数会停在限制值上，看不出到底尝试了多少步。
func TestBoundaryIsCountedOnTheCrossingStep(t *testing.T) {
	clk := newClock()
	g := newGuard(guard.Budget{MaxSteps: 2}, clk)

	if err := g.BeginStep(); err != nil {
		t.Fatal(err)
	}
	if err := g.BeginStep(); err != nil {
		t.Fatalf("step 2 is exactly the limit and must be allowed: %v", err)
	}
	err := g.BeginStep()
	var ex *guard.Exceeded
	if !errors.As(err, &ex) {
		t.Fatalf("step 3 must trip, got %v", err)
	}
	if ex.Used != 3 || ex.Limit != 2 {
		t.Fatalf("Exceeded = %+v, want used=3 limit=2", ex)
	}
}

// 非正数 token 增量不得被当作"消耗"计入，也不该触发越界。
func TestNonPositiveTokensAreIgnored(t *testing.T) {
	clk := newClock()
	g := newGuard(guard.Budget{MaxTokens: 100}, clk)
	if err := g.AddTokens(0); err != nil {
		t.Fatal(err)
	}
	if err := g.AddTokens(-50); err != nil {
		t.Fatal(err)
	}
	if u := g.Usage(); u.Tokens != 0 {
		t.Fatalf("tokens = %d, want 0", u.Tokens)
	}
}

func TestIdentityIsCarriedThrough(t *testing.T) {
	clk := newClock()
	g := newGuard(guard.Default(), clk)
	tenant, session, run := g.Identity()
	if tenant != "t-a" || session != "s-1" || run != "run-1" {
		t.Fatalf("identity = %s/%s/%s", tenant, session, run)
	}
}

// Exhausted 必须覆盖**全部四个**维度，而不只是墙钟。
//
// 它是给「开始新工作之前」用的快速预检：预检的唯一价值就是不放过任何一条
// 已经到顶的维度。只看墙钟的话，一个在 10 步预算里烧掉 10000 步的 run
// 也会被告知「还有额度」——预检通过了，却什么也没挡住。
func TestExhaustedCoversEveryDimension(t *testing.T) {
	const steps, calls, tokens = 2, 3, 40

	cases := []struct {
		name    string
		budget  guard.Budget
		spend   func(g *guard.Guard)
		advance time.Duration
		want    bool
	}{
		{
			name: "各维度都还有余量时不报用尽",
			budget: guard.Budget{
				MaxSteps: steps, MaxToolCalls: calls,
				MaxTokens: tokens, WallClock: time.Hour,
			},
			spend: func(g *guard.Guard) {
				_ = g.BeginStep()
				_ = g.ToolCall()
				_ = g.AddTokens(1)
			},
			advance: time.Minute,
			want:    false,
		},
		{
			// 用量正好等于上限：这一步已被放行，但下一步必被拒。
			name:   "步数正好用满",
			budget: guard.Budget{MaxSteps: steps},
			spend: func(g *guard.Guard) {
				for i := 0; i < steps; i++ {
					_ = g.BeginStep()
				}
			},
			want: true,
		},
		{
			name:   "工具调用数正好用满",
			budget: guard.Budget{MaxToolCalls: calls},
			spend: func(g *guard.Guard) {
				for i := 0; i < calls; i++ {
					_ = g.ToolCall()
				}
			},
			want: true,
		},
		{
			name:   "token 正好用满",
			budget: guard.Budget{MaxTokens: tokens},
			spend: func(g *guard.Guard) {
				_ = g.AddTokens(tokens)
			},
			want: true,
		},
		{
			name:    "墙钟到点",
			budget:  guard.Budget{WallClock: time.Minute},
			spend:   func(*guard.Guard) {},
			advance: time.Minute,
			want:    true,
		},
		{
			// 0 表示不限，不是「零额度」——四个维度都不设限时永远不该报用尽。
			name:   "全部不设限时永远不报用尽",
			budget: guard.Budget{},
			spend: func(g *guard.Guard) {
				for i := 0; i < 100; i++ {
					_ = g.BeginStep()
				}
				_ = g.AddTokens(1_000_000)
			},
			advance: 24 * time.Hour,
			want:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := newClock()
			g := newGuard(tc.budget, clk)
			if got := g.Budget(); got != tc.budget {
				t.Fatalf("Budget() = %+v，期望构造时传入的 %+v", got, tc.budget)
			}
			tc.spend(g)
			if tc.advance > 0 {
				clk.Advance(tc.advance)
			}
			if got := g.Exhausted(); got != tc.want {
				t.Fatalf("Exhausted() = %v，期望 %v（Usage = %+v）", got, tc.want, g.Usage())
			}
		})
	}
}
