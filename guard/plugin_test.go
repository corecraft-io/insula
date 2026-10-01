package guard_test

import (
	"errors"
	"testing"

	"github.com/corecraft-io/insula/guard"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/realm"
)

// TestExceededErrorVocabulary 钉住预算越界的对外文案。
//
// 与 Tier.String / State.String 同类：这条消息落在日志、审计与（按设计）
// 最终回答里 —— 「我在完成前用尽了预算」正是从这里出发的。它因为
// "测试通过时没人格式化它"而覆盖率天然是 0，改错了不会有任何东西变红。
func TestExceededErrorVocabulary(t *testing.T) {
	e := &guard.Exceeded{
		Tenant:    ident.Tenant("t-a"),
		Session:   ident.Session("s-1"),
		Run:       ident.Run("run-1"),
		Dimension: guard.DimensionToolCalls,
		Limit:     6,
		Used:      7,
	}

	const want = "insula/guard: run run-1 exceeded tool_calls budget: used 7 of 6"
	if got := e.Error(); got != want {
		t.Fatalf("Exceeded.Error()\n got = %q\nwant = %q", got, want)
	}

	// 四个维度必须给出四个不同的词：运维读日志时只有这一个分辨依据，
	// 两维撞词会让"到底撞了哪条线"变成一次猜测。
	seen := map[string]guard.Dimension{}
	for _, d := range []guard.Dimension{
		guard.DimensionSteps, guard.DimensionToolCalls,
		guard.DimensionTokens, guard.DimensionWallClock,
	} {
		msg := (&guard.Exceeded{Run: "r", Dimension: d, Limit: 1, Used: 1}).Error()
		if prev, dup := seen[msg]; dup {
			t.Fatalf("维度 %q 与 %q 的越界消息完全相同，运维分不出来", prev, d)
		}
		seen[msg] = d
	}
}

// TestExceededMatchesErrBudgetExhausted 守住 errors.Is 的那条契约。
//
// 调用方按设计会写 errors.Is(err, ErrBudgetExhausted) ——它是一个**正常
// 终止条件**而不是故障（模型应当基于已有结果诚实收尾）。Is 一旦被删掉，
// errors.Is 会退化成指针比较，而"预算用尽"会被当成一个内部错误往上冒。
func TestExceededMatchesErrBudgetExhausted(t *testing.T) {
	err := &guard.Exceeded{Run: "run-1", Dimension: guard.DimensionSteps, Limit: 1, Used: 2}

	if !errors.Is(err, guard.ErrBudgetExhausted) {
		t.Error("errors.Is(*Exceeded, ErrBudgetExhausted) 必须成立")
	}
	var target *guard.Exceeded
	if !errors.As(err, &target) {
		t.Error("errors.As 必须能取出 *Exceeded")
	}
}

// TestLoopGuardPluginValidatesConfig 守住插件配置的负向分支。
//
// Validate 是租户差异进入插件的唯一入口（`Config: guard.NewPolicy(...)`）。
// 它的负向分支此前从未被执行过：类型不对的配置会以 `cfg.(*Policy)` 的类型
// 断言 panic 收场，而不是返回一条能读的错误——而这条路径在有人写错
// 租户配置时**一定**会被走到，且发生在启动装配期。
func TestLoopGuardPluginValidatesConfig(t *testing.T) {
	pl := guard.Plugin()
	if pl == nil {
		t.Fatal("Plugin() 必须返回定义")
	}
	if pl.Name != realm.PluginLoopGuard {
		t.Errorf("插件名 = %q，期望 %q（它是入口装配的查找键）", pl.Name, realm.PluginLoopGuard)
	}
	if pl.Validate == nil {
		t.Fatal("缺少 Validate：类型不对的租户配置将没有地方被拦住")
	}

	// 类型化 nil 也要被拦住：`(*guard.Policy)(nil)` 的类型断言是**成功**的，
	// 只判 `!ok` 会让一个 nil 策略一路装到服务表里。
	for _, bad := range []any{guard.Budget{}, "not a policy", (*guard.Policy)(nil)} {
		if _, err := pl.Validate(bad); err == nil {
			t.Errorf("Validate(%T) 必须被拒绝", bad)
		}
	}

	p := guard.NewPolicy(guard.Budget{MaxSteps: 3}, nil)
	got, err := pl.Validate(p)
	if err != nil {
		t.Fatalf("合法配置不该被拒: %v", err)
	}
	if got != any(p) {
		t.Errorf("合法配置必须**原样**通过（指针身份即隔离依据），实际得到 %T", got)
	}
}
