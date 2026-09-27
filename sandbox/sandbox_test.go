package sandbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/metaRobin/insula/caps"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/sandbox"
)

func req(name string) sandbox.Request {
	return sandbox.Request{
		Invocation: caps.Invocation{
			Tenant:  ident.Tenant("t-a"),
			Session: ident.Session("s-1"),
			Run:     ident.Run("run-1"),
			Name:    name,
		},
		Tool: name,
	}
}

// 这是本包最关键的一条：未配置进程外沙箱时，不受信的代码**绝不能**
// 悄悄退回进程内执行。"暂时先用 Direct 顶一下"是最常见的生产事故来源。
func TestUntrustedTierNeverFallsBackToInProcess(t *testing.T) {
	ran := false
	direct := sandbox.NewDirect(func(context.Context, sandbox.Request) (sandbox.Result, error) {
		ran = true
		return sandbox.Result{Output: "should not happen"}, nil
	}, 0)

	// 只配了进程内通道，没配进程外沙箱。
	m := sandbox.NewManager(direct, nil)

	res, err := m.Run(context.Background(), sandbox.TierUntrusted, req("user-uploaded-code"))
	if !errors.Is(err, sandbox.ErrNoSandbox) {
		t.Fatalf("untrusted tier must fail closed, got err=%v res=%+v", err, res)
	}
	if ran {
		t.Fatal("untrusted code ran in-process — this is the exact failure this design exists to prevent")
	}
}

// 信任一/二级允许进程内执行（域隔离 + 能力最小化 + 出口审计）。
func TestTrustedTiersUseInProcess(t *testing.T) {
	for _, tier := range []sandbox.Tier{sandbox.TierTrusted, sandbox.TierSemi} {
		ran := false
		direct := sandbox.NewDirect(func(context.Context, sandbox.Request) (sandbox.Result, error) {
			ran = true
			return sandbox.Result{Output: "ok"}, nil
		}, 0)
		m := sandbox.NewManager(direct, nil)
		if _, err := m.Run(context.Background(), tier, req("builtin")); err != nil {
			t.Fatalf("tier %v should be allowed in-process: %v", tier, err)
		}
		if !ran {
			t.Fatalf("tier %v should have executed", tier)
		}
		if !tier.AllowsInProcess() {
			t.Fatalf("AllowsInProcess(%v) = false, want true", tier)
		}
	}
	if sandbox.TierUntrusted.AllowsInProcess() {
		t.Fatal("untrusted tier must never allow in-process execution")
	}
}

// 装配期注入真实沙箱后，不受信的调用必须走它。
func TestOutOfProcessTakesOverWhenConfigured(t *testing.T) {
	inProcessRan := false
	direct := sandbox.NewDirect(func(context.Context, sandbox.Request) (sandbox.Result, error) {
		inProcessRan = true
		return sandbox.Result{}, nil
	}, 0)

	outRan := false
	out := sandbox.NewDirect(func(context.Context, sandbox.Request) (sandbox.Result, error) {
		outRan = true
		return sandbox.Result{Output: "from sandbox"}, nil
	}, 0)

	m := sandbox.NewManager(direct, nil)
	m.SetOutOfProcess(out)

	res, err := m.Run(context.Background(), sandbox.TierUntrusted, req("user-uploaded-code"))
	if err != nil {
		t.Fatal(err)
	}
	if !outRan || inProcessRan {
		t.Fatalf("untrusted call must go out-of-process only: out=%v in=%v", outRan, inProcessRan)
	}
	if res.Output != "from sandbox" {
		t.Fatalf("output = %q", res.Output)
	}
}

// 清空进程外通道必须回到 fail-closed，而不是退回进程内。
func TestClearingSandboxReturnsToFailClosed(t *testing.T) {
	m := sandbox.NewManager(nil, nil)
	m.SetOutOfProcess(nil)
	if _, err := m.Run(context.Background(), sandbox.TierUntrusted, req("t")); !errors.Is(err, sandbox.ErrNoSandbox) {
		t.Fatalf("want ErrNoSandbox, got %v", err)
	}
}

// 输出截断必须显式可见：静默截断会让模型基于残缺信息继续推理。
func TestOutputTruncationIsVisible(t *testing.T) {
	direct := sandbox.NewDirect(func(context.Context, sandbox.Request) (sandbox.Result, error) {
		return sandbox.Result{Output: strings.Repeat("x", 1000)}, nil
	}, 100)

	r := req("t")
	r.MaxOutputBytes = 100
	res, err := direct.Run(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated {
		t.Fatal("truncation must be reported")
	}
	if len(res.Output) != 100 {
		t.Fatalf("output len = %d, want 100", len(res.Output))
	}
}

// 未超限时不得误报截断。
func TestNoTruncationWhenUnderLimit(t *testing.T) {
	direct := sandbox.NewDirect(func(context.Context, sandbox.Request) (sandbox.Result, error) {
		return sandbox.Result{Output: "short"}, nil
	}, 100)
	res, err := direct.Run(context.Background(), req("t"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Truncated {
		t.Fatal("must not report truncation for a short output")
	}
}

// 身份缺失意味着这次调用无法归属到任何租户，也就无法审计。
// 归属不清时执行是最坏的选择。
func TestEmptyTenantIsRejected(t *testing.T) {
	ran := false
	direct := sandbox.NewDirect(func(context.Context, sandbox.Request) (sandbox.Result, error) {
		ran = true
		return sandbox.Result{}, nil
	}, 0)

	r := req("t")
	r.Invocation.Tenant = ""
	if _, err := direct.Run(context.Background(), r); err == nil {
		t.Fatal("empty tenant identity must be rejected")
	}
	if ran {
		t.Fatal("runner must not execute without a tenant identity")
	}
}

// 默认上限必须存在：一个工具可以返回 100MB，不截断就是内存耗尽路径，
// 而这些内容最终还要进模型上下文。
func TestDefaultOutputLimitIsBounded(t *testing.T) {
	if sandbox.DefaultMaxOutputBytes <= 0 {
		t.Fatal("there must be a default output cap")
	}
	if sandbox.DefaultMaxOutputBytes > 8<<20 {
		t.Fatalf("default cap %d is too permissive to be a guard", sandbox.DefaultMaxOutputBytes)
	}
}

func TestDescribeMentionsTierLimits(t *testing.T) {
	direct := sandbox.NewDirect(func(context.Context, sandbox.Request) (sandbox.Result, error) {
		return sandbox.Result{}, nil
	}, 0)
	d := direct.Describe()
	if !strings.Contains(d, "direct") || !strings.Contains(strings.ToLower(d), "trust") {
		t.Fatalf("Describe must state that direct is for trusted tiers only, got %q", d)
	}

	m := sandbox.NewManager(nil, nil)
	desc := m.Describe()
	if len(desc) != 2 {
		t.Fatalf("Describe = %v", desc)
	}
	for _, v := range desc {
		if !strings.Contains(strings.ToLower(v), "closed") {
			t.Fatalf("default manager must report closed channels, got %q", v)
		}
	}
}

func TestClosedCarriesItsReason(t *testing.T) {
	c := &sandbox.Closed{Reason: "wasm sandbox not deployed"}
	_, err := c.Run(context.Background(), req("t"))
	if !errors.Is(err, sandbox.ErrNoSandbox) {
		t.Fatalf("want ErrNoSandbox, got %v", err)
	}
	if !strings.Contains(err.Error(), "wasm sandbox not deployed") {
		t.Fatalf("the reason must reach the operator: %v", err)
	}
}
