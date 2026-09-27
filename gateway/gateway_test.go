package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	cordis "github.com/metaRobin/cordis"
	"github.com/metaRobin/insula/caps"
	"github.com/metaRobin/insula/creds"
	"github.com/metaRobin/insula/gateway"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/realm"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// fakeClock 可推进的假时钟。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeUpstream 可编程的模型适配器。
type fakeUpstream struct {
	mu       sync.Mutex
	fail     bool
	calls    int
	lastCred *creds.Handle
	lastTen  ident.Tenant
	reply    func(req caps.Request) caps.Response
}

func (u *fakeUpstream) Complete(_ context.Context, t ident.Tenant, req caps.Request,
	cred *creds.Handle) (caps.Response, error) {
	u.mu.Lock()
	u.calls++
	u.lastTen = t
	u.lastCred = cred
	fail := u.fail
	reply := u.reply
	u.mu.Unlock()
	if fail {
		return caps.Response{}, errors.New("upstream exploded")
	}
	if reply != nil {
		return reply(req), nil
	}
	return caps.Response{
		Message:          caps.Message{Role: "assistant", Content: "ok"},
		PromptTokens:     len(req.Messages) * 10,
		CompletionTokens: 5,
	}, nil
}

func (u *fakeUpstream) setFail(v bool) {
	u.mu.Lock()
	u.fail = v
	u.mu.Unlock()
}

func (u *fakeUpstream) callCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

func req() caps.Request {
	return caps.Request{Model: "demo", Messages: []caps.Message{{Role: "user", Content: "hi"}}}
}

// ---------------------------------------------------------------------------
// 额度分账
// ---------------------------------------------------------------------------

// 一个租户打满自己的额度，不得影响另一个租户——这是「额度是租户级」的定义。
func TestQuotaIsPerTenant(t *testing.T) {
	up := &fakeUpstream{}
	p := gateway.New(gateway.Config{
		Upstream: up,
		Quota:    gateway.Quota{MaxCalls: 2, Window: time.Minute},
	})
	a, b := ident.Tenant("tenant-a"), ident.Tenant("tenant-b")

	for i := 0; i < 2; i++ {
		if _, err := p.Complete(context.Background(), a, req()); err != nil {
			t.Fatalf("call %d for A should succeed: %v", i, err)
		}
	}
	var qe *gateway.ErrQuotaExceeded
	if _, err := p.Complete(context.Background(), a, req()); !errors.As(err, &qe) {
		t.Fatalf("A's third call should be rejected with ErrQuotaExceeded, got %v", err)
	}

	// B 完全不受影响。
	for i := 0; i < 2; i++ {
		if _, err := p.Complete(context.Background(), b, req()); err != nil {
			t.Fatalf("call %d for B should succeed while A is exhausted: %v", i, err)
		}
	}

	if got := up.callCount(); got != 4 {
		t.Fatalf("upstream calls = %d, want 4 (rejected calls must not reach upstream)", got)
	}
}

// 窗口到期后额度自动恢复。
func TestQuotaWindowRolls(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	p := gateway.New(gateway.Config{
		Upstream: up,
		Clock:    clk.Now,
		Quota:    gateway.Quota{MaxCalls: 1, Window: time.Minute},
	})
	a := ident.Tenant("tenant-a")

	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Complete(context.Background(), a, req()); err == nil {
		t.Fatal("second call in same window should be rejected")
	}
	clk.Advance(time.Minute)
	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatalf("after window roll the tenant should be able to call again: %v", err)
	}
}

// 每个租户都在自己的额度内，加起来仍会撞上平台总量上限——
// 这是「大家都不作恶但一起打满」的那一层防护。
func TestPlatformQuotaBoundsAggregate(t *testing.T) {
	up := &fakeUpstream{}
	p := gateway.New(gateway.Config{
		Upstream:      up,
		Quota:         gateway.Quota{MaxCalls: 10, Window: time.Minute},
		PlatformQuota: gateway.Quota{MaxCalls: 3, Window: time.Minute},
	})

	tenants := []ident.Tenant{"t1", "t2", "t3", "t4", "t5", "t6"}
	ok := 0
	for _, tn := range tenants {
		if _, err := p.Complete(context.Background(), tn, req()); err == nil {
			ok++
		}
	}
	if ok != 3 {
		t.Fatalf("successful calls = %d, want 3 (platform cap)", ok)
	}
	if got := up.callCount(); got != 3 {
		t.Fatalf("upstream calls = %d, want 3", got)
	}
}

// 被拒绝的调用不得在上游留痕，也不得被计入熔断失败——
// 否则一个租户的额度用尽会把整个池熔掉。
func TestRejectedCallDoesNotAffectBreaker(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	p := gateway.New(gateway.Config{
		Upstream: up,
		Clock:    clk.Now,
		Quota:    gateway.Quota{MaxCalls: 1, Window: time.Minute},
		Breaker:  gateway.BreakerConfig{Threshold: 2, Clock: clk.Now},
	})
	a := ident.Tenant("tenant-a")
	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, _ = p.Complete(context.Background(), a, req())
	}
	if st := p.Stats(); st.State != gateway.StateClosed || st.Failures != 0 {
		t.Fatalf("quota rejections must not touch the breaker: %+v", st)
	}
}

// ---------------------------------------------------------------------------
// 熔断
// ---------------------------------------------------------------------------

func TestBreakerOpensAndHalfOpens(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	p := gateway.New(gateway.Config{
		Upstream: up,
		Clock:    clk.Now,
		Quota:    gateway.Quota{MaxCalls: 100, Window: time.Hour},
		Breaker:  gateway.BreakerConfig{Threshold: 3, Cooldown: 10 * time.Second, Clock: clk.Now},
	})
	a := ident.Tenant("tenant-a")
	up.setFail(true)

	for i := 0; i < 3; i++ {
		if _, err := p.Complete(context.Background(), a, req()); err == nil {
			t.Fatal("upstream is failing, call should error")
		}
	}
	if p.Breaker().State() != gateway.StateOpen {
		t.Fatalf("breaker = %v, want open after 3 failures", p.Breaker().State())
	}
	if p.Healthy() {
		t.Fatal("an open breaker must make the pool unhealthy")
	}

	// Open 期间不应再打上游。
	before := up.callCount()
	if _, err := p.Complete(context.Background(), a, req()); !errors.Is(err, gateway.ErrUnavailable) {
		t.Fatalf("call while open should be ErrUnavailable, got %v", err)
	}
	if up.callCount() != before {
		t.Fatal("an open breaker must not reach the upstream")
	}

	// 冷却到期 → HalfOpen 只放行一个探测。
	clk.Advance(10 * time.Second)
	up.setFail(false)
	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatalf("half-open probe should be allowed: %v", err)
	}
	if p.Breaker().State() != gateway.StateClosed {
		t.Fatalf("a successful probe should close the breaker, got %v", p.Breaker().State())
	}
}

// 这是本包最关键的一条集成测试：模型池不可用时，依赖它的 fiber 必须被
// cordis 自动撤销，而不是靠业务代码判 if。
func TestUnhealthyPoolTearsDownDependents(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	p := gateway.New(gateway.Config{
		Upstream: up,
		Clock:    clk.Now,
		Quota:    gateway.Quota{MaxCalls: 100, Window: time.Hour},
		Breaker:  gateway.BreakerConfig{Threshold: 2, Cooldown: time.Minute, Clock: clk.Now},
	})
	app := cordis.New()
	defer app.Close()
	run := func(f func(ctx *cordis.Context)) {
		app.DoSync(f)
		if !app.Wait() {
			t.Fatal("app did not settle")
		}
	}

	a := ident.Tenant("tenant-a")
	scope := func(c *cordis.Context) *cordis.Context {
		for _, name := range realm.Services() {
			c = c.Isolate(name, "#t-aaa")
		}
		return c
	}

	var consumer *cordis.Fiber
	consumerPlugin := &cordis.Plugin{
		Name:   "consumer",
		Inject: map[string]any{realm.ServiceModels: nil},
		Apply: func(*cordis.Context, any) error {
			// 依赖满足时激活。这里不做任何健康度判断——
			// 健康度由 cordis 的 check 机制负责。
			return nil
		},
	}

	run(func(ctx *cordis.Context) {
		if _, err := scope(ctx).Plugin(p.Provider(a), nil); err != nil {
			t.Fatal(err)
		}
		var err error
		consumer, err = scope(ctx).Plugin(consumerPlugin, nil)
		if err != nil {
			t.Fatal(err)
		}
	})
	if consumer.State() != cordis.StateActive {
		t.Fatalf("consumer should be active while pool is healthy, got %v", consumer.State())
	}

	// 让上游连续失败，把熔断器打开。
	up.setFail(true)
	for i := 0; i < 2; i++ {
		_, _ = p.Complete(context.Background(), a, req())
	}
	if p.Breaker().State() != gateway.StateOpen {
		t.Fatalf("breaker should be open, got %v", p.Breaker().State())
	}
	run(func(*cordis.Context) {}) // 等 Publish 触发的重载收敛

	if consumer.State() == cordis.StateActive {
		t.Fatal("consumer must be torn down while the pool is unhealthy")
	}

	// 恢复：冷却 + 探测成功后依赖者应重新激活。
	clk.Advance(time.Minute)
	up.setFail(false)
	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatalf("recovery probe failed: %v", err)
	}
	run(func(*cordis.Context) {})
	if consumer.State() != cordis.StateActive {
		t.Fatalf("consumer should reactivate after recovery, got %v", consumer.State())
	}
}

// ---------------------------------------------------------------------------
// 凭证
// ---------------------------------------------------------------------------

// 每次调用都签发新句柄会让凭证提供者的在途表随调用量无界增长；
// 句柄必须在有效期内复用。
func TestCredentialHandleIsCachedAndRotated(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	cps := creds.NewStaticProvider(creds.WithClock(clk.Now), creds.WithTTL(10*time.Minute))
	a := ident.Tenant("tenant-a")
	if err := cps.Put(a, "model-api", "secret-A"); err != nil {
		t.Fatal(err)
	}
	p := gateway.New(gateway.Config{
		Upstream: up, Creds: cps, Clock: clk.Now,
		Quota: gateway.Quota{MaxCalls: 100, Window: time.Hour},
	})

	for i := 0; i < 5; i++ {
		if _, err := p.Complete(context.Background(), a, req()); err != nil {
			t.Fatal(err)
		}
	}
	if got := cps.Live(a); got != 1 {
		t.Fatalf("live handles = %d, want 1 (reused within TTL)", got)
	}

	// 越过 4/5 寿命后应换新句柄，且旧句柄不应堆积。
	clk.Advance(9 * time.Minute)
	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatal(err)
	}
	if got := cps.Live(a); got != 2 {
		t.Fatalf("live handles = %d, want 2 after rotation", got)
	}
}

// 每个租户拿到自己的句柄，且句柄不能被拿去兑现别的租户的凭证。
func TestCredentialIsPerTenant(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	cps := creds.NewStaticProvider(creds.WithClock(clk.Now))
	a, b := ident.Tenant("tenant-a"), ident.Tenant("tenant-b")
	for _, kv := range []struct {
		t      ident.Tenant
		secret string
	}{{a, "secret-A"}, {b, "secret-B"}} {
		if err := cps.Put(kv.t, "model-api", kv.secret); err != nil {
			t.Fatal(err)
		}
	}
	p := gateway.New(gateway.Config{
		Upstream: up, Creds: cps, Clock: clk.Now,
		Quota: gateway.Quota{MaxCalls: 100, Window: time.Hour},
	})

	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatal(err)
	}
	handleA := up.lastCred
	if _, err := p.Complete(context.Background(), b, req()); err != nil {
		t.Fatal(err)
	}
	handleB := up.lastCred

	if handleA == nil || handleB == nil {
		t.Fatal("upstream must receive a credential handle")
	}
	if handleA.Tenant() != a || handleB.Tenant() != b {
		t.Fatalf("handles bound to wrong tenants: %s / %s", handleA.Tenant(), handleB.Tenant())
	}

	// 拿 A 的句柄去兑现 B 的凭证必须失败。
	if _, err := cps.ResolveFor(b, handleA); !errors.Is(err, creds.ErrForeignHandle) {
		t.Fatalf("cross-tenant handle redemption must fail, got %v", err)
	}
}

// 句柄不得回显明文——这是「凭证不进日志」的结构保障。
func TestCredentialHandleNeverRendersSecret(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	cps := creds.NewStaticProvider(creds.WithClock(clk.Now))
	a := ident.Tenant("tenant-a")
	const secret = "sk-super-secret-value"
	if err := cps.Put(a, "model-api", secret); err != nil {
		t.Fatal(err)
	}
	p := gateway.New(gateway.Config{
		Upstream: up, Creds: cps, Clock: clk.Now,
		Quota: gateway.Quota{MaxCalls: 10, Window: time.Hour},
	})
	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatal(err)
	}
	h := up.lastCred
	for _, s := range []string{fmt.Sprint(h), fmt.Sprintf("%#v", h), h.String()} {
		if s == "" {
			t.Fatal("handle must render something")
		}
		if strings.Contains(s, secret) {
			t.Fatalf("handle rendering leaked the secret: %q", s)
		}
	}
}

// 没有配置凭证是配置错误，不该把池熔掉——否则一个租户的疏漏会拖垮所有人。
func TestMissingCredentialDoesNotTripBreaker(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	cps := creds.NewStaticProvider(creds.WithClock(clk.Now)) // 未 Put 任何凭证
	p := gateway.New(gateway.Config{
		Upstream: up, Creds: cps, Clock: clk.Now,
		Quota:   gateway.Quota{MaxCalls: 100, Window: time.Hour},
		Breaker: gateway.BreakerConfig{Threshold: 2, Clock: clk.Now},
	})
	a := ident.Tenant("tenant-a")

	for i := 0; i < 5; i++ {
		if _, err := p.Complete(context.Background(), a, req()); err == nil {
			t.Fatal("missing credential should error")
		}
	}
	if st := p.Stats(); st.State != gateway.StateClosed || st.Failures != 0 {
		t.Fatalf("config errors must not trip the breaker: %+v", st)
	}
	if up.callCount() != 0 {
		t.Fatal("no upstream call should happen without a credential")
	}
}

// 注销租户必须清掉账户与凭证缓存，否则是一条无界增长路径，
// 而且会把注销租户的句柄继续留在内存里。
func TestForgetReleasesTenantState(t *testing.T) {
	clk := newClock()
	cps := creds.NewStaticProvider(creds.WithClock(clk.Now))
	a := ident.Tenant("tenant-a")
	if err := cps.Put(a, "model-api", "s"); err != nil {
		t.Fatal(err)
	}
	p := gateway.New(gateway.Config{
		Upstream: &fakeUpstream{}, Creds: cps, Clock: clk.Now,
		Quota: gateway.Quota{MaxCalls: 10, Window: time.Hour},
	})
	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatal(err)
	}
	if cps.Live(a) != 1 {
		t.Fatalf("live = %d, want 1", cps.Live(a))
	}

	p.Forget(a)
	if cps.Live(a) != 0 {
		t.Fatalf("Forget must revoke outstanding handles, live = %d", cps.Live(a))
	}
	if st := p.Stats(); st.Accounts != 0 {
		t.Fatalf("Forget must drop the account, accounts = %d", st.Accounts)
	}
}

// 空租户标识必须 fail-closed，绝不能变成一个隐藏的全局账户。
func TestEmptyTenantIsRejected(t *testing.T) {
	p := gateway.New(gateway.Config{Upstream: &fakeUpstream{}})
	if _, err := p.Complete(context.Background(), ident.Tenant(""), req()); err == nil {
		t.Fatal("empty tenant identity must be rejected")
	}
}
