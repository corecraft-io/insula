package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	cordis "github.com/corecraft-io/cordis"
	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/creds"
	"github.com/corecraft-io/insula/gateway"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/realm"
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

// ---------------------------------------------------------------------------
// 闸门与归还
// ---------------------------------------------------------------------------

// 运维硬闸门必须真的关得住，而且**不许扣租户的额度**。
//
// 后半句是这个闸门最容易写错的地方：拒绝一次调用却记一次用量，等于
// 池下线期间用「拒绝」本身把租户额度烧光。上游一恢复，租户拿到的不是
// 「服务恢复」而是「额度用尽」——一次运维事件变成了一次资损。
// 平台总额度同样不许被拒绝路径吃掉，否则 A 的运维事件会连带 B 一起饿死。
func TestDisabledPoolRejectsWithoutBurningQuota(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	p := gateway.New(gateway.Config{
		Upstream: up, Clock: clk.Now,
		Quota:         gateway.Quota{MaxCalls: 3, Window: time.Hour},
		PlatformQuota: gateway.Quota{MaxCalls: 6, Window: time.Hour},
	})
	a, b := ident.Tenant("tenant-a"), ident.Tenant("tenant-b")

	if !p.Healthy() {
		t.Fatal("a fresh pool must be healthy")
	}
	p.SetEnabled(false)
	if p.Healthy() {
		t.Fatal("SetEnabled(false) must be reflected by Healthy()")
	}

	// 闸门关着时打一波。全部必须以 ErrUnavailable 拒绝——不能因为额度
	// 被这些拒绝耗光而改判成 ErrQuotaExceeded，那是错误归因。
	for i := 0; i < 5; i++ {
		_, err := p.Complete(context.Background(), a, req())
		if !errors.Is(err, gateway.ErrUnavailable) {
			t.Fatalf("call %d while disabled: want ErrUnavailable, got %v", i, err)
		}
	}
	if up.callCount() != 0 {
		t.Fatal("a disabled pool must not reach the upstream")
	}
	if got := p.Account(a).Usage().Calls; got != 0 {
		t.Fatalf("5 rejected calls burned %d units of A's own quota, want 0", got)
	}

	p.SetEnabled(true)
	if !p.Healthy() {
		t.Fatal("re-enabling must restore health")
	}

	// B 的 3 次调用证明**平台**额度没被拒绝路径吃掉。
	for i := 0; i < 3; i++ {
		if _, err := p.Complete(context.Background(), b, req()); err != nil {
			t.Fatalf("B call %d after re-enable (platform quota was burned?): %v", i, err)
		}
	}
	// A 的 3 次调用证明**租户**额度没被拒绝路径吃掉。
	for i := 0; i < 3; i++ {
		if _, err := p.Complete(context.Background(), a, req()); err != nil {
			t.Fatalf("A call %d after re-enable (tenant quota was burned?): %v", i, err)
		}
	}
	// 闸门恢复后两个额度都仍然真的在限。
	var qe *gateway.ErrQuotaExceeded
	if _, err := p.Complete(context.Background(), a, req()); !errors.As(err, &qe) {
		t.Fatalf("A's 4th call should exceed its own quota, got %v", err)
	}
	if _, err := p.Complete(context.Background(), b, req()); !errors.As(err, &qe) {
		t.Fatalf("B's 4th call should exceed the platform quota, got %v", err)
	}
}

// switchableCreds 是「先能用、后失灵」的凭证提供者。
//
// 需要它是因为要制造一次**恰好发生在 HalfOpen 上**的凭证错误：那正是
// Allow 文档警告的路径（放行了探测却没归还，熔断器就再也放不出探测）。
// 它包在真提供者外面，因此句柄的签发与兑换语义完全不变。
type switchableCreds struct {
	inner creds.Provider
	mu    sync.Mutex
	fail  bool
}

func (c *switchableCreds) Issue(t ident.Tenant, scope string) (*creds.Handle, error) {
	c.mu.Lock()
	f := c.fail
	c.mu.Unlock()
	if f {
		return nil, errors.New("credential backend down")
	}
	return c.inner.Issue(t, scope)
}

func (c *switchableCreds) Resolve(h *creds.Handle) (string, bool) { return c.inner.Resolve(h) }
func (c *switchableCreds) Revoke(t ident.Tenant)                  { c.inner.Revoke(t) }

func (c *switchableCreds) setFail(v bool) {
	c.mu.Lock()
	c.fail = v
	c.mu.Unlock()
}

// 在 HalfOpen 上撞到凭证错误时，探测额度必须归还。
//
// 这是本包唯一一条能**永久**锁死模型池的路径：Allow 占掉探测额度，
// 凭证签发失败既不报 Success 也不报 Failure，额度就再也没人还。
// 熔断器从此卡在 HalfOpen，Allow 永远返回 false，池再也无法通过
// 真实流量自愈——比「被熔掉」更糟，熔掉至少还会自己恢复。
func TestCredentialErrorReleasesHalfOpenProbe(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	cps := &switchableCreds{inner: creds.NewStaticProvider(creds.WithClock(clk.Now))}
	a := ident.Tenant("tenant-a")
	if err := cps.inner.(*creds.StaticProvider).Put(a, "model-api", "sk-test"); err != nil {
		t.Fatal(err)
	}
	p := gateway.New(gateway.Config{
		Upstream: up, Creds: cps, Clock: clk.Now,
		Quota:   gateway.Quota{MaxCalls: 100, Window: time.Hour},
		Breaker: gateway.BreakerConfig{Threshold: 2, Cooldown: 2 * time.Second, Clock: clk.Now},
	})

	// 打熔断器：2 次上游失败即 Open。
	up.setFail(true)
	for i := 0; i < 2; i++ {
		if _, err := p.Complete(context.Background(), a, req()); err == nil {
			t.Fatal("upstream is failing, call should error")
		}
	}
	if p.Breaker().State() != gateway.StateOpen {
		t.Fatalf("breaker = %v, want open", p.Breaker().State())
	}

	// 推进到冷却之后，同时越过凭证句柄的刷新阈值（TTL 的 4/5），
	// 这样下一次调用会真的去问凭证提供者，而不是命中缓存。
	clk.Advance(13 * time.Minute)
	if p.Breaker().State() != gateway.StateHalfOpen {
		t.Fatalf("breaker = %v, want half-open after cooldown", p.Breaker().State())
	}

	failuresBefore := p.Breaker().Failures()
	callsBefore := up.callCount()
	cps.setFail(true)
	_, err := p.Complete(context.Background(), a, req())
	if err == nil {
		t.Fatal("a failing credential provider must surface an error")
	}
	if up.callCount() != callsBefore {
		t.Fatal("the request must not reach the upstream when credential issuance fails")
	}
	// 配置错误不计入熔断：状态必须仍是 HalfOpen，而不是被推回 Open。
	if got := p.Breaker().State(); got != gateway.StateHalfOpen {
		t.Fatalf("breaker = %v after a credential error, want half-open "+
			"(a local config error must not re-open the pool)", got)
	}
	if got := p.Breaker().Failures(); got != failuresBefore {
		t.Fatalf("failures = %d after a credential error, want %d (unchanged)", got, failuresBefore)
	}

	// 关键断言：凭证恢复正常后，探测额度必须还在——池要能自愈。
	cps.setFail(false)
	up.setFail(false)
	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatalf("the half-open probe was never returned, the pool can no longer "+
			"heal itself: %v", err)
	}
	if up.callCount() != callsBefore+1 {
		t.Fatalf("the recovery probe should reach the upstream, calls = %d want %d",
			up.callCount(), callsBefore+1)
	}
	if got := p.Breaker().State(); got != gateway.StateClosed {
		t.Fatalf("a successful recovery probe should close the breaker, got %v", got)
	}
	if !p.Healthy() {
		t.Fatal("a closed breaker must make the pool healthy again")
	}
}

// 被闸门拒掉的调用不得把成本记到共享的上游账上——池已判定不可用时
// 不该做任何多余的动作，这是 Complete 的文档写下的第二条。
func TestGateRejectionDoesNotPushToUpstream(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	p := gateway.New(gateway.Config{
		Upstream: up, Clock: clk.Now,
		Quota:   gateway.Quota{MaxCalls: 100, Window: time.Hour},
		Breaker: gateway.BreakerConfig{Threshold: 1, Cooldown: time.Hour, Clock: clk.Now},
	})
	a := ident.Tenant("tenant-a")

	// 一次失败即 Open，之后 20 次调用全部只能看到冲突的闸门。
	up.setFail(true)
	_, _ = p.Complete(context.Background(), a, req())
	if p.Breaker().State() != gateway.StateOpen {
		t.Fatalf("breaker = %v, want open", p.Breaker().State())
	}
	spent := p.Account(a).Usage().Calls
	before := up.callCount()
	for i := 0; i < 20; i++ {
		if _, err := p.Complete(context.Background(), a, req()); !errors.Is(err, gateway.ErrUnavailable) {
			t.Fatalf("call %d while open: want ErrUnavailable, got %v", i, err)
		}
	}
	if up.callCount() != before {
		t.Fatal("an open breaker must not reach the upstream")
	}
	// 熔断期打进来的流量是**流量**，不是用量。记上它等于让上游的故障
	// 连带把租户的额度烧掉，故障恢复后租户还要再等一个窗口。
	if got := p.Account(a).Usage().Calls; got != spent {
		t.Fatalf("20 calls rejected by the breaker burned %d units of quota, want 0",
			got-spent)
	}
}

// 运维闸门必须是**纯**闸门：无论怎样交错，它都不许消费、更不许耗尽额度。
//
// 这条断言只有在并发下才有意义，而它正是「把闸门放在预占之后」会踩的坑：
// 那样写等于每次被拒都先短暂占一次额度，N 个并发请求可以瞬间占满只够 1 次
// 的额度，让其中一部分**正常**的拒绝被误报成 ErrQuotaExceeded。代码看上去
// 仍然「会归还」，但错误归因已经错了——租户看到的是「额度用尽」。
func TestDisabledPoolGateIsPureUnderConcurrency(t *testing.T) {
	clk := newClock()
	up := &fakeUpstream{}
	p := gateway.New(gateway.Config{
		Upstream: up, Clock: clk.Now,
		// 额度故意只有 1 次：只要闸门有任何一个瞬间是被「先占后退」实现的，
		// 32 个并发请求里必然有人撞上。
		Quota:         gateway.Quota{MaxCalls: 1, Window: time.Hour},
		PlatformQuota: gateway.Quota{MaxCalls: 1, Window: time.Hour},
	})
	a := ident.Tenant("tenant-a")
	p.SetEnabled(false)
	if p.Healthy() {
		t.Fatal("SetEnabled(false) must be reflected by Healthy()")
	}

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = p.Complete(context.Background(), a, req())
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, gateway.ErrUnavailable) {
			t.Fatalf("concurrent rejected call %d: want ErrUnavailable, got %v", i, err)
		}
	}
	if got := p.Account(a).Usage().Calls; got != 0 {
		t.Fatalf("%d concurrent rejections left %d reserved calls on the account, want 0", n, got)
	}
	if up.callCount() != 0 {
		t.Fatal("a disabled pool must not reach the upstream")
	}

	// 闸门重开后，那 1 次额度必须还在——被拒绝的请求一次都不许吃它。
	p.SetEnabled(true)
	if _, err := p.Complete(context.Background(), a, req()); err != nil {
		t.Fatalf("the single available call should still be there: %v", err)
	}
}

// 熔断状态的名字是**对外词汇**：它进运维面板的告警文本、runbook 和值班
// 交接记录，改名等于改一个已经对外的接口。它平时也永远走不到——只有测试
// 失败时才会被格式化出来，所以覆盖率天然是 0，坏了没有任何东西会变红。
func TestStateVocabularyIsStable(t *testing.T) {
	cases := []struct {
		state gateway.State
		want  string
	}{
		{gateway.StateClosed, "closed"},
		{gateway.StateOpen, "open"},
		{gateway.StateHalfOpen, "half-open"},
		{gateway.State(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.state.String(); got != tc.want {
			t.Errorf("State(%d).String() = %q，期望 %q", int(tc.state), got, tc.want)
		}
	}

	// 三个状态必须给出三个不同的词，否则告警文本分不清是哪一种。
	seen := map[string]gateway.State{}
	for _, st := range []gateway.State{
		gateway.StateClosed, gateway.StateOpen, gateway.StateHalfOpen,
	} {
		name := st.String()
		if prev, dup := seen[name]; dup {
			t.Fatalf("State(%d) 与 State(%d) 都显示成 %q，告警文本分不出来",
				int(st), int(prev), name)
		}
		seen[name] = st
	}
}
