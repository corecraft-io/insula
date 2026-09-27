package creds_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metaRobin/insula/creds"
	"github.com/metaRobin/insula/ident"
)

const (
	tenantA = ident.Tenant("tenant-a")
	tenantB = ident.Tenant("tenant-b")
	scope   = "model-api"

	secretA = "sk-live-A-0123456789abcdef"
	secretB = "sk-live-B-fedcba9876543210"
)

// fakeClock 让过期路径可测。真实时钟下测 TTL 要么 sleep 到测试变慢，
// 要么把 TTL 调到毫秒级从而测不到"边界那一纳秒"。
type fakeClock struct {
	mu sync.Mutex
	at time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{at: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

func newProvider(t *testing.T, opts ...creds.Option) *creds.StaticProvider {
	t.Helper()
	p := creds.NewStaticProvider(opts...)
	for _, kv := range []struct {
		tt     ident.Tenant
		secret string
	}{{tenantA, secretA}, {tenantB, secretB}} {
		if err := p.Put(kv.tt, scope, kv.secret); err != nil {
			t.Fatalf("Put(%s): %v", kv.tt, err)
		}
	}
	return p
}

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

func TestPutRejectsEmptyIdentityOrScope(t *testing.T) {
	p := creds.NewStaticProvider()
	cases := []struct {
		name  string
		t     ident.Tenant
		scope string
	}{
		{"空租户", "", scope},
		{"空 scope", tenantA, ""},
		{"全空", "", ""},
	}
	for _, c := range cases {
		if err := p.Put(c.t, c.scope, secretA); err == nil {
			t.Errorf("%s: Put 接受了非法配置", c.name)
		}
	}
	// 被拒绝的配置不得留下半成品：否则 Issue 会认为凭证存在。
	if _, err := p.Issue(tenantA, scope); !errors.Is(err, creds.ErrNotFound) {
		t.Fatalf("非法 Put 之后 Issue 的 err = %v，期望 ErrNotFound", err)
	}
}

func TestIssueRequiresConfiguredSecret(t *testing.T) {
	p := newProvider(t)

	if _, err := p.Issue(tenantA, scope); err != nil {
		t.Fatalf("已配置的 scope 应当能签发: %v", err)
	}
	if _, err := p.Issue(tenantA, "vector-db"); !errors.Is(err, creds.ErrNotFound) {
		t.Fatalf("未配置的 scope: err = %v，期望 ErrNotFound", err)
	}
	if _, err := p.Issue(ident.Tenant("tenant-c"), scope); !errors.Is(err, creds.ErrNotFound) {
		t.Fatalf("未配置的租户: err = %v，期望 ErrNotFound", err)
	}
	if _, err := p.Issue("", scope); err == nil {
		t.Fatal("空租户身份必须被拒绝：那会变成全局命名空间")
	}
}

func TestPutIsLastWriteWins(t *testing.T) {
	p := newProvider(t)
	if err := p.Put(tenantA, scope, "rotated-"+secretA); err != nil {
		t.Fatal(err)
	}
	h, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := p.Resolve(h)
	if !ok {
		t.Fatal("轮转之后句柄兑换失败")
	}
	if got != "rotated-"+secretA {
		t.Fatalf("兑换到 %q，期望轮转后的值", got)
	}
}

// ---------------------------------------------------------------------------
// 签发 / 兑换
// ---------------------------------------------------------------------------

func TestIssueResolveRoundTrip(t *testing.T) {
	p := newProvider(t)

	h, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if h.Tenant() != tenantA {
		t.Errorf("Handle.Tenant() = %q", h.Tenant())
	}
	if h.Scope() != scope {
		t.Errorf("Handle.Scope() = %q", h.Scope())
	}
	if h.ExpiresAt().IsZero() {
		t.Error("Handle.ExpiresAt() 是零值")
	}

	got, ok := p.Resolve(h)
	if !ok {
		t.Fatal("Resolve 兑不出刚签发的句柄")
	}
	if got != secretA {
		t.Fatalf("兑换到 %q，期望租户 A 的明文", got)
	}
}

// 句柄渲染里只能有租户与作用域，不能有句柄 ID。
//
// 句柄 ID 是 bearer 能力（能猜出它就能兑换明文），而 String() 是日志
// 的默认出口。ID 的形状与唯一性由 internal_test.go 覆盖——它是未导出
// 字段，外部测试读不到，只能间接断言「渲染里没有它」。
func TestHandleRenderingCarriesOnlyTenantAndScope(t *testing.T) {
	p := newProvider(t)

	h1, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}

	for _, h := range []*creds.Handle{h1, h2} {
		got := h.String()
		if !strings.Contains(got, string(tenantA)) || !strings.Contains(got, scope) {
			t.Fatalf("渲染缺少租户或作用域，运维无法区分: %q", got)
		}
		if strings.Contains(got, secretA) || strings.Contains(got, secretB) {
			t.Fatalf("渲染含明文: %q", got)
		}
	}

	// 两个不同的句柄渲染**完全相同**：这正说明渲染里不含任何
	// 能区分句柄的信息（即句柄 ID）。
	if h1.String() != h2.String() {
		t.Fatalf("同一租户的两个句柄渲染不同，说明渲染带上了句柄 ID: %q vs %q",
			h1.String(), h2.String())
	}
}

// ---------------------------------------------------------------------------
// TTL
// ---------------------------------------------------------------------------

func TestHandleExpiresExactlyAtTheTTLBoundary(t *testing.T) {
	clk := newFakeClock()
	const ttl = 15 * time.Minute
	p := newProvider(t, creds.WithTTL(ttl), creds.WithClock(clk.now))

	h, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}
	if want := clk.now().Add(ttl); !h.ExpiresAt().Equal(want) {
		t.Fatalf("ExpiresAt = %v，期望 %v", h.ExpiresAt(), want)
	}
	if h.Expired(clk.now()) {
		t.Fatal("刚签发就被判为过期")
	}

	// 边界前 1ns 仍然有效。
	clk.advance(ttl - time.Nanosecond)
	if h.Expired(clk.now()) {
		t.Fatal("TTL 边界前 1ns 被判为过期")
	}
	if _, ok := p.Resolve(h); !ok {
		t.Fatal("TTL 边界前 1ns 兑换失败")
	}

	// 恰好到期即失效：Expired 的实现是 !now.Before(expiresAt)，
	// 「恰好等于」属于过期，这是有意的——凭证泄漏的时间窗就是 TTL，
	// 不该因为边界比较的宽松多出一个纳秒级的口子。
	clk.advance(time.Nanosecond)
	if !h.Expired(clk.now()) {
		t.Fatal("恰好到期时 Expired 仍报 false")
	}
	if _, ok := p.Resolve(h); ok {
		t.Fatal("过期句柄仍能兑换出明文")
	}
}

// 过期即回收：live map 不能随着时间无界增长。
// 同时钉死 Live 的语义——它统计「签发出去还没回收」的句柄，
// 而不是「当前可用」的句柄（可用性只有 Resolve 能给）。
func TestExpiredHandleIsReclaimedOnFirstResolveAttempt(t *testing.T) {
	clk := newFakeClock()
	p := newProvider(t, creds.WithTTL(time.Minute), creds.WithClock(clk.now))

	h, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Live(tenantA); got != 1 {
		t.Fatalf("Live = %d，期望 1", got)
	}

	clk.advance(2 * time.Minute)

	// 尚未尝试兑换时仍计入 Live：这是有意的偏高，指标宁高勿低。
	if got := p.Live(tenantA); got != 1 {
		t.Fatalf("过期后未兑换时 Live = %d，期望仍为 1（记录尚未回收）", got)
	}
	if _, ok := p.Resolve(h); ok {
		t.Fatal("过期句柄兑换成功")
	}
	if got := p.Live(tenantA); got != 0 {
		t.Fatalf("兑换失败之后 Live = %d，期望 0（记录应被回收）", got)
	}
}

func TestWithTTLIgnoresNonPositiveAndKeepsDefault(t *testing.T) {
	p := newProvider(t, creds.WithTTL(0))
	h, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}
	if got := time.Until(h.ExpiresAt()); got > creds.DefaultTTL || got <= 0 {
		t.Fatalf("TTL 0 之后有效期剩余 %v，期望落在 (0, %v]", got, creds.DefaultTTL)
	}

	q := newProvider(t, creds.WithTTL(-time.Hour))
	h2, err := q.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}
	if h2.Expired(time.Now()) {
		t.Fatal("负 TTL 导致句柄立即过期：负值应当被忽略")
	}
}

func TestWithClockIgnoresNil(t *testing.T) {
	p := newProvider(t, creds.WithClock(nil))
	h, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}
	if h.ExpiresAt().Before(time.Now()) {
		t.Fatal("nil 时钟导致句柄立即过期")
	}
}

// ---------------------------------------------------------------------------
// 撤销
// ---------------------------------------------------------------------------

func TestRevokeDropsOnlyTheTargetTenant(t *testing.T) {
	p := newProvider(t)

	a1, _ := p.Issue(tenantA, scope)
	a2, _ := p.Issue(tenantA, scope)
	b1, _ := p.Issue(tenantB, scope)

	p.Revoke(tenantA)

	if got := p.Live(tenantA); got != 0 {
		t.Errorf("撤销后 tenant-a 的在途句柄 = %d，期望 0", got)
	}
	if got := p.Live(tenantB); got != 1 {
		t.Errorf("撤销 tenant-a 波及了 tenant-b：在途句柄 = %d，期望 1", got)
	}
	for _, h := range []*creds.Handle{a1, a2} {
		if _, ok := p.Resolve(h); ok {
			t.Errorf("撤销后的句柄仍能兑换: %v", h)
		}
	}
	if got, ok := p.Resolve(b1); !ok || got != secretB {
		t.Errorf("未撤销的 tenant-b 句柄兑换失败: %q %v", got, ok)
	}
}

func TestRevokeUnknownTenantIsSafe(t *testing.T) {
	p := newProvider(t)
	h, _ := p.Issue(tenantA, scope)

	p.Revoke(ident.Tenant("tenant-ghost"))

	if _, ok := p.Resolve(h); !ok {
		t.Fatal("撤销一个不存在的租户影响到了已有句柄")
	}
}

// ---------------------------------------------------------------------------
// 跨租户：句柄转交必须被拒
// ---------------------------------------------------------------------------

func TestResolveForRejectsForeignHandle(t *testing.T) {
	p := newProvider(t)

	handleA, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}

	got, err := p.ResolveFor(tenantB, handleA)
	if !errors.Is(err, creds.ErrForeignHandle) {
		t.Fatalf("err = %v，期望 ErrForeignHandle", err)
	}
	if got != "" {
		t.Fatalf("被拒的兑换仍返回了明文 %q", got)
	}
	// 错误消息要能定位是谁拿了谁的句柄，但不得含明文。
	msg := err.Error()
	if strings.Contains(msg, secretA) || strings.Contains(msg, secretB) {
		t.Fatalf("错误消息泄漏了明文: %q", msg)
	}
	if !strings.Contains(msg, string(tenantA)) || !strings.Contains(msg, string(tenantB)) {
		t.Fatalf("错误消息没指出双方租户: %q", msg)
	}

	// 真正的主人不受影响。
	if got, err := p.ResolveFor(tenantA, handleA); err != nil || got != secretA {
		t.Fatalf("主人兑换自己的句柄失败: %q %v", got, err)
	}
}

func TestResolveForNilOrEmptyHandleIsErrNoHandle(t *testing.T) {
	p := newProvider(t)

	if _, err := p.ResolveFor(tenantA, nil); !errors.Is(err, creds.ErrNoHandle) {
		t.Fatalf("nil 句柄: err = %v，期望 ErrNoHandle", err)
	}
	// 空句柄与「凭证过期」是两件事：前者是调用方没给东西，
	// 后者是凭证本身失效了。在鉴权路径上对应的运维动作不同。
	if _, err := p.ResolveFor(tenantA, &creds.Handle{}); !errors.Is(err, creds.ErrNoHandle) {
		t.Fatalf("空句柄: err = %v，期望 ErrNoHandle", err)
	}
	if _, ok := p.Resolve(nil); ok {
		t.Fatal("Resolve(nil) 返回了成功")
	}
}

func TestResolveForExpiredHandleIsErrExpired(t *testing.T) {
	clk := newFakeClock()
	p := newProvider(t, creds.WithTTL(time.Minute), creds.WithClock(clk.now))

	h, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Minute)

	if _, err := p.ResolveFor(tenantA, h); !errors.Is(err, creds.ErrExpired) {
		t.Fatalf("过期句柄: err = %v，期望 ErrExpired", err)
	}
}

func TestResolveForRevokedHandleIsRejected(t *testing.T) {
	p := newProvider(t)
	h, _ := p.Issue(tenantA, scope)
	p.Revoke(tenantA)

	if _, err := p.ResolveFor(tenantA, h); err == nil {
		t.Fatal("撤销后的句柄仍能通过 ResolveFor")
	}
}

// Resolve（宽松版）不做租户校验——这是它的契约，不是缺陷。
// 它只回答「这个句柄在不在有效期内、能不能换出明文」。
// 需要租户校验的调用方必须用 ResolveFor，这条测试把两者的差别钉死，
// 免得有人以为 Resolve 也能防转交。
func TestResolveIsNotTenantScopedByDesign(t *testing.T) {
	p := newProvider(t)
	h, _ := p.Issue(tenantA, scope)

	got, ok := p.Resolve(h)
	if !ok || got != secretA {
		t.Fatalf("Resolve 应当只按句柄兑换: %q %v", got, ok)
	}
	// 而严格版拒绝：同一个句柄，两条路径的结论不同是有意的。
	if _, err := p.ResolveFor(tenantB, h); !errors.Is(err, creds.ErrForeignHandle) {
		t.Fatalf("ResolveFor 没拦住转交: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 渲染不得泄漏明文
// ---------------------------------------------------------------------------

// 这是本包的第一条硬规则：凭证不落盘、不进日志、不进 prompt。
// 把**所有**渲染路径逐个钉死——任何人给 Handle 加一个导出字段、
// 或去掉 GoString，都会在这里变红。
func TestRenderingNeverLeaksPlaintext(t *testing.T) {
	p := newProvider(t)
	h, err := p.Issue(tenantA, scope)
	if err != nil {
		t.Fatal(err)
	}

	hb, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("json.Marshal(handle): %v", err)
	}
	pb, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("json.Marshal(provider): %v", err)
	}

	renderings := map[string]string{
		"handle.String()":   h.String(),
		"handle.GoString()": h.GoString(),
		"handle %v":         fmt.Sprintf("%v", h),
		"handle %+v":        fmt.Sprintf("%+v", h),
		"handle %#v":        fmt.Sprintf("%#v", h),
		"handle %s":         fmt.Sprintf("%s", h),
		"handle Sprint":     fmt.Sprint(h),
		"handle json":       string(hb),
		"provider.String()": p.String(),
		"provider.GoString": p.GoString(),
		"provider %v":       fmt.Sprintf("%v", p),
		"provider %+v":      fmt.Sprintf("%+v", p),
		"provider %#v":      fmt.Sprintf("%#v", p),
		"provider json":     string(pb),
	}

	for name, out := range renderings {
		if strings.Contains(out, secretA) || strings.Contains(out, secretB) {
			t.Errorf("%s 泄漏了明文: %q", name, out)
		}
	}

	// %#v 走的是 GoString；如果拿掉了 GoString，Go 会打印出
	// 结构体全部字段（包括未导出的）——这条断言专门盯那个退化。
	if got := fmt.Sprintf("%#v", h); strings.Contains(got, "expiresAt:") {
		t.Errorf("handle %%#v 落到了默认结构体打印: %q", got)
	}
	if got := fmt.Sprintf("%#v", p); strings.Contains(got, "secrets:") {
		t.Errorf("provider %%#v 落到了默认结构体打印: %q", got)
	}
}

// Redact 是给运维看的降级渲染：保留 3 字符便于区分凭证，其余一律星号。
// 它必须真的不可逆——前 3 字符之外哪怕留下一个原字符，多次出现
// 就能拼回凭证。
func TestRedactKeepsOnlyAThreeCharacterPrefix(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"a", "*"},
		{"ab", "**"},
		{"abcde", "*****"},   // 5 < 6：短串的前缀本身就是信息，全部星号化
		{"abcdef", "abc***"}, // 恰好 6：前缀 3 + min(0,12)=0 星
		{"abcdefg", "abc****"},
		// 3 字保留 + min(26-6,12)=12 星；上界 12 是为了避免
		// 把日志变成一堵星号墙，同时保留足够长度让运维能分辨。
		{"sk-live-A-0123456789abcdef", "sk-" + strings.Repeat("*", 15)},
	}
	for _, c := range cases {
		got := creds.Redact(c.in)
		if got != c.want {
			t.Errorf("Redact(%q) = %q，期望 %q", c.in, got, c.want)
		}
		if len(c.in) < 6 {
			continue
		}
		// 不可逆：第 4 字符起不得留下原文的任何片段。
		if !strings.HasPrefix(got, c.in[:3]) {
			t.Errorf("Redact(%q) = %q 丢了前 3 字符前缀", c.in, got)
		}
		if strings.Contains(got, c.in[3:]) {
			t.Errorf("Redact(%q) = %q 仍含第 4 字符起的原文", c.in, got)
		}
	}
}

// ---------------------------------------------------------------------------
// 并发
// ---------------------------------------------------------------------------

// 句柄的签发/兑换/撤销分别发生在控制面（租户开通、凭证轮转）与数据面
// （模型调用）两侧，天然并发。这条测试的价值主要在 -race 下：
// map 的读写与过期回收的「读-改-写」都必须在一把锁里。
//
// 断言的是**不变量**而不是「每次兑换都成功」：撤销与兑换本来就可以
// 并发发生，此刻兑换失败是正确行为。真正不许发生的是——兑换成功却
// 拿到了别人的明文，或者失败却不是已知的失败原因。
func TestConcurrentIssueResolveRevoke(t *testing.T) {
	clk := newFakeClock()
	p := newProvider(t,
		creds.WithTTL(30*time.Minute), creds.WithClock(clk.now))

	secrets := map[ident.Tenant]string{tenantA: secretA, tenantB: secretB}

	const workers = 8
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tt := tenantA
			if i%2 == 0 {
				tt = tenantB
			}
			for j := 0; j < 200; j++ {
				h, err := p.Issue(tt, scope)
				if err != nil {
					// Issue 只在配置缺失或熵源故障时报错，两者都不该发生。
					t.Errorf("Issue: %v", err)
					return
				}
				if v, err := p.ResolveFor(tt, h); err == nil {
					if v != secrets[tt] {
						t.Errorf("租户 %s 兑换到 %q —— 跨租户泄漏", tt, v)
						return
					}
				} else if !errors.Is(err, creds.ErrExpired) && !errors.Is(err, creds.ErrForeignHandle) {
					t.Errorf("ResolveFor 的失败原因不在已知集合里: %v", err)
					return
				}
				if v, ok := p.Resolve(h); ok && v != secrets[tt] {
					t.Errorf("Resolve 返回了 %q —— 跨租户泄漏", v)
					return
				}
				p.Live(tt)
				if j%97 == 0 {
					p.Revoke(tt)
				}
			}
		}(i)
	}
	// 同时推进时钟，让过期回收路径与签发/兑换交错。
	done := make(chan struct{})
	go func() {
		defer close(done)
		for k := 0; k < 100; k++ {
			clk.advance(time.Second)
		}
	}()
	wg.Wait()
	<-done

	// 撤销是幂等的：反复撤销一个不存在的租户不该影响别人。
	p.Revoke(tenantA)
	if got, err := p.ResolveFor(tenantB, mustIssue(t, p, tenantB)); err != nil || got != secretB {
		t.Fatalf("撤销 tenant-a 之后 tenant-b 不可用: %q %v", got, err)
	}
}

// 没有并发撤销时，每一次兑换都必须成功——否则上面那条测试里
// 「允许失败」的宽容会掩盖一个真的 bug。
func TestConcurrentIssueAndResolveAlwaysSucceeds(t *testing.T) {
	p := newProvider(t, creds.WithTTL(time.Hour))

	const workers = 8
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tt := tenantA
			want := secretA
			if i%2 == 0 {
				tt, want = tenantB, secretB
			}
			for j := 0; j < 300; j++ {
				h, err := p.Issue(tt, scope)
				if err != nil {
					t.Errorf("Issue: %v", err)
					return
				}
				if got, err := p.ResolveFor(tt, h); err != nil || got != want {
					t.Errorf("ResolveFor = %q, %v；期望 %q", got, err, want)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func mustIssue(t *testing.T, p *creds.StaticProvider, tt ident.Tenant) *creds.Handle {
	t.Helper()
	h, err := p.Issue(tt, scope)
	if err != nil {
		t.Fatalf("Issue(%s): %v", tt, err)
	}
	return h
}
