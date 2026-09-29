package admit_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/corecraft-io/insula/admit"
	"github.com/corecraft-io/insula/ident"
)

// fakeClock 让速率测试不必真的 sleep。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
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

func TestRateLimitRejectsThenRecovers(t *testing.T) {
	clk := newFakeClock()
	p := admit.New(admit.Config{
		Default: admit.Limits{RatePerSecond: 1, Burst: 1, MaxConcurrent: 10},
		Clock:   clk.Now,
	})
	defer p.Close()

	a := ident.Tenant("tenant-a")

	pm1, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer pm1.Release()

	_, err = p.Acquire(a)
	le, ok := admit.AsLimit(err)
	if !ok || le.Reason != admit.ReasonRate {
		t.Fatalf("second acquire err = %v, want rate limit", err)
	}

	clk.Advance(time.Second)
	pm2, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("acquire after refill: %v", err)
	}
	pm2.Release()
}

func TestTenantConcurrencyCapIsPerTenant(t *testing.T) {
	p := admit.New(admit.Config{
		Default: admit.Limits{MaxConcurrent: 1},
	})
	defer p.Close()

	a := ident.Tenant("tenant-a")
	b := ident.Tenant("tenant-b")

	pa, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}

	// 同一租户被自己的并发上限挡住。
	if _, err := p.Acquire(a); err == nil {
		t.Fatal("second acquire for tenant a succeeded, want concurrency limit")
	} else if le, ok := admit.AsLimit(err); !ok || le.Reason != admit.ReasonConcurrency {
		t.Fatalf("err = %v, want tenant concurrency limit", err)
	}

	// 另一租户不受影响——这正是「单租户慢请求不拖垮他人」的最小验证。
	pb, err := p.Acquire(b)
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}

	pa.Release()
	if _, err := p.Acquire(a); err != nil {
		t.Fatalf("acquire a after release: %v", err)
	}
	pb.Release()
}

func TestPlatformTotalBoundsAggregateLoad(t *testing.T) {
	p := admit.New(admit.Config{
		Default:               admit.Limits{MaxConcurrent: 10},
		PlatformMaxConcurrent: 2,
	})
	defer p.Close()

	a := ident.Tenant("tenant-a")
	b := ident.Tenant("tenant-b")

	p1, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("acquire a#1: %v", err)
	}
	p2, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("acquire a#2: %v", err)
	}

	// 每个租户都远未触到自己的配额，但平台总量已满。
	_, err = p.Acquire(b)
	le, ok := admit.AsLimit(err)
	if !ok || le.Reason != admit.ReasonPlatformConcurrency {
		t.Fatalf("err = %v, want platform concurrency limit", err)
	}

	p1.Release()
	p2.Release()

	// 关键：被拒绝的那次请求不能泄漏平台额度。
	// 若拒绝路径少了一次回滚，platConc 会停在高位，下面的获取将持续失败。
	for i := 0; i < 2; i++ {
		pm, err := p.Acquire(b)
		if err != nil {
			t.Fatalf("acquire b after release #%d: %v (platform quota leaked on rejection path)", i, err)
		}
		pm.Release()
	}
}

func TestRejectedAcquireDoesNotLeakTenantQuota(t *testing.T) {
	p := admit.New(admit.Config{
		Default:               admit.Limits{MaxConcurrent: 2},
		PlatformMaxConcurrent: 1,
	})
	defer p.Close()

	a := ident.Tenant("tenant-a")

	pm, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// 10 次注定失败的请求：每次都在平台总量这一步被挡住，
	// 而租户并发计数在它之前尚未自增，因此不该留下任何痕迹。
	for i := 0; i < 10; i++ {
		if _, err := p.Acquire(a); err == nil {
			t.Fatalf("iteration %d unexpectedly succeeded", i)
		}
	}

	if got := p.Stats().InflightPerTen[a]; got != 1 {
		t.Fatalf("inflight after rejections = %d, want 1", got)
	}
	if got := p.Stats().PlatformInUse; got != 1 {
		t.Fatalf("platform in use = %d, want 1", got)
	}

	pm.Release()
	if got := p.Stats().PlatformInUse; got != 0 {
		t.Fatalf("platform in use after release = %d, want 0", got)
	}
}

func TestAcquireRequiresIdentity(t *testing.T) {
	p := admit.New(admit.Config{Default: admit.Limits{MaxConcurrent: 1}})
	defer p.Close()

	if _, err := p.Acquire(""); err == nil {
		t.Fatal("empty tenant identity was admitted")
	}
}

func TestCloseRejectsFurtherAcquires(t *testing.T) {
	p := admit.New(admit.Config{Default: admit.Limits{MaxConcurrent: 1}})
	pm, err := p.Acquire(ident.Tenant("a"))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	p.Close()

	if _, err := p.Acquire(ident.Tenant("a")); !errors.Is(err, admit.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}

	// 在途许可的释放必须仍然有效，否则停机时会计数错乱。
	pm.Release()
	if got := p.Stats().PlatformInUse; got != 0 {
		t.Fatalf("platform in use after release = %d, want 0", got)
	}
}

func TestSetLimitsTakesEffectImmediately(t *testing.T) {
	p := admit.New(admit.Config{Default: admit.Limits{MaxConcurrent: 1}})
	defer p.Close()

	a := ident.Tenant("enterprise")

	p1, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("acquire #1: %v", err)
	}
	if _, err := p.Acquire(a); err == nil {
		t.Fatal("expected concurrency limit at default quota")
	}

	p.SetLimits(a, admit.Limits{MaxConcurrent: 4})
	p2, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("acquire #2 after upgrade: %v", err)
	}
	p1.Release()
	p2.Release()
}

func TestPermitReleaseIsIdempotent(t *testing.T) {
	p := admit.New(admit.Config{Default: admit.Limits{MaxConcurrent: 1}})
	defer p.Close()

	pm, err := p.Acquire(ident.Tenant("a"))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	pm.Release()
	pm.Release()
	pm.Release()

	if got := p.Stats().PlatformInUse; got != 0 {
		t.Fatalf("platform in use = %d, want 0 (negative count from double release)", got)
	}
	if _, err := p.Acquire(ident.Tenant("a")); err != nil {
		t.Fatalf("acquire after triple release: %v", err)
	}
}

// Forget 丢弃的是**速率状态**，不是并发额度的账。
//
// 这两件事在同一个结构里，很容易一起清掉——而清掉 inflight 意味着
// 一个正在注销、尚有请求在途的租户会被放行超过 MaxConcurrent 个新请求，
// 把一次清理变成一次限额绕过。所以这里用一条断言同时钉住两半：
// 桶必须被丢弃（否则拿到的是 ReasonRate），并发账必须保留（否则没有错误）。
func TestForgetDropsRateStateButNotConcurrency(t *testing.T) {
	clk := newFakeClock()
	p := admit.New(admit.Config{
		Default: admit.Limits{RatePerSecond: 1, Burst: 1, MaxConcurrent: 1},
		Clock:   clk.Now,
	})
	defer p.Close()

	a := ident.Tenant("tenant-a")
	held, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer held.Release()

	// 桶已空且不推进时钟：同一个租户应当被速率拦下。
	if _, err := p.Acquire(a); err == nil {
		t.Fatal("前置条件不成立：桶应当是空的")
	} else if le, ok := admit.AsLimit(err); !ok || le.Reason != admit.ReasonRate {
		t.Fatalf("第二个 acquire 的错误 = %v，期望速率限流", err)
	}

	p.Forget(a)

	_, err = p.Acquire(a)
	le, ok := admit.AsLimit(err)
	if !ok {
		t.Fatalf("Forget 之后应当仍被并发额度拦下，得到 err = %v\n"+
			"（没有错误说明 inflight 也被清了：那是一次限额绕过）", err)
	}
	if le.Reason == admit.ReasonRate {
		t.Fatalf("Forget 之后仍报速率限流：令牌桶没有被丢弃，Forget 是空操作")
	}
	if le.Reason != admit.ReasonConcurrency {
		t.Fatalf("拒绝原因 = %v，期望并发上限", le.Reason)
	}

	// 腾出并发额度后必须能正常获取（新桶是满的）。
	held.Release()
	after, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("释放后的 acquire: %v", err)
	}
	after.Release()
}

// 注销之后配额覆盖也要一起失效：否则同一个租户 ID 重新开通时会带着
// 上一轮的套餐跑——而"我明明重新配置过了"是最难归因的一类问题。
func TestForgetClearsTenantOverrides(t *testing.T) {
	p := admit.New(admit.Config{Default: admit.Limits{MaxConcurrent: 1}})
	defer p.Close()

	a := ident.Tenant("tenant-a")
	p.SetLimits(a, admit.Limits{MaxConcurrent: 5})

	var held []*admit.Permit
	for i := 0; i < 3; i++ {
		pm, err := p.Acquire(a)
		if err != nil {
			t.Fatalf("第 %d 次 acquire 被拒（覆盖值应当允许 5 个并发）：%v", i+1, err)
		}
		held = append(held, pm)
	}
	defer func() {
		for _, pm := range held {
			pm.Release()
		}
	}()

	p.Forget(a)

	// 回到默认上限 1：此刻已有 3 个在途，第 4 个必须被拒。
	if _, err := p.Acquire(a); err == nil {
		t.Fatal("Forget 没有清掉配额覆盖：重新开通的租户会带着上一轮的套餐跑")
	} else if le, ok := admit.AsLimit(err); !ok || le.Reason != admit.ReasonConcurrency {
		t.Fatalf("拒绝原因 = %v，期望并发上限", err)
	}
}

// Forget 对未知租户必须是安全的空操作：清理路径上的"没东西可清"
// 不该是一次错误，否则它会被调用方包在 if 里，而那个 if 迟早会写错。
func TestForgetUnknownTenantIsSafe(t *testing.T) {
	p := admit.New(admit.Config{Default: admit.Limits{RatePerSecond: 10, Burst: 10}})
	defer p.Close()

	p.Forget("never-seen")
	p.Forget("")
	if _, err := p.Acquire(ident.Tenant("a")); err != nil {
		t.Fatalf("acquire after forget: %v", err)
	}
}

// 因并发上限被拒的请求不能消耗速率令牌。
//
// 这条规则守的是一个**报错正确性**问题：一个卡在并发上限、不停重试的
// 客户端，如果每次拒绝都扣令牌，它会在几毫秒内把桶烧空，于是拒绝原因
// 从真话（并发满）变成假话（速率超限），并被 Retry-After 按秒级拖住
// ——而它实际上只要等一个槽位空出来就能通过。
func TestConcurrencyRejectionDoesNotBurnRateTokens(t *testing.T) {
	clk := newFakeClock()
	p := admit.New(admit.Config{
		// 桶装 3 个：首个成功请求用掉 1 个，还剩 2 个。若重试会扣令牌，
		// 第 3 次重试就会变成速率超限——这是"令牌被烧掉"的可观测信号。
		// （Burst=1 不行：那时桶本来就被首个请求清空了，两种情形分不开。）
		Default: admit.Limits{RatePerSecond: 1, Burst: 3, MaxConcurrent: 1},
		Clock:   clk.Now,
	})
	defer p.Close()

	a := ident.Tenant("tenant-a")
	held, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer held.Release()

	// 顶着并发上限疯狂重试，一次时钟都不推进。
	for i := 0; i < 50; i++ {
		_, err := p.Acquire(a)
		le, ok := admit.AsLimit(err)
		if !ok {
			t.Fatalf("第 %d 次重试未被拒绝：%v", i+1, err)
		}
		if le.Reason != admit.ReasonConcurrency {
			t.Fatalf("第 %d 次重试的拒绝原因 = %v，期望并发上限。\n"+
				"（变成速率超限说明重试把令牌烧掉了：客户端会以为该等一整秒，"+
				"而它其实只差一个并发槽位）", i+1, le.Reason)
		}
	}

	// 槽位一空，下一次必须立刻成功——令牌一个都没被烧掉。
	held.Release()
	if _, err := p.Acquire(a); err != nil {
		t.Fatalf("槽位空出后仍被拒：%v（令牌被重试烧掉了）", err)
	}
}

// 平台级令牌同理：一个租户的重试不能收紧全局速率。
func TestPlatformRateIsNotBurnedByTenantConcurrencyRejections(t *testing.T) {
	p := admit.New(admit.Config{
		Default:               admit.Limits{MaxConcurrent: 1},
		PlatformRatePerSecond: 1,
		// 同样要给首个成功请求留出余量，否则重试撞的是"桶本来就空"。
		PlatformBurst: 2,
	})
	defer p.Close()

	a := ident.Tenant("tenant-a")
	held, err := p.Acquire(a)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer held.Release()

	for i := 0; i < 20; i++ {
		if _, err := p.Acquire(a); err == nil {
			t.Fatalf("第 %d 次重试未被拒绝", i+1)
		}
	}

	// 别的租户必须完全不受影响：平台桶里还有那个令牌。
	if _, err := p.Acquire(ident.Tenant("tenant-b")); err != nil {
		t.Fatalf("租户 b 被租户 a 的重试拖累了：%v\n"+
			"（一个租户一个请求都没跑成，却收紧了全局速率）", err)
	}
}

// 速率拒绝本身仍然消耗令牌：否则重试永远不会被限速，桶就成了摆设。
func TestRateRejectionStillConsumesTokens(t *testing.T) {
	clk := newFakeClock()
	p := admit.New(admit.Config{
		Default: admit.Limits{RatePerSecond: 1, Burst: 2, MaxConcurrent: 0},
		Clock:   clk.Now,
	})
	defer p.Close()

	a := ident.Tenant("tenant-a")
	var held []*admit.Permit
	for i := 0; i < 2; i++ { // burst=2，前两个放行
		pm, err := p.Acquire(a)
		if err != nil {
			t.Fatalf("第 %d 个 acquire: %v", i+1, err)
		}
		held = append(held, pm)
	}
	for _, pm := range held {
		pm.Release()
	}

	// 桶空，且这次拒绝的原因就是速率：令牌必须保持被消耗的状态。
	for i := 0; i < 5; i++ {
		_, err := p.Acquire(a)
		if le, ok := admit.AsLimit(err); !ok || le.Reason != admit.ReasonRate {
			t.Fatalf("第 %d 次：err = %v，期望持续速率限流", i+1, err)
		}
	}
	clk.Advance(time.Second)
	if _, err := p.Acquire(a); err != nil {
		t.Fatalf("一个速率周期后应当放行：%v", err)
	}
}
