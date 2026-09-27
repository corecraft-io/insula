package creds

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metaRobin/insula/ident"
)

// ---------------------------------------------------------------------------
// 为什么这个文件必须是内部测试（package creds 而不是 creds_test）
// ---------------------------------------------------------------------------
//
// Handle 的 tenant / scope 是**自述**字段。授权判断若建立在这两个字段上，
// 就等于让携带者给自己签发通行证。下面几条测试直接构造「改了自述」的
// 句柄——外部包做不到（字段未导出），所以它们只能放在这里。
//
// 它们盯的不是今天能实现的攻击，而是**这个不变量由谁保证**：
// 今天靠「没有构造入口」，明天若加上「由 ID 重建句柄」（反序列化、
// 进程间传递、缓存恢复、给外部适配器用的构造函数），
// 自述身份立刻变成攻击面。把判据放在凭据库一侧，以后加任何入口都绕不过去。

const (
	itTenantA = ident.Tenant("tenant-a")
	itTenantB = ident.Tenant("tenant-b")
	itScope   = "model-api"
	itSecretA = "sk-live-A-0123456789abcdef"
	itSecretB = "sk-live-B-fedcba9876543210"
)

func internalProvider(t *testing.T) *StaticProvider {
	t.Helper()
	p := NewStaticProvider()
	for _, kv := range []struct {
		tt     ident.Tenant
		secret string
	}{{itTenantA, itSecretA}, {itTenantB, itSecretB}} {
		if err := p.Put(kv.tt, itScope, kv.secret); err != nil {
			t.Fatalf("Put(%s): %v", kv.tt, err)
		}
	}
	return p
}

// ---------------------------------------------------------------------------
// 句柄 ID：形状与唯一性
// ---------------------------------------------------------------------------

// 句柄 ID 未导出，外部测试读不到，所以它的形状（长度、hex、随机来源）
// 只能在这里断言。这些性质是「句柄不可伪造」的全部依据。
func TestHandleIDIsRandomHexAndUnique(t *testing.T) {
	p := internalProvider(t)

	const n = 2048
	seen := make(map[string]struct{}, n)
	prefixes := make(map[string]struct{}, 256)

	for i := 0; i < n; i++ {
		h, err := p.Issue(itTenantA, itScope)
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		if len(h.id) != 32 {
			t.Fatalf("句柄 ID 长度 %d，期望 32（16 字节 hex）: %q", len(h.id), h.id)
		}
		if _, err := hex.DecodeString(h.id); err != nil {
			t.Fatalf("句柄 ID 不是合法 hex: %q", h.id)
		}
		if _, dup := seen[h.id]; dup {
			t.Fatalf("第 %d 次 Issue 撞上了已有句柄 ID %q —— 句柄可预测即可伪造", i, h.id)
		}
		seen[h.id] = struct{}{}
		prefixes[h.id[:4]] = struct{}{}
	}

	// 头两个字节有 256 种取值。若哪天有人把加密随机换成计数器
	// （例如为了「可复现的测试」），不同前缀数会塌到个位数。
	if len(prefixes) < 200 {
		t.Fatalf("%d 个句柄只出现 %d 种前缀，句柄 ID 已不是随机的", n, len(prefixes))
	}
}

// 句柄 ID 与明文没有任何可推导关系：把明文切片、取摘要、
// 或者用密钥派生都会让「泄漏一次明文就能推出全部句柄」成立。
func TestHandleIDIsNotDerivedFromTheSecret(t *testing.T) {
	p := internalProvider(t)
	h, err := p.Issue(itTenantA, itScope)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(h.id, itSecretA) || strings.Contains(itSecretA, h.id) {
		t.Fatalf("句柄 ID %q 与明文有关联", h.id)
	}
	// 同一个明文两次签发必须得到两个不同句柄。
	h2, err := p.Issue(itTenantA, itScope)
	if err != nil {
		t.Fatal(err)
	}
	if h.id == h2.id {
		t.Fatal("同一明文两次签发得到了同一个句柄 ID")
	}
}

// ---------------------------------------------------------------------------
// 授权判据必须来自凭据库，而不是句柄的自述身份
// ---------------------------------------------------------------------------

// 这条是本包最吃重的一条。
//
// 构造一个「ID 属于 tenant-a、但自述为 tenant-b」的句柄，
// 然后用 tenant-b 的名义兑换。若授权判断读的是 h.tenant，
// 检查会通过，tenant-a 的明文就落到了 tenant-b 手里。
func TestResolveRefusesHandleWhoseSelfDescriptionDisagrees(t *testing.T) {
	p := internalProvider(t)
	real, err := p.Issue(itTenantA, itScope)
	if err != nil {
		t.Fatal(err)
	}

	forged := &Handle{
		id:        real.id,
		tenant:    itTenantB, // 自述被改成了别人
		scope:     itScope,
		expiresAt: real.expiresAt,
	}

	if got, ok := p.Resolve(forged); ok {
		t.Fatalf("Resolve 兑换了自述租户与记录不符的句柄，拿到 %q", got)
	}
	got, err := p.ResolveFor(itTenantB, forged)
	if !errors.Is(err, ErrForeignHandle) {
		t.Fatalf("ResolveFor err = %v，期望 ErrForeignHandle", err)
	}
	if got != "" {
		t.Fatalf("ResolveFor 把 tenant-a 的明文发给了 tenant-b: %q", got)
	}

	// 即使以**真正的**所有者名义兑换也要拒绝：自述与记录不符本身
	// 就是「这个句柄被人动过」的证据，不该被"恰好对上了"掩盖过去。
	if _, err := p.ResolveFor(itTenantA, forged); !errors.Is(err, ErrForeignHandle) {
		t.Fatalf("ResolveFor(真主人, 被改造的句柄) err = %v，期望 ErrForeignHandle", err)
	}
}

// 拒绝路径不得顺手毁掉记录：否则攻击者只要不停地递假句柄，
// 就能把真租户的在途句柄清空，把「拒绝」变成一次服务中断。
func TestRefusalDoesNotConsumeTheLiveEntry(t *testing.T) {
	p := internalProvider(t)
	real, err := p.Issue(itTenantA, itScope)
	if err != nil {
		t.Fatal(err)
	}

	forged := &Handle{id: real.id, tenant: itTenantB, scope: itScope, expiresAt: real.expiresAt}
	for i := 0; i < 8; i++ {
		if _, ok := p.Resolve(forged); ok {
			t.Fatal("伪造句柄兑换成功")
		}
	}

	if got := p.Live(itTenantA); got != 1 {
		t.Fatalf("多次被拒之后 tenant-a 的在途句柄 = %d，期望 1", got)
	}
	if got, err := p.ResolveFor(itTenantA, real); err != nil || got != itSecretA {
		t.Fatalf("真句柄被伪造尝试牵连: %q %v", got, err)
	}
}

// 同样的规则作用于 scope：作用域不同意味着去问另一个系统拿凭证，
// 自述被改成别人作用域的句柄必须被拒。
func TestResolveRefusesHandleWhoseScopeDisagrees(t *testing.T) {
	p := internalProvider(t)
	if err := p.Put(itTenantA, "vector-db", "vec-"+itSecretA); err != nil {
		t.Fatal(err)
	}

	modelHandle, err := p.Issue(itTenantA, itScope)
	if err != nil {
		t.Fatal(err)
	}
	// 拿着 model-api 的句柄，自述成 vector-db。
	forged := &Handle{
		id:        modelHandle.id,
		tenant:    itTenantA,
		scope:     "vector-db",
		expiresAt: modelHandle.expiresAt,
	}

	got, ok := p.Resolve(forged)
	if ok {
		t.Fatalf("Resolve 兑换了作用域与记录不符的句柄，拿到 %q", got)
	}
	if _, err := p.ResolveFor(itTenantA, forged); !errors.Is(err, ErrForeignHandle) {
		t.Fatalf("ResolveFor err = %v，期望 ErrForeignHandle", err)
	}
	// 真句柄照常工作。
	if got, err := p.ResolveFor(itTenantA, modelHandle); err != nil || got != itSecretA {
		t.Fatalf("真句柄失败: %q %v", got, err)
	}
}

// 错误消息会被写进日志，因此不得含明文，也不得含句柄 ID 全文——
// 句柄 ID 是 bearer 能力。
func TestForeignHandleErrorLeaksNeitherSecretNorFullHandleID(t *testing.T) {
	p := internalProvider(t)
	real, err := p.Issue(itTenantA, itScope)
	if err != nil {
		t.Fatal(err)
	}
	forged := &Handle{id: real.id, tenant: itTenantB, scope: itScope, expiresAt: real.expiresAt}

	_, err = p.ResolveFor(itTenantB, forged)
	if err == nil {
		t.Fatal("期望报错")
	}
	msg := err.Error()
	if strings.Contains(msg, itSecretA) || strings.Contains(msg, itSecretB) {
		t.Fatalf("错误消息含明文: %q", msg)
	}
	if strings.Contains(msg, real.id) {
		t.Fatalf("错误消息含句柄 ID 全文（bearer 能力）: %q", msg)
	}
	if !strings.Contains(msg, string(itTenantA)) || !strings.Contains(msg, string(itTenantB)) {
		t.Fatalf("错误消息没指出双方租户，排查无从下手: %q", msg)
	}
}

// 记录里的 tenant/scope 必须真的存了：lookup 的判据来自它们。
// 这条测试直接检查 Issue 写进 live 的记录，防止哪天有人为了省内存
// 把 scope 字段删掉、改成从 key 解析——那样判据就变成了字符串解析，
// 而 key 里 scope 之前的部分是租户名。租户名可以含 "|" 吗？
// 现在不可以，但"现在不可以"不是一个可以依赖的前提。
func TestIssueRecordsOwnerAndScopeInTheLiveEntry(t *testing.T) {
	p := internalProvider(t)
	h, err := p.Issue(itTenantA, itScope)
	if err != nil {
		t.Fatal(err)
	}

	p.mu.Lock()
	e, ok := p.live[h.id]
	p.mu.Unlock()
	if !ok {
		t.Fatal("Issue 没有登记 live 记录")
	}
	if e.tenant != itTenantA {
		t.Errorf("记录里的 tenant = %q，期望 %q", e.tenant, itTenantA)
	}
	if e.scope != itScope {
		t.Errorf("记录里的 scope = %q，期望 %q", e.scope, itScope)
	}
	if !e.expiresAt.Equal(h.expiresAt) {
		t.Errorf("记录里的到期时刻与句柄不一致: %v vs %v", e.expiresAt, h.expiresAt)
	}
}

// ---------------------------------------------------------------------------
// 随机源失败
// ---------------------------------------------------------------------------

// errReader 让 crypto/rand 失败，覆盖 Issue 的错误分支。
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("熵池不可用") }

func (errReader) ReadAt([]byte, int64) (int, error) { return 0, errors.New("熵池不可用") }

var _ = rand.Reader

// 拿不到熵时必须报错，绝不能退回可预测的 ID：
// 可预测的句柄 ID 等于把明文交给任何能猜 ID 的代码。
func TestIssueFailsWhenTheEntropySourceFails(t *testing.T) {
	p := internalProvider(t)
	p.rand = errReader{}

	h, err := p.Issue(itTenantA, itScope)
	if err == nil {
		t.Fatalf("熵源失败时 Issue 竟然成功了: %v", h)
	}
	if h != nil {
		t.Fatalf("失败路径返回了非 nil 句柄: %v", h)
	}
	// 失败不得留下半个记录。
	if got := p.Live(itTenantA); got != 0 {
		t.Fatalf("签发失败仍在 live 里留下了 %d 条记录", got)
	}
}

// ---------------------------------------------------------------------------
// 并发
// ---------------------------------------------------------------------------

// 一条记录只能被兑换一次「回收」——即过期时多个 goroutine 同时进来，
// 不能有人删掉之后别人又读到。这条测试主要在 -race 下有意义。
func TestConcurrentExpiryReclaimIsRaceFree(t *testing.T) {
	clk := struct {
		mu sync.Mutex
		at time.Time
	}{at: time.Now()}
	p := NewStaticProvider(
		WithTTL(time.Minute),
		WithClock(func() time.Time { clk.mu.Lock(); defer clk.mu.Unlock(); return clk.at }),
	)
	if err := p.Put(itTenantA, itScope, itSecretA); err != nil {
		t.Fatal(err)
	}

	const n = 64
	handles := make([]*Handle, 0, n)
	for i := 0; i < n; i++ {
		h, err := p.Issue(itTenantA, itScope)
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
	}

	clk.mu.Lock()
	clk.at = clk.at.Add(2 * time.Minute)
	clk.mu.Unlock()

	var wg sync.WaitGroup
	for _, h := range handles {
		wg.Add(1)
		go func(h *Handle) {
			defer wg.Done()
			if _, ok := p.Resolve(h); ok {
				t.Error("过期句柄兑换成功")
			}
		}(h)
	}
	wg.Wait()

	if got := p.Live(itTenantA); got != 0 {
		t.Fatalf("过期后 live = %d，期望全部回收", got)
	}
}
