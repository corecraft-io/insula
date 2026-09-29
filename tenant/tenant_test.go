package tenant_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	cordis "github.com/metaRobin/cordis"
	"github.com/metaRobin/insula/audit"
	"github.com/metaRobin/insula/caps"
	"github.com/metaRobin/insula/creds"
	"github.com/metaRobin/insula/gateway"
	"github.com/metaRobin/insula/guard"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/memory"
	"github.com/metaRobin/insula/realm"
	"github.com/metaRobin/insula/session"
	"github.com/metaRobin/insula/tenant"
	"github.com/metaRobin/insula/tools"
)

// ---------------------------------------------------------------------------
// 假件
// ---------------------------------------------------------------------------

// fakeUpstream 是平台级模型后端的替身。它记录租户标签，供"请求确实
// 带着正确的租户身份出去"这类断言使用。
type fakeUpstream struct {
	mu     sync.Mutex
	seen   []ident.Tenant
	reply  string
	handle *creds.Handle
}

func (u *fakeUpstream) Complete(_ context.Context, t ident.Tenant, _ caps.Request,
	cred *creds.Handle) (caps.Response, error) {

	u.mu.Lock()
	defer u.mu.Unlock()
	u.seen = append(u.seen, t)
	u.handle = cred
	reply := u.reply
	if reply == "" {
		reply = "ok"
	}
	return caps.Response{Message: caps.Message{Role: "assistant", Content: reply}}, nil
}

func (u *fakeUpstream) tenants() []ident.Tenant {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]ident.Tenant(nil), u.seen...)
}

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

type harness struct {
	t      testing.TB
	app    *cordis.App
	loader *cordis.Loader
	man    *tenant.Manager
	store  *memory.MemStore
	creds  *creds.StaticProvider
	up     *fakeUpstream
	audit  *audit.Log
}

// newHarness 装配一个完整的租户子系统，并在测试结束时关停。
//
// 接收 testing.TB 而不是 *testing.T：基准测试要用**同一套**装配，
// 否则基准量到的是另一条代码路径，数字再好看也不代表测试覆盖的
// 那条路径。
func newHarness(t testing.TB) *harness {
	t.Helper()
	h := buildHarness(t)
	t.Cleanup(func() { h.app.Close() })
	return h
}

// buildHarness 只装配，**不注册 Cleanup**。
//
// 基准要在循环里反复装配与拆除：t.Cleanup 会把每个 App 的引用一直
// 留到测试结束，b.N 一涨内存就被撑爆，量出来的数也就不是被测代码
// 的数。调用方自己负责 app.Close。测试请用 newHarness。
func buildHarness(t testing.TB) *harness {
	t.Helper()

	app := cordis.New()
	h := &harness{
		t:      t,
		app:    app,
		loader: tenant.NewLoader(app),
		store:  memory.NewMemStore(memory.Config{}),
		creds:  creds.NewStaticProvider(),
		up:     &fakeUpstream{},
		audit:  audit.New(256, nil),
	}

	pool := gateway.New(gateway.Config{
		Upstream: h.up,
		Creds:    h.creds,
		Quota:    gateway.Quota{MaxTokens: 100000, MaxCalls: 1000, Window: time.Minute},
	})

	man, err := tenant.New(tenant.Deps{
		App:    app,
		Loader: h.loader,
		Hasher: realm.NewHasher(nil),
		Pool:   pool,
		Store:  h.store,
		Tools:  platformTools(),
		Audit:  h.audit,
		OnWarn: func(msg string, args ...any) {
			t.Logf("warn: "+msg, args...)
		},
	})
	if err != nil {
		t.Fatalf("tenant.New: %v", err)
	}
	h.man = man

	return h
}

func (h *harness) tree() *cordis.EntryTree { return h.loader.Tree() }

// resolve 在调度器上解析一个入口，返回是否可达。
func (h *harness) resolve(id string) error {
	h.t.Helper()
	var err error
	if !h.app.DoSync(func(*cordis.Context) { _, err = h.tree().Resolve(id) }) {
		h.t.Fatal("shard stopped")
	}
	h.app.Wait()
	return err
}

// occupy 在根组上占住一个短 ID，模拟"这个 ID 已经被别人用了"。
//
// 用一个解析不到的插件名做载体：入口会进 store 索引（这正是我们要的），
// 但没有 Fiber 需要清理。
func (h *harness) occupy(id string) {
	h.t.Helper()
	var err error
	if !h.app.DoSync(func(*cordis.Context) {
		_, err = h.tree().Create(cordis.EntryOptions{ID: id, Name: "squatter"}, "", -1)
	}) {
		h.t.Fatal("shard stopped")
	}
	h.app.Wait()
	if err != nil {
		h.t.Fatalf("occupy %s: %v", id, err)
	}
}

// platformTools 是平台级工具模板：实现与描述共享，白名单由租户自己给。
func platformTools() *tools.TenantConfig {
	return &tools.TenantConfig{
		Handlers: map[string]tools.Handler{
			"echo": tools.HandlerFunc(func(_ context.Context, inv caps.Invocation) (caps.ToolResult, error) {
				return caps.ToolResult{Content: "echo:" + inv.Name}, nil
			}),
			"danger": tools.HandlerFunc(func(context.Context, caps.Invocation) (caps.ToolResult, error) {
				return caps.ToolResult{Content: "should never run"}, nil
			}),
		},
		Specs: map[string]caps.ToolSpec{
			"echo":   {Name: "echo", Description: "回显"},
			"danger": {Name: "danger", Description: "危险操作"},
		},
	}
}

func (h *harness) spec(id string) tenant.Spec {
	return tenant.Spec{
		ID:            ident.Tenant(id),
		Quota:         gateway.Quota{MaxTokens: 1000, MaxCalls: 100, Window: time.Minute},
		ToolAllowlist: []string{"echo"},
		GuardBudget:   guard.Budget{MaxSteps: 4},
		SessionPolicy: session.Policy{SoftLimit: 8, KeepRecent: 3},
	}
}

func (h *harness) mustProvision(specs ...tenant.Spec) {
	h.t.Helper()
	if err := h.man.Provision(specs); err != nil {
		h.t.Fatalf("Provision: %v", err)
	}
}

func (h *harness) mustTenant(id string) *tenant.Tenant {
	h.t.Helper()
	tn, ok := h.man.Get(ident.Tenant(id))
	if !ok {
		h.t.Fatalf("tenant %s not found; provisioned: %v", id, h.man.List())
	}
	return tn
}

// ---------------------------------------------------------------------------
// 装配
// ---------------------------------------------------------------------------

// 开通成功后，租户的能力快照必须把 realm.Services() 里的每一项都解析到，
// 而且解析到的是**该租户自己的**对象。
func TestProvisionResolvesEveryTenantService(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"))

	tn := h.mustTenant("tenant-a")
	snap := tn.Snapshot()

	if !snap.Ready() {
		t.Fatalf("snapshot not ready after provision: %v", snap)
	}
	if snap.Degraded() {
		t.Fatalf("snapshot degraded right after provision: %v", snap)
	}
	if len(snap.Missing) != 0 || len(snap.BadType) != 0 {
		t.Fatalf("Missing/BadType = %v/%v, want both empty", snap.Missing, snap.BadType)
	}
	// 快照的 Present 只列数据面那几项（caps 之类不进快照，见 Take），
	// 所以覆盖度要看 Attached —— 它回答的是"每个服务是否都有提供者"，
	// 这正是"新增服务忘了挂 Watcher"会被抓住的地方。
	if got, want := tn.Caps.Attached(), realm.Services(); !sameSet(got, want) {
		t.Fatalf("attached = %v, want every tenant service %v", got, want)
	}
	if !sameSet(snap.Present, []string{
		realm.ServiceModels, realm.ServiceMemory, realm.ServiceTools, realm.ServiceGuard,
	}) {
		t.Fatalf("Present = %v, want the four data-plane services", snap.Present)
	}
	if snap.Gen == 0 {
		t.Fatal("Gen must advance once watchers attach")
	}

	// 解析到的东西必须带上本租户的身份，而不是随便一个能通过类型检查的值。
	if m, ok := snap.Memory.(*memory.Handle); !ok || m.Tenant() != "tenant-a" {
		t.Fatalf("memory capability = %#v, want a handle scoped to tenant-a", snap.Memory)
	}
	if g, ok := snap.Models.(*gateway.Handle); !ok || g.Tenant() != "tenant-a" {
		t.Fatalf("models capability = %#v, want a handle scoped to tenant-a", snap.Models)
	}
	if snap.Guard == nil || snap.Guard.Budget().MaxSteps != 4 {
		t.Fatalf("guard policy = %+v, want the tenant's budget", snap.Guard)
	}

	// 工具的可见清单必须已按租户白名单裁过。
	if snap.Tools == nil {
		t.Fatal("tools capability missing")
	}
	specs := snap.Tools.Specs()
	if len(specs) != 1 || specs[0].Name != "echo" {
		t.Fatalf("visible tools = %+v, want exactly [echo] (danger is not allowlisted)", specs)
	}
}

// 装配后校验必须真的能抓住"短 ID 被占用导致 reconcile 静默跳过"。
//
// 构造方式：抢在租户开通之前，在根上占住某个子入口的短 ID。
// reconcile 打的是 store 全局短 ID 索引，撞上就只记日志然后跳过
// （loader.go:154-157）——子入口会少建一个而不报错。verifyAssembly 把
// 这个静默行为变成 ErrShortAssembly。
//
// 注意这条也是"校验必须在 Wait 之后"的直接证据：分组插件展开 Config
// 发生在 doLoad 里，与发起的那个 DoSync 闭包共用同一个 goroutine，
// 闭包返回时 Subgroup() 还是 nil。
func TestProvisionDetectsSilentlySkippedChildEntry(t *testing.T) {
	h := newHarness(t)

	blocked := realm.Child(realm.NewHasher(nil).TenantEntry("tenant-a"), realm.ServiceModels)
	h.occupy(blocked)

	err := h.man.Provision([]tenant.Spec{h.spec("tenant-a")})
	if err == nil {
		t.Fatal("a silently skipped child entry must fail the provision")
	}
	if !errors.Is(err, tenant.ErrShortAssembly) {
		t.Fatalf("error = %v, want it to wrap ErrShortAssembly", err)
	}
	if _, ok := h.man.Get("tenant-a"); ok {
		t.Fatal("a short assembly must not be registered as a usable tenant")
	}

	// 我们自己的半成品必须被回滚掉……
	if rerr := h.resolve(realm.NewHasher(nil).TenantEntry("tenant-a")); rerr == nil {
		t.Fatal("the half-assembled tenant entry survived the rollback")
	}
	// ……但占位的那个入口不是我们建的，必须原样还在。
	if rerr := h.resolve(blocked); rerr != nil {
		t.Fatalf("the pre-existing entry was removed by the rollback: %v", rerr)
	}
}

// ---------------------------------------------------------------------------
// 隔离
// ---------------------------------------------------------------------------

func TestTwoTenantsAreIsolated(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"), h.spec("tenant-b"))

	a := h.mustTenant("tenant-a").Snapshot()
	b := h.mustTenant("tenant-b").Snapshot()

	if !a.Ready() || !b.Ready() {
		t.Fatalf("both tenants must be ready: a=%v b=%v", a, b)
	}
	for _, pair := range []struct {
		name string
		x, y any
	}{
		{realm.ServiceModels, a.Models, b.Models},
		{realm.ServiceMemory, a.Memory, b.Memory},
		{realm.ServiceTools, a.Tools, b.Tools},
		{realm.ServiceGuard, a.Guard, b.Guard},
	} {
		if realm.SameInstance(pair.x, pair.y) {
			t.Fatalf("two tenants resolved the same %s instance — realm isolation is broken", pair.name)
		}
	}

	// 黑盒逐服务断言（平台唯一的可编程隔离验证手段）。
	if err := h.man.IsolationCheck("tenant-a", "tenant-b"); err != nil {
		t.Fatalf("IsolationCheck: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 批量语义
// ---------------------------------------------------------------------------

func TestProvisionReportsDuplicatesAndKeepsGoing(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"))

	// 一批里混入重复、空身份与合法项：合法的照常开通，坏项以 error 汇总。
	err := h.man.Provision([]tenant.Spec{
		h.spec("tenant-a"), // 已存在
		{ID: ""},           // 空身份
		h.spec("tenant-b"), // 合法
	})
	if err == nil {
		t.Fatal("the batch must report the bad specs")
	}
	if !errors.Is(err, tenant.ErrDuplicateTenant) {
		t.Fatalf("error = %v, want it to wrap ErrDuplicateTenant", err)
	}
	// 关键：一个坏项不能拖垮整批。
	if _, ok := h.man.Get("tenant-b"); !ok {
		t.Fatalf("a valid spec in the same batch was dropped: %v", h.man.List())
	}
	if snap := h.mustTenant("tenant-b").Snapshot(); !snap.Ready() {
		t.Fatalf("tenant-b was registered but not assembled: %v", snap)
	}
	if got := h.man.List(); len(got) != 2 {
		t.Fatalf("List() = %v, want 2 tenants", got)
	}
}

func TestEnsureIsIdempotent(t *testing.T) {
	h := newHarness(t)

	first, err := h.man.Ensure(h.spec("tenant-a"))
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	second, err := h.man.Ensure(h.spec("tenant-a"))
	if err != nil {
		t.Fatalf("Ensure (again): %v", err)
	}
	if first != second {
		t.Fatal("Ensure must return the existing handle, not provision a second entry")
	}
	if got := h.man.List(); len(got) != 1 {
		t.Fatalf("List() = %v, want one tenant", got)
	}
}

// 创建失败不得动别人的入口。这一条钉的是"回滚必须只清自己造的东西"：
// 并发的两个 Ensure 里，输的那个会把赢的那个拆掉，而 Ensure 还会把
// 这个坏句柄返回给调用方。
func TestFailedProvisionDoesNotRemoveForeignEntry(t *testing.T) {
	h := newHarness(t)

	entryID := realm.NewHasher(nil).TenantEntry("tenant-a")
	if err := realm.CheckTenantEntry(entryID); err != nil {
		t.Fatalf("test setup: %v", err)
	}
	h.occupy(entryID)

	err := h.man.Provision([]tenant.Spec{h.spec("tenant-a")})
	if err == nil {
		t.Fatal("provisioning onto an occupied entry id must fail")
	}
	if _, ok := h.man.Get("tenant-a"); ok {
		t.Fatal("a failed provision must not be registered")
	}

	// 那个入口不是我们建的，必须原样还在。
	if rerr := h.resolve(entryID); rerr != nil {
		t.Fatalf("the pre-existing entry was removed by a failed provision: %v", rerr)
	}
}

// ---------------------------------------------------------------------------
// 会话
// ---------------------------------------------------------------------------

func TestSessionIdsAreNamespacedPerTenant(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"), h.spec("tenant-b"))

	// 两个租户用**同一个**会话名——这是最容易写出串租的输入。
	sa, err := h.man.Session("tenant-a", "default")
	if err != nil {
		t.Fatalf("Session(a): %v", err)
	}
	sb, err := h.man.Session("tenant-b", "default")
	if err != nil {
		t.Fatalf("Session(b): %v", err)
	}

	if sa.EntryID == sb.EntryID {
		t.Fatalf("both tenants got entry id %q — session ids must be namespaced by tenant", sa.EntryID)
	}
	for _, s := range []*tenant.Session{sa, sb} {
		if !strings.HasPrefix(s.EntryID, string(s.Tenant)) && !strings.HasPrefix(s.EntryID, "t-") {
			t.Fatalf("session entry id %q is not namespaced", s.EntryID)
		}
	}
	if realm.SameInstance(sa.Runtime, sb.Runtime) {
		t.Fatal("two tenants share one session runtime")
	}
	if realm.SameInstance(sa.Runtime.History, sb.Runtime.History) {
		t.Fatal("two tenants share one history window")
	}
	if sa.Key() != "tenant-a/default" || sb.Key() != "tenant-b/default" {
		t.Fatalf("Keys = %q/%q", sa.Key(), sb.Key())
	}
}

func TestSessionIsStableAndDroppable(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"))

	first, err := h.man.Session("tenant-a", "chat-1")
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	again, err := h.man.Session("tenant-a", "chat-1")
	if err != nil {
		t.Fatalf("Session (again): %v", err)
	}
	if first != again {
		t.Fatal("re-requesting the same session must return the same handle")
	}
	if got := h.mustTenant("tenant-a").Sessions(); len(got) != 1 || got[0] != "chat-1" {
		t.Fatalf("Sessions() = %v, want [chat-1]", got)
	}

	// 会话数据也写进去，用于验证 DropSession 的数据清理。
	if _, err := h.store.AppendTurn("tenant-a", "chat-1", memory.Turn{
		Role: memory.RoleUser, Content: "hi",
	}); err != nil {
		t.Fatalf("AppendTurn: %v", err)
	}

	if err := h.man.DropSession("tenant-a", "chat-1"); err != nil {
		t.Fatalf("DropSession: %v", err)
	}
	if got := h.mustTenant("tenant-a").Sessions(); len(got) != 0 {
		t.Fatalf("Sessions() = %v after drop, want empty", got)
	}
	if !first.Runtime.IsClosed() {
		t.Fatal("dropping a session must close its runtime")
	}
	// 数据必须一起清：留下"下次同名会话读到上一次内容"是最难解释的行为。
	turns, err := h.store.Turns("tenant-a", "chat-1")
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turns) != 0 {
		t.Fatalf("session data survived DropSession: %+v", turns)
	}

	// 同名会话重建必须是空的。
	reborn, err := h.man.Session("tenant-a", "chat-1")
	if err != nil {
		t.Fatalf("Session (reborn): %v", err)
	}
	if reborn == first {
		t.Fatal("a dropped session must not be reused")
	}
	if turns, _ := h.store.Turns("tenant-a", "chat-1"); len(turns) != 0 {
		t.Fatalf("a reborn session inherited the old window: %+v", turns)
	}
}

func TestSessionRequiresKnownTenant(t *testing.T) {
	h := newHarness(t)
	if _, err := h.man.Session("nobody", "s"); !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("Session(unknown) = %v, want ErrNoTenant", err)
	}

	h.mustProvision(h.spec("tenant-a"))
	if _, err := h.man.Session("tenant-a", ""); err == nil {
		t.Fatal("an empty session identity must be rejected")
	}
}

// ---------------------------------------------------------------------------
// 暂停与注销
// ---------------------------------------------------------------------------

// 暂停必须是**有效**的：Spec.Disabled 写在分组根上是无效的（分组恒启用），
// 必须落到子入口上，才能让能力 Watcher 不激活、快照不就绪。
func TestDisabledTenantIsAssembledButNotRunnable(t *testing.T) {
	h := newHarness(t)
	sp := h.spec("tenant-a")
	sp.Disabled = true
	h.mustProvision(sp)

	tn := h.mustTenant("tenant-a")
	snap := tn.Snapshot()
	if snap.Ready() {
		t.Fatalf("a paused tenant must not report a ready snapshot: %v", snap)
	}
	if len(snap.Present) != 0 {
		t.Fatalf("Present = %v, want empty for a paused tenant", snap.Present)
	}
	// 入口还在——暂停不是注销，必须能原地恢复。
	if err := h.resolve(tn.EntryID); err != nil {
		t.Fatalf("a paused tenant must keep its entry tree: %v", err)
	}

	// 新会话要明确拒绝，而不是造一个永远不会就绪的会话。
	if _, err := h.man.Session("tenant-a", "s"); !errors.Is(err, tenant.ErrTenantDisabled) {
		t.Fatalf("Session on a paused tenant = %v, want ErrTenantDisabled", err)
	}
}

// 注销是级联的：一次 Remove 带走整棵子树，且数据面状态一起清干净。
func TestDeprovisionTearsDownEverythingOfTheTenant(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"), h.spec("tenant-b"))

	sess, err := h.man.Session("tenant-a", "chat-1")
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	if _, err := h.store.AppendTurn("tenant-a", "chat-1", memory.Turn{
		Role: memory.RoleUser, Content: "这条应当随租户一起消失",
	}); err != nil {
		t.Fatalf("AppendTurn: %v", err)
	}
	if err := h.creds.Put("tenant-a", "model-api", "sk-a"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := h.creds.Issue("tenant-a", "model-api"); err != nil {
		t.Fatalf("Issue: %v", err)
	}

	tn := h.mustTenant("tenant-a")
	entryID := tn.EntryID

	if err := h.man.Deprovision([]ident.Tenant{"tenant-a"}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}

	if _, ok := h.man.Get("tenant-a"); ok {
		t.Fatal("deprovisioned tenant still registered")
	}
	if got := h.man.List(); len(got) != 1 || got[0] != "tenant-b" {
		t.Fatalf("List() = %v, want [tenant-b]", got)
	}

	// 入口子树整体摘除：根与会话入口都不该再解析得到。
	// 用全路径寻址——会话入口在租户根之下，短 ID 解析不到它。
	for _, id := range []string{entryID, sess.Path} {
		if err := h.resolve(id); err == nil {
			t.Fatalf("entry %q survived deprovision", id)
		}
	}
	if !sess.Runtime.IsClosed() {
		t.Fatal("deprovision must close every session runtime of the tenant")
	}

	// 凭证句柄不得留在内存里。
	if live := h.creds.Live("tenant-a"); live != 0 {
		t.Fatalf("Live(tenant-a) = %d credential handles after deprovision, want 0", live)
	}
	// 记忆一并清除：留下的历史既占内存，又会被重新开通的同名租户读到。
	if turns, _ := h.store.Turns("tenant-a", "chat-1"); len(turns) != 0 {
		t.Fatalf("tenant memory survived deprovision: %+v", turns)
	}
	// 其他租户不受影响。
	if snap := h.mustTenant("tenant-b").Snapshot(); !snap.Ready() {
		t.Fatalf("deprovisioning tenant-a broke tenant-b: %v", snap)
	}
	// 注销留下的残渣不得影响后续开通：新租户必须与既有租户互相隔离。
	h.mustProvision(h.spec("tenant-c"))
	if err := h.man.IsolationCheck("tenant-b", "tenant-c"); err != nil {
		t.Fatalf("IsolationCheck after deprovision: %v", err)
	}
}

func TestDeprovisionUnknownTenantIsReported(t *testing.T) {
	h := newHarness(t)
	if err := h.man.Deprovision([]ident.Tenant{"nobody"}); !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("Deprovision(unknown) = %v, want ErrNoTenant", err)
	}
}

// ---------------------------------------------------------------------------
// 审计
// ---------------------------------------------------------------------------

func TestProvisionAndDeprovisionAreAudited(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"))
	if err := h.man.Deprovision([]ident.Tenant{"tenant-a"}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}

	actions := map[audit.Action]int{}
	for _, e := range h.audit.For("tenant-a", 0) {
		actions[e.Action]++
		if e.Outcome != audit.OutcomeOK {
			t.Fatalf("unexpected outcome %q on %q", e.Outcome, e.Action)
		}
	}
	if actions[audit.ActionTenantProvision] != 1 || actions[audit.ActionTenantDeprovision] != 1 {
		t.Fatalf("audit actions = %v, want one provision and one deprovision", actions)
	}
	// 审计必须按租户可分离。
	if got := h.audit.For("tenant-b", 0); len(got) != 0 {
		t.Fatalf("audit log leaked %d events to tenant-b", len(got))
	}
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

func has(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, v := range want {
		if !has(got, v) {
			return false
		}
	}
	return true
}

// TestSessionPathIsAFullPathNotAShortID 钉住 cordis 两套寻址的分界。
//
// 这是本仓库记录过的一类静默失败：store 索引以**短 ID** 为扁平键查重，
// 而 Resolve/Remove 认**路径**（":" 分隔）。把 "<租户根>-s0…" 当 path 传进
// Remove，会被当成"根组下的一个同名入口"去解析，报 cannot resolve——
// 而报错信息里那个 ID 看起来完全正确，是最容易看错的一类失败。
//
// 会话入口是嵌套的（t-<hash> : t-<hash>-sessions : t-<hash>-s0…），
// 因此它正是这个坑的高发处：它的短 ID 与它的路径**不同形**。
// 租户根恰好在根组下，两者同形，所以调用方很容易误以为"短 ID 到处能用"。
func TestSessionPathIsAFullPathNotAShortID(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"))

	sess, err := h.man.Session("tenant-a", "chat-1")
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	entryID, ok := h.man.ResolveEntry("tenant-a")
	if !ok {
		t.Fatal("取不到租户根入口 ID")
	}

	// 1) 路径必须由 ":" 分段，且末段就是短 ID（realm.Path 的定义）。
	segments := strings.Split(sess.Path, ":")
	if len(segments) != 3 {
		t.Fatalf("Path = %q，应当由 3 段组成（租户根 : 会话分组 : 会话入口）", sess.Path)
	}
	if segments[0] != entryID {
		t.Errorf("Path 的第一段 = %q，期望租户根 %q", segments[0], entryID)
	}
	if segments[len(segments)-1] != sess.EntryID {
		t.Errorf("Path 的末段 = %q，期望会话短 ID %q", segments[len(segments)-1], sess.EntryID)
	}

	// 2) 会话是嵌套入口，短 ID 与路径必须**不同形**——同形会让这个坑消失，
	//    也会让"短 ID 到处能用"的错误直觉站得住。
	if sess.Path == sess.EntryID {
		t.Fatal("会话的 Path 与 EntryID 相同：嵌套入口不可能是同形的，短 ID 被当成了路径")
	}

	// 3) 用 Path 必须解析得到；这是"它是一条合法路径"的直接证据。
	if _, err := h.tree().Resolve(sess.Path); err != nil {
		t.Fatalf("Resolve(%q) = %v；这条路径应当可用（Remove 走的就是它）", sess.Path, err)
	}

	// 4) 反过来：把**短 ID 当路径**用必须失败。这正是这个坑的形态——
	//    它解析不到就报 cannot resolve，而报错里那个 ID 看起来完全正确。
	//    若它能解析成功，说明两套寻址被混为一谈，静默误路由的前提就成立了。
	if _, err := h.tree().Resolve(sess.EntryID); err == nil {
		t.Fatalf("Resolve(%q)（会话短 ID）成功了：短 ID 被当成了路径", sess.EntryID)
	}
}

// TestConcurrentSameSessionNameLeavesOneEntry 守并发去重的**清理**。
//
// 会话名的唯一性检查发生在入口**建好之后**（先建、再在锁里比、输了就把
// 自己摘掉，见 Manager.Session 末尾）。之所以只能这样做：唯一性判据是
// "这个会话名有没有人注册过"，而注册与建入口无法在一个临界区里完成
// ——建入口要走 cordis 的调度器，不能在锁里等。
//
// 于是"输的那个把自己摘掉"这一步（dropSessionEntry）成了正确性的必需项，
// 而不是打扫卫生：漏摘一个入口，树里就留下一个**重复的短 ID**。而本仓库
// 记录过的地雷正是这个——EntryGroup.reconcile 遇到重复 ID 只记一条日志
// 然后跳过，不报错，随后任何按短 ID 的寻址都可能命中另一个租户的入口。
//
// 所以这条断言不是"entry 数好看"，而是"没有留下第二个入口"。
func TestConcurrentSameSessionNameLeavesOneEntry(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"))

	entryID, ok := h.man.ResolveEntry("tenant-a")
	if !ok {
		t.Fatal("取不到租户根入口 ID")
	}
	sessionsPath := realm.SessionsGroupPath(entryID)

	const K = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	got := make([]*tenant.Session, K)
	errs := make([]error, K)
	for i := 0; i < K; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // 尽可能让 K 个调用挤进同一个窗口
			got[i], errs[i] = h.man.Session("tenant-a", "same")
		}(i)
	}
	close(start)
	wg.Wait()

	// 1) 每个调用都要成功。

	for i, err := range errs {
		if err != nil {
			t.Errorf("第 %d 个并发调用失败：%v", i, err)
		}
	}

	// 2) 必须**全部**拿到同一个句柄。拿到不同句柄意味着有两个调用都
	//    认为自己赢了——那正是"重复入口"的另一种表现。
	winner := got[0]
	if winner == nil {
		t.Fatal("第一个调用没有返回会话")
	}
	for i, s := range got {
		if s != winner {
			t.Errorf("第 %d 个调用拿到不同的会话句柄：去重以先到的为准，不该出现两个赢家", i)
		}
	}

	// 3) 树里只能剩**一个**会话入口。
	group, err := h.tree().Resolve(sessionsPath)
	if err != nil {
		t.Fatalf("Resolve(%q) = %v", sessionsPath, err)
	}
	sub := group.Subgroup()
	if sub == nil {
		t.Fatal("会话分组没有子组：容器入口的分组应当由分组插件建立")
	}
	children := sub.Children()
	if len(children) != 1 {
		var ids []string
		for _, c := range children {
			ids = append(ids, c.Options().ID)
		}
		t.Fatalf("并发建同名会话后树里有 %d 个入口 %v，期望 1 个："+
			"输的那些没有被摘掉，留下的是重复短 ID（静默串租的前提）", len(children), ids)
	}
	if gotID := children[0].Options().ID; gotID != winner.EntryID {
		t.Errorf("树里留下的是 %q，期望赢家的 %q", gotID, winner.EntryID)
	}

	// 4) 索引与树必须一致：两边各自只有一条，且是同一条。
	if sessions := h.mustTenant("tenant-a").Sessions(); len(sessions) != 1 || sessions[0] != "same" {
		t.Errorf("索引里的会话 = %v，期望 [same]", sessions)
	}
	// 5) 留下来的那个入口必须仍然可寻址（清理不能把赢家一起摘了）。
	if _, err := h.tree().Resolve(winner.Path); err != nil {
		t.Errorf("赢家的路径 Resolve(%q) = %v：清理误伤了赢家", winner.Path, err)
	}
}
