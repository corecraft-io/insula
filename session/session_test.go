package session_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/realm"
	"github.com/corecraft-io/insula/session"
	cordis "github.com/metaRobin/cordis"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

func turnList(n int) []memory.Turn {
	out := make([]memory.Turn, 0, n)
	for i := 0; i < n; i++ {
		role := memory.RoleUser
		if i%2 == 1 {
			role = memory.RoleAssistant
		}
		out = append(out, memory.Turn{Role: role, Content: fmt.Sprintf("m%d", i)})
	}
	return out
}

func mustEntry(t *testing.T, tenant, sess string) *session.Entry {
	t.Helper()
	e, err := session.NewEntry(ident.Tenant(tenant), ident.Session(sess), session.Policy{}, 8)
	if err != nil {
		t.Fatalf("NewEntry(%s, %s): %v", tenant, sess, err)
	}
	return e
}

// ---------------------------------------------------------------------------
// Policy / Plan
// ---------------------------------------------------------------------------

func TestPolicyNormalizeFillsDefaults(t *testing.T) {
	got := session.Policy{}.Normalize()
	if got.SoftLimit != 40 || got.KeepRecent != 12 || got.MaxPromptChars != 12000 {
		t.Fatalf("Normalize() = %+v, want {SoftLimit:40 KeepRecent:12 MaxPromptChars:12000}", got)
	}

	got = session.Policy{SoftLimit: 5, KeepRecent: 3, MaxPromptChars: 100}.Normalize()
	if got.SoftLimit != 5 || got.KeepRecent != 3 || got.MaxPromptChars != 100 {
		t.Fatalf("Normalize() clobbered explicit values: %+v", got)
	}
}

func TestPlanIsNoOpAtOrBelowSoftLimit(t *testing.T) {
	h := session.NewHistory(session.Policy{SoftLimit: 10, KeepRecent: 4})
	for _, n := range []int{0, 1, 9, 10} {
		if p := h.Plan(turnList(n)); p.Compact {
			t.Fatalf("Plan(%d turns) compacted at the soft limit of 10: %+v", n, p)
		}
	}
}

func TestPlanSplitsOlderAndKeep(t *testing.T) {
	h := session.NewHistory(session.Policy{SoftLimit: 10, KeepRecent: 4})
	list := turnList(11)

	p := h.Plan(list)
	if !p.Compact {
		t.Fatal("11 turns against a soft limit of 10 must compact")
	}
	if len(p.Older) != 7 || len(p.Keep) != 4 {
		t.Fatalf("Older/Keep = %d/%d, want 7/4", len(p.Older), len(p.Keep))
	}
	// Keep 必须是**最近**的 4 轮。保留最旧的 4 轮等于把用户刚刚做的事丢掉，
	// 而摘要只能覆盖 Older —— 那部分信息就永久没了。
	if p.Keep[0].Content != "m7" || p.Keep[3].Content != "m10" {
		t.Fatalf("Keep = [%s … %s], want [m7 … m10]",
			p.Keep[0].Content, p.Keep[3].Content)
	}
	// Older 与 Keep 必须无缝衔接：既不重叠，也不漏。
	if p.Older[len(p.Older)-1].Content != "m6" {
		t.Fatalf("Older ends at %s, want m6", p.Older[len(p.Older)-1].Content)
	}

	// 返回的切片必须是副本：调用方就地修改不得污染原列表。
	p.Keep[0].Content = "mutated"
	if list[7].Content != "m7" {
		t.Fatal("Plan returned a slice aliasing the input; mutations leak back")
	}
}

// 策略自相矛盾（保留数 ≥ 现有轮数却还触发了压缩）时宁可不压：
// 压了会把全部上下文换成一段摘要，那是比"历史有点长"严重得多的损失。
func TestPlanRefusesContradictoryPolicy(t *testing.T) {
	h := session.NewHistory(session.Policy{SoftLimit: 2, KeepRecent: 8})
	if p := h.Plan(turnList(5)); p.Compact {
		t.Fatalf("keep >= len must not compact: %+v", p)
	}
}

// 上一轮的摘要在压缩时必须被**折叠进新的摘要输入**，而不是留在 Keep 里。
//
// 留在 Keep 里的后果是多轮之后上下文里堆着好几段互不衔接的摘要；
// 直接丢掉则更糟——早前的信息永久消失。
func TestPlanFoldsPriorSummaryIntoOlder(t *testing.T) {
	h := session.NewHistory(session.Policy{SoftLimit: 4, KeepRecent: 2})
	list := turnList(6)
	list[0] = memory.Turn{Role: memory.RoleSummary, Content: "旧摘要"}

	p := h.Plan(list)
	if !p.Compact {
		t.Fatal("expected compaction")
	}
	if len(p.Older) == 0 || p.Older[0].Role != memory.RoleSummary {
		t.Fatalf("prior summary must be folded into Older, got %+v", p.Older)
	}
	for _, k := range p.Keep {
		if k.Role == memory.RoleSummary {
			t.Fatal("prior summary must not survive in Keep")
		}
	}
}

// ---------------------------------------------------------------------------
// SummaryPrompt
// ---------------------------------------------------------------------------

func TestSummaryPromptReadsFromTail(t *testing.T) {
	// 每轮渲染后长度固定为 7("[user] ") + 4("[NN]") + 36 = 47 字符，
	// 预算 100 只够两轮多一点。
	list := make([]memory.Turn, 0, 10)
	for i := 0; i < 10; i++ {
		list = append(list, memory.Turn{
			Role:    memory.RoleUser,
			Content: fmt.Sprintf("[%02d]%s", i, strings.Repeat("x", 36)),
		})
	}

	prompt := session.SummaryPrompt(list, 100)
	if strings.Contains(prompt, "[00]") {
		t.Fatal("prompt spent its budget on the oldest turns; it must read from the tail")
	}
	if !strings.Contains(prompt, "[09]") {
		t.Fatal("the most recent turn must always be included")
	}
	// 时间是正序的：模型读倒序对话会误判因果。
	if strings.Index(prompt, "[08]") > strings.Index(prompt, "[09]") {
		t.Fatalf("turns must be rendered chronologically:\n%s", prompt)
	}
}

// 单轮就超出全部预算时仍须产出非空提示。空提示会让模型自由发挥出一段
// 无关的摘要，而那段摘要接下来会**替换掉**真实历史。
func TestSummaryPromptAlwaysIncludesAtLeastOneTurn(t *testing.T) {
	huge := []memory.Turn{{Role: memory.RoleUser, Content: "SENTINEL" + strings.Repeat("x", 100000)}}
	prompt := session.SummaryPrompt(huge, 100)
	if prompt == "" {
		t.Fatal("an oversized single turn must still produce a prompt")
	}
	if !strings.Contains(prompt, "SENTINEL") {
		t.Fatalf("the oversized turn was dropped from its own prompt:\n%.200s", prompt)
	}
}

func TestSummaryPromptEmptyForNoTurns(t *testing.T) {
	if got := session.SummaryPrompt(nil, 1000); got != "" {
		t.Fatalf("empty input must produce an empty prompt, got %q", got)
	}
}

func TestSummaryPromptRendersEveryRole(t *testing.T) {
	prompt := session.SummaryPrompt([]memory.Turn{
		{Role: memory.RoleSummary, Content: "S"},
		{Role: memory.RoleTool, Content: "T", ToolName: "search"},
		{Role: memory.RoleTool, Content: "U"},
		{Role: memory.RoleAssistant, Content: "A"},
	}, 10000)

	for _, want := range []string{
		"[此前摘要] S",
		"[search 结果] T",
		"tool 结果] U", // 无名工具必须有一个可读的回退名，而不是空字符串
		"[assistant] A",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	// 摘要指令的四条要求一条都不能少：少一条就可能引入原文没有的推断。
	for _, want := range []string{"关键事实", "寒暄", "推断", "只输出摘要正文"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing requirement %q:\n%s", want, prompt)
		}
	}
}

// ---------------------------------------------------------------------------
// History 统计
// ---------------------------------------------------------------------------

func TestHistoryAccumulatesUsageAndCompactions(t *testing.T) {
	h := session.NewHistory(session.Policy{})
	h.RecordUsage(10, 20, 3)
	h.RecordUsage(1, 2, 1)
	h.RecordTurns(2)
	h.RecordCompaction(7)

	st := h.Stats()
	if st.Usage.TokensIn != 11 || st.Usage.TokensOut != 22 || st.Usage.ToolCalls != 4 || st.Usage.Turns != 2 {
		t.Fatalf("Usage = %+v, want {11 22 4 2}", st.Usage)
	}
	if st.Usage.Total() != 33 {
		t.Fatalf("Usage.Total() = %d, want 33", st.Usage.Total())
	}
	if st.Compactions != 1 || st.SummarizedTurns != 7 {
		t.Fatalf("Stats = %+v, want one compaction folding 7 turns", st)
	}
	if got := h.Policy(); got != (session.Policy{}.Normalize()) {
		t.Fatalf("Policy() = %+v, want defaults", got)
	}
}

// ---------------------------------------------------------------------------
// Scratch
// ---------------------------------------------------------------------------

func TestScratchIsBoundedAndFailsLoudly(t *testing.T) {
	s := session.NewScratch(2)
	if err := s.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("b", 2); err != nil {
		t.Fatal(err)
	}
	// 满了以后新增必须报错。淘汰哪个键是调用方的语义，本类型无从判断，
	// 静默淘汰只会变成一次难以复现的"我的值怎么没了"。
	if err := s.Set("c", 3); err == nil {
		t.Fatal("set beyond capacity must fail rather than silently evict")
	}
	if s.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", s.Len())
	}
	// 覆写已有键不占新额度。
	if err := s.Set("a", 9); err != nil {
		t.Fatalf("overwriting an existing key must succeed: %v", err)
	}
	if v, ok := s.Get("a"); !ok || v != 9 {
		t.Fatalf("Get(a) = %v/%v, want 9/true", v, ok)
	}

	s.Delete("a")
	if _, ok := s.Get("a"); ok {
		t.Fatal("Delete did not remove the key")
	}
	if got := s.Keys(); len(got) != 1 || got[0] != "b" {
		t.Fatalf("Keys() = %v, want [b]", got)
	}
}

func TestScratchZeroMaxFallsBackToFiniteDefault(t *testing.T) {
	s := session.NewScratch(0)
	if s.Len() == 0 && session.DefaultMaxScratchEntries == 0 {
		t.Fatal("DefaultMaxScratchEntries must be positive")
	}
	for i := 0; i < session.DefaultMaxScratchEntries; i++ {
		if err := s.Set(fmt.Sprintf("k%d", i), i); err != nil {
			t.Fatalf("set #%d failed before the default capacity: %v", i, err)
		}
	}
	if err := s.Set("one-too-many", nil); err == nil {
		t.Fatal("max<=0 must fall back to a finite default, not to unbounded")
	}
}

func TestScratchAfterCloseIsUnusable(t *testing.T) {
	s := session.NewScratch(4)
	if err := s.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	s.Close()

	if !s.Closed() {
		t.Fatal("Closed() = false after Close")
	}
	if s.Len() != 0 {
		t.Fatalf("Close must clear entries, Len() = %d", s.Len())
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("a closed scratch must not serve reads")
	}
	if err := s.Set("b", 2); !errors.Is(err, session.ErrClosed) {
		t.Fatalf("Set after Close = %v, want ErrClosed", err)
	}
	s.Close() // 幂等
}

func TestCloseIsIdempotentAndSignalsWaiters(t *testing.T) {
	e := mustEntry(t, "tenant-a", "session-a")

	if e.IsClosed() {
		t.Fatal("a fresh entry must be open")
	}
	select {
	case <-e.Closed():
		t.Fatal("Closed() must not be signalled before Close")
	default:
	}
	if err := e.Scratch.Set("k", 1); err != nil {
		t.Fatal(err)
	}

	e.Close()
	select {
	case <-e.Closed(): // 必须立刻可读，否则数据面的 select 取消会挂住
	default:
		t.Fatal("Closed() must be readable immediately after Close")
	}
	if !e.IsClosed() {
		t.Fatal("IsClosed() = false after Close")
	}
	e.Close() // 幂等：重复 Close 不得 panic

	if !e.Scratch.Closed() || e.Scratch.Len() != 0 {
		t.Fatal("closing the runtime must clear and close its scratch")
	}
	if e.Key() != "tenant-a/session-a" {
		t.Fatalf("Key() = %q", e.Key())
	}
	if e.CreatedAt().IsZero() {
		t.Fatal("CreatedAt() must be set")
	}
}

func TestNewEntryRequiresIdentity(t *testing.T) {
	if _, err := session.NewEntry("", "s", session.Policy{}, 4); err == nil {
		t.Fatal("empty tenant must be rejected")
	}
	if _, err := session.NewEntry("t", "", session.Policy{}, 4); err == nil {
		t.Fatal("empty session must be rejected")
	}
}

// ---------------------------------------------------------------------------
// cordis 集成
// ---------------------------------------------------------------------------

type harness struct {
	t      *testing.T
	app    *cordis.App
	loader *cordis.Loader

	mu   sync.Mutex
	ctxs map[string]*cordis.Context
}

// newHarness 装一个真实 loader，并在插件 Apply 里顺手记下每个入口的上下文。
//
// cordis 的 Entry / Fiber 都不导出 Context，而"两个会话解析到的 history
// 是不是同一个实例"这件事只能从上下文上问——所以测试自己把它捞出来。
func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, app: cordis.New(), ctxs: make(map[string]*cordis.Context)}
	base := session.Plugin()
	h.loader = cordis.NewLoader(h.app, func(name string) (*cordis.Plugin, error) {
		if name != realm.PluginSession {
			return nil, fmt.Errorf("unknown plugin %q", name)
		}
		return &cordis.Plugin{
			Name:     base.Name,
			Validate: base.Validate,
			Apply: func(ctx *cordis.Context, cfg any) error {
				h.mu.Lock()
				h.ctxs[ctx.Entry().ID()] = ctx
				h.mu.Unlock()
				return base.Apply(ctx, cfg)
			},
		}, nil
	})
	return h
}

func (h *harness) run(f func(ctx *cordis.Context)) {
	h.t.Helper()
	if !h.app.DoSync(f) {
		h.t.Fatal("shard stopped")
	}
	if !h.app.Wait() {
		h.t.Fatal("app did not settle")
	}
}

func (h *harness) ctxOf(id string) *cordis.Context {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.ctxs[id]
	if !ok {
		got := make([]string, 0, len(h.ctxs))
		for k := range h.ctxs {
			got = append(got, k)
		}
		h.t.Fatalf("no context captured for entry %q; captured: %v", id, got)
	}
	return c
}

func (h *harness) create(o cordis.EntryOptions, parent string) error {
	h.t.Helper()
	var err error
	h.run(func(*cordis.Context) {
		_, err = h.loader.Tree().Create(o, parent, -1)
	})
	return err
}

func (h *harness) remove(id string) error {
	h.t.Helper()
	var err error
	h.run(func(*cordis.Context) { err = h.loader.Tree().Remove(id) })
	return err
}

// 同一租户的两个会话必须拿到各自的 history 与 scratch。
//
// 这条断言的意义在于：租户隔离是**不够**的。同一个用户的两个会话若共享
// 历史窗口，一次会话的内容会直接出现在另一次会话的上下文里，而那属于
// 用户可感知的串味，不是平台内部细节。
func TestTwoSessionsOfOneTenantAreIsolated(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	rtA := mustEntry(t, "tenant-a", "session-a")
	rtB := mustEntry(t, "tenant-a", "session-b")

	// Isolate 声明在**每个会话入口**上（与 tenant.Manager.sessionOptions 一致）。
	for _, s := range []struct {
		id string
		rt *session.Entry
	}{{"s-1", rtA}, {"s-2", rtB}} {
		if err := h.create(cordis.EntryOptions{
			ID: s.id, Name: realm.PluginSession, Config: s.rt, Isolate: realm.Session(),
		}, ""); err != nil {
			t.Fatalf("create %s: %v", s.id, err)
		}
	}

	ca, cb := h.ctxOf("s-1"), h.ctxOf("s-2")

	ha, oka := ca.Get(realm.ServiceHistory)
	hb, okb := cb.Get(realm.ServiceHistory)
	if !oka || !okb {
		t.Fatalf("both sessions must resolve history: A=%v B=%v", oka, okb)
	}
	if realm.SameInstance(ha, hb) {
		t.Fatal("two sessions of one tenant share one History — session isolation is broken")
	}
	if !realm.SameInstance(ha, rtA.History) || !realm.SameInstance(hb, rtB.History) {
		t.Fatal("a session must resolve its own runtime's History")
	}

	sa, _ := ca.Get(realm.ServiceScratch)
	sb, _ := cb.Get(realm.ServiceScratch)
	if realm.SameInstance(sa, sb) {
		t.Fatal("two sessions of one tenant share one Scratch — session isolation is broken")
	}

	// 行为层面：写进 A 的值在 B 里读不到。
	if err := rtA.Scratch.Set("token", "secret"); err != nil {
		t.Fatal(err)
	}
	if _, ok := rtB.Scratch.Get("token"); ok {
		t.Fatal("a value written into session A's scratch is visible from session B")
	}
}

// 反向验证：把会话 Isolate 声明在**父分组**上（而不是每个会话入口上），
// 同一组内的两个会话会落进同一个域键。
//
// 域键取的是「声明 Isolate 的那个入口」自己的短 ID（loader.go 的
// Entry.realmKey），所以分组入口一声明，组内所有会话都映射到
// "#<分组ID>"。后果是两个能力同名注册，第二个失败，而**黑盒隔离断言
// 依然通过**——因为 AssertIsolated 把"一侧缺失"也当成隔离成立。
//
// 这条钉死了 tenant.Manager.sessionOptions 那条注释的结论：
// Isolate 必须声明在每个会话入口上，它不是保险，是必要条件。
func TestSessionIsolateOnParentGroupBreaksTheSecondSession(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	rtA := mustEntry(t, "tenant-a", "session-a")
	rtB := mustEntry(t, "tenant-a", "session-b")

	// 分组入口带 Isolate，子入口不带（这正是错误的写法）。
	if err := h.create(cordis.EntryOptions{
		ID: "g-1", Group: true, Isolate: realm.Session(),
		Config: []cordis.EntryOptions{},
	}, ""); err != nil {
		t.Fatalf("create group: %v", err)
	}
	for _, s := range []struct {
		id string
		rt *session.Entry
	}{{"s-1", rtA}, {"s-2", rtB}} {
		// 第二个注册同名服务会失败——这里故意忽略返回错误，
		// 要观察的是"之后能解析到什么"。
		_ = h.create(cordis.EntryOptions{
			ID: s.id, Name: realm.PluginSession, Config: s.rt,
		}, "g-1")
	}

	ca, cb := h.ctxOf("g-1:s-1"), h.ctxOf("g-1:s-2")

	_, oka := ca.Get(realm.ServiceHistory)
	_, okb := cb.Get(realm.ServiceHistory)
	if oka == okb {
		t.Fatalf("exactly one of the two sessions should have won the shared realm key, got A=%v B=%v",
			oka, okb)
	}

	// 黑盒隔离断言**看不出**这个问题：一侧缺失被当作隔离成立。
	if err := realm.AssertIsolated(ca, cb, realm.ServiceHistory); err != nil {
		t.Fatalf("AssertIsolated should report no breach here (that is the point): %v", err)
	}
}

// 会话入口卸载 → 效果回收 → 运行时关闭。清理绑定在生命周期上，
// 而不是绑在"调用方记得关"上——后者在真实代码里总是会被漏掉。
func TestUnmountingSessionEntryClosesItsRuntime(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	rt := mustEntry(t, "tenant-a", "session-a")
	if err := h.create(cordis.EntryOptions{
		ID: "s-1", Name: realm.PluginSession, Config: rt, Isolate: realm.Session(),
	}, ""); err != nil {
		t.Fatalf("create: %v", err)
	}
	if rt.IsClosed() {
		t.Fatal("a mounted session must be open")
	}
	if err := rt.Scratch.Set("k", 1); err != nil {
		t.Fatal(err)
	}

	if err := h.remove("s-1"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	if !rt.IsClosed() {
		t.Fatal("unmounting the session entry must close its runtime")
	}
	if !rt.Scratch.Closed() || rt.Scratch.Len() != 0 {
		t.Fatal("closing the runtime must clear and close its scratch")
	}
	select {
	case <-rt.Closed():
	default:
		t.Fatal("Closed() must be readable after unmount")
	}
}
