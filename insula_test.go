package insula_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corecraft-io/insula"
	"github.com/corecraft-io/insula/admit"
	"github.com/corecraft-io/insula/audit"
	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/creds"
	"github.com/corecraft-io/insula/edge"
	"github.com/corecraft-io/insula/gateway"
	"github.com/corecraft-io/insula/guard"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/session"
	"github.com/corecraft-io/insula/shard"
	"github.com/corecraft-io/insula/tenant"
	"github.com/corecraft-io/insula/tools"
)

// ---------------------------------------------------------------------------
// 假件：只替换"外部世界"，其余全部是真的
// ---------------------------------------------------------------------------

// fakeUpstream 是平台级的模型后端。
//
// 它记录每一次调用带的租户标签与消息序列——这两样正好是端到端测试
// 唯一无法从响应里看出来的东西：请求到底带着谁的身份出去了、
// 历史有没有真的接上。
type fakeUpstream struct {
	mu    sync.Mutex
	seen  []ident.Tenant
	reqs  []caps.Request
	reply string
}

func (u *fakeUpstream) Complete(_ context.Context, t ident.Tenant, req caps.Request,
	_ *creds.Handle) (caps.Response, error) {

	u.mu.Lock()
	defer u.mu.Unlock()
	u.seen = append(u.seen, t)
	u.reqs = append(u.reqs, req)
	reply := u.reply
	if reply == "" {
		reply = "回答"
	}
	return caps.Response{
		Message:          caps.Message{Role: "assistant", Content: reply},
		PromptTokens:     11,
		CompletionTokens: 5,
	}, nil
}

func (u *fakeUpstream) calls() ([]ident.Tenant, []caps.Request) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]ident.Tenant(nil), u.seen...), append([]caps.Request(nil), u.reqs...)
}

func echoTools() *tools.TenantConfig {
	return &tools.TenantConfig{
		Handlers: map[string]tools.Handler{
			"echo": tools.HandlerFunc(func(_ context.Context, inv caps.Invocation) (caps.ToolResult, error) {
				return caps.ToolResult{Content: "echo:" + fmt.Sprint(inv.Args["q"])}, nil
			}),
		},
		Specs: map[string]caps.ToolSpec{"echo": {Name: "echo", Description: "回显"}},
	}
}

type harness struct {
	t        *testing.T
	svc      *insula.Service
	up       *fakeUpstream
	creds    *creds.StaticProvider
	auth     *edge.StaticTokens
	warns    *warnLog
	tenants  []tenant.Spec
	baseCfg  insula.Config
	httpSrv  *httptest.Server
	listener net.Listener
}

type warnLog struct {
	mu   sync.Mutex
	msgs []string
}

func (w *warnLog) add(msg string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, fmt.Sprintf(msg, args...))
}

func (w *warnLog) joined() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Join(w.msgs, "\n")
}

// credScope 是 gateway 的默认凭证作用域。测试里显式写出来，
// 这样"scope 不匹配会怎样"就不是一个隐藏假设。
const credScope = "model-api"

func spec(id string) tenant.Spec {
	return tenant.Spec{
		ID:            ident.Tenant(id),
		Quota:         gateway.Quota{MaxTokens: 100000, MaxCalls: 1000, Window: time.Minute},
		ToolAllowlist: []string{"echo"},
		GuardBudget:   guard.Budget{MaxSteps: 6, MaxToolCalls: 6},
		SessionPolicy: session.Policy{SoftLimit: 40, KeepRecent: 12},
	}
}

// newHarness 装一个**完全真实**的平台，只把上游模型且鉴权令牌换成假的。
func newHarness(t *testing.T, mutate func(*insula.Config)) *harness {
	t.Helper()
	h := &harness{
		t:     t,
		up:    &fakeUpstream{},
		creds: creds.NewStaticProvider(),
		auth:  edge.NewStaticTokens(),
		warns: &warnLog{},
	}
	for _, id := range []string{"tenant-a", "tenant-b"} {
		// scope 必须与 gateway 的默认值一致，否则网关换不到凭证。
		if err := h.creds.Put(ident.Tenant(id), credScope, "secret-"+id); err != nil {
			t.Fatalf("creds.Put: %v", err)
		}
	}
	if err := h.auth.Put("tok-a", edge.Principal{Tenant: "tenant-a", Actor: "alice"}); err != nil {
		t.Fatalf("auth.Put: %v", err)
	}
	if err := h.auth.Put("tok-b", edge.Principal{Tenant: "tenant-b", Actor: "bob"}); err != nil {
		t.Fatalf("auth.Put: %v", err)
	}

	h.tenants = []tenant.Spec{spec("tenant-a"), spec("tenant-b")}
	cfg := insula.Config{
		Shards:       2,
		Upstream:     h.up,
		Creds:        h.creds,
		Auth:         h.auth,
		Tools:        echoTools(),
		SystemPrompt: "你是平台的助手。",
		Tenants:      h.tenants,
		CredScope:    credScope,
		Admit:        admit.Config{Default: admit.Limits{MaxConcurrent: 4}},
		OnWarn:       h.warns.add,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	h.baseCfg = cfg

	svc, err := insula.Open(cfg)
	if err != nil {
		t.Fatalf("insula.Open: %v", err)
	}
	h.svc = svc
	t.Cleanup(func() { _ = svc.Close() })
	return h
}

// post 发一次请求，走真实的 HTTP 栈（httptest.NewServer）。
func (h *harness) post(token, body, query string) (*http.Response, string) {
	h.t.Helper()
	if h.httpSrv == nil {
		h.httpSrv = httptest.NewServer(h.svc.Handler())
		h.t.Cleanup(h.httpSrv.Close)
	}
	url := h.httpSrv.URL + "/v1/runs"
	if query != "" {
		url += "?" + query
	}
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		h.t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := h.httpSrv.Client().Do(req)
	if err != nil {
		h.t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("ReadAll: %v", err)
	}
	return resp, string(raw)
}

func (h *harness) runOK(token, input string) edge.RunResponse {
	h.t.Helper()
	resp, body := h.post(token, fmt.Sprintf(`{"input":%q}`, input), "")
	if resp.StatusCode != http.StatusOK {
		h.t.Fatalf("状态码 = %d：%s", resp.StatusCode, body)
	}
	var out edge.RunResponse
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		h.t.Fatalf("响应不是合法 JSON: %v\n%s", err, body)
	}
	return out
}

// capabilities 从租户真正所在的分片上取能力快照。
//
// 不用 Pool.Capabilities（那在生产路径上是对的），是为了让测试在这里
// 显式走一遍"找到它在哪一片"——万一粘性路由坏了，这条测试会以
// "找不到租户"的形式失败，而不是悄悄换一片继续通过。
func (h *harness) capabilities(t *testing.T, id ident.Tenant) caps.Snapshot {
	t.Helper()
	for _, s := range h.svc.Shards() {
		if _, ok := s.Get(id); !ok {
			continue
		}
		snap, err := s.Capabilities(id)
		if err != nil {
			t.Fatalf("Capabilities(%s) @%d: %v", id, s.ID(), err)
		}
		return snap
	}
	t.Fatalf("租户 %s 不在任何分片上", id)
	return caps.Snapshot{}
}

// ---------------------------------------------------------------------------
// 构造契约
// ---------------------------------------------------------------------------

func TestOpenRequiresExternalInputs(t *testing.T) {
	base := func() insula.Config {
		return insula.Config{
			Shards:   1,
			Upstream: &fakeUpstream{},
			Creds:    creds.NewStaticProvider(),
			Auth:     edge.NewStaticTokens(),
		}
	}

	c := base()
	c.Upstream = nil
	if _, err := insula.Open(c); err == nil {
		t.Error("缺模型上游必须构造失败：平台无法替调用方决定模型从哪来")
	}

	c = base()
	c.Creds = nil
	if _, err := insula.Open(c); err == nil {
		t.Error("缺凭证提供者必须构造失败")
	}

	c = base()
	c.Auth = nil
	if _, err := insula.Open(c); err == nil {
		t.Error("缺鉴权必须构造失败：没有它，租户身份就没有来源")
	}
}

func TestOpenWiresEveryLayer(t *testing.T) {
	h := newHarness(t, nil)
	svc := h.svc

	if svc.ShardCount() != 2 {
		t.Errorf("分片数 = %d", svc.ShardCount())
	}
	if len(svc.Shards()) != 2 {
		t.Errorf("Shards() 长度 = %d", len(svc.Shards()))
	}
	if svc.Handler() == nil {
		t.Fatal("Handler 为空")
	}
	// nil 时应当被补上，而不是留一个 nil 让每一处调用都写一遍判空。
	for name, v := range map[string]any{
		"Metrics": svc.Metrics(),
		"Audit":   svc.Audit(),
		"Store":   svc.Store(),
		"Admit":   svc.Admit(),
		"Gateway": svc.Gateway(),
	} {
		if v == nil {
			t.Errorf("%s 为空", name)
		}
	}
	if svc.Closed() {
		t.Error("刚打开的平台不该是停机状态")
	}
	if got := len(svc.Tenants()); got != 2 {
		t.Errorf("启动租户数 = %d，期望 2", got)
	}
	if _, err := svc.SelfCheck(); err != nil {
		t.Errorf("启动后的自检失败：%v", err)
	}
}

// 启动时开通失败必须让 Open 整体失败，而不是"起来了但少了几个租户"。
func TestOpenFailsOnBadStartupTenant(t *testing.T) {
	h := &harness{up: &fakeUpstream{}, creds: creds.NewStaticProvider(), auth: edge.NewStaticTokens(), warns: &warnLog{}}
	cfg := insula.Config{
		Shards:   1,
		Upstream: h.up,
		Creds:    h.creds,
		Auth:     h.auth,
		Tenants:  []tenant.Spec{spec("good"), {ID: ""}},
	}
	svc, err := insula.Open(cfg)
	if err == nil {
		_ = svc.Close()
		t.Fatal("启动租户里有非法项时 Open 必须失败：'起来了但少一个租户'是最难查的一类问题")
	}
	if svc != nil {
		t.Error("失败时不该返回半启动的平台")
	}
}

func TestOpenWithSelfCheckDisabled(t *testing.T) {
	svc, err := insula.Open(insula.Config{
		Shards:   1,
		Upstream: &fakeUpstream{},
		Creds:    creds.NewStaticProvider(),
		Auth:     edge.NewStaticTokens(),
		Admit:    admit.Config{Default: admit.Limits{MaxConcurrent: 2}},
	}.WithSelfCheckDisabled())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer svc.Close()
	if _, err := svc.SelfCheck(); err != nil {
		t.Errorf("关掉的是启动那一次，不是自检本身：%v", err)
	}
}

// Open 默认必须跑一次启动自检。
//
// 这条断言的可观测信号是隔离自检的计数器：它证明"装配完之后确实比过
// 每个分片内的租户对"。少了这一步，构造被破坏这类回归就只能等到
// 跨租户串数据时才被发现。
func TestOpenRunsStartupSelfCheck(t *testing.T) {
	h := newHarness(t, func(c *insula.Config) {
		// 单片才能保证两个租户构成一对可比。
		c.Shards = 1
		c.Tenants = []tenant.Spec{spec("tenant-a"), spec("tenant-b")}
	})

	if n := h.svc.Metrics().Global().Snapshot("").IsolationChecks; n == 0 {
		t.Error("Open 之后隔离自检计数为 0：启动自检没有跑")
	}
	if n := h.svc.Metrics().Global().Snapshot("").IsolationBreaches; n != 0 {
		t.Errorf("干净的装配报出了 %d 次隔离破坏", n)
	}
}

// ---------------------------------------------------------------------------
// 端到端
// ---------------------------------------------------------------------------

// 这是整个仓库里唯一一条把全部层串起来的测试。
//
// 它存在的理由：每一层的单元测试都只能证明"我在我的假设下是对的"。
// 层与层之间的接缝——能力句柄的租户归属、审计里的 actor、指标记到谁
// 头上、请求带着谁的身份出去——只有在这里才能被证明。
func TestEndToEndRun(t *testing.T) {
	h := newHarness(t, nil)

	out := h.runOK("tok-a", "你好")
	if out.Output != "回答" || out.Stop != "completed" || out.Steps != 1 {
		t.Fatalf("结果 = %+v", out)
	}
	if out.TokensIn != 11 || out.TokensOut != 5 {
		t.Errorf("token = (%d, %d)", out.TokensIn, out.TokensOut)
	}

	// 上游必须收到正确的租户身份。
	seen, reqs := h.up.calls()
	if len(seen) != 1 || seen[0] != "tenant-a" {
		t.Fatalf("上游看到的租户 = %v", seen)
	}
	// 系统提示由平台构造，且必须是最前面那条。
	if len(reqs[0].Messages) == 0 || reqs[0].Messages[0].Role != "system" {
		t.Fatalf("消息序列 = %+v", reqs[0].Messages)
	}
	if reqs[0].Messages[0].Content != "你是平台的助手。" {
		t.Errorf("系统提示 = %q", reqs[0].Messages[0].Content)
	}

	// 历史写到 A 名下，B 名下什么都没有。
	turnsA, err := h.svc.Store().Turns("tenant-a", "default")
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turnsA) != 2 {
		t.Fatalf("tenant-a 的历史有 %d 轮，期望 2", len(turnsA))
	}
	turnsB, err := h.svc.Store().Turns("tenant-b", "default")
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turnsB) != 0 {
		t.Fatalf("tenant-b 读到了 %d 轮 A 的内容", len(turnsB))
	}

	// 指标与审计记在 A 头上，且带着 actor。
	snap := h.svc.Metrics().Tenant("tenant-a").Snapshot("tenant-a")
	if snap.RunsStarted != 1 || snap.RunsCompleted != 1 {
		t.Errorf("指标 = %+v", snap)
	}
	if snap.TokensIn != 11 {
		t.Errorf("token 指标 = %d", snap.TokensIn)
	}
	// A 的 run 事件绝不能挂在 B 名下。这里只挑 run 类事件看：
	// 启动时两个租户各有一条 tenant.provision，那是正当的。
	for _, e := range h.svc.Audit().For("tenant-b", 0) {
		switch e.Action {
		case audit.ActionRunStart, audit.ActionRunFinish, audit.ActionToolCall, audit.ActionToolDenied:
			t.Errorf("租户 B 的审计里出现了 A 的 run 事件：%+v", e)
		}
	}
	var sawActor, sawStart bool
	for _, e := range h.svc.Audit().For("tenant-a", 0) {
		if e.Action == audit.ActionRunStart {
			sawStart = true
		}
		if e.Detail["actor"] == "alice" {
			sawActor = true
		}
	}
	if !sawStart {
		t.Error("审计里没有 run.start")
	}
	if !sawActor {
		t.Error("审计里没有 actor：数据面转发的事件也必须能回答「是谁做的」")
	}

	// 第二条消息必须看得见第一条的历史（跨请求、跨分片调度）。
	out2 := h.runOK("tok-a", "再说一次")
	if out2.Run == out.Run {
		t.Error("两次 run 的标识相同：run 标识必须唯一，它是串起审计与指标的键")
	}
	_, reqs2 := h.up.calls()
	if len(reqs2) != 2 {
		t.Fatalf("上游调用数 = %d", len(reqs2))
	}
	if len(reqs2[1].Messages) <= len(reqs2[0].Messages) {
		t.Fatalf("第二次请求的消息数（%d）没有增长（第一次 %d）：历史没有接上",
			len(reqs2[1].Messages), len(reqs2[0].Messages))
	}

	// 两个租户各自独立：B 的第一次请求里不该出现 A 的内容。
	h.runOK("tok-b", "我是 B")
	seen, reqs3 := h.up.calls()
	if len(seen) != 3 || seen[2] != "tenant-b" {
		t.Fatalf("上游看到的租户序列 = %v", seen)
	}
	for _, m := range reqs3[2].Messages {
		if strings.Contains(m.Content, "你好") || strings.Contains(m.Content, "再说一次") {
			t.Fatalf("tenant-b 看到了 tenant-a 的内容：%+v", m)
		}
	}
}

func TestEndToEndToolCall(t *testing.T) {
	h := newHarness(t, nil)

	// 能力快照只能从租户真正所在的那一片取——这正是粘性路由的含义。
	snap := h.capabilities(t, "tenant-a")
	if snap.Tools == nil {
		t.Fatal("租户应当有工具能力")
	}
	specs := snap.Tools.Specs()
	if len(specs) != 1 || specs[0].Name != "echo" {
		t.Fatalf("工具清单 = %+v，期望只有白名单里的 echo", specs)
	}

	res, err := snap.Tools.Call(context.Background(), caps.Invocation{
		Tenant: "tenant-a", Session: "default", Run: ident.NewRun(),
		Name: "echo", Args: map[string]any{"q": "hi"},
	})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if res.Content != "echo:hi" {
		t.Errorf("工具结果 = %q", res.Content)
	}

	// 模型给出的身份字段必须被剥离：Args 是"已经剥过的"那一份，
	// 但实现必须自己再挡一道（模型不可信）。
	res, err = snap.Tools.Call(context.Background(), caps.Invocation{
		Tenant: "tenant-a", Session: "default", Run: ident.NewRun(),
		Name: "echo", Args: map[string]any{"q": "x", "tenant_id": "tenant-b"},
	})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if strings.Contains(res.Content, "tenant-b") {
		t.Errorf("工具看到了夹带的租户名：%q", res.Content)
	}
}

func TestEndToEndStreaming(t *testing.T) {
	h := newHarness(t, nil)
	resp, body := h.post("tok-a", `{"input":"流式"}`, "stream=1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d：%s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Errorf("Content-Type = %q", got)
	}
	for _, want := range []string{"event: accepted", "event: done"} {
		if !strings.Contains(body, want) {
			t.Errorf("流里缺少 %q：\n%s", want, body)
		}
	}
}

func TestEndToEndUnauthenticatedAndRateLimited(t *testing.T) {
	h := newHarness(t, func(c *insula.Config) {
		c.Admit = admit.Config{Default: admit.Limits{MaxConcurrent: 1, RatePerSecond: 1, Burst: 1}}
	})

	if resp, body := h.post("", `{"input":"x"}`, ""); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("无令牌：状态码 = %d（%s）", resp.StatusCode, body)
	}
	if resp, body := h.post("guess", `{"input":"x"}`, ""); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("坏令牌：状态码 = %d（%s）", resp.StatusCode, body)
	}
	// 第一次成功用掉唯一的令牌；第二次撞速率上限。
	if resp, body := h.post("tok-a", `{"input":"一"}`, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("第一次：状态码 = %d（%s）", resp.StatusCode, body)
	}
	resp, body := h.post("tok-a", `{"input":"二"}`, "")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("第二次：状态码 = %d，期望 429（%s）", resp.StatusCode, body)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("429 缺 Retry-After")
	}
	// 另一个租户不受影响：限额是每租户的。
	if resp, body := h.post("tok-b", `{"input":"B"}`, ""); resp.StatusCode != http.StatusOK {
		t.Errorf("tenant-b 被 tenant-a 的限流拖累：%d（%s）", resp.StatusCode, body)
	}
}

// ---------------------------------------------------------------------------
// 运维面
// ---------------------------------------------------------------------------

func TestProvisionEnsureAndDeprovision(t *testing.T) {
	h := newHarness(t, nil)

	// Ensure 是幂等的；Provision 不是。
	tn, err := h.svc.Ensure(spec("tenant-c"))
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if tn.ID != "tenant-c" {
		t.Errorf("Ensure 返回了 %s", tn.ID)
	}
	again, err := h.svc.Ensure(spec("tenant-c"))
	if err != nil {
		t.Fatalf("第二次 Ensure: %v", err)
	}
	if again != tn {
		t.Error("Ensure 两次返回了不同的句柄")
	}
	if err := h.svc.Provision([]tenant.Spec{spec("tenant-c")}); err == nil {
		t.Error("Provision 遇到已存在的租户必须报错（要幂等语义就用 Ensure）")
	}

	if got := len(h.svc.Tenants()); got != 3 {
		t.Fatalf("租户数 = %d，期望 3", got)
	}

	// 让它先真的能跑起来。凭证与令牌都要先备好，否则"跑不通"会有
	// 别的解释，后面的 404 就说明不了任何事。
	if err := h.creds.Put("tenant-c", credScope, "secret-c"); err != nil {
		t.Fatalf("creds.Put: %v", err)
	}
	if err := h.auth.Put("tok-c", edge.Principal{Tenant: "tenant-c", Actor: "carol"}); err != nil {
		t.Fatalf("auth.Put: %v", err)
	}
	if out := h.runOK("tok-c", "先确认它能跑"); out.Output != "回答" {
		t.Fatalf("tenant-c 的第一次运行 = %+v", out)
	}

	// 塞一点数据和进程级残留，用来验证注销确实清干净了。
	if _, err := h.svc.Store().AppendTurn("tenant-c", "default", memory.Turn{
		Role: memory.RoleUser, Content: "残留",
	}); err != nil {
		t.Fatalf("AppendTurn: %v", err)
	}
	h.svc.Metrics().Tenant("tenant-c").RunsStarted.Add(1)

	if err := h.svc.Deprovision([]ident.Tenant{"tenant-c"}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if got := len(h.svc.Tenants()); got != 2 {
		t.Errorf("注销后租户数 = %d，期望 2", got)
	}
	sessions, turns, _ := h.svc.Store().Count("tenant-c")
	if sessions != 0 || turns != 0 {
		t.Errorf("注销后记忆仍留有 %d/%d 条", sessions, turns)
	}
	for _, tt := range h.svc.Metrics().Tenants() {
		if tt == "tenant-c" {
			t.Error("注销后指标注册表仍留着该租户：Render() 的 label 基数会无界增长")
		}
	}
	// 注销之后，请求路径必须报"本实例上没有这个租户"，而不是
	// 拿到一个入口已摘、数据已删的空壳继续服务。
	resp, body := h.post("tok-c", `{"input":"还在吗"}`, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("注销后状态码 = %d，期望 404（%s）", resp.StatusCode, body)
	}
	// 别的租户完全不受影响。
	if out := h.runOK("tok-a", "我还在"); out.Output != "回答" {
		t.Errorf("注销 tenant-c 波及了 tenant-a：%+v", out)
	}
}

// Shutdown 与 Close 是两件事：前者只关网络，后者拆平台。
func TestShutdownKeepsPlatformUsable(t *testing.T) {
	h := newHarness(t, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- h.svc.Serve(ln) }()

	// 确认服务真的起来了。
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", ln.Addr().String(), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("服务未起来：%v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.svc.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve 返回了错误：%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown 之后 Serve 没有返回")
	}

	// 平台本身必须还活着：Shutdown 关的是网络。
	if h.svc.Closed() {
		t.Error("Shutdown 把整个平台标记成停机了")
	}
	if _, err := h.svc.Ensure(spec("tenant-after-shutdown")); err != nil {
		t.Errorf("Shutdown 之后运维面不可用了：%v", err)
	}
	if _, err := h.svc.SelfCheck(); err != nil {
		t.Errorf("Shutdown 之后自检不可用了：%v", err)
	}
}

func TestServeRejectsBadListenerAndDoubleServe(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.svc.Serve(nil); err == nil {
		t.Error("nil listener 必须被拒绝")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = h.svc.Serve(ln) }()
	time.Sleep(20 * time.Millisecond)

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln2.Close()
	if err := h.svc.Serve(ln2); err == nil {
		t.Error("第二次 Serve 必须被拒绝：两个监听器会绕过准入器成为两条独立入口")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = h.svc.Shutdown(ctx)
}

// ---------------------------------------------------------------------------
// 停机
// ---------------------------------------------------------------------------

// 停机之后：新请求拿到 503（"在按计划停机"），而不是 5xx（"平台坏了"）。
//
// 这个区别不是洁癖：5xx 在监控里会触发告警，把每一次可预期的发布
// 都变成一次告警，人就会开始忽略告警。
func TestCloseRefusesNewRequestsWith503(t *testing.T) {
	h := newHarness(t, nil)
	h.runOK("tok-a", "先确认它能跑")

	if err := h.svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !h.svc.Closed() {
		t.Error("Close 之后 Closed() 为 false")
	}

	resp, body := h.post("tok-a", `{"input":"x"}`, "")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("停机后状态码 = %d，期望 503（%s）", resp.StatusCode, body)
	}

	// 准入器必须在拆池之前被关掉：它是"排空窗口边缘挤进来的请求"唯一的
	// 拦断面。少了这一步，那种请求会走进一个正在拆的池。
	if _, err := h.svc.Admit().Acquire("tenant-a"); !errors.Is(err, admit.ErrClosed) {
		t.Errorf("停机后准入器仍可用（err = %v）：拆池期间挤进来的请求会撞上半个池", err)
	}

	// 幂等：再关一次不该 panic，也不该报错。
	if err := h.svc.Close(); err != nil {
		t.Errorf("第二次 Close: %v", err)
	}
	// 运维面在停机后一律拒绝，而不是在半拆的状态上继续动。
	if err := h.svc.Deprovision([]ident.Tenant{"tenant-a"}); !errors.Is(err, shard.ErrClosed) {
		t.Errorf("停机后 Deprovision 的错误 = %v，期望 ErrClosed", err)
	}
	if _, err := h.svc.SelfCheck(); !errors.Is(err, shard.ErrClosed) {
		t.Errorf("停机后 SelfCheck 的错误 = %v，期望 ErrClosed", err)
	}
}

// 停机必须把租户在共享单例里的状态清干净。
func TestCloseForgetsTenantState(t *testing.T) {
	h := newHarness(t, nil)
	h.runOK("tok-a", "你好")
	h.svc.Metrics().Tenant("tenant-a").RunsStarted.Add(1)

	if err := h.svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, tt := range h.svc.Metrics().Tenants() {
		if tt == "tenant-a" {
			t.Error("停机后指标注册表仍留着租户")
		}
	}
	for _, s := range h.svc.Shards() {
		if n := len(s.List()); n != 0 {
			t.Errorf("分片 %d 停机后仍挂着 %d 个租户", s.ID(), n)
		}
	}
}
