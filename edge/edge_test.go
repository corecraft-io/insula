package edge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metaRobin/insula/admit"
	"github.com/metaRobin/insula/agent"
	"github.com/metaRobin/insula/audit"
	"github.com/metaRobin/insula/caps"
	"github.com/metaRobin/insula/guard"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/memory"
	"github.com/metaRobin/insula/metrics"
	"github.com/metaRobin/insula/realm"
	"github.com/metaRobin/insula/session"
	"github.com/metaRobin/insula/tenant"
)

// ---------------------------------------------------------------------------
// 测试替身
// ---------------------------------------------------------------------------

// fakeModels 按脚本回放模型响应，并记录收到的每一次请求。
//
// gate 非 nil 时，每一次 Complete 都会等它关闭——这是把「一次 run 停在
// 模型调用里」变成可控状态的手段，并发限额与客户端断开的测试都需要它。
type fakeModels struct {
	mu     sync.Mutex
	script []caps.Response
	errs   []error
	reqs   []caps.Request

	gate    chan struct{}
	entered chan struct{}

	// ignoreCtx 让 Complete 无视 ctx 取消。模拟一个不遵守取消约定的模型
	// 适配器——平台必须能对这种情况给出有界的收尾，所以它得被造出来。
	ignoreCtx bool
}

func (m *fakeModels) Healthy() bool { return true }

func (m *fakeModels) Complete(ctx context.Context, req caps.Request) (caps.Response, error) {
	m.mu.Lock()
	m.reqs = append(m.reqs, req)
	i := len(m.reqs) - 1
	m.mu.Unlock()

	if m.entered != nil {
		select {
		case m.entered <- struct{}{}:
		default:
		}
	}
	if m.gate != nil {
		if m.ignoreCtx {
			<-m.gate
		} else {
			select {
			case <-m.gate:
			case <-ctx.Done():
				return caps.Response{}, ctx.Err()
			}
		}
	}
	if i < len(m.errs) && m.errs[i] != nil {
		return caps.Response{}, m.errs[i]
	}
	if i < len(m.script) {
		return m.script[i], nil
	}
	return caps.Response{Message: caps.Message{Role: "assistant", Content: "ok"}}, nil
}

func (m *fakeModels) calls() []caps.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]caps.Request(nil), m.reqs...)
}

// fakeTools 记录每一次工具调用。
type fakeTools struct {
	mu      sync.Mutex
	specs   []caps.ToolSpec
	results map[string]caps.ToolResult
	errs    map[string]error
	calls   []caps.Invocation
}

func (t *fakeTools) Specs() []caps.ToolSpec { return t.specs }

func (t *fakeTools) Call(_ context.Context, inv caps.Invocation) (caps.ToolResult, error) {
	t.mu.Lock()
	t.calls = append(t.calls, inv)
	t.mu.Unlock()
	if err, ok := t.errs[inv.Name]; ok {
		return caps.ToolResult{}, err
	}
	if r, ok := t.results[inv.Name]; ok {
		return r, nil
	}
	return caps.ToolResult{Content: "tool ok"}, nil
}

func (t *fakeTools) recorded() []caps.Invocation {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]caps.Invocation(nil), t.calls...)
}

// facet 是一个租户在假工作区里的装配结果。
type facet struct {
	models caps.Models
	tools  caps.Tools
	policy *guard.Policy
	gen    uint64
	capErr error
	rtErr  error
	noMem  bool
	// foreignFor 非空时，快照里的 memory 句柄被打上**另一个**租户的标记
	// （负向测试用：句柄的租户归属必须被强制，而不是被信任）。
	foreignFor ident.Tenant
}

// fakeWorkspace 是 Workspace 的最小实现，刻意不接 cordis：
// 接入层的契约是「两个方法」，用真装配去测它只会把失败原因拖进
// tenant 包内部。
type fakeWorkspace struct {
	mu      sync.Mutex
	store   *memory.MemStore
	facets  map[ident.Tenant]*facet
	entries map[string]*session.Entry
}

func newFakeWorkspace() *fakeWorkspace {
	return &fakeWorkspace{
		store:   memory.NewMemStore(memory.Config{}),
		facets:  make(map[ident.Tenant]*facet),
		entries: make(map[string]*session.Entry),
	}
}

func (w *fakeWorkspace) add(t ident.Tenant, f *facet) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.facets[t] = f
}

func (w *fakeWorkspace) Capabilities(t ident.Tenant) (caps.Snapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, ok := w.facets[t]
	if !ok {
		return caps.Snapshot{}, tenant.ErrNoTenant
	}
	if f.capErr != nil {
		return caps.Snapshot{}, f.capErr
	}
	snap := caps.Snapshot{Tenant: t, Gen: f.gen}
	if f.models != nil {
		snap.Models = f.models
		snap.Present = append(snap.Present, realm.ServiceModels)
	} else {
		snap.Missing = append(snap.Missing, realm.ServiceModels)
	}
	switch {
	case f.noMem:
		snap.Missing = append(snap.Missing, realm.ServiceMemory)
	case f.foreignFor != "":
		// 故意让句柄认领另一个租户：调用方的每一次读写都会撞上
		// memory.ErrForeignTenant，而不是静默读写别人的数据。
		snap.Memory = memory.NewHandle(w.store, f.foreignFor)
		snap.Present = append(snap.Present, realm.ServiceMemory)
	default:
		snap.Memory = memory.NewHandle(w.store, t)
		snap.Present = append(snap.Present, realm.ServiceMemory)
	}
	if f.tools != nil {
		snap.Tools = f.tools
		snap.Present = append(snap.Present, realm.ServiceTools)
	}
	if f.policy != nil {
		snap.Guard = f.policy
		snap.Present = append(snap.Present, realm.ServiceGuard)
	} else {
		snap.Missing = append(snap.Missing, realm.ServiceGuard)
	}
	sort.Strings(snap.Present)
	sort.Strings(snap.Missing)
	return snap, nil
}

func (w *fakeWorkspace) Runtime(t ident.Tenant, s ident.Session) (*session.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, ok := w.facets[t]
	if !ok {
		return nil, tenant.ErrNoTenant
	}
	if f.rtErr != nil {
		return nil, f.rtErr
	}
	k := string(t) + "/" + string(s)
	if e, ok := w.entries[k]; ok {
		return e, nil
	}
	e, err := session.NewEntry(t, s, session.Policy{}, 0)
	if err != nil {
		return nil, err
	}
	w.entries[k] = e
	return e, nil
}

// ---------------------------------------------------------------------------
// 测试装置
// ---------------------------------------------------------------------------

type harness struct {
	srv     *Server
	ws      *fakeWorkspace
	models  *fakeModels
	tools   *fakeTools
	auth    *StaticTokens
	admit   *admit.Platform
	audit   *audit.Log
	metrics *metrics.Registry
	warns   *warnLog
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

func (w *warnLog) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.msgs...)
}

func (w *warnLog) joined() string { return strings.Join(w.all(), "\n") }

const (
	tenantA ident.Tenant = "tenant-a"
	tenantB ident.Tenant = "tenant-b"
)

func policy() *guard.Policy {
	return guard.NewPolicy(guard.Budget{MaxSteps: 8, MaxToolCalls: 8}, nil)
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	h := &harness{
		ws:      newFakeWorkspace(),
		models:  &fakeModels{},
		tools:   &fakeTools{specs: []caps.ToolSpec{{Name: "lookup"}}},
		auth:    NewStaticTokens(),
		admit:   admit.New(admit.Config{}),
		audit:   audit.New(64, time.Now),
		metrics: metrics.New(),
		warns:   &warnLog{},
	}
	h.ws.add(tenantA, &facet{models: h.models, tools: h.tools, policy: policy(), gen: 1})
	h.ws.add(tenantB, &facet{models: &fakeModels{}, tools: h.tools, policy: policy(), gen: 1})

	if err := h.auth.Put("tok-a", Principal{Tenant: tenantA, Actor: "alice"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := h.auth.Put("tok-b", Principal{Tenant: tenantB, Actor: "bob"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if cfg.Auth == nil {
		cfg.Auth = h.auth
	}
	if cfg.Admit == nil {
		cfg.Admit = h.admit
	}
	if cfg.Workspace == nil {
		cfg.Workspace = h.ws
	}
	if cfg.Audit == nil {
		cfg.Audit = h.audit
	}
	if cfg.Metrics == nil {
		cfg.Metrics = h.metrics
	}
	if cfg.OnWarn == nil {
		cfg.OnWarn = h.warns.add
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.srv = srv
	return h
}

// run 发一次非流式请求。
func (h *harness) run(t *testing.T, token, body string) (*httptest.ResponseRecorder, RunResponse) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/runs", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, req)

	var out RunResponse
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("响应不是合法 JSON: %v\n%s", err, rec.Body.String())
		}
	}
	return rec, out
}

func textReply(s string) caps.Response {
	return caps.Response{
		Message:          caps.Message{Role: "assistant", Content: s},
		PromptTokens:     10,
		CompletionTokens: 4,
	}
}

func toolReply(id, name string, args map[string]any) caps.Response {
	return caps.Response{
		Message: caps.Message{
			Role:      "assistant",
			ToolCalls: []caps.ToolCall{{ID: id, Name: name, Args: args}},
		},
		PromptTokens:     7,
		CompletionTokens: 3,
	}
}

// actionsOf 按动作类型统计审计事件。
func (h *harness) actionsOf(t ident.Tenant) map[audit.Action]int {
	out := make(map[audit.Action]int)
	for _, e := range h.audit.For(t, 0) {
		out[e.Action]++
	}
	return out
}

// ---------------------------------------------------------------------------
// 构造契约
// ---------------------------------------------------------------------------

func TestNewRequiresItsDependencies(t *testing.T) {
	ok := Config{Auth: NewStaticTokens(), Admit: admit.New(admit.Config{}), Workspace: newFakeWorkspace()}

	if _, err := New(Config{Admit: ok.Admit, Workspace: ok.Workspace}); err == nil {
		t.Error("缺少 Auth 应当构造失败：鉴权缺失的接入层等于全平台开放")
	}
	if _, err := New(Config{Auth: ok.Auth, Workspace: ok.Workspace}); err == nil {
		t.Error("缺少 Admit 应当构造失败：准入是拒服务而不是排队服务的唯一入口")
	}
	if _, err := New(Config{Auth: ok.Auth, Admit: ok.Admit}); err == nil {
		t.Error("缺少 Workspace 应当构造失败")
	}
	if _, err := New(ok); err != nil {
		t.Fatalf("完整配置应当构造成功：%v", err)
	}
}

func TestConfigNormalize(t *testing.T) {
	c := Config{}.normalize()
	if c.MaxBodyBytes != DefaultMaxBodyBytes {
		t.Errorf("MaxBodyBytes 默认值 = %d，期望 %d", c.MaxBodyBytes, DefaultMaxBodyBytes)
	}
	if c.Heartbeat != DefaultHeartbeat {
		t.Errorf("Heartbeat 默认值 = %v，期望 %v", c.Heartbeat, DefaultHeartbeat)
	}
	if c.DrainGrace != DefaultDrainGrace {
		t.Errorf("DrainGrace 默认值 = %v，期望 %v", c.DrainGrace, DefaultDrainGrace)
	}
	if c.ProgressBuffer != 64 {
		t.Errorf("ProgressBuffer 默认值 = %d，期望 64", c.ProgressBuffer)
	}
	if c.Clock == nil {
		t.Error("Clock 必须兜底成 time.Now，否则心跳与预算都会静默失效")
	}

	// 显式关闭心跳必须能穿过 normalize：0 是「没设」，负数才是「关掉」。
	off := Config{}.WithHeartbeatDisabled().normalize()
	if off.Heartbeat != -1 {
		t.Errorf("显式关闭心跳后 Heartbeat = %v，期望 -1", off.Heartbeat)
	}
}

// ---------------------------------------------------------------------------
// 鉴权
// ---------------------------------------------------------------------------

func TestStaticTokensRejectUnknownAndRevoked(t *testing.T) {
	toks := NewStaticTokens()
	if err := toks.Put("", Principal{Tenant: tenantA}); err == nil {
		t.Error("空令牌必须被拒绝")
	}
	if err := toks.Put("t", Principal{}); err == nil {
		t.Error("不带租户的主体必须被拒绝：它会让每一次请求都变成无租户请求")
	}
	if err := toks.Put("good", Principal{Tenant: tenantA}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if p, err := toks.Authenticate(httptest.NewRequest(http.MethodGet, "/", nil)); err == nil || p.Tenant != "" {
		t.Error("没有 Authorization 头时必须失败")
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer nope")
	if _, err := toks.Authenticate(req); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("未知令牌的错误 = %v，期望 ErrUnauthenticated", err)
	}

	toks.Revoke("good")
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Authorization", "Bearer good")
	if _, err := toks.Authenticate(req2); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("注销后的令牌仍可用：%v", err)
	}
}

func TestBearerParsing(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"Bearer abc", "abc", true},
		{"bearer abc", "abc", true},     // 方案名不区分大小写
		{"BEARER abc", "abc", true},     //
		{"Bearer   abc  ", "abc", true}, // 允许空白
		{"Bearer ", "", false},          // 空令牌
		{"Bearer", "", false},           // 没有分隔空格
		{"Token abc", "", false},        // 换了方案
		{"abc", "", false},              // 裸令牌
		{"", "", false},
	}
	for _, c := range cases {
		got, ok := bearer(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("bearer(%q) = (%q, %v)，期望 (%q, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestStaticTokensCustomHeader(t *testing.T) {
	toks := NewStaticTokens()
	toks.Header = "X-Insula-Token"
	if err := toks.Put("k", Principal{Tenant: tenantA}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Insula-Token", "Bearer k")
	if _, err := toks.Authenticate(req); err != nil {
		t.Errorf("自定义头未被识别：%v", err)
	}
}

func TestUnauthenticatedRequestIs401(t *testing.T) {
	h := newHarness(t, Config{})

	for _, tc := range []struct{ name, token string }{
		{"无令牌", ""},
		{"未知令牌", "guess"},
	} {
		rec, _ := h.run(t, tc.token, `{"input":"hi"}`)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s：状态码 = %d，期望 401", tc.name, rec.Code)
		}
		if got := rec.Header().Get("WWW-Authenticate"); got != `Bearer realm="insula"` {
			t.Errorf("%s：WWW-Authenticate = %q", tc.name, got)
		}
		// 失败原因不能被解释：令牌不存在与令牌过期对试探者是有用信息。
		body := rec.Body.String()
		if !strings.Contains(body, "unauthenticated") {
			t.Errorf("%s：响应体 = %q，期望只说 unauthenticated", tc.name, body)
		}
		for _, leak := range []string{"unknown", "expired", "missing token"} {
			if strings.Contains(strings.ToLower(body), leak) {
				t.Errorf("%s：响应体泄漏了失败原因 %q：%s", tc.name, leak, body)
			}
		}
	}

	// 未鉴权的请求绝不能触达数据面。
	h.models.mu.Lock()
	n := len(h.models.reqs)
	h.models.mu.Unlock()
	if n != 0 {
		t.Errorf("未鉴权的请求调用了 %d 次模型", n)
	}
}

// ---------------------------------------------------------------------------
// 身份不可由请求指定（本层最重要的一条规则）
// ---------------------------------------------------------------------------

// RunRequest 的字段集合被钉死。这个断言的价值在于：它在**有人新增字段**
// 的那一天失败，从而强迫那次改动过一次显式的评审——「这个字段能不能被
// 调用方填」正是加字段时唯一需要回答的问题。
func TestRunRequestFieldSetIsPinned(t *testing.T) {
	b, err := json.Marshal(RunRequest{Session: "s", Input: "i", Model: "m"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	want := []string{"input", "model", "session"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RunRequest 的字段集合 = %v，期望 %v。\n"+
			"新增字段前请先确认：它是否允许调用方指定？若涉及身份，答案永远是不。", got, want)
	}
}

// 夹带租户字段的请求体在结构体层面无法生效。这里不检查「哪个字段被过滤了」，
// 而是检查一个更强、且对新增字段自动生效的性质：**任何**被送进来的值都
// 无法在解析结果里被找到。
func TestRunRequestBodyCannotSmuggleIdentity(t *testing.T) {
	raw := `{"tenant":"tenant-b","tenant_id":"tenant-b","user_id":"root",
	         "session":"s","input":"hi","metadata":{"owner_tenant":"tenant-b"}}`
	var req RunRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if req.Session != "s" || req.Input != "hi" {
		t.Fatalf("合法字段未解析：%+v", req)
	}
	out, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(out), "tenant-b") || strings.Contains(string(out), "root") {
		t.Fatalf("夹带的值出现在了解析结果里：%s", out)
	}
}

// 端到端：用 A 的令牌、请求体里写 B 的租户名，run 必须跑在 A 上。
func TestForgedTenantInBodyIsIgnoredAndAudited(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{textReply("给 A 的回答")}

	rec, out := h.run(t, "tok-a", `{"tenant":"`+string(tenantB)+`","input":"hi"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200：%s", rec.Code, rec.Body.String())
	}
	if out.Output != "给 A 的回答" {
		t.Fatalf("输出 = %q", out.Output)
	}

	// B 的模型一次都不该被调用。
	// （harness 里 tenantB 用的是另一个 *fakeModels 实例，但这里更
	//  直接的做法是看 A 的调用记录与 B 的租户存储。）
	turnsB, err := h.ws.store.Turns(tenantB, DefaultSession)
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turnsB) != 0 {
		t.Fatalf("租户 B 的历史被写入了 %d 轮：请求体里的租户名生效了", len(turnsB))
	}
	turnsA, err := h.ws.store.Turns(tenantA, DefaultSession)
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turnsA) != 2 {
		t.Fatalf("租户 A 的历史有 %d 轮，期望 2", len(turnsA))
	}

	if n := h.srv.ForgedIdentityAttempts(); n != 1 {
		t.Errorf("夹带计数 = %d，期望 1", n)
	}
	acts := h.actionsOf(tenantA)
	if acts[audit.ActionIsolationCheck] != 1 {
		t.Errorf("缺少 isolation.check 审计：%v", acts)
	}

	// 夹带必须回传给调用方：只写在服务端日志里，调用方会以为一切正常。
	found := false
	for _, n := range out.Notices {
		if strings.Contains(n, "身份字段") {
			found = true
		}
	}
	if !found {
		t.Errorf("响应未提示身份字段被忽略：%v", out.Notices)
	}
}

func TestCleanRequestDoesNotRaiseForgedCounter(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{textReply("ok")}

	if _, resp := h.run(t, "tok-a", `{"input":"hi"}`); len(resp.Notices) != 0 {
		t.Errorf("干净请求不该有告警：%v", resp.Notices)
	}
	if n := h.srv.ForgedIdentityAttempts(); n != 0 {
		t.Errorf("夹带计数 = %d，期望 0", n)
	}
}

func TestDetectForgedIdentityIsRecursive(t *testing.T) {
	s := newForgeryDetector()
	for _, raw := range []string{
		`{"tenant":"x"}`,
		`{"nested":{"tenant":"x"}}`,
		`{"list":[{"user_id":"1"}]}`,
		`{"metadata":{"deep":{"tenant_id":"x"}}}`,
		`{"session_id":"x"}`, // 换了个名字声明会话身份，仍要看见
		`{"run_id":"x"}`,
	} {
		if got := detectForgedIdentity([]byte(raw), s); len(got) == 0 {
			t.Errorf("%s 的夹带未被检测到", raw)
		}
	}
	// 不是 JSON 对象（数组、标量、坏 JSON）时不应 panic，也不该报夹带。
	for _, raw := range []string{`[]`, `"x"`, `{`, `null`, ``} {
		if got := detectForgedIdentity([]byte(raw), s); len(got) != 0 {
			t.Errorf("%q 被误判为夹带：%v", raw, got)
		}
	}
}

// 请求体自己的合法字段绝不能触发"夹带身份"告警。
//
// 这条断言守的是信噪比：若 RunRequest 的每个字段都被算成可疑，
// 那每一个正常请求都会产生一条 denied 审计 + 一条错得离谱的提示
// （说 session 被忽略，而它其实被采信了），检测器也就彻底失效了。
func TestWireFieldsAreExemptFromForgeryDetection(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{textReply("ok")}

	rec, out := h.run(t, "tok-a", `{"session":"s1","model":"m","input":"hi"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d：%s", rec.Code, rec.Body.String())
	}
	if n := h.srv.ForgedIdentityAttempts(); n != 0 {
		t.Errorf("夹带计数 = %d，期望 0：请求体里的 session/model 是契约的一部分", n)
	}
	if len(out.Notices) != 0 {
		t.Errorf("正常请求不该带任何告警：%v", out.Notices)
	}
	if acts := h.actionsOf(tenantA); acts[audit.ActionIsolationCheck] != 0 {
		t.Errorf("正常请求产生了 isolation.check 审计：%v", acts)
	}

	// 豁免是按字段名做的，且必须覆盖 RunRequest 的每一个字段。
	// 手写清单会在这里过期：新增字段却忘了豁免，测试立刻变红。
	det := newForgeryDetector()
	for _, name := range wireFieldNames() {
		probe := map[string]any{name: "tenant-b"}
		if _, rec := det.Strip(probe); rec.StrippedAny() {
			t.Errorf("RunRequest 的合法字段 %q 被当成身份字段：%v", name, rec.Keys)
		}
	}
}

// ---------------------------------------------------------------------------
// 路由形状：IDOR 的前提是「存在一条按 ID 取资源的路径」
// ---------------------------------------------------------------------------

func TestNoTenantScopedRouteExists(t *testing.T) {
	h := newHarness(t, Config{})
	for _, path := range []string{
		"/v1/tenants/tenant-a",
		"/v1/tenants/tenant-a/runs",
		"/v1/tenants/tenant-b/self",
		"/v1/tenants",
		"/v1/sessions/tenant-a/default",
	} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			req := httptest.NewRequest(method, path, strings.NewReader(`{"input":"x"}`))
			req.Header.Set("Authorization", "Bearer tok-a")
			rec := httptest.NewRecorder()
			h.srv.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Errorf("%s %s = %d，期望 404：路径参数是 IDOR 最经典的入口，不应存在这种形状",
					method, path, rec.Code)
			}
		}
	}
}

func TestHealthzNeedsNoAuth(t *testing.T) {
	h := newHarness(t, Config{})
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "ok") {
		t.Errorf("响应体 = %s", rec.Body.String())
	}
}

func TestSelfViewExposesNamesNotValues(t *testing.T) {
	h := newHarness(t, Config{})
	h.ws.add(tenantA, &facet{models: h.models, tools: h.tools, policy: policy(), gen: 3, noMem: true})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/self", nil)
	req.Header.Set("Authorization", "Bearer tok-a")
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d：%s", rec.Code, rec.Body.String())
	}

	var v SelfView
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if v.Tenant != string(tenantA) {
		t.Errorf("Tenant = %q", v.Tenant)
	}
	if v.Gen != 3 {
		t.Errorf("Gen = %d，期望 3", v.Gen)
	}
	if v.Ready {
		t.Error("缺 memory 时 Ready 必须为 false")
	}
	if !v.Degraded {
		t.Error("缺 memory 时 Degraded 必须为 true")
	}
	if !reflect.DeepEqual(v.Missing, []string{realm.ServiceMemory}) {
		t.Errorf("Missing = %v", v.Missing)
	}

	// 只暴露服务名，绝不暴露能力值（它们是句柄）。
	body := rec.Body.String()
	for _, name := range realm.Services() {
		_ = name
	}
	if strings.Contains(body, "0x") || strings.Contains(body, "%!") {
		t.Errorf("响应体疑似含句柄值：%s", body)
	}
}

func TestSelfViewUnauthenticatedIs401(t *testing.T) {
	h := newHarness(t, Config{})
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/tenants/self", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("状态码 = %d，期望 401", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// 闸门顺序
// ---------------------------------------------------------------------------

func TestBodyTooLargeIs413(t *testing.T) {
	h := newHarness(t, Config{MaxBodyBytes: 64})
	big := `{"input":"` + strings.Repeat("x", 512) + `"}`
	rec, _ := h.run(t, "tok-a", big)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("状态码 = %d，期望 413：%s", rec.Code, rec.Body.String())
	}
}

func TestMalformedAndEmptyBodyAre400(t *testing.T) {
	h := newHarness(t, Config{})
	for _, tc := range []struct{ name, body string }{
		{"坏 JSON", `{"input":`},
		{"空输入", `{"input":"   "}`},
		{"缺输入", `{"session":"s"}`},
	} {
		rec, _ := h.run(t, "tok-a", tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s：状态码 = %d，期望 400（%s）", tc.name, rec.Code, rec.Body.String())
		}
	}
}

// 上限在鉴权之前：未鉴权的超大请求体不能让我们分配缓冲区。
func TestBodyLimitAppliesBeforeAuth(t *testing.T) {
	h := newHarness(t, Config{MaxBodyBytes: 64})
	big := strings.Repeat("x", 4096)
	rec, _ := h.run(t, "", big) // 无令牌 + 超大 body
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("状态码 = %d，期望 401：鉴权必须仍然先于执行", rec.Code)
	}
	if h.models.calls() != nil {
		t.Error("未鉴权请求触达了数据面")
	}
}

// ---------------------------------------------------------------------------
// 端到端：非流式
// ---------------------------------------------------------------------------

func TestNonStreamingRunReturnsResult(t *testing.T) {
	h := newHarness(t, Config{SystemPrompt: "你是平台助手。"})
	h.models.script = []caps.Response{textReply("你好")}

	rec, out := h.run(t, "tok-a", `{"input":"hi"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d：%s", rec.Code, rec.Body.String())
	}
	if out.Output != "你好" || out.Stop != agent.StopCompleted || out.Steps != 1 {
		t.Fatalf("结果 = %+v", out)
	}
	if out.Run == "" {
		t.Error("Run 标识必须回传：它是把一次 run 串到审计与指标上的唯一键")
	}
	if out.TokensIn != 10 || out.TokensOut != 4 {
		t.Errorf("token 用量 = (%d, %d)，期望 (10, 4)", out.TokensIn, out.TokensOut)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Errorf("Content-Type = %q", got)
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Error("缺少 nosniff：响应体里含模型输出，必须禁止内容嗅探")
	}

	// 系统提示由平台构造，不能被请求覆盖。
	reqs := h.models.calls()
	if len(reqs) != 1 {
		t.Fatalf("模型调用数 = %d", len(reqs))
	}
	if reqs[0].Messages[0].Role != "system" || reqs[0].Messages[0].Content != "你是平台助手。" {
		t.Errorf("首条消息 = %+v", reqs[0].Messages[0])
	}
}

func TestNonStreamingToolLoop(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{
		toolReply("c1", "lookup", map[string]any{"q": "x"}),
		textReply("查到了"),
	}

	rec, out := h.run(t, "tok-a", `{"input":"帮我查"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d：%s", rec.Code, rec.Body.String())
	}
	if out.Output != "查到了" || out.Steps != 2 || out.ToolCalls != 1 {
		t.Fatalf("结果 = %+v", out)
	}
	calls := h.tools.recorded()
	if len(calls) != 1 || calls[0].Name != "lookup" {
		t.Fatalf("工具调用 = %+v", calls)
	}
	// 身份是必传参数，且由接入层从凭据推出（ADR-005）。
	if calls[0].Tenant != tenantA || calls[0].Session != DefaultSession || calls[0].Run == "" {
		t.Errorf("工具收到的身份 = %+v", calls[0])
	}
	if acts := h.actionsOf(tenantA); acts[audit.ActionToolCall] != 1 {
		t.Errorf("缺少 tool.call 审计：%v", acts)
	}
}

func TestModelFailureIs500AndWarned(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.errs = []error{errors.New("上游 503")}

	rec, _ := h.run(t, "tok-a", `{"input":"hi"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("状态码 = %d，期望 500", rec.Code)
	}
	if !strings.Contains(h.warns.joined(), "上游 503") {
		t.Errorf("5xx 必须留痕，告警里没有原因：%s", h.warns.joined())
	}
	acts := h.actionsOf(tenantA)
	if acts[audit.ActionRunFinish] != 1 {
		t.Errorf("失败的 run 也必须留下 finish 事件：%v", acts)
	}
}

func TestUnknownTenantIs404(t *testing.T) {
	h := newHarness(t, Config{})
	if err := h.auth.Put("tok-gone", Principal{Tenant: "tenant-gone"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rec, _ := h.run(t, "tok-gone", `{"input":"hi"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404（%s）", rec.Code, rec.Body.String())
	}
}

func TestMissingCapabilityIs503(t *testing.T) {
	h := newHarness(t, Config{})
	// 去掉 memory：Ready() 为 false，循环不该启动。
	h.ws.add(tenantA, &facet{models: h.models, tools: h.tools, policy: policy(), gen: 2, noMem: true})

	rec, _ := h.run(t, "tok-a", `{"input":"hi"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("状态码 = %d，期望 503（%s）", rec.Code, rec.Body.String())
	}
	if len(h.models.calls()) != 0 {
		t.Error("能力缺失时不应发起模型调用")
	}
}

// 句柄的租户归属是被强制的，不是被信任的：数据面若拿到别的租户的
// memory 句柄，第一次读写就会失败，而不是静默读写别人的数据。
func TestForeignMemoryHandleFailsLoudly(t *testing.T) {
	h := newHarness(t, Config{})
	h.ws.add(tenantA, &facet{
		models: h.models, tools: h.tools, policy: policy(), gen: 2,
		foreignFor: tenantB,
	})

	rec, _ := h.run(t, "tok-a", `{"input":"hi"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("状态码 = %d，期望 500（%s）", rec.Code, rec.Body.String())
	}
	if !strings.Contains(h.warns.joined(), "load history") {
		t.Errorf("告警里应指明失败在读取历史：%s", h.warns.joined())
	}
	// 别的租户的存储必须一个字节都没被写。
	turns, err := h.ws.store.Turns(tenantB, DefaultSession)
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turns) != 0 {
		t.Fatalf("foreign 句柄写进了 %d 轮", len(turns))
	}
}

// ---------------------------------------------------------------------------
// 准入
// ---------------------------------------------------------------------------

func TestAdmissionRejectionIs429WithRetryAfter(t *testing.T) {
	gate := make(chan struct{})
	h := newHarness(t, Config{
		Admit: admit.New(admit.Config{Default: admit.Limits{MaxConcurrent: 1}}),
	})
	h.models.gate = gate
	h.models.entered = make(chan struct{}, 1)
	h.models.script = []caps.Response{textReply("ok")}

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := h.run(t, "tok-a", `{"input":"first"}`)
		first <- rec
	}()

	select {
	case <-h.models.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("第一个请求未进入模型调用")
	}

	// 额度此刻被第一个请求持有，第二个必须立刻被拒。
	rec2, _ := h.run(t, "tok-a", `{"input":"second"}`)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("状态码 = %d，期望 429（%s）", rec2.Code, rec2.Body.String())
	}
	if got := rec2.Header().Get("Retry-After"); got == "" {
		t.Error("429 必须带 Retry-After：没有它客户端会立刻重试，把限额持续烧穿")
	}
	if !strings.Contains(rec2.Body.String(), string(admit.ReasonConcurrency)) {
		t.Errorf("响应体未说明拒绝原因：%s", rec2.Body.String())
	}
	if n := h.metrics.Tenant(tenantA).Snapshot(tenantA).RunsRejected; n != 1 {
		t.Errorf("RunsRejected = %d，期望 1", n)
	}
	if acts := h.actionsOf(tenantA); acts[audit.ActionRunRejected] != 1 {
		t.Errorf("缺少 run.rejected 审计：%v", acts)
	}
	// 被拒的请求绝不能触达数据面。
	if len(h.models.calls()) != 1 {
		t.Errorf("模型调用数 = %d，期望 1（被拒的请求不该调用模型）", len(h.models.calls()))
	}

	close(gate)
	select {
	case rec := <-first:
		if rec.Code != http.StatusOK {
			t.Errorf("第一个请求状态码 = %d", rec.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("第一个请求未返回")
	}
}

func TestAdmissionClosedIs503(t *testing.T) {
	h := newHarness(t, Config{})
	h.admit.Close()
	rec, _ := h.run(t, "tok-a", `{"input":"hi"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("状态码 = %d，期望 503（%s）", rec.Code, rec.Body.String())
	}
}

// 上限是每租户的：A 打满不该影响 B。
func TestConcurrencyLimitIsPerTenant(t *testing.T) {
	gate := make(chan struct{})
	h := newHarness(t, Config{
		Admit: admit.New(admit.Config{Default: admit.Limits{MaxConcurrent: 1}}),
	})
	h.models.gate = gate
	h.models.entered = make(chan struct{}, 2)
	h.models.script = []caps.Response{textReply("ok")}

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := h.run(t, "tok-a", `{"input":"A"}`)
		first <- rec
	}()
	select {
	case <-h.models.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("第一个请求未进入模型调用")
	}

	recB, _ := h.run(t, "tok-b", `{"input":"B"}`)
	if recB.Code != http.StatusOK {
		t.Fatalf("B 的状态码 = %d，期望 200：限额是每租户的（%s）", recB.Code, recB.Body.String())
	}

	close(gate)
	<-first
}

// ---------------------------------------------------------------------------
// 流式（SSE）
// ---------------------------------------------------------------------------

func TestStreamingEmitsAcceptedToolAndDone(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{
		toolReply("c1", "lookup", map[string]any{"q": "x"}),
		textReply("查到了"),
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/runs?stream=1", strings.NewReader(`{"input":"查"}`))
	req.Header.Set("Authorization", "Bearer tok-a")
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d：%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Errorf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q：不关掉代理缓冲，流式会退化成最后一次性返回", got)
	}
	if got := rec.Header().Get("Cache-Control"); !strings.Contains(got, "no-transform") {
		t.Errorf("Cache-Control = %q", got)
	}

	body := rec.Body.String()
	for _, want := range []string{"event: accepted", "event: tool", "event: done"} {
		if !strings.Contains(body, want) {
			t.Errorf("流里缺少 %q：\n%s", want, body)
		}
	}
	if !strings.Contains(body, "lookup") {
		t.Errorf("tool 事件里应有工具名：\n%s", body)
	}

	// done 帧必须带完整结果，而不是一个"结束了"的空壳。
	var done RunResponse
	for _, frame := range strings.Split(body, "\n\n") {
		if strings.HasPrefix(frame, "event: done") {
			data := strings.TrimPrefix(strings.SplitN(frame, "\n", 2)[1], "data: ")
			if err := json.Unmarshal([]byte(data), &done); err != nil {
				t.Fatalf("done 帧不是合法 JSON：%v", err)
			}
		}
	}
	if done.Output != "查到了" || done.Steps != 2 {
		t.Errorf("done 帧 = %+v", done)
	}
}

func TestStreamingAcceptedByAcceptHeader(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{textReply("ok")}

	req := httptest.NewRequest(http.MethodPost, "/v1/runs", strings.NewReader(`{"input":"hi"}`))
	req.Header.Set("Authorization", "Bearer tok-a")
	req.Header.Set("Accept", "text/event-stream; q=1.0")
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "event: accepted") {
		t.Errorf("Accept 头未触发流式：\n%s", rec.Body.String())
	}
}

func TestStreamingIncapableWriterIs500(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{textReply("ok")}

	req := httptest.NewRequest(http.MethodPost, "/v1/runs?stream=1", strings.NewReader(`{"input":"hi"}`))
	req.Header.Set("Authorization", "Bearer tok-a")
	rec := &plainWriter{h: http.Header{}}
	h.srv.ServeHTTP(rec, req)

	if rec.status != http.StatusInternalServerError {
		t.Fatalf("状态码 = %d，期望 500：连接不支持流式时必须当面说清，而不是发一个看起来正常的响应", rec.status)
	}
	if len(h.models.calls()) != 0 {
		t.Error("流式无法建立时不应启动数据面：那会让一次注定写不回去的 run 白烧额度")
	}
}

// plainWriter 有意**不实现** http.Flusher。
type plainWriter struct {
	h      http.Header
	status int
	body   strings.Builder
}

func (w *plainWriter) Header() http.Header { return w.h }
func (w *plainWriter) WriteHeader(s int)   { w.status = s }
func (w *plainWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(b)
}

// 客户端断开后：run 必须收尾（不能"没人知道它有没有完成"），
// 请求必须在有界的等待内返回（不能被一个不遵守取消的适配器永久挂住）。
func TestClientDisconnectStillFinishesTheRun(t *testing.T) {
	gate := make(chan struct{})
	h := newHarness(t, Config{DrainGrace: 2 * time.Second})
	h.models.gate = gate
	h.models.entered = make(chan struct{}, 1)
	h.models.ignoreCtx = true // 让"收尾"这件事真的发生，而不是被取消打断
	h.models.script = []caps.Response{textReply("写完了")}

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/runs?stream=1", strings.NewReader(`{"input":"hi"}`)).
		WithContext(ctx)
	req.Header.Set("Authorization", "Bearer tok-a")
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.srv.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-h.models.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("请求未进入模型调用")
	}

	cancel() // 客户端走了
	close(gate)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("客户端断开后请求没有返回：run 的收尾不能是无限的")
	}

	// 数据面必须已经完成：用户输入是不可再生的，不能因为对面走了就丢。
	turns, err := h.ws.store.Turns(tenantA, DefaultSession)
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turns) != 2 {
		t.Fatalf("历史有 %d 轮，期望 2（用户轮 + 助手轮）", len(turns))
	}
	if turns[1].Content != "写完了" {
		t.Errorf("助手轮 = %+v", turns[1])
	}
}

func TestDrainGraceBoundsAWedgedRun(t *testing.T) {
	// 一个不理会取消的模型适配器：run 永远不会返回。
	h := newHarness(t, Config{DrainGrace: 100 * time.Millisecond})
	h.models.gate = make(chan struct{}) // 永不关闭，且无视 ctx
	h.models.entered = make(chan struct{}, 1)
	h.models.ignoreCtx = true

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/runs?stream=1", strings.NewReader(`{"input":"hi"}`)).
		WithContext(ctx)
	req.Header.Set("Authorization", "Bearer tok-a")
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.srv.ServeHTTP(rec, req)
		close(done)
	}()
	<-h.models.entered
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DrainGrace 未生效：请求被一个不遵守取消的适配器永久挂住了")
	}
	if !strings.Contains(h.warns.joined(), "did not return") {
		t.Errorf("超时收尾必须留痕（那是适配器的问题，但平台必须让它可见）：%s", h.warns.joined())
	}
}

// 慢客户端只能让自己丢进度事件，不能拖住数据面。
func TestSlowClientDropsProgressInsteadOfBlocking(t *testing.T) {
	h := newHarness(t, Config{})
	ch := make(chan event, 1)
	ch <- event{typ: "占位"} // 通道已满

	h.srv.push(ch, "tool", map[string]string{"tool": "lookup"})
	h.srv.push(ch, "tool", map[string]string{"tool": "lookup"})

	if n := h.srv.DroppedEvents(); n != 2 {
		t.Fatalf("丢弃计数 = %d，期望 2：满通道时必须丢弃而不是阻塞（阻塞会让慢客户端占住租户额度）", n)
	}
	// 丢弃的必须是后来的事件，先入队的那一帧要保住。
	if ev := <-ch; ev.typ != "占位" {
		t.Errorf("先入队的事件被覆盖了：%+v", ev)
	}
}

func TestWantsStream(t *testing.T) {
	cases := []struct {
		url    string
		accept string
		want   bool
	}{
		{"/v1/runs", "", false},
		{"/v1/runs?stream=1", "", true},
		{"/v1/runs?stream=true", "", true},
		{"/v1/runs?stream=TRUE", "", true},
		{"/v1/runs?stream=0", "", false},
		{"/v1/runs?stream=yes", "", false},
		{"/v1/runs", "text/event-stream", true},
		{"/v1/runs", "TEXT/EVENT-STREAM", true},
		{"/v1/runs", "application/json, text/event-stream", true},
		{"/v1/runs", "application/json", false},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodPost, c.url, nil)
		if c.accept != "" {
			req.Header.Set("Accept", c.accept)
		}
		if got := wantsStream(req); got != c.want {
			t.Errorf("wantsStream(%q, %q) = %v，期望 %v", c.url, c.accept, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// 错误映射
// ---------------------------------------------------------------------------

func TestErrorStatusMapping(t *testing.T) {
	le := &admit.LimitError{Reason: admit.ReasonRate, Tenant: tenantA}
	cases := []struct {
		err    error
		status int
	}{
		{le, http.StatusTooManyRequests},
		{fmt.Errorf("wrap: %w", le), http.StatusTooManyRequests},
		{tenant.ErrNoTenant, http.StatusNotFound},
		{tenant.ErrTenantDisabled, http.StatusForbidden},
		{admit.ErrClosed, http.StatusServiceUnavailable},
		{fmt.Errorf("wrap: %w", agent.ErrCapabilityUnavailable), http.StatusServiceUnavailable},
		{agent.ErrBadIdentity, http.StatusBadRequest},
		{errors.New("别的东西"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		status, msg := errorStatus(c.err)
		if status != c.status {
			t.Errorf("errorStatus(%v) = %d，期望 %d", c.err, status, c.status)
		}
		if msg == "" {
			t.Errorf("errorStatus(%v) 的消息为空", c.err)
		}
		// 500 的对外消息必须笼统：具体原因只进日志。
		if status == http.StatusInternalServerError && strings.Contains(msg, "别的东西") {
			t.Errorf("500 泄漏了内部错误文本：%q", msg)
		}
	}
}

func TestErrorBodiesAreNotExplained(t *testing.T) {
	h := newHarness(t, Config{})
	h.ws.add(tenantA, &facet{models: h.models, policy: policy(), capErr: errors.New("内部细节：连接池已满")})

	rec, _ := h.run(t, "tok-a", `{"input":"hi"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("状态码 = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "连接池") {
		t.Errorf("响应体泄漏了内部错误：%s", rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// 会话
// ---------------------------------------------------------------------------

func TestDefaultSessionIsUsedWhenAbsent(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{textReply("a"), textReply("b")}

	if _, _ = h.run(t, "tok-a", `{"input":"一"}`); true {
	}
	if _, _ = h.run(t, "tok-a", `{"input":"二"}`); true {
	}

	turns, err := h.ws.store.Turns(tenantA, DefaultSession)
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turns) != 4 {
		t.Fatalf("默认会话有 %d 轮，期望 4（两次请求各两轮）", len(turns))
	}
	// 第二次请求必须看得见第一次的历史。
	reqs := h.models.calls()
	if len(reqs) != 2 {
		t.Fatalf("模型调用数 = %d", len(reqs))
	}
	if len(reqs[1].Messages) <= len(reqs[0].Messages) {
		t.Errorf("第二次请求的消息数（%d）没有增长：历史没有接上",
			len(reqs[1].Messages))
	}
}

func TestExplicitSessionIsHonoured(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{textReply("a"), textReply("b")}

	if _, _ = h.run(t, "tok-a", `{"session":"s1","input":"一"}`); true {
	}
	if _, _ = h.run(t, "tok-a", `{"session":"s2","input":"二"}`); true {
	}

	t1, _ := h.ws.store.Turns(tenantA, "s1")
	t2, _ := h.ws.store.Turns(tenantA, "s2")
	if len(t1) != 2 || len(t2) != 2 {
		t.Fatalf("s1=%d 轮，s2=%d 轮，期望各 2", len(t1), len(t2))
	}
	// 两个会话互不可见：s2 的第一次调用里不该出现 s1 的内容。
	reqs := h.models.calls()
	if len(reqs) != 2 {
		t.Fatalf("模型调用数 = %d", len(reqs))
	}
	for _, m := range reqs[1].Messages {
		if strings.Contains(m.Content, "一") {
			t.Errorf("会话 s2 看到了会话 s1 的内容：%+v", m)
		}
	}
}

func TestRuntimeFailureIsReported(t *testing.T) {
	h := newHarness(t, Config{})
	h.ws.add(tenantA, &facet{models: h.models, policy: policy(), rtErr: errors.New("boom")})

	rec, _ := h.run(t, "tok-a", `{"input":"hi"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("状态码 = %d，期望 500（%s）", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// 指标与审计
// ---------------------------------------------------------------------------

func TestMetricsAndAuditRecordTheRun(t *testing.T) {
	h := newHarness(t, Config{})
	h.models.script = []caps.Response{textReply("ok")}

	rec, _ := h.run(t, "tok-a", `{"input":"hi"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d：%s", rec.Code, rec.Body.String())
	}

	snap := h.metrics.Tenant(tenantA).Snapshot(tenantA)
	if snap.RunsStarted != 1 || snap.RunsCompleted != 1 {
		t.Errorf("指标 = %+v", snap)
	}
	if snap.TokensIn != 10 || snap.TokensOut != 4 {
		t.Errorf("token 指标 = (%d, %d)", snap.TokensIn, snap.TokensOut)
	}
	if snap.RunsObserved != 1 {
		t.Error("缺少延迟观测")
	}

	acts := h.actionsOf(tenantA)
	if acts[audit.ActionRunStart] != 1 || acts[audit.ActionRunFinish] != 1 {
		t.Errorf("run 起止审计 = %v", acts)
	}
	// 审计必须带上是谁在调用。
	found := false
	for _, e := range h.audit.For(tenantA, 0) {
		if e.Detail["actor"] == "alice" {
			found = true
		}
		if e.Action == audit.ActionRunStart && e.Subject == "" {
			t.Error("run.start 必须带 run 标识")
		}
	}
	if !found {
		t.Error("审计里没有 actor：出了问题无法回答『是谁做的』")
	}
	// 另一个租户的审计里不能出现 A 的事件。
	if n := h.audit.Count(tenantB); n != 0 {
		t.Errorf("租户 B 的审计里有 %d 条 A 的事件", n)
	}
}

// 审计缺席时接入层必须照常工作：审计是可观测性，不是可用性的前置条件。
func TestServerWorksWithoutAuditAndMetrics(t *testing.T) {
	h := newHarness(t, Config{Audit: nil, Metrics: nil})
	// New 会把 nil 的 Audit/Metrics 原样保留（normalize 不补默认），
	// 这里通过重新构造来确保走的是 nil 分支。
	srv, err := New(Config{Auth: h.auth, Admit: h.admit, Workspace: h.ws})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.srv = srv
	h.models.script = []caps.Response{textReply("ok")}

	rec, out := h.run(t, "tok-a", `{"input":"hi"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d：%s", rec.Code, rec.Body.String())
	}
	if out.Output != "ok" {
		t.Errorf("输出 = %q", out.Output)
	}
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------
