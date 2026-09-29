package agent_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/corecraft-io/insula/agent"
	"github.com/corecraft-io/insula/audit"
	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/guard"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/metrics"
	"github.com/corecraft-io/insula/realm"
	"github.com/corecraft-io/insula/session"
)

// ---------------------------------------------------------------------------
// 假件
// ---------------------------------------------------------------------------

// scriptedModels 按调用序号返回脚本回复，并记录每次请求。
//
// 记录请求是必要的：这个包里不少行为只在「模型看到了什么」这一层可见
// ——截断后的消息序列、摘要是否以单条 user 消息送出、工具清单有没有带上。
type scriptedModels struct {
	mu     sync.Mutex
	script []caps.Response
	fails  map[int]error
	hooks  map[int]func()
	reqs   []caps.Request
}

func (m *scriptedModels) Complete(_ context.Context, req caps.Request) (caps.Response, error) {
	m.mu.Lock()
	idx := len(m.reqs)
	m.reqs = append(m.reqs, req)
	err := m.fails[idx]
	hook := m.hooks[idx]
	var resp caps.Response
	if err == nil {
		if idx < len(m.script) {
			resp = m.script[idx]
		} else {
			err = errors.New("scriptedModels: out of script")
		}
	}
	m.mu.Unlock()

	if hook != nil {
		// 在锁外执行：hook 可能要取消 context，或去读别的受锁状态。
		hook()
	}
	return resp, err
}

func (m *scriptedModels) Healthy() bool { return true }

func (m *scriptedModels) Requests() []caps.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]caps.Request(nil), m.reqs...)
}

// fakeMemory 是按键隔离的假存储。键始终包含租户，免得测试自己
// 复现出"忘了租户"这个正是要被防住的错误。
type fakeMemory struct {
	mu    sync.Mutex
	turns map[string][]memory.Turn
	docs  map[string][]memory.Doc
}

func newFakeMemory() *fakeMemory {
	return &fakeMemory{turns: map[string][]memory.Turn{}, docs: map[string][]memory.Doc{}}
}

func memKey(t ident.Tenant, s ident.Session) string { return string(t) + "/" + string(s) }

func (m *fakeMemory) AppendTurn(t ident.Tenant, s ident.Session, turn memory.Turn) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey(t, s)
	m.turns[k] = append(m.turns[k], turn)
	return 0, nil
}

func (m *fakeMemory) Turns(t ident.Tenant, s ident.Session) ([]memory.Turn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]memory.Turn(nil), m.turns[memKey(t, s)]...), nil
}

func (m *fakeMemory) ReplaceTurns(t ident.Tenant, s ident.Session, turns []memory.Turn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.turns[memKey(t, s)] = append([]memory.Turn(nil), turns...)
	return nil
}

func (m *fakeMemory) PutDoc(t ident.Tenant, d memory.Doc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.docs[string(t)] = append(m.docs[string(t)], d)
	return nil
}

func (m *fakeMemory) Search(ident.Tenant, memory.Query) ([]memory.Hit, error) { return nil, nil }

func (m *fakeMemory) seed(t ident.Tenant, s ident.Session, turns ...memory.Turn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.turns[memKey(t, s)] = append(m.turns[memKey(t, s)], turns...)
}

func (m *fakeMemory) window(t ident.Tenant, s ident.Session) []memory.Turn {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]memory.Turn(nil), m.turns[memKey(t, s)]...)
}

type fakeTools struct {
	mu      sync.Mutex
	specs   []caps.ToolSpec
	results map[string]caps.ToolResult
	errs    map[string]error
	calls   []caps.Invocation
}

func (f *fakeTools) Specs() []caps.ToolSpec { return f.specs }

func (f *fakeTools) Call(_ context.Context, inv caps.Invocation) (caps.ToolResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, inv)
	if err := f.errs[inv.Name]; err != nil {
		return caps.ToolResult{}, err
	}
	if r, ok := f.results[inv.Name]; ok {
		return r, nil
	}
	return caps.ToolResult{Content: "ok:" + inv.Name}, nil
}

func (f *fakeTools) invocations() []caps.Invocation {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]caps.Invocation(nil), f.calls...)
}

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

func textReply(s string) caps.Response {
	return caps.Response{
		Message:          caps.Message{Role: "assistant", Content: s},
		CompletionTokens: 1,
	}
}

func toolReply(calls ...caps.ToolCall) caps.Response {
	return caps.Response{Message: caps.Message{Role: "assistant", ToolCalls: calls}}
}

func call(id, name string) caps.ToolCall {
	return caps.ToolCall{ID: id, Name: name, Args: map[string]any{"q": name}}
}

func newDeps(models caps.Models, mem caps.Memory, tl caps.Tools, g *guard.Policy) agent.Deps {
	present := append([]string(nil), caps.Required...)
	if tl != nil {
		present = append(present, realm.ServiceTools)
		sort.Strings(present)
	}
	return agent.Deps{
		Caps: caps.Snapshot{
			Tenant: "tenant-a",
			Models: models, Memory: mem, Tools: tl, Guard: g,
			Present: present,
		},
		History: session.NewHistory(session.Policy{}),
	}
}

func newRequest() agent.Request {
	return agent.Request{Tenant: "tenant-a", Session: "session-a", Run: "run-1"}
}

func request(input string) agent.Request {
	r := newRequest()
	r.Input = input
	return r
}

// turns 生成 n 轮内容可辨识的对话。
func turns(n int) []memory.Turn {
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

func lastToolContent(t *testing.T, msgs []caps.Message) string {
	t.Helper()
	content := ""
	found := false
	for _, m := range msgs {
		if m.Role == "tool" {
			content, found = m.Content, true
		}
	}
	if !found {
		t.Fatalf("no tool message in %+v", msgs)
	}
	return content
}

// ---------------------------------------------------------------------------
// 前置校验
// ---------------------------------------------------------------------------

func TestRunRefusesWithoutIdentity(t *testing.T) {
	d := newDeps(&scriptedModels{}, newFakeMemory(), nil, guard.NewPolicy(guard.Default(), nil))
	for _, r := range []agent.Request{
		{Tenant: "", Session: "s", Input: "hi"},
		{Tenant: "t", Session: "", Input: "hi"},
	} {
		if _, err := agent.Run(context.Background(), d, r); !errors.Is(err, agent.ErrBadIdentity) {
			t.Fatalf("Run(%+v) = %v, want ErrBadIdentity", r, err)
		}
	}
}

func TestRunRefusesWhenRequiredCapabilityMissing(t *testing.T) {
	d := newDeps(&scriptedModels{}, newFakeMemory(), nil, guard.NewPolicy(guard.Default(), nil))
	// 摘掉 memory：这是"控制面还没把能力推上来"的形态。
	d.Caps.Present = []string{realm.ServiceModels, realm.ServiceGuard}
	if d.Caps.Ready() {
		t.Fatal("snapshot must not report itself ready with memory missing")
	}

	_, err := agent.Run(context.Background(), d, request("hi"))
	if !errors.Is(err, agent.ErrCapabilityUnavailable) {
		t.Fatalf("Run = %v, want ErrCapabilityUnavailable", err)
	}
}

// 守卫生成不了就要拒绝启动。给一个默认预算看似更友好，但那只是把
// 一次配置疏漏藏起来——没有预算的循环可以无限跑。
func TestRunRefusesWithoutGuardPolicy(t *testing.T) {
	d := newDeps(&scriptedModels{}, newFakeMemory(), nil, nil)
	if _, err := agent.Run(context.Background(), d, request("hi")); !errors.Is(err, agent.ErrCapabilityUnavailable) {
		t.Fatalf("Run = %v, want ErrCapabilityUnavailable", err)
	}
}

// ---------------------------------------------------------------------------
// 基本路径
// ---------------------------------------------------------------------------

func TestSimpleRunPersistsBothTurns(t *testing.T) {
	models := &scriptedModels{script: []caps.Response{textReply("你好，我在。")}}
	mem := newFakeMemory()
	d := newDeps(models, mem, nil, guard.NewPolicy(guard.Default(), nil))

	res, err := agent.Run(context.Background(), d, request("在吗"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Stop != agent.StopCompleted {
		t.Fatalf("Stop = %q, want %q", res.Stop, agent.StopCompleted)
	}
	if res.Output != "你好，我在。" {
		t.Fatalf("Output = %q", res.Output)
	}
	if res.Steps != 1 || res.ToolCalls != 0 {
		t.Fatalf("Steps/ToolCalls = %d/%d, want 1/0", res.Steps, res.ToolCalls)
	}
	if res.TokensOut != 1 || res.Total() != 1 {
		t.Fatalf("Tokens = %d/%d, want 0/1", res.TokensIn, res.TokensOut)
	}

	w := mem.window("tenant-a", "session-a")
	if len(w) != 2 || w[0].Role != memory.RoleUser || w[1].Role != memory.RoleAssistant {
		t.Fatalf("persisted window = %+v, want [user assistant]", w)
	}
	if w[0].Content != "在吗" {
		t.Fatalf("user turn content = %q", w[0].Content)
	}
	if st := d.History.Stats(); st.Usage.Turns != 2 {
		t.Fatalf("History turns = %d, want 2", st.Usage.Turns)
	}
}

// 用户输入是不可再生的输入：循环可以失败，但它必须已经落库。
func TestUserTurnSurvivesModelFailure(t *testing.T) {
	models := &scriptedModels{fails: map[int]error{0: errors.New("upstream down")}}
	mem := newFakeMemory()
	d := newDeps(models, mem, nil, guard.NewPolicy(guard.Default(), nil))

	if _, err := agent.Run(context.Background(), d, request("这条不能丢")); err == nil {
		t.Fatal("a model failure must be reported as an error")
	}

	w := mem.window("tenant-a", "session-a")
	if len(w) != 1 || w[0].Role != memory.RoleUser || w[0].Content != "这条不能丢" {
		t.Fatalf("user turn must be persisted before the loop, got %+v", w)
	}
}

func TestEmptyAssistantReplyIsNotPersisted(t *testing.T) {
	models := &scriptedModels{script: []caps.Response{textReply("")}}
	mem := newFakeMemory()
	d := newDeps(models, mem, nil, guard.NewPolicy(guard.Default(), nil))

	res, err := agent.Run(context.Background(), d, request("你好"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Stop != agent.StopCompleted {
		t.Fatalf("Stop = %q", res.Stop)
	}
	// 空回复不写库：把空字符串当一轮对话存下来会在历史里堆积无信息的
	// 轮次，压缩时还要花 token 处理它们。
	if w := mem.window("tenant-a", "session-a"); len(w) != 1 {
		t.Fatalf("empty assistant reply must not be persisted, window = %+v", w)
	}
}

// ---------------------------------------------------------------------------
// 工具调用
// ---------------------------------------------------------------------------

func TestToolResultsAreFedBackAndLoopContinues(t *testing.T) {
	tools := &fakeTools{specs: []caps.ToolSpec{{Name: "search"}}}
	models := &scriptedModels{script: []caps.Response{
		toolReply(call("c1", "search")),
		textReply("查到了。"),
	}}
	d := newDeps(models, newFakeMemory(), tools, guard.NewPolicy(guard.Default(), nil))

	res, err := agent.Run(context.Background(), d, request("查一下"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Stop != agent.StopCompleted {
		t.Fatalf("Stop = %q", res.Stop)
	}
	if res.Steps != 2 || res.ToolCalls != 1 {
		t.Fatalf("Steps/ToolCalls = %d/%d, want 2/1", res.Steps, res.ToolCalls)
	}

	reqs := models.Requests()
	if len(reqs) != 2 {
		t.Fatalf("model calls = %d, want 2", len(reqs))
	}
	// 工具清单只在确有工具时才带上。
	if len(reqs[0].Tools) != 1 || reqs[0].Tools[0].Name != "search" {
		t.Fatalf("tool specs not forwarded: %+v", reqs[0].Tools)
	}

	var toolMsg *caps.Message
	for i := range reqs[1].Messages {
		if reqs[1].Messages[i].Role == "tool" {
			toolMsg = &reqs[1].Messages[i]
		}
	}
	if toolMsg == nil {
		t.Fatalf("second request carries no tool result: %+v", reqs[1].Messages)
	}
	if toolMsg.ToolCallID != "c1" || toolMsg.Name != "search" {
		t.Fatalf("tool result must echo call id and name: %+v", *toolMsg)
	}
	if !strings.Contains(toolMsg.Content, "ok:search") {
		t.Fatalf("tool output was not fed back: %q", toolMsg.Content)
	}
}

func TestToolsOmittedWhenTenantHasNone(t *testing.T) {
	models := &scriptedModels{script: []caps.Response{textReply("纯问答。")}}
	d := newDeps(models, newFakeMemory(), nil, guard.NewPolicy(guard.Default(), nil))

	if _, err := agent.Run(context.Background(), d, request("你好")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 一个空清单和一个不存在的清单对模型是不同的信号。
	if got := models.Requests()[0].Tools; len(got) != 0 {
		t.Fatalf("tool list = %+v, want omitted", got)
	}
}

// 工具故障必须回灌成工具结果，而不是中断 run：模型看到错误文本可以换
// 参数重试或改用别的工具；直接失败则让一次偶发故障升级成整次会话失败。
func TestToolErrorIsFedBackAndRunContinues(t *testing.T) {
	tools := &fakeTools{errs: map[string]error{"boom": errors.New("后端超时")}}
	models := &scriptedModels{script: []caps.Response{
		toolReply(call("c1", "boom")),
		textReply("工具挂了，先按已有信息回答。"),
	}}
	d := newDeps(models, newFakeMemory(), tools, guard.NewPolicy(guard.Default(), nil))

	res, err := agent.Run(context.Background(), d, request("试试"))
	if err != nil {
		t.Fatalf("a tool failure must not fail the run: %v", err)
	}
	if res.Stop != agent.StopCompleted {
		t.Fatalf("Stop = %q", res.Stop)
	}

	content := lastToolContent(t, models.Requests()[1].Messages)
	if !strings.Contains(content, "工具执行失败") || !strings.Contains(content, "后端超时") {
		t.Fatalf("tool error must be fed back as a tool result, got %q", content)
	}
}

// 被拒绝的调用必须明确告诉模型"重试无用"。只说"被拒绝"会让模型当成
// 瞬时故障反复重试，直到把预算烧完。
func TestDeniedToolIsLabelledNonRetryable(t *testing.T) {
	tools := &fakeTools{results: map[string]caps.ToolResult{
		"rm": {Denied: true, Reason: "该工具不在本租户白名单内"},
	}}
	models := &scriptedModels{script: []caps.Response{
		toolReply(call("c1", "rm")),
		textReply("好。"),
	}}
	d := newDeps(models, newFakeMemory(), tools, guard.NewPolicy(guard.Default(), nil))

	res, err := agent.Run(context.Background(), d, request("删掉"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Denied) != 1 || res.Denied[0] != "rm" {
		t.Fatalf("Denied = %v, want [rm]", res.Denied)
	}

	content := lastToolContent(t, models.Requests()[1].Messages)
	if !strings.Contains(content, "重试不会改变结果") || !strings.Contains(content, "白名单") {
		t.Fatalf("denial must be labelled non-retryable, got %q", content)
	}
}

// 截断必须按字符而不是按字节：按字节切会把一个多字节字符切两半，
// 产出非法 UTF-8 —— 进模型上下文是乱码，进日志是看不出问题的坏数据。
func TestToolResultIsTruncatedOnRuneBoundary(t *testing.T) {
	tools := &fakeTools{results: map[string]caps.ToolResult{
		"big": {Content: strings.Repeat("中", 4000)},
	}}
	models := &scriptedModels{script: []caps.Response{
		toolReply(call("c1", "big")),
		textReply("好。"),
	}}
	d := newDeps(models, newFakeMemory(), tools, guard.NewPolicy(guard.Default(), nil))
	d.MaxToolResultChars = 100

	if _, err := agent.Run(context.Background(), d, request("大输出")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	content := lastToolContent(t, models.Requests()[1].Messages)
	if !strings.HasSuffix(content, "（输出已截断）") {
		t.Fatalf("oversized tool output was not marked truncated")
	}
	if !utf8.ValidString(content) {
		t.Fatal("truncation produced invalid UTF-8 — a multi-byte character was cut in half")
	}
	if n := utf8.RuneCountInString(content); n > 100+len("…（输出已截断）") {
		t.Fatalf("truncated content has %d runes, want about 100", n)
	}
}

// 这条测试盯的是一个协议一致性不变量：助手消息里**声明的工具调用数**
// 必须等于随后回灌的**工具结果条数**。
//
// 主流模型 API 都校验这一点，少一条整个下一次请求会被判非法。所以
// 单步超限时截断的必须是消息本身，而不只是执行列表——否则"这一步太长"
// 的限流会升级成下一轮的硬失败。
func TestPerStepToolCallLimitKeepsProtocolConsistent(t *testing.T) {
	tools := &fakeTools{}
	models := &scriptedModels{script: []caps.Response{
		toolReply(call("c1", "a"), call("c2", "b"), call("c3", "c")),
		textReply("只用前两个结果回答。"),
	}}
	d := newDeps(models, newFakeMemory(), tools, guard.NewPolicy(guard.Default(), nil))
	d.MaxToolCallsPerStep = 2

	res, err := agent.Run(context.Background(), d, request("一次发三个"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.TruncatedToolCalls != 1 {
		t.Fatalf("TruncatedToolCalls = %d, want 1", res.TruncatedToolCalls)
	}
	if res.ToolCalls != 2 {
		t.Fatalf("ToolCalls = %d, want 2", res.ToolCalls)
	}
	if len(tools.invocations()) != 2 {
		t.Fatalf("tool invocations = %d, want 2", len(tools.invocations()))
	}
	// 单步超限不终止 run：模型有能力自我纠正，终止等于剥夺这次机会。
	if res.Stop != agent.StopCompleted {
		t.Fatalf("Stop = %q, want %q", res.Stop, agent.StopCompleted)
	}

	second := models.Requests()[1].Messages
	declared, results := 0, 0
	for _, m := range second {
		switch m.Role {
		case "assistant":
			declared = len(m.ToolCalls)
		case "tool":
			results++
		}
	}
	if declared != 2 {
		t.Fatalf("assistant declared %d tool calls, want 2", declared)
	}
	if declared != results {
		t.Fatalf("assistant declared %d tool calls but %d results followed — "+
			"model APIs reject the next request when these disagree", declared, results)
	}

	// 必须显式告知模型有几条没执行，否则它会以为结果丢了，
	// 在下一步把同一批调用再发一遍。
	told := false
	for _, m := range second {
		if m.Role == "system" && strings.Contains(m.Content, "未被执行") {
			told = true
		}
	}
	if !told {
		t.Fatalf("model was not told about the dropped calls: %+v", second)
	}
}

// 循环不得"顺手先剥一遍"身份字段：剥离是 tools.Registry 的职责（它有
// 自己的键名归一化规则），在这里再做一次就是两处实现，迟早漂移。
// 这条同时钉死：工具的调用身份只能来自 Request，不能来自模型给的参数。
func TestArgsReachTheToolUnmodified(t *testing.T) {
	tools := &fakeTools{}
	models := &scriptedModels{script: []caps.Response{
		toolReply(caps.ToolCall{ID: "c1", Name: "search", Args: map[string]any{
			"tenant_id": "attacker",
			"query":     "hello",
			"nested":    map[string]any{"session_id": "forged"},
		}}),
		textReply("好。"),
	}}
	d := newDeps(models, newFakeMemory(), tools, guard.NewPolicy(guard.Default(), nil))

	if _, err := agent.Run(context.Background(), d, request("查")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	invs := tools.invocations()
	if len(invs) != 1 {
		t.Fatalf("tool invocations = %d, want 1", len(invs))
	}
	if invs[0].Args["tenant_id"] != "attacker" {
		t.Fatalf("the loop must hand raw args to the registry, got %+v", invs[0].Args)
	}
	if invs[0].Tenant != "tenant-a" || invs[0].Session != "session-a" || invs[0].Run != "run-1" {
		t.Fatalf("invocation identity must come from the request, got %+v", invs[0])
	}
}

// ---------------------------------------------------------------------------
// 退出点
// ---------------------------------------------------------------------------

// 预算用尽是正常终止条件，不是错误。把正常的终止条件编码成 error 会让
// 调用方在错误路径上做正常流程的事，还会把已经拿到的部分结果丢掉。
func TestBudgetExhaustionIsNotAnError(t *testing.T) {
	tools := &fakeTools{}
	models := &scriptedModels{script: []caps.Response{
		toolReply(call("c1", "a")),
		textReply("不该走到这里"),
	}}
	d := newDeps(models, newFakeMemory(), tools, guard.NewPolicy(guard.Budget{MaxSteps: 1}, nil))

	res, err := agent.Run(context.Background(), d, request("跑"))
	if err != nil {
		t.Fatalf("budget exhaustion must not be an error: %v", err)
	}
	if res.Stop != agent.StopBudget {
		t.Fatalf("Stop = %q, want %q", res.Stop, agent.StopBudget)
	}
	if res.Steps != 1 || res.ToolCalls != 1 {
		t.Fatalf("Steps/ToolCalls = %d/%d, want 1/1", res.Steps, res.ToolCalls)
	}
	if got := len(models.Requests()); got != 1 {
		t.Fatalf("model calls = %d, want 1 — the loop must stop at the budget", got)
	}
}

func TestCancellationStopsTheLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	models := &scriptedModels{
		script: []caps.Response{toolReply(call("c1", "a")), textReply("不该走到这里")},
		// 第一次模型返回后立刻取消，模拟客户端断开。
		hooks: map[int]func(){0: cancel},
	}
	d := newDeps(models, newFakeMemory(), &fakeTools{}, guard.NewPolicy(guard.Default(), nil))

	res, err := agent.Run(ctx, d, request("跑"))
	if err != nil {
		t.Fatalf("cancellation is a stop reason, not an error: %v", err)
	}
	if res.Stop != agent.StopCanceled {
		t.Fatalf("Stop = %q, want %q", res.Stop, agent.StopCanceled)
	}
	if got := len(models.Requests()); got != 1 {
		t.Fatalf("model calls = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// 压缩
// ---------------------------------------------------------------------------

func TestCompactionReplacesTheWindowWithASummary(t *testing.T) {
	mem := newFakeMemory()
	seed := turns(6)
	mem.seed("tenant-a", "session-a", seed...)

	models := &scriptedModels{script: []caps.Response{
		{
			Message:          caps.Message{Role: "assistant", Content: "前六轮讲了三件事：A、B、C。"},
			PromptTokens:     100,
			CompletionTokens: 20,
		},
		textReply("好。"),
	}}
	d := newDeps(models, mem, nil, guard.NewPolicy(guard.Default(), nil))
	d.History = session.NewHistory(session.Policy{SoftLimit: 4, KeepRecent: 2})

	res, err := agent.Run(context.Background(), d, request("继续"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Compactions != 1 {
		t.Fatalf("Compactions = %d, want 1", res.Compactions)
	}

	reqs := models.Requests()
	if len(reqs) != 2 {
		t.Fatalf("model calls = %d, want 2 (summarize + answer)", len(reqs))
	}
	// 第一次调用必须是压缩：单条 user 消息、不带工具清单、单独的 token 上限。
	if len(reqs[0].Messages) != 1 || reqs[0].Messages[0].Role != "user" ||
		!strings.Contains(reqs[0].Messages[0].Content, "事实性摘要") {
		t.Fatalf("first call must be the summarization prompt, got %+v", reqs[0].Messages)
	}
	if len(reqs[0].Tools) != 0 {
		t.Fatal("the summarization call must not carry the tool list")
	}
	if reqs[0].MaxTokens != 512 {
		t.Fatalf("summarize MaxTokens = %d, want the default 512", reqs[0].MaxTokens)
	}

	// 第二次调用必须看到摘要，而不是原始六轮。
	seenSummary := false
	for _, m := range reqs[1].Messages {
		if m.Role == "system" && strings.Contains(m.Content, "[此前对话摘要]") {
			seenSummary = true
		}
	}
	if !seenSummary {
		t.Fatalf("summary was not carried into the next request: %+v", reqs[1].Messages)
	}

	// 落库结果：摘要 + 保留的最近 2 轮 + 本轮 user + assistant。
	w := mem.window("tenant-a", "session-a")
	if len(w) != 5 {
		t.Fatalf("window = %d turns, want 5: %+v", len(w), w)
	}
	if w[0].Role != memory.RoleSummary {
		t.Fatalf("window[0].Role = %q, want %q", w[0].Role, memory.RoleSummary)
	}
	if w[1].Content != seed[4].Content || w[2].Content != seed[5].Content {
		t.Fatalf("the recent turns must be preserved verbatim, got %q %q", w[1].Content, w[2].Content)
	}
	if w[3].Content != "继续" || w[4].Role != memory.RoleAssistant {
		t.Fatalf("current turn missing from the window: %+v", w[3:])
	}

	if st := d.History.Stats(); st.Compactions != 1 || st.SummarizedTurns != 4 {
		t.Fatalf("History stats = %+v, want 1 compaction over 4 turns", st)
	}
	// 压缩消耗必须计入 Result：否则 Result.Total() 与租户被扣的额度对不上。
	if res.TokensIn != 100 || res.TokensOut != 21 {
		t.Fatalf("Result tokens = %d/%d, want 100/21 (summarize + answer)",
			res.TokensIn, res.TokensOut)
	}
}

// 压缩失败只意味着上下文变长，不是故障。但它必须留痕，
// 否则"上下文越来越贵"会变成一个没人知道原因的现象。
func TestCompactionFailureIsNotFatal(t *testing.T) {
	mem := newFakeMemory()
	mem.seed("tenant-a", "session-a", turns(6)...)

	log := audit.New(64, nil)
	models := &scriptedModels{
		script: []caps.Response{{}, textReply("按原样继续。")},
		fails:  map[int]error{0: errors.New("摘要服务不可用")},
	}
	d := newDeps(models, mem, nil, guard.NewPolicy(guard.Default(), nil))
	d.History = session.NewHistory(session.Policy{SoftLimit: 4, KeepRecent: 2})
	d.Audit = log.Record

	res, err := agent.Run(context.Background(), d, request("继续"))
	if err != nil {
		t.Fatalf("a failed compaction must not fail the run: %v", err)
	}
	if res.Stop != agent.StopCompleted || res.Compactions != 0 {
		t.Fatalf("res = %+v, want completed with no compaction", res)
	}
	// 历史保持原样：压缩失败不该让一段本来可用的历史消失。
	if w := mem.window("tenant-a", "session-a"); len(w) != 8 {
		t.Fatalf("window = %d turns, want 6 preloaded + 2 from this run", len(w))
	}

	warned := false
	for _, e := range log.For("tenant-a", 0) {
		if e.Action == audit.ActionCapabilityDown {
			warned = true
		}
	}
	if !warned {
		t.Fatal("a failed compaction must be recorded in the audit log")
	}
}

// ---------------------------------------------------------------------------
// 可观测性
// ---------------------------------------------------------------------------

func TestMetricsAndAuditRecordTheRun(t *testing.T) {
	counters := metrics.New().Tenant("tenant-a")
	log := audit.New(64, nil)

	models := &scriptedModels{script: []caps.Response{
		toolReply(call("c1", "a")),
		textReply("好。"),
	}}
	d := newDeps(models, newFakeMemory(), &fakeTools{}, guard.NewPolicy(guard.Default(), nil))
	d.Metrics = counters
	d.Audit = log.Record

	if _, err := agent.Run(context.Background(), d, newRequest()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	snap := counters.Snapshot("tenant-a")
	if snap.RunsStarted != 1 || snap.RunsCompleted != 1 || snap.RunsFailed != 0 {
		t.Fatalf("counters = %+v, want 1/1/0", snap)
	}
	if snap.ToolsCalled != 1 || snap.RunsObserved != 1 {
		t.Fatalf("ToolsCalled/RunsObserved = %d/%d, want 1/1", snap.ToolsCalled, snap.RunsObserved)
	}

	starts, finishes, toolEvents := 0, 0, 0
	for _, e := range log.For("tenant-a", 0) {
		switch e.Action {
		case audit.ActionRunStart:
			starts++
		case audit.ActionRunFinish:
			finishes++
		case audit.ActionToolCall:
			toolEvents++
			if e.Subject != "a" {
				t.Fatalf("tool audit subject = %q, want the tool name", e.Subject)
			}
		}
	}
	if starts != 1 || finishes != 1 || toolEvents != 1 {
		t.Fatalf("audit start/finish/tool = %d/%d/%d, want 1/1/1", starts, finishes, toolEvents)
	}

	// 审计日志必须按租户可分离：另一个租户看不到别人的 run 痕迹。
	if got := log.For("tenant-b", 0); len(got) != 0 {
		t.Fatalf("audit log leaked %d events to another tenant: %+v", len(got), got)
	}
}
