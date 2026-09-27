package tools_test

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/metaRobin/insula/caps"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/tools"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// recorder 记录 handler 实际收到的调用，用于断言「模型给的东西到没到 handler」。
type recorder struct {
	got  []caps.Invocation
	next caps.ToolResult
	err  error
}

func (r *recorder) Call(_ context.Context, inv caps.Invocation) (caps.ToolResult, error) {
	r.got = append(r.got, inv)
	if r.err != nil {
		return caps.ToolResult{}, r.err
	}
	return r.next, nil
}

func (r *recorder) last() caps.Invocation { return r.got[len(r.got)-1] }

func inv(name string, args map[string]any) caps.Invocation {
	return caps.Invocation{
		Tenant:  ident.Tenant("victim-tenant"),
		Session: ident.Session("victim-session"),
		Run:     ident.Run("run-1"),
		Name:    name,
		Args:    args,
	}
}

// ---------------------------------------------------------------------------
// 身份剥离（本包最要紧的一条）
// ---------------------------------------------------------------------------

// 模型在工具参数里伪造身份，必须到不了 handler。
//
// 注意这个假"受害者"名字：如果剥离失效，handler 收到的就是攻击者指定的
// 身份，下游 API 会按受害者的权限执行——这是 prompt 注入最直接的越权路径。
func TestForgedIdentityIsStripped(t *testing.T) {
	rec := &recorder{}
	reg := tools.New([]string{"search"})
	reg.Register(caps.ToolSpec{Name: "search"}, rec)

	_, err := reg.Call(context.Background(), inv("search", map[string]any{
		"query":      "hello",
		"tenant_id":  "attacker-tenant",
		"session_id": "attacker-session",
		"user_id":    "attacker-user",
	}))
	if err != nil {
		t.Fatal(err)
	}

	got := rec.last()
	for _, k := range []string{"tenant_id", "session_id", "user_id"} {
		if _, present := got.Args[k]; present {
			t.Fatalf("handler received forged identity field %q: %v", k, got.Args)
		}
	}
	if got.Args["query"] != "hello" {
		t.Fatalf("legitimate argument was lost: %v", got.Args)
	}
	// 运行时身份必须来自 Invocation，而不是模型。
	if got.Tenant != "victim-tenant" || got.Session != "victim-session" {
		t.Fatalf("runtime identity was overridden by the model: %s/%s", got.Tenant, got.Session)
	}
}

// 身份可以被藏进嵌套结构里，剥离必须递归。
func TestForgedIdentityInNestedPayloadIsStripped(t *testing.T) {
	rec := &recorder{}
	reg := tools.New([]string{"http"})
	reg.Register(caps.ToolSpec{Name: "http"}, rec)

	_, err := reg.Call(context.Background(), inv("http", map[string]any{
		"url": "https://example.com",
		"payload": map[string]any{
			"user_id": "attacker-user",
			"nested": map[string]any{
				"tenant_id": "attacker-tenant",
			},
		},
		"headers": []any{
			map[string]any{"session_id": "attacker-session", "accept": "json"},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	got := rec.last()
	payload := got.Args["payload"].(map[string]any)
	if _, present := payload["user_id"]; present {
		t.Fatalf("nested identity field survived: %v", payload)
	}
	nested := payload["nested"].(map[string]any)
	if _, present := nested["tenant_id"]; present {
		t.Fatalf("deeply nested identity field survived: %v", nested)
	}
	headers := got.Args["headers"].([]any)
	first := headers[0].(map[string]any)
	if _, present := first["session_id"]; present {
		t.Fatalf("identity field inside a slice survived: %v", first)
	}
	if first["accept"] != "json" {
		t.Fatalf("legitimate header was lost: %v", first)
	}
}

// 键名的大小写与分隔符差异不能成为绕过手段。
func TestIdentityKeyVariantsAreNormalized(t *testing.T) {
	variants := []string{
		"tenant_id", "tenantId", "TenantID", "TENANT_ID", "tenant-id", "Tenant.Id", "tenantid",
	}
	for _, v := range variants {
		rec := &recorder{}
		reg := tools.New([]string{"t"})
		reg.Register(caps.ToolSpec{Name: "t"}, rec)
		if _, err := reg.Call(context.Background(), inv("t", map[string]any{
			v:       "attacker-tenant",
			"other": "keep",
		})); err != nil {
			t.Fatal(err)
		}
		got := rec.last()
		if len(got.Args) != 1 || got.Args["other"] != "keep" {
			t.Fatalf("variant %q was not stripped: %v", v, got.Args)
		}
	}
}

// 剥离不是静默的：被剥掉的键必须进审计，因为「模型试图指定身份」
// 本身就是一条值得看见的信号。
func TestStrippedFieldsAreReportedForAudit(t *testing.T) {
	var records []tools.Stripped
	var seen caps.Invocation
	reg := tools.New([]string{"t"}, tools.WithStrippedHook(func(i caps.Invocation, rec tools.Stripped) {
		seen = i
		records = append(records, rec)
	}))
	reg.Register(caps.ToolSpec{Name: "t"}, &recorder{})

	if _, err := reg.Call(context.Background(), inv("t", map[string]any{
		"tenant_id": "attacker",
		"user_id":   "attacker",
		"nested":    map[string]any{"session_id": "attacker"},
	})); err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("expected exactly one strip record, got %d", len(records))
	}
	got := records[0].Keys
	sort.Strings(got)
	want := []string{"session_id", "tenant_id", "user_id"}
	if len(got) != len(want) {
		t.Fatalf("stripped keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stripped keys = %v, want %v", got, want)
		}
	}
	if records[0].Count != 3 {
		t.Fatalf("stripped count = %d, want 3", records[0].Count)
	}
	if seen.Tenant != "victim-tenant" {
		t.Fatalf("audit hook got wrong tenant: %s", seen.Tenant)
	}
}

// 没剥离任何东西时不该产生审计噪声。
func TestNoStripHookForCleanArgs(t *testing.T) {
	called := 0
	reg := tools.New([]string{"t"}, tools.WithStrippedHook(func(caps.Invocation, tools.Stripped) {
		called++
	}))
	reg.Register(caps.ToolSpec{Name: "t"}, &recorder{})
	if _, err := reg.Call(context.Background(), inv("t", map[string]any{"query": "ok"})); err != nil {
		t.Fatal(err)
	}
	if called != 0 {
		t.Fatalf("strip hook fired %d times for clean args, want 0", called)
	}
}

// 调用方（循环）可能还要把模型原始参数回灌给模型，因此剥离不得就地修改。
func TestStripDoesNotMutateOriginal(t *testing.T) {
	original := map[string]any{"query": "ok", "tenant_id": "attacker"}
	reg := tools.New([]string{"t"})
	reg.Register(caps.ToolSpec{Name: "t"}, &recorder{})
	if _, err := reg.Call(context.Background(), inv("t", original)); err != nil {
		t.Fatal(err)
	}
	if original["tenant_id"] != "attacker" {
		t.Fatalf("original args map was mutated: %v", original)
	}
	if len(original) != 2 {
		t.Fatalf("original args map lost keys: %v", original)
	}
}

// ---------------------------------------------------------------------------
// 白名单
// ---------------------------------------------------------------------------

// 白名单外的工具不得出现在模型的提示里——出现了再拒绝，
// 等于把平台策略暴露给模型去试探。
func TestSpecsExposeOnlyAllowlistedTools(t *testing.T) {
	reg := tools.New([]string{"search", "fetch"})
	reg.Register(caps.ToolSpec{Name: "search"}, &recorder{})
	reg.Register(caps.ToolSpec{Name: "fetch"}, &recorder{})
	reg.Register(caps.ToolSpec{Name: "shell"}, &recorder{}) // 已注册但不在白名单

	specs := reg.Specs()
	if len(specs) != 2 {
		t.Fatalf("specs = %d, want 2", len(specs))
	}
	for _, s := range specs {
		if s.Name == "shell" {
			t.Fatal("non-allowlisted tool leaked into the model-visible spec list")
		}
	}
	if reg.Allowed("shell") {
		t.Fatal("shell must not be allowed")
	}
}

// 空白名单必须关掉一切：默认开放的工具集就是一个默认打开的越权面。
func TestEmptyAllowlistDisablesEverything(t *testing.T) {
	rec := &recorder{}
	reg := tools.New(nil)
	reg.Register(caps.ToolSpec{Name: "shell"}, rec)

	if got := reg.Specs(); len(got) != 0 {
		t.Fatalf("specs = %v, want none", got)
	}
	res, err := reg.Call(context.Background(), inv("shell", nil))
	if err != nil {
		t.Fatalf("denial must be a result, not an error: %v", err)
	}
	if !res.Denied {
		t.Fatal("call should be denied")
	}
	if len(rec.got) != 0 {
		t.Fatal("a denied tool must never reach its handler")
	}
}

// 拒绝必须返回 Denied 结果而不是 error：error 通常被循环当作瞬时故障
// 并重试，结果才会被当作一次已完成的工具回合回灌给模型。
func TestDeniedCallReturnsResultNotError(t *testing.T) {
	var reasons []string
	rec := &recorder{}
	reg := tools.New([]string{"search"}, tools.WithDeniedHook(func(caps.Invocation, string) {
		// hook 只记录调用次数；原因通过返回值断言。
		reasons = append(reasons, "denied")
	}))
	reg.Register(caps.ToolSpec{Name: "search"}, rec)

	res, err := reg.Call(context.Background(), inv("shell", nil))
	if err != nil {
		t.Fatalf("denial must not be an error: %v", err)
	}
	if !res.Denied || res.Reason == "" {
		t.Fatalf("denial must carry a reason: %+v", res)
	}
	if len(reasons) != 1 {
		t.Fatalf("denied hook calls = %d, want 1", len(reasons))
	}
	if len(rec.got) != 0 {
		t.Fatal("denied tool must not reach its handler")
	}
}

// 白名单里有、实现没挂，是平台自己的装配缺口，必须以 error 暴露，
// 而不是伪装成一次成功的工具回合。
func TestAllowlistedToolWithoutHandlerIsAnError(t *testing.T) {
	reg := tools.New([]string{"search"})
	_, err := reg.Call(context.Background(), inv("search", nil))
	if !errors.Is(err, tools.ErrUnknownTool) {
		t.Fatalf("want ErrUnknownTool, got %v", err)
	}
}

// 运行期调整白名单必须立即生效（ADR-005 的运行时覆盖）。
func TestSetAllowedTakesEffectImmediately(t *testing.T) {
	rec := &recorder{}
	reg := tools.New([]string{"search"})
	reg.Register(caps.ToolSpec{Name: "search"}, rec)
	reg.Register(caps.ToolSpec{Name: "shell"}, rec)

	if _, err := reg.Call(context.Background(), inv("shell", nil)); err != nil {
		t.Fatal(err)
	}
	if len(rec.got) != 0 {
		t.Fatal("shell should be denied before being allowlisted")
	}

	reg.SetAllowed([]string{"shell"})
	if reg.Allowed("search") {
		t.Fatal("SetAllowed must replace the list, not extend it")
	}
	if _, err := reg.Call(context.Background(), inv("shell", nil)); err != nil {
		t.Fatal(err)
	}
	if len(rec.got) != 1 {
		t.Fatalf("shell should now be callable, handler calls = %d", len(rec.got))
	}
}

// 空租户标识必须 fail-closed：归属不清时执行是最坏的选择。
func TestEmptyTenantIsRejected(t *testing.T) {
	rec := &recorder{}
	reg := tools.New([]string{"search"})
	reg.Register(caps.ToolSpec{Name: "search"}, rec)

	i := inv("search", nil)
	i.Tenant = ""
	if _, err := reg.Call(context.Background(), i); err == nil {
		t.Fatal("empty tenant identity must be rejected")
	}
	if len(rec.got) != 0 {
		t.Fatal("handler must not run without a tenant identity")
	}
}

// 默认身份键集合必须覆盖设计里点名的三个字段，否则硬规则 1 形同虚设。
func TestDefaultIdentityKeysCoverTheNamedFields(t *testing.T) {
	reg := tools.New([]string{"t"})
	have := make(map[string]bool)
	for _, k := range reg.IdentityKeys() {
		have[k] = true
	}
	for _, want := range []string{"tenantid", "sessionid", "userid"} {
		if !have[want] {
			t.Fatalf("default identity keys must include %q, got %v", want, reg.IdentityKeys())
		}
	}
}

// 收窄身份键集合是允许的，但行为必须与配置一致（便于评估风险）。
func TestCustomIdentityKeysAreHonored(t *testing.T) {
	rec := &recorder{}
	reg := tools.New([]string{"t"}, tools.WithIdentityKeys([]string{"tenant_id"}))
	reg.Register(caps.ToolSpec{Name: "t"}, rec)

	if _, err := reg.Call(context.Background(), inv("t", map[string]any{
		"tenant_id": "attacker",
		"user_id":   "kept-on-purpose",
	})); err != nil {
		t.Fatal(err)
	}
	got := rec.last()
	if _, present := got.Args["tenant_id"]; present {
		t.Fatalf("configured key was not stripped: %v", got.Args)
	}
	if got.Args["user_id"] != "kept-on-purpose" {
		t.Fatalf("keys outside the configured set must be kept: %v", got.Args)
	}
}

func TestNormalizeKey(t *testing.T) {
	cases := map[string]string{
		"tenant_id": "tenantid",
		"tenantId":  "tenantid",
		"TENANT-ID": "tenantid",
		"tenant.id": "tenantid",
		"query":     "query",
		"Query_Str": "querystr",
	}
	for in, want := range cases {
		if got := tools.NormalizeKey(in); got != want {
			t.Fatalf("NormalizeKey(%q) = %q, want %q", in, got, want)
		}
	}
}
