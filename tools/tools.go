// Package tools 是租户可见的工具注册表：白名单、身份剥离、拒绝即审计。
//
// # 身份剥离为什么必须在这一层
//
// prompt 注入最直接的越权路径就是让模型在工具参数里写
//
//	{"tenant_id": "victim-tenant", "session_id": "victim-session"}
//
// 如果工具实现把这个 map 直接转发给下游 API，越权就完成了。防线只有一条：
// **模型给出的身份字段一律不可信，丢弃后由运行时覆盖**（SAFETY.md 硬规则 1）。
//
// 本包把这条规则做成不可绕过的：
//
//   - Call 的入参是结构性剥离过的 Args，且身份以独立字段随 Invocation 传递，
//     因此「模型指定的身份」在实现里**无从表达**，而不是「记得不要去用」；
//   - 剥离是递归的（嵌套 map 与 slice 都查），因为模型完全可以把身份
//     藏进 {"payload": {"tenant_id": ...}}；
//   - 键名比较是归一化的（大小写、下划线、连字符都不影响），
//     因为 tenantId / tenant_id / Tenant-ID / TENANT_ID 是同一个东西；
//   - 剥离不是静默的：被剥掉的键会进审计。模型试图指定身份这件事
//     本身就是一条值得看见的信号。
//
// # 拒绝不是失败
//
// 白名单外的工具返回 Denied=true 的**结果**而不是 error。理由是行为差异：
// 返回 error 通常被循环当作瞬时故障并重试，返回结果则会被当成一次
// 已完成的工具回合回灌给模型。被拒绝的调用必须回灌，否则模型会
// 在一个它得不到反馈的动作上原地打转。
package tools

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/realm"
	"github.com/metaRobin/cordis"
)

// ErrUnknownTool 调用了注册表中不存在的工具（非白名单拒绝，而是根本不存在）。
var ErrUnknownTool = errors.New("insula/tools: unknown tool")

// Handler 是工具的真实实现（信任一级：平台自研或经评审的适配器）。
//
// 实现拿到的是已经剥离过身份字段的 inv.Args；需要身份时从 inv 的显式
// 字段读取，不要去 Args 里找。
type Handler interface {
	Call(ctx context.Context, inv caps.Invocation) (caps.ToolResult, error)
}

// HandlerFunc 把函数适配为 Handler。
type HandlerFunc func(ctx context.Context, inv caps.Invocation) (caps.ToolResult, error)

// Call 实现 Handler。
func (f HandlerFunc) Call(ctx context.Context, inv caps.Invocation) (caps.ToolResult, error) {
	return f(ctx, inv)
}

// ---------------------------------------------------------------------------
// 身份字段
// ---------------------------------------------------------------------------

// defaultIdentityKeys 是默认被剥离的归一化键集合。
//
// 归一化规则：转小写、去掉下划线与连字符。因此 "tenant_id"、"tenantId"、
// "Tenant-ID"、"TENANT_ID" 归一到同一个 "tenantid"。
//
// 裸键 "user" / "session" / "tenant" 也在默认集合里。这会**过度剥离**
// 一部分合法参数（比如查一个叫 user 的字段），是有意为之：身份字段的
// 误剥只会让某个工具少一个参数，漏剥则直接是跨租户越权，两者的代价
// 不对称。确有需要时用 Registry.SetIdentityKeys 收窄，并接受随之而来的风险。
var defaultIdentityKeys = []string{
	"tenantid", "tenant",
	"sessionid", "session",
	"userid", "user",
	"runid", "run",
	"orgid", "organizationid", "workspaceid", "accountid", "principalid",
}

// DefaultIdentityKeys 返回默认身份键集的副本。
//
// 导出它是为了让别的层能做**减法**而不是重新写一份：接入层要检测请求体里
// 夹带的身份字段，但请求体自己有 session 这类合法字段。若那边手抄一份键集，
// 两份清单迟早会分叉，而分叉的表现是「一处挡住、另一处放过」——
// 恰好是最难查的一类漏洞。键集只能有一份定义，差异必须以显式的减法表达。
func DefaultIdentityKeys() []string { return append([]string(nil), defaultIdentityKeys...) }

// NormalizeKey 把键名归一化为用于身份比较的形式。
func NormalizeKey(k string) string {
	var b strings.Builder
	b.Grow(len(k))
	for _, r := range k {
		switch {
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r - 'A' + 'a')
		case r == '_' || r == '-' || r == '.':
			// 分隔符一律忽略，让 tenant_id 与 tenantId 归一。
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// IdentityStripper 按给定键集合递归剥离身份字段。
type IdentityStripper struct {
	keys map[string]bool
}

// NewStripper 构造剥离器。keys 为空时用默认集合；键名会先归一化。
func NewStripper(keys []string) *IdentityStripper {
	if len(keys) == 0 {
		keys = defaultIdentityKeys
	}
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		m[NormalizeKey(k)] = true
	}
	return &IdentityStripper{keys: m}
}

// Stripped 记录一次剥离的结果。
type Stripped struct {
	// Keys 是被剥掉的键的原始写法（去重后升序），供审计展示。
	Keys []string
	// Count 是被剥掉的字段总数（含嵌套层级里的）。
	Count int
}

// StrippedAny 报告是否剥掉了任何字段。
func (s Stripped) StrippedAny() bool { return s.Count > 0 }

// Strip 递归剥离 args 中的身份字段，返回剥离后的副本与剥离记录。
//
// 原 map 不会被修改：调用方可能还要把它原样回灌给模型，而模型应当
// 看到自己"给过"的参数（否则它会认为自己的参数被静默篡改并反复重试）。
func (s *IdentityStripper) Strip(args map[string]any) (map[string]any, Stripped) {
	var rec Stripped
	seen := make(map[string]bool)
	out := s.stripMap(args, &rec, seen)
	sort.Strings(rec.Keys)
	return out, rec
}

func (s *IdentityStripper) stripMap(in map[string]any, rec *Stripped, seen map[string]bool) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if s.keys[NormalizeKey(k)] {
			rec.Count++
			if !seen[k] {
				seen[k] = true
				rec.Keys = append(rec.Keys, k)
			}
			continue
		}
		out[k] = s.stripValue(v, rec, seen)
	}
	return out
}

func (s *IdentityStripper) stripValue(v any, rec *Stripped, seen map[string]bool) any {
	switch t := v.(type) {
	case map[string]any:
		return s.stripMap(t, rec, seen)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = s.stripValue(e, rec, seen)
		}
		return out
	default:
		// 其他类型（含 []map[string]any 之类的具体切片类型）不深入：
		// 它们不是 JSON 解码的自然产物，模型无法通过它们注入身份。
		// 若将来出现别的来源，这里需要相应扩展而不是放宽上层的判断。
		return v
	}
}

// ---------------------------------------------------------------------------
// 注册表
// ---------------------------------------------------------------------------

type toolEntry struct {
	spec    caps.ToolSpec
	handler Handler
	allowed bool
}

// Registry 是某个租户的工具注册表。
//
// 每个租户一份实例，注册在租户隔离域内（realm.ServiceTools）。
// 白名单在构造时就固定下来，运行期只能通过 SetAllowed 调整——
// 不给「让模型自己决定能用哪些工具」留接口。
type Registry struct {
	mu       sync.RWMutex
	entries  map[string]*toolEntry
	order    []string // 保持注册顺序，Specs 输出稳定
	stripper *IdentityStripper

	// OnDenied 在拒绝一次调用时被调用（审计钩子）。不得阻塞。
	OnDenied func(inv caps.Invocation, reason string)
	// OnStripped 在剥离掉身份字段时被调用（审计钩子）。不得阻塞。
	OnStripped func(inv caps.Invocation, rec Stripped)
}

// Compile-time 断言：注册表必须满足数据面契约。
var _ caps.Tools = (*Registry)(nil)

// Option 调整注册表。
type Option func(*Registry)

// WithIdentityKeys 覆盖被剥离的身份字段集合。
func WithIdentityKeys(keys []string) Option {
	return func(r *Registry) { r.stripper = NewStripper(keys) }
}

// WithDeniedHook 注册拒绝审计钩子。
func WithDeniedHook(fn func(inv caps.Invocation, reason string)) Option {
	return func(r *Registry) { r.OnDenied = fn }
}

// WithStrippedHook 注册剥离审计钩子。
func WithStrippedHook(fn func(inv caps.Invocation, rec Stripped)) Option {
	return func(r *Registry) { r.OnStripped = fn }
}

// New 构造注册表。allowlist 为空表示**不开放任何工具**——
// fail-closed 是唯一的默认：空清单配一个空策略是配置疏漏，
// 而一个默认开放的工具集就是一个默认打开的越权面。
func New(allowlist []string, opts ...Option) *Registry {
	r := &Registry{
		entries:  make(map[string]*toolEntry),
		stripper: NewStripper(nil),
	}
	for _, opt := range opts {
		opt(r)
	}
	for _, name := range allowlist {
		if name == "" {
			continue
		}
		if _, ok := r.entries[name]; ok {
			continue
		}
		r.entries[name] = &toolEntry{spec: caps.ToolSpec{Name: name}, allowed: true}
		r.order = append(r.order, name)
	}
	return r
}

// Register 登记一个工具实现。
//
// handler 为 nil 且该工具在白名单里时，调用会返回 ErrUnknownTool——
// 这与「不在白名单」是两种不同的拒绝原因，审计上要能区分：
// 前者是平台自己的装配缺口，后者是策略决定。
func (r *Registry) Register(spec caps.ToolSpec, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if spec.Name == "" {
		return
	}
	e, ok := r.entries[spec.Name]
	if !ok {
		e = &toolEntry{spec: spec}
		r.entries[spec.Name] = e
		r.order = append(r.order, spec.Name)
	}
	e.spec = spec
	e.handler = h
}

// SetAllowed 调整白名单。空清单会关掉全部工具。
func (r *Registry) SetAllowed(names []string) {
	next := make(map[string]bool, len(names))
	for _, n := range names {
		next[n] = true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.entries {
		e.allowed = false
	}
	for name, on := range next {
		if !on {
			continue
		}
		e, ok := r.entries[name]
		if !ok {
			e = &toolEntry{spec: caps.ToolSpec{Name: name}}
			r.entries[name] = e
			r.order = append(r.order, name)
		}
		e.allowed = true
	}
}

// Specs 实现 caps.Tools：只返回白名单内的工具。
//
// 这一点很关键：白名单外的工具**不应出现在模型的提示里**。
// 出现在提示里然后被拒绝，等于把平台的策略暴露给模型去试探。
func (r *Registry) Specs() []caps.ToolSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]caps.ToolSpec, 0, len(r.order))
	for _, name := range r.order {
		e := r.entries[name]
		if e != nil && e.allowed {
			out = append(out, e.spec)
		}
	}
	return out
}

// Call 实现 caps.Tools。
//
// 顺序：白名单 → 剥离 → 查实现 → 执行。剥离放在白名单之后、
// 执行之前——白名单拒绝时根本不会执行，剥离与否不影响安全，
// 但把它放在前面会让「被拒绝的调用也留下剥离记录」这种噪声进审计。
func (r *Registry) Call(ctx context.Context, inv caps.Invocation) (caps.ToolResult, error) {
	if !inv.Tenant.Valid() {
		return caps.ToolResult{}, fmt.Errorf("insula/tools: empty tenant identity")
	}

	r.mu.RLock()
	e := r.entries[inv.Name]
	stripper := r.stripper
	onDenied := r.OnDenied
	onStripped := r.OnStripped
	r.mu.RUnlock()

	if e == nil {
		// 不存在的工具与不在白名单的工具有意合并为同一条对外信息：
		// 区分它们会让调用方（可能是被注入的模型）探测出平台装了哪些
		// 它无权使用的工具。
		reason := "tool not available"
		if onDenied != nil {
			onDenied(inv, reason)
		}
		return caps.ToolResult{Denied: true, Reason: reason}, nil
	}
	if !e.allowed {
		reason := "tool not in tenant allowlist"
		if onDenied != nil {
			onDenied(inv, reason)
		}
		return caps.ToolResult{Denied: true, Reason: reason}, nil
	}

	safeArgs, rec := stripper.Strip(inv.Args)
	if rec.StrippedAny() && onStripped != nil {
		onStripped(inv, rec)
	}
	inv.Args = safeArgs

	if e.handler == nil {
		// 装配缺口：白名单里有、实现没挂。这是平台自己的 bug，
		// 必须以 error 暴露而不是伪装成一次成功的工具回合。
		return caps.ToolResult{}, fmt.Errorf("%w: %s (allowlisted but no handler registered)",
			ErrUnknownTool, inv.Name)
	}
	return e.handler.Call(ctx, inv)
}

// Names 返回白名单内的工具名（升序），供装配期的诊断与断言。
func (r *Registry) Names() []string {
	specs := r.Specs()
	out := make([]string, 0, len(specs))
	for _, s := range specs {
		out = append(out, s.Name)
	}
	sort.Strings(out)
	return out
}

// Allowed 报告某工具是否在白名单内（供测试与装配断言）。
func (r *Registry) Allowed(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[name]
	return ok && e.allowed
}

// IdentityKeys 返回当前生效的归一化身份键集合（升序），供自检与文档。
func (r *Registry) IdentityKeys() []string {
	r.mu.RLock()
	s := r.stripper
	r.mu.RUnlock()
	out := make([]string, 0, len(s.keys))
	for k := range s.keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// cordis 集成
// ---------------------------------------------------------------------------

// TenantConfig 是装配一个租户工具注册表所需的全部输入。
//
// 它作为入口的 Config 传入（`Config: tenantConfig`），而不是让插件
// 闭包持有——loader 的插件解析是「名字 → 一个定义」的静态映射。
//
// Handlers 与 Specs 通常是平台级的（同一批实现服务所有租户），
// Allowlist 与两个钩子才是租户差异所在。把这些放在一个结构里，
// 是为了让「租户能改什么」一目了然：改不了处理器，只能改白名单。
type TenantConfig struct {
	// Allowlist 该租户可见的工具名。为空表示不开放任何工具（fail-closed）。
	Allowlist []string
	// Handlers 工具实现，平台级共享。调用方不应修改它。
	Handlers map[string]Handler
	// Specs 工具描述，按名索引。
	Specs map[string]caps.ToolSpec
	// IdentityKeys 覆盖被剥离的身份字段集合；为空用默认集合。
	IdentityKeys []string
	// OnDenied / OnStripped 审计钩子。
	OnDenied   func(inv caps.Invocation, reason string)
	OnStripped func(inv caps.Invocation, rec Stripped)
}

// Registry 以该配置构造注册表。
func (c *TenantConfig) Registry() *Registry {
	opts := []Option{WithIdentityKeys(c.IdentityKeys)}
	if c.OnDenied != nil {
		opts = append(opts, WithDeniedHook(c.OnDenied))
	}
	if c.OnStripped != nil {
		opts = append(opts, WithStrippedHook(c.OnStripped))
	}
	r := New(c.Allowlist, opts...)
	for name, h := range c.Handlers {
		spec, ok := c.Specs[name]
		if !ok {
			spec = caps.ToolSpec{Name: name}
		}
		r.Register(spec, h)
	}
	// 白名单里有、实现没挂的，也要留下描述，否则 Specs() 会漏掉它，
	// 而调用时又会得到 ErrUnknownTool——那种不一致很难排查。
	for _, name := range c.Allowlist {
		if _, ok := c.Handlers[name]; ok {
			continue
		}
		spec, ok := c.Specs[name]
		if !ok {
			spec = caps.ToolSpec{Name: name}
		}
		r.Register(spec, nil)
	}
	return r
}

// Plugin 返回工具注册表插件**定义**（一个进程内一份）。
//
// 租户差异全部经 Config 传入（见 TenantConfig）。注册出去的服务值
// 是**每租户各一份**的 *Registry，因此两租户的白名单互不可见，
// 黑盒隔离断言也能通过。
func Plugin() *cordis.Plugin {
	return &cordis.Plugin{
		Name: realm.PluginToolRegistry,
		Validate: func(cfg any) (any, error) {
			tc, ok := cfg.(*TenantConfig)
			if !ok || tc == nil {
				return nil, fmt.Errorf("insula/tools: tool-registry config must be a non-nil *tools.TenantConfig, got %T", cfg)
			}
			return tc, nil
		},
		Apply: func(ctx *cordis.Context, cfg any) error {
			_, err := ctx.Provide(realm.ServiceTools, cfg.(*TenantConfig).Registry(), nil)
			return err
		},
	}
}
