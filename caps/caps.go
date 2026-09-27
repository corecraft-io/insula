// Package caps 定义数据面所需的能力契约，以及把控制面的服务解析结果
// 搬运到数据面的那一个薄层。
//
// # 为什么能力快照不该在数据面现取
//
// 设计骨架（附录 A）在每次 run 开始时做一次 DoSync 取句柄，成本约 3.79 µs。
// 单看不贵，但它有两个问题：
//
//  1. 每次 run 都要占用分片调度器一个回合，高并发下又变成排队点；
//  2. 骨架里的取法本身是错的——DoSync 的回调参数永远是 app.root，
//     而 root 落在**默认域**，在它上面 Get 会解析到默认域的实现，
//     等于绕过全部租户隔离。
//
// 本包把这两件事一起解决：让 cordis 自己把「已解析的租户服务」推送到
// Handle 里，数据面只做一次带锁读。
//
// # 推送机制：用协效应表达可用性
//
// 每个能力服务配一个 Watcher 插件，它以 Inject **精确声明一个** 依赖。
// 由于 cordis 的 Inject 要求依赖全部满足才会激活，Watcher 的存活状态
// 就等于「该服务此刻可用」——可用性判断由 cordis 的依赖机制免费完成，
// 不需要任何轮询。Watcher 在 Apply 里把已经解析好的值交给 Handle，
// 卸载时（Effect 的逆操作）再交还。
//
// 这么做有三个好处：
//
//   - 值是在租户隔离域内由 cordis 解析出来的，隔离正确性由构造保证，
//     数据面无从绕过；
//   - 数据面每次 run 零 cordis 往返，没有调度器占用；
//   - 某个能力缺失时，只有那一个 Watcher 不激活，其余能力照常可用，
//     天然得到「降级而非全挂」的语义。
//
// 注意 Watcher 之间是**兄弟关系**：它们各自 Inject，因此各自从全局
// 隔离域存储里解析（reflect.getImpl），而不是靠沿 fiber 链向上找。
// 这正是 sibling 之间能互相看见的唯一合法途径——不要在 Watcher 里
// 试图用 ctx.Get 去够一个没有 Inject 声明的服务，那是取不到的。
package caps

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/metaRobin/cordis"
	"github.com/metaRobin/insula/guard"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/memory"
	"github.com/metaRobin/insula/realm"
)

// ---------------------------------------------------------------------------
// 数据面契约
// ---------------------------------------------------------------------------

// Message 一条模型消息。
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content,omitempty"`
	Name    string `json:"name,omitempty"`
	// ToolCalls 是助手回合发出的工具调用；ToolCallID 是工具回合的应答目标。
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolCall 一次工具调用请求。
type ToolCall struct {
	ID   string         `json:"id"`
	Name string         `json:"name"`
	Args map[string]any `json:"args,omitempty"`
}

// ToolSpec 暴露给模型的工具描述。
type ToolSpec struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Schema      map[string]any `json:"schema,omitempty"`
}

// Request 一次模型补全请求。
//
// 这里**没有**任何凭证字段，且永远不会有：凭证由网关在服务端注入
// （见 SAFETY.md 硬规则 2）。结构体层面不存在该字段，就不会有人
// 从模型输出里把它填进来。
type Request struct {
	Model       string
	Messages    []Message
	Tools       []ToolSpec
	MaxTokens   int
	Temperature float64
}

// Response 一次模型补全结果。
type Response struct {
	Message          Message
	PromptTokens     int
	CompletionTokens int
	FinishReason     string
}

// Models 是模型补全能力。
//
// Healthy 是给调用方做**局部**降级判断用的（比如跳过需要大模型的步骤）；
// 平台级的反应式降级走 cordis 的 Provide check，不依赖这个返回值。
type Models interface {
	Complete(ctx context.Context, req Request) (Response, error)
	Healthy() bool
}

// Invocation 是一次工具调用的完整上下文。
//
// 身份字段是**必传参数**而不是从 context.Context 里推导的值（ADR-005）：
// 工具的实现在签名上就必须显式声明它需要哪些身份，不能悄悄从一个
// 可能已被替换或丢失的上下文里读。
//
// Args 是**已经剥离过身份字段**的模型参数。模型给出的 tenant_id /
// session_id / user_id 一律不可信，剥离动作发生在 tools 包，且剥离结果
// 会进审计——模型试图指定身份这件事本身就是要被看见的信号。
type Invocation struct {
	Tenant  ident.Tenant
	Session ident.Session
	Run     ident.Run
	Name    string
	Args    map[string]any
}

// ToolResult 一次工具调用结果。
type ToolResult struct {
	Content string
	Data    map[string]any
	// Denied 表示调用被策略拒绝（而非执行失败）。被拒绝的调用必须
	// 进入审计，并且必须作为工具结果回灌给模型——静默失败会让模型
	// 反复重试同一个被拒工具。
	Denied bool
	Reason string
}

// Tools 是工具能力。
type Tools interface {
	// Specs 返回当前租户可见的工具清单（已过白名单）。
	Specs() []ToolSpec
	// Call 执行一次工具调用。身份字段由实现负责剥离并覆盖，
	// inv.Args 里若出现身份字段，实现必须忽略而不是当真。
	Call(ctx context.Context, inv Invocation) (ToolResult, error)
}

// Memory 是数据面所需的记忆读写子集。
//
// 刻意**不复用** memory.Store：agent 循环只需要下面这几件事，
// 而 Store 里还有 DropTenant / DropSession 这类管理操作。
// 收窄接口让「循环代码拿得到注销租户的能力」在类型层面不成立。
//
// ReplaceTurns 在列表里，因为摘要压缩必须能整体替换一个会话的窗口
// （压缩的语义就是"用一段摘要替换若干轮"）。它按 (租户, 会话) 定位，
// 影响范围止于一个会话，不构成跨租户能力。
type Memory interface {
	AppendTurn(t ident.Tenant, s ident.Session, turn memory.Turn) (int, error)
	Turns(t ident.Tenant, s ident.Session) ([]memory.Turn, error)
	ReplaceTurns(t ident.Tenant, s ident.Session, turns []memory.Turn) error
	PutDoc(t ident.Tenant, d memory.Doc) error
	Search(t ident.Tenant, q memory.Query) ([]memory.Hit, error)
}

// 编译期断言：租户存储句柄必须满足收窄后的数据面接口。
var _ Memory = (*memory.Handle)(nil)

// ---------------------------------------------------------------------------
// 必需服务
// ---------------------------------------------------------------------------

// Required 是跑一次 agent 循环所必需的服务名。
//
// tools 刻意不在其中：没有工具的纯问答租户是合法配置，属于降级而非故障。
// 把 tools 也列进来会让「租户没配工具」直接变成启动失败，那是过度收紧。
//
// guard 必须在其中。它是唯一一处「缺失会让平台失去防护」的能力：
// 没有预算策略的循环可以无限跑。缺失时给一个默认预算看似更友好，
// 但那只是把一次配置疏漏藏起来——宁可这一租户跑不起来。
var Required = []string{realm.ServiceModels, realm.ServiceMemory, realm.ServiceGuard}

// Snapshot 是一次 run 开始时的能力视图。
//
// 它是一份**值拷贝**：拿到之后无论控制面怎么变，这次 run 都按这一份跑，
// 避免同一次 run 中途换掉模型网关导致前后行为不一致。
type Snapshot struct {
	Tenant ident.Tenant
	Models Models
	Memory Memory
	Tools  Tools
	Guard  *guard.Policy

	// Present / Missing / BadType 均为服务名的升序列表，便于断言与日志。
	Present []string
	Missing []string
	BadType []string

	// Gen 是快照代次。控制面每发生一次能力增减就递增；
	// 数据面可据此判断「这次 run 用的视图是否已经过期」。
	Gen uint64
}

// Degraded 报告是否存在任何能力缺失或类型不符（含可选的 tools）。
func (s Snapshot) Degraded() bool { return len(s.Missing) > 0 || len(s.BadType) > 0 }

// Ready 报告必需能力是否齐备。为 false 时不应启动 agent 循环。
func (s Snapshot) Ready() bool {
	for _, name := range Required {
		if !s.has(name) {
			return false
		}
	}
	return true
}

func (s Snapshot) has(name string) bool {
	for _, n := range s.Present {
		if n == name {
			return true
		}
	}
	return false
}

// String 用于日志。不包含任何能力值，避免把句柄打进日志。
func (s Snapshot) String() string {
	return fmt.Sprintf("caps{tenant=%s gen=%d present=%v missing=%v badtype=%v}",
		s.Tenant, s.Gen, s.Present, s.Missing, s.BadType)
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

// ErrNoSnapshot 尚无可用能力快照（租户子树未就绪或已拆除）。
var ErrNoSnapshot = errors.New("insula/caps: no capability snapshot available")

type entry struct {
	value any
	ctx   *cordis.Context
	refs  int
}

// Handle 持有某个租户当前可用的能力值。
//
// 所有写入都发生在控制面（Watcher 的 Apply / 卸载），读取发生在数据面，
// 因此用一把 RWMutex 即可：数据面是纯读，不会有锁竞争的热点。
type Handle struct {
	mu     sync.RWMutex
	tenant ident.Tenant
	// entries 的键是服务名。值来自 cordis 在租户隔离域内的解析结果。
	entries map[string]*entry
	// ctx 是该租户的上下文，**仅供控制面自检使用**（如 realm.AssertIsolated）。
	// 数据面不得用它取服务：它的 store 只含该 Watcher 自己 Inject 的名字。
	ctx *cordis.Context
	gen uint64
}

// NewHandle 构造某个租户的能力句柄。
func NewHandle(t ident.Tenant) *Handle {
	return &Handle{tenant: t, entries: make(map[string]*entry)}
}

// Tenant 返回句柄所属租户。
func (h *Handle) Tenant() ident.Tenant { return h.tenant }

// Attach 登记一个已解析的能力值。由 Watcher 的 Apply 调用。
//
// 同一服务名允许被多个 Watcher 引用（配置错误时可能发生），因此用计数
// 而不是布尔：只有最后一个引用撤销时才真正移除，避免先卸载的那个
// Watcher 把仍然可用的能力误标为缺失。
func (h *Handle) Attach(ctx *cordis.Context, name string, value any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.entries[name]
	if e == nil {
		e = &entry{}
		h.entries[name] = e
	}
	e.refs++
	e.value = value
	if e.ctx == nil {
		e.ctx = ctx
	}
	if h.ctx == nil {
		h.ctx = ctx
	}
	h.gen++
}

// Detach 撤销一个能力注册。由 Watcher 的 Effect 逆操作调用。
func (h *Handle) Detach(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.entries[name]
	if e == nil {
		return
	}
	e.refs--
	if e.refs > 0 {
		return
	}
	delete(h.entries, name)
	h.gen++
	// 最后一个 Watcher 也走了，说明租户子树正在拆除。此时必须把上下文
	// 一并收回：继续持有它只会让调用方以为租户还活着，进而去用一个
	// 已经悬垂的 *cordis.Context。
	if len(h.entries) == 0 {
		h.ctx = nil
	}
}

// Context 返回租户上下文，仅供控制面自检使用。
//
// 刻意返回 ok 而不是裸指针：当所有 Watcher 都已卸载（租户正在拆除）时
// 返回 false，调用方就不会去用一个已经悬垂的上下文。
func (h *Handle) Context() (*cordis.Context, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ctx, h.ctx != nil
}

// AssertIsolated 对两个租户句柄逐服务做黑盒隔离断言。
//
// 这是隔离自检的**唯一**实现，供分片内与跨分片两条路径共用：
// 分片内两租户同域不同 fiber，跨分片两租户根本在不同的 App 里。
// 两者的结论来源不同（前者靠 isolateKey，后者靠"不同的 Reflect 实例"），
// 但可编程的验证手段是同一个——所以断言函数也应当只有一个。
//
// names 传 realm.Services()。任一侧尚未就绪（还没有 Watcher 激活）
// 时按 realm.AssertIsolated 的规则处理：双方都不可见也算隔离成立。
// 这正是它作为**哨兵**而非完整性证明的定位——见 shard 包的自检注释。
func AssertIsolated(a, b *Handle, names []string) error {
	if a == nil || b == nil {
		return fmt.Errorf("%w: nil capability handle", realm.ErrIsolationBreach)
	}
	ca, oka := a.Context()
	cb, okb := b.Context()
	if !oka || !okb {
		// 一侧还没就绪（新建或正在拆除）：没有可比的上下文。
		// 不报越界，但调用方（自检）应当把它计入"未覆盖"而不是"通过"。
		return fmt.Errorf("%w: one side carries no tenant context", ErrNoSnapshot)
	}
	for _, name := range names {
		if err := realm.AssertIsolated(ca, cb, name); err != nil {
			return err
		}
	}
	return nil
}

// Attached 返回当前已登记的服务名（升序）。
// 比 Snapshot 更宽：快照只关心数据面那几项（models/memory/tools/guard），
// 而"每个服务是否都有提供者"是**启动自检**要回答的问题。一个永远
// 不激活的探测入口不会让任何东西报错，它只会让某个能力在你第一次
// 用到时才发现不对——所以覆盖度必须可断言。
//
// 顺带：它列出的正是 realm.Services() 的覆盖情况，把两边一比就能
// 发现"新增服务忘了挂 Watcher"这类漏配。
func (h *Handle) Attached() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.entries))
	for name := range h.entries {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Take 取一份能力快照。纯内存读，不触碰 cordis。
func (h *Handle) Take() Snapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	snap := Snapshot{Tenant: h.tenant, Gen: h.gen}
	for name, e := range h.entries {
		// 类型化 nil（如 (*guard.Policy)(nil)）会让类型断言成功，
		// 于是一个不可用的值被记成「能力已就绪」，调用方下一步
		// 解引用就崩。这类值必须按「注册了但不可用」处理。
		if e.value == nil || isNilValue(e.value) {
			snap.BadType = append(snap.BadType, name)
			continue
		}
		switch name {
		case realm.ServiceModels:
			if m, ok := e.value.(Models); ok {
				snap.Models = m
			} else {
				snap.BadType = append(snap.BadType, name)
				continue
			}
		case realm.ServiceMemory:
			if m, ok := e.value.(Memory); ok {
				snap.Memory = m
			} else {
				snap.BadType = append(snap.BadType, name)
				continue
			}
		case realm.ServiceTools:
			if t, ok := e.value.(Tools); ok {
				snap.Tools = t
			} else {
				snap.BadType = append(snap.BadType, name)
				continue
			}
		case realm.ServiceGuard:
			if g, ok := e.value.(*guard.Policy); ok {
				snap.Guard = g
			} else {
				snap.BadType = append(snap.BadType, name)
				continue
			}
		default:
			// 非数据面服务（caps 等）不需要进入快照。
			continue
		}
		snap.Present = append(snap.Present, name)
	}
	for _, name := range realm.Services() {
		if _, ok := h.entries[name]; ok {
			continue
		}
		if isDataPlane(name) {
			snap.Missing = append(snap.Missing, name)
		}
	}
	sort.Strings(snap.Present)
	sort.Strings(snap.Missing)
	sort.Strings(snap.BadType)
	return snap
}

// isNilValue 报告一个接口值是否为类型化 nil 指针/映射/通道/函数。
//
// 它存在的唯一理由是：`any((*T)(nil)) != nil`。不处理这一条，
// "注册了一个空值"就会被当成"能力可用"。
func isNilValue(v any) bool {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}

// isDataPlane 报告某服务名是否属于快照关心的数据面能力。
func isDataPlane(name string) bool {
	switch name {
	case realm.ServiceModels, realm.ServiceMemory, realm.ServiceTools, realm.ServiceGuard:
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Watcher
// ---------------------------------------------------------------------------

// attachWatcher 是 Watcher 的公共实现体。
func attachWatcher(ctx *cordis.Context, h *Handle, name string) error {
	// 能进入 Apply 就说明 Inject 声明的依赖已解析成功，
	// 因此这次 Get 必然命中——命中不了说明 cordis 的
	// 「Inject 满足才激活」契约被破坏了，必须当场报错。
	v, ok := ctx.Get(name)
	if !ok {
		return fmt.Errorf("%w: service %q resolved for inject but not gettable",
			ErrNoSnapshot, name)
	}
	_, err := ctx.Effect("caps.attach("+name+")", func() (cordis.Dispose, error) {
		h.Attach(ctx, name, v)
		return func() { h.Detach(name) }, nil
	})
	return err
}

// watcherName 返回某服务的探测插件名。
func watcherName(name string) string { return "caps-watch-" + name }

// Watcher 返回声明单个依赖的探测插件，句柄直接闭包在里面。
//
// 适用于**命令式**挂载（`ctx.Plugin(h.Watcher(name), nil)`），
// 例如自检与测试。经 loader 的入口树装配时必须用 WatcherPlugin：
// loader 的插件解析是「名字 → 一个定义」的静态映射，闭包在这里的
// 句柄无法按租户区分。
func (h *Handle) Watcher(name string) *cordis.Plugin {
	return &cordis.Plugin{
		Name:   watcherName(name),
		Inject: map[string]any{name: nil},
		Apply: func(ctx *cordis.Context, _ any) error {
			return attachWatcher(ctx, h, name)
		},
	}
}

// WatcherPlugin 返回某租户级服务的探测插件**定义**。
//
// 它是经 loader 装配时必须用的形状：一个进程内**一份**定义，
// 租户身份从入口的 Config 传入（`Config: capsHandle`）。
// 原因是 loader 的 resolvePlugin(name) 是静态映射，每个租户
// 各持一份不同插件实例的做法挂不上入口树。
//
// Config 类型不符时经 Validate 报错，入口停在可恢复的 FAILED 状态，
// 而不是在 Apply 里 panic 成一个只有栈帧能解释的现场。
func WatcherPlugin(name string) *cordis.Plugin {
	return &cordis.Plugin{
		Name:   watcherName(name),
		Inject: map[string]any{name: nil},
		Validate: func(cfg any) (any, error) {
			h, ok := cfg.(*Handle)
			if !ok || h == nil {
				return nil, fmt.Errorf("%w: watcher %q config must be a non-nil *caps.Handle, got %T",
					ErrNoSnapshot, watcherName(name), cfg)
			}
			return h, nil
		},
		Apply: func(ctx *cordis.Context, cfg any) error {
			return attachWatcher(ctx, cfg.(*Handle), name)
		},
	}
}

// Watchers 返回覆盖全部租户级服务的 Watcher 清单（命令式挂载用）。
//
// 覆盖的是 realm.Services() 而不是仅数据面三项：guard 之类的服务
// 也需要「存在即可用」的探活，且让清单由 realm 单一来源派生，
// 将来新增服务不会漏挂 Watcher。
func (h *Handle) Watchers() []*cordis.Plugin {
	names := realm.Services()
	out := make([]*cordis.Plugin, 0, len(names))
	for _, name := range names {
		out = append(out, h.Watcher(name))
	}
	return out
}

// WatcherPluginNames 返回全部探测插件的名字清单。
//
// 供 loader 的插件解析表使用：名字必须与 WatcherPlugin 生成的完全一致，
// 因此这里从同一函数派生而不是手写字符串，避免两处漂移。
func WatcherPluginNames() []string {
	names := realm.Services()
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, watcherName(name))
	}
	return out
}

// PublisherPlugin 返回把 Handle 自身注册为 caps 服务的插件定义。
//
// 为什么需要它：realm.Services() 里声明了 ServiceCaps，而探测依赖是
// 按"服务是否可解析"表达可用性的——若没有提供者，那个 Watcher 会
// 永远停在待激活状态。一个永远不激活的探针不是"无害的冗余"，
// 它会让自检报告里出现一个每条租户都缺的能力，从而稀释真正的告警。
//
// 注册出去的是 Handle 自身：会话级组件若需要在 cordis 内部读到能力
// 快照，Inject("caps") 即可，不必绕到数据面。
func PublisherPlugin() *cordis.Plugin {
	return &cordis.Plugin{
		Name: realm.PluginCapsProbe,
		Validate: func(cfg any) (any, error) {
			h, ok := cfg.(*Handle)
			if !ok || h == nil {
				return nil, fmt.Errorf("%w: caps-probe config must be a non-nil *caps.Handle, got %T",
					ErrNoSnapshot, cfg)
			}
			return h, nil
		},
		Apply: func(ctx *cordis.Context, cfg any) error {
			_, err := ctx.Provide(realm.ServiceCaps, cfg.(*Handle), nil)
			return err
		},
	}
}
