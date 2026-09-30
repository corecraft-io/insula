// Package tenant 是租户子系统：把「一个租户」装配成 cordis 入口树里的一棵子树。
//
// # 租户 = 入口子树 + 强制私有域 + 命名空间化的短 ID
//
// 三件事必须一起做，少一件都不成立：
//
//  1. **一棵子树**：租户根是一个分组入口，模型网关/记忆/工具/守卫/能力探测
//     都是它的子入口。子系统之间靠 cordis 的依赖机制互相看见，不靠全局变量。
//  2. **一次 Isolate 声明**：只在租户根上写 realm.Tenant()，整棵子树经上下文
//     父链继承 "#t-<hash>"。平台里没有任何第二处写 Isolate 字面量的地方
//     （见 realm 包）。
//  3. **命名空间化的短 ID**：EntryTree.store 以**短 ID** 为扁平索引键，
//     短 ID 必须全树唯一；而 reconcile 遇到重复 ID 只记日志然后跳过。
//     所以所有入口 ID 都带租户哈希前缀。
//
// # 两个必须避开的坑（都有源码依据）
//
// **坑一：不要用 Loader 的便捷方法做批量。** loader.go:705/721/734 显示
// Loader.Create/Remove/Update 各自做一次 DoSync **并且**调一次 app.Wait()，
// 而 Wait() 会遍历全部 fiber（app.go 的 settled）。循环调用它们做 N 个
// 租户的开通就是 N 次全量扫描——这正是设计 §9 里那个最隐蔽的 O(N) 退化。
// 正确做法：一次 app.DoSync 里循环调 tree.Create，整批只 Wait 一次。
//
// **坑二：绝不 Update 分组入口。** 分组入口的 update 会把 Config 无条件
// 转发给分组插件做 reconcile（loader.go:374），而 Config 为 nil 时
// reconcile 会**清空全部子入口**（loader.go:463 + 483）。会话是动态挂进去的，
// 不在任何配置列表里，所以对租户分组（或会话分组）调用 Update 会把
// 已经存在的会话全部摘掉。租户的配置变更一律改**子入口**，不改分组。
package tenant

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/corecraft-io/cordis"
	"github.com/corecraft-io/insula/audit"
	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/gateway"
	"github.com/corecraft-io/insula/guard"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/realm"
	"github.com/corecraft-io/insula/session"
	"github.com/corecraft-io/insula/tools"
)

// 错误值。
var (
	// ErrNoTenant 该租户不在本分片上。
	ErrNoTenant = errors.New("insula/tenant: tenant not found on this shard")
	// ErrDuplicateTenant 该租户已存在。
	ErrDuplicateTenant = errors.New("insula/tenant: tenant already exists")
	// ErrShortAssembly 入口装配完成后子入口数量不符。
	//
	// 这个错误的存在是为了把 cordis 的一处静默行为变成响亮失败：
	// reconcile 遇到重复短 ID 只记日志并跳过（loader.go:144-157），
	// 若不做装配后校验，一个短 ID 冲突会表现为"某个服务莫名其妙不可用"，
	// 而不会指向真正的原因。
	ErrShortAssembly = errors.New("insula/tenant: entry tree assembled with fewer children than requested")
	// ErrTenantDisabled 租户处于暂停态，不接受新会话。
	ErrTenantDisabled = errors.New("insula/tenant: tenant is disabled")
)

// ---------------------------------------------------------------------------
// Spec / Deps
// ---------------------------------------------------------------------------

// Spec 描述一个租户的装配参数。它必须能完整决定这棵子树的形状——
// 任何"平台默认值"都在装配前解析完，不在运行期现取。
type Spec struct {
	// ID 租户标识。
	ID ident.Tenant
	// Quota 模型额度。
	Quota gateway.Quota
	// ToolAllowlist 工具白名单。为空表示不开放任何工具。
	ToolAllowlist []string
	// GuardBudget 循环预算。
	GuardBudget guard.Budget
	// SessionPolicy 会话历史窗口策略。
	SessionPolicy session.Policy
	// MaxScratchEntries 会话临时空间条目上限。
	MaxScratchEntries int
	// Disabled 装配为暂停态：入口存在但全部子服务不激活，可原地恢复。
	//
	// 暂停与注销的分界线是数据：Disabled 保留记忆，Deprovision 清除记忆。
	// 要"临时停一下"就用它，不要用注销。
	Disabled bool
}

// Normalize 补齐默认值。
func (s Spec) Normalize() Spec {
	if s.GuardBudget == (guard.Budget{}) {
		s.GuardBudget = guard.Default()
	}
	s.SessionPolicy = s.SessionPolicy.Normalize()
	if s.MaxScratchEntries <= 0 {
		s.MaxScratchEntries = session.DefaultMaxScratchEntries
	}
	return s
}

// Deps 是全平台共享的依赖。
//
// 这些都是**进程级单例**，不是每租户一份：共享的连接池、共享的存储、
// 共享的工具实现。每租户一份的只有"句柄"（见 realm 包与 SAFETY 硬规则 4）。
type Deps struct {
	App    *cordis.App
	Loader *cordis.Loader
	Hasher *realm.Hasher
	Pool   *gateway.Pool
	Store  *memory.MemStore

	// Tools 是平台级工具模板：Handlers 与 Specs 所有租户共用，
	// 每租户的 Allowlist 在装配时覆盖。
	Tools *tools.TenantConfig

	Clock func() time.Time
	// Audit 可为 nil。
	Audit *audit.Log
	// OnWarn 接收非致命告警，可为 nil。
	OnWarn func(msg string, args ...any)
}

func (d Deps) now() time.Time {
	if d.Clock != nil {
		return d.Clock()
	}
	return time.Now()
}

func (d Deps) warn(msg string, args ...any) {
	if d.OnWarn != nil {
		d.OnWarn(msg, args...)
	}
}

// PluginTable 返回平台插件的解析表：插件名 → 插件定义。
//
// 它是 loader 的 resolve 参数的来源。注意这里是**静态映射**——
// 每个名字一个定义，租户差异全部经入口 Config 传入。这正是
// "每个租户各持一份插件实例"挂不上入口树的原因（见各包的 Plugin()）。
func PluginTable() map[string]*cordis.Plugin {
	table := map[string]*cordis.Plugin{
		realm.PluginModelGateway: gateway.Plugin(),
		realm.PluginMemoryStore:  memory.Plugin(),
		realm.PluginToolRegistry: tools.Plugin(),
		realm.PluginLoopGuard:    guard.Plugin(),
		realm.PluginCapsProbe:    caps.PublisherPlugin(),
		realm.PluginSession:      session.Plugin(),
	}
	for _, name := range caps.WatcherPluginNames() {
		// WatcherPluginNames 与 WatcherPlugin 从同一个函数派生名字，
		// 因此这里取服务名时前缀是已知的。
		svc := name[len("caps-watch-"):]
		table[name] = caps.WatcherPlugin(svc)
	}
	return table
}

// Resolver 返回可直接交给 loader.NewLoader 的解析函数。
func Resolver() func(name string) (*cordis.Plugin, error) {
	table := PluginTable()
	return func(name string) (*cordis.Plugin, error) {
		p, ok := table[name]
		if !ok {
			return nil, fmt.Errorf("insula/tenant: unknown plugin %q", name)
		}
		return p, nil
	}
}

// NewLoader 以平台插件表创建 loader。
func NewLoader(app *cordis.App) *cordis.Loader {
	return cordis.NewLoader(app, Resolver())
}

// ---------------------------------------------------------------------------
// 租户与会话
// ---------------------------------------------------------------------------

// Tenant 是一个已开通租户的句柄。
type Tenant struct {
	ID  ident.Tenant
	sim Spec

	// EntryID 租户根入口的短 ID（t-<hash>）。它恰好处在根组下，
	// 因此短 ID 与全路径同形，可以直接用于 Create/Remove/Resolve。
	EntryID string
	// SessionsEntryID 会话分组入口的**短 ID**（<租户根>-sessions）。
	SessionsEntryID string
	// sessionsPath 会话分组入口的**全路径**。
	//
	// 与短 ID 分开存不是冗余：cordis 的 store 索引用短 ID，而
	// Resolve/Create/Remove 认路径。会话分组在租户根之下，寻址必须
	// 用路径——用短 ID 会被当成"根组下的同名入口"，报 cannot resolve。
	// 两者由 realm.Path 一处分界，见那里注释。
	sessionsPath string

	// children 是租户根的静态子入口列表，在装配前一次算定。
	//
	// 之所以缓存而不是每次现算：装配后校验要拿它与实际子入口数比，
	// 若两边各自现算，读者就得先证明"两次调用必然产出等长列表"
	// 才能相信那个比对是有意义的。
	children []cordis.EntryOptions

	// Caps 是能力快照持有者：由控制面的 Watcher 填充，数据面只读。
	Caps *caps.Handle
	// Gateway 是模型池瘦句柄。
	Gateway *gateway.Handle

	createdAt time.Time

	mu       sync.Mutex
	seq      uint64
	sessions map[ident.Session]*Session
}

// Spec 返回租户的装配参数（已归一化）。
func (t *Tenant) Spec() Spec { return t.sim }

// CreatedAt 返回开通时刻。
func (t *Tenant) CreatedAt() time.Time { return t.createdAt }

// Snapshot 取一份能力快照（数据面零 cordis 往返）。
func (t *Tenant) Snapshot() caps.Snapshot { return t.Caps.Take() }

// Sessions 返回当前会话 ID 列表（升序）。
func (t *Tenant) Sessions() []ident.Session {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]ident.Session, 0, len(t.sessions))
	for id := range t.sessions {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Session 是一个已创建的会话句柄。
type Session struct {
	Tenant ident.Tenant
	ID     ident.Session
	// EntryID 会话入口短 ID：<tenantEntry>-s<seq>。
	// 它与调用方给的会话 ID 无关——调用方给的 ID 从不参与短 ID 构造，
	// 因此不存在"两个租户用同一个会话名"这种冲突。
	EntryID string
	// Path 会话入口的全路径，寻址与摘除都用它（见 Tenant.sessionsPath）。
	Path string
	// Runtime 是会话级运行时（历史窗口 + 临时空间）。
	Runtime *session.Entry

	createdAt time.Time
}

// Key 返回 "tenant/session" 稳定键。
func (s *Session) Key() string { return string(s.Tenant) + "/" + string(s.ID) }

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

// Manager 管理一个分片上的全部租户。
//
// 它是分片内部唯一的租户入口：开通、注销、建会话、取快照都经它，
// 这样"入口树里的东西"与"manager 索引里的东西"不会出现两套真相。
type Manager struct {
	d Deps

	mu      sync.RWMutex
	tenants map[ident.Tenant]*Tenant
	// byEntry 供按入口 ID 反查（自检与诊断用）。
	byEntry map[string]*Tenant
}

// New 构造 Manager。
func New(d Deps) (*Manager, error) {
	if d.App == nil || d.Loader == nil {
		return nil, errors.New("insula/tenant: App and Loader are required")
	}
	if d.Hasher == nil {
		return nil, errors.New("insula/tenant: Hasher is required")
	}
	if d.Store == nil {
		return nil, errors.New("insula/tenant: memory store is required")
	}
	// 池是必需的：模型网关的 Config 是它的瘦句柄，池缺失时入口会停在
	// FAILED。与其让每个租户各失败一次，不如在构造期就说清楚。
	if d.Pool == nil {
		return nil, errors.New("insula/tenant: model pool is required")
	}
	return &Manager{
		d:       d,
		tenants: make(map[ident.Tenant]*Tenant),
		byEntry: make(map[string]*Tenant),
	}, nil
}

// Get 按租户标识取句柄。
func (m *Manager) Get(id ident.Tenant) (*Tenant, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tenants[id]
	return t, ok
}

// List 返回本分片上的租户标识（升序）。
func (m *Manager) List() []ident.Tenant {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ident.Tenant, 0, len(m.tenants))
	for id := range m.tenants {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ResolveEntry 按租户标识返回其根入口短 ID（供寻址与自检）。
func (m *Manager) ResolveEntry(id ident.Tenant) (string, bool) {
	t, ok := m.Get(id)
	if !ok {
		return "", false
	}
	return t.EntryID, true
}

// ---------------------------------------------------------------------------
// 开通
// ---------------------------------------------------------------------------

// Provision 批量开通租户。
//
// 全程只有**一次** DoSync 与**一次** Wait：整批共享一次调度器往返与
// 一次收敛等待。逐个调用 Loader.Create 会退化成 N 次全量扫描（见包注释坑一）。
//
// 部分失败是允许的：成功的租户照常开通，失败的以 errors.Join 返回，
// 调用方可以只重试失败的那些。全批共用一次往返也是这个设计的前提——
// 否则一个坏配置会拖慢整批。
func (m *Manager) Provision(specs []Spec) error {
	if len(specs) == 0 {
		return nil
	}

	// 1) 数据面准备：构造句柄、句柄、命名空间 ID。任何 DoSync 之前完成，
	//    这样调度器上只剩纯粹的树操作。
	var (
		prepared []*Tenant
		errs     []error
	)
	for _, spec := range specs {
		spec = spec.Normalize()
		if !spec.ID.Valid() {
			errs = append(errs, errors.New("insula/tenant: empty tenant identity"))
			continue
		}
		if _, dup := m.Get(spec.ID); dup {
			errs = append(errs, fmt.Errorf("%w: %s", ErrDuplicateTenant, spec.ID))
			continue
		}
		t, err := m.prepare(spec)
		if err != nil {
			errs = append(errs, fmt.Errorf("tenant %s: %w", spec.ID, err))
			continue
		}
		prepared = append(prepared, t)
	}
	if len(prepared) == 0 {
		return errors.Join(errs...)
	}

	// 2) 一次 DoSync + 一次 Wait。闭包里只建入口，不做装配后校验
	//    （原因见 createEntries 的注释：此刻子入口还没被分组插件展开）。
	//
	//    这里**不做**创建失败的即时回滚。看似稳妥，实则有害：tree.Create
	//    的失败原因只有一个——短 ID 已被占用，而这恰恰意味着那个入口
	//    **不是我们的**（并发 Ensure 撞上、哈希碰撞、或有人手工造过同名
	//    入口）。此时"回滚"会把别人的子树摘掉：并发的两个 Ensure 里，
	//    输的那个会把赢的那个拆了，随后 Ensure 还把这个坏句柄返回给调用方。
	//    Create 本身是原子的（查重通过才插入，失败不留下半成品），
	//    因此失败路径上没有任何属于我们的东西需要清理。
	var failed []*Tenant
	ok := m.d.App.DoSync(func(*cordis.Context) {
		for _, t := range prepared {
			if err := m.createEntries(t); err != nil {
				errs = append(errs, fmt.Errorf("tenant %s: %w", t.ID, err))
				failed = append(failed, t)
			}
		}
	})
	if !ok {
		return errors.Join(append(errs, errors.New("insula/tenant: shard stopped"))...)
	}
	if !m.d.App.Wait() {
		m.d.warn("shard did not settle after provisioning %d tenants", len(prepared))
	}

	// 3) 装配后校验。Wait 之后分组插件的 Apply 已经跑完，子入口数量
	//    才是可观察的。校验失败的同样要回滚——而且回滚必须回到调度器上
	//    去做，因为它要动树的索引。
	if short := m.verifyAssembly(prepared, failed); len(short) > 0 {
		for _, t := range short {
			errs = append(errs, fmt.Errorf("%w: tenant %s", ErrShortAssembly, t.ID))
		}
		m.d.App.DoSync(func(*cordis.Context) {
			for _, t := range short {
				m.removeEntries(t)
			}
		})
		if !m.d.App.Wait() {
			m.d.warn("shard did not settle after rolling back %d short assemblies", len(short))
		}
		failed = append(failed, short...)
	}

	// 4) 只登记成功的。
	for _, t := range prepared {
		if containsTenant(failed, t) {
			continue
		}
		m.mu.Lock()
		m.tenants[t.ID] = t
		m.byEntry[t.EntryID] = t
		m.mu.Unlock()
		m.audit(audit.Event{
			At: m.d.now(), Tenant: t.ID, Action: audit.ActionTenantProvision,
			Outcome: audit.OutcomeOK, Subject: t.EntryID,
		})
	}
	return errors.Join(errs...)
}

// Ensure 开通单个租户，已存在则直接返回既有句柄。
func (m *Manager) Ensure(spec Spec) (*Tenant, error) {
	if t, ok := m.Get(spec.ID); ok {
		return t, nil
	}
	if err := m.Provision([]Spec{spec}); err != nil {
		// 并发下可能被别人抢先开通了，这时不算错误。
		if t, ok := m.Get(spec.ID); ok {
			return t, nil
		}
		return nil, err
	}
	t, ok := m.Get(spec.ID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoTenant, spec.ID)
	}
	return t, nil
}

// prepare 在数据面构造租户句柄。
func (m *Manager) prepare(spec Spec) (*Tenant, error) {
	entryID := m.d.Hasher.TenantEntry(string(spec.ID))
	if err := realm.CheckTenantEntry(entryID); err != nil {
		return nil, err
	}
	t := &Tenant{
		ID:              spec.ID,
		sim:             spec,
		EntryID:         entryID,
		SessionsEntryID: realm.SessionsGroupEntry(entryID),
		sessionsPath:    realm.SessionsGroupPath(entryID),
		Caps:            caps.NewHandle(spec.ID),
		createdAt:       m.d.now(),
		sessions:        make(map[ident.Session]*Session),
	}
	t.Gateway = m.d.Pool.Handle(spec.ID)
	// 账户与额度在装配期就建好：等第一次调用才建会把"额度没配"
	// 推迟到一个正在跑的请求里。
	m.d.Pool.Account(spec.ID)
	m.d.Pool.SetQuota(spec.ID, spec.Quota)
	t.children = m.childOptions(t)
	return t, nil
}

// createEntries 在调度器上创建租户子树。调用方必须已在 DoSync 内。
//
// 这里**只**建入口，不做装配后校验：分组插件展开 Config 的动作发生在
// doLoad 里（loader.go:427-433 的 entry.subgroup = g），而 doLoad 由
// 调度器驱动，与调用本函数的闭包共用同一个 goroutine——此刻
// e.Subgroup() 还是 nil。校验见 verifyAssembly。
//
// 失败路径上不需要回滚：EntryTree.Create 先查重再插入（loader.go:588-597），
// 返回错误时树里没有留下任何属于本次调用的东西。
func (m *Manager) createEntries(t *Tenant) error {
	_, err := m.d.Loader.Tree().Create(m.tenantOptions(t), "", -1)
	return err
}

// verifyAssembly 校验每棵租户子树的子入口数量。**必须在 app.Wait() 之后调用。**
//
// 这是把 cordis 的一处静默行为变成响亮失败：reconcile 遇到重复短 ID
// 只记日志然后跳过（loader.go:144-157），少建的那个入口不会报错，
// 只会在将来某次 Get 返回 false 时表现为"某个服务莫名其妙不可用"。
// 数量不符几乎总意味着有人造了撞车的短 ID。
//
// 返回装配不完整的租户列表，调用方负责回滚。
func (m *Manager) verifyAssembly(created, failed []*Tenant) []*Tenant {
	var short []*Tenant
	for _, t := range created {
		if containsTenant(failed, t) {
			continue
		}
		reason := ""
		e, err := m.d.Loader.Tree().Resolve(t.EntryID)
		switch {
		case err != nil:
			reason = fmt.Sprintf("entry %s not resolvable: %v", t.EntryID, err)
		case e.Subgroup() == nil:
			// 分组插件的 Apply 没跑完或失败了。这等于整棵子树没建起来。
			reason = fmt.Sprintf("entry %s carries no subgroup (group plugin did not apply)", t.EntryID)
		default:
			want := len(t.children)
			if got := len(e.Subgroup().Children()); got != want {
				reason = fmt.Sprintf("%d children, want %d (duplicate short id?)", got, want)
			}
		}
		if reason != "" {
			m.d.warn("tenant %s assembled short: %s", t.ID, reason)
			short = append(short, t)
		}
	}
	return short
}

// removeEntries 摘除租户子树。调用方必须已在 DoSync 内。
func (m *Manager) removeEntries(t *Tenant) {
	if err := m.d.Loader.Tree().Remove(t.EntryID); err != nil {
		m.d.warn("rollback tenant %s failed: %v", t.ID, err)
	}
}

// childOptions 返回租户根的静态子入口列表。
//
// 会话**不**在这里：它们是运行期动态挂到会话分组下的。这也是坑二的
// 来源——如果会话在配置列表里，reconcile 就能正确重建它们，也就
// 不需要"绝不 Update 分组"这条纪律了。
func (m *Manager) childOptions(t *Tenant) []cordis.EntryOptions {
	id := t.EntryID

	options := []cordis.EntryOptions{
		// 模型网关：Config 是**每租户一份的瘦句柄**，池本身是进程级单例。
		{ID: realm.Child(id, "models"), Name: realm.PluginModelGateway, Config: t.Gateway},
		// 记忆：同样是瘦句柄，共享存储。
		{ID: realm.Child(id, "memory"), Name: realm.PluginMemoryStore,
			Config: memory.NewHandle(m.d.Store, t.ID)},
		// 守卫：每租户一份策略指针。
		{ID: realm.Child(id, "guard"), Name: realm.PluginLoopGuard,
			Config: guard.NewPolicy(t.sim.GuardBudget, m.d.Clock)},
		// 工具：每租户一份注册表，白名单来自 Spec。
		{ID: realm.Child(id, "tools"), Name: realm.PluginToolRegistry,
			Config: m.toolConfig(t)},
	}

	// caps 服务：把能力句柄本身注册进隔离域，供会话级组件在 cordis 内部
	// 读快照（数据面则直接用 t.Caps.Take()，两边读的是同一份状态）。
	options = append(options, cordis.EntryOptions{
		ID:     realm.Child(id, "caps"),
		Name:   realm.PluginCapsProbe,
		Config: t.Caps,
	})

	// 能力探测：一个服务一个入口。它们的存活状态就是"该能力此刻可用"，
	// 由 cordis 的依赖机制维护，不需要任何轮询。
	for _, name := range realm.Services() {
		options = append(options, cordis.EntryOptions{
			ID:     realm.Child(id, "caps-"+name),
			Name:   "caps-watch-" + name,
			Config: t.Caps,
		})
	}

	// 会话分组：只是一个容器，**不带 Isolate**（见下面 sessionOptions 的说明）。
	options = append(options, cordis.EntryOptions{
		ID:     t.SessionsEntryID,
		Name:   realm.PluginSessions,
		Group:  true,
		Config: []cordis.EntryOptions{},
	})

	if t.sim.Disabled {
		// 禁用必须落到**每个子入口**上，不能写在租户根上：
		// Entry.Disabled 对分组入口恒返回 false（loader.go:207-210，
		// "禁用标志只作用于后代"），所以写在根上等于什么都没做。
		//
		// 效果链条是完整的：子入口不实例化 → 能力 Watcher 解析不到依赖
		// → 快照不 Ready → agent.Run 以 ErrCapabilityUnavailable 拒绝。
		// 这就是"暂停"应有的样子：入口在、可寻址、可原地恢复，但跑不起来。
		for i := range options {
			options[i].Disabled = true
		}
	}
	return options
}

// toolConfig 把平台模板与租户白名单合成一份配置。
func (m *Manager) toolConfig(t *Tenant) *tools.TenantConfig {
	cfg := &tools.TenantConfig{Allowlist: t.sim.ToolAllowlist}
	if m.d.Tools != nil {
		cfg.Handlers = m.d.Tools.Handlers
		cfg.Specs = m.d.Tools.Specs
		cfg.IdentityKeys = m.d.Tools.IdentityKeys
		cfg.OnDenied = m.d.Tools.OnDenied
		cfg.OnStripped = m.d.Tools.OnStripped
	}
	return cfg
}

// tenantOptions 返回租户根入口的声明。
//
// Isolate 只在这里出现一次：realm.Tenant() 是平台里唯一允许构造
// Isolate 字面量的地方（见 realm 包注释）。
//
// 这里**不设** Disabled：分组入口的禁用标志是无效的（见 childOptions
// 末尾），禁用一律由子入口承载。
func (m *Manager) tenantOptions(t *Tenant) cordis.EntryOptions {
	return cordis.EntryOptions{
		ID:      t.EntryID,
		Name:    realm.PluginTenantRoot,
		Group:   true,
		Isolate: realm.Tenant(),
		Config:  t.children,
	}
}

func containsTenant(list []*Tenant, t *Tenant) bool {
	for _, cur := range list {
		if cur == t {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 注销
// ---------------------------------------------------------------------------

// Deprovision 批量注销租户。
//
// 摘除是级联的：Entry.remove 会先注销根 Fiber，再 detachSubgroup →
// Stop 递归摘掉整棵子树（loader.go:335-341）。所以会话、会话运行时、
// 各服务入口都在一次 Remove 里被带走，不需要逐个处理。
//
// 会话运行时的 Close 由 cordis 的效果回收触发（session.Plugin 的
// Effect 逆操作），它是异步的——因此这里必须等一次 Wait，否则
// 调用方可能观察到"入口已摘除但临时空间还开着"。
func (m *Manager) Deprovision(ids []ident.Tenant) error {
	if len(ids) == 0 {
		return nil
	}

	var (
		targets []*Tenant
		errs    []error
	)
	for _, id := range ids {
		t, ok := m.Get(id)
		if !ok {
			errs = append(errs, fmt.Errorf("%w: %s", ErrNoTenant, id))
			continue
		}
		targets = append(targets, t)
	}

	ok := m.d.App.DoSync(func(*cordis.Context) {
		for _, t := range targets {
			if err := m.d.Loader.Tree().Remove(t.EntryID); err != nil {
				errs = append(errs, fmt.Errorf("tenant %s: %w", t.ID, err))
			}
		}
	})
	if !ok {
		return errors.Join(append(errs, errors.New("insula/tenant: shard stopped"))...)
	}
	if !m.d.App.Wait() {
		m.d.warn("shard did not settle after deprovisioning %d tenants", len(targets))
	}

	for _, t := range targets {
		m.mu.Lock()
		delete(m.tenants, t.ID)
		delete(m.byEntry, t.EntryID)
		m.mu.Unlock()

		// 数据面清理：额度账户与凭证缓存都按租户累积，不清理就是
		// 一条无界增长路径，还会把注销租户的凭证句柄留在内存里。
		m.d.Pool.Forget(t.ID)
		// 记忆同样要清。这里的分界线是：
		//
		//   - Spec.Disabled  = 暂停（入口还在、数据保留、可原地恢复）；
		//   - Deprovision    = 注销（入口没了、数据不留）。
		//
		// 若注销后保留数据，会出现两件都不该发生的事：一份没有入口指向的
		// 历史长期占内存；同一个租户 ID 重新开通时会读到上一轮的内容——
		// 那正是 DropSession 注释里说的"很难解释的行为"。要暂停就设 Disabled。
		if err := m.d.Store.DropTenant(t.ID); err != nil {
			m.d.warn("drop tenant memory %s failed: %v", t.ID, err)
		}
		m.audit(audit.Event{
			At: m.d.now(), Tenant: t.ID, Action: audit.ActionTenantDeprovision,
			Outcome: audit.OutcomeOK, Subject: t.EntryID,
		})
	}
	return errors.Join(errs...)
}

// ---------------------------------------------------------------------------
// 会话
// ---------------------------------------------------------------------------

// Session 取或创建会话。
//
// 短 ID 由**本分片分配的序号**派生，与调用方给的会话名无关：
// 用户传 "default"、"1"、甚至 "t-abc-s1" 都不会污染入口索引空间，
// 也就不会出现"两个租户的会话名撞车导致静默串租"。
func (m *Manager) Session(t ident.Tenant, s ident.Session) (*Session, error) {
	tn, ok := m.Get(t)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoTenant, t)
	}
	if !s.Valid() {
		return nil, errors.New("insula/tenant: empty session identity")
	}
	// 已暂停的租户不接受新会话。子入口的禁用已经让能力 Watcher 不激活，
	// 所以这里挡的是"建出一个永远不会就绪的会话"——那种会话只会让
	// 调用方困惑，而不是得到一条明确的拒绝。
	if tn.sim.Disabled {
		return nil, fmt.Errorf("%w: tenant %s", ErrTenantDisabled, t)
	}

	tn.mu.Lock()
	if sess, ok := tn.sessions[s]; ok {
		tn.mu.Unlock()
		return sess, nil
	}
	seq := tn.seq
	tn.seq++
	tn.mu.Unlock()

	entryID := realm.SessionEntry(tn.EntryID, seq)
	if err := realm.CheckChildEntry(tn.EntryID, entryID); err != nil {
		return nil, err
	}

	rt, err := session.NewEntry(t, s, tn.sim.SessionPolicy, tn.sim.MaxScratchEntries)
	if err != nil {
		return nil, err
	}
	sess := &Session{
		Tenant:  t,
		ID:      s,
		EntryID: entryID,
		// 路径一律经 realm 拼：本包里**不要**自己写 Path(a, b, c)。
		// 短 ID 与全路径是 cordis 的两套并存寻址，混用会静默失败，
		// 而失败的样子（cannot resolve）与"ID 写错了"无法分辨。
		// realm.Path 的注释把这件事定成了"由本包一处分界"。
		Path:      realm.SessionPath(tn.EntryID, seq),
		Runtime:   rt,
		createdAt: m.d.now(),
	}

	var createErr error
	ok = m.d.App.DoSync(func(*cordis.Context) {
		_, createErr = m.d.Loader.Tree().Create(m.sessionOptions(tn, sess), tn.sessionsPath, -1)
	})
	if !ok {
		return nil, errors.New("insula/tenant: shard stopped")
	}
	if createErr != nil {
		// 运行时已经建好了，但入口没建成——把它关掉，别留下一个
		// 只存在于内存里的会话。
		rt.Close()
		return nil, fmt.Errorf("session %s: %w", sess.Key(), createErr)
	}
	if !m.d.App.Wait() {
		m.d.warn("shard did not settle after creating session %s", sess.Key())
	}

	tn.mu.Lock()
	// 并发下可能已经有人建了同一个会话名：以先到的为准，把自己关掉。
	if existing, dup := tn.sessions[s]; dup {
		tn.mu.Unlock()
		m.dropSessionEntry(sess.Path)
		rt.Close()
		return existing, nil
	}
	tn.sessions[s] = sess
	tn.mu.Unlock()

	m.audit(audit.Event{
		At: m.d.now(), Tenant: t, Action: audit.ActionSessionCreate,
		Outcome: audit.OutcomeOK, Subject: string(s),
	})
	return sess, nil
}

// sessionOptions 返回会话入口的声明。
//
// **Isolate 必须声明在每个会话入口上，不能声明在会话分组上。**
// realmKey 用的是「声明 Isolate 的那个入口」的 ID（loader.go:227-241），
// 若把它写在分组入口上，分组内全部会话会共享同一个域键，
// history/scratch 就串了——而这恰恰是会话级隔离要防的事。
//
// 同时注意这里**没有**静态子入口：会话不挂子入口，会话内的资源都由
// session.Plugin 直接注册。
func (m *Manager) sessionOptions(tn *Tenant, s *Session) cordis.EntryOptions {
	return cordis.EntryOptions{
		ID:      s.EntryID,
		Name:    realm.PluginSession,
		Config:  s.Runtime,
		Isolate: realm.Session(),
	}
}

// DropSession 关闭并摘除一个会话。
func (m *Manager) DropSession(t ident.Tenant, s ident.Session) error {
	tn, ok := m.Get(t)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNoTenant, t)
	}
	tn.mu.Lock()
	sess, ok := tn.sessions[s]
	if ok {
		delete(tn.sessions, s)
	}
	tn.mu.Unlock()
	if !ok {
		return nil
	}

	var removeErr error
	ok2 := m.d.App.DoSync(func(*cordis.Context) {
		removeErr = m.d.Loader.Tree().Remove(sess.Path)
	})
	if !ok2 {
		return errors.New("insula/tenant: shard stopped")
	}
	if removeErr != nil {
		return removeErr
	}
	if !m.d.App.Wait() {
		m.d.warn("shard did not settle after dropping session %s", sess.Key())
	}

	// 会话数据也一并清掉：留下一份没有入口指向的历史只会占内存，
	// 并且下次同名会话会读到上一次的内容——那是很难解释的行为。
	if err := m.d.Store.DropSession(t, s); err != nil {
		m.d.warn("drop session data %s failed: %v", sess.Key(), err)
	}
	m.audit(audit.Event{
		At: m.d.now(), Tenant: t, Action: audit.ActionSessionClose,
		Outcome: audit.OutcomeOK, Subject: string(s),
	})
	return nil
}

// dropSessionEntry 摘除一个尚未登记进索引的会话入口（并发去重路径用）。
// path 必须是全路径（见 Session.Path）。
func (m *Manager) dropSessionEntry(path string) {
	m.d.App.DoSync(func(*cordis.Context) {
		if err := m.d.Loader.Tree().Remove(path); err != nil {
			m.d.warn("drop duplicate session entry %s failed: %v", path, err)
		}
	})
	m.d.App.Wait()
}

// ---------------------------------------------------------------------------
// 接入层用的收窄面
// ---------------------------------------------------------------------------

// Capabilities 返回某租户当前的能力快照。
//
// 存在的意义是让接入层**不必**拿到整个 Manager：那些方法（Provision /
// Deprovision）是平台管理能力，一个能被 HTTP 处理器调用的"注销租户"
// 接口是纯粹的额外攻击面。收窄成这两个方法之后，"处理器能注销租户"
// 在类型层面就不成立了。
func (m *Manager) Capabilities(t ident.Tenant) (caps.Snapshot, error) {
	tn, ok := m.Get(t)
	if !ok {
		return caps.Snapshot{}, fmt.Errorf("%w: %s", ErrNoTenant, t)
	}
	return tn.Snapshot(), nil
}

// Runtime 取或创建会话运行时（接入层视角）。
func (m *Manager) Runtime(t ident.Tenant, s ident.Session) (*session.Entry, error) {
	sess, err := m.Session(t, s)
	if err != nil {
		return nil, err
	}
	return sess.Runtime, nil
}

// ---------------------------------------------------------------------------
// 自检
// ---------------------------------------------------------------------------

// IsolationCheck 对某租户逐服务做黑盒隔离断言：把它的能力快照与
// 另一个租户比，任何一项解析到同一实例即为越界。
//
// 这是平台级安全声明唯一可编程的验证手段（isolateKey 未导出，
// 白盒断言做不到），因此它在两个地方都要用：启动自检与回归测试。
//
// 同一分片内两个租户的比较。跨分片的比较见 shard 包——那里两个租户
// 根本在不同的 App 里，结论来源不同但断言手段相同（caps.AssertIsolated）。
func (m *Manager) IsolationCheck(a, b ident.Tenant) error {
	ta, ok := m.Get(a)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNoTenant, a)
	}
	tb, ok := m.Get(b)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNoTenant, b)
	}
	if err := caps.AssertIsolated(ta.Caps, tb.Caps, realm.Services()); err != nil {
		return fmt.Errorf("tenant %s vs %s: %w", a, b, err)
	}
	return nil
}

// CheckIsolateDeclarations 扫描某租户整棵入口子树的 Isolate 声明，
// 断言没有任何一条是共享域标签或 false。
//
// 与 IsolationCheck 的分工是**互补的**，两个都要跑：
//
//   - IsolationCheck 比两个租户的**解析结果**，因此对「这个服务名没进
//     realm.Services()」是瞎的——而漏声明正是静默共享的典型形态；
//   - 本方法看**声明本身**，与服务是否注册无关，任何一条写错的值
//     都跑不掉，包括写在平台清单之外的服务名上的。
//
// 返回扫过的声明条数；调用方要把它报出去，否则「查过且干净」与
// 「什么都没查」在报告里无法分辨。
func (m *Manager) CheckIsolateDeclarations(t ident.Tenant) (int, error) {
	entryID, ok := m.ResolveEntry(t)
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrNoTenant, t)
	}
	entry, err := m.d.Loader.Tree().Resolve(entryID)
	if err != nil {
		return 0, fmt.Errorf("insula/tenant: resolve subtree of %s (%s): %w", t, entryID, err)
	}
	return realm.CheckIsolateDeclarations(entry)
}

func (m *Manager) audit(e audit.Event) {
	if m.d.Audit != nil {
		m.d.Audit.Record(e)
	}
}
