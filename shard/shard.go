// Package shard 是分片池与租户粘性路由。
//
// # 为什么要分片（ADR-002）
//
// 单个 cordis.App 只有**一个**调度 goroutine，且 Registry / Reflect.store /
// Reflect.index / Events.hooks 都是 App 内的全局结构。全部租户挤在一个
// App 里有三个后果：
//
//   - 一个租户的插件死循环或 OOM 会拖垮全平台（爆炸半径 = 100%）；
//   - Wait() / Resolve() 的扫描量随租户数线性增长，没有上界（设计 §9）；
//   - 装配吞吐被单个 goroutine 卡死在 1×。
//
// N 个 App 把这三件事都切成 1/N。租户按稳定哈希粘在某一片上——**粘性**
// 是必须的：租户的入口子树只存在于它所在的那一片，请求路由到别的片
// 会表现为"这个租户不存在"。
//
// # 哈希取模不是一致性哈希
//
// For() 用的是 hash(tenant) % N。它稳定、无状态、零分配，但**扩缩容时
// 几乎每个租户都会换片**。这是有意的取舍：一致性哈希（虚拟节点环）能
// 把迁移量压到 1/N，代价是路由表本身变成需要持久化与同步的状态，
// 而分片迁移是 P5 的事（设计 §12）。在那之前，宁可要一个简单到
// 不会错的映射，也不要一个半实现的环——半实现的环在扩容时会静默
// 把租户分到不同片上去，而那种错误看起来就像"数据丢了"。
//
// # 共享资源怎么放
//
// 模型池、记忆存储、准入器、指标、审计都是**进程级单例**，各片注册的
// 只是指向同一份的瘦句柄（设计 §10 硬规则 4）。共享存储而隔离句柄是
// 刻意的：隔离应当是存储结构本身的属性（租户是一级索引维度），
// 而不是"N 个互不相干的实例"这种只靠约定维持的属性。
package shard

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/corecraft-io/insula/admit"
	"github.com/corecraft-io/insula/audit"
	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/gateway"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/metrics"
	"github.com/corecraft-io/insula/realm"
	"github.com/corecraft-io/insula/session"
	"github.com/corecraft-io/insula/tenant"
	"github.com/corecraft-io/insula/tools"
	"github.com/metaRobin/cordis"
)

// ErrClosed 分片池已停机。
var ErrClosed = errors.New("insula/shard: pool is closed")

// DefaultShards 是默认分片数。
//
// 4 是设计建议区间（4–8）的下沿：能拿到 1/4 的爆炸半径与 4× 装配吞吐，
// 而不会把每片的活跃租户数摊到太薄（租户少的时候，多分片只是多几份
// 互相隔离的空 App，代价是 N 倍的基础内存）。
const DefaultShards = 4

// Config 是分片池的配置。
type Config struct {
	// Shards 分片数。<=0 取 DefaultShards。
	//
	// 单租户开发或小规模部署可以显式设 1：分片是横向扩展的开关，
	// 不是前提（ADR-002 的"可逆性"一节）。
	Shards int
	// Hasher 租户哈希器。
	//
	// **必须全进程唯一**：同一个租户在不同进程上算出不同哈希，粘性路由
	// 与入口 ID 就都对不上，表现是"同一个租户时而存在时而不存在"。
	Hasher *realm.Hasher
	// Pool 进程级共享的模型池。
	Pool *gateway.Pool
	// Store 进程级共享的记忆存储。
	Store *memory.MemStore
	// Tools 平台级工具模板（处理器与描述共享，白名单由各租户给）。
	Tools *tools.TenantConfig
	// Admit 进程级唯一的准入器。
	//
	// 它出现在这里**只为生命周期清理**（注销租户时丢弃它的令牌桶），
	// 准入判断仍然发生在接入层——分片不该、也不能替请求限速，因为
	// 平台级总额度必须是全局的，每片一个等于把总量放大 N 倍。
	Admit *admit.Platform
	// Metrics / Audit 进程级共享；可为 nil。
	Metrics *metrics.Registry
	Audit   *audit.Log
	// SelfCheckPairs 单次自检在每个分片内抽检的租户对数上限。<=0 取 8。
	//
	// 见 SelfCheck 的注释：自检是**抽样哨兵**，不是穷尽证明。
	SelfCheckPairs int
	// Clock 注入时钟，nil 表示 time.Now。
	Clock func() time.Time
	// OnWarn 接收非致命告警，可为 nil。
	OnWarn func(msg string, args ...any)
}

func (c Config) normalize() Config {
	if c.Shards <= 0 {
		c.Shards = DefaultShards
	}
	if c.SelfCheckPairs <= 0 {
		c.SelfCheckPairs = 8
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	return c
}

func (c Config) warn(msg string, args ...any) {
	if c.OnWarn != nil {
		c.OnWarn(msg, args...)
	}
}

// ---------------------------------------------------------------------------
// Shard
// ---------------------------------------------------------------------------

// Shard 是一个分片：一个 cordis.App + 一个调度 goroutine + 它承载的租户。
type Shard struct {
	id      int
	app     *cordis.App
	tenants *tenant.Manager
	metrics *metrics.Registry
	cfg     Config

	closed atomic.Bool
}

// ID 返回分片序号。
func (s *Shard) ID() int { return s.id }

// App 返回该分片的 cordis App。
//
// **仅供诊断、自检与测试使用。** 数据面绝不能持有它：ADR-001 的整条
// 纪律就是从"数据面手里有 app"开始的——一旦有了，迟早会出现一次
// "顺手读一下配置"的同步回调，而那次回调会冻结整个分片。
func (s *Shard) App() *cordis.App { return s.app }

// Tenants 返回该分片的租户管理器。
//
// 只读用法（Get / List / Capabilities / Runtime / IsolationCheck）是安全的；
// 写用法（Provision / Deprovision）应当经本包的 Shard.Provision —— 那里
// 才带着"停机后拒绝服务"与指标记账。
func (s *Shard) Tenants() *tenant.Manager { return s.tenants }

// Closed 报告该分片是否已停机。
func (s *Shard) Closed() bool { return s.closed.Load() }

// Provision 在本分片开通租户。
func (s *Shard) Provision(specs []tenant.Spec) error {
	if s.closed.Load() {
		return fmt.Errorf("%w: shard %d", ErrClosed, s.id)
	}
	return s.tenants.Provision(specs)
}

// Deprovision 在本分片注销租户。
//
// 注销之后要顺手清掉进程级单例里该租户的那一份状态。这不是可选的
// 收尾工作，而是**这条路径的一半职责**：所有按租户建索引的进程级结构，
// 只加不减就是无界增长路径，而"注销"正是唯一能安全回收它们的时刻。
//
// 谁清什么，分工是明确的：
//
//   - gateway 的额度账户与凭证缓存 → tenant.Manager.Deprovision（Pool.Forget）
//   - 记忆存储 → tenant.Manager.Deprovision（Store.DropTenant）
//   - 准入令牌桶 → 这里（shard 持有那个进程级单例）
//   - 指标计数器 → 这里（同上）
//
// 后两项没做的话，`Registry.Render()` 的 label 基数会随注销过的租户数
// 无界增长——那是 Prometheus 最经典的一种故障，而且它的表现是"监控
// 先挂、业务后挂"，排查方向容易被带偏。
//
// 只在**确实摘掉了**的租户上清理：tenant.Deprovision 对不存在的租户返回
// 错误，而"现在查不到了"并不能区分"刚被摘掉"和"本来就不存在"——一个
// 拼错的租户名同样查不到。误清一个仍然活着的租户的令牌桶，等于白送它
// 一次完整的突发额度：一次由别人的失败操作触发的限额绕过。
// 所以先记下进来时确实在场的那些，事后再看它们是否真的消失了。
func (s *Shard) Deprovision(ids []ident.Tenant) error {
	if s.closed.Load() {
		return fmt.Errorf("%w: shard %d", ErrClosed, s.id)
	}
	var present []ident.Tenant
	for _, id := range ids {
		if _, ok := s.Get(id); ok {
			present = append(present, id)
		}
	}
	err := s.tenants.Deprovision(ids)
	for _, id := range present {
		if _, still := s.Get(id); still {
			continue // 摘除失败，它的状态留着
		}
		s.forgetShared(id)
	}
	return err
}

// forgetShared 丢弃某租户在进程级单例里的残留状态。
func (s *Shard) forgetShared(t ident.Tenant) {
	if s.cfg.Admit != nil {
		s.cfg.Admit.Forget(t)
	}
	if s.metrics != nil {
		s.metrics.Forget(t)
	}
}

// Get 按租户标识取句柄。
func (s *Shard) Get(t ident.Tenant) (*tenant.Tenant, bool) { return s.tenants.Get(t) }

// List 返回本分片上的租户标识（升序）。
func (s *Shard) List() []ident.Tenant { return s.tenants.List() }

// Capabilities 返回某租户的能力快照（接入层的读路径）。
func (s *Shard) Capabilities(t ident.Tenant) (caps.Snapshot, error) {
	return s.tenants.Capabilities(t)
}

// Runtime 取或创建会话运行时（接入层的写路径）。
func (s *Shard) Runtime(t ident.Tenant, sess ident.Session) (*session.Entry, error) {
	return s.tenants.Runtime(t, sess)
}

// IsolationCheck 做一次片内两租户的隔离断言。
func (s *Shard) IsolationCheck(a, b ident.Tenant) error {
	if err := s.tenants.IsolationCheck(a, b); err != nil {
		s.noteIsolation(false, a)
		return err
	}
	s.noteIsolation(true, a)
	return nil
}

func (s *Shard) noteIsolation(ok bool, t ident.Tenant) {
	if s.metrics == nil {
		return
	}
	s.metrics.Global().IsolationChecks.Add(1)
	s.metrics.Tenant(t).IsolationChecks.Add(1)
	if !ok {
		s.metrics.Global().IsolationBreaches.Add(1)
		s.metrics.Tenant(t).IsolationBreaches.Add(1)
	}
}

// Stats 返回分片状态快照。
type Stats struct {
	Shard   int
	Tenants int
	Closed  bool
	// Bound 是按租户标识排序的分片内租户清单，供诊断与基准使用。
	Bound []ident.Tenant
}

// Stats 返回该分片的状态。
func (s *Shard) Stats() Stats {
	bound := s.tenants.List()
	return Stats{Shard: s.id, Tenants: len(bound), Closed: s.closed.Load(), Bound: bound}
}

// Close 关闭本分片：级联摘下全部租户子树并冻结 App。
//
// App.Close 是整体冻结（app.go:197-207，先 seal 调度器再逐 fiber 回收），
// 因此关闭顺序必须是"先摘租户、再关 App"——反过来的话，摘租户的
// DoSync 会因为调度器已 seal 而失败，租户的效果回收就没人驱动了。
func (s *Shard) Close() {
	if s.closed.Swap(true) {
		return
	}
	if ids := s.tenants.List(); len(ids) > 0 {
		if err := s.tenants.Deprovision(ids); err != nil {
			s.cfg.warn("shard %d: deprovision on close: %v", s.id, err)
		}
		// 停机路径也清一遍共享状态。进程退出时它其实无关紧要，但
		// "只在停机时省掉"会让这条清理规则变成两条——而两条规则里
		// 总有一条会在重构中被改错，且改错的正是被调用得少的那条。
		for _, id := range ids {
			s.forgetShared(id)
		}
	}
	s.app.Close()
}

// selfCheck 抽样做片内隔离断言，返回实际比较过的对数。
func (s *Shard) selfCheck(pairs int) (checked int, err error) {
	ids := s.tenants.List()
	n := len(ids)
	if n < 2 {
		return 0, nil
	}
	for i := 0; i < n && checked < pairs; i++ {
		for j := i + 1; j < n && checked < pairs; j++ {
			checked++
			if cerr := s.IsolationCheck(ids[i], ids[j]); cerr != nil {
				return checked, cerr
			}
		}
	}
	return checked, nil
}

// declarationCheck 扫本片全部租户子树里的 Isolate 声明。
//
// 成本是 O(子树)，与租户数成正比、与租户对**无关**——因此它不参与
// SelfCheckPairs 的抽检预算：一个探针在一个租户上就够，不需要配对。
// 计数失败路径同样记一次隔离破坏：一条共享域声明就是一次隔离失效，
// 不该只出现在日志里而为指标所无。
func (s *Shard) declarationCheck() (int, error) {
	total := 0
	for _, id := range s.tenants.List() {
		n, err := s.tenants.CheckIsolateDeclarations(id)
		total += n
		if err != nil {
			s.noteIsolation(false, id)
			return total, fmt.Errorf("shard %d: %w", s.id, err)
		}
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

// Pool 是分片池。
//
// 它同时提供**粘性路由**（For）与**跨分片自检**（SelfCheck）。这两件事
// 放在一起是因为它们用的是同一个哈希：一个租户总是路由到同一片，
// 因此"同一租户的两次请求落在不同片"在类型与实现上都不成立。
type Pool struct {
	cfg    Config
	shards []*Shard

	mu     sync.RWMutex
	closed bool
	// checked 是累计比较过的租户对数，供自检报告"覆盖了多少"。
	checked atomic.Int64
}

// New 构造分片池并启动全部分片。
func New(cfg Config) (*Pool, error) {
	cfg = cfg.normalize()
	if cfg.Hasher == nil {
		return nil, errors.New("insula/shard: Hasher is required")
	}
	// 池与存储是必需的：它们缺席时每个租户的装配都会失败，
	// 与其让 N 个租户各失败一次，不如在构造期就说清楚。
	if cfg.Pool == nil {
		return nil, errors.New("insula/shard: model pool is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("insula/shard: memory store is required")
	}

	p := &Pool{cfg: cfg, shards: make([]*Shard, 0, cfg.Shards)}
	for i := 0; i < cfg.Shards; i++ {
		s, err := newShard(i, cfg)
		if err != nil {
			// 构造失败时把已经起来的片关干净，不要留下半个池：
			// 半启动的池会表现为"某些租户能开通、某些不能"。
			for _, prev := range p.shards {
				prev.Close()
			}
			return nil, fmt.Errorf("insula/shard: shard %d: %w", i, err)
		}
		p.shards = append(p.shards, s)
	}
	return p, nil
}

func newShard(id int, cfg Config) (*Shard, error) {
	app := cordis.New()
	man, err := tenant.New(tenant.Deps{
		App:    app,
		Loader: tenant.NewLoader(app),
		Hasher: cfg.Hasher,
		Pool:   cfg.Pool,
		Store:  cfg.Store,
		Tools:  cfg.Tools,
		Clock:  cfg.Clock,
		Audit:  cfg.Audit,
		OnWarn: cfg.warn,
	})
	if err != nil {
		app.Close()
		return nil, err
	}
	return &Shard{
		id:      id,
		app:     app,
		tenants: man,
		metrics: cfg.Metrics,
		cfg:     cfg,
	}, nil
}

// Shards 返回全部分片（按 id 升序）。
func (p *Pool) Shards() []*Shard {
	out := make([]*Shard, len(p.shards))
	copy(out, p.shards)
	return out
}

// Len 返回分片数。
func (p *Pool) Len() int { return len(p.shards) }

// For 按租户标识返回承载它的分片（粘性路由）。
//
// 哈希用 FNV-1a：确定性、零分配、与进程无关。**不要**换成
// map 迭代或随机数——那会让粘性失效，而失效的表现是"租户时而存在
// 时而不存在"，非常难归因。
func (p *Pool) For(t ident.Tenant) (*Shard, error) {
	if !t.Valid() {
		return nil, errors.New("insula/shard: empty tenant identity")
	}
	if p.isClosed() {
		return nil, ErrClosed
	}
	return p.shards[hashTenant(t)%uint64(len(p.shards))], nil
}

// Find 在池中定位一个已开通的租户：返回它的分片与句柄。
//
// 与 For 的区别是它**不路由**，而是真的去找。管理面（注销、查询、
// 迁移）需要这个语义：调用方可能根本不知道租户当前落在哪一片，
// 而"按哈希算一片然后假设它在那儿"会在分片数变更后静默出错。
func (p *Pool) Find(t ident.Tenant) (*Shard, *tenant.Tenant, error) {
	if !t.Valid() {
		return nil, nil, errors.New("insula/shard: empty tenant identity")
	}
	if p.isClosed() {
		return nil, nil, ErrClosed
	}
	// 先按哈希直查（绝大多数情况命中，零额外开销）。
	if s, err := p.For(t); err == nil {
		if tn, ok := s.Get(t); ok {
			return s, tn, nil
		}
	}
	// 再全池找。这是分片数变更、或调用方手工建过入口时的兜底路径。
	for _, s := range p.shards {
		if tn, ok := s.Get(t); ok {
			return s, tn, nil
		}
	}
	return nil, nil, fmt.Errorf("%w: %s", tenant.ErrNoTenant, t)
}

// Provision 按粘性路由把租户批量分派到各分片。
//
// 分派发生在调用方这一侧、DoSync 之外：**每片仍然只有一次**
// DoSync 与一次 Wait，而不是每租户一次（设计 §9 的第 1 处退化）。
func (p *Pool) Provision(specs []tenant.Spec) error {
	if p.isClosed() {
		return ErrClosed
	}
	var errs []error
	seen := make(map[*Shard][]tenant.Spec)
	order := make([]*Shard, 0, len(p.shards))
	for _, spec := range specs {
		s, err := p.For(spec.ID)
		if err != nil {
			errs = append(errs, fmt.Errorf("tenant %s: %w", spec.ID, err))
			continue
		}
		if _, ok := seen[s]; !ok {
			order = append(order, s)
		}
		seen[s] = append(seen[s], spec)
	}
	for _, s := range order {
		if err := s.Provision(seen[s]); err != nil {
			errs = append(errs, fmt.Errorf("shard %d: %w", s.ID(), err))
		}
	}
	return errors.Join(errs...)
}

// Deprovision 在池中定位并注销租户（不依赖哈希）。
//
// 用 Find 而不是 For，理由见 Find 的注释：注销是一次**破坏性**操作，
// 它必须作用在租户真正所在的分片上，而不能作用在哈希算出来的那一片。
func (p *Pool) Deprovision(ids []ident.Tenant) error {
	if p.isClosed() {
		return ErrClosed
	}
	var errs []error
	byShard := make(map[*Shard][]ident.Tenant)
	order := make([]*Shard, 0, len(p.shards))
	for _, id := range ids {
		s, _, err := p.Find(id)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if _, ok := byShard[s]; !ok {
			order = append(order, s)
		}
		byShard[s] = append(byShard[s], id)
	}
	for _, s := range order {
		if err := s.Deprovision(byShard[s]); err != nil {
			errs = append(errs, fmt.Errorf("shard %d: %w", s.ID(), err))
		}
	}
	return errors.Join(errs...)
}

// Capabilities 按粘性路由返回租户的能力快照（接入层的读路径）。
//
// 数据面按**哈希**路由，而不是 Find。哈希就是粘性的定义：一个不在哈希
// 指向的分片上的租户，对请求路径而言等于不存在。那正是正确的失败形态
// ——响亮地报 ErrNoTenant，而不是"顺手在别的片上把它找出来"。后者会把
// 一次错误的手工装配永久掩盖起来，直到某次重启把路由改到另一片，
// 问题才以"数据丢失"的样子爆出来。
func (p *Pool) Capabilities(t ident.Tenant) (caps.Snapshot, error) {
	s, err := p.For(t)
	if err != nil {
		return caps.Snapshot{}, err
	}
	return s.Capabilities(t)
}

// Runtime 按粘性路由取或创建会话运行时（接入层的写路径）。
func (p *Pool) Runtime(t ident.Tenant, sess ident.Session) (*session.Entry, error) {
	s, err := p.For(t)
	if err != nil {
		return nil, err
	}
	return s.Runtime(t, sess)
}

// SelfCheckReport 是一次自检的结果。
type SelfCheckReport struct {
	Shards int
	// Tenants 是参与检查的租户总数。
	Tenants int
	// PairsChecked 是实际比较过的租户对数。
	PairsChecked int
	// Exhaustive 为 true 表示抽检预算足以覆盖全部分片内的所有租户对，
	// 因此这次自检是穷尽的而不是抽样的。
	Exhaustive bool
	// Declarations 是实际扫过的 Isolate 声明条数（逐租户子树）。
	//
	// 与 PairsChecked 同一条纪律：报出来是为了让「查过且干净」可被
	// 监控分辨。一个恒为 0 的 Declarations 配着「自检通过」的结论，
	// 说明声明层根本没跑，而不是说明声明层是干净的。
	Declarations int
}

// SelfCheck 做一次隔离自检。
//
// # 它是抽样哨兵，不是穷尽证明
//
// 片内穷尽比较是 O(N²·S)：2000 个租户就是 200 万对，乘上服务数就是
// 两千万次 Get。启动自检不能这么做，而"每隔一会儿跑一次"的更慢版本
// 也没必要——**平台级隔离不靠自检保证，靠构造保证**：
//
//   - 租户入口子树在私有域（realm.Tenant()，唯一一处 Isolate 声明）；
//   - 跨分片根本是不同的 App，不同 Reflect 实例。
//
// 自检要抓的是**构造被破坏**（有人手工造了入口、漏了 Isolate、
// 或者在插件里用了共享域标签）这一类回归。这类破坏一旦发生，
// 抽检几乎必然命中——因为它影响的是某一个服务的域键，而不是
// 某一对租户的偶然组合。
//
// 想让覆盖变穷尽就把 Config.SelfCheckPairs 调大（或设一个大于
// N(N-1)/2 的值），报告里的 Exhaustive 会告诉你这次到底覆没覆盖全。
//
// # 两层，看的东西不同
//
//   - **两两比对**（`PairsChecked`）：两个租户解析到的**实例**是不是同一个。
//     受抽检预算 `SelfCheckPairs` 限制。
//   - **声明扫描**（`Declarations`）：每条 `Isolate` 声明**写的是什么值**。
//     逐租户 O(子树)，不参与抽检预算，因此永远是穷尽的。
//
// 两者互补而非重复：前者对「服务名没进 realm.Services()」是瞎的，
// 而那正是漏声明导致静默共享的形态。见 SAFETY.md R3 与 ADR-0003。
func (p *Pool) SelfCheck() (SelfCheckReport, error) {
	rep := SelfCheckReport{Shards: len(p.shards), Exhaustive: true}
	if p.isClosed() {
		return rep, ErrClosed
	}
	totalPairs := 0
	for _, s := range p.shards {
		n := len(s.List())
		rep.Tenants += n
		if n >= 2 {
			totalPairs += n * (n - 1) / 2
		}
	}
	for _, s := range p.shards {
		n, err := s.selfCheck(p.cfg.SelfCheckPairs)
		rep.PairsChecked += n
		if err != nil {
			return rep, err
		}
	}
	if rep.PairsChecked < totalPairs {
		rep.Exhaustive = false
	}
	p.checked.Add(int64(rep.PairsChecked))

	// 第二层：声明扫描。它与上面的两两比对看的是**不同的东西**，
	// 因此不是重复劳动——
	//
	//   - 两两比对比的是「两个租户解析到的实例是否相同」；
	//   - 声明扫描读的是每条 Isolate 声明**写的是什么值**。
	//
	// 前者对「服务名没进 realm.Services()」是瞎的，而那正是漏声明
	// 导致静默共享的形态；后者与服务是否注册无关。SAFETY.md 的 R3
	// 靠这一层落地。
	for _, s := range p.shards {
		n, err := s.declarationCheck()
		rep.Declarations += n
		if err != nil {
			return rep, err
		}
	}

	// 跨分片：两个租户落在不同的 App 里，隔离由"不同的 Reflect 实例"
	// 保证，而不是靠 isolateKey。这里各取一个做哨兵比对——它对
	// 片内破坏不敏感，但能抓住"两片意外共用了同一个 App"这类装配错误。
	if err := p.crossShardCheck(); err != nil {
		return rep, err
	}
	return rep, nil
}

func (p *Pool) crossShardCheck() error {
	if len(p.shards) < 2 {
		return nil
	}
	var left, right *caps.Handle
	for _, s := range p.shards {
		ids := s.List()
		if len(ids) == 0 {
			continue
		}
		tn, ok := s.Get(ids[0])
		if !ok {
			continue
		}
		if left == nil {
			left = tn.Caps
			continue
		}
		right = tn.Caps
		break
	}
	if left == nil || right == nil {
		return nil
	}
	if err := caps.AssertIsolated(left, right, realm.Services()); err != nil {
		return fmt.Errorf("insula/shard: cross-shard isolation breach: %w", err)
	}
	return nil
}

// Checked 返回累计比较过的租户对数（指标用）。
func (p *Pool) Checked() int64 { return p.checked.Load() }

// Stats 返回全池状态。
func (p *Pool) Stats() []Stats {
	out := make([]Stats, 0, len(p.shards))
	for _, s := range p.shards {
		out = append(out, s.Stats())
	}
	return out
}

// Tenants 返回全池的租户标识（升序）。
//
// 它是 O(总数) 的运维视图，不要在请求路径上调用。
func (p *Pool) Tenants() []ident.Tenant {
	var out []ident.Tenant
	for _, s := range p.shards {
		out = append(out, s.List()...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Close 停机：逐片关闭，随后拒绝一切路由与开通。
//
// 顺序很重要：先把 closed 置位，让**新的**请求立刻拿到 ErrClosed，
// 再逐片拆除。反过来的话，拆除期间的请求会拿到一个已经拆了一半的分片。
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	shards := p.shards
	p.mu.Unlock()

	for _, s := range shards {
		s.Close()
	}
}

func (p *Pool) isClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// hashTenant 是租户 → 分片的稳定哈希。
//
// FNV-1a 而不是 maphash：后者每个进程的种子不同，进程重启后
// 同一个租户会落到另一片上——而分片归属必须是跨进程稳定的，
// 否则"多副本"这一形态根本不成立。
func hashTenant(t ident.Tenant) uint64 {
	h := fnv.New64a()
	h.Write([]byte(t))
	return h.Sum64()
}
