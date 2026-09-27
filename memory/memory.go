// Package memory 是会话历史与长期记忆的存储抽象与内存实现。
//
// # 这一层 cordis 帮不上忙
//
// cordis 管的是「装配与拆除」，没有任何持久化抽象。记忆的隔离必须自建，
// 而自建的关键只有一条：**不要把租户身份当成从上下文推导出来的东西，
// 要把租户标识作为必传参数**（ADR-005）。
//
// 本包在两个层面强制这条规则：
//
//  1. **类型层。** 所有 Store 方法都要求 ident.Tenant 参数，省略写不出来。
//  2. **结构层。** 租户是**一级索引维度**，不是检索之后的过滤条件：
//     向量库以 tenant → []Doc 分桶存储，检索只在桶内进行。
//     这样「全库检索后过滤」这种既慢又漏的写法根本无从表达——
//     它在 Top-K 被其他租户占满时会漏掉本租户的结果。
//
// 空租户标识被 fail-closed 拒绝：多数存储实现无法表达「无租户」，
// 静默接受只会把它变成一个隐藏的全局命名空间。
package memory

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/metaRobin/cordis"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/realm"
)

// ErrNoTenant 未提供租户标识。这是编程错误，不是运行时状况。
var ErrNoTenant = errors.New("insula/memory: tenant identity is required")

// ErrDimension 向量维度与已存数据不一致。
var ErrDimension = errors.New("insula/memory: vector dimension mismatch")

// Role 对话角色。
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
	// RoleSummary 是上下文压缩产生的摘要轮，不是原始对话。
	RoleSummary Role = "summary"
)

// Turn 一轮对话。
type Turn struct {
	Role       Role              `json:"role"`
	Content    string            `json:"content"`
	ToolName   string            `json:"tool,omitempty"`
	ToolCallID string            `json:"tool_call_id,omitempty"`
	Tokens     int               `json:"tokens,omitempty"`
	Meta       map[string]string `json:"meta,omitempty"`
	At         time.Time         `json:"at"`
}

// Doc 一条长期记忆（可带向量）。
type Doc struct {
	ID     string            `json:"id"`
	Text   string            `json:"text"`
	Vector []float32         `json:"vector,omitempty"`
	Meta   map[string]string `json:"meta,omitempty"`
	At     time.Time         `json:"at"`
}

// Query 向量检索请求。
type Query struct {
	Vector   []float32
	TopK     int
	MinScore float32
	// Filter 是可选的元数据谓词，在租户命名空间**内部**再过滤。
	Filter func(Doc) bool
}

// Hit 一条检索结果。
type Hit struct {
	Doc   Doc     `json:"doc"`
	Score float32 `json:"score"`
}

// Store 是记忆读写的唯一入口。
//
// 所有方法都要显式携带租户标识；没有任何「按会话 ID 全局查找」的接口，
// 因为那正是跨租户泄漏最常见的入口形态。
type Store interface {
	// AppendTurn 追加一轮对话，返回因超出硬上限而被丢弃的最旧条数。
	AppendTurn(t ident.Tenant, s ident.Session, turn Turn) (evicted int, err error)
	// Turns 返回某会话的全部对话轮（按时间正序）。
	Turns(t ident.Tenant, s ident.Session) ([]Turn, error)
	// ReplaceTurns 用压缩后的轮次替换会话历史（会话层压缩后调用）。
	ReplaceTurns(t ident.Tenant, s ident.Session, turns []Turn) error
	// Sessions 返回该租户的会话 ID 列表。
	Sessions(t ident.Tenant) ([]ident.Session, error)
	// DropSession 删除某会话的全部数据。
	DropSession(t ident.Tenant, s ident.Session) error
	// PutDoc 写入一条长期记忆。
	PutDoc(t ident.Tenant, d Doc) error
	// Search 在**本租户命名空间内部**做向量检索。
	Search(t ident.Tenant, q Query) ([]Hit, error)
	// Count 返回某租户的会话数、对话轮数、文档数。
	Count(t ident.Tenant) (sessions, turns, docs int)
	// DropTenant 删除某租户的全部数据（租户注销）。
	DropTenant(t ident.Tenant) error
}

type turnKey struct {
	tenant  ident.Tenant
	session ident.Session
}

// MemStore 是内存实现。它同时是隔离语义的参考实现：
// 任何替换它的持久化实现都必须保持同样的「租户是一级维度」结构。
type MemStore struct {
	mu sync.RWMutex

	turns map[turnKey][]Turn
	docs  map[ident.Tenant][]Doc

	maxTurnsPerSession int
	maxDocsPerTenant   int

	clock func() time.Time
	seq   uint64
}

// Config 调整内存存储的硬上限。
type Config struct {
	// MaxTurnsPerSession 单会话对话轮硬上限（0 取默认 512）。
	// 这是防御性底线：会话层的摘要压缩负责让历史保持在合理长度，
	// 但压缩逻辑失效时不能让分片内存被单个会话吃掉。
	MaxTurnsPerSession int
	// MaxDocsPerTenant 单租户长期记忆条数硬上限（0 取默认 8192）。
	MaxDocsPerTenant int
	// Clock 注入时钟。
	Clock func() time.Time
}

// NewMemStore 构造内存存储。
func NewMemStore(cfg Config) *MemStore {
	if cfg.MaxTurnsPerSession <= 0 {
		cfg.MaxTurnsPerSession = 512
	}
	if cfg.MaxDocsPerTenant <= 0 {
		cfg.MaxDocsPerTenant = 8192
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &MemStore{
		turns:              make(map[turnKey][]Turn),
		docs:               make(map[ident.Tenant][]Doc),
		maxTurnsPerSession: cfg.MaxTurnsPerSession,
		maxDocsPerTenant:   cfg.MaxDocsPerTenant,
		clock:              cfg.Clock,
	}
}

// AppendTurn 实现 Store。
func (m *MemStore) AppendTurn(t ident.Tenant, s ident.Session, turn Turn) (int, error) {
	if err := requireScope(t, s); err != nil {
		return 0, err
	}
	if turn.At.IsZero() {
		turn.At = m.clock()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	k := turnKey{t, s}
	list := append(m.turns[k], turn)
	evicted := 0
	if len(list) > m.maxTurnsPerSession {
		evicted = len(list) - m.maxTurnsPerSession
		list = append([]Turn(nil), list[evicted:]...)
	}
	m.turns[k] = list
	return evicted, nil
}

// Turns 实现 Store。
func (m *MemStore) Turns(t ident.Tenant, s ident.Session) ([]Turn, error) {
	if err := requireScope(t, s); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]Turn(nil), m.turns[turnKey{t, s}]...), nil
}

// ReplaceTurns 实现 Store。
func (m *MemStore) ReplaceTurns(t ident.Tenant, s ident.Session, turns []Turn) error {
	if err := requireScope(t, s); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := turnKey{t, s}
	if len(turns) == 0 {
		delete(m.turns, k)
		return nil
	}
	if len(turns) > m.maxTurnsPerSession {
		turns = turns[len(turns)-m.maxTurnsPerSession:]
	}
	m.turns[k] = append([]Turn(nil), turns...)
	return nil
}

// Sessions 实现 Store。
func (m *MemStore) Sessions(t ident.Tenant) ([]ident.Session, error) {
	if !t.Valid() {
		return nil, ErrNoTenant
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []ident.Session
	for k := range m.turns {
		if k.tenant == t {
			out = append(out, k.session)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// DropSession 实现 Store。
func (m *MemStore) DropSession(t ident.Tenant, s ident.Session) error {
	if err := requireScope(t, s); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.turns, turnKey{t, s})
	return nil
}

// PutDoc 实现 Store。
func (m *MemStore) PutDoc(t ident.Tenant, d Doc) error {
	if !t.Valid() {
		return ErrNoTenant
	}
	if d.ID == "" {
		m.mu.Lock()
		m.seq++
		d.ID = fmt.Sprintf("doc-%016x", m.seq)
		m.mu.Unlock()
	}
	if d.At.IsZero() {
		d.At = m.clock()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	bucket := m.docs[t]
	for i := range bucket {
		if bucket[i].ID == d.ID {
			bucket[i] = d
			m.docs[t] = bucket
			return nil
		}
	}
	bucket = append(bucket, d)
	if len(bucket) > m.maxDocsPerTenant {
		bucket = append([]Doc(nil), bucket[len(bucket)-m.maxDocsPerTenant:]...)
	}
	m.docs[t] = bucket
	return nil
}

// Search 实现 Store。
//
// 检索范围是 docs[t] 这一个桶——不是「全部文档再过滤」。
// 结构上就排除了跨租户漏读与跨租户命中两个方向的问题。
func (m *MemStore) Search(t ident.Tenant, q Query) ([]Hit, error) {
	if !t.Valid() {
		return nil, ErrNoTenant
	}
	if len(q.Vector) == 0 {
		return nil, nil
	}
	topK := q.TopK
	if topK <= 0 {
		topK = 10
	}

	m.mu.RLock()
	bucket := m.docs[t]
	hits := make([]Hit, 0, len(bucket))
	for _, d := range bucket {
		if len(d.Vector) == 0 {
			continue
		}
		if len(d.Vector) != len(q.Vector) {
			m.mu.RUnlock()
			return nil, fmt.Errorf("%w: doc %s has %d dims, query has %d",
				ErrDimension, d.ID, len(d.Vector), len(q.Vector))
		}
		if q.Filter != nil && !q.Filter(d) {
			continue
		}
		score := Cosine(q.Vector, d.Vector)
		if score < q.MinScore {
			continue
		}
		hits = append(hits, Hit{Doc: d, Score: score})
	}
	m.mu.RUnlock()

	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].Doc.ID < hits[j].Doc.ID // 稳定排序，结果可复现
		}
		return hits[i].Score > hits[j].Score
	})
	if len(hits) > topK {
		hits = hits[:topK]
	}
	return hits, nil
}

// Count 实现 Store。
func (m *MemStore) Count(t ident.Tenant) (sessions, turns, docs int) {
	if !t.Valid() {
		return 0, 0, 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, list := range m.turns {
		if k.tenant == t {
			sessions++
			turns += len(list)
		}
	}
	return sessions, turns, len(m.docs[t])
}

// DropTenant 实现 Store。
//
// 只删该租户的键：其他租户的数据在同一个循环里都不会被触碰。
func (m *MemStore) DropTenant(t ident.Tenant) error {
	if !t.Valid() {
		return ErrNoTenant
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.turns {
		if k.tenant == t {
			delete(m.turns, k)
		}
	}
	delete(m.docs, t)
	return nil
}

// Totals 返回全库计数，供指标与容量看板使用。
func (m *MemStore) Totals() (sessions, turns, docs int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, list := range m.turns {
		sessions++
		turns += len(list)
	}
	for _, bucket := range m.docs {
		docs += len(bucket)
	}
	return sessions, turns, docs
}

func requireScope(t ident.Tenant, s ident.Session) error {
	if !t.Valid() {
		return ErrNoTenant
	}
	if !s.Valid() {
		return errors.New("insula/memory: session identity is required")
	}
	return nil
}

// Cosine 返回两个向量的余弦相似度。任一方为零向量时返回 0。
//
// 维度不一致时返回 0（调用方应先自行校验维度，见 Search）。
func Cosine(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
}

// ---------------------------------------------------------------------------
// 租户句柄
// ---------------------------------------------------------------------------

// ErrForeignTenant 调用携带的租户标识与句柄绑定的租户不一致。
//
// 这是一个**应当不可达**的错误：句柄在租户隔离域内装配，调用方拿到它
// 时就已经在一个租户的上下文里。它存在是为了让"参数被写错/被替换"
// 这类最弱一环立刻暴露，而不是变成一次跨租户读写。
var ErrForeignTenant = errors.New("insula/memory: call carries a foreign tenant identity")

// Handle 是暴露给某个租户隔离域的瘦句柄。
//
// 它和 MemStore 的关系是「共享单例 + 每租户句柄」：
//
//   - MemStore 是**全平台共享**的（它已经在结构上按租户分桶，
//     再按租户各建一份反而把「隔离是存储的性质」变成「隔离靠 N 份实例」，
//     后者一旦有人建错一份就全丢了）；
//   - Handle 每租户一份，只含两个字段，是可注册进私有域的值。
//
// 这样既满足设计硬规则 4（跨租户资源走显式的全平台单例 + 瘦句柄），
// 又让 realm.AssertIsolated 这类黑盒断言仍能验证「两个租户拿到的
// 不是同一个实例」。
type Handle struct {
	store  Store
	tenant ident.Tenant
}

// NewHandle 构造绑定到某租户的存储句柄。
func NewHandle(s Store, t ident.Tenant) *Handle {
	return &Handle{store: s, tenant: t}
}

// Tenant 返回句柄绑定的租户。
func (h *Handle) Tenant() ident.Tenant { return h.tenant }

// Store 返回底层共享存储（供管理路径使用：注销租户、容量看板）。
func (h *Handle) Store() Store { return h.store }

func (h *Handle) check(t ident.Tenant) error {
	if !t.Valid() {
		return ErrNoTenant
	}
	if t != h.tenant {
		return fmt.Errorf("%w: handle for %s used with %s", ErrForeignTenant, h.tenant, t)
	}
	return nil
}

// AppendTurn 实现 Store。
func (h *Handle) AppendTurn(t ident.Tenant, s ident.Session, turn Turn) (int, error) {
	if err := h.check(t); err != nil {
		return 0, err
	}
	return h.store.AppendTurn(t, s, turn)
}

// Turns 实现 Store。
func (h *Handle) Turns(t ident.Tenant, s ident.Session) ([]Turn, error) {
	if err := h.check(t); err != nil {
		return nil, err
	}
	return h.store.Turns(t, s)
}

// ReplaceTurns 实现 Store。
func (h *Handle) ReplaceTurns(t ident.Tenant, s ident.Session, turns []Turn) error {
	if err := h.check(t); err != nil {
		return err
	}
	return h.store.ReplaceTurns(t, s, turns)
}

// Sessions 实现 Store。
func (h *Handle) Sessions(t ident.Tenant) ([]ident.Session, error) {
	if err := h.check(t); err != nil {
		return nil, err
	}
	return h.store.Sessions(t)
}

// DropSession 实现 Store。
func (h *Handle) DropSession(t ident.Tenant, s ident.Session) error {
	if err := h.check(t); err != nil {
		return err
	}
	return h.store.DropSession(t, s)
}

// PutDoc 实现 Store。
func (h *Handle) PutDoc(t ident.Tenant, d Doc) error {
	if err := h.check(t); err != nil {
		return err
	}
	return h.store.PutDoc(t, d)
}

// Search 实现 Store。
func (h *Handle) Search(t ident.Tenant, q Query) ([]Hit, error) {
	if err := h.check(t); err != nil {
		return nil, err
	}
	return h.store.Search(t, q)
}

// Count 实现 Store。
//
// 刻意只报**本租户**的计数：把全库计数暴露给租户上下文里的代码，
// 等于给了一个侧信道（看得到别人有多少数据）。
func (h *Handle) Count(t ident.Tenant) (sessions, turns, docs int) {
	if err := h.check(t); err != nil {
		return 0, 0, 0
	}
	return h.store.Count(t)
}

// DropTenant 实现 Store。
//
// 允许租户注销自己的数据；传别人的租户标识会被 check 拒绝。
func (h *Handle) DropTenant(t ident.Tenant) error {
	if err := h.check(t); err != nil {
		return err
	}
	return h.store.DropTenant(t)
}

// ---------------------------------------------------------------------------
// cordis 集成
// ---------------------------------------------------------------------------

// Plugin 返回记忆存储插件**定义**（一个进程内一份）。
//
// 租户身份从入口 Config 传入（`Config: memory.NewHandle(store, tenant)`）。
// 这是 loader 要求的形状：resolvePlugin(name) 是「名字 → 一个定义」的
// 静态映射，per-tenant 的插件实例挂不上入口树。
//
// 之所以还能满足「每租户一个实例」的隔离断言：这个定义注册出去的
// 是**每租户各一份的瘦句柄**，而不是共享的 MemStore 本身。
func Plugin() *cordis.Plugin {
	return &cordis.Plugin{
		Name: realm.PluginMemoryStore,
		Validate: func(cfg any) (any, error) {
			h, ok := cfg.(*Handle)
			if !ok || h == nil {
				return nil, fmt.Errorf("%w: memory-store config must be a non-nil *memory.Handle, got %T",
					ErrNoTenant, cfg)
			}
			return h, nil
		},
		Apply: func(ctx *cordis.Context, cfg any) error {
			_, err := ctx.Provide(realm.ServiceMemory, cfg.(*Handle), nil)
			return err
		},
	}
}

// 编译期断言：租户句柄必须完整实现存储接口（收窄到数据面接口的断言
// 放在 caps 包，因为那个接口定义在那里，反向引用会成环）。
var _ Store = (*Handle)(nil)
