package shard

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	cordis "github.com/metaRobin/cordis"
	"github.com/metaRobin/insula/admit"
	"github.com/metaRobin/insula/audit"
	"github.com/metaRobin/insula/caps"
	"github.com/metaRobin/insula/creds"
	"github.com/metaRobin/insula/gateway"
	"github.com/metaRobin/insula/guard"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/memory"
	"github.com/metaRobin/insula/metrics"
	"github.com/metaRobin/insula/realm"
	"github.com/metaRobin/insula/session"
	"github.com/metaRobin/insula/tenant"
	"github.com/metaRobin/insula/tools"
)

// ---------------------------------------------------------------------------
// 假件
// ---------------------------------------------------------------------------

type fakeUpstream struct {
	mu    sync.Mutex
	seen  []ident.Tenant
	reply string
}

func (u *fakeUpstream) Complete(_ context.Context, t ident.Tenant, _ caps.Request,
	_ *creds.Handle) (caps.Response, error) {

	u.mu.Lock()
	defer u.mu.Unlock()
	u.seen = append(u.seen, t)
	reply := u.reply
	if reply == "" {
		reply = "ok"
	}
	return caps.Response{Message: caps.Message{Role: "assistant", Content: reply}}, nil
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

func platformTools() *tools.TenantConfig {
	return &tools.TenantConfig{
		Handlers: map[string]tools.Handler{
			"echo": tools.HandlerFunc(func(_ context.Context, inv caps.Invocation) (caps.ToolResult, error) {
				return caps.ToolResult{Content: "echo:" + inv.Name}, nil
			}),
		},
		Specs: map[string]caps.ToolSpec{
			"echo": {Name: "echo", Description: "回显"},
		},
	}
}

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

type harness struct {
	t       *testing.T
	pool    *Pool
	store   *memory.MemStore
	metrics *metrics.Registry
	warns   *warnLog
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	h := &harness{
		t:       t,
		store:   memory.NewMemStore(memory.Config{}),
		metrics: metrics.New(),
		warns:   &warnLog{},
	}
	if cfg.Hasher == nil {
		cfg.Hasher = realm.NewHasher(nil)
	}
	if cfg.Pool == nil {
		cfg.Pool = gateway.New(gateway.Config{
			Upstream: &fakeUpstream{},
			Creds:    creds.NewStaticProvider(),
			Quota:    gateway.Quota{MaxTokens: 100000, MaxCalls: 1000, Window: time.Minute},
		})
	}
	if cfg.Store == nil {
		cfg.Store = h.store
	}
	if cfg.Tools == nil {
		cfg.Tools = platformTools()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = h.metrics
	}
	if cfg.OnWarn == nil {
		cfg.OnWarn = h.warns.add
	}
	pool, err := New(cfg)
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	h.pool = pool
	t.Cleanup(pool.Close)
	return h
}

func spec(id string) tenant.Spec {
	return tenant.Spec{
		ID:            ident.Tenant(id),
		Quota:         gateway.Quota{MaxTokens: 1000, MaxCalls: 100, Window: time.Minute},
		ToolAllowlist: []string{"echo"},
		GuardBudget:   guard.Budget{MaxSteps: 4},
		SessionPolicy: session.Policy{SoftLimit: 8, KeepRecent: 3},
	}
}

func specs(ids ...string) []tenant.Spec {
	out := make([]tenant.Spec, 0, len(ids))
	for _, id := range ids {
		out = append(out, spec(id))
	}
	return out
}

// idsFor 造一批确定性命名的租户，并按哈希把它们分派到各片。
// 返回的是"预期分片归属"，测试用它来验证 Pool 的分派与 For 一致。
func idsFor(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("t-%03d", i))
	}
	return out
}

// coverShards 造一批租户，保证每一片至少 minPerShard 个。
//
// 为什么要凑够每片的数量而不是"覆盖到所有片就停"：片内隔离自检只对
// 同一片内的租户对生效，每片只有一个租户时自检会一对都比不了，
// 测试会"通过"却什么都没验证。
func coverShards(t *testing.T, p *Pool, minPerShard int) map[int][]string {
	t.Helper()
	want := make(map[int][]string)
	for _, id := range idsFor(400) {
		s, err := p.For(ident.Tenant(id))
		if err != nil {
			t.Fatalf("For(%s): %v", id, err)
		}
		want[s.ID()] = append(want[s.ID()], id)
		if len(want) < p.Len() {
			continue
		}
		enough := true
		for _, ids := range want {
			if len(ids) < minPerShard {
				enough = false
				break
			}
		}
		if enough {
			return want
		}
	}
	t.Fatalf("400 个候选租户没能让每一片都凑够 %d 个：%v", minPerShard, want)
	return nil
}

func (h *harness) mustProvision(ids ...string) {
	h.t.Helper()
	if err := h.pool.Provision(specs(ids...)); err != nil {
		h.t.Fatalf("Provision(%v): %v", ids, err)
	}
}

// ---------------------------------------------------------------------------
// 构造契约
// ---------------------------------------------------------------------------

func TestNewRequiresSharedInfrastructure(t *testing.T) {
	base := func() Config {
		return Config{
			Shards: 1,
			Hasher: realm.NewHasher(nil),
			Pool: gateway.New(gateway.Config{
				Upstream: &fakeUpstream{},
				Creds:    creds.NewStaticProvider(),
			}),
			Store: memory.NewMemStore(memory.Config{}),
		}
	}

	c := base()
	c.Hasher = nil
	if _, err := New(c); err == nil {
		t.Error("缺 Hasher 必须构造失败：哈希是粘性路由与入口 ID 的共同来源")
	}

	c = base()
	c.Pool = nil
	if _, err := New(c); err == nil {
		t.Error("缺模型池必须构造失败：与其让 N 个租户各失败一次，不如构造期就说清")
	}

	c = base()
	c.Store = nil
	if _, err := New(c); err == nil {
		t.Error("缺记忆存储必须构造失败")
	}

	p, err := New(base())
	if err != nil {
		t.Fatalf("完整配置应当构造成功：%v", err)
	}
	defer p.Close()
	if p.Len() != 1 {
		t.Errorf("Len = %d，期望 1", p.Len())
	}
	if len(p.Shards()) != 1 {
		t.Errorf("Shards() 长度 = %d", len(p.Shards()))
	}
	if p.Shards()[0].ID() != 0 {
		t.Errorf("分片序号 = %d，期望 0", p.Shards()[0].ID())
	}
	if p.Shards()[0].App() == nil {
		t.Error("每片必须有自己的 cordis.App：那是爆炸半径隔离的物理基础")
	}
	if p.Shards()[0].Closed() {
		t.Error("刚构造的分片不该是关闭的")
	}
}

func TestConfigNormalize(t *testing.T) {
	c := Config{}.normalize()
	if c.Shards != DefaultShards {
		t.Errorf("Shards 默认 = %d，期望 %d", c.Shards, DefaultShards)
	}
	if c.SelfCheckPairs != 8 {
		t.Errorf("SelfCheckPairs 默认 = %d，期望 8", c.SelfCheckPairs)
	}
	if c.Clock == nil {
		t.Error("Clock 必须兜底成 time.Now")
	}
	// 分片数是可逆开关：显式 1 是合法配置，不能被"补"成默认值。
	if got := (Config{Shards: 1}).normalize().Shards; got != 1 {
		t.Errorf("显式 Shards=1 被改成了 %d：分片是横向扩展的开关，不是前提", got)
	}
}

func TestEachShardHasItsOwnApp(t *testing.T) {
	h := newHarness(t, Config{Shards: 4})
	shards := h.pool.Shards()
	if len(shards) != 4 {
		t.Fatalf("分片数 = %d", len(shards))
	}
	seen := make(map[*cordis.App]int)
	for _, s := range shards {
		seen[s.App()]++
	}
	if len(seen) != 4 {
		t.Fatalf("4 个分片共用 %d 个 App：跨分片隔离正是靠「不同的 Reflect 实例」成立的", len(seen))
	}
	// 关闭一片不能连带关闭另一片。
	shards[0].Close()
	if !shards[0].Closed() {
		t.Error("分片 0 未关闭")
	}
	if shards[1].Closed() {
		t.Error("关闭分片 0 影响了分片 1：爆炸半径应当是 1/N")
	}
	if h.pool.Shards()[1].App() == nil {
		t.Error("分片 1 的 App 被连带清掉了")
	}
}

// ---------------------------------------------------------------------------
// 粘性路由
// ---------------------------------------------------------------------------

// 路由必须跨进程稳定。这个测试钉的是**算法本身**：换成 maphash 或
// fnv32 都会让它变红，而那种替换的后果是多副本部署下同一个租户在不同
// 副本上落到不同分片——表现为"这个租户时而存在时而不存在"。
func TestHashIsFNV1aAndStable(t *testing.T) {
	const (
		goldenA = uint64(14046587775414411003) // FNV-1a 64a("tenant-a")
		golden1 = uint64(6207940615468131841)  // FNV-1a 64a("t-1")
	)
	if got := hashTenant("tenant-a"); got != goldenA {
		t.Errorf("hashTenant(tenant-a) = %d，期望 %d。\n"+
			"改这个值意味着路由算法变了：跨进程粘性会失效，多副本部署不再成立。", got, goldenA)
	}
	if got := hashTenant("t-1"); got != golden1 {
		t.Errorf("hashTenant(t-1) = %d，期望 %d", got, golden1)
	}
	// 同一输入必须永远同一输出。
	for i := 0; i < 100; i++ {
		if hashTenant("tenant-a") != goldenA {
			t.Fatal("hashTenant 不是确定性的：粘性路由会随机失效")
		}
	}
}

func TestForIsSticky(t *testing.T) {
	h := newHarness(t, Config{Shards: 4})
	for _, id := range idsFor(40) {
		tn := ident.Tenant(id)
		first, err := h.pool.For(tn)
		if err != nil {
			t.Fatalf("For(%s): %v", id, err)
		}
		for i := 0; i < 20; i++ {
			again, err := h.pool.For(tn)
			if err != nil {
				t.Fatalf("For(%s): %v", id, err)
			}
			if again != first {
				t.Fatalf("租户 %s 的路由漂移了：第一次 %d，第 %d 次 %d",
					id, first.ID(), i+2, again.ID())
			}
		}
	}
}

func TestForRejectsEmptyTenantAndClosedPool(t *testing.T) {
	h := newHarness(t, Config{Shards: 2})
	if _, err := h.pool.For(""); err == nil {
		t.Error("空租户标识必须被拒绝：它会哈希到某一固定分片上，变成一个可预测的默认桶")
	}
	if _, _, err := h.pool.Find(""); err == nil {
		t.Error("Find 也必须拒绝空租户标识")
	}

	h.pool.Close()
	if _, err := h.pool.For("tenant-a"); !errors.Is(err, ErrClosed) {
		t.Errorf("停机后 For 的错误 = %v，期望 ErrClosed", err)
	}
	if _, _, err := h.pool.Find("tenant-a"); !errors.Is(err, ErrClosed) {
		t.Errorf("停机后 Find 的错误 = %v，期望 ErrClosed", err)
	}
	if err := h.pool.Provision(specs("tenant-a")); !errors.Is(err, ErrClosed) {
		t.Errorf("停机后 Provision 的错误 = %v，期望 ErrClosed", err)
	}
	if err := h.pool.Deprovision([]ident.Tenant{"tenant-a"}); !errors.Is(err, ErrClosed) {
		t.Errorf("停机后 Deprovision 的错误 = %v，期望 ErrClosed", err)
	}
	if _, err := h.pool.SelfCheck(); !errors.Is(err, ErrClosed) {
		t.Errorf("停机后 SelfCheck 的错误 = %v，期望 ErrClosed", err)
	}
}

// 分派必须与路由用同一个哈希：Pool 说租户在哪一片，For 就必须指那一片。
func TestProvisionLandsOnTheHashedShard(t *testing.T) {
	h := newHarness(t, Config{Shards: 4})
	ids := idsFor(40)
	h.mustProvision(ids...)

	// 全池清单 = 输入集合。
	got := h.pool.Tenants()
	if len(got) != len(ids) {
		t.Fatalf("全池租户数 = %d，期望 %d", len(got), len(ids))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("Tenants() 未升序：%v", got)
		}
	}

	// 每个租户都在哈希指向的那一片上，而且只在那一片上。
	perShard := 0
	for _, id := range ids {
		s, err := h.pool.For(ident.Tenant(id))
		if err != nil {
			t.Fatalf("For(%s): %v", id, err)
		}
		if _, ok := s.Get(ident.Tenant(id)); !ok {
			t.Fatalf("租户 %s 不在哈希指向的分片 %d 上", id, s.ID())
		}
		for _, other := range h.pool.Shards() {
			if other == s {
				continue
			}
			if _, ok := other.Get(ident.Tenant(id)); ok {
				t.Fatalf("租户 %s 同时出现在分片 %d 和 %d 上", id, s.ID(), other.ID())
			}
		}
	}
	for _, s := range h.pool.Shards() {
		perShard += s.Stats().Tenants
		for _, id := range s.Stats().Bound {
			// 分片清单必须与哈希一致。
			hs, err := h.pool.For(id)
			if err != nil {
				t.Fatalf("For(%s): %v", id, err)
			}
			if hs.ID() != s.ID() {
				t.Fatalf("租户 %s 躺在分片 %d，哈希却指向 %d", id, s.ID(), hs.ID())
			}
		}
	}
	if perShard != len(ids) {
		t.Errorf("各分片租户数之和 = %d，期望 %d", perShard, len(ids))
	}
}

// Pool.Provision 不是幂等的：重复开通会被 tenant 层报出来，而不是静默成功。
//
// 这个断言是刻意的。批量的 Provision 报重是**要看到的信号**——它通常
// 意味着调用方把两套不同的 spec 当成了同一批。需要"确保存在"的语义
// 应当走 tenant.Manager.Ensure。这里钉住的是"不许悄悄把第二份 spec
// 覆盖掉第一份"：租户的配额、工具白名单、预算都来自 spec，静默覆盖
// 等于让一次配置变更在没人知道的情况下生效。
func TestProvisionAgainReportsDuplicatesWithoutDuplicating(t *testing.T) {
	h := newHarness(t, Config{Shards: 3})
	h.mustProvision("tenant-a", "tenant-b")

	err := h.pool.Provision(specs("tenant-a", "tenant-b"))
	if err == nil {
		t.Fatal("重复开通必须报错：静默成功会让调用方以为新 spec 生效了")
	}
	if !strings.Contains(err.Error(), "tenant-a") || !strings.Contains(err.Error(), "tenant-b") {
		t.Errorf("错误里应点名重复的租户：%v", err)
	}
	if n := len(h.pool.Tenants()); n != 2 {
		t.Fatalf("重复开通后租户数 = %d，期望 2", n)
	}
}

// 个别租户失败不能拖垮整批。
func TestProvisionKeepsGoingOnBadSpec(t *testing.T) {
	h := newHarness(t, Config{Shards: 2})
	err := h.pool.Provision(specs("tenant-a", "", "tenant-b"))
	if err == nil {
		t.Fatal("空租户标识必须产生错误")
	}
	if !strings.Contains(err.Error(), "tenant") {
		t.Errorf("错误里应指明是哪个 spec 出的问题：%v", err)
	}
	// 好的那两个必须已经开通。
	got := h.pool.Tenants()
	if len(got) != 2 {
		t.Fatalf("租户数 = %d，期望 2（%v）", len(got), got)
	}
}

// ---------------------------------------------------------------------------
// Find 与 For 的分工
// ---------------------------------------------------------------------------

func TestFindFindsProvisionedTenant(t *testing.T) {
	h := newHarness(t, Config{Shards: 4})
	h.mustProvision(idsFor(20)...)

	for _, id := range idsFor(20) {
		s, tn, err := h.pool.Find(ident.Tenant(id))
		if err != nil {
			t.Fatalf("Find(%s): %v", id, err)
		}
		if tn.ID != ident.Tenant(id) {
			t.Fatalf("Find(%s) 返回了别的租户 %s", id, tn.ID)
		}
		hashed, _ := h.pool.For(ident.Tenant(id))
		if s.ID() != hashed.ID() {
			t.Fatalf("Find 与 For 不一致：%d vs %d", s.ID(), hashed.ID())
		}
	}

	if _, _, err := h.pool.Find("never-provisioned"); !errors.Is(err, tenant.ErrNoTenant) {
		t.Errorf("未知租户的错误 = %v，期望 tenant.ErrNoTenant", err)
	}
}

// Find 存在的唯一理由：租户可能不在哈希指的那一片上（分片数变更、
// 或有人手工建过入口）。For 会给出一个看起来正确却错误的答案，
// Find 必须找到真的那一片。
func TestFindFindsTenantSittingOffHash(t *testing.T) {
	h := newHarness(t, Config{Shards: 3})

	// 把租户直接开通到"哈希指向的另一片"上，模拟分片数变更后的残留。
	const id = "tenant-stray"
	hashed, err := h.pool.For(id)
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	var other *Shard
	for _, s := range h.pool.Shards() {
		if s != hashed {
			other = s
			break
		}
	}
	if other == nil {
		t.Fatal("需要至少两片才能构造这个场景")
	}
	if err := other.Provision(specs(id)); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	// For 会指向错误的一片——这正是它不能用于管理面操作的原因。
	wrong, _ := h.pool.For(id)
	if _, ok := wrong.Get(id); ok {
		t.Fatal("前置条件不成立：租户意外落在了哈希指向的分片上")
	}

	// 但**数据面**必须按哈希路由：对一个离位的租户来说，"不在哈希指向的
	// 分片上"就等于不存在。响亮地报 ErrNoTenant，而不是顺手在别的片上
	// 把它找出来——后者会让一次错误的手工装配被永久掩盖，直到某次重启
	// 把路由改到另一片，问题才以"数据丢失"的样子爆出来。
	if _, err := h.pool.Capabilities(id); !errors.Is(err, tenant.ErrNoTenant) {
		t.Errorf("数据面在错误的分片上找到了租户（err = %v）：粘性路由被静默绕过", err)
	}
	if _, err := h.pool.Runtime(id, "s"); !errors.Is(err, tenant.ErrNoTenant) {
		t.Errorf("数据面的写路径同样必须按哈希路由（err = %v）", err)
	}

	// Find 必须找到真的那一片。
	s, tn, err := h.pool.Find(id)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if s.ID() != other.ID() || tn.ID != ident.Tenant(id) {
		t.Fatalf("Find 返回分片 %d，期望 %d", s.ID(), other.ID())
	}

	// 并且必须能被注销掉——这是"用 Find 而不是 For"的真实收益。
	if err := h.pool.Deprovision([]ident.Tenant{id}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if _, ok := other.Get(id); ok {
		t.Fatal("租户未被真正注销：Deprovision 作用到了错误的分片上")
	}
	if n := len(h.pool.Tenants()); n != 0 {
		t.Fatalf("注销后剩余租户数 = %d", n)
	}
}

// ---------------------------------------------------------------------------
// 数据面 / 管理面的读写路径
// ---------------------------------------------------------------------------

// *Shard 必须满足接入层的 Workspace 契约——这是 edge 与 shard 之间
// 唯一的接缝。这条断言在编译期把接缝钉死：将来谁改了签名，这里先红。
func TestShardFitsTheEdgeWorkspaceContract(t *testing.T) {
	var w interface {
		Capabilities(ident.Tenant) (caps.Snapshot, error)
		Runtime(ident.Tenant, ident.Session) (*session.Entry, error)
	} = (*Shard)(nil)
	_ = w

	h := newHarness(t, Config{Shards: 2})
	h.mustProvision("tenant-a")

	s, err := h.pool.For("tenant-a")
	if err != nil {
		t.Fatalf("For: %v", err)
	}

	snap, err := s.Capabilities("tenant-a")
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if !snap.Ready() {
		t.Fatalf("快照未就绪：%v", snap)
	}
	if snap.Tenant != "tenant-a" {
		t.Errorf("快照的租户 = %q", snap.Tenant)
	}

	rt, err := s.Runtime("tenant-a", "default")
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	// 会话运行时必须是稳定的：两次取到同一个，历史才会连续。
	again, err := s.Runtime("tenant-a", "default")
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	if rt != again {
		t.Fatal("同一个会话两次取到了不同的运行时：历史会被拆成两半")
	}

	// 不在本片上的租户：报"不存在"，而不是悄悄路由到别的片。
	if _, err := s.Capabilities("tenant-elsewhere"); !errors.Is(err, tenant.ErrNoTenant) {
		t.Errorf("非本片租户的错误 = %v，期望 ErrNoTenant", err)
	}
}

// Workspace 的读路径必须作用在同一片上：A 的句柄不能出现在 B 的片里。
func TestCapabilitiesAreShardLocal(t *testing.T) {
	h := newHarness(t, Config{Shards: 2})
	cover := coverShards(t, h.pool, 3)
	for _, ids := range cover {
		h.mustProvision(ids...)
	}

	for _, s := range h.pool.Shards() {
		for _, id := range s.List() {
			snap, err := s.Capabilities(id)
			if err != nil {
				t.Fatalf("Capabilities(%s) @%d: %v", id, s.ID(), err)
			}
			if snap.Tenant != id {
				t.Fatalf("分片 %d 上 %s 的快照标着 %s", s.ID(), id, snap.Tenant)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 停机顺序
// ---------------------------------------------------------------------------

// 停机顺序必须是"先摘租户、再关 App"。反过来的话，摘租户的 DoSync 会因为
// 调度器已 seal 而失败，租户的效果回收就没人驱动了。
func TestCloseDeprovisionsBeforeClosingApps(t *testing.T) {
	h := newHarness(t, Config{Shards: 2})
	cover := coverShards(t, h.pool, 3)
	var all []string
	for _, ids := range cover {
		h.mustProvision(ids...)
		all = append(all, ids...)
	}

	// 往租户存储里塞点东西，用来证明 DropTenant 真的跑了。
	for _, id := range all {
		if _, err := h.store.AppendTurn(ident.Tenant(id), "s", memory.Turn{
			Role: memory.RoleUser, Content: "残留",
		}); err != nil {
			t.Fatalf("AppendTurn: %v", err)
		}
	}

	h.pool.Close()

	for _, s := range h.pool.Shards() {
		if !s.Closed() {
			t.Errorf("分片 %d 未关闭", s.ID())
		}
		if n := len(s.List()); n != 0 {
			t.Errorf("分片 %d 停机后仍挂着 %d 个租户：说明 App 是先关的，租户被留在了半拆除状态", s.ID(), n)
		}
		// DoSync 在调度器 seal 之后必须失败——这证明 App 确实被冻结了。
		if s.App().DoSync(func(*cordis.Context) {}) {
			t.Errorf("分片 %d 的调度器在停机后仍可调度", s.ID())
		}
	}
	if n := len(h.pool.Tenants()); n != 0 {
		t.Errorf("停机后全池仍有 %d 个租户", n)
	}

	// 租户记忆必须被清干净：一个"没有入口指向它"的历史，
	// 会在同 ID 重新开通时被读成上一轮的残留。
	for _, id := range all {
		sessions, turns, docs := h.store.Count(ident.Tenant(id))
		if sessions != 0 || turns != 0 || docs != 0 {
			t.Errorf("租户 %s 停机后仍留下 %d/%d/%d 条记录", id, sessions, turns, docs)
		}
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	h := newHarness(t, Config{Shards: 2})
	h.mustProvision("tenant-a")
	h.pool.Close()
	h.pool.Close() // 不能 panic，也不能二次拆坏什么

	if !h.pool.isClosed() {
		t.Error("池未标记为关闭")
	}
	for _, s := range h.pool.Shards() {
		if !s.Closed() {
			t.Errorf("分片 %d 未关闭", s.ID())
		}
	}
}

// 分片被单独关闭后，它的写路径必须拒绝服务——否则租户会被开通到一个
// 已经没人驱动的 App 上，表现为"开通成功但一切都不工作"。
func TestClosedShardRejectsWrites(t *testing.T) {
	h := newHarness(t, Config{Shards: 2})
	s := h.pool.Shards()[0]
	s.Close()

	if err := s.Provision(specs("tenant-x")); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭的分片上 Provision 的错误 = %v，期望 ErrClosed", err)
	}
	if err := s.Deprovision([]ident.Tenant{"tenant-x"}); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭的分片上 Deprovision 的错误 = %v，期望 ErrClosed", err)
	}
}

// ---------------------------------------------------------------------------
// 自检
// ---------------------------------------------------------------------------

func TestSelfCheckPassesForProvisionedPool(t *testing.T) {
	h := newHarness(t, Config{Shards: 4, SelfCheckPairs: 64})
	cover := coverShards(t, h.pool, 3)
	total := 0
	withPairs := 0
	for _, ids := range cover {
		h.mustProvision(ids...)
		total += len(ids)
		if len(ids) >= 2 {
			withPairs++
		}
	}

	rep, err := h.pool.SelfCheck()
	if err != nil {
		t.Fatalf("SelfCheck: %v", err)
	}
	if rep.Shards != 4 {
		t.Errorf("Shards = %d", rep.Shards)
	}
	if rep.Tenants != total {
		t.Errorf("Tenants = %d，期望 %d", rep.Tenants, total)
	}
	if rep.PairsChecked == 0 {
		t.Error("一对都没比较：自检没有覆盖到任何东西")
	}
	if rep.Declarations == 0 {
		t.Error("一条 Isolate 声明都没扫到：自检的声明层没跑，而报告里看不出差别")
	}
	if !rep.Exhaustive {
		t.Errorf("SelfCheckPairs=64 时应当穷尽，报告为抽样：%+v", rep)
	}
	if withPairs < 1 {
		t.Fatal("测试自身没有造出含 ≥2 租户的分片")
	}
	if got := h.pool.Checked(); got != int64(rep.PairsChecked) {
		t.Errorf("Checked() = %d，期望 %d", got, rep.PairsChecked)
	}

	// 再跑一次必须仍然通过（没有"只能跑一次"的隐藏状态）。
	if _, err := h.pool.SelfCheck(); err != nil {
		t.Fatalf("第二次 SelfCheck: %v", err)
	}
	if got, want := h.pool.Checked(), int64(2*rep.PairsChecked); got != want {
		t.Errorf("累计 Checked() = %d，期望 %d", got, want)
	}

	// 成功的自检必须被记账。
	snap := h.metrics.Global().Snapshot("")
	if snap.IsolationChecks == 0 {
		t.Error("自检成功没有计入 IsolationChecks")
	}
	if snap.IsolationBreaches != 0 {
		t.Errorf("干净的分片池报出了 %d 次隔离破坏", snap.IsolationBreaches)
	}
}

func TestSelfCheckReportsSamplingHonestly(t *testing.T) {
	h := newHarness(t, Config{Shards: 1, SelfCheckPairs: 1})
	h.mustProvision("t-a", "t-b", "t-c")

	rep, err := h.pool.SelfCheck()
	if err != nil {
		t.Fatalf("SelfCheck: %v", err)
	}
	if rep.Tenants != 3 {
		t.Fatalf("Tenants = %d，期望 3", rep.Tenants)
	}
	if rep.PairsChecked != 1 {
		t.Errorf("PairsChecked = %d，期望 1（抽检上限）", rep.PairsChecked)
	}
	if rep.Exhaustive {
		t.Error("3 个租户有 3 对，只比较 1 对却报告为穷尽：报告必须诚实")
	}

	// 把抽检上限调大之后，同一个池应当能报告穷尽。
	h.pool.cfg.SelfCheckPairs = 100
	rep2, err := h.pool.SelfCheck()
	if err != nil {
		t.Fatalf("SelfCheck: %v", err)
	}
	if rep2.PairsChecked != 3 {
		t.Errorf("PairsChecked = %d，期望 3（全部对）", rep2.PairsChecked)
	}
	if !rep2.Exhaustive {
		t.Error("覆盖了全部 3 对，应报告穷尽")
	}
}

func TestSelfCheckWithTooFewTenants(t *testing.T) {
	h := newHarness(t, Config{Shards: 3})

	rep, err := h.pool.SelfCheck()
	if err != nil {
		t.Fatalf("空池自检不该报错：%v", err)
	}
	if rep.PairsChecked != 0 || !rep.Exhaustive || rep.Tenants != 0 {
		t.Errorf("空池报告 = %+v", rep)
	}

	h.mustProvision("tenant-only")
	rep, err = h.pool.SelfCheck()
	if err != nil {
		t.Fatalf("单租户自检不该报错：%v", err)
	}
	if rep.PairsChecked != 0 {
		t.Errorf("单租户无法组成对，PairsChecked = %d", rep.PairsChecked)
	}
	if !rep.Exhaustive {
		t.Error("没有任何对可比时应当报告穷尽（0 对就是全部 0 对）")
	}
	if rep.Tenants != 1 {
		t.Errorf("Tenants = %d", rep.Tenants)
	}
}

// 自检失败必须计入 IsolationBreaches——那是这个指标唯一的用途。
//
// 用单分片：片内自检只对同一片内的租户对生效，两个租户分处两片时
// 根本走不到比较那一步。
func TestIsolationCheckCountsBothOutcomes(t *testing.T) {
	h := newHarness(t, Config{Shards: 1})
	h.mustProvision("tenant-a", "tenant-b")

	s, err := h.pool.For("tenant-a")
	if err != nil {
		t.Fatalf("For: %v", err)
	}

	if err := s.IsolationCheck("tenant-a", "tenant-b"); err != nil {
		t.Fatalf("两个租户应当彼此隔离：%v", err)
	}
	g := h.metrics.Global().Snapshot("")
	if g.IsolationChecks != 1 || g.IsolationBreaches != 0 {
		t.Errorf("成功路径指标 = %d/%d，期望 1/0", g.IsolationChecks, g.IsolationBreaches)
	}
	a := h.metrics.Tenant("tenant-a").Snapshot("tenant-a")
	if a.IsolationChecks != 1 {
		t.Errorf("租户侧 IsolationChecks = %d", a.IsolationChecks)
	}

	// 不存在的租户必然失败：这里只需要一条"会报错"的路径来验证计数。
	if err := s.IsolationCheck("ghost-a", "ghost-b"); err == nil {
		t.Fatal("未知租户的隔离断言应当失败")
	}
	g = h.metrics.Global().Snapshot("")
	if g.IsolationChecks != 2 || g.IsolationBreaches != 1 {
		t.Errorf("失败路径指标 = %d/%d，期望 2/1", g.IsolationChecks, g.IsolationBreaches)
	}
}

// 自检必须真的比得动跨分片的两个租户（不同 App / 不同 Reflect 实例）。
func TestSelfCheckCoversCrossShardPairs(t *testing.T) {
	h := newHarness(t, Config{Shards: 3, SelfCheckPairs: 8})
	cover := coverShards(t, h.pool, 3)
	for shardID, ids := range cover {
		if len(ids) < 1 {
			t.Fatalf("分片 %d 没有租户", shardID)
		}
		h.mustProvision(ids...)
	}

	if _, err := h.pool.SelfCheck(); err != nil {
		t.Fatalf("跨分片自检失败：%v", err)
	}

	// 直接钉住跨分片那一对具备"隔离的两个证据来源"：不同的能力句柄。
	var handles []*caps.Handle
	for _, s := range h.pool.Shards() {
		ids := s.List()
		if len(ids) == 0 {
			continue
		}
		tn, ok := s.Get(ids[0])
		if !ok {
			t.Fatalf("分片 %d 上 %s 取不到句柄", s.ID(), ids[0])
		}
		handles = append(handles, tn.Caps)
	}
	if len(handles) < 2 {
		t.Fatal("需要至少两个分片各有一个租户")
	}
	if err := caps.AssertIsolated(handles[0], handles[1], realm.Services()); err != nil {
		t.Fatalf("跨分片租户未被判定为隔离：%v", err)
	}
}

// ---------------------------------------------------------------------------
// 状态视图
// ---------------------------------------------------------------------------

func TestStatsAndTenantsView(t *testing.T) {
	h := newHarness(t, Config{Shards: 3})
	cover := coverShards(t, h.pool, 3)
	var all []string
	for _, ids := range cover {
		h.mustProvision(ids...)
		all = append(all, ids...)
	}

	stats := h.pool.Stats()
	if len(stats) != 3 {
		t.Fatalf("Stats 长度 = %d", len(stats))
	}
	total := 0
	for i, st := range stats {
		if st.Shard != i {
			t.Errorf("Stats[%d].Shard = %d", i, st.Shard)
		}
		if st.Closed {
			t.Errorf("分片 %d 未关闭却报 Closed", i)
		}
		if st.Tenants != len(st.Bound) {
			t.Errorf("分片 %d：Tenants = %d，Bound 长度 = %d", i, st.Tenants, len(st.Bound))
		}
		for j := 1; j < len(st.Bound); j++ {
			if st.Bound[j-1] >= st.Bound[j] {
				t.Errorf("分片 %d 的 Bound 未升序：%v", i, st.Bound)
			}
		}
		total += st.Tenants
	}
	if total != len(all) {
		t.Errorf("各片租户数之和 = %d，期望 %d", total, len(all))
	}
	if got := h.pool.Tenants(); len(got) != len(all) {
		t.Errorf("Tenants() 长度 = %d，期望 %d", len(got), len(all))
	}

	// 单片的 Stats 与池视图必须一致。
	for _, s := range h.pool.Shards() {
		if got := s.Stats(); got.Shard != s.ID() || got.Tenants != len(s.List()) {
			t.Errorf("分片 %d 的 Stats 与 List 不一致：%+v", s.ID(), got)
		}
	}
}

// ---------------------------------------------------------------------------
// 单分片形态（ADR-002 的"可逆性"）
// ---------------------------------------------------------------------------

func TestSingleShardPoolIsFullyFunctional(t *testing.T) {
	h := newHarness(t, Config{Shards: 1})
	h.mustProvision("tenant-a", "tenant-b", "tenant-c")

	if h.pool.Len() != 1 {
		t.Fatalf("Len = %d", h.pool.Len())
	}
	// 全部租户都在片 0 上。
	for _, s := range h.pool.Shards() {
		if s.ID() != 0 {
			t.Errorf("出现了分片 %d", s.ID())
		}
	}
	if n := len(h.pool.Shards()[0].List()); n != 3 {
		t.Fatalf("片 0 上有 %d 个租户，期望 3", n)
	}
	rep, err := h.pool.SelfCheck()
	if err != nil {
		t.Fatalf("SelfCheck: %v", err)
	}
	if rep.PairsChecked != 3 || !rep.Exhaustive {
		t.Errorf("单分片 3 租户的自检报告 = %+v，期望 3 对且穷尽", rep)
	}
	// 单分片下跨分片检查应当自动跳过，而不是报错。
	if err := h.pool.crossShardCheck(); err != nil {
		t.Errorf("单分片不该做跨分片检查：%v", err)
	}
}

// ---------------------------------------------------------------------------
// 并发
// ---------------------------------------------------------------------------

// 路由与读路径必须能并发使用：它们在请求路径上。
func TestConcurrentRoutingAndReadsAreRaceFree(t *testing.T) {
	h := newHarness(t, Config{Shards: 4})
	ids := idsFor(16)
	h.mustProvision(ids...)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				id := ident.Tenant(ids[(i+j)%len(ids)])
				s, err := h.pool.For(id)
				if err != nil {
					t.Errorf("For(%s): %v", id, err)
					return
				}
				if _, ok := s.Get(id); !ok {
					t.Errorf("租户 %s 不在它自己的分片上", id)
					return
				}
				if _, err := s.Capabilities(id); err != nil {
					t.Errorf("Capabilities(%s): %v", id, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// 与接入层的接缝
// ---------------------------------------------------------------------------

// 一条最小但完整的链路：分片池 → 分片 → 会话运行时 → 记忆隔离。
// 这里不经过 edge，只验证 shard 交出去的东西确实是"已经隔离好的"。
func TestShardRuntimeIsolatesSessions(t *testing.T) {
	h := newHarness(t, Config{Shards: 2})
	h.mustProvision("tenant-a")

	s, err := h.pool.For("tenant-a")
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	if _, err := s.Runtime("tenant-a", "s1"); err != nil {
		t.Fatalf("Runtime(s1): %v", err)
	}
	if _, err := s.Runtime("tenant-a", "s2"); err != nil {
		t.Fatalf("Runtime(s2): %v", err)
	}

	snap, err := s.Capabilities("tenant-a")
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if _, err := snap.Memory.AppendTurn("tenant-a", "s1", memory.Turn{
		Role: memory.RoleUser, Content: "只属于 s1",
	}); err != nil {
		t.Fatalf("AppendTurn: %v", err)
	}
	turns, err := snap.Memory.Turns("tenant-a", "s2")
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turns) != 0 {
		t.Fatalf("会话 s2 读到了 s1 的 %d 轮内容", len(turns))
	}
	// 别的租户的存储必须是空的。
	turns, err = h.store.Turns("tenant-other", "s1")
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turns) != 0 {
		t.Fatalf("另一个租户读到了 %d 轮", len(turns))
	}
}

// 审计是进程级共享的，但事件必须带租户归属。
func TestAuditIsSharedButTenantScoped(t *testing.T) {
	log := audit.New(64, time.Now)
	h := newHarness(t, Config{Shards: 2, Audit: log})
	h.mustProvision("tenant-a")

	if log.Count("tenant-a") == 0 {
		t.Fatal("开通租户没有留下审计：装配是管理面操作，必须留痕")
	}
	if log.Count("tenant-b") != 0 {
		t.Errorf("租户 b 的审计里有 %d 条 tenant-a 的事件", log.Count("tenant-b"))
	}
	// 每个事件都要能回答"是谁做的"。
	for _, e := range log.For("tenant-a", 0) {
		if e.Tenant != "tenant-a" {
			t.Errorf("审计事件挂在 %q 上", e.Tenant)
		}
	}
}

// ---------------------------------------------------------------------------
// 注销时的共享状态回收
// ---------------------------------------------------------------------------

// 注销必须把租户在**进程级单例**里的残留一起清掉。
//
// 这是注销路径的一半职责：指标注册表与准入器都按租户建索引，只加不减
// 就是无界增长路径。指标那半的表现是 Registry.Render() 的 label 基数
// 随注销过的租户数增长——Prometheus 最经典的一种故障，而且是"监控先挂"；
// 准入那半是一份永不回收的令牌桶。
func TestDeprovisionForgetsSharedState(t *testing.T) {
	adm := admit.New(admit.Config{
		Default: admit.Limits{RatePerSecond: 1, Burst: 1, MaxConcurrent: 1},
	})
	defer adm.Close()

	h := newHarness(t, Config{Shards: 1, Admit: adm})
	h.mustProvision("tenant-a")

	// 让准入器为该租户建桶，并把它烧空。
	held, err := adm.Acquire("tenant-a")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if _, err := adm.Acquire("tenant-a"); err == nil {
		t.Fatal("前置条件不成立：桶应当是空的")
	}
	held.Release()

	// 让指标注册表为该租户建计数器。
	h.metrics.Tenant("tenant-a").RunsStarted.Add(7)
	h.metrics.Tenant("tenant-a").ObserveRun(time.Millisecond)
	if !containsTenant(h.metrics.Tenants(), "tenant-a") {
		t.Fatal("前置条件不成立：指标注册表里应当有该租户")
	}

	if err := h.pool.Deprovision([]ident.Tenant{"tenant-a"}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}

	// 指标：注册表里不该再有它。
	if containsTenant(h.metrics.Tenants(), "tenant-a") {
		t.Error("注销后指标注册表仍留着该租户：Render() 的 label 基数会无界增长")
	}
	// 准入：桶被丢弃，新桶是满的，所以立刻就能获取。
	pm, err := adm.Acquire("tenant-a")
	if err != nil {
		t.Errorf("注销后准入器仍持有旧桶（空桶）：%v", err)
	} else {
		pm.Release()
	}
}

// 注销一个**不在本片上**的租户不得动到任何共享状态。
//
// 守的是一个很容易写错的边界：清理如果以"这个 ID 现在在本片查不到"
// 为依据，就会走进清理分支——而查不到有两种截然不同的原因：
//
//  1. 它刚被本片摘掉（该清）；
//  2. 它压根不在本片：名字拼错了，或者它**活着、但在别的片上**（不该清）。
//
// 情形 2 的代价不是"多清了一份垃圾"：清掉一个仍在服务的租户的令牌桶，
// 等于白送它一次完整的突发额度——一次由别人失败的管理操作触发的限额
// 绕过。所以这里用同一个 ID 同时扮演"被注销的"和"状态仍在的"，
// 让两种实现给出不同答案。
func TestDeprovisionForeignTenantKeepsItsState(t *testing.T) {
	adm := admit.New(admit.Config{
		Default: admit.Limits{RatePerSecond: 1, Burst: 1, MaxConcurrent: 1},
	})
	defer adm.Close()

	h := newHarness(t, Config{Shards: 1, Admit: adm})

	// 刻意不 provision：这个 ID 只存在于进程级单例里（像是一个曾经
	// 开通过、随后被别的分片接管的租户的残留视角）。
	ghost := ident.Tenant("tenant-ghost")
	held, err := adm.Acquire(ghost)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	held.Release()
	if _, err := adm.Acquire(ghost); err == nil {
		t.Fatal("前置条件不成立：ghost 的桶应当是空的")
	}
	h.metrics.Tenant(ghost).RunsStarted.Add(1)

	// 注销这个 ID：本片会报"不认识它"。
	if err := h.pool.Deprovision([]ident.Tenant{ghost}); !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("注销非本片租户的错误 = %v，期望 ErrNoTenant", err)
	}

	if !containsTenant(h.metrics.Tenants(), ghost) {
		t.Error("注销非本片租户清掉了它的指标：本片的失败操作动到了别处的活租户")
	}
	if _, err := adm.Acquire(ghost); err == nil {
		t.Error("注销非本片租户清掉了它的令牌桶：等于白送它一次完整突发额度")
	}
}

// 同一个道理放在跨分片上：在 A 片上注销一个活在 B 片上的租户，
// 必须既不摘掉 B 的租户，也不碰它的共享状态。
func TestDeprovisionOnWrongShardLeavesTheRealOneAlone(t *testing.T) {
	adm := admit.New(admit.Config{
		Default: admit.Limits{RatePerSecond: 1, Burst: 1, MaxConcurrent: 1},
	})
	defer adm.Close()

	h := newHarness(t, Config{Shards: 2, Admit: adm})
	cover := coverShards(t, h.pool, 1)
	if len(cover) < 2 {
		t.Fatal("需要两个分片各有一个租户")
	}
	var shardIDs []int
	for id := range cover {
		shardIDs = append(shardIDs, id)
	}
	sort.Ints(shardIDs)

	ownerID, otherID := shardIDs[0], shardIDs[1]
	ownerShard := h.pool.Shards()[ownerID]
	otherShard := h.pool.Shards()[otherID]
	ownerShard.Provision(specs(cover[ownerID]...))
	otherShard.Provision(specs(cover[otherID]...))

	victim := ident.Tenant(cover[ownerID][0])
	h.metrics.Tenant(victim).RunsStarted.Add(1)
	held, err := adm.Acquire(victim)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	held.Release()
	if _, err := adm.Acquire(victim); err == nil {
		t.Fatal("前置条件不成立：victim 的桶应当是空的")
	}

	// 在**错误的分片**上注销它。
	if err := otherShard.Deprovision([]ident.Tenant{victim}); !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("在错误分片上注销的错误 = %v，期望 ErrNoTenant", err)
	}

	if _, ok := ownerShard.Get(victim); !ok {
		t.Fatal("真实的租户被别的分片的失败操作摘掉了")
	}
	if !containsTenant(h.metrics.Tenants(), victim) {
		t.Error("真实的租户的指标被别的分片的失败操作清掉了")
	}
	if _, err := adm.Acquire(victim); err == nil {
		t.Error("真实的租户的令牌桶被别的分片的失败操作清掉了")
	}
}

// 停机路径也要清（与注销共用一条规则，避免出现两套行为）。
func TestCloseForgetsSharedState(t *testing.T) {
	adm := admit.New(admit.Config{
		Default: admit.Limits{RatePerSecond: 1, Burst: 1, MaxConcurrent: 1},
	})
	defer adm.Close()

	h := newHarness(t, Config{Shards: 1, Admit: adm})
	h.mustProvision("tenant-a")
	h.metrics.Tenant("tenant-a").RunsStarted.Add(1)

	h.pool.Close()

	if containsTenant(h.metrics.Tenants(), "tenant-a") {
		t.Error("停机后指标注册表仍留着租户")
	}
}

func containsTenant(list []ident.Tenant, want ident.Tenant) bool {
	for _, t := range list {
		if t == want {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 声明层的自检（SAFETY R3）
// ---------------------------------------------------------------------------

// loaderOf 取某分片自己的 loader。
//
// 分片之间是**不同的 App**，因此各有各的入口树；自检的声明层扫的是
// 本片那棵。这里从 App 上把它取回来，是为了能用与生产相同的注入方式
// （手工建入口）来验证自检确实会红。
func loaderOf(t *testing.T, s *Shard) *cordis.Loader {
	t.Helper()
	v, ok := s.App().Root().Get("loader")
	if !ok {
		t.Fatal("分片的 App 上没有 loader 服务")
	}
	l, ok := v.(*cordis.Loader)
	if !ok {
		t.Fatalf("loader 服务的类型是 %T，期望 *cordis.Loader", v)
	}
	return l
}

// TestSelfCheckScansEveryTenantSubtree 钉住计数。
//
// 计数不是装饰：没有它，「声明层跑了且干净」与「声明层根本没跑」
// 在报告里完全同形，而这两件事的含义正相反。
func TestSelfCheckScansEveryTenantSubtree(t *testing.T) {
	h := newHarness(t, Config{Shards: 2})
	h.mustProvision("tenant-a", "tenant-b")

	rep, err := h.pool.SelfCheck()
	if err != nil {
		t.Fatalf("SelfCheck: %v", err)
	}

	// 每个租户的根上至少挂着 realm.Services() 条声明。
	if want := 2 * len(realm.Services()); rep.Declarations < want {
		t.Errorf("Declarations = %d，期望至少 %d（2 个租户 × %d 条）",
			rep.Declarations, want, len(realm.Services()))
	}

	// 再多开通一个租户，计数必须跟着涨——否则它可能是个常数，
	// 而不是真的逐租户扫出来的。
	before := rep.Declarations
	h.mustProvision("tenant-c")
	rep, err = h.pool.SelfCheck()
	if err != nil {
		t.Fatalf("SelfCheck: %v", err)
	}
	if rep.Declarations <= before {
		t.Errorf("多开通一个租户后 Declarations 仍为 %d（此前 %d）：声明层没有跟着租户走",
			rep.Declarations, before)
	}

	// 空池：没有租户就没有声明，计数为 0 是**正确**的（而不是没跑）。
	empty := newHarness(t, Config{Shards: 2})
	repEmpty, err := empty.pool.SelfCheck()
	if err != nil {
		t.Fatalf("空池 SelfCheck: %v", err)
	}
	if repEmpty.Declarations != 0 {
		t.Errorf("空池 Declarations = %d，期望 0", repEmpty.Declarations)
	}
}

// TestSelfCheckFailsOnSharedRealmLabel 是这条链路的正面控制。
//
// 它端到端地证明「手工造一个带共享域标签的入口 → 自检响亮地失败 →
// 指标记一次隔离破坏」。只测干净那一向的话，一个恒返回 (n, nil) 的
// 实现同样能全绿——而这正是 SAFETY.md 声称存在、实际却谁也没执行过的
// 那条断言。
func TestSelfCheckFailsOnSharedRealmLabel(t *testing.T) {
	h := newHarness(t, Config{Shards: 2})
	h.mustProvision("tenant-a", "tenant-b")

	// 自检必须是干净的起点，否则下面就不清楚是谁让它红的。
	if _, err := h.pool.SelfCheck(); err != nil {
		t.Fatalf("起点自检就失败了：%v", err)
	}
	breachesBefore := h.metrics.Global().Snapshot("").IsolationBreaches

	// 挑一个租户，手工在它的子树下建一个带共享域标签的入口。
	// 这就是「有人绕过 realm.Tenant() 手工造入口」的形态：
	// cordis 会老实地把它建成 "@shared" 共享域，两个租户静默同域。
	s := h.pool.Shards()[0]
	ids := s.Tenants().List()
	if len(ids) == 0 {
		t.Fatal("第 0 片上没有租户，测试没造出前置条件")
	}
	entryID, ok := s.Tenants().ResolveEntry(ids[0])
	if !ok {
		t.Fatalf("取不到 %s 的入口 ID", ids[0])
	}
	dirty := realm.Child(entryID, "dirty")
	if _, err := loaderOf(t, s).Create(cordis.EntryOptions{
		ID:      dirty,
		Name:    realm.PluginSessions,
		Isolate: map[string]any{realm.ServiceHistory: "@shared"},
	}, entryID, 0); err != nil {
		t.Fatalf("建脏入口失败：%v", err)
	}

	_, err := h.pool.SelfCheck()
	if err == nil {
		t.Fatal("自检对一条共享域声明无动于衷：SAFETY R3 的声明层没有真的在跑")
	}
	if !errors.Is(err, realm.ErrSharedRealm) {
		t.Fatalf("错误 = %v，期望是 realm.ErrSharedRealm", err)
	}
	if !strings.Contains(err.Error(), dirty) {
		t.Errorf("错误 %q 必须指名到出错的入口 %q", err, dirty)
	}

	// 而且必须记一次隔离破坏：只进日志不进指标的话，"隔离失效了"
	// 这件事在监控上看不见，而监控先挂正是这条链路要避免的。
	if got := h.metrics.Global().Snapshot("").IsolationBreaches; got <= breachesBefore {
		t.Errorf("IsolationBreaches = %d（此前 %d）：隔离失效没有计入指标",
			got, breachesBefore)
	}
}
