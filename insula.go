// Package insula 是整个平台的装配根与运维门面。
//
// # 这个包为什么存在
//
// 前面每一个包都只认识自己的直接依赖，谁也说不清"整个平台该按什么顺序
// 立起来、又该按什么顺序拆下去"。这两件事都不是可以从各包注释里推导出来
// 的：它们是**装配者**才知道的知识。装配者只有一个，所以这里只有一个包。
//
// 具体承担三件事：
//
//   - **装配顺序**（装配顺序 = 依赖顺序，自底向上，见 Open）；
//   - **停机顺序**（它不是装配顺序的逆，理由见 Close）；
//   - **运维门面与数据面出口的边界**（Handler 是唯一对外的出口，
//     Provision / Deprovision 是运维面的，见下面的"门面纪律"）。
//
// # 门面纪律
//
// 本包导出的方法分两类，混用会出事：
//
//   - Handler() 返回的东西是给**世界**的。它背后只有一个 HTTP 处理器，
//     而那个处理器拿不到 Provision。
//   - Provision / Deprovision / Shards / SelfCheck 是给**运维者**的。
//     它们必须留在进程的装配代码里（cmd/insula），绝不能挂到某个
//     请求处理器、插件或工具上——"一个 HTTP 处理器能注销租户"是纯粹的
//     额外攻击面，而类型系统能让它在编译期不成立，就不该靠代码评审去记。
//
// 这条边界不是靠自觉：接入层的 Workspace 接口只声明了 Capabilities 与
// Runtime 两个方法，Provision 根本不在它的类型里。本包把 *shard.Pool
// 直接交给接入层，正是因为上游已经收窄过了。
//
// # 共享与隔离的分界
//
// 模型池、记忆存储、准入器、指标、审计是**进程级单例**（设计 §10 硬规则 4）。
// 各分片注册的只是指向同一份的瘦句柄，隔离由存储结构本身保证——租户是
// 一级索引维度，而不是"N 个互不相干的实例"这种只靠约定维持的属性。
//
// 这决定了一件容易被写错的事：**平台级限额必须是全局的**。准入器因此
// 只在此处构造一份并交给接入层，而不是每片一个（每片一个等于把"全平台
// 总量"变成 N 倍总量）。
package insula

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/corecraft-io/insula/admit"
	"github.com/corecraft-io/insula/audit"
	"github.com/corecraft-io/insula/creds"
	"github.com/corecraft-io/insula/edge"
	"github.com/corecraft-io/insula/gateway"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/metrics"
	"github.com/corecraft-io/insula/realm"
	"github.com/corecraft-io/insula/shard"
	"github.com/corecraft-io/insula/tenant"
	"github.com/corecraft-io/insula/tools"
)

// DefaultAuditCapacity 是审计环形缓冲的默认容量。
//
// 容量只决定"内存里能翻回多久"，不决定正确性：审计的持久化由 Sink 负责
// （见 audit.Log）。4096 条足以覆盖一次部署窗口内的排查。
const DefaultAuditCapacity = 4096

// DefaultShutdownGrace 是停机时留给在途请求的排空窗口。
//
// 有上限是必须的：一个握着长连接的客户端（SSE 正是长连接）能让优雅停机
// 无限期挂着，而那会让一次滚动发布卡住。
const DefaultShutdownGrace = 10 * time.Second

// Config 是平台的全部装配输入。
//
// 大部分字段可以为空并在 Open 里补默认值；真正必须由调用方给出的只有
// 三样：模型上游、凭证提供者、鉴权。它们的共同点是**都指向外部世界**
// ——平台无法替调用方决定"模型从哪来""凭证从哪来""谁有资格调用"。
type Config struct {
	// ---- 分片 ----

	// Shards 分片数，<=0 取 shard.DefaultShards。
	//
	// 单租户部署显式设 1 是合法的：分片是横向扩展的开关，不是前提。
	Shards int
	// Hasher 租户哈希器，nil 时新建。
	//
	// 它是**进程级唯一**的：同一个租户在不同副本上算出不同哈希，
	// 粘性路由与入口 ID 就都对不上。
	Hasher *realm.Hasher

	// ---- 外部世界 ----

	// Upstream 模型后端。必需。
	Upstream gateway.Upstream
	// Creds 凭证提供者。必需。
	//
	// 凭证只在服务端存在（SAFETY.md 硬规则 2）：caps.Request 里没有
	// 任何凭证字段，结构体层面就不存在"从模型输出里把它填进来"这条路。
	Creds creds.Provider
	// CredScope 模型凭证的作用域，空表示 gateway 的默认值（"model-api"）。
	//
	// 它必须能配：凭证存储里的键是 (租户, scope)，而 scope 由上游的
	// 结算方式决定（按模型 API 计费、按网关计费、按自建推理集群分配，
	// 用的都是不同的 scope）。硬编码一个等于假设所有部署都一样。
	CredScope string
	// Auth 接入层鉴权。必需。
	//
	// 它的唯一职责是把凭据解析成主体——租户身份只能从这里来。
	Auth edge.Auth

	// ---- 进程级单例（nil 时新建）----

	Store   *memory.MemStore
	Tools   *tools.TenantConfig
	Metrics *metrics.Registry
	Audit   *audit.Log
	// AuditCapacity 仅在 Audit 为 nil 时生效，<=0 取 DefaultAuditCapacity。
	AuditCapacity int

	// ---- 策略 ----

	// Admit 准入策略。
	Admit admit.Config
	// SelfCheckPairs 每次自检在单片内抽检的租户对数上限，<=0 取 shard 的默认。
	SelfCheckPairs int
	// Tenants 启动时开通的租户。
	//
	// 开通失败会让 Open **整体失败**。这是刻意的：运维者在这里显式声明
	// 了这些租户，"启动成功但少了一个租户"是那种看起来一切正常、直到
	// 有人打过来才发现的问题。要"尽力而为"就用 Provision 单独调。
	Tenants []tenant.Spec

	// ---- 接入层 ----

	// Edge 透传给接入层的配置。
	//
	// 其中的 Auth / Admit / Workspace / Metrics / Audit 由 Open 填写，
	// 调用方在这里给的值会被覆盖——它们在本包里是装配结果，不是输入。
	Edge edge.Config
	// SystemPrompt 平台构造的系统提示。
	//
	// 它不能由请求填充（那是最直接的 prompt 注入入口）。需要按租户定制
	// 时应当走 tenant.Spec，而不是请求体。
	SystemPrompt string

	// ---- 通用 ----

	// Clock 注入时钟，nil 表示 time.Now。
	Clock func() time.Time
	// OnWarn 接收非致命告警，可为 nil。
	OnWarn func(msg string, args ...any)

	// skipSelfCheck 记录"启动自检被显式关掉"。
	skipSelfCheck bool
}

// WithSelfCheckDisabled 关闭 Open 末尾的启动自检。
//
// 只在测试或已知不可用的场景里用。默认开着，因为自检能抓的是一类
// **构造回归**（有人手工造了入口、漏了 Isolate、或者在插件里用了共享域
// 标签）——它在部署时抓到，比在凌晨三点以"跨租户串数据"的样子爆出来好得多。
func (c Config) WithSelfCheckDisabled() Config {
	c.skipSelfCheck = true
	return c
}

func (c Config) warn(msg string, args ...any) {
	if c.OnWarn != nil {
		c.OnWarn(msg, args...)
	}
}

func (c Config) now() time.Time {
	if c.Clock != nil {
		return c.Clock()
	}
	return time.Now()
}

// Service 是装配完成的平台。
//
// 它的零值不可用。它持有的都是进程级资源，因此**不要拷贝它**，
// 也**不要把它交给插件或任何请求级代码**——那等于把注销租户的能力
// 递给了不受信的一侧。
type Service struct {
	cfg     Config
	metrics *metrics.Registry
	audit   *audit.Log
	store   *memory.MemStore
	gateway *gateway.Pool
	tools   *tools.TenantConfig
	admit   *admit.Platform
	shards  *shard.Pool
	edge    *edge.Server

	mu      sync.Mutex
	httpSrv *http.Server
	closed  bool
}

// Open 按依赖顺序装配并启动整个平台。
//
// # 顺序就是语义
//
// 自底向上，每一步都只用到前面已经立起来的东西：
//
//  1. **观测先行**（metrics / audit）。它们没有依赖，而后面每一层都可能
//     要记点什么。晚一步构造的代价不是"少记几条"，而是那一层的 nil 判断
//     会一路传下去，最后表现成"某些事件莫名其妙地不见了"。
//  2. **存储与凭证**（store / creds）。纯数据，无依赖。
//  3. **模型网关**。依赖上游与凭证提供者。
//  4. **准入器**。进程级唯一，必须在分片池之前——分片池要拿它做注销时的清理。
//  5. **分片池**。它把上面所有单例分发给每片的租户管理器。
//  6. **接入层**。它需要准入器与分片池（后者满足它的 Workspace 契约）。
//  7. **开通启动租户**，然后**自检**。
//
// 任何一步失败都会把**已经立起来的部分按相反顺序拆干净**再返回错误：
// 半启动的平台比启动失败更难查——它会以"某些租户能开通、某些不能"
// 的样子存在，而那种现象看起来像业务问题。
func Open(cfg Config) (svc *Service, err error) {
	s := &Service{cfg: cfg}

	// 失败路径集中在这里：只写一次，就不会有某条分支忘了拆。
	defer func() {
		if err != nil {
			_ = s.Close()
			svc = nil
		}
	}()

	// 1) 观测。
	s.metrics = cfg.Metrics
	if s.metrics == nil {
		s.metrics = metrics.New()
	}
	s.audit = cfg.Audit
	if s.audit == nil {
		capacity := cfg.AuditCapacity
		if capacity <= 0 {
			capacity = DefaultAuditCapacity
		}
		s.audit = audit.New(capacity, s.cfg.now)
	}

	// 2) 存储与凭证。
	s.store = cfg.Store
	if s.store == nil {
		s.store = memory.NewMemStore(memory.Config{})
	}
	if cfg.Creds == nil {
		return nil, errors.New("insula: Config.Creds is required")
	}

	// 3) 模型网关。
	if cfg.Upstream == nil {
		return nil, errors.New("insula: Config.Upstream is required")
	}
	s.gateway = gateway.New(gateway.Config{
		Upstream:  cfg.Upstream,
		Creds:     cfg.Creds,
		CredScope: cfg.CredScope,
		// 不在这里填 Quotas：每租户配额由 tenant.Spec.Quota 在开通时
		// 经 Pool.SetQuota 生效。两处都能配就会有两处不一致，而
		// 不一致的那一次一定发生在有人改了其中一处之后。
		Clock:  s.cfg.now,
		OnWarn: cfg.warn,
	})

	s.tools = cfg.Tools
	if s.tools == nil {
		// 空模板是合法配置：租户可以有零个工具（纯问答是降级，不是故障）。
		s.tools = &tools.TenantConfig{}
	}

	// 4) 准入器：进程级唯一。
	s.admit = admit.New(admit.Config{
		Default:               cfg.Admit.Default,
		PlatformMaxConcurrent: cfg.Admit.PlatformMaxConcurrent,
		PlatformRatePerSecond: cfg.Admit.PlatformRatePerSecond,
		PlatformBurst:         cfg.Admit.PlatformBurst,
		TenantOverrides:       cfg.Admit.TenantOverrides,
		Clock:                 s.cfg.now,
	})

	// 5) 分片池。
	hasher := cfg.Hasher
	if hasher == nil {
		hasher = realm.NewHasher(nil)
	}
	pool, perr := shard.New(shard.Config{
		Shards:         cfg.Shards,
		Hasher:         hasher,
		Pool:           s.gateway,
		Store:          s.store,
		Tools:          s.tools,
		Admit:          s.admit,
		Metrics:        s.metrics,
		Audit:          s.audit,
		SelfCheckPairs: cfg.SelfCheckPairs,
		Clock:          s.cfg.now,
		OnWarn:         cfg.warn,
	})
	if perr != nil {
		return nil, fmt.Errorf("insula: shard pool: %w", perr)
	}
	s.shards = pool

	// 6) 接入层。
	if cfg.Auth == nil {
		return nil, errors.New("insula: Config.Auth is required")
	}
	edgeCfg := cfg.Edge
	// 这几个字段是装配结果，不是输入：调用方给了也会被覆盖。
	// 覆盖而不是校验，是为了让"接入层拿到的准入器就是平台唯一那一个"
	// 成为构造性的保证，而不是一句配置纪律。
	edgeCfg.Auth = cfg.Auth
	edgeCfg.Admit = s.admit
	edgeCfg.Workspace = s.shards
	edgeCfg.Metrics = s.metrics
	edgeCfg.Audit = s.audit
	if edgeCfg.SystemPrompt == "" {
		edgeCfg.SystemPrompt = cfg.SystemPrompt
	}
	if edgeCfg.Clock == nil {
		edgeCfg.Clock = s.cfg.now
	}
	if edgeCfg.OnWarn == nil {
		edgeCfg.OnWarn = cfg.warn
	}
	srv, serr := edge.New(edgeCfg)
	if serr != nil {
		return nil, fmt.Errorf("insula: edge: %w", serr)
	}
	s.edge = srv

	// 7) 开通启动租户。
	if len(cfg.Tenants) > 0 {
		if err := s.shards.Provision(cfg.Tenants); err != nil {
			return nil, fmt.Errorf("insula: provision startup tenants: %w", err)
		}
	}

	// 8) 自检。
	if !cfg.skipSelfCheck {
		if _, err := s.shards.SelfCheck(); err != nil {
			return nil, fmt.Errorf("insula: startup self-check: %w", err)
		}
	}

	return s, nil
}

// ---------------------------------------------------------------------------
// 数据面出口
// ---------------------------------------------------------------------------

// Handler 返回给世界的 HTTP 处理器。
//
// 它是本包唯一应当被交给不受信一侧的东西。它背后只有一个处理器，
// 而那个处理器既不能开通也不能注销租户。
func (s *Service) Handler() http.Handler { return s.edge }

// Serve 在 ln 上提供服务，阻塞直到出错或被 Shutdown。
//
// 超时只设了 ReadHeaderTimeout。**不要**顺手补上 WriteTimeout：接入层
// 的流式路径是一个可能持续几分钟的 SSE 响应，写超时会把它拦腰剪断，
// 而且表现是"长回答总是缺尾巴"——很容易被归因成模型的问题。
//
// 超时只影响读请求头，因此它挡的是慢速发头那类连接，不影响任何正常请求。
func (s *Service) Serve(ln net.Listener) error {
	if ln == nil {
		return errors.New("insula: nil listener")
	}
	srv := &http.Server{
		Handler:           s.edge,
		ReadHeaderTimeout: 10 * time.Second,
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("insula: service is closed")
	}
	if s.httpSrv != nil {
		s.mu.Unlock()
		return errors.New("insula: already serving")
	}
	s.httpSrv = srv
	s.mu.Unlock()

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown 停止接受新连接并等待在途请求结束。
//
// 它与 Close 是两件事：Shutdown 只关网络，平台本身仍然可用（可以直接
// 重新 Serve，或者继续用运维面的方法）。Close 拆的是平台。
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	srv := s.httpSrv
	s.mu.Unlock()
	if srv == nil {
		return nil
	}
	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("insula: http shutdown: %w", err)
	}
	return nil
}

// Close 完整停机。幂等。
//
// # 停机顺序不是装配顺序的逆
//
// 装配是自底向上的（观测 → 存储 → 网关 → 准入 → 分片 → 接入），
// 而停机是：
//
//  1. **排空接入层**（Shutdown）。在途请求握着的会话运行时与能力句柄
//     都来自分片池，池必须先活着。这一步有上限，因为 SSE 是长连接，
//     没有上限的优雅停机会让一次滚动发布无限期挂住。
//  2. **关闭准入器**。此后到达的请求一律拿到 503。必须在拆池之前：
//     反过来的话，排空窗口边缘挤进来的请求会走进一个正在拆的池，
//     拿到的是 5xx。5xx 在监控里是"平台坏了"，503 是"平台在按计划
//     停机"——把可预期的停机伪装成故障，会让每一次发布都触发一次告警。
//  3. **拆分片池**：逐片注销租户（并把它们在进程级单例里的残留一起清掉），
//     然后冻结每片的 `cordis.App`。
//
// 注意第 3 步内部还有一个更细的顺序，那个顺序由 shard 包负责：
// 必须先摘租户、再关 App。反过来的话，摘租户的调度会失败，租户的效果
// 回收就没人驱动了。
func (s *Service) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	srv := s.httpSrv
	s.mu.Unlock()

	var errs []error

	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultShutdownGrace)
		if err := srv.Shutdown(ctx); err != nil {
			// 排空超时不是停机失败：下面照样拆。但必须留痕，
			// 否则"某个客户端能让发布永远卡住"这件事没人知道。
			s.cfg.warn("insula: graceful drain did not complete in %s: %v", DefaultShutdownGrace, err)
			errs = append(errs, fmt.Errorf("insula: drain: %w", err))
		}
		cancel()
	}

	if s.admit != nil {
		s.admit.Close()
	}
	if s.shards != nil {
		s.shards.Close()
	}

	return errors.Join(errs...)
}

// Closed 报告平台是否已停机。
func (s *Service) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// ---------------------------------------------------------------------------
// 运维面
// ---------------------------------------------------------------------------

// Provision 批量开通租户，按粘性路由分派到各分片。
//
// 它是**幂等以外的**操作：重复开通会被报出来（见 shard.Pool.Provision）。
// 需要"确保存在"的语义用 Ensure。
func (s *Service) Provision(specs []tenant.Spec) error {
	if err := s.shards.Provision(specs); err != nil {
		return fmt.Errorf("insula: provision: %w", err)
	}
	return nil
}

// Ensure 开通租户（已存在则返回现有的那个）。
//
// 与 Provision 的区别只在重复时：Provision 把重复当成要看见的信号，
// Ensure 把它当成"已经是想要的状态"。自动化流程用后者，运维手工调用
// 用前者——那正是"我想让它是这样"与"我以为它不是这样"的差别。
func (s *Service) Ensure(spec tenant.Spec) (*tenant.Tenant, error) {
	sh, err := s.shards.For(spec.ID)
	if err != nil {
		return nil, fmt.Errorf("insula: ensure %s: %w", spec.ID, err)
	}
	tn, err := sh.Tenants().Ensure(spec)
	if err != nil {
		return nil, fmt.Errorf("insula: ensure %s: %w", spec.ID, err)
	}
	return tn, nil
}

// Deprovision 注销租户：入口子树、进程级单例里的残留、记忆数据一并清掉。
//
// 用 Find 而不是哈希定位（见 shard.Pool.Deprovision）：注销是破坏性操作，
// 它必须作用在租户真正所在的分片上。
func (s *Service) Deprovision(ids []ident.Tenant) error {
	if err := s.shards.Deprovision(ids); err != nil {
		return fmt.Errorf("insula: deprovision: %w", err)
	}
	return nil
}

// SelfCheck 做一次隔离自检。
//
// 它是**抽样哨兵**，不是穷尽证明：平台级隔离靠构造保证（私有域 + 每片
// 独立的 App），自检抓的是构造被破坏这一类回归。详见 shard.Pool.SelfCheck。
func (s *Service) SelfCheck() (shard.SelfCheckReport, error) { return s.shards.SelfCheck() }

// ---------------------------------------------------------------------------
// 只读视图
// ---------------------------------------------------------------------------

// Shards 返回全部分片。
func (s *Service) Shards() []*shard.Shard { return s.shards.Shards() }

// Stats 返回全池状态。
func (s *Service) Stats() []shard.Stats { return s.shards.Stats() }

// Tenants 返回全池租户标识（升序）。O(总数)，不要在请求路径上调用。
func (s *Service) Tenants() []ident.Tenant { return s.shards.Tenants() }

// ShardCount 返回分片数。
func (s *Service) ShardCount() int { return s.shards.Len() }

// Gateway 返回进程级共享的模型池（额度分账与熔断状态在这里）。
func (s *Service) Gateway() *gateway.Pool { return s.gateway }

// Store 返回进程级共享的记忆存储。
func (s *Service) Store() *memory.MemStore { return s.store }

// Metrics 返回指标注册表（Render() 输出 Prometheus 文本）。
func (s *Service) Metrics() *metrics.Registry { return s.metrics }

// Audit 返回审计日志。
func (s *Service) Audit() *audit.Log { return s.audit }

// Admit 返回准入器（Stats() 可看平台级在途与限流情况）。
func (s *Service) Admit() *admit.Platform { return s.admit }
