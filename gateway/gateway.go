// Package gateway 是进程级共享的模型池，以及暴露给租户隔离域的瘦句柄。
//
// # 为什么池必须在 cordis 之外
//
// 模型连接池、限流、凭证缓存是**跨租户共享**的资源。cordis 的隔离域
// 机制恰恰是为了让同名服务互不可见，用它来共享资源只有两条路：
// 声明共享域（"@label"，平台层禁用），或者把池注册成平台级单例。
// 本包走后者——池是普通 Go 对象，cordis 里只注册一个只含
// {pool, tenant} 两个字段的瘦句柄。这对应 SAFETY.md 硬规则 4。
//
// 有一个副作用很有价值：池不经 cordis，因此**网关插件热重载时池不受影响**。
// 句柄重建了，但它指向的还是同一个池，在途请求不会被打断。
//
// # 反应式降级：check 只在解析时求值
//
// cordis 的 Provide 接受一个 check 回调，check 失败时依赖者视为依赖未满足
// （见 cordis/fiber.go 的 checkImpl）。用它可以把「模型池不可用」直接表达为
// 「依赖它的 fiber 自动卸载」。
//
// 但有一个必须记住的坑：**check 只在依赖解析时被求值，它不是一个轮询器**。
// 健康度翻转后如果没有任何东西触发重新解析，依赖者会一直停留在旧状态。
// cordis 自己的测试里是靠 db.Update(nil) 手动触发的。
//
// 所以本包提供 Publish：它把已登记的网关 fiber 逐个 Update 一遍，
// 迫使 cordis 重新解析依赖、重新求值 check。调用点是 Complete 的收尾
// （真实流量驱动的翻转能立刻生效），以及运维手工调用。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/corecraft-io/cordis"
	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/creds"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/realm"
)

// Feature 是可选能力开关。
//
// 它表达的是「降级时允许失去什么」。与 check 的区别：check 是硬开关
// （池挂了，依赖者必须卸载），Feature 是软开关（池还在，但这次不调模型，
// 比如用来做只读巡检或演练）。
type Feature struct {
	// ModelsEnabled 关闭时 Complete 直接返回 ErrDisabled，不打上游。
	ModelsEnabled bool
}

// ErrUnavailable 模型池当前不可用（熔断打开或运维下线）。
var ErrUnavailable = errors.New("insula/gateway: model pool unavailable")

// ErrDisabled 该租户的模型能力被显式关闭。
var ErrDisabled = errors.New("insula/gateway: model capability disabled")

// Upstream 是模型后端适配器（信任一级：平台自研）。
//
// 它拿到的是凭证**句柄**而不是明文：兑换明文的动作发生在适配器内部
// 紧贴 HTTP 请求的那一行，明文不经过网关的任何中间变量，也就不会被
// 顺手放进日志字段或错误消息。
//
// tenant 必须显式传入而不是让适配器从 context 里推导——这是 ADR-005
// 在全平台的一致体现，也让适配器可以把租户标签打进上游的计量与审计。
type Upstream interface {
	Complete(ctx context.Context, tenant ident.Tenant, req caps.Request, cred *creds.Handle) (caps.Response, error)
}

// Config 池的构造参数。
type Config struct {
	// Upstream 模型后端适配器。nil 时 Complete 返回 ErrUnavailable。
	Upstream Upstream
	// Creds 凭证签发者。nil 表示该池不需要凭证（如本地回环模型）。
	Creds creds.Provider
	// CredScope 凭证作用域（默认 "model-api"）。
	CredScope string
	// Quota 新租户的默认额度。
	Quota Quota
	// Quotas 按租户覆盖额度。
	Quotas map[ident.Tenant]Quota
	// PlatformQuota 全平台总量上限。
	//
	// 租户级配额可以全都合规，但 1000 个租户一起打满，共享上游照样枯竭。
	// 两层限额都要有，且两者的粒度不同：租户级是「谁用得太多」，
	// 平台级是「大家加起来太多」。
	PlatformQuota Quota
	// Breaker 熔断参数。
	Breaker BreakerConfig
	// Clock 注入时钟。
	Clock func() time.Time
	// OnWarn 接收非致命的装配期告警（如未配置凭证）。nil 时静默。
	OnWarn func(msg string, args ...any)
}

// Pool 是进程级共享的模型池。它必须是单例：每个分片各建一个池
// 就等于把「共享上游」这件事做成了 N 份，配额与熔断都不再是全局的。
type Pool struct {
	upstream  Upstream
	credScope string
	credProv  creds.Provider
	breaker   *Breaker
	clock     func() time.Time
	onWarn    func(msg string, args ...any)

	fallback Quota
	mu       sync.RWMutex
	quotas   map[ident.Tenant]Quota
	accounts map[ident.Tenant]*Account
	credents map[ident.Tenant]*credEntry
	watchers map[*cordis.Fiber]struct{}

	platform *Account

	// 以下三个用原子量：Healthy 会在数据面被高频调用，不能拿锁。
	forcedDown atomic.Bool
	unhealthy  atomic.Bool
	gen        atomic.Uint64
	// pending 标记「自上次 Publish 以来健康度发生过翻转」。
	pending atomic.Bool
}

type credEntry struct {
	handle   *creds.Handle
	issuedAt time.Time
}

// New 构造模型池。
func New(cfg Config) *Pool {
	if cfg.CredScope == "" {
		cfg.CredScope = "model-api"
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.OnWarn == nil {
		cfg.OnWarn = func(string, ...any) {}
	}

	p := &Pool{
		upstream:  cfg.Upstream,
		credScope: cfg.CredScope,
		credProv:  cfg.Creds,
		clock:     cfg.Clock,
		onWarn:    cfg.OnWarn,
		fallback:  cfg.Quota,
		quotas:    make(map[ident.Tenant]Quota, len(cfg.Quotas)),
		accounts:  make(map[ident.Tenant]*Account),
		credents:  make(map[ident.Tenant]*credEntry),
		watchers:  make(map[*cordis.Fiber]struct{}),
		platform:  NewAccount(cfg.PlatformQuota, cfg.Clock),
	}
	for t, q := range cfg.Quotas {
		p.quotas[t] = q
	}

	// 熔断状态迁移 → 更新健康度原子量。这里**只**写原子量、不碰锁，
	// 因为 OnChange 是在 Breaker 持锁时被调用的；若在此处再取 Pool 的锁，
	// 就与 Healthy 的读路径形成锁序反转。
	//
	// 「不健康」定义为状态**不等于 Closed**：HalfOpen 虽然在放探测，
	// 但它对依赖者而言仍是不可依赖的——让依赖者此时上线只会收到一堆
	// ErrUnavailable。只有当探测成功、回到 Closed 时才把依赖者放回来。
	//
	// 另外只在健康度**真的变化**时递增代次并置待发布标记：
	// Open → HalfOpen 不改变健康度，却会让每个网关 fiber 白走一遍
	// 卸载-重载循环。
	bc := cfg.Breaker
	inner := bc.OnChange
	bc.OnChange = func(from, to State) {
		if old := p.unhealthy.Swap(to != StateClosed); old != (to != StateClosed) {
			p.gen.Add(1)
			p.pending.Store(true)
		}
		if inner != nil {
			inner(from, to)
		}
	}
	bc.Clock = cfg.Clock
	p.breaker = NewBreaker(bc)
	return p
}

// Breaker 暴露熔断器，供指标与运维使用。
func (p *Pool) Breaker() *Breaker { return p.breaker }

// Healthy 报告池是否可用。无锁，可被数据面高频调用。
func (p *Pool) Healthy() bool { return !p.forcedDown.Load() && !p.unhealthy.Load() }

// Gen 返回健康度代次，每次翻转递增。
func (p *Pool) Gen() uint64 { return p.gen.Load() }

// SetEnabled 运维开关。false 时所有租户的模型调用立即被拒。
func (p *Pool) SetEnabled(enabled bool) {
	p.forcedDown.Store(!enabled)
	p.gen.Add(1)
	p.pending.Store(true)
}

// Account 返回（必要时创建）某租户的额度账户。
func (p *Pool) Account(t ident.Tenant) *Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accountLocked(t)
}

func (p *Pool) accountLocked(t ident.Tenant) *Account {
	if a, ok := p.accounts[t]; ok {
		return a
	}
	q, ok := p.quotas[t]
	if !ok {
		q = p.fallback
	}
	a := NewAccount(q, p.clock)
	p.accounts[t] = a
	return a
}

// SetQuota 调整某租户的额度，立即生效。
func (p *Pool) SetQuota(t ident.Tenant, q Quota) {
	p.mu.Lock()
	p.quotas[t] = q
	a := p.accountLocked(t)
	p.mu.Unlock()
	a.SetQuota(q)
}

// Forget 清理某租户的账户与凭证缓存（租户注销时调用）。
//
// 必须做：账户与凭证缓存都按租户累积，不清理就是一条无界增长路径，
// 而且会把注销租户的凭证句柄继续留在内存里。
func (p *Pool) Forget(t ident.Tenant) {
	p.mu.Lock()
	delete(p.accounts, t)
	delete(p.quotas, t)
	delete(p.credents, t)
	p.mu.Unlock()
	if p.credProv != nil {
		p.credProv.Revoke(t)
	}
}

// credential 返回租户的凭证句柄，必要时重新签发。
//
// 缓存而不是每次调用都 Issue：句柄兑现前的生命周期都记在凭证提供者的
// live 表里，每次调用签发一个会让那张表随调用量无界增长。
// 刷新阈值取句柄自身寿命的 4/5，因此与具体 TTL 无关。
func (p *Pool) credential(t ident.Tenant) (*creds.Handle, error) {
	if p.credProv == nil {
		return nil, nil
	}
	now := p.clock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.credents[t]; ok {
		lifetime := e.handle.ExpiresAt().Sub(e.issuedAt)
		remaining := e.handle.ExpiresAt().Sub(now)
		if remaining > 0 && remaining > lifetime/5 {
			return e.handle, nil
		}
	}
	h, err := p.credProv.Issue(t, p.credScope)
	if err != nil {
		return nil, err
	}
	p.credents[t] = &credEntry{handle: h, issuedAt: now}
	return h, nil
}

// Complete 执行一次模型调用，串起熔断、额度、凭证三件事。
//
// 顺序是有讲究的：
//
//  1. 额度检查放最前——额度用尽时不该消耗熔断器的探测额度，
//     也不该产生凭证签发。它是**只读**判断，因此不产生副作用；
//  2. 运维硬闸门紧随其后、在任何预占之前。它同样是只读判断；
//     若放在预占之后，「拒绝」本身就会烧掉租户额度（见下）。
//  3. 熔断检查在凭证之前——池已判定不可用时不要做任何多余的动作；
//  4. 只有真正打到上游的失败才计入熔断。凭证未配置是配置错误，
//     把它计入熔断会让一个租户的配置疏漏把整个池熔掉。
//
// 另有一条不是顺序、而是纪律：**预占之后没能发出请求，必须归还额度**。
// 租户额度、平台额度、熔断探测额度三者都是「先占后用」，中间每一条
// 早退路径都对应一次需要撤销的副作用。归还统一由下面那**一个** defer
// 完成，不在各分支里手写：手写会漏，而漏掉的表现是「拒绝一次、
// 扣一次额度」——它在代码评审里几乎看不出来，要等上游故障恢复之后
// 租户报「还是用不了」才会暴露。
func (p *Pool) Complete(ctx context.Context, t ident.Tenant, req caps.Request) (caps.Response, error) {
	var zero caps.Response
	if !t.Valid() {
		return zero, fmt.Errorf("%w: empty tenant identity", ErrUnavailable)
	}
	if p.upstream == nil {
		return zero, ErrUnavailable
	}

	// 这两个判断都只读，必须在任何 Reserve 之前。
	acct := p.Account(t)
	if acct.Exhausted() {
		return zero, &ErrQuotaExceeded{Tenant: t, Quota: acct.Quota(),
			Used: acct.Usage(), Reason: "token or call budget exhausted"}
	}
	if p.forcedDown.Load() {
		return zero, ErrUnavailable
	}

	if err := p.platform.Reserve(); err != nil {
		return zero, &ErrQuotaExceeded{Tenant: t, Quota: p.platform.Quota(),
			Used: p.platform.Usage(), Reason: "platform-wide budget exhausted"}
	}

	// 平台额度已预占。此后每一条早退路径都要把它（以及下面陆续占到的
	// 租户额度、探测额度）还回去。三个布尔量分别对应「占到了什么」，
	// 各自的 false 分支都有真实场景，不能省：
	//   tenantReserved —— 租户额度是自己占的，未占到就自减会偷走
	//                     别的并发调用的预占；
	//   probe          —— Allow 返回 false 的原因之一正是「探测已被
	//                     别的调用占用」，那时归还等于放出第二个探测，
	//                     破坏 HalfOpen「只放一个」的定义。
	tenantReserved, probe, refundable := false, false, true
	defer func() {
		if !refundable {
			return
		}
		p.platform.Release()
		if tenantReserved {
			acct.Release()
		}
		if probe {
			p.breaker.ReleaseProbe()
		}
	}()

	if err := acct.Reserve(); err != nil {
		return zero, err
	}
	tenantReserved = true

	// 数据面**不能**用 Healthy() 做前置闸门：Healthy 只在熔断回到 Closed
	// 时复位，而回到 Closed 必须靠 Allow() 放行一次探测。若在这里先拦一道
	// Healthy()，探测永远拿不到机会，池打开后就再也无法通过流量自愈。
	// 因此放行判断统一交给 breaker.Allow()，它已经编码了 Open/HalfOpen 的
	// 全部语义；forcedDown 是运维开关，它才是真正的硬闸门。
	if !p.breaker.Allow() {
		return zero, ErrUnavailable
	}
	probe = true

	cred, err := p.credential(t)
	if err != nil {
		// 配置错误不算上游故障，因此不报给熔断器——但也**不能就这么
		// 走掉**：那会把探测额度永久留在占用态（见 Breaker.ReleaseProbe）。
		p.onWarn("tenant %s has no usable model credential: %v", t, err)
		return zero, err
	}

	// 越过这条线，请求真的发出去了：额度与探测额度都算花掉。上游即使
	// 失败也不归还——那次调用确实发生过，熔断器由 Failure() 记账。
	refundable = false

	res, err := p.upstream.Complete(ctx, t, req, cred)
	if err != nil {
		p.breaker.Failure()
		p.Publish()
		return zero, err
	}
	p.breaker.Success()
	acct.Commit(int64(res.PromptTokens), int64(res.CompletionTokens))
	p.platform.Commit(int64(res.PromptTokens), int64(res.CompletionTokens))
	p.Publish()
	return res, nil
}

// ---------------------------------------------------------------------------
// cordis 集成
// ---------------------------------------------------------------------------

// Handle 是暴露给租户隔离域的瘦句柄。它只含两个字段：池的引用与租户标识。
//
// 它实现 caps.Models，因此可以直接注册为 realm.ServiceModels。
// 它是**值语义安全**的：句柄本身无状态，所以网关插件热重载、
// 甚至一个租户同时存在两个句柄，都不会产生不一致。
type Handle struct {
	pool   *Pool
	tenant ident.Tenant
}

// Compile-time 断言：瘦句柄必须满足数据面契约。
var _ caps.Models = (*Handle)(nil)

// Tenant 返回句柄绑定到的租户。
func (h *Handle) Tenant() ident.Tenant { return h.tenant }

// Complete 实现 caps.Models。
func (h *Handle) Complete(ctx context.Context, req caps.Request) (caps.Response, error) {
	return h.pool.Complete(ctx, h.tenant, req)
}

// Healthy 实现 caps.Models。
func (h *Handle) Healthy() bool { return h.pool.Healthy() }

// Handle 返回绑定到某租户的瘦句柄。
func (p *Pool) Handle(t ident.Tenant) *Handle { return &Handle{pool: p, tenant: t} }

// Plugin 返回网关插件**定义**（一个进程内一份），供 loader 的
// 插件解析表使用。
//
// 租户身份从入口 Config 传入（`Config: pool.Handle(tenant)`）。
// 这是插件必须长成的形状：loader 的 resolvePlugin(name) 是静态映射，
// 「每个租户一个插件实例」挂不上入口树。
//
// check 用池的 Healthy：池不可用时 cordis 把依赖这个服务的 fiber 判为
// 依赖未满足并撤销它们——这就是反应式降级，业务代码里不必写任何
// if unhealthy 分支。check 的求值时机见包注释：它需要 Publish 触发，
// 因此插件在 Apply 里把自己登记进 Watch。
func Plugin() *cordis.Plugin {
	return &cordis.Plugin{
		Name: realm.PluginModelGateway,
		Validate: func(cfg any) (any, error) {
			h, ok := cfg.(*Handle)
			if !ok || h == nil {
				return nil, fmt.Errorf("%w: model-gateway config must be a non-nil *gateway.Handle, got %T",
					ErrUnavailable, cfg)
			}
			return h, nil
		},
		Apply: applyHandle,
	}
}

// Provider 返回把瘦句柄注册进租户隔离域的插件（**命令式**挂载用）。
//
// 经 loader 装配时应改用 Plugin() + 入口 Config；这里保留一个把租户
// 闭包进去的形态，供 ctx.Plugin(...) 直接挂载与测试使用。
func (p *Pool) Provider(t ident.Tenant) *cordis.Plugin {
	return &cordis.Plugin{
		Name: realm.PluginModelGateway,
		Apply: func(ctx *cordis.Context, _ any) error {
			return applyHandleFor(ctx, p.Handle(t))
		},
	}
}

// applyHandle 是网关插件的公共实现体：从 Config 取句柄并注册服务。
func applyHandle(ctx *cordis.Context, cfg any) error {
	h, ok := cfg.(*Handle)
	if !ok || h == nil {
		return fmt.Errorf("%w: model-gateway config must be a non-nil *gateway.Handle, got %T",
			ErrUnavailable, cfg)
	}
	return applyHandleFor(ctx, h)
}

// applyHandleFor 以给定句柄注册服务并登记健康度观察。
func applyHandleFor(ctx *cordis.Context, h *Handle) error {
	if _, err := ctx.Provide(realm.ServiceModels, h, h.pool.Healthy); err != nil {
		return err
	}
	_, err := ctx.Effect("gateway.watch", func() (cordis.Dispose, error) {
		return h.pool.Watch(ctx.Fiber()), nil
	})
	return err
}

// Watch 登记一个网关 fiber，使它在健康度翻转时被重新解析。
//
// 返回的 Dispose 必须在 fiber 卸载时调用，否则池会持有一个已销毁的
// fiber 指针并在下次翻转时对它调用 Update（会返回错误，但仍是泄漏）。
// 典型用法是在 Provider 的 Apply 里用 ctx.Effect 登记。
func (p *Pool) Watch(f *cordis.Fiber) cordis.Dispose {
	p.mu.Lock()
	p.watchers[f] = struct{}{}
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		delete(p.watchers, f)
		p.mu.Unlock()
	}
}

// Publish 在健康度发生变化时触发依赖重解析。
//
// 它做的是对每个已登记的网关 fiber 调用 Update(nil)，走一遍完整的
// 卸载-重载循环，从而让 cordis 重新求值 check。这很重，但健康度翻转
// 是罕见事件；更重要的是它换来一个强保证：check 的结果永远不会
// 停留在过期状态。
//
// 幂等且廉价：没有待发布的翻转时立即返回。可安全地在数据面高频调用。
func (p *Pool) Publish() {
	if !p.pending.CompareAndSwap(true, false) {
		return
	}
	p.mu.RLock()
	targets := make([]*cordis.Fiber, 0, len(p.watchers))
	for f := range p.watchers {
		targets = append(targets, f)
	}
	p.mu.RUnlock()

	for _, f := range targets {
		// Update 是异步投递到调度器的，因此这里不会阻塞数据面。
		// 已销毁的 fiber 会返回错误，那是预期内的竞态，忽略即可。
		_ = f.Update(nil)
	}
}

// ---------------------------------------------------------------------------
// 统计
// ---------------------------------------------------------------------------

// Stats 是池的只读快照。
type Stats struct {
	Healthy  bool
	Gen      uint64
	State    State
	Failures int
	Accounts int
}

// Stats 返回当前统计（不含任何租户明细，避免日志里出现租户清单）。
func (p *Pool) Stats() Stats {
	p.mu.RLock()
	n := len(p.accounts)
	p.mu.RUnlock()
	return Stats{
		Healthy:  p.Healthy(),
		Gen:      p.Gen(),
		State:    p.breaker.State(),
		Failures: p.breaker.Failures(),
		Accounts: n,
	}
}
