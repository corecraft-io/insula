// Package edge 是接入层：把一次 HTTP 请求变成一个受鉴权、受准入约束的
// agent run，并把过程与结果回传给调用方。
//
// # 这一层唯一必须做对的事：租户不由请求指定
//
// 接入层是唯一能被外部直接触及的地方，因此它承担了平台侧最容易被写错的
// 一条规则——**租户身份只能从凭据推出来**（设计 §10 硬规则 1）。落地方式
// 比"运行时过滤"更强一级：
//
//   - 请求体的结构体里**根本没有租户字段**（见 RunRequest）。结构体层面
//     不存在该字段，攻击者就无法通过它做任何事——这比"读进来再丢掉"
//     彻底，因为后者依赖每个新接口作者都记得去丢。
//   - 路由里也**没有按租户取资源的路径**。查询自己的状态走
//     /v1/tenants/self，而不是 /v1/tenants/{id}：路径参数是 IDOR 最
//     经典的入口，不给它这个形状，就不存在"忘了校验归属"这种漏洞。
//   - 夹带身份字段的行为仍然会被**检测并审计**（见 detectForgedIdentity）：
//     它会被上面那条结构体规则天然忽略，但夹带本身是要被看见的信号。
//
// # 三层闸门的顺序
//
// 请求体的字节上限 → 鉴权 → 解析请求体 → 准入 → 执行。
//
// 三个顺序决定都是刻意的：
//
//   - **上限在鉴权之前**：`http.MaxBytesReader` 是读取器级别的限制，
//     它不分配内存。没有它的话，一个未鉴权的连接就能让我们分配
//     任意大的缓冲区——而在鉴权之前做这件事正是攻击者想要的。
//   - **准入在解析之后**：并发额度与速率令牌都是每租户的稀缺资源。
//     若在解析前就占用，一个发垃圾 body 的客户端可以凭着"我不读响应"
//     持续占用并发槽位，把同租户的正常请求挤出去。请求体已被字节
//     上限约束，所以解析开销是 O(上限)，这个代价换得的是自己人
//     不被自己人的垃圾请求挡住。
//   - **响应体不解释鉴权失败的原因**："令牌不存在"与"令牌过期"对
//     攻击者是有用信息，对正常客户端没有用——它只能重新认证。
package edge

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/corecraft-io/insula/admit"
	"github.com/corecraft-io/insula/agent"
	"github.com/corecraft-io/insula/audit"
	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/metrics"
	"github.com/corecraft-io/insula/session"
	"github.com/corecraft-io/insula/tenant"
	"github.com/corecraft-io/insula/tools"
)

// ---------------------------------------------------------------------------
// 主体与鉴权
// ---------------------------------------------------------------------------

// Principal 是一次请求已认证的主体。
//
// 它没有"来自请求体的字段"这回事：Tenant 只能由 Authenticate 产生。
type Principal struct {
	Tenant ident.Tenant
	// Actor 是审计里"谁做的"（人 / 服务账号）。
	Actor string
}

// Auth 把凭据解析成主体。
type Auth interface {
	// Authenticate 返回主体；无法认证时返回错误。
	//
	// 实现必须自己做全部判断：接入层**不做**任何"没带凭据就当作匿名"
	// 的兜底——那种兜底一旦存在，就会在某次重构里变成默认放行。
	Authenticate(r *http.Request) (Principal, error)
}

// AuthFunc 是 Auth 的函数适配器。
type AuthFunc func(r *http.Request) (Principal, error)

// Authenticate 实现 Auth。
func (f AuthFunc) Authenticate(r *http.Request) (Principal, error) { return f(r) }

// ErrUnauthenticated 凭据缺失或无效。
var ErrUnauthenticated = errors.New("insula/edge: unauthenticated")

// StaticTokens 是开发与测试用的令牌表：Bearer token → 主体。
//
// 生产部署应当换成可验证的凭据（JWT/JWKS，或由网关注入的 mTLS 主体），
// 但**契约不变**：租户由凭据决定，不由请求体决定。换实现不需要
// 改动接入层的任何一行——这正是把 Auth 做成接口的意义。
type StaticTokens struct {
	mu     sync.RWMutex
	tokens map[string]Principal
	// Header 是承载令牌的请求头名，空为 "Authorization"。
	Header string
}

// NewStaticTokens 构造令牌表。
func NewStaticTokens() *StaticTokens {
	return &StaticTokens{tokens: make(map[string]Principal)}
}

// Put 登记一个令牌。令牌为空或主体缺租户时返回错误。
func (s *StaticTokens) Put(token string, p Principal) error {
	if token == "" {
		return errors.New("insula/edge: empty token")
	}
	if !p.Tenant.Valid() {
		return errors.New("insula/edge: principal must carry a tenant")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[token] = p
	return nil
}

// Revoke 注销一个令牌。
func (s *StaticTokens) Revoke(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tokens, token)
}

// Authenticate 实现 Auth。
//
// 逐项常数时间比较而不是直接查表：直接 map 查询会让"这个令牌是否存在"
// 通过耗时差异泄漏出去，而那正好是枚举令牌时最有用的一个比特。
func (s *StaticTokens) Authenticate(r *http.Request) (Principal, error) {
	token, ok := bearer(r.Header.Get(s.header()))
	if !ok {
		return Principal{}, fmt.Errorf("%w: missing bearer token", ErrUnauthenticated)
	}
	probe := []byte(token)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for cand, p := range s.tokens {
		if subtle.ConstantTimeCompare([]byte(cand), probe) == 1 {
			return p, nil
		}
	}
	return Principal{}, fmt.Errorf("%w: unknown token", ErrUnauthenticated)
}

func (s *StaticTokens) header() string {
	if s.Header == "" {
		return "Authorization"
	}
	return s.Header
}

func bearer(v string) (string, bool) {
	const prefix = "Bearer "
	if len(v) <= len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return "", false
	}
	tok := strings.TrimSpace(v[len(prefix):])
	return tok, tok != ""
}

// ---------------------------------------------------------------------------
// 工作区
// ---------------------------------------------------------------------------

// Workspace 是接入层对租户子系统的**全部**需求。
//
// 刻意收窄成两个方法：接入层不该拿到 Provision / Deprovision 这类管理
// 能力。"一个 HTTP 处理器能注销租户"是纯粹的额外攻击面，而类型系统的
// 作用就是让它在编译期不成立，而不是靠代码评审记得检查。
//
// *shard.Shard 与 *tenant.Manager 都直接满足这个接口。
type Workspace interface {
	// Capabilities 返回租户当前的能力快照。
	Capabilities(t ident.Tenant) (caps.Snapshot, error)
	// Runtime 取或创建会话运行时。
	Runtime(t ident.Tenant, s ident.Session) (*session.Entry, error)
}

// ---------------------------------------------------------------------------
// 请求与响应
// ---------------------------------------------------------------------------

// DefaultSession 是请求未指定会话时使用的会话标识。
const DefaultSession ident.Session = "default"

// RunRequest 是 POST /v1/runs 的请求体。
//
// **这里没有租户字段，也不会加。** 租户来自 Principal（凭据），
// 与工具参数里的身份字段是同一条规则（设计 §10 硬规则 1）的两处体现：
// 任何"调用方说自己是谁"的字段都是越权入口。结构体层面不存在该字段，
// 攻击者就无法通过它做任何事。
type RunRequest struct {
	// Session 会话标识，作用域限于该租户。为空时用 DefaultSession。
	//
	// 传入别人租户下的会话名是**无害**的：会话的入口短 ID 由平台按
	// 分片内序号生成，与调用方给的名字无关（见 tenant.Manager.Session）。
	// 这正是命名空间化 ID 的收益——这里不需要任何归属校验就天然安全。
	Session string `json:"session,omitempty"`
	// Input 本轮用户输入。
	Input string `json:"input"`
	// Model 可选模型名，空由网关决定默认模型。
	Model string `json:"model,omitempty"`
}

// RunResponse 是非流式路径的响应体。
type RunResponse struct {
	Run    string           `json:"run"`
	Output string           `json:"output"`
	Stop   agent.StopReason `json:"stop"`
	Steps  int              `json:"steps"`
	// ToolCalls 本次实际执行的工具调用数。
	ToolCalls int `json:"tool_calls"`
	TokensIn  int `json:"tokens_in"`
	TokensOut int `json:"tokens_out"`
	// Compactions 本次触发的历史压缩次数。
	Compactions int `json:"compactions,omitempty"`
	// Denied 被策略拒绝的工具名。
	Denied []string `json:"denied,omitempty"`
	// Notices 是运行中的非致命告警（压缩失败、请求体夹带身份字段等）。
	//
	// 它必须回传：若降级只写在服务端日志里，调用方会以为一切正常。
	Notices []string `json:"notices,omitempty"`
}

// SelfView 是 GET /v1/tenants/self 的响应。
//
// 只暴露**服务名**与就绪状态，不含任何能力值——那些是句柄。
// 与 Snapshot.String 是同一条纪律：句柄不进任何输出。
type SelfView struct {
	Tenant   string   `json:"tenant"`
	Ready    bool     `json:"ready"`
	Degraded bool     `json:"degraded"`
	Present  []string `json:"present,omitempty"`
	Missing  []string `json:"missing,omitempty"`
	BadType  []string `json:"bad_type,omitempty"`
	Gen      uint64   `json:"gen"`
}

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

// DefaultMaxBodyBytes 是请求体默认上限（1 MiB）。
const DefaultMaxBodyBytes = 1 << 20

// DefaultHeartbeat 是 SSE 心跳默认间隔。
//
// 必须有：中间的反向代理会在静默期把连接掐掉，而一次模型调用可能
// 几十秒不产出任何事件。没有心跳的话，长回答会在代理这一层被拦腰断掉。
const DefaultHeartbeat = 15 * time.Second

// DefaultDrainGrace 是客户端断开后等待 run 收尾的时长。
//
// 它只影响"要不要为这次运行再写一条告警"，不影响正确性：额度由
// defer 归还，与是否等到收尾无关。
const DefaultDrainGrace = 3 * time.Second

// Config 是接入层配置。
type Config struct {
	// Auth 必需。
	Auth Auth
	// Admit 必需。进程级唯一——平台级限额必须是全局的，
	// 每片一个等于把"全平台总量"变成"N 倍总量"。
	Admit *admit.Platform
	// Workspace 必需。
	Workspace Workspace
	// Metrics / Audit 可为 nil。
	Metrics *metrics.Registry
	Audit   *audit.Log

	// SystemPrompt 是平台构造的系统提示。
	//
	// 它**不接受**任何来自请求的输入：System 一旦可以由调用方填充，
	// 就成了最直接的 prompt 注入入口。需要按租户定制时应当从装配配置
	// （tenant.Spec）里取，而不是从请求里取。
	SystemPrompt string

	// MaxBodyBytes 请求体上限。<=0 取 DefaultMaxBodyBytes。
	MaxBodyBytes int64
	// Heartbeat SSE 心跳间隔。<=0 取 DefaultHeartbeat；显式设负数关闭心跳。
	Heartbeat time.Duration
	// DrainGrace 客户端断开后的收尾等待。<=0 取 DefaultDrainGrace。
	DrainGrace time.Duration

	// MaxToolResultChars / MaxToolCallsPerStep / SummarizeMaxTokens
	// 透传给数据面；0 表示用 agent 包的默认值。
	MaxToolResultChars  int
	MaxToolCallsPerStep int
	SummarizeMaxTokens  int

	// ProgressBuffer 是 SSE 进度事件的通道容量。<=0 取 64。
	ProgressBuffer int
	// Clock 注入时钟；nil 表示 time.Now。
	Clock func() time.Time
	// OnWarn 接收非致命告警；可为 nil。
	OnWarn func(msg string, args ...any)

	// heartbeatSet 记录 Heartbeat 是否被显式设为负数（关闭）。
	heartbeatSet bool
}

// WithHeartbeatDisabled 关闭 SSE 心跳。
func (c Config) WithHeartbeatDisabled() Config {
	c.Heartbeat = -1
	c.heartbeatSet = true
	return c
}

func (c Config) normalize() Config {
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if c.Heartbeat == 0 && !c.heartbeatSet {
		c.Heartbeat = DefaultHeartbeat
	}
	if c.DrainGrace <= 0 {
		c.DrainGrace = DefaultDrainGrace
	}
	if c.ProgressBuffer <= 0 {
		c.ProgressBuffer = 64
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

func (c Config) now() time.Time { return c.Clock() }

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server 是接入层的 http.Handler。
type Server struct {
	cfg     Config
	mux     *http.ServeMux
	striper *tools.IdentityStripper

	// droppedEvents 统计因客户端太慢而被丢弃的进度事件数。
	//
	// 丢弃是**正确行为**而不是缺陷：进度事件是尽力而为的，让慢客户端
	// 回头阻塞数据面（进而占住租户并发额度）才是错。这个计数存在的
	// 意义是让"某些客户端看不到过程"这件事可观测，而不是静默发生。
	droppedEvents atomic.Int64
	forged        atomic.Int64
}

// New 构造接入层。
func New(cfg Config) (*Server, error) {
	cfg = cfg.normalize()
	if cfg.Auth == nil {
		return nil, errors.New("insula/edge: Auth is required")
	}
	if cfg.Admit == nil {
		return nil, errors.New("insula/edge: Admit is required")
	}
	if cfg.Workspace == nil {
		return nil, errors.New("insula/edge: Workspace is required")
	}
	s := &Server{
		cfg:     cfg,
		striper: newForgeryDetector(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/runs", s.handleRun)
	mux.HandleFunc("GET /v1/tenants/self", s.handleSelf)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux = mux
	return s, nil
}

// ServeHTTP 实现 http.Handler。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// DroppedEvents 返回累计被丢弃的进度事件数（指标用）。
func (s *Server) DroppedEvents() int64 { return s.droppedEvents.Load() }

// ForgedIdentityAttempts 返回累计检测到的"请求体里夹带身份字段"次数。
//
// 正常客户端不会这么做。非零值意味着有人在试探，而每次试探都应该
// 有一条审计记录（见 handleRun）。
func (s *Server) ForgedIdentityAttempts() int64 { return s.forged.Load() }

// ---------------------------------------------------------------------------
// 处理函数
// ---------------------------------------------------------------------------

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleSelf(w http.ResponseWriter, r *http.Request) {
	p, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	snap, err := s.cfg.Workspace.Capabilities(p.Tenant)
	if err != nil {
		s.writeError(w, p, err)
		return
	}
	writeJSON(w, http.StatusOK, SelfView{
		Tenant:   string(p.Tenant),
		Ready:    snap.Ready(),
		Degraded: snap.Degraded(),
		Present:  snap.Present,
		Missing:  snap.Missing,
		BadType:  snap.BadType,
		Gen:      snap.Gen,
	})
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	// 1) 读取器级别的字节上限。必须在鉴权之前：它不分配内存，
	//    而无上限的 body 读取是一条在鉴权前就成立的内存耗尽路径。
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)

	// 2) 鉴权。租户从此只来自这里。
	p, ok := s.authenticate(w, r)
	if !ok {
		return
	}

	// 3) 解析请求体。放在准入之前：见包注释的顺序说明。
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeStatus(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeStatus(w, http.StatusBadRequest, "request body unreadable")
		return
	}
	var body RunRequest
	if err := json.Unmarshal(raw, &body); err != nil {
		writeStatus(w, http.StatusBadRequest, "malformed JSON body")
		return
	}
	if strings.TrimSpace(body.Input) == "" {
		writeStatus(w, http.StatusBadRequest, "input is required")
		return
	}

	sess := ident.Session(body.Session)
	if !sess.Valid() {
		sess = DefaultSession
	}

	// 4) 夹带身份字段的检测。它会被上面那个"没有该字段的结构体"天然
	//    忽略，但夹带这个行为本身要被看见——它意味着对面在试探。
	forged := detectForgedIdentity(raw, s.striper)
	if len(forged) > 0 {
		s.forged.Add(1)
		s.audit(p, "", audit.ActionIsolationCheck, audit.OutcomeDenied, "", map[string]string{
			"reason": "request body carried identity fields (ignored)",
			"keys":   strings.Join(forged, ","),
		})
	}

	// 5) 准入。非阻塞，超限直接 429，绝不排队。
	permit, err := s.cfg.Admit.Acquire(p.Tenant)
	if err != nil {
		s.writeError(w, p, err)
		return
	}
	defer permit.Release()

	// 6) 执行。
	s.serve(w, r, p, ident.NewRun(), sess, body, forged)
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	p, err := s.cfg.Auth.Authenticate(r)
	if err == nil && p.Tenant.Valid() {
		return p, true
	}
	// 响应体里**不解释**失败原因（见包注释最后一条），
	// 但 WWW-Authenticate 要给对，否则标准客户端不会重试认证。
	w.Header().Set("WWW-Authenticate", `Bearer realm="insula"`)
	writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
	return Principal{}, false
}

// ---------------------------------------------------------------------------
// 执行与回传
// ---------------------------------------------------------------------------

// runOutcome 是数据面 goroutine 的产出。
type runOutcome struct {
	res agent.Result
	err error
}

// event 是一帧进度事件。
type event struct {
	typ  string
	data any
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request, p Principal, run ident.Run,
	sess ident.Session, body RunRequest, forged []string) {

	// 会话运行时是必须的：历史窗口与临时空间都挂在它上面。
	rt, err := s.cfg.Workspace.Runtime(p.Tenant, sess)
	if err != nil {
		s.writeError(w, p, err)
		return
	}

	// 能力快照在 run 开始**前**取一次，整轮不再变：同一次 run 中途
	// 换掉模型网关会让前后行为不一致（见 caps.Snapshot 的注释）。
	snap, err := s.cfg.Workspace.Capabilities(p.Tenant)
	if err != nil {
		s.writeError(w, p, err)
		return
	}

	streaming := wantsStream(r)
	var progress chan event
	if streaming {
		progress = make(chan event, s.cfg.ProgressBuffer)
		if !prepareSSE(w) {
			writeStatus(w, http.StatusInternalServerError, "streaming unsupported by client connection")
			return
		}
		s.writeSSE(w, "accepted", map[string]string{"run": string(run), "session": string(sess)})
	}

	var (
		noteMu  sync.Mutex
		notices []string
	)
	record := func(e audit.Event) {
		if s.cfg.Audit != nil {
			s.cfg.Audit.Record(s.stamp(p, run, e))
		}
		switch e.Action {
		case audit.ActionToolCall, audit.ActionToolDenied:
			if streaming {
				s.push(progress, "tool", map[string]string{
					"run": string(run), "tool": e.Subject, "outcome": string(e.Outcome),
				})
			}
		case audit.ActionCapabilityDown:
			note := e.Detail["msg"]
			noteMu.Lock()
			notices = append(notices, note)
			noteMu.Unlock()
			if streaming {
				s.push(progress, "notice", map[string]string{"run": string(run), "note": note})
			}
		}
	}

	deps := agent.Deps{
		Caps:                snap,
		History:             rt.History,
		Scratch:             rt.Scratch,
		Audit:               record,
		Metrics:             s.counter(p.Tenant),
		MaxToolResultChars:  s.cfg.MaxToolResultChars,
		MaxToolCallsPerStep: s.cfg.MaxToolCallsPerStep,
		SummarizeMaxTokens:  s.cfg.SummarizeMaxTokens,
		Clock:               s.cfg.Clock,
	}

	req := agent.Request{
		Tenant:  p.Tenant,
		Session: sess,
		Run:     run,
		System:  s.cfg.SystemPrompt,
		Input:   body.Input,
		Model:   body.Model,
	}

	done := make(chan runOutcome, 1)
	go func() {
		res, err := agent.Run(r.Context(), deps, req)
		done <- runOutcome{res: res, err: err}
	}()

	if !streaming {
		o := <-done
		if o.err != nil {
			s.writeError(w, p, o.err)
			return
		}
		noteMu.Lock()
		o.res.Notices = append(o.res.Notices, notices...)
		o.res.Notices = append(o.res.Notices, forgedNotice(forged)...)
		noteMu.Unlock()
		writeJSON(w, http.StatusOK, toResponse(run, o.res))
		return
	}

	s.pump(w, r, p, run, done, progress)
}

// pump 在 SSE 路径上搬运进度事件直到 run 结束。
//
// 四个 select 分支的意义各不相同：
//
//   - 客户端断开 → 取消已经通过 r.Context() 传进数据面，这里只等它收尾
//     （有上限，见 DrainGrace），然后走人。额度由 defer 归还，不依赖这里。
//   - run 结束 → **终态由本函数直接写**，不经过事件通道。通道是尽力而为的，
//     而"这次运行最后怎样了"绝不能丢。
//   - 有进度 → 写一帧。写失败（客户端已走）就转去等收尾。
//   - 心跳 → 写一帧 SSE 注释。注释不会被客户端当成事件，但会让连接
//     保持活跃，从而穿过反向代理的空闲超时。
func (s *Server) pump(w http.ResponseWriter, r *http.Request, p Principal, run ident.Run,
	done <-chan runOutcome, progress <-chan event) {

	var beats <-chan time.Time
	if s.cfg.Heartbeat > 0 {
		ticker := time.NewTicker(s.cfg.Heartbeat)
		defer ticker.Stop()
		beats = ticker.C
	}

	for {
		select {
		case <-r.Context().Done():
			s.drain(run, done)
			return

		case o := <-done:
			s.finish(w, run, o)
			return

		case ev := <-progress:
			if !s.writeSSE(w, ev.typ, ev.data) {
				s.drain(run, done)
				return
			}

		case <-beats:
			if !s.writeSSEComment(w, "hb") {
				s.drain(run, done)
				return
			}
		}
	}
}

// drain 等数据面收尾，但有上限。
//
// 不等的话，一次客户端断开就会让 run 的 goroutine 与租户的历史写回
// 变成"没人知道它有没有完成"；无限等的话，一个不遵守取消的模型适配器
// 能把 HTTP 连接永久挂住。上限之内没回来就记一条告警——
// 那是适配器的问题，不是平台的问题，但平台必须让它可见。
func (s *Server) drain(run ident.Run, done <-chan runOutcome) {
	select {
	case <-done:
	case <-time.After(s.cfg.DrainGrace):
		s.cfg.warn("run %s did not return within %s of client disconnect", run, s.cfg.DrainGrace)
	}
}

func (s *Server) finish(w http.ResponseWriter, run ident.Run, o runOutcome) {
	if o.err != nil {
		s.writeSSE(w, "error", map[string]string{"run": string(run), "error": o.err.Error()})
		return
	}
	s.writeSSE(w, "done", toResponse(run, o.res))
}

// push 尽力投递一个进度事件。
//
// 通道满时**丢弃**而不是阻塞：阻塞意味着一个不读响应的客户端能让
// 数据面停下来，而数据面握着租户的并发额度。丢弃一帧进度是遗憾，
// 让一个慢客户端拖住整个租户是故障。
func (s *Server) push(ch chan event, typ string, data any) {
	select {
	case ch <- event{typ: typ, data: data}:
	default:
		s.droppedEvents.Add(1)
	}
}

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------

// prepareSSE 设置 SSE 响应头并立刻把头部刷出去。
//
// 立刻刷头是必要的：客户端要等到第一个字节才认为连接建立了。
// 若不刷，客户端会一直等，而我们的第一个事件可能在模型调用之后。
//
// 返回 false 表示底层连接不支持流式（没有 http.Flusher）。
func prepareSSE(w http.ResponseWriter) bool {
	f, ok := w.(http.Flusher)
	if !ok {
		return false
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	// 关掉 nginx 一类反向代理的响应缓冲：开着的话事件会被攒起来
	// 一次性下发，流式就退化成"最后一次性返回"。
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	f.Flush()
	return true
}

// writeSSE 写一帧事件，返回是否写入成功。
//
// data 必须是 JSON 可序列化的**非敏感**内容：它原样发给客户端。
// 审计事件里的工具名与错误文本可以出去；凭证与身份不可能出现在这里，
// 因为它们的类型层面就不进 audit.Event。
func (s *Server) writeSSE(w http.ResponseWriter, typ string, data any) bool {
	buf, err := json.Marshal(data)
	if err != nil {
		// 序列化失败不该让流断掉：退化成一条说明，继续往下走。
		buf = []byte(`{"error":"event not serializable"}`)
	}
	var b strings.Builder
	b.WriteString("event: ")
	b.WriteString(typ)
	b.WriteString("\ndata: ")
	b.Write(buf)
	b.WriteString("\n\n")
	if _, err := io.WriteString(w, b.String()); err != nil {
		return false
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return true
}

// writeSSEComment 写一帧 SSE 注释（心跳）。
func (s *Server) writeSSEComment(w http.ResponseWriter, text string) bool {
	if _, err := io.WriteString(w, ": "+text+"\n\n"); err != nil {
		return false
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return true
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

func (s *Server) counter(t ident.Tenant) *metrics.Counters {
	if s.cfg.Metrics == nil {
		return nil
	}
	return s.cfg.Metrics.Tenant(t)
}

// stamp 补上接入层才知道、而数据面无从知道的字段：是谁在调用。
//
// 数据面只掌握自己拿到的身份（租户 / 会话 / run），不知道"人"。
// 而"这条 run.start 是谁触发的"正是审计最先要回答的问题——不补这一步，
// 同一个动作经由不同层发出的事件就会长出不同的字段形状：接入层自己发的
// 带 actor，数据面转发的不带。审计消费方只能靠"字段在不在"去猜事件来源，
// 那比字段干脆缺失更糟。
//
// 合并而不是赋值：数据面给的 Detail 里可能有别处拿不到的信息
// （stop / steps / msg），这里只做补齐，不覆盖。
func (s *Server) stamp(p Principal, run ident.Run, e audit.Event) audit.Event {
	detail := make(map[string]string, len(e.Detail)+2)
	for k, v := range e.Detail {
		detail[k] = v
	}
	if p.Actor != "" {
		detail["actor"] = p.Actor
	}
	if run.Valid() {
		detail["run"] = string(run)
	}
	e.Detail = detail
	return e
}

func (s *Server) audit(p Principal, run ident.Run, action audit.Action,
	outcome audit.Outcome, subject string, detail map[string]string) {

	if s.cfg.Audit == nil {
		return
	}
	s.cfg.Audit.Record(s.stamp(p, run, audit.Event{
		At: s.cfg.now(), Tenant: p.Tenant, Action: action,
		Outcome: outcome, Subject: subject, Detail: detail,
	}))
}

// wireFieldNames 返回 RunRequest 里调用方**合法拥有**的 JSON 字段名。
//
// 由结构体标签推导，不手写清册：手写的清册会在有人加字段时静默过期，
// 而这里的过期方式是破坏性的——新字段被当成"夹带身份"，于是每一个正常
// 请求都报一次伪造告警。检测器唯一的资产是信噪比，一旦每个请求都告警，
// 它就从"有人正在试探"退化成了"有人正在使用本接口"。
func wireFieldNames() []string {
	t := reflect.TypeOf(RunRequest{})
	out := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			out = append(out, name)
		}
	}
	return out
}

// newForgeryDetector 构造"请求体夹带身份字段"的检测器。
//
// 键集 = tools 包的默认身份键集 **减去** RunRequest 自己的字段名。
// 同一个名字在两处的含义不同：在工具参数里 `session` 是可疑的（模型没有
// 任何正当理由传它，传了就说明有东西在试图指定身份），在请求体里它却是
// 契约本身（文档明确允许调用方选会话，而且选了也无害——会话入口短 ID
// 由平台按分片内序号生成，与调用方给的名字无关）。
//
// 减完之后剩下的键（tenant_id / user_id / run_id / org_id …）在请求体里
// 没有任何合法用途：RunRequest 里没有对应字段，它们注定被忽略。
// 而忽略这个行为本身要被看见——有人在对着一份不存在的接口试探。
//
// 键集不可能被减空：`tenantid` 等键永远不会是 RunRequest 的字段，
// 因为那正是这个结构体存在的前提（见 TestRunRequestFieldSetIsPinned）。
func newForgeryDetector() *tools.IdentityStripper {
	exempt := make(map[string]bool)
	for _, name := range wireFieldNames() {
		exempt[tools.NormalizeKey(name)] = true
	}
	keys := make([]string, 0, len(tools.DefaultIdentityKeys()))
	for _, k := range tools.DefaultIdentityKeys() {
		if !exempt[tools.NormalizeKey(k)] {
			keys = append(keys, k)
		}
	}
	return tools.NewStripper(keys)
}

// detectForgedIdentity 检查请求体里是否夹带了身份字段。
//
// 返回值只用于审计与提示——请求**不会**因为夹带而被拒绝，也不会被采信：
// RunRequest 里根本没有那些字段，它们本来就影响不了任何事。之所以仍然
// 检测，是因为"有人在试"这件事本身值得被看见；而**拒绝**反而给了试探者
// 一个反馈通道（他能通过状态码判断自己的哪个猜测命中了规则）。
func detectForgedIdentity(raw []byte, striper *tools.IdentityStripper) []string {
	var probe map[string]any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil
	}
	_, rec := striper.Strip(probe)
	if !rec.StrippedAny() {
		return nil
	}
	return rec.Keys
}

func forgedNotice(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	return []string{"请求体包含身份字段，已被忽略：" + strings.Join(keys, ", ")}
}

func wantsStream(r *http.Request) bool {
	q := r.URL.Query().Get("stream")
	if q == "1" || strings.EqualFold(q, "true") {
		return true
	}
	return strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/event-stream")
}

func toResponse(run ident.Run, res agent.Result) RunResponse {
	return RunResponse{
		Run:         string(run),
		Output:      res.Output,
		Stop:        res.Stop,
		Steps:       res.Steps,
		ToolCalls:   res.ToolCalls,
		TokensIn:    res.TokensIn,
		TokensOut:   res.TokensOut,
		Compactions: res.Compactions,
		Denied:      res.Denied,
		Notices:     res.Notices,
	}
}

// errorStatus 把下层错误映射成 HTTP 状态码。
//
// 映射表刻意保持小且集中：它的每一行都是"这个错误该不该让调用方知道"
// 的一次决定，散落在各处就没法一致地审。
func errorStatus(err error) (int, string) {
	if le, ok := admit.AsLimit(err); ok {
		// 429 由调用方补 Retry-After（见 writeError）。
		return http.StatusTooManyRequests, "rate limit exceeded: " + string(le.Reason)
	}
	switch {
	case errors.Is(err, tenant.ErrNoTenant):
		return http.StatusNotFound, "tenant is not provisioned on this instance"
	case errors.Is(err, tenant.ErrTenantDisabled):
		return http.StatusForbidden, "tenant is disabled"
	case errors.Is(err, admit.ErrClosed):
		return http.StatusServiceUnavailable, "service is shutting down"
	case errors.Is(err, agent.ErrCapabilityUnavailable):
		return http.StatusServiceUnavailable, "capability unavailable"
	case errors.Is(err, agent.ErrBadIdentity):
		return http.StatusBadRequest, "invalid identity"
	}
	return http.StatusInternalServerError, "internal error"
}

func (s *Server) writeError(w http.ResponseWriter, p Principal, err error) {
	status, msg := errorStatus(err)
	if status == http.StatusTooManyRequests {
		// Retry-After 必须给：没有它，客户端会在收到 429 后立刻重试，
		// 把限额持续烧穿，而每次重试都要再走一遍完整的鉴权与解析。
		w.Header().Set("Retry-After", strconv.Itoa(RetryAfter))
	}
	if status == http.StatusInternalServerError {
		// 服务端错误必须留痕：调用方拿到 5xx 时通常无法提供任何有用的
		// 上下文，只有平台侧的日志能说清是什么坏了。
		s.cfg.warn("run request failed: %v", err)
	}
	if status == http.StatusTooManyRequests {
		s.audit(p, "", audit.ActionRunRejected, audit.OutcomeDenied, "", map[string]string{"reason": msg})
		if c := s.counter(p.Tenant); c != nil {
			c.RunsRejected.Add(1)
		}
	}
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/json; charset=utf-8")
	}
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeStatus 是**还没有 Principal** 的那些失败路径的出口。
//
// 它与 writeError 的区别不是格式而是审计能力：这条路上我们连"是谁"
// 都不知道（鉴权前、或 body 都没解析出来），因此既不能记账到租户头上，
// 也无法在审计里留下有意义的 actor。
//
// 它刻意是个**普通函数**而不是 Server 的方法：没有 receiver 就说明
// "这里用不到任何服务端状态"——而这正是"这里没有调用方身份"的同一个
// 事实。做成方法的话，将来总会有一次"顺手在 s 上拿点东西"的改动，
// 而那一步八成就把 p 也一起带进来了。
func writeStatus(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// RetryAfter 是 429 响应上 Retry-After 的秒数。
//
// 取 1 秒而不是更长：额度按租户分桶、按秒补充，1 秒足以让"踩到瞬时上限"
// 的客户端在下一次尝试时落在新的一桶里；再大会平白增加正常客户端的延迟。
const RetryAfter = 1
