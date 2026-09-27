// Package sandbox 是工具与代码的执行通道。
//
// # 先说清楚它不是什么
//
// 本包里的 Direct 执行器**在进程内直接调用**，因此它**不是安全边界**。
// 这是设计文档 §10 已经写明的立场：cordis 是进程内组件库，同一个进程里的
// Go 代码可以通过反射、unsafe、全局单例绕过任何域机制；Go 语言层面
// 无法阻止这一点。任何声称"用接口包一层就安全了"的说法都是错的。
//
// Direct 的定位是**传输通道**：它把一次调用需要的输入整理好、把不需要的
// 东西（凭证）结构性排除掉、把输出截断到有界大小。它适用于信任一级
// （平台自研工具）与信任二级（经能力清单评审的适配器）。
//
// # 那不受信的代码怎么办
//
// 信任三级（用户上传的工具代码、任意代码执行类工具、外部 MCP 服务）
// **必须进程外**：gRPC 或 WASM 沙箱，无凭证、无网络或白名单出口，
// 进程内只留一个 per-tenant 的代理持有身份。
//
// 本包不假装实现了它。取而代之的是提供 Closed：一个 fail-closed 的
// 执行器，任何调用都返回 ErrNoSandbox。装配层在未配置真实沙箱时必须
// 用它，而不是"暂时先用 Direct 顶一下"——那个"暂时"是最常见的
// 生产事故来源。Manager 的默认构造就是这个行为。
//
// # 凭证为什么不在 Request 里
//
// Request 里没有凭证字段，且永远不会有。沙箱进程不需要知道自己是
// 哪个租户，也不知道任何凭证：身份由宿主侧的代理在调用时注入，
// 凭证由代理在贴到 HTTP 请求那一行才兑换。这是 SAFETY.md 硬规则 2
// 在类型层面的体现——不是"记得别传"，而是"没有东西可传"。
package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/metaRobin/insula/caps"
)

// ErrNoSandbox 未配置执行通道，调用被 fail-closed 拒绝。
var ErrNoSandbox = errors.New("insula/sandbox: no execution channel configured")

// DefaultMaxOutputBytes 是输出硬上限。一个工具可以返回 100MB，
// 不截断就是一条内存耗尽路径，而且这些内容最终还要进模型上下文。
const DefaultMaxOutputBytes = 256 << 10 // 256 KiB

// Request 一次执行请求。
//
// 注意字段的顺序与注释是刻意的：Invocation 在最前，因为它携带的是
// **宿主注入的身份**，而不是模型提供的参数。任何实现都必须以它为准。
type Request struct {
	Invocation caps.Invocation

	// Tool 是要执行的工具名（用于审计与路由）。
	Tool string
	// Code 仅信任三级使用：待执行的代码。信任一/二级为空。
	Code string
	// Input 是**已剥离身份字段**的输入。
	Input map[string]any
	// Timeout 覆盖默认超时。
	Timeout time.Duration
	// MaxOutputBytes 覆盖默认输出上限。
	MaxOutputBytes int
}

// Result 一次执行结果。
type Result struct {
	Output   string
	ExitCode int
	Duration time.Duration
	// Truncated 报告输出是否因超过上限而被截断。
	// 截断必须显式可见：静默截断会让模型基于残缺信息继续推理。
	Truncated bool
}

// Executor 是执行通道。
type Executor interface {
	// Run 执行一次请求。
	//
	// 实现必须保证：不向被测代码暴露凭证、不把身份从 Input 里读取
	// （只读 Request.Invocation）、输出不超过上限。
	Run(ctx context.Context, req Request) (Result, error)
	// Describe 返回一句人类可读的描述，用于启动日志与能力清单。
	Describe() string
}

// ---------------------------------------------------------------------------
// Closed：fail-closed 执行器
// ---------------------------------------------------------------------------

// Closed 拒绝一切调用。
//
// 它是**默认值**而不是异常态：未显式配置执行通道时，平台宁愿让工具
// 调用失败，也不愿让不受信代码在进程内跑起来。失败是可观测的，
// 而"跑起来了但没人知道"不是。
type Closed struct {
	// Reason 说明为什么关闭，会出现在错误消息里帮助定位配置问题。
	Reason string
}

// Run 实现 Executor。
func (c *Closed) Run(context.Context, Request) (Result, error) {
	reason := c.Reason
	if reason == "" {
		reason = "no execution channel configured"
	}
	return Result{}, fmt.Errorf("%w: %s", ErrNoSandbox, reason)
}

// Describe 实现 Executor。
func (c *Closed) Describe() string {
	if c.Reason == "" {
		return "closed (fail-closed: no execution channel)"
	}
	return "closed (" + c.Reason + ")"
}

// ---------------------------------------------------------------------------
// Direct：进程内直通
// ---------------------------------------------------------------------------

// Runner 是进程内的实际执行函数。
type Runner func(ctx context.Context, req Request) (Result, error)

// Direct 在进程内直接执行。它适用于信任一/二级，**不适用于信任三级**。
type Direct struct {
	runner         Runner
	maxOutputBytes int
}

// NewDirect 构造直通执行器。
func NewDirect(runner Runner, maxOutputBytes int) *Direct {
	if runner == nil {
		panic("insula/sandbox: NewDirect requires a non-nil runner")
	}
	if maxOutputBytes <= 0 {
		maxOutputBytes = DefaultMaxOutputBytes
	}
	return &Direct{runner: runner, maxOutputBytes: maxOutputBytes}
}

// Run 实现 Executor。
func (d *Direct) Run(ctx context.Context, req Request) (Result, error) {
	if !req.Invocation.Tenant.Valid() {
		// 身份缺失意味着这次调用无法被归属到任何租户，也就无法审计。
		// 在归属不清的情况下执行是最坏的选择。
		return Result{}, fmt.Errorf("insula/sandbox: empty tenant identity")
	}
	start := time.Now()
	res, err := d.runner(ctx, req)
	res.Duration = time.Since(start)
	if err != nil {
		return res, err
	}

	limit := d.maxOutputBytes
	if req.MaxOutputBytes > 0 {
		limit = req.MaxOutputBytes
	}
	if len(res.Output) > limit {
		res.Output = res.Output[:limit]
		res.Truncated = true
	}
	return res, nil
}

// Describe 实现 Executor。
func (d *Direct) Describe() string {
	return fmt.Sprintf("direct in-process (trust tier 1-2 only, max output %d bytes)", d.maxOutputBytes)
}

// ---------------------------------------------------------------------------
// Tier 与 Manager
// ---------------------------------------------------------------------------

// Tier 是能力清单里的信任等级（对应设计 §10）。
type Tier int

const (
	// TierTrusted 平台自研插件、核心 agent 循环、模型网关适配器。
	TierTrusted Tier = iota
	// TierSemi 第三方工具适配器、企业内部团队贡献的插件。
	TierSemi
	// TierUntrusted 用户上传的工具代码、任意代码执行类工具、外部 MCP 服务。
	TierUntrusted
)

// String 便于日志与配置。
func (t Tier) String() string {
	switch t {
	case TierTrusted:
		return "trusted"
	case TierSemi:
		return "semi-trusted"
	case TierUntrusted:
		return "untrusted"
	default:
		return "unknown"
	}
}

// AllowsInProcess 报告该信任等级是否允许在进程内执行。
//
// 三级一律 false，且这条规则不提供开关：它对应的是"进程内无法阻止
// 恶意 Go 代码"这个语言事实，不是一条可协商的政策。
func (t Tier) AllowsInProcess() bool {
	return t == TierTrusted || t == TierSemi
}

// Manager 按信任等级分派执行通道。
type Manager struct {
	mu         sync.RWMutex
	inProcess  Executor
	outProcess Executor

	// UntrustedTier 是判定"必须进程外"的阈值，默认 TierUntrusted。
	UntrustedTier Tier
}

// NewManager 构造分派器。
//
// outProcess 为 nil 时落到 Closed——这是刻意的默认：未配置进程外通道时，
// 不受信的调用必须失败，而不是退回进程内。
func NewManager(inProcess, outProcess Executor) *Manager {
	if inProcess == nil {
		inProcess = &Closed{Reason: "no in-process executor configured"}
	}
	if outProcess == nil {
		outProcess = &Closed{Reason: "no out-of-process sandbox configured; " +
			"untrusted tools are disabled by default"}
	}
	return &Manager{inProcess: inProcess, outProcess: outProcess, UntrustedTier: TierUntrusted}
}

// SetOutOfProcess 装配期注入真实的沙箱通道（如 gRPC / WASM 适配器）。
func (m *Manager) SetOutOfProcess(e Executor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e == nil {
		e = &Closed{Reason: "out-of-process sandbox was cleared"}
	}
	m.outProcess = e
}

// ExecutorFor 返回给定信任等级应当使用的通道。
func (m *Manager) ExecutorFor(t Tier) Executor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t >= m.UntrustedTier {
		return m.outProcess
	}
	return m.inProcess
}

// Run 按信任等级分派一次执行。
func (m *Manager) Run(ctx context.Context, t Tier, req Request) (Result, error) {
	return m.ExecutorFor(t).Run(ctx, req)
}

// Describe 返回各通道的描述，用于启动日志与能力清单评审。
func (m *Manager) Describe() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]string{
		"in-process":  m.inProcess.Describe(),
		"out-process": m.outProcess.Describe(),
	}
}
