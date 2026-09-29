// Package metrics 提供租户级 RED 指标（Rate / Errors / Duration）。
//
// # 为什么不用现成的指标库
//
// cordis 自身零第三方依赖，平台侧沿用这条约定——整个依赖图只有 cordis 一个模块，
// 让依赖可以整条读完。一个多租户平台的指标层如果引入外部客户端，就等于给每个租户的
// 请求路径上多挂一个未知行为的全局单例——而那正是 SAFETY.md 里
// 「跨租户共享通道」要重点防范的形态。
//
// 本包只做无锁计数与固定桶直方图，导出交给接入层（/metrics 端点）。
package metrics

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/corecraft-io/insula/ident"
)

// 直方图默认桶上界，覆盖「一次 agent run」的合理区间。
var defaultBounds = []time.Duration{
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	time.Minute,
}

// Histogram 固定桶直方图。桶上界之外的值计入最后一个溢出桶。
//
// 分位数是桶上界的近似（返回「命中比例首次达到 q 的那个桶的上界」），
// 对容量规划与告警阈值足够，不要用它做精确计费。
type Histogram struct {
	bounds []time.Duration
	counts []int64 // len(bounds)+1
	sum    time.Duration
	total  int64
	mu     sync.Mutex
}

// NewHistogram 以给定上界构造直方图；bounds 为 nil 时用默认桶。
func NewHistogram(bounds []time.Duration) *Histogram {
	if len(bounds) == 0 {
		bounds = defaultBounds
	}
	b := append([]time.Duration(nil), bounds...)
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	return &Histogram{bounds: b, counts: make([]int64, len(b)+1)}
}

// Observe 记录一次观测。
func (h *Histogram) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += d
	h.total++
	for i, ub := range h.bounds {
		if d <= ub {
			h.counts[i]++
			return
		}
	}
	h.counts[len(h.bounds)]++ // 溢出桶
}

// Count 返回观测次数。
func (h *Histogram) Count() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.total
}

// Mean 返回平均观测值（无观测时返回 0）。
func (h *Histogram) Mean() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	return h.sum / time.Duration(h.total)
}

// Percentile 返回 q 分位的近似值（q ∈ [0,1]）。
func (h *Histogram) Percentile(q float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	if q <= 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	target := int64(float64(h.total)*q + 0.5)
	if target < 1 {
		target = 1
	}
	var cum int64
	for i, c := range h.counts {
		cum += c
		if cum < target {
			continue
		}
		if i < len(h.bounds) {
			return h.bounds[i]
		}
		return h.bounds[len(h.bounds)-1] // 溢出桶按最大上界报告
	}
	return h.bounds[len(h.bounds)-1]
}

// Bounds 返回桶上界（只读副本）。
func (h *Histogram) Bounds() []time.Duration {
	return append([]time.Duration(nil), h.bounds...)
}

// Counts 返回各桶计数（只读副本）。
func (h *Histogram) Counts() []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int64(nil), h.counts...)
}

// Counters 一个租户（或全局）的计数器集合。
//
// 热字段用原子量：run 的生命周期里会被多次递增，且可能来自
// 数据面 goroutine 与 cordis 调度器两侧。
//
// **零值不可用。** latency 是内部指针，只能经 Registry.Tenant /
// Registry.Global 取得实例；直接 &metrics.Counters{} 之后调
// ObserveRun 会 panic（且是带解释的 panic，不是裸的空指针崩溃）。
// 之所以不做惰性初始化：那会让"忘了从注册表取"这类错误一路静默
// 到监控图上，只表现为某个租户的延迟永远是 0。
type Counters struct {
	RunsStarted   atomic.Int64
	RunsCompleted atomic.Int64
	RunsFailed    atomic.Int64
	RunsRejected  atomic.Int64
	ToolsCalled   atomic.Int64
	ToolsDenied   atomic.Int64
	TokensIn      atomic.Int64
	TokensOut     atomic.Int64
	// IsolationChecks / IsolationBreaches 是隔离自检的计数。
	// Breaches 永远应当为 0；非 0 意味着有租户解析到了别人的服务实例。
	IsolationChecks   atomic.Int64
	IsolationBreaches atomic.Int64

	latency *Histogram
}

func newCounters() *Counters {
	return &Counters{latency: NewHistogram(nil)}
}

// ObserveRun 记录一次 run 的耗时。
func (c *Counters) ObserveRun(d time.Duration) {
	c.mustBeWired("ObserveRun")
	c.latency.Observe(d)
}

// Latency 返回耗时直方图。
func (c *Counters) Latency() *Histogram {
	c.mustBeWired("Latency")
	return c.latency
}

// mustBeWired 把「Counters 零值被当成可用实例」变成一个立刻能读懂的失败。
func (c *Counters) mustBeWired(op string) {
	if c == nil || c.latency == nil {
		panic("insula/metrics: Counters 零值不可用，请经 Registry.Tenant/Global 获取（调用 " + op + "）")
	}
}

// Snapshot 是计数器在某一时刻的只读快照。
type Snapshot struct {
	Tenant            ident.Tenant
	RunsStarted       int64
	RunsCompleted     int64
	RunsFailed        int64
	RunsRejected      int64
	ToolsCalled       int64
	ToolsDenied       int64
	TokensIn          int64
	TokensOut         int64
	IsolationChecks   int64
	IsolationBreaches int64
	LatencyP50        time.Duration
	LatencyP95        time.Duration
	LatencyP99        time.Duration
	LatencyMean       time.Duration
	RunsObserved      int64
}

// Snapshot 返回当前快照。
func (c *Counters) Snapshot(t ident.Tenant) Snapshot {
	c.mustBeWired("Snapshot")
	return Snapshot{
		Tenant:            t,
		RunsStarted:       c.RunsStarted.Load(),
		RunsCompleted:     c.RunsCompleted.Load(),
		RunsFailed:        c.RunsFailed.Load(),
		RunsRejected:      c.RunsRejected.Load(),
		ToolsCalled:       c.ToolsCalled.Load(),
		ToolsDenied:       c.ToolsDenied.Load(),
		TokensIn:          c.TokensIn.Load(),
		TokensOut:         c.TokensOut.Load(),
		IsolationChecks:   c.IsolationChecks.Load(),
		IsolationBreaches: c.IsolationBreaches.Load(),
		LatencyP50:        c.latency.Percentile(0.50),
		LatencyP95:        c.latency.Percentile(0.95),
		LatencyP99:        c.latency.Percentile(0.99),
		LatencyMean:       c.latency.Mean(),
		RunsObserved:      c.latency.Count(),
	}
}

// Registry 按租户分桶的计数器注册表。全局计数器单独一份。
type Registry struct {
	mu      sync.RWMutex
	tenants map[ident.Tenant]*Counters
	global  *Counters
}

// New 构造注册表。
func New() *Registry {
	return &Registry{
		tenants: make(map[ident.Tenant]*Counters),
		global:  newCounters(),
	}
}

// Tenant 返回该租户的计数器，不存在时惰性创建。
//
// 空租户标识会被拒绝——计数器会被打上 ""标签，随后任何按租户的
// 聚合都会退化成一个隐藏的全局桶。
func (r *Registry) Tenant(t ident.Tenant) *Counters {
	if !t.Valid() {
		return r.global
	}
	r.mu.RLock()
	c, ok := r.tenants[t]
	r.mu.RUnlock()
	if ok {
		return c
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.tenants[t]; ok {
		return c
	}
	c = newCounters()
	r.tenants[t] = c
	return c
}

// Global 返回全局计数器。
func (r *Registry) Global() *Counters { return r.global }

// Tenants 返回已登记租户的有序列表。
func (r *Registry) Tenants() []ident.Tenant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ident.Tenant, 0, len(r.tenants))
	for t := range r.tenants {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Forget 丢弃某租户的计数器（租户注销时调用，避免注册表无界增长）。
func (r *Registry) Forget(t ident.Tenant) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tenants, t)
}

// Render 输出 Prometheus 文本格式，供 /metrics 端点使用。
func (r *Registry) Render() string {
	var b []byte
	appendCounters := func(label string, c *Counters) {
		s := c.Snapshot(ident.Tenant(label))
		b = append(b, fmt.Sprintf("insula_runs_started{tenant=%q} %d\n", label, s.RunsStarted)...)
		b = append(b, fmt.Sprintf("insula_runs_completed{tenant=%q} %d\n", label, s.RunsCompleted)...)
		b = append(b, fmt.Sprintf("insula_runs_failed{tenant=%q} %d\n", label, s.RunsFailed)...)
		b = append(b, fmt.Sprintf("insula_runs_rejected{tenant=%q} %d\n", label, s.RunsRejected)...)
		b = append(b, fmt.Sprintf("insula_tools_called{tenant=%q} %d\n", label, s.ToolsCalled)...)
		b = append(b, fmt.Sprintf("insula_tools_denied{tenant=%q} %d\n", label, s.ToolsDenied)...)
		b = append(b, fmt.Sprintf("insula_tokens_in{tenant=%q} %d\n", label, s.TokensIn)...)
		b = append(b, fmt.Sprintf("insula_tokens_out{tenant=%q} %d\n", label, s.TokensOut)...)
		// checks 与 breaches 必须成对导出。只导出 breaches 的话，
		// 「查过且干净」与「自检根本没跑起来」在监控上都是 0，
		// 于是针对 breaches 的告警会在这两件事之间失去分辨力——
		// 而"自检悄悄停了"正是它最该报警的情形。
		b = append(b, fmt.Sprintf("insula_isolation_checks{tenant=%q} %d\n", label, s.IsolationChecks)...)
		b = append(b, fmt.Sprintf("insula_isolation_breaches{tenant=%q} %d\n", label, s.IsolationBreaches)...)
		b = append(b, fmt.Sprintf("insula_run_latency_p95_millis{tenant=%q} %d\n",
			label, s.LatencyP95.Milliseconds())...)
		b = append(b, fmt.Sprintf("insula_run_latency_p99_millis{tenant=%q} %d\n",
			label, s.LatencyP99.Milliseconds())...)
	}
	appendCounters("", r.global)
	for _, t := range r.Tenants() {
		c, ok := r.counters(t)
		if !ok {
			continue
		}
		appendCounters(t.String(), c)
	}
	return string(b)
}

func (r *Registry) counters(t ident.Tenant) (*Counters, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.tenants[t]
	return c, ok
}
