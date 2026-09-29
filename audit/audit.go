// Package audit 提供租户域的追加式审计日志。
//
// # 两条约束
//
//  1. **租户不可写、不可删。** 租户只能经平台动作间接产生事件；
//     没有任何按租户的删除接口（只有进程退出时整体释放）。
//  2. **读取按租户域过滤。** For(tenant) 只返回该租户的事件——
//     这条约束由测试守着，因为它是「审计视图不跨租户」的最后一环。
//
// # 为什么用环形缓冲而不是直接写文件
//
// 记录点分布在 cordis 调度器（租户开通/注销）与数据面 goroutine
// （工具调用）两侧，不能有任何阻塞 I/O。环形缓冲留在内存里，
// 由外部 Sink 异步落盘。
package audit

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/corecraft-io/insula/ident"
)

// Outcome 动作结果。
type Outcome string

const (
	OutcomeOK     Outcome = "ok"
	OutcomeDenied Outcome = "denied"
	OutcomeError  Outcome = "error"
)

// Action 平台动作名。做成命名类型而不是裸 string：审计词表是一个
// **封闭集合**，谁在什么情况下写了什么动作必须可枚举——裸字符串会
// 让 "tenant.provsion" 这类笔误一路通过编译，然后永远查不出来。
type Action string

// 平台动作名。
const (
	ActionTenantProvision   Action = "tenant.provision"
	ActionTenantDeprovision Action = "tenant.deprovision"
	ActionSessionCreate     Action = "session.create"
	ActionSessionClose      Action = "session.close"
	ActionRunStart          Action = "run.start"
	ActionRunFinish         Action = "run.finish"
	ActionRunRejected       Action = "run.rejected"
	ActionToolCall          Action = "tool.call"
	ActionToolDenied        Action = "tool.denied"
	ActionIsolationCheck    Action = "isolation.check"
	ActionCapabilityDown    Action = "capability.down"
)

// Event 一条审计记录。
type Event struct {
	At      time.Time         `json:"at"`
	Tenant  ident.Tenant      `json:"tenant"`
	Actor   string            `json:"actor,omitempty"`
	Action  Action            `json:"action"`
	Subject string            `json:"subject,omitempty"`
	Outcome Outcome           `json:"outcome"`
	Detail  map[string]string `json:"detail,omitempty"`
}

// Sink 接收审计事件的外部出口。Write 不得阻塞。
type Sink interface {
	Write(Event)
}

// SinkFunc 把函数适配成 Sink。
type SinkFunc func(Event)

// Write 实现 Sink。
func (f SinkFunc) Write(e Event) { f(e) }

// WriterSink 把事件以 JSON Lines 写入 w。
//
// 每次 Write 都加锁：多个调度器 goroutine 会并发写入同一个 writer。
type WriterSink struct {
	mu      sync.Mutex
	w       io.Writer
	written atomic.Int64
	dropped atomic.Int64
}

// NewWriterSink 构造 WriterSink。
func NewWriterSink(w io.Writer) *WriterSink { return &WriterSink{w: w} }

// Write 实现 Sink。
func (s *WriterSink) Write(e Event) {
	b, err := json.Marshal(e)
	if err != nil {
		s.dropped.Add(1)
		return
	}
	b = append(b, '\n')
	s.mu.Lock()
	_, err = s.w.Write(b)
	s.mu.Unlock()
	if err != nil {
		s.dropped.Add(1)
		return
	}
	s.written.Add(1)
}

// Written 返回成功交给下游 writer 的条数。
func (s *WriterSink) Written() int64 { return s.written.Load() }

// Dropped 返回因为序列化或写入失败而**丢失**的条数。
//
// 审计出口静默丢事件比丢数据本身更严重：事后看日志，"没有这条记录"
// 与"这条记录没写成功"完全无法区分，而审计的用途恰恰是事后追溯。
// 因此丢弃必须留下痕迹，由调用方决定是告警还是降级。
//
// 说明：当前 Event 的字段全部可无错序列化（map[string]string 与
// time.Time 都不会让 json.Marshal 报错），所以 marshal 分支实际上
// 到不了；保留它是为了防止将来某个字段类型把"不可能"变成"可能"。
// 写入分支是可达的，也是本计数真正的用途（磁盘满、对端断开）。
func (s *WriterSink) Dropped() int64 { return s.dropped.Load() }

// Log 固定容量的环形审计日志。
type Log struct {
	mu       sync.RWMutex
	ring     []Event
	next     int
	count    int
	capacity int
	sinks    []Sink
	clock    func() time.Time
}

// New 构造审计日志。capacity ≤ 0 时取默认值 8192。
func New(capacity int, clock func() time.Time, sinks ...Sink) *Log {
	if capacity <= 0 {
		capacity = 8192
	}
	if clock == nil {
		clock = time.Now
	}
	return &Log{
		ring:     make([]Event, capacity),
		capacity: capacity,
		sinks:    sinks,
		clock:    clock,
	}
}

// Record 记录一条事件。At 为零值时自动补当前时间。
//
// 空租户标识会被替换为哨兵值：平台级动作（如跨租户推送配置）
// 不属于某个租户，但仍必须可追溯。
func (l *Log) Record(e Event) {
	if e.At.IsZero() {
		e.At = l.clock()
	}
	if !e.Tenant.Valid() {
		e.Tenant = "-"
	}
	if e.Outcome == "" {
		e.Outcome = OutcomeOK
	}

	l.mu.Lock()
	l.ring[l.next] = e
	l.next = (l.next + 1) % l.capacity
	if l.count < l.capacity {
		l.count++
	}
	sinks := l.sinks
	l.mu.Unlock()

	// Sink 在锁外调用：它们可能做序列化，不应阻塞其他记录者。
	for _, s := range sinks {
		s.Write(e)
	}
}

// For 返回某租户最近的 limit 条事件（按时间正序）。limit ≤ 0 表示全部。
func (l *Log) For(t ident.Tenant, limit int) []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.collect(limit, func(e Event) bool { return e.Tenant == t })
}

// Recent 返回全体租户最近的 limit 条事件（按时间正序）。
func (l *Log) Recent(limit int) []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.collect(limit, nil)
}

// Count 返回某租户的事件条数。
func (l *Log) Count(t ident.Tenant) int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	n := 0
	for i := 0; i < l.count; i++ {
		if l.at(i).Tenant == t {
			n++
		}
	}
	return n
}

// Len 返回当前保留的事件总数。
func (l *Log) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.count
}

// at 返回第 i 个逻辑位置（0 为最旧）。调用方须持锁。
func (l *Log) at(i int) Event {
	start := l.next - l.count
	if start < 0 {
		start += l.capacity
	}
	return l.ring[(start+i)%l.capacity]
}

func (l *Log) collect(limit int, keep func(Event) bool) []Event {
	out := make([]Event, 0, l.count)
	for i := 0; i < l.count; i++ {
		e := l.at(i)
		if keep != nil && !keep(e) {
			continue
		}
		out = append(out, e)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// String 便于调试时观察容量与水位。
func (l *Log) String() string {
	return fmt.Sprintf("audit.Log{%d/%d events, %d sinks}", l.Len(), l.capacity, len(l.sinks))
}
