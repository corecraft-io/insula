// Package session 是会话级运行时：有界历史窗口、摘要压缩策略、会话临时空间。
//
// # 为什么压缩策略是纯函数
//
// 摘要压缩需要调模型，也就是一次 I/O。而 cordis 只有一个调度 goroutine，
// 在里面做 I/O 会冻结整个分片的装配与卸载（设计 §8 第一条约束）。
//
// 因此本包把压缩拆成两半：
//
//   - **决策**（Plan / SummaryPrompt）是纯函数，不碰存储、不调模型；
//   - **执行**（真正调模型拿摘要、再写回）在数据面的 goroutine 上做。
//
// 拆开之后有一条额外收益：决策部分可以直接用表驱动测试覆盖各种边界
// （首轮即超限、已有摘要要折叠进新摘要、压缩后仍然超限等），
// 而不需要伪造一个模型。
package session

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/corecraft-io/cordis"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/realm"
)

// Policy 是会话历史窗口策略。
type Policy struct {
	// SoftLimit 轮数软上限：超过它就建议压缩。0 取默认 40。
	//
	// 它是"软"的，因为压缩需要一次模型调用，不能在写入路径上同步完成；
	// 真正的硬上限在存储层（memory.Config.MaxTurnsPerSession），
	// 负责在压缩逻辑失效时兜住内存。
	SoftLimit int
	// KeepRecent 压缩后保留的最近轮数。0 取默认 12。
	//
	// 保留最近若干轮而不是全压掉：最近几轮几乎总是当前任务的直接上下文，
	// 压掉它们会让模型在下一步立刻"忘记"用户刚才说的话。
	KeepRecent int
	// MaxPromptChars 生成摘要提示时的字数上限，0 取默认 12000。
	MaxPromptChars int
}

func (p Policy) softLimit() int {
	if p.SoftLimit <= 0 {
		return 40
	}
	return p.SoftLimit
}

func (p Policy) keepRecent() int {
	if p.KeepRecent <= 0 {
		return 12
	}
	return p.KeepRecent
}

func (p Policy) maxPromptChars() int {
	if p.MaxPromptChars <= 0 {
		return 12000
	}
	return p.MaxPromptChars
}

// Normalize 返回补齐默认值后的策略。
func (p Policy) Normalize() Policy {
	return Policy{
		SoftLimit:      p.softLimit(),
		KeepRecent:     p.keepRecent(),
		MaxPromptChars: p.maxPromptChars(),
	}
}

// Plan 是一次压缩决策。
type Plan struct {
	// Compact 为 false 时其余字段无意义。
	Compact bool
	// Older 是需要被摘要替换的轮次（含上一轮的摘要，若有）。
	Older []memory.Turn
	// Keep 是保留原样的最近轮次。
	Keep []memory.Turn
}

// History 是单个会话的历史窗口。
type History struct {
	policy Policy

	mu          sync.Mutex
	usage       Usage
	compactions int
	summarized  int
}

// NewHistory 构造历史窗口。
func NewHistory(p Policy) *History {
	return &History{policy: p.Normalize()}
}

// Policy 返回生效策略。
func (h *History) Policy() Policy { return h.policy }

// Plan 决定是否需要压缩，并给出压缩范围。
//
// 关键细节：**上一轮的摘要必须被折叠进新的摘要输入**。如果把它当作
// 普通轮次保留，几轮之后上下文里会堆着好几段互不衔接的摘要；如果
// 直接丢掉，早前的信息就永久没了。正确做法是让它成为"待摘要"的第一段。
func (h *History) Plan(turns []memory.Turn) Plan {
	limit := h.policy.softLimit()
	if len(turns) <= limit {
		return Plan{}
	}
	keep := h.policy.keepRecent()
	if keep >= len(turns) {
		// 策略自相矛盾（保留数 ≥ 现有轮数却还触发了压缩）。
		// 不猜用户意图，直接不压——压了反而会丢掉全部上下文。
		return Plan{}
	}
	cut := len(turns) - keep
	return Plan{
		Compact: true,
		Older:   append([]memory.Turn(nil), turns[:cut]...),
		Keep:    append([]memory.Turn(nil), turns[cut:]...),
	}
}

// SummaryPrompt 把待压缩轮次渲染成摘要提示。
//
// 它从**尾部**（最近的）往前取，直到用完字数预算：早前的信息已经
// 被上一轮摘要浓缩过一次，而最近的信息还没被浓缩过，优先级更高。
// 这一点看起来反直觉，但如果从头取，一次超长会话会把预算全花在
// 最古老的、信息密度最低的部分上。
func SummaryPrompt(older []memory.Turn, maxChars int) string {
	if len(older) == 0 {
		return ""
	}
	if maxChars <= 0 {
		maxChars = 12000
	}
	var lines []string
	used := 0
	for i := len(older) - 1; i >= 0; i-- {
		t := older[i]
		line := renderTurn(t)
		if used+len(line) > maxChars && len(lines) > 0 {
			break
		}
		lines = append(lines, line)
		used += len(line)
	}
	// 上面是倒序收集的，恢复成时间正序再交给模型。
	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
	}

	var b strings.Builder
	b.WriteString("请把下面这段对话压缩成一段事实性摘要。要求：\n")
	b.WriteString("1. 保留全部关键事实、结论、数字、标识符与未完成的待办；\n")
	b.WriteString("2. 不要保留寒暄与重复表述；\n")
	b.WriteString("3. 不要加入原文没有的推断；\n")
	b.WriteString("4. 只输出摘要正文，不要任何前后缀。\n\n")
	b.WriteString(strings.Join(lines, "\n"))
	return b.String()
}

func renderTurn(t memory.Turn) string {
	switch t.Role {
	case memory.RoleSummary:
		return "[此前摘要] " + t.Content
	case memory.RoleTool:
		name := t.ToolName
		if name == "" {
			name = "tool"
		}
		return "[" + name + " 结果] " + t.Content
	default:
		return "[" + string(t.Role) + "] " + t.Content
	}
}

// Usage 是一次会话的累计用量。
type Usage struct {
	TokensIn  int
	TokensOut int
	ToolCalls int
	Turns     int
}

// Total 返回输入+输出 token。
func (u Usage) Total() int { return u.TokensIn + u.TokensOut }

// Stats 是压缩统计。
type Stats struct {
	Compactions     int
	SummarizedTurns int
	Usage           Usage
}

// RecordUsage 累加用量。
func (h *History) RecordUsage(tokensIn, tokensOut, toolCalls int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.usage.TokensIn += tokensIn
	h.usage.TokensOut += tokensOut
	h.usage.ToolCalls += toolCalls
}

// RecordTurns 记入本轮新增的对话轮数。
func (h *History) RecordTurns(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.usage.Turns += n
}

// RecordCompaction 记入一次压缩。
func (h *History) RecordCompaction(summarized int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.compactions++
	h.summarized += summarized
}

// Stats 返回统计快照。
func (h *History) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()
	return Stats{Compactions: h.compactions, SummarizedTurns: h.summarized, Usage: h.usage}
}

// ---------------------------------------------------------------------------
// Scratch
// ---------------------------------------------------------------------------

// ErrClosed 会话已注销，其临时空间不再可用。
//
// 这个错误的存在意义是让"用了已拆除会话的资源"立刻失败。若只是
// 返回零值，一个已经下线的会话会继续悄悄地跑下去。
var ErrClosed = errors.New("insula/session: session is closed")

// DefaultMaxScratchEntries 是临时空间的条目上限。
//
// 必须有上限：临时空间是"什么都能往里放"的地方，而上限缺失时它就是
// 一条无界增长路径（尤其在会话长期存活时）。
const DefaultMaxScratchEntries = 256

// Scratch 是会话级临时空间。
//
// 它由 cordis 的效果回收保证清除：会话入口卸载时 Effect 的逆操作
// 会调用 Entry.Close。这是把"清理"绑到生命周期而不是绑到调用方是否
// 记得——后者在真实代码里总是会被漏掉。
type Scratch struct {
	mu     sync.Mutex
	items  map[string]any
	max    int
	closed bool
}

// NewScratch 构造临时空间。max <= 0 时用 DefaultMaxScratchEntries。
func NewScratch(max int) *Scratch {
	if max <= 0 {
		max = DefaultMaxScratchEntries
	}
	return &Scratch{items: make(map[string]any), max: max}
}

// Set 写入一个值。超出上限时返回错误而不是静默淘汰：
// 淘汰哪个键是调用方的语义，本类型无从判断。
func (s *Scratch) Set(key string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if _, exists := s.items[key]; !exists && len(s.items) >= s.max {
		return fmt.Errorf("insula/session: scratch is full (%d entries)", s.max)
	}
	s.items[key] = v
	return nil
}

// Get 读取一个值。
func (s *Scratch) Get(key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false
	}
	v, ok := s.items[key]
	return v, ok
}

// Delete 删除一个键。
func (s *Scratch) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
}

// Len 返回当前条目数。
func (s *Scratch) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// Keys 返回键名（升序），供诊断与断言。不含值——
// 临时空间里可能有不适合进日志的东西。
func (s *Scratch) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.items))
	for k := range s.items {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Closed 报告是否已关闭。
func (s *Scratch) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close 清空并关闭。幂等。
func (s *Scratch) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[string]any)
	s.closed = true
}

// ---------------------------------------------------------------------------
// Entry：会话级运行时
// ---------------------------------------------------------------------------

// Entry 是一份会话级运行时，作为会话入口的 Config 传入插件。
//
// 它由租户管理器在数据面创建（见 tenant 包），再交给 cordis 托管：
// 插件把它提供的两个服务注册进**会话级隔离域**，并在入口卸载时
// 触发 Close。这样"会话下线"这件事只剩一个入口，不会出现
// "cordis 里拆了、内存里还留着"的半拆除状态。
type Entry struct {
	Tenant  ident.Tenant
	Session ident.Session
	History *History
	Scratch *Scratch

	createdAt time.Time
	once      sync.Once
	closed    chan struct{}
}

// NewEntry 构造会话级运行时。
func NewEntry(t ident.Tenant, s ident.Session, p Policy, maxScratch int) (*Entry, error) {
	if !t.Valid() {
		return nil, fmt.Errorf("insula/session: tenant identity is required")
	}
	if !s.Valid() {
		return nil, fmt.Errorf("insula/session: session identity is required")
	}
	return &Entry{
		Tenant:    t,
		Session:   s,
		History:   NewHistory(p),
		Scratch:   NewScratch(maxScratch),
		createdAt: time.Now(),
		closed:    make(chan struct{}),
	}, nil
}

// Key 返回 "tenant/session" 形式的稳定键，供日志与指标使用。
func (e *Entry) Key() string { return string(e.Tenant) + "/" + string(e.Session) }

// CreatedAt 返回会话运行时的创建时刻。
func (e *Entry) CreatedAt() time.Time { return e.createdAt }

// Closed 返回一个在 Entry 关闭时被关闭的通道，供数据面 select 取消。
func (e *Entry) Closed() <-chan struct{} { return e.closed }

// IsClosed 报告是否已关闭。
func (e *Entry) IsClosed() bool {
	select {
	case <-e.closed:
		return true
	default:
		return false
	}
}

// Close 关闭会话运行时：清空临时空间并唤醒等待者。幂等。
func (e *Entry) Close() {
	e.once.Do(func() {
		e.Scratch.Close()
		close(e.closed)
	})
}

// ---------------------------------------------------------------------------
// cordis 集成
// ---------------------------------------------------------------------------

// Plugin 返回会话插件**定义**（一个进程内一份）。
//
// 它在**会话级隔离域**内注册 history 与 scratch：会话入口带着
// realm.Session() 的 Isolate 声明，域键比租户域更细一层（"#<会话入口ID>"）。
// 因此同一租户的两个会话拿到的 history/scratch 互不可见——
// 这是"会话级隔离"唯一的可编程验证点。
//
// 逆操作绑定 Close：会话入口卸载 → cordis 按 LIFO 回收效果 →
// 临时空间被清空。清理的触发点来自生命周期而不是调用方纪律。
func Plugin() *cordis.Plugin {
	return &cordis.Plugin{
		Name: realm.PluginSession,
		Validate: func(cfg any) (any, error) {
			e, ok := cfg.(*Entry)
			if !ok || e == nil {
				return nil, fmt.Errorf("insula/session: session config must be a non-nil *session.Entry, got %T", cfg)
			}
			return e, nil
		},
		Apply: func(ctx *cordis.Context, cfg any) error {
			e := cfg.(*Entry)
			if _, err := ctx.Provide(realm.ServiceHistory, e.History, nil); err != nil {
				return err
			}
			if _, err := ctx.Provide(realm.ServiceScratch, e.Scratch, nil); err != nil {
				return err
			}
			_, err := ctx.Effect("session.close", func() (cordis.Dispose, error) {
				return func() { e.Close() }, nil
			})
			return err
		},
	}
}
