// Package agent 是 agent 主循环：**纯数据面**，全程不进入 cordis 调度器。
//
// # 唯一的纪律
//
// 这个包里没有 `*cordis.App`、没有 `*cordis.Context`，也没有任何
// `DoSync` / `Wait` 调用。这不是疏漏，是设计：一旦循环手里有了 app，
// 迟早会出现一次"顺手读一下配置"的同步回调，而 cordis 只有一个调度
// goroutine（用户回调天然跑在其中），任何 I/O 阻塞会冻结整个分片的
// 装配、卸载与服务发布。
//
// 循环需要的一切都通过 caps.Snapshot 一次性取走（见 caps 包：它由
// 控制面的 Watcher 维护，数据面只做一次带锁读，零 cordis 往返）。
//
// # 退出点
//
//   - 模型给出不含工具调用的回复 → 正常完成（StopCompleted）；
//   - 预算用尽（步数 / 工具调用 / token / 墙钟）→ **优雅收尾**，
//     带着已经拿到的部分结果返回，而不是报错。用户拿到"我在完成前
//     用尽了预算"比拿到一个内部错误有用得多；
//   - 上下文被取消（客户端断开、截止时间到）→ StopCanceled。
//
// 注意这里没有"工具调用数超单步上限"这个退出点：单步超限只截断并
// 告知模型，不终止 run。终止会让模型失去自我纠正的机会，而它本来是
// 有能力纠正的。
package agent

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/corecraft-io/insula/audit"
	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/guard"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/metrics"
	"github.com/corecraft-io/insula/session"
)

// StopReason 说明循环为什么停下。它必须能区分「正常完成」与各种「被迫停下」，
// 因为调用方对两者的处理完全不同（前者回消息，后者要提示用户重试或降级）。
type StopReason string

const (
	// StopCompleted 模型给出了最终回复。
	StopCompleted StopReason = "completed"
	// StopBudget 预算用尽，返回的是部分结果。
	StopBudget StopReason = "budget_exhausted"
	// StopCanceled 上下文被取消。
	StopCanceled StopReason = "canceled"
)

// ErrCapabilityUnavailable 必需能力缺失，循环拒绝启动。
var ErrCapabilityUnavailable = errors.New("insula/agent: required capability unavailable")

// ErrBadIdentity 请求缺少租户或会话标识。
var ErrBadIdentity = errors.New("insula/agent: tenant and session identity are required")

// Request 一次 agent 运行请求。
type Request struct {
	Tenant  ident.Tenant
	Session ident.Session
	Run     ident.Run
	// System 是可选的系统提示。它由平台构造，**不来自用户输入**。
	System string
	// Input 是本轮用户输入。
	Input string
	// Model 指定模型；为空时由网关决定默认模型。
	Model string
}

// Result 一次运行的结果。
type Result struct {
	// Output 是最终回复。被迫停下时它是部分结果（可能为空），
	// 调用方应当结合 Stop 判断要不要展示。
	Output string
	Stop   StopReason

	Steps     int
	ToolCalls int
	TokensIn  int
	TokensOut int

	// Compactions 本次运行触发的历史压缩次数。
	Compactions int
	// Denied 记录被策略拒绝的工具调用名，供上层提示与审计核对。
	Denied []string
	// TruncatedToolCalls 记录因单步上限而未执行的工具调用数。
	TruncatedToolCalls int
	// Notices 收集运行中出现的非致命告警（压缩失败、写回失败等）。
	//
	// 它必须回传而不是只进审计：非流式调用方拿不到审计流，如果一个
	// 降级发生了却只写在服务端的日志里，客户端会以为一切正常。
	Notices []string
}

// maxNotices 是单次运行收集的告警上限。
//
// 有上限是因为 Result 会原样变成响应体：一条反复触发的告警不该把
// 响应撑大。丢掉的是重复信息，第一条一定在。
const maxNotices = 8

// note 记一条非致命告警。重复的告警只保留一条。
func (r *Result) note(msg string) {
	for _, n := range r.Notices {
		if n == msg {
			return
		}
	}
	if len(r.Notices) >= maxNotices {
		return
	}
	r.Notices = append(r.Notices, msg)
}

// Total 返回本次运行的 token 总量。
func (r Result) Total() int { return r.TokensIn + r.TokensOut }

// DefaultMaxToolResultChars 是回灌给模型的单条工具结果字数上限。
//
// 必须有：一个工具可以返回几 MB，直接塞进上下文既昂贵又会把
// 真正有用的内容挤出去。
const DefaultMaxToolResultChars = 8000

// DefaultMaxToolCallsPerStep 是单步内工具调用数上限。
//
// 总工具调用预算（guard）拦不住这个：模型可以在一次回复里发出几十个
// 工具调用，让单步的延迟被拉长到不可接受。两者限的是不同的东西。
const DefaultMaxToolCallsPerStep = 8

// Deps 是数据面的全部依赖。
//
// 刻意做成一个纯数据结构而不是接口集合：循环的可测性来自「给它一个
// 快照和一组函数就能跑」，而不是来自 mock 框架。
type Deps struct {
	// Caps 是本次运行使用的能力快照。必需。
	Caps caps.Snapshot
	// History 是会话历史窗口（提供压缩策略与统计）。必需。
	History *session.History
	// Scratch 是会话临时空间，可为 nil。
	Scratch *session.Scratch

	// Audit 接收审计事件，可为 nil。
	Audit func(audit.Event)
	// Metrics 接收计数与耗时，可为 nil。
	Metrics *metrics.Counters
	// OnUsage 在每次模型调用后回调实际用量，供网关外的配额分账（可为 nil）。
	OnUsage func(promptTokens, completionTokens int)

	// Clock 注入时钟。为 nil 时用 time.Now。
	Clock func() time.Time

	// MaxToolResultChars / MaxToolCallsPerStep 覆盖默认上限。
	MaxToolResultChars  int
	MaxToolCallsPerStep int
	// SummarizeMaxTokens 是压缩调用自己的 token 上限（默认 512）。
	SummarizeMaxTokens int
}

func (d Deps) now() time.Time {
	if d.Clock != nil {
		return d.Clock()
	}
	return time.Now()
}

func (d Deps) maxToolResultChars() int {
	if d.MaxToolResultChars > 0 {
		return d.MaxToolResultChars
	}
	return DefaultMaxToolResultChars
}

func (d Deps) maxToolCallsPerStep() int {
	if d.MaxToolCallsPerStep > 0 {
		return d.MaxToolCallsPerStep
	}
	return DefaultMaxToolCallsPerStep
}

func (d Deps) summarizeMaxTokens() int {
	if d.SummarizeMaxTokens > 0 {
		return d.SummarizeMaxTokens
	}
	return 512
}

// Run 执行一次完整的 agent 循环。
//
// 返回的 error 只表示「没能跑起来」或「运行中发生了不可恢复的错误」。
// 任何预算用尽都通过 Result.Stop 表达，不是 error——把正常的终止条件
// 编码成错误，会让调用方在错误路径上做正常流程的事。
func Run(ctx context.Context, d Deps, req Request) (Result, error) {
	var res Result
	start := d.now()

	if !req.Tenant.Valid() || !req.Session.Valid() {
		return res, ErrBadIdentity
	}
	if !d.Caps.Ready() {
		return res, fmt.Errorf("%w: %s", ErrCapabilityUnavailable, d.Caps)
	}
	if d.Caps.Guard == nil || d.History == nil {
		return res, fmt.Errorf("%w: guard policy and history are required", ErrCapabilityUnavailable)
	}

	g := d.Caps.Guard.New(req.Tenant, req.Session, req.Run)

	// 墙钟预算交给 context：Guard 只负责体面地停，context 负责让一个
	// 卡在 I/O 上的调用无论如何都能返回。
	if dl := g.Deadline(); !dl.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, dl)
		defer cancel()
	}

	d.emit(audit.Event{
		At: start, Tenant: req.Tenant, Action: audit.ActionRunStart, Outcome: audit.OutcomeOK,
		Subject: string(req.Run),
	})
	if d.Metrics != nil {
		d.Metrics.RunsStarted.Add(1)
	}

	// 1) 读历史并按需压缩。压缩要在读之后、组装消息之前做，
	//    否则这一步仍然会把膨胀的历史喂给模型。
	turns, err := d.Caps.Memory.Turns(req.Tenant, req.Session)
	if err != nil {
		d.finish(req, start, audit.OutcomeError)
		if d.Metrics != nil {
			d.Metrics.RunsFailed.Add(1)
		}
		return res, fmt.Errorf("insula/agent: load history: %w", err)
	}

	comp, err := compactIfNeeded(ctx, d, req, g, turns)
	if err != nil {
		// 压缩失败不致命：历史只是变长。但必须留下痕迹，
		// 否则「上下文越来越贵」会变成一个没人知道原因的现象。
		d.warn(&res, req, start, "历史压缩失败，本轮按完整历史继续", err)
	} else {
		turns = comp.turns
		res.Compactions = comp.count
		// 压缩消耗的 token 也算这次运行的消耗。只把它记进指标而不记进
		// Result，会让 Result.Total() 与租户实际被扣的额度对不上——
		// 而这两者迟早会被拿来互相核对。
		res.TokensIn += comp.tokensIn
		res.TokensOut += comp.tokensOut
	}

	// 2) 先把用户轮写入存储。放在循环之前是有意的：循环中途失败或进程
	//    重启时，用户说的话不该丢——它是不可再生的输入。
	userTurn := memory.Turn{Role: memory.RoleUser, Content: req.Input, At: start}
	if _, err := d.Caps.Memory.AppendTurn(req.Tenant, req.Session, userTurn); err != nil {
		d.finish(req, start, audit.OutcomeError)
		if d.Metrics != nil {
			d.Metrics.RunsFailed.Add(1)
		}
		return res, fmt.Errorf("insula/agent: append user turn: %w", err)
	}
	d.History.RecordTurns(1)
	turns = append(turns, userTurn)

	// 3) 组装模型可见的消息。工具清单只在确有工具时才带上——
	//    一个空清单和一个不存在的清单对模型是不同的信号。
	msgs := toMessages(req.System, turns)

	var toolSpecs []caps.ToolSpec
	if d.Caps.Tools != nil {
		toolSpecs = d.Caps.Tools.Specs()
	}

	// 4) 循环。
	var assistant string
	for {
		if ctx.Err() != nil {
			res.Stop = StopCanceled
			break
		}
		if err := g.BeginStep(); err != nil {
			res.Stop = StopBudget
			break
		}
		res.Steps++

		resp, err := d.Caps.Models.Complete(ctx, caps.Request{
			Model:    req.Model,
			Messages: msgs,
			Tools:    toolSpecs,
		})
		if err != nil {
			if ctx.Err() != nil {
				res.Stop = StopCanceled
				break
			}
			d.finish(req, start, audit.OutcomeError)
			if d.Metrics != nil {
				d.Metrics.RunsFailed.Add(1)
			}
			return res, fmt.Errorf("insula/agent: model call: %w", err)
		}

		res.TokensIn += resp.PromptTokens
		res.TokensOut += resp.CompletionTokens
		if d.Metrics != nil {
			d.Metrics.TokensIn.Add(int64(resp.PromptTokens))
			d.Metrics.TokensOut.Add(int64(resp.CompletionTokens))
		}
		if d.OnUsage != nil {
			d.OnUsage(resp.PromptTokens, resp.CompletionTokens)
		}
		d.History.RecordUsage(resp.PromptTokens, resp.CompletionTokens, 0)
		_ = g.AddTokens(resp.PromptTokens + resp.CompletionTokens)

		assistant = resp.Message.Content
		if len(resp.Message.ToolCalls) == 0 {
			msgs = append(msgs, resp.Message)
			res.Stop = StopCompleted
			break
		}

		// 单步工具调用数上限：超出部分本轮不执行。
		//
		// 关键：助手消息里**声明的工具调用数必须等于随后回灌的工具结果数**。
		// 主流模型 API 都校验这一点（声明了 N 个调用就要有 N 条结果），
		// 少一条整个请求会被判非法——那会把一次"这一步太长了"的限流
		// 升级成下一轮的硬失败。所以截断的是消息本身，不只是执行列表。
		msg := resp.Message
		calls := msg.ToolCalls
		dropped := 0
		if limit := d.maxToolCallsPerStep(); len(calls) > limit {
			dropped = len(calls) - limit
			calls = calls[:limit]
			msg.ToolCalls = calls
			res.TruncatedToolCalls += dropped
		}
		msgs = append(msgs, msg)

		stopped := false
		for _, tc := range calls {
			if ctx.Err() != nil {
				res.Stop = StopCanceled
				stopped = true
				break
			}
			if err := g.ToolCall(); err != nil {
				res.Stop = StopBudget
				stopped = true
				break
			}
			res.ToolCalls++
			d.History.RecordUsage(0, 0, 1)
			if d.Metrics != nil {
				d.Metrics.ToolsCalled.Add(1)
			}

			content, denied := invokeTool(ctx, d, req, start, tc)
			if denied {
				res.Denied = append(res.Denied, tc.Name)
				if d.Metrics != nil {
					d.Metrics.ToolsDenied.Add(1)
				}
			}
			msgs = append(msgs, caps.Message{
				Role:       "tool",
				Content:    content,
				ToolCallID: tc.ID,
				Name:       tc.Name,
			})
		}
		if stopped {
			break
		}
		if dropped > 0 {
			// 告知模型剩下几个没执行，并让它继续——它有纠正的能力，
			// 直接终止 run 反而剥夺了这次机会。
			msgs = append(msgs, caps.Message{
				Role: "system",
				Content: fmt.Sprintf(
					"本轮还有 %d 个工具调用未被执行（单步上限为 %d）。请只用已执行的结果继续，或在下一步重新发起必要的调用。",
					dropped, d.maxToolCallsPerStep()),
			})
		}
	}

	// 5) 写回助手轮。只在确有内容时写：把空字符串当成一轮对话存下来
	//    会在历史里堆积无信息的轮次，压缩时还要花 token 处理它们。
	if strings.TrimSpace(assistant) != "" {
		if _, err := d.Caps.Memory.AppendTurn(req.Tenant, req.Session,
			memory.Turn{Role: memory.RoleAssistant, Content: assistant, At: d.now()}); err != nil {
			d.warn(&res, req, start, "助手回复未能写回历史", err)
		} else {
			d.History.RecordTurns(1)
		}
	}
	res.Output = assistant

	if d.Metrics != nil {
		d.Metrics.RunsCompleted.Add(1)
		d.Metrics.ObserveRun(d.now().Sub(start))
	}
	d.emit(audit.Event{
		At: d.now(), Tenant: req.Tenant, Action: audit.ActionRunFinish, Outcome: audit.OutcomeOK,
		Subject: string(req.Run),
		Detail: map[string]string{
			"stop":       string(res.Stop),
			"steps":      strconv.Itoa(res.Steps),
			"tool_calls": strconv.Itoa(res.ToolCalls),
			"tokens":     strconv.Itoa(res.Total()),
		},
	})
	return res, nil
}

// invokeTool 执行一次工具调用，返回回灌给模型的内容以及是否被拒。
//
// 三条不可省的规则：
//
//  1. **工具错误必须回灌成工具结果，不能中断 run。** 模型看到错误文本
//     可以换参数重试或改用别的工具；直接失败则让一次偶发的工具故障
//     升级成整次会话失败。
//  2. **工具缺失必须显式告知。** 静默丢弃工具调用会让模型在下一步继续
//     发同一个调用，直到预算耗尽——而且它永远不知道自己错在哪。
//  3. **结果必须截断。** 见 MaxToolResultChars。
func invokeTool(ctx context.Context, d Deps, req Request, now time.Time, tc caps.ToolCall) (string, bool) {
	if d.Caps.Tools == nil {
		return "工具能力当前不可用。请不要继续调用工具，直接依据已有信息作答。", false
	}

	inv := caps.Invocation{
		Tenant:  req.Tenant,
		Session: req.Session,
		Run:     req.Run,
		Name:    tc.Name,
		// 模型给的参数原样交给 Registry：剥离身份字段是它的职责，
		// 在循环里"顺手先剥一遍"会造成两处实现、迟早漂移。
		Args: tc.Args,
	}
	res, err := d.Caps.Tools.Call(ctx, inv)
	if err != nil {
		d.emit(audit.Event{
			At: now, Tenant: req.Tenant, Action: audit.ActionToolCall,
			Subject: tc.Name, Outcome: audit.OutcomeError,
			Detail: map[string]string{"error": truncate(err.Error(), 512)},
		})
		return "工具执行失败：" + truncate(err.Error(), 512), false
	}

	outcome := audit.OutcomeOK
	if res.Denied {
		outcome = audit.OutcomeDenied
	}
	d.emit(audit.Event{
		At: now, Tenant: req.Tenant, Action: audit.ActionToolCall,
		Subject: tc.Name, Outcome: outcome,
		Detail: map[string]string{"reason": truncate(res.Reason, 256)},
	})

	content := res.Content
	if res.Denied {
		reason := res.Reason
		if reason == "" {
			reason = "被平台策略拒绝"
		}
		// 回灌时必须明确"这不是瞬时故障"，否则模型会当成可重试错误，
		// 反复重试同一个被拒工具直到把预算烧完。
		content = "该工具调用被平台策略拒绝，且重试不会改变结果。原因：" + reason
	}
	if content == "" {
		content = "(工具无输出)"
	}
	return truncate(content, d.maxToolResultChars()), res.Denied
}

// compaction 是一次压缩的产出。
//
// 做成结构而不是多返回值：压缩的产物有三样（新窗口、压缩次数、它自己
// 花的 token），全部都要回流到 Result 与历史统计里，拆成三个返回值
// 只会让调用点变成一行难以核对的长赋值。
type compaction struct {
	turns     []memory.Turn
	count     int
	tokensIn  int
	tokensOut int
}

// compactIfNeeded 在需要时压缩历史。
//
// 决策（Plan / SummaryPrompt）是纯函数，执行（调模型 + 写回）在这里。
// 必须先落库再返回新列表：只更新内存里的列表而不落库，下一次 run
// 读到的还是膨胀的历史，压缩就白做了。
//
// 任何失败都返回**原窗口**：压缩是优化，它的失败不该让一段本来可用的
// 历史消失。
func compactIfNeeded(ctx context.Context, d Deps, req Request, g *guard.Guard,
	turns []memory.Turn) (compaction, error) {

	keep := compaction{turns: turns}

	plan := d.History.Plan(turns)
	if !plan.Compact {
		return keep, nil
	}

	prompt := session.SummaryPrompt(plan.Older, d.History.Policy().MaxPromptChars)
	if prompt == "" {
		return keep, nil
	}

	resp, err := d.Caps.Models.Complete(ctx, caps.Request{
		Model:     req.Model,
		Messages:  []caps.Message{{Role: "user", Content: prompt}},
		MaxTokens: d.summarizeMaxTokens(),
	})
	if err != nil {
		return keep, err
	}
	if d.Metrics != nil {
		d.Metrics.TokensIn.Add(int64(resp.PromptTokens))
		d.Metrics.TokensOut.Add(int64(resp.CompletionTokens))
	}
	if d.OnUsage != nil {
		d.OnUsage(resp.PromptTokens, resp.CompletionTokens)
	}
	// 压缩消耗也要计入预算，否则"压缩本身把预算吃光"是一条看不见的死路。
	_ = g.AddTokens(resp.PromptTokens + resp.CompletionTokens)
	d.History.RecordUsage(resp.PromptTokens, resp.CompletionTokens, 0)

	summary := strings.TrimSpace(resp.Message.Content)
	if summary == "" {
		return keep, errors.New("insula/agent: summarizer returned empty content")
	}

	compacted := make([]memory.Turn, 0, len(plan.Keep)+1)
	compacted = append(compacted, memory.Turn{
		Role:    memory.RoleSummary,
		Content: summary,
		At:      d.now(),
	})
	compacted = append(compacted, plan.Keep...)

	if err := d.Caps.Memory.ReplaceTurns(req.Tenant, req.Session, compacted); err != nil {
		return keep, err
	}
	d.History.RecordCompaction(len(plan.Older))
	return compaction{
		turns:     compacted,
		count:     1,
		tokensIn:  resp.PromptTokens,
		tokensOut: resp.CompletionTokens,
	}, nil
}

// toMessages 把历史轮次转成模型消息。
//
// 摘要轮映射为 system 消息：它不是"用户说过的话"，把它伪装成 user
// 轮会让模型以为用户亲口讲了那段压缩后的内容。
func toMessages(system string, turns []memory.Turn) []caps.Message {
	msgs := make([]caps.Message, 0, len(turns)+2)
	if strings.TrimSpace(system) != "" {
		msgs = append(msgs, caps.Message{Role: "system", Content: system})
	}
	for _, t := range turns {
		switch t.Role {
		case memory.RoleSummary:
			msgs = append(msgs, caps.Message{Role: "system", Content: "[此前对话摘要] " + t.Content})
		case memory.RoleTool:
			msgs = append(msgs, caps.Message{
				Role:       "tool",
				Content:    t.Content,
				Name:       t.ToolName,
				ToolCallID: t.ToolCallID,
			})
		default:
			msgs = append(msgs, caps.Message{Role: string(t.Role), Content: t.Content})
		}
	}
	return msgs
}

// truncate 按**字符**（而非字节）截断，并留一个显式标记。
//
// 按字节截断会把一个多字节字符切成两半，产出非法 UTF-8——它进了模型
// 上下文就是一个没法解释的乱码，进了日志就是一条看不出问题的坏数据。
func truncate(s string, maxChars int) string {
	if maxChars <= 0 || utf8.RuneCountInString(s) <= maxChars {
		return s
	}
	return string([]rune(s)[:maxChars]) + "\n…（输出已截断）"
}

func (d Deps) emit(e audit.Event) {
	if d.Audit != nil {
		d.Audit(e)
	}
}

// warn 记一条非致命告警：既进 Result（给调用方看），也进审计（给平台看）。
//
// 两处都要有。只进审计的话调用方会以为一切正常；只进 Result 的话
// 平台侧失去"这个租户正在持续降级"的历史。
func (d Deps) warn(res *Result, req Request, now time.Time, msg string, err error) {
	detail := msg
	if err != nil {
		detail = msg + ": " + truncate(err.Error(), 256)
	}
	if res != nil {
		res.note(detail)
	}
	d.emit(audit.Event{
		At: now, Tenant: req.Tenant, Action: audit.ActionCapabilityDown,
		Outcome: audit.OutcomeError,
		Detail:  map[string]string{"msg": msg, "error": truncate(errText(err), 512)},
	})
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (d Deps) finish(req Request, now time.Time, outcome audit.Outcome) {
	d.emit(audit.Event{
		At: now, Tenant: req.Tenant, Action: audit.ActionRunFinish,
		Outcome: outcome, Subject: string(req.Run),
	})
}
