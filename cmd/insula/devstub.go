package main

// 本文件集中放置**开发桩件**。
//
// 指向外部世界的东西有三样：模型上游、凭证提供者、鉴权。真实部署里这三样
// 必须由使用方提供，本仓库不附带生产适配器。把它们收在一个文件里，
// 是为了让「到底假装了什么」能被一次读完——分散在各子命令里的话，
// 审查者得先把整个 CLI 读完才能确认没有一个悄悄生效的默认凭证。
//
// 因此这里有一条纪律：**本文件里的每个符号都以 Dev 开头，且只被
// serve -dev 与 demo 使用。** 任何非 Dev 命名的东西出现在这里，
// 都说明桩件正在往主干里渗。
//
// 桩件刻意做得**不像生产**：上游不发网络请求，凭证是内存里的假字符串，
// 令牌是固定常量。桩件越是装作能干，越容易被误当成能用。

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/creds"
	"github.com/corecraft-io/insula/edge"
	"github.com/corecraft-io/insula/gateway"
	"github.com/corecraft-io/insula/guard"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/session"
	"github.com/corecraft-io/insula/tenant"
	"github.com/corecraft-io/insula/tools"
)

// devCredScope 与 gateway 的默认作用域一致。
//
// 显式写出来而不是依赖默认值：凭证存储的键是 (租户, scope)，两者不一致
// 时网关换不到凭证，而失败会表现为"模型调用鉴权失败"，看起来像上游的问题。
const devCredScope = "model-api"

// devTenantIDs 是桩件开通的租户。
//
// 三个而不是一个：两个才能演示隔离，三个才能让"分片数 2"显示出不均匀分布
// ——而分布不均匀恰恰是粘性路由必须被验证的地方。
var devTenantIDs = []ident.Tenant{"tenant-a", "tenant-b", "tenant-c"}

// devToken 返回某个租户的开发令牌。
//
// 固定常量而不是随机生成，因为 demo / serve 的输出要能直接拿去 curl。
// 它之所以不算"硬编码凭证"，是因为 DevAuth 只接受这几个令牌、只映射到
// 内置租户（固定名字、无真实权限），而且 serve 要求显式 -dev 才启动。
func devToken(t ident.Tenant) string { return "dev-token-" + string(t) }

// DevUpstream 是最小的模型上游：不发任何网络请求。
//
// 行为由最后一条用户消息的前缀决定：
//
//   - "/tool <q>"：发出一次 echo 工具调用
//   - 其它：把输入原样回显
type DevUpstream struct {
	mu    sync.Mutex
	seen  []ident.Tenant
	calls int
}

// Complete 实现 gateway.Upstream。
//
// 注意第一个参数 t：它是**网关**从能力句柄里读出来的租户，不是从请求体里
// 解出来的。这里把它记下来，是因为"模型实际看到的身份是谁"正是越权最直接
// 的证据面——demo 会把它打出来。
func (u *DevUpstream) Complete(_ context.Context, t ident.Tenant, req caps.Request,
	_ *creds.Handle) (caps.Response, error) {

	u.mu.Lock()
	u.seen = append(u.seen, t)
	u.calls++
	u.mu.Unlock()

	last := ""
	sawToolResult := false
	for _, m := range req.Messages {
		if m.Role == "tool" {
			sawToolResult = true
		}
		if m.Role == "user" {
			last = m.Content
		}
	}

	// 已经有过一次工具结果就收尾，并把工具结果**原样带回**最终回答。
	//
	// 没有"收尾"这个判断，桩件会在「模型要工具 → 工具回结果 → 模型又要
	// 工具」之间一直转到守卫预算耗尽——那条路径展示的是守卫，不是正常流程。
	// 把结果带回去则是因为：工具实际收到了什么，只有让它出现在响应里
	// 才能被观察到（见 demo 第 4 步的断言）。真实的模型上游会自己总结。
	if sawToolResult {
		return caps.Response{
			Message:          caps.Message{Role: "assistant", Content: "工具结果：" + lastToolContent(req.Messages)},
			PromptTokens:     9,
			CompletionTokens: 4,
		}, nil
	}

	if q, ok := strings.CutPrefix(last, "/tool "); ok {
		// 桩件**故意**在工具参数里夹带一个身份字段。正常模型不会这么干，
		// 但它正好用来演示 tools 包的剥离器：工具实际收到的参数里
		// 不该有 tenant_id。
		return caps.Response{
			Message: caps.Message{
				Role: "assistant",
				ToolCalls: []caps.ToolCall{{
					ID:   "call-1",
					Name: "echo",
					Args: map[string]any{"q": q, "tenant_id": string(devTenantIDs[1])},
				}},
			},
			PromptTokens:     7,
			CompletionTokens: 3,
		}, nil
	}

	return caps.Response{
		Message:          caps.Message{Role: "assistant", Content: "回显：" + last},
		PromptTokens:     7,
		CompletionTokens: 3,
	}, nil
}

// lastToolContent 取最后一条工具结果的正文。
//
// 单独一个函数是为了让"桩件把工具结果带进最终回答"这件事显式——它是
// demo 唯一能观察到工具体实际收到了什么的地方。
func lastToolContent(msgs []caps.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "tool" {
			return msgs[i].Content
		}
	}
	return "(无工具结果)"
}

// Seen 返回上游实际见到的租户序列。
func (u *DevUpstream) Seen() []ident.Tenant {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]ident.Tenant(nil), u.seen...)
}

// Calls 返回上游被调用的次数。
func (u *DevUpstream) Calls() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

// DevAuth 只认每个内置租户各自的固定令牌。
func DevAuth() (*edge.StaticTokens, error) {
	auth := edge.NewStaticTokens()
	for _, id := range devTenantIDs {
		if err := auth.Put(devToken(id), edge.Principal{Tenant: id, Actor: "dev"}); err != nil {
			return nil, fmt.Errorf("dev auth: %w", err)
		}
	}
	return auth, nil
}

// DevCreds 是内存里的凭证提供者，为每个内置租户放一份假密钥。
//
// 假密钥的形状（"dev-secret-…"）是刻意的：它一眼就能被认出来，
// 万一哪天出现在日志或响应里，读的人立刻知道这是桩件而不是真凭证。
func DevCreds() (*creds.StaticProvider, error) {
	p := creds.NewStaticProvider()
	for _, id := range devTenantIDs {
		if err := p.Put(id, devCredScope, "dev-secret-"+string(id)); err != nil {
			return nil, fmt.Errorf("dev creds: %w", err)
		}
	}
	return p, nil
}

// DevTools 是内置工具表：只有一个 echo。
//
// 它的返回值刻意带上**实际收到的参数名**。这是为了让"身份字段被剥离"
// 可被观察：模型发出的是 {q, tenant_id}，工具看到的是 {q}。
// 若只回显 q，剥离器坏掉与没坏掉的输出完全一样。
func DevTools() *tools.TenantConfig {
	return &tools.TenantConfig{
		Handlers: map[string]tools.Handler{
			"echo": tools.HandlerFunc(func(_ context.Context, inv caps.Invocation) (caps.ToolResult, error) {
				keys := make([]string, 0, len(inv.Args))
				for k := range inv.Args {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				return caps.ToolResult{
					Content: fmt.Sprintf("q=%v args=[%s]", inv.Args["q"], strings.Join(keys, " ")),
				}, nil
			}),
		},
		Specs: map[string]caps.ToolSpec{
			"echo": {Name: "echo", Description: "回显参数（演示用）"},
		},
	}
}

// DevTenants 返回内置租户的规格。
func DevTenants() []tenant.Spec {
	specs := make([]tenant.Spec, 0, len(devTenantIDs))
	for _, id := range devTenantIDs {
		specs = append(specs, tenant.Spec{
			ID:            id,
			Quota:         gateway.Quota{MaxTokens: 100000, MaxCalls: 1000, Window: time.Minute},
			ToolAllowlist: []string{"echo"},
			GuardBudget:   guard.Budget{MaxSteps: 6, MaxToolCalls: 6},
			SessionPolicy: session.Policy{SoftLimit: 40, KeepRecent: 12},
		})
	}
	return specs
}

// DevSystemPrompt 是桩件的系统提示。
//
// 它由装配代码给出，**不从请求里取**——请求能填 System 就是最直接的
// prompt 注入入口（见 edge.Config.SystemPrompt）。
const DevSystemPrompt = "你是 Insula 的开发桩件助手。用一句话回答。"

// devPlatform 把桩件打包成一份装配输入，供 serve 与 demo 共用。
type devPlatform struct {
	Upstream *DevUpstream
	Creds    *creds.StaticProvider
	Auth     *edge.StaticTokens
	Tools    *tools.TenantConfig
	Tenants  []tenant.Spec
}

func newDevPlatform() (*devPlatform, error) {
	credsP, err := DevCreds()
	if err != nil {
		return nil, err
	}
	auth, err := DevAuth()
	if err != nil {
		return nil, err
	}
	return &devPlatform{
		Upstream: &DevUpstream{},
		Creds:    credsP,
		Auth:     auth,
		Tools:    DevTools(),
		Tenants:  DevTenants(),
	}, nil
}

// tokenTable 返回 "租户 → 令牌" 的展示用文本行。
func (p *devPlatform) tokenTable() []string {
	lines := make([]string, 0, len(devTenantIDs))
	for _, id := range devTenantIDs {
		lines = append(lines, fmt.Sprintf("  %-10s %s", id, devToken(id)))
	}
	return lines
}

// devWarning 是启动时必打的横幅。
//
// 它不是装饰。开发桩件与生产装配共用同一个 insula 包，肉眼分辨不出
// 眼前这个进程用的是哪一种；打一行大写的告示是最便宜的分辨手段。
func devWarning() string {
	return strings.Join([]string{
		"",
		"  ┌──────────────────────────────────────────────────────────────┐",
		"  │  开发桩件：模型上游 / 凭证 / 鉴权 全部是假的，切勿用于生产  │",
		"  └──────────────────────────────────────────────────────────────┘",
		"",
	}, "\n")
}
