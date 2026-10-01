package insula_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/corecraft-io/insula"
	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/creds"
	"github.com/corecraft-io/insula/edge"
	"github.com/corecraft-io/insula/gateway"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/tenant"
)

// exampleUpstream 是最小的模型后端：把最后一条用户消息原样回显，
// 并带上**网关告诉它的**租户身份。
//
// 身份那个参数就是本平台的第一条硬规则：租户不是从请求体里解出来的，
// 而是由网关从能力句柄里读出来传下来的。所以这里打印它对不对，
// 等价于问"模型看到的身份是不是请求方冒充的那个人"。
type exampleUpstream struct{}

// Complete 实现 gateway.Upstream。
func (exampleUpstream) Complete(_ context.Context, t ident.Tenant, req caps.Request,
	_ *creds.Handle) (caps.Response, error) {

	last := ""
	for _, m := range req.Messages {
		if m.Role == "user" {
			last = m.Content
		}
	}
	return caps.Response{
		Message:          caps.Message{Role: "assistant", Content: fmt.Sprintf("[%s] %s", t, last)},
		PromptTokens:     3,
		CompletionTokens: 5,
	}, nil
}

// Example 把一个完整的平台装配起来，并向它发一次请求。
//
// 它演示的是**嵌入的形状**，不是一份可抄的生产装配：
//
//   - `Upstream` / `Creds` / `Auth` 三样都指向外部世界，必须由使用方给出
//     （本仓库不附带生产适配器）。下面三行是替身，只为让示例能跑。
//   - `Handler()` 是交给世界的唯一出口；`Provision` / `Deprovision` 不在它
//     的背后，所以"一个能注销租户的 HTTP 处理器"是编译期错误而不是评审项。
//   - 运维面（开通、注销、自检、指标）留在调用方自己的装配代码里。
//
// 生产里把 `httptest` 换成自己的监听器即可：`Service.Serve(ln)` 与
// `Service.Handler()` 走的是同一条路径。
//
// 想看更完整、带断言的一遍流程（工具调用、身份夹带被剥离、隔离自检、
// 指标、审计、优雅停机），跑 `go run ./cmd/insula demo`。
func Example() {
	const (
		tenantID = "t-example"
		token    = "example-token"
		scope    = "model-api"
	)

	// 1) 外部世界的三样东西。替身只在这里，装配代码里不该有第二种写法。
	upstream := exampleUpstream{}

	provider := creds.NewStaticProvider()
	if err := provider.Put(tenantID, scope, "example-secret"); err != nil {
		panic(err)
	}

	auth := edge.NewStaticTokens()
	if err := auth.Put(token, edge.Principal{Tenant: tenantID, Actor: "example"}); err != nil {
		panic(err)
	}

	// 2) 装配。Shards=1 是合法的单租户部署；横向扩展是改这个数字，
	//    不是改代码。
	svc, err := insula.Open(insula.Config{
		Shards:    1,
		Upstream:  upstream,
		Creds:     provider,
		CredScope: scope,
		Auth:      auth,
		Tenants: []tenant.Spec{{
			ID: tenantID,
			// 额度是**每租户**的、由网关分账的，与接入层的令牌桶
			// （挡"打得太快"）管的是两件事。
			Quota: gateway.Quota{MaxTokens: 100_000, MaxCalls: 1_000, Window: time.Minute},
			// 空白名单 = 不开放任何工具（fail-closed）。
			ToolAllowlist: nil,
		}},
		SystemPrompt: "用一句话回答。",
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = svc.Close() }()

	// 3) 发一次请求。身份只从 Authorization 头来——请求体里放什么都不作数。
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/runs",
		strings.NewReader(`{"input":"你好"}`))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer func() { _ = resp.Body.Close() }()

	var out edge.RunResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		panic(err)
	}

	// 4) 只读视图是运维面的入口：分片数、租户清单、指标、审计都在这里。
	fmt.Printf("status=%d stop=%s output=%s\n", resp.StatusCode, out.Stop, out.Output)
	fmt.Printf("shards=%d tenants=%d\n", svc.ShardCount(), len(svc.Tenants()))

	// Output:
	// status=200 stop=completed output=[t-example] 你好
	// shards=1 tenants=1
}
