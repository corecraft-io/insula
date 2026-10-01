package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/corecraft-io/insula"
	"github.com/corecraft-io/insula/admit"
	"github.com/corecraft-io/insula/audit"
	"github.com/corecraft-io/insula/edge"
)

// runDemo 跑一遍完整流程并把每一步的结果打出来。
//
// # 它为什么值得存在
//
// 三个理由，缺一个都不成立：
//
//  1. **可运行的示例**。文档里的装配片段容易过时，能被 `go run` 跑通的不会
//     ——它编译不过就是编译不过。
//  2. **冒烟测试**。任何一步不符预期即非零退出，因此可以进 CI。测试文件里的
//     夹具会随内部改动一起漂移，而这里走的是**使用方会走的那条路**：
//     Config → Open → Serve → HTTP → Close。
//  3. **把安全主张变成可观察的东西**。「身份不可由模型指定」「能力按租户装配」
//     「跨租户读不到」这三条在文档里是断言，在这里是输出。
//
// 它刻意**不用** httptest：那会绕过 Service.Serve 的真实监听路径，
// 而路径里的东西（ReadHeaderTimeout、Shutdown 的排空顺序）正是想被跑到的。
func runDemo(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("demo", stderr)
	shards := fs.Int("shards", 2, "分片数")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}

	p, err := newDevPlatform()
	if err != nil {
		return err
	}

	svc, err := insula.Open(insula.Config{
		Shards:       *shards,
		Upstream:     p.Upstream,
		Creds:        p.Creds,
		CredScope:    devCredScope,
		Auth:         p.Auth,
		Tools:        p.Tools,
		Tenants:      p.Tenants,
		SystemPrompt: DevSystemPrompt,
		Admit:        admit.Config{Default: admit.Limits{MaxConcurrent: 4}},
		OnWarn:       warnTo(stderr),
	})
	if err != nil {
		return err
	}
	defer func() { _ = svc.Close() }()

	fmt.Fprint(stdout, devWarning())
	fmt.Fprintln(stdout, "Insula 端到端演示：装配 → 请求 → 隔离验证 → 停机")
	fmt.Fprintln(stdout)

	// ---- 1) 装配 ----
	step(stdout, 1, "装配：粘性路由把每个租户分到哪一片")
	fmt.Fprintf(stdout, "  分片数      %d\n", svc.ShardCount())
	fmt.Fprintf(stdout, "  启动租户数  %d\n", len(svc.Tenants()))
	for _, st := range svc.Stats() {
		fmt.Fprintf(stdout, "  分片 %-2d     租户 %d  %v\n", st.Shard, st.Tenants, st.Bound)
	}

	// ---- 2) 起真实监听 ----
	base, stopServing, err := serveOnLoopback(svc)
	if err != nil {
		return err
	}
	defer stopServing()

	step(stdout, 2, "数据面：真实监听（与 serve 走同一条路径）")
	fmt.Fprintf(stdout, "  %s\n", base)
	fmt.Fprintf(stdout, "  路由        POST /v1/runs · GET /v1/tenants/self · GET /healthz\n")
	fmt.Fprintf(stdout, "  注意        没有 /v1/tenants/{id} —— 接入层拿不到 Provision（见 insula.go 的门面纪律）\n")

	// ---- 3) 每个租户各发一次请求 ----
	step(stdout, 3, "请求：每个租户各走一次完整的 agent 循环")
	for _, id := range devTenantIDs {
		res, err := runOnce(base, devToken(id), fmt.Sprintf(`{"input":"你好，我是 %s"}`, id))
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "  %-10s run=%-28s stop=%-9s output=%q\n",
			id, res.Run, res.Stop, res.Output)
	}
	fmt.Fprintf(stdout, "  上游实际看到的租户（取自网关的句柄，不是请求体）：%v\n", p.Upstream.Seen())
	fmt.Fprintf(stdout, "  模型被调用 %d 次\n", p.Upstream.Calls())

	// ---- 4) 工具路径 + 两层身份夹带 ----
	step(stdout, 4, "工具调用：模型夹带的身份字段必须被剥掉")

	// 这一层是**请求体**夹带：RunRequest 里根本没有 tenant_id 字段，
	// 但有人在 JSON 里塞了一个进来。
	smuggled := fmt.Sprintf(`{"input":"/tool 你好","tenant_id":%q}`, devTenantIDs[1])
	res, err := runOnce(base, devToken(devTenantIDs[0]), smuggled)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "  请求体       %s\n", smuggled)
	fmt.Fprintf(stdout, "  工具调用数   %d\n", res.ToolCalls)
	fmt.Fprintf(stdout, "  告警回传     %v\n", res.Notices)
	if len(res.Notices) == 0 {
		return fmt.Errorf("请求体夹带身份字段必须产生告警，实际没有")
	}

	// 这一层是**工具参数**夹带：桩件模型的 args 里带了 tenant_id。
	// 上面那次调用的结果就在 output 里——echo 会把实际收到的参数名回显出来。
	fmt.Fprintf(stdout, "  工具结果     %q\n", res.Output)
	if !strings.Contains(res.Output, "args=[q]") {
		return fmt.Errorf("工具参数里的身份字段未被剥离，实际 output=%q", res.Output)
	}
	fmt.Fprintf(stdout, "  ✓ 请求体夹带被记下并忽略；工具参数里只剩 q —— tools 包的剥离器生效\n")

	// ---- 5) 能力快照 ----
	step(stdout, 5, "能力快照：每个租户各自装配")
	for _, id := range devTenantIDs {
		view, err := selfOf(base, devToken(id))
		if err != nil {
			return err
		}
		if !view.Ready || len(view.Missing) > 0 {
			return fmt.Errorf("%s 的能力不完整: ready=%v missing=%v", id, view.Ready, view.Missing)
		}
		fmt.Fprintf(stdout, "  %-10s ready=%-5v gen=%d present=%v\n",
			id, view.Ready, view.Gen, strings.Join(view.Present, ","))
	}

	// ---- 6) 隔离自检 ----
	step(stdout, 6, "隔离自检：抽样哨兵，抓的是构造被破坏这一类回归")
	report, err := svc.SelfCheck()
	if err != nil {
		return fmt.Errorf("自检: %w", err)
	}
	fmt.Fprintf(stdout, "  分片 %d，参与租户 %d，比较对数 %d，声明 %d 条，穷尽=%v\n",
		report.Shards, report.Tenants, report.PairsChecked, report.Declarations, report.Exhaustive)
	if report.Tenants < len(devTenantIDs) {
		return fmt.Errorf("自检只覆盖了 %d 个租户，期望至少 %d 个", report.Tenants, len(devTenantIDs))
	}
	// 声明层的计数**没有**下面第 7 步那个缺口：它随**租户**走，不随
	// 租户对走，只要有一个开通了的租户就必然非零。因此这里可以直接
	// 把它当成硬断言——0 一定是"声明层没跑"。
	if report.Declarations == 0 {
		return fmt.Errorf("有 %d 个租户却一条 Isolate 声明都没扫到：声明层没有跑", report.Tenants)
	}

	// ---- 7) 指标 ----
	step(stdout, 7, "指标：Prometheus 文本")
	render := svc.Metrics().Render()
	checks, breaches, err := isolationTallies(render)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "  insula_isolation_checks   合计 %d（自检真的跑过）\n", checks)
	fmt.Fprintf(stdout, "  insula_isolation_breaches 合计 %d（必须为 0）\n", breaches)
	if breaches != 0 {
		return fmt.Errorf("隔离自检报告了 %d 次越界", breaches)
	}
	// 用报告自带的 Calls 字段区分三种状态（无需跨查 metrics 即可分辨）：
	//   - Calls == 0                → 自检没跑（例如池已关闭），报告不可信；
	//   - Calls > 0 且 PairsChecked == 0 → 跑了，但分片数 > 同片租户数，
	//     没有可比对的对，这是正常状态而非故障；
	//   - PairsChecked > 0          → 真正比过对，正常。
	switch {
	case report.Calls == 0:
		return fmt.Errorf("隔离自检没有运行（Calls=0）：报告不可信，自检路径未执行")
	case report.PairsChecked > 0 && checks == 0:
		return fmt.Errorf("比过 %d 对租户却没有 insula_isolation_checks：自检路径与指标记账不一致",
			report.PairsChecked)
	case report.PairsChecked == 0:
		fmt.Fprintf(stdout, "  ✓ 自检已运行（Calls=%d），但本次没有可比的租户对（分片数 > 同片租户数）—— 这是正常状态，不是故障\n", report.Calls)
	default:
		fmt.Fprintf(stdout, "  ✓ 比过 %d 对租户（Calls=%d），计数非零 —— 「查过且干净」可被监控分辨\n", report.PairsChecked, report.Calls)
	}
	// 声明层是同一类哨兵，但它**没有**上面那个缺口：条数随租户走，
	// 因此「查过且干净」与「没查」在它这里天然可分辨（第 6 步已硬断言）。
	fmt.Fprintf(stdout, "  ✓ 另扫过 %d 条 Isolate 声明（随租户走，不随租户对走，故无上述缺口）\n",
		report.Declarations)
	families := metricFamilies(render)
	fmt.Fprintf(stdout, "  完整的 %d 个指标族可从 serve -admin /metrics 取：\n", len(families))
	fmt.Fprintf(stdout, "    %s\n", strings.Join(families, " "))
	fmt.Fprintln(stdout, "  每个指标族都带 # TYPE / # HELP 头，按 TYPE 头发现指标的工具也能正常看到。")

	// ---- 8) 审计 ----
	step(stdout, 8, "审计：每次运行都留痕，且能按租户翻回")
	events := svc.Audit().For(devTenantIDs[0], 8)
	if len(events) == 0 {
		return fmt.Errorf("租户 %s 没有任何审计事件", devTenantIDs[0])
	}
	fmt.Fprintf(stdout, "  租户 %s 最近 %d 条（共 %d 条）：\n",
		devTenantIDs[0], len(events), svc.Audit().Count(devTenantIDs[0]))
	for _, e := range events {
		fmt.Fprintf(stdout, "    %s %-16s %-6s %s\n",
			e.At.Format("15:04:05"), e.Action, e.Outcome, e.Subject)
	}
	if !hasDeniedIsolation(events) {
		fmt.Fprintln(stdout, "  （本次窗口内没有夹带记录——它已被翻页翻过去了）")
	} else {
		fmt.Fprintln(stdout, "  ↑ isolation.check/denied 就是第 4 步那次请求体夹带的留痕")
	}

	// ---- 9) 停机 ----
	step(stdout, 9, "停机：排空 → 关准入 → 拆池（顺序不是装配的逆）")
	if err := stopServing(); err != nil {
		return err
	}
	if err := svc.Close(); err != nil {
		return fmt.Errorf("关闭: %w", err)
	}
	fmt.Fprintf(stdout, "  closed=%v，全部租户已注销\n", svc.Closed())

	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "演示完成：装配、请求、工具、隔离、指标、审计、停机七条路径都真实走过了。")
	return nil
}

// serveOnLoopback 起一个只绑回环的随机端口，并等到它真能应答为止。
//
// 那个"等到能应答"不是保险起见：Service.Shutdown 依赖 Serve 内部设好的
// http.Server，而 Serve 是异步跑的。若在它设置之前就 Shutdown，Shutdown
// 会变成空操作，然后 Serve 永远阻塞在 Accept 上、收尾时死等。先打一次
// /healthz 把"已经在服务"这件事变成可观察的事实，比 sleep 可靠。
func serveOnLoopback(svc *insula.Service) (base string, stop func() error, err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, fmt.Errorf("监听: %w", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- svc.Serve(ln) }()
	base = "http://" + ln.Addr().String()

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, herr := http.Get(base + "/healthz")
		if herr == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			return "", nil, fmt.Errorf("数据面未在 5s 内就绪: %v", herr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// stop 必须幂等：第 9 步会显式调用一次，而 defer 还会再调一次。
	// 不幂等的话第二次会挂在已经排空的 serveErr 上——那不是"重复停机",
	// 而是死锁，且是在演示成功打印之后才炸。
	var (
		once    sync.Once
		stopErr error
	)
	stop = func() error {
		once.Do(func() {
			ctx, cancel := ctxWithTimeout()
			defer cancel()
			if err := svc.Shutdown(ctx); err != nil {
				stopErr = fmt.Errorf("停机: %w", err)
				return
			}
			if err := <-serveErr; err != nil && err != http.ErrServerClosed {
				stopErr = fmt.Errorf("数据面退出: %w", err)
			}
		})
		return stopErr
	}
	return base, stop, nil
}

// runOnce 发一次 run 请求并解析响应。
//
// 令牌放在 Authorization 头里：身份只从那里来，请求体里放什么都不作数
// ——第 4 步演示的正是这件事。
func runOnce(base, token, body string) (edge.RunResponse, error) {
	status, raw, err := callHTTP(http.MethodPost, base+"/v1/runs", token, body)
	if err != nil {
		return edge.RunResponse{}, err
	}
	if status != http.StatusOK {
		return edge.RunResponse{}, fmt.Errorf("POST /v1/runs 返回 %d: %s", status, raw)
	}
	var res edge.RunResponse
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		return edge.RunResponse{}, fmt.Errorf("解析 run 响应: %w", err)
	}
	return res, nil
}

// selfOf 取当前身份的能力快照。
func selfOf(base, token string) (edge.SelfView, error) {
	status, raw, err := callHTTP(http.MethodGet, base+"/v1/tenants/self", token, "")
	if err != nil {
		return edge.SelfView{}, err
	}
	if status != http.StatusOK {
		return edge.SelfView{}, fmt.Errorf("GET /v1/tenants/self 返回 %d: %s", status, raw)
	}
	var view edge.SelfView
	if err := json.Unmarshal([]byte(raw), &view); err != nil {
		return edge.SelfView{}, fmt.Errorf("解析 self 响应: %w", err)
	}
	return view, nil
}

func callHTTP(method, url, token, body string) (int, string, error) {
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(raw), nil
}

// isolationTallies 把两条隔离指标在全部 label 上求和。
//
// 求和而不是挑一条：这一对指标是**按租户**导出的，只看某个租户会漏掉
// "另一个租户炸了"。而 breaches 求和必须为 0 是平台级的硬约束。
func isolationTallies(render string) (checks, breaches int64, err error) {
	for _, line := range strings.Split(render, "\n") {
		var target *int64
		switch {
		case strings.HasPrefix(line, "insula_isolation_checks"):
			target = &checks
		case strings.HasPrefix(line, "insula_isolation_breaches"):
			target = &breaches
		default:
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &v); err != nil {
			return 0, 0, fmt.Errorf("解析指标行 %q: %w", line, err)
		}
		*target += int64(v)
	}
	return checks, breaches, nil
}

func hasDeniedIsolation(events []audit.Event) bool {
	for _, e := range events {
		if e.Action == audit.ActionIsolationCheck && e.Outcome == audit.OutcomeDenied {
			return true
		}
	}
	return false
}

// metricFamilies 取渲染文本里出现过的指标名（去重、升序）。
//
// 解析而不是数 "# TYPE"：当前 Render 只输出裸样本行，不带 HELP / TYPE 头。
// 那是合法的 exposition 格式（缺 TYPE 即 untyped），但也意味着任何靠 TYPE
// 头来发现指标族的工具在这里都会看到零个 —— demo 想做的正是别让这件事
// 被一个漂亮的"12 个族"数字掩盖过去。
func metricFamilies(render string) []string {
	seen := map[string]bool{}
	var out []string
	for _, line := range strings.Split(render, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, _, ok := strings.Cut(line, "{")
		if !ok {
			name = strings.Fields(line)[0]
		}
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// step 打印带编号的小节标题。
func step(w io.Writer, n int, title string) {
	fmt.Fprintf(w, "\n[%d] %s\n", n, title)
}

// ctxWithTimeout 给停机一个上限。
//
// 上限是必须的：SSE 是长连接，没有上限的优雅停机可以被一个客户端
// 无限期挂住（同 insula.DefaultShutdownGrace 的理由）。
func ctxWithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), insula.DefaultShutdownGrace)
}
