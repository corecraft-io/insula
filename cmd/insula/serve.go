package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/metaRobin/insula"
	"github.com/metaRobin/insula/admit"
)

// devRequired 是没加 -dev 时的报错。
//
// 它是一段说明而不是一句"缺少参数"：拒绝的理由本身就是这个仓库最重要的
// 使用约束，写清楚比写短有用。
const devRequired = `serve 需要 -dev

本仓库不附带生产适配器：模型上游、凭证提供者、鉴权这三样都指向外部世界，
平台无法替使用方决定它们从哪来（见 insula.Config 的注释）。于是有两条路：

  · 只是想试用：insula serve -dev        （以开发桩件启动）
  · 要真正上线：在你自己的装配代码里构造 insula.Config 的 Upstream / Creds /
    Auth，然后直接调 insula.Open + Service.Serve。

第二条路与本命令共用同一个 insula 包——它不需要、也不应该经过这里。
把生产装配写进 CLI 会诱使人们把凭证塞进命令行参数，而那些参数在
ps 的输出里对全机器可见。`

func runServe(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("serve", stderr)
	addr := fs.String("addr", "127.0.0.1:8080", "HTTP 监听地址")
	admin := fs.String("admin", "127.0.0.1:8081", "运维监听地址（/metrics、/healthz；空串关闭）")
	shards := fs.Int("shards", 4, "分片数")
	dev := fs.Bool("dev", false, "确认以开发桩件运行")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	if !*dev {
		return errors.New(devRequired)
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
	// Close 幂等，因此这里 defer 一次、正常路径上再显式收尾一次都不算重复。
	defer func() { _ = svc.Close() }()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("监听数据面 %s: %w", *addr, err)
	}

	adminSrv, err := startAdmin(*admin, svc, stderr)
	if err != nil {
		_ = ln.Close()
		return err
	}
	if adminSrv != nil {
		defer func() { _ = adminSrv.Close() }()
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- svc.Serve(ln) }()

	printServeBanner(stdout, p, ln.Addr().String(), *admin, *shards)

	// 只在意外退出时才走这条分支：正常停机走下面的 ctx.Done()。
	//
	// 两条路分开写是必要的——Serve 在 Shutdown 之后返回 nil，若只监听
	// 这个 channel，Ctrl-C 会"正常返回"，看起来像服务自己停了。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("数据面: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	fmt.Fprintln(stdout, "\n收到停止信号，开始停机（排空 → 关准入 → 拆池）…")
	// 顺序由 Service.Close 一处决定，这里不重复实现：见 insula.go 的
	// 「停机顺序不是装配顺序的逆」。
	if err := svc.Close(); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "已停机。")
	return nil
}

// startAdmin 起运维面的监听。
//
// 它为什么是一个**独立**监听而不是数据面上的几条路由：/metrics 的
// label 里带租户标识，/healthz 会暴露平台状态。把它们挂在数据面等于
// 让每一个能发请求的人都能读到全平台的租户清单——而那是运维信息，
// 不是用户信息。独立监听让"只绑回环"成为一个可以单独讨论的决定。
func startAdmin(addr string, svc *insula.Service, stderr io.Writer) (*http.Server, error) {
	if addr == "" {
		return nil, nil
	}
	if !isLoopbackAddr(addr) {
		fmt.Fprintf(stderr,
			"warn: 运维面绑定在 %s，不是回环地址。/metrics 的 label 含租户标识，"+
				"把它暴露到网络上是信息泄漏。\n", addr)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if svc.Closed() {
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, "closed\n")
			return
		}
		io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		io.WriteString(w, svc.Metrics().Render())
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("监听运维面 %s: %w", addr, err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return srv, nil
}

// warnTo 把 OnWarn 接到 stderr。
//
// 中间那个 Sprintf 不是多余的：直接把 msg 当格式串传下去会让 vet 的
// printf 检查无从下手（非恒定格式串），而平台产生的告警文案里本来就
// 带格式化占位符。
func warnTo(w io.Writer) func(string, ...any) {
	return func(msg string, args ...any) {
		fmt.Fprintf(w, "warn: %s\n", fmt.Sprintf(msg, args...))
	}
}

// isLoopbackAddr 判断监听地址是否只绑回环。
//
// 空 host（如 ":8080"）绑的是**所有网卡**，不是回环——这是最容易看错的
// 一种写法，所以它必须显式返回 false 而不是被"没解析出 host"糊过去。
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func printServeBanner(w io.Writer, p *devPlatform, dataAddr, adminAddr string, shards int) {
	fmt.Fprint(w, devWarning())
	fmt.Fprintln(w, "开发桩件已启动")
	fmt.Fprintf(w, "  分片数    %d\n", shards)
	fmt.Fprintf(w, "  数据面    http://%s  （唯一的对外出口）\n", dataAddr)
	if adminAddr == "" {
		fmt.Fprintln(w, "  运维面    已关闭（-admin \"\"）")
	} else {
		fmt.Fprintf(w, "  运维面    http://%s  （/metrics /healthz，勿暴露到公网）\n", adminAddr)
	}
	fmt.Fprintln(w, "  租户与令牌")
	for _, line := range p.tokenTable() {
		fmt.Fprintln(w, line)
	}
	fmt.Fprintln(w, "  试用")
	fmt.Fprintf(w, "    curl -sS -X POST http://%s/v1/runs \\\n", dataAddr)
	fmt.Fprintf(w, "      -H 'Authorization: Bearer %s' \\\n", devToken("tenant-a"))
	fmt.Fprintln(w, "      -H 'Content-Type: application/json' \\")
	fmt.Fprintln(w, `      -d '{"input":"/tool hello"}'`)
	fmt.Fprintln(w, "  停止    Ctrl-C（先排空在途请求，上限 10s）")
	fmt.Fprintln(w)
}
