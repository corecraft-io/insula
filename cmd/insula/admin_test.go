package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/corecraft-io/insula"
	"github.com/corecraft-io/insula/admit"
)

// newTestService 装配一个最小的真实平台。
//
// 它走的是使用方那条路（Config → Open），而不是造一个替身：运维面的两个
// handler 读的是真实平台的状态（Closed()、Metrics().Render()），替身会把
// 被测对象换掉，那这些测试就只剩下"函数被调用了"这一条信息。
func newTestService(t *testing.T) *insula.Service {
	t.Helper()

	p, err := newDevPlatform()
	if err != nil {
		t.Fatalf("桩件装配失败: %v", err)
	}
	svc, err := insula.Open(insula.Config{
		Shards:       2,
		Upstream:     p.Upstream,
		Creds:        p.Creds,
		CredScope:    devCredScope,
		Auth:         p.Auth,
		Tools:        p.Tools,
		Tenants:      p.Tenants,
		SystemPrompt: DevSystemPrompt,
		Admit:        admit.Config{Default: admit.Limits{MaxConcurrent: 4}},
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

// TestAdminHealthzReportsClosedState 守住运维面最容易被写错的一条。
//
// /healthz 在平台停机后必须返回 503。返回 200 会让负载均衡器继续把流量
// 打到一个正在拆的池上——而拆池期间的表现是 5xx，在监控里读作"平台坏了"，
// 而不是"平台在按计划停机"。
func TestAdminHealthzReportsClosedState(t *testing.T) {
	svc := newTestService(t)
	mux := adminMux(svc)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("运行中 /healthz 状态码 = %d，期望 200", rec.Code)
	}
	if got := rec.Body.String(); got != "ok\n" {
		t.Errorf("运行中 /healthz 正文 = %q，期望 %q", got, "ok\n")
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("运行中 /healthz Content-Type = %q，期望 text/plain", ct)
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("停机失败: %v", err)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("停机后 /healthz 状态码 = %d，期望 503", rec.Code)
	}
	if got := rec.Body.String(); got != "closed\n" {
		t.Errorf("停机后 /healthz 正文 = %q，期望 %q", got, "closed\n")
	}
}

// TestAdminMetricsExposesPrometheusText 守住 /metrics 的两个可协商点。
//
// Content-Type 里的 version=0.0.4 是 Prometheus 选择文本协商格式的依据；
// # TYPE / # HELP 头是按 TYPE 发现指标族的工具能不能看见这些族的前提。
// 两者都属于"坏了也没人当场发现"的那一类——图会变空，而不是报错。
func TestAdminMetricsExposesPrometheusText(t *testing.T) {
	svc := newTestService(t)

	rec := httptest.NewRecorder()
	adminMux(svc).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics 状态码 = %d，期望 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "version=0.0.4") {
		t.Errorf("/metrics Content-Type = %q，期望含 version=0.0.4", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"# TYPE insula_runs_started counter",
		"# HELP insula_isolation_breaches",
		`insula_runs_started{tenant=""}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics 输出里缺少 %q", want)
		}
	}
}

// TestStartAdminDisabledByEmptyAddr 守住"关掉运维面"这条配置。
//
// 空地址必须返回 (nil, nil) 而不是去监听 ":0"：后者会绑到所有网卡上的
// 随机端口——运维面带着租户 label 的 /metrics，正好是设计上要防的暴露。
func TestStartAdminDisabledByEmptyAddr(t *testing.T) {
	svc := newTestService(t)

	srv, err := startAdmin("", svc, io.Discard)
	if err != nil {
		t.Fatalf("空地址应当表示关闭，实际返回错误: %v", err)
	}
	if srv != nil {
		t.Fatal("空地址应当返回 nil server")
	}
}

// TestStartAdminWarnsWhenBoundBeyondLoopback 守住那条告警。
//
// 绑非回环地址是允许的（有人确实需要），但必须留痕：/metrics 的 label 里
// 带租户标识，把它暴露到网络上就是一次信息泄漏。
func TestStartAdminWarnsWhenBoundBeyondLoopback(t *testing.T) {
	svc := newTestService(t)

	var stderr bytes.Buffer
	srv, err := startAdmin("0.0.0.0:0", svc, &stderr)
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer func() { _ = srv.Close() }()

	msg := stderr.String()
	if !strings.Contains(msg, "不是回环地址") {
		t.Errorf("绑非回环地址时应当告警，实际 stderr = %q", msg)
	}
	if !strings.Contains(msg, "泄漏") {
		t.Errorf("告警里应当说明理由（信息泄漏），实际 = %q", msg)
	}
}

// TestStartAdminOnLoopbackIsSilent 是上一条的反面。
//
// 告警必须只在真的越界时出现。一条"绑定回环也告警"的实现会让告警失去
// 区分度，运维很快学会忽略它——那等于没有告警。
func TestStartAdminOnLoopbackIsSilent(t *testing.T) {
	svc := newTestService(t)

	var stderr bytes.Buffer
	srv, err := startAdmin("127.0.0.1:0", svc, &stderr)
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer func() { _ = srv.Close() }()

	if got := stderr.String(); got != "" {
		t.Errorf("绑回环地址不应当告警，实际 stderr = %q", got)
	}
}

// TestWarnToFormatsWithArgs 守住告警文案的格式化。
//
// warnTo 里的 Sprintf 不是多余的：平台产生的告警文案本来就带占位符，
// 直接把它当格式串传给 Fprintf 会让 vet 无从检查。这里钉住"参数真的被
// 代入"——否则告警会以 "%d 个租户" 的样子原样打出来。
func TestWarnToFormatsWithArgs(t *testing.T) {
	var buf bytes.Buffer
	warnTo(&buf)("shard did not settle after provisioning %d tenants", 3)

	if got, want := buf.String(), "warn: shard did not settle after provisioning 3 tenants\n"; got != want {
		t.Errorf("warnTo 输出 = %q，期望 %q", got, want)
	}
}

// TestPrintServeBannerCoversBothAdminModes 守住启动横幅。
//
// 横幅是运维唯一能在启动日志里确认"数据面绑在哪、运维面开没开、用哪个
// 令牌试"的地方。运维面关闭时那行尤其重要：它要把"关掉了"说出来，
// 而不是让那一行凭空消失——后者看起来像输出被截断了。
func TestPrintServeBannerCoversBothAdminModes(t *testing.T) {
	p, err := newDevPlatform()
	if err != nil {
		t.Fatalf("桩件装配失败: %v", err)
	}

	var withAdmin bytes.Buffer
	printServeBanner(&withAdmin, p, "127.0.0.1:8080", "127.0.0.1:8081", 4)
	got := withAdmin.String()
	for _, want := range []string{
		"开发桩件已启动",  // 桩件必须被明说
		"分片数    4", // 分片数是启动参数里唯一影响容量的一格
		"127.0.0.1:8080",
		"127.0.0.1:8081",
		"dev-token-tenant-a", // 试用命令必须可直接复制
		"Ctrl-C",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("启动横幅缺少 %q", want)
		}
	}

	var noAdmin bytes.Buffer
	printServeBanner(&noAdmin, p, "127.0.0.1:8080", "", 1)
	if got := noAdmin.String(); !strings.Contains(got, "已关闭") {
		t.Errorf("运维面关闭时横幅应当说明，实际 = %q", got)
	}
}
