package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// TestRunDispatches 覆盖命令分发与"用错了"这条路径。
//
// 它值得测：默认的 flag.ExitOnError 会在解析失败时直接 os.Exit，
// 于是"命令行用错"这条最常被走到的路径反而没有测试守着。这里用
// ContinueOnError + 注入 writer，就是为了让它可测。
func TestRunDispatches(t *testing.T) {
	var out, errOut bytes.Buffer

	if err := run([]string{"help"}, &out, &errOut); err != nil {
		t.Fatalf("help 应当成功: %v", err)
	}
	for _, want := range []string{"serve", "demo"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("帮助里缺少子命令 %q", want)
		}
	}

	out.Reset()
	errOut.Reset()
	err := run(nil, &out, &errOut)
	if !errors.Is(err, errUsage) {
		t.Errorf("无参数应当返回 errUsage，实际 %v", err)
	}
	if !strings.Contains(errOut.String(), "用法") {
		t.Error("无参数时应当把用法写到 stderr")
	}

	out.Reset()
	errOut.Reset()
	err = run([]string{"nope"}, &out, &errOut)
	if !errors.Is(err, errUsage) {
		t.Errorf("未知子命令应当返回 errUsage，实际 %v", err)
	}
	if !strings.Contains(errOut.String(), "nope") {
		t.Error("未知子命令的错误里应当带上它")
	}
}

// TestServeRequiresDevAcknowledgement 守住"默认失守"这件事。
//
// 桩件与生产装配共用同一个 insula 包，肉眼分辨不出眼前这个进程用的是
// 哪一种。因此 -dev 不是一个便利开关，而是唯一的显式确认点——它一旦
// 被默认打开，开发凭证就会在不经意间上线。
func TestServeRequiresDevAcknowledgement(t *testing.T) {
	var out, errOut bytes.Buffer

	err := run([]string{"serve"}, &out, &errOut)
	if err == nil {
		t.Fatal("不带 -dev 的 serve 必须拒绝启动")
	}
	if errors.Is(err, errUsage) {
		t.Error("这是使用约束而不是参数解析错误，不该被归成 errUsage")
	}
	msg := err.Error()
	for _, want := range []string{"-dev", "Creds", "Auth"} {
		if !strings.Contains(msg, want) {
			t.Errorf("拒绝的理由里应当提到 %q，实际：%s", want, msg)
		}
	}
}

// TestServeListenFailureIsReturned 守住"失败要出得来"这件事。
//
// 端口非法时必须在启动阶段就把错误返回，而不是 Serve 里静默卡住——
// 一个启动不了却还挂着的进程，在部署脚本看来和"启动成功"没有区别。
func TestServeListenFailureIsReturned(t *testing.T) {
	var out, errOut bytes.Buffer

	if err := run([]string{"serve", "-dev", "-addr", "127.0.0.1:99999"}, &out, &errOut); err == nil {
		t.Fatal("非法监听地址必须返回错误")
	}
	if err := run([]string{"serve", "-dev", "-addr", "127.0.0.1:0", "-admin", "127.0.0.1:99999"},
		&out, &errOut); err == nil {
		t.Fatal("非法运维面地址必须返回错误")
	}
}

// TestDemoEndToEnd 是 CLI 的冒烟测试。
//
// 它走的是使用方会走的那条路：Config → Open → Serve → HTTP → Close。
// 测试文件里的夹具会随内部改动一起漂移，而这条路径不会——它一旦坏掉，
// README 里的「Run from source」就是错的。
func TestDemoEndToEnd(t *testing.T) {
	var out, errOut bytes.Buffer

	if err := run([]string{"demo"}, &out, &errOut); err != nil {
		t.Fatalf("demo 失败: %v\n--- 输出 ---\n%s", err, out.String())
	}

	got := out.String()
	for _, want := range []string{
		"开发桩件",                    // 横幅：桩件必须被明说
		"args=[q]",                // 工具参数里的身份字段被剥离
		"已被忽略",                    // 请求体夹带产生了告警
		"insula_isolation_breach", // 隔离指标成对导出
		"closed=true",             // 停机真的走完了
	} {
		if !strings.Contains(got, want) {
			t.Errorf("demo 输出里缺少 %q", want)
		}
	}
	if !strings.Contains(got, "tenant-c") {
		t.Error("三个租户都应当出现在输出里")
	}
}

// TestIsLoopbackAddr 守住一个很容易看错的判断。
//
// ":8080" 绑的是**所有网卡**而不是回环。把它误判成回环，会让
// "运维面只绑回环"这条安全默认值悄悄失效——而它的表现是"一切正常"。
func TestIsLoopbackAddr(t *testing.T) {
	cases := []struct {
		addr string
		want bool
		why  string
	}{
		{"127.0.0.1:8080", true, "显式回环"},
		{"localhost:8080", true, "主机名回环"},
		{"[::1]:8080", true, "IPv6 回环"},
		{":8080", false, "空主机名 = 所有网卡，不是回环"},
		{"0.0.0.0:8080", false, "显式全网卡"},
		{"192.168.1.5:8080", false, "内网地址仍然不是回环"},
		{"nonsense", false, "解析不了就不该当成回环"},
		{"", false, "空串"},
	}
	for _, tc := range cases {
		if got := isLoopbackAddr(tc.addr); got != tc.want {
			t.Errorf("isLoopbackAddr(%q) = %v，期望 %v（%s）", tc.addr, got, tc.want, tc.why)
		}
	}
}

// TestMetricFamiliesParsesBareSamples 守住指标族的解析。
//
// 当前 Render 输出的是裸样本行（没有 # HELP / # TYPE 头）。若哪天它
// 加上了头，这个解析必须继续正确——否则 demo 报的族数会突然变成 0，
// 而那种"看起来什么都没坏"的错误最难被发现。
func TestMetricFamiliesParsesBareSamples(t *testing.T) {
	render := strings.Join([]string{
		`insula_runs_started{tenant=""} 3`,
		`insula_runs_started{tenant="a"} 1`,
		`insula_isolation_breaches{tenant="a"} 0`,
		"",
		`insula_untouched 5`,
	}, "\n")

	got := metricFamilies(render)
	want := []string{"insula_isolation_breaches", "insula_runs_started", "insula_untouched"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("指标族 = %v，期望 %v", got, want)
	}
}

// TestIsolationTalliesSumsAcrossLabels 守住"求和而不是取一条"。
//
// 这个函数按 label 求和，是因为只挑一个租户会漏掉"另一个租户炸了"。
// breaches 的求和必须为 0 是平台级硬约束，因此这里必须把任何一条
// 非零都算进来。
func TestIsolationTalliesSumsAcrossLabels(t *testing.T) {
	render := strings.Join([]string{
		`insula_isolation_checks{tenant=""} 4`,
		`insula_isolation_checks{tenant="a"} 2`,
		`insula_isolation_breaches{tenant=""} 0`,
		`insula_isolation_breaches{tenant="a"} 0`,
		`insula_runs_started{tenant=""} 9`,
	}, "\n")

	checks, breaches, err := isolationTallies(render)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if checks != 6 || breaches != 0 {
		t.Fatalf("checks=%d breaches=%d，期望 6 与 0", checks, breaches)
	}

	// 任一条非零都必须被算进来——这条断言是"求和"与"取一条"的分水岭。
	checks, breaches, err = isolationTallies(
		"insula_isolation_checks{tenant=\"\"} 1\ninsula_isolation_breaches{tenant=\"b\"} 1\n")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if checks != 1 || breaches != 1 {
		t.Fatalf("checks=%d breaches=%d，期望 1 与 1", checks, breaches)
	}
}
