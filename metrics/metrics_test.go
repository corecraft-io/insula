package metrics_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/metrics"
)

var (
	b10 = 10 * time.Millisecond
	b20 = 20 * time.Millisecond
	b30 = 30 * time.Millisecond
)

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

// 桶上界是**闭区间**（d <= ub），且入参会被排序后拷走。
// 两条都是契约：闭区间决定了恰好等于阈值的观测落在哪个桶，
// 而"拷走"决定了调用方之后改自己的切片不会影响直方图。
func TestNewHistogramSortsAndCopiesBounds(t *testing.T) {
	in := []time.Duration{b30, b10, b20}
	h := metrics.NewHistogram(in)

	if got := h.Bounds(); !equalDur(got, []time.Duration{b10, b20, b30}) {
		t.Fatalf("Bounds() = %v，期望升序 [10ms 20ms 30ms]", got)
	}

	// 改入参：直方图不受影响。
	in[0] = time.Hour
	if got := h.Bounds(); !equalDur(got, []time.Duration{b10, b20, b30}) {
		t.Fatalf("改动入参之后 Bounds() = %v", got)
	}
	// 改返回值：直方图同样不受影响。
	got := h.Bounds()
	got[0] = time.Hour
	if again := h.Bounds(); !equalDur(again, []time.Duration{b10, b20, b30}) {
		t.Fatalf("改动 Bounds() 的返回值之后直方图被改了: %v", again)
	}
}

func TestNewHistogramWithoutBoundsUsesDefaults(t *testing.T) {
	h := metrics.NewHistogram(nil)
	if got := len(h.Bounds()); got == 0 {
		t.Fatal("nil bounds 没有回退到默认桶")
	}
	// counts 的长度恒为 len(bounds)+1：多出来的是溢出桶。
	if got, want := len(h.Counts()), len(h.Bounds())+1; got != want {
		t.Fatalf("Counts 长度 %d，期望 %d（桶数 + 溢出桶）", got, want)
	}
}

// 恰好落在上界的观测进**该**桶，不是下一个桶。
// 阈值告警的全部意义在于边界，这里差一纳秒就是一个假警报或漏报。
func TestObserveUsesInclusiveUpperBounds(t *testing.T) {
	h := metrics.NewHistogram([]time.Duration{b10, b20, b30})

	h.Observe(b10)                   // 恰好 10ms → 桶 0
	h.Observe(b10 + time.Nanosecond) // 超过 10ms → 桶 1
	h.Observe(b20)                   // 恰好 20ms → 桶 1
	h.Observe(b20 + time.Nanosecond) // 超过 20ms → 桶 2

	if got, want := h.Counts(), []int64{1, 2, 1, 0}; !equalInts(got, want) {
		t.Fatalf("Counts() = %v，期望 %v", got, want)
	}
	if got := h.Count(); got != 4 {
		t.Fatalf("Count() = %d，期望 4", got)
	}
}

// 超过最大上界的观测进溢出桶；分位数报告为最大上界（这是文档化的近似，
// 用它做容量规划可以，做计费不行）。
func TestObserveOverflowBucketAndNegativeClamp(t *testing.T) {
	h := metrics.NewHistogram([]time.Duration{b10, b20, b30})

	h.Observe(time.Hour)             // 溢出
	h.Observe(-5 * time.Millisecond) // 负值按 0 计

	if got, want := h.Counts(), []int64{1, 0, 0, 1}; !equalInts(got, want) {
		t.Fatalf("Counts() = %v，期望 %v（0 进首桶，1h 进溢出桶）", got, want)
	}
	if got := h.Percentile(1); got != b30 {
		t.Fatalf("溢出桶的 P100 = %v，期望报告最大上界 %v", got, b30)
	}
	if got := h.Mean(); got != 30*time.Minute {
		t.Fatalf("Mean() = %v，期望 30min（负值按 0 计，1h/2）", got)
	}
}

// 分位数是「最近秩」近似：返回命中比例首次达到 q 的那个桶的上界。
// 下面每个期望值都是手算的，改动 Percentile 的取整方式必然让它们变红。
func TestPercentileNearestRankTable(t *testing.T) {
	h := metrics.NewHistogram([]time.Duration{b10, b20, b30})
	for _, d := range []time.Duration{5 * time.Millisecond, 15 * time.Millisecond,
		25 * time.Millisecond, 35 * time.Millisecond} {
		h.Observe(d)
	}
	// counts = [1 1 1 1]，total = 4，sum = 80ms。
	if got, want := h.Counts(), []int64{1, 1, 1, 1}; !equalInts(got, want) {
		t.Fatalf("Counts() = %v，期望 %v", got, want)
	}
	if got := h.Mean(); got != 20*time.Millisecond {
		t.Fatalf("Mean() = %v，期望 20ms", got)
	}

	cases := []struct {
		q    float64
		want time.Duration
		why  string
	}{
		{-1, b10, "q<0 按 0 处理 → target 至少为 1 → 首桶"},
		{0, b10, "target = int(0*4+0.5)=0 → 提升到 1 → 首桶"},
		{0.25, b10, "target = int(1+0.5)=1 → 首桶"},
		{0.5, b20, "target = int(2+0.5)=2 → 累计到桶 1 才够"},
		{0.75, b30, "target = int(3+0.5)=3 → 累计到桶 2 才够"},
		{0.95, b30, "target = int(3.8+0.5)=4 → 落在溢出桶，报告最大上界"},
		{1, b30, "target = int(4.5)=4 → 同上"},
		{2, b30, "q>1 按 1 处理"},
	}
	for _, c := range cases {
		if got := h.Percentile(c.q); got != c.want {
			t.Errorf("Percentile(%v) = %v，期望 %v（%s）", c.q, got, c.want, c.why)
		}
	}
}

func TestPercentileAndMeanOnEmptyHistogramAreZero(t *testing.T) {
	h := metrics.NewHistogram([]time.Duration{b10})
	if got := h.Count(); got != 0 {
		t.Errorf("Count() = %d", got)
	}
	if got := h.Mean(); got != 0 {
		t.Errorf("Mean() = %v，期望 0", got)
	}
	for _, q := range []float64{-1, 0, 0.5, 0.99, 1, 2} {
		if got := h.Percentile(q); got != 0 {
			t.Errorf("空直方图 Percentile(%v) = %v，期望 0", q, got)
		}
	}
}

func TestCountsIsACopy(t *testing.T) {
	h := metrics.NewHistogram([]time.Duration{b10, b20})
	h.Observe(5 * time.Millisecond)

	got := h.Counts()
	got[0] = 999
	if again := h.Counts(); again[0] != 1 {
		t.Fatalf("改动 Counts() 的返回值影响了直方图: %v", again)
	}
}

// ---------------------------------------------------------------------------
// Counters / Snapshot
// ---------------------------------------------------------------------------

// 快照必须逐字段带出计数器，否则运维只看得到一半的 RED 指标。
func TestSnapshotCarriesEveryCounter(t *testing.T) {
	r := metrics.New()
	c := r.Tenant("tenant-a")

	c.RunsStarted.Add(11)
	c.RunsCompleted.Add(9)
	c.RunsFailed.Add(2)
	c.RunsRejected.Add(3)
	c.ToolsCalled.Add(7)
	c.ToolsDenied.Add(1)
	c.TokensIn.Add(1000)
	c.TokensOut.Add(250)
	c.IsolationChecks.Add(5)
	c.IsolationBreaches.Add(0)
	for _, d := range []time.Duration{
		5 * time.Millisecond, 15 * time.Millisecond, 25 * time.Millisecond, 35 * time.Millisecond,
	} {
		c.ObserveRun(d)
	}

	s := c.Snapshot("tenant-a")
	if s.Tenant != "tenant-a" {
		t.Errorf("Tenant = %q", s.Tenant)
	}
	for _, kv := range []struct {
		name string
		got  int64
		want int64
	}{
		{"RunsStarted", s.RunsStarted, 11},
		{"RunsCompleted", s.RunsCompleted, 9},
		{"RunsFailed", s.RunsFailed, 2},
		{"RunsRejected", s.RunsRejected, 3},
		{"ToolsCalled", s.ToolsCalled, 7},
		{"ToolsDenied", s.ToolsDenied, 1},
		{"TokensIn", s.TokensIn, 1000},
		{"TokensOut", s.TokensOut, 250},
		{"IsolationChecks", s.IsolationChecks, 5},
		{"IsolationBreaches", s.IsolationBreaches, 0},
		{"RunsObserved", s.RunsObserved, 4},
	} {
		if kv.got != kv.want {
			t.Errorf("Snapshot.%s = %d，期望 %d", kv.name, kv.got, kv.want)
		}
	}
	// 默认桶下：5ms→桶0，15ms/25ms→桶1（上界 25ms），35ms→桶2（上界 50ms）。
	// total=4 → P50 的 target=2 落在桶1 → 25ms；P95/P99 的 target=4 落在桶2 → 50ms。
	if s.LatencyMean != 20*time.Millisecond {
		t.Errorf("LatencyMean = %v，期望 20ms", s.LatencyMean)
	}
	if s.LatencyP50 != 25*time.Millisecond {
		t.Errorf("LatencyP50 = %v，期望 25ms（就近秩落在 25ms 桶）", s.LatencyP50)
	}
	if s.LatencyP95 != 50*time.Millisecond {
		t.Errorf("LatencyP95 = %v，期望 50ms", s.LatencyP95)
	}
	if s.LatencyP99 < s.LatencyP95 {
		t.Errorf("P99 (%v) 小于 P95 (%v)", s.LatencyP99, s.LatencyP95)
	}

	// 快照是值语义：之后再观察不影响已取到的快照。
	c.RunsStarted.Add(1)
	if s.RunsStarted != 11 {
		t.Errorf("快照随计数器变化了: %d", s.RunsStarted)
	}
}

// Counters 的零值不可用（latency 是内部指针）。fail-loud 而不是
// 静默惰性初始化：后者会把「忘了从注册表取」变成某个租户的延迟
// 永远是 0，一路静默到监控图上。
func TestZeroValueCountersPanicsWithAnExplanation(t *testing.T) {
	var c metrics.Counters

	cases := []struct {
		name string
		fn   func()
	}{
		{"ObserveRun", func() { c.ObserveRun(time.Millisecond) }},
		{"Latency", func() { _ = c.Latency() }},
		{"Snapshot", func() { _ = c.Snapshot("tenant-a") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("Counters 零值上的 %s 没有 panic", tc.name)
				}
				msg, ok := r.(string)
				if !ok || !strings.Contains(msg, "Registry") {
					t.Fatalf("panic 消息没有指出正确用法: %v", r)
				}
			}()
			tc.fn()
		})
	}
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

func TestTenantIsLazyIsolatedAndStable(t *testing.T) {
	r := metrics.New()

	a1 := r.Tenant("tenant-a")
	a2 := r.Tenant("tenant-a")
	b := r.Tenant("tenant-b")

	if a1 != a2 {
		t.Fatal("同一租户两次取到不同计数器")
	}
	if a1 == b {
		t.Fatal("不同租户共享了计数器 —— 指标会串租")
	}
	if a1 != r.Global() && a1.RunsStarted.Load() != 0 {
		t.Fatal("新租户的计数器不是从零开始")
	}

	a1.RunsStarted.Add(1)
	if b.RunsStarted.Load() != 0 {
		t.Fatal("递增一个租户影响到了另一个租户")
	}
	if r.Global().RunsStarted.Load() != 0 {
		t.Fatal("递增租户影响到了全局")
	}
	if got := r.Tenants(); !equalTenants(got, []ident.Tenant{"tenant-a", "tenant-b"}) {
		t.Fatalf("Tenants() = %v，期望按字典序", got)
	}
}

// 空租户标识回退到全局桶，而不是创建一个 "" 标签的隐藏租户。
// 否则任何按租户的聚合都会多出一个来源不明的桶。
func TestEmptyTenantFallsBackToGlobal(t *testing.T) {
	r := metrics.New()
	if got := r.Tenant(""); got != r.Global() {
		t.Fatal("空租户没有回退到全局计数器")
	}
	if got := r.Tenants(); len(got) != 0 {
		t.Fatalf("空租户被登记成了独立租户: %v", got)
	}
}

// 注册表按租户 ID 累积，租户注销时**必须**清掉，否则标签基数随
// 「历史上开通过多少个租户」无界增长——这正是监控先死掉的那种故障。
func TestForgetDropsTenantStateButLeavesOthersAndGlobalAlone(t *testing.T) {
	r := metrics.New()
	a := r.Tenant("tenant-a")
	b := r.Tenant("tenant-b")
	a.RunsStarted.Add(3)
	b.RunsStarted.Add(5)
	r.Global().RunsStarted.Add(7)

	r.Forget("tenant-a")

	if got := r.Tenants(); !equalTenants(got, []ident.Tenant{"tenant-b"}) {
		t.Fatalf("Forget 之后 Tenants() = %v，期望只剩 tenant-b", got)
	}
	// 重新取到的是**全新**的计数器，不是旧的那份。
	if fresh := r.Tenant("tenant-a"); fresh == a || fresh.RunsStarted.Load() != 0 {
		t.Fatal("Forget 没有真正丢弃状态（取回了旧计数器或非零值）")
	}
	if b.RunsStarted.Load() != 5 {
		t.Error("Forget 波及了别的租户")
	}
	if r.Global().RunsStarted.Load() != 7 {
		t.Error("Forget 波及了全局计数器")
	}
}

func TestForgetUnknownTenantIsSafe(t *testing.T) {
	r := metrics.New()
	r.Tenant("tenant-a").RunsStarted.Add(1)

	r.Forget("tenant-ghost")
	r.Forget("")

	if got := r.Tenants(); !equalTenants(got, []ident.Tenant{"tenant-a"}) {
		t.Fatalf("Tenants() = %v", got)
	}
	if r.Tenant("tenant-a").RunsStarted.Load() != 1 {
		t.Fatal("撤销不存在的租户影响到了已有租户")
	}
}

// ---------------------------------------------------------------------------
// Render：导出契约
// ---------------------------------------------------------------------------

// renderFamilies 是 /metrics 端点的**导出契约**。
//
// 新增一族指标必须同时改这个清单——这正是这条测试的用途：
// 让"少导出一族"变成一个必须显式确认的动作，而不是一次疏忽。
var renderFamilies = []string{
	"insula_runs_started",
	"insula_runs_completed",
	"insula_runs_failed",
	"insula_runs_rejected",
	"insula_tools_called",
	"insula_tools_denied",
	"insula_tokens_in",
	"insula_tokens_out",
	"insula_isolation_checks",
	"insula_isolation_breaches",
	"insula_run_latency_p95_millis",
	"insula_run_latency_p99_millis",
}

// checks 与 breaches 必须成对导出。
//
// 只导出 breaches 的话，「自检跑过且干净」与「自检根本没跑起来」在
// 监控上都是 0，针对 breaches 的告警会在这两件事之间失去分辨力——
// 而"自检悄悄停了"恰恰是它最该报警的情形。
func TestRenderExportsIsolationChecksAlongsideBreaches(t *testing.T) {
	r := metrics.New()
	c := r.Tenant("tenant-a")
	c.IsolationChecks.Add(9)
	c.IsolationBreaches.Add(0)

	out := r.Render()

	if !hasLine(out, `insula_isolation_checks{tenant="tenant-a"} 9`) {
		t.Fatalf("Render 没有导出自检次数，breaches 无从解释:\n%s", out)
	}
	if !hasLine(out, `insula_isolation_breaches{tenant="tenant-a"} 0`) {
		t.Fatalf("Render 没有导出隔离破坏计数:\n%s", out)
	}
}

func TestRenderEmitsEveryFamilyForGlobalAndEachTenant(t *testing.T) {
	r := metrics.New()
	r.Global().RunsStarted.Add(1)
	r.Tenant("tenant-a").RunsStarted.Add(2)
	r.Tenant("tenant-b").RunsStarted.Add(3)

	out := r.Render()
	lines := splitLines(out)

	// 每族 2 行头（# TYPE + # HELP）+ 3 个桶（全局 + tenant-a + tenant-b）样本。
	if want := len(renderFamilies)*2 + len(renderFamilies)*3; len(lines) != want {
		t.Fatalf("输出 %d 行，期望 %d（%d 族 × (2 头 + 3 桶)）",
			len(lines), want, len(renderFamilies))
	}

	for _, fam := range renderFamilies {
		for _, label := range []string{"", "tenant-a", "tenant-b"} {
			want := fmt.Sprintf("%s{tenant=%q} ", fam, label)
			if !hasLinePrefix(out, want) {
				t.Errorf("缺少指标行 %q", want)
			}
		}
	}

	// 全局桶用空标签，不能借用一个保留名（那样会和真租户名撞车）。
	if !hasLine(out, `insula_runs_started{tenant=""} 1`) {
		t.Errorf("全局桶标签不对:\n%s", out)
	}
	if !hasLine(out, `insula_runs_started{tenant="tenant-a"} 2`) {
		t.Error("tenant-a 的计数值不对")
	}
}

// 租户名是外部输入。含引号或换行的名字若不转义，会把一行指标撕成
// 两行、或伪造出一条不存在的指标（Prometheus 文本格式的经典注入路径）。
func TestRenderEscapesTenantLabels(t *testing.T) {
	r := metrics.New()
	hostile := ident.Tenant("wo\"rld\nX")
	r.Tenant(hostile).RunsStarted.Add(1)
	r.Tenant("plain").RunsStarted.Add(1)

	out := r.Render()

	if !hasLine(out, `insula_runs_started{tenant="wo\"rld\nX"} 1`) {
		t.Fatalf("恶意租户名没有被转义:\n%s", out)
	}
	// 换行必须被转义掉：行数只能是「族数 × (2 头 + 桶数)」。
	if want := len(renderFamilies)*2 + len(renderFamilies)*3; len(splitLines(out)) != want {
		t.Fatalf("注入的换行把输出撕成了 %d 行，期望 %d 行:\n%s",
			len(splitLines(out)), want, out)
	}
	// 未转义的形态一次都不许出现。
	if strings.Contains(out, `tenant="wo"rld`) {
		t.Fatalf("输出含未转义的引号:\n%s", out)
	}
}

func TestRenderOnEmptyRegistryStillEmitsGlobalFamilies(t *testing.T) {
	out := metrics.New().Render()
	// 空注册表只有全局桶：每族 2 行头 + 1 行样本。
	if want := len(renderFamilies)*2 + len(renderFamilies); len(splitLines(out)) != want {
		t.Fatalf("空注册表输出 %d 行，期望 %d 行（每族 2 头 + 全局桶始终存在）",
			len(splitLines(out)), want)
	}
}

// 每族必须输出一次 # TYPE / # HELP 头，且头要在该族首个样本之前。
// 这是补掉 demo 自检里点的那个缺口：没有 # TYPE 头的族，按 TYPE 发现的
// 工具（Prometheus 解析器、Grafana 指标浏览器）会看不到它。
func TestRenderEmitsTypeAndHelpHeaders(t *testing.T) {
	out := metrics.New().Render()

	for _, fam := range renderFamilies {
		typeLine := "# TYPE " + fam + " "
		helpLine := "# HELP " + fam + " "
		if !hasLinePrefix(out, typeLine) {
			t.Errorf("缺少 # TYPE 头: %q", typeLine)
		}
		if !hasLinePrefix(out, helpLine) {
			t.Errorf("缺少 # HELP 头: %q", helpLine)
		}
		// 头里声明的类型必须落在 {counter, gauge} 之内。
		var typ string
		for _, l := range splitLines(out) {
			if strings.HasPrefix(l, typeLine) {
				typ = strings.TrimSpace(strings.TrimPrefix(l, typeLine))
				break
			}
		}
		if typ != "counter" && typ != "gauge" {
			t.Errorf("指标 %q 的 # TYPE 值非法: %q", fam, typ)
		}
		// 第一个样本必须排在 # TYPE 头之后。
		typeIdx, sampleIdx := -1, -1
		for i, l := range splitLines(out) {
			if typeIdx < 0 && strings.HasPrefix(l, typeLine) {
				typeIdx = i
			}
			if sampleIdx < 0 && strings.HasPrefix(l, fam+"{tenant=") {
				sampleIdx = i
				break
			}
		}
		if typeIdx < 0 || sampleIdx < 0 || sampleIdx <= typeIdx {
			t.Errorf("指标 %q 的样本（行 %d）没有排在 # TYPE 头（行 %d）之后", fam, sampleIdx, typeIdx)
		}
	}
}

// ---------------------------------------------------------------------------
// 并发
// ---------------------------------------------------------------------------

func TestConcurrentCountersRegistryAndRender(t *testing.T) {
	r := metrics.New()

	const workers, each = 8, 300
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tt := ident.Tenant(fmt.Sprintf("tenant-%d", i))
			c := r.Tenant(tt)
			for j := 0; j < each; j++ {
				c.RunsStarted.Add(1)
				c.ObserveRun(time.Duration(j) * time.Millisecond)
				c.Snapshot(tt)
				if j%50 == 0 {
					// 注销与使用交错：这是真实存在的竞态
					//（租户注销与在途请求的指标写入同时发生）。
					r.Forget(ident.Tenant(fmt.Sprintf("tenant-%d", (i+1)%workers)))
				}
			}
		}(i)
	}
	// 并发的导出与租户枚举。
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 3; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r.Render()
				r.Tenants()
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

func equalDur(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalInts(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalTenants(a, b []ident.Tenant) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func splitLines(s string) []string {
	s = strings.TrimSuffix(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func hasLine(s, want string) bool {
	for _, l := range splitLines(s) {
		if l == want {
			return true
		}
	}
	return false
}

func hasLinePrefix(s, prefix string) bool {
	for _, l := range splitLines(s) {
		if strings.HasPrefix(l, prefix) {
			return true
		}
	}
	return false
}
