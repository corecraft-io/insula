package tenant_test

// 本文件是**体量级**基准：量的是「租户数 N 增长时代价怎么变」，
// 而不是「单次操作有多快」。两者常被混为一谈，但对一个多租户平台
// 来说前者才决定容量（ADR-002 的 ~2000 活跃租户/分片就是这么来的）。
//
// 运行方式：
//
//	go test -run '^$' -bench . -benchtime 1x ./tenant/          # 快速跑通
//	go test -run '^$' -bench . -benchtime 5x -count 3 ./tenant/  # 要数字
//
// n=10000 的三条用例各要 10–90 秒，整套约两分钟。取数时**单独跑这个包**
// 并关掉并行（-cpu 1）：Go 默认并行跑不同包的测试二进制，邻居的负载会
// 直接进到这些数字里。
//
// 大 N 用例在 -short 下自动跳过（见 skipLarge）：它们要几十秒，
// 混在默认回归里只会让人把整个基准集关掉。
//
// 所有用例共用 buildHarness（即测试用的同一套装配）。基准跑另一条
// 代码路径得到的数没有参考价值——它不能预测测试覆盖的那条路径。

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	cordis "github.com/metaRobin/cordis"
	"github.com/metaRobin/insula/gateway"
	"github.com/metaRobin/insula/guard"
	"github.com/metaRobin/insula/ident"
	"github.com/metaRobin/insula/session"
	"github.com/metaRobin/insula/tenant"
)

// benchSizes 是体量曲线的采样点。
//
// 100 是"一个开发者的全部租户"，1000 是"一个分片的活跃量级"，
// 10000 是"单分片容量的下限估计"——曲线要跨过目标容量，
// 否则外推全靠想象。
var benchSizes = []int{100, 1000, 10000}

// benchShortLimit 之上的 N 在 -short 下跳过。
const benchShortLimit = 1000

func skipLarge(b *testing.B, n int) {
	b.Helper()
	if n > benchShortLimit && testing.Short() {
		b.Skipf("n=%d 太大，跳过（需要 -short=false）", n)
	}
}

// benchSpecs 造 n 份租户规格。
//
// 规格刻意做得**同构**：如果每份规格都不一样，测出来的差异会来自
// 配置分支而不是 N。租户名也固定宽度（t-0…t-9999），避免字符串
// 长度随 N 变化污染内存曲线。
func benchSpecs(n int) []tenant.Spec {
	specs := make([]tenant.Spec, 0, n)
	for i := 0; i < n; i++ {
		specs = append(specs, tenant.Spec{
			ID:            ident.Tenant(fmt.Sprintf("t-%d", i)),
			Quota:         gateway.Quota{MaxTokens: 1000, MaxCalls: 100, Window: time.Minute},
			ToolAllowlist: []string{"echo"},
			GuardBudget:   guard.Budget{MaxSteps: 4},
			SessionPolicy: session.Policy{SoftLimit: 8, KeepRecent: 3},
		})
	}
	return specs
}

func idsOf(specs []tenant.Spec) []ident.Tenant {
	out := make([]ident.Tenant, 0, len(specs))
	for _, s := range specs {
		out = append(out, s.ID)
	}
	return out
}

// countFibers 数出整棵入口树上的 fiber 数。
//
// 用 Entry.Fiber() 而不是入口数：一个入口可能没有 fiber
// （分组入口在分组插件还没跑起来时如此），而「一个租户占多少常驻
// 资源」的分母是 fiber——每个 fiber 都有自己的状态机、store 与
// 效果链，它们才是真正跟着租户数增长的东西。
func countFibers(t *cordis.EntryTree) int {
	n := 0
	var walk func(g *cordis.EntryGroup)
	walk = func(g *cordis.EntryGroup) {
		for _, e := range g.Children() {
			if e.Fiber() != nil {
				n++
			}
			if sg := e.Subgroup(); sg != nil {
				walk(sg)
			}
		}
	}
	walk(t.Root())
	return n
}

// ---------------------------------------------------------------------------
// 开通
// ---------------------------------------------------------------------------

// BenchmarkTenantProvision 量开通 N 个租户的一次批量往返。
//
// 关键在"一次"：tenant.Manager.Provision 对整批只做一次 DoSync + 一次
// app.Wait()（见 tenant.go 的注释）。这个设计决定了开通成本随 N 的
// 增长是**近似线性于树操作**，而不是线性于"每租户一次调度器往返"。
// 若哪天有人把批量拆成逐租户循环，ns/tenant 会突然抬高一个量级，
// 这条曲线就是抓它的地方。
func BenchmarkTenantProvision(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			skipLarge(b, n)

			specs := benchSpecs(n)
			ids := idsOf(specs)

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// 装配与拆除不计入：这里量的是开通，不是建进程。
				b.StopTimer()
				h := buildHarness(b)
				b.StartTimer()

				if err := h.man.Provision(specs); err != nil {
					b.Fatalf("Provision: %v", err)
				}
				if got := len(h.man.List()); got != n {
					b.Fatalf("登记了 %d 个租户，期望 %d", got, n)
				}

				b.StopTimer()
				if err := h.man.Deprovision(ids); err != nil {
					b.Fatalf("清理: %v", err)
				}
				h.app.Close()
				b.StartTimer()
			}
			b.ReportMetric(nsPer(b.Elapsed(), b.N, n), "ns/tenant")
		})
	}
}

// BenchmarkTenantTeardownAll 是上面那条的反向：注销整批。
//
// 它与开通**不**对偶——注销要摘整棵子树，而每棵子树里挂着若干 fiber
// 与它们的效果链（会话运行时、工具配置、能力 Watcher…）。回收是 LIFO
// 且异步的，因此这里必须等到收敛为止；曲线里包含了那次等待。
// 「注销比开通慢」这件事本身是结论，不是缺陷：它换来的是一次
// Remove 带走全部，而不是逐个入口地清理（那才会真的泄漏）。
func BenchmarkTenantTeardownAll(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			skipLarge(b, n)

			specs := benchSpecs(n)
			ids := idsOf(specs)

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				h := buildHarness(b)
				if err := h.man.Provision(specs); err != nil {
					b.Fatalf("准备阶段 Provision: %v", err)
				}
				b.StartTimer()

				if err := h.man.Deprovision(ids); err != nil {
					b.Fatalf("Deprovision: %v", err)
				}

				b.StopTimer()
				if got := len(h.man.List()); got != 0 {
					b.Fatalf("注销后还剩 %d 个租户", got)
				}
				// 树也要空：只清登记表不清树就是泄漏本身。
				if got := countFibers(h.loader.Tree()); got != 0 {
					b.Fatalf("注销后入口树里还剩 %d 个 fiber", got)
				}
				h.app.Close()
				b.StartTimer()
			}
			b.ReportMetric(nsPer(b.Elapsed(), b.N, n), "ns/tenant")
		})
	}
}

// ---------------------------------------------------------------------------
// Wait 扫描
// ---------------------------------------------------------------------------

// BenchmarkWaitScan 量的是 app.Wait() 里那次全局扫描的代价。
//
// 为什么要单独量：Wait 是 insula **唯一**的同步点（Loader 的每个
// 变更方法、Provision、Deprovision、闭环前的装配校验都要调它），
// 而 cordis 的 settled() 遍历的是 root registry 的**全部** fiber，
// 不是"待处理任务数"。也就是说它是 O(总 fiber 数) 而不是 O(队列长度)。
//
// 这个性质决定了 insula 的一条设计纪律：能并成一批的树操作必须并成
// 一批（否则 N 个租户就是 N 次 O(N) 扫描，O(N²)）。这条曲线是那条
// 纪律的量化依据，也是它的回归守卫。
func BenchmarkWaitScan(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			skipLarge(b, n)

			h := buildHarness(b)
			defer h.app.Close()

			if err := h.man.Provision(benchSpecs(n)); err != nil {
				b.Fatalf("Provision: %v", err)
			}
			fibers := countFibers(h.loader.Tree())
			b.ReportMetric(float64(fibers), "fibers")
			b.ReportMetric(float64(len(h.man.List())), "tenants")

			// 已经收敛之后的 Wait：量的是纯扫描，不含收敛等待。
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !h.app.Wait() {
					b.Fatal("app.Wait() 未收敛")
				}
			}
			b.StopTimer()
			b.ReportMetric(nsPer(b.Elapsed(), b.N, fibers), "ns/fiber")
		})
	}
}

// ---------------------------------------------------------------------------
// 单租户足迹
// ---------------------------------------------------------------------------

// BenchmarkFiberFootprint 量「一个租户在 cordis 里占多少常驻内存与
// 多少 fiber」。
//
// 用途：分片容量规划的分母就是这个数。它与租户的业务无关，因此
// 可以一次测准、长期引用；一旦有人给租户子树加了新的常驻结构
// （多挂一个服务、多开一份 store），这里会立刻显形。
//
// 这是一次性快照式的测量，不是吞吐测量：所以固定 b.N=1，
// 结果全部经 ReportMetric 给出，ns/op 没有意义。
func BenchmarkFiberFootprint(b *testing.B) {
	b.N = 1 // 见函数注释：这是快照，不是吞吐。

	for _, n := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			skipLarge(b, n)
			specs := benchSpecs(n)

			// 预热一次，把"只付一次"的成本摊掉：反射类型缓存、
			// 调度器缓冲、map/slice 的首次扩容。这些属于进程成本，
			// 不属于每个租户的成本。
			warm := buildHarness(b)
			if err := warm.man.Provision(specs); err != nil {
				b.Fatalf("预热 Provision: %v", err)
			}
			warm.app.Close()

			var before, after runtime.MemStats

			// 基线取在**空装载**之后：App、Manager、store、调度器
			// 这些是进程级的，若不减掉，n=1 的曲线会把它们全算到
			// 单个租户头上（n 越小误差越大）。
			h := buildHarness(b)
			runtime.GC()
			runtime.ReadMemStats(&before)

			if err := h.man.Provision(specs); err != nil {
				b.Fatalf("Provision: %v", err)
			}
			runtime.GC()
			runtime.ReadMemStats(&after)

			fibers := countFibers(h.loader.Tree())
			heap := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			if heap < 0 {
				// GC 之后小幅回落是正常的（例如审计环缓冲被压缩），
				// 报 0 比报一个负的字节数诚实。
				heap = 0
			}

			// 先断言"确实装上了"，再报数：一条量到 0 的曲线
			// 看起来像"租户不占内存"，实际是没装配成功。
			if got := len(h.man.List()); got != n {
				b.Fatalf("登记了 %d 个租户，期望 %d", got, n)
			}
			if fibers == 0 {
				b.Fatal("入口树上没有 fiber，装配未生效")
			}
			if got := fibers / n; got < 2 {
				b.Fatalf("每租户只有 %d 个 fiber，看起来租户子树没展开", got)
			}

			b.ReportMetric(float64(heap)/float64(n), "bytes/tenant")
			b.ReportMetric(float64(fibers)/float64(n), "fibers/tenant")
			b.ReportMetric(float64(fibers), "fibers")
			b.ReportMetric(float64(heap), "bytes")

			h.app.Close()
		})
	}
}

// nsPer 把总耗时摊到 N 个租户上。b.N 或 n 为 0 时返回 0，
// 避免基准里出现二分之一的除零 panic。
func nsPer(elapsed time.Duration, iterations, n int) float64 {
	if iterations <= 0 || n <= 0 {
		return 0
	}
	return float64(elapsed.Nanoseconds()) / float64(iterations) / float64(n)
}
