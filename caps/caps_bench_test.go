package caps_test

// BenchmarkEventFanout 量「一个能力变为可用 → K 个订阅方同时激活」
// 的扇出代价。
//
// # 为什么量的是这个扇出
//
// insula 一行 cordis 的 Emit/On 都没用（可 grep 验证），所以事件总线
// 扇出不是平台的热点。真正随订阅方数量增长的开销在这里：caps 的
// 可用性判断**整个**建立在 Inject 依赖机制上——Watcher 的存活状态
// 就等于「该服务此刻可用」，可用性由 cordis 的依赖机制免费完成，
// 不需要任何轮询（见 caps 包注释）。也就是说 cordis 的通知扇出
// 直接就是平台的可用性信号通路，它的常数因子就是每次服务变更的价格。
//
// # 为什么订阅方全在**同一个**隔离域
//
// 量的是「一个服务在一处变为可用，唤醒该处全部订阅方」。若把 K 个
// 订阅方撒到 K 个不同隔离域，测到的就变成「K 个互不相关的通知」，
// 那是另一件事（而且会掩盖跨域扫描的开销）。
//
// # 为什么用重复测量而不是让框架调 b.N
//
// 测量是「一次性快照 + K 次激活」，不是吞吐；让框架把 b.N 涨到
// 几千只会把同一份测量重复几千遍。因此固定 b.N=1，内部重复
// fanoutRepeats 次取平均，结果经 ReportMetric 给出。
//
//	go test -run '^$' -bench BenchmarkEventFanout -benchtime 1x ./caps/
//
// 取数时**单独跑这个包**（不要 go test ./...）：Go 默认并行跑不同包的
// 测试二进制，本基准的 20 次重复平均会被邻居的负载污染，K=1 那一行
// 能差出一倍。要稳定数字就加 -cpu 1。

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/realm"
	cordis "github.com/metaRobin/cordis"
)

// fanoutRepeats 是每个 K 下的重复次数。单次测量在微秒量级，
// 噪声足以淹没 10% 的差异；重复取平均把它压下去。
const fanoutRepeats = 20

// modelsOnlyProvider 只注册 models 一个服务。
//
// 不用 caps_test.go 里的 provider()：它一次注册四个服务，于是每轮
// 扇出会连带产生另外三次通知，量到的就不是「一个服务」的扇出。
func modelsOnlyProvider(m caps.Models) *cordis.Plugin {
	return &cordis.Plugin{
		Name: "bench-models-only",
		Apply: func(ctx *cordis.Context, _ any) error {
			_, err := ctx.Provide(realm.ServiceModels, m, nil)
			return err
		},
	}
}

// fanoutCase 是一轮扇出实验的现场。
type fanoutCase struct {
	app     *cordis.App
	realmID string
	handles []*caps.Handle
}

// newFanoutCase 装配 app、K 个句柄与 K 个 Watcher，全部落在同一隔离域。
//
// Watcher 此时**停在待激活状态**：models 还没人提供。这是扇出的起点——
// 订阅方先到齐，服务后到。反过来（先提供服务再挂订阅方）测不到
// 「一次提供唤醒 K 个等待者」这件事，因为订阅方挂上时就已经满足了。
//
// 注意 K 个 Watcher 共用**同一个插件定义**：registry 以插件指针为键
// 复用 Runtime，同一份定义挂 K 次就是 K 个 fiber。这也顺带说明
// 「一份定义、N 个实例」是 cordis 的原生形状。
func newFanoutCase(b *testing.B, k int) *fanoutCase {
	b.Helper()

	app := cordis.New()
	c := &fanoutCase{
		app:     app,
		realmID: "#bench-fan",
		handles: make([]*caps.Handle, 0, k),
	}

	watcher := caps.WatcherPlugin(realm.ServiceModels)
	ok := app.DoSync(func(ctx *cordis.Context) {
		rc := scope(ctx, c.realmID)
		for i := 0; i < k; i++ {
			h := caps.NewHandle(ident.Tenant(fmt.Sprintf("sub-%d", i)))
			c.handles = append(c.handles, h)
			if _, err := rc.Plugin(watcher, h); err != nil {
				b.Fatalf("挂第 %d 个 Watcher: %v", i, err)
			}
		}
	})
	if !ok {
		b.Fatal("调度器已停止")
	}
	if !app.Wait() {
		b.Fatal("钉住阶段未收敛")
	}

	// 起点必须是「全都在等」：若此刻已经有人激活，说明隔离域写错了
	// （K 个订阅方实际共享了某个已有提供者），后面的数字没有意义。
	for i, h := range c.handles {
		if got := len(h.Attached()); got != 0 {
			b.Fatalf("第 %d 个订阅方在提供服务之前就已激活: %v", i, h.Attached())
		}
	}
	return c
}

// fanout 提供 models 并等待全部 K 个订阅方激活，返回耗时。
func (c *fanoutCase) fanout(b *testing.B) time.Duration {
	b.Helper()

	prov := modelsOnlyProvider(&modelsTag{tag: "bench"})
	start := time.Now()

	if !c.app.DoSync(func(ctx *cordis.Context) {
		if _, err := scope(ctx, c.realmID).Plugin(prov, nil); err != nil {
			b.Fatalf("挂 provider: %v", err)
		}
	}) {
		b.Fatal("调度器已停止")
	}
	if !c.app.Wait() {
		b.Fatal("扇出未收敛")
	}

	elapsed := time.Since(start)

	// 激活数量必须精确等于 K。少一个都说明"扇出"没扇全，
	// 那样的数字是「K 个订阅方里激活了 n 个」的时间，不能除以 K。
	for i, h := range c.handles {
		if _, ok := h.Take().Models.(*modelsTag); !ok {
			b.Fatalf("第 %d 个订阅方没有被唤醒（Attached=%v）", i, h.Attached())
		}
	}
	return elapsed
}

// modelsTag 是本次基准专用的 models 实现：用一个只有本文件知道的
// 类型做断言，避免和 caps_test.go 里的 fakeModels 混淆。
type modelsTag struct{ tag string }

func (m *modelsTag) Complete(context.Context, caps.Request) (caps.Response, error) {
	return caps.Response{Message: caps.Message{Role: "assistant", Content: m.tag}}, nil
}

func (m *modelsTag) Healthy() bool { return true }

func BenchmarkEventFanout(b *testing.B) {
	b.N = 1 // 见文件头注释：快照式测量，不是吞吐。

	// 两个族必须**并列**看，它们量的是两件不同的事：
	//
	//   in_realm    一个隔离域内的扇出（本包自己的机制）；
	//   cross_realm 域外订阅者对这个域的干扰（cordis 通知索引的键设计）。
	//
	// 只测前者会得出「扇出是线性的，没问题」的结论；只有把后者也量出来，
	// 才可能看到「一个域的事件要扫描全部域的订阅者」——那是 2026-09-27 之前
	// 的真实行为。索引改按隔离域分桶之后，这条曲线掉了一个量级多，剩下的
	// 斜率不再是索引，而是计时区间里的 App.Wait()（见本文件末尾的附录）。
	b.Run("in_realm", func(b *testing.B) {
		// K 跨三个量级：1 是常数地板，10 是"一个租户顺手挂几个订阅方"，
		// 1000 是"一次服务变更要唤醒全部分片的租户"。
		for _, k := range []int{1, 10, 1000} {
			b.Run(fmt.Sprintf("subscribers=%d", k), func(b *testing.B) {
				var total time.Duration
				for r := 0; r < fanoutRepeats; r++ {
					c := newFanoutCase(b, k)
					total += c.fanout(b)
					c.app.Close()
				}
				per := total.Nanoseconds() / int64(fanoutRepeats)
				b.ReportMetric(float64(per), "ns/fanout")
				b.ReportMetric(float64(per)/float64(k), "ns/subscriber")
			})
		}
	})

	b.Run("cross_realm", func(b *testing.B) {
		// R 是"分片里已经有多少个租户"。measuring 只在**最后一个**域里
		// 提供服务，因此若通知成本与 R 无关，这条曲线应当是平的。
		for _, r := range []int{1, 100, 1000, 10000} {
			b.Run(fmt.Sprintf("realms=%d", r), func(b *testing.B) {
				var total time.Duration
				for rep := 0; rep < fanoutRepeats; rep++ {
					c := newCrossRealmCase(b, r)
					total += c.fanoutLastRealm(b)
					c.app.Close()
				}
				per := total.Nanoseconds() / int64(fanoutRepeats)
				b.ReportMetric(float64(per), "ns/notify")
				b.ReportMetric(float64(per)/float64(r), "ns/per-realm")
			})
		}
	})
}

// newCrossRealmCase 装配 R 个隔离域，每个域里挂 1 个待激活的 Watcher。
//
// 服务只在最后一个域里被提供。于是「提供一次」的期望成本与 R 无关：
// 只有那一个域里的订阅方需要被唤醒。若数字随 R 增长，说明通知在扫描
// 与本次事件无关的域。
func newCrossRealmCase(b *testing.B, r int) *fanoutCase {
	b.Helper()

	app := cordis.New()
	c := &fanoutCase{app: app, handles: make([]*caps.Handle, 0, r)}

	watcher := caps.WatcherPlugin(realm.ServiceModels)
	ok := app.DoSync(func(ctx *cordis.Context) {
		for i := 0; i < r; i++ {
			h := caps.NewHandle(ident.Tenant(fmt.Sprintf("r-%d", i)))
			c.handles = append(c.handles, h)
			if _, err := scope(ctx, fanoutRealm(i)).Plugin(watcher, h); err != nil {
				b.Fatalf("挂第 %d 个域的 Watcher: %v", i, err)
			}
		}
	})
	if !ok {
		b.Fatal("调度器已停止")
	}
	if !app.Wait() {
		b.Fatal("钉住阶段未收敛")
	}
	for i, h := range c.handles {
		if got := len(h.Attached()); got != 0 {
			b.Fatalf("第 %d 个域在提供服务之前就已激活: %v", i, got)
		}
	}
	return c
}

// fanoutLastRealm 只在最后一个域提供服务，返回「一次服务可用」的耗时。
func (c *fanoutCase) fanoutLastRealm(b *testing.B) time.Duration {
	b.Helper()

	last := len(c.handles) - 1
	prov := modelsOnlyProvider(&modelsTag{tag: "bench"})
	start := time.Now()

	if !c.app.DoSync(func(ctx *cordis.Context) {
		if _, err := scope(ctx, fanoutRealm(last)).Plugin(prov, nil); err != nil {
			b.Fatalf("挂 provider: %v", err)
		}
	}) {
		b.Fatal("调度器已停止")
	}
	if !c.app.Wait() {
		b.Fatal("扇出未收敛")
	}

	elapsed := time.Since(start)

	if _, ok := c.handles[last].Take().Models.(*modelsTag); !ok {
		b.Fatal("目标域的订阅方没有被唤醒")
	}
	// 别的域不该被顺带唤醒：那说明过滤是按名字做的，不是按域。
	for i, h := range c.handles {
		if i == last {
			continue
		}
		if h.Take().Models != nil {
			b.Fatalf("第 %d 个域被误唤醒 —— 通知没有按隔离域过滤", i)
		}
	}
	return elapsed
}

func fanoutRealm(i int) string { return fmt.Sprintf("#bench-realm-%d", i) }

// ---------------------------------------------------------------------------
// 附录：cross_realm 的残留斜率是谁的
// ---------------------------------------------------------------------------
//
// # 修复前（2026-09-27 之前）
//
// cordis 的服务通知索引是 `Reflect.index: map[string][]*Fiber`——**只按
// 服务名分桶，不按隔离域**。因此 notify() 必须先 candidates(names)
// 把整个桶复制一份（因为通知过程中 fiber 状态变化会反过来改这个桶，
// 不能边遍历边改），再逐个用 isolateKey 过滤掉不属于本次事件的订阅方。
//
// 后果：一个域里的事件，代价正比于**全部域**的订阅者总数。域内扇出本身
// 是线性的（in_realm 曲线），但只要域的数量也长起来，每次通知就要为别的
// 域付钱。实测 4.2 / 8.9 / 116 / 1975 µs（R = 1 / 100 / 1000 / 10000）。
//
// # 修复后（当前）
//
// 索引改为 `map[isolateKey][]*Fiber`，桶键与 store 的键同构（这是本层性能
// 的全部依据：问「该通知谁」的键，必须与解析会命中的键是同一个）。同一个
// 基准现在是 2.4 / 4.2 / 9.0 / 72.2 µs，R=10000 处改善 27 倍。
//
// # 残留的 72 µs 不是索引
//
// fanoutLastRealm 的计时区间是 `DoSync` + `app.Wait()`。索引查表已是 O(1)，
// 但 Wait() 仍会扫描**应用内全部 fiber**，而不是本次事件相关的那些。R 个域
// 各有一个待激活 Watcher，fiber 总数就正比于 R，于是这条曲线只剩下
// 「总 fiber 数 × ≈7 ns」——与 BenchmarkWaitScan 量到的是同一个 O(N)，
// 与索引改造无关。
//
// 换句话说：这条曲线现在测的其实是 cordis 缺陷清单里的第 1 处（Wait 的
// O(全部 fiber) 扫描），第 3 处（索引）已经从这里消失了。把两者分开看，
// 才不会把「Wait 慢」误记到索引头上，也不会在索引已经修好之后继续
// 找它的斜率。

// 扇出的**数量**正确性是功能问题，不是性能问题，但放在这里：
// 性能基准会调 K，而"K 变大时有人漏唤醒"正是最容易被数字掩盖的
// 失败模式（漏唤醒只会让总耗时变快，看起来像优化）。
func TestFanoutWakesEverySubscriber(t *testing.T) {
	app := cordis.New()
	defer app.Close()

	for _, k := range []int{1, 7} {
		t.Run(fmt.Sprintf("subscribers=%d", k), func(t *testing.T) {
			realmID := fmt.Sprintf("#fan-%d", k)
			watcher := caps.WatcherPlugin(realm.ServiceModels)
			handles := make([]*caps.Handle, 0, k)

			if !app.DoSync(func(ctx *cordis.Context) {
				rc := scope(ctx, realmID)
				for i := 0; i < k; i++ {
					h := caps.NewHandle(ident.Tenant(fmt.Sprintf("s-%d", i)))
					handles = append(handles, h)
					if _, err := rc.Plugin(watcher, h); err != nil {
						t.Fatalf("挂 Watcher: %v", err)
					}
				}
			}) {
				t.Fatal("调度器已停止")
			}
			if !app.Wait() {
				t.Fatal("未收敛")
			}
			for i, h := range handles {
				if got := h.Attached(); len(got) != 0 {
					t.Fatalf("订阅方 %d 提前激活: %v", i, got)
				}
			}

			if !app.DoSync(func(ctx *cordis.Context) {
				rc := scope(ctx, realmID)
				if _, err := rc.Plugin(modelsOnlyProvider(&modelsTag{tag: "on"}), nil); err != nil {
					t.Fatalf("挂 provider: %v", err)
				}
			}) {
				t.Fatal("调度器已停止")
			}
			if !app.Wait() {
				t.Fatal("未收敛")
			}

			for i, h := range handles {
				snap := h.Take()
				if snap.Models == nil {
					t.Fatalf("订阅方 %d 没有被唤醒", i)
				}
				if got, ok := snap.Models.(*modelsTag); !ok || got.tag != "on" {
					t.Fatalf("订阅方 %d 拿到 %#v", i, snap.Models)
				}
				if names := h.Attached(); !strings.Contains(strings.Join(names, ","), realm.ServiceModels) {
					t.Fatalf("订阅方 %d 的 Attached = %v", i, names)
				}
			}
		})
	}
}
