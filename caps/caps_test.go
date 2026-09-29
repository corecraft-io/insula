package caps_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/corecraft-io/insula/caps"
	"github.com/corecraft-io/insula/guard"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/realm"
	cordis "github.com/metaRobin/cordis"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// scope 把 ctx 派生到一个租户私有域。
//
// 这与 loader 处理 EntryOptions.Isolate = realm.Tenant() 的效果等价：
// cordis 对 true 值会把域键解析为 "#" + 入口ID。测试里直接给一个
// 可读的域标签，便于失败时定位是哪个租户。关键点是**逐服务**声明，
// 与 realm.Services() 保持同一来源。
func scope(parent *cordis.Context, realmID string) *cordis.Context {
	c := parent
	for _, name := range realm.Services() {
		c = c.Isolate(name, realmID)
	}
	return c
}

// fakeModels 是一个可区分实例的假模型网关。
type fakeModels struct {
	tag     string
	healthy bool
}

func (f *fakeModels) Complete(ctx context.Context, req caps.Request) (caps.Response, error) {
	return caps.Response{Message: caps.Message{Role: "assistant", Content: f.tag}}, nil
}

func (f *fakeModels) Healthy() bool { return f.healthy }

type fakeMemory struct{ tag string }

func (m *fakeMemory) AppendTurn(ident.Tenant, ident.Session, memory.Turn) (int, error) {
	return 0, nil
}
func (m *fakeMemory) Turns(ident.Tenant, ident.Session) ([]memory.Turn, error) { return nil, nil }
func (m *fakeMemory) ReplaceTurns(ident.Tenant, ident.Session, []memory.Turn) error {
	return nil
}
func (m *fakeMemory) PutDoc(ident.Tenant, memory.Doc) error                   { return nil }
func (m *fakeMemory) Search(ident.Tenant, memory.Query) ([]memory.Hit, error) { return nil, nil }

type fakeTools struct{ tag string }

func (t *fakeTools) Specs() []caps.ToolSpec { return nil }
func (t *fakeTools) Call(context.Context, caps.Invocation) (caps.ToolResult, error) {
	return caps.ToolResult{Content: t.tag}, nil
}

// provider 返回一个把租户级能力注册到当前上下文的插件。
//
// 传 nil 的能力**不注册**——这才是真实的「缺失」场景：Watcher 的 Inject
// 解析不到，fiber 干脆不激活。若把 nil 也注册进去，就变成了「注册了一个
// 空值」，那是类型不符而不是缺失，走的是另一条分支。
//
// guard 是必需能力，因此这里总是注册一份（每个 provider 一份独立策略，
// 顺带让隔离断言能覆盖它）。要测「缺 guard」用 providerWithoutGuard。
func provider(models caps.Models, mem caps.Memory, tools caps.Tools) *cordis.Plugin {
	return provide(models, mem, tools, guard.NewPolicy(guard.Default(), nil))
}

func providerWithoutGuard(models caps.Models, mem caps.Memory, tools caps.Tools) *cordis.Plugin {
	return provide(models, mem, tools, nil)
}

// reflectiveNil 识别类型化 nil（(*guard.Policy)(nil) 这类）：
// 它在 any 里不等于 nil，但解引用必崩。
func reflectiveNil(v any) bool {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	}
	return false
}

func provide(models caps.Models, mem caps.Memory, tools caps.Tools, g *guard.Policy) *cordis.Plugin {
	return &cordis.Plugin{
		Name: "fake-tenant-services",
		Apply: func(ctx *cordis.Context, _ any) error {
			for _, kv := range []struct {
				name  string
				value any
			}{
				{realm.ServiceModels, models},
				{realm.ServiceMemory, mem},
				{realm.ServiceTools, tools},
				{realm.ServiceGuard, g},
			} {
				if kv.value == nil || reflectiveNil(kv.value) {
					continue
				}
				if _, err := ctx.Provide(kv.name, kv.value, nil); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

type harness struct {
	t   *testing.T
	app *cordis.App
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return &harness{t: t, app: cordis.New()}
}

func (h *harness) run(f func(ctx *cordis.Context)) {
	h.app.DoSync(f)
	if !h.app.Wait() {
		h.t.Fatal("app did not settle")
	}
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

// 这是本包最吃重的一条：两个租户各自注册同名服务，各自的能力句柄必须
// 拿到自己的实例。如果 Watcher 依赖的是沿 fiber 链向上查找（而不是
// 隔离域内的 Inject 解析），这里会解析到同一个实例。
func TestTwoTenantsGetTheirOwnCapabilities(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	hA := caps.NewHandle(ident.Tenant("tenant-a"))
	hB := caps.NewHandle(ident.Tenant("tenant-b"))

	modelsA, modelsB := &fakeModels{tag: "A", healthy: true}, &fakeModels{tag: "B", healthy: true}
	memA, memB := &fakeMemory{tag: "A"}, &fakeMemory{tag: "B"}
	toolsA, toolsB := &fakeTools{tag: "A"}, &fakeTools{tag: "B"}

	h.run(func(ctx *cordis.Context) {
		scope(ctx, "#t-aaa").Plugin(provider(modelsA, memA, toolsA), nil)
		scope(ctx, "#t-bbb").Plugin(provider(modelsB, memB, toolsB), nil)
		for _, w := range hA.Watchers() {
			scope(ctx, "#t-aaa").Plugin(w, nil)
		}
		for _, w := range hB.Watchers() {
			scope(ctx, "#t-bbb").Plugin(w, nil)
		}
	})

	snapA, snapB := hA.Take(), hB.Take()
	if !snapA.Ready() || !snapB.Ready() {
		t.Fatalf("both snapshots should be ready: A=%v B=%v", snapA, snapB)
	}
	if snapA.Degraded() || snapB.Degraded() {
		t.Fatalf("neither snapshot should be degraded: A=%v B=%v", snapA, snapB)
	}

	if got := snapA.Models.(*fakeModels).tag; got != "A" {
		t.Fatalf("tenant A resolved foreign models instance: tag=%q", got)
	}
	if got := snapB.Models.(*fakeModels).tag; got != "B" {
		t.Fatalf("tenant B resolved foreign models instance: tag=%q", got)
	}

	// 更强的一条：两个句柄拿到的必须是不同实例，而不只是标签不同。
	if realm.SameInstance(snapA.Models, snapB.Models) {
		t.Fatal("two tenants resolved the same models instance — realm isolation is broken")
	}
	if realm.SameInstance(snapA.Memory, snapB.Memory) {
		t.Fatal("two tenants resolved the same memory instance — realm isolation is broken")
	}
	if realm.SameInstance(snapA.Tools, snapB.Tools) {
		t.Fatal("two tenants resolved the same tools instance — realm isolation is broken")
	}
}

// 缺失可选能力（tools）应当是降级而不是故障：快照仍 Ready，但标记 Degraded，
// 且 Missing 精确指出缺的是哪一个。
func TestMissingOptionalCapabilityDegradesWithoutBlocking(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	hA := caps.NewHandle(ident.Tenant("tenant-a"))
	h.run(func(ctx *cordis.Context) {
		// 只注册 models 与 memory，不注册 tools。
		scope(ctx, "#t-aaa").Plugin(provider(&fakeModels{tag: "A", healthy: true},
			&fakeMemory{tag: "A"}, nil), nil)
		for _, w := range hA.Watchers() {
			scope(ctx, "#t-aaa").Plugin(w, nil)
		}
	})

	snap := hA.Take()
	if !snap.Ready() {
		t.Fatalf("missing optional service must not block a run: %v", snap)
	}
	if !snap.Degraded() {
		t.Fatalf("missing tools should mark the snapshot degraded: %v", snap)
	}
	if len(snap.Missing) != 1 || snap.Missing[0] != realm.ServiceTools {
		t.Fatalf("Missing = %v, want exactly [%s]", snap.Missing, realm.ServiceTools)
	}

	// tools 为 nil 时必须能安全判断，而不是拿到一个非 nil 的坏接口值。
	if snap.Tools != nil {
		t.Fatalf("absent tools must be a nil interface, got %#v", snap.Tools)
	}
}

// 缺失**必需**能力（models）必须让 Ready() 为 false —— 数据面据此拒绝启动循环。
func TestMissingRequiredCapabilityBlocksRun(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	hA := caps.NewHandle(ident.Tenant("tenant-a"))
	h.run(func(ctx *cordis.Context) {
		scope(ctx, "#t-aaa").Plugin(provider(nil,
			&fakeMemory{tag: "A"}, &fakeTools{tag: "A"}), nil)
		for _, w := range hA.Watchers() {
			scope(ctx, "#t-aaa").Plugin(w, nil)
		}
	})

	snap := hA.Take()
	if snap.Ready() {
		t.Fatalf("missing models must block a run: %v", snap)
	}
	if snap.Models != nil {
		t.Fatalf("absent models must be a nil interface, got %#v", snap.Models)
	}
}

// 缺失 guard 必须拦下 run：它是唯一一处「缺失会让平台失去防护」的能力，
// 没有预算策略的循环可以无限跑。
func TestMissingGuardBlocksRun(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	hA := caps.NewHandle(ident.Tenant("tenant-a"))
	h.run(func(ctx *cordis.Context) {
		scope(ctx, "#t-aaa").Plugin(providerWithoutGuard(
			&fakeModels{tag: "A", healthy: true}, &fakeMemory{tag: "A"}, &fakeTools{tag: "A"}), nil)
		for _, w := range hA.Watchers() {
			scope(ctx, "#t-aaa").Plugin(w, nil)
		}
	})

	snap := hA.Take()
	if snap.Ready() {
		t.Fatalf("missing guard must block a run: %v", snap)
	}
	if snap.Guard != nil {
		t.Fatalf("absent guard must be a nil pointer, got %#v", snap.Guard)
	}
	if !snap.Degraded() {
		t.Fatalf("missing guard should mark the snapshot degraded: %v", snap)
	}
}

// 预算策略必须是每租户独立的实例，否则黑盒隔离断言会误判。
func TestGuardPolicyIsPerTenant(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	hA := caps.NewHandle(ident.Tenant("tenant-a"))
	hB := caps.NewHandle(ident.Tenant("tenant-b"))
	h.run(func(ctx *cordis.Context) {
		scope(ctx, "#t-aaa").Plugin(provider(&fakeModels{tag: "A", healthy: true},
			&fakeMemory{tag: "A"}, &fakeTools{tag: "A"}), nil)
		scope(ctx, "#t-bbb").Plugin(provider(&fakeModels{tag: "B", healthy: true},
			&fakeMemory{tag: "B"}, &fakeTools{tag: "B"}), nil)
		for _, w := range hA.Watchers() {
			scope(ctx, "#t-aaa").Plugin(w, nil)
		}
		for _, w := range hB.Watchers() {
			scope(ctx, "#t-bbb").Plugin(w, nil)
		}
	})

	snapA, snapB := hA.Take(), hB.Take()
	if snapA.Guard == nil || snapB.Guard == nil {
		t.Fatalf("both tenants should have a guard policy: A=%v B=%v", snapA, snapB)
	}
	if realm.SameInstance(snapA.Guard, snapB.Guard) {
		t.Fatal("two tenants resolved the same guard policy — realm isolation is broken")
	}
}

// 没有任何 Watcher 时快照为空且不可用；租户子树拆除后必须回到这个状态，
// 不能因为句柄里还留着旧值而让已下线的租户继续跑。
func TestSnapshotEmptiesWhenTenantTearsDown(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	hA := caps.NewHandle(ident.Tenant("tenant-a"))
	if snap := hA.Take(); snap.Ready() || !snap.Degraded() {
		t.Fatalf("fresh handle must not be ready: %v", snap)
	}
	if _, ok := hA.Context(); ok {
		t.Fatal("fresh handle must not expose a tenant context")
	}

	var watchers = make(map[string]*cordis.Fiber)
	h.run(func(ctx *cordis.Context) {
		scope(ctx, "#t-aaa").Plugin(provider(&fakeModels{tag: "A", healthy: true},
			&fakeMemory{tag: "A"}, &fakeTools{tag: "A"}), nil)
		// 按服务名逐个挂载，才能按服务名精确拆除。
		for _, name := range realm.Services() {
			f, err := scope(ctx, "#t-aaa").Plugin(hA.Watcher(name), nil)
			if err != nil {
				t.Fatal(err)
			}
			watchers[name] = f
		}
	})
	if !hA.Take().Ready() {
		t.Fatal("snapshot should be ready after provisioning")
	}
	if _, ok := hA.Context(); !ok {
		t.Fatal("snapshot should expose the tenant context while live")
	}

	// 拆掉一个**可选**能力的 Watcher：快照应变为降级但仍可运行。
	h.run(func(ctx *cordis.Context) {
		watchers[realm.ServiceTools].Dispose()
	})
	snap := hA.Take()
	if !snap.Ready() {
		t.Fatalf("losing an optional capability must not block runs: %v", snap)
	}
	if !snap.Degraded() {
		t.Fatalf("losing tools must mark the snapshot degraded: %v", snap)
	}

	// 再拆掉一个**必需**能力：快照变得不可运行。
	h.run(func(ctx *cordis.Context) {
		watchers[realm.ServiceModels].Dispose()
	})
	if hA.Take().Ready() {
		t.Fatalf("losing models must block runs: %v", hA.Take())
	}

	// 全部拆完：快照清空，上下文一并收回。
	h.run(func(ctx *cordis.Context) {
		for _, f := range watchers {
			f.Dispose()
		}
	})
	if snap := hA.Take(); snap.Ready() {
		t.Fatalf("snapshot must be empty after full teardown: %v", snap)
	}
	if _, ok := hA.Context(); ok {
		t.Fatal("context must be withdrawn once every watcher is gone")
	}
}

// 同一服务名被两个 Watcher 引用时，先卸载的那个不得把仍然可用的能力
// 误标为缺失——这是引用计数存在的唯一理由。
func TestDuplicateWatcherUsesRefcount(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	hA := caps.NewHandle(ident.Tenant("tenant-a"))
	var first, second *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		scope(ctx, "#t-aaa").Plugin(provider(&fakeModels{tag: "A", healthy: true},
			&fakeMemory{tag: "A"}, &fakeTools{tag: "A"}), nil)
		first, _ = scope(ctx, "#t-aaa").Plugin(hA.Watcher(realm.ServiceModels), nil)
		second, _ = scope(ctx, "#t-aaa").Plugin(hA.Watcher(realm.ServiceModels), nil)
	})
	// 这里只看 models 是否在册：本用例没有挂 memory 的 Watcher，
	// 所以不能用 Ready()（它要求必需能力齐备）。
	if !hasService(hA.Take(), realm.ServiceModels) {
		t.Fatalf("models should be present with two watchers: %v", hA.Take())
	}

	h.run(func(ctx *cordis.Context) { first.Dispose() })
	if !hasService(hA.Take(), realm.ServiceModels) {
		t.Fatalf("one of two watchers leaving must not remove the capability: %v", hA.Take())
	}

	h.run(func(ctx *cordis.Context) { second.Dispose() })
	if hasService(hA.Take(), realm.ServiceModels) {
		t.Fatalf("last watcher leaving must remove the capability: %v", hA.Take())
	}
}

// hasService 报告快照的 Present 列表里是否含某服务。
func hasService(s caps.Snapshot, name string) bool {
	for _, n := range s.Present {
		if n == name {
			return true
		}
	}
	return false
}

// Watcher 的数量必须覆盖 realm.Services()，否则新增服务时会漏探活。
// 这条守卫的是「新增服务忘了挂 Watcher」这一类静默缺口。
func TestWatchersCoverEveryTenantService(t *testing.T) {
	hA := caps.NewHandle(ident.Tenant("tenant-a"))
	ws := hA.Watchers()
	if len(ws) != realm.ServiceCount() {
		t.Fatalf("watchers = %d, want %d (one per realm.Services() entry)",
			len(ws), realm.ServiceCount())
	}
	seen := make(map[string]bool)
	for _, w := range ws {
		if len(w.Inject) != 1 {
			t.Fatalf("watcher %s must declare exactly one dependency, got %d",
				w.Name, len(w.Inject))
		}
		for name := range w.Inject {
			if seen[name] {
				t.Fatalf("duplicate watcher for service %q", name)
			}
			seen[name] = true
		}
	}
	for _, name := range realm.Services() {
		if !seen[name] {
			t.Fatalf("service %q has no watcher", name)
		}
	}
}

// 提供者注册一个类型化 nil 指针时，类型断言会成功——若不显式处理，
// 快照会宣称「能力已就绪」，调用方下一步解引用就崩。这类值必须
// 按「注册了但不可用」处理。
func TestTypedNilCapabilityCountsAsUnusable(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	hA := caps.NewHandle(ident.Tenant("tenant-a"))
	// 注意：提供者必须注册在隔离域内。直接在 DoSync 闭包里 ctx.Provide
	// 用的是 app.root，落在默认域——隔离域里的 Watcher 看不见它，
	// 测试就会因为「什么都没提供」而通过，掩盖真正要验的那条。
	typedNil := &cordis.Plugin{
		Name: "typed-nil-provider",
		Apply: func(c *cordis.Context, _ any) error {
			var nilPolicy *guard.Policy
			if _, err := c.Provide(realm.ServiceGuard, nilPolicy, nil); err != nil {
				return err
			}
			if _, err := c.Provide(realm.ServiceModels, &fakeModels{tag: "A", healthy: true}, nil); err != nil {
				return err
			}
			_, err := c.Provide(realm.ServiceMemory, &fakeMemory{tag: "A"}, nil)
			return err
		},
	}
	h.run(func(ctx *cordis.Context) {
		scope(ctx, "#t-aaa").Plugin(typedNil, nil)
		for _, w := range hA.Watchers() {
			scope(ctx, "#t-aaa").Plugin(w, nil)
		}
	})

	snap := hA.Take()
	if snap.Ready() {
		t.Fatalf("a typed-nil capability must not count as ready: %v", snap)
	}
	if snap.Guard != nil {
		t.Fatalf("typed-nil guard must surface as nil, got %#v", snap.Guard)
	}
	found := false
	for _, n := range snap.BadType {
		if n == realm.ServiceGuard {
			found = true
		}
	}
	if !found {
		t.Fatalf("typed-nil service must be reported in BadType, got %v", snap)
	}
}
