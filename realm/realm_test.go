package realm_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/corecraft-io/insula/realm"
	cordis "github.com/metaRobin/cordis"
)

func TestHasherIsStableAndDistinct(t *testing.T) {
	h := realm.NewHasher(nil)

	if a, b := h.Sum("tenant-a"), h.Sum("tenant-a"); a != b {
		t.Fatalf("hash is not deterministic: %q != %q", a, b)
	}
	if h.Sum("tenant-a") == h.Sum("tenant-b") {
		t.Fatal("distinct tenants produced the same hash")
	}
	if got := len(h.Sum("x")); got != 12 {
		t.Fatalf("hash length = %d, want 12", got)
	}

	// 加盐哈希必须既稳定又与无盐不同（IDOR 防线）。
	salted := realm.NewHasher([]byte("platform-secret"))
	if salted.Sum("tenant-a") != salted.Sum("tenant-a") {
		t.Fatal("salted hash is not deterministic")
	}
	if salted.Sum("tenant-a") == h.Sum("tenant-a") {
		t.Fatal("salted hash equals unsalted hash")
	}
}

func TestEntryIDsCarryTenantNamespace(t *testing.T) {
	h := realm.NewHasher(nil)
	a := h.TenantEntry("tenant-a")
	b := h.TenantEntry("tenant-b")

	for _, id := range []string{a, b} {
		if err := realm.CheckTenantEntry(id); err != nil {
			t.Fatalf("CheckTenantEntry(%q): %v", id, err)
		}
	}

	// 这是全篇最关键的一条不变量：同名子入口在两个租户下必须得到不同的短 ID，
	// 否则 EntryTree.store 的扁平索引会静默串租。
	for _, name := range []string{"models", "caps", realm.PluginSession} {
		if realm.Child(a, name) == realm.Child(b, name) {
			t.Fatalf("child id for %q collides across tenants", name)
		}
	}
	if realm.SessionEntry(a, 1) == realm.SessionEntry(b, 1) {
		t.Fatal("session entry id collides across tenants")
	}
	if realm.SessionsGroupEntry(a) == realm.SessionsGroupEntry(b) {
		t.Fatal("sessions group id collides across tenants")
	}

	if err := realm.CheckChildEntry(a, realm.Child(a, "models")); err != nil {
		t.Fatalf("valid child rejected: %v", err)
	}
	if err := realm.CheckChildEntry(a, realm.SessionEntry(a, 7)); err != nil {
		t.Fatalf("valid session entry rejected: %v", err)
	}
}

func TestCheckRejectsForeignAndMalformedIDs(t *testing.T) {
	h := realm.NewHasher(nil)
	a := h.TenantEntry("tenant-a")
	b := h.TenantEntry("tenant-b")

	cases := []struct {
		name string
		id   string
	}{
		{"foreign tenant namespace", realm.Child(b, "models")},
		{"bare sequence number", "s1"},
		{"missing namespace", "models"},
		{"empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := realm.CheckChildEntry(a, tc.id); err == nil {
				t.Fatalf("CheckChildEntry(%q, %q) = nil, want error", a, tc.id)
			}
		})
	}

	// 路径分隔符出现在短 ID 里会破坏 Resolve 的分段语义。
	if err := realm.CheckChildEntry(a, a+"-x:y"); err == nil {
		t.Fatal("id containing ':' was accepted")
	}

	for _, bad := range []string{"t-", "x-0123456789ab", "t-tooshort", a + "0"} {
		if err := realm.CheckTenantEntry(bad); err == nil {
			t.Fatalf("CheckTenantEntry(%q) = nil, want error", bad)
		}
	}
}

// TestTenantIsolateCoversEveryService 是防止「漏声明 Isolate」的守门测试。
//
// 任何新增的租户级服务如果忘记登记进 tenantServices，这里会失败——
// 而漏声明的后果是静默的全平台共享，不是报错。
func TestTenantIsolateCoversEveryService(t *testing.T) {
	iso := realm.Tenant()

	services := realm.Services()
	if len(services) == 0 {
		t.Fatal("no tenant services declared")
	}
	if len(iso) != len(services) {
		t.Fatalf("Tenant() has %d entries but Services() has %d", len(iso), len(services))
	}
	for _, name := range services {
		v, ok := iso[name]
		if !ok {
			t.Fatalf("service %q missing from Tenant() isolate map", name)
		}
		if v != true {
			t.Fatalf("service %q isolate value = %#v, want true (private realm)", name, v)
		}
		if realm.SharedRealmLabel(v) {
			t.Fatalf("service %q uses a shared realm label", name)
		}
	}
}

func TestSessionIsolateIsNarrowerThanTenant(t *testing.T) {
	tenant := realm.Tenant()
	session := realm.Session()

	if len(session) == 0 {
		t.Fatal("no session services declared")
	}
	for _, name := range realm.SessionServices() {
		if _, ok := session[name]; !ok {
			t.Fatalf("session service %q missing from Session()", name)
		}
		// 会话级服务允许覆盖出租户域；但若它同时出现在租户清单里，
		// 说明两个层级的划分本身有歧义。
		if _, dup := tenant[name]; dup {
			t.Fatalf("service %q is declared both tenant-level and session-level", name)
		}
	}
}

func TestSameInstance(t *testing.T) {
	type handle struct{ n int }
	h := &handle{1}

	if !realm.SameInstance(h, h) {
		t.Fatal("same pointer reported as different")
	}
	if realm.SameInstance(h, &handle{1}) {
		t.Fatal("distinct pointers reported as same")
	}
	if !realm.SameInstance(nil, nil) {
		t.Fatal("nil,nil reported as different")
	}
	if realm.SameInstance(h, nil) {
		t.Fatal("ptr,nil reported as same")
	}
	if realm.SameInstance(h, "string") {
		t.Fatal("different types reported as same")
	}
}

func TestServiceNamesAreNonEmptyAndUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, name := range append(realm.Services(), realm.SessionServices()...) {
		if name == "" {
			t.Fatal("empty service name")
		}
		if strings.ContainsAny(name, ": \t") {
			t.Fatalf("service name %q contains a path separator or whitespace", name)
		}
		if seen[name] {
			t.Fatalf("duplicate service name %q", name)
		}
		seen[name] = true
	}
}

// ---------------------------------------------------------------------------
// 共享域标签与 Isolate 声明扫描
// ---------------------------------------------------------------------------

// TestSharedRealmLabelDetectsStrings 让这个哨兵真的有判别力。
//
// 它此前只在 TestTenantIsolateCoversEveryService 里被调用，而那里的值
// 已经被上一行断言成 true 了——也就是说它**只被喂过 true**，判别分支
// 从未执行；把实现改成恒返回 false，全套测试仍然全绿。那正是最危险的
// 一类测试：守门函数本身没被守。
func TestSharedRealmLabelDetectsStrings(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  bool
	}{
		{"private realm (true)", true, false},
		{"explicitly not isolated (false)", false, false},
		{"nil", nil, false},
		{"shared realm label", "@shared", true},
		{"empty label", "", true},
		{"label-looking string", "#t-abc", true},
		{"number", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := realm.SharedRealmLabel(tc.value); got != tc.want {
				t.Fatalf("SharedRealmLabel(%#v) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

// newTree 起一个可以建入口的最小 cordis 树。
//
// 用真的 loader 而不是替身：被扫的正是 loader 写进 Options 的那些声明，
// 替身会把这条通路的正确性排除在测试之外。
func newTree(t *testing.T) *cordis.Loader {
	t.Helper()
	app := cordis.New()
	t.Cleanup(app.Close)
	return cordis.NewLoader(app, func(name string) (*cordis.Plugin, error) {
		return &cordis.Plugin{
			Name:  name,
			Apply: func(*cordis.Context, any) error { return nil },
		}, nil
	})
}

// TestCheckIsolateDeclarationsAcceptsPrivateRealms 是干净那一向。
//
// 除了「没报错」，还要断言**扫到了东西**：一个遍历写错、根本没进循环的
// 实现同样不报错，而它会让这条守门测试永远绿。（PairsChecked 那条纪律。）
func TestCheckIsolateDeclarationsAcceptsPrivateRealms(t *testing.T) {
	loader := newTree(t)
	root, err := loader.Create(cordis.EntryOptions{
		ID:      "t-abc",
		Name:    realm.PluginTenantRoot,
		Group:   true,
		Isolate: realm.Tenant(),
		Config: []cordis.EntryOptions{{
			ID:      "t-abc-sessions",
			Name:    realm.PluginSessions,
			Group:   true,
			Config:  []cordis.EntryOptions{},
			Isolate: nil, // 分组刻意不带声明
		}},
	}, "", 0)
	if err != nil {
		t.Fatalf("create root: %v", err)
	}

	n, err := realm.CheckIsolateDeclarations(root)
	if err != nil {
		t.Fatalf("CheckIsolateDeclarations: %v", err)
	}
	if want := len(realm.Services()); n != want {
		t.Fatalf("declarations = %d, want %d (realm.Tenant() 的条数)", n, want)
	}
}

// TestCheckIsolateDeclarationsRejectsEveryNonPrivateValue 是脏的那一向。
//
// 三种写法都要被拒，因为 cordis 对它们的处理方式不同，但后果都是共享：
// 字符串落到 "@标签" 共享域；false 把自己从外层域摘出去；其它类型被
// 当成未声明而回落。三种都不报错——所以必须由这里拒绝。
func TestCheckIsolateDeclarationsRejectsEveryNonPrivateValue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		label any
		want  string // 错误消息里必须出现的东西
	}{
		{"shared label", "@shared", "shared realm label"},
		{"explicit false", false, "removes it from the enclosing realm"},
		{"nil value", nil, "nil"},
		{"wrong type", 42, "42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loader := newTree(t)
			root, err := loader.Create(cordis.EntryOptions{
				ID:      "t-bad",
				Name:    realm.PluginTenantRoot,
				Isolate: map[string]any{"memory": tc.label},
			}, "", 0)
			if err != nil {
				t.Fatalf("create root: %v", err)
			}

			n, err := realm.CheckIsolateDeclarations(root)
			if !errors.Is(err, realm.ErrSharedRealm) {
				t.Fatalf("err = %v, want ErrSharedRealm", err)
			}
			if n != 1 {
				t.Errorf("declarations = %d, want 1（坏的那条也要计入已扫过的数）", n)
			}
			// 错误消息必须能直接定位：入口 ID 与服务名缺一不可，
			// 否则线上拿到这条日志的人得自己把树打出来找。
			if !strings.Contains(err.Error(), "t-bad") {
				t.Errorf("error %q must name the offending entry id", err)
			}
			if !strings.Contains(err.Error(), `"memory"`) {
				t.Errorf("error %q must name the offending service", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q must explain the value (%q)", err, tc.want)
			}
		})
	}
}

// TestCheckIsolateDeclarationsWalksSubgroups 钉住递归。
//
// 会写错声明的地方恰恰是**子入口**：租户根是平台自己建的，而子入口
// 数量多、新增频繁。只扫根的实现会把这一整类回归放过去。
func TestCheckIsolateDeclarationsWalksSubgroups(t *testing.T) {
	loader := newTree(t)
	root, err := loader.Create(cordis.EntryOptions{
		ID:      "t-abc",
		Name:    realm.PluginTenantRoot,
		Group:   true,
		Isolate: realm.Tenant(), // 根是干净的……
		Config: []cordis.EntryOptions{{
			ID:     "t-abc-sessions",
			Name:   realm.PluginSessions,
			Group:  true,
			Config: []cordis.EntryOptions{},
			// ……脏的这一条埋在第一层子入口上。
			Isolate: map[string]any{realm.ServiceHistory: "@shared"},
		}},
	}, "", 0)
	if err != nil {
		t.Fatalf("create root: %v", err)
	}

	n, err := realm.CheckIsolateDeclarations(root)
	if !errors.Is(err, realm.ErrSharedRealm) {
		t.Fatalf("err = %v, want the subgroup's shared label to be caught", err)
	}
	if !strings.Contains(err.Error(), "t-abc-sessions") {
		t.Errorf("error %q must name the nested entry", err)
	}
	// 根上那 5 条 + 子入口那 1 条 = 6 条才报错。
	if want := len(realm.Services()) + 1; n != want {
		t.Errorf("declarations = %d, want %d", n, want)
	}
}

// TestCheckIsolateDeclarationsRejectsNilRoot 断言「子树丢了」不会伪装成
// 「查过且干净」。
//
// 返回 (0, nil) 会让调用方把一个装配错误读成一次成功的自检——
// 而自检的全部价值就在于它说的"干净"是可信的。
func TestCheckIsolateDeclarationsRejectsNilRoot(t *testing.T) {
	n, err := realm.CheckIsolateDeclarations(nil)
	if !errors.Is(err, realm.ErrSharedRealm) {
		t.Fatalf("err = %v, want ErrSharedRealm", err)
	}
	if n != 0 {
		t.Errorf("declarations = %d, want 0", n)
	}
}
