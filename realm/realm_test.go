package realm_test

import (
	"strings"
	"testing"

	"github.com/metaRobin/insula/realm"
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
