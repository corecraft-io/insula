package tenant_test

import (
	"errors"
	"strings"
	"testing"

	cordis "github.com/corecraft-io/cordis"
	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/realm"
	"github.com/corecraft-io/insula/tenant"
)

// 本文件守的是 SAFETY.md 的 **R3：共享域（@label）在平台层禁用**。
//
// 在补上它之前，「Isolate 只接受 true」这条规则实际上只被
// realm_test.go 的一个循环守着一半：那个循环检查的是
// realm.Tenant()/realm.Session() **两张映射的内容**，而不是入口**真正
// 声明了什么**。于是下面每一条都曾经能在全绿的情况下溜过去：
//
//   - 某个子入口的 EntryOptions.Isolate 里写了 "@shared"（cordis 会
//     老实地建成共享域，两个租户静默拿到同一个实例，不报错）；
//   - 某处写了 false（把自己从外层域摘出去，效果同样是共享）；
//   - 租户根忘了把 realm.Tenant() 挂上去（那么整棵子树落回默认域）。
//
// 也就是说：规则的两半——「映射的内容对」与「入口声明的是这个映射」——
// 只有前一半被守住了。这半条守门测试的设计意图（一旦有人手工造入口就
// 立刻失败）此前并不成立。

// declaration 是一条被扫到的 Isolate 声明。
type declaration struct {
	entry   string // 入口全路径 ID
	service string
	label   any
}

// collectDeclarations 独立地走一遍整棵树，把每条声明连入口一起记下来。
//
// 刻意不复用 realm.CheckIsolateDeclarations：那是被测对象，用它来
// 构造期望值等于自证。这里要的是「树里到底写了什么」这个事实。
func collectDeclarations(t *testing.T, root *cordis.EntryGroup, out *[]declaration) {
	t.Helper()
	var visit func(e *cordis.Entry)
	visit = func(e *cordis.Entry) {
		for name, label := range e.Options().Isolate {
			*out = append(*out, declaration{entry: e.ID(), service: name, label: label})
		}
		sub := e.Subgroup()
		if sub == nil {
			return
		}
		for _, child := range sub.Children() {
			visit(child)
		}
	}
	for _, child := range root.Children() {
		visit(child)
	}
}

// tenantDeclarationFixture 开通两个租户、各起一个会话，然后返回全部声明
// 以及租户根的入口 ID。
//
// 会话是必要的：租户根与租户根的静态子入口在 provision 时就建好了，
// 而**会话入口是懒建的**，并且它恰好是第二处会写 Isolate 的地方
// （realm.Session()）。不建会话就扫不到那一类。
func tenantDeclarationFixture(t *testing.T) (tree *cordis.EntryTree, decls []declaration, rootIDs []string) {
	t.Helper()
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"), h.spec("tenant-b"))

	for _, id := range []ident.Tenant{"tenant-a", "tenant-b"} {
		if _, err := h.man.Session(id, "default"); err != nil {
			t.Fatalf("session for %s: %v", id, err)
		}
		entryID, ok := h.man.ResolveEntry(id)
		if !ok {
			t.Fatalf("no entry id for %s", id)
		}
		rootIDs = append(rootIDs, entryID)
	}

	collectDeclarations(t, h.tree().Root(), &decls)
	return h.tree(), decls, rootIDs
}

// TestProvisionedSubtreeDeclaresOnlyPrivateRealms 是 R3 的正向守门测试。
func TestProvisionedSubtreeDeclaresOnlyPrivateRealms(t *testing.T) {
	tree, decls, rootIDs := tenantDeclarationFixture(t)

	// 必须先证明"扫到了东西"，否则一个遍历写坏、一条都没读到的实现
	// 会让这条测试永远绿——而它的全部价值就是可信地说出"干净"。
	if len(decls) == 0 {
		t.Fatal("扫到 0 条 Isolate 声明：遍历没进循环，这条断言什么也没验证")
	}

	for _, d := range decls {
		if d.label != true {
			t.Errorf("entry %s declares service %q as %#v; only true (a private realm) is allowed (SAFETY R3)",
				d.entry, d.service, d.label)
		}
	}

	// 租户根必须逐字挂上 realm.Tenant()：漏掉任何一个服务名都会让
	// 那个服务落回默认域，而默认域是"只要有一个租户注册了，其他租户
	// 就能解析到它"——静默共享，不报错。
	for _, id := range rootIDs {
		entry, err := tree.Resolve(id)
		if err != nil {
			t.Fatalf("resolve %s: %v", id, err)
		}
		got := entry.Options().Isolate
		want := realm.Tenant()
		if len(got) != len(want) {
			t.Fatalf("tenant root %s declares %d services, want %d", id, len(got), len(want))
		}
		for name, wv := range want {
			gv, ok := got[name]
			if !ok {
				t.Errorf("tenant root %s is missing service %q", id, name)
				continue
			}
			if gv != wv {
				t.Errorf("tenant root %s declares %q as %#v, want %#v", id, name, gv, wv)
			}
		}
	}
}

// TestOnlyTenantRootsAndSessionsDeclareIsolate 钉住「Isolate 只出现在两处」。
//
// tenant.go 的包注释把这句话写成了设计前提：「平台里没有任何第二处写
// Isolate 字面量的地方」。语句本身没法断言，但它的**可观测后果**可以：
//
//   - 租户根声明的必须是租户级服务（realm.Services()）；
//   - 其他任何入口声明的只能是会话级服务（realm.SessionServices()）。
//
// 一个新入口如果偷偷声明了别的服务名，无论值是 true 还是字符串，
// 这里都会失败——这正是"新服务必须先进 realm.Services()"这条纪律的
// 机器可检查形式。
func TestOnlyTenantRootsAndSessionsDeclareIsolate(t *testing.T) {
	_, decls, rootIDs := tenantDeclarationFixture(t)

	isRoot := make(map[string]bool, len(rootIDs))
	for _, id := range rootIDs {
		isRoot[id] = true
	}
	tenantLevel := make(map[string]bool)
	for _, name := range realm.Services() {
		tenantLevel[name] = true
	}
	sessionLevel := make(map[string]bool)
	for _, name := range realm.SessionServices() {
		sessionLevel[name] = true
	}

	sawSessionDecl := false
	for _, d := range decls {
		switch {
		case isRoot[d.entry]:
			if !tenantLevel[d.service] {
				t.Errorf("tenant root %s declares %q, which is not in realm.Services()",
					d.entry, d.service)
			}
		default:
			if !sessionLevel[d.service] {
				t.Errorf("entry %s declares %q, but only realm.SessionServices() may be declared below the tenant root",
					d.entry, d.service)
			}
			sawSessionDecl = true
		}
	}

	if !sawSessionDecl {
		t.Error("没有任何子入口声明 Isolate：会话的收窄声明没被扫到，这条断言只覆盖了一半")
	}
}

// TestSessionsGroupCarriesNoIsolate 钉住那条刻意的"不声明"。
//
// 会话分组不带 Isolate 是必要条件，不是疏忽：cordis 的域键取的是
// **声明 Isolate 的那个入口**自己的短 ID，若声明在分组上，全部会话会
// 共享同一个 "#<分组ID>" 域。session_test.go 里有一条反向测试证明
// 「声明在分组上」会让第二个会话串到第一个（TestSessionIsolateOnParentGroup…）。
//
// 那条测试证明的是"这样写会坏"；这条证明的是"我们没这样写"。
func TestSessionsGroupCarriesNoIsolate(t *testing.T) {
	tree, _, rootIDs := tenantDeclarationFixture(t)

	for _, id := range rootIDs {
		// 必须用**全路径**：会话分组在租户根之下，而 Resolve 认的是
		// ":" 分隔的路径，短 ID 会被当成"根组下的同名入口"去解析并报
		// cannot resolve（见 realm.Path 的注释）。这个坑踩过一次。
		sessionsPath := realm.SessionsGroupPath(id)
		entry, err := tree.Resolve(sessionsPath)
		if err != nil {
			t.Fatalf("resolve sessions group %s: %v", sessionsPath, err)
		}
		if n := len(entry.Options().Isolate); n != 0 {
			t.Fatalf("sessions group %s declares %d isolate entries, want 0 "+
				"(a declaration here would collapse every session into one realm)",
				sessionsPath, n)
		}
	}
}

// TestManagerCheckIsolateDeclarationsIsWired 守被接进自检的那条通路。
//
// 上一条测试走的是**测试自己写的**遍历；这一条走的是**平台自己会跑的**
// 那一个（tenant.Manager → realm.CheckIsolateDeclarations → loader 树）。
// 两者都要有：前者说明树的形状对，后者说明自检真的会看它。
func TestManagerCheckIsolateDeclarationsIsWired(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"), h.spec("tenant-b"))

	total := 0
	for _, id := range h.man.List() {
		n, err := h.man.CheckIsolateDeclarations(id)
		if err != nil {
			t.Fatalf("CheckIsolateDeclarations(%s): %v", id, err)
		}
		if want := len(realm.Services()); n < want {
			t.Errorf("%s: scanned %d declarations, want at least %d", id, n, want)
		}
		total += n
	}
	if total == 0 {
		t.Fatal("两个租户一共扫到 0 条声明")
	}

	// 未开通的租户必须报错，而不是"空扫一遍、干净通过"。
	if _, err := h.man.CheckIsolateDeclarations("nobody"); !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("err = %v, want ErrNoTenant", err)
	}
}

// TestCheckIsolateDeclarationsReportsTheOffendingEntry 用一条真实入口上的
// 坏声明，验证自检会**指名**到入口与服务。
//
// 这是正面控制：前几条只能证明"干净时通过"，而一个恒返回 (n, nil) 的
// 实现同样能通过它们。这条必须变红，自检才算真的在看。
func TestCheckIsolateDeclarationsReportsTheOffendingEntry(t *testing.T) {
	h := newHarness(t)
	h.mustProvision(h.spec("tenant-a"))
	entryID, _ := h.man.ResolveEntry("tenant-a")

	// 在租户根之下手工加一个入口——「有人手工造了入口」正是自检要抓的
	// 那类构造破坏（它绕过了 realm.Tenant() 这个唯一来源）。
	// 租户根处在根组下，因此它的短 ID 与全路径同形，可以直接当 parent 用。
	dirty := realm.Child(entryID, "dirty")
	if _, err := h.loader.Create(cordis.EntryOptions{
		ID:      dirty,
		Name:    realm.PluginSessions,
		Isolate: map[string]any{realm.ServiceHistory: "@shared"},
	}, entryID, 0); err != nil {
		t.Fatalf("create dirty entry: %v", err)
	}

	_, err := h.man.CheckIsolateDeclarations("tenant-a")
	if err == nil {
		t.Fatal("a shared realm label on a real entry was not reported")
	}
	if !errors.Is(err, realm.ErrSharedRealm) {
		t.Fatalf("err = %v, want ErrSharedRealm", err)
	}
	if !strings.Contains(err.Error(), dirty) {
		t.Errorf("error %q must name the offending entry %q", err, dirty)
	}
	if !strings.Contains(err.Error(), realm.ServiceHistory) {
		t.Errorf("error %q must name the offending service %q", err, realm.ServiceHistory)
	}
}
