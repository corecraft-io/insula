package memory_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
	"github.com/corecraft-io/insula/realm"
)

// 本文件守的是 memory.Handle —— 租户隔离域里那个**瘦句柄**。
//
// 为什么它需要单独的回归测试：MemStore 已经在结构上按租户分桶，所以
// 「存储本身不串租」由 memory_test.go 守住了。但句柄是另一层：它是唯一
// 会把「调用方携带的租户标识」与「句柄绑定的租户」做比对的地方，而它
// 的 8 个公开方法里曾有 7 个从未被任何测试执行过（check 的
// ErrForeignTenant 分支同样没有）。
//
// 这条路径在运行时是可达的：caps.Memory 接口要求 PutDoc / Search /
// ReplaceTurns，agent 循环就是经这个接口拿到的句柄。一旦有人把
// 「参数写错」这类最弱一环漏过去，它就变成一次**静默的跨租户读写**，
// 而不是一次报错。

// foreignCall 是一次「用别人的租户标识调本租户句柄」的尝试。
type foreignCall struct {
	name string
	call func(h *memory.Handle, other ident.Tenant) error
}

// foreignCalls 覆盖除 Count 之外的全部 Store 方法。
//
// Count 的签名不返回 error（返回计数三元组），因此无法塞进这张表，
// 由 TestHandleCountForForeignTenantReportsZero 单独守。
func foreignCalls() []foreignCall {
	return []foreignCall{
		{"AppendTurn", func(h *memory.Handle, o ident.Tenant) error {
			_, err := h.AppendTurn(o, "s1", memory.Turn{Role: memory.RoleUser, Content: "越界写入"})
			return err
		}},
		{"Turns", func(h *memory.Handle, o ident.Tenant) error {
			_, err := h.Turns(o, "s1")
			return err
		}},
		{"ReplaceTurns", func(h *memory.Handle, o ident.Tenant) error {
			return h.ReplaceTurns(o, "s1", []memory.Turn{{Role: memory.RoleSummary, Content: "越界替换"}})
		}},
		{"Sessions", func(h *memory.Handle, o ident.Tenant) error {
			_, err := h.Sessions(o)
			return err
		}},
		{"DropSession", func(h *memory.Handle, o ident.Tenant) error {
			return h.DropSession(o, "s1")
		}},
		{"PutDoc", func(h *memory.Handle, o ident.Tenant) error {
			return h.PutDoc(o, memory.Doc{ID: "越界", Vector: []float32{1, 0}})
		}},
		{"Search", func(h *memory.Handle, o ident.Tenant) error {
			_, err := h.Search(o, memory.Query{Vector: []float32{1, 0}, TopK: 5})
			return err
		}},
		{"DropTenant", func(h *memory.Handle, o ident.Tenant) error {
			return h.DropTenant(o)
		}},
	}
}

// TestHandleRejectsForeignTenant 是这一层最重要的一条。
//
// 句柄绑定 A，调用却携带 B 的租户标识：每一个方法都必须报
// ErrForeignTenant，一个都不能漏。漏掉任意一个，那个方法就成了
// 「拿别人的身份读写别人的记忆」的入口。
func TestHandleRejectsForeignTenant(t *testing.T) {
	m := newStore(t)
	h := memory.NewHandle(m, tenantA)

	for _, fc := range foreignCalls() {
		t.Run(fc.name, func(t *testing.T) {
			err := fc.call(h, tenantB)
			if err == nil {
				t.Fatalf("Handle.%s with a foreign tenant succeeded, want ErrForeignTenant", fc.name)
			}
			if !errors.Is(err, memory.ErrForeignTenant) {
				t.Fatalf("Handle.%s err = %v, want ErrForeignTenant", fc.name, err)
			}
			// 错误消息必须同时指出句柄绑定的租户与实际传入的租户，
			// 否则线上拿到这条日志的人无法判断是哪一侧写错了。
			if msg := err.Error(); !strings.Contains(msg, string(tenantA)) || !strings.Contains(msg, string(tenantB)) {
				t.Fatalf("Handle.%s error %q must name both the bound and the passed tenant", fc.name, msg)
			}
		})
	}
}

// TestForeignTenantCallHasNoSideEffect 断言拒绝是**真正的拒绝**，
// 而不是"先写后报错"。
//
// 只断言返回了 ErrForeignTenant 是不够的：一个先转发给底层存储、
// 再检查租户的实现同样能通过上一个测试，而它已经把数据写进去了。
func TestForeignTenantCallHasNoSideEffect(t *testing.T) {
	m := newStore(t)
	h := memory.NewHandle(m, tenantA)

	// 两个租户各准备一份可被观测的数据，任何越界改动都会让下面的比对失败。
	if _, err := m.AppendTurn(tenantA, "s1", memory.Turn{Role: memory.RoleUser, Content: "A"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AppendTurn(tenantB, "s1", memory.Turn{Role: memory.RoleUser, Content: "B"}); err != nil {
		t.Fatal(err)
	}
	if err := m.PutDoc(tenantA, memory.Doc{ID: "a-doc", Vector: []float32{1, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := m.PutDoc(tenantB, memory.Doc{ID: "b-doc", Vector: []float32{1, 0}}); err != nil {
		t.Fatal(err)
	}

	beforeA := snapshotOf(t, m, tenantA)
	beforeB := snapshotOf(t, m, tenantB)

	for _, fc := range foreignCalls() {
		_ = fc.call(h, tenantB) // 结果已由上一个测试断言，这里只关心副作用
	}

	if after := snapshotOf(t, m, tenantA); after != beforeA {
		t.Errorf("bound tenant changed by a rejected call: %s -> %s", beforeA, after)
	}
	if after := snapshotOf(t, m, tenantB); after != beforeB {
		t.Errorf("foreign tenant changed by a rejected call: %s -> %s", beforeB, after)
	}

	// 反向也要守：越界调用不该把**句柄自己的**租户写脏。
	// 上面对 A 的比对已经覆盖了这一点，这里再确认 A 仍在原样。
	if _, turns, _ := m.Count(tenantA); turns != 1 {
		t.Errorf("bound tenant now has %d turns, want 1", turns)
	}
}

// TestHandleCountForForeignTenantReportsZero 单独守 Count。
//
// 它不返回 error，只能靠返回值判断。这里的关键是**不能退化成
// 底层存储的计数**——那会把「B 有多少数据」这个侧信道直接交给 A
// 的上下文（Handle.Count 的注释把这一点写成了刻意设计）。
func TestHandleCountForForeignTenantReportsZero(t *testing.T) {
	m := newStore(t)
	h := memory.NewHandle(m, tenantA)

	if _, err := m.AppendTurn(tenantB, "s1", memory.Turn{Role: memory.RoleUser}); err != nil {
		t.Fatal(err)
	}
	if err := m.PutDoc(tenantB, memory.Doc{ID: "b-doc", Vector: []float32{1}}); err != nil {
		t.Fatal(err)
	}

	s, turns, docs := h.Count(tenantB)
	if s != 0 || turns != 0 || docs != 0 {
		t.Fatalf("Count(foreign) = %d/%d/%d, want 0/0/0 (a non-zero count leaks another tenant's size)",
			s, turns, docs)
	}
}

// TestHandleDropTenantCannotDropAnotherTenant 是最有杀伤力的一条。
//
// DropTenant 是破坏性操作。若句柄不校验租户，A 的隔离域里只要有人
// 写错一个标识，就能把 B 的全部记忆删掉——而且是静默删掉。
func TestHandleDropTenantCannotDropAnotherTenant(t *testing.T) {
	m := newStore(t)
	h := memory.NewHandle(m, tenantA)

	if _, err := m.AppendTurn(tenantB, "s1", memory.Turn{Role: memory.RoleUser, Content: "B 的数据"}); err != nil {
		t.Fatal(err)
	}

	if err := h.DropTenant(tenantB); !errors.Is(err, memory.ErrForeignTenant) {
		t.Fatalf("DropTenant(foreign) err = %v, want ErrForeignTenant", err)
	}

	// 拒绝之后 B 必须**完好**。
	s, turns, _ := m.Count(tenantB)
	if s != 1 || turns != 1 {
		t.Fatalf("tenant B was damaged by a rejected DropTenant: s=%d turns=%d, want 1/1", s, turns)
	}

	// 自己的可以删自己的。
	if err := h.DropTenant(tenantA); err != nil {
		t.Fatalf("DropTenant(own) = %v, want nil", err)
	}
}

// TestHandleRejectsEmptyTenantFirst 钉住两条检查的顺序。
//
// 空租户标识要先报 ErrNoTenant：它是编程错误（参数漏传），
// 而 ErrForeignTenant 描述的是另一类问题（参数被替换）。
// 顺序反了的话，漏传参数会被报成"跨租户调用"，把排查方向带偏。
func TestHandleRejectsEmptyTenantFirst(t *testing.T) {
	m := newStore(t)
	h := memory.NewHandle(m, tenantA)

	if _, err := h.AppendTurn("", "s1", memory.Turn{}); !errors.Is(err, memory.ErrNoTenant) {
		t.Fatalf("err = %v, want ErrNoTenant", err)
	}
	if _, err := h.Turns("", "s1"); !errors.Is(err, memory.ErrNoTenant) {
		t.Fatalf("err = %v, want ErrNoTenant", err)
	}
	if err := h.DropTenant(""); !errors.Is(err, memory.ErrNoTenant) {
		t.Fatalf("err = %v, want ErrNoTenant", err)
	}
}

// TestHandleDelegatesForItsOwnTenant 覆盖正常路径。
//
// 只守"越界被拒"是不够的：一个把每个方法都写成 return ErrForeignTenant
// 的实现能满足上面全部断言。这些方法必须在**自己的租户**上原样工作，
// 且改动必须真的落到共享存储上（否则句柄退化成只读的假接口）。
func TestHandleDelegatesForItsOwnTenant(t *testing.T) {
	m := newStore(t)
	h := memory.NewHandle(m, tenantA)

	// AppendTurn / Turns
	if _, err := h.AppendTurn(tenantA, "s1", memory.Turn{Role: memory.RoleUser, Content: "一"}); err != nil {
		t.Fatalf("AppendTurn: %v", err)
	}
	turns, err := h.Turns(tenantA, "s1")
	if err != nil {
		t.Fatalf("Turns: %v", err)
	}
	if len(turns) != 1 || turns[0].Content != "一" {
		t.Fatalf("Turns = %+v, want the appended turn", turns)
	}
	// 写入必须对底层存储可见（证明是转发，不是本地缓存）。
	if viaStore, err := m.Turns(tenantA, "s1"); err != nil || len(viaStore) != 1 {
		t.Fatalf("shared store sees %d turns (err %v), want 1", len(viaStore), err)
	}

	// ReplaceTurns
	if err := h.ReplaceTurns(tenantA, "s1", []memory.Turn{{Role: memory.RoleSummary, Content: "摘要"}}); err != nil {
		t.Fatalf("ReplaceTurns: %v", err)
	}
	if turns, err = h.Turns(tenantA, "s1"); err != nil || len(turns) != 1 || turns[0].Role != memory.RoleSummary {
		t.Fatalf("after ReplaceTurns: %+v (err %v)", turns, err)
	}

	// Sessions
	if sessions, err := h.Sessions(tenantA); err != nil {
		t.Fatalf("Sessions: %v", err)
	} else if len(sessions) != 1 || sessions[0] != "s1" {
		t.Fatalf("Sessions = %v, want [s1]", sessions)
	}

	// PutDoc / Search
	if err := h.PutDoc(tenantA, memory.Doc{ID: "d1", Text: "记忆", Vector: []float32{1, 0}}); err != nil {
		t.Fatalf("PutDoc: %v", err)
	}
	hits, err := h.Search(tenantA, memory.Query{Vector: []float32{1, 0}, TopK: 5})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].Doc.ID != "d1" {
		t.Fatalf("Search = %+v, want the stored doc", hits)
	}

	// Count
	if s, turns, docs := h.Count(tenantA); s != 1 || turns != 1 || docs != 1 {
		t.Fatalf("Count = %d/%d/%d, want 1/1/1", s, turns, docs)
	}

	// DropSession
	if err := h.DropSession(tenantA, "s1"); err != nil {
		t.Fatalf("DropSession: %v", err)
	}
	if s, _, _ := h.Count(tenantA); s != 0 {
		t.Fatalf("sessions after DropSession = %d, want 0", s)
	}

	// DropTenant
	if err := h.DropTenant(tenantA); err != nil {
		t.Fatalf("DropTenant: %v", err)
	}
	if s, turns, docs := h.Count(tenantA); s != 0 || turns != 0 || docs != 0 {
		t.Fatalf("after DropTenant = %d/%d/%d, want 0/0/0", s, turns, docs)
	}
}

// TestHandleDoesNotWeakenStoreChecks 断言句柄是**加一层**校验，不是取代它。
//
// 句柄只比对租户；会话标识必传、空租户 fail-closed 这些仍然由
// 底层存储负责。若句柄在转发时把参数"补全"（比如给空会话塞个默认值），
// 就把存储层的 fail-closed 悄悄绕过去了。
func TestHandleDoesNotWeakenStoreChecks(t *testing.T) {
	m := newStore(t)
	h := memory.NewHandle(m, tenantA)

	if _, err := h.AppendTurn(tenantA, "", memory.Turn{}); err == nil {
		t.Fatal("handle accepted an empty session id")
	}
	if _, err := m.AppendTurn(tenantA, "", memory.Turn{}); err == nil {
		t.Fatal("store accepted an empty session id")
	}
}

// TestHandlesShareOneStoreButStayDistinct 同时钉住 ADR 硬规则 4 的两半。
//
//   - **共享**：两个租户的句柄背后是同一个 MemStore 单例（隔离是存储
//     的结构性质，不是"N 份实例"）；
//   - **可分辨**：两个句柄本身仍是不同实例，因此 realm.AssertIsolated
//     这类黑盒断言依然能把它们区分开——否则隔离断言会因为"大家拿到
//     同一个东西"而反过来变成噪声。
func TestHandlesShareOneStoreButStayDistinct(t *testing.T) {
	m := newStore(t)
	ha := memory.NewHandle(m, tenantA)
	hb := memory.NewHandle(m, tenantB)

	if ha.Tenant() != tenantA || hb.Tenant() != tenantB {
		t.Fatalf("bindings = %q/%q, want %q/%q", ha.Tenant(), hb.Tenant(), tenantA, tenantB)
	}
	if ha == hb {
		t.Fatal("two tenants received the same handle instance")
	}

	// Store() 是管理路径的出口（注销、容量看板），两个句柄必须指向同一份。
	sa, sb := ha.Store(), hb.Store()
	if sa == nil || sb == nil {
		t.Fatal("Store() returned nil")
	}
	if !realm.SameInstance(sa, sb) {
		t.Fatal("handles over one MemStore reported different stores")
	}

	// 经 A 的句柄写入，必须能被 B 的句柄所在的那份存储看到——
	// 证明"共享单例"不是两个各自独立的副本。
	if _, err := ha.AppendTurn(tenantA, "s1", memory.Turn{Role: memory.RoleUser, Content: "A"}); err != nil {
		t.Fatal(err)
	}
	all, ok := sa.(*memory.MemStore)
	if !ok {
		t.Fatalf("Store() = %T, want *memory.MemStore", sa)
	}
	if s, _, _ := all.Totals(); s != 1 {
		t.Fatalf("shared store totals has %d sessions, want 1", s)
	}
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

// snapshotOf 把某租户的全部可观测状态压成一个字符串，供副作用比对。
func snapshotOf(t *testing.T, m *memory.MemStore, tt ident.Tenant) string {
	t.Helper()
	sessions, err := m.Sessions(tt)
	if err != nil {
		t.Fatalf("sessions of %s: %v", tt, err)
	}
	out := fmt.Sprintf("count=%v", mustCount(m, tt))
	for _, s := range sessions {
		turns, err := m.Turns(tt, s)
		if err != nil {
			t.Fatalf("turns of %s/%s: %v", tt, s, err)
		}
		for _, turn := range turns {
			out += fmt.Sprintf("|%s:%s=%s", s, turn.Role, turn.Content)
		}
	}
	hits, err := m.Search(tt, memory.Query{Vector: []float32{1, 0}, TopK: 100})
	if err != nil {
		t.Fatalf("search of %s: %v", tt, err)
	}
	for _, hit := range hits {
		out += fmt.Sprintf("|doc:%s=%s", hit.Doc.ID, hit.Doc.Text)
	}
	return out
}

func mustCount(m *memory.MemStore, t ident.Tenant) [3]int {
	s, turns, docs := m.Count(t)
	return [3]int{s, turns, docs}
}
