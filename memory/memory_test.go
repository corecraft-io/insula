package memory_test

import (
	"errors"
	"testing"

	"github.com/corecraft-io/insula/ident"
	"github.com/corecraft-io/insula/memory"
)

const (
	tenantA = ident.Tenant("tenant-a")
	tenantB = ident.Tenant("tenant-b")
)

func newStore(t *testing.T) *memory.MemStore {
	t.Helper()
	return memory.NewMemStore(memory.Config{MaxTurnsPerSession: 8, MaxDocsPerTenant: 16})
}

// TestIdenticalSessionIDDoesNotCollideAcrossTenants 是这一层最重要的一条。
//
// 两个租户使用**完全相同**的会话 ID 字符串——这在真实平台上很常见
// （会话 ID 往往由客户端生成，如 "default"、"s-1"）。若存储把会话 ID
// 当成全局键，两个租户的历史就会读写到同一份数据。
func TestIdenticalSessionIDDoesNotCollideAcrossTenants(t *testing.T) {
	m := newStore(t)
	const shared = ident.Session("default")

	if _, err := m.AppendTurn(tenantA, shared, memory.Turn{Role: memory.RoleUser, Content: "A 的秘密"}); err != nil {
		t.Fatalf("append for A: %v", err)
	}
	if _, err := m.AppendTurn(tenantB, shared, memory.Turn{Role: memory.RoleUser, Content: "B 的秘密"}); err != nil {
		t.Fatalf("append for B: %v", err)
	}

	a, err := m.Turns(tenantA, shared)
	if err != nil {
		t.Fatalf("turns for A: %v", err)
	}
	b, err := m.Turns(tenantB, shared)
	if err != nil {
		t.Fatalf("turns for B: %v", err)
	}

	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("turn counts = A:%d B:%d, want 1 and 1", len(a), len(b))
	}
	if a[0].Content != "A 的秘密" {
		t.Fatalf("tenant A read %q", a[0].Content)
	}
	if b[0].Content != "B 的秘密" {
		t.Fatalf("tenant B read %q", b[0].Content)
	}
}

func TestSessionsAreScopedWithinTenant(t *testing.T) {
	m := newStore(t)

	if _, err := m.AppendTurn(tenantA, "s1", memory.Turn{Role: memory.RoleUser, Content: "one"}); err != nil {
		t.Fatal(err)
	}
	got, err := m.Turns(tenantA, "s2")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("session s2 returned %d turns, want 0", len(got))
	}

	sessions, err := m.Sessions(tenantA)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0] != "s1" {
		t.Fatalf("sessions = %v, want [s1]", sessions)
	}
}

func TestIdentityIsRequiredFailClosed(t *testing.T) {
	m := newStore(t)

	if _, err := m.AppendTurn("", "s1", memory.Turn{}); !errors.Is(err, memory.ErrNoTenant) {
		t.Fatalf("append with empty tenant err = %v, want ErrNoTenant", err)
	}
	if _, err := m.Turns("", "s1"); !errors.Is(err, memory.ErrNoTenant) {
		t.Fatalf("turns with empty tenant err = %v, want ErrNoTenant", err)
	}
	if err := m.PutDoc("", memory.Doc{ID: "d1"}); !errors.Is(err, memory.ErrNoTenant) {
		t.Fatalf("put doc with empty tenant err = %v, want ErrNoTenant", err)
	}
	if _, err := m.Search("", memory.Query{Vector: []float32{1, 0}}); !errors.Is(err, memory.ErrNoTenant) {
		t.Fatalf("search with empty tenant err = %v, want ErrNoTenant", err)
	}
	if err := m.DropTenant(""); !errors.Is(err, memory.ErrNoTenant) {
		t.Fatalf("drop tenant with empty tenant err = %v, want ErrNoTenant", err)
	}
	// 会话标识同样必传：缺失会话的写入会落进一个无名桶。
	if _, err := m.AppendTurn(tenantA, "", memory.Turn{}); err == nil {
		t.Fatal("append with empty session succeeded")
	}
}

// TestVectorSearchStaysInsideTenantNamespace 验证向量检索的结构性隔离。
//
// 租户 A 与 B 各存一条**向量完全相同**的文档。若实现是「先全库检索
// 再按租户过滤」，A 的 Top-1 会被 B 的同分文档挤占，且过滤逻辑一旦
// 漏写就直接泄漏。本实现只遍历 A 的桶，所以两者都命中自己那条。
func TestVectorSearchStaysInsideTenantNamespace(t *testing.T) {
	m := newStore(t)
	vec := []float32{1, 0, 0}

	if err := m.PutDoc(tenantA, memory.Doc{ID: "a1", Text: "A 的文档", Vector: vec}); err != nil {
		t.Fatal(err)
	}
	if err := m.PutDoc(tenantB, memory.Doc{ID: "b1", Text: "B 的文档", Vector: vec}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		tenant ident.Tenant
		want   string
	}{
		{tenantA, "a1"},
		{tenantB, "b1"},
	} {
		hits, err := m.Search(tc.tenant, memory.Query{Vector: vec, TopK: 10})
		if err != nil {
			t.Fatalf("search for %s: %v", tc.tenant, err)
		}
		if len(hits) != 1 {
			t.Fatalf("search for %s returned %d hits, want 1 (leaked across namespaces?)",
				tc.tenant, len(hits))
		}
		if hits[0].Doc.ID != tc.want {
			t.Fatalf("search for %s returned %q, want %q", tc.tenant, hits[0].Doc.ID, tc.want)
		}
		if hits[0].Doc.Text == "" {
			t.Fatalf("hit for %s has empty text", tc.tenant)
		}
	}
}

func TestVectorSearchRanksAndTruncates(t *testing.T) {
	m := newStore(t)

	docs := []memory.Doc{
		{ID: "exact", Text: "x", Vector: []float32{1, 0, 0}},
		{ID: "near", Text: "y", Vector: []float32{0.9, 0.1, 0}},
		{ID: "orthogonal", Text: "z", Vector: []float32{0, 1, 0}},
	}
	for _, d := range docs {
		if err := m.PutDoc(tenantA, d); err != nil {
			t.Fatal(err)
		}
	}

	hits, err := m.Search(tenantA, memory.Query{Vector: []float32{1, 0, 0}, TopK: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Fatalf("got %d hits, want 2 (TopK not applied)", len(hits))
	}
	if hits[0].Doc.ID != "exact" || hits[1].Doc.ID != "near" {
		t.Fatalf("ranking = [%s %s], want [exact near]", hits[0].Doc.ID, hits[1].Doc.ID)
	}

	// MinScore 过滤掉正交项。
	hits, err = m.Search(tenantA, memory.Query{Vector: []float32{1, 0, 0}, TopK: 10, MinScore: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Fatalf("min-score filter kept %d hits, want 2", len(hits))
	}
}

func TestVectorDimensionMismatchIsRejected(t *testing.T) {
	m := newStore(t)
	if err := m.PutDoc(tenantA, memory.Doc{ID: "d", Vector: []float32{1, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Search(tenantA, memory.Query{Vector: []float32{1, 0}}); !errors.Is(err, memory.ErrDimension) {
		t.Fatalf("err = %v, want ErrDimension", err)
	}
}

func TestTurnCapEvictsOldestAndReportsCount(t *testing.T) {
	m := memory.NewMemStore(memory.Config{MaxTurnsPerSession: 3})

	evictedTotal := 0
	for i := 0; i < 5; i++ {
		n, err := m.AppendTurn(tenantA, "s1", memory.Turn{
			Role:    memory.RoleUser,
			Content: string(rune('a' + i)),
		})
		if err != nil {
			t.Fatal(err)
		}
		evictedTotal += n
	}

	if evictedTotal != 2 {
		t.Fatalf("evicted = %d, want 2", evictedTotal)
	}
	turns, err := m.Turns(tenantA, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 3 {
		t.Fatalf("kept %d turns, want 3", len(turns))
	}
	if turns[0].Content != "c" || turns[2].Content != "e" {
		t.Fatalf("kept window = %q..%q, want c..e", turns[0].Content, turns[2].Content)
	}
}

func TestDocCapIsPerTenantNotGlobal(t *testing.T) {
	m := memory.NewMemStore(memory.Config{MaxDocsPerTenant: 2})

	for i := 0; i < 2; i++ {
		if err := m.PutDoc(tenantA, memory.Doc{ID: "a", Vector: []float32{1, 0}}); err != nil {
			t.Fatal(err)
		}
		if err := m.PutDoc(tenantB, memory.Doc{ID: "b", Vector: []float32{1, 0}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.PutDoc(tenantA, memory.Doc{ID: "a2", Vector: []float32{1, 0}}); err != nil {
		t.Fatal(err)
	}

	_, _, docsA := m.Count(tenantA)
	_, _, docsB := m.Count(tenantB)
	if docsA != 2 || docsB != 1 {
		t.Fatalf("docs = A:%d B:%d, want 2 and 1 (caps must be per-tenant)", docsA, docsB)
	}
}

func TestDropTenantLeavesOtherTenantsIntact(t *testing.T) {
	m := newStore(t)

	for _, tt := range []ident.Tenant{tenantA, tenantB} {
		if _, err := m.AppendTurn(tt, "s1", memory.Turn{Role: memory.RoleUser, Content: "x"}); err != nil {
			t.Fatal(err)
		}
		if err := m.PutDoc(tt, memory.Doc{ID: "d", Vector: []float32{1}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := m.DropTenant(tenantA); err != nil {
		t.Fatal(err)
	}

	sA, tA, dA := m.Count(tenantA)
	if sA != 0 || tA != 0 || dA != 0 {
		t.Fatalf("tenant A still has s=%d t=%d d=%d after drop", sA, tA, dA)
	}
	sB, tB, dB := m.Count(tenantB)
	if sB != 1 || tB != 1 || dB != 1 {
		t.Fatalf("tenant B was damaged: s=%d t=%d d=%d, want 1/1/1", sB, tB, dB)
	}
}

func TestDropSessionIsScoped(t *testing.T) {
	m := newStore(t)
	for _, s := range []ident.Session{"s1", "s2"} {
		if _, err := m.AppendTurn(tenantA, s, memory.Turn{Role: memory.RoleUser}); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.DropSession(tenantA, "s1"); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := m.Count(tenantA); s != 1 {
		t.Fatalf("sessions after drop = %d, want 1", s)
	}
	// 删除另一个租户的同名会话不应影响本租户。
	if err := m.DropSession(tenantB, "s2"); err != nil {
		t.Fatal(err)
	}
	// 注意：这里的局部变量不能叫 t，否则会遮蔽 *testing.T。
	// 这个遮蔽此前确实把编译打挂过一次，留作教训。
	if _, turns, _ := m.Count(tenantA); turns != 1 {
		t.Fatalf("tenant A turns after dropping tenant B session = %d, want 1", turns)
	}
}

func TestReplaceTurnsOverwritesAndClears(t *testing.T) {
	m := newStore(t)
	if _, err := m.AppendTurn(tenantA, "s1", memory.Turn{Role: memory.RoleUser, Content: "old"}); err != nil {
		t.Fatal(err)
	}
	if err := m.ReplaceTurns(tenantA, "s1", []memory.Turn{
		{Role: memory.RoleSummary, Content: "摘要"},
	}); err != nil {
		t.Fatal(err)
	}
	turns, err := m.Turns(tenantA, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 1 || turns[0].Role != memory.RoleSummary {
		t.Fatalf("turns = %+v, want single summary turn", turns)
	}

	if err := m.ReplaceTurns(tenantA, "s1", nil); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := m.Count(tenantA); s != 0 {
		t.Fatalf("sessions after clearing with empty slice = %d, want 0", s)
	}
}

func TestTotalsCountEverything(t *testing.T) {
	m := newStore(t)
	for _, tt := range []ident.Tenant{tenantA, tenantB} {
		if _, err := m.AppendTurn(tt, "s1", memory.Turn{Role: memory.RoleUser}); err != nil {
			t.Fatal(err)
		}
		if err := m.PutDoc(tt, memory.Doc{ID: "d"}); err != nil {
			t.Fatal(err)
		}
	}
	s, turns, docs := m.Totals()
	if s != 2 || turns != 2 || docs != 2 {
		t.Fatalf("totals = %d/%d/%d, want 2/2/2", s, turns, docs)
	}
}
