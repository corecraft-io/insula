package ident_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/metaRobin/insula/ident"
)

// ---------------------------------------------------------------------------
// NewRun
// ---------------------------------------------------------------------------

// run 标识是审计里唯一能把「哪一次请求」串起来的键。三条性质必须同时
// 成立：非空、全树唯一、不可预测。
//
// 前两条是可用性要求，第三条是**安全**要求：可预测的 run 标识让
// 「伪造一条属于别人的审计记录」变成可能，而审计一旦被污染就再也
// 分辨不出哪条是真的。所以这里不只断言唯一，还断言随机性。
func TestNewRunIsUniqueShapedAndUnpredictable(t *testing.T) {
	const n = 4096

	seen := make(map[ident.Run]struct{}, n)
	prefixes := make(map[string]struct{}, 256)

	for i := 0; i < n; i++ {
		r := ident.NewRun()
		if !r.Valid() {
			t.Fatalf("第 %d 次 NewRun 返回了空标识", i)
		}
		if _, dup := seen[r]; dup {
			t.Fatalf("第 %d 次 NewRun 撞上了已有标识 %q", i, r)
		}
		seen[r] = struct{}{}

		s := r.String()
		if !strings.HasPrefix(s, "r-") {
			t.Fatalf("标识 %q 缺少 r- 前缀：前缀是把它与租户/会话标识区分开的依据", s)
		}
		const wantLen = 2 + 24 // "r-" + 12 字节 hex
		if len(s) != wantLen {
			t.Fatalf("标识 %q 长度 %d，期望 %d", s, len(s), wantLen)
		}
		for _, c := range s[2:] {
			if !strings.ContainsRune("0123456789abcdef", c) {
				t.Fatalf("标识 %q 含非 hex 字符 %q", s, c)
			}
		}
		prefixes[s[:4]] = struct{}{}
	}

	// 头两个字节有 256 种取值。4096 个随机标识几乎必然铺满；
	// 而计数器 / 时间戳派生的实现只会有个位数的不同前缀。
	// 阈值取 200（期望 ~256，标准差 ~0.1），足以在两种实现之间劈开，
	// 又不会因为随机波动偶发红。
	if len(prefixes) < 200 {
		t.Fatalf("%d 个标识只出现 %d 种前缀，说明标识可预测", n, len(prefixes))
	}
}

// 降级路径（crypto/rand 失败）不是常规路径，但它的产物也必须是
// 合法的运行标识——审计写入方不会为它特判。
func TestRunFromFallbackPathStillValid(t *testing.T) {
	// 无法在测试里强制 crypto/rand 失败，只能约束契约面：
	// 任何 Valid 的 Run 都必须能安全地当字符串用。
	r := ident.NewRun()
	if got := r.String(); got != string(r) {
		t.Fatalf("String() = %q，期望 %q", got, string(r))
	}
	if ident.Run("").Valid() {
		t.Fatal("空 Run 被判为有效：审计里会出现一条无法追溯的记录")
	}
}

// ---------------------------------------------------------------------------
// 三个标识类型
// ---------------------------------------------------------------------------

// Valid 的语义是「非空」，而空值必须被当成**编程错误**而不是
// 「无租户」：多数存储实现表达不了「无租户」，静默接受空值只会
// 把它变成全局命名空间。
func TestValidRejectsEmptyForAllThreeTypes(t *testing.T) {
	cases := []struct {
		name  string
		valid bool
	}{
		{"tenant", ident.Tenant("tenant-a").Valid()},
		{"session", ident.Session("s-1").Valid()},
		{"run", ident.Run("r-abc").Valid()},
		{"empty tenant", ident.Tenant("").Valid()},
		{"empty session", ident.Session("").Valid()},
		{"empty run", ident.Run("").Valid()},
	}
	want := map[string]bool{
		"tenant": true, "session": true, "run": true,
		"empty tenant": false, "empty session": false, "empty run": false,
	}
	for _, c := range cases {
		if c.valid != want[c.name] {
			t.Errorf("%s: Valid() = %v，期望 %v", c.name, c.valid, want[c.name])
		}
	}
}

func TestStringRoundTrips(t *testing.T) {
	if got := ident.Tenant("tenant-a").String(); got != "tenant-a" {
		t.Errorf("Tenant.String() = %q", got)
	}
	if got := ident.Session("s-1").String(); got != "s-1" {
		t.Errorf("Session.String() = %q", got)
	}
	r := ident.NewRun()
	if got := r.String(); got != string(r) {
		t.Errorf("Run.String() = %q，期望 %q", got, string(r))
	}
}

// 身份类型是**强类型**的，这是 ADR-005「租户身份是必传参数」在编译期
// 生效的地方：任何试图省略租户标识的调用都写不出来。
//
// 这条断言盯的是一个具体的退化方式：有人图省事写成
// `type Session = Tenant`（类型别名而不是新类型）。别名会让
// 「把会话标识当租户标识传」重新通过编译，而编译期正是这条 ADR
// 唯一的防线——运行时的 fail-closed 校验只能拦住空值，拦不住张冠李戴。
func TestIdentityTypesAreSeparateNamedTypes(t *testing.T) {
	types := []struct {
		name string
		rt   reflect.Type
	}{
		{"Tenant", reflect.TypeOf(ident.Tenant(""))},
		{"Session", reflect.TypeOf(ident.Session(""))},
		{"Run", reflect.TypeOf(ident.Run(""))},
	}

	seen := make(map[reflect.Type]string, len(types))
	for _, ty := range types {
		if ty.rt.Kind() != reflect.String {
			t.Errorf("%s 的底层类型是 %s，期望 string", ty.name, ty.rt.Kind())
		}
		if ty.rt.Name() == "" {
			t.Errorf("%s 是匿名类型", ty.name)
		}
		if prev, dup := seen[ty.rt]; dup {
			t.Errorf("%s 与 %s 解析到同一个类型 %v —— 类型别名让编译期检查失效了",
				ty.name, prev, ty.rt)
		}
		seen[ty.rt] = ty.name
	}
}
