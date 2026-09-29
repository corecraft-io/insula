package audit_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corecraft-io/insula/audit"
	"github.com/corecraft-io/insula/ident"
)

const (
	tenantA = ident.Tenant("tenant-a")
	tenantB = ident.Tenant("tenant-b")
)

var base = time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

// at 造一条带可识别 subject 的事件，用于断言顺序。
func at(subject string) audit.Event {
	return audit.Event{
		At:      base,
		Tenant:  tenantA,
		Action:  audit.ActionRunStart,
		Subject: subject,
		Outcome: audit.OutcomeOK,
	}
}

func subjects(ev []audit.Event) []string {
	out := make([]string, 0, len(ev))
	for _, e := range ev {
		out = append(out, e.Subject)
	}
	return out
}

func equalStrings(a, b []string) bool {
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

// ---------------------------------------------------------------------------
// 环形缓冲：截断与顺序
// ---------------------------------------------------------------------------

// 环形缓冲的语义只有两条：容量到顶时丢**最旧的**，且读出来必须是
// 时间正序（最旧在前）。第二条靠的是 at() 里那段取模算术，
// 它是本文件里最容易写错、也最容易被后续重构改坏的地方。
func TestRingTruncatesOldestAndKeepsChronologicalOrder(t *testing.T) {
	const cap = 4
	l := audit.New(cap, nil)

	for _, s := range []string{"e1", "e2", "e3", "e4", "e5", "e6"} {
		l.Record(at(s))
	}

	if got := l.Len(); got != cap {
		t.Fatalf("Len() = %d，期望 %d（容量即水位上限）", got, cap)
	}
	got := subjects(l.Recent(0))
	want := []string{"e3", "e4", "e5", "e6"} // 最旧的两条被挤掉
	if !equalStrings(got, want) {
		t.Fatalf("Recent(0) = %v，期望 %v", got, want)
	}

	// 再绕两圈，确认取模回绕之后顺序依然正确。
	for _, s := range []string{"e7", "e8", "e9", "e10", "e11", "e12", "e13"} {
		l.Record(at(s))
	}
	if got, want := subjects(l.Recent(0)), []string{"e10", "e11", "e12", "e13"}; !equalStrings(got, want) {
		t.Fatalf("多圈之后 Recent(0) = %v，期望 %v", got, want)
	}
	if got, want := subjects(l.Recent(2)), []string{"e12", "e13"}; !equalStrings(got, want) {
		t.Fatalf("Recent(2) = %v，期望 %v（取最新的两条）", got, want)
	}
}

func TestRecentWithoutLimitReturnsEverythingRetained(t *testing.T) {
	l := audit.New(8, nil)
	for i := 0; i < 3; i++ {
		l.Record(at(fmt.Sprintf("e%d", i)))
	}
	if got := len(l.Recent(0)); got != 3 {
		t.Fatalf("Recent(0) 返回 %d 条，期望 3", got)
	}
	if got := len(l.Recent(-1)); got != 3 {
		t.Fatalf("Recent(-1) 返回 %d 条，期望 3（负 limit 视为全部）", got)
	}
}

func TestCapacityFallsBackToDefaultWhenNonPositive(t *testing.T) {
	for _, capacity := range []int{0, -1} {
		l := audit.New(capacity, nil)
		for i := 0; i < 50; i++ {
			l.Record(at(fmt.Sprintf("e%d", i)))
		}
		if got := l.Len(); got != 50 {
			t.Fatalf("capacity=%d 时 Len() = %d，期望 50（未截断，说明用的是默认容量）",
				capacity, got)
		}
		if !strings.Contains(l.String(), "8192") {
			t.Fatalf("capacity=%d 的 String() = %q，未体现默认容量", capacity, l.String())
		}
	}
}

// ---------------------------------------------------------------------------
// 按租户过滤：审计视图不跨租户
// ---------------------------------------------------------------------------

// 这是本包第二条硬约束的守卫：For(t) 只返回该租户的事件。
// 它是「审计视图不跨租户」的最后一环——上游按租户分域做得再好，
// 这里过滤错了同样把别人的动作暴露出去。
func TestForReturnsOnlyThatTenantInOrder(t *testing.T) {
	l := audit.New(64, nil)

	l.Record(at("a1"))
	l.Record(audit.Event{Tenant: tenantB, Action: audit.ActionRunStart, Subject: "b1"})
	l.Record(at("a2")) // tenantA
	l.Record(audit.Event{Tenant: tenantB, Action: audit.ActionRunStart, Subject: "b2"})
	l.Record(at("a3"))

	gotA := subjects(l.For(tenantA, 0))
	if want := []string{"a1", "a2", "a3"}; !equalStrings(gotA, want) {
		t.Fatalf("For(tenant-a) = %v，期望 %v", gotA, want)
	}
	gotB := subjects(l.For(tenantB, 0))
	if want := []string{"b1", "b2"}; !equalStrings(gotB, want) {
		t.Fatalf("For(tenant-b) = %v，期望 %v", gotB, want)
	}
	if got := l.For("tenant-c", 0); len(got) != 0 {
		t.Fatalf("未知租户返回了 %d 条事件", len(got))
	}

	if got := l.Count(tenantA); got != 3 {
		t.Errorf("Count(tenant-a) = %d，期望 3", got)
	}
	if got := l.Count(tenantB); got != 2 {
		t.Errorf("Count(tenant-b) = %d，期望 2", got)
	}
	if got := l.Count("tenant-c"); got != 0 {
		t.Errorf("Count(未知租户) = %d，期望 0", got)
	}

	// For 的 limit 取该租户的**最新** N 条，不是全局的最新 N 条。
	if got, want := subjects(l.For(tenantA, 2)), []string{"a2", "a3"}; !equalStrings(got, want) {
		t.Fatalf("For(tenant-a, 2) = %v，期望 %v", got, want)
	}
	// 过滤在截断之前发生：全局最新 2 条里只有一条属于 tenant-b。
	if got, want := subjects(l.For(tenantB, 2)), []string{"b1", "b2"}; !equalStrings(got, want) {
		t.Fatalf("For(tenant-b, 2) = %v，期望 %v（limit 作用于过滤后的集合）", got, want)
	}
}

// 环形缓冲回绕之后，过滤仍然只看到保留窗口内的事件——
// 被挤掉的旧事件不该因为「计数没减」而复活。
func TestForAndCountAfterWrap(t *testing.T) {
	l := audit.New(4, nil)
	// 交错写入 8 条：窗口只留最后 4 条。
	for i := 0; i < 8; i++ {
		tt := tenantA
		if i%2 == 0 {
			tt = tenantB
		}
		l.Record(audit.Event{Tenant: tt, Action: audit.ActionRunStart,
			Subject: fmt.Sprintf("e%d", i)})
	}
	// 保留的是 e4..e7：tenant-b 得 e4,e6；tenant-a 得 e5,e7。
	if got, want := subjects(l.For(tenantB, 0)), []string{"e4", "e6"}; !equalStrings(got, want) {
		t.Fatalf("回绕后 For(tenant-b) = %v，期望 %v", got, want)
	}
	if got, want := subjects(l.For(tenantA, 0)), []string{"e5", "e7"}; !equalStrings(got, want) {
		t.Fatalf("回绕后 For(tenant-a) = %v，期望 %v", got, want)
	}
	if got := l.Count(tenantB); got != 2 {
		t.Fatalf("回绕后 Count(tenant-b) = %d，期望 2", got)
	}
	if got := l.Len(); got != 4 {
		t.Fatalf("Len() = %d，期望 4", got)
	}
}

// ---------------------------------------------------------------------------
// Record 的默认值填空
// ---------------------------------------------------------------------------

func TestRecordFillsDefaults(t *testing.T) {
	clk := func() time.Time { return base.Add(time.Hour) }
	l := audit.New(8, clk)

	// At 零值 → 用时钟；Tenant 空 → 哨兵；Outcome 空 → ok。
	l.Record(audit.Event{Action: audit.ActionTenantProvision})

	got := l.Recent(0)
	if len(got) != 1 {
		t.Fatalf("记录条数 = %d", len(got))
	}
	e := got[0]
	if !e.At.Equal(clk()) {
		t.Errorf("At = %v，期望时钟给的 %v", e.At, clk())
	}
	if e.Tenant != "-" {
		t.Errorf("空租户没被替换成哨兵: %q", e.Tenant)
	}
	if e.Outcome != audit.OutcomeOK {
		t.Errorf("Outcome = %q，期望 ok", e.Outcome)
	}
}

// 显式给的时间戳与结果不得被覆盖：平台级事件的时间来自动作发生点，
// 不是记录点。
func TestRecordKeepsExplicitTimestampAndOutcome(t *testing.T) {
	l := audit.New(8, func() time.Time { return base.Add(time.Hour) })

	explicit := base.Add(-time.Minute)
	l.Record(audit.Event{
		At: explicit, Tenant: tenantA, Action: audit.ActionRunRejected,
		Outcome: audit.OutcomeDenied, Subject: "quota",
	})

	e := l.Recent(0)[0]
	if !e.At.Equal(explicit) {
		t.Errorf("At 被覆盖成 %v，期望保留 %v", e.At, explicit)
	}
	if e.Outcome != audit.OutcomeDenied {
		t.Errorf("Outcome = %q，期望 denied", e.Outcome)
	}
}

func TestNilClockFallsBackToWallClock(t *testing.T) {
	l := audit.New(8, nil)
	before := time.Now().Add(-time.Second)
	l.Record(audit.Event{Tenant: tenantA, Action: audit.ActionRunStart})
	after := time.Now().Add(time.Second)

	if got := l.Recent(0)[0].At; got.Before(before) || got.After(after) {
		t.Fatalf("At = %v，落在真实时间的窗外", got)
	}
}

// 平台级动作不属于某个租户，但仍必须可追溯：哨兵值保证它不会
// 被 For(任何租户) 捞出来，也不会在按租户聚合时消失。
func TestPlatformLevelEventsUseTheSentinelAndStayOutOfTenantViews(t *testing.T) {
	l := audit.New(8, nil)
	l.Record(audit.Event{Action: audit.ActionCapabilityDown, Subject: "models"})
	l.Record(at("a1"))

	if got := len(l.For(tenantA, 0)); got != 1 {
		t.Fatalf("For(tenant-a) 捞到了 %d 条，平台级事件不该进来", got)
	}
	if got := len(l.For("-", 0)); got != 1 {
		t.Fatalf("平台级事件没有被登记到哨兵租户下: got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

func TestSinksReceiveEveryEventIncludingThoseElidedFromTheRing(t *testing.T) {
	var mu sync.Mutex
	var got []string
	sink := audit.SinkFunc(func(e audit.Event) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, e.Subject)
	})

	l := audit.New(2, nil, sink)
	for i := 0; i < 5; i++ {
		l.Record(at(fmt.Sprintf("e%d", i)))
	}

	// Sink 是**出口**：环形缓冲丢的是内存副本，不是出口投递。
	// 若哪天有人把 Sink 调用挪到"确定要入环"之后，审计就会随
	// 容量静默丢事件——这正是外部落盘存在的意义。
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"e0", "e1", "e2", "e3", "e4"}; !equalStrings(got, want) {
		t.Fatalf("Sink 收到 %v，期望全部 %v", got, want)
	}
	if l.Len() != 2 {
		t.Fatalf("内存里保留 %d 条，期望 2", l.Len())
	}
}

// Record 在锁**外**调用 Sink。
//
// 若有人图省事把调用挪进临界区，一个会回读日志的 Sink（比如
// 「把最近 N 条一起打包上报」）就会自锁死：写锁持有期间去拿读锁。
// 用一个超时把死锁变成失败，而不是让测试挂住。
func TestSinkRunsOutsideTheLogLock(t *testing.T) {
	var lg *audit.Log
	done := make(chan struct{})

	lg = audit.New(8, nil, audit.SinkFunc(func(audit.Event) {
		lg.Len() // 读锁：只有在 Record 不持写锁时才能拿到
		lg.For(tenantA, 1)
		lg.Count(tenantA)
		lg.Recent(1)
		close(done)
	}))

	go lg.Record(at("e0"))

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Sink 在日志锁内被调用（或根本没被调用）：回读日志的 Sink 会自锁死")
	}
}

// Sink 收到的必须是**补齐默认值之后**的事件：出口不该自己再判一次
// 空租户、空时间戳，否则每个适配器都会抄一遍同一段逻辑。
func TestSinkSeesTheCompletedEvent(t *testing.T) {
	var mu sync.Mutex
	var got audit.Event
	l := audit.New(8, func() time.Time { return base }, audit.SinkFunc(func(e audit.Event) {
		mu.Lock()
		defer mu.Unlock()
		got = e
	}))

	l.Record(audit.Event{Action: audit.ActionToolCall})

	mu.Lock()
	defer mu.Unlock()
	if !got.At.Equal(base) || got.Tenant != "-" || got.Outcome != audit.OutcomeOK {
		t.Fatalf("Sink 收到的事件未补齐默认值: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// WriterSink
// ---------------------------------------------------------------------------

func TestWriterSinkWritesJSONLinesAndCountsWrites(t *testing.T) {
	var buf bytes.Buffer
	s := audit.NewWriterSink(&buf)

	for i := 0; i < 3; i++ {
		s.Write(at(fmt.Sprintf("e%d", i)))
	}

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("写了 %d 行，期望 3: %q", len(lines), buf.String())
	}
	for i, line := range lines {
		var e audit.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("第 %d 行不是合法 JSON: %v (%q)", i, err, line)
		}
		if e.Subject != fmt.Sprintf("e%d", i) {
			t.Fatalf("第 %d 行的 subject = %q", i, e.Subject)
		}
		if e.Action != audit.ActionRunStart || e.Tenant != tenantA {
			t.Fatalf("第 %d 行丢字段: %+v", i, e)
		}
	}

	if got := s.Written(); got != 3 {
		t.Errorf("Written() = %d，期望 3", got)
	}
	if got := s.Dropped(); got != 0 {
		t.Errorf("Dropped() = %d，期望 0", got)
	}
}

// errWriter 模拟下游写失败（磁盘满、对端断开）。
type errWriter struct{ calls int }

func (w *errWriter) Write(p []byte) (int, error) {
	w.calls++
	return 0, errors.New("下游不可写")
}

// 审计出口丢事件必须留下痕迹：事后看日志，「没有这条记录」与
// 「这条记录没写成功」完全无法区分，而审计的用途恰恰是事后追溯。
func TestWriterSinkCountsDroppedEvents(t *testing.T) {
	w := &errWriter{}
	s := audit.NewWriterSink(w)

	for i := 0; i < 4; i++ {
		s.Write(at(fmt.Sprintf("e%d", i)))
	}

	if got := s.Written(); got != 0 {
		t.Errorf("Written() = %d，期望 0", got)
	}
	if got := s.Dropped(); got != 4 {
		t.Fatalf("Dropped() = %d，期望 4 —— 静默丢弃让审计缺口无法被发现", got)
	}
	if w.calls != 4 {
		t.Errorf("下游被调用 %d 次，期望 4", w.calls)
	}
}

// 部分失败（前两条成功、后面失败）两个计数器都要对。
func TestWriterSinkCountsMixedOutcomes(t *testing.T) {
	var buf bytes.Buffer
	flaky := &flakyWriter{buf: &buf, failAfter: 2}
	s := audit.NewWriterSink(flaky)

	for i := 0; i < 5; i++ {
		s.Write(at(fmt.Sprintf("e%d", i)))
	}
	if got := s.Written(); got != 2 {
		t.Errorf("Written() = %d，期望 2", got)
	}
	if got := s.Dropped(); got != 3 {
		t.Errorf("Dropped() = %d，期望 3", got)
	}
}

type flakyWriter struct {
	mu        sync.Mutex
	buf       *bytes.Buffer
	failAfter int
	calls     int
}

func (w *flakyWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.calls > w.failAfter {
		return 0, errors.New("下游不可写")
	}
	return w.buf.Write(p)
}

// 多个调度器 goroutine 会并发写同一个 writer，加锁是硬要求。
// bytes.Buffer 本身不是并发安全的，因此这条测试在 -race 下必须干净，
// 且行数必须精确等于写入次数（不能有一行被覆盖或撕开）。
func TestWriterSinkIsConcurrencySafe(t *testing.T) {
	var buf bytes.Buffer
	s := audit.NewWriterSink(&buf)

	const workers, each = 8, 50
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < each; j++ {
				s.Write(at(fmt.Sprintf("w%d-%d", i, j)))
			}
		}(i)
	}
	wg.Wait()

	if got := s.Written(); got != workers*each {
		t.Errorf("Written() = %d，期望 %d", got, workers*each)
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != workers*each {
		t.Fatalf("输出 %d 行，期望 %d —— 并发写把行撕开了", len(lines), workers*each)
	}
	for _, line := range lines {
		var e audit.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("并发写产生了非法 JSON 行: %v (%q)", err, line)
		}
	}
}

// ---------------------------------------------------------------------------
// 词表与并发
// ---------------------------------------------------------------------------

// Action 是命名类型而不是裸 string，理由是**封闭词表**：谁在什么情况下
// 写了什么动作必须可枚举。裸字符串会让 "tenant.provsion" 这类笔误
// 一路通过编译。
//
// 这条测试守住词表的两条可枚举性要求：取值两两不同（复制粘贴事故），
// 且都带合法的命名空间前缀（按前缀过滤是运维的常用手段）。
func TestActionVocabularyIsDistinctAndNamespaced(t *testing.T) {
	allowedNS := map[string]bool{
		"tenant": true, "session": true, "run": true,
		"tool": true, "isolation": true, "capability": true,
	}

	all := []struct {
		name string
		a    audit.Action
	}{
		{"ActionTenantProvision", audit.ActionTenantProvision},
		{"ActionTenantDeprovision", audit.ActionTenantDeprovision},
		{"ActionSessionCreate", audit.ActionSessionCreate},
		{"ActionSessionClose", audit.ActionSessionClose},
		{"ActionRunStart", audit.ActionRunStart},
		{"ActionRunFinish", audit.ActionRunFinish},
		{"ActionRunRejected", audit.ActionRunRejected},
		{"ActionToolCall", audit.ActionToolCall},
		{"ActionToolDenied", audit.ActionToolDenied},
		{"ActionIsolationCheck", audit.ActionIsolationCheck},
		{"ActionCapabilityDown", audit.ActionCapabilityDown},
	}

	seen := make(map[audit.Action]string, len(all))
	for _, n := range all {
		if n.a == "" {
			t.Errorf("%s 是空动作名", n.name)
			continue
		}
		if prev, dup := seen[n.a]; dup {
			t.Errorf("%s 与 %s 取值相同（%q）—— 两条不同的动作在审计里无法区分", n.name, prev, n.a)
		}
		seen[n.a] = n.name

		ns, _, ok := strings.Cut(string(n.a), ".")
		if !ok || ns == "" {
			t.Errorf("%s = %q 缺少命名空间前缀", n.name, n.a)
			continue
		}
		if !allowedNS[ns] {
			t.Errorf("%s = %q 的命名空间 %q 不在词表内", n.name, n.a, ns)
		}
	}
}

func TestOutcomeVocabularyIsClosed(t *testing.T) {
	seen := map[audit.Outcome]string{}
	for _, c := range []struct {
		name string
		o    audit.Outcome
	}{
		{"OutcomeOK", audit.OutcomeOK},
		{"OutcomeDenied", audit.OutcomeDenied},
		{"OutcomeError", audit.OutcomeError},
	} {
		if c.o == "" {
			t.Errorf("%s 是空结果", c.name)
		}
		if prev, dup := seen[c.o]; dup {
			t.Errorf("%s 与 %s 取值相同（%q）", c.name, prev, c.o)
		}
		seen[c.o] = c.name
	}
}

func TestStringReportsWaterLevelAndSinkCount(t *testing.T) {
	l := audit.New(4, nil, audit.SinkFunc(func(audit.Event) {}))
	l.Record(at("e0"))
	l.Record(at("e1"))

	got := l.String()
	if !strings.Contains(got, "2/4") {
		t.Errorf("String() = %q，未体现水位 2/4", got)
	}
	if !strings.Contains(got, "1 sinks") {
		t.Errorf("String() = %q，未体现 sink 数量", got)
	}
}

func TestConcurrentRecordAndRead(t *testing.T) {
	var mu sync.Mutex
	var sinkCount int
	l := audit.New(128, nil, audit.SinkFunc(func(audit.Event) {
		mu.Lock()
		defer mu.Unlock()
		sinkCount++
	}))

	const workers, each = 8, 200
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tt := tenantA
			if i%2 == 0 {
				tt = tenantB
			}
			for j := 0; j < each; j++ {
				l.Record(audit.Event{Tenant: tt, Action: audit.ActionToolCall,
					Subject: fmt.Sprintf("w%d-%d", i, j)})
			}
		}(i)
	}
	// 并发读者：For/Count/Recent/Len 都必须能在写入过程中安全调用。
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				l.For(tenantA, 8)
				l.Count(tenantB)
				l.Recent(8)
				l.Len()
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()

	if got := l.Len(); got != 128 {
		t.Errorf("Len() = %d，期望容量上限 128", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if sinkCount != workers*each {
		t.Errorf("Sink 收到 %d 条，期望 %d（并发下不得丢投递）", sinkCount, workers*each)
	}
	// 每个租户各写一半，窗口足够大，两边都应该被完整保留。
	if a, b := l.Count(tenantA), l.Count(tenantB); a+b != 128 {
		t.Errorf("按租户计数之和 = %d，期望 128", a+b)
	}
}
