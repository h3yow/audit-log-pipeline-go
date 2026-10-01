package replay

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// realStoreEnv 用真实 storage.Store 构造回放测试环境：
// 小分段阈值制造大量已封存分段，模拟“几百 MB 历史留存”的多段形态。
type realStoreEnv struct {
	store *storage.Store
	mgr   *Manager
}

func newRealStoreEnv(t *testing.T, maxSeg int64) *realStoreEnv {
	t.Helper()
	s, err := storage.Open(storage.Options{
		Dir:             t.TempDir(),
		Retention:       storage.Retention{MaxAge: time.Hour},
		MaxSegmentBytes: maxSeg,
	})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	keyPath := filepath.Join(t.TempDir(), "key.bin")
	if err := writeKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	m, err := Open(filepath.Join(t.TempDir(), "replay"), keyPath, s)
	if err != nil {
		t.Fatalf("open replay manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return &realStoreEnv{store: s, mgr: m}
}

// appendLate 写入 seq 1..n：事件自身时间从 base 起每秒一条，
// 但落盘时间 StoredAt 一律为 late（远晚于任何回放区间上界），
// 模拟“事件发生得早、过了一段时间才接进来落盘”的迟到数据。
func appendLate(t *testing.T, s *storage.Store, src string, n int64, base time.Time, late int64) {
	t.Helper()
	for i := int64(1); i <= n; i++ {
		e := &event.Event{
			ID: src + "-e" + strconv.FormatInt(i, 10), Source: src, Seq: i,
			Type: "login", Action: "signin",
			Timestamp: base.Add(time.Duration(i) * time.Second),
		}
		if err := s.Append(storage.Record{Event: e, StoredAt: late, State: storage.StatePending}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
}

// TestTimeRangeLateIngestLargeRetention 是本次缺陷的核心复现场景：
// 事件自身时间落在回放区间内、但接入落盘时间晚于区间上界、且历史数据
// 跨大量已封存分段。命中判定必须按事件时间：区间内记录全部按序投递，
// 不允许整段静默丢失（修复前：段按落盘时间被剪枝，delivered=0 却报完成）。
func TestTimeRangeLateIngestLargeRetention(t *testing.T) {
	env := newRealStoreEnv(t, 4<<10) // 4KB 分段 => 上千条记录产生上百个已封存段
	const n = int64(1500)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix() // 落盘远晚于事件时间
	appendLate(t, env.store, "late-src", n, base, late)
	if err := env.store.SealActiveSegment("late-src"); err != nil {
		t.Fatal(err)
	}
	snap, err := env.store.ReplaySnapshot("late-src")
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Segments) < 10 {
		t.Fatalf("expected many sealed segments, got %d", len(snap.Segments))
	}
	t.Logf("RETENTION segments=%d records=%d (all stored at %s, event times %s..%s)",
		len(snap.Segments), n,
		time.Unix(late, 0).Format(time.RFC3339),
		base.Add(time.Second).Format(time.RFC3339), base.Add(time.Duration(n)*time.Second).Format(time.RFC3339))

	// 回放区间 [base+100s, base+1399s]：按事件时间应命中 seq 100..1399。
	from := base.Add(100 * time.Second)
	to := base.Add(1399 * time.Second)
	sk := newFakeSink(0)
	env.mgr.RegisterSink("replay-audit", sk)
	prog, err := env.mgr.Start(context.Background(), StartRequest{
		JobID: "job-late", Source: "late-src", Target: "replay-audit",
		Rng: Range{Source: "late-src", From: &from, To: &to},
	})
	if err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, env.mgr, "job-late", StatusComplete)

	const wantMatched = int64(1300) // seq 100..1399
	t.Logf("PLAN eff=[%d,%d] matched=%d skipped=%d pruned=%d note=%q",
		prog.Plan.EffFromSeq, prog.Plan.EffToSeq, prog.Plan.Matched,
		prog.Plan.SkippedTotal, len(prog.Plan.Pruned), prog.Plan.Note)
	if prog.Plan.Matched != wantMatched {
		t.Fatalf("matched=%d want %d (event-time hits must not be dropped by stored-time pruning)",
			prog.Plan.Matched, wantMatched)
	}
	if final.Delivered != wantMatched {
		t.Fatalf("delivered=%d want %d; job reported complete with missing deliveries",
			final.Delivered, wantMatched)
	}
	// 投递集合与顺序：严格 seq 100..1399。
	got := sk.seqs()
	if len(got) != int(wantMatched) {
		t.Fatalf("delivered set size=%d want %d", len(got), wantMatched)
	}
	for i, s := range got {
		if s != int64(100+i) {
			t.Fatalf("delivery order/set mismatch at %d: got seq %d", i, s)
		}
	}
	// 对账：扫描区间内的区间外记录逐条有跳过说明（SkippedTotal），
	// 段级整体排除的记录逐段有说明（Pruned）；三者合计必须覆盖全部留存，
	// 不允许任何记录“没投出去也没说明”。
	var prunedCovered int64
	for _, p := range prog.Plan.Pruned {
		if p.Kind != SkipOutsideTime || p.Reason == "" {
			t.Fatalf("pruned segment lacks explanation: %+v", p)
		}
		prunedCovered += p.MaxSeq - p.MinSeq + 1
	}
	scanned := prog.Plan.EffToSeq - prog.Plan.EffFromSeq + 1
	if prog.Plan.Matched+prog.Plan.SkippedTotal != scanned {
		t.Fatalf("accounting mismatch: matched(%d)+skipped(%d) != scanned(%d)",
			prog.Plan.Matched, prog.Plan.SkippedTotal, scanned)
	}
	if scanned+prunedCovered != n {
		t.Fatalf("accounting mismatch: scanned(%d)+pruned(%d) != retained(%d)",
			scanned, prunedCovered, n)
	}
	if final.Remaining() != 0 {
		t.Fatalf("remaining=%d", final.Remaining())
	}
}

// TestTimeRangeInclusiveBounds：区间两端时刻本身算命中（闭区间）。
func TestTimeRangeInclusiveBounds(t *testing.T) {
	env := newRealStoreEnv(t, 1<<20)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	late := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
	appendLate(t, env.store, "b-src", 10, base, late)
	if err := env.store.SealActiveSegment("b-src"); err != nil {
		t.Fatal(err)
	}
	// From/To 恰好等于 seq 3 / seq 7 的事件时间戳。
	from := base.Add(3 * time.Second)
	to := base.Add(7 * time.Second)
	sk := newFakeSink(0)
	env.mgr.RegisterSink("replay-audit", sk)
	prog, err := env.mgr.Start(context.Background(), StartRequest{
		JobID: "job-bounds", Source: "b-src", Target: "replay-audit",
		Rng: Range{Source: "b-src", From: &from, To: &to},
	})
	if err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, env.mgr, "job-bounds", StatusComplete)
	got := sk.seqs()
	want := []int64{3, 4, 5, 6, 7}
	if len(got) != len(want) {
		t.Fatalf("inclusive bounds delivered=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("inclusive bounds delivered=%v want=%v", got, want)
		}
	}
	if prog.Plan.Matched != 5 || final.Delivered != 5 {
		t.Fatalf("matched=%d delivered=%d want 5/5 (boundary instants are hits)",
			prog.Plan.Matched, final.Delivered)
	}
}

// TestTimeRangePartialSegmentAccounting：一段数据里只有一部分落在区间内：
// 命中部分投出，区间外部分逐条给出 outside_time 跳过说明，总数对得上。
func TestTimeRangePartialSegmentAccounting(t *testing.T) {
	env := newRealStoreEnv(t, 1<<20) // 单段
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
	appendLate(t, env.store, "p-src", 10, base, late)
	if err := env.store.SealActiveSegment("p-src"); err != nil {
		t.Fatal(err)
	}
	from := base.Add(4 * time.Second)
	to := base.Add(6 * time.Second)
	sk := newFakeSink(0)
	env.mgr.RegisterSink("replay-audit", sk)
	prog, err := env.mgr.Start(context.Background(), StartRequest{
		JobID: "job-partial", Source: "p-src", Target: "replay-audit",
		Rng: Range{Source: "p-src", From: &from, To: &to},
	})
	if err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, env.mgr, "job-partial", StatusComplete)
	got := sk.seqs()
	if len(got) != 3 || got[0] != 4 || got[2] != 6 {
		t.Fatalf("partial segment delivered=%v want [4 5 6]", got)
	}
	if prog.Plan.Matched != 3 || prog.Plan.SkippedTotal != 7 {
		t.Fatalf("matched=%d skipped=%d want 3/7", prog.Plan.Matched, prog.Plan.SkippedTotal)
	}
	// 7 条区间外记录必须逐条有 outside_time 说明（未超上限，全量保留）。
	if len(prog.Plan.Skipped) != 7 {
		t.Fatalf("skipped entries=%d want 7", len(prog.Plan.Skipped))
	}
	explained := map[int64]bool{}
	for _, s := range prog.Plan.Skipped {
		if s.Kind != SkipOutsideTime {
			t.Fatalf("skip kind=%s want outside_time", s.Kind)
		}
		explained[s.Seq] = true
	}
	for _, seq := range []int64{1, 2, 3, 7, 8, 9, 10} {
		if !explained[seq] {
			t.Fatalf("seq %d outside range but not explained", seq)
		}
	}
	if final.Delivered+prog.Plan.SkippedTotal != 10 {
		t.Fatalf("accounting: delivered(%d)+skipped(%d) != 10",
			final.Delivered, prog.Plan.SkippedTotal)
	}
}

// TestTimeRangeEntirelyBeforeLateBatch：区间整个落在“更晚才落盘”那批数据的
// 事件时间之前——0 命中是合法结果，但任务必须 complete 且每个被排除的段
// 都有显式说明（Pruned），不允许无声无息地结束。
func TestTimeRangeEntirelyBeforeLateBatch(t *testing.T) {
	env := newRealStoreEnv(t, 4<<10)
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
	appendLate(t, env.store, "z-src", 300, base, late)
	if err := env.store.SealActiveSegment("z-src"); err != nil {
		t.Fatal(err)
	}
	// 区间在整个批次的事件时间之前。
	from := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 12, 2, 0, 0, 0, 0, time.UTC)
	sk := newFakeSink(0)
	env.mgr.RegisterSink("replay-audit", sk)
	prog, err := env.mgr.Start(context.Background(), StartRequest{
		JobID: "job-zero", Source: "z-src", Target: "replay-audit",
		Rng: Range{Source: "z-src", From: &from, To: &to},
	})
	if err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, env.mgr, "job-zero", StatusComplete)
	t.Logf("ZERO-HIT PLAN matched=%d pruned=%d note=%q",
		prog.Plan.Matched, len(prog.Plan.Pruned), prog.Plan.Note)
	if final.Delivered != 0 || prog.Plan.Matched != 0 {
		t.Fatalf("delivered=%d matched=%d want 0/0", final.Delivered, prog.Plan.Matched)
	}
	if len(sk.seqs()) != 0 {
		t.Fatalf("unexpected deliveries: %v", sk.seqs())
	}
	// 0 命中必须有解释：所有已封存段按事件时间界被显式排除。
	if len(prog.Plan.Pruned) == 0 {
		t.Fatalf("zero-hit replay gave no explanation: plan=%+v", prog.Plan)
	}
	var covered int64
	for _, p := range prog.Plan.Pruned {
		if p.Kind != SkipOutsideTime || p.Reason == "" {
			t.Fatalf("pruned segment lacks reason: %+v", p)
		}
		covered += p.MaxSeq - p.MinSeq + 1
	}
	if covered != 300 {
		t.Fatalf("pruned segments cover %d records, want 300 (every record accounted)", covered)
	}
}

// TestTimeRangeResumeAfterRestart：时间区间回放跑到一半进程重启，
// 接着上次位置继续时，剩余部分不按落盘时间被跳过，不重不漏。
func TestTimeRangeResumeAfterRestart(t *testing.T) {
	s, err := storage.Open(storage.Options{
		Dir:             t.TempDir(),
		Retention:       storage.Retention{MaxAge: time.Hour},
		MaxSegmentBytes: 4 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
	appendLate(t, s, "r-src", 200, base, late)
	if err := s.SealActiveSegment("r-src"); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key.bin")
	if err := writeKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	rpDir := filepath.Join(dir, "replay")
	from := base.Add(50 * time.Second)
	to := base.Add(180 * time.Second)

	// 第一次运行：下游成功 30 次后永久失败，进度卡住后关闭（模拟重启）。
	m1, err := Open(rpDir, keyPath, s)
	if err != nil {
		t.Fatal(err)
	}
	m1.backoff = 20 * time.Millisecond
	m1.RegisterSink("replay-audit", &countingSink{failAfter: 30})
	if _, err := m1.Start(context.Background(), StartRequest{
		JobID: "job-restart", Source: "r-src", Target: "replay-audit",
		Rng: Range{Source: "r-src", From: &from, To: &to},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		p, _ := m1.Get("job-restart")
		if p.Delivered == 30 && p.Retries > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("progress did not stick at 30: %+v", p)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启续跑：seq 50..79 已投（30 条），剩余 seq 80..180（101 条）继续投递。
	m2, err := Open(rpDir, keyPath, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close() })
	sk2 := newFakeSink(0)
	m2.RegisterSink("replay-audit", sk2)
	if _, blocked, err := m2.ResumeAll(); err != nil || len(blocked) != 0 {
		t.Fatalf("resume: blocked=%v err=%v", blocked, err)
	}
	final := waitStatus(t, m2, "job-restart", StatusComplete)
	const wantTotal = int64(131) // seq 50..180
	if final.Delivered != wantTotal {
		t.Fatalf("delivered=%d want %d", final.Delivered, wantTotal)
	}
	got := sk2.seqs()
	if len(got) != 101 {
		t.Fatalf("post-restart deliveries=%d want 101", len(got))
	}
	for i, seq := range got {
		if seq != int64(80+i) {
			t.Fatalf("post-restart order mismatch at %d: seq=%d", i, seq)
		}
	}
}

// TestTimeRangeConcurrentPurgeFreeze（-race）：大批量时间区间回放进行中，
// 并发执行保留清理与合规冻结增删：回放占位保护范围内分段不被清理，
// 范围判定不漏掉本该命中的事件，最终投递集合完整且有序。
func TestTimeRangeConcurrentPurgeFreeze(t *testing.T) {
	env := newRealStoreEnv(t, 4<<10)
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	late := time.Now().Add(-2 * time.Hour).Unix() // 落盘时间已超保留期（可清理候选）
	appendLate(t, env.store, "c-src", 600, base, late)
	if err := env.store.SealActiveSegment("c-src"); err != nil {
		t.Fatal(err)
	}

	// 区间覆盖全部 600 条：所有分段都在回放占位保护范围内，
	// 清理不得删除其中任何一个。
	from := base.Add(1 * time.Second)
	to := base.Add(600 * time.Second)
	// 下游先不可用，让回放任务保持活跃占位。
	blocking := &countingSink{failAfter: 0}
	env.mgr.RegisterSink("replay-audit", blocking)
	if _, err := env.mgr.Start(context.Background(), StartRequest{
		JobID: "job-conc", Source: "c-src", Target: "replay-audit",
		Rng: Range{Source: "c-src", From: &from, To: &to},
	}); err != nil {
		t.Fatal(err)
	}
	waitForRetries := func() {
		deadline := time.Now().Add(3 * time.Second)
		for {
			p, _ := env.mgr.Get("job-conc")
			if p.Status == StatusRunning && p.Retries > 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("replay not retrying: %+v", p)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitForRetries()

	var wg sync.WaitGroup
	// 并发清理：范围内分段被回放占位（或冻结）保护，一律不得删除。
	var purgeRes []storage.PurgeResult
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			res, err := env.store.Purge(time.Now().Unix())
			if err != nil {
				t.Errorf("purge: %v", err)
				return
			}
			purgeRes = append(purgeRes, res)
			time.Sleep(time.Millisecond)
		}
	}()
	// 并发冻结增删（与回放范围相交）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			id := "fz-" + strconv.Itoa(i)
			if err := env.store.AddFreeze(storage.Freeze{
				ID: id, Source: "c-src", FromSeq: 1, ToSeq: 600,
				Until: time.Now().Add(time.Minute),
			}); err != nil {
				t.Errorf("freeze: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
			if err := env.store.RemoveFreeze(id); err != nil {
				t.Errorf("unfreeze: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	var totalProtected int
	for i, res := range purgeRes {
		if res.DeletedSegments != 0 {
			t.Fatalf("purge #%d deleted %d segments under active replay hold", i, res.DeletedSegments)
		}
		totalProtected += res.SkippedReplay + res.SkippedFrozen
	}
	t.Logf("PURGE DURING REPLAY protected(replay+frozen)=%d over %d runs", totalProtected, len(purgeRes))
	if totalProtected == 0 {
		t.Fatalf("no segment was protected by replay hold or freeze; holds not effective")
	}

	// 下游恢复，回放必须完整投出 600 条。
	env.mgr.RegisterSink("replay-audit", newFakeSink(0))
	final := waitStatus(t, env.mgr, "job-conc", StatusComplete)
	if final.Delivered != 600 {
		t.Fatalf("delivered=%d want 600 after concurrent purge/freeze", final.Delivered)
	}
	// 回放完成后占位释放：到期段现在可以清理。
	res, err := env.store.Purge(time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedSegments == 0 {
		t.Fatalf("hold not released after completion: %+v", res)
	}
	if err := env.store.Verify(); err != nil {
		t.Fatalf("verify after purge: %v", err)
	}
}

// TestSegmentCouldMatchByEventTime 直接锁定剪枝语义：
// 只允许按事件时间界剪枝；落盘时间与区间对不上不得排除段。
func TestSegmentCouldMatchByEventTime(t *testing.T) {
	mk := func(minEv, maxEv, firstStored, lastStored int64, sealed bool) storage.SegmentMeta {
		return storage.SegmentMeta{
			Name: "seg-000001.dat", MinSeq: 1, MaxSeq: 100, Records: 100,
			MinEventTS: minEv, MaxEventTS: maxEv,
			FirstStored: firstStored, LastStored: lastStored, Sealed: sealed,
		}
	}
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	rng := Range{Source: "s", From: &from, To: &to}
	lateStore := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()

	cases := []struct {
		name string
		seg  storage.SegmentMeta
		want bool
	}{
		// 核心缺陷：事件时间在区间内、落盘时间远晚于区间上界 => 必须保留。
		{"late-ingest kept", mk(from.UnixNano(), to.UnixNano(), lateStore, lateStore, true), true},
		// 段事件时间界整体严格早于区间 => 排除并给出说明。
		{"before range pruned", mk(from.Add(-2*time.Hour).UnixNano(), from.Add(-time.Hour).UnixNano(), lateStore, lateStore, true), false},
		// 段事件时间界整体严格晚于区间 => 排除。
		{"after range pruned", mk(to.Add(time.Hour).UnixNano(), to.Add(2*time.Hour).UnixNano(), lateStore, lateStore, true), false},
		// 段界端点与区间端点相等（闭区间）=> 保留。
		{"boundary equal kept", mk(to.UnixNano(), to.Add(time.Hour).UnixNano(), lateStore, lateStore, true), true},
		// 事件时间界未知 => 不排除（安全方向）。
		{"unknown bounds kept", mk(0, 0, lateStore, lateStore, true), true},
		// 未封存段 => 不排除（元数据仍在变化）。
		{"unsealed kept", mk(from.Add(-2*time.Hour).UnixNano(), from.Add(-time.Hour).UnixNano(), lateStore, lateStore, false), true},
	}
	for _, c := range cases {
		ok, reason := segmentCouldMatch(c.seg, rng)
		if ok != c.want {
			t.Fatalf("%s: couldMatch=%v want %v (reason=%q)", c.name, ok, c.want, reason)
		}
		if !ok && reason == "" {
			t.Fatalf("%s: pruned without explanation", c.name)
		}
	}
	// 序号范围外的段：排除但无需说明（不属于本次判定对象）。
	segRng := Range{Source: "s", FromSeq: 200, ToSeq: 300}
	ok, reason := segmentCouldMatch(mk(from.UnixNano(), to.UnixNano(), lateStore, lateStore, true), segRng)
	if ok || reason != "" {
		t.Fatalf("seq-outside segment: ok=%v reason=%q, want false/\"\"", ok, reason)
	}
}

// TestTimeRangeInterleavedEventTimes：事件时间不随 seq 单调（迟到批次乱序落盘），
// 一个事件时间严格在区间外的段被夹在两个命中段之间。该段不用于界定扫描区间，
// 但其序号落在扫描区间内、记录会被逐条判定——因此它不得再出现在 Pruned 里
// 重复说明；命中 + 逐条跳过必须精确覆盖全部记录。
func TestTimeRangeInterleavedEventTimes(t *testing.T) {
	env := newRealStoreEnv(t, 1<<20)
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
	appendBatch := func(lo, hi int64, tsBase time.Time) {
		t.Helper()
		for i := lo; i <= hi; i++ {
			e := &event.Event{
				ID: "x-src-e" + strconv.FormatInt(i, 10), Source: "x-src", Seq: i,
				Type: "login", Action: "signin",
				Timestamp: tsBase.Add(time.Duration(i) * time.Second),
			}
			if err := env.store.Append(storage.Record{Event: e, StoredAt: late, State: storage.StatePending}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		if err := env.store.SealActiveSegment("x-src"); err != nil {
			t.Fatal(err)
		}
	}
	// 段A：seq 1..50，事件时间 base+1s..base+50s（部分命中）
	appendBatch(1, 50, base)
	// 段B：seq 51..100，事件时间远在未来（严格在区间外，被夹在中间）
	appendBatch(51, 100, base.Add(10000*time.Second))
	// 段C：seq 101..150，事件时间 base+101s..base+150s（部分命中）
	appendBatch(101, 150, base)

	from := base.Add(10 * time.Second)
	to := base.Add(140 * time.Second)
	sk := newFakeSink(0)
	env.mgr.RegisterSink("replay-audit", sk)
	prog, err := env.mgr.Start(context.Background(), StartRequest{
		JobID: "job-interleave", Source: "x-src", Target: "replay-audit",
		Rng: Range{Source: "x-src", From: &from, To: &to},
	})
	if err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, env.mgr, "job-interleave", StatusComplete)

	const wantMatched = int64(41 + 40) // seq 10..50 ∪ 101..140
	t.Logf("PLAN eff=[%d,%d] matched=%d skipped=%d pruned=%d",
		prog.Plan.EffFromSeq, prog.Plan.EffToSeq, prog.Plan.Matched,
		prog.Plan.SkippedTotal, len(prog.Plan.Pruned))
	if prog.Plan.Matched != wantMatched || final.Delivered != wantMatched {
		t.Fatalf("matched=%d delivered=%d want %d", prog.Plan.Matched, final.Delivered, wantMatched)
	}
	// 段B 与扫描区间相交：不段级列出（避免与逐条说明重复）。
	if len(prog.Plan.Pruned) != 0 {
		t.Fatalf("interleaved excluded segment must not be double-reported: %+v", prog.Plan.Pruned)
	}
	// 对账：matched + skipped == 150（段B 的 50 条逐条计入 skipped）。
	if prog.Plan.Matched+prog.Plan.SkippedTotal != 150 {
		t.Fatalf("accounting: matched(%d)+skipped(%d) != 150",
			prog.Plan.Matched, prog.Plan.SkippedTotal)
	}
	// 投递集合与顺序：seq 10..50 接 101..140，严格升序。
	got := sk.seqs()
	if len(got) != int(wantMatched) {
		t.Fatalf("delivered set=%d want %d", len(got), wantMatched)
	}
	for i, s := range got {
		want := int64(10 + i)
		if i >= 41 {
			want = int64(101 + (i - 41))
		}
		if s != want {
			t.Fatalf("delivery mismatch at %d: got %d want %d", i, s, want)
		}
	}
}
