package pipeline

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/replay"
	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// ingestNAt 以固定事件时间戳 base+i*step 接入 1..n 条事件。
// 落盘时间 StoredAt 由管道用当前墙钟写入，因此调用方可制造
// “事件发生得很早、但很晚才接入落盘”的留存批次。
func ingestNAt(t *testing.T, p *Pipeline, src string, n int64, base time.Time, step time.Duration) {
	t.Helper()
	ctx := context.Background()
	for i := int64(1); i <= n; i++ {
		e := &event.Event{
			ID: src + "-e" + strconv.FormatInt(i, 10), Source: src, Seq: i, Type: "login",
			Subject: "alice", Action: "do", Resource: "c",
			Timestamp: base.Add(time.Duration(i) * step),
		}
		if _, err := p.Ingest(ctx, e); err != nil {
			t.Fatalf("ingest %s %d: %v", src, i, err)
		}
	}
}

// TestReplayTimeRangeLateStoredBatch：核心复现。
//
//	大批量历史事件（几百 MB 量级由“多个已封存分段”模拟）自身时间落在
//	2026-01-01 的某个区间内，但它们是在很晚（当前墙钟）才接入落盘的，
//	因此分段的 StoredAt 整体晚于回放时间上界。时间区间回放必须按事件
//	自身时间戳判定命中，把区间内的记录全部投出去，不能因为落盘时间晚
//	就把整段（乃至整个任务）默默剪掉、投递 0 条还报完成。
func TestReplayTimeRangeLateStoredBatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	live := sink.NewMemorySink()
	rep := sink.NewMemorySink()
	p, err := New(replayTestConfig(t, dir, live, rep))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// 小分段滚动阈值：让这批事件分布到多个已封存分段上（模拟大批量历史留存）。
	p.store.SetMaxSegmentBytesForTest(4096)

	const n int64 = 600
	src := "srcLate"
	// 事件自身时间：2026-01-01 00:00:01 起，每秒一条（早发生）。
	evBase := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ingestNAt(t, p, src, n, evBase, time.Second)
	waitFor(t, func() bool { return p.CheckpointWatermark(src) == n }, "live watermark")
	// 封存尾段，使全部留存分段都处于 sealed 状态（触发 StoredAt 剪枝）。
	if err := p.store.SealActiveSegment(src); err != nil {
		t.Fatal(err)
	}

	// 回放时间闭区间 [100s, 200s]：事件时间命中，落盘时间却是“现在”（远晚上界）。
	from := time.Date(2026, 1, 1, 0, 1, 40, 0, time.UTC) // 第 100 条
	to := time.Date(2026, 1, 1, 0, 3, 20, 0, time.UTC)   // 第 200 条
	prog, err := p.StartReplay(context.Background(), "rjLate", src, "replay-audit",
		ReplayRange{From: &from, To: &to})
	if err != nil {
		t.Fatal(err)
	}
	final, err := p.WaitReplay(context.Background(), "rjLate")
	if err != nil {
		t.Fatal(err)
	}

	got := rep.Visible("replay-audit", src)
	t.Logf("LATE-STORED TIME RANGE [%s,%s]", from.Format(time.RFC3339), to.Format(time.RFC3339))
	t.Logf("PLAN eff=[%d,%d] matched=%d gaps=%d skipped=%d",
		prog.Plan.EffFromSeq, prog.Plan.EffToSeq, prog.Plan.Matched,
		len(prog.Plan.Gaps), len(prog.Plan.Skipped))
	t.Logf("PROGRESS status=%s delivered=%d remaining=%d next=%d visible=%d",
		final.Status, final.Delivered, final.Remaining(), final.NextSeq, len(got))

	if final.Status != replay.StatusComplete {
		t.Fatalf("status=%s want complete", final.Status)
	}
	// 闭区间两端时刻本身命中 => seq 100..200，共 101 条，全部必须投出。
	want := make([]string, 0, 101)
	for i := int64(100); i <= 200; i++ {
		want = append(want, src+"-e"+strconv.FormatInt(i, 10))
	}
	if len(got) != len(want) {
		t.Fatalf("delivered=%d want %d (matched=%d)", len(got), len(want), prog.Plan.Matched)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order mismatch at %d: got %s want %s", i, got[i], want[i])
		}
	}
	if final.Delivered != int64(len(want)) || final.Remaining() != 0 {
		t.Fatalf("delivered=%d remaining=%d want %d/0", final.Delivered, final.Remaining(), len(want))
	}
	// 命中数、逐条跳过数必须对得上扫描范围内的实际记录数。
	if prog.Plan.Matched != int64(len(want)) {
		t.Fatalf("plan matched=%d want %d", prog.Plan.Matched, len(want))
	}
	if int64(len(prog.Plan.Skipped))+prog.Plan.Matched != n {
		t.Fatalf("skipped=%d matched=%d total=%d want %d (no silent drop)",
			len(prog.Plan.Skipped), prog.Plan.Matched,
			int64(len(prog.Plan.Skipped))+prog.Plan.Matched, n)
	}
}

// TestReplayTimeRangeLateStoredResumeAfterRestart：时间区间回放一批
// 晚落盘的历史事件，中途进程重启；续跑剩余部分时仍按事件自身时间判定，
// 不能因同样的 StoredAt 剪枝把后半段跳过。
func TestReplayTimeRangeLateStoredResumeAfterRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	src := "srcResume"
	evBase := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	live1 := sink.NewMemorySink()
	rep1 := sink.NewMemorySink()
	p1, err := New(replayTestConfig(t, dir, live1, rep1))
	if err != nil {
		t.Fatal(err)
	}
	p1.store.SetMaxSegmentBytesForTest(4096)
	const n int64 = 400
	ingestNAt(t, p1, src, n, evBase, time.Second)
	waitFor(t, func() bool { return p1.CheckpointWatermark(src) == n }, "wm")
	if err := p1.store.SealActiveSegment(src); err != nil {
		t.Fatal(err)
	}

	from := evBase.Add(50 * time.Second)
	to := evBase.Add(350 * time.Second) // 命中 seq50..350，共 301 条

	// 下游先不可用，让任务进入重试，再用“成功若干条后失败”的方式难以精确卡停，
	// 这里改为：启动时间区间回放、等待确认若干条后 Close 模拟中途重启。
	rep1.SetAvailable(false)
	if _, err := p1.StartReplay(context.Background(), "rjR", src, "replay-audit",
		ReplayRange{From: &from, To: &to}); err != nil {
		t.Fatal(err)
	}
	// 确认任务已处于 running 重试状态（占位已登记、计划已持久化）。
	waitFor(t, func() bool {
		pr, _ := p1.ReplayProgress("rjR")
		return pr.Status == replay.StatusRunning && pr.Retries > 0 && pr.Plan.Matched == 301
	}, "replay blocked before first delivery")
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：全新下游，从头续跑（此前一条都没确认）。
	live2 := sink.NewMemorySink()
	rep2 := sink.NewMemorySink()
	p2, err := New(replayTestConfig(t, dir, live2, rep2))
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	final, err := p2.WaitReplay(context.Background(), "rjR")
	if err != nil {
		t.Fatal(err)
	}
	got := rep2.Visible("replay-audit", src)
	if final.Status != replay.StatusComplete || final.Delivered != 301 {
		t.Fatalf("after restart final=%+v want complete/301", final)
	}
	if len(got) != 301 || got[0] != src+"-e50" || got[300] != src+"-e350" {
		t.Fatalf("after restart visible=%d first=%s last=%s", len(got), got[0], got[len(got)-1])
	}
}

// blockingReplaySink 先放行前 allow 条，之后阻塞直到 release，便于把任务
// 精确卡在“已确认一部分、剩余未投”的中途状态。
type blockingReplaySink struct {
	*sink.MemorySink
	allowed int
	mu      chan struct{}
	rel     chan struct{}
}

func newBlockingReplaySink(allow int) *blockingReplaySink {
	return &blockingReplaySink{
		MemorySink: sink.NewMemorySink(), allowed: allow,
		mu: make(chan struct{}, 1<<20), rel: make(chan struct{}),
	}
}

func (b *blockingReplaySink) Deliver(ctx context.Context, route string, e *event.Event) error {
	b.mu <- struct{}{}
	defer func() { <-b.mu }()
	if n := b.VisibleCount(); n >= b.allowed {
		select {
		case <-b.rel:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.MemorySink.Deliver(ctx, route, e)
}

// TestReplayTimeRangeLateStoredMidwayCrash：确认部分投递后进程崩溃，
// 重启续跑剩余部分；最终集合 == 区间命中集合，顺序稳定，无重复可见。
func TestReplayTimeRangeLateStoredMidwayCrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	src := "srcMid"
	evBase := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	live := sink.NewMemorySink()
	cfg := replayTestConfig(t, dir, live, sink.NewMemorySink())
	p1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p1.store.SetMaxSegmentBytesForTest(4096)
	const n int64 = 300
	ingestNAt(t, p1, src, n, evBase, time.Second)
	waitFor(t, func() bool { return p1.CheckpointWatermark(src) == n }, "wm")
	if err := p1.store.SealActiveSegment(src); err != nil {
		t.Fatal(err)
	}

	// 用自定义可阻塞下游替换注册的回放目标。
	block := newBlockingReplaySink(40)
	p1.RegisterReplaySink("replay-audit", block)

	from := evBase.Add(10 * time.Second) // seq10..250 => 241 条
	to := evBase.Add(250 * time.Second)
	if _, err := p1.StartReplay(context.Background(), "rjM", src, "replay-audit",
		ReplayRange{From: &from, To: &to}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		pr, _ := p1.ReplayProgress("rjM")
		return pr.Delivered == 40
	}, "40 delivered then blocked")
	// 模拟崩溃：不解除阻塞直接 Close（worker 会因 stop 退出；已确认 40 条落盘）。
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：新下游，从持久化 next_seq 续跑，只能看到 seq50..250。
	live2 := sink.NewMemorySink()
	rep2 := sink.NewMemorySink()
	p2, err := New(replayTestConfig(t, dir, live2, rep2))
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	final, err := p2.WaitReplay(context.Background(), "rjM")
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != replay.StatusComplete || final.Delivered != 241 {
		t.Fatalf("final=%+v want complete/241", final)
	}
	got := rep2.Visible("replay-audit", src)
	if len(got) != 201 || got[0] != src+"-e50" || got[200] != src+"-e250" {
		t.Fatalf("post-restart visible=%d first=%s last=%s want seq50..250",
			len(got), got[0], got[len(got)-1])
	}
}

// TestReplayTimeRangeLateStoredConcurrentPurgeFreeze（-race）：
// 晚落盘历史批次的时间区间回放进行中，并发清理/冻结/接入。
// 时间判定必须稳定命中（不被 StoredAt 内部范围判定漏掉），
// 也不能读到被清掉的内容（回放占位保护），顺序与集合最终正确。
func TestReplayTimeRangeLateStoredConcurrentPurgeFreeze(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	src := "srcConc"
	evBase := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	live := sink.NewMemorySink()
	rep := sink.NewMemorySink()
	cfg := replayTestConfig(t, dir, live, rep)
	cfg.Retention = storage.Retention{MaxAge: time.Hour}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.store.SetMaxSegmentBytesForTest(4096)
	const n int64 = 500
	ingestNAt(t, p, src, n, evBase, time.Second)
	waitFor(t, func() bool { return p.CheckpointWatermark(src) == n }, "wm")
	if err := p.store.SealActiveSegment(src); err != nil {
		t.Fatal(err)
	}

	// 命中 seq100..400，共 301 条；StoredAt 都是“现在”。
	from := evBase.Add(100 * time.Second)
	to := evBase.Add(400 * time.Second)

	// 先把已封存段旧化到“已过保留期”，制造真正会被清理的候选；
	// 但回放占位 [1,500] 必须让 Purge 全程跳过它们。
	old := time.Now().Add(-2 * time.Hour).Unix()
	p.store.AgeSealedSegmentsForTest(src, old)

	rep.SetAvailable(false)
	prog, err := p.StartReplay(context.Background(), "rjConc", src, "replay-audit",
		ReplayRange{From: &from, To: &to})
	if err != nil {
		t.Fatal(err)
	}
	if prog.Plan.Matched != 301 {
		t.Fatalf("matched=%d want 301", prog.Plan.Matched)
	}
	waitFor(t, func() bool {
		pr, _ := p.ReplayProgress("rjConc")
		return pr.Status == replay.StatusRunning && pr.Retries > 0
	}, "replay running")

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(2)
	go func() { // 并发清理：活动占位下不得删除任何段
		defer wg.Done()
		for i := 0; i < 30; i++ {
			select {
			case <-stop:
				return
			default:
			}
			res, err := p.PurgeNow(time.Now())
			if err != nil {
				t.Errorf("purge: %v", err)
				return
			}
			if res.DeletedSegments != 0 {
				t.Errorf("purge deleted %d segments under time-range replay hold", res.DeletedSegments)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	go func() { // 并发加/解冻
		defer wg.Done()
		for i := 0; i < 15; i++ {
			id := "fz-" + strconv.Itoa(i)
			if err := p.AddFreeze(storage.Freeze{
				ID: id, Source: src, FromSeq: 1, ToSeq: 500, Until: time.Now().Add(time.Minute),
			}); err != nil {
				t.Errorf("freeze: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
			_ = p.RemoveFreeze(id)
		}
	}()

	time.Sleep(30 * time.Millisecond)
	rep.SetAvailable(true)
	final, err := p.WaitReplay(context.Background(), "rjConc")
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if final.Delivered != 301 || final.Remaining() != 0 {
		t.Fatalf("final delivered=%d remaining=%d want 301/0", final.Delivered, final.Remaining())
	}
	got := rep.Visible("replay-audit", src)
	if len(got) != 301 {
		t.Fatalf("visible=%d want 301", len(got))
	}
	for i, id := range got {
		wantSeq := int64(100 + i)
		if id != src+"-e"+strconv.FormatInt(wantSeq, 10) {
			t.Fatalf("order break at %d: %s want seq %d", i, id, wantSeq)
		}
	}
	if err := p.store.Verify(); err != nil {
		t.Fatalf("storage verify: %v", err)
	}
}

// ingestEvents 直接接入给定事件列表（事件时间可与 seq 顺序无关地混排）。
func ingestEvents(t *testing.T, p *Pipeline, src string, evs []*event.Event) {
	t.Helper()
	for _, e := range evs {
		if _, err := p.Ingest(context.Background(), e); err != nil {
			t.Fatalf("ingest %s %d: %v", src, e.Seq, err)
		}
	}
}

func mkEvAt(src string, seq int64, ts time.Time) *event.Event {
	return &event.Event{
		ID: src + "-e" + strconv.FormatInt(seq, 10), Source: src, Seq: seq,
		Type: "login", Subject: "alice", Action: "do", Resource: "c", Timestamp: ts,
	}
}

// TestReplayTimeRangeBoundariesInclusive：闭区间——from/to 时刻本身算命中；
// 同一段数据里只有一部分记录落在区间内，其余必须逐条 outside_time 说明。
func TestReplayTimeRangeBoundariesInclusive(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	live := sink.NewMemorySink()
	rep := sink.NewMemorySink()
	p, err := New(replayTestConfig(t, dir, live, rep))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	src := "srcBound"
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// 8 条事件，事件时间 = seq*10s（10s..80s），全部在一个活动段。
	var evs []*event.Event
	for i := int64(1); i <= 8; i++ {
		evs = append(evs, mkEvAt(src, i, base.Add(time.Duration(i)*10*time.Second)))
	}
	ingestEvents(t, p, src, evs)
	waitFor(t, func() bool { return p.CheckpointWatermark(src) == 8 }, "wm")

	// 闭区间 [30s,60s]：恰好命中 seq3,4,5,6（两端时刻本身命中）。
	from := base.Add(30 * time.Second)
	to := base.Add(60 * time.Second)
	prog, err := p.StartReplay(context.Background(), "rjB", src, "replay-audit",
		ReplayRange{From: &from, To: &to})
	if err != nil {
		t.Fatal(err)
	}
	final, err := p.WaitReplay(context.Background(), "rjB")
	if err != nil {
		t.Fatal(err)
	}
	got := rep.Visible("replay-audit", src)
	wantIDs := []string{"srcBound-e3", "srcBound-e4", "srcBound-e5", "srcBound-e6"}
	if len(got) != len(wantIDs) {
		t.Fatalf("inclusive boundary visible=%v want %v (matched=%d)", got, wantIDs, prog.Plan.Matched)
	}
	for i := range wantIDs {
		if got[i] != wantIDs[i] {
			t.Fatalf("order %d: %s want %s", i, got[i], wantIDs[i])
		}
	}
	if prog.Plan.Matched != 4 || int64(len(prog.Plan.Skipped)) != 4 {
		t.Fatalf("matched=%d skipped=%d want 4/4", prog.Plan.Matched, len(prog.Plan.Skipped))
	}
	if final.Delivered != 4 {
		t.Fatalf("delivered=%d want 4", final.Delivered)
	}

	// 单侧边界：仅 From（含该时刻），命中 seq3..8。
	rep2 := sink.NewMemorySink()
	p.RegisterReplaySink("rep2", rep2)
	if _, err := p.StartReplay(context.Background(), "rjB2", src, "rep2",
		ReplayRange{From: &from}); err != nil {
		t.Fatal(err)
	}
	f2, err := p.WaitReplay(context.Background(), "rjB2")
	if err != nil {
		t.Fatal(err)
	}
	if f2.Delivered != 6 || rep2.Visible("rep2", src)[0] != "srcBound-e3" {
		t.Fatalf("from-only delivered=%d want 6 from seq3", f2.Delivered)
	}
}

// TestReplayTimeRangeSpansRetainedHistory：区间跨越历史留存（含被清理空洞）
// 且全部数据“很晚才接入落盘”时：命中投递、缺失按 purged 显式标注、
// 范围外逐条说明，命中/跳过/缺失数量互相对得上。
func TestReplayTimeRangeSpansRetainedHistory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	src := "srcSpan"
	early := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	live := sink.NewMemorySink()
	rep := sink.NewMemorySink()
	cfg := replayTestConfig(t, dir, live, rep)
	cfg.Retention = storage.Retention{MaxAge: time.Hour}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.store.SetMaxSegmentBytesForTest(2048)

	const n int64 = 200
	ingestNAt(t, p, src, n, early, time.Second)
	waitFor(t, func() bool { return p.CheckpointWatermark(src) == n }, "wm")
	if err := p.store.SealActiveSegment(src); err != nil {
		t.Fatal(err)
	}

	// 取实际分段布局，动态选一个“中间段”做清理：冻结除它之外的所有段。
	// 封存滚动后尾部会留下一个空活动段（MinSeq=0），不参与选择。
	snap, err := p.store.ReplaySnapshot(src)
	if err != nil {
		t.Fatal(err)
	}
	segs := make([]storage.SegmentMeta, 0, len(snap.Segments))
	for _, sg := range snap.Segments {
		if sg.Records > 0 && sg.MaxSeq > 0 {
			segs = append(segs, sg)
		}
	}
	if len(segs) < 3 {
		t.Fatalf("expected >=3 sealed segments, got %d", len(segs))
	}
	victimIdx := len(segs) / 2
	victim := segs[victimIdx]
	for i, sg := range segs {
		if i == victimIdx {
			continue
		}
		if err := p.AddFreeze(storage.Freeze{
			ID: "fz" + strconv.Itoa(i), Source: src,
			FromSeq: sg.MinSeq, ToSeq: sg.MaxSeq, Until: time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-2 * time.Hour).Unix()
	p.store.AgeSealedSegmentsForTest(src, old)
	res, err := p.PurgeNow(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedSegments != 1 {
		t.Fatalf("deleted segments=%d want exactly 1 (victim %s [%d,%d])",
			res.DeletedSegments, victim.Name, victim.MinSeq, victim.MaxSeq)
	}
	victimCount := victim.MaxSeq - victim.MinSeq + 1

	// 时间区间覆盖全部事件时间：保留段命中，被清理的 victim 段必须在
	// Gaps 里以 purged 标注（不能静默，也不能算成命中/投递）。
	from := early.Add(1 * time.Second)
	to := early.Add(time.Duration(n) * time.Second)
	prog, err := p.StartReplay(context.Background(), "rjSpan", src, "replay-audit",
		ReplayRange{From: &from, To: &to})
	if err != nil {
		t.Fatal(err)
	}
	final, err := p.WaitReplay(context.Background(), "rjSpan")
	if err != nil {
		t.Fatal(err)
	}
	wantMatched := n - victimCount
	if final.Delivered != wantMatched || prog.Plan.Matched != wantMatched {
		t.Fatalf("delivered=%d matched=%d want %d", final.Delivered, prog.Plan.Matched, wantMatched)
	}
	var purged int64
	for _, g := range prog.Plan.Gaps {
		t.Logf("GAP [%d,%d] kind=%s reason=%s", g.FromSeq, g.ToSeq, g.Kind, g.Reason)
		if g.Kind == string(replay.SkipPurged) {
			purged += g.ToSeq - g.FromSeq + 1
		}
	}
	if purged != victimCount {
		t.Fatalf("purged gap seqs=%d want %d (%s)", purged, victimCount, victim.Name)
	}
	if final.Remaining() != 0 {
		t.Fatalf("remaining=%d want 0", final.Remaining())
	}
	got := rep.Visible("replay-audit", src)
	if len(got) != int(wantMatched) {
		t.Fatalf("visible count=%d want %d", len(got), wantMatched)
	}
	// 顺序与集合：1..n 中跳过 victim 段。
	missing := map[int64]bool{}
	for s := victim.MinSeq; s <= victim.MaxSeq; s++ {
		missing[s] = true
	}
	idx := 0
	for seq := int64(1); seq <= n; seq++ {
		if missing[seq] {
			continue
		}
		if got[idx] != src+"-e"+strconv.FormatInt(seq, 10) {
			t.Fatalf("order/set break at seq %d: got %s", seq, got[idx])
		}
		idx++
	}
}
