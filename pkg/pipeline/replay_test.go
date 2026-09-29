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
	"github.com/h3yow/audit-log-pipeline-go/pkg/router"
	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

func replayTestConfig(t *testing.T, dir string, live, rep sink.Sink) Config {
	t.Helper()
	r := router.New(nil, "audit")
	return Config{
		Dir:           dir,
		Router:        r,
		Sinks:         map[string]sink.Sink{"audit": live, "default": live},
		ReplaySinks:   map[string]sink.Sink{"replay-audit": rep},
		Retention:     storage.Retention{MaxAge: time.Hour},
		PurgeInterval: time.Hour,
		MaxInFlight:   64,
		Logger:        testLogger(t),
	}
}

// ingestN 接入 src 的 1..n 条 login 事件（时间戳 2026-01-01 起每秒一条）。
func ingestN(t *testing.T, p *Pipeline, src string, n int64) {
	t.Helper()
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := int64(1); i <= n; i++ {
		e := &event.Event{
			ID: src + "-e" + strconv.FormatInt(i, 10), Source: src, Seq: i, Type: "login",
			Subject: "alice", Action: "do", Resource: "c",
			Timestamp: base.Add(time.Duration(i) * time.Second),
		}
		if _, err := p.Ingest(ctx, e); err != nil {
			t.Fatalf("ingest %s %d: %v", src, i, err)
		}
	}
}

// TestReplayEndToEndRangeAndIndependence：真实存储上回放一个序号区间，
// 验证顺序、独立下游、实时检查点水位不受回放影响。
func TestReplayEndToEndRangeAndIndependence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	live := sink.NewMemorySink()
	rep := sink.NewMemorySink()
	p, err := New(replayTestConfig(t, dir, live, rep))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ingestN(t, p, "srcA", 12)
	waitFor(t, func() bool { return p.CheckpointWatermark("srcA") == 12 }, "live watermark 12")

	from := time.Date(2026, 1, 1, 0, 0, 4, 0, time.UTC)
	to := time.Date(2026, 1, 1, 0, 0, 8, 0, time.UTC)
	prog, err := p.StartReplay(context.Background(), "rj1", "srcA", "replay-audit",
		ReplayRange{FromSeq: 3, ToSeq: 10, From: &from, To: &to})
	if err != nil {
		t.Fatal(err)
	}
	final, err := p.WaitReplay(context.Background(), "rj1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("REPLAY REQ source=srcA seq[3,10] time[%s,%s]", from.Format(time.RFC3339), to.Format(time.RFC3339))
	t.Logf("PLAN eff=[%d,%d] matched=%d gaps=%d skipped=%d",
		prog.Plan.EffFromSeq, prog.Plan.EffToSeq, prog.Plan.Matched, len(prog.Plan.Gaps), len(prog.Plan.Skipped))
	for _, s := range prog.Plan.Skipped {
		t.Logf("  SKIP seq=%d kind=%s", s.Seq, s.Kind)
	}
	t.Logf("PROGRESS status=%s delivered=%d remaining=%d retries=%d",
		final.Status, final.Delivered, final.Remaining(), final.Retries)

	// 序号 3..10 ∩ 时间 4..8 秒 => seq 4,5,6,7,8
	got := rep.Visible("replay-audit", "srcA")
	wantIDs := []string{"srcA-e4", "srcA-e5", "srcA-e6", "srcA-e7", "srcA-e8"}
	if len(got) != len(wantIDs) {
		t.Fatalf("replay visible=%v want=%v", got, wantIDs)
	}
	for i := range wantIDs {
		if got[i] != wantIDs[i] {
			t.Fatalf("replay order %d: %v want %v", i, got, wantIDs)
		}
	}
	// 实时下游看到的是全部 12 条；回放目标与实时目标是两个独立 MemorySink。
	if n := len(live.Visible("audit", "srcA")); n != 12 {
		t.Fatalf("live visible=%d want 12", n)
	}
	// 回放进度不得回带实时检查点。
	if wm := p.CheckpointWatermark("srcA"); wm != 12 {
		t.Fatalf("live watermark affected by replay: %d", wm)
	}
	// 实时投递的事件不会因为回放而二次出现在实时下游。
	if rep.VisibleCount() != 5 {
		t.Fatalf("replay sink count=%d want 5", rep.VisibleCount())
	}
}

// TestReplayHoldBlocksPurge：活跃回放占位期间，保留清理不得删除范围内分段；
// 回放完成后占位释放，清理恢复生效。
func TestReplayHoldBlocksPurge(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	live := sink.NewMemorySink()
	rep := sink.NewMemorySink()
	cfg := replayTestConfig(t, dir, live, rep)
	cfg.Retention = storage.Retention{MaxAge: time.Hour}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ingestN(t, p, "srcB", 20)
	waitFor(t, func() bool { return p.CheckpointWatermark("srcB") == 20 }, "wm 20")

	// 封存当前段并旧化：形成“已封存且已过保留期”的清理候选。
	if err := p.store.SealActiveSegment("srcB"); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour).Unix()
	p.store.AgeSealedSegmentsForTest("srcB", old)

	// 回放下游先不可用：任务持续重试，保持活跃占位。
	rep.SetAvailable(false)
	if _, err := p.StartReplay(context.Background(), "rj2", "srcB", "replay-audit",
		ReplayRange{FromSeq: 1, ToSeq: 20}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		pr, _ := p.ReplayProgress("rj2")
		return pr.Status == replay.StatusRunning && pr.Retries > 0
	}, "replay running and retrying")

	now := time.Now()
	res, err := p.PurgeNow(now)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PURGE DURING REPLAY deleted=%d skipped_frozen=%d skipped_replay=%d",
		res.DeletedSegments, res.SkippedFrozen, res.SkippedReplay)
	if res.DeletedSegments != 0 {
		t.Fatalf("purge deleted %d segments under active replay", res.DeletedSegments)
	}
	if res.SkippedReplay == 0 {
		t.Fatalf("expected segments protected by replay hold")
	}
	// 范围内事件仍可读，回放不会读到半清理数据。
	rv, err := p.store.Get("srcB", 10)
	if err != nil || rv.Seq != 10 {
		t.Fatalf("protected record unreadable: %v", err)
	}
	// 下游恢复后回放完成；占位释放，再清理时到期段可以删除。
	rep.SetAvailable(true)
	final, err := p.WaitReplay(context.Background(), "rj2")
	if err != nil {
		t.Fatal(err)
	}
	if final.Delivered != 20 || final.Status != replay.StatusComplete {
		t.Fatalf("final=%+v", final)
	}
	res2, err := p.PurgeNow(now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PURGE AFTER COMPLETE deleted=%d skipped_replay=%d",
		res2.DeletedSegments, res2.SkippedReplay)
	if res2.SkippedReplay != 0 {
		t.Fatalf("hold not released: skipped_replay=%d", res2.SkippedReplay)
	}
	if res2.DeletedSegments == 0 {
		t.Fatalf("expired segment not purged after replay finished")
	}
}

// TestReplayRestartResumes：回放进行中关闭并重启 Pipeline，
// 回放从持久化进度继续，下游无重复可见，实时投递也正常恢复。
func TestReplayRestartResumes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	live1 := sink.NewMemorySink()
	rep1 := sink.NewMemorySink()
	p1, err := New(replayTestConfig(t, dir, live1, rep1))
	if err != nil {
		t.Fatal(err)
	}
	ingestN(t, p1, "srcC", 30)
	waitFor(t, func() bool { return p1.CheckpointWatermark("srcC") == 30 }, "wm 30")
	if _, err := p1.StartReplay(context.Background(), "rj3", "srcC", "replay-audit",
		ReplayRange{FromSeq: 1, ToSeq: 30}); err != nil {
		t.Fatal(err)
	}
	fin, err := p1.WaitReplay(context.Background(), "rj3")
	if err != nil {
		t.Fatal(err)
	}
	if fin.Delivered != 30 {
		t.Fatalf("first replay delivered=%d", fin.Delivered)
	}
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：已完成回放不得重放；实时水位也保持 30。
	live2 := sink.NewMemorySink()
	rep2 := sink.NewMemorySink()
	p2, err := New(replayTestConfig(t, dir, live2, rep2))
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	progs, _ := p2.AllReplays()
	if len(progs) != 1 || progs[0].Status != replay.StatusComplete {
		t.Fatalf("after restart replays=%+v", progs)
	}
	time.Sleep(100 * time.Millisecond)
	if rep2.VisibleCount() != 0 {
		t.Fatalf("completed replay redelivered after restart: %d", rep2.VisibleCount())
	}
	if wm := p2.CheckpointWatermark("srcC"); wm != 30 {
		t.Fatalf("live watermark after restart=%d", wm)
	}
}

// TestReplayRetriesNoDuplicateVisible：回放下游故障窗口内重试，
// 恢复后无重复可见事件，进度 retries 可观测。
func TestReplayRetriesNoDuplicateVisible(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	live := sink.NewMemorySink()
	rep := sink.NewMemorySink()
	p, err := New(replayTestConfig(t, dir, live, rep))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ingestN(t, p, "srcD", 8)
	waitFor(t, func() bool { return p.CheckpointWatermark("srcD") == 8 }, "wm 8")

	rep.SetAvailable(false)
	if _, err := p.StartReplay(context.Background(), "rj4", "srcD", "replay-audit",
		ReplayRange{FromSeq: 1, ToSeq: 8}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		pr, _ := p.ReplayProgress("rj4")
		return pr.Retries > 0
	}, "replay retries recorded")
	rep.SetAvailable(true)
	final, err := p.WaitReplay(context.Background(), "rj4")
	if err != nil {
		t.Fatal(err)
	}
	got := rep.Visible("replay-audit", "srcD")
	t.Logf("REPLAY AFTER OUTAGE delivered=%d retries=%d visible=%v",
		final.Delivered, final.Retries, got)
	if len(got) != 8 {
		t.Fatalf("visible=%v want 8", got)
	}
	if final.Delivered != 8 {
		t.Fatalf("delivered=%d", final.Delivered)
	}
	if final.Retries == 0 {
		t.Fatalf("retries not observed")
	}
}

// TestReplayConcurrentWithIngestPurgeFreeze（-race）：
// 大批量回放进行中，并发接入新事件（触发分段滚动）、并发清理、并发加/解冻，
// 回放顺序与数量正确、不读到半清理数据、全程无数据竞争。
func TestReplayConcurrentWithIngestPurgeFreeze(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	live := sink.NewMemorySink()
	rep := sink.NewMemorySink()
	cfg := replayTestConfig(t, dir, live, rep)
	cfg.Retention = storage.Retention{MaxAge: time.Hour}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ingestN(t, p, "srcE", 200)
	waitFor(t, func() bool { return p.CheckpointWatermark("srcE") == 200 }, "wm 200")

	// 回放 1..150；启动后并发做三件事。
	rep.SetAvailable(false)
	if _, err := p.StartReplay(context.Background(), "rjC", "srcE", "replay-audit",
		ReplayRange{FromSeq: 1, ToSeq: 150}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		pr, _ := p.ReplayProgress("rjC")
		return pr.Status == replay.StatusRunning
	}, "replay running")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 1) 并发接入新事件（201..230），触发活动段增长。
	wg.Add(1)
	go func() {
		defer wg.Done()
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		for i := int64(201); i <= 230; i++ {
			select {
			case <-stop:
				return
			default:
			}
			e := &event.Event{
				ID: "srcE-late" + strconv.FormatInt(i, 10), Source: "srcE", Seq: i,
				Type: "login", Action: "do", Resource: "c",
				Timestamp: base.Add(time.Duration(i) * time.Second),
			}
			if _, err := p.Ingest(context.Background(), e); err != nil {
				t.Errorf("concurrent ingest: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	// 2) 并发清理（保留期 1h，刚写入的段不满足；仅产生锁竞争）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := p.PurgeNow(time.Now()); err != nil {
				t.Errorf("concurrent purge: %v", err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	// 3) 并发加/解冻（与回放范围相交）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			id := "freeze-" + strconv.Itoa(i)
			if err := p.AddFreeze(storage.Freeze{
				ID: id, Source: "srcE", FromSeq: 1, ToSeq: 150,
				Until: time.Now().Add(time.Minute),
			}); err != nil {
				t.Errorf("add freeze: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
			if err := p.RemoveFreeze(id); err != nil {
				t.Errorf("remove freeze: %v", err)
				return
			}
		}
	}()

	// 让并发操作跑一会儿再恢复回放下游。
	time.Sleep(30 * time.Millisecond)
	rep.SetAvailable(true)
	final, err := p.WaitReplay(context.Background(), "rjC")
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	got := rep.Visible("replay-audit", "srcE")
	t.Logf("CONCURRENT REPLAY delivered=%d/%d retries=%d visible=%d",
		final.Delivered, final.Plan.Matched, final.Retries, len(got))
	if len(got) != 150 {
		t.Fatalf("replay visible=%d want 150", len(got))
	}
	// 顺序严格 1..150
	for i, id := range got {
		want := "srcE-e" + strconv.Itoa(i+1)
		if id != want {
			t.Fatalf("order break at %d: got %s want %s", i, id, want)
		}
	}
	// 计划是静态快照：回放结束后才接入的新事件不属于本任务（NextSeq 越过 effTo）。
	if final.NextSeq != 151 {
		t.Fatalf("next_seq=%d want 151 (snapshot must not include late events)", final.NextSeq)
	}
	if err := p.store.Verify(); err != nil {
		t.Fatalf("storage verify after concurrency: %v", err)
	}
}
