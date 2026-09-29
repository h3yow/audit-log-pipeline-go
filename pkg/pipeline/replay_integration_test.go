package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/replay"
	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

func replayConfig(t *testing.T) (Config, *sink.MemorySink, *sink.MemorySink) {
	t.Helper()
	cfg, live := testConfig(t)
	rep := sink.NewMemorySink()
	cfg.Sinks["replay"] = rep
	cfg.ReplaySinkName = "replay"
	return cfg, live, rep
}

func waitReplayPhase(t *testing.T, p *Pipeline, id string, want replay.Phase) ReplayStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last ReplayStatus
	for time.Now().Before(deadline) {
		st, err := p.ReplayStatus(id)
		if err == nil {
			last = st
			if st.Phase == want {
				return st
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("replay %s phase=%s want %s (%+v)", id, last.Phase, want, last)
	return last
}

// 端到端：实时投递与历史回放互不影响；两套进度独立。
func TestReplayEndToEndIsolatedProgress(t *testing.T) {
	cfg, live, rep := replayConfig(t)
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	for i := int64(1); i <= 8; i++ {
		if _, err := p.Ingest(ctx, mkEv(fmt.Sprintf("h%d", i), "hy", i, "login")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return p.CheckpointWatermark("hy") == 8 }, "live delivered")

	st, err := p.StartReplay(ReplayRequest{ID: "r1", Source: "hy", FromSeq: 2, ToSeq: 5})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("REPLAY request source=hy seq=[2,5] route=%s resolved=[%d,%d]",
		st.Route, st.FromSeq, st.ToSeq)
	st = waitReplayPhase(t, p, "r1", replay.PhaseCompleted)

	if st.Delivered != 4 || st.Cursor != 5 {
		t.Fatalf("replay status=%+v", st)
	}
	ids := rep.Visible("replay", "hy")
	want := []string{"h2", "h3", "h4", "h5"}
	if len(ids) != 4 {
		t.Fatalf("replay visible=%v", ids)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("order got %v want %v", ids, want)
		}
	}
	// 实时进度不受回放影响，实时下游也不出现回放副本。
	if p.CheckpointWatermark("hy") != 8 {
		t.Fatalf("live watermark drifted: %d", p.CheckpointWatermark("hy"))
	}
	if n := len(live.Visible("audit", "hy")); n != 8 {
		t.Fatalf("live visible=%d", n)
	}
	remaining := ReplayRemaining(st)
	t.Logf("REPLAY progress: phase=%s cursor=%d/%d delivered=%d remaining=%d retries=%d",
		st.Phase, st.Cursor, st.ToSeq, st.Delivered, remaining, st.Retries)
	if remaining != 0 {
		t.Fatalf("remaining=%d", remaining)
	}

	// 进度可列。
	list, err := p.ListReplays()
	if err != nil || len(list) != 1 {
		t.Fatalf("list=%v err=%v", list, err)
	}
}

// 回放期间清理被保留锁挡住；冻结/解冻与回放过期间不丢不乱。
func TestReplayConcurrentPurgeAndFreeze(t *testing.T) {
	cfg, _, rep := replayConfig(t)
	cfg.Retention = storage.Retention{MaxAge: time.Millisecond} // 保留期极短
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	// 强制每 ~2 条滚动一个分段，制造多个已封存分段。
	p.store.SetMaxSegmentBytesForTest(200)
	const n = 40
	for i := int64(1); i <= n; i++ {
		e := mkEv(fmt.Sprintf("c%d", i), "cp", i, "login")
		e.Timestamp = time.Now().UTC()
		if _, err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return p.CheckpointWatermark("cp") == n }, "live drained")

	// 冻结整个回放区间（双重保护演示：冻结 + 回放保留锁）。
	if err := p.AddFreeze(storage.Freeze{
		ID: "rep-freeze", Source: "cp", FromSeq: 1, ToSeq: int64(n),
		Until: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	st, err := p.StartReplay(ReplayRequest{ID: "big", Source: "cp"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("REPLAY big range resolved=[%d,%d]", st.FromSeq, st.ToSeq)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			res, _ := p.PurgeNow(time.Now())
			if res.DeletedSegments != 0 {
				t.Errorf("segment purged while replay in progress: %+v", res)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	st = waitReplayPhase(t, p, "big", replay.PhaseCompleted)
	close(stop)
	wg.Wait()

	if st.Delivered != n {
		t.Fatalf("delivered=%d want %d (status=%+v)", st.Delivered, n, st)
	}
	if got := len(rep.Visible("replay", "cp")); got != n {
		t.Fatalf("replay visible=%d want %d", got, n)
	}
	t.Logf("REPLAY under purge/freeze: delivered=%d cursor=%d skipped=%+v retries=%d",
		st.Delivered, st.Cursor, st.Skipped, st.Retries)

	// 解冻并释放回放保留锁（回放已完成自动释放）后，清理应可推进。
	if err := p.RemoveFreeze("rep-freeze"); err != nil {
		t.Fatal(err)
	}
	p.store.AgeSealedSegmentsForTest("cp", time.Now().Add(-2*time.Hour).Unix())
	res, err := p.PurgeNow(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PURGE after replay+unfreeze: deleted=%d skipped_frozen=%d skipped_replay=%d",
		res.DeletedSegments, res.SkippedFrozen, res.SkippedReplay)
	// 活动分段永不删；旧的已封存分段应被清掉。
	if res.DeletedSegments == 0 {
		t.Fatalf("expected cleanup after replay completion")
	}
}

// 回放下游故障 + 进程重启：续传不重不漏，实时投递继续不受影响。
func TestReplayRestartResumes(t *testing.T) {
	cfg, live, rep := replayConfig(t)
	cfg.Dir = t.TempDir() + "/node"

	p1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := int64(1); i <= 10; i++ {
		if _, err := p1.Ingest(ctx, mkEv(fmt.Sprintf("k%d", i), "kr", i, "login")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return p1.CheckpointWatermark("kr") == 10 }, "live 10")

	rep.SetAvailable(false)
	if _, err := p1.StartReplay(ReplayRequest{ID: "rr1", Source: "kr"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	st1, _ := p1.ReplayStatus("rr1")
	t.Logf("REPLAY before crash: phase=%s cursor=%d retries=%d",
		st1.Phase, st1.Cursor, st1.Retries)
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启（同一目录；回放下游恢复）。
	rep.SetAvailable(true)
	p2, err := New(cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer p2.Close()
	st2 := waitReplayPhase(t, p2, "rr1", replay.PhaseCompleted)
	if st2.Delivered != 10 || st2.Cursor != 10 {
		t.Fatalf("resumed replay=%+v", st2)
	}
	ids := rep.Visible("replay", "kr")
	if len(ids) != 10 {
		t.Fatalf("visible=%v", ids)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate replay visible: %s", id)
		}
		seen[id] = true
	}
	// 实时检查点仍是 10，未被回放进度带动。
	if p2.CheckpointWatermark("kr") != 10 {
		t.Fatalf("live watermark after restart=%d", p2.CheckpointWatermark("kr"))
	}
	if n := len(live.Visible("audit", "kr")); n != 10 {
		t.Fatalf("live visible=%d", n)
	}
	t.Logf("REPLAY after restart: cursor=%d delivered=%d unique=%d live_wm=%d",
		st2.Cursor, st2.Delivered, len(ids), p2.CheckpointWatermark("kr"))
}

var _ = event.DecisionAccepted
