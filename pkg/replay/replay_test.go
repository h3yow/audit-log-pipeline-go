package replay

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// ---- 测试夹具 ----

type harness struct {
	dir   string
	store *storage.Store
	ms    *sink.MemorySink
	repMs *sink.MemorySink
	adapt *SinkAdapter
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "node")
	st, err := storage.Open(storage.Options{
		Dir:       filepath.Join(dir, "store"),
		Retention: storage.Retention{MaxAge: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	ms := sink.NewMemorySink()
	repMs := sink.NewMemorySink()
	adapt := NewSinkAdapter(map[string]sink.Sink{
		"audit":  ms,
		"replay": repMs,
	})
	return &harness{dir: dir, store: st, ms: ms, repMs: repMs, adapt: adapt}
}

func (h *harness) seed(t *testing.T, source string, n int, ts0 time.Time) {
	t.Helper()
	for i := 1; i <= n; i++ {
		e := &event.Event{
			ID: fmt.Sprintf("%s-e%d", source, i), Source: source,
			Seq: int64(i), Type: "login", Action: "do",
			Timestamp: ts0.Add(time.Duration(i) * time.Minute),
		}
		if err := h.store.Append(storage.Record{
			Event: e, Route: "audit", StoredAt: ts0.Unix() + int64(i),
			State: storage.StatePending,
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
}

// seedRejected 在连续序号流中插入一条 rejected 留存记录。
func (h *harness) seedRejected(t *testing.T, source string, seq int, ts0 time.Time) {
	e := &event.Event{
		ID: fmt.Sprintf("%s-e%d", source, seq), Source: source,
		Seq: int64(seq), Type: "debug", Action: "do",
		Timestamp: ts0.Add(time.Duration(seq) * time.Minute),
	}
	if err := h.store.Append(storage.Record{
		Event: e, StoredAt: ts0.Unix() + int64(seq),
		State: storage.StateRejected, Decision: "deny-debug",
	}); err != nil {
		t.Fatal(err)
	}
}

func (h *harness) newManager(t *testing.T) *Manager {
	t.Helper()
	m, err := New(Config{
		Dir:          filepath.Join(h.dir, "replays"),
		KeyFile:      filepath.Join(h.dir, "store", "key.bin"),
		Store:        h.store,
		Sink:         h.adapt,
		FailureRetry: 20 * time.Millisecond,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func waitPhase(t *testing.T, m *Manager, id string, want Phase) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last Status
	for time.Now().Before(deadline) {
		st, err := m.Status(id)
		if err == nil {
			last = st
			if st.Phase == want {
				t.Logf("RANGE job=%s resolved=[%d,%d] -> phase=%s cursor=%d delivered=%d retries=%d skipped=%+v",
					id, st.FromSeq, st.ToSeq, st.Phase, st.Cursor,
					st.Delivered, st.Retries, st.Skipped)
				return st
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s phase=%s, want %s (status=%+v)", id, last.Phase, want, last)
	return Status{}
}

func waitPhasePtr(t *testing.T, m *Manager, id string, want Phase) *Status {
	st := waitPhase(t, m, id, want)
	return &st
}

func waitVisible(t *testing.T, h *harness, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.repMs.Visible("replay", "s1")) >= want ||
			len(h.repMs.Visible("replay", "s3")) >= want {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// ---- 用例 1：回放顺序与范围边界 ----

func TestReplayOrderAndRangeBoundaries(t *testing.T) {
	h := newHarness(t)
	defer h.store.Close()
	ts0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	h.seed(t, "s1", 10, ts0)

	m := h.newManager(t)
	defer m.Close()

	// 闭区间 [4,7]：起终点都包含，顺序与接入顺序一致。
	st, err := m.Start(Request{
		ID: "j1", Source: "s1", Route: "replay",
		FromSeq: 4, ToSeq: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("REQUEST j1 seq=[4,7] resolved=[%d,%d]", st.FromSeq, st.ToSeq)
	st = waitPhasePtr(t, m, "j1", PhaseCompleted)

	ids := h.repMs.Visible("replay", "s1")
	want := []string{"s1-e4", "s1-e5", "s1-e6", "s1-e7"}
	if len(ids) != len(want) {
		t.Fatalf("replayed ids=%v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("replay order mismatch: got %v want %v", ids, want)
		}
	}
	if st.Delivered != 4 || st.Cursor != 7 {
		t.Fatalf("progress delivered=%d cursor=%d", st.Delivered, st.Cursor)
	}

	// 开放上界：ToSeq=0 解析为启动时最大序号 10。
	st2, err := m.Start(Request{ID: "j2", Source: "s1", Route: "replay", FromSeq: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("REQUEST j2 open-upper from=8 resolved=[%d,%d]", st2.FromSeq, st2.ToSeq)
	st2fin := waitPhase(t, m, "j2", PhaseCompleted)
	st2 = &st2fin
	if st2.ToSeq != 10 || st2.Delivered != 3 || st2.Cursor != 10 {
		t.Fatalf("open upper: %+v", st2)
	}

	// 时间闭区间：只有部分命中（事件时间 = ts0 + seq*min）。
	from := ts0.Add(3*time.Minute - time.Second)
	to := ts0.Add(5*time.Minute + time.Second)
	st3, err := m.Start(Request{
		ID: "j3", Source: "s1", Route: "replay",
		FromTime: from, ToTime: to,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("REQUEST j3 time=[%s,%s] resolved seq=[%d,%d]",
		from.Format(time.RFC3339), to.Format(time.RFC3339),
		st3.FromSeq, st3.ToSeq)
	st3fin := waitPhase(t, m, "j3", PhaseCompleted)
	st3 = &st3fin
	if st3.Delivered != 3 {
		t.Fatalf("time-window delivered=%d, want 3", st3.Delivered)
	}
	if st3.Skipped.BeforeTime+st3.Skipped.AfterTime != 7 {
		t.Fatalf("time skips=%+v, want total 7", st3.Skipped)
	}
	// 三次回放并集：e3..e10（j1 的 4-7、j2 的 8-10、j3 时间窗 3-5）= 8 条唯一。
	if got := h.repMs.Visible("replay", "s1"); len(got) != 8 {
		t.Fatalf("cumulative replay visible=%v", got)
	}
	// 实时下游完全不被回放污染。
	if n := h.ms.VisibleCount(); n != 0 {
		t.Fatalf("live sink contaminated by replay: %d", n)
	}

	// 同 ID 重复提交必须显式拒绝。
	if _, err := m.Start(Request{ID: "j1", Source: "s1", Route: "replay"}); !errors.Is(err, ErrJobExists) {
		t.Fatalf("duplicate job id err=%v", err)
	}
	// 未知路由 fail-closed。
	if _, err := m.Start(Request{ID: "jx", Source: "s1", Route: "nope"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unknown route err=%v", err)
	}
}

// rejected 留存事件不回放，计入 skipped.rejected，顺序中不产生空洞投递。
func TestReplaySkipsRejectedWithExplanation(t *testing.T) {
	h := newHarness(t)
	defer h.store.Close()
	ts0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	h.seed(t, "s1", 3, ts0)
	h.seedRejected(t, "s1", 4, ts0)
	seedRange(t, h, "s1", 5, 6, ts0)

	m := h.newManager(t)
	defer m.Close()
	st, err := m.Start(Request{ID: "rj", Source: "s1", Route: "replay"})
	if err != nil {
		t.Fatal(err)
	}
	st = waitPhasePtr(t, m, "rj", PhaseCompleted)
	if st.Delivered != 5 || st.Skipped.Rejected != 1 {
		t.Fatalf("delivered=%d skipped=%+v", st.Delivered, st.Skipped)
	}
	ids := h.repMs.Visible("replay", "s1")
	want := []string{"s1-e1", "s1-e2", "s1-e3", "s1-e5", "s1-e6"}
	if len(ids) != len(want) {
		t.Fatalf("replayed=%v want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("order: got %v want %v", ids, want)
		}
	}
}

func seedRange(t *testing.T, h *harness, source string, from, to int, ts0 time.Time) {
	for i := from; i <= to; i++ {
		e := &event.Event{
			ID: fmt.Sprintf("%s-e%d", source, i), Source: source,
			Seq: int64(i), Type: "login", Action: "do",
			Timestamp: ts0.Add(time.Duration(i) * time.Minute),
		}
		if err := h.store.Append(storage.Record{
			Event: e, Route: "audit", StoredAt: ts0.Unix() + int64(i),
			State: storage.StatePending,
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
}

// ---- 用例 2：投递中途"进程崩溃"，重启接着上次位置续，不重不漏 ----

func TestReplayResumeAcrossCrash(t *testing.T) {
	h := newHarness(t)
	defer h.store.Close()
	ts0 := time.Date(2026, 6, 2, 8, 0, 0, 0, time.UTC)
	h.seed(t, "s3", 20, ts0)
	rpDir := filepath.Join(h.dir, "replays")

	// 下游先不可用：任务启动后零投递、进度不动。
	h.repMs.SetAvailable(false)
	m1 := h.newManager(t)
	if _, err := m1.Start(Request{ID: "resume", Source: "s3", Route: "replay"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	st, _ := m1.Status("resume")
	if st.Cursor != 0 || st.Delivered != 0 {
		t.Fatalf("progress moved during outage: %+v", st)
	}
	t.Logf("DURING outage: phase=%s cursor=%d delivered=%d retries=%d",
		st.Phase, st.Cursor, st.Delivered, st.Retries)
	if st.Retries == 0 {
		t.Fatalf("expected retry attempts recorded")
	}
	if err := m1.Close(); err != nil { // 模拟进程崩溃（任务未完成）
		t.Fatal(err)
	}
	if n := len(h.repMs.Visible("replay", "s3")); n != 0 {
		t.Fatalf("visible during outage = %d", n)
	}

	// 重启进程：下游恢复后从 cursor+1 继续，最终恰好投递 20 条且无重复。
	h.repMs.SetAvailable(true)
	m2, err := New(Config{
		Dir: rpDir, KeyFile: filepath.Join(h.dir, "store", "key.bin"),
		Store: h.store, Sink: h.adapt, FailureRetry: 20 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer m2.Close()
	st2 := waitPhase(t, m2, "resume", PhaseCompleted)
	if st2.Cursor != 20 || st2.Delivered != 20 {
		t.Fatalf("resumed job: %+v", st2)
	}
	ids := h.repMs.Visible("replay", "s3")
	if len(ids) != 20 {
		t.Fatalf("visible=%d, want 20", len(ids))
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate visible after resume: %s", id)
		}
		seen[id] = true
	}
	t.Logf("AFTER crash-resume: cursor=%d delivered=%d visible=%d",
		st2.Cursor, st2.Delivered, len(ids))
}

// ---- 用例 3：运行中下游故障，原地重试，恢复后不产生重复可见 ----

func TestReplayDownstreamFailureRetriesNoDuplicate(t *testing.T) {
	h := newHarness(t)
	defer h.store.Close()
	ts0 := time.Date(2026, 6, 4, 8, 0, 0, 0, time.UTC)
	h.seed(t, "s1", 12, ts0)

	m := h.newManager(t)
	defer m.Close()
	if _, err := m.Start(Request{ID: "flaky", Source: "s1", Route: "replay"}); err != nil {
		t.Fatal(err)
	}
	// 先放 5 条，再故障，再恢复：失败期间进度停在故障序号上。
	waitCount := func(want int) Status {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if len(h.repMs.Visible("replay", "s1")) >= want {
				st, _ := m.Status("flaky")
				return st
			}
			time.Sleep(2 * time.Millisecond)
		}
		t.Fatalf("never reached %d visible", want)
		return Status{}
	}
	_ = waitCount(5)
	frozen := h.repMs.Visible("replay", "s1")
	t.Logf("PRE-FAILURE visible=%v", frozen)

	h.repMs.SetAvailable(false)
	time.Sleep(200 * time.Millisecond)
	st, _ := m.Status("flaky")
	if st.Phase != PhaseRunning {
		t.Fatalf("phase during outage=%s", st.Phase)
	}
	t.Logf("DURING outage: cursor=%d retries=%d (must retry same seq, no skip)",
		st.Cursor, st.Retries)
	if st.Retries == 0 {
		t.Fatalf("retries not counted")
	}
	h.repMs.SetAvailable(true)
	st = waitPhase(t, m, "flaky", PhaseCompleted)
	if st.Delivered != 12 || st.Cursor != 12 {
		t.Fatalf("completed=%+v", st)
	}
	ids := h.repMs.Visible("replay", "s1")
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate visible: %s", id)
		}
		seen[id] = true
	}
	t.Logf("POST-RECOVERY delivered=%d retries=%d unique_visible=%d",
		st.Delivered, st.Retries, len(ids))
}

// ---- 用例 4：进度被篡改/倒退时明确拒绝 ----

func TestReplayTamperedProgressRejected(t *testing.T) {
	h := newHarness(t)
	defer h.store.Close()
	ts0 := time.Date(2026, 6, 3, 8, 0, 0, 0, time.UTC)
	h.seed(t, "s4", 5, ts0)
	rpDir := filepath.Join(h.dir, "replays")

	m := h.newManager(t)
	if _, err := m.Start(Request{ID: "rb", Source: "s4", Route: "replay"}); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, m, "rb", PhaseCompleted)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// 把游标改小且不改 MAC：自校验必须失败（旧副本/倒退一律拒绝）。
	p := filepath.Join(rpDir, rpName("rb"))
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatal(err)
	}
	st := env["state"].(map[string]any)
	st["cursor"] = float64(1)
	nb, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, nb, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{
		Dir: rpDir, KeyFile: filepath.Join(h.dir, "store", "key.bin"),
		Store: h.store, Sink: h.adapt,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}); !errors.Is(err, ErrRollback) {
		t.Fatalf("tampered cursor accepted, err=%v", err)
	}
	t.Logf("TAMPER/rollback rejected: %v", ErrRollback)
}

// 区间命中已被清理的分段：任务 failed 且错误解释缺口，绝不静默跳过。
func TestReplayRangeHitsPurgedGap(t *testing.T) {
	h := newHarness(t)
	ts0 := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	h.seed(t, "s5", 6, ts0)
	// 强制滚动出多段，再旧化并清理中间段。
	h.store.SetMaxSegmentBytesForTest(1)
	e := &event.Event{
		ID: "s5-e7", Source: "s5", Seq: 7, Type: "login", Action: "do",
		Timestamp: ts0.Add(7 * time.Minute),
	}
	if err := h.store.Append(storage.Record{
		Event: e, Route: "audit", StoredAt: ts0.Unix() + 7,
		State: storage.StatePending,
	}); err != nil {
		t.Fatal(err)
	}
	h.store.AgeSealedSegmentsForTest("s5", time.Now().Add(-2*time.Hour).Unix())
	res, err := h.store.Purge(time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PURGE deleted=%d skipped=%+v before replay", res.DeletedSegments, res)
	if res.DeletedSegments == 0 {
		t.Fatalf("test setup: nothing purged")
	}

	m := h.newManager(t)
	defer m.Close()
	defer h.store.Close()
	st, err := m.Start(Request{ID: "gap", Source: "s5", Route: "replay"})
	if err != nil {
		t.Fatal(err)
	}
	st = waitPhasePtr(t, m, "gap", PhaseFailed)
	if st.Skipped.Missing == 0 || st.LastError == "" {
		t.Fatalf("gap not explained: %+v", st)
	}
	t.Logf("GAP phase=%s cursor=%d missing=%d err=%q",
		st.Phase, st.Cursor, st.Skipped.Missing, st.LastError)
}

// 旧版本进度文件被换回复刻：锚点记录的是更新版本 MAC，必须拒绝。
func TestReplayOldCopyRestoredRejected(t *testing.T) {
	h := newHarness(t)
	defer h.store.Close()
	ts0 := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	h.seed(t, "s6", 6, ts0)
	rpDir := filepath.Join(h.dir, "replays")

	m := h.newManager(t)
	if _, err := m.Start(Request{ID: "oc", Source: "s6", Route: "replay"}); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, m, "oc", PhaseCompleted)
	// 记录"当前版本"的状态文件内容（完成版）。
	curPath := filepath.Join(rpDir, rpName("oc"))
	completedBytes, _ := os.ReadFile(curPath)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// 用一个"更早版本"替换状态文件：构造首版（cursor=0）的合法自签名状态，
	// 但其 PrevMAC 不是锚点的直接前驱 → 必须 ErrRollback。
	now := time.Now()
	old := progressState{
		JobID: "oc", Source: "s6", Route: "replay",
		FromSeq: 1, ToSeq: 6, Phase: PhaseRunning,
		StartedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	}
	ps, err := newProgressStore(rpDir, mustKey(t, h))
	if err != nil {
		t.Fatal(err)
	}
	old.PrevMAC = genesisMAC("oc")
	env := rpEnvelope{State: old, MAC: ps.stateMAC(old.PrevMAC, old)}
	nb, _ := jsonMarshalIndent(env)
	if err := os.WriteFile(curPath, nb, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{
		Dir: rpDir, KeyFile: filepath.Join(h.dir, "store", "key.bin"),
		Store: h.store, Sink: h.adapt,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}); !errors.Is(err, ErrRollback) {
		t.Fatalf("old-copy restore accepted, err=%v", err)
	}
	t.Logf("OLD-COPY restore rejected: %v", ErrRollback)

	// 恢复完成版文件后可正常打开（证明拒绝的是回退本身，而非目录损坏）。
	if err := os.WriteFile(curPath, completedBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	m2 := h.newManager(t)
	defer m2.Close()
	st := waitPhase(t, m2, "oc", PhaseCompleted)
	if st.Cursor != 6 {
		t.Fatalf("completed job not restored: %+v", st)
	}
}

// 显式 Cancel 落盘 canceled 终态，重启后不再自动续跑；已投递不回退。
func TestReplayCancelIsTerminal(t *testing.T) {
	h := newHarness(t)
	defer h.store.Close()
	ts0 := time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC)
	h.seed(t, "s7", 8, ts0)

	m := h.newManager(t)
	if _, err := m.Start(Request{ID: "cc1", Source: "s7", Route: "replay", FromSeq: 1, ToSeq: 8}); err != nil {
		t.Fatal(err)
	}
	// 下游不可用让任务停在重试中，再取消。
	h.repMs.SetAvailable(false)
	time.Sleep(100 * time.Millisecond)
	if err := m.Cancel("cc1"); err != nil {
		t.Fatal(err)
	}
	st := waitPhase(t, m, "cc1", PhaseCanceled)
	t.Logf("CANCELED: cursor=%d delivered=%d retries=%d", st.Cursor, st.Delivered, st.Retries)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：canceled 是终态，任务保持 canceled，不自动恢复。
	m2 := h.newManager(t)
	defer m2.Close()
	h.repMs.SetAvailable(true)
	time.Sleep(150 * time.Millisecond)
	st2, err := m2.Status("cc1")
	if err != nil {
		t.Fatal(err)
	}
	if st2.Phase != PhaseCanceled {
		t.Fatalf("phase after restart=%s", st2.Phase)
	}
}

func mustKey(t *testing.T, h *harness) []byte {
	t.Helper()
	k, err := os.ReadFile(filepath.Join(h.dir, "store", "key.bin"))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func jsonMarshalIndent(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}
