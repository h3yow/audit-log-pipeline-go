package replay

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"
)

// 同来源并发任务互斥；终态后可再发起；终态任务的保留锁已释放。
func TestEdgeConflictAndHoldCleanup(t *testing.T) {
	h := newHarness(t)
	defer h.store.Close()
	ts0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	h.seed(t, "s9", 4, ts0)
	h.repMs.SetAvailable(false)
	m, err := New(Config{
		Dir:     filepath.Join(h.dir, "replays"),
		KeyFile: filepath.Join(h.dir, "store", "key.bin"),
		Store:   h.store, Sink: h.adapt, FailureRetry: time.Hour,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if _, err := m.Start(Request{ID: "a", Source: "s9", Route: "replay"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	// 同来源第二个运行中任务 → conflict
	if _, err := m.Start(Request{ID: "b", Source: "s9", Route: "replay"}); err != ErrConflict {
		t.Fatalf("conflict err=%v", err)
	}
	// 不同来源无数据 → invalid
	if _, err := m.Start(Request{ID: "c", Source: "other", Route: "replay"}); err != ErrInvalidRequest {
		t.Fatalf("empty source err=%v", err)
	}
	// 取消 a（hold 应释放）
	if err := m.Cancel("a"); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, m, "a", PhaseCanceled)

	// 现在可以为同来源再发起任务；旧锁已释放（AcquireReplayHold 替换而非报错）。
	h.repMs.SetAvailable(true)
	st, err := m.Start(Request{ID: "d", Source: "s9", Route: "replay"})
	if err != nil {
		t.Fatalf("restart after cancel: %v", err)
	}
	_ = st
	waitPhase(t, m, "d", PhaseCompleted)
}
