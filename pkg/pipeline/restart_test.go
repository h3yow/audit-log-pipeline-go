package pipeline

import (
	"context"
	"testing"
	"time"
)

// TestRestartResumesPending: 故障期间关闭进程，重启后未完成事件继续投递。
func TestRestartResumesPending(t *testing.T) {
	cfg, ms := testConfig(t)

	p1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ms.SetAvailable(false)
	for i := int64(1); i <= 4; i++ {
		_, _ = p1.Ingest(ctx, mkEv("r"+seqStr(i), "s3", i, "login"))
	}
	time.Sleep(150 * time.Millisecond)
	if wm := p1.CheckpointWatermark("s3"); wm != 0 {
		t.Fatalf("watermark before close = %d", wm)
	}
	t.Logf("CHECKPOINT at crash: source=s3 watermark=%d (all 4 durable pending on disk)",
		p1.CheckpointWatermark("s3"))
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：用同一目录与一个新的内存下游
	cfg2 := cfg
	ms2 := sinkNewReuse(t, &cfg2)
	p2, err := New(cfg2)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer p2.Close()
	t.Logf("CHECKPOINT after restart load: source=s3 watermark=%d", p2.CheckpointWatermark("s3"))

	waitFor(t, func() bool { return len(ms2.Visible("audit", "s3")) == 0 }, "downstream still down")
	ms2.SetAvailable(true)
	waitFor(t, func() bool { return len(ms2.Visible("audit", "s3")) == 4 }, "4 delivered after restart")
	waitFor(t, func() bool { return p2.CheckpointWatermark("s3") == 4 }, "watermark 4 after restart")
	t.Logf("CHECKPOINT after catch-up: source=s3 watermark=%d", p2.CheckpointWatermark("s3"))
}

// TestRestartAfterPartialDelivery: 部分投递后崩溃，重启不重复可见、不丢。
func TestRestartAfterPartialDelivery(t *testing.T) {
	cfg, ms := testConfig(t)
	p1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := int64(1); i <= 3; i++ {
		_, _ = p1.Ingest(ctx, mkEv("p"+seqStr(i), "s4", i, "login"))
	}
	waitFor(t, func() bool { return p1.CheckpointWatermark("s4") == 3 }, "watermark 3")
	ms.SetAvailable(false)
	_, _ = p1.Ingest(ctx, mkEv("p4", "s4", 4, "login"))
	time.Sleep(150 * time.Millisecond)
	t.Logf("CHECKPOINT at crash: source=s4 watermark=%d", p1.CheckpointWatermark("s4"))
	_ = p1.Close()

	p2, err := New(cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer p2.Close()
	ms.SetAvailable(true)
	waitFor(t, func() bool { return len(ms.Visible("audit", "s4")) == 4 }, "4 visible total")
	ids := ms.Visible("audit", "s4")
	if len(ids) != 4 {
		t.Fatalf("visible after restart = %v", ids)
	}
	waitFor(t, func() bool { return p2.CheckpointWatermark("s4") == 4 }, "watermark 4")
	t.Logf("CHECKPOINT recovered: source=s4 watermark=%d visible=%v",
		p2.CheckpointWatermark("s4"), ids)
}
