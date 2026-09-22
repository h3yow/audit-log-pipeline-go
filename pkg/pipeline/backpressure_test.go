package pipeline

import (
	"context"
	"errors"
	"testing"
)

// TestBackpressureWhenDownDown: 下游故障 + 磁盘待确认软上限触及时，
// Ingest 返回 ErrBackpressure 而不是无限占用内存/磁盘；
// 下游恢复、队列排空后可重新接入，不丢事件。
func TestBackpressureWhenDownDown(t *testing.T) {
	cfg, ms := testConfig(t)
	cfg.MaxPendingDisk = 3
	cfg.MaxInFlight = 2
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	ms.SetAvailable(false)

	accepted := 0
	gotBackpressure := false
	for i := int64(1); i <= 10; i++ {
		_, err := p.Ingest(ctx, mkEv("bp"+seqStr(i), "bp", i, "login"))
		if errors.Is(err, ErrBackpressure) {
			gotBackpressure = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		accepted++
	}
	if !gotBackpressure {
		t.Fatalf("expected ErrBackpressure, accepted=%d", accepted)
	}
	m := p.Metrics()
	t.Logf("BACKPRESSURE accepted=%d pending_on_disk=%d in_flight=%d",
		accepted, m.PendingOnDisk, m.InFlight)
	if m.PendingOnDisk > cfg.MaxPendingDisk {
		t.Fatalf("pending disk unbounded: %d > %d", m.PendingOnDisk, cfg.MaxPendingDisk)
	}

	ms.SetAvailable(true)
	waitFor(t, func() bool { return p.Metrics().PendingOnDisk == 0 }, "drained")
	// 恢复后可继续接入
	d, err := p.Ingest(ctx, mkEv("bp-after", "bp", int64(accepted)+1, "login"))
	if err != nil || d.Kind != "accepted" {
		t.Fatalf("post-recovery ingest: kind=%s err=%v", d.Kind, err)
	}
}
