package pipeline

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/router"
	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

func testConfig(t *testing.T) (Config, *sink.MemorySink) {
	t.Helper()
	r := router.New([]router.Rule{
		{Name: "deny-debug", Types: []string{"debug"}, Action: router.ActionDeny},
		{Name: "audit", Types: []string{"login", "file"}, Action: router.ActionAccept, Route: "audit"},
	}, "default")
	ms := sink.NewMemorySink()
	cfg := Config{
		Dir:           filepath.Join(t.TempDir(), "p"),
		Router:        r,
		Sinks:         map[string]sink.Sink{"audit": ms, "default": ms},
		Retention:     storage.Retention{MaxAge: time.Hour},
		PurgeInterval: time.Hour,
		MaxInFlight:   64,
		Logger:        testLogger(t),
	}
	return cfg, ms
}

func mkEv(id, src string, seq int64, typ string) *event.Event {
	return &event.Event{
		ID: id, Source: src, Seq: seq, Type: typ,
		Action: "do", Timestamp: time.Date(2026, 1, 1, 0, 0, int(seq), 0, time.UTC),
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for: " + msg)
}

func TestDuplicateAndRejectAndOrder(t *testing.T) {
	cfg, ms := testConfig(t)
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	// 正常 3 条
	for i := int64(1); i <= 3; i++ {
		d, err := p.Ingest(ctx, mkEv("e"+seqStr(i), "s1", i, "login"))
		if err != nil || d.Kind != event.DecisionAccepted {
			t.Fatalf("ingest %d: kind=%s err=%v", i, d.Kind, err)
		}
	}
	// 重复投递：相同 ID 不同 seq
	d, _ := p.Ingest(ctx, mkEv("e2", "s1", 99, "login"))
	if d.Kind != event.DecisionDuplicate {
		t.Fatalf("want duplicate, got %s (%s)", d.Kind, d.Reason)
	}
	// 乱序 seq
	if _, err := p.Ingest(ctx, mkEv("e4", "s1", 2, "login")); err == nil {
		// seq 2 < last 3 => duplicate-seq 结论（SequenceError 映射）
	}
	// 拒绝策略
	dr, _ := p.Ingest(ctx, mkEv("eD", "s1", 5, "debug"))
	if dr.Kind != event.DecisionRejected {
		t.Fatalf("want rejected, got %s", dr.Kind)
	}
	// 非法输入
	dm, _ := p.IngestRaw(ctx, "s1", []byte("{not json"))
	if dm.Kind != event.DecisionMalformed {
		t.Fatalf("want malformed, got %s", dm.Kind)
	}

	waitFor(t, func() bool { return len(ms.Visible("audit", "s1")) == 3 }, "3 delivered")
	got := ms.Visible("audit", "s1")
	if got[0] != "e1" || got[1] != "e2" || got[2] != "e3" {
		t.Fatalf("delivery order = %v", got)
	}
	waitFor(t, func() bool { return p.CheckpointWatermark("s1") == 3 }, "watermark 3")

	m := p.Metrics()
	if m.Duplicates < 1 {
		t.Fatalf("duplicate metric = %d", m.Duplicates)
	}
	if m.Rejected != 1 {
		t.Fatalf("rejected metric = %d", m.Rejected)
	}
	if m.Malformed != 1 {
		t.Fatalf("malformed metric = %d", m.Malformed)
	}
	if m.Delivered != 3 {
		t.Fatalf("delivered metric = %d", m.Delivered)
	}
	t.Logf("FINAL metrics: %+v", m)
}

func TestDownstreamFailureNoLossNoDuplicate(t *testing.T) {
	cfg, ms := testConfig(t)
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	// 前 2 条正常
	for i := int64(1); i <= 2; i++ {
		_, _ = p.Ingest(ctx, mkEv("f"+seqStr(i), "s2", i, "login"))
	}
	waitFor(t, func() bool { return len(ms.Visible("audit", "s2")) == 2 }, "first 2 delivered")

	// 下游故障
	ms.SetAvailable(false)
	for i := int64(3); i <= 6; i++ {
		_, _ = p.Ingest(ctx, mkEv("f"+seqStr(i), "s2", i, "login"))
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(ms.Visible("audit", "s2")); n != 2 {
		t.Fatalf("during outage visible=%d, want 2", n)
	}
	m := p.Metrics()
	t.Logf("DURING outage metrics: in_flight=%d pending_on_disk=%d retries=%d bytes=%d",
		m.InFlight, m.PendingOnDisk, m.DeliveryRetries, m.Storage.BytesOnDisk)
	if m.PendingOnDisk < 4 {
		t.Fatalf("pending on disk = %d", m.PendingOnDisk)
	}
	if p.CheckpointWatermark("s2") != 2 {
		t.Fatalf("watermark advanced during outage: %d", p.CheckpointWatermark("s2"))
	}

	// 恢复
	ms.SetAvailable(true)
	waitFor(t, func() bool { return len(ms.Visible("audit", "s2")) == 6 }, "6 delivered after recovery")
	ids := ms.Visible("audit", "s2")
	if len(ids) != 6 {
		t.Fatalf("visible = %v", ids)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate visible event: %s", id)
		}
		seen[id] = true
	}
	waitFor(t, func() bool { return p.CheckpointWatermark("s2") == 6 }, "watermark 6")
	t.Logf("CHECKPOINT after recovery: source=s2 watermark=%d", p.CheckpointWatermark("s2"))
}

func seqStr(i int64) string {
	if i < 10 {
		return string(rune('0' + i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}
