package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/downstream"
	"github.com/h3yow/audit-log-pipeline-go/internal/event"
)

// TestDownstreamOutageBoundedThenRecover verifies that while a
// downstream is continuously unavailable:
//   - accepted events durably accumulate in the bounded spool;
//   - disk usage stays at/under the configured limit (observable);
//   - once the downstream recovers, every event is delivered exactly
//     once (no loss, no duplicate visible events).
func TestDownstreamOutageBoundedThenRecover(t *testing.T) {
	dir := t.TempDir()
	arc := downstream.NewMockTarget("archive")

	cfg := func() Config {
		c := baseCfg(t, 4096, 4096) // tiny bounded spool
		c.Dir = dir                 // SAME directory across the "restart"
		c.DefaultTargets = []string{"archive"}
		c.DeliveryConcurrency = 1
		return c
	}

	// --- Process #1: downstream is DOWN from the start. ---
	arc.SetAvailable(false)
	p1, err := New(cfg(), map[string]downstream.Target{"archive": arc})
	if err != nil {
		t.Fatal(err)
	}
	p1.Start(context.Background())

	ctx := context.Background()
	var accepted, overloaded int
	for i := 1; i <= 50; i++ {
		r := p1.Submit(ctx, eventJSON(fmt.Sprintf("e%d", i), "srcA", int64(i), "login"))
		logReceipt(t, p1, "", r)
		switch r.Code {
		case event.OutcomeAccepted:
			accepted++
		case event.OutcomeOverloaded:
			overloaded++
		default:
			t.Fatalf("unexpected code %s for e%d (%s)", r.Code, i, r.Reason)
		}
	}
	t.Logf("DURING OUTAGE: accepted=%d overloaded=%d", accepted, overloaded)
	if accepted == 0 {
		t.Fatal("expected some events buffered during outage")
	}
	// Some events could not be enqueued while the spool was full. Their
	// audit records are nevertheless durable. The background reconciler
	// is responsible for creating those delivery obligations once ack
	// frees capacity; it must not (and cannot) make progress while the
	// outage keeps the spool full.
	if overloaded == 0 {
		t.Fatal("expected bounded spool to report OVERLOADED")
	}
	// Disk bound is observable and respected.
	st := p1.Stats()
	t.Logf("BOUNDED OBSERVABLE: spool queued=%d bytes=%d/%d overloaded_total=%d",
		st.Spool.Queued, st.Spool.BytesOnDisk, st.Spool.BytesLimit, p1.Stats().Overloaded)
	if st.Spool.BytesOnDisk > st.Spool.BytesLimit {
		t.Fatalf("disk unbounded during outage: %d > %d",
			st.Spool.BytesOnDisk, st.Spool.BytesLimit)
	}
	if len(arc.Visible()) != 0 {
		t.Fatal("downstream observed events while down")
	}

	// --- Simulate a process CRASH: close without graceful drain and
	//     reopen the same directory with the downstream still down. ---
	if err := p1.hardClose(); err != nil {
		t.Fatal(err)
	}
	p2, err := New(cfg(), map[string]downstream.Target{"archive": arc})
	if err != nil {
		t.Fatal(err)
	}
	p2.Start(context.Background())

	pendingBefore := p2.Spool().Pending()
	t.Logf("AFTER RESTART (still down): spool pending=%d", pendingBefore)
	if pendingBefore == 0 {
		t.Fatal("pending deliveries were lost across restart")
	}
	cp := p2.Checkpoints()
	t.Logf("CHECKPOINT AFTER RESTART: %+v", cp)
	if cp["srcA"].Seq == 0 {
		t.Fatal("checkpoint lost across restart")
	}

	// --- Downstream RECOVERS: the spool drains, the reconciler keeps
	//     filling previously-overloaded obligations, and every durable
	//     event is delivered exactly once. ---
	arc.SetAvailable(true)
	if !waitFor(8*time.Second, func() bool {
		// All 50 events delivered uniquely.
		return len(arc.Visible()) == 50
	}) {
		t.Fatalf("not all events delivered: visible=%d pending=%d",
			len(arc.Visible()), p2.Spool().Pending())
	}
	defer p2.Close()

	if !waitFor(2*time.Second, func() bool { return p2.Spool().Pending() == 0 }) {
		t.Fatalf("spool did not fully drain: pending=%d", p2.Spool().Pending())
	}

	// No loss and no duplicates: each record id appears exactly once.
	delivered := arc.Visible()
	t.Logf("AFTER RECOVERY: downstream visible=%d (unique, in order)", len(delivered))
	if len(delivered) != 50 {
		t.Fatalf("delivered=%d want 50", len(delivered))
	}
	seen := map[int64]int{}
	for _, id := range delivered {
		seen[id]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("record %d visible %d times", id, n)
		}
	}
	// In-order visibility per source: delivered ids must be ascending.
	for i := 1; i < len(delivered); i++ {
		if delivered[i] <= delivered[i-1] {
			t.Fatalf("downstream order violated at %d: %v", i, delivered)
		}
	}
}
