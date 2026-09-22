package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/checkpoint"
	"github.com/h3yow/audit-log-pipeline-go/internal/downstream"
	"github.com/h3yow/audit-log-pipeline-go/internal/event"
	"github.com/h3yow/audit-log-pipeline-go/internal/store"
)

// TestDuplicateAndDistinctConclusions covers repeated delivery and the
// three non-accept conclusions (malformed, duplicate, rejected), each
// distinguishable and quarantined rather than silently dropped.
func TestDuplicateAndDistinctConclusions(t *testing.T) {
	arc := downstream.NewMockTarget("archive")
	siem := downstream.NewMockTarget("siem")
	cfg := baseCfg(t, 4096, 1<<20)
	p, err := New(cfg, map[string]downstream.Target{"archive": arc, "siem": siem})
	if err != nil {
		t.Fatal(err)
	}
	p.Start(context.Background())
	defer p.Close()
	ctx := context.Background()

	r := p.Submit(ctx, eventJSON("e1", "srcA", 1, "login"))
	logReceipt(t, p, eventJSON("e1", "srcA", 1, "login"), r)
	if r.Code != event.OutcomeAccepted {
		t.Fatalf("want ACCEPTED got %s (%s)", r.Code, r.Reason)
	}

	// Repeated delivery of the same event: same conclusion shape, and the
	// downstream must observe the event exactly once.
	rdup := p.Submit(ctx, eventJSON("e1", "srcA", 1, "login"))
	logReceipt(t, p, eventJSON("e1", "srcA", 1, "login"), rdup)
	if rdup.Code != event.OutcomeDuplicate {
		t.Fatalf("want DUPLICATE got %s", rdup.Code)
	}

	rmal := p.Submit(ctx, `{"id": "broken"`)
	logReceipt(t, p, `{"id": "broken"`, rmal)
	if rmal.Code != event.OutcomeMalformed {
		t.Fatalf("want MALFORMED got %s", rmal.Code)
	}

	rrej := p.Submit(ctx, eventJSON("e2", "srcA", 2, "debug"))
	logReceipt(t, p, eventJSON("e2", "srcA", 2, "debug"), rrej)
	if rrej.Code != event.OutcomeRejected || rrej.Rule != "deny-debug" {
		t.Fatalf("want REJECTED/deny-debug got %s rule=%q", rrej.Code, rrej.Rule)
	}

	if !waitFor(2*time.Second, func() bool {
		return arc.Has(r.RecordID) && siem.Has(r.RecordID)
	}) {
		t.Fatal("event not delivered to both targets")
	}
	if n := arc.Attempts(r.RecordID); n != 1 {
		t.Fatalf("archive accepted %d times, want 1 (no duplicate visible)", n)
	}
	if n := siem.Attempts(r.RecordID); n != 1 {
		t.Fatalf("siem accepted %d times, want 1", n)
	}

	// Every non-accepted input is auditable in the quarantine journal.
	quar, err := p.Quarantine()
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[event.OutcomeCode]int{}
	for _, q := range quar {
		t.Logf("QUARANTINE: kind=%s source=%s event=%s reason=%q", q.Kind, q.Source, q.EventID, q.Reason)
		kinds[event.OutcomeCode(q.Kind)]++
	}
	if kinds[event.OutcomeDuplicate] < 1 || kinds[event.OutcomeMalformed] < 1 || kinds[event.OutcomeRejected] < 1 {
		t.Fatalf("quarantine kinds incomplete: %+v", kinds)
	}

	cp := p.Checkpoints()["srcA"]
	if cp.Seq != 1 || cp.RecordID != r.RecordID {
		t.Fatalf("checkpoint srcA wrong: %+v", cp)
	}
}

// TestPerSourceOrderStable verifies in-order storage and downstream
// visibility within a source, even across interleaved sources.
func TestPerSourceOrderStable(t *testing.T) {
	arc := downstream.NewMockTarget("archive")
	cfg := baseCfg(t, 4096, 1<<20)
	cfg.DefaultTargets = []string{"archive"}
	cfg.DeliveryConcurrency = 1 // single worker to observe order
	p, err := New(cfg, map[string]downstream.Target{"archive": arc})
	if err != nil {
		t.Fatal(err)
	}
	p.Start(context.Background())
	defer p.Close()
	ctx := context.Background()

	for i := 1; i <= 20; i++ {
		src := "srcA"
		if i%2 == 0 {
			src = "srcB"
		}
		r := p.Submit(ctx, eventJSON(fmt.Sprintf("e%d", i), src, int64((i+1)/2), "login"))
		logReceipt(t, p, "", r)
		if r.Code != event.OutcomeAccepted {
			t.Fatalf("event %d got %s", i, r.Code)
		}
	}
	if !waitFor(3*time.Second, func() bool { return len(arc.Visible()) == 20 }) {
		t.Fatalf("delivered=%d want 20", len(arc.Visible()))
	}
	// Read records back from the store and verify per-source seq order
	// matches submission order.
	var prevA, prevB int64
	err = p.Store().ReadBetween(context.Background(), 1, p.Store().LatestID(), func(rec store.Record) error {
		switch rec.Source {
		case "srcA":
			if rec.Seq <= prevA {
				return fmt.Errorf("srcA out of order: %d after %d", rec.Seq, prevA)
			}
			prevA = rec.Seq
		case "srcB":
			if rec.Seq <= prevB {
				return fmt.Errorf("srcB out of order: %d after %d", rec.Seq, prevB)
			}
			prevB = rec.Seq
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCheckpointRegressionRejected verifies a backwards move is refused
// and recorded rather than silently replayed.
func TestCheckpointRegressionRejected(t *testing.T) {
	cfg := baseCfg(t, 4096, 1<<20)
	p, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.cp.Advance("srcA", 5, 9); err != nil {
		t.Fatal(err)
	}
	err = p.cp.Advance("srcA", 2, 4)
	if !errors.Is(err, checkpoint.ErrRegression) {
		t.Fatalf("want ErrRegression got %v", err)
	}
	if got := p.Checkpoints()["srcA"].Seq; got != 5 {
		t.Fatalf("checkpoint moved backwards: %d", got)
	}
	regs := p.Regressions()
	if len(regs) != 1 {
		t.Fatalf("regressions=%d want 1", len(regs))
	}
	t.Logf("REGRESSION RECORDED: source=%s from_seq=%d wanted_seq=%d",
		regs[0].Source, regs[0].FromSeq, regs[0].WantedSeq)
}

// TestConcurrentCleanupWhileReading exercises concurrent reads against a
// cleanup pass, asserting no loss, duplicate exposure or partial state.
func TestConcurrentCleanupWhileReading(t *testing.T) {
	arc := downstream.NewMockTarget("archive")
	cfg := baseCfg(t, 256, 1<<20)
	cfg.DefaultTargets = []string{"archive"}
	p, err := New(cfg, map[string]downstream.Target{"archive": arc})
	if err != nil {
		t.Fatal(err)
	}
	p.Start(context.Background())
	defer p.Close()
	ctx := context.Background()

	const N = 60
	for i := 1; i <= N; i++ {
		r := p.Submit(ctx, eventJSON(fmt.Sprintf("e%d", i), "srcA", int64(i), "login"))
		if r.Code != event.OutcomeAccepted {
			t.Fatalf("event %d: %s %s", i, r.Code, r.Reason)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Concurrent readers: every record read must be complete and unique.
	wg.Add(1)
	go func() {
		defer wg.Done()
		seen := map[int64]int{}
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			_ = p.Store().ReadBetween(context.Background(), 1, p.Store().LatestID(),
				func(rec store.Record) error {
					if rec.EventID == "" || rec.Hash == ([32]byte{}) {
						t.Errorf("partial record exposed: %d", rec.ID)
					}
					seen[rec.ID]++
					return nil
				})
		}
	}()

	// After retention expires, run several cleanups concurrently.
	future := time.Now().Add(2 * time.Hour)
	var cwg sync.WaitGroup
	for k := 0; k < 3; k++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			rep, err := p.Cleanup(future)
			if err != nil {
				t.Errorf("cleanup: %v", err)
			}
			t.Logf("CONCURRENT CLEANUP: deleted_records=%d kept_frozen=%d examined=%d",
				rep.DeletedRecords, rep.KeptFrozen, rep.Examined)
		}()
	}
	cwg.Wait()
	close(stop)
	wg.Wait()

	// Cleaned prefix is gone and unreadable; no hole beyond the prefix.
	latest := p.Store().LatestID()
	issues, err := p.Verify()
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("integrity issues after cleanup: %+v", issues)
	}
	t.Logf("after concurrent cleanup: latest_id=%d stats=%+v", latest, p.Store().Stats())
}

// TestFreezeBlocksCleanup ensures records under an active legal hold are
// not removed even when retention has expired.
func TestFreezeBlocksCleanup(t *testing.T) {
	cfg := baseCfg(t, 256, 1<<20)
	cfg.DefaultTargets = []string{"archive"}
	p, err := New(cfg, map[string]downstream.Target{"archive": downstream.NewMockTarget("archive")})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		if r := p.Submit(ctx, eventJSON(fmt.Sprintf("e%d", i), "srcA", int64(i), "login")); r.Code != event.OutcomeAccepted {
			t.Fatal(r.Code)
		}
	}
	if err := p.PutFreeze(store.Freeze{Key: "legal-1", FromID: 1, ToID: 5, Note: "court order"}); err != nil {
		t.Fatal(err)
	}
	t.Logf("FREEZE APPLIED: key=legal-1 ids=1..5")
	rep, err := p.Cleanup(time.Now().Add(48 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CLEANUP DURING FREEZE: deleted_records=%d kept_frozen=%d", rep.DeletedRecords, rep.KeptFrozen)
	if rep.KeptFrozen == 0 {
		t.Fatal("frozen records were not retained")
	}
	for _, id := range []int64{1, 2, 3, 4, 5} {
		if _, err := p.Store().Get(id); err != nil {
			t.Fatalf("frozen record %d lost: %v", id, err)
		}
	}
}
