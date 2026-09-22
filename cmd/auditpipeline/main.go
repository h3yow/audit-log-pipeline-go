// Command auditpipeline runs an end-to-end local demonstration of the
// audit pipeline: multi-source ingestion with per-source ordering,
// explicit malformed/duplicate/rejected conclusions, a simulated
// downstream outage with bounded buffering, a process restart, recovery
// with exactly-once-visible delivery, and finally retention cleanup with
// a legal hold.
//
// All state lives under a single local directory, so the demo can be
// re-run (delete the directory first).
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/downstream"
	"github.com/h3yow/audit-log-pipeline-go/internal/event"
	"github.com/h3yow/audit-log-pipeline-go/internal/pipeline"
	"github.com/h3yow/audit-log-pipeline-go/internal/policy"
	"github.com/h3yow/audit-log-pipeline-go/internal/store"
)

func main() {
	dir := filepath.Join(os.TempDir(), "auditpipeline-demo")
	_ = os.RemoveAll(dir)

	cfg := pipeline.Config{
		Dir: dir,
		Retention: store.RetentionPolicy{
			DefaultRetention: 200 * time.Millisecond, // short for the demo
			MaxSegmentBytes:  512,
		},
		Rules: []policy.Rule{
			{Name: "deny-debug", Type: "debug", Action: policy.ActionDeny},
		},
		DefaultTargets:      []string{"archive", "siem"},
		MaxSpoolDiskBytes:   8 * 1024,
		DeliveryConcurrency: 2,
	}

	archive := downstream.NewMockTarget("archive")
	siem := downstream.NewMockTarget("siem")
	targets := map[string]downstream.Target{"archive": archive, "siem": siem}

	// ---- Phase 1: downstream DOWN, ingest from two sources. ----------
	fmt.Println("== phase 1: downstream DOWN, ingesting events ==")
	archive.SetAvailable(false)
	siem.SetAvailable(false)

	p, err := pipeline.New(cfg, targets)
	must(err)
	p.Start(context.Background())

	ctx := context.Background()
	submit := func(raw, label string) {
		r := p.Submit(ctx, raw)
		fmt.Printf("  %-28s -> %-10s event=%s record=%d %s\n",
			label, r.Code, r.EventID, r.RecordID, r.Reason)
	}

	for i := 1; i <= 6; i++ {
		submit(evt("srcA", fmt.Sprintf("a%d", i), int64(i), "login"), fmt.Sprintf("srcA login #%d", i))
	}
	for i := 1; i <= 4; i++ {
		submit(evt("srcB", fmt.Sprintf("b%d", i), int64(i), "login"), fmt.Sprintf("srcB login #%d", i))
	}
	// Non-accepted inputs: each gets a distinct, auditable conclusion.
	submit(`{"id":"bad"}`, "malformed input")
	submit(evt("srcA", "a1", 1, "login"), "duplicate of a1")
	submit(evt("srcA", "deny1", 100, "debug"), "deny-rule match")

	printStats("during outage", p)
	quar, _ := p.Quarantine()
	fmt.Printf("  quarantined (not silently dropped): %d\n", len(quar))

	// ---- Phase 2: simulate a process crash + restart. ----------------
	fmt.Println("== phase 2: process CRASH and RESTART (still down) ==")
	must(p.HardClose())
	p2, err := pipeline.New(cfg, targets)
	must(err)
	p2.Start(context.Background())
	fmt.Printf("  pending deliveries after restart: %d\n", p2.Spool().Pending())
	for src, pos := range p2.Checkpoints() {
		fmt.Printf("  checkpoint restored: source=%s seq=%d record=%d\n", src, pos.Seq, pos.RecordID)
	}

	// ---- Phase 3: downstream RECOVERS. --------------------------------
	fmt.Println("== phase 3: downstream RECOVERS ==")
	archive.SetAvailable(true)
	siem.SetAvailable(true)
	waitUntil(5*time.Second, func() bool {
		return p2.Spool().Pending() == 0
	})
	fmt.Printf("  archive visible: %d unique records\n", len(archive.Visible()))
	fmt.Printf("  siem    visible: %d unique records\n", len(siem.Visible()))
	dupes := 0
	for _, id := range archive.Visible() {
		if archive.Attempts(id) > 1 {
			dupes++
		}
	}
	fmt.Printf("  archive records accepted more than once: %d\n", dupes)

	// ---- Phase 4: retention + legal hold. -----------------------------
	fmt.Println("== phase 4: retention cleanup with legal hold ==")
	must(p2.PutFreeze(store.Freeze{Key: "demo-hold", FromID: 1, ToID: 3, Note: "court order"}))
	time.Sleep(300 * time.Millisecond) // let retention expire
	rep, err := p2.Cleanup(time.Now())
	must(err)
	fmt.Printf("  cleanup: examined=%d deleted_records=%d kept_frozen=%d kept_retained=%d\n",
		rep.Examined, rep.DeletedRecords, rep.KeptFrozen, rep.KeptRetained)
	issues, _ := p2.Verify()
	fmt.Printf("  integrity issues after cleanup: %d\n", len(issues))

	must(p2.ReleaseFreeze("demo-hold"))
	rep2, err := p2.Cleanup(time.Now())
	must(err)
	fmt.Printf("  cleanup after release: deleted_records=%d\n", rep2.DeletedRecords)

	must(p2.Close())
	fmt.Println("== demo complete ==")
}

func evt(source, id string, seq int64, typ string) string {
	b, _ := jsonMarshal(event.Event{
		ID: id, Source: source, Seq: seq, Type: typ,
		OccurredAt: time.Now().UTC(),
		Payload:    fmt.Sprintf("payload-%s", id),
	})
	return string(b)
}

func waitUntil(d time.Duration, cond func() bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func printStats(label string, p *pipeline.Pipeline) {
	s := p.Stats()
	fmt.Printf("  [%s] spool queued=%d bytes=%d/%d accepted=%d quarantined=%d overloaded=%d\n",
		label, s.Spool.Queued, s.Spool.BytesOnDisk, s.Spool.BytesLimit,
		s.Accepted, s.Quarantined, s.Overloaded)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
