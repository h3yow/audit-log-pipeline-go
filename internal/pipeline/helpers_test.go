package pipeline

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/event"
	"github.com/h3yow/audit-log-pipeline-go/internal/policy"
	"github.com/h3yow/audit-log-pipeline-go/internal/store"
)

func baseCfg(t *testing.T, segBytes, spoolBytes int64) Config {
	t.Helper()
	return Config{
		Dir: t.TempDir(),
		Retention: store.RetentionPolicy{
			DefaultRetention: time.Hour,
			MaxSegmentBytes:  segBytes,
		},
		Rules: []policy.Rule{
			{Name: "allow-login", Type: "login", Action: policy.ActionAllow},
			{Name: "deny-debug", Type: "debug", Action: policy.ActionDeny},
		},
		DefaultTargets:      []string{"archive", "siem"},
		DeliveryConcurrency: 2,
		MaxSpoolDiskBytes:   spoolBytes,
	}
}

func eventJSON(id, source string, seq int64, typ string) string {
	b, _ := json.Marshal(event.Event{
		ID: id, Source: source, Seq: seq, Type: typ,
		OccurredAt: time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC),
		Payload:    fmt.Sprintf("payload-%s", id),
	})
	return string(b)
}

// logReceipt prints the event input, policy conclusion and checkpoint
// state so test output is reproducible and failure causes are separable.
func logReceipt(t *testing.T, p *Pipeline, raw string, r event.Receipt) {
	t.Helper()
	t.Logf("INPUT: %s", raw)
	t.Logf("DECISION: code=%s source=%s seq=%d event=%s record=%d rule=%q reason=%q targets=%v",
		r.Code, r.Source, r.Seq, r.EventID, r.RecordID, r.Rule, r.Reason, r.Targets)
	cps := p.Checkpoints()
	for src, pos := range cps {
		t.Logf("CHECKPOINT: source=%s seq=%d record_id=%d", src, pos.Seq, pos.RecordID)
	}
}

// waitFor blocks until cond is true or the timeout elapses.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
