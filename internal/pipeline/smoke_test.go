package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/downstream"
	"github.com/h3yow/audit-log-pipeline-go/internal/event"
	"github.com/h3yow/audit-log-pipeline-go/internal/policy"
	"github.com/h3yow/audit-log-pipeline-go/internal/store"
)

func testCfg(t *testing.T) Config {
	t.Helper()
	return Config{
		Dir: t.TempDir(),
		Retention: store.RetentionPolicy{
			DefaultRetention: time.Hour,
			MaxSegmentBytes:  4096,
		},
		Rules:               []policy.Rule{{Name: "deny-debug", Source: "", Type: "debug", Action: policy.ActionDeny}},
		DefaultTargets:      []string{"archive"},
		DeliveryConcurrency: 1,
	}
}

func validEvent(id string, seq int64, typ string) string {
	return fmt.Sprintf(`{"id":%q,"source":"srcA","seq":%d,"type":%q,"occurred_at":"2026-09-22T10:00:00Z","payload":"x"}`,
		id, seq, typ)
}

func TestSmoke(t *testing.T) {
	tgt := downstream.NewMockTarget("archive")
	p, err := New(testCfg(t), map[string]downstream.Target{"archive": tgt})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p.Start(ctx)
	defer p.Close()

	r := p.Submit(ctx, validEvent("e1", 1, "login"))
	if r.Code != event.OutcomeAccepted {
		t.Fatalf("got %s: %s", r.Code, r.Reason)
	}
	time.Sleep(150 * time.Millisecond)
	if !tgt.Has(1) {
		t.Fatal("record not delivered")
	}
	// malformed
	r3 := p.Submit(ctx, "{not json")
	if r3.Code != event.OutcomeMalformed {
		t.Fatalf("malformed got %s", r3.Code)
	}
	// rejected
	r4 := p.Submit(ctx, validEvent("e2", 2, "debug"))
	if r4.Code != event.OutcomeRejected {
		t.Fatalf("reject got %s", r4.Code)
	}
	quar, _ := p.Quarantine()
	if len(quar) != 2 {
		t.Fatalf("quarantine=%d want 2", len(quar))
	}
	// duplicate of an already fully delivered event
	r2 := p.Submit(ctx, validEvent("e1", 1, "login"))
	if r2.Code != event.OutcomeDuplicate {
		t.Fatalf("dup got %s", r2.Code)
	}
	if len(tgt.Visible()) != 1 {
		t.Fatalf("downstream saw duplicates: %v", tgt.Visible())
	}
}
