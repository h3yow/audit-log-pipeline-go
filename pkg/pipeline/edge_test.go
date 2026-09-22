package pipeline

import (
	"context"
	"testing"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
)

// TestDuplicateIDThenSameSeqDifferentID:
// 先同 ID 重复（duplicate），再用相同 seq 配不同 ID 投递——
// 两种重复必须给出可区分且明确的结论，不得静默接受。
func TestDuplicateIDThenSameSeqDifferentID(t *testing.T) {
	cfg, ms := testConfig(t)
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	d1, _ := p.Ingest(ctx, mkEv("dupX", "edge", 1, "login"))
	if d1.Kind != event.DecisionAccepted {
		t.Fatalf("first: %s", d1.Kind)
	}
	// 同 ID、不同 seq
	d2, _ := p.Ingest(ctx, mkEv("dupX", "edge", 5, "login"))
	if d2.Kind != event.DecisionDuplicate {
		t.Fatalf("same id different seq: %s", d2.Kind)
	}
	// seq 1 已被占用，配新 ID
	d3, _ := p.Ingest(ctx, mkEv("other-id", "edge", 1, "login"))
	if d3.Kind != event.DecisionDuplicate {
		t.Fatalf("same seq different id: kind=%s reason=%q", d3.Kind, d3.Reason)
	}
	// 正常推进 seq 2 仍可接受
	d4, _ := p.Ingest(ctx, mkEv("next", "edge", 2, "login"))
	if d4.Kind != event.DecisionAccepted {
		t.Fatalf("seq 2: %s", d4.Kind)
	}
	waitFor(t, func() bool { return len(ms.Visible("audit", "edge")) == 2 }, "2 visible")
}
