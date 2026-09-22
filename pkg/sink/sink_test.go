package sink

import (
	"context"
	"errors"
	"testing"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
)

func TestMemorySinkIdempotentAndFailure(t *testing.T) {
	ms := NewMemorySink()
	ctx := context.Background()
	e := &event.Event{ID: "1", Source: "s", Seq: 1, Type: "t", Action: "a"}
	if err := ms.Deliver(ctx, "audit", e); err != nil {
		t.Fatal(err)
	}
	// 重复投递幂等
	if err := ms.Deliver(ctx, "audit", e); err != nil {
		t.Fatal(err)
	}
	if got := ms.Visible("audit", "s"); len(got) != 1 || got[0] != "1" {
		t.Fatalf("visible = %v", got)
	}
	ms.SetAvailable(false)
	if err := ms.Deliver(ctx, "audit",
		&event.Event{ID: "2", Source: "s", Seq: 2}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unavailable: %v", err)
	}
	if n := ms.VisibleCount(); n != 1 {
		t.Fatalf("failed delivery visible count = %d", n)
	}
	if ms.FailCount() != 1 {
		t.Fatalf("fail count = %d", ms.FailCount())
	}
}
