package downstream

import (
	"context"
	"errors"
	"testing"
)

func TestMockIdempotentAndOutage(t *testing.T) {
	m := NewMockTarget("arc")
	rec := []byte(`{"record_id":7,"event_id":"e","source":"s"}`)
	if err := m.Deliver(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	// Repeated delivery of the same record is a no-op visibility-wise.
	if err := m.Deliver(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if len(m.Visible()) != 1 || m.Attempts(7) != 2 {
		t.Fatalf("visible=%v attempts=%d", m.Visible(), m.Attempts(7))
	}
	m.SetAvailable(false)
	if err := m.Deliver(context.Background(), rec); !errors.Is(err, ErrDown) {
		t.Fatalf("want ErrDown got %v", err)
	}
	m.SetAvailable(true)
	if err := m.Deliver(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if len(m.Visible()) != 1 {
		t.Fatalf("recovery created a duplicate: %v", m.Visible())
	}
}
