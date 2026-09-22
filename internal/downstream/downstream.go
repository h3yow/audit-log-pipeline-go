// Package downstream defines the delivery sink interface and a
// controllable in-memory implementation used to simulate prolonged
// outages and idempotent recovery.
package downstream

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrDown is returned by a sink that is simulating an outage.
var ErrDown = errors.New("downstream unavailable")

// deliveredRecord is the minimal idempotency envelope carried in the
// record JSON payload produced by the pipeline.
type deliveredRecord struct {
	RecordID int64  `json:"record_id"`
	EventID  string `json:"event_id"`
	Source   string `json:"source"`
}

// Target is a delivery destination.
type Target interface {
	Name() string
	// Deliver succeeds at-least-once. Implementations must be idempotent
	// for repeated records with the same RecordID/EventID.
	Deliver(ctx context.Context, recordJSON []byte) error
}

// MockTarget is a controllable, idempotent, observable in-memory sink.
type MockTarget struct {
	mu        sync.Mutex
	name      string
	available bool
	seen      map[int64]int // record id -> delivery attempt count
	order     []int64       // visible order of first acceptance
	latency   time.Duration
}

// NewMockTarget creates a mock sink (starts available).
func NewMockTarget(name string) *MockTarget {
	return &MockTarget{name: name, available: true, seen: make(map[int64]int)}
}

// Name returns the sink name.
func (t *MockTarget) Name() string { return t.name }

// Deliver records an idempotent acceptance; returns ErrDown while
// unavailable. Duplicate deliveries (same record id) never create a
// second visible occurrence, mirroring an idempotent real downstream.
func (t *MockTarget) Deliver(ctx context.Context, recordJSON []byte) error {
	t.mu.Lock()
	available := t.available
	latency := t.latency
	t.mu.Unlock()

	if !available {
		return ErrDown
	}
	if latency > 0 {
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	id := parseRecordID(recordJSON)

	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.available {
		return ErrDown // may have flipped during the wait
	}
	t.seen[id]++
	if t.seen[id] == 1 {
		t.order = append(t.order, id)
	}
	return nil
}

// SetAvailable toggles the simulated outage.
func (t *MockTarget) SetAvailable(ok bool) {
	t.mu.Lock()
	t.available = ok
	t.mu.Unlock()
}

// SetLatency injects per-delivery latency.
func (t *MockTarget) SetLatency(d time.Duration) {
	t.mu.Lock()
	t.latency = d
	t.mu.Unlock()
}

// Visible returns accepted record ids in first-acceptance order.
func (t *MockTarget) Visible() []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]int64(nil), t.order...)
}

// Attempts returns how many times a record id was attempted (accepted
// attempts; retries during outage return ErrDown and are not counted).
func (t *MockTarget) Attempts(id int64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seen[id]
}

// Has reports whether a record id was accepted.
func (t *MockTarget) Has(id int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seen[id] > 0
}
