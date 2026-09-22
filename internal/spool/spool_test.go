package spool

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func enq(t *testing.T, s *Spool, source, eid string, seq int64, target string) {
	t.Helper()
	err := s.Enqueue(context.Background(), Item{
		Source: source, EventID: eid, Seq: seq, RecordID: seq,
		Target: target, Payload: []byte(eid),
	})
	if err != nil {
		t.Fatalf("enqueue %s: %v", eid, err)
	}
}

func TestPerSourceFIFO(t *testing.T) {
	s, err := Open(Config{Dir: t.TempDir(), MaxDiskBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		enq(t, s, "srcA", fmt.Sprintf("a%d", i), int64(i), "t")
	}
	var got []int64
	for i := 0; i < 5; i++ {
		l, err := s.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, l.Item.Seq)
		_ = s.Ack(l)
	}
	want := []int64{1, 2, 3, 4, 5}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("fifo=%v want %v", got, want)
	}
}

func TestNackRequeuesAtHead(t *testing.T) {
	s, _ := Open(Config{Dir: t.TempDir(), MaxDiskBytes: 1 << 20})
	defer s.Close()
	ctx := context.Background()
	enq(t, s, "s", "e1", 1, "t")
	enq(t, s, "s", "e2", 2, "t")
	l1, _ := s.Next(ctx) // e1 leased

	// Deterministic assertion: while e1 is leased, the next item of the
	// same source must not be dispatched (strict per-source FIFO).
	s.mu.Lock()
	if s.leasedBySource["s"] != 1 {
		t.Fatalf("leasedBySource=%d want 1", s.leasedBySource["s"])
	}
	s.rr = []string{"s"}
	if e := s.dequeueLocked(); e != nil {
		t.Fatalf("dispatched %s while older event in flight", e.item.EventID)
	}
	s.mu.Unlock()

	// Failing delivery: nack e1; it must come back at the head, before e2.
	if err := s.Nack(l1); err != nil {
		t.Fatal(err)
	}
	l1b, _ := s.Next(ctx)
	if l1b.Item.EventID != "e1" {
		t.Fatalf("after nack got %s want e1", l1b.Item.EventID)
	}
	_ = s.Ack(l1b)
	l2, _ := s.Next(ctx)
	if l2.Item.EventID != "e2" {
		t.Fatalf("got %s want e2", l2.Item.EventID)
	}
	_ = s.Ack(l2)
}

func TestBoundedCapacity(t *testing.T) {
	// Never consume: queue fills to the bounded disk limit, then refuses.
	s, err := Open(Config{Dir: t.TempDir(), MaxDiskBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	sawFull := false
	for i := 1; i <= 200; i++ {
		err := s.Enqueue(ctx, Item{
			Source: "s", EventID: fmt.Sprintf("e%d", i), Seq: int64(i),
			Target: "t", Payload: make([]byte, 64),
		})
		if errors.Is(err, ErrSpoolFull) {
			sawFull = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !sawFull {
		t.Fatal("expected bounded spool to reach ErrSpoolFull")
	}
	st := s.Stats()
	if st.BytesOnDisk > st.BytesLimit {
		t.Fatalf("disk unbounded: %d > %d", st.BytesOnDisk, st.BytesLimit)
	}
	t.Logf("bounded stats: queued=%d bytes=%d/%d", st.Queued, st.BytesOnDisk, st.BytesLimit)
}

func TestRestartRebuildsPending(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(Config{Dir: dir, MaxDiskBytes: 1 << 20})
	ctx := context.Background()
	for i := 1; i <= 4; i++ {
		enq(t, s, "s", fmt.Sprintf("e%d", i), int64(i), "t")
	}
	// Deliver (ack) the first two, leave the rest pending, then "crash".
	for i := 0; i < 2; i++ {
		l, _ := s.Next(ctx)
		if err := s.Ack(l); err != nil {
			t.Fatal(err)
		}
	}
	// Compact so acked bytes are reclaimed, then close.
	s.mu.Lock()
	if err := s.compactLocked(); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: only the two pending items must survive.
	s2, err := Open(Config{Dir: dir, MaxDiskBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Pending(); got != 2 {
		t.Fatalf("pending after restart=%d want 2", got)
	}
	l3, _ := s2.Next(ctx)
	if l3.Item.EventID != "e3" {
		t.Fatalf("after restart got %s want e3", l3.Item.EventID)
	}
	// Strict per-source FIFO: e4 is only handed out once e3 is settled.
	if err := s2.Ack(l3); err != nil {
		t.Fatal(err)
	}
	l4, _ := s2.Next(ctx)
	if l4.Item.EventID != "e4" {
		t.Fatalf("after restart got %s want e4", l4.Item.EventID)
	}
	if err := s2.Ack(l4); err != nil {
		t.Fatal(err)
	}
}

func TestIdempotentEnqueue(t *testing.T) {
	s, _ := Open(Config{Dir: t.TempDir(), MaxDiskBytes: 1 << 20})
	defer s.Close()
	it := Item{Source: "s", EventID: "dup", Seq: 1, Target: "t", Payload: []byte("x")}
	if err := s.Enqueue(context.Background(), it); err != nil {
		t.Fatal(err)
	}
	// Same (source,event,target) while pending is a no-op.
	if err := s.Enqueue(context.Background(), it); err != nil {
		t.Fatalf("idempotent enqueue failed: %v", err)
	}
	if s.Pending() != 1 {
		t.Fatalf("pending=%d want 1 (no duplicate obligation)", s.Pending())
	}
}
