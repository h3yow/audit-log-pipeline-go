package checkpoint

import (
	"errors"
	"testing"
)

func TestAdvanceAndReopen(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Advance("srcA", 5, 100); err != nil {
		t.Fatal(err)
	}
	if err := m.Advance("srcA", 6, 101); err != nil {
		t.Fatal(err)
	}
	if got := m.Get("srcA").Seq; got != 6 {
		t.Fatalf("seq=%d want 6", got)
	}
	// Same value is an idempotent no-op (restart retries same position).
	if err := m.Advance("srcA", 6, 101); err != nil {
		t.Fatalf("idempotent advance failed: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	if got := m2.Get("srcA").Seq; got != 6 {
		t.Fatalf("after reopen seq=%d want 6", got)
	}
	if got := m2.Get("srcA").RecordID; got != 101 {
		t.Fatalf("record id=%d want 101", got)
	}
}

func TestRegressionRejected(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if err := m.Advance("srcA", 10, 50); err != nil {
		t.Fatal(err)
	}
	// A stale process tries to roll the position backwards: must be
	// explicitly rejected, not silently applied.
	err = m.Advance("srcA", 3, 20)
	if !errors.Is(err, ErrRegression) {
		t.Fatalf("want ErrRegression, got %v", err)
	}
	if got := m.Get("srcA").Seq; got != 10 {
		t.Fatalf("position moved after regression: %d", got)
	}
	regs := m.Regressions()
	if len(regs) != 1 {
		t.Fatalf("regressions=%d want 1", len(regs))
	}
	if regs[0].FromSeq != 10 || regs[0].WantedSeq != 3 {
		t.Fatalf("regression record wrong: %+v", regs[0])
	}
	// Forward progress still works.
	if err := m.Advance("srcA", 11, 51); err != nil {
		t.Fatal(err)
	}
}
