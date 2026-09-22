package policy

import (
	"testing"

	"github.com/h3yow/audit-log-pipeline-go/internal/event"
)

func mkEv(id, source, typ string, seq int64) event.Event {
	return event.Event{ID: id, Source: source, Seq: seq, Type: typ}
}

func TestRulesOrderedAndDeny(t *testing.T) {
	eng := NewEngine([]string{"arc", "siem"}, []Rule{
		{Name: "deny-debug", Type: "debug", Action: ActionDeny},
		{Name: "allow-billing", Source: "billing", Action: ActionAllow},
	})
	d := eng.Evaluate(mkEv("e1", "billing", "debug", 1))
	// First matching rule by order wins: deny-debug matches the type first.
	if d.Action != ActionDeny || d.Rule != "deny-debug" {
		t.Fatalf("got action=%s rule=%s", d.Action, d.Rule)
	}
	d2 := eng.Evaluate(mkEv("e2", "billing", "login", 2))
	if d2.Action != ActionAllow || len(d2.Targets) != 2 {
		t.Fatalf("billing login should route to defaults: %+v", d2)
	}
	d3 := eng.Evaluate(mkEv("e3", "other", "login", 3))
	if d3.Action != ActionAllow || d3.Rule != "" {
		t.Fatalf("default allow expected, got %+v", d3)
	}
}

func TestDedupScopedToSource(t *testing.T) {
	eng := NewEngine([]string{"arc"}, nil)
	if eng.IsDuplicate(mkEv("x", "a", "login", 1)) {
		t.Fatal("unexpected duplicate before mark")
	}
	eng.MarkAccepted(mkEv("x", "a", "login", 1))
	if !eng.IsDuplicate(mkEv("x", "a", "login", 1)) {
		t.Fatal("same source+id must be duplicate")
	}
	// Same event id in a different source is NOT a duplicate.
	if eng.IsDuplicate(mkEv("x", "b", "login", 1)) {
		t.Fatal("dedup must be scoped within a source")
	}
	// Reused id with a different seq in the same source is still a dup:
	// the id is the idempotency key.
	if !eng.IsDuplicate(mkEv("x", "a", "login", 99)) {
		t.Fatal("same id different seq must be duplicate")
	}
}

func TestResetSeen(t *testing.T) {
	eng := NewEngine([]string{"arc"}, nil)
	eng.MarkAccepted(mkEv("old", "a", "login", 1))
	eng.ResetSeen(map[string]int64{"a\x00new": 2})
	if eng.IsDuplicate(mkEv("old", "a", "login", 1)) {
		t.Fatal("old id should be gone after reset")
	}
	if !eng.IsDuplicate(mkEv("new", "a", "login", 2)) {
		t.Fatal("rehydrated id should be recognized")
	}
}
