package store

import (
	"testing"
)

func TestQuarantineOrderAndReopen(t *testing.T) {
	dir := t.TempDir()
	q, err := OpenQuarantine(dir)
	if err != nil {
		t.Fatal(err)
	}
	entries := []struct {
		kind   QuarantineKind
		source string
		eid    string
		reason string
		rule   string
	}{
		{QuarantineMalformed, "", "", "bad json", ""},
		{QuarantineDuplicate, "s", "e1", "already seen", ""},
		{QuarantineRejected, "s", "e2", "denied", "deny-x"},
	}
	for _, e := range entries {
		if err := q.Append(e.kind, e.source, e.eid, "raw-"+e.eid, e.reason, e.rule); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q2, err := OpenQuarantine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	got, err := q2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(entries) {
		t.Fatalf("quarantine len=%d want %d", len(got), len(entries))
	}
	for i, e := range entries {
		if got[i].Kind != e.kind || got[i].Source != e.source ||
			got[i].EventID != e.eid || got[i].Rule != e.rule {
			t.Fatalf("entry %d mismatch: %+v", i, got[i])
		}
	}
}
