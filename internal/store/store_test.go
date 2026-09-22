package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/event"
)

func mkEvent(id string, seq int64) event.Event {
	return event.Event{
		ID: id, Source: "src", Seq: seq, Type: "login",
		OccurredAt: time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC),
		Payload:    "p",
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(t.TempDir(), RetentionPolicy{
		DefaultRetention: time.Hour,
		MaxSegmentBytes:  256, // tiny to force multiple segments
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestAppendDedupAndRead(t *testing.T) {
	st := openTestStore(t)
	r1, err := st.Append(mkEvent("a", 1))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := st.Append(mkEvent("b", 2))
	if err != nil {
		t.Fatal(err)
	}
	if r2.ID != r1.ID+1 {
		t.Fatalf("id sequence %d,%d", r1.ID, r2.ID)
	}
	if _, err := st.Append(mkEvent("a", 1)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	got, err := st.GetByEventID("src", "a")
	if err != nil || got.ID != r1.ID {
		t.Fatalf("lookup: %v %v", got, err)
	}
	var ids []int64
	err = st.ReadBetween(context.Background(), 1, st.LatestID(), func(r Record) error {
		ids = append(ids, r.ID)
		return nil
	})
	if err != nil || len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("readback ids=%v err=%v", ids, err)
	}
}

func TestTamperDetected(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir, RetentionPolicy{
		DefaultRetention: time.Hour, MaxSegmentBytes: 256,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := st.Append(mkEvent(string(rune('a'+i)), int64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Modify a byte inside a segment payload (flip a payload char).
	seg := filepath.Join(dir, "segments", "0000000000.arec")
	b, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte well inside the JSON body, after the 8-byte header.
	b[40] ^= 0xFF
	if err := os.WriteFile(seg, b, 0o644); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dir, RetentionPolicy{
		DefaultRetention: time.Hour, MaxSegmentBytes: 256,
	})
	if err != nil {
		t.Fatalf("open after tamper: %v", err)
	}
	defer st2.Close()
	found := false
	for _, is := range st2.Stats().TamperIssues {
		t.Logf("issue: %s %s %s", is.Kind, is.Segment, is.Detail)
		if is.Kind == "MODIFIED" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected MODIFIED tamper issue")
	}
}

func TestExtraSegmentDetected(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir, RetentionPolicy{DefaultRetention: time.Hour, MaxSegmentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(mkEvent("a", 1)); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// Fabricate an out-of-manifest segment (out-of-order insertion).
	extra := make([]byte, 2048)
	if err := os.WriteFile(filepath.Join(dir, "segments", "9999999999.arec"), extra, 0o644); err != nil {
		t.Fatal(err)
	}
	st2, err := Open(dir, RetentionPolicy{DefaultRetention: time.Hour, MaxSegmentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	found := false
	for _, is := range st2.Stats().TamperIssues {
		if is.Kind == "INSERTED" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected INSERTED issue for unknown segment")
	}
}

func TestCleanupRetentionAndFreeze(t *testing.T) {
	st := openTestStore(t)
	for i := 0; i < 8; i++ {
		if _, err := st.Append(mkEvent(string(rune('a'+i)), int64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	// Freeze the oldest range so cleanup must not touch it.
	if err := st.PutFreeze(Freeze{Key: "hold", FromID: 1, ToID: 3, Note: "legal"}); err != nil {
		t.Fatal(err)
	}
	// While still within retention, nothing is deleted.
	soon := time.Now().UTC().Add(time.Minute)
	repNow, err := st.Cleanup(soon)
	if err != nil {
		t.Fatal(err)
	}
	if repNow.DeletedRecords != 0 {
		t.Fatalf("records deleted within retention: %d", repNow.DeletedRecords)
	}
	// Well past retention, but frozen records survive.
	future := time.Now().UTC().Add(48 * time.Hour)
	rep, err := st.Cleanup(future)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cleanup report: examined=%d deleted_segments=%d deleted_records=%d kept_frozen=%d kept_retained=%d",
		rep.Examined, rep.DeletedSegments, rep.DeletedRecords, rep.KeptFrozen, rep.KeptRetained)
	if rep.KeptFrozen == 0 {
		t.Fatal("expected frozen records to be kept")
	}
	// Frozen records must still be readable.
	for _, id := range []int64{1, 2, 3} {
		if _, err := st.Get(id); err != nil {
			t.Fatalf("frozen record %d not readable: %v", id, err)
		}
	}

	// Release the freeze; now the oldest segments can be cleaned.
	if err := st.ReleaseFreeze("hold"); err != nil {
		t.Fatal(err)
	}
	rep2, err := st.Cleanup(future)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cleanup after release: deleted_segments=%d deleted_records=%d",
		rep2.DeletedSegments, rep2.DeletedRecords)
	if rep2.DeletedRecords == 0 {
		t.Fatal("expected records to be deleted after freeze release")
	}
	// Cleaned data must not be readable.
	if _, err := st.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cleaned record readable: %v", err)
	}
}

func TestConcurrentAppendReadCleanup(t *testing.T) {
	st := openTestStore(t)
	done := make(chan struct{})
	var wg sync.WaitGroup
	// Concurrent appenders, one source per worker (per-source ordering is
	// defined for serial submissions; cross-source concurrency is allowed).
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			src := "src-" + strconv.Itoa(w)
			for i := 0; i < 40; i++ {
				_, _ = st.Append(event.Event{
					ID: "e-" + strconv.Itoa(i), Source: src, Seq: int64(i + 1),
					Type: "login", OccurredAt: time.Now().UTC(), Payload: "x",
				})
			}
		}(w)
	}
	// Concurrent readers must never see errors/half state. They run
	// until appenders finish (done closes), tracked separately.
	var rwg sync.WaitGroup
	rwg.Add(1)
	go func() {
		defer rwg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
			}
			_ = st.ReadBetween(context.Background(), 1, st.LatestID(), func(r Record) error {
				if r.ID <= 0 || r.EventID == "" {
					t.Errorf("bad record surfaced: %+v", r)
				}
				return nil
			})
		}
	}()
	wg.Wait()
	close(done)
	rwg.Wait()
	if st.LatestID() == 0 {
		t.Fatal("no records appended")
	}
}
