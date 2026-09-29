package storage

import (
	"testing"
	"time"
)

// TestReplayHoldAndSnapshot：保留占位保护相交分段，快照反映 deleted 墓碑。
func TestReplayHoldAndSnapshot(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20)
	for i := int64(1); i <= 2; i++ {
		if err := s.Append(Record{Event: mkEvent("hold", "s", i), StoredAt: 100}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SealActiveSegment("s"); err != nil {
		t.Fatal(err)
	}
	for i := int64(3); i <= 4; i++ {
		if err := s.Append(Record{Event: mkEvent("hold", "s", i), StoredAt: 100000}); err != nil {
			t.Fatal(err)
		}
	}
	// 占位覆盖旧段 seq[1,2]：Purge 跳过。
	s.ReplayHoldAdd("job-hold", "s", 1, 2)
	res, err := s.Purge(100 + 3600 + 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedSegments != 0 || res.SkippedReplay != 1 {
		t.Fatalf("purge under hold: %+v", res)
	}
	// 释放后旧段可清。
	s.ReplayHoldRemove("job-hold")
	res, err = s.Purge(100 + 3600 + 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedSegments != 1 {
		t.Fatalf("purge after release: %+v", res)
	}
	snap, err := s.ReplaySnapshot("s")
	if err != nil {
		t.Fatal(err)
	}
	var deleted, alive int
	for _, seg := range snap.Segments {
		if seg.Deleted {
			deleted++
			if seg.MinSeq != 1 || seg.MaxSeq != 2 {
				t.Fatalf("deleted tombstone range=%d..%d", seg.MinSeq, seg.MaxSeq)
			}
		} else {
			alive++
		}
	}
	if deleted != 1 || alive != 1 {
		t.Fatalf("snapshot deleted=%d alive=%d", deleted, alive)
	}
}
