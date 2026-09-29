package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func appendN(t *testing.T, s *Store, source string, n int64, storedAt0 int64) {
	t.Helper()
	for i := int64(1); i <= n; i++ {
		rec := Record{
			Event:    mkEvent(fmt.Sprintf("%s-e%d", source, i), source, i),
			StoredAt: storedAt0 + i,
			State:    StatePending,
			Route:    "audit",
		}
		rec.Event.Timestamp = time.Unix(storedAt0+i, 0).UTC()
		if err := s.Append(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
}

func readAll(t *testing.T, r RangeReader) []int64 {
	t.Helper()
	var seqs []int64
	for {
		rv, ok, err := r.Next()
		if err != nil {
			t.Fatalf("reader: %v", err)
		}
		if !ok {
			return seqs
		}
		seqs = append(seqs, rv.Seq)
	}
}

// 区间读取器：闭区间边界、跨分段顺序、哈希链逐条校验。
func TestReplayReaderRangeAndOrder(t *testing.T) {
	s := newTestStore(t, time.Hour, 300) // 小分段强制跨段
	appendN(t, s, "rr", 12, 1000)

	r, err := s.OpenReplay("rr", 4, 9)
	if err != nil {
		t.Fatal(err)
	}
	seqs := readAll(t, r)
	_ = r.Close()
	want := []int64{4, 5, 6, 7, 8, 9}
	if len(seqs) != len(want) {
		t.Fatalf("seqs=%v want %v", seqs, want)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("order seqs=%v want %v", seqs, want)
		}
	}
	t.Logf("READER range=[4,9] -> %v", seqs)

	// 单条边界。
	r2, _ := s.OpenReplay("rr", 5, 5)
	if got := readAll(t, r2); len(got) != 1 || got[0] != 5 {
		t.Fatalf("point range=%v", got)
	}
	_ = r2.Close()

	// 未知来源：空读取器而非报错。
	r3, err := s.OpenReplay("nope", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, r3); len(got) != 0 {
		t.Fatalf("unknown source=%v", got)
	}
	_ = r3.Close()
}

// 保留锁：持锁范围在锁释放前不被清理，释放后可清理。
func TestReplayHoldBlocksPurge(t *testing.T) {
	s := newTestStore(t, time.Hour, 300)
	appendN(t, s, "hd", 12, 1000)
	// 再写一条迫使前面的分段封存。
	if err := s.Append(Record{
		Event: mkEvent("hd-e13", "hd", 13), StoredAt: 1013, State: StatePending,
	}); err != nil {
		t.Fatal(err)
	}
	s.AgeSealedSegmentsForTest("hd", time.Now().Add(-2*time.Hour).Unix())

	gaps, err := s.AcquireReplayHold("job-hold", "hd", 1, 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 0 {
		t.Fatalf("unexpected gaps on fresh data: %v", gaps)
	}
	res, err := s.Purge(time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedSegments != 0 || res.SkippedReplay == 0 {
		t.Fatalf("purge under hold = %+v", res)
	}
	t.Logf("PURGE under hold: deleted=%d skipped_replay=%d",
		res.DeletedSegments, res.SkippedReplay)

	// 锁内读取仍然完整。
	r, err := s.OpenReplay("hd", 1, 12)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, r); len(got) != 12 {
		t.Fatalf("held range read = %d", len(got))
	}
	_ = r.Close()

	if err := s.ReleaseReplayHold("job-hold"); err != nil {
		t.Fatal(err)
	}
	res2, err := s.Purge(time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	if res2.DeletedSegments == 0 {
		t.Fatalf("purge after release = %+v", res2)
	}
	t.Logf("PURGE after release: deleted=%d bytes=%d",
		res2.DeletedSegments, res2.ReclaimedBytes)

	// 已清理缺口：开读即 ErrPurgedGap，缺口区间可枚举，不静默跳过。
	gaps, _ = s.PurgedGapsInRange("hd", 1, 13)
	if len(gaps) == 0 {
		t.Fatalf("expected purged gaps")
	}
	t.Logf("GAPS after purge: %v", gaps)
	if _, err := s.OpenReplay("hd", 1, 13); !errors.Is(err, ErrPurgedGap) {
		t.Fatalf("OpenReplay over gap err=%v", err)
	}
}

// 并发：一边持续接入与清理（保留锁保护），一边流式回放，
// 不出现丢条、乱序、panic 或数据竞争。
func TestReplayReaderConcurrentAppendAndPurge(t *testing.T) {
	s := newTestStore(t, time.Hour, 400)
	appendN(t, s, "cc", 30, 1_000_000)
	s.AgeSealedSegmentsForTest("cc", time.Now().Add(-2*time.Hour).Unix())

	if _, err := s.AcquireReplayHold("cjob", "cc", 1, 30); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 清理循环：持锁分段必须全部跳过。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			res, _ := s.Purge(time.Now().Unix())
			if res.DeletedSegments != 0 {
				t.Errorf("held segment purged during replay: %+v", res)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// 并发接入新事件（活动分段尾部持续增长）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(31); i <= 120; i++ {
			rec := Record{
				Event:    mkEvent(fmt.Sprintf("cc-e%d", i), "cc", i),
				StoredAt: time.Now().Unix() + i, // 保留期内
				State:    StatePending,
			}
			if err := s.Append(rec); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 回放读取器在 [1,30] 上反复全量扫描：必须稳定拿到 30 条严格递增。
	for k := 0; k < 20; k++ {
		r, err := s.OpenReplay("cc", 1, 30)
		if err != nil {
			t.Fatalf("open replay: %v", err)
		}
		seqs := readAll(t, r)
		_ = r.Close()
		if len(seqs) != 30 {
			t.Fatalf("iter %d: got %d seqs, want 30", k, len(seqs))
		}
		for i, v := range seqs {
			if v != int64(i+1) {
				t.Fatalf("iter %d: seq mismatch at %d: %d", k, i, v)
			}
		}
		time.Sleep(2 * time.Millisecond)
	}

	close(stop)
	wg.Wait()
	t.Logf("CONCURRENT replay/purge/append stable: 20 full scans x 30 events OK")
}
