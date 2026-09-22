package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// TestConcurrentWriteAndPurge: 多来源并发写入 + 读取 + 清理期间，
// 不出现丢失、重复暴露、半成品状态、panic 或数据竞争。
func TestConcurrentWriteAndPurge(t *testing.T) {
	cfg, ms := testConfig(t)
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	const writers = 4
	const perWriter = 60
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			src := fmt.Sprintf("cw-%d", w) // 每来源独立，保证来源内单调顺序
			for i := 1; i <= perWriter; i++ {
				id := fmt.Sprintf("w%d-%d", w, i)
				d, err := p.Ingest(ctx, mkEv(id, src, int64(i), "file"))
				if err != nil {
					t.Errorf("ingest %s: %v", id, err)
					return
				}
				if d.Kind != event.DecisionAccepted {
					t.Errorf("ingest %s kind=%s", id, d.Kind)
					return
				}
			}
		}(w)
	}

	stop := make(chan struct{})
	var purgeWG sync.WaitGroup
	purgeWG.Add(1)
	go func() {
		defer purgeWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = p.PurgeNow(time.Now())
				time.Sleep(time.Millisecond)
			}
		}
	}()

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for w := 0; w < writers; w++ {
				src := fmt.Sprintf("cw-%d", w)
				var prev int64
				if err := p.store.Scan(src, 1, 1<<62, func(rv storage.RecordView) bool {
					if rv.Seq <= prev {
						t.Errorf("scan %s returned non-increasing seq: %d after %d",
							src, rv.Seq, prev)
					}
					prev = rv.Seq
					return true
				}); err != nil {
					t.Errorf("scan: %v", err)
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(stop)
	purgeWG.Wait()
	<-readerDone

	total := writers * perWriter
	waitFor(t, func() bool { return ms.VisibleCount() == total },
		fmt.Sprintf("all %d visible", total))

	seen := map[string]bool{}
	for w := 0; w < writers; w++ {
		src := fmt.Sprintf("cw-%d", w)
		ids := ms.Visible("audit", src)
		if len(ids) != perWriter {
			t.Fatalf("source %s visible = %d, want %d", src, len(ids), perWriter)
		}
		for _, id := range ids {
			if seen[id] {
				t.Fatalf("duplicate visible: %s", id)
			}
			seen[id] = true
		}
		waitFor(t, func() bool { return p.CheckpointWatermark(src) == perWriter },
			src+" watermark")
	}
	t.Logf("CONCURRENT done: visible=%d, metrics=%+v", len(seen), p.Metrics())
}

// TestFreezeDuringPurge: 冻结生效期间即使保留期到期也不得清理；
// 解除冻结后允许清理（活动分段除外）。
func TestFreezeDuringPurge(t *testing.T) {
	cfg, _ := testConfig(t)
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		_, _ = p.Ingest(ctx, mkEv("z"+seqStr(i), "fz", i, "login"))
	}
	waitFor(t, func() bool { return p.CheckpointWatermark("fz") == 5 }, "delivered")

	// 小分段强制滚动：先登记冻结，再写入迫使封存旧分段，
	// 被封存的旧分段处于冻结范围内，清理必须跳过它。
	p.store.SetMaxSegmentBytesForTest(1)
	if err := p.AddFreeze(storage.Freeze{
		ID: "hold", Source: "fz", FromSeq: 1, ToSeq: 0,
		Until: time.Now().Add(time.Hour), Note: "investigation",
	}); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Ingest(ctx, mkEv("z6", "fz", 6, "login"))
	waitFor(t, func() bool { return p.CheckpointWatermark("fz") == 6 }, "seq6 delivered")
	// 旧化封存分段，使其超出保留期（但仍被冻结覆盖）。
	p.store.AgeSealedSegmentsForTest("fz", time.Now().Add(-2*time.Hour).Unix())

	res, err := p.PurgeNow(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedSegments != 0 || res.SkippedFrozen == 0 {
		t.Fatalf("purge with active freeze = %+v", res)
	}
	t.Logf("FROZEN purge: deleted=%d skipped_frozen=%d",
		res.DeletedSegments, res.SkippedFrozen)

	if err := p.RemoveFreeze("hold"); err != nil {
		t.Fatal(err)
	}
	res2, err := p.PurgeNow(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res2.DeletedSegments < 1 {
		t.Fatalf("purge after freeze release = %+v", res2)
	}
	t.Logf("POST-RELEASE purge: deleted=%d bytes=%d",
		res2.DeletedSegments, res2.ReclaimedBytes)
}
