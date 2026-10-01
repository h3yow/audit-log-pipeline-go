package replay

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// mkRecordAt 构造一条“事件时间 ev、落盘时间 stored”的记录。
func mkRecordAt(src string, seq int64, ev, stored time.Time, state storage.DeliveryState) storage.RecordView {
	return storage.RecordView{Seq: seq, Record: storage.Record{
		Event: &event.Event{
			ID: "id-" + src + "-" + seqStr(seq), Source: src, Seq: seq,
			Type: "login", Action: "do", Timestamp: ev,
		},
		Route: "audit", State: state, StoredAt: stored.Unix(),
	}}
}

// meta 构造一个已封存分段元数据（StoredAt 与事件时间可独立设置）。
func meta(name string, lo, hi int64, firstStored, lastStored int64) storage.SegmentMeta {
	return storage.SegmentMeta{
		Name: name, MinSeq: lo, MaxSeq: hi, Records: int(hi - lo + 1),
		FirstStored: firstStored, LastStored: lastStored, Sealed: true,
	}
}

func ptr(tt time.Time) *time.Time { return &tt }

// TestSegmentCouldMatchIgnoresStoredAt：段级剪枝只看序号，绝不看落盘时间。
// 晚接入的历史批次 StoredAt 整体晚于回放时间上界，也必须保留到逐条判定。
func TestSegmentCouldMatchIgnoresStoredAt(t *testing.T) {
	early := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	// 事件 seq 1..100 发生在 2026-01-01，却在 2026-10-01 才落盘。
	seg := meta("seg-000001.dat", 1, 100, late.Unix(), late.Unix())

	cases := []struct {
		name string
		rng  Range
		want bool
	}{
		// 时间条件绝不影响段级剪枝：只要没有序号边界把整段排除，段都保留，
		// 交给逐条按“事件自身时间戳”判定。
		{"time-only around event time, stored much later",
			Range{From: ptr(early.Add(10 * time.Second)), To: ptr(early.Add(20 * time.Second))}, true},
		{"time-only entirely before event time",
			Range{From: ptr(early.Add(-200 * time.Hour)), To: ptr(early.Add(-100 * time.Hour))}, true},
		{"time-only entirely after event time",
			Range{From: ptr(early.Add(200 * time.Hour)), To: ptr(early.Add(300 * time.Hour))}, true},
		// 序号边界：相交保留、整段不相交才排除。
		{"seq overlaps segment", Range{FromSeq: 50, ToSeq: 150}, true},
		{"seq open-upper overlaps segment", Range{FromSeq: 100}, true},
		{"seq open-lower overlaps segment", Range{ToSeq: 1}, true},
		{"seq entirely after segment", Range{FromSeq: 101, ToSeq: 200}, false},
		{"no bounds keeps segment", Range{}, true},
	}
	for _, c := range cases {
		if got := segmentCouldMatch(seg, c.rng); got != c.want {
			t.Errorf("%s: segmentCouldMatch=%v want %v", c.name, got, c.want)
		}
	}
}

// TestPlanLateStoredBatchReconciles：整段历史晚落盘时，时间回放的
// matched/skipped 必须与留存记录总数对得上，不能静默丢段；
// 区间整体早于这批事件时 matched=0 也要逐条给出 outside_time 说明。
func TestPlanLateStoredBatchReconciles(t *testing.T) {
	src := "sLate"
	early := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	f := newFakeStore()
	const n = int64(50)
	for i := int64(1); i <= n; i++ {
		f.put(src, mkRecordAt(src, i, early.Add(time.Duration(i)*time.Second), late, storage.StatePending))
	}
	// 拆成两个已封存段，StoredAt 都是“晚落盘”。
	f.meta[src] = []storage.SegmentMeta{
		meta("seg-000001.dat", 1, 25, late.Unix(), late.Unix()),
		meta("seg-000002.dat", 26, 50, late.Unix(), late.Unix()),
	}
	m := newTestManager(t, f)
	sk := newFakeSink(0)
	m.RegisterSink("audit", sk)

	// 区间 [10,20] 秒：命中 seq10..20，共 11 条；其余全部 outside_time 可解释。
	from := early.Add(10 * time.Second)
	to := early.Add(20 * time.Second)
	prog, err := m.Start(context.Background(), StartRequest{
		JobID: "j1", Source: src, Target: "audit",
		Rng: Range{From: &from, To: &to},
	})
	if err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, m, "j1", StatusComplete)

	if prog.Plan.EffFromSeq != 1 || prog.Plan.EffToSeq != n {
		t.Fatalf("eff=[%d,%d], want [1,%d] (both segments retained)",
			prog.Plan.EffFromSeq, prog.Plan.EffToSeq, n)
	}
	if prog.Plan.Matched != 11 {
		t.Fatalf("matched=%d want 11", prog.Plan.Matched)
	}
	if got := int64(len(prog.Plan.Skipped)); got != n-11 {
		t.Fatalf("skipped=%d want %d", got, n-11)
	}
	for _, s := range prog.Plan.Skipped {
		if s.Kind != SkipOutsideTime {
			t.Fatalf("seq %d skip kind=%s want outside_time", s.Seq, s.Kind)
		}
	}
	if prog.Plan.Matched+int64(len(prog.Plan.Skipped)) != n {
		t.Fatalf("matched+skipped != retained: %d+%d != %d",
			prog.Plan.Matched, len(prog.Plan.Skipped), n)
	}
	if len(prog.Plan.Gaps) != 0 {
		t.Fatalf("unexpected gaps: %+v", prog.Plan.Gaps)
	}
	if final.Delivered != 11 || final.Remaining() != 0 {
		t.Fatalf("delivered=%d remaining=%d want 11/0", final.Delivered, final.Remaining())
	}
	got := sk.seqs()
	if len(got) != 11 || got[0] != 10 || got[10] != 20 {
		t.Fatalf("delivered=%v want seq 10..20 inclusive", got)
	}

	// 区间整体早于这批事件：matched=0，但全部记录以 outside_time 显式说明，任务完成。
	from2 := early.Add(-200 * time.Hour)
	to2 := early.Add(-100 * time.Hour)
	prog2, err := m.Start(context.Background(), StartRequest{
		JobID: "j2", Source: src, Target: "audit",
		Rng: Range{From: &from2, To: &to2},
	})
	if err != nil {
		t.Fatal(err)
	}
	final2 := waitStatus(t, m, "j2", StatusComplete)
	if prog2.Plan.Matched != 0 || int64(len(prog2.Plan.Skipped)) != n {
		t.Fatalf("before-batch: matched=%d skipped=%d want 0/%d",
			prog2.Plan.Matched, len(prog2.Plan.Skipped), n)
	}
	if final2.Delivered != 0 || final2.Status != StatusComplete {
		t.Fatalf("before-batch final=%+v want complete with 0 delivered", final2)
	}
}
