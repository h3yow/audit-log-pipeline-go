package replay

import (
	"context"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// fakeStore 是内存版 Store：可控故障、分段元数据与记录。
type fakeStore struct {
	mu       sync.Mutex
	records  map[string]map[int64]storage.RecordView
	meta     map[string][]storage.SegmentMeta
	holds    map[string][2]int64
	scanHook func() // 每次 Scan 回调前触发（并发测试注入清理）
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		records: map[string]map[int64]storage.RecordView{},
		meta:    map[string][]storage.SegmentMeta{},
		holds:   map[string][2]int64{},
	}
}

func (f *fakeStore) put(src string, rv storage.RecordView) {
	if f.records[src] == nil {
		f.records[src] = map[int64]storage.RecordView{}
	}
	f.records[src][rv.Seq] = rv
}

func (f *fakeStore) ReplaySnapshot(source string) (storage.ReplaySnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	metas := f.meta[source]
	cp := make([]storage.SegmentMeta, len(metas))
	copy(cp, metas)
	return storage.ReplaySnapshot{Source: source, Segments: cp}, nil
}

func (f *fakeStore) Scan(source string, fromSeq, toSeq int64, fn func(storage.RecordView) bool) error {
	f.mu.Lock()
	recs := f.records[source]
	seqs := make([]int64, 0, len(recs))
	for s := range recs {
		if s >= fromSeq && s <= toSeq {
			seqs = append(seqs, s)
		}
	}
	views := make([]storage.RecordView, 0, len(seqs))
	for _, s := range seqs {
		views = append(views, recs[s])
	}
	hook := f.scanHook
	f.mu.Unlock()
	sort.Slice(views, func(i, j int) bool { return views[i].Seq < views[j].Seq })
	for _, rv := range views {
		if hook != nil {
			hook()
		}
		if !fn(rv) {
			break
		}
	}
	return nil
}

func (f *fakeStore) ReplayHoldAdd(jobID, source string, fromSeq, toSeq int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.holds[jobID] = [2]int64{fromSeq, toSeq}
}

func (f *fakeStore) ReplayHoldRemove(jobID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.holds, jobID)
}

// fakeSink 记录可见 (source,seq)，可注入前 N 次失败。
type fakeSink struct {
	mu        sync.Mutex
	got       []int64 // 投递顺序
	seen      map[[2]any]bool
	failLeft  int
	deliverMu chan struct{} // 可选：投递时阻塞控制
}

func newFakeSink(failFirst int) *fakeSink {
	return &fakeSink{seen: map[[2]any]bool{}, failLeft: failFirst}
}

func (s *fakeSink) Deliver(ctx context.Context, route string, e *event.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.failLeft > 0 {
		s.failLeft--
		s.mu.Unlock()
		return errDown
	}
	key := [2]any{e.Source, e.Seq}
	if !s.seen[key] {
		s.seen[key] = true
		s.got = append(s.got, e.Seq)
	}
	s.mu.Unlock()
	return nil
}

func (s *fakeSink) seqs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, len(s.got))
	copy(out, s.got)
	return out
}

var errDown = sterr("sink unavailable")

type sterr string

func (e sterr) Error() string { return string(e) }

func mkRecord(src string, seq int64, state storage.DeliveryState) storage.RecordView {
	return storage.RecordView{Seq: seq, Record: storage.Record{
		Event: &event.Event{
			ID: "id-" + src + "-" + seqStr(seq), Source: src, Seq: seq,
			Type: "login", Action: "do",
			Timestamp: time.Date(2026, 1, 1, 0, 0, int(seq), 0, time.UTC),
		},
		Route: "audit", State: state, StoredAt: time.Date(2026, 1, 1, 0, 0, int(seq), 0, time.UTC).Unix(),
	}}
}

func seqStr(n int64) string {
	return strconv.FormatInt(n, 10)
}

func newTestManager(t *testing.T, f *fakeStore) *Manager {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "replay")
	// key 文件：写 32 字节
	kdir := filepath.Join(t.TempDir(), "k")
	_ = kdir
	keyPath := filepath.Join(t.TempDir(), "key.bin")
	if err := writeKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	m, err := Open(dir, keyPath, f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func writeKeyFile(p string) error {
	return writeFileAtomicReplay(p, make([]byte, 32))
}
