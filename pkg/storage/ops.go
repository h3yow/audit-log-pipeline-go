package storage

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
)

// Append 将一条事件记录按来源顺序追加落盘并 fsync。
// 同一来源必须以单调递增序号追加；重复序号被显式拒绝。
func (s *Store) Append(rec Record) error {
	if rec.Event == nil {
		return errors.New("storage: nil event")
	}
	e := rec.Event
	if err := e.Validate(); err != nil {
		return err
	}
	now := rec.StoredAt
	if now == 0 {
		now = time.Now().Unix()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.ensureSource(e.Source)
	if err != nil {
		return err
	}
	last := int64(0)
	if st.active != nil {
		last = st.active.MaxSeq
	}
	if e.Seq <= last {
		return &SequenceError{Source: e.Source, Got: e.Seq, Last: last}
	}
	if st.active == nil || st.active.Size >= s.maxSeg {
		if err := s.rotate(st); err != nil {
			return err
		}
		last = 0
		if prior := len(st.man.Entries); prior >= 2 {
			last = st.man.Entries[prior-2].MaxSeq
		}
		if e.Seq <= last {
			return &SequenceError{Source: e.Source, Got: e.Seq, Last: last}
		}
	}
	evb, err := e.Marshal()
	if err != nil {
		return err
	}
	mac, err := writeFrame(st.f, s.key, st.tail, recordBody{
		Event: evb, Route: rec.Route, StoredAt: now, Seq: e.Seq,
		State: string(rec.State), Decision: rec.Decision,
	})
	if err != nil {
		return err
	}
	if err := st.f.Sync(); err != nil {
		return err
	}
	entryFirst := st.active.Records == 0
	if entryFirst {
		st.active.MinSeq = e.Seq
		st.active.FirstStored = now
	}
	st.active.MaxSeq = e.Seq
	st.active.LastStored = now
	st.active.Records++
	st.active.Size = 0 // 大小定期由扫描/封存校准；此处用文件偏移更准
	if fi, err := st.f.Stat(); err == nil {
		st.active.Size = fi.Size()
	}
	st.tail = mac
	if err := s.saveManifest(st); err != nil {
		return err
	}
	return nil
}

// SequenceError 表示非单调序号追加（重复/乱序）。
type SequenceError struct {
	Source string
	Got    int64
	Last   int64
}

func (e *SequenceError) Error() string {
	return "storage: out-of-order sequence for " + e.Source
}

// LastSeq 返回来源已持久化的最大序号；无记录返回 0。
func (s *Store) LastSeq(source string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, err := s.ensureSource(source)
	if err != nil {
		return 0, err
	}
	if st.active != nil {
		return st.active.MaxSeq, nil
	}
	return 0, nil
}

// Get 按来源+序号读出单条记录；已清理/不存在返回 ErrNotFound。
func (s *Store) Get(source string, seq int64) (*RecordView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, err := s.ensureSource(source)
	if err != nil {
		return nil, err
	}
	for _, e := range st.man.Entries {
		if e.Deleted || seq < e.MinSeq || seq > e.MaxSeq {
			continue
		}
		if e == st.active {
			// 活动分段直接在锁外不可重入；这里顺序读取。
			if err := st.f.Sync(); err != nil {
				return nil, err
			}
		}
		rv, ok, err := readOne(filepath.Join(st.dir, e.Name), s.key, seq)
		if err != nil {
			return nil, err
		}
		if ok {
			return &rv, nil
		}
	}
	return nil, ErrNotFound
}

func readOne(path string, key []byte, seq int64) (RecordView, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return RecordView{}, false, err
	}
	defer f.Close()
	br := bufio.NewReader(f)
	prev := make([]byte, macSize)
	for {
		b, mac, err := readFrame(br, key, prev)
		if err == io.EOF {
			return RecordView{}, false, nil
		}
		if err != nil {
			return RecordView{}, false, mapFrameErr(err)
		}
		if b.Seq == seq {
			rv, err := bodyToView(b)
			return rv, true, err
		}
		prev = mac
	}
}

func bodyToView(b recordBody) (RecordView, error) {
	ev, err := eventFromJSON(b.Event)
	if err != nil {
		return RecordView{}, err
	}
	return RecordView{
		Seq: b.Seq,
		Record: Record{
			Event: ev, Route: b.Route, StoredAt: b.StoredAt,
			State: DeliveryState(b.State), Decision: b.Decision,
		},
	}, nil
}

// Scan 在某来源的序号闭区间内顺序扫描，回调返回 false 停止。
// 已合法清理的分段不会出现；任何哈希链断裂立即返回 ErrTampered。
func (s *Store) Scan(source string, fromSeq, toSeq int64, fn func(RecordView) bool) error {
	if fromSeq <= 0 {
		fromSeq = 1
	}
	if toSeq < fromSeq {
		return nil
	}
	s.mu.RLock()
	snapshot := make([]*segEntry, 0)
	dir := s.sourceDir(source)
	st, ok := s.srcs[source]
	if ok {
		dir = st.dir
		// 深拷贝条目：Append 会原地更新活动条目的字段并追加切片，
		// Scan 全程在锁外读文件，必须持有不可变快照避免数据竞争。
		for _, e := range st.man.Entries {
			cp := *e
			snapshot = append(snapshot, &cp)
		}
	}
	key := s.key
	s.mu.RUnlock()

	entries := snapshot
	if len(entries) == 0 {
		// 全新来源：尝试加载清单。
		man, err := s.loadManifest(source, dir)
		if err != nil {
			return err
		}
		entries = man.Entries
	}

	for _, e := range entries {
		prev := make([]byte, macSize) // 每个分段独立起链
		if e.Deleted || toSeq < e.MinSeq || fromSeq > e.MaxSeq {
			continue
		}
		f, err := os.Open(filepath.Join(dir, e.Name))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return ErrTampered
			}
			return err
		}
		br := bufio.NewReader(f)
		stop := false
		for {
			b, mac, err := readFrame(br, key, prev)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return mapFrameErr(err)
			}
			if b.Seq >= fromSeq && b.Seq <= toSeq {
				rv, err := bodyToView(b)
				if err != nil {
					f.Close()
					return err
				}
				if !fn(rv) {
					stop = true
					break
				}
			}
			if b.Seq > toSeq {
				stop = true
				break
			}
			prev = mac
		}
		f.Close()
		if stop {
			return nil
		}
	}
	return nil
}

// Has 判断来源内某事件 ID 是否已存在（重启后重建去重索引）。
func (s *Store) Has(source, eventID string) (bool, error) {
	found := false
	err := s.Scan(source, 1, 1<<62, func(rv RecordView) bool {
		if rv.Record.Event.ID == eventID {
			found = true
			return false
		}
		return true
	})
	return found, err
}

// Sources 返回全部已知来源（清单存在且至少有一条未清理记录的目录）。
func (s *Store) Sources() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.srcs))
	for name := range s.srcs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// Stats 返回可观测指标。
func (s *Store) Stats() (Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var st Stats
	now := time.Now()
	for _, fr := range s.freezes {
		if fr.Until.IsZero() || fr.Until.After(now) {
			st.FrozenSources++
		}
	}
	for _, src := range s.srcs {
		for _, e := range src.man.Entries {
			if e.Deleted {
				continue
			}
			st.SegmentCount++
			st.RecordCount += int64(e.Records)
			if fi, err := os.Stat(filepath.Join(src.dir, e.Name)); err == nil {
				st.BytesOnDisk += fi.Size()
			}
		}
	}
	return st, nil
}

func mapFrameErr(err error) error {
	switch {
	case errors.Is(err, errFrameMAC), errors.Is(err, errFrameMagic), errors.Is(err, errFrameVersion):
		return ErrTampered
	default:
		return err
	}
}

var _ = event.Event{}
