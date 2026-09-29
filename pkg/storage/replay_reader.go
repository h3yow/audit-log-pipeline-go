package storage

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// replayReader 顺序流式读取某来源序号闭区间内的留存记录，
// 每次 Next 只反序列化一条，不把整个区间载入内存。
//
// 并发安全要点：
//   - 构造时在 RLock 内深拷贝不可变分段快照（含分段字节上界 Size），
//     随后在锁外逐段读文件；Purge 全程持写锁，故读取期间快照里
//     的分段文件不会被删除；
//   - 每个分段以快照 Size 为读取上限（io.LimitReader）：活动分段的
//     Size 只在一帧完整写入并 fsync 后才在锁内推进，因此读不到
//     追加进行中"写了一半"的帧；
//   - 逐帧校验哈希链：清到一半/篡改一律 ErrTampered，不静默跳过。
type replayReader struct {
	dir     string
	key     []byte
	entries []*segEntry
	fromSeq int64
	toSeq   int64

	idx  int      // 当前分段下标
	f    *os.File // 当前分段句柄
	br   *bufio.Reader
	prev []byte // 当前分段内前一帧 MAC
	seq  int64  // 当前分段内已读到的最大序号
	done bool
}

// OpenReplay 打开区间读取器；若区间命中已被清理的分段，返回
// ErrPurgedGap（绝不静默跳过已删数据）。
func (s *Store) OpenReplay(source string, fromSeq, toSeq int64) (RangeReader, error) {
	if fromSeq <= 0 {
		fromSeq = 1
	}
	if toSeq < fromSeq {
		return &replayReader{done: true}, nil
	}
	s.mu.RLock()
	st, ok := s.srcs[source]
	if !ok {
		s.mu.RUnlock()
		return &replayReader{done: true}, nil
	}
	snapshot := make([]*segEntry, 0, len(st.man.Entries))
	var gap bool
	for _, e := range st.man.Entries {
		cp := *e
		snapshot = append(snapshot, &cp)
		if cp.Deleted && cp.MinSeq <= toSeq && fromSeq <= cp.MaxSeq {
			gap = true
		}
	}
	dir := st.dir
	key := s.key
	s.mu.RUnlock()
	if gap {
		return nil, ErrPurgedGap
	}
	return &replayReader{
		dir: dir, key: key, entries: snapshot,
		fromSeq: fromSeq, toSeq: toSeq,
		prev: make([]byte, macSize),
	}, nil
}

// ErrPurgedGap 表示读取区间命中了已被合法清理的分段。
var ErrPurgedGap = errors.New("storage: replay range intersects purged segment")

// RangeReader 是流式区间读取接口（由 replay 包通过 EventStore 消费）。
type RangeReader interface {
	Next() (RecordView, bool, error)
	Close() error
}

func (r *replayReader) Close() error {
	if r.f != nil {
		err := r.f.Close()
		r.f = nil
		return err
	}
	return nil
}

// nextSegment 定位下一个可能相交且未删除的分段并打开它，
// 读取字节数以快照 Size 为上限（杜绝并发追加的半截帧）。
func (r *replayReader) nextSegment() error {
	if r.f != nil {
		_ = r.f.Close()
		r.f = nil
	}
	for r.idx < len(r.entries) {
		e := r.entries[r.idx]
		r.idx++
		if e.Deleted || r.toSeq < e.MinSeq || r.fromSeq > e.MaxSeq {
			continue
		}
		f, err := os.Open(filepath.Join(r.dir, e.Name))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return ErrTampered
			}
			return err
		}
		r.f = f
		r.br = bufio.NewReader(io.LimitReader(f, e.Size))
		r.prev = make([]byte, macSize)
		r.seq = 0
		return nil
	}
	r.done = true
	return nil
}

func (r *replayReader) Next() (RecordView, bool, error) {
	if r.done {
		return RecordView{}, false, nil
	}
	if r.f == nil {
		if err := r.nextSegment(); err != nil {
			return RecordView{}, false, err
		}
		if r.done {
			return RecordView{}, false, nil
		}
	}
	for {
		b, mac, err := readFrame(r.br, r.key, r.prev)
		if err == io.EOF {
			// 到达本分段快照上界或文件末尾：进入下一个候选分段。
			if err := r.nextSegment(); err != nil {
				return RecordView{}, false, err
			}
			if r.done {
				return RecordView{}, false, nil
			}
			continue
		}
		if err != nil {
			// 上限内出现半截帧或 MAC/魔数错误：数据被改写或清单被伪造，
			// 一律按篡改处理，绝不静默跳过。
			return RecordView{}, false, mapFrameErr(err)
		}
		if b.Seq <= r.seq {
			return RecordView{}, false, ErrTampered // 分段内序号必须严格递增
		}
		r.seq = b.Seq
		r.prev = mac
		if b.Seq < r.fromSeq {
			continue
		}
		if b.Seq > r.toSeq {
			r.done = true
			return RecordView{}, false, nil
		}
		rv, err := bodyToView(b)
		if err != nil {
			return RecordView{}, false, err
		}
		return rv, true, nil
	}
}
