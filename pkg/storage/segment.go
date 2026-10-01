package storage

import (
	"errors"
	"io"
	"os"
)

// errTornFrame 表示分段尾部存在写一半的帧（进程崩溃），可安全截断。
var errTornFrame = errors.New("storage: torn trailing frame")

// countingReader 统计已消费字节数。
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

type segScanResult struct {
	goodBytes   int64 // 最后一个完整帧结束时的字节偏移
	tail        []byte
	records     int
	minSeq      int64
	maxSeq      int64
	firstStored int64
	lastStored  int64
	minEventTS  int64 // 段内事件自身时间戳最小值（unix 纳秒）；0 表示未知
	maxEventTS  int64 // 段内事件自身时间戳最大值（unix 纳秒）；0 表示未知
}

// scanSegment 从头校验整个分段。
// 完整读完返回 (r, nil)；尾部半帧返回 (r, errTornFrame)；
// MAC/魔数/序号错乱返回 ErrTampered（不可自动修复）。
func scanSegment(path string, key []byte, strictSeq bool) (segScanResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return segScanResult{}, err
	}
	defer f.Close()

	cr := &countingReader{r: f}
	r := segScanResult{tail: make([]byte, macSize)}
	var prevSeq int64
	first := true
	tsUnknown := false // 任一记录事件时间不可解析：整段时间界置未知
	for {
		frameStart := cr.n
		b, mac, ferr := readFrame(cr, key, r.tail)
		if ferr == io.EOF && cr.n == frameStart {
			return r, nil
		}
		if ferr != nil {
			if errors.Is(ferr, io.EOF) || errors.Is(ferr, io.ErrUnexpectedEOF) {
				return r, errTornFrame
			}
			if errors.Is(ferr, errFrameMAC) || errors.Is(ferr, errFrameMagic) ||
				errors.Is(ferr, errFrameVersion) {
				return r, ErrTampered
			}
			return r, ferr
		}
		if strictSeq {
			if !first && b.Seq != prevSeq+1 {
				return r, ErrTampered
			}
		} else if !first && b.Seq <= prevSeq {
			return r, ErrTampered
		}
		if first {
			r.minSeq = b.Seq
			r.firstStored = b.StoredAt
			first = false
		}
		r.maxSeq = b.Seq
		r.lastStored = b.StoredAt
		r.records++
		// 事件自身时间界：回放按事件时间判定命中，段级剪枝只能用这个界，
		// 绝不能用落盘时间（迟到数据的落盘时间远晚于事件时间）。
		// 任一记录解析失败则整段置未知（0），回放侧对未知界一律不剪枝。
		if !tsUnknown {
			ev, perr := eventFromJSON(b.Event)
			if perr != nil || ev.Timestamp.IsZero() {
				r.minEventTS, r.maxEventTS = 0, 0
				tsUnknown = true
			} else {
				ts := ev.Timestamp.UnixNano()
				if r.minEventTS == 0 || ts < r.minEventTS {
					r.minEventTS = ts
				}
				if ts > r.maxEventTS {
					r.maxEventTS = ts
				}
			}
		}
		r.goodBytes = cr.n
		r.tail = mac
		prevSeq = b.Seq
	}
}
