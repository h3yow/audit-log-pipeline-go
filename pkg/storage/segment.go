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
		r.goodBytes = cr.n
		r.tail = mac
		prevSeq = b.Seq
	}
}
