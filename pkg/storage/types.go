package storage

import (
	"errors"
	"time"
)

// ErrTampered 表示落盘记录的哈希链校验失败：数据被改写、删除或乱序插入。
var ErrTampered = errors.New("storage: tampered segment or manifest detected")

// ErrNotFound 表示记录不存在或已被清理。
var ErrNotFound = errors.New("storage: record not found")

// Retention 描述保留策略：仅清理末条记录早于 MaxAge 的已封存分段。
type Retention struct {
	MaxAge time.Duration
}

// Freeze 描述一个合规冻结范围。命中范围的记录禁止清理。
type Freeze struct {
	ID        string    `json:"id"`
	Source    string    `json:"source"` // 来源；为空表示冻结全部来源
	FromSeq   int64     `json:"from_seq"`
	ToSeq     int64     `json:"to_seq"` // 0 表示开放上界（到尾部）
	Until     time.Time `json:"until"`
	Note      string    `json:"note"`
	CreatedAt time.Time `json:"created_at"`
}

// Stats 是可观测的磁盘/记录指标。
type Stats struct {
	BytesOnDisk   int64 `json:"bytes_on_disk"`
	SegmentCount  int   `json:"segment_count"`
	RecordCount   int64 `json:"record_count"`
	FrozenSources int   `json:"frozen_sources"`
}

// Options 构造本地存储。
type Options struct {
	Dir       string
	Retention Retention
	// MaxSegmentBytes 超过该大小滚动新分段。
	MaxSegmentBytes int64
}

// PurgeResult 描述一次清理的结果。
type PurgeResult struct {
	DeletedSegments int   `json:"deleted_segments"`
	DeletedRecords  int64 `json:"deleted_records"`
	ReclaimedBytes  int64 `json:"reclaimed_bytes"`
	SkippedFrozen   int   `json:"skipped_frozen"`
}
