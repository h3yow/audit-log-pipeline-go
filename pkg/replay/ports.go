package replay

import "github.com/h3yow/audit-log-pipeline-go/pkg/storage"

// EventStore 是回放所需的存储能力（由 *storage.Store 实现）。
type EventStore interface {
	// OpenReplay 打开一个区间流式读取器（受回放保留锁保护）。
	OpenReplay(source string, fromSeq, toSeq int64) (storage.RangeReader, error)
	// LastSeq 返回来源已持久化的最大序号。
	LastSeq(source string) (int64, error)
	// AcquireReplayHold 原子地注册保留锁并返回相交的已清理缺口
	// （空切片表示区间内数据完整）。注册与缺口检查在同一把锁内完成。
	AcquireReplayHold(jobID, source string, fromSeq, toSeq int64) ([][2]int64, error)
	// ReleaseReplayHold 释放保留锁。
	ReleaseReplayHold(jobID string) error
	// PurgedGapsInRange 枚举与区间相交的已清理序号区间（失败原因解释用）。
	PurgedGapsInRange(source string, fromSeq, toSeq int64) ([][2]int64, error)
}

// DeliveringSink 投递单条回放记录到独立目标下游。
// 实现必须按 (route, source, seq) 幂等：同一键重试不产生第二条可见记录。
type DeliveringSink interface {
	DeliverReplayRecord(route string, rv storage.RecordView) error
}
