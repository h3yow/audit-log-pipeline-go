package replay

import (
	"context"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// Store 是回放所需的存储能力（由 storage.Store 实现）：
//   - ReplaySnapshot：在存储锁内取清单不可变快照，用于范围静态判定；
//   - Scan：锁外顺序读帧，大批量回放内存有界；
//   - ReplayHoldAdd/Remove：注册/释放回放保留占位，阻止清理范围内分段。
type Store interface {
	ReplaySnapshot(source string) (storage.ReplaySnapshot, error)
	Scan(source string, fromSeq, toSeq int64, fn func(storage.RecordView) bool) error
	ReplayHoldAdd(jobID, source string, fromSeq, toSeq int64)
	ReplayHoldRemove(jobID string)
}

// Deliverer 是回放目标下游的投递能力（由 sink.Sink 实现）。
// 下游必须按 (source, seq) 幂等：故障重试与重启续传都依赖该契约
// 保证不产生重复可见事件。
type Deliverer interface {
	Deliver(ctx context.Context, route string, e *event.Event) error
}

var _ Deliverer = sink.Sink(nil)
