package replay

import (
	"context"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// SinkAdapter 把实时投递侧的 sink.Sink 适配为回放投递接口。
// 回放走独立路由名（如 "replay"），实时与回放在下游中天然隔离；
// 底层内存下游按 (route, source, seq) 幂等，重试/重启不产生重复可见事件。
type SinkAdapter struct {
	routes map[string]sink.Sink
}

// NewSinkAdapter 用路由表构造适配器。
func NewSinkAdapter(routes map[string]sink.Sink) *SinkAdapter {
	return &SinkAdapter{routes: routes}
}

// HasReplayRoute 供 Manager.Start 做 fail-closed 配置校验。
func (a *SinkAdapter) HasReplayRoute(route string) bool {
	_, ok := a.routes[route]
	return ok
}

// DeliverReplayRecord 投递一条回放记录；5s 投递超时，失败交由上层重试。
func (a *SinkAdapter) DeliverReplayRecord(route string, rv storage.RecordView) error {
	s, ok := a.routes[route]
	if !ok {
		return sink.ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Deliver(ctx, route, rv.Record.Event)
}
