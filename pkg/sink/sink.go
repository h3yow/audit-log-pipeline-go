package sink

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
)

// ErrUnavailable 表示下游当前不可用（投递应重试，不推进检查点）。
var ErrUnavailable = errors.New("sink: downstream unavailable")

// Sink 是下游投递目标。
type Sink interface {
	Name() string
	// Deliver 投递一条事件。下游按 (source, seq) 做幂等。
	Deliver(ctx context.Context, route string, e *event.Event) error
}

// MemorySink 是内存模拟下游：支持注入故障、记录可见事件、按 key 幂等。
type MemorySink struct {
	mu        sync.Mutex
	available bool
	// delivered: route -> source -> seq -> event
	delivered map[string]map[string]map[int64]*event.Event
	failCount int
}

// NewMemorySink 构造一个默认可用的内存下游。
func NewMemorySink() *MemorySink {
	return &MemorySink{
		available: true,
		delivered: map[string]map[string]map[int64]*event.Event{},
	}
}

func (s *MemorySink) Name() string { return "memory" }

// Deliver 幂等投递。重复的 (route, source, seq) 直接返回成功且不产生
// 第二条可见记录；不可用时返回 ErrUnavailable 且不留任何可见状态。
func (s *MemorySink) Deliver(ctx context.Context, route string, e *event.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.available {
		s.failCount++
		return ErrUnavailable
	}
	byRoute, ok := s.delivered[route]
	if !ok {
		byRoute = map[string]map[int64]*event.Event{}
		s.delivered[route] = byRoute
	}
	bySource, ok := byRoute[e.Source]
	if !ok {
		bySource = map[int64]*event.Event{}
		byRoute[e.Source] = bySource
	}
	if _, dup := bySource[e.Seq]; dup {
		return nil // 幂等：重试不产生重复可见事件
	}
	cp := *e
	bySource[e.Seq] = &cp
	return nil
}

// SetAvailable 注入/解除故障。
func (s *MemorySink) SetAvailable(ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = ok
}

// FailFor 在一段时间窗口内模拟故障（测试用）。
func (s *MemorySink) FailFor(d time.Duration) {
	s.SetAvailable(false)
	time.AfterFunc(d, func() { s.SetAvailable(true) })
}

// Visible 返回某路由下、某来源按序号排序的可见事件 ID。
func (s *MemorySink) Visible(route, source string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var seqs []int64
	if byRoute := s.delivered[route]; byRoute != nil {
		for seq := range byRoute[source] {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	out := make([]string, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, s.delivered[route][source][seq].ID)
	}
	return out
}

// VisibleCount 返回可见事件总数。
func (s *MemorySink) VisibleCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, byRoute := range s.delivered {
		for _, bySource := range byRoute {
			n += len(bySource)
		}
	}
	return n
}

// FailCount 返回累计投递失败次数（可观测）。
func (s *MemorySink) FailCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failCount
}

// RouterSink 按路由名分发到不同 Sink，未配置路由显式报错。
type RouterSink struct {
	sinks map[string]Sink
}

// NewRouterSink 构造路由分发器。
func NewRouterSink(sinks map[string]Sink) *RouterSink {
	return &RouterSink{sinks: sinks}
}

func (r *RouterSink) Deliver(ctx context.Context, route string, e *event.Event) error {
	s, ok := r.sinks[route]
	if !ok {
		return errors.New("sink: no sink for route " + route)
	}
	return s.Deliver(ctx, route, e)
}
