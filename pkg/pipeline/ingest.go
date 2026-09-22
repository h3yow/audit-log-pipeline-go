package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/router"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// IngestRaw 接入一段原始字节：非法格式返回 malformed 结论而非静默吞掉。
func (p *Pipeline) IngestRaw(ctx context.Context, source string, data []byte) (event.Decision, error) {
	e, err := event.ParseEvent(data)
	if err != nil {
		p.incMetric(func(m *Metrics) { m.Malformed++ })
		d := event.Decision{
			Kind:      event.DecisionMalformed,
			Source:    source,
			Reason:    err.Error(),
			Timestamp: time.Now(),
		}
		p.log.Warn("event rejected: malformed input",
			"source", source, "raw_len", len(data), "reason", err)
		return d, nil
	}
	return p.Ingest(ctx, e)
}

// Ingest 接入一条已解析事件，返回彼此可区分的处理结论：
// accepted / duplicate / rejected。
//
// 同一来源由独立互斥锁串行处理，保证来源内顺序稳定。
func (p *Pipeline) Ingest(ctx context.Context, e *event.Event) (event.Decision, error) {
	if err := e.Validate(); err != nil {
		p.incMetric(func(m *Metrics) { m.Malformed++ })
		d := event.Decision{
			Kind: event.DecisionMalformed, EventID: e.ID, Source: e.Source,
			Seq: e.Seq, Reason: err.Error(), Timestamp: time.Now(),
		}
		p.log.Warn("event rejected: invalid fields", "reason", err)
		return d, nil
	}

	p.mu.Lock()
	mu := p.ingestMu[e.Source]
	if mu == nil {
		mu = &sync.Mutex{}
		p.ingestMu[e.Source] = mu
	}
	p.mu.Unlock()

	mu.Lock()
	defer mu.Unlock()
	return p.processLocked(ctx, e)
}

// processLocked 调用时已持有该来源的接入锁。
func (p *Pipeline) processLocked(ctx context.Context, e *event.Event) (event.Decision, error) {
	// 1. 去重（内存索引，启动时由磁盘重建；磁盘是事实来源）。
	p.mu.Lock()
	seen, isDup := p.dedup[e.Source][e.ID]
	p.mu.Unlock()
	p.metricsMu.Lock()
	pendingDisk := p.metrics.PendingOnDisk
	p.metricsMu.Unlock()
	if isDup {
		p.incMetric(func(m *Metrics) { m.Duplicates++ })
		d := event.Decision{
			Kind: event.DecisionDuplicate, EventID: e.ID, Source: e.Source,
			Seq: e.Seq, Reason: "event id already stored",
			Timestamp: time.Now(),
		}
		p.log.Info("event duplicate",
			"event_input", logEvent(e),
			"existing_seq", seen,
			"decision", "duplicate")
		return d, nil
	}

	// 2. 磁盘级去重兜底（防止内存索引与磁盘不一致时漏判）。
	if exists, err := p.store.Has(e.Source, e.ID); err != nil {
		return event.Decision{}, err
	} else if exists {
		p.mu.Lock()
		p.dedup[e.Source][e.ID] = e.Seq
		p.mu.Unlock()
		p.incMetric(func(m *Metrics) { m.Duplicates++ })
		return event.Decision{
			Kind: event.DecisionDuplicate, EventID: e.ID, Source: e.Source,
			Seq: e.Seq, Reason: "event id found on disk", Timestamp: time.Now(),
		}, nil
	}

	// 3. 策略判定。
	res := p.cfg.Router.Decide(e)
	p.log.Info("policy evaluated",
		"event_input", logEvent(e),
		"rule", res.Rule, "action", string(res.Action),
		"route", res.Route, "reason", res.Reason)

	now := time.Now().Unix()
	rec := storage.Record{Event: e, StoredAt: now, Decision: res.Rule}
	if res.Action == router.ActionDeny {
		rec.State = storage.StateRejected
		rec.Decision = res.Reason
	} else {
		rec.State = storage.StatePending
		rec.Route = res.Route
	}

	// 4. 背压：磁盘待确认总量软上限（下游长期故障时磁盘有界且可观测）。
	//    拒绝事件不进入待投递队列，不受此限。
	if rec.State == storage.StatePending && pendingDisk >= p.cfg.MaxPendingDisk {
		return event.Decision{}, ErrBackpressure
	}

	// 5. 顺序落盘（Append 内部强制 seq 单调；乱序/重复序号显式报错）。
	if err := p.store.Append(rec); err != nil {
		var se *storage.SequenceError
		if asSequenceError(err, &se) {
			p.incMetric(func(m *Metrics) { m.Duplicates++ })
			return event.Decision{
				Kind: event.DecisionDuplicate, EventID: e.ID, Source: e.Source,
				Seq: e.Seq, Reason: err.Error(), Timestamp: time.Now(),
			}, nil
		}
		return event.Decision{}, err
	}

	// 6. 落盘成功后更新内存去重索引。
	p.mu.Lock()
	if p.dedup[e.Source] == nil {
		p.dedup[e.Source] = map[string]int64{}
	}
	p.dedup[e.Source][e.ID] = e.Seq
	p.mu.Unlock()

	if res.Action == router.ActionDeny {
		p.incMetric(func(m *Metrics) { m.Rejected++ })
		d := event.Decision{
			Kind: event.DecisionRejected, EventID: e.ID, Source: e.Source,
			Seq: e.Seq, Reason: res.Reason, Rule: res.Rule,
			Timestamp: time.Now(),
		}
		p.log.Info("event rejected by policy",
			"event_input", logEvent(e), "decision", "rejected", "rule", res.Rule)
		return d, nil
	}

	p.incMetric(func(m *Metrics) {
		m.Accepted++
		m.PendingOnDisk++
	})
	cp, _ := p.cp.Get(e.Source)
	p.log.Info("event accepted",
		"event_input", logEvent(e),
		"decision", "accepted", "route", res.Route,
		"checkpoint_watermark", cp.Watermark)

	p.ensureWorker(e.Source)
	p.wakeWorker(e.Source)
	return event.Decision{
		Kind: event.DecisionAccepted, EventID: e.ID, Source: e.Source,
		Seq: e.Seq, Route: res.Route, Rule: res.Rule, Reason: res.Reason,
		Timestamp: time.Now(),
	}, nil
}

// AddFreeze / RemoveFreeze 转发到存储层。
func (p *Pipeline) AddFreeze(f storage.Freeze) error {
	p.log.Info("freeze added", "id", f.ID, "source", f.Source,
		"from", f.FromSeq, "to", f.ToSeq, "until", f.Until)
	return p.store.AddFreeze(f)
}

func (p *Pipeline) RemoveFreeze(id string) error {
	p.log.Info("freeze removed", "id", id)
	return p.store.RemoveFreeze(id)
}

// PurgeNow 立即触发一次清理（测试与手动运维用）。
func (p *Pipeline) PurgeNow(now time.Time) (storage.PurgeResult, error) {
	return p.store.Purge(now.Unix())
}

// CheckpointWatermark 暴露检查点水位（测试断言/可观测）。
func (p *Pipeline) CheckpointWatermark(source string) int64 {
	st, err := p.cp.Get(source)
	if err != nil {
		return -1
	}
	return st.Watermark
}
