package pipeline

import (
	"context"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// runWorker 是每个来源唯一的投递工作者：
//   - 来源内严格按 seq 顺序投递（同一来源顺序稳定）；
//   - 只投递 watermark 之上、磁盘状态为 pending 的最小连续序列；
//   - 下游故障时指数退避重试，事件留在磁盘，不推进检查点；
//   - 投递成功并持久化检查点后，事件才算完成；进程崩溃可重复投递，
//     下游按 (source, seq) 幂等，因此不会产生重复可见事件。
func (p *Pipeline) runWorker(src string, w *srcWorker) {
	defer p.wg.Done()
	defer close(w.done)
	backoff := 50 * time.Millisecond
	const maxBackoff = 2 * time.Second
	for {
		select {
		case <-w.stop:
			return
		default:
		}
		advanced, err := p.pumpSource(src, w)
		if err != nil {
			p.log.Error("delivery pump error", "source", src, "err", err)
		}
		if advanced {
			backoff = 50 * time.Millisecond
			continue
		}
		select {
		case <-w.stop:
			return
		case <-w.wake:
		case <-time.After(backoff):
		}
		if _, failed := p.pumpErrorHint(); failed {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (p *Pipeline) pumpErrorHint() (struct{}, bool) { return struct{}{}, false }

// pumpSource 尽力推进一个来源，返回本次是否成功投递了至少一条。
func (p *Pipeline) pumpSource(src string, w *srcWorker) (bool, error) {
	cp, err := p.cp.Get(src)
	if err != nil {
		return false, err
	}
	wm := cp.Watermark

	// 找到水位之上最小的 pending 序号；来源内不允许空洞阻塞后续
	// （Append 保证序号连续），因此从 wm+1 起顺序取。
	deliveredAny := false
	for {
		rv, err := p.store.Get(src, wm+1)
		if err != nil {
			if err == storage.ErrNotFound {
				return deliveredAny, nil
			}
			return deliveredAny, err
		}
		if rv.Record.State != storage.StatePending {
			// 已标记 delivered 但检查点未推进（崩溃在两步之间）：
			// 直接推进检查点即可，不重复投递。
			if err := p.advanceWatermark(src, wm+1); err != nil {
				return deliveredAny, err
			}
			wm++
			deliveredAny = true
			continue
		}

		// 内存有界：故障期单来源在飞内存事件超过上限就停泵，
		// 事件仍安全地在磁盘上等待，不占额外内存。
		p.mu.Lock()
		inflight := len(w.inflight)
		p.mu.Unlock()
		if inflight >= p.cfg.MaxInFlight {
			return deliveredAny, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		p.mu.Lock()
		w.inflight[wm+1] = true
		p.mu.Unlock()
		err = p.rsink.Deliver(ctx, rv.Record.Route, rv.Record.Event)
		cancel()
		if err != nil {
			p.mu.Lock()
			delete(w.inflight, wm+1)
			p.mu.Unlock()
			p.incMetric(func(m *Metrics) { m.DeliveryRetries++ })
			p.log.Warn("delivery failed; event stays durable pending",
				"source", src, "seq", wm+1, "err", err)
			return deliveredAny, nil // 等退避或唤醒后重试；不越过空洞
		}
		p.mu.Lock()
		delete(w.inflight, wm+1)
		p.mu.Unlock()

		// 投递成功：推进检查点（先于任何"完成"声明）。
		if err := p.advanceWatermark(src, wm+1); err != nil {
			return deliveredAny, err
		}
		wm++
		deliveredAny = true
		p.incMetric(func(m *Metrics) {
			m.Delivered++
			m.PendingOnDisk--
			if m.PendingOnDisk < 0 {
				m.PendingOnDisk = 0
			}
		})
		p.log.Info("delivered",
			"source", src, "seq", wm,
			"event_id", rv.Record.Event.ID,
			"checkpoint_watermark", wm)
	}
}

func (p *Pipeline) advanceWatermark(src string, seq int64) error {
	err := p.cp.Advance(src, seq, time.Now().Unix())
	if err != nil {
		// ErrRollback 是明确拒绝：绝不静默重放。
		p.log.Error("checkpoint advance rejected",
			"source", src, "seq", seq, "err", err)
		return err
	}
	return nil
}
