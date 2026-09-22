package pipeline

import (
	"context"
	"sync"

	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// recover 在启动时执行一致性恢复：
//  1. 枚举全部来源，顺序扫描磁盘重建内存去重索引（磁盘是事实来源）；
//  2. 统计每个来源待投递（pending）序号；
//  3. 对比检查点水位：磁盘存在 < watermark 的 pending 记录属于异常暴露状态，
//     但检查点回退本身（watermark 与磁盘链不一致）由 checkpoint 包拒绝；
//  4. 为每个来源启动工作者，优先重放 pending 事件（下游幂等，不产生重复可见）。
func (p *Pipeline) recover() error {
	sources, err := p.store.Sources()
	if err != nil {
		return err
	}
	for _, src := range sources {
		p.dedup[src] = map[string]int64{}
		pending := map[int64]bool{}
		err := p.store.Scan(src, 1, 1<<62, func(rv storage.RecordView) bool {
			p.dedup[src][rv.Record.Event.ID] = rv.Seq
			if rv.Record.State == storage.StatePending {
				pending[rv.Seq] = true
			}
			return true
		})
		if err != nil {
			return err
		}
		st, err := p.cp.Get(src)
		if err != nil {
			return err
		}
		// 检查点水位以下不应再有 pending；若有说明上次投递确认与落盘状态脱节，
		// 显式记录并把这些事件重新纳入投递（下游幂等保证不重复可见）。
		var recoverList []int64
		for seq := range pending {
			if seq > st.Watermark {
				recoverList = append(recoverList, seq)
			}
		}
		p.log.Info("recovery: source loaded",
			"source", src,
			"known_events", len(p.dedup[src]),
			"checkpoint_watermark", st.Watermark,
			"pending_to_resume", len(recoverList))
		p.ensureWorker(src)
		p.incMetric(func(m *Metrics) { m.PendingOnDisk += int64(len(recoverList)) })
		p.wakeWorker(src)
	}
	return nil
}

func (p *Pipeline) ensureWorker(src string) *srcWorker {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ensureWorkerLocked(src)
}

func (p *Pipeline) ensureWorkerLocked(src string) *srcWorker {
	if w, ok := p.workers[src]; ok {
		return w
	}
	if _, ok := p.ingestMu[src]; !ok {
		p.ingestMu[src] = &sync.Mutex{}
	}
	if _, ok := p.dedup[src]; !ok {
		p.dedup[src] = map[string]int64{}
	}
	w := &srcWorker{
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		inflight: map[int64]bool{},
	}
	p.workers[src] = w
	p.wg.Add(1)
	go p.runWorker(src, w)
	return w
}

func (p *Pipeline) wakeWorker(src string) {
	p.mu.Lock()
	w := p.workers[src]
	p.mu.Unlock()
	if w == nil {
		return
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

var _ = context.Background
