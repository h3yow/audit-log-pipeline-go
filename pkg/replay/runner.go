package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// Start 静态判定范围、注册保留占位、持久化初始进度并启动回放工作者。
// 同一 JobID 重复请求返回 ErrExists；进度倒退/复刻由 loadAll 与 saveProgress 拒绝。
func (m *Manager) Start(ctx context.Context, req StartRequest) (*Progress, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.JobID == "" {
		return nil, fmt.Errorf("%w: job id required", ErrInvalidRequest)
	}
	if req.Target == "" {
		return nil, fmt.Errorf("%w: target required", ErrInvalidRequest)
	}
	// 范围判定在读锁快照之外流式读，不与 Purge 互斥整个过程；
	// 但 Purge 是整段删除，先判后删也会让 Scan 跳过——因此必须先占位再判定。
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("replay: manager closed")
	}
	if _, exists := m.jobs[req.JobID]; exists {
		m.mu.Unlock()
		return nil, ErrExists
	}
	if _, exists := m.reserved[req.JobID]; exists {
		m.mu.Unlock()
		return nil, ErrExists
	}
	m.reserved[req.JobID] = struct{}{}
	// 先用“请求给出的最大可能区间”占位，判定完成后收窄到实际区间。
	holdFrom, holdTo := req.Rng.FromSeq, req.Rng.ToSeq
	if holdFrom <= 0 {
		holdFrom = 1
	}
	if holdTo <= 0 {
		holdTo = 0 // 开放上界，覆盖时间范围回放可能触及的尾段
	}
	m.store.ReplayHoldAdd(req.JobID, req.Source, holdFrom, holdTo)
	m.mu.Unlock()

	plan, err := m.buildPlan(req)
	if err != nil {
		m.store.ReplayHoldRemove(req.JobID)
		m.mu.Lock()
		delete(m.reserved, req.JobID)
		m.mu.Unlock()
		return nil, err
	}

	now := time.Now().UTC()
	prog := Progress{
		JobID:     req.JobID,
		Source:    req.Source,
		Target:    req.Target,
		Plan:      plan,
		NextSeq:   plan.EffFromSeq,
		Status:    StatusRunning,
		UpdatedAt: now,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.reserved, req.JobID)
	if err := m.createJob(prog); err != nil {
		m.store.ReplayHoldRemove(req.JobID)
		return nil, err
	}
	// 收窄占位到实际命中区间（仍保护所有 effFrom..effTo，含空洞，
	// 因为重试用 Scan 顺序推进时还要顺序穿过它们）。
	if plan.EffFromSeq > 0 {
		m.store.ReplayHoldAdd(req.JobID, req.Source, plan.EffFromSeq, plan.EffToSeq)
	} else {
		// 空计划：直接完成并释放占位。
		m.store.ReplayHoldRemove(req.JobID)
	}
	j := m.jobs[req.JobID]
	m.spawnLocked(j)
	p := j.prog
	return &p, nil
}

// Resume 让 paused/blocked 的任务重新运行（例如下游已注册）。
func (m *Manager) Resume(jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	if j.isRunning() {
		return nil
	}
	if j.prog.Status == StatusComplete {
		return ErrFinished
	}
	j.prog.Status = StatusRunning
	j.prog.UpdatedAt = time.Now().UTC()
	prev := m.anchors[jobID]
	j.prog.PrevMAC = prev
	if err := m.saveProgress(j.prog); err != nil {
		return err
	}
	m.spawnLocked(j)
	return nil
}

// ResumeAll 重启后调用：为每个未完成、且目标下游已注册的任务续跑；
// 目标缺失的任务保持 blocked（不抛错，可稍后 RegisterSink 再 Resume）。
func (m *Manager) ResumeAll() (resumed int, blocked []string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		if j.prog.Status == StatusComplete || j.isRunning() {
			continue
		}
		if _, ok := m.sinks[j.prog.Target]; !ok {
			if j.prog.Status != StatusBlocked {
				j.prog.Status = StatusBlocked
				j.prog.UpdatedAt = time.Now().UTC()
				j.prog.PrevMAC = m.anchors[j.prog.JobID]
				if e := m.saveProgress(j.prog); e != nil {
					return resumed, blocked, e
				}
			}
			blocked = append(blocked, j.prog.JobID)
			continue
		}
		// 重新登记保留占位（进程重启后内存占位丢失）。
		m.store.ReplayHoldAdd(j.prog.JobID, j.prog.Source,
			j.prog.Plan.EffFromSeq, j.prog.Plan.EffToSeq)
		j.prog.Status = StatusRunning
		j.prog.UpdatedAt = time.Now().UTC()
		j.prog.PrevMAC = m.anchors[j.prog.JobID]
		if e := m.saveProgress(j.prog); e != nil {
			return resumed, blocked, e
		}
		m.spawnLocked(j)
		resumed++
	}
	return resumed, blocked, nil
}

// spawnLocked 启动工作者 goroutine，调用者持有 m.mu。
func (m *Manager) spawnLocked(j *job) {
	j.stop = make(chan struct{})
	j.done = make(chan struct{})
	m.wg.Add(1)
	go m.runJob(j)
}

func (j *job) isRunning() bool {
	select {
	case <-j.done:
		return false
	default:
		return true
	}
}

// isRunningLocked 与 isRunning 相同，但调用方已持有 Manager.mu
// （读取 done 通道状态无需额外同步，这里保留语义清晰）。
func (j *job) isRunningLocked() bool {
	select {
	case <-j.done:
		return false
	default:
		return true
	}
}

// runJob 是单任务回放工作者：严格按原始接入顺序（seq 升序）流式扫描投递。
// 内存有界：Scan 逐条回调，任何时刻内存中只有当前一条事件。
func (m *Manager) runJob(j *job) {
	defer m.wg.Done()
	defer close(j.done)
	backoff := m.backoff
	const maxBackoff = 2 * time.Second

	for {
		select {
		case <-j.stop:
			return
		default:
		}
		m.mu.Lock()
		prog := j.prog
		deliverer, ok := m.sinks[prog.Target]
		m.mu.Unlock()
		if !ok {
			m.markBlocked(j)
			return
		}

		advanced, stop, err := m.pumpJob(j, deliverer)
		if err != nil {
			// 存储损坏等致命错误：任务转 blocked 并等待人工介入，不静默重来。
			m.logf("replay job fatal error", "job", j.prog.JobID, "err", err)
			m.markBlocked(j)
			return
		}
		if stop {
			m.finishJob(j)
			return
		}
		if advanced {
			backoff = m.backoff
			continue
		}
		// 投递失败：等待退避（响应停止信号）。
		select {
		case <-j.stop:
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// pumpJob 从 NextSeq 起顺序扫描投递，返回：
//   - advanced：本次是否确认了至少一条；
//   - done：是否已放到计划末尾；
//
// 下游失败时立即返回 (false,false,nil)，位置不前进，等退避后重投当前条。
func (m *Manager) pumpJob(j *job, deliverer Deliverer) (advanced, done bool, err error) {
	m.mu.Lock()
	prog := j.prog
	m.mu.Unlock()
	from := prog.NextSeq
	if from == 0 || from > prog.Plan.EffToSeq {
		return false, true, nil
	}

	rng := prog.Plan.Requested
	deliveredAny := false
	scanErr := m.store.Scan(prog.Source, from, prog.Plan.EffToSeq, func(rv storage.RecordView) bool {
		seq := rv.Seq
		// 跳过计划已标记的各类不投递记录与范围外记录（顺序穿过，不投递）。
		if !shouldDeliver(rng, rv) {
			if e := m.advanceSkip(j, seq); e != nil {
				err = e
				return false
			}
			return true
		}
		// 故障重试与重启续传：下游按 (source, seq) 幂等，重复投递不产生重复可见。
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		dErr := deliverer.Deliver(ctx, prog.Target, rv.Record.Event)
		cancel()
		if dErr != nil {
			m.bumpRetries(j)
			m.logf("replay delivery failed; position retained",
				"job", prog.JobID, "source", prog.Source, "seq", seq, "err", dErr)
			err = nil
			return false // 停止扫描，保留 NextSeq=seq，等待重试
		}
		if e := m.advanceDelivered(j, seq); e != nil {
			err = e
			return false
		}
		deliveredAny = true
		return true
	})
	if scanErr != nil {
		return deliveredAny, false, scanErr
	}
	if err != nil {
		// advance 失败（ErrRollback 等）：致命，停止任务。
		return deliveredAny, false, err
	}
	m.mu.Lock()
	finalNext := j.prog.NextSeq
	effTo := j.prog.Plan.EffToSeq
	deliveredCount := j.prog.Delivered
	matched := j.prog.Plan.Matched
	m.mu.Unlock()
	// 范围内可能存在空洞（purged/missing）：Scan 不会经过这些 seq。
	// 因此完成条件是“计划内应投记录全部确认”，而非 NextSeq > effTo。
	if deliveredCount >= matched {
		return deliveredAny, true, nil
	}
	// 未投完但已越过区间右端（极端情况：计划记录数与扫描结果不一致），
	// 以位置判定兜底，避免空转。
	return deliveredAny, finalNext > effTo, nil
}

// shouldDeliver 与 buildPlan 的逐条判定保持一致。
func shouldDeliver(rng Range, rv storage.RecordView) bool {
	seq := rv.Seq
	if rng.FromSeq > 0 && seq < rng.FromSeq {
		return false
	}
	if rng.ToSeq > 0 && seq > rng.ToSeq {
		return false
	}
	if rv.Record.State == storage.StateRejected {
		return false
	}
	ts := rv.Record.Event.Timestamp
	if rng.From != nil && ts.Before(*rng.From) {
		return false
	}
	if rng.To != nil && ts.After(*rng.To) {
		return false
	}
	return true
}

// advanceDelivered 投递确认后推进进度（先持久化再承认完成）。
func (m *Manager) advanceDelivered(j *job, seq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := j.prog
	p.NextSeq = seq + 1
	p.Delivered++
	p.UpdatedAt = time.Now().UTC()
	p.PrevMAC = m.anchors[p.JobID]
	return m.saveProgress(p)
}

// advanceSkip 穿过一条计划内不投递的记录：推进位置但不计 delivered。
func (m *Manager) advanceSkip(j *job, seq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := j.prog
	p.NextSeq = seq + 1
	p.UpdatedAt = time.Now().UTC()
	p.PrevMAC = m.anchors[p.JobID]
	return m.saveProgress(p)
}

func (m *Manager) bumpRetries(j *job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := j.prog
	p.Retries++
	p.UpdatedAt = time.Now().UTC()
	p.PrevMAC = m.anchors[p.JobID]
	if err := m.saveProgress(p); err != nil {
		m.logf("replay progress persist failed", "job", p.JobID, "err", err)
	}
}

// finishJob 标记完成并释放保留占位。
func (m *Manager) finishJob(j *job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j.prog.Status == StatusComplete {
		return
	}
	j.prog.Status = StatusComplete
	j.prog.UpdatedAt = time.Now().UTC()
	j.prog.PrevMAC = m.anchors[j.prog.JobID]
	if err := m.saveProgress(j.prog); err != nil {
		m.logf("replay finish persist failed", "job", j.prog.JobID, "err", err)
	}
	m.store.ReplayHoldRemove(j.prog.JobID)
	m.logf("replay complete", "job", j.prog.JobID,
		"delivered", j.prog.Delivered, "retries", j.prog.Retries)
}

// markBlocked 任务因下游缺失或致命错误暂停（进度保持不变，可 Resume）。
func (m *Manager) markBlocked(j *job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j.prog.Status == StatusBlocked {
		return
	}
	j.prog.Status = StatusBlocked
	j.prog.UpdatedAt = time.Now().UTC()
	j.prog.PrevMAC = m.anchors[j.prog.JobID]
	if err := m.saveProgress(j.prog); err != nil {
		m.logf("replay blocked persist failed", "job", j.prog.JobID, "err", err)
	}
}

func (m *Manager) logf(msg string, args ...any) {
	// 默认输出到 slog 默认 logger；Pipeline 注入自己的 logger 时可替换。
	if m.log != nil {
		m.log.Info(msg, args...)
	}
}
