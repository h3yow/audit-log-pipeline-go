package replay

import (
	"fmt"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// runJob 是单个回放任务的执行循环：
//   - 从持久化进度 Cursor+1 继续，按 seq 顺序流式读取；
//   - 逐条过滤（时间范围）、分类跳过（rejected/新事件）或投递；
//   - 每条处理完立即持久化前进的进度（崩溃/重启不重不漏）；
//   - 下游故障在同一序号上指数退避重试（下游幂等保证不重复可见）；
//   - 读到清理缺口/哈希链断裂：任务落盘 failed 终态并解释，绝不静默跳过；
//   - 读完 [FromSeq,ToSeq] 全部可能命中的事件后 completed。
func (m *Manager) runJob(j *job) {
	defer m.wg.Done()
	defer close(j.done)

	j.mu.Lock()
	st := j.st
	j.mu.Unlock()

	reader, err := m.cfg.Store.OpenReplay(st.Source, st.FromSeq, st.ToSeq)
	if err != nil {
		m.failReader(j, err)
		return
	}
	defer reader.Close()

	// 跳到已处理位置之后：已处理事件不重复投递（其进度已持久化）。
	backoff := m.cfg.FailureRetry
	const maxBackoff = 2 * time.Second

	for {
		select {
		case <-j.stop:
			m.stopOrCancel(j)
			return
		default:
		}

		rv, ok, err := reader.Next()
		if err != nil {
			m.failReader(j, err)
			return
		}
		if !ok {
			m.finishCompleted(j)
			return
		}

		j.mu.Lock()
		st = j.st
		j.mu.Unlock()
		// 崩溃重启后，跳过本次任务已处理过的序号（幂等恢复）。
		if rv.Seq <= st.Cursor {
			continue
		}

		// ---- 分类 ----
		if !st.FromTime.IsZero() && rv.Record.Event.Timestamp.Before(st.FromTime) {
			st.Skipped.BeforeTime++
			st.Cursor = rv.Seq
			if !m.persist(j, &st) {
				return
			}
			continue
		}
		if !st.ToTime.IsZero() && rv.Record.Event.Timestamp.After(st.ToTime) {
			st.Skipped.AfterTime++
			st.Cursor = rv.Seq
			if !m.persist(j, &st) {
				return
			}
			continue
		}
		if rv.Record.State == storage.StateRejected {
			st.Skipped.Rejected++
			st.Cursor = rv.Seq
			if !m.persist(j, &st) {
				return
			}
			continue
		}

		// ---- 投递（故障时原地重试，不越过当前序号）----
		for {
			select {
			case <-j.stop:
				m.finishCanceled(j)
				return
			default:
			}
			err := m.cfg.Sink.DeliverReplayRecord(st.Route, rv)
			if err == nil {
				break
			}
			st.Retries++
			st.LastError = err.Error()
			st.UpdatedAt = time.Now()
			// 同序号持久化重试计数（Cursor 等值，计数单调增）：可观测、可重启续。
			if perr := m.ps.update(st); perr != nil {
				m.failWith(j, &st, "progress persist on retry: %v (delivery err: %v)", perr, err)
				return
			}
			j.mu.Lock()
			j.st = st
			j.mu.Unlock()
			m.cfg.Logger.Warn("replay delivery failed; retrying same seq",
				"job", st.JobID, "seq", rv.Seq, "retries", st.Retries, "err", err)
			select {
			case <-j.stop:
				m.stopOrCancel(j)
				return
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
		backoff = m.cfg.FailureRetry

		st.Delivered++
		st.LastError = ""
		st.Cursor = rv.Seq
		if !m.persist(j, &st) {
			return
		}
		m.cfg.Logger.Info("replay delivered",
			"job", st.JobID, "source", st.Source, "seq", rv.Seq,
			"delivered", st.Delivered, "of_to_seq", st.ToSeq)
	}
}

// persist 持久化前进一步的进度并同步内存态；返回 false 表示任务已终止。
func (m *Manager) persist(j *job, st *progressState) bool {
	st.UpdatedAt = time.Now()
	if err := m.ps.update(*st); err != nil {
		m.failWith(j, st, "%v", err)
		return false
	}
	j.mu.Lock()
	j.st = *st
	j.mu.Unlock()
	return true
}

// failReader 把存储层读取错误（含清理缺口、篡改）映射为 failed 终态。
func (m *Manager) failReader(j *job, err error) {
	j.mu.Lock()
	st := j.st
	j.mu.Unlock()
	switch {
	case err == storage.ErrPurgedGap:
		gaps := m.gapList(&st)
		st.Skipped.Missing = int64(len(gaps))
		m.failWith(j, &st, "%v: segment under replay was purged mid-run gaps=%v",
			ErrDataGap, gaps)
	case err == storage.ErrTampered:
		m.failWith(j, &st, "%v: hash chain broken while reading", err)
	default:
		m.failWith(j, &st, "replay read error: %v", err)
	}
}

// gapList 返回进度之后与已清理分段相交的序号区间（供失败原因解释）。
func (m *Manager) gapList(st *progressState) [][2]int64 {
	from := st.Cursor + 1
	if from < st.FromSeq {
		from = st.FromSeq
	}
	gaps, err := m.cfg.Store.PurgedGapsInRange(st.Source, from, st.ToSeq)
	if err != nil {
		return nil
	}
	return gaps
}

func (m *Manager) finishCompleted(j *job) {
	j.mu.Lock()
	st := j.st
	j.mu.Unlock()
	st.Phase = PhaseCompleted
	st.Cursor = st.ToSeq
	st.CompletedAt = time.Now()
	st.UpdatedAt = st.CompletedAt
	if err := m.ps.update(st); err != nil && !isRollback(err) {
		m.cfg.Logger.Error("replay complete persist failed", "job", st.JobID, "err", err)
	}
	j.mu.Lock()
	j.st = st
	j.mu.Unlock()
	m.releaseHoldOnce(j)
	m.cfg.Logger.Info("replay completed",
		"job", st.JobID, "delivered", st.Delivered,
		"skipped", st.Skipped, "retries", st.Retries)
}

// stopOrCancel 在停止信号到达时区分两种语义：
//   - 显式 Cancel：落盘 canceled 终态并释放保留锁；
//   - 进程停机（Close）：不写终态，保留 running 与最新进度，
//     仅释放内存保留锁，下次启动由 New 自动续跑。
func (m *Manager) stopOrCancel(j *job) {
	j.mu.Lock()
	canceled := j.canceled
	j.mu.Unlock()
	if !canceled {
		m.releaseHoldOnce(j)
		m.cfg.Logger.Info("replay stopped with process; progress durable, will resume",
			"job", j.st.JobID, "cursor", j.st.Cursor)
		return
	}
	m.finishCanceled(j)
}

func (m *Manager) finishCanceled(j *job) {
	j.mu.Lock()
	st := j.st
	j.mu.Unlock()
	if terminalPhases[st.Phase] {
		return
	}
	st.Phase = PhaseCanceled
	st.CompletedAt = time.Now()
	st.UpdatedAt = st.CompletedAt
	if err := m.ps.update(st); err != nil && !isRollback(err) {
		m.cfg.Logger.Error("replay cancel persist failed", "job", st.JobID, "err", err)
	}
	j.mu.Lock()
	j.st = st
	j.mu.Unlock()
	m.releaseHoldOnce(j)
	m.cfg.Logger.Info("replay canceled",
		"job", st.JobID, "cursor", st.Cursor, "delivered", st.Delivered)
}

// terminate 在启动/恢复阶段把任务落盘为 failed（此时尚未运行）。
func (m *Manager) terminate(j *job, phase Phase, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	j.mu.Lock()
	j.st.Phase = phase
	j.st.LastError = msg
	j.st.CompletedAt = time.Now()
	j.st.UpdatedAt = j.st.CompletedAt
	st := j.st
	j.mu.Unlock()
	if err := m.ps.update(st); err != nil {
		m.cfg.Logger.Error("replay terminal persist failed",
			"job", st.JobID, "err", err, "reason", msg)
	}
	close(j.done)
	m.cfg.Logger.Error("replay failed before run", "job", st.JobID, "err", msg)
}

// failWith 在运行阶段把任务落盘为 failed 并释放保留锁。
func (m *Manager) failWith(j *job, st *progressState, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	st.Phase = PhaseFailed
	st.LastError = msg
	st.CompletedAt = time.Now()
	st.UpdatedAt = st.CompletedAt
	if err := m.ps.update(*st); err != nil && !isRollback(err) {
		m.cfg.Logger.Error("replay failure persist failed",
			"job", st.JobID, "err", err, "reason", msg)
	}
	j.mu.Lock()
	j.st = *st
	j.mu.Unlock()
	m.releaseHoldOnce(j)
	m.cfg.Logger.Error("replay failed", "job", st.JobID, "err", msg)
}

func isRollback(err error) bool { return err == ErrRollback }
