package storage

// replayHold 是回放期间的保留锁：落在 [FromSeq, ToSeq] 内的分段
// 在锁释放前不得被 Purge 清理，即使已过保留期且无冻结。
//
// 锁仅存内存：回放管理器启动/重启时，会根据持久化的回放进度
// 为所有未完成任务重新注册，因此进程存活期间清理永远与回放互斥；
// 进程不运行时本来也没有清理在执行。
type replayHold struct {
	jobID   string
	source  string
	fromSeq int64
	toSeq   int64
}

// AcquireReplayHold 原子地注册回放保留锁，并返回与 [fromSeq,toSeq]
// 相交的已清理序号缺口（空表示数据完整）。注册与缺口检查在同一把
// 存储写锁内完成，Purge 无法插入两步之间。
//
// 同一 jobID 重复注册会替换旧锁（重试用）。
func (s *Store) AcquireReplayHold(jobID, source string, fromSeq, toSeq int64) ([][2]int64, error) {
	if jobID == "" || source == "" {
		return nil, errBadHold("job id and source required")
	}
	if fromSeq <= 0 {
		fromSeq = 1
	}
	if toSeq < fromSeq {
		return nil, errBadHold("to seq before from seq")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.holds == nil {
		s.holds = map[string]*replayHold{}
	}
	s.holds[jobID] = &replayHold{
		jobID: jobID, source: source, fromSeq: fromSeq, toSeq: toSeq,
	}
	st, ok := s.srcs[source]
	if !ok {
		// 未知来源：没有分段，也就没有缺口。
		return nil, nil
	}
	return purgedGapsLocked(st.man.Entries, fromSeq, toSeq), nil
}

// ReleaseReplayHold 释放回放保留锁（回放结束/取消/终态后调用）。
// 不存在的锁返回 ErrNotFound。
func (s *Store) ReleaseReplayHold(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.holds[jobID]; !ok {
		return ErrNotFound
	}
	delete(s.holds, jobID)
	return nil
}

// PurgedGapsInRange 返回与 [fromSeq,toSeq] 相交的已清理序号区间。
func (s *Store) PurgedGapsInRange(source string, fromSeq, toSeq int64) ([][2]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.srcs[source]
	if !ok {
		return nil, nil
	}
	return purgedGapsLocked(st.man.Entries, fromSeq, toSeq), nil
}

// purgedGapsLocked 汇总已删除分段与请求区间的交集（调用者持锁）。
func purgedGapsLocked(entries []*segEntry, fromSeq, toSeq int64) [][2]int64 {
	var gaps [][2]int64
	for _, e := range entries {
		if !e.Deleted {
			continue
		}
		lo := e.MinSeq
		if lo < fromSeq {
			lo = fromSeq
		}
		hi := e.MaxSeq
		if hi > toSeq {
			hi = toSeq
		}
		if lo <= hi {
			gaps = append(gaps, [2]int64{lo, hi})
		}
	}
	return gaps
}

// overlapsHold 判断某来源的 [minSeq,maxSeq] 分段是否被任一回放锁覆盖。
func overlapsHold(holds map[string]*replayHold, source string, minSeq, maxSeq int64) bool {
	for _, h := range holds {
		if h.source != source {
			continue
		}
		if minSeq <= h.toSeq && h.fromSeq <= maxSeq {
			return true
		}
	}
	return false
}

type holdError string

func (e holdError) Error() string { return "storage: " + string(e) }

func errBadHold(msg string) error { return holdError(msg) }
