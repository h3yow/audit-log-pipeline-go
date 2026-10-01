package storage

// SegmentMeta 是清单中一个分段的不可变元数据快照（回放范围判定用）。
type SegmentMeta struct {
	Name        string
	MinSeq      int64
	MaxSeq      int64
	Records     int
	FirstStored int64 // 段内首条记录 StoredAt（unix 秒）
	LastStored  int64 // 段内末条记录 StoredAt（unix 秒）
	MinEventTS  int64 // 段内事件自身时间戳最小值（unix 纳秒）；0=未知（不用于剪枝）
	MaxEventTS  int64 // 段内事件自身时间戳最大值（unix 纳秒）；0=未知（不用于剪枝）
	Sealed      bool
	Deleted     bool
}

// ReplaySnapshot 是某来源清单在某一时刻的不可变快照。
type ReplaySnapshot struct {
	Source   string
	Segments []SegmentMeta
}

// replayHold 是一个活跃回放任务在存储侧的保留占位：
// 与其序号区间相交的未清理分段在释放前一律不参与 Purge。
type replayHold struct {
	source  string
	fromSeq int64
	toSeq   int64 // 0 表示开放上界（覆盖到当前尾部）
}

// SealActiveSegment 封存某来源当前活动分段并滚动出新段。
// 供测试/运维把一批历史记录切分为可独立保留管理的已封存段。
func (s *Store) SealActiveSegment(source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.ensureSource(source)
	if err != nil {
		return err
	}
	if st.active == nil || st.active.Records == 0 {
		return nil
	}
	return s.rotate(st)
}

// ReplaySnapshot 在存储读锁内深拷贝清单条目，返回不可变快照。
// 与 Scan 的快照语义一致：快照之后发生的滚动/清理不影响回放判定。
func (s *Store) ReplaySnapshot(source string) (ReplaySnapshot, error) {
	s.mu.RLock()
	st, ok := s.srcs[source]
	s.mu.RUnlock()
	if !ok {
		// 全新/未载入来源：不加写锁地加载只读清单（与 Scan 一致）。
		dir := s.sourceDir(source)
		man, err := s.loadManifest(source, dir)
		if err != nil {
			return ReplaySnapshot{}, err
		}
		st = &srcState{dir: dir, man: man}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := ReplaySnapshot{Source: source, Segments: make([]SegmentMeta, 0, len(st.man.Entries))}
	for _, e := range st.man.Entries {
		snap.Segments = append(snap.Segments, SegmentMeta{
			Name:        e.Name,
			MinSeq:      e.MinSeq,
			MaxSeq:      e.MaxSeq,
			Records:     e.Records,
			FirstStored: e.FirstStored,
			LastStored:  e.LastStored,
			MinEventTS:  e.MinEventTS,
			MaxEventTS:  e.MaxEventTS,
			Sealed:      e.Sealed,
			Deleted:     e.Deleted,
		})
	}
	return snap, nil
}

// ReplayHoldAdd 注册回放保留占位，阻止 Purge 删除与区间相交的分段。
// 对同一 jobID 重复注册会更新区间（例如时间范围回放首帧后收窄）。
func (s *Store) ReplayHoldAdd(jobID, source string, fromSeq, toSeq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replayHolds[jobID] = replayHold{source: source, fromSeq: fromSeq, toSeq: toSeq}
}

// ReplayHoldRemove 释放回放保留占位。
func (s *Store) ReplayHoldRemove(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.replayHolds, jobID)
}

// overlapsReplayHold 判断某来源 [minSeq,maxSeq] 分段是否被任一活跃回放占用。
// 调用者必须持有 s.mu（Purge 持写锁）。
func overlapsReplayHold(holds map[string]replayHold, source string, minSeq, maxSeq int64) bool {
	for _, h := range holds {
		if h.source != source {
			continue
		}
		hi := h.toSeq
		if hi <= 0 {
			hi = maxSeq
		}
		if minSeq <= hi && h.fromSeq <= maxSeq {
			return true
		}
	}
	return false
}
