package storage

import (
	"os"
	"path/filepath"
	"time"
)

// Purge 按保留策略清理到期分段。
// 规则：
//   - 活动（正在写入）分段永不清理；
//   - 仅处理已封存、且末条记录时间早于 now-MaxAge 的分段；
//   - 与任一仍生效的冻结范围相交的分段跳过并计数；
//   - 删除文件后在清单中标记 deleted 并重算哈希链（删除本身可审计）；
//   - 全程持写锁，与读/写互斥，杜绝半成品状态与重复暴露。
func (s *Store) Purge(nowUnix int64) (PurgeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var res PurgeResult
	if s.retention.MaxAge <= 0 {
		return res, nil
	}
	cutoff := nowUnix - int64(s.retention.MaxAge/time.Second)

	for source, st := range s.srcs {
		active := st.active
		var activeFreezes []Freeze
		now := time.Unix(nowUnix, 0)
		for _, f := range s.freezes {
			if f.Until.IsZero() || f.Until.After(now) {
				activeFreezes = append(activeFreezes, f)
			}
		}
		dirty := false
		for _, e := range st.man.Entries {
			if e == active || e.Deleted || !e.Sealed {
				continue
			}
			if e.LastStored >= cutoff {
				continue // 仍在保留期内
			}
			if overlapsFreeze(activeFreezes, source, e.MinSeq, e.MaxSeq) {
				res.SkippedFrozen++
				continue
			}
			if overlapsReplayHold(s.replayHolds, source, e.MinSeq, e.MaxSeq) {
				res.SkippedReplay++
				continue
			}
			size := e.Size
			path := filepath.Join(st.dir, e.Name)
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return res, err
			}
			e.Deleted = true
			e.Size = 0
			e.TailMAC = ""
			res.DeletedSegments++
			res.DeletedRecords += int64(e.Records)
			res.ReclaimedBytes += size
			dirty = true
		}
		if dirty {
			if err := s.saveManifest(st); err != nil {
				return res, err
			}
		}
	}
	return res, nil
}

// Verify 对全部来源执行完整性校验（清单链 + 分段链 + 磁盘一致性）。
func (s *Store) Verify() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	for _, st := range s.srcs {
		if err := s.verifySource(st, now); err != nil {
			return err
		}
	}
	return nil
}
