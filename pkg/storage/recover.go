package storage

import (
	"os"
	"path/filepath"
	"time"
)

func keyOf(s *Store) []byte { return s.key }

// recoverAll 枚举所有来源目录，加载清单并校验：
// 缺失分段、孤立分段、哈希链断裂均显式报错。
func (s *Store) recoverAll() error {
	base := filepath.Join(s.dir, sourcesDir)
	entries, err := os.ReadDir(base)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	now := time.Now()
	for _, de := range entries {
		if !de.IsDir() {
			continue
		}
		source, ok := decodeSrcDir(de.Name())
		if !ok {
			return ErrTampered
		}
		st, err := s.ensureSource(source)
		if err != nil {
			return err
		}
		if err := s.verifySource(st, now); err != nil {
			return err
		}
	}
	return nil
}

// verifySource 校验单个来源所有未删除分段与磁盘实际状态一致。
func (s *Store) verifySource(st *srcState, now interface{}) error {
	_ = now
	onDisk := map[string]bool{}
	files, err := os.ReadDir(st.dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if len(f.Name()) > 4 && f.Name()[:4] == "seg-" {
			onDisk[f.Name()] = true
		}
	}
	for i, e := range st.man.Entries {
		if e.Deleted {
			if onDisk[e.Name] {
				return ErrTampered
			}
			continue
		}
		if !onDisk[e.Name] {
			return ErrTampered // 清单说有，文件没了
		}
		if e == st.active {
			continue // 活动分段已在打开时校验/修复
		}
		r, err := scanSegment(filepath.Join(st.dir, e.Name), s.key, true)
		if err != nil {
			return err
		}
		if r.minSeq != e.MinSeq || r.maxSeq != e.MaxSeq || r.records != e.Records {
			return ErrTampered // 内容被改写
		}
		// 事件时间界是回放剪枝依据，同样纳入一致性校验；
		// 清单中为 0（未知/旧数据）时跳过，回放侧对未知界本就不剪枝。
		if e.MinEventTS != 0 && r.minEventTS != e.MinEventTS {
			return ErrTampered
		}
		if e.MaxEventTS != 0 && r.maxEventTS != e.MaxEventTS {
			return ErrTampered
		}
		_ = i
	}
	// 孤立分段：磁盘有但清单没有（乱序/非法插入）。
	for name := range onDisk {
		found := false
		for _, e := range st.man.Entries {
			if e.Name == name {
				found = true
				break
			}
		}
		if !found {
			return ErrTampered
		}
	}
	return nil
}
