package storage

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
)

// segEntry 描述一个分段在清单中的元数据。
type segEntry struct {
	Name        string `json:"name"`
	MinSeq      int64  `json:"min_seq"`
	MaxSeq      int64  `json:"max_seq"`
	Records     int    `json:"records"`
	Size        int64  `json:"size"`
	FirstStored int64  `json:"first_stored"`
	LastStored  int64  `json:"last_stored"`
	Sealed      bool   `json:"sealed"`
	Deleted     bool   `json:"deleted"` // 合法清理：文件已删除，条目留在链上可审计
	TailMAC     string `json:"tail_mac,omitempty"`
	Hash        string `json:"hash"` // 清单哈希链
}

type manifest struct {
	Entries []*segEntry `json:"entries"`
}

// entryHashInput 是除 Hash 外字段的规范编码。
type entryHashInput struct {
	Name        string
	MinSeq      int64
	MaxSeq      int64
	Records     int
	Size        int64
	FirstStored int64
	LastStored  int64
	Sealed      bool
	Deleted     bool
	TailMAC     string
}

func entryHash(key, prev []byte, e *segEntry) string {
	in := entryHashInput{
		Name: e.Name, MinSeq: e.MinSeq, MaxSeq: e.MaxSeq,
		Records: e.Records, Size: e.Size, FirstStored: e.FirstStored,
		LastStored: e.LastStored, Sealed: e.Sealed, Deleted: e.Deleted,
		TailMAC: e.TailMAC,
	}
	body, _ := json.Marshal(in)
	mac := hmac.New(sha256.New, key)
	mac.Write(prev)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// chain 重算清单哈希链，返回末端摘要；不匹配即返回 ErrTampered。
func (m *manifest) verify(key []byte) error {
	prev := make([]byte, macSize)
	seen := map[string]bool{}
	for i, e := range entriesOf(m) {
		if seen[e.Name] {
			return ErrTampered
		}
		seen[e.Name] = true
		want := entryHash(key, prev, e)
		if !hmac.Equal([]byte(want), []byte(e.Hash)) {
			return ErrTampered
		}
		// 乱序插入检测：名称序号必须严格递增。
		if i > 0 && e.Name <= entriesOf(m)[i-1].Name {
			return ErrTampered
		}
		h, _ := hex.DecodeString(want)
		prev = h
	}
	return nil
}

// entriesOf 保持与 manifest 的解耦引用，便于测试替换。
func entriesOf(m *manifest) []*segEntry { return m.Entries }

func (s *Store) manifestPath(sourceDir string) string {
	return filepath.Join(sourceDir, manifestName)
}

func (s *Store) loadManifest(source, dir string) (*manifest, error) {
	m := &manifest{}
	b, err := os.ReadFile(s.manifestPath(dir))
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, ErrTampered
	}
	if err := m.verify(s.key); err != nil {
		return nil, err
	}
	return m, nil
}

// saveManifest 重算全链并原子落盘（调用者持写锁）。
func (s *Store) saveManifest(st *srcState) error {
	prev := make([]byte, macSize)
	for _, e := range st.man.Entries {
		e.Hash = entryHash(keyOf(s), prev, e)
		h, _ := hex.DecodeString(e.Hash)
		prev = h
	}
	b, err := json.MarshalIndent(st.man, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.manifestPath(st.dir), b)
}
