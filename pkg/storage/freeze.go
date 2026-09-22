package storage

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type freezeFile struct {
	Freezes []Freeze `json:"freezes"`
	MAC     string   `json:"mac"`
}

func (s *Store) freezesPath() string { return filepath.Join(s.dir, freezesFile) }

func canonicalFreezeMAC(key []byte, fs []Freeze) string {
	sorted := make([]Freeze, len(fs))
	copy(sorted, fs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	body, _ := json.Marshal(sorted)
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Store) loadFreezes() error {
	b, err := os.ReadFile(s.freezesPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var ff freezeFile
	if err := json.Unmarshal(b, &ff); err != nil {
		return ErrTampered
	}
	if !hmac.Equal([]byte(canonicalFreezeMAC(s.key, ff.Freezes)), []byte(ff.MAC)) {
		return ErrTampered
	}
	s.freezes = ff.Freezes
	return nil
}

func (s *Store) persistFreezes() error {
	ff := freezeFile{
		Freezes: s.freezes,
		MAC:     canonicalFreezeMAC(s.key, s.freezes),
	}
	b, err := json.MarshalIndent(ff, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.freezesPath(), b)
}

// AddFreeze 注册合规冻结范围。
func (s *Store) AddFreeze(f Freeze) error {
	if f.ID == "" {
		return errBadFreeze("freeze id required")
	}
	if f.CreatedAt.IsZero() {
		f.CreatedAt = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, ex := range s.freezes {
		if ex.ID == f.ID {
			s.freezes[i] = f
			return s.persistFreezes()
		}
	}
	s.freezes = append(s.freezes, f)
	return s.persistFreezes()
}

// RemoveFreeze 显式解除冻结。
func (s *Store) RemoveFreeze(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.freezes[:0]
	found := false
	for _, f := range s.freezes {
		if f.ID == id {
			found = true
			continue
		}
		out = append(out, f)
	}
	if !found {
		return ErrNotFound
	}
	s.freezes = out
	return s.persistFreezes()
}

// ActiveFreezes 返回当前仍生效（未到期）的冻结。
func (s *Store) ActiveFreezes(now time.Time) ([]Freeze, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Freeze
	for _, f := range s.freezes {
		if f.Until.IsZero() || f.Until.After(now) {
			out = append(out, f)
		}
	}
	return out, nil
}

type freezeError string

func (e freezeError) Error() string { return "storage: " + string(e) }

func errBadFreeze(msg string) error { return freezeError(msg) }

// overlapsFreeze 判断某来源的 [minSeq,maxSeq] 分段是否被冻结覆盖。
// 调用者需保证 freezes 已过滤为生效集合。
func overlapsFreeze(fs []Freeze, source string, minSeq, maxSeq int64) bool {
	for _, f := range fs {
		if f.Source != "" && f.Source != source {
			continue
		}
		lo := f.FromSeq
		hi := f.ToSeq // 0 表示开放上界
		if hi == 0 {
			hi = maxSeq
		}
		if minSeq <= hi && lo <= maxSeq {
			return true
		}
	}
	return false
}
