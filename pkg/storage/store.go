package storage

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// 磁盘布局：
//
//	<dir>/key.bin                 HMAC-SHA256 密钥（32B，0600）
//	<dir>/freezes.json            合规冻结集（带 MAC）
//	<dir>/src/<hex(source)>/manifest.json
//	<dir>/src/<hex(source)>/seg-000001.dat ...
//
// manifest 中每个条目带哈希链；段内帧也带哈希链。
// 合法清理会把条目标记为 deleted 并保留在链上（可审计），非法删除/改写/乱序即断链。

const (
	keyFile      = "key.bin"
	sourcesDir   = "src"
	freezesFile  = "freezes.json"
	manifestName = "manifest.json"
)

// Store 是基于本地目录的分片审计存储。
type Store struct {
	dir       string
	key       []byte
	retention Retention
	maxSeg    int64

	mu          sync.RWMutex
	srcs        map[string]*srcState
	freezes     []Freeze
	replayHolds map[string]replayHold // jobID -> 活跃回放保留占位
	closed      bool
}

type srcState struct {
	dir    string
	man    *manifest
	f      *os.File // 活动分段句柄
	active *segEntry
	tail   []byte // 活动分段末帧 MAC
}

// Open 打开/初始化存储，并校验全部已存在来源的完整性。
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, errors.New("storage: dir required")
	}
	maxSeg := opts.MaxSegmentBytes
	if maxSeg <= 0 {
		maxSeg = 4 << 20
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	key, err := loadOrCreateKey(opts.Dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:         opts.Dir,
		key:         key,
		retention:   opts.Retention,
		maxSeg:      maxSeg,
		srcs:        map[string]*srcState{},
		replayHolds: map[string]replayHold{},
	}
	if err := s.loadFreezes(); err != nil {
		return nil, err
	}
	if err := s.recoverAll(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close 关闭所有活动句柄。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, st := range s.srcs {
		if st.f != nil {
			if err := st.f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	s.closed = true
	return firstErr
}

func loadOrCreateKey(dir string) ([]byte, error) {
	p := filepath.Join(dir, keyFile)
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		k := make([]byte, 32)
		if _, err := rand.Read(k); err != nil {
			return nil, err
		}
		if err := os.WriteFile(p, k, 0o600); err != nil {
			return nil, err
		}
		return k, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) != 32 {
		return nil, fmt.Errorf("%w: key file has wrong size", ErrTampered)
	}
	return b, nil
}

// ---- 来源目录与清单 ----

func srcDirName(source string) string {
	return hex.EncodeToString([]byte(source))
}

func decodeSrcDir(name string) (string, bool) {
	b, err := hex.DecodeString(name)
	return string(b), err == nil
}

func (s *Store) sourceDir(source string) string {
	return filepath.Join(s.dir, sourcesDir, srcDirName(source))
}

// ensureSource 必须在持有 s.mu 时调用。
func (s *Store) ensureSource(source string) (*srcState, error) {
	if st, ok := s.srcs[source]; ok {
		return st, nil
	}
	dir := s.sourceDir(source)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	man, err := s.loadManifest(source, dir)
	if err != nil {
		return nil, err
	}
	st := &srcState{dir: dir, man: man, tail: make([]byte, macSize)}
	if len(man.Entries) > 0 {
		st.active = man.Entries[len(man.Entries)-1]
		if st.active.Deleted {
			st.active = nil
		}
	}
	if st.active != nil {
		if err := s.repairActive(st); err != nil {
			return nil, err
		}
	}
	s.srcs[source] = st
	return st, nil
}

// repairActive 校验活动分段：修复崩溃残留的半帧（截断），
// 真正的 MAC/魔数篡改返回 ErrTampered。
func (s *Store) repairActive(st *srcState) error {
	p := filepath.Join(st.dir, st.active.Name)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: missing segment %s", ErrTampered, st.active.Name)
		}
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	sr, err := scanSegment(p, s.key, false)
	if err != nil && !errors.Is(err, errTornFrame) {
		f.Close()
		return err
	}
	if errors.Is(err, errTornFrame) {
		if err := f.Truncate(sr.goodBytes); err != nil {
			f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	st.f = f
	st.tail = sr.tail
	st.active.Size = sr.goodBytes
	st.active.MinSeq = sr.minSeq
	st.active.MaxSeq = sr.maxSeq
	st.active.Records = sr.records
	st.active.FirstStored = sr.firstStored
	st.active.LastStored = sr.lastStored
	if info.Size() != sr.goodBytes {
		if err := s.saveManifest(st); err != nil {
			return err
		}
	}
	return nil
}

func segName(index int) string {
	return fmt.Sprintf("seg-%06d.dat", index)
}

// rotate 封存当前活动分段并创建下一个。调用者持有写锁。
func (s *Store) rotate(st *srcState) error {
	if st.f != nil {
		if err := st.f.Sync(); err != nil {
			return err
		}
		if err := st.f.Close(); err != nil {
			return err
		}
		st.f = nil
	}
	if st.active != nil {
		st.active.Sealed = true
		st.active.TailMAC = hex.EncodeToString(st.tail)
	}
	idx := len(st.man.Entries) + 1
	e := &segEntry{Name: segName(idx), MinSeq: 0, MaxSeq: 0}
	p := filepath.Join(st.dir, e.Name)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	st.f = f
	st.active = e
	st.tail = make([]byte, macSize)
	st.man.Entries = append(st.man.Entries, e)
	return s.saveManifest(st)
}

// ---- 通用原子写 ----

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

var _ = strconv.Itoa
var _ = json.Marshal
var _ = hmac.Equal
var _ = sha256.New

// SetMaxSegmentBytesForTest 调整滚动阈值（仅测试使用）。
func (s *Store) SetMaxSegmentBytesForTest(n int64) {
	s.mu.Lock()
	s.maxSeg = n
	s.mu.Unlock()
}

// AgeSealedSegmentsForTest 将某来源已封存分段的时间戳旧化（仅测试用）。
func (s *Store) AgeSealedSegmentsForTest(source string, storedAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.srcs[source]
	if !ok {
		return
	}
	changed := false
	for _, e := range st.man.Entries {
		if e == st.active {
			continue
		}
		e.FirstStored = storedAt
		e.LastStored = storedAt
		changed = true
	}
	if changed {
		_ = s.saveManifest(st)
	}
}
