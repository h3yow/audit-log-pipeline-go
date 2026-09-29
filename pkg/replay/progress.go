package replay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// progressState 是单个回放任务进度的磁盘格式（带 HMAC 状态链）。
//
// 文件布局：<dir>/<hex(jobID)>.rp；全局锚点 <dir>/anchors.json。
// 与实时投递检查点使用不同文件、不同 MAC 域前缀与创世锚，
// 两套进度互不影响：回放过时不带动实时水位，反之亦然。
type progressState struct {
	JobID       string    `json:"job_id"`
	Source      string    `json:"source"`
	Route       string    `json:"route"`
	FromSeq     int64     `json:"from_seq"`
	ToSeq       int64     `json:"to_seq"`
	FromTime    time.Time `json:"from_time"`
	ToTime      time.Time `json:"to_time"`
	Phase       Phase     `json:"phase"`
	Cursor      int64     `json:"cursor"`
	Delivered   int64     `json:"delivered"`
	Retries     int64     `json:"retries"`
	Skipped     Skipped   `json:"skipped"`
	StartedAt   time.Time `json:"started_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	CompletedAt time.Time `json:"completed_at"`
	LastError   string    `json:"last_error,omitempty"`
	PrevMAC     string    `json:"prev_mac"`
}

type rpEnvelope struct {
	State progressState `json:"state"`
	MAC   string        `json:"mac"`
}

type rpAnchorsFile struct {
	Anchors map[string]string `json:"anchors"`
	MAC     string            `json:"mac"`
}

const rpAnchorsName = "anchors.json"

// progressStore 持久化回放进度。
type progressStore struct {
	mu      sync.Mutex
	dir     string
	key     []byte
	anchors map[string]string // jobID -> 当前已提交版本 MAC
}

// newProgressStore 打开/初始化回放进度存储并交叉校验全部已存在状态。
// 任何回退/旧副本/篡改均返回 ErrRollback。
func newProgressStore(dir string, key []byte) (*progressStore, error) {
	if len(key) != 32 {
		return nil, errors.New("replay: bad key length")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ps := &progressStore{dir: dir, key: key, anchors: map[string]string{}}

	anchors := map[string]string{}
	if b, err := os.ReadFile(filepath.Join(dir, rpAnchorsName)); err == nil {
		var af rpAnchorsFile
		if err := json.Unmarshal(b, &af); err != nil {
			return nil, ErrRollback
		}
		if !hmac.Equal([]byte(af.MAC), []byte(ps.anchorsMAC(af.Anchors))) {
			return nil, ErrRollback
		}
		anchors = af.Anchors
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	ps.anchors = anchors
	dirty := false

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".rp" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		var env rpEnvelope
		if err := json.Unmarshal(b, &env); err != nil {
			return nil, ErrRollback
		}
		want := ps.stateMAC(env.State.PrevMAC, env.State)
		if !hmac.Equal([]byte(want), []byte(env.MAC)) {
			return nil, ErrRollback
		}
		if committed, ok := anchors[env.State.JobID]; ok {
			if committed != env.MAC {
				if env.State.PrevMAC == committed {
					// 崩溃窗口：新状态已落盘、锚点未更新——合法前向提交，补提交。
					anchors[env.State.JobID] = env.MAC
					dirty = true
				} else {
					// 既非已提交版本也不是其直接后继：旧副本/回退，拒绝。
					return nil, ErrRollback
				}
			}
		} else {
			if env.State.PrevMAC != genesisMAC(env.State.JobID) {
				return nil, ErrRollback
			}
			anchors[env.State.JobID] = env.MAC
			dirty = true
		}
	}
	for id := range anchors {
		if _, err := os.Stat(filepath.Join(dir, rpName(id))); err != nil {
			return nil, ErrRollback // 锚点在、状态文件缺失：链断裂
		}
	}
	if dirty {
		if err := ps.writeAnchors(anchors); err != nil {
			return nil, err
		}
	}
	return ps, nil
}

func genesisMAC(jobID string) string {
	g := sha256.Sum256([]byte("REPLAY-GENESIS:" + jobID))
	return hex.EncodeToString(g[:])
}

// stateMAC 绑定前驱 MAC 与全部状态字段（含过滤范围、计数、终态、错误）。
func (ps *progressStore) stateMAC(prevMAC string, st progressState) string {
	body, _ := json.Marshal(struct {
		JobID, Source, Route              string
		FromSeq, ToSeq                    int64
		FromTime, ToTime                  time.Time
		Phase                             Phase
		Cursor, Delivered, Retries        int64
		Skipped                           Skipped
		StartedAt, UpdatedAt, CompletedAt time.Time
		LastError, PrevMAC                string
	}{
		st.JobID, st.Source, st.Route,
		st.FromSeq, st.ToSeq,
		st.FromTime, st.ToTime,
		st.Phase,
		st.Cursor, st.Delivered, st.Retries,
		st.Skipped,
		st.StartedAt, st.UpdatedAt, st.CompletedAt,
		st.LastError, st.PrevMAC,
	})
	mac := hmac.New(sha256.New, ps.key)
	mac.Write([]byte("replay-state-v1"))
	mac.Write([]byte(prevMAC))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func (ps *progressStore) anchorsMAC(a map[string]string) string {
	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	mac := hmac.New(sha256.New, ps.key)
	mac.Write([]byte("replay-anchors-v1"))
	for _, k := range keys {
		mac.Write([]byte(k))
		mac.Write([]byte(a[k]))
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func rpName(jobID string) string {
	return hex.EncodeToString([]byte(jobID)) + ".rp"
}

func (ps *progressStore) writeAnchors(a map[string]string) error {
	af := rpAnchorsFile{Anchors: a, MAC: ps.anchorsMAC(a)}
	b, err := json.MarshalIndent(af, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomicReplay(filepath.Join(ps.dir, rpAnchorsName), b)
}

func writeFileAtomicReplay(path string, data []byte) error {
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

// create 写入首版状态（仅允许创世前驱）；同 ID 已存在返回 ErrJobExists。
func (ps *progressStore) create(st progressState) error {
	if st.JobID == "" {
		return ErrInvalidRequest
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if _, ok := ps.anchors[st.JobID]; ok {
		return ErrJobExists
	}
	st.PrevMAC = genesisMAC(st.JobID)
	mac := ps.stateMAC(st.PrevMAC, st)
	b, err := json.MarshalIndent(rpEnvelope{State: st, MAC: mac}, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomicReplay(filepath.Join(ps.dir, rpName(st.JobID)), b); err != nil {
		return err
	}
	next := map[string]string{}
	for k, v := range ps.anchors {
		next[k] = v
	}
	next[st.JobID] = mac
	if err := ps.writeAnchors(next); err != nil {
		return err
	}
	ps.anchors = next
	return nil
}

var terminalPhases = map[Phase]bool{
	PhaseCompleted: true, PhaseFailed: true, PhaseCanceled: true,
}

// update 提交下一版状态：
//   - 任务定义字段（来源/路由/范围/起始时间）不可变，被改即 ErrRollback；
//   - Cursor 只许不退（允许等值，用于在同序号上持久化重试计数）；
//   - Delivered/Retries/各 skipped 计数只许不减；
//   - 终态（completed/failed/canceled）不可再变更。
func (ps *progressStore) update(st progressState) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	curMAC, ok := ps.anchors[st.JobID]
	if !ok {
		return ErrJobNotFound
	}
	cur, err := ps.loadLocked(st.JobID)
	if err != nil {
		return err
	}
	if terminalPhases[cur.Phase] {
		return ErrRollback
	}
	if st.Source != cur.Source || st.Route != cur.Route ||
		st.FromSeq != cur.FromSeq || st.ToSeq != cur.ToSeq ||
		!st.FromTime.Equal(cur.FromTime) || !st.ToTime.Equal(cur.ToTime) ||
		!st.StartedAt.Equal(cur.StartedAt) {
		return ErrRollback
	}
	if st.Cursor < cur.Cursor ||
		st.Delivered < cur.Delivered || st.Retries < cur.Retries ||
		st.Skipped.BeforeTime < cur.Skipped.BeforeTime ||
		st.Skipped.AfterTime < cur.Skipped.AfterTime ||
		st.Skipped.Rejected < cur.Skipped.Rejected ||
		st.Skipped.Missing < cur.Skipped.Missing {
		return ErrRollback
	}
	if st.Phase == PhasePending {
		return ErrRollback // pending -> running 之后不得退回 pending
	}
	st.PrevMAC = curMAC
	mac := ps.stateMAC(curMAC, st)
	b, err := json.MarshalIndent(rpEnvelope{State: st, MAC: mac}, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomicReplay(filepath.Join(ps.dir, rpName(st.JobID)), b); err != nil {
		return err
	}
	next := map[string]string{}
	for k, v := range ps.anchors {
		next[k] = v
	}
	next[st.JobID] = mac
	if err := ps.writeAnchors(next); err != nil {
		return err
	}
	ps.anchors[st.JobID] = mac
	return nil
}

// load 读取并校验单个任务状态（锚点版本一致性由 Open 保证，这里做自校验）。
func (ps *progressStore) load(jobID string) (progressState, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.loadLocked(jobID)
}

// loadLocked 调用者持锁。
func (ps *progressStore) loadLocked(jobID string) (progressState, error) {
	b, err := os.ReadFile(filepath.Join(ps.dir, rpName(jobID)))
	if err != nil {
		if os.IsNotExist(err) {
			return progressState{}, ErrJobNotFound
		}
		return progressState{}, err
	}
	var env rpEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return progressState{}, ErrRollback
	}
	if !hmac.Equal([]byte(ps.stateMAC(env.State.PrevMAC, env.State)), []byte(env.MAC)) {
		return progressState{}, ErrRollback
	}
	return env.State, nil
}

// all 返回全部已持久化任务状态。
func (ps *progressStore) all() ([]progressState, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	out := make([]progressState, 0, len(ps.anchors))
	for id := range ps.anchors {
		st, err := ps.loadLocked(id)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobID < out[j].JobID })
	return out, nil
}
