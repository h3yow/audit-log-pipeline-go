package checkpoint

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
)

// ErrRollback 表示检查点发生回退或被篡改：
//   - 试图把水位推进到 <= 当前值；
//   - 状态文件 MAC 与自身链锚不匹配；
//   - 全局锚点记录的最新 MAC 与状态文件不一致（旧文件被换回复刻）。
//
// 此类情况一律显式报错，绝不静默重放。
var ErrRollback = errors.New("checkpoint: rollback or tampering detected")

// State 是单个来源的投递检查点。
type State struct {
	Source        string `json:"source"`
	Watermark     int64  `json:"watermark"`
	PrevMAC       string `json:"prev_mac"` // 链：上一版状态 MAC（首版为创世锚）
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

// Manager 持久化每个来源的检查点。
type Manager struct {
	dir     string
	key     []byte
	mu      sync.Mutex
	states  map[string]State
	anchors map[string]string // source -> 当前版本 MAC
}

type cpEnvelope struct {
	State State  `json:"state"`
	MAC   string `json:"mac"` // MAC(key, domain || prevMAC || canonical(state-body))
}

type anchorsFile struct {
	Anchors map[string]string `json:"anchors"`
	MAC     string            `json:"mac"`
}

const anchorsName = "anchors.json"

// Open 打开检查点目录并执行全部链校验，任何断裂返回 ErrRollback。
func Open(dir string, keyFile string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	key, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, errors.New("checkpoint: bad key length")
	}
	m := &Manager{dir: dir, key: key, states: map[string]State{}, anchors: map[string]string{}}

	// 1. 加载并校验锚点文件。
	anchors := map[string]string{}
	if b, err := os.ReadFile(filepath.Join(dir, anchorsName)); err == nil {
		var af anchorsFile
		if err := json.Unmarshal(b, &af); err != nil {
			return nil, ErrRollback
		}
		if !hmac.Equal([]byte(af.MAC), []byte(m.anchorsMAC(af.Anchors))) {
			return nil, ErrRollback
		}
		anchors = af.Anchors
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	m.anchors = anchors
	anchorDirty := false

	// 2. 加载每个来源状态并与锚点交叉校验。
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".cp" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		var env cpEnvelope
		if err := json.Unmarshal(b, &env); err != nil {
			return nil, ErrRollback
		}
		want := m.stateMAC(env.State.PrevMAC, env.State)
		if !hmac.Equal([]byte(want), []byte(env.MAC)) {
			return nil, ErrRollback
		}
		if committed, ok := anchors[env.State.Source]; ok {
			if committed != env.MAC {
				// 状态文件不是锚点指向的版本：
				if env.State.PrevMAC == committed {
					// 崩溃窗口：新版状态已落盘、锚点未更新——合法的前向提交，补提交锚点。
					anchors[env.State.Source] = env.MAC
					anchorDirty = true
				} else {
					// 既非已提交版本、也不是它的直接后继 → 回退/复刻，拒绝。
					return nil, ErrRollback
				}
			}
			// committed == env.MAC（当前版本）时，stateMAC 校验已绑定 PrevMAC，
			// 无密钥无法伪造，无需再独立比对历史锚点。
		} else {
			// 无锚点：只允许首版（PrevMAC 为创世锚）。
			if env.State.PrevMAC != genesisMAC(env.State.Source) {
				return nil, ErrRollback
			}
			anchors[env.State.Source] = env.MAC
			anchorDirty = true
		}
		m.states[env.State.Source] = env.State
	}
	// 3. 反向：锚点存在但状态文件缺失，同样视为链断裂。
	for source := range anchors {
		if _, ok := m.states[source]; !ok {
			return nil, ErrRollback
		}
	}
	if anchorDirty {
		af := anchorsFile{Anchors: anchors, MAC: m.anchorsMAC(anchors)}
		ab, err := json.MarshalIndent(af, "", "  ")
		if err != nil {
			return nil, err
		}
		ap := filepath.Join(dir, anchorsName)
		tmp := ap + ".tmp"
		if err := os.WriteFile(tmp, ab, 0o644); err != nil {
			return nil, err
		}
		if err := os.Rename(tmp, ap); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func genesisMAC(source string) string {
	g := sha256.Sum256([]byte("GENESIS:" + source))
	return hex.EncodeToString(g[:])
}

func (m *Manager) stateMAC(prevMAC string, st State) string {
	body, _ := json.Marshal(struct {
		Source        string
		Watermark     int64
		PrevMAC       string
		UpdatedAtUnix int64
	}{st.Source, st.Watermark, st.PrevMAC, st.UpdatedAtUnix})
	mac := hmac.New(sha256.New, m.key)
	mac.Write([]byte("checkpoint-v1"))
	mac.Write([]byte(prevMAC))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func (m *Manager) anchorsMAC(a map[string]string) string {
	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	mac := hmac.New(sha256.New, m.key)
	mac.Write([]byte("anchors-v1"))
	for _, k := range keys {
		mac.Write([]byte(k))
		mac.Write([]byte(a[k]))
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func cpName(source string) string {
	return hex.EncodeToString([]byte(source)) + ".cp"
}

// Get 读取来源检查点；不存在返回零值（Watermark=0）。
func (m *Manager) Get(source string) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.states[source], nil
}

// Advance 仅允许水位严格单调递增地推进；回退或相等返回 ErrRollback。
func (m *Manager) Advance(source string, watermark int64, nowUnix int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.states[source]
	if watermark <= cur.Watermark {
		return ErrRollback
	}
	prevMAC := genesisMAC(source)
	if cur.Watermark > 0 {
		prevMAC = m.anchors[source]
	}
	next := State{
		Source:        source,
		Watermark:     watermark,
		PrevMAC:       prevMAC,
		UpdatedAtUnix: nowUnix,
	}
	mac := m.stateMAC(prevMAC, next)

	// 先原子写状态文件，再更新锚点（锚点提交 = 可见）。
	b, err := json.MarshalIndent(cpEnvelope{State: next, MAC: mac}, "", "  ")
	if err != nil {
		return err
	}
	cpPath := filepath.Join(m.dir, cpName(source))
	tmp := cpPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, cpPath); err != nil {
		return err
	}

	newAnchors := map[string]string{}
	for k, v := range m.anchors {
		newAnchors[k] = v
	}
	newAnchors[source] = mac
	af := anchorsFile{Anchors: newAnchors, MAC: m.anchorsMAC(newAnchors)}
	ab, err := json.MarshalIndent(af, "", "  ")
	if err != nil {
		return err
	}
	ap := filepath.Join(m.dir, anchorsName)
	atmp := ap + ".tmp"
	if err := os.WriteFile(atmp, ab, 0o644); err != nil {
		return err
	}
	if err := os.Rename(atmp, ap); err != nil {
		return err
	}
	m.states[source] = next
	m.anchors[source] = mac
	return nil
}

// All 返回全部来源检查点（重启恢复用）。
func (m *Manager) All() ([]State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]State, 0, len(m.states))
	for _, st := range m.states {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Source < out[j].Source })
	return out, nil
}

// Close 释放资源。
func (m *Manager) Close() error { return nil }
