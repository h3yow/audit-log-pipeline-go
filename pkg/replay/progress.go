package replay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

// 持久化布局（独立目录，与实时投递 checkpoints 物理隔离）：
//
//	<dir>/anchors.json            全部任务的最新版本锚点（带 MAC）
//	<dir>/<hex(jobID)>.job        单任务进度状态（带链 MAC）
//
// 链语义与 checkpoint 包相同但域独立：
//   - 版本号必须严格 +1；
//   - MAC_i = HMAC(key, "replay-v1" ‖ prevMAC ‖ canonical(body))；
//   - 状态先原子落盘，再提交锚点（锚点=可见）；
//   - 崩溃窗口（新状态已写、锚点未提）允许自动补提；
//   - 旧副本换回 / 版本倒退 / MAC 不符 => ErrRollback，绝不从头重放。

const replayAnchorsName = "anchors.json"

type jobEnvelope struct {
	Progress Progress `json:"progress"`
	MAC      string   `json:"mac"`
}

type replayAnchorsFile struct {
	Anchors map[string]string `json:"anchors"`
	MAC     string            `json:"mac"`
}

func genesisJobMAC(jobID string) string {
	g := sha256.Sum256([]byte("REPLAY-GENESIS:" + jobID))
	return hex.EncodeToString(g[:])
}

// jobBody 是参与 MAC 的规范字段（MAC 字段本身不参与）。
func jobBodyMAC(key []byte, prevMAC string, p Progress) string {
	body, _ := json.Marshal(struct {
		JobID     string
		Source    string
		Target    string
		Plan      Plan
		NextSeq   int64
		Delivered int64
		Retries   int64
		Status    Status
		Version   int64
		PrevMAC   string
		UpdatedAt string
	}{
		p.JobID, p.Source, p.Target, p.Plan, p.NextSeq, p.Delivered,
		p.Retries, p.Status, p.Version, p.PrevMAC,
		p.UpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
	})
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("replay-v1"))
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
	mac.Write([]byte("replay-anchors-v1"))
	for _, k := range keys {
		mac.Write([]byte(k))
		mac.Write([]byte(a[k]))
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func jobFileName(jobID string) string {
	return hex.EncodeToString([]byte(jobID)) + ".job"
}

// loadAll 读取并校验全部任务进度与锚点（Open 时调用）。
func (m *Manager) loadAll() error {
	anchors := map[string]string{}
	if b, err := os.ReadFile(filepath.Join(m.dir, replayAnchorsName)); err == nil {
		var af replayAnchorsFile
		if err := json.Unmarshal(b, &af); err != nil {
			return ErrRollback
		}
		if !hmac.Equal([]byte(af.MAC), []byte(m.anchorsMAC(af.Anchors))) {
			return ErrRollback
		}
		anchors = af.Anchors
	} else if !os.IsNotExist(err) {
		return err
	}
	anchorDirty := false

	files, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".job" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(m.dir, f.Name()))
		if err != nil {
			return err
		}
		var env jobEnvelope
		if err := json.Unmarshal(b, &env); err != nil {
			return ErrRollback
		}
		want := jobBodyMAC(m.key, env.Progress.PrevMAC, env.Progress)
		if !hmac.Equal([]byte(want), []byte(env.MAC)) {
			return ErrRollback
		}
		if committed, ok := anchors[env.Progress.JobID]; ok {
			if committed != env.MAC {
				if env.Progress.PrevMAC == committed {
					// 崩溃窗口：合法的前向提交，补提锚点。
					anchors[env.Progress.JobID] = env.MAC
					anchorDirty = true
				} else {
					return ErrRollback // 旧副本/分叉
				}
			}
		} else {
			// 无锚点：只允许首版（PrevMAC 为创世锚、版本为 1）。
			if env.Progress.PrevMAC != genesisJobMAC(env.Progress.JobID) || env.Progress.Version != 1 {
				return ErrRollback
			}
			anchors[env.Progress.JobID] = env.MAC
			anchorDirty = true
		}
		jb := &job{
			prog: env.Progress,
			stop: make(chan struct{}),
			done: make(chan struct{}),
		}
		close(jb.done) // 重启后未 spawn 前视为“未在运行”，Wait 立即可见终态
		m.jobs[env.Progress.JobID] = jb
	}
	for id := range anchors {
		if _, ok := m.jobs[id]; !ok {
			return ErrRollback // 锚点在、状态缺失
		}
	}
	m.anchors = anchors
	if anchorDirty {
		if err := m.writeAnchors(anchors); err != nil {
			return err
		}
	}
	return nil
}

// createJob 写入首版进度；已有锚点/状态即 ErrExists。
// 调用者持有 m.mu。
func (m *Manager) createJob(p Progress) error {
	if _, ok := m.anchors[p.JobID]; ok {
		return ErrExists
	}
	if _, ok := m.jobs[p.JobID]; ok {
		return ErrExists
	}
	p.Version = 1
	p.PrevMAC = genesisJobMAC(p.JobID)
	mac := jobBodyMAC(m.key, p.PrevMAC, p)

	if err := m.writeState(p, mac); err != nil {
		return err
	}
	newAnchors := cloneAnchors(m.anchors)
	newAnchors[p.JobID] = mac
	if err := m.writeAnchors(newAnchors); err != nil {
		return err
	}
	m.anchors = newAnchors
	m.jobs[p.JobID] = &job{prog: p, stop: make(chan struct{}), done: make(chan struct{})}
	return nil
}

// saveProgress 持久化下一版：版本由 saveProgress 内部严格 +1；
// 调用方只需设置新状态与新的 UpdatedAt，不得自行修改 Version。
// PrevMAC 必须接上当前锚点，否则 ErrRollback。
// 调用者持有 m.mu。
func (m *Manager) saveProgress(next Progress) error {
	cur, ok := m.jobs[next.JobID]
	if !ok {
		return ErrNotFound
	}
	next.Version = cur.prog.Version + 1
	prevMAC := m.anchors[next.JobID]
	if next.PrevMAC != prevMAC {
		return ErrRollback
	}
	mac := jobBodyMAC(m.key, prevMAC, next)
	if err := m.writeState(next, mac); err != nil {
		return err
	}
	newAnchors := cloneAnchors(m.anchors)
	newAnchors[next.JobID] = mac
	if err := m.writeAnchors(newAnchors); err != nil {
		return err
	}
	m.anchors = newAnchors
	cur.prog = next
	return nil
}

func (m *Manager) writeState(p Progress, mac string) error {
	b, err := json.MarshalIndent(jobEnvelope{Progress: p, MAC: mac}, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomicReplay(filepath.Join(m.dir, jobFileName(p.JobID)), b)
}

func (m *Manager) writeAnchors(a map[string]string) error {
	b, err := json.MarshalIndent(replayAnchorsFile{Anchors: a, MAC: m.anchorsMAC(a)}, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomicReplay(filepath.Join(m.dir, replayAnchorsName), b)
}

func cloneAnchors(a map[string]string) map[string]string {
	out := make(map[string]string, len(a)+1)
	for k, v := range a {
		out[k] = v
	}
	return out
}

// writeFileAtomicReplay 与 storage 的实现一致：tmp + rename + 目录 fsync。
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
