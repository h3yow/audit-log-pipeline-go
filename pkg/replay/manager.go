// Package replay 在既有留存存储之上提供按范围的历史事件回放：
// 指定来源与序号/时间区间，把命中的留存事件按原始接入顺序重新投递到
// 独立的目标下游，并用独立于实时投递的、带哈希链防回退的持久化进度
// 支持中断续传。
package replay

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
)

// Manager 管理全部回放任务的生命周期与持久化进度。
type Manager struct {
	dir     string
	key     []byte
	store   Store
	log     *slog.Logger
	backoff time.Duration // 单条投递失败的初始退避

	mu       sync.Mutex
	jobs     map[string]*job
	reserved map[string]struct{} // Start 判定期间预占的 jobID，防止并发重复创建
	anchors  map[string]string   // jobID -> 当前已提交版本 MAC
	sinks    map[string]Deliverer
	wg       sync.WaitGroup
	closed   bool
}

// StartRequest 描述一次回放请求。
type StartRequest struct {
	JobID  string // 调用方指定的幂等任务 ID
	Source string
	Target string // 独立目标下游名（与实时路由互不影响）
	Rng    Range
}

// job 是任务的运行期状态（骨架阶段为空壳）。
type job struct {
	prog Progress
	stop chan struct{}
	done chan struct{}
}

// Open 打开回放进度目录并校验全部已存任务的进度链。
// 重启后任务不会自动运行；目标下游由调用方注册后调用 ResumeAll。
func Open(dir string, keyFile string, store Store) (*Manager, error) {
	if dir == "" {
		return nil, errors.New("replay: dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	key, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, errors.New("replay: bad key length")
	}
	m := &Manager{
		dir:      dir,
		key:      key,
		store:    store,
		log:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
		backoff:  50 * time.Millisecond,
		jobs:     map[string]*job{},
		reserved: map[string]struct{}{},
		anchors:  map[string]string{},
		sinks:    map[string]Deliverer{},
	}
	if err := m.loadAll(); err != nil {
		return nil, err
	}
	return m, nil
}

// SetLogger 替换运行期日志器（Pipeline 用自己的 slog 实例注入）。
func (m *Manager) SetLogger(l *slog.Logger) {
	if l != nil {
		m.log = l
	}
}

// RegisterSink 注册/替换一个可用的目标下游；
// 重启后下游配置缺失的 blocked 任务可在注册后 Resume。
func (m *Manager) RegisterSink(name string, d Deliverer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sinks[name] = d
}

// Start / Resume / ResumeAll 的实现见 runner.go。

// Get 返回任务当前进度快照。
func (m *Manager) Get(jobID string) (Progress, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return Progress{}, ErrNotFound
	}
	return j.prog, nil
}

// All 返回全部回放任务的进度快照（按 JobID 排序）。
func (m *Manager) All() ([]Progress, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Progress, 0, len(m.jobs))
	for _, j := range m.jobs {
		out = append(out, j.prog)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].JobID < out[k].JobID })
	return out, nil
}

// Wait 阻塞直到任务到达终态（complete/blocked）或 ctx 取消。
// running 状态下任务的 done 通道由 worker 退出时关闭；非 running 立即返回当前状态。
func (m *Manager) Wait(ctx context.Context, jobID string) (Progress, error) {
	m.mu.Lock()
	j, ok := m.jobs[jobID]
	if !ok {
		m.mu.Unlock()
		return Progress{}, ErrNotFound
	}
	done := j.done
	prog := j.prog
	running := j.isRunningLocked()
	m.mu.Unlock()
	if !running {
		return prog, nil
	}
	select {
	case <-ctx.Done():
		m.mu.Lock()
		p := j.prog
		m.mu.Unlock()
		return p, ctx.Err()
	case <-done:
		m.mu.Lock()
		p := j.prog
		m.mu.Unlock()
		return p, nil
	}
}

// Resume 实现见 runner.go。

// Close 停止全部回放工作者；进度在每条确认后已持久化，停止不丢位置。
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	jobs := make([]*job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	m.mu.Unlock()
	for _, j := range jobs {
		select {
		case <-j.done:
			// 未运行，无需停止
		default:
			close(j.stop)
		}
	}
	m.wg.Wait()
	return nil
}
