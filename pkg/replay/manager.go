package replay

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
)

// Config 回放子系统配置。
type Config struct {
	// Dir 回放进度目录（独立于实时投递 checkpoints 目录）。
	Dir string
	// KeyFile 共享的 HMAC 密钥文件（与存储同一把密钥；MAC 域前缀分隔）。
	KeyFile string
	Store   EventStore
	Sink    DeliveringSink
	// MaxBatch 预留的批量大小配置。当前实现逐条流式读取（任意时刻内存中
	// 只有一条事件 + 一个读取缓冲），该值仅用于未来批量投递扩展，默认 64。
	MaxBatch int
	// FailureRetry 下游故障首次重试退避（指数翻倍，上限 2s）。
	FailureRetry time.Duration
	Logger       *slog.Logger
}

// job 是运行期任务句柄。
type job struct {
	mu           sync.Mutex
	st           progressState
	stop         chan struct{}
	done         chan struct{}
	holdReleased bool
	canceled     bool // Cancel() 显式取消（区别于进程停机）
}

// Manager 管理历史回放任务的生命周期与进度。
type Manager struct {
	cfg Config
	ps  *progressStore

	mu   sync.Mutex
	jobs map[string]*job
	wg   sync.WaitGroup
}

// New 构造回放管理器：打开进度存储、校验进度链、为未完成任务
// 重新注册保留锁并恢复执行。
func New(cfg Config) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, errors.New("replay: dir required")
	}
	if cfg.Store == nil {
		return nil, errors.New("replay: store required")
	}
	if cfg.Sink == nil {
		return nil, errors.New("replay: sink required")
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 64
	}
	if cfg.FailureRetry <= 0 {
		cfg.FailureRetry = 50 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}
	key, err := os.ReadFile(cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, errors.New("replay: bad key length")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	ps, err := newProgressStore(cfg.Dir, key)
	if err != nil {
		return nil, err
	}
	m := &Manager{cfg: cfg, ps: ps, jobs: map[string]*job{}}

	// 恢复所有持久化任务。
	states, err := ps.all()
	if err != nil {
		return nil, err
	}
	for _, st := range states {
		j := &job{
			st:   st,
			stop: make(chan struct{}),
			done: make(chan struct{}),
		}
		m.jobs[st.JobID] = j
		if !terminalPhases[st.Phase] {
			// 重新注册保留锁，并检查恢复点之后是否已被清理。
			gaps, err := cfg.Store.AcquireReplayHold(st.JobID, st.Source, st.FromSeq, st.ToSeq)
			if err != nil {
				return nil, err
			}
			resumeFrom := st.Cursor + 1
			var intersect [][2]int64
			for _, g := range gaps {
				if g[1] >= resumeFrom {
					intersect = append(intersect, g)
				}
			}
			if len(intersect) > 0 {
				st.Skipped.Missing = int64(len(intersect))
				st.Phase = PhaseFailed
				st.LastError = fmt.Sprintf(
					"%v after resume at seq %d: %v", ErrDataGap, resumeFrom, intersect)
				st.CompletedAt = time.Now()
				st.UpdatedAt = st.CompletedAt
				if err := ps.update(st); err != nil {
					return nil, err
				}
				j.st = st
				continue
			}
			m.logResume(j)
			m.wg.Add(1)
			go m.runJob(j)
		}
	}
	return m, nil
}

// Start 校验请求、解析确定范围、注册保留锁、写首版进度并启动执行。
func (m *Manager) Start(req Request) (*Status, error) {
	if req.ID == "" || req.Source == "" || req.Route == "" {
		return nil, ErrInvalidRequest
	}
	if req.FromSeq < 0 || req.ToSeq < 0 || (req.ToSeq > 0 && req.ToSeq < req.FromSeq) {
		return nil, ErrInvalidRequest
	}
	if !req.FromTime.IsZero() && !req.ToTime.IsZero() && req.ToTime.Before(req.FromTime) {
		return nil, ErrInvalidRequest
	}
	if rc, ok := m.cfg.Sink.(interface{ HasReplayRoute(string) bool }); ok {
		if !rc.HasReplayRoute(req.Route) {
			return nil, ErrInvalidRequest
		}
	}

	lo, hi, err := m.resolveRange(req)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	if _, exists := m.jobs[req.ID]; exists {
		m.mu.Unlock()
		return nil, ErrJobExists
	}
	for _, j := range m.jobs {
		j.mu.Lock()
		conflict := j.st.Source == req.Source && !terminalPhases[j.st.Phase]
		j.mu.Unlock()
		if conflict {
			m.mu.Unlock()
			return nil, ErrConflict
		}
	}
	// 注册保留锁与缺口检查在存储层同一把锁内原子完成，Purge 无法插入。
	gaps, err := m.cfg.Store.AcquireReplayHold(req.ID, req.Source, lo, hi)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	now := time.Now()
	st := progressState{
		JobID: req.ID, Source: req.Source, Route: req.Route,
		FromSeq: lo, ToSeq: hi,
		FromTime: req.FromTime, ToTime: req.ToTime,
		Phase: PhaseRunning, StartedAt: now, UpdatedAt: now,
	}
	if len(gaps) > 0 {
		// 起始即命中已清理缺口：落盘 failed 终态并释放保留锁，
		// 不静默跳过任何一条，错误信息列出缺口序号区间。
		_ = m.ps.create(st)
		j := &job{
			st: st, stop: make(chan struct{}), done: make(chan struct{}),
		}
		m.jobs[req.ID] = j
		m.mu.Unlock()
		msg := fmt.Sprintf(
			"%v: requested [%d,%d] intersects purged gaps %v",
			ErrDataGap, lo, hi, gaps)
		st.Skipped.Missing = int64(len(gaps))
		st.Phase = PhaseFailed
		st.LastError = msg
		st.CompletedAt = time.Now()
		st.UpdatedAt = st.CompletedAt
		if err := m.ps.update(st); err != nil {
			m.cfg.Logger.Error("replay failure persist failed",
				"job", st.JobID, "err", err, "reason", msg)
		}
		j.st = st
		close(j.done)
		m.releaseHoldOnce(j)
		m.cfg.Logger.Error("replay failed before run", "job", st.JobID, "err", msg)
		st2, _ := m.Status(req.ID)
		return &st2, nil
	}
	if err := m.ps.create(st); err != nil {
		_ = m.cfg.Store.ReleaseReplayHold(req.ID)
		m.mu.Unlock()
		return nil, err
	}
	j := &job{
		st: st, stop: make(chan struct{}), done: make(chan struct{}),
	}
	m.jobs[req.ID] = j
	m.mu.Unlock()

	m.cfg.Logger.Info("replay started",
		"job", req.ID, "source", req.Source, "route", req.Route,
		"from_seq", lo, "to_seq", hi,
		"from_time", req.FromTime, "to_time", req.ToTime)
	m.wg.Add(1)
	go m.runJob(j)
	stOut, err := m.Status(req.ID)
	if err != nil {
		return nil, err
	}
	return &stOut, nil
}

// resolveRange 把开放边界解析为确定闭区间：
//   - FromSeq<=0 → 1；ToSeq<=0 → 该来源当前持久化最大序号；
//   - 来源不存在或无记录 → 返回 ErrInvalidRequest（无数据可回放，显式报错）。
func (m *Manager) resolveRange(req Request) (int64, int64, error) {
	last, err := m.cfg.Store.LastSeq(req.Source)
	if err != nil {
		return 0, 0, err
	}
	if last <= 0 {
		return 0, 0, ErrInvalidRequest
	}
	lo := req.FromSeq
	if lo <= 0 {
		lo = 1
	}
	hi := req.ToSeq
	if hi <= 0 || hi > last {
		hi = last
	}
	if lo > last {
		return 0, 0, ErrInvalidRequest
	}
	return lo, hi, nil
}

// Cancel 请求取消任务。已投递的事件不回退；取消点持久化后由运行循环退出。
func (m *Manager) Cancel(jobID string) error {
	m.mu.Lock()
	j := m.jobs[jobID]
	m.mu.Unlock()
	if j == nil {
		return ErrJobNotFound
	}
	j.mu.Lock()
	if terminalPhases[j.st.Phase] {
		j.mu.Unlock()
		return nil
	}
	j.canceled = true
	j.mu.Unlock()
	close(j.stop)
	return nil
}

// Status 查询单个任务进度快照。
func (m *Manager) Status(jobID string) (Status, error) {
	m.mu.Lock()
	j := m.jobs[jobID]
	m.mu.Unlock()
	if j == nil {
		return Status{}, ErrJobNotFound
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return statusOf(j.st), nil
}

// List 列出全部回放任务（按 JobID 排序）。
func (m *Manager) List() ([]Status, error) {
	m.mu.Lock()
	ids := make([]string, 0, len(m.jobs))
	for id := range m.jobs {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	sort.Strings(ids)
	out := make([]Status, 0, len(ids))
	for _, id := range ids {
		st, err := m.Status(id)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

// Close 停止全部运行中任务并等待退出。
//
// 语义对齐"进程停机"而非"取消任务"：未完成任务不写 canceled 终态，
// 其最新进度（已 fsync）保留在磁盘上，下次 New 时自动从 Cursor+1 续跑。
// 如需显式取消，调用 Cancel（落盘 canceled 终态）。
func (m *Manager) Close() error {
	m.mu.Lock()
	pending := make([]*job, 0, len(m.jobs))
	for _, j := range m.jobs {
		j.mu.Lock()
		live := !terminalPhases[j.st.Phase]
		j.mu.Unlock()
		if !live {
			continue
		}
		select {
		case <-j.stop:
		default:
			close(j.stop)
		}
		pending = append(pending, j)
	}
	m.mu.Unlock()
	for _, j := range pending {
		<-j.done
	}
	m.wg.Wait()
	return nil
}

func statusOf(st progressState) Status {
	return Status{
		ID: st.JobID, Source: st.Source, Route: st.Route, Phase: st.Phase,
		FromSeq: st.FromSeq, ToSeq: st.ToSeq,
		FromTime: st.FromTime, ToTime: st.ToTime,
		Cursor: st.Cursor, Delivered: st.Delivered, Retries: st.Retries,
		Skipped:     st.Skipped,
		StartedAt:   st.StartedAt,
		UpdatedAt:   st.UpdatedAt,
		CompletedAt: st.CompletedAt,
		LastError:   st.LastError,
	}
}

func (m *Manager) logResume(j *job) {
	j.mu.Lock()
	st := j.st
	j.mu.Unlock()
	m.cfg.Logger.Info("replay resumed",
		"job", st.JobID, "source", st.Source,
		"cursor", st.Cursor, "to_seq", st.ToSeq,
		"delivered", st.Delivered, "retries", st.Retries)
}

// releaseHoldOnce 幂等释放该任务的存储保留锁。
func (m *Manager) releaseHoldOnce(j *job) {
	j.mu.Lock()
	if j.holdReleased {
		j.mu.Unlock()
		return
	}
	j.holdReleased = true
	id := j.st.JobID
	j.mu.Unlock()
	_ = m.cfg.Store.ReleaseReplayHold(id)
}
