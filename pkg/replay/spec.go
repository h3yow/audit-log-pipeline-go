package replay

import (
	"errors"
	"time"
)

// 历史回放：按来源 + 序号区间/时间区间，把留存事件按原始接入顺序
// 重新投递到独立目标下游。回放进度独立持久化，与实时投递检查点互不影响。

var (
	// ErrInvalidRequest 表示回放请求参数非法。
	ErrInvalidRequest = errors.New("replay: invalid request")
	// ErrJobNotFound 表示回放任务不存在。
	ErrJobNotFound = errors.New("replay: job not found")
	// ErrJobExists 表示同 ID 任务已存在（不允许静默覆盖）。
	ErrJobExists = errors.New("replay: job already exists")
	// ErrConflict 表示同一来源已有运行中的回放任务。
	ErrConflict = errors.New("replay: another running job for source")
	// ErrRollback 表示回放进度倒退、旧副本被换回或状态被篡改。
	ErrRollback = errors.New("replay: progress rollback or tampering detected")
	// ErrDataGap 表示请求区间命中已被清理的缺口，不能静默跳过。
	ErrDataGap = errors.New("replay: requested range intersects purged data gap")
)

// Phase 表示回放任务生命周期阶段。
type Phase string

const (
	PhasePending   Phase = "pending"
	PhaseRunning   Phase = "running"
	PhaseCompleted Phase = "completed"
	PhaseFailed    Phase = "failed"
	PhaseCanceled  Phase = "canceled"
)

// Request 描述一次历史回放请求。
//
// 范围语义（闭区间，边界均包含）：
//   - 序号范围：FromSeq<=seq<=ToSeq，FromSeq<=0 表示从 1 开始，
//     ToSeq<=0 表示以任务启动时该来源的持久化最大序号为终点；
//   - 时间范围：FromTime<=事件时间戳<=ToTime，零值表示该侧开放；
//   - 两类范围同时给出时取交集，未命中过滤条件的事件计入 skipped 并分类解释，
//     不会被"默默跳过"。
type Request struct {
	ID       string // 任务唯一标识（也是进度文件名）
	Source   string
	Route    string // 独立目标下游路由名（必须在下游注册表中存在）
	FromSeq  int64
	ToSeq    int64
	FromTime time.Time
	ToTime   time.Time
	Note     string
}

// Skipped 分类统计未重新投递的事件及原因（每种原因都显式可查）。
type Skipped struct {
	BeforeTime int64 `json:"before_time"` // 序号在区间内，但事件时间早于 FromTime
	AfterTime  int64 `json:"after_time"`  // 序号在区间内，但事件时间晚于 ToTime
	Rejected   int64 `json:"rejected"`    // 留存为 rejected（命中 deny），不投递
	Missing    int64 `json:"missing"`     // 命中已清理缺口的分段数（出现即任务 failed）
}

// Status 是回放任务的可查询进度快照。
type Status struct {
	ID          string    `json:"id"`
	Source      string    `json:"source"`
	Route       string    `json:"route"`
	Phase       Phase     `json:"phase"`
	FromSeq     int64     `json:"from_seq"`
	ToSeq       int64     `json:"to_seq"`
	FromTime    time.Time `json:"from_time"`
	ToTime      time.Time `json:"to_time"`
	Cursor      int64     `json:"cursor"`    // 已处理到的序号（投递成功或显式跳过）
	Delivered   int64     `json:"delivered"` // 已重新投递条数
	Retries     int64     `json:"retries"`   // 下游失败重试次数
	Skipped     Skipped   `json:"skipped"`   // 分类跳过计数
	StartedAt   time.Time `json:"started_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	CompletedAt time.Time `json:"completed_at"`
	LastError   string    `json:"last_error,omitempty"`
}
