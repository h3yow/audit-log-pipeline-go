package replay

import (
	"errors"
	"time"
)

// ErrRollback 表示回放进度发生回退或被篡改：
//   - 进度版本号未严格单调推进；
//   - 状态文件 MAC 与自身链锚不匹配；
//   - 全局锚点记录的最新版本 MAC 与状态文件不一致（旧副本被换回）。
//
// 此类情况一律显式报错，拒绝继续，绝不静默从头重放。
var ErrRollback = errors.New("replay: progress rollback or tampering detected")

var ErrNotFound = errors.New("replay: job not found")

var ErrExists = errors.New("replay: job already exists")

var ErrFinished = errors.New("replay: job already finished")

// ErrInvalidRequest 表示回放请求本身不合法（缺来源/缺边界/区间倒置）。
var ErrInvalidRequest = errors.New("replay: invalid request")

// ErrNoTarget 表示目标下游未注册。
var ErrNoTarget = errors.New("replay: target sink not registered")

// Status 是回放任务的生命周期状态。
type Status string

const (
	StatusRunning  Status = "running"  // 正在投递或可继续
	StatusPaused   Status = "paused"   // 已持久化暂停（下游配置缺失等）
	StatusComplete Status = "complete" // 范围内全部可投记录已确认
	StatusBlocked  Status = "blocked"  // 重启后目标下游未配置，需先 RegisterSink 再 Resume
)

// Range 描述回放的选择条件：可按序号闭区间、时间闭区间或组合圈定。
// 至少要给出一个边界（序号或时间）。组合时同时满足两个维度才命中。
type Range struct {
	Source  string
	FromSeq int64      // <=0 表示不设序号下界
	ToSeq   int64      // <=0 表示不设序号上界
	From    *time.Time // nil 表示不设时间下界（包含该时刻）
	To      *time.Time // nil 表示不设时间上界（包含该时刻）
}

// SkipKind 描述一条记录未被回放的可解释原因。
type SkipKind string

const (
	SkipBeforeRange SkipKind = "before_range" // 在起始边界之前（部分命中的分段）
	SkipAfterRange  SkipKind = "after_range"  // 在终止边界之后（部分命中的分段）
	SkipRejected    SkipKind = "rejected"     // 接入时命中拒绝策略、留存但不投递
	SkipPurged      SkipKind = "purged"       // 请求范围内但已被保留清理删除
	SkipOutsideTime SkipKind = "outside_time" // 在序号范围内但不在时间区间内
)

// Skipped 是一条记录/一个空洞被跳过的显式记录。
type Skipped struct {
	Seq    int64    `json:"seq"`
	Kind   SkipKind `json:"kind"`
	Reason string   `json:"reason"`
}

// Gap 描述请求范围内缺失的序号区间，并给出可判定的解释。
type Gap struct {
	FromSeq int64  `json:"from_seq"`
	ToSeq   int64  `json:"to_seq"`
	Kind    string `json:"kind"`   // purged | missing
	Reason  string `json:"reason"` // 判定依据（清单项 deleted / 从未存在）
}

// Plan 是回放开始前对范围的静态判定结果（快照语义）。
type Plan struct {
	Source     string    `json:"source"`
	Requested  Range     `json:"requested"`
	EffFromSeq int64     `json:"job_from_seq"` // 实际扫描的序号闭区间
	EffToSeq   int64     `json:"job_to_seq"`
	Matched    int64     `json:"matched"` // 通过全部过滤、应当投递的记录数
	Gaps       []Gap     `json:"gaps"`
	Skipped    []Skipped `json:"skipped"`
	Note       string    `json:"note"`
	CreatedAt  time.Time `json:"created_at"`
}

// Progress 是一个回放任务持久化的全部状态。
type Progress struct {
	JobID     string    `json:"job_id"`
	Source    string    `json:"source"`
	Target    string    `json:"target"` // 独立目标下游名
	Plan      Plan      `json:"plan"`
	NextSeq   int64     `json:"next_seq"`  // 下一条待确认的序号
	Delivered int64     `json:"delivered"` // 已确认条数
	Retries   int64     `json:"retries"`   // 下游失败重试次数
	Status    Status    `json:"status"`
	Version   int64     `json:"version"` // 进度链版本，严格单调
	PrevMAC   string    `json:"prev_mac"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Remaining 返回按计划还剩多少条待确认。
func (p Progress) Remaining() int64 {
	r := p.Plan.Matched - p.Delivered
	if r < 0 {
		return 0
	}
	return r
}
