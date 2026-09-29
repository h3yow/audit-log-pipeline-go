package pipeline

import (
	"errors"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/replay"
)

// ErrReplayDisabled 表示管道未配置回放独立目标下游。
var ErrReplayDisabled = errors.New("pipeline: replay is not enabled (ReplaySinkName unset)")

// ReplayRequest 是历史回放请求（参数语义见 replay.Request）。
type ReplayRequest struct {
	ID       string
	Source   string
	FromSeq  int64
	ToSeq    int64
	FromTime time.Time
	ToTime   time.Time
	Note     string
}

// ReplayStatus 是回放进度快照（透传 replay.Status）。
type ReplayStatus = replay.Status

func (p *Pipeline) replayEnabled() (*replay.Manager, error) {
	if p.replays == nil {
		return nil, ErrReplayDisabled
	}
	return p.replays, nil
}

// StartReplay 启动一次历史回放：
// 按来源 + 序号闭区间/时间闭区间，把留存的 accepted 事件按原始接入顺序
// 投递到独立目标下游；进度独立持久化，可中断续传。
//
// 边界语义：
//   - 序号区间闭区间，FromSeq<=0 表示从 1 开始，ToSeq<=0 表示到
//     任务启动时该来源的持久化最大序号；
//   - 时间区间闭区间（按事件自身时间戳过滤），零值表示该侧开放；
//   - rejected（命中 deny 但留存）的事件计入 skipped.rejected 不投递；
//   - 区间命中已被清理的分段时任务立即 failed，错误里列出缺口序号区间，
//     不静默跳过。回放终点在任务启动时即固定，之后新接入的事件不属于本次回放。
func (p *Pipeline) StartReplay(req ReplayRequest) (ReplayStatus, error) {
	m, err := p.replayEnabled()
	if err != nil {
		return ReplayStatus{}, err
	}
	st, err := m.Start(replay.Request{
		ID:       req.ID,
		Source:   req.Source,
		Route:    p.cfg.ReplaySinkName,
		FromSeq:  req.FromSeq,
		ToSeq:    req.ToSeq,
		FromTime: req.FromTime,
		ToTime:   req.ToTime,
		Note:     req.Note,
	})
	if err != nil {
		return ReplayStatus{}, err
	}
	return *st, nil
}

// CancelReplay 请求取消回放任务（已投递不回退，取消点持久化）。
func (p *Pipeline) CancelReplay(jobID string) error {
	m, err := p.replayEnabled()
	if err != nil {
		return err
	}
	return m.Cancel(jobID)
}

// ReplayStatus 查询单个回放任务进度：阶段、当前位置、已投递、
// 剩余、重试次数、分类跳过计数与最后错误。
func (p *Pipeline) ReplayStatus(jobID string) (ReplayStatus, error) {
	m, err := p.replayEnabled()
	if err != nil {
		return ReplayStatus{}, err
	}
	return m.Status(jobID)
}

// ListReplays 列出全部回放任务。
func (p *Pipeline) ListReplays() ([]ReplayStatus, error) {
	m, err := p.replayEnabled()
	if err != nil {
		return nil, err
	}
	return m.List()
}

// ReplayRemaining 返回任务剩余待处理序号数的最佳估计
// （终点 ToSeq - 当前 Cursor；已完成返回 0）。
func ReplayRemaining(st ReplayStatus) int64 {
	switch st.Phase {
	case replay.PhaseCompleted, replay.PhaseCanceled, replay.PhaseFailed:
		return 0
	}
	if st.ToSeq <= st.Cursor {
		return 0
	}
	return st.ToSeq - st.Cursor
}
