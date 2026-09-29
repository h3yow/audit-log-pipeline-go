package pipeline

import (
	"context"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/replay"
	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
)

// ReplayRange 是 Pipeline 层的回放范围（与 replay.Range 对应，
// 时间指针用 time.Time，nil 表示该侧无界）。
type ReplayRange struct {
	FromSeq int64
	ToSeq   int64
	From    *time.Time
	To      *time.Time
}

// StartReplay 发起一次历史回放：把 source 在 rng 范围内留存的事件按
// 原始接入顺序重新投递到独立目标下游 target（必须在 ReplaySinks 中注册）。
// 返回范围判定 + 进度快照；任何范围外/跳过/空洞都在 Plan 中显式给出。
func (p *Pipeline) StartReplay(ctx context.Context, jobID, source, target string, rng ReplayRange) (replay.Progress, error) {
	prog, err := p.repm.Start(ctx, replay.StartRequest{
		JobID:  jobID,
		Source: source,
		Target: target,
		Rng: replay.Range{
			Source:  source,
			FromSeq: rng.FromSeq,
			ToSeq:   rng.ToSeq,
			From:    rng.From,
			To:      rng.To,
		},
	})
	if err != nil {
		return replay.Progress{}, err
	}
	p.log.Info("replay started",
		"job", jobID, "source", source, "target", target,
		"seq_range", [2]int64{prog.Plan.EffFromSeq, prog.Plan.EffToSeq},
		"matched", prog.Plan.Matched, "gaps", len(prog.Plan.Gaps),
		"skipped", len(prog.Plan.Skipped))
	return *prog, nil
}

// ReplayProgress 查询回放进度：当前位置、已投、剩余、重试次数、状态。
func (p *Pipeline) ReplayProgress(jobID string) (replay.Progress, error) {
	return p.repm.Get(jobID)
}

// AllReplays 返回全部回放任务进度。
func (p *Pipeline) AllReplays() ([]replay.Progress, error) {
	return p.repm.All()
}

// WaitReplay 等待回放任务结束（complete/blocked）或 ctx 取消。
func (p *Pipeline) WaitReplay(ctx context.Context, jobID string) (replay.Progress, error) {
	return p.repm.Wait(ctx, jobID)
}

// RegisterReplaySink 在运行期注册/替换回放目标下游（例如重建后恢复）。
func (p *Pipeline) RegisterReplaySink(name string, s sink.Sink) {
	p.repm.RegisterSink(name, s)
}

// ResumeReplay 让 blocked/paused 的回放继续。
func (p *Pipeline) ResumeReplay(jobID string) error {
	return p.repm.Resume(jobID)
}
