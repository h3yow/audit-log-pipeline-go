// Command auditd 是审计采集管道的本地端到端演示：
//
//	go run ./cmd/auditd -dir ./data/demo
//
// 它会：接入多来源事件（含重复、非法、拒绝、乱序输入）、
// 注入一次下游故障验证不丢不重、设置/解除合规冻结并触发清理，
// 周期性打印检查点水位与内存/磁盘指标。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/pipeline"
	"github.com/h3yow/audit-log-pipeline-go/pkg/router"
	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

func main() {
	dir := flag.String("dir", "./data/demo", "本地数据目录")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	r := router.New([]router.Rule{
		{Name: "deny-debug", Types: []string{"debug"}, Action: router.ActionDeny},
		{Name: "billing-route", Sources: []string{"billing"}, Action: router.ActionAccept, Route: "audit"},
	}, "audit")
	ms := sink.NewMemorySink()
	replayMs := sink.NewMemorySink()
	p, err := pipeline.New(pipeline.Config{
		Dir:           *dir,
		Router:        r,
		Sinks:         map[string]sink.Sink{"audit": ms},
		ReplaySinks:   map[string]sink.Sink{"replay-audit": replayMs},
		Retention:     storage.Retention{MaxAge: 24 * time.Hour},
		PurgeInterval: 10 * time.Second,
		MaxInFlight:   128,
		Logger:        logger,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start failed: %v\n", err)
		os.Exit(1)
	}
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ingest := func(e *event.Event) {
		d, err := p.Ingest(ctx, e)
		if err != nil {
			logger.Error("ingest error", "err", err)
			return
		}
		logger.Info("decision", "kind", d.Kind, "id", d.EventID,
			"source", d.Source, "seq", d.Seq, "reason", d.Reason, "route", d.Route)
	}
	ingestRaw := func(source string, v any) {
		b, _ := json.Marshal(v)
		d, _ := p.IngestRaw(ctx, source, b)
		logger.Info("decision(raw)", "kind", d.Kind, "source", d.Source, "reason", d.Reason)
	}

	// 1. 正常事件 + 重复 + 非法 + 拒绝
	now := time.Now().UTC()
	ev := func(id, src string, seq int64, typ string) *event.Event {
		return &event.Event{ID: id, Source: src, Seq: seq, Type: typ,
			Subject: "alice", Action: "signin",
			Resource: "console", Timestamp: now}
	}
	ingest(ev("evt-1", "web", 1, "login"))
	ingest(ev("evt-2", "web", 2, "login"))
	ingest(ev("evt-2-dup", "web", 2, "login")) // seq 重复：duplicate
	ingest(ev("evt-1", "web", 3, "login"))     // id 重复：duplicate
	ingest(ev("evt-3", "web", 3, "file"))
	ingest(ev("evt-4", "web", 4, "debug")) // 命中 deny
	ingest(ev("bill-1", "billing", 1, "login"))
	ingestRaw("web", map[string]any{"id": "bad"}) // malformed

	time.Sleep(500 * time.Millisecond)
	printState(logger, p, ms, "after initial ingest")

	// 2. 注入下游故障
	logger.Info("=== injecting downstream outage for 2s ===")
	ms.SetAvailable(false)
	for i := int64(4); i <= 8; i++ {
		ingest(ev(fmt.Sprintf("evt-out-%d", i), "web", i, "login"))
	}
	time.Sleep(800 * time.Millisecond)
	printState(logger, p, ms, "during outage")
	ms.SetAvailable(true)
	logger.Info("=== downstream recovered ===")
	time.Sleep(1500 * time.Millisecond)
	printState(logger, p, ms, "after recovery")

	// 3. 冻结与清理演示
	_ = p.AddFreeze(storage.Freeze{
		ID: "demo-hold", Source: "web", FromSeq: 1, ToSeq: 3,
		Until: time.Now().Add(time.Hour), Note: "demo legal hold",
	})
	res, _ := p.PurgeNow(time.Now())
	logger.Info("purge with active freeze",
		"deleted", res.DeletedSegments, "skipped_frozen", res.SkippedFrozen,
		"skipped_replay", res.SkippedReplay)

	// 4. 历史回放演示：把 web 来源 seq 1..3 的留存事件按原始顺序
	//    重新投递到独立下游 replay-audit（实时水位不受影响）。
	logger.Info("=== starting historical replay web seq[1,3] -> replay-audit ===")
	prog, err := p.StartReplay(ctx, "demo-replay-1", "web", "replay-audit",
		pipeline.ReplayRange{FromSeq: 1, ToSeq: 3})
	if err != nil {
		// 同一 JobID 重复发起是显式错误（幂等任务键）：已完成的不重放，
		// 未完成的重启后会被 ResumeAll 自动续跑。
		logger.Info("replay start skipped (job already exists; completed jobs are never replayed)",
			"err", err)
		if existing, gerr := p.ReplayProgress("demo-replay-1"); gerr == nil {
			logger.Info("existing replay", "status", existing.Status,
				"delivered", existing.Delivered, "next_seq", existing.NextSeq)
		}
	} else {
		logger.Info("replay plan",
			"job", prog.JobID, "eff_seq", [2]int64{prog.Plan.EffFromSeq, prog.Plan.EffToSeq},
			"matched", prog.Plan.Matched, "gaps", len(prog.Plan.Gaps),
			"skipped", len(prog.Plan.Skipped), "note", prog.Plan.Note)
		final, werr := p.WaitReplay(ctx, prog.JobID)
		if werr != nil {
			logger.Error("replay wait failed", "err", werr)
		} else {
			logger.Info("replay complete",
				"job", final.JobID, "status", final.Status,
				"delivered", final.Delivered, "remaining", final.Remaining(),
				"retries", final.Retries)
			logger.Info("replay target visible", "ids", replayMs.Visible("replay-audit", "web"))
		}
	}

	// 5. 时间区间回放复现：一批“很早发生、现在才接入”的历史事件。
	//    事件时间戳是 2026-01-01，落盘时间是当下（StoredAt 晚于回放区间上界）。
	//    时间回放必须按事件自身时间戳命中，而不是按落盘时间剪枝。
	logger.Info("=== ingesting a late-arriving historical batch (event ts=2026-01-01, stored now) ===")
	histBase := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := int64(1); i <= 12; i++ {
		ingest(&event.Event{
			ID: fmt.Sprintf("hist-%d", i), Source: "history", Seq: i, Type: "login",
			Subject: "bob", Action: "signin", Resource: "archive",
			Timestamp: histBase.Add(time.Duration(i) * time.Second),
		})
	}
	time.Sleep(300 * time.Millisecond)
	hFrom := histBase.Add(4 * time.Second)
	hTo := histBase.Add(8 * time.Second)
	logger.Info("=== starting TIME-range replay history [00:00:04,00:00:08] (by event timestamp) ===")
	hprog, err := p.StartReplay(ctx, "demo-replay-time-1", "history", "replay-audit",
		pipeline.ReplayRange{From: &hFrom, To: &hTo})
	if err != nil {
		logger.Info("time replay start skipped (job already exists)", "err", err)
		if existing, gerr := p.ReplayProgress("demo-replay-time-1"); gerr == nil {
			logger.Info("existing time replay", "status", existing.Status,
				"delivered", existing.Delivered, "next_seq", existing.NextSeq)
		}
	} else {
		logger.Info("time replay plan",
			"job", hprog.JobID, "eff_seq", [2]int64{hprog.Plan.EffFromSeq, hprog.Plan.EffToSeq},
			"matched", hprog.Plan.Matched, "gaps", len(hprog.Plan.Gaps),
			"skipped", len(hprog.Plan.Skipped), "note", hprog.Plan.Note)
		hfin, werr := p.WaitReplay(ctx, hprog.JobID)
		if werr != nil {
			logger.Error("time replay wait failed", "err", werr)
		} else {
			logger.Info("time replay complete",
				"job", hfin.JobID, "status", hfin.Status,
				"delivered", hfin.Delivered, "remaining", hfin.Remaining())
			logger.Info("time replay target visible (expect hist-4..hist-8)",
				"ids", replayMs.Visible("replay-audit", "history"))
		}
	}

	// 优雅关闭
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case <-time.After(500 * time.Millisecond):
	}
	logger.Info("shutdown complete; restart the same command to see recovery")
}

func printState(logger *slog.Logger, p *pipeline.Pipeline, ms *sink.MemorySink, phase string) {
	m := p.Metrics()
	logger.Info("STATE",
		"phase", phase,
		"checkpoint_web", p.CheckpointWatermark("web"),
		"checkpoint_billing", p.CheckpointWatermark("billing"),
		"visible", ms.VisibleCount(),
		"in_flight", m.InFlight,
		"pending_on_disk", m.PendingOnDisk,
		"retries", m.DeliveryRetries,
		"accepted", m.Accepted, "duplicates", m.Duplicates,
		"malformed", m.Malformed, "rejected", m.Rejected,
		"disk_bytes", m.Storage.BytesOnDisk,
		"segments", m.Storage.SegmentCount)
}
