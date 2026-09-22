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
	p, err := pipeline.New(pipeline.Config{
		Dir:           *dir,
		Router:        r,
		Sinks:         map[string]sink.Sink{"audit": ms},
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
		"deleted", res.DeletedSegments, "skipped_frozen", res.SkippedFrozen)

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
