package pipeline

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/checkpoint"
	"github.com/h3yow/audit-log-pipeline-go/pkg/router"
	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// ErrBackpressure 表示待确认队列已满（故障期间内存有界），
// 调用方应稍后重试，事件不会被静默丢弃。
var ErrBackpressure = errors.New("pipeline: in-flight queue full (downstream unavailable)")

// Config 管道配置。
type Config struct {
	Dir           string
	Router        *router.Router
	Sinks         map[string]sink.Sink
	Retention     storage.Retention
	PurgeInterval time.Duration
	// MaxInFlight 每来源内存待确认事件上限（故障期间内存有界）。
	MaxInFlight int
	// MaxPendingDisk 磁盘待确认总量软上限；超过后 Ingest 返回 ErrBackpressure。
	MaxPendingDisk int64
	Logger         *slog.Logger
}

type srcWorker struct {
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	inflight map[int64]bool // 内存待确认序号（仅用于有界计数）
}

// Pipeline 编排采集、去重、路由、落盘、投递与检查点。
type Pipeline struct {
	cfg   Config
	log   *slog.Logger
	store *storage.Store
	cp    *checkpoint.Manager
	rsink *sink.RouterSink

	mu       sync.Mutex
	ingestMu map[string]*sync.Mutex      // 每来源接入串行化，保证顺序稳定
	dedup    map[string]map[string]int64 // source -> eventID -> seq
	workers  map[string]*srcWorker
	wg       sync.WaitGroup

	metricsMu sync.Mutex
	metrics   Metrics

	stopPurge chan struct{}
	closed    bool
}

// New 构造并恢复管道：校验存储/检查点完整性、重建去重索引、
// 重新装载崩溃时未完成的事件并继续投递。
func New(cfg Config) (*Pipeline, error) {
	if cfg.Dir == "" {
		return nil, errors.New("pipeline: dir required")
	}
	if cfg.Router == nil {
		return nil, errors.New("pipeline: router required")
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 256
	}
	if cfg.MaxPendingDisk <= 0 {
		cfg.MaxPendingDisk = 1 << 20
	}
	if cfg.PurgeInterval <= 0 {
		cfg.PurgeInterval = time.Minute
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	st, err := storage.Open(storage.Options{
		Dir:       filepath.Join(cfg.Dir, "store"),
		Retention: cfg.Retention,
	})
	if err != nil {
		return nil, fmt.Errorf("pipeline: open storage: %w", err)
	}
	keyFile := filepath.Join(cfg.Dir, "store", "key.bin")
	cpm, err := checkpoint.Open(filepath.Join(cfg.Dir, "checkpoints"), keyFile)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("pipeline: open checkpoints: %w", err)
	}

	p := &Pipeline{
		cfg:       cfg,
		log:       cfg.Logger,
		store:     st,
		cp:        cpm,
		rsink:     sink.NewRouterSink(cfg.Sinks),
		ingestMu:  map[string]*sync.Mutex{},
		dedup:     map[string]map[string]int64{},
		workers:   map[string]*srcWorker{},
		stopPurge: make(chan struct{}),
	}
	if err := p.recover(); err != nil {
		_ = st.Close()
		_ = cpm.Close()
		return nil, err
	}
	p.wg.Add(1)
	go p.purgeLoop()
	return p, nil
}

// Close 优雅关闭：停止清理循环与全部工作者，落盘数据由 fsync 保证。
func (p *Pipeline) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.stopPurge)
	workers := make([]*srcWorker, 0, len(p.workers))
	for _, w := range p.workers {
		workers = append(workers, w)
		close(w.stop)
	}
	p.mu.Unlock()
	for _, w := range workers {
		<-w.done
	}
	p.wg.Wait()
	err1 := p.cp.Close()
	err2 := p.store.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *Pipeline) purgeLoop() {
	defer p.wg.Done()
	t := time.NewTicker(p.cfg.PurgeInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stopPurge:
			return
		case <-t.C:
			res, err := p.store.Purge(time.Now().Unix())
			if err != nil {
				p.log.Error("purge failed", "err", err)
				continue
			}
			if res.DeletedSegments > 0 || res.SkippedFrozen > 0 {
				p.log.Info("purge complete",
					"segments", res.DeletedSegments,
					"records", res.DeletedRecords,
					"bytes", res.ReclaimedBytes,
					"skipped_frozen", res.SkippedFrozen)
			}
		}
	}
}

// 指标计数辅助。
func (p *Pipeline) incMetric(f func(m *Metrics)) {
	p.metricsMu.Lock()
	f(&p.metrics)
	p.metricsMu.Unlock()
}

// Metrics 是运行期可观测状态。
type Metrics struct {
	InFlight        int           `json:"in_flight"`
	Accepted        int64         `json:"accepted"`
	Duplicates      int64         `json:"duplicates"`
	Malformed       int64         `json:"malformed"`
	Rejected        int64         `json:"rejected"`
	Delivered       int64         `json:"delivered"`
	DeliveryRetries int64         `json:"delivery_retries"`
	PendingOnDisk   int64         `json:"pending_on_disk"`
	Storage         storage.Stats `json:"storage"`
}

// Metrics 返回当前指标快照。
func (p *Pipeline) Metrics() Metrics {
	p.metricsMu.Lock()
	m := p.metrics
	p.metricsMu.Unlock()
	p.mu.Lock()
	for _, w := range p.workers {
		m.InFlight += len(w.inflight)
	}
	p.mu.Unlock()
	st, _ := p.store.Stats()
	m.Storage = st
	return m
}
