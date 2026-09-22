// Package pipeline wires ingestion, policy, the durable audit store,
// the bounded delivery spool, monotone checkpoints and downstream
// delivery into one restartable local process.
package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/checkpoint"
	"github.com/h3yow/audit-log-pipeline-go/internal/downstream"
	"github.com/h3yow/audit-log-pipeline-go/internal/event"
	"github.com/h3yow/audit-log-pipeline-go/internal/policy"
	"github.com/h3yow/audit-log-pipeline-go/internal/spool"
	"github.com/h3yow/audit-log-pipeline-go/internal/store"
)

// Config configures a pipeline backed by one local directory.
type Config struct {
	Dir                 string
	Retention           store.RetentionPolicy
	Rules               []policy.Rule
	DefaultTargets      []string
	MaxSpoolDiskBytes   int64
	MaxSpoolMemItems    int
	DeliveryConcurrency int
}

// Stats is the pipeline-level observable snapshot.
type Stats struct {
	Store       store.Stats                    `json:"store"`
	Spool       spool.Stats                    `json:"spool"`
	Checkpoint  map[string]checkpoint.Position `json:"checkpoint"`
	Accepted    int64                          `json:"accepted"`
	Quarantined int64                          `json:"quarantined"`
	Overloaded  int64                          `json:"overloaded"`
}

// Pipeline is the orchestrator.
type Pipeline struct {
	cfg     Config
	targets map[string]downstream.Target

	st  *store.Store
	q   *store.Quarantine
	eng *policy.Engine
	cp  *checkpoint.Manager
	sp  *spool.Spool
	wm  *watermark

	wg      sync.WaitGroup
	closeMu sync.Mutex
	closed  bool

	ctx    context.Context
	cancel context.CancelFunc

	accepted    int64
	quarantined int64
	overloaded  int64
	countersMu  sync.Mutex
}

// New builds and opens every component. The store is integrity-checked
// on open and the dedup index and spool are rebuilt from durable state,
// so a process restart continues with the same conclusions.
func New(cfg Config, targets map[string]downstream.Target) (*Pipeline, error) {
	if cfg.DeliveryConcurrency <= 0 {
		cfg.DeliveryConcurrency = 2
	}
	if len(cfg.DefaultTargets) == 0 {
		cfg.DefaultTargets = []string{"archive"}
	}
	st, err := store.Open(cfg.Dir+"/store", cfg.Retention)
	if err != nil {
		return nil, err
	}
	q, err := store.OpenQuarantine(cfg.Dir + "/quarantine")
	if err != nil {
		st.Close()
		return nil, err
	}
	cp, err := checkpoint.Open(cfg.Dir + "/checkpoint")
	if err != nil {
		q.Close()
		st.Close()
		return nil, err
	}
	sp, err := spool.Open(spool.Config{
		Dir:            cfg.Dir + "/spool",
		MaxDiskBytes:   cfg.MaxSpoolDiskBytes,
		MaxMemoryItems: cfg.MaxSpoolMemItems,
	})
	if err != nil {
		cp.Close()
		q.Close()
		st.Close()
		return nil, err
	}
	p := &Pipeline{
		cfg:     cfg,
		targets: targets,
		st:      st,
		q:       q,
		cp:      cp,
		sp:      sp,
		eng:     policy.NewEngine(cfg.DefaultTargets, cfg.Rules),
	}
	wm, err := openWatermark(cfg.Dir)
	if err != nil {
		sp.Close()
		cp.Close()
		q.Close()
		st.Close()
		return nil, err
	}
	p.wm = wm
	p.rebuildDedupIndex()
	return p, nil
}

// rebuildDedupIndex seeds the in-memory dedup set from durable records.
func (p *Pipeline) rebuildDedupIndex() {
	accepted := map[string]int64{}
	ctx := context.Background()
	_ = p.st.ReadBetween(ctx, 1, p.st.LatestID(), func(r store.Record) error {
		accepted[r.Source+"\x00"+r.EventID] = r.Seq
		return nil
	})
	p.eng.ResetSeen(accepted)
}

// Start launches background delivery workers and the store->spool
// reconciler. Workers stop when Close/HardClose is called (or the
// supplied context is cancelled).
func (p *Pipeline) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	p.ctx = ctx
	p.cancel = cancel
	for i := 0; i < p.cfg.DeliveryConcurrency; i++ {
		p.wg.Add(1)
		go p.deliveryLoop(ctx, i)
	}
	// Recover any store->spool gap immediately (e.g. a prior crash after
	// durable store commit but before enqueue), then keep reconciling.
	p.reconcileOnce(ctx)
	p.wg.Add(1)
	go p.reconcileLoop(ctx)
}

// Submit processes one raw input and ALWAYS returns an explicit receipt:
// malformed, duplicate and rejected inputs are quarantined, never
// silently dropped. Storage order per source is submission order.
func (p *Pipeline) Submit(ctx context.Context, raw string) event.Receipt {
	now := time.Now().UTC()

	ev, err := event.Parse(raw)
	if err != nil {
		_ = p.q.Append(store.QuarantineMalformed, "", "", raw, err.Error(), "")
		p.bump(func() { p.quarantined++ })
		return event.Receipt{
			Code: event.OutcomeMalformed, Reason: err.Error(), Timestamp: now,
		}
	}
	if err := ev.Validate(); err != nil {
		_ = p.q.AppendEvent(store.QuarantineMalformed, ev, err.Error(), "")
		p.bump(func() { p.quarantined++ })
		return event.Receipt{
			Code: event.OutcomeMalformed, EventID: ev.ID, Source: ev.Source,
			Seq: ev.Seq, Reason: err.Error(), Timestamp: now,
		}
	}

	// Dedup: durable (store) check is authoritative across restarts.
	if p.eng.IsDuplicate(ev) {
		reason := "event id already accepted within source"
		_ = p.q.AppendEvent(store.QuarantineDuplicate, ev, reason, "")
		p.bump(func() { p.quarantined++ })
		return event.Receipt{
			Code: event.OutcomeDuplicate, EventID: ev.ID, Source: ev.Source,
			Seq: ev.Seq, Reason: reason, Timestamp: now,
		}
	}

	dec := p.eng.Evaluate(ev)
	if dec.Action == policy.ActionDeny {
		_ = p.q.AppendEvent(store.QuarantineRejected, ev, dec.Reason, dec.Rule)
		p.bump(func() { p.quarantined++ })
		return event.Receipt{
			Code: event.OutcomeRejected, EventID: ev.ID, Source: ev.Source,
			Seq: ev.Seq, Reason: dec.Reason, Rule: dec.Rule, Timestamp: now,
		}
	}

	// 1) Durable audit record first (global commit order; per-source
	//    submission order is stable because Store serializes appends).
	rec, err := p.st.Append(ev)
	if err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			// The durable record exists: this is a retry (typically of an
			// earlier OVERLOADED submission). Resume its missing delivery
			// obligations instead of dropping or duplicating anything.
			if existing, gerr := p.st.GetByEventID(ev.Source, ev.ID); gerr == nil {
				rcpt := p.resumeDeliveries(ctx, *existing, ev, raw, now)
				if rcpt != nil {
					return *rcpt
				}
			}
			reason := "event id already present in durable store"
			_ = p.q.AppendEvent(store.QuarantineDuplicate, ev, reason, "")
			p.bump(func() { p.quarantined++ })
			return event.Receipt{
				Code: event.OutcomeDuplicate, EventID: ev.ID, Source: ev.Source,
				Seq: ev.Seq, Reason: reason, Timestamp: now,
			}
		}
		return event.Receipt{Code: event.OutcomeMalformed, Reason: err.Error(), Timestamp: now}
	}

	// 2) Enqueue for every routed target (durable). Capacity is bounded:
	//    if a downstream outage fills the spool, the caller gets the
	//    distinct OVERLOADED conclusion and may retry; nothing is
	//    silently dropped.
	payload := recordPayload(*rec)
	for _, target := range dec.Targets {
		err := p.sp.Enqueue(ctx, spool.Item{
			Source:   rec.Source,
			EventID:  rec.EventID,
			Seq:      rec.Seq,
			RecordID: rec.ID,
			Target:   target,
			Payload:  payload,
		})
		if err != nil {
			// The audit record is already durable. The caller retries the
			// same input later; it then dedups at the store while its
			// delivery remains pending in the spool.
			p.bump(func() { p.overloaded++ })
			return event.Receipt{
				Code: event.OutcomeOverloaded, EventID: ev.ID, Source: ev.Source,
				Seq: ev.Seq, RecordID: rec.ID,
				Reason:  "bounded spool full while downstream unavailable: " + err.Error(),
				Targets: dec.Targets, Timestamp: now,
			}
		}
	}

	// 3) Now that store + spool are durable, advance the monotone ingest
	//    checkpoint. A restart replaying an older offset cannot move it
	//    backwards: Advance rejects and records regressions.
	_ = p.cp.Advance(rec.Source, rec.Seq, rec.ID)

	p.eng.MarkAccepted(ev)
	p.bump(func() { p.accepted++ })
	return event.Receipt{
		Code: event.OutcomeAccepted, EventID: ev.ID, Source: ev.Source,
		Seq: ev.Seq, RecordID: rec.ID, Targets: dec.Targets, Timestamp: now,
	}
}

func (p *Pipeline) bump(f func()) {
	p.countersMu.Lock()
	f()
	p.countersMu.Unlock()
}

// PutFreeze / ReleaseFreeze manage legal holds on the store.
func (p *Pipeline) PutFreeze(f store.Freeze) error { return p.st.PutFreeze(f) }
func (p *Pipeline) ReleaseFreeze(key string) error { return p.st.ReleaseFreeze(key) }

// Cleanup runs an immediate retention pass.
func (p *Pipeline) Cleanup(now time.Time) (store.CleanupReport, error) {
	return p.st.Cleanup(now)
}

// Verify forces an integrity verification pass.
func (p *Pipeline) Verify() ([]store.TamperIssue, error) { return p.st.Verify() }

// Checkpoints exposes durable positions.
func (p *Pipeline) Checkpoints() map[string]checkpoint.Position { return p.cp.All() }

// Regressions exposes refused checkpoint rollback attempts.
func (p *Pipeline) Regressions() []checkpoint.Regression { return p.cp.Regressions() }

// Quarantine returns non-accepted input records.
func (p *Pipeline) Quarantine() ([]store.QuarantineEntry, error) { return p.q.ReadAll() }

// Store gives tests read access to the durable repository.
func (p *Pipeline) Store() *store.Store { return p.st }

// Spool gives tests observability over the delivery queue.
func (p *Pipeline) Spool() *spool.Spool { return p.sp }

// Stats returns the combined observable snapshot.
func (p *Pipeline) Stats() Stats {
	p.countersMu.Lock()
	accepted, quar, over := p.accepted, p.quarantined, p.overloaded
	p.countersMu.Unlock()
	return Stats{
		Store:       p.st.Stats(),
		Spool:       p.sp.Stats(),
		Checkpoint:  p.cp.All(),
		Accepted:    accepted,
		Quarantined: quar,
		Overloaded:  over,
	}
}

// HardClose simulates a process crash: workers are cancelled and files
// are closed without waiting for deliveries to drain. Every Submit that
// returned ACCEPTED is already individually fsynced (store + spool); on
// reopening, a torn spool tail is truncated and pending deliveries and
// checkpoints are rebuilt, so unfinished work resumes exactly once.
func (p *Pipeline) HardClose() error {
	return p.hardClose()
}

func (p *Pipeline) hardClose() error {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return nil
	}
	p.closed = true
	p.closeMu.Unlock()

	if p.cancel != nil {
		p.cancel()
	}
	// Wait for workers to observe cancellation so no goroutine touches
	// the spool file after it is closed (avoids a close-vs-write race).
	// Delivery workers exit promptly on a cancelled Next/Deliver; bounded
	// by a short deadline as a safety net.
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	_ = p.sp.AbortClose()
	_ = p.cp.Close()
	_ = p.q.Close()
	return p.st.Close()
}

// Close stops workers and closes all components.
func (p *Pipeline) Close() error {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return nil
	}
	p.closed = true
	p.closeMu.Unlock()

	if p.cancel != nil {
		p.cancel() // unblock delivery workers before closing the spool
	}
	p.wg.Wait()
	_ = p.sp.Close()
	_ = p.cp.Close()
	_ = p.q.Close()
	return p.st.Close()
}
