package pipeline

import (
	"context"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/event"
	"github.com/h3yow/audit-log-pipeline-go/internal/policy"
	"github.com/h3yow/audit-log-pipeline-go/internal/spool"
	"github.com/h3yow/audit-log-pipeline-go/internal/store"
)

// reconcileInterval bounds how often the gap scan runs. A scan also runs
// once at startup, so a crash after durable-store commit but before
// spool enqueue is recovered promptly.
const reconcileInterval = 50 * time.Millisecond

// targetsForType returns the routed targets for a stored record's type.
// Accepted stored records were ALLOW at ingest time; routing is fixed.
func (p *Pipeline) targetsForType(typ string) []string {
	d := p.eng.Evaluate(event.Event{
		ID: "reconcile-probe", Source: "probe", Seq: 1, Type: typ,
	})
	if d.Action == policy.ActionDeny {
		return nil
	}
	if len(d.Targets) > 0 {
		return d.Targets
	}
	return p.cfg.DefaultTargets
}

// reconcileOnce scans, per source, only durable records newer than that
// source's reconciliation watermark and creates any missing delivery
// obligations. It is conservative (at-least-once): spool-pending is the
// authority and downstream delivery is idempotent by record id, so a
// re-enqueued obligation never produces a duplicate visible event.
//
// The watermark advances only for contiguous prefixes fully enqueued, so
// a spool-full stop leaves the mark behind the first gap and the next
// pass resumes exactly there.
func (p *Pipeline) reconcileOnce(ctx context.Context) (filled int) {
	// Collect candidates per source above the current mark.
	sources := map[string]struct{}{}
	_ = p.st.ReadBetween(ctx, 1, p.st.LatestID(), func(rec store.Record) error {
		if rec.ID > p.wm.get(rec.Source) {
			sources[rec.Source] = struct{}{}
		}
		return nil
	})
	for src := range sources {
		from := p.wm.get(src) + 1
		nextMark := from - 1
		stop := false
		_ = p.st.ReadBetween(ctx, from, p.st.LatestID(), func(rec store.Record) error {
			if stop || rec.Source != src {
				return nil
			}
			allOK := true
			for _, target := range p.targetsForType(rec.Type) {
				if p.sp.HasPending(rec.Source, rec.EventID, target) {
					continue
				}
				if err := p.sp.Enqueue(ctx, spool.Item{
					Source:   rec.Source,
					EventID:  rec.EventID,
					Seq:      rec.Seq,
					RecordID: rec.ID,
					Target:   target,
					Payload:  recordPayload(rec),
				}); err != nil {
					// Capacity exhausted: leave the mark below this id and
					// stop; a later pass resumes here once ack frees space.
					allOK = false
					stop = true
					return nil
				}
				filled++
			}
			if allOK {
				nextMark = rec.ID
			}
			return nil
		})
		if nextMark >= from {
			_ = p.wm.advance(src, nextMark)
		}
	}
	return filled
}

// reconcileLoop periodically closes store->spool gaps until shutdown.
func (p *Pipeline) reconcileLoop(ctx context.Context) {
	defer p.wg.Done()
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.reconcileOnce(ctx)
		}
	}
}
