package pipeline

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/downstream"
	"github.com/h3yow/audit-log-pipeline-go/internal/event"
	"github.com/h3yow/audit-log-pipeline-go/internal/spool"
	"github.com/h3yow/audit-log-pipeline-go/internal/store"
)

// baseBackoff / maxBackoff bound the retry cadence while a downstream
// is unavailable, keeping the process idle and observable instead of
// busy-looping.
const (
	baseBackoff = 20 * time.Millisecond
	maxBackoff  = 2 * time.Second
)

// deliveryLoop leases items with strict per-source FIFO, delivers them
// to the routed target, acks on success and nacks (requeue at head) on
// failure with capped exponential backoff.
func (p *Pipeline) deliveryLoop(ctx context.Context, workerID int) {
	defer p.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		lease, err := p.sp.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		p.deliverOne(ctx, workerID, lease)
	}
}

func (p *Pipeline) deliverOne(ctx context.Context, workerID int, lease *spool.Lease) {
	target, ok := p.targets[lease.Item.Target]
	if !ok {
		// Misconfiguration is observable: nack with backoff rather than
		// dropping. In a real system this would raise an alert.
		log.Printf("[worker %d] unknown target %q for event %s, requeuing",
			workerID, lease.Item.Target, lease.Item.EventID)
		_ = p.sp.Nack(lease)
		sleepCtx(ctx, baseBackoff)
		return
	}

	backoff := baseBackoff
	for {
		err := target.Deliver(ctx, lease.Item.Payload)
		if err == nil {
			_ = p.sp.Ack(lease)
			log.Printf("[worker %d] delivered source=%s seq=%d event=%s record=%d target=%s attempts=%d",
				workerID, lease.Item.Source, lease.Item.Seq, lease.Item.EventID,
				lease.Item.RecordID, lease.Item.Target, lease.Attempt)
			return
		}
		if ctx.Err() != nil {
			// Shutting down: return the lease so the item is pending on
			// restart; the durable spool already contains it.
			_ = p.sp.Nack(lease)
			return
		}
		if isDown(err) {
			log.Printf("[worker %d] downstream %q down, backing off %s (source=%s seq=%d attempt=%d)",
				workerID, target.Name(), backoff, lease.Item.Source, lease.Item.Seq, lease.Attempt)
		} else {
			log.Printf("[worker %d] delivery error: %v (retrying)", workerID, err)
		}
		if !sleepCtx(ctx, backoff) {
			_ = p.sp.Nack(lease)
			return
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func isDown(err error) bool {
	return err == downstream.ErrDown
}

// sleepCtx waits for d or ctx cancellation; returns false on cancel.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// resumeDeliveries completes enqueues for a previously stored event whose
// submission did not finish (e.g. OVERLOADED). Already-pending targets
// are not re-enqueued. It returns a non-nil receipt to return (overload,
// or duplicate once everything was already delivered), or nil when at
// least one new obligation was durably enqueued (treated as accepted).
func (p *Pipeline) resumeDeliveries(ctx context.Context, rec store.Record, ev event.Event, raw string, now time.Time) *event.Receipt {
	dec := p.eng.Evaluate(ev)
	payload := recordPayload(rec)
	added := 0
	for _, target := range dec.Targets {
		if p.sp.HasPending(rec.Source, rec.EventID, target) {
			continue // obligation already durable/leased
		}
		if err := p.sp.Enqueue(ctx, spool.Item{
			Source: rec.Source, EventID: rec.EventID, Seq: rec.Seq,
			RecordID: rec.ID, Target: target, Payload: payload,
		}); err != nil {
			p.bump(func() { p.overloaded++ })
			return &event.Receipt{
				Code: event.OutcomeOverloaded, EventID: ev.ID, Source: ev.Source,
				Seq: ev.Seq, RecordID: rec.ID, Targets: dec.Targets,
				Reason:    "bounded spool full on retry resume: " + err.Error(),
				Timestamp: now,
			}
		}
		added++
	}
	if added == 0 {
		// Everything for this event was already enqueued or delivered:
		// it is a plain duplicate, recorded as such (not silently lost).
		reason := "event already accepted and deliveries complete or pending"
		_ = p.q.AppendEvent(store.QuarantineDuplicate, ev, reason, "")
		p.bump(func() { p.quarantined++ })
		return &event.Receipt{
			Code: event.OutcomeDuplicate, EventID: ev.ID, Source: ev.Source,
			Seq: ev.Seq, RecordID: rec.ID, Reason: reason, Timestamp: now,
		}
	}
	_ = p.cp.Advance(rec.Source, rec.Seq, rec.ID)
	p.eng.MarkAccepted(ev)
	p.bump(func() { p.accepted++ })
	return &event.Receipt{
		Code: event.OutcomeAccepted, EventID: ev.ID, Source: ev.Source,
		Seq: ev.Seq, RecordID: rec.ID, Targets: dec.Targets, Timestamp: now,
	}
}

func recordPayload(rec store.Record) []byte {
	b, _ := json.Marshal(map[string]any{
		"record_id":   rec.ID,
		"event_id":    rec.EventID,
		"source":      rec.Source,
		"seq":         rec.Seq,
		"type":        rec.Type,
		"occurred_at": rec.OccurredAt,
		"payload":     rec.Payload,
		"stored_at":   rec.StoredAt,
	})
	return b
}
