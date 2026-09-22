// Package event defines the audit event model and the distinct
// processing conclusions the pipeline must report for every input.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Event is a single audit record submitted by a source.
type Event struct {
	// ID is the source-assigned unique event identifier used for dedup
	// and downstream idempotency.
	ID string `json:"id"`
	// Source identifies the producing system. Ordering is guaranteed
	// within a source, never across sources.
	Source string `json:"source"`
	// Seq is the per-source monotonically increasing sequence number.
	Seq int64 `json:"seq"`
	// Type is an application defined event type.
	Type string `json:"type"`
	// OccurredAt is the event's production timestamp.
	OccurredAt time.Time `json:"occurred_at"`
	// Payload is the opaque audit payload.
	Payload string `json:"payload"`
}

// OutcomeCode is the distinguishable conclusion reached for an input.
type OutcomeCode string

const (
	// OutcomeAccepted means the event was valid, new, allowed and durably
	// stored, and either delivered or enqueued for delivery.
	OutcomeAccepted OutcomeCode = "ACCEPTED"
	// OutcomeMalformed means the input could not be parsed or failed
	// semantic validation. Nothing about it is silently dropped: it is
	// quarantined.
	OutcomeMalformed OutcomeCode = "MALFORMED"
	// OutcomeDuplicate means an event with the same (source,id) was
	// already accepted. The duplicate is quarantined.
	OutcomeDuplicate OutcomeCode = "DUPLICATE"
	// OutcomeRejected means the event matched a DENY routing policy.
	// The rejected event is quarantined with the matching rule.
	OutcomeRejected OutcomeCode = "REJECTED"
	// OutcomeOverloaded means bounded memory/disk capacity is exhausted
	// while a downstream is down. The caller may retry the same input.
	OutcomeOverloaded OutcomeCode = "OVERLOADED"
)

// Receipt is the explicit, distinguishable conclusion for one submission.
type Receipt struct {
	Code      OutcomeCode `json:"code"`
	EventID   string      `json:"event_id"`
	Source    string      `json:"source"`
	Seq       int64       `json:"seq"`
	Reason    string      `json:"reason,omitempty"`
	Rule      string      `json:"rule,omitempty"`
	Targets   []string    `json:"targets,omitempty"`
	RecordID  int64       `json:"record_id,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// Parse decodes one raw JSON input into an Event.
func Parse(raw string) (Event, error) {
	var ev Event
	if len(raw) == 0 {
		return ev, errors.New("empty input")
	}
	if err := json.Unmarshal([]byte(raw), &ev); err != nil {
		return ev, fmt.Errorf("invalid json: %w", err)
	}
	return ev, nil
}

// Validate performs semantic validation of a parsed event.
func (e Event) Validate() error {
	switch {
	case e.ID == "":
		return errors.New("missing event id")
	case e.Source == "":
		return errors.New("missing source")
	case e.Seq <= 0:
		return errors.New("seq must be positive")
	case e.Type == "":
		return errors.New("missing event type")
	case e.OccurredAt.IsZero():
		return errors.New("missing occurred_at")
	default:
		return nil
	}
}
