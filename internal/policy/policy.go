// Package policy decides whether an event is valid, novel and where
// it must be routed.
package policy

import (
	"errors"
	"sync"

	"github.com/h3yow/audit-log-pipeline-go/internal/event"
)

// ErrOverloaded is returned when bounded capacity is exhausted.
var ErrOverloaded = errors.New("pipeline capacity exhausted (downstream unavailable)")

// Action is the policy decision for an event.
type Action string

const (
	ActionAllow Action = "ALLOW"
	ActionDeny  Action = "DENY"
)

// Rule is one ordered policy rule. Empty Source/Type matches any value.
// The first matching rule wins; a DENY rule rejects the event and the
// event is quarantined with the rule name.
type Rule struct {
	Name   string
	Source string
	Type   string
	Action Action
}

// Decision is the result of evaluating one event.
type Decision struct {
	Action  Action
	Rule    string // name of the matched rule; "" when only the default applies
	Targets []string
	Reason  string
}

// Engine validates, de-duplicates and routes events.
type Engine struct {
	mu             sync.Mutex
	defaultTargets []string
	rules          []Rule
	seen           map[string]int64 // key source|id -> seq of accepted event
}

// NewEngine builds an engine with default allow targets and ordered rules.
func NewEngine(defaultTargets []string, rules []Rule) *Engine {
	targets := append([]string(nil), defaultTargets...)
	return &Engine{
		defaultTargets: targets,
		rules:          append([]Rule(nil), rules...),
		seen:           make(map[string]int64),
	}
}

func dedupKey(source, id string) string { return source + "\x00" + id }

// IsDuplicate reports whether the event id was already accepted in this
// source. A re-submission with the same id but a different seq is still
// a duplicate: the id is the idempotency key.
func (e *Engine) IsDuplicate(ev event.Event) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.seen[dedupKey(ev.Source, ev.ID)]
	return ok
}

// Evaluate returns the policy decision for a (validated, novel) event.
func (e *Engine) Evaluate(ev event.Event) Decision {
	for _, r := range e.rules {
		if r.Source != "" && r.Source != ev.Source {
			continue
		}
		if r.Type != "" && r.Type != ev.Type {
			continue
		}
		if r.Action == ActionDeny {
			return Decision{
				Action: ActionDeny,
				Rule:   r.Name,
				Reason: "matched deny rule " + r.Name,
			}
		}
		// Explicit ALLOW rule: fall through to default targets but keep
		// the rule name recorded on the decision.
		return Decision{
			Action:  ActionAllow,
			Rule:    r.Name,
			Targets: append([]string(nil), e.defaultTargets...),
		}
	}
	return Decision{
		Action:  ActionAllow,
		Targets: append([]string(nil), e.defaultTargets...),
	}
}

// MarkAccepted records an event as the accepted occurrence of its id.
func (e *Engine) MarkAccepted(ev event.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seen[dedupKey(ev.Source, ev.ID)] = ev.Seq
}

// ResetSeen replaces the in-memory dedup index, e.g. after rebuilding
// it from the durable store on restart.
func (e *Engine) ResetSeen(accepted map[string]int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	next := make(map[string]int64, len(accepted))
	for k, v := range accepted {
		next[k] = v
	}
	e.seen = next
}
