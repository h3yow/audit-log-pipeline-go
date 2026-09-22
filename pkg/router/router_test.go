package router

import (
	"testing"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
)

func TestRouterDecisions(t *testing.T) {
	r := New([]Rule{
		{Name: "deny-debug", Types: []string{"debug"}, Action: ActionDeny},
		{Name: "from-billing", Sources: []string{"billing"}, Action: ActionAccept, Route: "billing-sink"},
	}, "default-sink")

	ev := func(source, typ string) *event.Event {
		return &event.Event{ID: "x", Source: source, Seq: 1, Type: typ, Action: "a"}
	}
	if d := r.Decide(ev("web", "debug")); d.Action != ActionDeny || d.Rule != "deny-debug" {
		t.Fatalf("deny: %+v", d)
	}
	if d := r.Decide(ev("billing", "login")); d.Action != ActionAccept || d.Route != "billing-sink" {
		t.Fatalf("source rule: %+v", d)
	}
	if d := r.Decide(ev("web", "login")); d.Route != "default-sink" || d.Rule != "default" {
		t.Fatalf("default: %+v", d)
	}
}

func TestRouterAcceptWithoutRouteIsDeny(t *testing.T) {
	r := New([]Rule{{Name: "bad", Action: ActionAccept}}, "d")
	d := r.Decide(&event.Event{ID: "1", Source: "s", Seq: 1, Type: "t", Action: "a"})
	if d.Action != ActionDeny {
		t.Fatalf("accept without route: %+v", d)
	}
}
