package event

import (
	"errors"
	"strings"
	"testing"
)

func TestParseEventValid(t *testing.T) {
	in := []byte(`{"id":"1","source":"a","seq":1,"type":"login","action":"signin","timestamp":"2026-01-01T00:00:00Z"}`)
	e, err := ParseEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	if e.ID != "1" || e.Source != "a" || e.Seq != 1 {
		t.Fatalf("parsed wrong: %+v", e)
	}
}

func TestParseEventMalformed(t *testing.T) {
	cases := [][]byte{
		nil,
		[]byte("   "),
		[]byte("{not json"),
		[]byte(`{"id":"1","source":"a","seq":1,"type":"x","action":"y","timestamp":"not-a-time"}`),
		[]byte(`{"id":"1","source":"a","seq":1,"type":"x","action":"y","timestamp":"2026-01-01T00:00:00Z","unknown":1}`),
		[]byte(`{"id":"1","source":"a","seq":1,"type":"x","action":"y","timestamp":"2026-01-01T00:00:00Z"}{}`),
	}
	for i, in := range cases {
		if _, err := ParseEvent(in); !errors.Is(err, ErrMalformed) {
			t.Fatalf("case %d: err=%v, want ErrMalformed", i, err)
		}
	}
}

func TestValidateRequired(t *testing.T) {
	base := func() *Event {
		return &Event{ID: "1", Source: "a", Seq: 1, Type: "t", Action: "do"}
	}
	if err := base().Validate(); err == nil || !strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("missing timestamp: %v", err)
	}
	e := base()
	e.ID = ""
	if err := e.Validate(); err == nil {
		t.Fatal("want error for empty id")
	}
	e = base()
	e.Seq = 0
	if err := e.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("seq=0: %v", err)
	}
}
