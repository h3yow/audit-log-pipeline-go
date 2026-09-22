package event

import "testing"

func TestParseValidate(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		ok   bool
	}{
		{"empty", "", false},
		{"bad json", "{", false},
		{"missing id", `{"source":"s","seq":1,"type":"t","occurred_at":"2026-09-22T00:00:00Z"}`, false},
		{"bad seq", `{"id":"e","source":"s","seq":0,"type":"t","occurred_at":"2026-09-22T00:00:00Z"}`, false},
		{"valid", `{"id":"e","source":"s","seq":1,"type":"t","occurred_at":"2026-09-22T00:00:00Z"}`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev, err := Parse(c.raw)
			if c.ok {
				if err != nil || ev.Validate() != nil {
					t.Fatalf("expected valid, parse=%v validate=%v", err, ev.Validate())
				}
			} else {
				good := err == nil && ev.Validate() == nil
				if good {
					t.Fatal("expected invalid input")
				}
			}
		})
	}
}
