// Package checkpoint persists monotone per-source consumer positions in
// an append-only journal. A position that goes backwards (e.g. an old
// process replaying a stale offset after restart) is explicitly rejected
// and recorded as a regression, never silently applied.
package checkpoint

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ErrRegression is returned when a caller attempts to advance a source
// to a position lower than the durable one.
var ErrRegression = errors.New("checkpoint regression rejected")

// Position is one persisted checkpoint value.
type Position struct {
	Source       string    `json:"source"`
	Seq          int64     `json:"seq"`
	RecordID     int64     `json:"record_id"`
	UpdatedAt    time.Time `json:"updated_at"`
	AttemptedSeq int64     `json:"attempted_seq,omitempty"`
}

// Regression is an auditable record of one refused backwards move.
type Regression struct {
	At         time.Time `json:"at"`
	Source     string    `json:"source"`
	FromSeq    int64     `json:"from_seq"`
	WantedSeq  int64     `json:"wanted_seq"`
	FromRecord int64     `json:"from_record"`
}

type journalLine struct {
	Kind      string    `json:"kind"` // advance | regression
	Source    string    `json:"source"`
	Seq       int64     `json:"seq,omitempty"`
	RecordID  int64     `json:"record_id,omitempty"`
	Attempted int64     `json:"attempted,omitempty"`
	At        time.Time `json:"at"`
}

// Manager persists positions.
type Manager struct {
	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	pos     map[string]Position
	regress []Regression
}

// Open creates or loads the checkpoint journal under dir.
func Open(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint mkdir: %w", err)
	}
	path := filepath.Join(dir, "checkpoint.log")
	m := &Manager{pos: make(map[string]Position)}
	if err := m.replay(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("checkpoint open: %w", err)
	}
	m.file = f
	m.writer = bufio.NewWriter(f)
	return m, nil
}

func (m *Manager) replay(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("checkpoint replay: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var jl journalLine
		if err := json.Unmarshal(line, &jl); err != nil {
			// A torn tail from a crash is tolerated once at startup; a
			// corrupted committed line would surface here as well.
			return fmt.Errorf("checkpoint journal corrupt: %w", err)
		}
		switch jl.Kind {
		case "advance":
			cur := m.pos[jl.Source]
			if jl.Seq >= cur.Seq {
				m.pos[jl.Source] = Position{
					Source:    jl.Source,
					Seq:       jl.Seq,
					RecordID:  jl.RecordID,
					UpdatedAt: jl.At,
				}
			}
		case "regression":
			m.regress = append(m.regress, Regression{
				At:         jl.At,
				Source:     jl.Source,
				FromSeq:    m.pos[jl.Source].Seq,
				WantedSeq:  jl.Attempted,
				FromRecord: m.pos[jl.Source].RecordID,
			})
		default:
			return fmt.Errorf("checkpoint journal unknown kind %q", jl.Kind)
		}
	}
	return sc.Err()
}

func (m *Manager) writeLine(jl journalLine) error {
	b, err := json.Marshal(jl)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := m.writer.Write(b); err != nil {
		return err
	}
	if err := m.writer.Flush(); err != nil {
		return err
	}
	return m.file.Sync()
}

// Get returns the durable position for a source (zero value if unknown).
func (m *Manager) Get(source string) Position {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pos[source]
}

// All returns a snapshot of every known source position.
func (m *Manager) All() map[string]Position {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Position, len(m.pos))
	for k, v := range m.pos {
		out[k] = v
	}
	return out
}

// Advance moves a source forward. Re-advancing to the same value is an
// idempotent no-op (a restart may retry an already-committed position).
// Going backwards returns ErrRegression and appends a regression marker
// instead of changing the position.
func (m *Manager) Advance(source string, seq, recordID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.pos[source]
	if seq < cur.Seq {
		rg := Regression{
			At:         time.Now().UTC(),
			Source:     source,
			FromSeq:    cur.Seq,
			WantedSeq:  seq,
			FromRecord: cur.RecordID,
		}
		m.regress = append(m.regress, rg)
		if err := m.writeLine(journalLine{
			Kind:      "regression",
			Source:    source,
			At:        rg.At,
			Attempted: seq,
		}); err != nil {
			return fmt.Errorf("persist regression marker: %w", err)
		}
		return fmt.Errorf("%w: source=%s durable_seq=%d attempted_seq=%d",
			ErrRegression, source, cur.Seq, seq)
	}
	if seq == cur.Seq {
		return nil // idempotent
	}
	now := time.Now().UTC()
	if err := m.writeLine(journalLine{
		Kind:     "advance",
		Source:   source,
		Seq:      seq,
		RecordID: recordID,
		At:       now,
	}); err != nil {
		return err
	}
	m.pos[source] = Position{Source: source, Seq: seq, RecordID: recordID, UpdatedAt: now}
	return nil
}

// Regressions returns all refused backwards moves.
func (m *Manager) Regressions() []Regression {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Regression, len(m.regress))
	copy(out, m.regress)
	return out
}

// Close flushes the journal.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file == nil {
		return nil
	}
	if err := m.writer.Flush(); err != nil {
		return err
	}
	return m.file.Close()
}
