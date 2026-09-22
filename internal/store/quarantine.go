package store

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/event"
)

const quarantineName = "quarantine.log"

// QuarantineKind distinguishes why an input was not accepted.
type QuarantineKind string

const (
	QuarantineMalformed QuarantineKind = "MALFORMED"
	QuarantineDuplicate QuarantineKind = "DUPLICATE"
	QuarantineRejected  QuarantineKind = "REJECTED"
)

// QuarantineEntry records one non-accepted input so it is never silently lost.
type QuarantineEntry struct {
	At      time.Time      `json:"at"`
	Kind    QuarantineKind `json:"kind"`
	Source  string         `json:"source,omitempty"`
	EventID string         `json:"event_id,omitempty"`
	Reason  string         `json:"reason,omitempty"`
	Rule    string         `json:"rule,omitempty"`
	Raw     string         `json:"raw"`
}

type quarantineLine struct {
	QuarantineEntry
	PrevHash *[32]byte `json:"prev_hash,omitempty"`
	Hash     [32]byte  `json:"hash"`
}

// Quarantine is a small append-only hash-chained journal of inputs that
// were malformed, duplicated or policy-denied. It makes "rejected" data
// auditable instead of silently dropped.
type Quarantine struct {
	mu   sync.Mutex
	file *os.File
	w    *bufio.Writer
	prev [32]byte
}

// OpenQuarantine opens (or creates) the quarantine journal under dir and
// replays it to recover the chain head.
func OpenQuarantine(dir string) (*Quarantine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, quarantineName)
	q := &Quarantine{}
	if err := q.replay(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	q.file = f
	q.w = bufio.NewWriter(f)
	return q, nil
}

func (q *Quarantine) replay(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var prev [32]byte
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ql quarantineLine
		if err := json.Unmarshal(line, &ql); err != nil {
			return fmt.Errorf("%w: quarantine journal unreadable: %v", ErrTampered, err)
		}
		// Integrity: recompute the chain hash.
		qlCopy := ql
		qlCopy.Hash = [32]byte{}
		calc := hashJSON(qlCopy)
		if calc != ql.Hash {
			return fmt.Errorf("%w: quarantine chain mismatch", ErrTampered)
		}
		prev = ql.Hash
	}
	if err := sc.Err(); err != nil {
		return err
	}
	q.prev = prev
	return nil
}

func hashJSON(v any) [32]byte {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return h
}

func (q *Quarantine) write(e QuarantineEntry) error {
	ql := quarantineLine{QuarantineEntry: e}
	if q.prev != ([32]byte{}) {
		ph := q.prev
		ql.PrevHash = &ph
	}
	ql.Hash = [32]byte{} // hash computed over the zeroed hash field
	ql.Hash = hashJSON(ql)
	b, err := json.Marshal(ql)
	if err != nil {
		return err
	}
	if _, err := q.w.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := q.w.Flush(); err != nil {
		return err
	}
	if err := q.file.Sync(); err != nil {
		return err
	}
	q.prev = ql.Hash
	return nil
}

// Append records a non-accepted raw input durably.
func (q *Quarantine) Append(kind QuarantineKind, source, eventID, raw, reason, rule string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.write(QuarantineEntry{
		At:      time.Now().UTC(),
		Kind:    kind,
		Source:  source,
		EventID: eventID,
		Raw:     raw,
		Reason:  reason,
		Rule:    rule,
	})
}

// AppendEvent records a non-accepted parsed event durably.
func (q *Quarantine) AppendEvent(kind QuarantineKind, ev event.Event, reason, rule string) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return q.Append(kind, ev.Source, ev.ID, string(raw), reason, rule)
}

// ReadAll returns every quarantine entry in journal order.
func (q *Quarantine) ReadAll() ([]QuarantineEntry, error) {
	q.mu.Lock()
	path := q.file.Name()
	q.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []QuarantineEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		var ql quarantineLine
		if err := json.Unmarshal(sc.Bytes(), &ql); err != nil {
			return nil, err
		}
		out = append(out, ql.QuarantineEntry)
	}
	return out, sc.Err()
}

// Close releases the journal.
func (q *Quarantine) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.file == nil {
		return nil
	}
	if err := q.w.Flush(); err != nil {
		return err
	}
	return q.file.Close()
}
