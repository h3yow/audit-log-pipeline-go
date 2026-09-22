// Package spool is a bounded disk queue of events waiting for downstream
// delivery. It survives process restarts, keeps memory and disk usage
// bounded while downstreams are unavailable, and guarantees per-source
// FIFO redelivery without loss. Downstream idempotency keys (event ids)
// make retries free of duplicate visible effects.
package spool

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ErrSpoolFull is returned when the bounded disk capacity is exhausted.
var ErrSpoolFull = errors.New("spool disk capacity exhausted")

const (
	spoolName  = "spool.log"
	spoolMagic = "SPL1"
	headerLen  = 8
)

// Item is one queued delivery attempt.
type Item struct {
	Source     string    `json:"source"`
	EventID    string    `json:"event_id"`
	Seq        int64     `json:"seq"`
	RecordID   int64     `json:"record_id"`
	Target     string    `json:"target"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	Payload    []byte    `json:"payload"` // canonical JSON of the audit record
}

// Lease is a checked-out item. Ack removes it; Nack requeues it while
// preserving per-source FIFO.
type Lease struct {
	Item     Item
	Attempt  int
	LeasedAt time.Time
	// queue position used internally
	key uint64
}

// Stats exposes bounded-queue observables.
type Stats struct {
	Queued         int   `json:"queued"`
	BytesOnDisk    int64 `json:"bytes_on_disk"`
	BytesLimit     int64 `json:"bytes_limit"`
	MemoryBuffered int   `json:"memory_buffered"`
	Leased         int   `json:"leased"`
	TotalEnqueued  int64 `json:"total_enqueued"`
	TotalAcked     int64 `json:"total_acked"`
	TotalNacked    int64 `json:"total_nacked"`
	Compactions    int64 `json:"compactions"`
}

type entry struct {
	key     uint64
	item    Item
	attempt int
}

// Config configures capacity bounds.
type Config struct {
	Dir            string
	MaxDiskBytes   int64
	MaxMemoryItems int
}

// Spool is the durable bounded queue.
type Spool struct {
	mu sync.Mutex

	dir       string
	maxBytes  int64
	maxMemory int
	file      *os.File
	logBytes  int64
	gcBytes   int64 // bytes belonging to acked/lease-removed records
	seq       uint64

	// per-source FIFO of available entries; leased entries live in leased.
	queues         map[string][]*entry
	leased         map[uint64]*entry
	pendingKeys    map[string]bool // source|event|target -> queued/leased
	leasedBySource map[string]int  // strict per-source FIFO: one in flight
	rr             []string        // source round-robin order

	totalEnq, totalAck, totalNack, compactions int64

	// wake is closed-and-replaced under mu to wake all blocked Next
	// callers without risking a lost signal (a send-based channel can
	// drop a wake-up in the unlock->select race window).
	wake chan struct{}
}

// wakeLocked closes the current wake channel (broadcast) and installs a
// fresh one. Caller must hold mu.
func (s *Spool) wakeLocked() {
	close(s.wake)
	s.wake = make(chan struct{})
}

// Open loads or creates a spool, rebuilding pending state from the log.
func Open(cfg Config) (*Spool, error) {
	if cfg.MaxDiskBytes <= 0 {
		cfg.MaxDiskBytes = 64 << 20
	}
	if cfg.MaxMemoryItems <= 0 {
		cfg.MaxMemoryItems = 1024
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("spool mkdir: %w", err)
	}
	s := &Spool{
		dir:            cfg.Dir,
		maxBytes:       cfg.MaxDiskBytes,
		maxMemory:      cfg.MaxMemoryItems,
		queues:         make(map[string][]*entry),
		leased:         make(map[uint64]*entry),
		pendingKeys:    make(map[string]bool),
		leasedBySource: make(map[string]int),
		wake:           make(chan struct{}),
	}
	if err := s.rebuild(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(cfg.Dir, spoolName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s.file = f
	return s, nil
}

func frame(item Item) ([]byte, error) {
	body, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, headerLen+len(body))
	out = append(out, spoolMagic...)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(body)))
	out = append(out, l[:]...)
	out = append(out, body...)
	return out, nil
}

// rebuild scans the log and re-enqueues every frame, reusing the
// natural FIFO order. A torn tail is truncated. The on-disk size after
// rebuild is exactly the live prefix, so restart bounds disk usage.
func (s *Spool) rebuild() error {
	path := filepath.Join(s.dir, spoolName)
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	var valid int64
	hdr := make([]byte, headerLen)
	for {
		off, _ := f.Seek(0, io.SeekCurrent)
		n, err := io.ReadFull(f, hdr)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF || n < headerLen {
			return os.Truncate(path, off)
		}
		if err != nil {
			return err
		}
		if string(hdr[:4]) != spoolMagic {
			return fmt.Errorf("spool corrupt at offset %d", off)
		}
		bodyLen := binary.BigEndian.Uint32(hdr[4:8])
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(f, body); err != nil {
			return os.Truncate(path, off)
		}
		var item Item
		if err := json.Unmarshal(body, &item); err != nil {
			return fmt.Errorf("spool frame corrupt: %w", err)
		}
		s.seq++
		e := &entry{key: s.seq, item: item}
		s.queues[item.Source] = append(s.queues[item.Source], e)
		s.pendingKeys[pendingKey(item)] = true
		valid = off + int64(headerLen) + int64(bodyLen)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fi.Size() != valid {
		if err := os.Truncate(path, valid); err != nil {
			return err
		}
	}
	s.logBytes = valid
	s.gcBytes = 0
	s.refreshSourcesLocked()
	return nil
}

func (s *Spool) refreshSourcesLocked() {
	s.rr = s.rr[:0]
	for src := range s.queues {
		if len(s.queues[src]) > 0 {
			s.rr = append(s.rr, src)
		}
	}
	sort.Strings(s.rr)
}

// Enqueue durably appends an item; fails with ErrSpoolFull when bounded
// capacity is exhausted. Submission order per source is preserved.
func (s *Spool) Enqueue(ctx context.Context, it Item) error {
	if it.EnqueuedAt.IsZero() {
		it.EnqueuedAt = time.Now().UTC()
	}
	b, err := frame(it)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.pendingKeys[pendingKey(it)] {
		// Delivery obligation already queued/leased: idempotent no-op,
		// never a second enqueue (would create a duplicate delivery).
		s.mu.Unlock()
		return nil
	}
	if s.logBytes+int64(len(b)) > s.maxBytes {
		// Try compaction to reclaim acked space before refusing.
		if cerr := s.compactLocked(); cerr != nil {
			s.mu.Unlock()
			return cerr
		}
	}
	if s.logBytes+int64(len(b)) > s.maxBytes {
		s.mu.Unlock()
		return ErrSpoolFull
	}
	if _, err := s.file.Write(b); err != nil {
		s.mu.Unlock()
		return err
	}
	if err := s.file.Sync(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.logBytes += int64(len(b))
	s.seq++
	s.queues[it.Source] = append(s.queues[it.Source], &entry{key: s.seq, item: it})
	s.pendingKeys[pendingKey(it)] = true
	if len(s.rr) == 0 || !containsStr(s.rr, it.Source) {
		s.refreshSourcesLocked()
	}
	s.totalEnq++
	s.wakeLocked()
	s.mu.Unlock()
	return nil
}

// Next leases the oldest available item, blocking while empty,
// scheduling sources round-robin (fair) while preserving strict
// per-source FIFO.
func (s *Spool) Next(ctx context.Context) (*Lease, error) {
	for {
		s.mu.Lock()
		if e := s.dequeueLocked(); e != nil {
			s.leased[e.key] = e
			l := &Lease{Item: e.item, Attempt: e.attempt, LeasedAt: time.Now().UTC(), key: e.key}
			s.mu.Unlock()
			return l, nil
		}
		// Capture the wake channel under the lock so an enqueue that
		// happens after this point must close exactly this channel (it
		// takes the lock first) and cannot be lost.
		w := s.wake
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w:
		}
	}
}

// dequeueLocked pops round-robin from non-empty per-source queues. A
// source with an item currently leased is skipped so per-source FIFO is
// strict: the older event is always settled (ack or nack) before the
// next one of that source is handed out.
func (s *Spool) dequeueLocked() *entry {
	if len(s.rr) == 0 {
		return nil
	}
	// Search every source starting from the current rotation head.
	for try := 0; try < len(s.rr); try++ {
		src := s.rr[0]
		if s.leasedBySource[src] == 0 && len(s.queues[src]) > 0 {
			q := s.queues[src]
			e := q[0]
			s.queues[src] = q[1:]
			s.rr = s.rr[1:]
			s.rr = append(s.rr, src) // rotate to back
			s.leasedBySource[src]++
			e.attempt++
			return e
		}
		// Busy or empty right now: rotate and try the next source.
		s.rr = append(s.rr[1:], src)
	}
	return nil
}

// Ack confirms delivery: the item's bytes become GC candidates and the
// pending set shrinks immediately.
func (s *Spool) Ack(l *Lease) error {
	s.mu.Lock()
	e, ok := s.leased[l.key]
	if !ok {
		s.mu.Unlock()
		return nil // idempotent ack
	}
	delete(s.leased, l.key)
	delete(s.pendingKeys, pendingKey(e.item))
	s.leasedBySource[e.item.Source]--
	b, _ := frame(e.item)
	s.gcBytes += int64(len(b))
	s.totalAck++
	s.mu.Unlock()
	return nil
}

// Nack returns a leased item to the FRONT of its source queue, so a
// failing downstream retries the oldest item and per-source order is
// preserved. The item is already durable, so no log append is needed.
func (s *Spool) Nack(l *Lease) error {
	s.mu.Lock()
	e, ok := s.leased[l.key]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	delete(s.leased, l.key)
	s.leasedBySource[e.item.Source]--
	src := e.item.Source
	q := s.queues[src]
	// The item was the oldest in flight, so requeue at the head: a
	// failing downstream retries the oldest event first.
	s.queues[src] = append([]*entry{e}, q...)
	if !containsStr(s.rr, src) {
		s.rr = append(s.rr, src)
	}
	s.totalNack++
	s.wakeLocked()
	s.mu.Unlock()
	return nil
}

// Pending returns the queued item count (excluding active leases).
func (s *Spool) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, q := range s.queues {
		n += len(q)
	}
	return n
}

// Stats returns bounded-queue observables.
func (s *Spool) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, mem := 0, 0
	for _, q := range s.queues {
		n += len(q)
		mem += len(q)
	}
	return Stats{
		Queued:         n,
		BytesOnDisk:    s.logBytes,
		BytesLimit:     s.maxBytes,
		MemoryBuffered: mem,
		Leased:         len(s.leased),
		TotalEnqueued:  s.totalEnq,
		TotalAcked:     s.totalAck,
		TotalNacked:    s.totalNack,
		Compactions:    s.compactions,
	}
}

// compactLocked rewrites the log containing only currently pending and
// leased entries, atomically swapping the file. Caller holds mu.
func (s *Spool) compactLocked() error {
	var live []*entry
	for src, q := range s.queues {
		_ = src
		live = append(live, q...)
	}
	for _, e := range s.leased {
		live = append(live, e)
	}
	if len(live) == 0 {
		// Nothing live: truncate to empty directly.
		if err := s.file.Close(); err != nil {
			return err
		}
		nf, err := os.OpenFile(filepath.Join(s.dir, spoolName),
			os.O_TRUNC|os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		s.file = nf
		s.logBytes, s.gcBytes = 0, 0
		s.compactions++
		return nf.Sync()
	}
	// Preserve per-source FIFO in the new log for clean rebuild order.
	sort.SliceStable(live, func(i, j int) bool {
		if live[i].item.Source != live[j].item.Source {
			return live[i].item.Source < live[j].item.Source
		}
		return live[i].item.Seq < live[j].item.Seq
	})
	tmp, err := os.CreateTemp(s.dir, "spool.*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	var size int64
	for _, e := range live {
		b, err := frame(e.item)
		if err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return err
		}
		if _, err := tmp.Write(b); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return err
		}
		size += int64(len(b))
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	old := s.file
	if err := os.Rename(tmpName, filepath.Join(s.dir, spoolName)); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	nf, err := os.OpenFile(filepath.Join(s.dir, spoolName),
		os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	s.file = nf
	_ = old.Close()
	s.logBytes = size
	s.gcBytes = 0
	s.compactions++
	return nil
}

// Close flushes and releases files.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	return s.file.Close()
}

func containsStr(a []string, v string) bool {
	for _, x := range a {
		if x == v {
			return true
		}
	}
	return false
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

// pendingKey identifies one (event,target) delivery obligation.
func pendingKey(it Item) string {
	return it.Source + "\x00" + it.EventID + "\x00" + it.Target
}

// HasPending reports whether an obligation for (source,eventID,target)
// is currently queued or leased. It is used to resume interrupted
// enqueues after an OVERLOADED retry without duplicating deliveries.
func (s *Spool) HasPending(source, eventID, target string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingKeys[pendingKey(Item{Source: source, EventID: eventID, Target: target})]
}

// AbortClose drops all waiting/active callers and closes the file
// WITHOUT coordinating with in-flight workers, modelling an abrupt
// process crash. Durable frames were already fsynced at enqueue time;
// Open truncates any torn tail on the next start, so no committed data
// is lost and no file descriptor is shared across processes.
func (s *Spool) AbortClose() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Wake any blocked Next so worker goroutines observe a closed state.
	s.wakeLocked()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}
