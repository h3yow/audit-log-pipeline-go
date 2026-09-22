// Package store implements the durable, tamper-evident local audit
// repository: append-only hash-chained segment files, retention and
// legal-hold enforcement, and a quarantine journal for inputs that are
// malformed, duplicate or policy-denied.
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/internal/event"
)

var (
	// ErrDuplicate is returned when an event id is already stored for a source.
	ErrDuplicate = errors.New("duplicate event")
	// ErrTampered is returned (or recorded in VerifyReport) when on-disk
	// content was modified, deleted or inserted out of order.
	ErrTampered = errors.New("store tampered")
	// ErrFrozen is returned when a cleanup touches a frozen record.
	ErrFrozen = errors.New("record under legal hold")
	// ErrNotFound is returned for unknown ids or already-cleaned records.
	ErrNotFound = errors.New("record not found")
)

// DefaultSegmentBytes rolls the active segment at 1 MiB.
const DefaultSegmentBytes int64 = 1 << 20

// Record is one persisted audit record with retention metadata.
type Record struct {
	ID          int64
	EventID     string
	Source      string
	Seq         int64
	Type        string
	OccurredAt  time.Time
	StoredAt    time.Time
	Payload     string
	RetainUntil time.Time
	// FreezeKey is non-empty while a legal hold is active.
	FreezeKey string
	PrevHash  [32]byte
	Hash      [32]byte
}

// RetentionPolicy maps a source to a retention duration.
type RetentionPolicy struct {
	DefaultRetention time.Duration
	PerSource        map[string]time.Duration
	// MaxSegmentBytes controls segment roll size; zero uses the default.
	MaxSegmentBytes int64
}

// RetentionFor returns the retention duration of a source.
func (p RetentionPolicy) RetentionFor(source string) time.Duration {
	if d, ok := p.PerSource[source]; ok {
		return d
	}
	return p.DefaultRetention
}

// Freeze describes an active legal hold over a record id range.
type Freeze struct {
	Key       string
	FromID    int64
	ToID      int64
	CreatedAt time.Time
	Note      string
}

// CleanupReport summarises one cleanup pass.
type CleanupReport struct {
	At              time.Time
	Examined        int
	DeletedSegments int
	DeletedRecords  int
	KeptFrozen      int
	KeptRetained    int
	DeletedFromID   int64
	DeletedToID     int64
}

// TamperIssue describes one integrity violation found at startup.
type TamperIssue struct {
	Segment string `json:"segment"`
	Kind    string `json:"kind"`
	Detail  string `json:"detail"`
}

// Stats reports bounded storage observables.
type Stats struct {
	RecordCount      int64         `json:"record_count"`
	BytesOnDisk      int64         `json:"bytes_on_disk"`
	Segments         int           `json:"segments"`
	ActiveFreezes    int           `json:"active_freezes"`
	LastVerifiedAt   time.Time     `json:"last_verified_at"`
	TamperIssues     []TamperIssue `json:"tamper_issues"`
	LastCleanupAt    time.Time     `json:"last_cleanup_at"`
	LastDeletedToID  int64         `json:"last_deleted_to_id"`
	LastDeletedCount int           `json:"last_deleted_count"`
}

// Store is the durable audit repository.
type Store struct {
	mu sync.RWMutex

	dir       string
	segDir    string
	retention RetentionPolicy
	segBytes  int64

	manifest    manifest
	seqSegment  int
	active      *segmentWriter
	nextID      int64
	index       map[int64]loc    // record id -> location/meta
	byEvent     map[string]int64 // source\x00eventID -> id
	freezes     map[string]Freeze
	issues      []TamperIssue
	verifiedAt  time.Time
	lastCleanup time.Time
	lastDelTo   int64
	lastDelN    int
}

// Open loads (or creates) a store under dir, verifies integrity, and
// applies the given retention policy to future appends. Tampering is
// recorded (visible via Stats/Verify) rather than aborting, except for
// unreadable structural metadata.
func Open(dir string, retention RetentionPolicy) (*Store, error) {
	segDir := filepath.Join(dir, "segments")
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		return nil, fmt.Errorf("store mkdir: %w", err)
	}
	s := &Store{
		dir:       dir,
		segDir:    segDir,
		retention: retention,
		segBytes:  DefaultSegmentBytes,
		index:     make(map[int64]loc),
		byEvent:   make(map[string]int64),
		freezes:   make(map[string]Freeze),
		nextID:    1,
	}
	if retention.MaxSegmentBytes > 0 {
		s.segBytes = retention.MaxSegmentBytes
	}
	if err := s.loadOrInitManifest(); err != nil {
		return nil, err
	}
	if err := s.replayFreezes(); err != nil {
		return nil, err
	}
	if err := s.loadSegments(); err != nil {
		return nil, err
	}
	s.verifiedAt = time.Now().UTC()
	return s, nil
}

// loadSegments rebuilds the in-memory index. A torn frame at the tail
// of the (unsealed) active segment is treated as a crash artifact: only
// the sealed prefix is indexed and the torn tail is truncated away.
func (s *Store) loadSegments() error {
	namesOnDisk := map[string]bool{}
	entries, err := os.ReadDir(s.segDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".arec") {
			namesOnDisk[e.Name()] = true
		}
	}

	order := append([]segMeta(nil), s.manifest.Segments...)
	sort.SliceStable(order, func(i, j int) bool { return order[i].File < order[j].File })

	var (
		expectedID int64 = 1
		tornFile   string
		tornValid  int64
	)
	for _, m := range order {
		path := s.segmentPath(m)
		if m.Deleted {
			if namesOnDisk[m.File] {
				// Data marked deleted must not reappear.
				s.issues = append(s.issues, TamperIssue{
					Segment: m.File, Kind: "RESURRECTED",
					Detail: "segment was deleted per manifest but exists on disk",
				})
				if err := os.Remove(path); err != nil {
					return err
				}
			}
			delete(namesOnDisk, m.File)
			continue
		}
		delete(namesOnDisk, m.File)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			s.issues = append(s.issues, TamperIssue{
				Segment: m.File, Kind: "DELETED",
				Detail: "live segment file missing",
			})
			continue
		} else if err != nil {
			return err
		}
		out, nextID, _, err := s.scanSegment(m, expectedID, [32]byte{})
		if err != nil {
			return err
		}
		if out.count > 0 {
			expectedID = nextID
		}
		if out.torn && !m.Sealed {
			tornFile = m.File
			tornValid = out.validEnd
		}
	}
	// Files on disk that the manifest does not know about are evidence
	// of out-of-band insertion attempts.
	for extra := range namesOnDisk {
		s.issues = append(s.issues, TamperIssue{
			Segment: extra, Kind: "INSERTED",
			Detail: "segment file not present in manifest",
		})
	}
	if tornFile != "" {
		if err := os.Truncate(filepath.Join(s.segDir, tornFile), tornValid); err != nil {
			return fmt.Errorf("truncate torn tail: %w", err)
		}
	}
	s.nextID = expectedID
	return nil
}

// Close flushes and releases files.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil {
		if err := s.sealSegment(s.active); err != nil {
			return err
		}
		s.active = nil
	}
	return nil
}

// LatestID returns the highest committed record id (0 when empty).
func (s *Store) LatestID() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextID - 1
}

// Stats returns observable storage state.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.statsLocked()
}

func (s *Store) statsLocked() Stats {
	var bytes int64
	live := 0
	for _, m := range s.manifest.Segments {
		if m.Deleted {
			continue
		}
		live++
		if fi, err := os.Stat(s.segmentPath(m)); err == nil {
			bytes += fi.Size()
		}
	}
	issues := append([]TamperIssue(nil), s.issues...)
	return Stats{
		RecordCount:      int64(len(s.index)),
		BytesOnDisk:      bytes,
		Segments:         live,
		ActiveFreezes:    len(s.freezes),
		LastVerifiedAt:   s.verifiedAt,
		TamperIssues:     issues,
		LastCleanupAt:    s.lastCleanup,
		LastDeletedToID:  s.lastDelTo,
		LastDeletedCount: s.lastDelN,
	}
}

// Append durably stores a validated, allowed event and returns its record.
func (s *Store) Append(ev event.Event) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ev.Source + "\x00" + ev.ID
	if _, ok := s.byEvent[key]; ok {
		return nil, fmt.Errorf("%w: source=%s id=%s", ErrDuplicate, ev.Source, ev.ID)
	}

	id := s.nextID
	now := time.Now().UTC()
	r := Record{
		ID:          id,
		EventID:     ev.ID,
		Source:      ev.Source,
		Seq:         ev.Seq,
		Type:        ev.Type,
		OccurredAt:  ev.OccurredAt,
		StoredAt:    now,
		Payload:     ev.Payload,
		RetainUntil: now.Add(s.retention.RetentionFor(ev.Source)),
		FreezeKey:   s.freezeKeyFor(id),
	}
	// Encode against the current active segment to learn the real frame
	// size; if the frame would cross the segment boundary, seal and create
	// a fresh segment and re-encode (the frame embeds segment/position).
	if s.active == nil {
		w, err := s.createSegment(id)
		if err != nil {
			return nil, err
		}
		s.active = w
	}
	if s.active.count > 0 {
		r.PrevHash = s.active.lastHash
	}
	frame, err := encodeRecord(r, s.active.meta.File, s.active.count+1)
	if err != nil {
		return nil, err
	}
	if s.active.bytes > 0 && s.active.bytes+int64(len(frame)) > s.segBytes {
		if err := s.sealSegment(s.active); err != nil {
			return nil, err
		}
		w, err := s.createSegment(id)
		if err != nil {
			return nil, err
		}
		s.active = w
		// Fresh segment: chain restarts and frame is re-encoded.
		r.PrevHash = [32]byte{}
		frame, err = encodeRecord(r, s.active.meta.File, 1)
		if err != nil {
			return nil, err
		}
	}

	off := s.active.bytes
	if _, err := s.active.out.Write(frame); err != nil {
		return nil, err
	}
	if err := s.active.out.Flush(); err != nil {
		return nil, err
	}
	if err := s.active.file.Sync(); err != nil {
		return nil, err
	}
	// Commit point: the frame is durable. Publish to in-memory state.
	s.active.bytes += int64(len(frame))
	s.active.count++
	s.active.meta.ToID = id
	r.Hash = hashOf(frame)
	s.active.lastHash = r.Hash
	s.nextID++
	s.index[id] = loc{
		segment:     s.active.meta.File,
		offset:      off,
		length:      int64(len(frame)),
		eventID:     ev.ID,
		source:      ev.Source,
		seq:         ev.Seq,
		retainUntil: r.RetainUntil.UnixNano(),
	}
	s.byEvent[key] = id
	return &r, nil
}

func hashOf(frame []byte) [32]byte {
	var h [32]byte
	copy(h[:], frame[len(frame)-frameHashLen:])
	return h
}

// freezeKeyFor returns the key of any active freeze covering id.
// Caller may hold the lock.
func (s *Store) freezeKeyFor(id int64) string {
	for _, f := range s.freezes {
		if id >= f.FromID && (f.ToID == 0 || id <= f.ToID) {
			return f.Key
		}
	}
	return ""
}

// PutFreeze / ReleaseFreeze manage legal holds durably.
func (s *Store) PutFreeze(f Freeze) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.Key == "" {
		return errors.New("freeze requires a key")
	}
	if f.ToID == 0 {
		f.ToID = s.nextID - 1
	}
	if f.CreatedAt.IsZero() {
		f.CreatedAt = time.Now().UTC()
	}
	if err := s.appendFreezeLine(freezeLine{
		Op: "put", Key: f.Key, FromID: f.FromID, ToID: f.ToID,
		CreatedAt: f.CreatedAt, Note: f.Note,
	}); err != nil {
		return err
	}
	s.freezes[f.Key] = f
	return nil
}

func (s *Store) ReleaseFreeze(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.freezes[key]; !ok {
		return fmt.Errorf("freeze %q not found", key)
	}
	if err := s.appendFreezeLine(freezeLine{Op: "release", Key: key}); err != nil {
		return err
	}
	delete(s.freezes, key)
	return nil
}

// Freezes returns the active legal holds.
func (s *Store) Freezes() []Freeze {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Freeze, 0, len(s.freezes))
	for _, f := range s.freezes {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Compile-time assertion that context stays used by ReadBetween.
var _ = context.Background
