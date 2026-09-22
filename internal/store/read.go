package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Get reads a single stored record; cleaned records return ErrNotFound.
func (s *Store) Get(id int64) (*Record, error) {
	s.mu.RLock()
	l, ok := s.index[id]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: id=%d", ErrNotFound, id)
	}
	// readRecordAt opens the file, which requires no store lock.
	r, err := s.readRecordAt(l)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// GetByEventID looks up a record by (source, event id).
func (s *Store) GetByEventID(source, eventID string) (*Record, error) {
	s.mu.RLock()
	id, ok := s.byEvent[source+"\x00"+eventID]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: source=%s event=%s", ErrNotFound, source, eventID)
	}
	return s.Get(id)
}

// ReadBetween streams records with id in [from,to] in commit order.
// Cleaned records are never visible, and a concurrent cleanup can never
// surface a partial result: ids are resolved under the read lock, and a
// record whose file disappeared between resolution and read is skipped
// silently only when the id was deleted in a committed cleanup; any
// other mismatch is reported as tampering.
func (s *Store) ReadBetween(ctx context.Context, from, to int64, fn func(Record) error) error {
	s.mu.RLock()
	ids := make([]int64, 0)
	for id := range s.index {
		if id >= from && id <= to {
			ids = append(ids, id)
		}
	}
	s.mu.RUnlock()
	// Index order is commit order; sort a copy for stable streaming.
	sortInts(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := s.Get(id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue // committed cleanup raced with this read
			}
			return err
		}
		if err := fn(*r); err != nil {
			return err
		}
	}
	return nil
}

// Verify re-reads every live segment and checks the hash chain and
// manifest. It is the same check performed at Open and is safe to run
// concurrently with reads and cleanups.
func (s *Store) Verify() ([]TamperIssue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	issues, err := s.verifyLocked()
	if err != nil {
		return nil, err
	}
	s.issues = issues
	s.verifiedAt = time.Now().UTC()
	return append([]TamperIssue(nil), issues...), nil
}

// verifyLocked performs a full scan of live segments in file order.
func (s *Store) verifyLocked() ([]TamperIssue, error) {
	// Manifest-level checks (missing/extra/resurrected files) plus a
	// fresh content scan; start from an empty issue list.
	issues := []TamperIssue{}

	tmpIndex := map[int64]loc{}
	tmpByEvent := map[string]int64{}
	origIndex, origByEvent, origIssues := s.index, s.byEvent, s.issues
	s.index, s.byEvent, s.issues = tmpIndex, tmpByEvent, issues
	defer func() {
		s.index, s.byEvent, s.issues = origIndex, origByEvent, origIssues
	}()

	var expectedID int64 = 1
	for _, m := range s.manifest.Segments {
		if m.Deleted {
			continue
		}
		out, nextID, _, err := s.scanSegment(m, expectedID, [32]byte{})
		if err != nil {
			return append([]TamperIssue(nil), s.issues...), err
		}
		if out.count > 0 {
			expectedID = nextID
		}
	}
	// Manifest-vs-disk checks.
	onDisk, err := readDirNames(s.segDir)
	if err != nil {
		return nil, err
	}
	known := map[string]bool{}
	for _, m := range s.manifest.Segments {
		known[m.File] = m.Deleted
	}
	for _, name := range onDisk {
		if deleted, ok := known[name]; !ok {
			issues = append(issues, TamperIssue{
				Segment: name, Kind: "INSERTED",
				Detail: "segment file not present in manifest",
			})
		} else if deleted {
			issues = append(issues, TamperIssue{
				Segment: name, Kind: "RESURRECTED",
				Detail: "segment deleted per manifest exists on disk",
			})
		}
	}
	return append([]TamperIssue(nil), issues...), nil
}
