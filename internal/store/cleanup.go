package store

import (
	"os"
	"time"
)

// Cleanup deletes sealed segments in id order when EVERY record in the
// segment is both past its retention deadline and outside all active
// freezes. Segments partially retained or partially frozen are kept
// whole, so cleanup can never expose "half" state and records that are
// still protected can never be removed.
//
// Deletion order (crash-safe):
//  1. manifest marks the segment deleted and is atomically persisted +
//     fsynced (so the intent survives a crash);
//  2. the file is unlinked and the directory is fsynced.
func (s *Store) Cleanup(now time.Time) (CleanupReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rep := CleanupReport{At: now, DeletedFromID: 0, DeletedToID: 0}
	// Only examine sealed segments, ordered by file (== by id range).
	type candidate struct {
		idx  int
		meta segMeta
		from int64
		to   int64
	}
	var candidates []candidate
	for i, m := range s.manifest.Segments {
		if m.Deleted {
			continue
		}
		if !m.Sealed {
			// The active segment and everything after it stay.
			rep.Examined++
			break
		}
		rep.Examined++
		eligible, from, to, keptFrozen, keptRetained := s.segmentEligible(m, now)
		rep.KeptFrozen += keptFrozen
		rep.KeptRetained += keptRetained
		if !eligible {
			// Deletion never creates holes: stop at the first segment
			// that must be kept (retention or legal hold).
			break
		}
		candidates = append(candidates, candidate{idx: i, meta: m, from: from, to: to})
	}
	var deletedRecords int
	var deletedTo int64
	var deletedFrom int64 = -1
	for _, c := range candidates {
		m := c.meta
		// Phase 1: persist deletion intent.
		s.manifest.Segments[c.idx].Deleted = true
		s.manifest.Segments[c.idx].DeletedAt = now.UTC().Format(time.RFC3339Nano)
		if err := s.persistManifest(); err != nil {
			return rep, err
		}
		// Phase 2: remove index entries while the lock is held, so
		// concurrent readers can never resolve an id whose file is
		// about to disappear.
		var n int
		for id := c.from; id <= c.to; id++ {
			l, ok := s.index[id]
			if !ok {
				continue
			}
			delete(s.byEvent, l.source+"\x00"+l.eventID)
			delete(s.index, id)
			n++
		}
		// Phase 3: unlink and fsync the directory.
		if err := os.Remove(s.segmentPath(m)); err != nil && !os.IsNotExist(err) {
			return rep, err
		}
		if err := syncDir(s.segDir); err != nil {
			return rep, err
		}
		deletedRecords += n
		if deletedFrom < 0 {
			deletedFrom = c.from
		}
		deletedTo = c.to
		rep.DeletedSegments++
	}
	rep.DeletedRecords = deletedRecords
	if deletedFrom > 0 {
		rep.DeletedFromID = deletedFrom
	}
	rep.DeletedToID = deletedTo
	s.lastCleanup = now.UTC()
	s.lastDelTo = deletedTo
	s.lastDelN = deletedRecords
	return rep, nil
}

// segmentRange returns the actual [from,to] record ids present in a
// segment according to the in-memory index. The manifest's ToID can lag
// for the active segment, so the index is authoritative.
func (s *Store) segmentRange(m segMeta) (from, to int64, count int) {
	from = -1
	for id, l := range s.index {
		if l.segment != m.File {
			continue
		}
		if from < 0 || id < from {
			from = id
		}
		if id > to {
			to = id
		}
		count++
	}
	if from < 0 {
		return 0, 0, 0
	}
	return from, to, count
}

// segmentEligible decides whether one sealed segment may be removed.
// It also returns accounting counters for the report.
func (s *Store) segmentEligible(m segMeta, now time.Time) (eligible bool, from, to int64, keptFrozen, keptRetained int) {
	eligible = true
	from, to, _ = s.segmentRange(m)
	for id := from; id <= to; id++ {
		l, ok := s.index[id]
		if !ok || l.segment != m.File {
			continue
		}
		frozen := s.freezeKeyFor(id) != ""
		retained := now.UnixNano() < l.retainUntil
		if frozen {
			keptFrozen++
			eligible = false
		}
		if retained {
			keptRetained++
			eligible = false
		}
	}
	return eligible, from, to, keptFrozen, keptRetained
}
