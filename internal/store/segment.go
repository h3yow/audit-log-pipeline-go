package store

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// segMeta is one manifest entry describing a segment file.
type segMeta struct {
	File      string `json:"file"`
	FromID    int64  `json:"from_id"`
	ToID      int64  `json:"to_id"`
	Sealed    bool   `json:"sealed"`
	Deleted   bool   `json:"deleted"`
	DeletedAt string `json:"deleted_at,omitempty"`
}

type segmentWriter struct {
	meta     segMeta
	file     *os.File
	out      *bufio.Writer
	bytes    int64
	count    int
	lastHash [32]byte // chain head inside this segment
}

func (s *Store) segmentPath(m segMeta) string {
	return filepath.Join(s.segDir, m.File)
}

// createSegment creates a new active segment, records it in the manifest
// and durably persists the manifest BEFORE any record is written into
// the segment.
func (s *Store) createSegment(fromID int64) (*segmentWriter, error) {
	name := fmt.Sprintf("%010d.arec", s.seqSegment)
	s.seqSegment++
	m := segMeta{File: name, FromID: fromID, ToID: fromID - 1}
	f, err := os.OpenFile(filepath.Join(s.segDir, name),
		os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create segment: %w", err)
	}
	s.manifest.Segments = append(s.manifest.Segments, m)
	if err := s.persistManifest(); err != nil {
		f.Close()
		return nil, err
	}
	return &segmentWriter{meta: m, file: f, out: bufio.NewWriter(f)}, nil
}

// sealSegment flushes, syncs and marks the active segment sealed.
func (s *Store) sealSegment(w *segmentWriter) error {
	if err := w.out.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	for i := range s.manifest.Segments {
		if s.manifest.Segments[i].File == w.meta.File {
			s.manifest.Segments[i].Sealed = true
			s.manifest.Segments[i].ToID = w.meta.ToID
		}
	}
	return s.persistManifest()
}

// loc is the in-memory location of one committed record.
type loc struct {
	segment     string
	offset      int64
	length      int64
	eventID     string
	source      string
	seq         int64
	retainUntil int64 // unix nano
}

// readRecordAt reads and decodes one record by its location.
func (s *Store) readRecordAt(l loc) (Record, error) {
	f, err := os.Open(filepath.Join(s.segDir, l.segment))
	if err != nil {
		if os.IsNotExist(err) {
			return Record{}, ErrNotFound
		}
		return Record{}, err
	}
	defer f.Close()
	buf := make([]byte, l.length)
	if _, err := f.ReadAt(buf, l.offset); err != nil && err != io.EOF {
		return Record{}, err
	}
	r, calc, err := decodeFrameAt(buf)
	if err != nil {
		return Record{}, err
	}
	if r.Hash != calc {
		return Record{}, fmt.Errorf("%w: record %d hash mismatch", ErrTampered, r.ID)
	}
	r.FreezeKey = s.freezeKeyFor(r.ID)
	return r, nil
}
