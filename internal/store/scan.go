package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// scanOutcome reports what one segment scan found.
type scanOutcome struct {
	firstID  int64
	lastID   int64
	count    int
	torn     bool  // ends with a partial/torn frame
	validEnd int64 // bytes of the complete-frame prefix when torn
}

// scanSegment reads every complete frame of one segment file and
// verifies its self-contained hash chain: frames belong to this segment,
// seg_index is dense from 1, and each prev-hash links the prior frame
// of the same segment. Records are published into the in-memory index.
// A single torn frame at the tail of an unsealed segment (crash
// artifact) is tolerated and reported via out.torn.
func (s *Store) scanSegment(m segMeta, startID int64, _ [32]byte) (scanOutcome, int64, [32]byte, error) {
	var out scanOutcome
	out.firstID = startID
	f, err := os.Open(s.segmentPath(m))
	if err != nil {
		return out, startID, [32]byte{}, err
	}
	defer f.Close()

	var (
		expectedID   = startID
		expectedPrev [32]byte
		expectedIdx  = 1
	)

	header := make([]byte, frameHeaderLen)
	for {
		off, rerr := f.Seek(0, io.SeekCurrent)
		if rerr != nil {
			return out, expectedID, expectedPrev, rerr
		}
		n, err := io.ReadFull(f, header)
		if err == io.EOF {
			return out, expectedID, expectedPrev, nil
		}
		if err == io.ErrUnexpectedEOF || (err == nil && n < frameHeaderLen) {
			out.torn = true
			out.validEnd = off
			s.tailIssue(m, true, "partial frame header at tail")
			return out, expectedID, expectedPrev, nil
		}
		if err != nil {
			return out, expectedID, expectedPrev, err
		}
		var magic [4]byte
		copy(magic[:], header[:4])
		if magic != frameMagic {
			return out, expectedID, expectedPrev,
				fmt.Errorf("%w: %s bad magic at offset %d", ErrTampered, m.File, off)
		}
		bodyLen := int64(binary.BigEndian.Uint32(header[4:8]))
		if bodyLen > maxFrameSize {
			return out, expectedID, expectedPrev,
				fmt.Errorf("%w: %s oversized frame", ErrTampered, m.File)
		}
		total := frameHeaderLen + bodyLen + frameHashLen
		frame := make([]byte, total)
		copy(frame, header)
		if _, err := io.ReadFull(f, frame[frameHeaderLen:]); err != nil {
			out.torn = true
			out.validEnd = off
			s.tailIssue(m, true, "incomplete frame body at tail")
			return out, expectedID, expectedPrev, nil
		}
		body := frame[frameHeaderLen : frameHeaderLen+bodyLen]
		var storedHash [32]byte
		copy(storedHash[:], frame[frameHeaderLen+bodyLen:])

		r, calc, derr := decodeRecord(body, storedHash)
		if derr != nil {
			s.issues = append(s.issues, TamperIssue{
				Segment: m.File, Kind: "MODIFIED",
				Detail: fmt.Sprintf("offset %d: %v", off, derr),
			})
			continue
		}
		if calc != storedHash {
			s.issues = append(s.issues, TamperIssue{
				Segment: m.File, Kind: "MODIFIED",
				Detail: fmt.Sprintf("record %d content/hash mismatch", r.ID),
			})
		}
		if segName, segIdx := frameSegment(body); segName != m.File {
			s.issues = append(s.issues, TamperIssue{
				Segment: m.File, Kind: "REORDERED",
				Detail: fmt.Sprintf("record %d claims segment %q", r.ID, segName),
			})
		} else if segIdx != expectedIdx {
			s.issues = append(s.issues, TamperIssue{
				Segment: m.File, Kind: "REORDERED",
				Detail: fmt.Sprintf("record %d seg_index=%d expected %d",
					r.ID, segIdx, expectedIdx),
			})
		}
		if r.ID != expectedID {
			s.issues = append(s.issues, TamperIssue{
				Segment: m.File, Kind: "REORDERED",
				Detail: fmt.Sprintf("expected record id %d, found %d", expectedID, r.ID),
			})
		}
		if expectedIdx > 1 && r.PrevHash != expectedPrev {
			s.issues = append(s.issues, TamperIssue{
				Segment: m.File, Kind: "REORDERED",
				Detail: fmt.Sprintf("record %d prev-hash chain broken", r.ID),
			})
		}

		s.index[r.ID] = loc{
			segment:     m.File,
			offset:      off,
			length:      total,
			eventID:     r.EventID,
			source:      r.Source,
			seq:         r.Seq,
			retainUntil: r.RetainUntil.UnixNano(),
		}
		s.byEvent[r.Source+"\x00"+r.EventID] = r.ID
		if out.count == 0 {
			out.firstID = r.ID
		}
		out.lastID = r.ID
		out.count++
		expectedID = r.ID + 1
		expectedPrev = storedHash
		expectedIdx++
	}
}

// tailIssue records a torn-tail anomaly as TRUNCATED only for sealed
// segments; an unsealed active segment's torn tail is a crash artifact
// that gets truncated away, not an attack.
func (s *Store) tailIssue(m segMeta, torn bool, detail string) {
	if m.Sealed {
		s.issues = append(s.issues, TamperIssue{
			Segment: m.File, Kind: "TRUNCATED", Detail: detail,
		})
	}
}
