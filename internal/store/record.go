package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// On-disk frame layout:
//
//	magic(4)="AREC" | length(4,big-endian uint32) | payload(length bytes) | sha256(payload)[32]
//
// The payload is canonical JSON of framePayload. Each record's hash
// chains the previous record hash, so modification, deletion and
// out-of-order insertion are detectable by re-reading and re-hashing.

var frameMagic = [4]byte{'A', 'R', 'E', 'C'}

const (
	frameHeaderLen = 8
	frameHashLen   = sha256.Size
	maxFrameSize   = 8 * 1024 * 1024
)

type framePayload struct {
	ID          int64     `json:"id"`
	Segment     string    `json:"segment"`
	SegIndex    int       `json:"seg_index"` // 1-based position inside its segment
	EventID     string    `json:"event_id"`
	Source      string    `json:"source"`
	Seq         int64     `json:"seq"`
	Type        string    `json:"type"`
	OccurredAt  time.Time `json:"occurred_at"`
	StoredAt    time.Time `json:"stored_at"`
	Payload     string    `json:"payload"`
	RetainUntil time.Time `json:"retain_until"`
	FreezeKey   string    `json:"freeze_key,omitempty"`
	PrevHash    *[32]byte `json:"prev_hash,omitempty"`
}

func encodeRecord(r Record, segment string, segIndex int) ([]byte, error) {
	fp := framePayload{
		ID:          r.ID,
		Segment:     segment,
		SegIndex:    segIndex,
		EventID:     r.EventID,
		Source:      r.Source,
		Seq:         r.Seq,
		Type:        r.Type,
		OccurredAt:  r.OccurredAt,
		StoredAt:    r.StoredAt,
		Payload:     r.Payload,
		RetainUntil: r.RetainUntil,
		FreezeKey:   r.FreezeKey,
	}
	if r.PrevHash != ([32]byte{}) {
		ph := r.PrevHash
		fp.PrevHash = &ph
	}
	body, err := json.Marshal(fp)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	if fp.PrevHash != nil {
		h.Write(fp.PrevHash[:])
	}
	h.Write(body)
	sum := h.Sum(nil)

	out := make([]byte, 0, frameHeaderLen+len(body)+frameHashLen)
	out = append(out, frameMagic[:]...)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
	out = append(out, lenBuf[:]...)
	out = append(out, body...)
	out = append(out, sum...)
	return out, nil
}

// errTruncatedFrame marks a torn tail written by a crashing process.
// Startup tolerates exactly one such frame at end of a segment.
var errTruncatedFrame = errors.New("truncated frame at tail")

// readFrames calls fn for every complete frame in r.
func readFrames(r io.Reader, fn func(body []byte, hash [32]byte) error) error {
	header := make([]byte, frameHeaderLen)
	for {
		n, err := io.ReadFull(r, header)
		if err == io.EOF {
			return nil
		}
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			if n > 0 {
				return errTruncatedFrame
			}
			return nil
		}
		if err != nil {
			return err
		}
		var magic [4]byte
		copy(magic[:], header[:4])
		if magic != frameMagic {
			return fmt.Errorf("bad frame magic %q", header[:4])
		}
		bodyLen := binary.BigEndian.Uint32(header[4:8])
		if bodyLen > maxFrameSize {
			return fmt.Errorf("frame length %d exceeds max %d", bodyLen, maxFrameSize)
		}
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return errTruncatedFrame
		}
		var hash [32]byte
		if _, err := io.ReadFull(r, hash[:]); err != nil {
			return errTruncatedFrame
		}
		if err := fn(body, hash); err != nil {
			return err
		}
	}
}

func decodeRecord(body []byte, hash [32]byte) (Record, [32]byte, error) {
	var calc [32]byte
	var fp framePayload
	if err := json.Unmarshal(body, &fp); err != nil {
		return Record{}, calc, fmt.Errorf("record payload corrupt: %w", err)
	}
	h := sha256.New()
	if fp.PrevHash != nil {
		h.Write(fp.PrevHash[:])
	}
	h.Write(body)
	copy(calc[:], h.Sum(nil))
	r := Record{
		ID:          fp.ID,
		EventID:     fp.EventID,
		Source:      fp.Source,
		Seq:         fp.Seq,
		Type:        fp.Type,
		OccurredAt:  fp.OccurredAt,
		StoredAt:    fp.StoredAt,
		Payload:     fp.Payload,
		RetainUntil: fp.RetainUntil,
		FreezeKey:   fp.FreezeKey,
		Hash:        hash,
	}
	_ = fp.Segment
	_ = fp.SegIndex
	if fp.PrevHash != nil {
		r.PrevHash = *fp.PrevHash
	}
	return r, calc, nil
}

// decodeFrameAt decodes one full frame (header+payload+hash) from buf.
func decodeFrameAt(buf []byte) (Record, [32]byte, error) {
	if len(buf) < frameHeaderLen+frameHashLen {
		return Record{}, [32]byte{}, errors.New("short frame")
	}
	var magic [4]byte
	copy(magic[:], buf[:4])
	if magic != frameMagic {
		return Record{}, [32]byte{}, fmt.Errorf("bad frame magic")
	}
	bodyLen := binary.BigEndian.Uint32(buf[4:8])
	if int(frameHeaderLen+bodyLen+frameHashLen) > len(buf) {
		return Record{}, [32]byte{}, errors.New("frame length exceeds location")
	}
	body := buf[frameHeaderLen : frameHeaderLen+bodyLen]
	var hash [32]byte
	copy(hash[:], buf[frameHeaderLen+bodyLen:])
	return decodeRecord(body, hash)
}

// frameSegment extracts the embedded segment name and in-segment index
// from a frame payload without touching the hash-sensitive decode path.
func frameSegment(body []byte) (string, int) {
	var probe struct {
		Segment  string `json:"segment"`
		SegIndex int    `json:"seg_index"`
	}
	_ = json.Unmarshal(body, &probe)
	return probe.Segment, probe.SegIndex
}
