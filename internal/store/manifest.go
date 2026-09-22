package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	manifestName = "manifest.json"
	freezesName  = "freezes.log"
)

// manifest is the authoritative list of segments. Deletion of a segment
// is first recorded here (atomically, with directory fsync) and only
// then applied to disk, so a crash cannot resurrect or hide data.
type manifest struct {
	Version    int       `json:"version"`
	SeqSegment int       `json:"seq_segment"`
	Segments   []segMeta `json:"segments"`
}

func (s *Store) manifestPath() string { return filepath.Join(s.dir, manifestName) }

// persistManifest atomically rewrites the manifest and fsyncs the dir.
func (s *Store) persistManifest() error {
	s.manifest.Version = 1
	s.manifest.SeqSegment = s.seqSegment
	tmp, err := os.CreateTemp(s.dir, "manifest.*.tmp")
	if err != nil {
		return fmt.Errorf("manifest write: %w", err)
	}
	tmpName := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s.manifest); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
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
	if err := os.Rename(tmpName, s.manifestPath()); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("manifest rename: %w", err)
	}
	return syncDir(s.dir)
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

func (s *Store) loadOrInitManifest() error {
	b, err := os.ReadFile(s.manifestPath())
	if os.IsNotExist(err) {
		s.manifest = manifest{Version: 1}
		return s.persistManifest()
	}
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	if err := json.Unmarshal(b, &s.manifest); err != nil {
		return fmt.Errorf("%w: manifest unreadable: %v", ErrTampered, err)
	}
	if s.manifest.Version != 1 {
		return fmt.Errorf("%w: unknown manifest version %d", ErrTampered, s.manifest.Version)
	}
	s.seqSegment = s.manifest.SeqSegment
	return nil
}

type freezeLine struct {
	Op        string    `json:"op"` // put | release
	Key       string    `json:"key"`
	FromID    int64     `json:"from_id,omitempty"`
	ToID      int64     `json:"to_id,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	Note      string    `json:"note,omitempty"`
	At        time.Time `json:"at"`
}

// replayFreezes rebuilds active freezes from the append-only journal.
func (s *Store) replayFreezes() error {
	path := filepath.Join(s.dir, freezesName)
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := readJSONLines(f, func(line []byte) error {
		var fl freezeLine
		if err := json.Unmarshal(line, &fl); err != nil {
			return fmt.Errorf("%w: freezes journal unreadable: %v", ErrTampered, err)
		}
		switch fl.Op {
		case "put":
			s.freezes[fl.Key] = Freeze{
				Key:       fl.Key,
				FromID:    fl.FromID,
				ToID:      fl.ToID,
				CreatedAt: fl.CreatedAt,
				Note:      fl.Note,
			}
		case "release":
			delete(s.freezes, fl.Key)
		default:
			return fmt.Errorf("%w: unknown freeze op %q", ErrTampered, fl.Op)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Store) appendFreezeLine(fl freezeLine) error {
	fl.At = time.Now().UTC()
	b, err := json.Marshal(fl)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(s.dir, freezesName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func readJSONLines(f *os.File, fn func([]byte) error) error {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		if err := fn(line); err != nil {
			return err
		}
	}
	return sc.Err()
}
