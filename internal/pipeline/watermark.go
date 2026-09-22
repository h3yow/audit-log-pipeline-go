package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// watermark persists, per source, the highest store record id that has
// been reconciled into the delivery spool. It lets the reconciler scan
// only new records after a restart, so cleaned historical records are
// never re-enqueued.
type watermark struct {
	mu        sync.Mutex
	path      string
	perSource map[string]int64
}

type watermarkFile struct {
	PerSource map[string]int64 `json:"per_source"`
	UpdatedAt time.Time        `json:"updated_at"`
}

func openWatermark(dir string) (*watermark, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &watermark{
		path:      filepath.Join(dir, "reconcile-watermark.json"),
		perSource: map[string]int64{},
	}
	b, err := os.ReadFile(w.path)
	if os.IsNotExist(err) {
		return w, nil
	}
	if err != nil {
		return nil, err
	}
	var f watermarkFile
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	if f.PerSource != nil {
		w.perSource = f.PerSource
	}
	return w, nil
}

// get returns the reconciled high-water mark for a source.
func (w *watermark) get(source string) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.perSource[source]
}

// sources returns the sources currently tracked.
func (w *watermark) sources() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.perSource))
	for s := range w.perSource {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// advance raises the mark for a source (monotone) and atomically persists.
func (w *watermark) advance(source string, recordID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if recordID <= w.perSource[source] {
		return nil
	}
	w.perSource[source] = recordID
	f := watermarkFile{PerSource: w.perSource, UpdatedAt: time.Now().UTC()}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(w.path), "wm.*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
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
	return os.Rename(tmpName, w.path)
}
