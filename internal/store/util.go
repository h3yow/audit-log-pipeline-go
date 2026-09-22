package store

import (
	"os"
	"sort"
	"strings"
)

func sortInts(a []int64) { sort.Slice(a, func(i, j int) bool { return a[i] < a[j] }) }

func readDirNames(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".arec") {
			out = append(out, e.Name())
		}
	}
	return out, nil
}
