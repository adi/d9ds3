package storage

import (
	"os"
	"path/filepath"
	"time"
)

// DefaultStagingTTL is how long a fanned-out payload may sit unclaimed in staging
// before the sweeper reclaims it. Committed payloads are renamed into vstore within
// seconds, so anything older than this belongs to an operation that never committed
// (leader crash before commit, abandoned/superseded upload) and is safe to delete.
const DefaultStagingTTL = 15 * time.Minute

// sweepStaging deletes staging and multipart-staging payloads older than maxAge.
// It returns the number of files removed. It never touches vstore/keys/buckets.
func (b *posixBackend) sweepStaging(maxAge time.Duration, now time.Time) int {
	removed := 0
	for _, dir := range []string{"staging", "mpstaging"} {
		root := b.idir(dir)
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			if now.Sub(info.ModTime()) > maxAge {
				if os.Remove(filepath.Join(root, e.Name())) == nil {
					removed++
				}
			}
		}
	}
	return removed
}
