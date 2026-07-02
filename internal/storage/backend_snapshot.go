package storage

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
)

// removeReplicatedObjectFiles deletes object files that were written through the
// log (real, non-synthesized metadata), leaving prefilled files untouched. Called
// before applying a snapshot so replicated deletes are honored without harming
// operator-provided data.
func (b *posixBackend) removeReplicatedObjectFiles() {
	entries, _ := os.ReadDir(b.root)
	for _, e := range entries {
		if !e.IsDir() || e.Name() == internalDir {
			continue
		}
		bucket := e.Name()
		keys, _ := b.walkObjectTree(bucket)
		for _, key := range keys {
			km, err := b.readKeyMeta(bucket, key) // sidecar only (no synthesis)
			if err == nil && !km.Synthesized {
				b.removeCurrent(bucket, key)
			}
		}
	}
}

// snapshotSkip are pre-commit payload dirs excluded from snapshots (relative to root).
var snapshotSkip = map[string]bool{
	filepath.Join(internalDir, "staging"):   true,
	filepath.Join(internalDir, "mpstaging"): true,
}

// snapshotTo streams the full local dataset (the browsable object trees plus .d9,
// minus pre-commit staging) as a tar archive. It holds the backend lock so no
// Apply interleaves, yielding a point-in-time consistent snapshot.
func (b *posixBackend) snapshotTo(w io.Writer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tw := tar.NewWriter(w)
	err := filepath.Walk(b.root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(b.root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if snapshotSkip[rel] {
			return filepath.SkipDir
		}
		if fi.IsDir() {
			return nil
		}
		hdr := &tar.Header{Name: filepath.ToSlash(rel), Mode: 0o644, Size: fi.Size(), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		_, cErr := io.Copy(tw, f)
		f.Close()
		return cErr
	})
	if err != nil {
		tw.Close()
		return err
	}
	return tw.Close()
}

// restoreFrom applies a snapshot without ever destroying prefilled data. It
// reconciles only the REPLICATED key set — object files written through the log
// (real, non-synthesized metadata) — against the snapshot. Prefilled files (a
// plain file with no/synthesized metadata) are preserved: they are operator data
// and are only ever removed by an explicit S3 delete, never by snapshot install.
func (b *posixBackend) restoreFrom(r io.Reader) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove replicated object files (so deletes that happened past our log tail
	// are honored); keep prefilled files. This reads sidecars before we clear them.
	b.removeReplicatedObjectFiles()

	// Reset internal replicated state (keep pre-commit staging dirs).
	for _, d := range []string{"versions", "buckets", "objmeta", "mpu", "iam"} {
		if err := os.RemoveAll(b.idir(d)); err != nil {
			return err
		}
	}
	for _, d := range []string{"staging", "mpstaging", "versions", "buckets", "objmeta", "mpu", "iam"} {
		if err := os.MkdirAll(b.idir(d), 0o755); err != nil {
			return err
		}
	}
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		dst := filepath.Join(b.root, filepath.FromSlash(hdr.Name))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		f, err := os.Create(dst)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}
	// Reload replicated IAM accounts from the restored file.
	return b.iam.load()
}
