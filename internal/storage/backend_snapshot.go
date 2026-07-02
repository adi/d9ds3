package storage

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
)

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

// restoreFrom replaces the local dataset with the contents of a snapshot archive.
func (b *posixBackend) restoreFrom(r io.Reader) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Clear everything except the pre-commit staging dirs, then recreate them.
	entries, _ := os.ReadDir(b.root)
	for _, e := range entries {
		os.RemoveAll(filepath.Join(b.root, e.Name()))
	}
	for _, d := range []string{"staging", "mpstaging", "versions", "buckets", "objmeta", "mpu", "iam"} {
		if err := os.MkdirAll(filepath.Join(b.root, internalDir, d), 0o755); err != nil {
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
