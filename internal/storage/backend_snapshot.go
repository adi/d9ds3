package storage

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
)

// snapshotDirs are the on-disk trees that constitute the full replicated dataset.
// (staging/ and mpstaging/ hold pre-commit payloads and are intentionally excluded.)
var snapshotDirs = []string{"buckets", "keys", "vstore", "mpu", "iam"}

// snapshotTo streams the full local dataset as a tar archive. It holds the backend
// lock so no Apply interleaves, yielding a point-in-time consistent snapshot.
func (b *posixBackend) snapshotTo(w io.Writer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tw := tar.NewWriter(w)
	for _, dir := range snapshotDirs {
		root := filepath.Join(b.root, dir)
		err := filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if fi.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(b.root, path)
			if err != nil {
				return err
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
	}
	return tw.Close()
}

// restoreFrom replaces the local dataset with the contents of a snapshot archive.
func (b *posixBackend) restoreFrom(r io.Reader) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Clear the existing dataset trees, then recreate them empty.
	for _, dir := range snapshotDirs {
		if err := os.RemoveAll(filepath.Join(b.root, dir)); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(b.root, dir), 0o755); err != nil {
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
