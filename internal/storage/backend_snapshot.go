package storage

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Snapshot tar entries are namespaced by root so restore can route them.
const (
	snapData  = "data/"  // the browsable object tree (files carry xattrs via PAX records)
	snapState = "state/" // internal bookkeeping (versions/history/buckets/mpu/iam)
)

// paxXattrPrefix is the conventional PAX record prefix for extended attributes,
// so object metadata rides inside the snapshot.
const paxXattrPrefix = "SCHILY.xattr."

// removeReplicatedObjectFiles deletes object files that were written through the
// log (they carry our managed xattr), leaving prefilled plain files untouched.
// Called before applying a snapshot so replicated deletes are honored without ever
// harming operator-provided data.
func (b *posixBackend) removeReplicatedObjectFiles() {
	entries, _ := os.ReadDir(b.root)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		bucket := e.Name()
		keys, _ := b.walkObjectTree(bucket)
		for _, key := range keys {
			if op, err := b.objectPath(bucket, key); err == nil && isManaged(op) {
				b.removeCurrent(bucket, key)
			}
		}
	}
}

// snapshotTo streams the full dataset — the object tree (with each managed file's
// xattrs as PAX records) and the state root (minus pre-commit staging) — as a tar.
// It holds the backend lock so no Apply interleaves.
func (b *posixBackend) snapshotTo(w io.Writer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tw := tar.NewWriter(w)
	// 1. Object tree → data/ entries, carrying xattrs.
	if err := b.tarTree(tw, b.root, snapData, true, nil); err != nil {
		tw.Close()
		return err
	}
	// 2. State root → state/ entries, skipping pre-commit staging and, when Raft
	// lives inside the state dir (the default), its consensus state.
	skip := map[string]bool{"staging": true, "mpstaging": true, "raft": true}
	if err := b.tarTree(tw, b.state, snapState, false, skip); err != nil {
		tw.Close()
		return err
	}
	return tw.Close()
}

// tarTree walks root and writes files under prefix. When withXattrs, each file's
// managed xattrs are attached as PAX records. Top-level dirs in skip are excluded.
func (b *posixBackend) tarTree(tw *tar.Writer, root, prefix string, withXattrs bool, skip map[string]bool) error {
	return filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if skip != nil {
			if top := strings.SplitN(rel, string(os.PathSeparator), 2)[0]; skip[top] {
				return filepath.SkipDir
			}
		}
		if fi.IsDir() {
			return nil
		}
		hdr := &tar.Header{Name: prefix + filepath.ToSlash(rel), Mode: 0o644, Size: fi.Size(), Typeflag: tar.TypeReg}
		if withXattrs {
			for name, val := range listManagedXattrs(path) {
				if hdr.PAXRecords == nil {
					hdr.PAXRecords = map[string]string{}
				}
				hdr.PAXRecords[paxXattrPrefix+fullXattrName(name)] = string(val)
			}
		}
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
}

// restoreFrom applies a snapshot without ever destroying prefilled data. It
// reconciles only the REPLICATED object files (those carrying our managed xattr)
// against the snapshot; prefilled plain files are always preserved — they are only
// ever removed by an explicit S3 delete, never by a snapshot install.
func (b *posixBackend) restoreFrom(r io.Reader) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Honor upstream deletes: drop replicated object files; keep prefilled ones.
	b.removeReplicatedObjectFiles()

	// Reset internal replicated state (keep pre-commit staging dirs).
	for _, d := range []string{"versions", "history", "buckets", "mpu", "iam"} {
		if err := os.RemoveAll(b.idir(d)); err != nil {
			return err
		}
	}
	for _, d := range []string{"staging", "mpstaging", "versions", "history", "buckets", "mpu", "iam"} {
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
		var dst string
		switch {
		case strings.HasPrefix(hdr.Name, snapData):
			dst = filepath.Join(b.root, filepath.FromSlash(strings.TrimPrefix(hdr.Name, snapData)))
		case strings.HasPrefix(hdr.Name, snapState):
			dst = filepath.Join(b.state, filepath.FromSlash(strings.TrimPrefix(hdr.Name, snapState)))
		default:
			continue
		}
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
		// Reapply object xattrs from PAX records.
		for k, v := range hdr.PAXRecords {
			if !strings.HasPrefix(k, paxXattrPrefix) {
				continue
			}
			if short, ok := shortXattrName(strings.TrimPrefix(k, paxXattrPrefix)); ok {
				setXattr(dst, short, []byte(v))
			}
		}
	}
	return b.iam.load()
}
