package storage

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// md5File computes the quoted MD5 (S3 ETag) of a file's contents. Used to derive
// an ETag for a prefilled object that has no metadata sidecar.
func md5File(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return `"` + hex.EncodeToString(h.Sum(nil)) + `"`
}

// guessContentType infers a content type from a key's extension.
func guessContentType(key string) string {
	if ct := mime.TypeByExtension(filepath.Ext(key)); ct != "" {
		if i := strings.IndexByte(ct, ';'); i >= 0 {
			return strings.TrimSpace(ct[:i])
		}
		return ct
	}
	return "application/octet-stream"
}

// moveFile moves src to dst, falling back to copy+remove when a plain rename
// fails (e.g. cross-device, because --data and the state dir may be on separate
// volumes).
func moveFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	if err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

// copyFile copies src to dst atomically via a temp file + rename.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".d9tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

// limitedReadCloser returns a ReadCloser reading at most n bytes from f, closing f.
func limitedReadCloser(f *os.File, n int64) io.ReadCloser {
	return &limitedFile{f: f, r: io.LimitReader(f, n)}
}

type limitedFile struct {
	f *os.File
	r io.Reader
}

func (l *limitedFile) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitedFile) Close() error               { return l.f.Close() }
