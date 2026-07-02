package storage

import (
	"io"
	"os"
	"strconv"
)

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

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
