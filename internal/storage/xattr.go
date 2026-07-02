package storage

import (
	"strings"

	"github.com/pkg/xattr"
)

// Object metadata is stored in extended attributes on the object file itself
// (versitygw-style), so the --data volume is nothing but the browsable object
// tree. All names live under this prefix.
const (
	xattrPrefix  = "user.d9ds3."
	xattrManaged = xattrPrefix + "managed" // presence marks a file written via S3 (vs prefilled)
)

// setXattr writes one attribute (name is the short form, without the prefix).
func setXattr(path, name string, val []byte) error {
	return xattr.LSet(path, xattrPrefix+name, val)
}

// getXattr reads one attribute; ok=false if it is absent.
func getXattr(path, name string) ([]byte, bool) {
	v, err := xattr.LGet(path, xattrPrefix+name)
	if err != nil {
		return nil, false
	}
	return v, true
}

// listManagedXattrs returns all d9ds3 attributes on a file, keyed by short name.
func listManagedXattrs(path string) map[string][]byte {
	names, err := xattr.LList(path)
	if err != nil {
		return nil
	}
	out := map[string][]byte{}
	for _, n := range names {
		if strings.HasPrefix(n, xattrPrefix) {
			if v, err := xattr.LGet(path, n); err == nil {
				out[strings.TrimPrefix(n, xattrPrefix)] = v
			}
		}
	}
	return out
}

// removeXattr deletes one attribute (best-effort; missing attr is not an error).
func removeXattr(path, name string) { _ = xattr.LRemove(path, xattrPrefix+name) }

// isManaged reports whether a file was written through S3 (has our marker xattr),
// as opposed to being an operator-prefilled plain file.
func isManaged(path string) bool {
	_, ok := getXattr(path, "managed")
	return ok
}

// hasPrefix/trim helpers for the full xattr name (used by snapshot PAX records).
func fullXattrName(short string) string { return xattrPrefix + short }
func shortXattrName(full string) (string, bool) {
	if strings.HasPrefix(full, xattrPrefix) {
		return strings.TrimPrefix(full, xattrPrefix), true
	}
	return "", false
}
