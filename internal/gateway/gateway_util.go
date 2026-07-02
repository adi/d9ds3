package gateway

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/s3err"
)

// checkContentMD5 compares a client-supplied base64 Content-MD5 against the
// computed digest.
func checkContentMD5(b64 string, computed []byte) error {
	want, err := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	if err != nil {
		return s3err.ErrInvalidArgument
	}
	if len(want) != len(computed) {
		return s3err.ErrBadDigest
	}
	for i := range want {
		if want[i] != computed[i] {
			return s3err.ErrBadDigest
		}
	}
	return nil
}

// multipartETag computes the S3 multipart ETag from the ordered part ETags:
// md5(concat of each part's raw md5) hex + "-" + partCount.
func multipartETag(parts []command.CompletedPart) string {
	h := md5.New()
	for _, p := range parts {
		if raw, err := hex.DecodeString(strings.Trim(p.ETag, `"`)); err == nil {
			h.Write(raw)
		}
	}
	return `"` + hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts)) + `"`
}
