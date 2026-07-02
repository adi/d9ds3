package s3api

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"time"

	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

const iso8601 = "2006-01-02T15:04:05.000Z"
const rfc1123 = "Mon, 02 Jan 2006 15:04:05 GMT"

// writeErr renders an error as an S3 XML error document.
func writeErr(w http.ResponseWriter, r *http.Request, err error) {
	ae := s3err.From(err)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(ae.HTTPStatus)
	if r.Method != http.MethodHead {
		w.Write(ae.XML(r.URL.Path, w.Header().Get("x-amz-request-id")))
	}
}

// writeXML marshals v as an S3 XML response body.
func writeXML(w http.ResponseWriter, status int, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	w.Write(body)
}

func writeNoContent(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }

// setObjectResponseHeaders sets the standard object headers for GET/HEAD.
func setObjectResponseHeaders(w http.ResponseWriter, oi *types.ObjectMeta) {
	h := w.Header()
	if oi.ContentType != "" {
		h.Set("Content-Type", oi.ContentType)
	}
	if oi.ContentEncoding != "" {
		h.Set("Content-Encoding", oi.ContentEncoding)
	}
	if oi.ContentLanguage != "" {
		h.Set("Content-Language", oi.ContentLanguage)
	}
	if oi.ContentDisposition != "" {
		h.Set("Content-Disposition", oi.ContentDisposition)
	}
	if oi.CacheControl != "" {
		h.Set("Cache-Control", oi.CacheControl)
	}
	if oi.Expires != "" {
		h.Set("Expires", oi.Expires)
	}
	if oi.ETag != "" {
		h.Set("ETag", oi.ETag)
	}
	if !oi.LastModified.IsZero() {
		h.Set("Last-Modified", oi.LastModified.UTC().Format(rfc1123))
	}
	if oi.VersionID != "" && oi.VersionID != "null" {
		h.Set("x-amz-version-id", oi.VersionID)
	}
	if oi.StorageClass != "" && oi.StorageClass != "STANDARD" {
		h.Set("x-amz-storage-class", oi.StorageClass)
	}
	if oi.Retention != nil {
		h.Set("x-amz-object-lock-mode", oi.Retention.Mode)
		h.Set("x-amz-object-lock-retain-until-date", oi.Retention.RetainUntil.UTC().Format(time.RFC3339))
	}
	if oi.LegalHold != nil {
		h.Set("x-amz-object-lock-legal-hold", oi.LegalHold.Status)
	}
	if oi.DeleteMarker {
		h.Set("x-amz-delete-marker", "true")
	}
	h.Set("Accept-Ranges", "bytes")
	if len(oi.Tags) > 0 {
		h.Set("x-amz-tagging-count", strconv.Itoa(len(oi.Tags)))
	}
	for k, v := range oi.UserMeta {
		h.Set("x-amz-meta-"+k, v)
	}
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		t = time.Unix(0, 0).UTC()
	}
	return t.UTC().Format(iso8601)
}
