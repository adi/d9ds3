// Package webui serves a static browser console at /console and a small JSON API
// (/console/api) that authenticates with HTTP Basic auth (access-key:secret) and
// performs operations through the gateway on the caller's behalf.
package webui

import (
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"

	"github.com/adi/d9ds3/internal/gateway"
	"github.com/adi/d9ds3/internal/types"
)

//go:embed dist
var distFS embed.FS

func fsSub() (fs.FS, error) { return fs.Sub(distFS, "dist") }

// Handler builds the web-console handler mounted at /console/.
func Handler(gw *gateway.Gateway) http.Handler {
	sub, _ := fsSub()
	files := http.StripPrefix("/console/", http.FileServer(http.FS(sub)))
	ui := &uiServer{gw: gw, files: files}
	return ui
}

type uiServer struct {
	gw    *gateway.Gateway
	files http.Handler
}

func (u *uiServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/console/api/") {
		u.api(w, r)
		return
	}
	// Bare /console or /console/ → serve index.html directly (FileServer would
	// redirect /index.html back to ./ and loop).
	if r.URL.Path == "/console" || r.URL.Path == "/console/" {
		sub, _ := fsSub()
		b, err := fs.ReadFile(sub, "index.html")
		if err != nil {
			http.Error(w, "ui not found", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(b)
		return
	}
	u.files.ServeHTTP(w, r)
}

// ---- auth ----

type identity struct {
	account string
	role    string
}

func (u *uiServer) auth(r *http.Request) (*identity, bool) {
	ak, sk, ok := r.BasicAuth()
	if !ok {
		return nil, false
	}
	a, err := u.gw.GetAccount(ak)
	if err != nil || a == nil || a.SecretKey != sk {
		return nil, false
	}
	return &identity{account: a.AccessKeyID, role: a.Role}, true
}

// ---- JSON API ----

func (u *uiServer) api(w http.ResponseWriter, r *http.Request) {
	id, ok := u.auth(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", `Basic realm="d9ds3"`)
		http.Error(w, `{"error":"invalid credentials"}`, http.StatusUnauthorized)
		return
	}
	ctx := gateway.Ctx{Account: id.account, SourceIP: r.RemoteAddr}
	q := r.URL.Query()
	path := strings.TrimPrefix(r.URL.Path, "/console/api/")

	switch {
	case path == "whoami":
		writeJSON(w, map[string]string{"access_key_id": id.account, "role": id.role})

	case path == "buckets" && r.Method == http.MethodGet:
		bs, err := u.gw.ListBuckets()
		if err != nil {
			apiErr(w, err)
			return
		}
		out := []map[string]any{}
		for _, b := range bs {
			if id.role == "admin" || b.Owner == id.account {
				out = append(out, map[string]any{"name": b.Name, "created_at": b.CreatedAt})
			}
		}
		writeJSON(w, out)

	case path == "buckets" && r.Method == http.MethodPost:
		var body struct {
			Name string `json:"name"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if _, err := u.gw.CreateBucket(ctx, gateway.CreateBucketInput{Bucket: body.Name}); err != nil {
			apiErr(w, err)
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})

	case path == "buckets" && r.Method == http.MethodDelete:
		if !u.owns(id, q.Get("bucket")) {
			forbidden(w)
			return
		}
		if _, err := u.gw.DeleteBucket(ctx, q.Get("bucket")); err != nil {
			apiErr(w, err)
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})

	case path == "objects" && r.Method == http.MethodGet:
		if !u.owns(id, q.Get("bucket")) {
			forbidden(w)
			return
		}
		res, err := u.gw.ListObjectsV2(types.ListInput{
			Bucket: q.Get("bucket"), Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"), MaxKeys: 1000,
		})
		if err != nil {
			apiErr(w, err)
			return
		}
		objs := []map[string]any{}
		for _, o := range res.Objects {
			objs = append(objs, map[string]any{"key": o.Key, "size": o.Size, "last_modified": o.LastModified, "etag": o.ETag})
		}
		writeJSON(w, map[string]any{"objects": objs, "prefixes": res.CommonPrefixes})

	case path == "object" && r.Method == http.MethodGet:
		if !u.owns(id, q.Get("bucket")) {
			forbidden(w)
			return
		}
		res, err := u.gw.GetObject(q.Get("bucket"), q.Get("key"), types.GetOptions{})
		if err != nil {
			apiErr(w, err)
			return
		}
		defer res.Body.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		io.Copy(w, res.Body)

	case path == "object" && r.Method == http.MethodPut:
		if !u.owns(id, q.Get("bucket")) {
			forbidden(w)
			return
		}
		if _, err := u.gw.PutObject(ctx, gateway.PutObjectInput{
			Bucket: q.Get("bucket"), Key: q.Get("key"), ContentType: r.Header.Get("Content-Type"),
		}, r.Body); err != nil {
			apiErr(w, err)
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})

	case path == "object" && r.Method == http.MethodDelete:
		if !u.owns(id, q.Get("bucket")) {
			forbidden(w)
			return
		}
		if _, err := u.gw.DeleteObject(ctx, q.Get("bucket"), q.Get("key"), ""); err != nil {
			apiErr(w, err)
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})

	case path == "accounts":
		if id.role != "admin" {
			forbidden(w)
			return
		}
		u.accounts(w, r, ctx, q)

	default:
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
	}
}

func (u *uiServer) accounts(w http.ResponseWriter, r *http.Request, ctx gateway.Ctx, q url.Values) {
	switch r.Method {
	case http.MethodGet:
		accts, err := u.gw.ListAccounts()
		if err != nil {
			apiErr(w, err)
			return
		}
		out := []map[string]string{}
		for _, a := range accts {
			out = append(out, map[string]string{"access_key_id": a.AccessKeyID, "role": a.Role})
		}
		writeJSON(w, out)
	case http.MethodPost:
		var a types.Account
		json.NewDecoder(r.Body).Decode(&a)
		if a.AccessKeyID == "" || a.SecretKey == "" {
			http.Error(w, `{"error":"missing fields"}`, http.StatusBadRequest)
			return
		}
		if a.Role == "" {
			a.Role = "user"
		}
		if _, err := u.gw.PutAccount(ctx, a); err != nil {
			apiErr(w, err)
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
	case http.MethodDelete:
		if _, err := u.gw.DeleteAccount(ctx, q.Get("access_key")); err != nil {
			apiErr(w, err)
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// owns reports whether the identity may act on the bucket (admin or owner).
func (u *uiServer) owns(id *identity, bucket string) bool {
	if id.role == "admin" {
		return true
	}
	bm, err := u.gw.GetBucketMeta(bucket)
	if err != nil {
		return true // let the op surface NoSuchBucket
	}
	return bm.Owner == id.account
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
func apiErr(w http.ResponseWriter, err error) {
	http.Error(w, `{"error":`+jsonString(err.Error())+`}`, http.StatusBadGateway)
}
func forbidden(w http.ResponseWriter) {
	http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
}
func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
