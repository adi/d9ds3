package storage

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
	"github.com/hashicorp/raft"
)

// objMetaHeader carries the full ObjectMeta (JSON, base64) alongside object bytes
// so the gateway reconstructs all S3 response headers in a single round-trip.
const objMetaHeader = "X-D9-Objectmeta"

func (n *Node) routes() http.Handler {
	mux := http.NewServeMux()
	// control plane
	mux.HandleFunc("GET /v1/status", n.handleStatus)
	mux.HandleFunc("POST /v1/join", n.handleJoin)
	mux.HandleFunc("POST /v1/rebalance", n.handleRebalance)
	mux.HandleFunc("POST /v1/command", n.handleCommand)
	// data plane (writes)
	mux.HandleFunc("PUT /v1/blob/{token}", n.handleBlobPut)
	mux.HandleFunc("PUT /v1/mppart/{uploadid}/{part}", n.handlePartPut)
	// data plane (peer pull)
	mux.HandleFunc("GET /v1/vblob", n.handleVBlobGet)
	mux.HandleFunc("GET /v1/mppart/{uploadid}/{part}", n.handlePartGet)
	// reads
	mux.HandleFunc("GET /v1/buckets", n.handleListBuckets)
	mux.HandleFunc("GET /v1/bucket", n.handleGetBucket)
	mux.HandleFunc("GET /v1/objmeta", n.handleObjMeta)
	mux.HandleFunc("GET /v1/object", n.handleObject)
	mux.HandleFunc("GET /v1/list", n.handleList)
	mux.HandleFunc("GET /v1/versions", n.handleListVersions)
	mux.HandleFunc("GET /v1/mpu", n.handleListMPU)
	mux.HandleFunc("GET /v1/mpu/get", n.handleGetMPU)
	// iam
	mux.HandleFunc("GET /v1/account", n.handleGetAccount)
	mux.HandleFunc("GET /v1/accounts", n.handleListAccounts)
	return mux
}

// ---- control plane ----

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSONResp(w, http.StatusOK, n.status())
}

// handleJoin adds the joining node as a voter in EVERY shard. Each shard's peer
// transport address is derived from the base raft address (port + shard index).
func (n *Node) handleJoin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID       string `json:"id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, sh := range n.shards {
		if _, leaderID := sh.raft.LeaderWithID(); leaderID != raft.ServerID(n.cfg.NodeID) {
			http.Error(w, fmt.Sprintf("not leader of shard %d; retry join via that shard's leader", sh.id), http.StatusMisdirectedRequest)
			return
		}
		peerAddr, err := shardAddr(body.RaftAddr, sh.id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f := sh.raft.AddVoter(raft.ServerID(body.ID), raft.ServerAddress(peerAddr), 0, 10*time.Second)
		if err := f.Error(); err != nil {
			http.Error(w, fmt.Sprintf("shard %d add voter: %v", sh.id, err), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// handleRebalance spreads shard leadership across the cluster.
func (n *Node) handleRebalance(w http.ResponseWriter, r *http.Request) {
	n.rebalance()
	w.WriteHeader(http.StatusOK)
}

func (n *Node) handleCommand(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c, err := command.Decode(raw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	idx, err := n.submitTo(n.shardOf(c.Bucket), raw)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			http.Error(w, "not leader", http.StatusMisdirectedRequest)
			return
		}
		ae := s3err.From(err)
		writeJSONResp(w, ae.HTTPStatus, errEnvelope{Code: ae.Code, Message: ae.Message})
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]uint64{"index": idx})
}

// ---- data plane: writes ----

func (n *Node) handleBlobPut(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	if !safeToken(token) {
		http.Error(w, "bad token", http.StatusBadRequest)
		return
	}
	n.streamTo(w, r, n.backend.stagingPath(token))
}

func (n *Node) handlePartPut(w http.ResponseWriter, r *http.Request) {
	uploadID := r.PathValue("uploadid")
	part, err := strconv.Atoi(r.PathValue("part"))
	if !safeToken(uploadID) || err != nil {
		http.Error(w, "bad part", http.StatusBadRequest)
		return
	}
	n.streamTo(w, r, n.backend.mpPartPath(uploadID, part))
}

func (n *Node) streamTo(w http.ResponseWriter, r *http.Request, path string) {
	if err := os.MkdirAll(pathDir(path), 0o755); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := io.Copy(f, r.Body); err != nil {
		f.Close()
		os.Remove(tmp)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ---- data plane: peer pull ----

func (n *Node) handleVBlobGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f, err := os.Open(n.backend.vblobPath(q.Get("bucket"), q.Get("blobid")))
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	defer f.Close()
	io.Copy(w, f)
}

func (n *Node) handlePartGet(w http.ResponseWriter, r *http.Request) {
	uploadID := r.PathValue("uploadid")
	part, _ := strconv.Atoi(r.PathValue("part"))
	f, err := os.Open(n.backend.mpPartPath(uploadID, part))
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	defer f.Close()
	io.Copy(w, f)
}

// ---- reads ----

func (n *Node) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	bs, err := n.backend.ListBuckets()
	if err != nil {
		writeBackendErr(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, bs)
}

func (n *Node) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	m, err := n.backend.GetBucketMeta(r.URL.Query().Get("bucket"))
	if err != nil {
		writeBackendErr(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, m)
}

func (n *Node) handleObjMeta(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	oi, err := n.backend.HeadObject(q.Get("bucket"), q.Get("key"), q.Get("version"))
	if err != nil {
		writeBackendErr(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, oi)
}

func (n *Node) handleObject(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	opt := types.GetOptions{VersionID: q.Get("version")}
	if br := parseRange(r.Header.Get("Range")); br != nil {
		opt.Range = br
	}
	res, err := n.backend.GetObject(q.Get("bucket"), q.Get("key"), opt)
	if err != nil {
		writeBackendErr(w, err)
		return
	}
	defer res.Body.Close()
	metaJSON, _ := json.Marshal(res.Info)
	w.Header().Set(objMetaHeader, base64.StdEncoding.EncodeToString(metaJSON))
	w.Header().Set("Content-Length", strconv.FormatInt(res.PartialLength, 10))
	if res.IsRange {
		w.Header().Set("Content-Range", res.ContentRange)
		w.WriteHeader(http.StatusPartialContent)
	}
	if r.Method != http.MethodHead {
		io.Copy(w, res.Body)
	}
}

func (n *Node) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	max, _ := strconv.Atoi(q.Get("max"))
	res, err := n.backend.ListObjectsV2(types.ListInput{
		Bucket: q.Get("bucket"), Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"),
		MaxKeys: max, ContinuationToken: q.Get("token"), StartAfter: q.Get("start"),
		Marker: q.Get("marker"),
	})
	if err != nil {
		writeBackendErr(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, res)
}

func (n *Node) handleListVersions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	max, _ := strconv.Atoi(q.Get("max"))
	res, err := n.backend.ListObjectVersions(types.ListVersionsInput{
		Bucket: q.Get("bucket"), Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"),
		MaxKeys: max, KeyMarker: q.Get("key-marker"), VersionIDMarker: q.Get("version-marker"),
	})
	if err != nil {
		writeBackendErr(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, res)
}

func (n *Node) handleListMPU(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	res, err := n.backend.ListMultipartUploads(q.Get("bucket"), q.Get("prefix"))
	if err != nil {
		writeBackendErr(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, res)
}

func (n *Node) handleGetMPU(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	mp, err := n.backend.GetMultipartUpload(q.Get("bucket"), q.Get("uploadid"))
	if err != nil {
		writeBackendErr(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, mp)
}

// ---- iam ----

func (n *Node) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	a, ok := n.backend.iam.lookup(r.URL.Query().Get("ak"))
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSONResp(w, http.StatusOK, a)
}

func (n *Node) handleListAccounts(w http.ResponseWriter, r *http.Request) {
	writeJSONResp(w, http.StatusOK, n.backend.iam.list())
}

// ---- helpers ----

type errEnvelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSONResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeBackendErr(w http.ResponseWriter, err error) {
	ae := s3err.From(err)
	writeJSONResp(w, ae.HTTPStatus, errEnvelope{Code: ae.Code, Message: ae.Message})
}

func safeToken(s string) bool {
	return s != "" && !strings.ContainsAny(s, "/\\.")
}

func pathDir(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return "."
}

// parseRange parses a single "bytes=start-end" range header (S3 supports one range).
func parseRange(h string) *types.ByteRange {
	if !strings.HasPrefix(h, "bytes=") {
		return nil
	}
	spec := strings.TrimPrefix(h, "bytes=")
	if strings.Contains(spec, ",") {
		return nil
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return nil
	}
	startS, endS := spec[:dash], spec[dash+1:]
	br := &types.ByteRange{Start: 0, End: -1}
	if startS == "" { // suffix range: bytes=-N (last N bytes) — handled by caller as full; approximate
		if endS == "" {
			return nil
		}
		nbytes, err := strconv.ParseInt(endS, 10, 64)
		if err != nil {
			return nil
		}
		br.Start = -nbytes // negative Start signals suffix; backend treats <0 as invalid, so map here
		br.End = -1
		return br
	}
	s, err := strconv.ParseInt(startS, 10, 64)
	if err != nil {
		return nil
	}
	br.Start = s
	if endS != "" {
		e, err := strconv.ParseInt(endS, 10, 64)
		if err != nil {
			return nil
		}
		br.End = e
	}
	return br
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

func urlq(s string) string { return url.QueryEscape(s) }
