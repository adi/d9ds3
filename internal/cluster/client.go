// Package cluster is the gateway's client to the storage tier: it resolves the
// Raft leader for command submission, fans object/part payloads out to every
// replica, proxies reads to a replica, and looks up IAM accounts.
package cluster

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

// objMetaHeader mirrors storage.objMetaHeader — the object metadata that rides
// alongside object bytes so a read is a single round-trip.
const objMetaHeader = "X-D9-Objectmeta"

// Client talks to a fixed set of storage-node data-plane addresses.
type Client struct {
	nodes  []string
	hc     *http.Client
	mu     sync.Mutex
	shards int // cached cluster shard count (0 = unknown)
}

func NewClient(nodes []string) *Client {
	return &Client{nodes: nodes, hc: &http.Client{Timeout: 60 * time.Second}}
}

// ---- topology ----

func (c *Client) statusOf(node string) (*types.Status, error) {
	resp, err := c.hc.Get("http://" + node + "/v1/status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var s types.Status
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (c *Client) leader() (string, error) {
	for _, node := range c.nodes {
		if s, err := c.statusOf(node); err == nil && s.IsLeader {
			return node, nil
		}
	}
	return "", s3err.ErrNoLeader
}

// reader picks a replica for reads. Any node holds all data (every node is a
// member of every shard), so any reachable node works; prefer a shard-0 leader.
func (c *Client) reader() (string, error) {
	if l, err := c.leader(); err == nil {
		return l, nil
	}
	for _, node := range c.nodes {
		if _, err := c.statusOf(node); err == nil {
			return node, nil
		}
	}
	return "", s3err.ErrNoLeader
}

// numShards returns the cluster's shard count (learned from any node's status,
// cached). Defaults to 1 until a status is seen.
func (c *Client) numShards() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shards > 0 {
		return c.shards
	}
	for _, node := range c.nodes {
		if s, err := c.statusOf(node); err == nil && s.NumShards > 0 {
			c.shards = s.NumShards
			return c.shards
		}
	}
	return 1
}

// shardFor maps a bucket to its owning shard (matches storage.shardOf).
func (c *Client) shardFor(bucket string) int {
	n := c.numShards()
	if bucket == "" || n <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(bucket))
	return int(h.Sum32() % uint32(n))
}

// HasLeader reports whether the root shard currently has a reachable leader
// (i.e. the cluster can serve reads/writes).
func (c *Client) HasLeader() bool {
	_, err := c.leaderForShard(0)
	return err == nil
}

// leaderForShard finds the node currently leading the given shard.
func (c *Client) leaderForShard(shard int) (string, error) {
	for _, node := range c.nodes {
		if s, err := c.statusOf(node); err == nil && s.LeadsShard(shard) {
			return node, nil
		}
	}
	return "", s3err.ErrNoLeader
}

// ---- write path ----

// Submit appends an encoded command via the leader of the bucket's shard,
// re-resolving on leader change.
func (c *Client) Submit(bucket string, raw []byte) (uint64, error) {
	shard := c.shardFor(bucket)
	for attempt := 0; attempt < 6; attempt++ {
		leader, err := c.leaderForShard(shard)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		resp, err := c.hc.Post("http://"+leader+"/v1/command", "application/json", bytesReader(raw))
		if err != nil {
			return 0, err
		}
		if resp.StatusCode == http.StatusMisdirectedRequest {
			resp.Body.Close()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return 0, parseErr(resp)
		}
		var out struct {
			Index uint64 `json:"index"`
		}
		err = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		return out.Index, err
	}
	return 0, s3err.ErrNoLeader
}

// FanOut streams the object payload at filePath to every replica's staging area.
func (c *Client) FanOut(token, filePath string) error {
	return c.fanOut(filePath, func(node string) string {
		return "http://" + node + "/v1/blob/" + token
	})
}

// FanOutPart streams a multipart part payload to every replica's part staging.
func (c *Client) FanOutPart(uploadID string, part int, filePath string) error {
	return c.fanOut(filePath, func(node string) string {
		return fmt.Sprintf("http://%s/v1/mppart/%s/%d", node, uploadID, part)
	})
}

func (c *Client) fanOut(filePath string, urlFor func(node string) string) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for _, node := range c.nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			if err := c.putStream(urlFor(node), filePath); err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}(node)
	}
	wg.Wait()
	if ok < c.quorum() {
		return fmt.Errorf("fan-out reached %d/%d nodes, need quorum %d", ok, len(c.nodes), c.quorum())
	}
	return nil
}

func (c *Client) quorum() int { return len(c.nodes)/2 + 1 }

func (c *Client) putStream(url, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, url, f)
	if err != nil {
		return err
	}
	req.ContentLength = fi.Size()
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stream to %s: %s", url, resp.Status)
	}
	return nil
}

// ---- IAM lookups ----

func (c *Client) GetAccount(accessKeyID string) (*types.Account, error) {
	node, err := c.readerForBucket("")
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Get("http://" + node + "/v1/account?ak=" + url.QueryEscape(accessKeyID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, s3err.ErrInvalidAccessKey
	}
	var a types.Account
	return &a, json.NewDecoder(resp.Body).Decode(&a)
}

func (c *Client) ListAccounts() ([]types.Account, error) {
	node, err := c.readerForBucket("")
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Get("http://" + node + "/v1/accounts")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out []types.Account
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

// ---- reads (proxied) ----

// readerForBucket routes a bucket-scoped read to that bucket's shard leader, which
// (by Raft) has applied every committed write for the shard — giving
// read-after-write consistency. Falls back to any reachable node.
func (c *Client) readerForBucket(bucket string) (string, error) {
	if l, err := c.leaderForShard(c.shardFor(bucket)); err == nil {
		return l, nil
	}
	return c.reader()
}

func (c *Client) getJSON(node, path string, out any) error {
	resp, err := c.hc.Get("http://" + node + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return parseErr(resp)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// getJSONBucket resolves the bucket's shard leader and reads from it.
func (c *Client) getJSONBucket(bucket, path string, out any) error {
	node, err := c.readerForBucket(bucket)
	if err != nil {
		return err
	}
	return c.getJSON(node, path, out)
}

func (c *Client) ListBuckets() ([]types.BucketMeta, error) {
	node, err := c.reader()
	if err != nil {
		return nil, err
	}
	var out []types.BucketMeta
	return out, c.getJSON(node, "/v1/buckets", &out)
}

func (c *Client) GetBucketMeta(bucket string) (*types.BucketMeta, error) {
	var m types.BucketMeta
	if err := c.getJSONBucket(bucket, "/v1/bucket?bucket="+url.QueryEscape(bucket), &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (c *Client) HeadObject(bucket, key, version string) (*types.ObjectMeta, error) {
	var m types.ObjectMeta
	q := "/v1/objmeta?bucket=" + url.QueryEscape(bucket) + "&key=" + url.QueryEscape(key) + "&version=" + url.QueryEscape(version)
	if err := c.getJSONBucket(bucket, q, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (c *Client) ListObjectsV2(in types.ListInput) (*types.ListResult, error) {
	q := fmt.Sprintf("/v1/list?bucket=%s&prefix=%s&delimiter=%s&max=%d&token=%s&start=%s&marker=%s",
		url.QueryEscape(in.Bucket), url.QueryEscape(in.Prefix), url.QueryEscape(in.Delimiter),
		in.MaxKeys, url.QueryEscape(in.ContinuationToken), url.QueryEscape(in.StartAfter), url.QueryEscape(in.Marker))
	var res types.ListResult
	return &res, c.getJSONBucket(in.Bucket, q, &res)
}

func (c *Client) ListObjectVersions(in types.ListVersionsInput) (*types.ListVersionsResult, error) {
	q := fmt.Sprintf("/v1/versions?bucket=%s&prefix=%s&delimiter=%s&max=%d&key-marker=%s&version-marker=%s",
		url.QueryEscape(in.Bucket), url.QueryEscape(in.Prefix), url.QueryEscape(in.Delimiter),
		in.MaxKeys, url.QueryEscape(in.KeyMarker), url.QueryEscape(in.VersionIDMarker))
	var res types.ListVersionsResult
	return &res, c.getJSONBucket(in.Bucket, q, &res)
}

func (c *Client) ListMultipartUploads(bucket, prefix string) ([]types.MultipartUpload, error) {
	var out []types.MultipartUpload
	q := "/v1/mpu?bucket=" + url.QueryEscape(bucket) + "&prefix=" + url.QueryEscape(prefix)
	return out, c.getJSONBucket(bucket, q, &out)
}

func (c *Client) GetMultipartUpload(bucket, uploadID string) (*types.MultipartUpload, error) {
	var mp types.MultipartUpload
	q := "/v1/mpu/get?bucket=" + url.QueryEscape(bucket) + "&uploadid=" + url.QueryEscape(uploadID)
	if err := c.getJSONBucket(bucket, q, &mp); err != nil {
		return nil, err
	}
	return &mp, nil
}

// GetObject proxies a streaming read (with optional version + range). The caller
// must Close the returned Body.
func (c *Client) GetObject(bucket, key string, opt types.GetOptions) (*types.GetResult, error) {
	node, err := c.readerForBucket(bucket)
	if err != nil {
		return nil, err
	}
	u := "http://" + node + "/v1/object?bucket=" + url.QueryEscape(bucket) + "&key=" + url.QueryEscape(key) +
		"&version=" + url.QueryEscape(opt.VersionID)
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	if opt.Range != nil {
		if opt.Range.Start < 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=-%d", -opt.Range.Start))
		} else if opt.Range.End >= 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", opt.Range.Start, opt.Range.End))
		} else {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", opt.Range.Start))
		}
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		defer resp.Body.Close()
		return nil, parseErr(resp)
	}
	res := &types.GetResult{Body: resp.Body}
	if enc := resp.Header.Get(objMetaHeader); enc != "" {
		if raw, derr := base64.StdEncoding.DecodeString(enc); derr == nil {
			json.Unmarshal(raw, &res.Info)
		}
	}
	res.PartialLength, _ = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if resp.StatusCode == http.StatusPartialContent {
		res.IsRange = true
		res.ContentRange = resp.Header.Get("Content-Range")
	}
	return res, nil
}

// ---- helpers ----

func parseErr(resp *http.Response) error {
	var env struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.NewDecoder(resp.Body).Decode(&env) == nil && env.Code != "" {
		return s3err.APIError{Code: env.Code, Message: env.Message, HTTPStatus: resp.StatusCode}
	}
	return s3err.APIError{Code: "InternalError", Message: resp.Status, HTTPStatus: resp.StatusCode}
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
