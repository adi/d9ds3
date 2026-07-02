package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func uiReq(t *testing.T, method, url, ak, sk string, body io.Reader) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(method, url, body)
	if ak != "" {
		req.SetBasicAuth(ak, sk)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

// TestWebUI checks the static console is served and its Basic-auth JSON API works.
func TestWebUI(t *testing.T) {
	h := startHarness(t, 1)
	base := h.url + "/console"

	// Static asset.
	resp := uiReq(t, "GET", base+"/", "", "", nil)
	page, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(strings.ToLower(string(page)), "<!doctype html") {
		t.Fatalf("index not served: status=%d", resp.StatusCode)
	}

	// Bad credentials rejected.
	resp = uiReq(t, "GET", base+"/api/whoami", "admin", "wrong", nil)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("bad creds should 401, got %d", resp.StatusCode)
	}

	// whoami.
	resp = uiReq(t, "GET", base+"/api/whoami", rootAK, rootSK, nil)
	who, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(who), `"role":"admin"`) {
		t.Fatalf("whoami: status=%d body=%s", resp.StatusCode, who)
	}

	// Create bucket via UI API.
	resp = uiReq(t, "POST", base+"/api/buckets", rootAK, rootSK, strings.NewReader(`{"name":"uib"}`))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}

	// Upload, list, download.
	resp = uiReq(t, "PUT", base+"/api/object?bucket=uib&key=hi.txt", rootAK, rootSK, strings.NewReader("hello ui"))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("upload: %d", resp.StatusCode)
	}
	resp = uiReq(t, "GET", base+"/api/objects?bucket=uib", rootAK, rootSK, nil)
	list, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(list), "hi.txt") {
		t.Fatalf("list objects missing hi.txt: %s", list)
	}
	resp = uiReq(t, "GET", base+"/api/object?bucket=uib&key=hi.txt", rootAK, rootSK, nil)
	dl, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(dl) != "hello ui" {
		t.Fatalf("download body: %q", dl)
	}

	// Admin: list accounts includes root.
	resp = uiReq(t, "GET", base+"/api/accounts", rootAK, rootSK, nil)
	accts, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(accts), rootAK) {
		t.Fatalf("accounts missing root: %s", accts)
	}
}
