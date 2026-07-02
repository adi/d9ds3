package s3event

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWebhookDeliversWellFormedEnvelope(t *testing.T) {
	type received struct {
		body        []byte
		contentType string
	}
	got := make(chan received, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		select {
		case got <- received{body: b, contentType: r.Header.Get("Content-Type")}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := New(srv.URL)
	if _, ok := n.(*webhookNotifier); !ok {
		t.Fatalf("New(%q) = %T, want *webhookNotifier", srv.URL, n)
	}
	defer n.Close()

	ev := Event{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "my-bucket",
		Key:       "path/to/object.txt",
		VersionID: "v1",
		ETag:      "abc123",
		Size:      42,
		Requester: "acct-9",
		SourceIP:  "10.0.0.1",
		Time:      time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC),
	}
	n.Notify(ev)

	var rc received
	select {
	case rc = <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for webhook delivery")
	}

	if rc.contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", rc.contentType)
	}

	var env envelope
	if err := json.Unmarshal(rc.body, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v (body=%s)", err, rc.body)
	}
	if len(env.Records) != 1 {
		t.Fatalf("Records len = %d, want 1", len(env.Records))
	}
	rec := env.Records[0]
	if rec.EventVersion != "2.1" {
		t.Errorf("eventVersion = %q, want 2.1", rec.EventVersion)
	}
	if rec.EventSource != "aws:s3" {
		t.Errorf("eventSource = %q, want aws:s3", rec.EventSource)
	}
	if rec.EventName != "ObjectCreated:Put" {
		t.Errorf("eventName = %q, want ObjectCreated:Put (s3: prefix stripped)", rec.EventName)
	}
	if rec.EventTime != "2026-07-02T12:00:00Z" {
		t.Errorf("eventTime = %q, want 2026-07-02T12:00:00Z", rec.EventTime)
	}
	if rec.RequestParameters.SourceIPAddress != "10.0.0.1" {
		t.Errorf("sourceIPAddress = %q, want 10.0.0.1", rec.RequestParameters.SourceIPAddress)
	}
	if rec.UserIdentity.PrincipalID != "acct-9" {
		t.Errorf("principalId = %q, want acct-9", rec.UserIdentity.PrincipalID)
	}
	if rec.S3.Bucket.Name != "my-bucket" {
		t.Errorf("bucket.name = %q, want my-bucket", rec.S3.Bucket.Name)
	}
	if rec.S3.Object.Key != "path/to/object.txt" {
		t.Errorf("object.key = %q, want path/to/object.txt", rec.S3.Object.Key)
	}
	if rec.S3.Object.Size != 42 {
		t.Errorf("object.size = %d, want 42", rec.S3.Object.Size)
	}
	if rec.S3.Object.ETag != "abc123" {
		t.Errorf("object.eTag = %q, want abc123", rec.S3.Object.ETag)
	}
	if rec.S3.Object.VersionID != "v1" {
		t.Errorf("object.versionId = %q, want v1", rec.S3.Object.VersionID)
	}
}

func TestNopNotifier(t *testing.T) {
	n := New("")
	if n == nil {
		t.Fatal("New(\"\") returned nil")
	}
	if _, ok := n.(NopNotifier); !ok {
		t.Errorf("New(\"\") = %T, want NopNotifier", n)
	}
	// Must not panic.
	n.Notify(Event{EventName: "s3:ObjectRemoved:Delete", Bucket: "b", Key: "k"})
	n.Close()
}

func TestNotifyNeverBlocksWhenBufferFull(t *testing.T) {
	// Point at an unreachable server via a slow handler to keep the worker busy,
	// then flood the buffer. Notify must return promptly without blocking.
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer srv.Close()

	n := New(srv.URL)
	// Deferred LIFO: close(block) runs first so the handler unblocks, letting
	// n.Close() drain the buffered events promptly instead of stalling on the
	// slow handler for the full HTTP timeout on every event.
	defer n.Close()
	defer close(block)

	done := make(chan struct{})
	go func() {
		for i := 0; i < bufferSize*4; i++ {
			n.Notify(Event{EventName: "s3:ObjectCreated:Put", Bucket: "b", Key: "k"})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Notify blocked when buffer was full")
	}
}

func TestFileNotifierAppendsJSONLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")

	n := New("file://" + path)
	if _, ok := n.(*fileNotifier); !ok {
		t.Fatalf("New(file://...) = %T, want *fileNotifier", n)
	}
	n.Notify(Event{EventName: "s3:ObjectCreated:Put", Bucket: "b", Key: "k1"})
	n.Notify(Event{EventName: "s3:ObjectRemoved:Delete", Bucket: "b", Key: "k2"})
	n.Close() // drains

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	lines := 0
	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var env envelope
		if err := dec.Decode(&env); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode line: %v", err)
		}
		lines++
	}
	if lines != 2 {
		t.Errorf("wrote %d envelopes, want 2", lines)
	}
}
