// Package s3event delivers S3-style object event notifications to external
// targets (webhooks, files) in a best-effort, non-blocking fashion.
//
// Notifiers are safe for concurrent use. Delivery happens asynchronously on a
// background worker so that callers on a request path are never blocked or
// failed by notification I/O. If the internal buffer is full, events are
// dropped rather than blocking the caller.
package s3event

import (
	"encoding/json"
	"strings"
	"time"
)

// Event is a single S3 notification.
type Event struct {
	EventName string // e.g. "s3:ObjectCreated:Put", "s3:ObjectCreated:CompleteMultipartUpload",
	// "s3:ObjectCreated:Copy", "s3:ObjectRemoved:Delete", "s3:ObjectRemoved:DeleteMarkerCreated"
	Bucket    string
	Key       string
	VersionID string
	ETag      string
	Size      int64
	Requester string // account id
	SourceIP  string
	Time      time.Time
}

// Notifier delivers events. Implementations must be safe for concurrent use and
// non-blocking / best-effort (never block or fail the caller's request path).
type Notifier interface {
	Notify(ev Event)
	Close()
}

// New returns a Notifier for the target:
//
//	""                      -> a no-op notifier
//	"http://..."/"https://" -> a webhook notifier POSTing JSON asynchronously
//	"file:///path"          -> appends one JSON envelope line per event
//
// Any unrecognized target yields a no-op notifier.
func New(target string) Notifier {
	switch {
	case target == "":
		return NopNotifier{}
	case strings.HasPrefix(target, "http://"), strings.HasPrefix(target, "https://"):
		return newWebhookNotifier(target)
	case strings.HasPrefix(target, "file://"):
		return newFileNotifier(strings.TrimPrefix(target, "file://"))
	default:
		return NopNotifier{}
	}
}

// NopNotifier is a Notifier that discards all events. It is returned for an
// empty or unrecognized target.
type NopNotifier struct{}

// Notify discards the event.
func (NopNotifier) Notify(Event) {}

// Close is a no-op.
func (NopNotifier) Close() {}

// --- AWS S3-style event envelope ---

type envelope struct {
	Records []record `json:"Records"`
}

type record struct {
	EventVersion      string            `json:"eventVersion"`
	EventSource       string            `json:"eventSource"`
	EventName         string            `json:"eventName"`
	EventTime         string            `json:"eventTime"`
	RequestParameters requestParameters `json:"requestParameters"`
	UserIdentity      userIdentity      `json:"userIdentity"`
	S3                s3Entity          `json:"s3"`
}

type requestParameters struct {
	SourceIPAddress string `json:"sourceIPAddress"`
}

type userIdentity struct {
	PrincipalID string `json:"principalId"`
}

type s3Entity struct {
	Bucket s3Bucket `json:"bucket"`
	Object s3Object `json:"object"`
}

type s3Bucket struct {
	Name string `json:"name"`
}

type s3Object struct {
	Key       string `json:"key"`
	Size      int64  `json:"size"`
	ETag      string `json:"eTag"`
	VersionID string `json:"versionId"`
}

// marshalEnvelope renders an Event as an AWS-S3-style event envelope. The
// leading "s3:" prefix is stripped from the event name to match AWS.
func marshalEnvelope(ev Event) ([]byte, error) {
	t := ev.Time
	if t.IsZero() {
		t = time.Now()
	}
	env := envelope{
		Records: []record{{
			EventVersion:      "2.1",
			EventSource:       "aws:s3",
			EventName:         strings.TrimPrefix(ev.EventName, "s3:"),
			EventTime:         t.UTC().Format(time.RFC3339),
			RequestParameters: requestParameters{SourceIPAddress: ev.SourceIP},
			UserIdentity:      userIdentity{PrincipalID: ev.Requester},
			S3: s3Entity{
				Bucket: s3Bucket{Name: ev.Bucket},
				Object: s3Object{
					Key:       ev.Key,
					Size:      ev.Size,
					ETag:      ev.ETag,
					VersionID: ev.VersionID,
				},
			},
		}},
	}
	return json.Marshal(env)
}
