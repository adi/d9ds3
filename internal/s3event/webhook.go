package s3event

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"time"
)

const (
	// bufferSize bounds the number of pending events. When full, new events
	// are dropped (best-effort delivery).
	bufferSize = 1024
	// httpTimeout caps how long a single delivery attempt may take.
	httpTimeout = 5 * time.Second
)

// webhookNotifier POSTs event envelopes as JSON to an HTTP endpoint. Delivery
// is asynchronous: Notify enqueues onto a bounded channel drained by a worker
// goroutine, and drops events when the buffer is full.
type webhookNotifier struct {
	url    string
	client *http.Client
	ch     chan Event

	closeOnce sync.Once
	done      chan struct{}
}

func newWebhookNotifier(url string) *webhookNotifier {
	w := &webhookNotifier{
		url:    url,
		client: &http.Client{Timeout: httpTimeout},
		ch:     make(chan Event, bufferSize),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

// Notify enqueues ev for asynchronous delivery. It never blocks: if the buffer
// is full the event is dropped.
func (w *webhookNotifier) Notify(ev Event) {
	select {
	case w.ch <- ev:
	default:
		// Buffer full: drop the event (best-effort).
	}
}

func (w *webhookNotifier) run() {
	defer close(w.done)
	for ev := range w.ch {
		w.deliver(ev)
	}
}

func (w *webhookNotifier) deliver(ev Event) {
	body, err := marshalEnvelope(ev)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return
	}
	// Drain and close so the connection can be reused.
	resp.Body.Close()
}

// Close stops accepting new events and waits for the worker to drain any
// already-buffered events. It is safe to call multiple times.
func (w *webhookNotifier) Close() {
	w.closeOnce.Do(func() {
		close(w.ch)
	})
	<-w.done
}
