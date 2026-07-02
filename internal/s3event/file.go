package s3event

import (
	"os"
	"sync"
)

// fileNotifier appends one JSON envelope line per event to a file. Writes are
// performed on a background worker so callers are never blocked, and events are
// dropped when the buffer is full.
type fileNotifier struct {
	path string
	ch   chan Event

	closeOnce sync.Once
	done      chan struct{}
}

func newFileNotifier(path string) *fileNotifier {
	f := &fileNotifier{
		path: path,
		ch:   make(chan Event, bufferSize),
		done: make(chan struct{}),
	}
	go f.run()
	return f
}

// Notify enqueues ev for asynchronous append. It never blocks: if the buffer is
// full the event is dropped.
func (f *fileNotifier) Notify(ev Event) {
	select {
	case f.ch <- ev:
	default:
		// Buffer full: drop the event (best-effort).
	}
}

func (f *fileNotifier) run() {
	defer close(f.done)

	fh, err := os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		// Cannot open target: still drain the channel so Close does not hang
		// and Notify does not eventually block once the buffer fills.
		for range f.ch {
		}
		return
	}
	defer fh.Close()

	for ev := range f.ch {
		body, err := marshalEnvelope(ev)
		if err != nil {
			continue
		}
		body = append(body, '\n')
		_, _ = fh.Write(body)
	}
}

// Close stops accepting new events and waits for buffered events to be written.
// It is safe to call multiple times.
func (f *fileNotifier) Close() {
	f.closeOnce.Do(func() {
		close(f.ch)
	})
	<-f.done
}
