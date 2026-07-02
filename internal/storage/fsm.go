package storage

import (
	"io"
	"sync/atomic"
	"time"

	"github.com/adi/d9ds3/internal/command"
	"github.com/hashicorp/raft"
)

// Blob recovery: a committed entry MUST be applied, so when this node missed the
// fan-out (e.g. it was down) we retry pulling the payload from a peer rather than
// silently skipping the entry.
const (
	recoveryAttempts = 120
	recoveryBackoff  = 250 * time.Millisecond
)

// fsm adapts the posix backend to raft.FSM. Every committed log entry flows
// through Apply, which decodes the Command and executes it locally. Because the
// log is totally ordered and Apply is deterministic, every replica converges.
type fsm struct {
	backend *posixBackend
	applied atomic.Uint64 // last Raft index applied to local storage

	// fetchBlob stages the payload for a command by pulling the committed object
	// bytes from a peer, used when this node missed the gateway's fan-out. Set by
	// the node after construction.
	fetchBlob func(c *command.Command) error
}

func newFSM(b *posixBackend) *fsm { return &fsm{backend: b} }

// Apply executes one committed command. Its return value is delivered to the
// caller of raft.Apply on the leader.
func (f *fsm) Apply(l *raft.Log) any {
	defer f.applied.Store(l.Index)

	if l.Type != raft.LogCommand {
		return nil
	}
	c, err := command.Decode(l.Data)
	if err != nil {
		return err
	}
	return f.applyWithRecovery(c)
}

// applyWithRecovery applies a command, recovering a missing payload from a peer if
// this node never received the fan-out. It retries with backoff because the entry
// is already committed and must be applied — the leader (part of the commit
// quorum) always holds the blob, so recovery converges.
func (f *fsm) applyWithRecovery(c *command.Command) error {
	err := f.backend.Apply(c)
	if err != ErrBlobMissing || c.BlobToken == "" || f.fetchBlob == nil {
		return err
	}
	for attempt := 0; attempt < recoveryAttempts; attempt++ {
		if perr := f.fetchBlob(c); perr == nil {
			if err = f.backend.Apply(c); err != ErrBlobMissing {
				return err
			}
		}
		time.Sleep(recoveryBackoff)
	}
	return err // gave up after exhausting retries (peers holding the blob unreachable)
}

// AppliedIndex is the last log index this node has applied to local storage.
func (f *fsm) AppliedIndex() uint64 { return f.applied.Load() }

// Snapshot captures the full local dataset so the log can be compacted and a
// fresh node can be bootstrapped from the snapshot (Raft InstallSnapshot) rather
// than replaying the entire log.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{backend: f.backend}, nil
}

// Restore replaces local state with the contents of a snapshot.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.backend.restoreFrom(rc)
}

type fsmSnapshot struct{ backend *posixBackend }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.backend.snapshotTo(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
