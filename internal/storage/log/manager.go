// Package log implements the on-disk, append-only partition logs — the storage
// engine described in docs/storage-and-durability.md. Record batches are stored verbatim
// (never decoded), offsets are assigned from the batch header, and steady-state
// memory is independent of on-disk size (the log is served by pread, never held
// in the heap).
package log

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// retentionCheckInterval is how often the maintenance loop sweeps for segments to
// delete. Retention is a soft bound, so a coarse sweep is fine; a second keeps
// local-dev feedback snappy without meaningful cost (sizes and ages are tracked in
// memory, so a sweep touches no disk unless it deletes).
const retentionCheckInterval = time.Second

// ManagerOptions tunes newly opened partitions and the background maintenance loop.
type ManagerOptions struct {
	SegmentBytes       int64
	IndexIntervalBytes int64

	// FsyncInterval is the coarse cadence at which active segments are flushed to
	// disk. Zero disables the timer (the OS still flushes eventually; a graceful
	// shutdown always syncs).
	FsyncInterval time.Duration

	// Retention bounds on-disk size; a zero policy keeps everything.
	Retention RetentionPolicy

	// Logger receives background-loop warnings (fsync/retention errors). Nil
	// discards them.
	Logger *slog.Logger
}

// Manager owns every open partition log under a single data directory and runs the
// background fsync + retention maintenance loop.
type Manager struct {
	dir  string
	opts ManagerOptions
	log  *slog.Logger

	mu         sync.Mutex
	partitions map[topicPartition]*Partition
	stopped    bool
	syncOps    atomic.Int64 // count of SyncPartition calls (durable-ack fsyncs).

	done chan struct{}
	wg   sync.WaitGroup
}

type topicPartition struct {
	topic     string
	partition int32
}

// ErrUnsafeTopicName is returned for a topic name that would resolve outside the
// data directory. The registry refuses to create such names, so this is the
// second lock on the same door: a name that predates that check, or reaches the
// log some other way, still cannot make the broker read, write or delete a
// directory that isn't its own.
var ErrUnsafeTopicName = errors.New("log: topic name is not safe to use as a directory")

// OpenManager opens (creating if needed) the data directory. Existing partitions
// are opened and recovered lazily, on first access. If a fsync cadence or a
// retention policy is configured, a background maintenance goroutine is started
// and runs until Close.
func OpenManager(dir string, opts ManagerOptions) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	m := &Manager{
		dir:        dir,
		opts:       opts,
		log:        logger,
		partitions: make(map[topicPartition]*Partition),
		done:       make(chan struct{}),
	}
	if opts.FsyncInterval > 0 || opts.Retention.enabled() {
		m.wg.Add(1)
		go m.maintain()
	}
	return m, nil
}

// Partition returns the log for topic/partition, opening and recovering it (and
// creating its directory) on first use.
func (m *Manager) Partition(topic string, partition int32) (*Partition, error) {
	tp := topicPartition{topic, partition}

	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.partitions[tp]; ok {
		return p, nil
	}
	dir, err := m.dirFor(tp)
	if err != nil {
		return nil, err
	}
	p, err := openPartition(dir, m.opts)
	if err != nil {
		return nil, err
	}
	m.partitions[tp] = p
	return p, nil
}

// dirFor returns the on-disk directory for a partition, e.g. data/orders-0. The
// topic name is one path component by construction; anything that is not (a
// separator, a NUL, or a name that is itself "." / "..") is refused rather than
// cleaned, because cleaning is how "../x" turns into a sibling of the data dir.
func (m *Manager) dirFor(tp topicPartition) (string, error) {
	t := tp.topic
	if t == "" || t == "." || t == ".." || strings.ContainsAny(t, "/\\\x00") {
		return "", fmt.Errorf("%w: %q", ErrUnsafeTopicName, t)
	}
	base := t + "-" + strconv.Itoa(int(tp.partition))
	if !filepath.IsLocal(base) {
		return "", fmt.Errorf("%w: %q", ErrUnsafeTopicName, t)
	}
	return filepath.Join(m.dir, base), nil
}

// RemoveTopic closes and deletes every partition of a topic from disk (used by
// DeleteTopics). It removes partitions [0, partitions) — the count the caller read
// from the registry before deleting the topic there. Best-effort: it returns the
// first error but continues removing the rest.
func (m *Manager) RemoveTopic(topic string, partitions int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for p := int32(0); p < partitions; p++ {
		tp := topicPartition{topic, p}
		dir, err := m.dirFor(tp)
		if err != nil {
			// Never RemoveAll a path we could not prove is ours.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if part, ok := m.partitions[tp]; ok {
			if err := part.close(); err != nil && firstErr == nil {
				firstErr = err
			}
			delete(m.partitions, tp)
		}
		if err := os.RemoveAll(dir); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// snapshot returns the currently open partitions. The maintenance loop and Sync
// use it so they never hold m.mu across a (potentially slow) fsync.
func (m *Manager) snapshot() []*Partition {
	m.mu.Lock()
	defer m.mu.Unlock()
	ps := make([]*Partition, 0, len(m.partitions))
	for _, p := range m.partitions {
		ps = append(ps, p)
	}
	return ps
}

// SyncPartition flushes a single open partition's active segment to disk. It backs
// durable acks (acks=all): a produce fsyncs the partitions it wrote before the
// response, so an acknowledged record is on stable storage, not just in the page
// cache. A no-op if the partition isn't open. The syncOps counter records how many
// such syncs have run (for tests).
func (m *Manager) SyncPartition(topic string, partition int32) error {
	m.mu.Lock()
	p, ok := m.partitions[topicPartition{topic, partition}]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	m.syncOps.Add(1)
	return p.Sync()
}

// SyncOps returns how many SyncPartition calls have run — used by tests to confirm
// durable acks actually fsync.
func (m *Manager) SyncOps() int64 { return m.syncOps.Load() }

// Sync flushes every open partition's active segment to stable storage, returning
// the first error. It backs both the periodic timer and the flush on shutdown.
func (m *Manager) Sync() error {
	var firstErr error
	for _, p := range m.snapshot() {
		if err := p.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// EnforceRetention applies the retention policy to every open partition. It is
// called by the maintenance loop and is exported so tests can trigger a sweep
// deterministically.
func (m *Manager) EnforceRetention() error {
	var firstErr error
	for _, p := range m.snapshot() {
		if err := m.opts.Retention.Enforce(p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// maintain runs the coarse fsync and retention timers until Close. A disabled
// timer leaves its channel nil, which blocks forever in the select.
func (m *Manager) maintain() {
	defer m.wg.Done()

	var fsyncC, retentionC <-chan time.Time
	if m.opts.FsyncInterval > 0 {
		t := time.NewTicker(m.opts.FsyncInterval)
		defer t.Stop()
		fsyncC = t.C
	}
	if m.opts.Retention.enabled() {
		t := time.NewTicker(retentionCheckInterval)
		defer t.Stop()
		retentionC = t.C
	}

	for {
		select {
		case <-m.done:
			return
		case <-fsyncC:
			if err := m.Sync(); err != nil {
				m.log.Warn("periodic fsync failed", "err", err)
			}
		case <-retentionC:
			if err := m.EnforceRetention(); err != nil {
				m.log.Warn("retention enforcement failed", "err", err)
			}
		}
	}
}

// Close stops the maintenance loop, then flushes and closes every open partition
// (a graceful, durable shutdown), returning the first error encountered.
func (m *Manager) Close() error {
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.done)
	}
	m.mu.Unlock()
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for _, p := range m.partitions {
		if err := p.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
