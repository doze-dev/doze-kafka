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

// closedRetentionInterval is how often partitions that are not open are swept.
const closedRetentionInterval = time.Minute

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

	// BeforeSync, if set, runs before every fsync, and a non-nil error is returned
	// in its place. It is fault injection for tests ("the disk failed"); leave it
	// nil in production.
	BeforeSync func() error

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

// ErrNoSuchPartition is returned by ExistingPartition when the partition has no
// directory: its topic was never created here, or has been deleted.
var ErrNoSuchPartition = errors.New("log: no such partition")

// ErrManagerClosed is returned once Close has run: nothing may be opened after
// shutdown, since nothing would ever flush or close it.
var ErrManagerClosed = errors.New("log: manager is closed")

// Partition returns the log for topic/partition, opening and recovering it (and
// creating its directory) on first use.
func (m *Manager) Partition(topic string, partition int32) (*Partition, error) {
	return m.open(topic, partition, true)
}

// ExistingPartition is Partition for a partition that must already have its
// directory (EnsurePartitionDirs creates them when the topic is created). The
// broker's produce and fetch paths use it: a request that resolved its topic just
// before DeleteTopics must not re-create the directory behind the delete, where
// the next topic of that name would find the old records.
func (m *Manager) ExistingPartition(topic string, partition int32) (*Partition, error) {
	return m.open(topic, partition, false)
}

func (m *Manager) open(topic string, partition int32, create bool) (*Partition, error) {
	tp := topicPartition{topic, partition}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return nil, ErrManagerClosed
	}
	if p, ok := m.partitions[tp]; ok {
		return p, nil
	}
	dir, err := m.dirFor(tp)
	if err != nil {
		return nil, err
	}
	if !create {
		if st, err := os.Stat(dir); err != nil || !st.IsDir() {
			return nil, fmt.Errorf("%w: %s-%d", ErrNoSuchPartition, topic, partition)
		}
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

// EnsurePartitionDirs creates the directories of a topic's partitions without
// opening them. Opening a partition costs two file descriptors per segment and a
// recovery scan, and creating a topic used to open every one of its partitions —
// a 5,000-partition topic held about 10,000 descriptors from the moment it was
// created. Partitions are opened when first produced to or fetched from; the
// registry, not an empty directory, is what makes a topic exist.
func (m *Manager) EnsurePartitionDirs(topic string, partitions int32) error {
	for p := int32(0); p < partitions; p++ {
		dir, err := m.dirFor(topicPartition{topic, p})
		if err != nil {
			return err
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	syncDir(m.dir) // one flush for all the new entries.
	return nil
}

// StrayDirs lists the partition directories on disk whose topic the registry does
// not know (known reports a topic's partition count). They are what is left when
// the metadata database is lost or pointed elsewhere: data with no topic.
func (m *Manager) StrayDirs(known func(topic string) (partitions int32, ok bool)) []string {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		topic, part, ok := splitPartitionDir(e.Name())
		if !ok {
			continue
		}
		if n, ok := known(topic); !ok || part >= n {
			out = append(out, e.Name())
		}
	}
	return out
}

// RemoveStrayTopicDirs deletes every directory that looks like a partition of
// topic, whatever its number, and reports how many it removed. CreateTopics calls
// it before making a new topic's directories: anything already there belongs to no
// topic (the registry has just said the name is free) and would otherwise become
// the new topic's first records.
func (m *Manager) RemoveStrayTopicDirs(topic string) (int, error) {
	if _, err := m.dirFor(topicPartition{topic, 0}); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range entries {
		t, part, ok := splitPartitionDir(e.Name())
		if !ok || !e.IsDir() || t != topic {
			continue
		}
		tp := topicPartition{topic, part}
		if p, open := m.partitions[tp]; open {
			_ = p.close()
			delete(m.partitions, tp)
		}
		if err := os.RemoveAll(filepath.Join(m.dir, e.Name())); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// splitPartitionDir parses "orders-3" into ("orders", 3). The partition is the
// digits after the last dash, so topic names containing dashes are unambiguous.
func splitPartitionDir(name string) (topic string, partition int32, ok bool) {
	i := strings.LastIndexByte(name, '-')
	if i <= 0 || i == len(name)-1 {
		return "", 0, false
	}
	n, err := strconv.ParseInt(name[i+1:], 10, 32)
	if err != nil || n < 0 || strconv.Itoa(int(n)) != name[i+1:] {
		return "", 0, false
	}
	return name[:i], int32(n), true
}

// OpenPartitions reports how many partitions are currently open (each holds file
// descriptors). For tests and diagnostics.
func (m *Manager) OpenPartitions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.partitions)
}

// Bounds returns a partition's log-start and log-end offsets. An open partition
// answers from memory; one that is not open is read from its files without being
// opened, so asking about every partition of a large topic (what a topic listing
// does, repeatedly) does not open them all.
func (m *Manager) Bounds(topic string, partition int32) (start, end int64, err error) {
	tp := topicPartition{topic, partition}
	m.mu.Lock()
	defer m.mu.Unlock() // held across the file reads so the partition cannot be opened mid-way.
	if p, ok := m.partitions[tp]; ok {
		return p.LogStartOffset(), p.LogEndOffset(), nil
	}
	dir, err := m.dirFor(tp)
	if err != nil {
		return 0, 0, err
	}
	return boundsFromDisk(dir)
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

	// Cold partitions are swept once now and then once a minute: it reads the
	// whole data directory, which is not something to do every second.
	var closedC <-chan time.Time
	if m.opts.Retention.enabled() {
		if err := m.EnforceClosedRetention(); err != nil {
			m.log.Warn("retention of unopened partitions failed", "err", err)
		}
		t := time.NewTicker(closedRetentionInterval)
		defer t.Stop()
		closedC = t.C
	}

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
		case <-closedC:
			if err := m.EnforceClosedRetention(); err != nil {
				m.log.Warn("retention of unopened partitions failed", "err", err)
			}
		}
	}
}

// Close stops the maintenance loop, then flushes and closes every open partition
// (a graceful, durable shutdown), returning the first error encountered.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil // already closed: closing twice is not an error.
	}
	m.stopped = true
	close(m.done)
	m.mu.Unlock()
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()
	defer func() { m.partitions = make(map[topicPartition]*Partition) }()
	// Close in parallel: each close is an fsync (when there is unflushed data) and
	// several file closes, and doing 5,000 of them one after another took half a
	// minute at shutdown. The pool is bounded so it cannot exhaust descriptors
	// or threads itself.
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
		sem      = make(chan struct{}, closeConcurrency)
	)
	for _, p := range m.partitions {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if err := p.close(); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// closeConcurrency bounds the parallel partition closes at shutdown.
const closeConcurrency = 32
