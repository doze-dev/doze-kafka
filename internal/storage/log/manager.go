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

	// TopicLimits are the topics that have a segment size or retention of their
	// own, known when the manager opens. They must be in place before the first
	// retention sweep, which is why they are an option rather than a call made
	// afterwards: a topic set to keep everything must not be trimmed by the
	// broker-wide default in the moment between the two.
	TopicLimits map[string]TopicLimits

	// BeforeSync, if set, runs before every fsync, and a non-nil error is returned
	// in its place. It is fault injection for tests ("the disk failed"); leave it
	// nil in production.
	BeforeSync func() error

	// Logger receives background-loop warnings (fsync/retention errors). Nil
	// discards them.
	Logger *slog.Logger

	// What the manager knows about a partition it is opening (cleanstop.go):
	// vouched, that the last stop was clean and flushed it; unvalidated, that its
	// sealed segments are not known to be whole. A partition opened without a
	// manager is neither vouched for nor unvalidated.
	vouched     bool
	unvalidated bool
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

	done   chan struct{}
	wg     sync.WaitGroup
	bounds map[topicPartition]cachedBounds // Bounds answers for closed partitions; guarded by mu.
	limits map[string]TopicLimits          // topics with limits of their own; guarded by mu.

	// From the last stop (cleanstop.go); guarded by mu after OpenManager.
	cleanStart  bool                // the last stop was clean.
	unvouched   map[string]struct{} // partition directories a clean stop did not cover.
	unvalidated map[string]struct{} // partition directories whose sealed segments are unchecked.
}

// TopicLimits is one topic's segment size and retention, where they differ from
// the manager's. They are the values in force, already resolved by the caller: a
// zero SegmentBytes means the manager's, and a zero bound in Retention means that
// bound is off for this topic (even if the manager's default has one).
type TopicLimits struct {
	SegmentBytes int64
	Retention    RetentionPolicy
	Compaction   CompactionPolicy
}

// SetTopicLimits gives a topic its own segment size and retention, from now on:
// partitions already open roll at the new size from their next append, and the
// next retention sweep uses the new bounds.
func (m *Manager) SetTopicLimits(topic string, l TopicLimits) {
	m.mu.Lock()
	m.limits[topic] = l
	var open []*Partition
	for tp, p := range m.partitions {
		if tp.topic == topic {
			open = append(open, p)
		}
	}
	size := m.segmentBytesLocked(topic)
	m.mu.Unlock()
	for _, p := range open {
		p.setSegmentBytes(size)
	}
}

// ClearTopicLimits returns a topic to the manager's own limits (its overrides were
// removed, or the topic was deleted).
func (m *Manager) ClearTopicLimits(topic string) {
	m.mu.Lock()
	delete(m.limits, topic)
	var open []*Partition
	for tp, p := range m.partitions {
		if tp.topic == topic {
			open = append(open, p)
		}
	}
	m.mu.Unlock()
	for _, p := range open {
		p.setSegmentBytes(m.opts.SegmentBytes)
	}
}

// segmentBytesLocked is the segment size in force for a topic. The caller holds mu.
func (m *Manager) segmentBytesLocked(topic string) int64 {
	if l, ok := m.limits[topic]; ok && l.SegmentBytes > 0 {
		return l.SegmentBytes
	}
	return m.opts.SegmentBytes
}

// retentionLocked is the retention policy in force for a topic. The caller holds mu.
func (m *Manager) retentionLocked(topic string) RetentionPolicy {
	if l, ok := m.limits[topic]; ok {
		return l.Retention
	}
	return m.opts.Retention
}

// compactionLocked is the compaction policy in force for a topic: none unless the topic
// asks for one. The caller holds mu.
func (m *Manager) compactionLocked(topic string) CompactionPolicy {
	if l, ok := m.limits[topic]; ok {
		return l.Compaction
	}
	return CompactionPolicy{}
}

// anyRetentionLocked reports whether any retention is in force anywhere, which is
// whether a sweep of the data directory is worth doing. The caller holds mu.
func (m *Manager) anyRetentionLocked() bool {
	if m.opts.Retention.enabled() {
		return true
	}
	for _, l := range m.limits {
		if l.Retention.enabled() {
			return true
		}
	}
	return false
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
	if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		// The new directory's own name. Best effort, unlike every flush inside the
		// data directory: its parent is the user's, and may be one this process can
		// search but not open.
		_ = fsSyncDir(filepath.Dir(dir))
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
		bounds:     make(map[topicPartition]cachedBounds),
		limits:     make(map[string]TopicLimits),
		done:       make(chan struct{}),
	}
	for topic, l := range opts.TopicLimits {
		m.limits[topic] = l
	}
	if err := m.loadStopState(); err != nil {
		return nil, fmt.Errorf("log: reading how the last stop ended: %w", err)
	}
	// Always: a topic can be given retention of its own at any time, whatever the
	// broker-wide setting was at start-up. An idle sweep costs nothing on disk.
	m.wg.Add(1)
	go m.maintain()
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
	opts := m.opts
	opts.SegmentBytes = m.segmentBytesLocked(topic)
	name := filepath.Base(dir)
	opts.vouched = m.vouchedLocked(name)
	_, opts.unvalidated = m.unvalidated[name]
	p, err := openPartition(dir, opts)
	if err != nil {
		return nil, err
	}
	p.topic, p.number = topic, partition
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
	made := false
	for p := int32(0); p < partitions; p++ {
		dir, err := m.dirFor(topicPartition{topic, p})
		if err != nil {
			return err
		}
		created, err := fsMkdir(dir)
		if err != nil {
			return err
		}
		made = made || created
	}
	if !made {
		return nil
	}
	// One flush for all the new entries, and it has to succeed: a topic whose
	// creation was acknowledged must not come back from a power cut without its
	// partition directories.
	return fsSyncDir(m.dir)
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
		if err := m.removeDirLocked(e.Name()); err != nil {
			return n, err
		}
		n++
	}
	if n > 0 {
		// The removals, on disk, before a new topic of this name is made: one that
		// came back in a power cut would be the new topic's first records.
		if err := fsSyncDir(m.dir); err != nil {
			return n, err
		}
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
	// The scan validates the tail as recovery does (checksums over up to 8 MiB),
	// so its answer is kept until the partition's files change.
	sig, ok := boundsSignature(dir)
	if c, hit := m.bounds[tp]; ok && hit && c.sig == sig {
		return c.start, c.end, nil
	}
	// As opening it would: every batch of the last segment checked unless the last
	// stop vouched for the partition.
	name := filepath.Base(dir)
	_, unvalidated := m.unvalidated[name]
	start, end, err = boundsFromDisk(dir, unvalidated || !m.vouchedLocked(name))
	if err == nil && ok {
		m.bounds[tp] = cachedBounds{sig: sig, start: start, end: end}
	} else {
		delete(m.bounds, tp)
	}
	return start, end, err
}

type cachedBounds struct {
	sig        string
	start, end int64
}

// RemoveTopic closes and deletes every partition of a topic from disk (used by
// DeleteTopics). It removes partitions [0, partitions) — the count the caller read
// from the registry before deleting the topic there. Best-effort: it returns the
// first error but continues removing the rest.
func (m *Manager) RemoveTopic(topic string, partitions int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.limits, topic) // the next topic of this name starts from the defaults.
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
		if err := m.removeDirLocked(filepath.Base(dir)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// On disk: a deleted topic's directories that came back in a power cut would
	// be data with no topic.
	if err := fsSyncDir(m.dir); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// removeDirLocked deletes a partition directory and forgets what was known about
// it: the next directory of that name is a new partition. The caller holds mu.
func (m *Manager) removeDirLocked(name string) error {
	path := filepath.Join(m.dir, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	if err := fsBefore(FSRemove, path, ""); err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	fsDone(FSRemove, path, "")
	delete(m.unvouched, name)
	delete(m.unvalidated, name)
	return nil
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

// periodicSync is Sync for the timer. A partition that cannot be flushed is
// reported by name, once per failure rather than once per tick: an offline
// partition fails every tick until the broker restarts, and the same line twice
// a second for hours buried everything else in the log.
func (m *Manager) periodicSync() {
	for _, p := range m.snapshot() {
		err := p.Sync()
		if err == nil {
			p.syncWarned.Store(nil)
			continue
		}
		// An offline partition fails every tick, first with the fsync's own error
		// and then with "offline" wrapped round it: one line for all of them.
		msg := err.Error()
		if p.Offline() {
			msg = "offline"
		}
		if last := p.syncWarned.Load(); last != nil && *last == msg {
			continue
		}
		p.syncWarned.Store(&msg)
		if p.Offline() {
			m.log.Error("periodic fsync failed: the partition takes no writes and acknowledges nothing until the broker restarts", "topic", p.topic, "partition", p.number, "err", err)
		} else {
			m.log.Warn("periodic fsync failed", "topic", p.topic, "partition", p.number, "err", err)
		}
	}
}

// EnforceRetention applies the retention policy to every open partition. It is
// called by the maintenance loop and is exported so tests can trigger a sweep
// deterministically.
func (m *Manager) EnforceRetention() error {
	var firstErr error
	for _, p := range m.snapshot() {
		m.mu.Lock()
		rp := m.retentionLocked(p.topic)
		m.mu.Unlock()
		if err := rp.Enforce(p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// EnforceCompaction runs a cleaning pass on every open partition whose topic is
// compacted, one at a time. It is called by the maintenance loop, ahead of retention (a
// topic that both compacts and deletes is compacted first), and exported so tests can
// trigger a pass deterministically. Partitions that are not open are cleaned when they are.
func (m *Manager) EnforceCompaction() error {
	var firstErr error
	for _, p := range m.snapshot() {
		m.mu.Lock()
		pol := m.compactionLocked(p.topic)
		m.mu.Unlock()
		if !pol.Enabled {
			continue
		}
		res, err := p.Compact(pol, time.Now())
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if res.Ran && (res.RecordsRemoved > 0 || res.MarkersRemoved > 0 || res.SegmentsRemoved > 0) {
			m.log.Info("compacted a partition", "topic", p.topic, "dir", filepath.Base(p.dir),
				"records_removed", res.RecordsRemoved, "markers_removed", res.MarkersRemoved,
				"segments_rewritten", res.SegmentsRewritten, "segments_removed", res.SegmentsRemoved,
				"bytes_before", res.BytesBefore, "bytes_after", res.BytesAfter, "complete", res.Complete)
		}
	}
	return firstErr
}

// maintain runs the coarse fsync and retention timers until Close. A disabled
// timer leaves its channel nil, which blocks forever in the select.
func (m *Manager) maintain() {
	defer m.wg.Done()

	// Cold partitions are swept once now and then once a minute: it reads the
	// whole data directory, which is not something to do every second. The sweeps
	// themselves do nothing where no retention is in force.
	if err := m.EnforceClosedRetention(); err != nil {
		m.log.Warn("retention of unopened partitions failed", "err", err)
	}
	closedT := time.NewTicker(closedRetentionInterval)
	defer closedT.Stop()
	closedC := closedT.C

	var fsyncC <-chan time.Time
	if m.opts.FsyncInterval > 0 {
		t := time.NewTicker(m.opts.FsyncInterval)
		defer t.Stop()
		fsyncC = t.C
	}
	retentionT := time.NewTicker(retentionCheckInterval)
	defer retentionT.Stop()
	retentionC := retentionT.C

	for {
		select {
		case <-m.done:
			return
		case <-fsyncC:
			m.periodicSync()
		case <-retentionC:
			if err := m.EnforceCompaction(); err != nil {
				m.log.Warn("compaction failed", "err", err)
			}
			if err := m.EnforceRetention(); err != nil {
				m.log.Warn("retention enforcement failed", "err", err)
			}
			if err := m.saveUnvalidated(); err != nil {
				m.log.Warn("could not record which partitions have been checked", "err", err)
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
	closed := make(map[string]struct{}, len(m.partitions))
	for _, p := range m.partitions {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			err := p.close()
			errMu.Lock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			if err == nil {
				closed[filepath.Base(p.dir)] = struct{}{}
			}
			errMu.Unlock()
		}()
	}
	wg.Wait()
	// What was checked and flushed need not be checked again, however this stop
	// ends. (The partitions are closed; validated only reads a flag.)
	if err := m.saveUnvalidatedLocked(); err != nil && firstErr == nil {
		firstErr = err
	}
	// Only a stop that flushed everything may say so. One that could not leaves no
	// marker, and the next start trusts nothing it finds.
	if firstErr == nil {
		firstErr = m.writeCleanStop(closed)
	}
	return firstErr
}

// AbandonForTest stops the manager the way SIGKILL stops a process: the
// maintenance loop ends and every file is closed, with nothing flushed and no
// marker of a clean stop written. What was written and not flushed stays in the
// page cache, so the next manager on the directory reads it back, which is the
// situation a restart after a kill is in. Nothing in the broker calls it.
func (m *Manager) AbandonForTest() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	close(m.done)
	m.mu.Unlock()
	m.wg.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.partitions {
		p.mu.Lock()
		for _, s := range p.segments {
			s.close()
		}
		p.mu.Unlock()
	}
	m.partitions = make(map[topicPartition]*Partition)
}

// closeConcurrency bounds the parallel partition closes at shutdown.
const closeConcurrency = 32
