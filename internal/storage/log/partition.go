package log

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrOffsetOutOfRange is returned by Read when the requested offset is below the
// log-start offset (it has been deleted by retention).
var ErrOffsetOutOfRange = errors.New("log: offset out of range")

// Partition is a single topic-partition's append-only log: an ordered sequence of
// immutable segments with exactly one active (append) segment. It owns the
// log-end offset (LEO) and log-start offset and assigns offsets to record batches.
type Partition struct {
	dir           string
	topic         string // set by the Manager; which topic's limits apply.
	number        int32  // set by the Manager; the partition's number, for its log lines.
	indexInterval int64
	beforeSync    func() error
	log           *slog.Logger

	mu             sync.Mutex
	segmentBytes   int64      // roll the active segment past this size; guarded by mu.
	segments       []*Segment // sorted by base offset; last == active.
	logEndOffset   int64
	logStartOffset int64
	txn            txnState // open and aborted transactions; see txn.go.

	// What this process does not know to be on disk (durability.go). unsynced are
	// the sealed segments it has not flushed itself; dirSeq counts changes to the
	// directory's entries and dirSynced is how many of them a directory flush has
	// covered; unvalidated is set while the partition is one whose sealed segments
	// had to be checked on open and have not all been flushed since. Guarded by mu.
	unsynced    []*Segment
	dirSeq      uint64
	dirSynced   uint64
	unvalidated bool

	// syncWarned is the last failure the periodic flush reported for this
	// partition, so the same one is not reported every tick.
	syncWarned atomic.Pointer[string]

	// txnMarked is set once this process knows the "transactions" file is on disk.
	txnMarked bool

	// failed is set when a flush of this partition failed, and never cleared: see
	// Segment.failed. It covers what a segment's own flag cannot, a sealed segment
	// and the directory.
	failed atomic.Pointer[error]

	// The cleaner (compact.go): one pass at a time, the offset below which the log has
	// been cleaned, and what the last pass did.
	cleaning      sync.Mutex
	cleanedTo     int64
	cleanedLoaded bool
	lastClean     CleanResult

	// Group commit. Every change that an acknowledgement can rest on (an append,
	// and at open whatever was read back without being known durable) bumps
	// appendSeq; syncedSeq is how far a completed flush is known to cover. A caller
	// that needs its write durable waits its turn at syncMu and, if a concurrent
	// caller's flush already covered its write, is done without issuing one: N
	// concurrent acks=all producers on a partition share fsyncs instead of paying
	// N in series.
	appendSeq atomic.Uint64
	syncMu    sync.Mutex
	syncedSeq atomic.Uint64
}

// openPartition opens (creating if needed) a partition directory, loading its
// segments and recovering what a crash may have left (recovery.go).
//
// Nothing read back here is taken to be on disk unless the caller vouches for
// it (opts.vouched: the last stop was clean and flushed this partition): the
// previous process may have been killed with these bytes still in the page
// cache, and they would be acknowledged again, by a duplicate retry or a
// transaction's end, with no fsync ever having covered them. What is not vouched
// for starts dirty, so the first flush of the partition covers it.
func openPartition(dir string, opts ManagerOptions) (*Partition, error) {
	if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
		if err := fsBefore(FSMkdir, dir, ""); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		fsDone(FSMkdir, dir, "")
		if err := fsSyncDir(filepath.Dir(dir)); err != nil { // persist the new directory's entry.
			return nil, err
		}
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	p := &Partition{dir: dir, segmentBytes: opts.SegmentBytes, indexInterval: opts.IndexIntervalBytes, beforeSync: opts.BeforeSync, log: logger}
	// Finish or undo a cleaner swap a crash interrupted, before any index is read.
	if err := recoverSwaps(dir); err != nil {
		return nil, err
	}

	bases, err := listSegmentBases(dir)
	if err != nil {
		return nil, err
	}
	if len(bases) == 0 {
		// Nothing here yet: there is nothing to recover and nothing to doubt.
		if err := p.addSegment(0); err != nil {
			return nil, err
		}
		return p, nil
	}

	closeAll := func() {
		for _, s := range p.segments {
			s.close()
		}
	}
	for _, base := range bases {
		seg, _, err := openSegment(dir, base, p.indexInterval, p.beforeSync)
		if err != nil {
			closeAll()
			return nil, err
		}
		p.segments = append(p.segments, seg)
	}
	if err := p.recover(opts); err != nil {
		closeAll()
		return nil, err
	}
	return p, nil
}

// addSegment creates the segment that begins at base and makes it the active
// one. Its name is not flushed here: a new name makes the directory dirty, and
// the flush that an acknowledgement of anything in the segment waits for (Sync)
// flushes the directory with it. Until then the segment holds nothing anyone was
// promised, and a power cut that takes it takes nothing. The caller holds mu (or
// is opening the partition).
func (p *Partition) addSegment(base int64) error {
	seg, created, err := openSegment(p.dir, base, p.indexInterval, p.beforeSync)
	if err != nil {
		return err
	}
	if created {
		p.dirSeq++
	}
	p.segments = append(p.segments, seg)
	return nil
}

// syncDirLocked flushes the directory if any of its entries changed since the
// last flush. The caller holds mu.
func (p *Partition) syncDirLocked() error {
	if p.dirSynced >= p.dirSeq {
		return nil
	}
	upto := p.dirSeq
	if err := fsSyncDir(p.dir); err != nil {
		return err
	}
	p.dirSynced = upto
	return nil
}

func (p *Partition) active() *Segment { return p.segments[len(p.segments)-1] }

// Append writes the record batches in records (a Produce partition payload may
// carry one or more concatenated batches). Each batch's base offset is stamped to
// the running LEO, the raw bytes are appended to the active segment (rolling first
// if it would overflow), and the LEO advances by the batch's record count. Batch
// bytes are never decoded beyond the header. It returns the base offset assigned
// to the first batch.
func (p *Partition) Append(records []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.appendLocked(records)
}

// appendLocked is Append for a caller holding p.mu.
func (p *Partition) appendLocked(records []byte) (int64, error) {
	if err := p.offline(); err != nil {
		return 0, err
	}
	// A payload is appended whole or not at all, as Kafka appends it: every
	// batch is checked before any is written, the payload goes into one segment
	// (the roll, if one is due, comes first), and a write that fails part-way
	// takes back the batches before it. Leaving them used to answer the
	// partition as failed with records of it in the log: a consumer read what
	// its producer was told had failed, and a plain producer's retry wrote them
	// twice.
	transactional := false
	for pos := 0; pos < len(records); {
		info, ok := parseHeader(records[pos:])
		if !ok || pos+int(info.totalBytes) > len(records) {
			return 0, fmt.Errorf("log: malformed record batch at byte %d of %d", pos, len(records))
		}
		if info.recordCount <= 0 {
			// A non-positive count would move the log-end offset backwards and
			// hand out offsets twice, permanently.
			return 0, fmt.Errorf("%w: record count %d at byte %d", ErrCorruptBatch, info.recordCount, pos)
		}
		batch := records[pos : pos+int(info.totalBytes)]
		if len(batch) >= batchFullHeaderBytes && attrsOf(batch)&(batchTransactionalBit|batchControlBit) != 0 {
			transactional = true
		}
		pos += int(info.totalBytes)
	}
	// The first transactional batch a partition takes starts it keeping
	// transaction state: the marker file says so for a later open, and the log so
	// far is scanned now, before this payload is in it.
	if transactional {
		if err := p.markTransactional(); err != nil {
			return 0, err
		}
		if err := p.ensureTxn(); err != nil {
			return 0, err
		}
	}
	if p.active().sizeBytes > 0 && p.active().sizeBytes+int64(len(records)) > p.segmentBytes {
		if err := p.rollLocked(p.logEndOffset); err != nil {
			return 0, err
		}
	}
	seg := p.active()
	mark := seg.mark()
	firstOffset := p.logEndOffset
	type placed struct {
		batch  []byte
		offset int64
	}
	var done []placed
	for pos := 0; pos < len(records); {
		info, _ := parseHeader(records[pos:])
		batch := records[pos : pos+int(info.totalBytes)]
		offset := p.logEndOffset
		stampBaseOffset(batch, offset)
		if err := seg.append(offset, batch); err != nil {
			if len(done) > 0 {
				seg.rewind(mark)
				p.logEndOffset = firstOffset
				p.appendSeq.Add(1) // the cut has to reach the disk like a write.
			}
			return 0, err
		}
		p.logEndOffset = offset + int64(info.recordCount)
		done = append(done, placed{batch, offset})
		pos += int(info.totalBytes)
	}
	p.appendSeq.Add(1)
	if transactional {
		for _, d := range done {
			if len(d.batch) >= batchFullHeaderBytes && attrsOf(d.batch)&(batchTransactionalBit|batchControlBit) != 0 {
				p.txn.observe(d.batch, d.offset)
			}
		}
	}
	return firstOffset, nil
}

// Read returns raw record batches beginning at the batch that contains
// fetchOffset, up to maxBytes (whole batches; at least one when data exists). An
// offset equal to the LEO yields an empty slice (the consumer is caught up); an
// offset below the log-start offset or above the LEO is ErrOffsetOutOfRange.
//
// Above the LEO matters: a consumer holding an offset the log never reached (a
// committed offset from a topic that was deleted and recreated, say) must be
// told so it can reset, as on Kafka (claim: api V7). Answering "no data" instead
// makes it wait, then silently skip every record up to that offset.
func (p *Partition) Read(fetchOffset, maxBytes int64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readLocked(fetchOffset, maxBytes, -1)
}

// readLocked is Read for a caller holding p.mu. A limit of zero or more is an offset
// at which to stop: no batch that begins at or past it is returned (the last stable
// offset, for a read-committed fetch).
func (p *Partition) readLocked(fetchOffset, maxBytes, limit int64) ([]byte, error) {
	if fetchOffset < p.logStartOffset {
		return nil, ErrOffsetOutOfRange
	}
	if fetchOffset > p.logEndOffset {
		return nil, ErrOffsetOutOfRange
	}
	if fetchOffset == p.logEndOffset {
		return []byte{}, nil
	}
	seg := p.segmentFor(fetchOffset)
	from := fetchOffset
	for seg != nil {
		if data, found, err := p.readSegment(seg, from, fetchOffset, maxBytes, limit); err != nil || found {
			return data, err
		}
		// Nothing at or after the offset in this segment. Compaction can empty a
		// segment's tail, or all of it, and the records that follow are in the next one:
		// carry on there rather than telling the consumer it has caught up.
		next := p.segmentAfter(seg)
		if next == nil {
			break
		}
		seg, from = next, next.baseOffset
	}
	return []byte{}, nil
}

// readSegment looks in one segment for the first batch holding an offset at or after from.
// found is false when there is none; the request's own fetchOffset is kept only for the
// "ends before it" test on the segment it started in.
func (p *Partition) readSegment(seg *Segment, from, fetchOffset, maxBytes, limit int64) (data []byte, found bool, err error) {
	pos := seg.index.lookup(from)
	for pos < seg.sizeBytes {
		header, err := seg.readAt(pos, batchHeaderMinBytes)
		if err != nil {
			return nil, false, err
		}
		info, ok := parseHeader(header)
		if !ok || pos+info.totalBytes > seg.sizeBytes {
			break
		}
		if info.baseOffset+int64(info.recordCount) <= from {
			pos += info.totalBytes // this batch ends before the requested offset.
			continue
		}
		n := seg.sizeBytes - pos
		if limit >= 0 || (maxBytes > 0 && maxBytes < n) {
			// Never split a batch: only trim to whole batches beyond the first.
			n = alignToBatch(seg, pos, maxBytes, limit)
			if n == 0 {
				return []byte{}, true, nil // the first batch is past the limit.
			}
		}
		data, err = seg.readAt(pos, int(n))
		return data, true, err
	}
	return nil, false, nil
}

// segmentAfter is the segment following seg, or nil if it is the last.
func (p *Partition) segmentAfter(seg *Segment) *Segment {
	for i, s := range p.segments {
		if s == seg && i+1 < len(p.segments) {
			return p.segments[i+1]
		}
	}
	return nil
}

// alignToBatch returns a byte count starting at pos that ends on a batch boundary
// and is ≤ maxBytes, but always includes at least the first batch.
func alignToBatch(seg *Segment, pos, maxBytes, limit int64) int64 {
	var total int64
	for pos+total < seg.sizeBytes {
		header, err := seg.readAt(pos+total, batchHeaderMinBytes)
		if err != nil {
			break
		}
		info, ok := parseHeader(header)
		if !ok || pos+total+info.totalBytes > seg.sizeBytes {
			break
		}
		if limit >= 0 && info.baseOffset >= limit {
			break
		}
		if total > 0 && maxBytes > 0 && total+info.totalBytes > maxBytes {
			break
		}
		total += info.totalBytes
	}
	if total == 0 && limit < 0 {
		total = seg.sizeBytes - pos // pathological: return the remainder.
	}
	return total
}

// segmentFor returns the segment whose base offset is the greatest ≤ offset.
func (p *Partition) segmentFor(offset int64) *Segment {
	for i := len(p.segments) - 1; i >= 0; i-- {
		if p.segments[i].baseOffset <= offset {
			return p.segments[i]
		}
	}
	return nil
}

// LogEndOffset returns the offset that will be assigned to the next batch. With no
// replication, this is also the high-watermark.
func (p *Partition) LogEndOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.logEndOffset
}

// LogStartOffset returns the earliest offset still retained on disk.
func (p *Partition) LogStartOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.logStartOffset
}

// dropOldestLocked deletes the oldest segment's files and advances the log-start
// offset. The caller must hold p.mu and ensure at least two segments exist (the
// active segment is never dropped).
func (p *Partition) dropOldestLocked() error {
	oldest := p.segments[0]
	if err := oldest.retire(); err != nil {
		return err
	}
	p.forgetUnsynced(oldest)
	p.segments = p.segments[1:]
	p.logStartOffset = max(p.logStartOffset, p.segments[0].baseOffset)
	if p.txn.ready {
		p.txn.dropBelow(p.logStartOffset)
	}
	// The segment is gone from the partition whether or not its files could be
	// removed: a file left behind is below the log start, and the next open
	// removes it.
	p.dirSeq++
	return removeSegmentFiles(p.dir, oldest.baseOffset)
}

// close flushes everything (making a graceful shutdown durable) and then closes
// every segment, returning the first error.
func (p *Partition) close() error {
	firstErr := p.Sync()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, seg := range p.segments {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// listSegmentBases returns the base offsets of a partition's .log files, ascending.
func listSegmentBases(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	slices.Sort(bases)
	return bases, nil
}

// removeSegmentFiles deletes a segment's .log and .index from disk.
func removeSegmentFiles(dir string, baseOffset int64) error {
	base := filepath.Join(dir, segmentName(baseOffset))
	if err := fsRemove(base + ".log"); err != nil {
		return err
	}
	return fsRemove(base + ".index")
}

// setSegmentBytes changes the size at which the active segment rolls, from the
// next append on. Segments already written are left as they are.
func (p *Partition) setSegmentBytes(n int64) {
	p.mu.Lock()
	p.segmentBytes = n
	p.mu.Unlock()
}
