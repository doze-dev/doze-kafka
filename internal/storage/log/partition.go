package log

import (
	"errors"
	"fmt"
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
	segmentBytes  int64
	indexInterval int64
	beforeSync    func() error

	mu             sync.Mutex
	segments       []*Segment // sorted by base offset; last == active.
	logEndOffset   int64
	logStartOffset int64

	// Group commit. Every Append bumps appendSeq; syncedSeq is how far a completed
	// fsync is known to cover. A caller that needs its write durable waits its turn
	// at syncMu and, if a concurrent caller's fsync already covered its write, is
	// done without issuing one — N concurrent acks=all producers on a partition
	// share fsyncs instead of paying N in series.
	appendSeq atomic.Uint64
	syncMu    sync.Mutex
	syncedSeq atomic.Uint64
}

// openPartition opens (creating if needed) a partition directory, loading its
// segments and recovering the active segment's tail.
func openPartition(dir string, opts ManagerOptions) (*Partition, error) {
	_, statErr := os.Stat(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if os.IsNotExist(statErr) {
		syncDir(filepath.Dir(dir)) // persist the new directory's entry.
	}
	p := &Partition{dir: dir, segmentBytes: opts.SegmentBytes, indexInterval: opts.IndexIntervalBytes, beforeSync: opts.BeforeSync}

	bases, err := listSegmentBases(dir)
	if err != nil {
		return nil, err
	}
	if len(bases) == 0 {
		seg, err := openSegment(dir, 0, p.indexInterval, p.beforeSync)
		if err != nil {
			return nil, err
		}
		p.segments = []*Segment{seg}
		return p, nil
	}

	for _, base := range bases {
		seg, err := openSegment(dir, base, p.indexInterval, p.beforeSync)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, seg)
	}
	p.logStartOffset = p.segments[0].baseOffset
	leo, err := recoverSegment(p.active())
	if err != nil {
		return nil, err
	}
	p.logEndOffset = leo
	return p, nil
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

	firstOffset := p.logEndOffset
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
		offset := p.logEndOffset
		stampBaseOffset(batch, offset)

		if p.active().sizeBytes > 0 && p.active().sizeBytes+int64(len(batch)) > p.segmentBytes {
			// Durably seal the segment being rolled off before opening the next, so
			// the periodic timer (which only syncs the active segment) never leaves
			// a completed segment unsynced.
			if err := p.active().sync(); err != nil {
				return 0, err
			}
			seg, err := openSegment(p.dir, offset, p.indexInterval, p.beforeSync)
			if err != nil {
				return 0, err
			}
			p.segments = append(p.segments, seg)
		}
		if err := p.active().append(offset, batch); err != nil {
			return 0, err
		}
		p.appendSeq.Add(1)
		p.logEndOffset = offset + int64(info.recordCount)
		pos += int(info.totalBytes)
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
// told so it can reset, as it would on real Kafka. Answering "no data" instead
// makes it wait, then silently skip every record up to that offset.
func (p *Partition) Read(fetchOffset, maxBytes int64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

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
	if seg == nil {
		return []byte{}, nil
	}

	pos := seg.index.lookup(fetchOffset)
	for pos < seg.sizeBytes {
		header, err := seg.readAt(pos, batchHeaderMinBytes)
		if err != nil {
			return nil, err
		}
		info, ok := parseHeader(header)
		if !ok || pos+info.totalBytes > seg.sizeBytes {
			break
		}
		if info.baseOffset+int64(info.recordCount) <= fetchOffset {
			pos += info.totalBytes // this batch ends before the requested offset.
			continue
		}
		n := seg.sizeBytes - pos
		if maxBytes > 0 && maxBytes < n {
			// Never split a batch: only trim to whole batches beyond the first.
			n = alignToBatch(seg, pos, maxBytes)
		}
		return seg.readAt(pos, int(n))
	}
	return []byte{}, nil
}

// alignToBatch returns a byte count starting at pos that ends on a batch boundary
// and is ≤ maxBytes, but always includes at least the first batch.
func alignToBatch(seg *Segment, pos, maxBytes int64) int64 {
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
		if total > 0 && total+info.totalBytes > maxBytes {
			break
		}
		total += info.totalBytes
	}
	if total == 0 {
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

// Sync flushes the active segment to stable storage. Sealed (non-active) segments
// were synced when they rolled, so the active one is all that can hold unsynced
// writes.
func (p *Partition) Sync() error {
	want := p.appendSeq.Load()
	if p.syncedSeq.Load() >= want {
		return nil // nothing written since the last fsync: no lock, no syscall.
	}
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	if p.syncedSeq.Load() >= want {
		return nil // a concurrent caller's fsync covered this write while we waited.
	}

	// Note how far the log has got BEFORE flushing: the fsync below covers at
	// least that much (it may cover more), so it is safe to record as durable.
	p.mu.Lock()
	upto := p.appendSeq.Load()
	seg := p.active()
	p.mu.Unlock()

	// The fsync itself runs without p.mu: holding it blocked every produce and
	// fetch on the partition for the duration of a disk flush. File operations are
	// safe to run concurrently with appends. A segment that rolled and was dropped
	// by retention in the meantime reports ErrClosed; it was sealed with an fsync
	// at the roll, so there is nothing left to flush.
	if err := seg.sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	p.syncedSeq.Store(upto)
	return nil
}

// dropOldestLocked deletes the oldest segment's files and advances the log-start
// offset. The caller must hold p.mu and ensure at least two segments exist (the
// active segment is never dropped).
func (p *Partition) dropOldestLocked() error {
	oldest := p.segments[0]
	if err := oldest.close(); err != nil {
		return err
	}
	if err := removeSegmentFiles(p.dir, oldest.baseOffset); err != nil {
		return err
	}
	p.segments = p.segments[1:]
	p.logStartOffset = p.segments[0].baseOffset
	return nil
}

// close syncs the active segment (making a graceful shutdown durable) and then
// closes every segment, returning the first error.
func (p *Partition) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	firstErr := p.active().sync() // a no-op if nothing is unflushed.
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
	if err := os.Remove(base + ".log"); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(base + ".index"); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
