package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Segment is one log file plus its sparse index. Only the active segment (the one
// with the highest base offset) accepts appends; the rest are immutable. The .log
// holds raw Kafka record batches (format v2) exactly as received on the wire.
type Segment struct {
	baseOffset    int64
	log           *os.File
	index         *Index
	sizeBytes     int64
	indexInterval int64
	sinceIndex    int64     // bytes appended since the last index entry.
	lastModified  time.Time // wall-clock time of the most recent append (for age-based retention).
	beforeSync    func() error
	logDirty      atomic.Bool // bytes written since the last fsync of the log.
	ts            tsScan      // timestamp summary for lookups (timestamps.go); guarded by the partition lock.

	// syncMu makes sync one-at-a-time per segment. The dirty flag is cleared
	// before the fsync it stands for has finished, so a second caller arriving
	// mid-flush (the roll sealing this segment, say) would see "clean", skip its
	// own fsync, and move on; if the first then failed, the segment was left
	// unsynced with nobody coming back for it. Waiting for the flush in progress
	// means every caller returns knowing how it actually ended.
	syncMu sync.Mutex

	// failed is set when an fsync of this segment's log or index failed, and never
	// cleared. After a failed fsync the kernel may have dropped the dirty pages and
	// cleared the error, so a retry that returns success proves nothing about the
	// bytes that were lost (the 2018 "fsyncgate" finding). Apache Kafka takes the
	// log directory offline instead of retrying; this segment, and with it the
	// partition's writes, is offline until the broker restarts and recovery
	// decides from what is really on disk (BAD-3).
	failed atomic.Pointer[error]

	// retired is set when the segment has left the partition and its files are
	// closed; see retire.
	retired atomic.Bool
}

// ErrStorageOffline is returned for every write to, and every flush of, a segment
// whose fsync once failed. It wraps the failure that put it offline.
var ErrStorageOffline = errors.New("log: an earlier fsync of this partition failed; its writes are refused until the broker restarts")

func (s *Segment) offline() error {
	if p := s.failed.Load(); p != nil {
		return fmt.Errorf("%w: %w", ErrStorageOffline, *p)
	}
	return nil
}

// segmentName is the 20-digit zero-padded base offset used for a segment's files.
func segmentName(baseOffset int64) string { return fmt.Sprintf("%020d", baseOffset) }

// openSegment opens (creating if needed) the .log and .index files for a segment.
// created reports that it made either file: the names are then only in memory
// until the caller has flushed the directory (Partition.addSegment does), and a
// fully fsynced segment whose name was never flushed vanishes in a power cut.
func openSegment(dir string, baseOffset, indexInterval int64, beforeSync func() error) (seg *Segment, created bool, err error) {
	base := filepath.Join(dir, segmentName(baseOffset))
	return openSegmentFiles(base+".log", base+".index", baseOffset, indexInterval, beforeSync)
}

// openSegmentAt opens a segment whose two files are not (yet) under their own
// names: the cleaner's, before it renames them into place.
func openSegmentAt(logPath, indexPath string, baseOffset, indexInterval int64, beforeSync func() error) (*Segment, error) {
	seg, _, err := openSegmentFiles(logPath, indexPath, baseOffset, indexInterval, beforeSync)
	return seg, err
}

func openSegmentFiles(logPath, indexPath string, baseOffset, indexInterval int64, beforeSync func() error) (seg *Segment, created bool, err error) {
	logFile, madeLog, err := fsOpenFile(logPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, false, err
	}
	info, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		return nil, false, err
	}
	index, madeIndex, err := openIndex(indexPath)
	if err != nil {
		logFile.Close()
		if madeLog {
			_ = fsRemove(logPath)
		}
		return nil, false, err
	}
	return &Segment{
		beforeSync:    beforeSync,
		baseOffset:    baseOffset,
		log:           logFile,
		index:         index,
		sizeBytes:     info.Size(),
		indexInterval: indexInterval,
		lastModified:  info.ModTime(),
	}, madeLog || madeIndex, nil
}

// append writes raw batch bytes at the end of the log and, on crossing the index
// interval (and always for a segment's first batch), records a sparse index
// entry. It never fsyncs — durability is the coarse-timer policy's job.
func (s *Segment) append(offset int64, batch []byte) error {
	if err := s.offline(); err != nil {
		return err
	}
	pos := s.sizeBytes
	if err := faultAt("log.write"); err != nil {
		return err
	}
	// Dirty before the write, not after: a write that fails part-way has still
	// changed the file, and the truncation that undoes it has to reach the disk.
	s.logDirty.Store(true)
	if err := fsWriteAt(s.log, batch, pos); err != nil {
		_ = fsTruncate(s.log, pos) // a short write leaves bytes the log does not count.
		return err
	}
	s.sizeBytes += int64(len(batch))
	s.sinceIndex += int64(len(batch))
	s.lastModified = time.Now()
	if len(s.index.entries) == 0 || s.sinceIndex >= s.indexInterval {
		if err := s.index.append(offset, pos); err != nil {
			// The append is reported as failed, so it must leave no trace: the
			// caller does not advance the log-end offset, and bytes left counted in
			// the segment would make the next append write the same offset again
			// after them — two batches claiming one offset, forever.
			s.sizeBytes = pos
			s.sinceIndex -= int64(len(batch))
			_ = fsTruncate(s.log, pos)
			return err
		}
		s.sinceIndex = 0
	}
	return nil
}

// segmentMark is where a segment's appends stood, for rewind.
type segmentMark struct {
	size, sinceIndex int64
	entries          int
	lastModified     time.Time
}

func (s *Segment) mark() segmentMark {
	return segmentMark{s.sizeBytes, s.sinceIndex, len(s.index.entries), s.lastModified}
}

// rewind takes back every append since m: the bytes and index entries are cut
// off and the segment counts as it did. A cut that fails leaves bytes past the
// size the segment counts; the next append writes over them, and recovery after
// a crash before then finds them as it would a torn tail.
func (s *Segment) rewind(m segmentMark) {
	s.sizeBytes, s.sinceIndex, s.lastModified = m.size, m.sinceIndex, m.lastModified
	_ = fsTruncate(s.log, m.size)
	if len(s.index.entries) > m.entries {
		s.index.entries = s.index.entries[:m.entries]
		_ = fsTruncate(s.index.file, int64(m.entries)*indexEntryBytes)
	}
}

// sync flushes this segment's log and index to stable storage. It is the unit of
// durability: the coarse fsync timer syncs the active segment, a roll syncs the
// segment being sealed, and graceful shutdown syncs everything before close.
func (s *Segment) sync() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if s.retired.Load() {
		// The cleaner replaced this segment (with files it flushed) or retention
		// removed it, after the caller picked it up: there is nothing of it left to
		// flush, and its files are closed.
		return nil
	}
	if err := s.offline(); err != nil {
		return err
	}
	if s.logDirty.Load() || s.index.dirty.Load() {
		// The fault seam's "log.sync" point and this manager's BeforeSync hook
		// are the same place: the seam first, the per-manager hook second.
		if err := faultAt("log.sync"); err != nil {
			return err
		}
		if s.beforeSync != nil {
			if err := s.beforeSync(); err != nil {
				return err
			}
		}
	}
	// Only flush what changed. The periodic timer used to fsync every open
	// partition's log and index whether or not anything had been written, which on
	// an idle broker is pure waste.
	if s.logDirty.Swap(false) {
		if err := fsSync(s.log); err != nil {
			s.logDirty.Store(true)
			s.failed.Store(&err)
			return err
		}
	}
	if err := s.index.sync(); err != nil {
		s.failed.Store(&err)
		return err
	}
	return nil
}

// readAt returns up to n raw bytes at position pos via pread, serving reads from
// the OS page cache without pulling the log into the heap.
func (s *Segment) readAt(pos int64, n int) ([]byte, error) {
	if err := faultAt("log.read"); err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	m, err := s.log.ReadAt(buf, pos)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:m], nil
}

// truncate cuts the log file to size bytes (used by recovery to drop a torn tail).
func (s *Segment) truncate(size int64) error {
	if err := fsTruncate(s.log, size); err != nil {
		return err
	}
	s.sizeBytes = size
	// Make the cut durable: otherwise a second crash resurrects the torn bytes
	// that this recovery just decided were not part of the log.
	return fsSync(s.log)
}

// retire marks a segment that is leaving the partition (replaced by the cleaner,
// or deleted) and closes its files, after any flush of it that is in progress.
func (s *Segment) retire() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.retired.Store(true)
	return s.close()
}

func (s *Segment) close() error {
	errIndex := s.index.close()
	errLog := s.log.Close()
	if errIndex != nil {
		return errIndex
	}
	return errLog
}
