package log

import (
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
}

// fsyncFile flushes a file to stable storage. A variable so tests can make an
// fsync fail or stall, which no real disk will do on request.
var fsyncFile = func(f *os.File) error { return f.Sync() }

// segmentName is the 20-digit zero-padded base offset used for a segment's files.
func segmentName(baseOffset int64) string { return fmt.Sprintf("%020d", baseOffset) }

// openSegment opens (creating if needed) the .log and .index files for a segment.
func openSegment(dir string, baseOffset, indexInterval int64, beforeSync func() error) (*Segment, error) {
	base := filepath.Join(dir, segmentName(baseOffset))
	_, statErr := os.Stat(base + ".log")
	created := os.IsNotExist(statErr)
	logFile, err := os.OpenFile(base+".log", os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		return nil, err
	}
	index, err := openIndex(base + ".index")
	if err != nil {
		logFile.Close()
		return nil, err
	}
	if created {
		// A new file is only a directory entry until the directory is flushed: an
		// fsynced segment can still vanish in a power cut if its name was never
		// persisted.
		syncDir(dir)
	}
	return &Segment{
		beforeSync:    beforeSync,
		baseOffset:    baseOffset,
		log:           logFile,
		index:         index,
		sizeBytes:     info.Size(),
		indexInterval: indexInterval,
		lastModified:  info.ModTime(),
	}, nil
}

// append writes raw batch bytes at the end of the log and, on crossing the index
// interval (and always for a segment's first batch), records a sparse index
// entry. It never fsyncs — durability is the coarse-timer policy's job.
func (s *Segment) append(offset int64, batch []byte) error {
	pos := s.sizeBytes
	if _, err := s.log.WriteAt(batch, pos); err != nil {
		return err
	}
	s.logDirty.Store(true)
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
			_ = s.log.Truncate(pos)
			return err
		}
		s.sinceIndex = 0
	}
	return nil
}

// sync flushes this segment's log and index to stable storage. It is the unit of
// durability: the coarse fsync timer syncs the active segment, a roll syncs the
// segment being sealed, and graceful shutdown syncs everything before close.
func (s *Segment) sync() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if s.logDirty.Load() || s.index.dirty.Load() {
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
		if err := fsyncFile(s.log); err != nil {
			s.logDirty.Store(true)
			return err
		}
	}
	return s.index.sync()
}

// readAt returns up to n raw bytes at position pos via pread, serving reads from
// the OS page cache without pulling the log into the heap.
func (s *Segment) readAt(pos int64, n int) ([]byte, error) {
	buf := make([]byte, n)
	m, err := s.log.ReadAt(buf, pos)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:m], nil
}

// truncate cuts the log file to size bytes (used by recovery to drop a torn tail).
func (s *Segment) truncate(size int64) error {
	if err := s.log.Truncate(size); err != nil {
		return err
	}
	s.sizeBytes = size
	// Make the cut durable: otherwise a second crash resurrects the torn bytes
	// that this recovery just decided were not part of the log.
	return fsyncFile(s.log)
}

// syncDir flushes a directory's entries to stable storage. Best effort: some
// platforms and filesystems cannot fsync a directory (Windows, certain network
// mounts), and refusing to run there would be worse than the gap it closes.
func syncDir(path string) {
	d, err := os.Open(path)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

func (s *Segment) close() error {
	errIndex := s.index.close()
	errLog := s.log.Close()
	if errIndex != nil {
		return errIndex
	}
	return errLog
}
