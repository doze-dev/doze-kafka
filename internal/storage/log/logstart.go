package log

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DeleteRecords moves a partition's log-start offset up, which a segment's name
// cannot say: segments are named for the offset they begin at, and the new start is
// usually in the middle of one. The offset is kept in a small file beside the
// segments, read when the partition opens (and when its bounds are read without
// opening it), and the larger of it and the first segment's base is the log start.
const logStartFile = "log-start"

// HighWatermark is the offset DeleteRecords uses to mean "everything".
const HighWatermark = -1

// readLogStart is the recorded log-start offset of the partition in dir, or 0 when
// none was recorded (or the file is unreadable, in which case nothing is hidden: the
// segments are what is on disk).
func readLogStart(dir string) int64 {
	raw, err := os.ReadFile(filepath.Join(dir, logStartFile))
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// writeLogStart records the log-start offset durably: a crash leaves either the old
// value or the new one, never a torn file, and a power cut after it returns does
// not bring the old one back.
func writeLogStart(dir string, offset int64) error {
	return writeFileDurably(filepath.Join(dir, logStartFile), []byte(strconv.FormatInt(offset, 10)+"\n"))
}

// DeleteBefore makes offset the partition's log-start offset: records below it are
// no longer served, and whole segments that lie below it are removed from disk.
// HighWatermark (-1) means the log-end offset, which empties the partition. An offset
// at or below the current start changes nothing and is not an error; one above the
// log end (or below -1) is ErrOffsetOutOfRange. It returns the log-start offset now in
// force.
//
// The new start is recorded before any file is removed, so a crash part-way leaves
// segments that are merely invisible, and the next open finishes the job.
func (p *Partition) DeleteBefore(offset int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset == HighWatermark {
		offset = p.logEndOffset
	}
	if offset < 0 || offset > p.logEndOffset {
		return p.logStartOffset, ErrOffsetOutOfRange
	}
	if offset <= p.logStartOffset {
		return p.logStartOffset, nil
	}
	// A partition whose flush failed takes no writes until a restart, and moving
	// its start is one. Refused before anything is recorded: the new start used
	// to be written and then the request answered as failed.
	if err := p.offline(); err != nil {
		return p.logStartOffset, err
	}
	// Emptying a partition that has records in its active segment: a fresh
	// segment is started at the log end so the old one can go too, rather than
	// holding what was deleted until the segment happens to roll. The old one is
	// sealed first, while failing still leaves nothing changed.
	empties := offset == p.logEndOffset && p.active().sizeBytes > 0
	if empties {
		if err := p.flushSegment(p.active()); err != nil {
			return p.logStartOffset, err
		}
	}
	if err := writeLogStart(p.dir, offset); err != nil {
		return p.logStartOffset, err
	}
	p.logStartOffset = offset
	if p.txn.ready {
		p.txn.dropBelow(offset)
	}

	// From here the start has moved, durably, and that is the answer whatever
	// happens to the files below it: one that cannot be removed now is invisible,
	// and is removed by a later call or the next open.
	if empties {
		if err := p.addSegment(offset); err != nil {
			p.log.Warn("delete records: the log start moved, but a new segment could not be started; the old one stays on disk until the partition rolls", "partition", filepath.Base(p.dir), "err", err)
			return p.logStartOffset, nil
		}
	}
	for len(p.segments) > 1 && p.segments[1].baseOffset <= offset {
		if err := p.dropOldestLocked(); err != nil {
			p.log.Warn("delete records: the log start moved, but a segment below it could not be removed yet", "partition", filepath.Base(p.dir), "err", err)
			break
		}
	}
	return p.logStartOffset, nil
}

// DeleteBefore is Partition.DeleteBefore for a partition the manager may not have
// open: it opens it (it must exist) and applies the change.
func (m *Manager) DeleteBefore(topic string, partition int32, offset int64) (int64, error) {
	p, err := m.ExistingPartition(topic, partition)
	if err != nil {
		return 0, err
	}
	return p.DeleteBefore(offset)
}
