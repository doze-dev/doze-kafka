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
// value or the new one, never a torn file.
func writeLogStart(dir string, offset int64) error {
	tmp := filepath.Join(dir, logStartFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strconv.FormatInt(offset, 10) + "\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, logStartFile)); err != nil {
		return err
	}
	syncDir(dir)
	return nil
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
	if err := writeLogStart(p.dir, offset); err != nil {
		return p.logStartOffset, err
	}
	p.logStartOffset = offset

	// Emptying a partition that has records in its active segment: start a fresh
	// segment at the log end so the old one can go too, rather than holding what was
	// deleted until the segment happens to roll.
	if offset == p.logEndOffset && p.active().sizeBytes > 0 {
		if err := p.active().sync(); err != nil {
			return p.logStartOffset, err
		}
		seg, err := openSegment(p.dir, offset, p.indexInterval, p.beforeSync)
		if err != nil {
			return p.logStartOffset, err
		}
		p.segments = append(p.segments, seg)
	}
	for len(p.segments) > 1 && p.segments[1].baseOffset <= offset {
		if err := p.dropOldestLocked(); err != nil {
			return p.logStartOffset, err
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
