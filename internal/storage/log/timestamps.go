package log

import (
	"encoding/binary"
	"math"
)

// Timestamp lookups (ListOffsets by time, offsetsForTimes, "replay from ten minutes
// ago"). There is no time index on disk. Each segment keeps, in memory, the largest
// timestamp it holds and a sparse list of marks (the byte position of a batch and the
// largest timestamp BEFORE it), built from batch headers the first time a lookup asks
// and extended for the active segment as it grows. A sealed segment is read once;
// opening the partition again after a restart rebuilds it the same way, so there is no
// second structure to keep consistent with the log, which is what an index file would
// have been.
//
// A lookup then picks the first segment whose largest timestamp reaches the target,
// jumps to the last mark that is still below it, and walks batch headers to the first
// batch whose newest record is at or past the target. Only that batch's records are
// decoded, to find the first record that is.

// markEvery is how many bytes of log lie between two marks.
var markEvery int64 = 64 << 10 // a variable so a test can have many marks in a little data.

type tsMark struct {
	position  int64
	maxBefore int64
}

// tsScan is a segment's timestamp summary, valid for the first `scanned` bytes.
type tsScan struct {
	scanned int64
	max     int64 // largest batch maxTimestamp seen; math.MinInt64 before any.
	marks   []tsMark
	since   int64 // bytes since the last mark.
}

const noTimestamp = math.MinInt64

// extendScan brings s.ts up to the segment's current size. Callers hold the
// partition's lock, which also serialises it against appends.
func (s *Segment) extendScan() error {
	if s.ts.scanned == 0 && len(s.ts.marks) == 0 {
		s.ts.max = noTimestamp
	}
	for s.ts.scanned < s.sizeBytes {
		pos := s.ts.scanned
		if s.sizeBytes-pos < batchFullHeaderBytes {
			break
		}
		header, err := s.readAt(pos, batchFullHeaderBytes)
		if err != nil {
			return err
		}
		info, ok := parseHeader(header)
		if !ok || pos+info.totalBytes > s.sizeBytes {
			break
		}
		if len(s.ts.marks) == 0 || s.ts.since >= markEvery {
			s.ts.marks = append(s.ts.marks, tsMark{position: pos, maxBefore: s.ts.max})
			s.ts.since = 0
		}
		if m := int64(binary.BigEndian.Uint64(header[batchMaxTimestampOff:])); m > s.ts.max {
			s.ts.max = m
		}
		s.ts.since += info.totalBytes
		s.ts.scanned = pos + info.totalBytes
	}
	return nil
}

// startFor is the byte position from which a walk for target must begin: the last
// mark whose preceding records are all below it.
func (s *Segment) startFor(target int64) int64 {
	pos := int64(0)
	for _, m := range s.ts.marks {
		if m.maxBefore != noTimestamp && m.maxBefore >= target {
			break
		}
		pos = m.position
	}
	return pos
}

// visibleFrom is the index of the first segment that holds an offset at or above the
// log-start offset.
func (p *Partition) visibleFrom() int {
	for i := 0; i+1 < len(p.segments); i++ {
		if p.segments[i+1].baseOffset > p.logStartOffset {
			return i
		}
	}
	return len(p.segments) - 1
}

// walkBatches calls visit with each whole batch of s from byte position from, in
// order, until visit returns false.
func (s *Segment) walkBatches(from int64, visit func(batch []byte, info batchInfo) (bool, error)) error {
	for pos := from; pos < s.ts.scanned; {
		header, err := s.readAt(pos, batchHeaderMinBytes)
		if err != nil {
			return err
		}
		info, ok := parseHeader(header)
		if !ok || pos+info.totalBytes > s.ts.scanned {
			return nil
		}
		batch, err := s.readAt(pos, int(info.totalBytes))
		if err != nil {
			return err
		}
		more, err := visit(batch, info)
		if err != nil || !more {
			return err
		}
		pos += info.totalBytes
	}
	return nil
}

// OffsetForTimestamp returns the offset and timestamp of the first record at or past
// the log-start offset whose timestamp is at least target, which is what
// offsetsForTimes and ListOffsets by time mean. found is false when no record
// qualifies (every record is older than target, or there are none).
func (p *Partition) OffsetForTimestamp(target int64) (offset, timestamp int64, found bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.offsetForTimestamp(target)
}

// offsetForTimestamp is OffsetForTimestamp for a caller holding the partition's lock.
func (p *Partition) offsetForTimestamp(target int64) (offset, timestamp int64, found bool, err error) {
	for i := p.visibleFrom(); i < len(p.segments); i++ {
		s := p.segments[i]
		if err := s.extendScan(); err != nil {
			return 0, 0, false, err
		}
		if s.ts.max < target {
			continue
		}
		err := s.walkBatches(s.startFor(target), func(batch []byte, info batchInfo) (bool, error) {
			if info.baseOffset+int64(info.recordCount) <= p.logStartOffset {
				return true, nil
			}
			if int64(binary.BigEndian.Uint64(batch[batchMaxTimestampOff:])) < target {
				return true, nil
			}
			decodeErr := batchRecords(batch, func(off, ts int64) bool {
				if off >= p.logStartOffset && ts >= target {
					offset, timestamp, found = off, ts, true
					return false
				}
				return true
			})
			return !found, decodeErr
		})
		if err != nil {
			return 0, 0, false, err
		}
		if found {
			return offset, timestamp, true, nil
		}
	}
	return -1, -1, false, nil
}

// OffsetOfMaxTimestamp returns the offset and timestamp of the visible record with
// the largest timestamp (the first of them if several tie), as Kafka does for the
// "max timestamp" ListOffsets query.
func (p *Partition) OffsetOfMaxTimestamp() (offset, timestamp int64, found bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	best := int64(noTimestamp)
	first := p.visibleFrom()
	for i := first; i < len(p.segments); i++ {
		s := p.segments[i]
		if err := s.extendScan(); err != nil {
			return 0, 0, false, err
		}
		m := s.ts.max
		if i == first && p.logStartOffset > s.baseOffset {
			// Some of this segment's records are deleted, and the largest timestamp
			// may have been one of them: take the maximum over what is visible.
			m = noTimestamp
			err := s.walkBatches(0, func(batch []byte, info batchInfo) (bool, error) {
				if info.baseOffset+int64(info.recordCount) <= p.logStartOffset {
					return true, nil
				}
				return true, batchRecords(batch, func(off, ts int64) bool {
					if off >= p.logStartOffset && ts > m {
						m = ts
					}
					return true
				})
			})
			if err != nil {
				return 0, 0, false, err
			}
		}
		if m > best {
			best = m
		}
	}
	if best == noTimestamp {
		return -1, -1, false, nil
	}
	return p.offsetForTimestamp(best)
}
