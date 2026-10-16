package log

import (
	"path/filepath"
	"slices"
)

// recoveryCRCWindow is how much of the active segment's tail has every batch's
// CRC verified when the partition is opened after a clean stop. A clean stop
// flushed everything, so there is nothing a crash can have damaged and the
// window is a second look, not the defence. After a stop that was not clean, or
// one this partition was not part of, the whole active segment is verified:
// what was written and not flushed is not bounded by a window (with the periodic
// fsync off, or a fast producer, it is as long as the segment), and pages reach
// the disk in any order, so a batch whose header made it and whose body did not
// can sit well before the tail.
const recoveryCRCWindow = 8 << 20

// recover brings an opened partition back to a consistent state and works out
// what of it this process may treat as already on disk. See durability.go.
func (p *Partition) recover(opts ManagerOptions) error {
	trusted := opts.vouched && !opts.unvalidated
	sealed := p.segments[:len(p.segments)-1]

	// Sealed segments are whole and on disk by the roll that sealed them, except
	// in a partition that was written without that guarantee: there every one is
	// checked batch by batch. A segment found damaged is cut at the first bad
	// batch and the segments after it are kept. (Apache Kafka deletes them, and
	// fetches them again from a replica; there is no replica here, and what
	// follows a damaged segment may be records that were acknowledged.)
	if opts.unvalidated {
		for _, s := range sealed {
			before := s.sizeBytes
			leo, err := recoverSegment(s, true)
			if err != nil {
				return err
			}
			if s.sizeBytes < before {
				p.log.Error("log recovery: a sealed segment was damaged and has been cut at the first bad batch; the records after it in that segment are lost",
					"partition", filepath.Base(p.dir), "segment", segmentName(s.baseOffset), "bytes_before", before, "bytes_after", s.sizeBytes, "next_offset", leo)
			}
		}
	}
	leo, err := recoverSegment(p.active(), !trusted)
	if err != nil {
		return err
	}
	p.logEndOffset = leo

	// DeleteRecords may have put the start inside a segment (see logstart.go); never
	// past the end, which only a log truncated below it could produce.
	p.logStartOffset = p.segments[0].baseOffset
	if recorded := min(readLogStart(p.dir), leo); recorded > p.logStartOffset {
		p.logStartOffset = recorded
	}
	// A crash between recording a new start and removing the segments below it
	// leaves them on disk, invisible. Finish the job.
	for len(p.segments) > 1 && p.segments[1].baseOffset <= p.logStartOffset {
		oldest := p.segments[0]
		oldest.close()
		if err := removeSegmentFiles(p.dir, oldest.baseOffset); err != nil {
			return err
		}
		p.segments = p.segments[1:]
		p.dirSeq++
	}

	if trusted {
		p.txnMarked = hasTxnMarker(p.dir)
		return nil
	}
	// Not known to be on disk: the active segment and the directory's entries
	// always, and in an unvalidated partition every sealed segment too.
	p.dirSeq++
	p.appendSeq.Store(1)
	dirty := p.segments[len(p.segments)-1:]
	if opts.unvalidated {
		dirty = p.segments
		p.unvalidated = true
		p.unsynced = slices.Clone(p.segments[:len(p.segments)-1])
	}
	for _, s := range dirty {
		s.logDirty.Store(true)
		s.index.dirty.Store(true)
	}
	return nil
}

// recoverSegment scans a segment from its start and brings it back to a
// consistent state, returning the offset after its last good batch. The log is
// its own write-ahead log, so there is no separate WAL to replay.
//
// It truncates the log at the first batch that is not whole: too short to hold a
// header, not a v2 batch, with lengths or counts that disagree, running past
// EOF, or whose CRC does not match (every batch when crcAll is set, the tail
// window otherwise). That covers an interrupted append, a zero-filled tail after
// a power cut, and a body that never fully reached the disk. It then makes the
// index agree with what is left.
func recoverSegment(s *Segment, crcAll bool) (int64, error) {
	// One buffered pass: a read per batch header and another per body is two
	// syscalls a batch, which over a whole segment is most of the time it takes.
	r := &windowReader{f: s.log, size: s.sizeBytes}
	valid, leo, want, since, err := scanLog(r.read, s.sizeBytes, s.baseOffset, s.indexInterval, crcAll)
	if err != nil {
		return 0, err
	}
	if valid < s.sizeBytes {
		if err := s.truncate(valid); err != nil {
			return 0, err
		}
	}

	// The index is derived from the log, so when they disagree the log wins. An
	// entry can outlive the bytes it points at (the index reached disk, the log
	// did not), or be missing, or partial; trusting it would serve a stale
	// position — mid-batch bytes, or an empty read — for every offset past it.
	if !slices.Equal(s.index.entries, want) {
		if err := s.index.rewrite(want); err != nil {
			return 0, err
		}
	}
	s.sinceIndex = since
	return leo, nil
}

// scanLog walks a segment's batches from the start and reports where the valid log
// ends: the byte position (validEnd), the offset after the last good batch (leo),
// and the sparse index that log should have. It changes nothing. Recovery acts on
// the result (truncating at validEnd); Bounds only reads it, so the two can never
// disagree about where a partition ends.
//
// A batch is good if its header is whole, its magic is 2, it fits in the file, it
// claims at least one record, and its shape and CRC check out. The CRC is
// verified for every batch when crcAll is set, and otherwise only within the last
// recoveryCRCWindow bytes.
func scanLog(read func(pos int64, n int) ([]byte, error), size, base, indexInterval int64, crcAll bool) (validEnd, leo int64, want []indexEntry, since int64, err error) {
	leo = base
	crcFrom := size - recoveryCRCWindow
	if crcAll {
		crcFrom = 0
	}
	var pos int64
	for pos < size {
		if size-pos < batchFullHeaderBytes {
			break
		}
		header, err := read(pos, batchFullHeaderBytes)
		if err != nil {
			return 0, 0, nil, 0, err
		}
		info, ok := parseHeader(header)
		if !ok || len(header) < batchFullHeaderBytes || header[batchMagicOff] != batchMagicV2 || pos+info.totalBytes > size || info.recordCount <= 0 {
			break
		}
		if pos >= crcFrom {
			batch, err := read(pos, int(info.totalBytes))
			if err != nil {
				return 0, 0, nil, 0, err
			}
			if _, err := checkBatchShape(batch); err != nil || !crcOK(batch) {
				break
			}
		}
		since += info.totalBytes
		if len(want) == 0 || since >= indexInterval {
			want = append(want, indexEntry{offset: info.baseOffset, position: pos})
			since = 0
		}
		leo = info.baseOffset + int64(info.recordCount)
		pos += info.totalBytes
	}
	return pos, leo, want, since, nil
}
