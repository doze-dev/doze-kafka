package log

import "slices"

// recoveryCRCWindow is how much of the active segment's tail has every batch's
// CRC verified on open. A crash can only damage what was being written, and the
// periodic fsync bounds that to a sub-second of data, so the tail is where
// corruption lives; verifying the whole segment would read up to 128 MiB per
// partition on first access. Everything before the window still gets its
// structure checked.
const recoveryCRCWindow = 8 << 20

// recoverSegment scans the active segment from its start and brings it back to a
// consistent state, returning the recovered log-end offset. The log is its own
// write-ahead log, so there is no separate WAL to replay.
//
// It truncates the log at the first batch that is not whole: too short to hold a
// header, not a v2 batch, with lengths or counts that disagree, running past
// EOF, or — within the tail window — whose CRC does not match. That covers an
// interrupted append, a zero-filled tail after a power cut, and a body that
// never fully reached the disk. It then makes the index agree with what is left.
func recoverSegment(s *Segment) (int64, error) {
	valid, leo, want, since, err := scanLog(s.readAt, s.sizeBytes, s.baseOffset, s.indexInterval)
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
// claims at least one record, and — within the last recoveryCRCWindow bytes, the
// only part a crash can have damaged — its shape and CRC check out.
func scanLog(read func(pos int64, n int) ([]byte, error), size, base, indexInterval int64) (validEnd, leo int64, want []indexEntry, since int64, err error) {
	leo = base
	crcFrom := size - recoveryCRCWindow
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
