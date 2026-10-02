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
	leo := s.baseOffset
	crcFrom := s.sizeBytes - recoveryCRCWindow

	var (
		pos   int64
		want  []indexEntry // the index this log should have, built as append would.
		since int64        // bytes since the last index entry.
	)
	for pos < s.sizeBytes {
		if s.sizeBytes-pos < batchFullHeaderBytes {
			if err := s.truncate(pos); err != nil {
				return 0, err
			}
			break
		}
		header, err := s.readAt(pos, batchFullHeaderBytes)
		if err != nil {
			return 0, err
		}
		info, ok := parseHeader(header)
		if !ok || header[batchMagicOff] != batchMagicV2 || pos+info.totalBytes > s.sizeBytes || info.recordCount <= 0 {
			if err := s.truncate(pos); err != nil {
				return 0, err
			}
			break
		}
		if pos >= crcFrom {
			batch, err := s.readAt(pos, int(info.totalBytes))
			if err != nil {
				return 0, err
			}
			if _, err := checkBatchShape(batch); err != nil || !crcOK(batch) {
				if err := s.truncate(pos); err != nil {
					return 0, err
				}
				break
			}
		}

		since += info.totalBytes
		if len(want) == 0 || since >= s.indexInterval {
			want = append(want, indexEntry{offset: info.baseOffset, position: pos})
			since = 0
		}
		leo = info.baseOffset + int64(info.recordCount)
		pos += info.totalBytes
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
