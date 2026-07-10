package log

// recoverSegment scans a segment from its start, validating each batch, truncates
// a torn final batch — a write interrupted mid-append (an incomplete header or a
// batch whose body runs past EOF) — and returns the recovered log-end offset. The
// log is its own write-ahead log, so there is no separate WAL to replay.
func recoverSegment(s *Segment) (int64, error) {
	logEndOffset := s.baseOffset
	var pos int64
	for pos < s.sizeBytes {
		if s.sizeBytes-pos < batchHeaderMinBytes {
			// Not even a full header remains: a torn tail. Truncate it away.
			return logEndOffset, s.truncate(pos)
		}
		header, err := s.readAt(pos, batchHeaderMinBytes)
		if err != nil {
			return 0, err
		}
		info, ok := parseHeader(header)
		if !ok || pos+info.totalBytes > s.sizeBytes {
			// Corrupt header or an incomplete batch body: truncate to the last
			// intact batch boundary.
			return logEndOffset, s.truncate(pos)
		}
		logEndOffset = info.baseOffset + int64(info.recordCount)
		pos += info.totalBytes
	}
	return logEndOffset, nil
}
