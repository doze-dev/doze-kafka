package log

import "math"

// ProducerBatch is one idempotent producer's batch as the log holds it: who wrote it,
// which sequence numbers it covers, and where it is.
type ProducerBatch struct {
	ProducerID int64
	Epoch      int16
	FirstSeq   int32
	LastSeq    int32
	BaseOffset int64
}

// ProducerHistory lists, oldest first, the idempotent batches in roughly the last
// maxBytes of the partition (whole segments, so never less than the active one). It is
// how the broker relearns each producer's recent sequence numbers after a restart, so
// that a batch whose acknowledgement was lost in a crash and is sent again is
// recognised as the one already written, instead of being appended a second time.
// Transaction markers carry no sequence and are skipped.
func (p *Partition) ProducerHistory(maxBytes int64) ([]ProducerBatch, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Start at the oldest segment that keeps the scan within maxBytes, but always
	// including the newest.
	first, size := len(p.segments)-1, int64(0)
	for i := len(p.segments) - 1; i >= 0; i-- {
		size += p.segments[i].sizeBytes
		first = i
		if size >= maxBytes {
			break
		}
	}
	var out []ProducerBatch
	for _, s := range p.segments[first:] {
		for pos := int64(0); pos+batchFullHeaderBytes <= s.sizeBytes; {
			head, err := s.readAt(pos, batchFullHeaderBytes)
			if err != nil {
				return nil, err
			}
			info, ok := parseHeader(head)
			if !ok || len(head) < batchFullHeaderBytes || pos+info.totalBytes > s.sizeBytes {
				break
			}
			h, ok := ParseBatch(head)
			if ok && h.ProducerID >= 0 && !h.Control && h.BaseSequence >= 0 && h.RecordCount > 0 {
				out = append(out, ProducerBatch{
					ProducerID: h.ProducerID, Epoch: h.ProducerEpoch, FirstSeq: h.BaseSequence,
					LastSeq:    int32((int64(h.BaseSequence) + int64(h.RecordCount) - 1) & math.MaxInt32),
					BaseOffset: info.baseOffset,
				})
			}
			pos += info.totalBytes
		}
	}
	return out, nil
}
