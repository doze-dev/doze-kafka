package log

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
)

// Transactions, as the log sees them.
//
// A transactional producer writes its records as ordinary batches with the
// transactional bit set, and the coordinator later ends the transaction by appending
// a control batch (a "marker") with the producer's id and epoch to every partition it
// wrote to. The marker is a batch of one record whose key says commit or abort. The
// records between a transaction's first batch and its marker are the transaction.
//
// The partition keeps three things about them, all derived from the batches in the log
// (so nothing separate can disagree with it, and a restart just reads them back):
//
//   - the transactions still open: producer id → the offset of its first batch;
//   - the aborted ones: producer id, first offset and marker offset, which a
//     read-committed fetch hands the consumer so it can drop their records;
//   - from the first: the last stable offset (LSO), the lowest first offset of any open
//     transaction, or the log end when none is. A read-committed consumer is never
//     given anything at or above it.
//
// This is built lazily. A partition that has never held a transactional batch has no
// "transactions" file in its directory and needs no scan; creating that file is the
// first thing a transactional append does, so the scan on a later open knows to run.

const (
	batchTransactionalBit = 0x10 // attributes bit 4.

	controlAbort  int16 = 0
	controlCommit int16 = 1

	txnMarkerFile = "transactions"
)

// AbortedTxn is a transaction that was aborted: the producer, where its first record
// is, and where its abort marker is.
type AbortedTxn struct {
	ProducerID  int64
	FirstOffset int64
	LastOffset  int64 // the marker's offset.
}

type txnState struct {
	ready   bool
	ongoing map[int64]int64 // producer id → first offset.
	aborted []AbortedTxn
}

func attrsOf(b []byte) uint16 { return binary.BigEndian.Uint16(b[batchAttributesOff:]) }

// controlTypeOf reads the commit/abort type from a control batch's single record.
func controlTypeOf(batch []byte) (int16, bool) {
	body := batch[batchFullHeaderBytes:]
	_, n := binary.Varint(body) // record length.
	if n <= 0 || len(body) < n+1 {
		return 0, false
	}
	body = body[n+1:]        // attributes.
	for i := 0; i < 2; i++ { // timestamp delta, offset delta.
		_, m := binary.Varint(body)
		if m <= 0 {
			return 0, false
		}
		body = body[m:]
	}
	keyLen, m := binary.Varint(body)
	if m <= 0 || keyLen < 4 || int64(len(body)-m) < keyLen {
		return 0, false
	}
	key := body[m:]
	return int16(binary.BigEndian.Uint16(key[2:])), true // key: version, type.
}

// observe updates the state for a batch just appended (or scanned) at baseOffset.
func (t *txnState) observe(batch []byte, baseOffset int64) {
	attrs := attrsOf(batch)
	if attrs&batchTransactionalBit == 0 {
		return
	}
	pid := int64(binary.BigEndian.Uint64(batch[batchProducerIDOff:]))
	if attrs&batchControlBit != 0 {
		first, open := t.ongoing[pid]
		delete(t.ongoing, pid)
		if typ, ok := controlTypeOf(batch); ok && open && typ == controlAbort {
			t.aborted = append(t.aborted, AbortedTxn{ProducerID: pid, FirstOffset: first, LastOffset: baseOffset})
		}
		return
	}
	if _, open := t.ongoing[pid]; !open {
		t.ongoing[pid] = baseOffset
	}
}

// ensureTxn makes the transaction state valid, scanning the log's batch headers if the
// partition has ever held a transactional batch. Callers hold p.mu.
func (p *Partition) ensureTxn() error {
	if p.txn.ready {
		return nil
	}
	p.txn = txnState{ongoing: map[int64]int64{}}
	if hasTxnMarker(p.dir) {
		for _, s := range p.segments {
			for pos := int64(0); pos+batchFullHeaderBytes <= s.sizeBytes; {
				// A control batch's one record follows the header; a data batch's
				// header is all that is needed.
				head, err := s.readAt(pos, batchFullHeaderBytes+32)
				if err != nil {
					return err
				}
				info, ok := parseHeader(head)
				if !ok || pos+info.totalBytes > s.sizeBytes || len(head) < batchFullHeaderBytes {
					break
				}
				if attrsOf(head)&batchControlBit != 0 && int64(len(head)) < info.totalBytes {
					if head, err = s.readAt(pos, int(info.totalBytes)); err != nil {
						return err
					}
				}
				p.txn.observe(head, info.baseOffset)
				pos += info.totalBytes
			}
		}
		p.txn.dropBelow(p.logStartOffset)
	}
	p.txn.ready = true
	return nil
}

// dropBelow forgets aborted transactions whose marker is below the log start: no
// consumer can be handed their records any more.
func (t *txnState) dropBelow(start int64) {
	kept := t.aborted[:0]
	for _, a := range t.aborted {
		if a.LastOffset >= start {
			kept = append(kept, a)
		}
	}
	t.aborted = kept
}

// markTransactional records, durably, that this partition holds transactional
// batches, so that a later open knows to scan for them. It must be on disk before
// the first such batch is in the log: without the file a restart builds no
// transaction state, and a read-committed consumer is handed the records of
// open and aborted transactions as if they had committed.
//
// The file is empty: its name is the mark, so what has to reach the disk is the
// directory entry, and flushing the file itself would flush nothing. "On disk" is
// something this process has to have seen to: a file that is merely there may be
// left from an attempt whose directory flush failed, or from a process that was
// killed before it flushed, so the first transactional append after every open
// flushes the directory unless the partition was vouched for. The caller holds mu.
func (p *Partition) markTransactional() error {
	if p.txnMarked {
		return nil
	}
	f, created, err := fsOpenFile(filepath.Join(p.dir, txnMarkerFile), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if created {
		p.dirSeq++
	}
	if err := p.syncDirLocked(); err != nil {
		return err
	}
	p.txnMarked = true
	return nil
}

// hasTxnMarker reports whether the partition has ever held a transactional batch.
func hasTxnMarker(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, txnMarkerFile))
	return err == nil
}

// LastStableOffset is the offset below which every transaction has been decided: the
// first offset of the oldest open transaction, or the log end when none is open.
func (p *Partition) LastStableOffset() (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ensureTxn(); err != nil {
		return 0, err
	}
	return p.lsoLocked(), nil
}

func (p *Partition) lsoLocked() int64 {
	lso := p.logEndOffset
	for _, first := range p.txn.ongoing {
		lso = min(lso, first)
	}
	return max(lso, p.logStartOffset)
}

// OpenTransactions is producer id → first offset for every transaction open on the
// partition.
func (p *Partition) OpenTransactions() (map[int64]int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ensureTxn(); err != nil {
		return nil, err
	}
	out := make(map[int64]int64, len(p.txn.ongoing))
	for pid, first := range p.txn.ongoing {
		out[pid] = first
	}
	return out, nil
}

// abortedIn lists the aborted transactions that touch [from, to), by first offset.
func (p *Partition) abortedIn(from, to int64) []AbortedTxn {
	var out []AbortedTxn
	for _, a := range p.txn.aborted {
		if a.LastOffset >= from && a.FirstOffset < to {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FirstOffset < out[j].FirstOffset })
	return out
}

// ReadIsolated is Read under an isolation level. Read-uncommitted is Read, plus the
// last stable offset. Read-committed returns nothing at or above the last stable
// offset, and with it the aborted transactions that touch what it returned, which the
// consumer needs to drop their records.
func (p *Partition) ReadIsolated(fetchOffset, maxBytes int64, committed bool) (data []byte, lso int64, aborted []AbortedTxn, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ensureTxn(); err != nil {
		return nil, 0, nil, err
	}
	lso = p.lsoLocked()
	limit := int64(-1)
	if committed {
		limit = lso
	}
	data, err = p.readLocked(fetchOffset, maxBytes, limit)
	if err != nil || !committed || len(data) == 0 {
		return data, lso, nil, err
	}
	// The aborted transactions that touch the offsets actually returned.
	upTo := fetchOffset
	for pos := 0; pos < len(data); {
		info, ok := parseHeader(data[pos:])
		if !ok || pos+int(info.totalBytes) > len(data) {
			break
		}
		upTo = info.baseOffset + int64(info.recordCount)
		pos += int(info.totalBytes)
	}
	return data, lso, p.abortedIn(fetchOffset, upTo), nil
}

// AppendMarker ends a transaction on this partition: it appends a commit or abort
// control batch for the producer and returns its offset. If the producer has no open
// transaction here (it never wrote, or the marker is already there) nothing is written
// and wrote is false, which makes ending a transaction safe to repeat after a crash.
func (p *Partition) AppendMarker(pid int64, epoch int16, commit bool, coordinatorEpoch int32, nowMs int64) (offset int64, wrote bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ensureTxn(); err != nil {
		return 0, false, err
	}
	if _, open := p.txn.ongoing[pid]; !open {
		return -1, false, nil
	}
	offset, err = p.appendLocked(controlBatch(pid, epoch, commit, coordinatorEpoch, nowMs))
	return offset, err == nil, err
}

// controlBatch builds the commit or abort marker for a producer: a batch of one record
// whose key is (version 0, type) and whose value is (version 0, coordinator epoch), with
// the transactional and control bits set.
func controlBatch(pid int64, epoch int16, commit bool, coordinatorEpoch int32, nowMs int64) []byte {
	var put [binary.MaxVarintLen64]byte
	typ := controlAbort
	if commit {
		typ = controlCommit
	}
	var rec []byte
	rec = append(rec, 0) // attributes.
	rec = append(rec, put[:binary.PutVarint(put[:], 0)]...)
	rec = append(rec, put[:binary.PutVarint(put[:], 0)]...)
	rec = append(rec, put[:binary.PutVarint(put[:], 4)]...)
	rec = binary.BigEndian.AppendUint16(rec, 0)
	rec = binary.BigEndian.AppendUint16(rec, uint16(typ))
	rec = append(rec, put[:binary.PutVarint(put[:], 6)]...)
	rec = binary.BigEndian.AppendUint16(rec, 0)
	rec = binary.BigEndian.AppendUint32(rec, uint32(coordinatorEpoch))
	rec = append(rec, put[:binary.PutVarint(put[:], 0)]...) // no headers.
	body := append(append([]byte(nil), put[:binary.PutVarint(put[:], int64(len(rec)))]...), rec...)

	buf := make([]byte, batchFullHeaderBytes+len(body))
	binary.BigEndian.PutUint32(buf[batchLengthOff:], uint32(len(buf)-batchOverhead))
	binary.BigEndian.PutUint32(buf[12:], 0) // partition leader epoch.
	buf[batchMagicOff] = batchMagicV2
	binary.BigEndian.PutUint16(buf[batchAttributesOff:], batchTransactionalBit|batchControlBit)
	binary.BigEndian.PutUint32(buf[batchLastOffsetDeltaOff:], 0)
	binary.BigEndian.PutUint64(buf[batchFirstTimestampOff:], uint64(nowMs))
	binary.BigEndian.PutUint64(buf[batchMaxTimestampOff:], uint64(nowMs))
	binary.BigEndian.PutUint64(buf[batchProducerIDOff:], uint64(pid))
	binary.BigEndian.PutUint16(buf[batchProducerEpochOff:], uint16(epoch))
	binary.BigEndian.PutUint32(buf[batchBaseSequenceOff:], 0xFFFFFFFF) // -1: markers carry no sequence.
	binary.BigEndian.PutUint32(buf[batchRecordCountOff:], 1)
	copy(buf[batchFullHeaderBytes:], body)
	sealCRCOf(buf)
	return buf
}

func sealCRCOf(b []byte) {
	binary.BigEndian.PutUint32(b[batchCRCOff:], crcOf(b))
}
