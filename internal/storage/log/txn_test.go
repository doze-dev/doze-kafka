package log

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// txBatch is a one-record batch from a transactional producer.
func txBatch(pid int64, epoch int16, seq int32) []byte {
	b := makeBatch(1, []byte("x"))
	binary.BigEndian.PutUint16(b[batchAttributesOff:], batchTransactionalBit)
	binary.BigEndian.PutUint64(b[batchProducerIDOff:], uint64(pid))
	binary.BigEndian.PutUint16(b[batchProducerEpochOff:], uint16(epoch))
	binary.BigEndian.PutUint32(b[batchBaseSequenceOff:], uint32(seq))
	sealCRC(b)
	return b
}

func mustMarker(t *testing.T, p *Partition, pid int64, commit bool) (int64, bool) {
	t.Helper()
	off, wrote, err := p.AppendMarker(pid, 0, commit, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return off, wrote
}

func lso(t *testing.T, p *Partition) int64 {
	t.Helper()
	v, err := p.LastStableOffset()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// The last stable offset is the first offset of the oldest open transaction, the log
// end when none is open, and moves as transactions are decided.
func TestLastStableOffsetFollowsOpenTransactions(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()

	mustAppend(t, p, makeBatch(1, []byte("plain"))) // offset 0: not transactional.
	if got := lso(t, p); got != 1 {
		t.Fatalf("LSO with nothing open = %d, want the log end 1", got)
	}
	mustAppend(t, p, txBatch(7, 0, 0))               // offset 1: pid 7 opens.
	mustAppend(t, p, makeBatch(1, []byte("plain2"))) // offset 2.
	mustAppend(t, p, txBatch(9, 0, 0))               // offset 3: pid 9 opens.
	if got := lso(t, p); got != 1 {
		t.Errorf("LSO with 7 and 9 open = %d, want 1 (pid 7's first record)", got)
	}
	mustAppend(t, p, txBatch(7, 0, 1)) // offset 4: more of pid 7's.
	if got := lso(t, p); got != 1 {
		t.Errorf("a second batch of the same transaction moved the LSO to %d", got)
	}

	off, wrote := mustMarker(t, p, 7, true) // offset 5: commit 7.
	if !wrote || off != 5 {
		t.Fatalf("commit marker for 7 = %d, %v, want offset 5", off, wrote)
	}
	if got := lso(t, p); got != 3 {
		t.Errorf("after 7 committed the LSO = %d, want 3 (pid 9's first record)", got)
	}
	if _, wrote := mustMarker(t, p, 7, true); wrote {
		t.Error("a second marker was written for a transaction already decided")
	}
	off, _ = mustMarker(t, p, 9, false) // offset 6: abort 9.
	if got, end := lso(t, p), p.LogEndOffset(); got != end || end != 7 {
		t.Errorf("with everything decided the LSO = %d and the end %d, want 7 and 7 (off %d)", got, end, off)
	}
	if _, wrote := mustMarker(t, p, 123, true); wrote {
		t.Error("a marker was written for a producer that never wrote here")
	}
}

// Aborted transactions are listed for the offsets a consumer is given, by where they
// began, and only when they touch that range.
func TestAbortedTransactionsAreReportedForTheRangeRead(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()

	mustAppend(t, p, txBatch(1, 0, 0)) // 0: pid 1.
	mustAppend(t, p, txBatch(2, 0, 0)) // 1: pid 2.
	mustMarker(t, p, 1, false)         // 2: abort 1 (began at 0).
	mustMarker(t, p, 2, true)          // 3: commit 2.
	mustAppend(t, p, txBatch(3, 0, 0)) // 4: pid 3 still open.

	data, lsoNow, aborted, err := p.ReadIsolated(0, 1<<20, true)
	if err != nil {
		t.Fatal(err)
	}
	if lsoNow != 4 {
		t.Errorf("LSO = %d, want 4", lsoNow)
	}
	// Four batches (offsets 0-3) are below the LSO; pid 3's at 4 is not given.
	var n int
	for pos := 0; pos < len(data); {
		info, _ := parseHeader(data[pos:])
		n++
		pos += int(info.totalBytes)
	}
	if n != 4 {
		t.Errorf("a read-committed read returned %d batches, want the 4 below the LSO", n)
	}
	if len(aborted) != 1 || aborted[0] != (AbortedTxn{ProducerID: 1, FirstOffset: 0, LastOffset: 2}) {
		t.Errorf("aborted = %+v, want pid 1 from 0 to 2", aborted)
	}

	// Read from past the aborted transaction: it does not touch the range.
	if _, _, aborted, _ := p.ReadIsolated(3, 1<<20, true); len(aborted) != 0 {
		t.Errorf("an aborted transaction that ended before the range was reported: %+v", aborted)
	}
	// Read-uncommitted returns everything and no aborted list.
	data, _, aborted, _ = p.ReadIsolated(0, 1<<20, false)
	count := 0
	for pos := 0; pos < len(data); {
		info, _ := parseHeader(data[pos:])
		count++
		pos += int(info.totalBytes)
	}
	if count != 5 || len(aborted) != 0 {
		t.Errorf("read-uncommitted: %d batches and %d aborted, want 5 and none", count, len(aborted))
	}
	// At the LSO a read-committed fetch has nothing.
	if data, _, _, _ := p.ReadIsolated(4, 1<<20, true); len(data) != 0 {
		t.Errorf("a read-committed fetch at the LSO returned %d bytes", len(data))
	}
}

// The state is rebuilt from the log: a reopened partition has the same open and aborted
// transactions, and a partition that never held a transactional batch does not scan.
func TestTransactionStateIsRebuiltFromTheLog(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	mustAppend(t, p, txBatch(5, 0, 0))
	mustAppend(t, p, txBatch(6, 0, 0))
	mustMarker(t, p, 5, false)
	end := p.LogEndOffset()
	p.close()

	p = openSmall(t, dir)
	defer p.close()
	open, err := p.OpenTransactions()
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 1 || open[6] != 1 {
		t.Errorf("open transactions after reopening = %v, want pid 6 from offset 1", open)
	}
	if got := lso(t, p); got != 1 || p.LogEndOffset() != end {
		t.Errorf("LSO %d end %d after reopening", got, p.LogEndOffset())
	}
	// Only offset 0 is readable (pid 6 is open at 1), and the abort of pid 5 touches it.
	if _, _, aborted, _ := p.ReadIsolated(0, 1<<20, true); len(aborted) != 1 || aborted[0].ProducerID != 5 {
		t.Errorf("aborted after reopening = %+v, want pid 5's", aborted)
	}

	// A partition that has never held a transaction has no marker file and no state.
	plain := openSmall(t, t.TempDir())
	defer plain.close()
	mustAppend(t, plain, makeBatch(1, []byte("x")))
	if _, err := os.Stat(filepath.Join(plain.dir, txnMarkerFile)); err == nil {
		t.Error("a partition with only plain batches got a transactions file")
	}
	if got := lso(t, plain); got != 1 {
		t.Errorf("a plain partition's LSO = %d, want its end", got)
	}
	if _, err := os.Stat(filepath.Join(p.dir, txnMarkerFile)); err != nil {
		t.Errorf("the transactional partition has no transactions file: %v", err)
	}
}

// Retention and DeleteRecords forget what is below the start.
func TestAbortedTransactionsBelowTheStartAreForgotten(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	mustAppend(t, p, txBatch(1, 0, 0)) // 0
	mustMarker(t, p, 1, false)         // 1
	mustAppend(t, p, makeBatch(1, []byte("x")))
	if _, err := p.DeleteBefore(2); err != nil {
		t.Fatal(err)
	}
	if _, _, aborted, _ := p.ReadIsolated(2, 1<<20, true); len(aborted) != 0 {
		t.Errorf("an aborted transaction entirely below the start is still listed: %+v", aborted)
	}
}

// What the marker is: a commit or abort control batch the log can read back.
func TestControlBatchRoundTrips(t *testing.T) {
	for _, commit := range []bool{true, false} {
		b := controlBatch(42, 3, commit, 9, 5000)
		if _, err := checkBatchShape(b); err != nil {
			t.Fatalf("commit=%v: %v", commit, err)
		}
		if !crcOK(b) {
			t.Errorf("commit=%v: the checksum is wrong", commit)
		}
		h, ok := ParseBatch(b)
		if !ok || !h.Control || !h.Transactional || h.ProducerID != 42 || h.ProducerEpoch != 3 || h.RecordCount != 1 || h.BaseSequence != -1 {
			t.Errorf("commit=%v: header %+v", commit, h)
		}
		typ, ok := controlTypeOf(b)
		want := controlAbort
		if commit {
			want = controlCommit
		}
		if !ok || typ != want {
			t.Errorf("commit=%v: control type %d %v, want %d", commit, typ, ok, want)
		}
		// A client may not write one.
		if err := ValidateBatches(b); err == nil {
			t.Errorf("commit=%v: a control batch passed validation", commit)
		}
	}
}
