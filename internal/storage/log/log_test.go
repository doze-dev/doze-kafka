package log

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func testOpts() ManagerOptions {
	return ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096}
}

// makeBatch builds a minimal but well-formed record-batch-v2 blob: a valid header
// (correct batchLength and lastOffsetDelta) followed by opaque payload. The engine
// only reads the header, so the payload can be anything.
func makeBatch(recordCount int32, payload []byte) []byte {
	buf := make([]byte, batchHeaderMinBytes+len(payload))
	binary.BigEndian.PutUint32(buf[batchLengthOff:], uint32(len(buf)-batchOverhead))
	binary.BigEndian.PutUint32(buf[batchLastOffsetDeltaOff:], uint32(recordCount-1))
	copy(buf[batchHeaderMinBytes:], payload)
	return buf
}

func TestPartitionAppendReadRoundTrip(t *testing.T) {
	p, err := openPartition(t.TempDir(), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()

	b0 := makeBatch(3, []byte("hello"))
	b1 := makeBatch(2, []byte("world"))

	if off, err := p.Append(b0); err != nil || off != 0 {
		t.Fatalf("append b0: off=%d err=%v (want 0)", off, err)
	}
	if off, err := p.Append(b1); err != nil || off != 3 {
		t.Fatalf("append b1: off=%d err=%v (want 3)", off, err)
	}
	if leo := p.LogEndOffset(); leo != 5 {
		t.Fatalf("LEO=%d, want 5", leo)
	}

	// Read from 0 returns both batches, byte-for-byte (with stamped base offsets).
	got, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{}, b0...), b1...)
	if !bytes.Equal(got, want) {
		t.Fatalf("read(0): got %d bytes, want %d", len(got), len(want))
	}

	// Read from 3 returns only the second batch.
	got, err = p.Read(3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, b1) {
		t.Fatalf("read(3): got %d bytes, want %d", len(got), len(b1))
	}
}

func TestReopenRecoversLogEndOffset(t *testing.T) {
	dir := t.TempDir()

	p, err := openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	p.Append(makeBatch(3, []byte("aaa")))
	p.Append(makeBatch(2, []byte("bb")))
	if err := p.close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if leo := reopened.LogEndOffset(); leo != 5 {
		t.Fatalf("recovered LEO=%d, want 5", leo)
	}
	if got, err := reopened.Read(0, 1<<20); err != nil || len(got) == 0 {
		t.Fatalf("read after reopen: %d bytes, err=%v", len(got), err)
	}
}

func TestTornTailTruncatedOnRecovery(t *testing.T) {
	dir := t.TempDir()

	p, err := openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	first := makeBatch(3, []byte("intact"))
	p.Append(first)
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	intactSize := int64(len(first))

	// Simulate a crash mid-append: a partial batch (an incomplete header) at EOF.
	logPath := filepath.Join(dir, segmentName(0)+".log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42}) // baseOffset + batchLength, no body.
	f.Close()

	reopened, err := openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()

	if leo := reopened.LogEndOffset(); leo != 3 {
		t.Fatalf("LEO after torn-tail recovery=%d, want 3", leo)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != intactSize {
		t.Fatalf("log size after recovery=%d, want %d (torn tail should be truncated)", info.Size(), intactSize)
	}
}

// TestAppendKeepsHeapFlat proves the load-bearing memory invariant: appending far
// more data than would fit comfortably in memory does not grow the heap, because
// batches are written straight to disk and never retained (the log is served by
// pread, not buffered).
func TestAppendKeepsHeapFlat(t *testing.T) {
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 8 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()

	const (
		payloadSize = 64 << 10 // 64 KiB per batch.
		batches     = 2048     // ~128 MiB written to disk.
	)
	payload := make([]byte, payloadSize)

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	for range batches {
		if _, err := p.Append(makeBatch(1, payload)); err != nil {
			t.Fatal(err)
		}
	}

	var after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&after)

	grew := int64(after.HeapInuse) - int64(before.HeapInuse)
	const wroteMB = batches * payloadSize >> 20
	if grew > 24<<20 {
		t.Fatalf("heap grew by %d bytes after writing %d MiB to disk; the log must not be buffered in memory", grew, wroteMB)
	}
	if p.LogEndOffset() != batches {
		t.Fatalf("LEO=%d, want %d", p.LogEndOffset(), batches)
	}
}
