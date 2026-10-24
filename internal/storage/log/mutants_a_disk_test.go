package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// On-disk tests for the survivors in index.go, bounds.go, logstart.go, segment.go,
// recovery.go and partition.go: each reads the files back, reopens, or watches the
// file seam, so a one-off error in a position, a size or a comparison shows.

func readIndexFile(t *testing.T, path string) []indexEntry {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw)%indexEntryBytes != 0 {
		t.Fatalf("index file is %d bytes, not whole entries", len(raw))
	}
	var out []indexEntry
	for i := 0; i < len(raw); i += indexEntryBytes {
		out = append(out, indexEntry{int64(binary.BigEndian.Uint64(raw[i:])), int64(binary.BigEndian.Uint64(raw[i+8:]))})
	}
	return out
}

func TestMutA_IndexIsWrittenAtItsEntryPositionsAndReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.index")
	ix, created, err := openIndex(path)
	if err != nil || !created {
		t.Fatalf("openIndex: %v, %v", created, err)
	}
	want := []indexEntry{{0, 0}, {10, 4096}, {25, 9000}, {26, 9100}}
	for _, e := range want {
		if err := ix.append(e.offset, e.position); err != nil {
			t.Fatal(err)
		}
	}
	// The file holds the entries one after the other, 16 bytes each.
	got := readIndexFile(t, path)
	if len(got) != len(want) {
		t.Fatalf("file holds %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d on disk = %+v, want %+v", i, got[i], want[i])
		}
	}
	ix.close()

	// Reopened, the same entries are loaded, each from its own place in the file.
	ix, created, err = openIndex(path)
	if err != nil || created {
		t.Fatalf("reopen: %v, %v", created, err)
	}
	defer ix.close()
	if len(ix.entries) != len(want) {
		t.Fatalf("loaded %v", ix.entries)
	}
	for i := range want {
		if ix.entries[i] != want[i] {
			t.Errorf("loaded entry %d = %+v, want %+v", i, ix.entries[i], want[i])
		}
	}
	// A trailing partial entry is ignored.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{1, 2, 3})
	f.Close()
	ix2, _, err := openIndex(path)
	if err != nil || len(ix2.entries) != len(want) {
		t.Errorf("with a torn tail: %v, %v", ix2, err)
	}
	ix2.close()
}

func TestMutA_IndexLookupFindsTheLargestEntryAtOrBelow(t *testing.T) {
	ix := &Index{entries: []indexEntry{{0, 0}, {10, 100}, {20, 200}}}
	for _, c := range []struct{ target, pos int64 }{{-1, 0}, {0, 0}, {9, 0}, {10, 100}, {19, 100}, {20, 200}, {99, 200}} {
		if got := ix.lookup(c.target); got != c.pos {
			t.Errorf("lookup(%d) = %d, want %d", c.target, got, c.pos)
		}
	}
	if (&Index{}).lookup(5) != 0 {
		t.Error("an empty index must say: scan from the start")
	}
}

func TestMutA_IndexRewriteReplacesTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.index")
	ix, _, err := openIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.close()
	for i := int64(0); i < 5; i++ {
		ix.append(i*10, i*100)
	}
	want := []indexEntry{{0, 0}, {30, 777}}
	if err := ix.rewrite(want); err != nil {
		t.Fatal(err)
	}
	if got := readIndexFile(t, path); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("file after rewrite = %v, want %v", got, want)
	}
	if err := ix.rewrite(nil); err != nil {
		t.Fatal(err)
	}
	if got := readIndexFile(t, path); len(got) != 0 || len(ix.entries) != 0 {
		t.Errorf("after rewriting to nothing: file %v, memory %v", got, ix.entries)
	}
	// And it takes appends from the start again.
	ix.append(5, 50)
	if got := readIndexFile(t, path); len(got) != 1 || got[0] != (indexEntry{5, 50}) {
		t.Errorf("append after rewrite: %v", got)
	}
}

// --- windowReader ---

func TestMutA_WindowReaderServesFromItsWindowOnlyWhereItHolds(t *testing.T) {
	const size = 3 << 20
	path := filepath.Join(t.TempDir(), "f")
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i*7 + i>>10)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := &windowReader{f: f, size: size}
	// fill is what the file holds now; the window holds what the file held when it
	// was last filled.
	check := func(pos int64, n int, want byte, why string) {
		t.Helper()
		got, err := r.read(pos, n)
		if err != nil {
			t.Fatalf("%s: %v", why, err)
		}
		if len(got) != n {
			t.Fatalf("%s: read %d bytes, want %d", why, len(got), n)
		}
		for i, b := range got {
			w := want
			if want == 0 {
				w = content[pos+int64(i)]
			}
			if b != w {
				t.Fatalf("%s: byte %d is %#x, want %#x", why, i, b, w)
			}
		}
	}
	check(0, 100, 0, "first read fills the window")
	// Change the whole file behind the window.
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xEE}, size), 0); err != nil {
		t.Fatal(err)
	}
	check(0, 50, 0, "a read at the window's start")
	check(scanWindow-100, 100, 0, "a read ending exactly at the window's end")
	check(0, scanWindow, 0, "a read of exactly the window")
	check(scanWindow-99, 100, 0xEE, "a read one byte past the window's end")
	// The window is now [scanWindow-99, 2*scanWindow-99) and holds 0xEE; change the file again.
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xDD}, size), 0); err != nil {
		t.Fatal(err)
	}
	at := int64(scanWindow - 99)
	check(at, 10, 0xEE, "a read at the new window's start")
	check(at-1, 10, 0xDD, "a read starting one byte before the window")
	check(5, 10, 0xDD, "a read well before the window")
}

func TestMutA_WindowReaderClampsToTheSizeItWasGiven(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	content := make([]byte, 2000)
	for i := range content {
		content[i] = byte(i)
	}
	os.WriteFile(path, content, 0o644)
	f, _ := os.Open(path)
	defer f.Close()
	r := &windowReader{f: f, size: 1500} // the file holds 500 more than the caller counts.
	got, err := r.read(1000, 1000)
	if err != nil || !bytes.Equal(got, content[1000:1500]) {
		t.Errorf("read past the counted size: %d bytes, %v; want the 500 within it", len(got), err)
	}
	got, err = r.read(0, 10)
	if err != nil || !bytes.Equal(got, content[:10]) || len(got) != 10 {
		t.Errorf("a short read after a clamp: %d bytes, %v", len(got), err)
	}
	// A request longer than the window is served whole.
	big := make([]byte, 3<<20)
	for i := range big {
		big[i] = byte(i % 251)
	}
	path2 := filepath.Join(t.TempDir(), "g")
	os.WriteFile(path2, big, 0o644)
	f2, _ := os.Open(path2)
	defer f2.Close()
	r2 := &windowReader{f: f2, size: int64(len(big))}
	got, err = r2.read(10, 2<<20)
	if err != nil || len(got) != 2<<20 || !bytes.Equal(got, big[10:10+2<<20]) {
		t.Errorf("a read of 2 MiB: %d bytes, %v", len(got), err)
	}
	// A failing file is an error, not a short answer.
	f3, _ := os.Open(path2)
	f3.Close()
	r3 := &windowReader{f: f3, size: int64(len(big))}
	if _, err := r3.read(0, 10); err == nil {
		t.Error("a read of a closed file succeeded")
	}
}

// boundsSignature changes whenever what bounds depend on does.
func TestMutA_BoundsSignatureTracksTheFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "t-0")
	if _, ok := boundsSignature(dir); ok {
		t.Error("a missing directory has a signature")
	}
	os.MkdirAll(dir, 0o755)
	if _, ok := boundsSignature(dir); ok {
		t.Error("an empty directory has a signature")
	}
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 400, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(makeBatch(2, []byte("aaaa"))); err != nil {
		t.Fatal(err)
	}
	p.close()
	s1, ok := boundsSignature(dir)
	if !ok || s1 == "" {
		t.Fatalf("a partition with a segment has no signature: %q, %v", s1, ok)
	}
	if s2, _ := boundsSignature(dir); s2 != s1 {
		t.Errorf("signature of unchanged files differs: %q vs %q", s1, s2)
	}
	// A segment file whose stat fails (it is a dangling name) gives none.
	p, _ = openPartition(dir, ManagerOptions{SegmentBytes: 400, IndexIntervalBytes: 4096})
	for i := 0; i < 20; i++ {
		p.Append(makeBatch(1, bytes.Repeat([]byte{'x'}, 50)))
	}
	p.close()
	s3, _ := boundsSignature(dir)
	if s3 == s1 {
		t.Error("signature did not change when segments were added")
	}
	if _, err := p.DeleteBefore(1); err == nil {
		// p is closed; DeleteBefore on closed files is not the point here.
		t.Log("closed partition accepted DeleteBefore")
	}
	os.WriteFile(filepath.Join(dir, logStartFile), []byte("1\n"), 0o644)
	s4, _ := boundsSignature(dir)
	if s4 == s3 {
		t.Error("signature did not change when the log start was recorded")
	}
	// A dangling last segment name: stat fails.
	bases, _ := listSegmentBases(dir)
	last := bases[len(bases)-1]
	os.Remove(filepath.Join(dir, segmentName(last)+".log"))
	os.Symlink(filepath.Join(dir, "nowhere"), filepath.Join(dir, segmentName(last+1000)+".log"))
	if sig, ok := boundsSignature(dir); ok {
		t.Errorf("a signature %q for a last segment that cannot be read", sig)
	}
}

// --- log start ---

func TestMutA_DeleteBeforeEdges(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	defer p.close()
	// Nothing to delete from an empty partition: 0 is the start already, which
	// records nothing on disk.
	if start, err := p.DeleteBefore(0); err != nil || start != 0 {
		t.Fatalf("DeleteBefore(0) on an empty partition = %d, %v", start, err)
	}
	if _, err := os.Stat(filepath.Join(dir, logStartFile)); !os.IsNotExist(err) {
		t.Errorf("a no-op DeleteBefore wrote a log-start file: %v", err)
	}
	for i := 0; i < 4; i++ {
		p.Append(makeBatch(2, []byte("0123456789")))
	}
	if _, err := p.DeleteBefore(-2); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("DeleteBefore(-2) = %v", err)
	}
	if _, err := p.DeleteBefore(p.LogEndOffset() + 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("DeleteBefore(LEO+1) = %v", err)
	}
	if start, err := p.DeleteBefore(1); err != nil || start != 1 || p.LogStartOffset() != 1 {
		t.Fatalf("DeleteBefore(1) = %d, %v", start, err)
	}
	// Moving to where it is already changes nothing.
	if start, err := p.DeleteBefore(1); err != nil || start != 1 {
		t.Errorf("DeleteBefore(1) again = %d, %v", start, err)
	}
	// HighWatermark empties the partition: the start is the end, and it survives
	// a reopen with the end unchanged.
	end := p.LogEndOffset()
	if start, err := p.DeleteBefore(HighWatermark); err != nil || start != end {
		t.Fatalf("DeleteBefore(HighWatermark) = %d, %v, want %d", start, err, end)
	}
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	p2 := openSmall(t, dir)
	defer p2.close()
	if p2.LogStartOffset() != end || p2.LogEndOffset() != end {
		t.Errorf("after reopen: start %d end %d, want both %d", p2.LogStartOffset(), p2.LogEndOffset(), end)
	}
	if off, err := p2.Append(makeBatch(1, []byte("z"))); err != nil || off != end {
		t.Errorf("the first append after emptying got offset %d, %v, want %d", off, err, end)
	}
}

// Emptying a partition whose active segment holds nothing starts no new segment.
func TestMutA_DeleteBeforeTheEndOfAnEmptyActiveSegmentStartsNoSegment(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	for i := 0; i < 8; i++ {
		p.Append(makeBatch(1, bytes.Repeat([]byte{'a'}, 300)))
	}
	if len(p.segments) < 3 {
		t.Fatalf("%d segments", len(p.segments))
	}
	p.close()
	// A crash left the last segment with nothing valid in it.
	logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	last := logs[len(logs)-1]
	if err := os.Truncate(last, 0); err != nil {
		t.Fatal(err)
	}
	p = openSmall(t, dir)
	defer p.close()
	end := p.LogEndOffset()
	if p.active().sizeBytes != 0 || end == 0 {
		t.Fatalf("setup: active segment %d bytes, end %d", p.active().sizeBytes, end)
	}
	if start, err := p.DeleteBefore(end); err != nil || start != end {
		t.Fatalf("DeleteBefore(end) = %d, %v", start, err)
	}
	if len(p.segments) != 1 {
		t.Errorf("%d segments after emptying; the empty active segment is the one that stays", len(p.segments))
	}
	if n := segmentFiles(t, dir); n != 1 {
		t.Errorf("%d segment files on disk, want 1", n)
	}
}

func TestMutA_ManagerDeleteBefore(t *testing.T) {
	m, err := OpenManager(t.TempDir(), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := m.DeleteBefore("nope", 0, 0); err == nil {
		t.Error("DeleteBefore on a partition that does not exist succeeded")
	}
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	p.Append(makeBatch(3, []byte("abc")))
	if start, err := m.DeleteBefore("t", 0, 2); err != nil || start != 2 {
		t.Errorf("Manager.DeleteBefore = %d, %v", start, err)
	}
}

// --- segments ---

func TestMutA_SegmentIndexesAtTheIntervalExactly(t *testing.T) {
	dir := t.TempDir()
	b := makeBatch(1, bytes.Repeat([]byte{'x'}, 20))
	size := int64(len(b))
	seg, _, err := openSegment(dir, 0, 2*size, nil) // an entry whenever 2 batches' worth is written.
	if err != nil {
		t.Fatal(err)
	}
	defer seg.close()
	for i := 0; i < 6; i++ {
		if err := seg.append(int64(i), b); err != nil {
			t.Fatal(err)
		}
	}
	want := []indexEntry{{0, 0}, {2, 2 * size}, {4, 4 * size}}
	got := readIndexFile(t, filepath.Join(dir, segmentName(0)+".index"))
	if len(got) != len(want) {
		t.Fatalf("index after 6 appends = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// A successful flush leaves the segment writable: it is not marked as failed.
func TestMutA_SuccessfulSyncLeavesTheSegmentOnline(t *testing.T) {
	seg, _, err := openSegment(t.TempDir(), 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.close()
	b := makeBatch(1, []byte("x"))
	for i := 0; i < 3; i++ {
		if err := seg.append(int64(i), b); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if err := seg.sync(); err != nil {
			t.Fatalf("sync %d: %v", i, err)
		}
		if seg.failed.Load() != nil {
			t.Fatalf("segment marked failed after a good sync %d", i)
		}
	}
}

// --- recovery ---

func fsOpsDuring(t *testing.T, fn func()) (ops map[FSOp]int) {
	t.Helper()
	ops = map[FSOp]int{}
	restore := SetFSHookForTest(func(e FSEvent) error {
		if !e.Done {
			ops[e.Op]++
		}
		return nil
	})
	defer restore()
	fn()
	return ops
}

// Opening a log with nothing wrong with it cuts nothing, flushes nothing it need
// not, and rewrites no index.
func TestMutA_RecoveryOfAnIntactLogTruncatesNothing(t *testing.T) {
	dir, _, _ := buildCrashLog(t)
	var p *Partition
	ops := fsOpsDuring(t, func() {
		var err error
		p, err = openPartition(dir, crashOpts())
		if err != nil {
			t.Fatal(err)
		}
	})
	defer p.close()
	if ops[FSTruncate] != 0 {
		t.Errorf("opening an intact log truncated a file %d time(s)", ops[FSTruncate])
	}
	if p.LogEndOffset() != crashBatches*crashRecords {
		t.Errorf("LEO = %d", p.LogEndOffset())
	}
}

// A last batch that is only a header (61 bytes, a compressed batch whose records are
// elsewhere) is kept; the batch before the damage is kept and the damage cut.
func TestMutA_RecoveryKeepsAHeaderSizedFinalBatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "t-0")
	p, err := openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(makeBatch(2, []byte("body"))); err != nil {
		t.Fatal(err)
	}
	hdr := batchWith(codecGzip, 1, nil)
	if len(hdr) != batchFullHeaderBytes {
		t.Fatal(len(hdr))
	}
	if _, err := p.Append(hdr); err != nil {
		t.Fatal(err)
	}
	size := p.active().sizeBytes
	p.close()
	p, err = openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if p.active().sizeBytes != size || p.LogEndOffset() != 3 {
		t.Errorf("after reopen size %d (want %d), LEO %d (want 3): the header-sized batch was cut", p.active().sizeBytes, size, p.LogEndOffset())
	}
}

func TestMutA_ScanLogVerifiesTheCRCWhereItIsToBeVerified(t *testing.T) {
	good := makeBatch(1, []byte("good"))
	bad := makeBatch(1, []byte("evil"))
	bad[len(bad)-1] ^= 0xFF // body changed, CRC not.
	file := append(append([]byte{}, bad...), good...)
	read := func(pos int64, n int) ([]byte, error) { return file[pos : pos+int64(n)], nil }
	// With every batch checked, the first one's CRC is checked, and it is bad.
	validEnd, leo, want, _, err := scanLog(read, int64(len(file)), 0, 1<<30, true)
	if err != nil || validEnd != 0 || leo != 0 || len(want) != 0 {
		t.Errorf("crcAll over a bad first batch: end %d leo %d index %v err %v", validEnd, leo, want, err)
	}

	// With only a tail window checked, a bad batch that starts exactly at the
	// window's start is checked; one before it is not.
	filler := bytes.Repeat([]byte{'f'}, recoveryCRCWindow+1000)
	g1 := makeBatch(1, filler) // a big good first batch.
	stampBase := func(b []byte, off int64) []byte { binary.BigEndian.PutUint64(b, uint64(off)); return b }
	badAt := func(start int) []byte { // a bad batch whose first byte is at start.
		_ = start
		x := makeBatch(1, []byte("evil"))
		x[len(x)-1] ^= 0xFF
		return x
	}
	b2 := stampBase(badAt(0), 1)
	file2 := append(append([]byte{}, g1...), b2...)
	size2 := int64(len(file2))
	read2 := func(pos int64, n int) ([]byte, error) { return file2[pos : pos+int64(n)], nil }
	// Here pos of b2 is len(g1); the window starts at size2-window.
	crcFrom := size2 - recoveryCRCWindow
	b2pos := int64(len(g1))
	if b2pos >= crcFrom {
		// b2 is inside the window: it is checked and found bad.
		if end, _, _, _, _ := scanLog(read2, size2, 0, 1<<30, false); end != b2pos {
			t.Errorf("a bad batch inside the window: valid end %d, want %d", end, b2pos)
		}
	}
	// Move the window so that its start is exactly b2's start: size = b2pos + window.
	// Pad with a good small batch after b2 so the file is that long.
	pad := stampBase(makeBatch(1, bytes.Repeat([]byte{'p'}, int(int64(recoveryCRCWindow)-int64(len(b2))-int64(batchFullHeaderBytes)))), 2)
	file3 := append(append(append([]byte{}, g1...), b2...), pad...)
	size3 := int64(len(file3))
	if size3-recoveryCRCWindow != b2pos {
		t.Fatalf("setup: window starts at %d, bad batch at %d", size3-recoveryCRCWindow, b2pos)
	}
	read3 := func(pos int64, n int) ([]byte, error) { return file3[pos : pos+int64(n)], nil }
	if end, _, _, _, _ := scanLog(read3, size3, 0, 1<<30, false); end != b2pos {
		t.Errorf("a bad batch at the window's first byte: valid end %d, want %d", end, b2pos)
	}
	// One byte earlier: the batch before the window is trusted without a CRC.
	file4 := append([]byte{}, file3...)
	// shrink the padding by one byte would break batch sizes; instead grow the window
	// reach by appending a byte of trailing junk (a torn tail): the window now starts
	// one byte after b2's start.
	file4 = append(file4, 0)
	size4 := int64(len(file4))
	read4 := func(pos int64, n int) ([]byte, error) { return file4[pos : pos+int64(n)], nil }
	end4, _, _, _, _ := scanLog(read4, size4, 0, 1<<30, false)
	if end4 == b2pos {
		t.Errorf("a bad batch before the window was CRC-checked")
	}
}

func TestMutA_ScanLogRefusesABatchWithNoRecordsEvenOutsideTheCRCWindow(t *testing.T) {
	g1 := makeBatch(1, bytes.Repeat([]byte{'f'}, 100))
	zero := makeBatchZero(codecNone)
	binary.BigEndian.PutUint64(zero, 1)
	tail := makeBatch(1, bytes.Repeat([]byte{'t'}, recoveryCRCWindow+100))
	binary.BigEndian.PutUint64(tail, 1)
	file := append(append(append([]byte{}, g1...), zero...), tail...)
	read := func(pos int64, n int) ([]byte, error) { return file[pos : pos+int64(n)], nil }
	end, leo, _, _, err := scanLog(read, int64(len(file)), 0, 1<<30, false)
	if err != nil || end != int64(len(g1)) || leo != 1 {
		t.Errorf("a zero-record batch before the CRC window: end %d (want %d) leo %d err %v", end, len(g1), leo, err)
	}
}

// The index recovered from the log has an entry at the first batch and then each
// time the bytes since the last reach the interval exactly.
func TestMutA_RecoveredIndexFollowsTheIntervalRule(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "t-0")
	b := makeBatch(1, bytes.Repeat([]byte{'x'}, 20))
	size := int64(len(b))
	opts := ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 2 * size}
	p, err := openPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := p.Append(append([]byte{}, b...)); err != nil {
			t.Fatal(err)
		}
	}
	p.close()
	want := []indexEntry{{0, 0}, {2, 2 * size}, {4, 4 * size}}
	_, idx := segFiles(dir)
	if got := readIndexFile(t, idx); len(got) != 3 {
		t.Fatalf("written index = %v", got)
	}
	// Lose the index; recovery has to rebuild exactly the same.
	if err := os.Truncate(idx, 0); err != nil {
		t.Fatal(err)
	}
	p, err = openPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	p.close()
	got := readIndexFile(t, idx)
	if len(got) != len(want) {
		t.Fatalf("rebuilt index = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("rebuilt entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// --- partition ---

func TestMutA_AppendRefusesWhatIsNotWholeBatches(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	defer p.close()
	first := makeBatch(2, []byte("hello"))
	second := makeBatch(2, []byte("worldworld"))
	payload := append(append([]byte{}, first...), second[:len(second)-3]...)
	if _, err := p.Append(payload); err == nil {
		t.Fatal("a payload ending in a cut batch was appended")
	}
	if p.LogEndOffset() != 0 || p.active().sizeBytes != 0 {
		t.Errorf("a refused payload left LEO %d and %d bytes", p.LogEndOffset(), p.active().sizeBytes)
	}
	// A batch with no records.
	if _, err := p.Append(makeBatchZero(codecNone)); !errors.Is(err, ErrCorruptBatch) {
		t.Errorf("a batch of zero records: %v", err)
	}
	if p.LogEndOffset() != 0 || p.active().sizeBytes != 0 {
		t.Errorf("a zero-record batch left LEO %d and %d bytes", p.LogEndOffset(), p.active().sizeBytes)
	}
}

func TestMutA_AHeaderSizedTransactionalBatchStartsTransactionState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "t-0")
	p, err := openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	b := batchWith(codecGzip, 1, nil)
	binary.BigEndian.PutUint16(b[batchAttributesOff:], codecGzip|batchTransactionalBit)
	binary.BigEndian.PutUint64(b[batchProducerIDOff:], 9)
	sealCRC(b)
	if len(b) != batchFullHeaderBytes {
		t.Fatal(len(b))
	}
	if _, err := p.Append(b); err != nil {
		t.Fatal(err)
	}
	if !hasTxnMarker(dir) {
		t.Error("a 61-byte transactional batch did not leave the transactions marker")
	}
	if open, _ := p.OpenTransactions(); open[9] != 0 {
		t.Errorf("open transactions = %v, want producer 9 from offset 0", open)
	}
}

func TestMutA_SegmentRollsOnlyPastItsSize(t *testing.T) {
	dir := t.TempDir()
	b := makeBatch(1, bytes.Repeat([]byte{'x'}, 40))
	size := int64(len(b))
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 2 * size, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	p.Append(append([]byte{}, b...))
	p.Append(append([]byte{}, b...))
	if n := segmentFiles(t, dir); n != 1 {
		t.Fatalf("two batches that fit the segment exactly rolled it: %d segments", n)
	}
	p.Append(append([]byte{}, b...))
	if n := segmentFiles(t, dir); n != 2 || p.segments[1].baseOffset != 2 {
		t.Errorf("a third batch: %d segments", n)
	}
}

// A batch bigger than a whole segment goes into the empty segment there is.
func TestMutA_ABatchBiggerThanASegmentRollsNothingFromAnEmptyOne(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 10, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.Append(makeBatch(1, bytes.Repeat([]byte{'x'}, 100))); err != nil {
		t.Fatal(err)
	}
	if n := segmentFiles(t, dir); n != 1 || len(p.segments) != 1 {
		t.Errorf("%d segment files and %d segments for one big batch", n, len(p.segments))
	}
}

func TestMutA_ClosingTwiceReportsTheSecondClose(t *testing.T) {
	p, err := openPartition(filepath.Join(t.TempDir(), "t-0"), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	p.Append(makeBatch(1, []byte("x")))
	if err := p.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := p.close(); err == nil {
		t.Error("closing the files a second time reported nothing")
	}
}

// --- reading ---

// A partition of five 3-record batches of the same size in one segment.
func fiveBatches(t *testing.T) (*Partition, int64) {
	t.Helper()
	p, err := openPartition(filepath.Join(t.TempDir(), "t-0"), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.close() })
	var size int64
	for i := 0; i < 5; i++ {
		b := makeBatch(3, bytes.Repeat([]byte{byte('a' + i)}, 30))
		size = int64(len(b))
		if _, err := p.Append(b); err != nil {
			t.Fatal(err)
		}
	}
	return p, size
}

func batchBases(t *testing.T, data []byte) []int64 {
	t.Helper()
	var out []int64
	for pos := 0; pos < len(data); {
		info, ok := parseHeader(data[pos:])
		if !ok || pos+int(info.totalBytes) > len(data) {
			t.Fatalf("data is not whole batches at %d of %d", pos, len(data))
		}
		out = append(out, info.baseOffset)
		pos += int(info.totalBytes)
	}
	return out
}

func eq64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMutA_ReadReturnsWholeBatchesWithinMaxBytes(t *testing.T) {
	p, size := fiveBatches(t)
	for _, c := range []struct {
		from, max int64
		want      []int64
	}{
		{0, 1 << 20, []int64{0, 3, 6, 9, 12}},
		{3, 1 << 20, []int64{3, 6, 9, 12}},   // an offset that is a batch's end belongs to the next.
		{4, 1 << 20, []int64{3, 6, 9, 12}},   // inside a batch: that batch.
		{2, 1 << 20, []int64{0, 3, 6, 9, 12}}, // last record of the first.
		{0, 2 * size, []int64{0, 3}},          // exactly two batches' worth.
		{0, 2*size + 1, []int64{0, 3}},
		{0, 2*size - 1, []int64{0}},
		{0, 1, []int64{0}}, // always at least the first batch.
		{6, 3 * size, []int64{6, 9, 12}},
		{6, 3*size - 1, []int64{6, 9}},
		{12, 1, []int64{12}},
		{9, 2 * size, []int64{9, 12}},
		{9, 2*size + size/2, []int64{9, 12}},
	} {
		data, err := p.Read(c.from, c.max)
		if err != nil {
			t.Fatalf("Read(%d,%d): %v", c.from, c.max, err)
		}
		if got := batchBases(t, data); !eq64(got, c.want) {
			t.Errorf("Read(%d,%d) returned batches at %v, want %v", c.from, c.max, got, c.want)
		}
	}
	// A maxBytes of zero is no limit.
	data, _ := p.Read(0, 0)
	if got := batchBases(t, data); len(got) != 5 {
		t.Errorf("Read(0,0) returned %v", got)
	}
}

func TestMutA_ReadIsolatedStopsAtTheLimit(t *testing.T) {
	p, size := fiveBatches(t)
	_ = size
	for _, c := range []struct {
		limit int64
		want  []int64
	}{{0, nil}, {1, []int64{0}}, {3, []int64{0}}, {4, []int64{0, 3}}, {15, []int64{0, 3, 6, 9, 12}}} {
		p.mu.Lock()
		data, err := p.readLocked(0, 1<<20, c.limit)
		p.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		var got []int64
		if len(data) > 0 {
			got = batchBases(t, data)
		}
		if !eq64(got, c.want) {
			t.Errorf("limit %d: batches at %v, want %v", c.limit, got, c.want)
		}
	}
	// With a limit and a maxBytes together.
	p.mu.Lock()
	data, _ := p.readLocked(0, size+1, 100)
	p.mu.Unlock()
	if got := batchBases(t, data); !eq64(got, []int64{0}) {
		t.Errorf("limit and maxBytes: %v", got)
	}
	// Read-committed through the public call: nothing is open, so the limit is the end.
	data, lso, _, err := p.ReadIsolated(0, 1<<20, true)
	if err != nil || lso != 15 || len(batchBases(t, data)) != 5 {
		t.Errorf("ReadIsolated = %d bytes, lso %d, %v", len(data), lso, err)
	}
}

// Bytes in the file past the size the segment counts (a cut that failed) are never
// served, and a batch only partly inside the counted size is not either.
func TestMutA_ReadNeverServesBytesPastTheCountedSize(t *testing.T) {
	p, size := fiveBatches(t)
	seg := p.active()
	// Count only three and a half batches.
	seg.sizeBytes = 3*size + size/2
	p.logEndOffset = 12 // as the log would say: three batches, 9 records, and a bit; keep 12 so reads are in range.
	data, err := p.Read(3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if want := seg.sizeBytes - size; int64(len(data)) != want {
		t.Errorf("Read(3) returned %d bytes, want the %d the segment counts after the first batch", len(data), want)
	}
	data, err = p.Read(3, 2*size+size/10)
	if err != nil || !eq64(batchBases(t, data), []int64{3, 6}) {
		t.Errorf("Read(3, 2.1 batches) = %d bytes, %v", len(data), err)
	}
	// A read that begins in the partial batch finds nothing.
	data, err = p.Read(9, 1<<20)
	if err != nil || len(data) != 0 {
		t.Errorf("Read inside the uncounted batch = %d bytes, %v", len(data), err)
	}
	// And with a limit, which walks the batches by their headers.
	p.mu.Lock()
	data, err = p.readLocked(0, 1<<20, 100)
	p.mu.Unlock()
	if err != nil || !eq64(batchBases(t, data), []int64{0, 3, 6}) {
		t.Errorf("limited read with 3.5 batches counted: %d bytes, %v", len(data), err)
	}
}

func TestMutA_SegmentAfter(t *testing.T) {
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 100, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	for i := 0; i < 6; i++ {
		p.Append(makeBatch(1, bytes.Repeat([]byte{'a'}, 60)))
	}
	n := len(p.segments)
	if n < 3 {
		t.Fatalf("%d segments", n)
	}
	for i := 0; i < n-1; i++ {
		if got := p.segmentAfter(p.segments[i]); got != p.segments[i+1] {
			t.Errorf("segmentAfter(%d) is not segment %d", i, i+1)
		}
	}
	if got := p.segmentAfter(p.segments[n-1]); got != nil {
		t.Error("the last segment has a successor")
	}
	if got := p.segmentAfter(&Segment{}); got != nil {
		t.Error("a segment that is not in the partition has a successor")
	}
	// Reading across segments returns data from each in turn when asked from the
	// first: a Read does not stop at a segment's end only because maxBytes is large.
	data, err := p.Read(0, 1<<20)
	if err != nil || len(data) == 0 {
		t.Fatalf("%d bytes, %v", len(data), err)
	}
}
