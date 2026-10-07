package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// boundsFromDisk reads a closed partition's log-start and log-end offsets from
// its files. The start is the first segment's base offset. The end comes from
// scanning the last segment with scanLog, the same walk recovery does, so the
// answer is exactly what opening the partition would report. An earlier version
// trusted the index and checked only a short header, and after a crash it
// reported an end offset above the one the opened partition then had.
func boundsFromDisk(dir string) (start, end int64, err error) {
	bases, err := listSegmentBases(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil // never written to.
		}
		return 0, 0, err
	}
	if len(bases) == 0 {
		return 0, 0, nil
	}
	start = bases[0]
	last := bases[len(bases)-1]

	f, err := os.Open(filepath.Join(dir, segmentName(last)+".log"))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	r := &windowReader{f: f, size: info.Size()}
	_, end, _, _, err = scanLog(r.read, info.Size(), last, 1<<62)
	if err != nil {
		return 0, 0, err
	}
	// DeleteRecords may have put the start inside the first segment.
	if recorded := min(readLogStart(dir), end); recorded > start {
		start = recorded
	}
	return start, end, nil
}

// boundsSignature identifies the state of a closed partition's files cheaply (a
// directory listing and one stat), so a computed answer can be reused until the
// files change. ok is false when there is nothing to identify.
func boundsSignature(dir string) (sig string, ok bool) {
	bases, err := listSegmentBases(dir)
	if err != nil || len(bases) == 0 {
		return "", false
	}
	last := bases[len(bases)-1]
	info, err := os.Stat(filepath.Join(dir, segmentName(last)+".log"))
	if err != nil {
		return "", false
	}
	return fmt.Sprintf("%d/%d/%d/%d/%d/%d", bases[0], last, len(bases), info.Size(), info.ModTime().UnixNano(), readLogStart(dir)), true
}

// windowReader serves the many small sequential reads of a scan from one larger
// read, instead of a syscall per batch header.
type windowReader struct {
	f    *os.File
	size int64
	buf  []byte
	at   int64 // file position of buf[0].
}

const scanWindow = 1 << 20

func (r *windowReader) read(pos int64, n int) ([]byte, error) {
	if pos >= r.at && pos+int64(n) <= r.at+int64(len(r.buf)) {
		return r.buf[pos-r.at : pos-r.at+int64(n)], nil
	}
	want := n
	if want < scanWindow {
		want = scanWindow
	}
	if pos+int64(want) > r.size {
		want = int(r.size - pos)
	}
	if cap(r.buf) < want {
		r.buf = make([]byte, want)
	}
	r.buf = r.buf[:want]
	m, err := r.f.ReadAt(r.buf, pos)
	if err != nil && err != io.EOF {
		return nil, err
	}
	r.buf, r.at = r.buf[:m], pos
	if m < n {
		n = m
	}
	return r.buf[:n], nil
}
