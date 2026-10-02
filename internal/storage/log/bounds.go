package log

import (
	"os"
	"path/filepath"
)

// boundsFromDisk reads a closed partition's log-start and log-end offsets from
// its files. The start is the first segment's base offset. The end is found by
// walking batch headers forward from the last index entry — at most one index
// interval of log — rather than from the start of the segment; if the index is
// missing or does not line up with the log, the walk starts from the beginning.
// A batch cut short by a torn write ends the walk, which is where recovery would
// also stop.
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
	end = last

	f, err := os.Open(filepath.Join(dir, segmentName(last)+".log"))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := info.Size()

	pos, off := int64(0), last
	if ix, err := openIndex(filepath.Join(dir, segmentName(last)+".index")); err == nil {
		if n := len(ix.entries); n > 0 {
			e := ix.entries[n-1]
			var h [batchHeaderMinBytes]byte
			if e.position+batchHeaderMinBytes <= size {
				if _, err := f.ReadAt(h[:], e.position); err == nil {
					if info, ok := parseHeader(h[:]); ok && info.baseOffset == e.offset {
						pos, off = e.position, e.offset
					}
				}
			}
		}
		ix.close()
	}
	var h [batchHeaderMinBytes]byte
	for pos+batchHeaderMinBytes <= size {
		if _, err := f.ReadAt(h[:], pos); err != nil {
			break
		}
		info, ok := parseHeader(h[:])
		if !ok || pos+info.totalBytes > size || info.recordCount <= 0 {
			break
		}
		off = info.baseOffset + int64(info.recordCount)
		pos += info.totalBytes
	}
	return start, off, nil
}
