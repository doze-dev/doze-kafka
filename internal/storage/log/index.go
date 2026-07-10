package log

import (
	"encoding/binary"
	"os"
	"sort"
)

// indexEntryBytes is the fixed on-disk size of one sparse index entry:
// offset(int64) + position(int64).
const indexEntryBytes = 16

// Index is a sparse map from record offset to byte position within a segment's
// log file, with one entry roughly every IndexIntervalBytes of log. Entries are
// held in memory (the index is small — one entry per interval) and appended to a
// backing .index file, from which they are rebuilt on open.
//
// (mmap'ing the index is a possible future optimization; for now the in-memory
// slice keeps the implementation simple and the footprint is modest.)
type Index struct {
	file    *os.File
	entries []indexEntry
}

type indexEntry struct {
	offset   int64
	position int64
}

// openIndex opens (creating if needed) the index file and loads its entries.
func openIndex(path string) (*Index, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	ix := &Index{file: f}
	if err := ix.load(); err != nil {
		f.Close()
		return nil, err
	}
	return ix, nil
}

func (ix *Index) load() error {
	info, err := ix.file.Stat()
	if err != nil {
		return err
	}
	count := info.Size() / indexEntryBytes
	ix.entries = make([]indexEntry, 0, count)
	var buf [indexEntryBytes]byte
	for i := range count {
		if _, err := ix.file.ReadAt(buf[:], i*indexEntryBytes); err != nil {
			return err
		}
		ix.entries = append(ix.entries, indexEntry{
			offset:   int64(binary.BigEndian.Uint64(buf[0:])),
			position: int64(binary.BigEndian.Uint64(buf[8:])),
		})
	}
	return nil
}

// append records a new (offset -> position) entry, persisting it.
func (ix *Index) append(offset, position int64) error {
	var buf [indexEntryBytes]byte
	binary.BigEndian.PutUint64(buf[0:], uint64(offset))
	binary.BigEndian.PutUint64(buf[8:], uint64(position))
	if _, err := ix.file.WriteAt(buf[:], int64(len(ix.entries))*indexEntryBytes); err != nil {
		return err
	}
	ix.entries = append(ix.entries, indexEntry{offset, position})
	return nil
}

// lookup returns the byte position of the largest indexed offset ≤ target, or 0
// (scan from the segment start) if no entry qualifies.
func (ix *Index) lookup(target int64) int64 {
	i := sort.Search(len(ix.entries), func(i int) bool { return ix.entries[i].offset > target })
	if i == 0 {
		return 0
	}
	return ix.entries[i-1].position
}

// sync flushes appended index entries to stable storage.
func (ix *Index) sync() error { return ix.file.Sync() }

func (ix *Index) close() error { return ix.file.Close() }
