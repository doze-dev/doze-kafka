package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Log compaction.
//
// A topic with cleanup.policy=compact keeps, for each key, the latest record and drops
// the older ones. The cleaner works on the sealed segments only (never the active one),
// and only below the last stable offset, so an open transaction is never touched:
//
//  1. It decides there is something worth doing: sealed data that has not been cleaned
//     (the "dirty" part), enough of it by min.cleanable.dirty.ratio, old enough by
//     min.compaction.lag.ms, or old enough that max.compaction.lag.ms forces it.
//  2. It reads the dirty sealed segments, newest first, into a map of key hash to the
//     offset of that key's latest record, until the map reaches its size limit.
//  3. It rewrites every eligible segment, oldest first, keeping a record only if it is
//     the latest for its key (or has no key, or its key is outside the map). Records of
//     aborted transactions go. A commit or abort marker stays only while some record of
//     its transaction does. A tombstone (null value) that is the latest for its key stays
//     until its segment has been cleaned before and is older than delete.retention.ms.
//  4. Each rewritten segment is swapped in without a window in which the log is
//     unreadable (see swapSegment). Offsets never move: a rewritten batch keeps its base
//     offset and last offset delta, so records leave gaps, as on a real broker.
//
// A segment the cleaner empties is deleted, except the first, whose base offset is the
// log-start offset: removing it would move the start and send consumers to reset.

// ErrNoKey is returned for a record with no key offered to a compacted topic.
var ErrNoKey = errors.New("log: a compacted topic cannot accept a message without a key")

// RequireKeys checks that every record in a Produce payload that has passed
// ValidateBatches has a key, which a compacted topic needs: the key is what a record
// replaces. Kafka refuses the batch (INVALID_RECORD) and so does this.
func RequireKeys(records []byte) error {
	for pos := 0; pos < len(records); {
		info, ok := parseHeader(records[pos:])
		if !ok || info.totalBytes <= 0 || pos+int(info.totalBytes) > len(records) {
			return nil
		}
		batch := records[pos : pos+int(info.totalBytes)]
		pos += int(info.totalBytes)
		if len(batch) < batchFullHeaderBytes || attrsOf(batch)&batchControlBit != 0 {
			continue
		}
		recs, err := DecodeRecords(batch)
		if err != nil {
			return err
		}
		for _, r := range recs {
			if r.Key == nil {
				return ErrNoKey
			}
		}
	}
	return nil
}

// CompactionPolicy is a topic's compaction settings as the cleaner uses them.
type CompactionPolicy struct {
	Enabled bool
	// MinDirtyRatio is the share of the sealed log that must be uncleaned before a pass
	// runs. Kafka's default is 0.5; here it is 0, so a topic compacts as soon as a segment
	// has rolled.
	MinDirtyRatio float64
	// MinLag keeps a record from being compacted until its segment is this old.
	MinLag time.Duration
	// MaxLag, when set, runs a pass once the oldest uncleaned segment is this old,
	// whatever the dirty ratio.
	MaxLag time.Duration
	// DeleteRetention is how long a tombstone is kept once its segment has been cleaned.
	DeleteRetention time.Duration
	// Codec is what rewritten batches are compressed with (0: not at all).
	Codec int
	// MaxMapEntries bounds the key map; 0 means the default.
	MaxMapEntries int
}

const defaultMaxMapEntries = 1 << 20

// CleanResult is what one pass did.
type CleanResult struct {
	Ran               bool
	At                time.Time
	SegmentsRewritten int
	SegmentsRemoved   int
	RecordsRemoved    int64
	MarkersRemoved    int64
	BytesBefore       int64
	BytesAfter        int64
	// Complete is false when the key map filled before the end of the sealed log, so
	// more of it is left for the next pass.
	Complete bool
}

const (
	cleanedFile  = "cleaner-offset"
	cleanedExt   = ".cleaned"
	swapMarkExt  = ".swap"
	cleanerIndex = ".index"
)

func hashKey(k []byte) uint64 {
	h := fnv.New64a()
	h.Write(k)
	return h.Sum64()
}

// cleanedToLocked is the offset below which the log has been cleaned. Callers hold p.mu.
func (p *Partition) cleanedToLocked() int64 {
	if !p.cleanedLoaded {
		p.cleanedLoaded = true
		if raw, err := os.ReadFile(filepath.Join(p.dir, cleanedFile)); err == nil {
			if n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); err == nil && n >= 0 {
				p.cleanedTo = n
			}
		}
	}
	return p.cleanedTo
}

func (p *Partition) setCleanedTo(offset int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleanedTo, p.cleanedLoaded = offset, true
	tmp := filepath.Join(p.dir, cleanedFile+".tmp")
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(offset, 10)+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(p.dir, cleanedFile))
}

// LastClean is the most recent pass that did anything or looked.
func (p *Partition) LastClean() CleanResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastClean
}

// abortedSet answers "is this record of a transaction that aborted?" quickly.
type abortedSet map[int64][]AbortedTxn

func newAbortedSet(list []AbortedTxn) abortedSet {
	s := abortedSet{}
	for _, a := range list {
		s[a.ProducerID] = append(s[a.ProducerID], a)
	}
	return s
}

func (s abortedSet) covers(pid, offset int64) bool {
	for _, a := range s[pid] {
		if a.FirstOffset <= offset && offset <= a.LastOffset {
			return true
		}
	}
	return false
}

// batchesOf calls visit for each whole batch of a sealed segment, in order.
func batchesOf(s *Segment, visit func(pos int64, batch []byte, info batchInfo) error) error {
	for pos := int64(0); pos+batchFullHeaderBytes <= s.sizeBytes; {
		head, err := s.readAt(pos, batchFullHeaderBytes)
		if err != nil {
			return err
		}
		info, ok := parseHeader(head)
		if !ok || len(head) < batchFullHeaderBytes || pos+info.totalBytes > s.sizeBytes {
			return nil
		}
		batch, err := s.readAt(pos, int(info.totalBytes))
		if err != nil {
			return err
		}
		if err := visit(pos, batch, info); err != nil {
			return err
		}
		pos += info.totalBytes
	}
	return nil
}

// keysOf is the latest offset of each key within a segment, leaving out what can never
// be the surviving record: aborted records, markers, and records with no key.
func keysOf(s *Segment, aborted abortedSet) (map[uint64]int64, error) {
	out := map[uint64]int64{}
	err := batchesOf(s, func(_ int64, batch []byte, info batchInfo) error {
		attrs := attrsOf(batch)
		if attrs&batchControlBit != 0 {
			return nil
		}
		if attrs&batchTransactionalBit != 0 && aborted.covers(int64(binary.BigEndian.Uint64(batch[batchProducerIDOff:])), info.baseOffset) {
			return nil
		}
		recs, err := DecodeRecords(batch)
		if err != nil {
			return err
		}
		for _, r := range recs {
			if r.Key != nil {
				out[hashKey(r.Key)] = info.baseOffset + int64(r.OffsetDelta)
			}
		}
		return nil
	})
	return out, err
}

// Compact runs one cleaning pass on the partition, if one is worth running. It is safe
// to call at any time and from any one goroutine at a time per partition (a second
// concurrent call returns at once).
func (p *Partition) Compact(pol CompactionPolicy, now time.Time) (CleanResult, error) {
	if !pol.Enabled || !p.cleaning.TryLock() {
		return CleanResult{}, nil
	}
	defer p.cleaning.Unlock()

	p.mu.Lock()
	if err := p.ensureTxn(); err != nil {
		p.mu.Unlock()
		return CleanResult{}, err
	}
	if len(p.segments) < 2 {
		p.mu.Unlock()
		return CleanResult{}, nil
	}
	lso := p.lsoLocked()
	cleanedTo := p.cleanedToLocked()
	sealed := append([]*Segment(nil), p.segments[:len(p.segments)-1]...)
	activeBase := p.segments[len(p.segments)-1].baseOffset
	aborted := newAbortedSet(p.txn.aborted)
	p.mu.Unlock()

	endOf := func(i int) int64 {
		if i+1 < len(sealed) {
			return sealed[i+1].baseOffset
		}
		return activeBase
	}
	// The eligible part: from the start, up to the first segment that reaches into an open
	// transaction or is too young to touch.
	var elig []*Segment
	var dirty, clean int64
	forced := false
	for i, s := range sealed {
		if endOf(i) > lso {
			break
		}
		age := now.Sub(s.lastModified)
		old := pol.MaxLag > 0 && age >= pol.MaxLag
		if !old && age < pol.MinLag {
			break
		}
		elig = append(elig, s)
		if s.baseOffset >= cleanedTo {
			dirty += s.sizeBytes
			forced = forced || old
		} else {
			clean += s.sizeBytes
		}
	}
	if len(elig) == 0 || dirty == 0 {
		return CleanResult{}, nil
	}
	if ratio := float64(dirty) / float64(dirty+clean); ratio < pol.MinDirtyRatio && !forced {
		return CleanResult{}, nil
	}

	// The key map, from the oldest uncleaned segment forward, until it is full. A pass
	// then cleans the log up to where the map ended, and the next pass picks up from
	// there, so a log with more keys than the map holds is cleaned over several passes
	// rather than being left half done.
	limit := pol.MaxMapEntries
	if limit <= 0 {
		limit = defaultMaxMapEntries
	}
	dirtyStart := 0
	for dirtyStart < len(elig) && elig[dirtyStart].baseOffset < cleanedTo {
		dirtyStart++
	}
	latest := map[uint64]int64{}
	mapEnd := dirtyStart
	for i := dirtyStart; i < len(elig); i++ {
		keys, err := keysOf(elig[i], aborted)
		if err != nil {
			return CleanResult{}, p.cleanerReadFailed(elig[i], err)
		}
		for h, off := range keys {
			latest[h] = off // a later segment's record supersedes an earlier one's.
		}
		mapEnd = i
		if len(latest) >= limit {
			break
		}
	}
	elig = elig[:mapEnd+1]
	eligEnd := endOf(mapEnd)
	res := CleanResult{Ran: true, At: now, Complete: mapEnd == len(sealed)-1 || endOf(mapEnd) >= activeBase}

	// Rewrite, oldest first. retained counts, per producer, the records of its current
	// transaction that survive, which is what decides whether its marker does.
	retained := map[int64]int64{}
	for i, s := range elig {
		stats, err := p.rewriteSegment(s, i == 0, rewriteCtx{
			latest: latest, aborted: aborted, pol: pol, now: now, cleanedTo: cleanedTo,
			retained: retained,
		})
		if err != nil {
			return res, p.cleanerReadFailed(s, err)
		}
		res.BytesBefore += s.sizeBytes
		res.BytesAfter += stats.bytesAfter
		p.forgetAborted(stats.droppedMarkers)
		res.RecordsRemoved += stats.records
		res.MarkersRemoved += stats.markers
		switch {
		case stats.removedFile:
			res.SegmentsRemoved++
		case stats.rewritten:
			res.SegmentsRewritten++
		}
	}
	if err := p.setCleanedTo(eligEnd); err != nil {
		return res, err
	}
	p.mu.Lock()
	p.lastClean = res
	p.mu.Unlock()
	return res, nil
}

// forgetAborted drops the aborted transactions whose abort marker the cleaner has just
// removed from the log. This is not tidiness: a consumer is told which producers' records
// to skip, and it stops skipping when it meets the abort marker. With the marker gone and
// the entry still listed, it would skip everything that producer wrote afterwards,
// including transactions that committed.
func (p *Partition) forgetAborted(markers []int64) {
	if len(markers) == 0 {
		return
	}
	gone := make(map[int64]bool, len(markers))
	for _, m := range markers {
		gone[m] = true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.txn.aborted[:0]
	for _, a := range p.txn.aborted {
		if !gone[a.LastOffset] {
			kept = append(kept, a)
		}
	}
	p.txn.aborted = kept
}

// cleanerReadFailed treats a read failure on a segment that retention has removed
// meanwhile as nothing to report (the pass is abandoned and the next one starts afresh).
func (p *Partition) cleanerReadFailed(s *Segment, err error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, x := range p.segments {
		if x == s {
			return err
		}
	}
	return nil
}

type rewriteCtx struct {
	latest    map[uint64]int64
	aborted   abortedSet
	pol       CompactionPolicy
	now       time.Time
	cleanedTo int64
	retained  map[int64]int64
}

type segmentStats struct {
	// droppedMarkers are the offsets of the markers this segment lost.
	droppedMarkers   []int64
	records, markers int64
	bytesAfter       int64
	rewritten        bool
	removedFile      bool
}

// rewriteSegment writes the cleaned form of s beside it and, if anything was removed,
// swaps it in.
func (p *Partition) rewriteSegment(s *Segment, first bool, c rewriteCtx) (segmentStats, error) {
	base := filepath.Join(p.dir, segmentName(s.baseOffset))
	logTmp, idxTmp := base+".log"+cleanedExt, base+cleanerIndex+cleanedExt
	f, err := os.OpenFile(logTmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return segmentStats{}, err
	}
	cleanup := func() { f.Close(); os.Remove(logTmp); os.Remove(idxTmp) }
	w := bufio.NewWriterSize(f, 1<<20)

	// A tombstone is dropped once its segment has been cleaned before and is older than
	// delete.retention.ms. Cleaned before is what makes that safe: a pass over a tombstone
	// removes every older record of its key, so by the next one nothing is left for the
	// tombstone to hide, and dropping it cannot bring an old value back.
	tombstoneExpired := s.baseOffset < c.cleanedTo && c.now.Sub(s.lastModified) >= c.pol.DeleteRetention

	var stats segmentStats
	var written, since int64
	var entries []indexEntry
	err = batchesOf(s, func(_ int64, batch []byte, info batchInfo) error {
		attrs := attrsOf(batch)
		pid := int64(binary.BigEndian.Uint64(batch[batchProducerIDOff:]))
		var out []byte
		switch {
		case attrs&batchControlBit != 0:
			// A marker stays while some record of its transaction does.
			n := c.retained[pid]
			delete(c.retained, pid)
			if n > 0 {
				out = batch
			} else {
				stats.markers++
				stats.droppedMarkers = append(stats.droppedMarkers, info.baseOffset)
			}
		default:
			recs, err := DecodeRecords(batch)
			if err != nil {
				return err
			}
			transactional := attrs&batchTransactionalBit != 0
			abortedBatch := transactional && c.aborted.covers(pid, info.baseOffset)
			keep := recs[:0:0]
			for _, r := range recs {
				off := info.baseOffset + int64(r.OffsetDelta)
				drop := abortedBatch
				if !drop && r.Key != nil {
					if latest, ok := c.latest[hashKey(r.Key)]; ok && off < latest {
						drop = true
					} else if r.Value == nil && tombstoneExpired {
						drop = true
					}
				}
				if !drop {
					keep = append(keep, r)
				}
			}
			stats.records += int64(len(recs) - len(keep))
			switch {
			case len(keep) == len(recs):
				out = batch
			case len(keep) > 0:
				if out, err = rebuildBatch(batch, keep, c.pol.Codec); err != nil {
					return err
				}
			}
			if transactional {
				c.retained[pid] += int64(len(keep))
			}
		}
		if out == nil {
			return nil
		}
		if len(entries) == 0 || since >= p.indexInterval {
			entries = append(entries, indexEntry{offset: info.baseOffset, position: written})
			since = 0
		}
		if _, err := w.Write(out); err != nil {
			return err
		}
		written += int64(len(out))
		since += int64(len(out))
		return nil
	})
	if err != nil {
		cleanup()
		return segmentStats{}, err
	}
	stats.bytesAfter = written
	if stats.records == 0 && stats.markers == 0 {
		cleanup() // nothing to remove: the segment, and its modification time, stay as they are.
		stats.bytesAfter = s.sizeBytes
		return stats, nil
	}
	if err := w.Flush(); err != nil {
		cleanup()
		return segmentStats{}, err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return segmentStats{}, err
	}
	if err := f.Close(); err != nil {
		os.Remove(logTmp)
		os.Remove(idxTmp)
		return segmentStats{}, err
	}
	if err := writeIndexFile(idxTmp, entries); err != nil {
		os.Remove(logTmp)
		os.Remove(idxTmp)
		return segmentStats{}, err
	}
	empty := written == 0
	done, err := p.swapSegment(s, logTmp, idxTmp, empty && !first)
	if err != nil {
		return segmentStats{}, err
	}
	if !done {
		return segmentStats{bytesAfter: s.sizeBytes}, nil // retention got there first.
	}
	stats.removedFile = empty && !first
	stats.rewritten = !stats.removedFile
	return stats, nil
}

func writeIndexFile(path string, entries []indexEntry) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	var buf [indexEntryBytes]byte
	for i, e := range entries {
		binary.BigEndian.PutUint64(buf[0:], uint64(e.offset))
		binary.BigEndian.PutUint64(buf[8:], uint64(e.position))
		if _, err := f.WriteAt(buf[:], int64(i)*indexEntryBytes); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// swapSegment replaces a sealed segment with its cleaned form (or, when remove is set,
// removes it). The two renames cannot be one atomic step, so a marker file records that a
// swap is under way: a crash between them leaves the new log beside the old index, and
// the next open (recoverSwaps) rebuilds that index from the log, which is the one source
// of truth. A crash before the renames leaves the old files and a leftover .cleaned file
// that the next open deletes. It returns false, changing nothing, if the segment is no
// longer in the partition (retention removed it while it was being rewritten).
func (p *Partition) swapSegment(old *Segment, logTmp, idxTmp string, remove bool) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	i := -1
	for j, s := range p.segments {
		if s == old {
			i = j
		}
	}
	if i < 0 || i == len(p.segments)-1 {
		os.Remove(logTmp)
		os.Remove(idxTmp)
		return false, nil
	}
	if remove {
		os.Remove(logTmp)
		os.Remove(idxTmp)
		if err := old.close(); err != nil {
			return false, err
		}
		if err := removeSegmentFiles(p.dir, old.baseOffset); err != nil {
			return false, err
		}
		p.segments = append(p.segments[:i], p.segments[i+1:]...)
		return true, nil
	}

	base := filepath.Join(p.dir, segmentName(old.baseOffset))
	mtime := old.lastModified
	if err := old.close(); err != nil {
		return false, err
	}
	mark := base + swapMarkExt
	if err := os.WriteFile(mark, nil, 0o644); err != nil {
		return false, err
	}
	syncDir(p.dir)
	if err := os.Rename(logTmp, base+".log"); err != nil {
		return false, err
	}
	if err := os.Rename(idxTmp, base+cleanerIndex); err != nil {
		return false, err
	}
	// The modification time is the segment's age for retention: rewriting it is not new
	// data, and must not postpone the day it is deleted.
	_ = os.Chtimes(base+".log", mtime, mtime)
	syncDir(p.dir)
	os.Remove(mark)
	ns, err := openSegment(p.dir, old.baseOffset, p.indexInterval, p.beforeSync)
	if err != nil {
		return false, fmt.Errorf("log: reopening a cleaned segment: %w", err)
	}
	p.segments[i] = ns
	return true, nil
}

// recoverSwaps finishes or undoes a cleaner swap a crash interrupted: a leftover
// .cleaned file belongs to a pass that never completed and is removed; a .swap marker
// means the log may have been replaced under an index that no longer matches, so that
// index is rebuilt from the log.
func recoverSwaps(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, cleanedExt):
			_ = os.Remove(filepath.Join(dir, name))
		case strings.HasSuffix(name, ".tmp") && strings.HasPrefix(name, cleanedFile):
			_ = os.Remove(filepath.Join(dir, name))
		case strings.HasSuffix(name, swapMarkExt):
			base := strings.TrimSuffix(name, swapMarkExt)
			if err := rebuildIndex(filepath.Join(dir, base)); err != nil {
				return err
			}
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// rebuildIndex writes a segment's offset index afresh from its log.
func rebuildIndex(base string) error {
	f, err := os.Open(base + ".log")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	var entries []indexEntry
	var since int64
	for pos := int64(0); pos+batchFullHeaderBytes <= info.Size(); {
		head := make([]byte, batchFullHeaderBytes)
		if _, err := f.ReadAt(head, pos); err != nil {
			break
		}
		bi, ok := parseHeader(head)
		if !ok || pos+bi.totalBytes > info.Size() {
			break
		}
		if len(entries) == 0 || since >= defaultIndexRebuildInterval {
			entries = append(entries, indexEntry{offset: bi.baseOffset, position: pos})
			since = 0
		}
		since += bi.totalBytes
		pos += bi.totalBytes
	}
	return writeIndexFile(base+cleanerIndex, entries)
}

// defaultIndexRebuildInterval is the index density a rebuild uses when the partition's
// own interval is not at hand: the broker default.
const defaultIndexRebuildInterval = 4 << 10
