package log

import (
	"fmt"
	"slices"
)

// What is on disk, and what this process knows about it.
//
// The rule: nothing is acknowledged as durable on the strength of bytes this
// process has not itself flushed. That matters at three moments.
//
// When a partition is opened. The bytes it finds may have been written by a
// process that was killed before it flushed them: they are in the page cache,
// they read back perfectly, and a power cut would take them. A duplicate retry
// at acks=all, or the end of a transaction whose marker is already in the log,
// answers from those bytes. So unless the manager vouches for the partition
// (the last stop was clean and flushed it; see cleanstop.go) everything that
// can hold such bytes starts dirty, and the first flush covers it. With the
// invariant below that is the active segment, the directory, and nothing else.
//
// When a segment is sealed. A segment that has a successor was flushed, whole,
// before the successor was created: the roll does it, whatever it believes
// about the segment, because the timer and acks=all only ever flush what is
// dirty. That invariant is why a sealed segment needs no second look after a
// crash. A partition written by a version that did not hold it (a roll right
// after a restart sealed a segment with a tail nobody had flushed) is listed
// by the manager as unvalidated: on open every one of its segments is checked
// batch by batch and starts dirty, and it leaves the list once all of them
// have been flushed.
//
// When a flush fails. The kernel may drop the pages and report success next
// time, so nothing of this partition is acknowledged again until a restart
// (BAD-3): the failure is kept, for a segment and for the partition.

// offline is the failure that took this partition's writes offline, if one did.
func (p *Partition) offline() error {
	if e := p.failed.Load(); e != nil {
		return fmt.Errorf("%w: %w", ErrStorageOffline, *e)
	}
	return nil
}

func (p *Partition) markOffline(err error) {
	p.failed.CompareAndSwap(nil, &err)
}

// Offline reports whether a failed flush has taken the partition's writes offline.
func (p *Partition) Offline() bool { return p.failed.Load() != nil }

// flushSegment flushes one segment and, if the flush itself failed (as opposed to
// being refused before it was tried), takes the partition offline.
func (p *Partition) flushSegment(s *Segment) error {
	err := s.sync()
	if err != nil {
		if f := s.failed.Load(); f != nil {
			p.markOffline(*f)
		}
	}
	return err
}

// rollLocked seals the active segment and starts the one that begins at offset.
// The caller holds mu.
func (p *Partition) rollLocked(offset int64) error {
	// Durably seal the segment being rolled off before opening the next: from
	// here on nothing looks at it again, not the timer, not acks=all and not
	// recovery, so this is the flush that every record in it rests on.
	if err := p.flushSegment(p.active()); err != nil {
		return err
	}
	return p.addSegment(offset)
}

// Sync flushes to stable storage everything of this partition that is not known
// to be there: the active segment, any sealed segment this process has not
// flushed, and the directory if its entries changed. When it returns nil, every
// record appended before the call, and everything read back when the partition
// was opened, is on disk.
func (p *Partition) Sync() error {
	want := p.appendSeq.Load()
	if p.syncedSeq.Load() >= want {
		return nil // nothing written since the last fsync: no lock, no syscall.
	}
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	if p.syncedSeq.Load() >= want {
		return nil // a concurrent caller's fsync covered this write while we waited.
	}
	if err := p.offline(); err != nil {
		return err
	}

	// Note how far the log has got BEFORE flushing: the fsync below covers at
	// least that much (it may cover more), so it is safe to record as durable.
	p.mu.Lock()
	upto := p.appendSeq.Load()
	sealed := slices.Clone(p.unsynced)
	active := p.active()
	dirUpto, dirDirty := p.dirSeq, p.dirSynced < p.dirSeq
	p.mu.Unlock()

	// The fsyncs run without p.mu: holding it blocked every produce and fetch on
	// the partition for the duration of a disk flush. File operations are safe to
	// run concurrently with appends. If the segment rolled in the meantime it was
	// sealed with its own fsync (which waits for this one), and a sealed, flushed
	// segment makes this a no-op. Any error, including a closed file, means the
	// flush did not happen and must be reported: "closed" was once treated as
	// "already safe", which acknowledged writes that a failed shutdown flush had
	// left in the page cache.
	for _, seg := range sealed {
		if err := p.flushSegment(seg); err != nil {
			return err
		}
	}
	if err := p.flushSegment(active); err != nil {
		return err
	}
	if dirDirty {
		if err := fsSyncDir(p.dir); err != nil {
			p.markOffline(err)
			return err
		}
	}

	p.mu.Lock()
	for _, seg := range sealed {
		p.forgetUnsynced(seg)
	}
	p.dirSynced = max(p.dirSynced, dirUpto)
	if len(p.unsynced) == 0 {
		p.unvalidated = false
	}
	p.mu.Unlock()
	p.syncedSeq.Store(upto)
	return nil
}

// forgetUnsynced takes a segment off the list of sealed segments still to be
// flushed. The caller holds mu.
func (p *Partition) forgetUnsynced(s *Segment) {
	if i := slices.Index(p.unsynced, s); i >= 0 {
		p.unsynced = slices.Delete(p.unsynced, i, i+1)
	}
}

// validated reports whether every segment the partition was opened with has
// been checked and flushed (or never needed to be).
func (p *Partition) validated() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.unvalidated
}
