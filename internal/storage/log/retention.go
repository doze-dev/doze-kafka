package log

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// RetentionPolicy bounds a partition's on-disk footprint. Enforcement is
// delete-only: whole non-active segment files past a bound are removed and the
// partition's log-start offset advances. There is no compaction. A zero field
// disables that bound; the active segment is never deleted, so a partition always
// keeps at least its most recent segment.
type RetentionPolicy struct {
	MaxBytes int64
	MaxAge   time.Duration
}

// enabled reports whether any bound is set (and thus whether the maintenance loop
// should schedule retention sweeps at all).
func (rp RetentionPolicy) enabled() bool { return rp.MaxBytes > 0 || rp.MaxAge > 0 }

// Enforce deletes whole, oldest, non-active segments that violate the policy:
// first by age (a sealed segment whose last write predates the MaxAge cutoff),
// then by total size (while the partition exceeds MaxBytes). It advances the
// log-start offset as segments fall away.
func (rp RetentionPolicy) Enforce(p *Partition) error {
	if !rp.enabled() {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if rp.MaxAge > 0 {
		cutoff := time.Now().Add(-rp.MaxAge)
		for len(p.segments) > 1 && p.segments[0].lastModified.Before(cutoff) {
			if err := p.dropOldestLocked(); err != nil {
				return err
			}
		}
	}

	if rp.MaxBytes > 0 {
		total := int64(0)
		for _, seg := range p.segments {
			total += seg.sizeBytes
		}
		for len(p.segments) > 1 && total > rp.MaxBytes {
			total -= p.segments[0].sizeBytes
			if err := p.dropOldestLocked(); err != nil {
				return err
			}
		}
	}
	return nil
}

// partitionDirName matches the "<topic>-<partition>" directory a partition lives in.
var partitionDirName = regexp.MustCompile(`^(.+)-(\d+)$`)

// enforceDir applies the policy to a partition directory that is not open, using
// file sizes and modification times alone. The result is the same as Enforce on
// an open partition — whole oldest segments are removed, the newest never — and
// the log-start offset follows from the surviving first segment the next time the
// partition is opened.
func (rp RetentionPolicy) enforceDir(dir string) error {
	if !rp.enabled() {
		return nil
	}
	bases, err := listSegmentBases(dir)
	if err != nil || len(bases) <= 1 {
		return err
	}
	type seg struct {
		base  int64
		size  int64
		mtime time.Time
	}
	segs := make([]seg, 0, len(bases))
	var total int64
	for _, base := range bases {
		info, err := os.Stat(filepath.Join(dir, segmentName(base)+".log"))
		if err != nil {
			return err
		}
		segs = append(segs, seg{base, info.Size(), info.ModTime()})
		total += info.Size()
	}
	cutoff := time.Now().Add(-rp.MaxAge)
	for len(segs) > 1 {
		old := rp.MaxAge > 0 && segs[0].mtime.Before(cutoff)
		big := rp.MaxBytes > 0 && total > rp.MaxBytes
		if !old && !big {
			break
		}
		if err := removeSegmentFiles(dir, segs[0].base); err != nil {
			return err
		}
		total -= segs[0].size
		segs = segs[1:]
	}
	return nil
}

// EnforceClosedRetention applies the retention policy to partitions that are on
// disk but not open. Partitions open lazily, so after a restart a topic nobody has
// touched since is not in the open set, and a sweep of only that set never trims
// it: its old data would sit on disk indefinitely. It never opens a partition (that
// would cost two file descriptors each); the directory is held under the manager
// lock so one cannot be opened mid-sweep.
func (m *Manager) EnforceClosedRetention() error {
	if !m.opts.Retention.enabled() {
		return nil
	}
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}
	var firstErr error
	for _, e := range entries {
		match := partitionDirName.FindStringSubmatch(e.Name())
		if !e.IsDir() || match == nil {
			continue
		}
		n, err := strconv.Atoi(match[2])
		if err != nil {
			continue
		}
		m.mu.Lock()
		_, open := m.partitions[topicPartition{match[1], int32(n)}]
		if !open && !m.stopped {
			if err := m.opts.Retention.enforceDir(filepath.Join(m.dir, e.Name())); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		m.mu.Unlock()
	}
	return firstErr
}
