package log

import "time"

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
