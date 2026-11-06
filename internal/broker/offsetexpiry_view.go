package broker

import "time"

// OffsetExpiry is when a group's committed offsets will expire, as the broker's own
// sweep would judge them now (GRP-19). It is computed by the rules the sweep applies,
// not estimated: an offset that is not in Offsets does not expire as things stand.
type OffsetExpiry struct {
	// Retention is offsets.retention.minutes; zero means offsets are kept for ever and
	// nothing else is set.
	Retention time.Duration
	// EmptySince is when a classic group lost its last member, if it has none and the
	// broker knows when. Its offsets expire Retention after that, whatever their commit
	// times. Zero when the offsets are counted from each commit instead.
	EmptySince time.Time
	// Offsets are the offsets that will expire, with when. Checked every Interval.
	Offsets []OffsetExpiryAt
	// Interval is how often the broker looks, so a time shown is "from" it, not exact.
	Interval time.Duration
}

// OffsetExpiryAt is one committed offset's expiry.
type OffsetExpiryAt struct {
	Topic     string
	Partition int32
	At        time.Time
}

// OffsetExpiry reports when the group's offsets expire. The zero value (Retention 0)
// says nothing expires.
func (b *Broker) OffsetExpiry(group string) OffsetExpiry {
	e := b.expiry
	if e == nil || e.retention <= 0 {
		return OffsetExpiry{}
	}
	out := OffsetExpiry{Retention: e.retention, Interval: e.interval()}
	b.cg848.mu.Lock()
	b.groups.mu.Lock()
	e.mu.Lock()
	defer func() {
		e.mu.Unlock()
		b.groups.mu.Unlock()
		b.cg848.mu.Unlock()
	}()
	now := e.now().UnixMilli()
	due := e.deadline(group, now, e.retention.Milliseconds())
	if due == nil {
		return out
	}
	entries, err := b.meta.ListOffsets(group)
	if err != nil {
		return out
	}
	if st := e.groups[group]; st.EmptySince != 0 && !st.Live {
		out.EmptySince = time.UnixMilli(st.EmptySince)
	}
	for _, o := range entries {
		if o.CommittedAt == 0 {
			continue
		}
		if at, ok := due(o.Topic, o.CommittedAt); ok {
			out.Offsets = append(out.Offsets, OffsetExpiryAt{Topic: o.Topic, Partition: o.Partition, At: time.UnixMilli(at)})
		}
	}
	return out
}
