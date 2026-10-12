package broker

import (
	"encoding/binary"
	"sync"
	"time"
)

// Committed offsets expire (offsets.retention.minutes, GRP-19 / D7), by the rules a
// real Kafka applies, which are recorded in conformance/recordings/offsets-retention.json:
//
//   - A group with no members, one that had some, keeps its offsets for the retention
//     from the moment it became empty, however late the last commit was: a commit made
//     into an empty group does not extend it.
//   - A group that never had a member (a standalone consumer committing by hand) keeps
//     each offset for the retention from its own commit.
//   - A group with members expires nothing for the topics it is subscribed to. The
//     offsets of a topic it is not subscribed to expire from their own commit, which
//     is how an application that stopped reading one topic stops carrying it. A group
//     in the middle of a rebalance is not looked at at all.
//   - A group whose offsets are all gone is gone: it is not listed, and DescribeGroups
//     says Dead.
//
// What a group's members subscribe to is read from the group itself, so an offset on
// a topic a live group reads is never swept from under it.

// offsetExpiry runs the sweep and keeps when each group became empty.
type offsetExpiry struct {
	b         *Broker
	retention time.Duration
	now       func() time.Time

	mu    sync.Mutex
	empty map[string]int64    // group -> unix ms it lost its last member.
	dirty map[string]struct{} // the groups whose entry is not yet written to the store.

	stop chan struct{}
	done chan struct{}
}

// newOffsetExpiry loads what the store remembers of when groups became empty. It does
// not start the sweep: Start does.
func newOffsetExpiry(b *Broker, retention time.Duration) (*offsetExpiry, error) {
	empty, err := b.meta.GroupsEmptySince()
	if err != nil {
		return nil, err
	}
	return &offsetExpiry{b: b, retention: retention, now: time.Now, empty: empty,
		dirty: map[string]struct{}{}, stop: make(chan struct{}), done: make(chan struct{})}, nil
}

// markEmpty records that group has just lost its last member. Called under a
// coordinator's lock, so it only touches memory; the store is written by the flusher.
func (e *offsetExpiry) markEmpty(group string) {
	e.mu.Lock()
	if _, already := e.empty[group]; !already { // it has been empty since the first time.
		e.empty[group] = e.now().UnixMilli()
		e.dirty[group] = struct{}{}
	}
	e.mu.Unlock()
}

// markActive records that a member has joined group.
func (e *offsetExpiry) markActive(group string) {
	e.mu.Lock()
	if _, was := e.empty[group]; was {
		delete(e.empty, group)
		e.dirty[group] = struct{}{}
	}
	e.mu.Unlock()
}

// flush writes what changed to the store.
func (e *offsetExpiry) flush() {
	e.mu.Lock()
	type write struct {
		group string
		at    int64
	}
	var writes []write
	for g := range e.dirty {
		writes = append(writes, write{g, e.empty[g]})
	}
	e.dirty = map[string]struct{}{}
	e.mu.Unlock()
	for _, w := range writes {
		if err := e.b.meta.SetGroupEmptySince(w.group, w.at); err != nil {
			e.b.log.Warn("offsets retention: could not record when a group became empty", "group", w.group, "err", err)
		}
	}
}

// interval is how often the sweep runs: a hundredth of the retention, between 50 ms and
// ten minutes (Kafka's offsets.retention.check.interval.ms default).
func (e *offsetExpiry) interval() time.Duration {
	return min(max(e.retention/100, 50*time.Millisecond), 10*time.Minute)
}

// start runs the sweep until stopAndFlush.
func (e *offsetExpiry) start() {
	go func() {
		defer close(e.done)
		sweep := time.NewTicker(e.interval())
		flush := time.NewTicker(500 * time.Millisecond)
		defer sweep.Stop()
		defer flush.Stop()
		for {
			select {
			case <-e.stop:
				return
			case <-flush.C:
				e.flush()
			case <-sweep.C:
				e.sweep()
			}
		}
	}()
}

// stopAndFlush ends the sweep and writes what is still only in memory.
func (e *offsetExpiry) stopAndFlush() {
	select {
	case <-e.stop:
		return
	default:
	}
	close(e.stop)
	<-e.done
	e.flush()
}

// sweep is one pass over every group that holds offsets.
func (e *offsetExpiry) sweep() {
	b := e.b
	ids, err := b.meta.OffsetGroups()
	if err != nil {
		b.log.Warn("offsets retention: could not list the groups with offsets", "err", err)
		return
	}
	now := e.now().UnixMilli()
	retention := e.retention.Milliseconds()
	hasOffsets := map[string]bool{}
	for _, id := range ids {
		hasOffsets[id] = true
		e.sweepGroup(id, now, retention)
	}
	// A group that went empty with no offsets, and has none a retention later, is
	// forgotten: its entry would otherwise outlive anything that could use it.
	e.mu.Lock()
	var gone []string
	for g, at := range e.empty {
		if !hasOffsets[g] && now-at >= retention {
			gone = append(gone, g)
		}
	}
	for _, g := range gone {
		delete(e.empty, g)
		e.dirty[g] = struct{}{}
	}
	e.mu.Unlock()
}

func (e *offsetExpiry) sweepGroup(id string, now, retention int64) {
	b := e.b
	// A group being deleted, or joined at this moment, is not looked at: its owner
	// holds the id.
	var live func(topic string) bool // nil: the group has no members.
	switch b.claims.ownerOf(id) {
	case protoClassic:
		var ok bool
		live, ok = b.groups.subscriptionOf(id)
		if !ok {
			return // in a rebalance, or not a consumer group: nothing is expired.
		}
	case protoConsumer:
		live = b.cg848.subscriptionOf(id)
	}

	e.mu.Lock()
	since, wasEmpty := e.empty[id]
	e.mu.Unlock()

	expire := func(topic string, _ int32, committedAt int64) bool {
		switch {
		case live != nil:
			return !live(topic) && now-committedAt >= retention
		case wasEmpty:
			return now-since >= retention
		default:
			return now-committedAt >= retention
		}
	}

	// Looking is a read; only a sweep that changes something takes a write (and an
	// fsync), which keeps an idle broker idle.
	entries, err := b.meta.ListOffsets(id)
	if err != nil {
		return
	}
	work := false
	for _, o := range entries {
		if o.CommittedAt == 0 || expire(o.Topic, o.Partition, o.CommittedAt) {
			work = true
			break
		}
	}
	if !work {
		return
	}
	n, err := b.meta.ExpireOffsets(id, expire)
	if err != nil {
		b.log.Warn("offsets retention: could not expire offsets", "group", id, "err", err)
		return
	}
	if n == 0 {
		return
	}
	b.log.Info("committed offsets expired", "group", id, "offsets", n, "retention", e.retention.String())
	if rest, err := b.meta.ListOffsets(id); err == nil && len(rest) == 0 {
		e.forgetGroup(id)
	}
}

// forgetGroup drops an emptied group whose last offset has expired: it is no longer
// listed, and its id is free of both protocols.
func (e *offsetExpiry) forgetGroup(id string) {
	b := e.b
	const expiring = "expiring"
	if !b.claims.claim(id, expiring) {
		return // a member joined while the sweep ran.
	}
	defer b.claims.release(id, expiring)
	b.groups.forget(id)
	b.cg848.forget(id)
	e.mu.Lock()
	if _, ok := e.empty[id]; ok {
		delete(e.empty, id)
		e.dirty[id] = struct{}{}
	}
	e.mu.Unlock()
}

// decodeConsumerSubscription reads the topics out of a classic consumer's protocol
// metadata (version 0 to 3: int16 version, array of topic names, user data).
//
//fuzz:parser FuzzDecodeConsumerProtocol
func decodeConsumerSubscription(b []byte) (topics []string, ok bool) {
	if len(b) < 6 {
		return nil, false
	}
	n := int(int32(binary.BigEndian.Uint32(b[2:])))
	b = b[6:]
	if n < 0 || n > len(b)/2 {
		return nil, false
	}
	for i := 0; i < n; i++ {
		if len(b) < 2 {
			return nil, false
		}
		l := int(binary.BigEndian.Uint16(b))
		b = b[2:]
		if len(b) < l {
			return nil, false
		}
		topics = append(topics, string(b[:l]))
		b = b[l:]
	}
	return topics, true
}

// subscriptionOf reports, for a classic group with members, whether a topic is one it
// reads. ok is false when nothing may be expired from it: it is in a rebalance, or it is
// not a consumer group whose subscriptions are known. A group with no members returns a
// nil function and ok.
func (gm *groupManager) subscriptionOf(id string) (live func(topic string) bool, ok bool) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[id]
	if g == nil || len(g.members) == 0 {
		return nil, true
	}
	if g.state != stateStable || g.protocolType != "consumer" {
		return nil, false
	}
	subs := map[string]bool{}
	for _, m := range g.members {
		var meta []byte
		for _, p := range m.protocols {
			if p.name == g.protocol {
				meta = p.metadata
			}
		}
		topics, decoded := decodeConsumerSubscription(meta)
		if !decoded {
			return nil, false // a subscription this broker cannot read: expire nothing.
		}
		for _, t := range topics {
			subs[t] = true
		}
	}
	return func(topic string) bool { return subs[topic] }, true
}

// subscriptionOf is the same for a KIP-848 group: what its members subscribe to, by
// name and by pattern. A group with no members returns a nil function.
func (m *cg848Manager) subscriptionOf(id string) (live func(topic string) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[id]
	if g == nil || len(g.members) == 0 {
		return nil
	}
	members := make([]*cg848Member, 0, len(g.members))
	for _, mem := range g.members {
		members = append(members, mem)
	}
	// The members are only read here, under the lock, to build a snapshot: the
	// closure must not touch them once it is released.
	names := map[string]bool{}
	var regexes []func(string) bool
	for _, mem := range members {
		for _, t := range mem.subscribed {
			names[t] = true
		}
		if mem.regex != nil {
			re := mem.regex
			regexes = append(regexes, re.MatchString)
		}
	}
	return func(topic string) bool {
		if names[topic] {
			return true
		}
		for _, match := range regexes {
			if match(topic) {
				return true
			}
		}
		return false
	}
}
