package broker

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// Committed offsets expire (offsets.retention.minutes, GRP-19 / D7), by the rules a
// real Kafka applies, which are recorded in conformance/recordings/offsets-retention.json,
// offsets-retention-restart.json and offsets-retention-recreate.json:
//
//   - A classic group with no members, one that had some, keeps its offsets for the
//     retention from the moment it became empty, however late the last commit was: a
//     commit made into an empty group does not extend it.
//   - A group that never had a member (a standalone consumer committing by hand) keeps
//     each offset for the retention from its own commit. So does a consumer-protocol
//     group once its members are gone, and a group that was deleted and whose id is
//     used again: nothing of the old group is left.
//   - A group with members expires nothing for the topics it is subscribed to. The
//     offsets of a topic it is not subscribed to expire from their own commit, which
//     is how an application that stopped reading one topic stops carrying it. A group
//     in the middle of a rebalance is not looked at at all, nor is a classic group
//     whose protocol type is not "consumer" (a Connect worker's), or whose members'
//     subscriptions cannot be read (claim: groups R24d).
//   - A group with no members and no offsets is gone at the next sweep: it is not
//     listed, DescribeGroups says Dead, and its id, used again, is a new group. Until
//     that sweep it is the group it was: a commit made into it in between is a commit
//     into an empty group.
//   - A restart changes none of this. A real broker has its members again when it is
//     back, and a member that does not return runs out of session: the group is
//     without members from then, not from the restart.
//
// This broker does not keep members across a restart, so it keeps what the rules need:
// for each group, that it has members and the longest session among them, or when it
// lost the last one. A group with a record of having members and none in memory was
// live when the broker stopped (or was killed): it is taken to have them until that
// session has run out after the start, and to have lost them then.
//
// Whatever is not known for certain keeps the offset. A group is judged from one view
// of it, taken with both coordinators and this record held still, and the record does
// not change again until what was decided has been done.

// groupLiveness is what is known of a group's members, for the retention.
type groupLiveness struct {
	meta.GroupLiveness
	// presumed is a group recorded as having members that nobody has joined since
	// the broker started: its members are the ones a real broker would have loaded.
	presumed bool
}

// offsetExpiry runs the sweep and keeps what it needs to know about each group.
type offsetExpiry struct {
	b         *Broker
	retention time.Duration // 0: nothing expires; what is known of the groups is kept all the same.
	every     time.Duration // the check interval; 0 is interval's default.
	now       func() time.Time
	started   time.Time // when the presumed groups' sessions began to run.

	// mu is taken after a coordinator's lock, never before one. It is held across the
	// store writes below, so the store is never told something older than what a
	// coordinator was told had been recorded.
	mu     sync.Mutex
	groups map[string]groupLiveness
	dirty  map[string]bool // the groups whose record is not yet in the store; true once a failure was logged.

	wake     chan struct{}      // something became dirty: write it now, not at the next tick.
	checkReq chan chan struct{} // CheckOffsets: one sweep now, on the sweep's goroutine.
	stop     chan struct{}
	done     chan struct{}
}

// adoptedSession is the session given to the groups of a store written before groups
// with members were recorded (the Java client's default).
const adoptedSession = 45 * time.Second

// newOffsetExpiry loads what the store remembers of the groups. It does not start the
// sweep: start does.
func newOffsetExpiry(b *Broker, retention, every time.Duration) (*offsetExpiry, error) {
	n, err := b.meta.AdoptGroupsWithOffsets(adoptedSession)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		b.log.Info("offsets retention: the groups of an earlier version's data are taken to have had members until now", "groups", n)
	}
	stored, err := b.meta.GroupLiveness()
	if err != nil {
		return nil, err
	}
	e := &offsetExpiry{b: b, retention: retention, every: every, now: time.Now, started: time.Now(), checkReq: make(chan chan struct{}),
		groups: make(map[string]groupLiveness, len(stored)), dirty: map[string]bool{},
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	for id, st := range stored {
		e.groups[id] = groupLiveness{GroupLiveness: st, presumed: st.Live}
	}
	return e, nil
}

// restoreEmptyGroups puts back, at start, the groups that were empty when the broker
// stopped, as the coordinators keep an emptied group: listed and described as what it
// was, a classic group with its protocol type, a consumer group with its epoch and
// assignor (recorded: groups-listed-after-restart). One without offsets goes at the
// first sweep, as it would have.
func (e *offsetExpiry) restoreEmptyGroups() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id, st := range e.groups {
		switch {
		case st.Consumer:
			e.b.cg848.restoreEmpty(id, st.Epoch, st.AssignmentEpoch, st.Assignor)
		case !st.Live && st.ProtocolType != "":
			e.b.groups.restoreEmpty(id, st.ProtocolType)
		}
	}
}

// joining records that group is about to have a member of this kind with this session,
// before the member is seated. It is called under a coordinator's lock. A group that
// was not known to have members, or not such members, is written to the store before
// joining returns: the member's commits must never be in a store that says its group
// has nobody. An error means it could not be, and the member must not be seated.
func (e *offsetExpiry) joining(group string, classic bool, session time.Duration, protocolType string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	was := e.groups[group]
	st := groupLiveness{GroupLiveness: meta.GroupLiveness{Live: true, Classic: classic, Session: session, ProtocolType: protocolType}}
	if was.Live && !was.presumed {
		st.Classic = st.Classic || was.Classic
		st.Session = max(st.Session, was.Session)
	}
	if st.GroupLiveness != was.GroupLiveness || e.isDirty(group) {
		if err := e.b.meta.SetGroupLiveness(group, &st.GroupLiveness); err != nil {
			e.b.log.Error("offsets retention: could not record that a group has members; the member is refused", "group", group, "err", err)
			return err
		}
		delete(e.dirty, group)
	}
	e.groups[group] = st
	return nil
}

// groupKind is what is shown of a group while it is empty, across a restart too: a
// classic group's protocol type, a consumer group's epoch and assignor.
type groupKind struct {
	protocolType string
	epoch        int32
	targetEpoch  int32 // the epoch of its last target assignment.
	assignor     string
}

// emptied records that group has just lost its last member. A classic group is empty
// from now; a consumer group has no such moment (recorded: its offsets expire from
// their commits, as those of a group that never had a member do), only what it is.
func (e *offsetExpiry) emptied(group string, classic bool, kind groupKind) {
	e.mu.Lock()
	if classic {
		e.groups[group] = groupLiveness{GroupLiveness: meta.GroupLiveness{EmptySince: e.now().UnixMilli(), ProtocolType: kind.protocolType}}
	} else {
		e.groups[group] = groupLiveness{GroupLiveness: meta.GroupLiveness{Consumer: true, Epoch: kind.epoch, AssignmentEpoch: kind.targetEpoch, Assignor: kind.assignor}}
	}
	e.markDirty(group)
	e.mu.Unlock()
}

// consumerOnly records that the last classic member of a consumer group has gone and
// others remain.
func (e *offsetExpiry) consumerOnly(group string, session time.Duration) {
	e.mu.Lock()
	if st := e.groups[group]; st.Live && st.Classic {
		e.groups[group] = groupLiveness{GroupLiveness: meta.GroupLiveness{Live: true, Session: session, ProtocolType: "consumer"}}
		e.markDirty(group)
	}
	e.mu.Unlock()
}

func (e *offsetExpiry) isDirty(group string) bool {
	_, ok := e.dirty[group]
	return ok
}

// markDirty has the record written soon. Caller holds e.mu.
func (e *offsetExpiry) markDirty(group string) {
	if !e.isDirty(group) {
		e.dirty[group] = false
	}
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// flush writes what changed to the store. What could not be written stays to be tried
// again: the record in memory is what the sweep reads, the one in the store is what a
// restart will, and the two must not be left apart.
func (e *offsetExpiry) flush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for g, logged := range e.dirty {
		var state *meta.GroupLiveness
		if st, ok := e.groups[g]; ok {
			state = &st.GroupLiveness
		}
		if err := e.b.meta.SetGroupLiveness(g, state); err != nil {
			if !logged {
				e.b.log.Warn("offsets retention: could not record a group's members; trying again", "group", g, "err", err)
				e.dirty[g] = true
			}
			continue
		}
		delete(e.dirty, g)
	}
}

// interval is how often the sweep runs: a hundredth of the retention, between 50 ms and
// ten minutes (Kafka's offsets.retention.check.interval.ms default), which is also what
// it is with no retention.
func (e *offsetExpiry) interval() time.Duration {
	if e.every > 0 {
		return e.every
	}
	if e.retention <= 0 {
		return 10 * time.Minute
	}
	return min(max(e.retention/100, 50*time.Millisecond), 10*time.Minute)
}

// start runs the sweep until stopAndFlush.
func (e *offsetExpiry) start() {
	go func() {
		defer close(e.done)
		sweep := time.NewTicker(e.interval())
		retry := time.NewTicker(500 * time.Millisecond) // a write that failed is tried again.
		defer sweep.Stop()
		defer retry.Stop()
		for {
			select {
			case <-e.stop:
				return
			case <-e.wake:
				e.flush()
			case <-retry.C:
				e.flush()
			case <-sweep.C:
				e.sweep()
			case done := <-e.checkReq:
				e.sweep()
				close(done)
			}
		}
	}()
}

// checkNow runs one sweep and returns once it has: what a scheduled check would do,
// at a moment the caller chooses. It does nothing once the sweep has stopped.
func (e *offsetExpiry) checkNow() {
	done := make(chan struct{})
	select {
	case e.checkReq <- done:
		<-done
	case <-e.done:
	}
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
	e.mu.Lock()
	for g := range e.groups {
		e.settle(g, now)
	}
	e.mu.Unlock()
	for _, id := range e.unused(hasOffsets) {
		e.forgetGroup(id)
	}
}

// unused is the groups that have no members and no offsets: whatever the retention is,
// such a group is gone at the sweep that finds it so (recorded: offsets-retention-recreate,
// g-offdel-late, g-none, g-c-none). hasOffsets was read before this, so a group that
// was committed into since is dropped all the same and its new offset starts a new
// group, which keeps the offset longer and never shorter.
func (e *offsetExpiry) unused(hasOffsets map[string]bool) []string {
	b := e.b
	seen := map[string]bool{}
	var ids []string
	add := func(id string) {
		if !hasOffsets[id] && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	for _, g := range b.groups.list() {
		if g.members == 0 {
			add(g.id)
		}
	}
	for _, g := range b.cg848.list() {
		if g.members == 0 {
			add(g.id)
		}
	}
	e.mu.Lock()
	for id, st := range e.groups {
		if !st.Live {
			add(id)
		}
	}
	e.mu.Unlock()
	return ids
}

// settle is the group's record at now: a group presumed to have the members it had
// when the broker stopped has lost them once their session has run out with nobody
// joining, and it is then that it lost them. Caller holds e.mu.
func (e *offsetExpiry) settle(id string, now int64) groupLiveness {
	st, ok := e.groups[id]
	if !ok || !st.presumed {
		return st
	}
	gone := e.started.Add(st.Session).UnixMilli()
	if now < gone {
		return st
	}
	if st.Classic {
		st = groupLiveness{GroupLiveness: meta.GroupLiveness{EmptySince: gone, ProtocolType: st.ProtocolType}}
	} else {
		st = groupLiveness{GroupLiveness: meta.GroupLiveness{Consumer: true}}
	}
	e.groups[id] = st
	e.markDirty(id)
	return st
}

// rule is what may be expired from the group now: nil is nothing. Caller holds both
// coordinators' locks and e.mu, so the members it sees, what they subscribe to and
// the record are of one moment.
func (e *offsetExpiry) rule(id string, now, retention int64) func(topic string, partition int32, committedAt int64) bool {
	b := e.b
	var live func(topic string) bool
	members := false
	if g := b.cg848.groups[id]; g != nil && len(g.members) > 0 {
		members, live = true, g.subscription()
	} else if g := b.groups.groups[id]; g != nil && len(g.members) > 0 {
		members, live = true, g.subscription()
	}
	st := e.settle(id, now)
	switch {
	case retention <= 0:
		return nil // offsets are kept for ever.
	case members && (live == nil || !st.Live):
		// In a rebalance, not a group whose subscriptions can be read, or a group the
		// record does not know to have members: nothing is expired.
		return nil
	case members:
		return func(topic string, _ int32, committedAt int64) bool {
			return !live(topic) && now-committedAt >= retention
		}
	case b.claims.ownerOf(id) != "":
		return nil // held with no members: being joined, deleted or forgotten at this moment.
	case st.Live:
		return nil // the members it had when the broker stopped, or members not accounted for.
	case st.EmptySince != 0:
		return func(string, int32, int64) bool { return now-st.EmptySince >= retention }
	default:
		return func(_ string, _ int32, committedAt int64) bool { return now-committedAt >= retention }
	}
}

func (e *offsetExpiry) sweepGroup(id string, now, retention int64) {
	b := e.b
	b.cg848.mu.Lock()
	b.groups.mu.Lock()
	e.mu.Lock()
	expire := e.rule(id, now, retention)
	b.groups.mu.Unlock()
	b.cg848.mu.Unlock()
	// e.mu stays: a member joining or the group being deleted waits until what was
	// decided has been done, instead of having it done to what the group is next.
	emptied := expire != nil && e.expireOffsets(id, expire)
	e.mu.Unlock()
	if emptied {
		e.forgetGroup(id)
	}
}

// expireOffsets removes what expire says and reports whether the group has none left.
// Caller holds e.mu.
func (e *offsetExpiry) expireOffsets(id string, expire func(topic string, partition int32, committedAt int64) bool) (emptied bool) {
	b := e.b
	// Looking is a read; only a sweep that changes something takes a write (and an
	// fsync), which keeps an idle broker idle.
	entries, err := b.meta.ListOffsets(id)
	if err != nil {
		return false
	}
	work := false
	for _, o := range entries {
		if o.CommittedAt == 0 || expire(o.Topic, o.Partition, o.CommittedAt) {
			work = true
			break
		}
	}
	if !work {
		return false
	}
	n, err := b.meta.ExpireOffsets(id, expire)
	if err != nil {
		b.log.Warn("offsets retention: could not expire offsets", "group", id, "err", err)
		return false
	}
	if n == 0 {
		return false
	}
	b.log.Info("committed offsets expired", "group", id, "offsets", n, "retention", e.retention.String())
	rest, err := b.meta.ListOffsets(id)
	return err == nil && len(rest) == 0
}

// forgetGroup drops a group with no members that holds no offsets (its last one has
// expired or been deleted, or it never had one): it is no longer listed, and its id is
// free of both protocols. Called with no lock held.
func (e *offsetExpiry) forgetGroup(id string) {
	b := e.b
	const expiring = "expiring"
	if !b.claims.claim(id, expiring) {
		return // a member joined while the sweep ran.
	}
	defer b.claims.release(id, expiring)
	b.groups.forget(id)
	b.cg848.forget(id)
	e.forget(id)
}

// forget drops what is recorded of a group that is gone, so that the id, used again,
// starts as a group that never had a member. It returns what was recorded, for a caller
// that has to put it back (restore).
func (e *offsetExpiry) forget(id string) (was groupLiveness, known bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if was, known = e.groups[id]; known {
		delete(e.groups, id)
		e.markDirty(id)
	}
	return was, known
}

// restore puts back what forget took, when the group turned out not to be gone.
func (e *offsetExpiry) restore(id string, was groupLiveness) {
	e.mu.Lock()
	e.groups[id] = was
	e.markDirty(id)
	e.mu.Unlock()
}

// decodeConsumerSubscription reads the topics out of a classic consumer's protocol
// metadata (version 0 to 3: int16 version, array of topic names, user data).
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

// subscription reports, for a classic group with members, whether a topic is one it
// reads. It is nil when nothing may be expired from the group: it is in a rebalance, or
// it is not a consumer group whose subscriptions are known (claim: groups R24d).
// Caller holds the coordinator's lock.
func (g *group) subscription() (live func(topic string) bool) {
	if g.state != stateStable || g.protocolType != "consumer" {
		return nil
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
			return nil // a subscription this broker cannot read: expire nothing.
		}
		for _, t := range topics {
			subs[t] = true
		}
	}
	return func(topic string) bool { return subs[topic] }
}

// subscription is the same for a consumer group: what its members subscribe to, by
// name and by pattern. Caller holds the coordinator's lock; the function it returns
// reads nothing of the group.
func (g *cg848Group) subscription() (live func(topic string) bool) {
	names := map[string]bool{}
	var regexes []func(string) bool
	for _, mem := range g.members {
		for _, t := range mem.subscribed {
			names[t] = true
		}
		if mem.regex != nil {
			regexes = append(regexes, mem.regex.MatchString)
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
