package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// What a restart does to the offsets of a group that was live when the broker stopped.
// Recorded against a real broker in conformance/recordings/offsets-retention-restart.json:
// it loads the members back, they run out of session, and only then is the group
// without members. This broker does not keep members, so it keeps that the group had
// some, and for how long a session.

const (
	rigClassicSession  = 30 * time.Second // what joinReq asks for.
	rigConsumerSession = 45 * time.Second // config.Default's ConsumerSessionTimeout.
)

// stopped ends the rig's broker the clean way, and reopen starts another over its files.
func (r *expiryRig) stopped() {
	r.exp.flush() // what Stop does; the rig stopped the sweep itself, to run it by hand.
	r.b.Stop()
	r.m.Close()
}

// killed is the broker dying where it stands: nothing in memory is written.
func (r *expiryRig) killed() { r.m.Close() }

// A group that had a member when the broker stopped is not a group that never had one
// (recorded: g-gone). Its offsets are kept while its members could still be there, and
// for the retention from the moment their session ran out.
func TestOffsetRetentionAGroupLiveAtShutdownKeepsItsOffsetsAcrossARestart(t *testing.T) {
	for name, end := range map[string]func(*expiryRig){"stopped": (*expiryRig).stopped, "killed": (*expiryRig).killed} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			r := newExpiryRig(t, dir, time.Now(), "a")
			r.join(t, "g", "a")
			r.commit(t, "g", "a", 5)
			r.advance(testRetention + time.Minute)
			r.exp.sweep()
			if !r.has("g", "a") {
				t.Fatal("a live group's offset on a topic it reads expired")
			}
			end(r)

			r2 := newExpiryRig(t, dir, r.now.Add(time.Second), "a")
			r2.exp.sweep()
			if !r2.has("g", "a") {
				t.Fatal("the first sweep after a restart expired the offset of a group that was live when the broker stopped")
			}
			// Nobody comes back. The group is empty from the end of the session.
			r2.advance(rigClassicSession + testRetention - time.Second)
			r2.exp.sweep()
			if !r2.has("g", "a") {
				t.Fatal("expired before a retention had passed since the members' session ran out")
			}
			r2.advance(2 * time.Second)
			r2.exp.sweep()
			if r2.has("g", "a") {
				t.Fatal("kept past a retention from when the members' session ran out")
			}
		})
	}
}

// When the members' session ran out is not counted again by a second restart.
func TestOffsetRetentionASecondRestartDoesNotStartTheClockAgain(t *testing.T) {
	dir := t.TempDir()
	r := newExpiryRig(t, dir, time.Now(), "a")
	r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)
	r.stopped()

	r2 := newExpiryRig(t, dir, r.now.Add(time.Second), "a")
	r2.advance(rigClassicSession + time.Minute)
	r2.exp.sweep() // the group is found to have been empty since the session ran out.
	r2.stopped()

	r3 := newExpiryRig(t, dir, r2.now.Add(testRetention), "a")
	r3.exp.sweep()
	if r3.has("g", "a") {
		t.Fatal("a second restart gave the emptied group a new retention")
	}
}

// A member that comes back in time keeps the group live (recorded: g-back), and when it
// then leaves the retention counts from that.
func TestOffsetRetentionAMemberReturningAfterARestartKeepsTheGroupLive(t *testing.T) {
	dir := t.TempDir()
	r := newExpiryRig(t, dir, time.Now(), "a")
	r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)
	r.advance(testRetention + time.Minute)
	r.killed()

	r2 := newExpiryRig(t, dir, r.now.Add(time.Second), "a")
	member, _ := r2.join(t, "g", "a")
	r2.advance(3 * testRetention)
	r2.exp.sweep()
	if !r2.has("g", "a") {
		t.Fatal("the offset of a group whose member came back expired")
	}
	r2.leave(t, "g", member)
	r2.advance(testRetention - time.Second)
	r2.exp.sweep()
	if !r2.has("g", "a") {
		t.Fatal("expired before a retention had passed since the member left")
	}
	r2.advance(2 * time.Second)
	r2.exp.sweep()
	if r2.has("g", "a") {
		t.Fatal("kept past a retention from when the member left")
	}
}

// A consumer-protocol group is kept while its members could still be there, and after
// that its offsets are as old as their commits (recorded: gc-gone is gone at the first
// read after its members' session, where the classic g-gone has a retention to go).
func TestOffsetRetentionAConsumerProtocolGroupAcrossARestart(t *testing.T) {
	for name, end := range map[string]func(*expiryRig){"stopped": (*expiryRig).stopped, "killed": (*expiryRig).killed} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			r := newExpiryRig(t, dir, time.Now(), "a")
			r.join848(t, "g", "n1", "a")
			r.commit(t, "g", "a", 5)
			r.advance(testRetention + time.Minute)
			r.exp.sweep()
			if !r.has("g", "a") {
				t.Fatal("a live consumer group's offset on a topic it reads expired")
			}
			end(r)

			r2 := newExpiryRig(t, dir, r.now.Add(time.Second), "a")
			r2.advance(rigConsumerSession - 2*time.Second)
			r2.exp.sweep()
			if !r2.has("g", "a") {
				t.Fatal("expired while the members the group had could still be there")
			}
			r2.advance(3 * time.Second)
			r2.exp.sweep()
			if r2.has("g", "a") {
				t.Fatal("a consumer group with no members kept an offset older than the retention")
			}
		})
	}
}

// A consumer-protocol group that empties keeps each offset for the retention from its
// own commit, not from when it emptied (recorded: offsets-retention-recreate, g-c-late).
func TestOffsetRetentionAnEmptiedConsumerProtocolGroupCountsFromTheCommit(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a", "b")
	r.join848(t, "g", "n1", "a")
	r.commit(t, "g", "a", 5)
	r.advance(testRetention - time.Minute)
	r.commit(t, "g", "b", 6)
	r.hb848("g", "n1", -1, nil) // leaves.
	r.advance(2 * time.Minute)
	r.exp.sweep()
	if r.has("g", "a") {
		t.Error("the offset committed a retention ago was kept")
	}
	if !r.has("g", "b") {
		t.Error("the offset committed three minutes ago expired")
	}
}

// A standalone committer's offsets are as old as their commits on both sides of a
// restart (recorded: g-alone).
func TestOffsetRetentionStandaloneAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	r := newExpiryRig(t, dir, time.Now(), "a", "b")
	r.commit(t, "solo", "a", 1)
	r.advance(testRetention - time.Minute)
	r.commit(t, "solo", "b", 2)
	r.killed()

	r2 := newExpiryRig(t, dir, r.now.Add(2*time.Minute), "a", "b")
	r2.exp.sweep()
	if r2.has("solo", "a") {
		t.Error("the offset committed a retention ago was kept across the restart")
	}
	if !r2.has("solo", "b") {
		t.Error("the offset committed three minutes ago expired at the restart")
	}
}

// A member that left a moment before the broker died: that it left was only in memory.
// What the store says is that the group has members, and that keeps its offsets.
func TestOffsetRetentionAnEmptyingLostInACrashKeepsTheOffsets(t *testing.T) {
	dir := t.TempDir()
	r := newExpiryRig(t, dir, time.Now(), "a")
	member, _ := r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)
	r.advance(testRetention + time.Minute)
	r.leave(t, "g", member)
	r.killed() // before the flush.

	r2 := newExpiryRig(t, dir, r.now.Add(time.Second), "a")
	r2.exp.sweep()
	if !r2.has("g", "a") {
		t.Fatal("a group whose emptying was lost in a crash was taken for one that never had a member")
	}
}

// Data written by a version that did not record which groups had members: on the first
// start every group with offsets is taken to have had some, once.
func TestOffsetRetentionAdoptsTheGroupsOfAnEarlierVersion(t *testing.T) {
	dir := t.TempDir()
	start := time.Now()
	m, err := meta.OpenWithClock(dir+"/meta.db", func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic("a", 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitOffset("was-live", "a", 0, 5, ""); err != nil {
		t.Fatal(err)
	}
	m.Close()

	r := newExpiryRig(t, dir, start.Add(testRetention+time.Minute), "a")
	r.exp.sweep()
	if !r.has("was-live", "a") {
		t.Fatal("an upgrade expired the offset of a group that may have been live")
	}
	r.advance(adoptedSession + testRetention + time.Second)
	r.exp.sweep()
	if r.has("was-live", "a") {
		t.Fatal("the adopted group's offset was kept for ever")
	}
	// A commit by hand after the upgrade is a standalone commit.
	r.commit(t, "solo", "a", 1)
	r.advance(testRetention + time.Second)
	r.exp.sweep()
	if r.has("solo", "a") {
		t.Fatal("a standalone group created after the upgrade was adopted")
	}
}

// flakyStore fails the writes of what is known about groups while broken is set.
type flakyStore struct {
	meta.Store
	mu     sync.Mutex
	broken bool
	writes int
}

var errStoreDown = errors.New("the store is down")

func (s *flakyStore) SetGroupLiveness(group string, st *meta.GroupLiveness) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken {
		return errStoreDown
	}
	s.writes++
	return s.Store.SetGroupLiveness(group, st)
}

func (s *flakyStore) set(broken bool) {
	s.mu.Lock()
	s.broken = broken
	s.mu.Unlock()
}

// A write that failed is tried again: what was known only in memory used to be marked
// written before it was.
func TestOffsetRetentionRetriesAFailedWrite(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	store := &flakyStore{Store: r.m}
	r.b.meta = store
	member, _ := r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)

	store.set(true)
	r.leave(t, "g", member)
	r.exp.flush()
	if st, _ := r.m.GroupLiveness(); !st["g"].Live {
		t.Fatalf("the store has %+v with its writes failing", st["g"])
	}
	store.set(false)
	r.exp.flush()
	st, _ := r.m.GroupLiveness()
	if st["g"].Live || st["g"].EmptySince != r.now.UnixMilli() {
		t.Fatalf("after the store came back the emptying is recorded as %+v", st["g"])
	}
}

// A member is not seated in a group that could not be recorded as having members: its
// commits would be in a store that says nobody reads them. Both protocols say the
// coordinator is not available, and the group is as it was.
func TestAJoinIsRefusedWhenTheGroupCannotBeRecordedAsLive(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	store := &flakyStore{Store: r.m}
	r.b.meta = store
	noDelay(t)
	store.set(true)

	req := joinReq("g", "")
	req.Protocols[0].Metadata = subscriptionBytes("a")
	res, err := r.b.groups.join(context.Background(), req, clientRef{})
	if err != nil || res.errorCode != kerr.CoordinatorNotAvailable.Code {
		t.Fatalf("classic join with the store down: %v %+v", err, res)
	}
	if r.b.groups.hasMembers("g") || r.b.claims.ownerOf("g") != "" || len(r.b.groups.list()) != 0 {
		t.Fatalf("the refused join left something behind: owner %q, groups %+v", r.b.claims.ownerOf("g"), r.b.groups.list())
	}

	hb := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb.Group, hb.MemberID, hb.RebalanceTimeoutMillis = "c", "n1", 60_000
	hb.SubscribedTopicNames = []string{"a"}
	hb.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	if got := r.b.cg848.heartbeat(hb, clientRef{}); got.errorCode != kerr.CoordinatorNotAvailable.Code {
		t.Fatalf("consumer-protocol join with the store down: %+v", got)
	}
	if r.b.cg848.hasMembers("c") || r.b.claims.ownerOf("c") != "" || len(r.b.cg848.list()) != 0 {
		t.Fatalf("the refused join left something behind: owner %q, groups %+v", r.b.claims.ownerOf("c"), r.b.cg848.list())
	}

	// A classic client is not seated in a consumer group either.
	store.set(false)
	r.join848(t, "mixed", "n1", "a")
	store.set(true)
	first := r.classicJoinSeat(t, "mixed", "", rigClassicSession, rigClassicSession, "a")
	if got := r.classicJoinSeat(t, "mixed", first.memberID, rigClassicSession, rigClassicSession, "a"); got.errorCode != kerr.CoordinatorNotAvailable.Code {
		t.Fatalf("classic join to a consumer group with the store down: %+v", got)
	}
	if len(r.members("mixed")) != 1 {
		t.Fatalf("members after the refused join: %v", r.members("mixed"))
	}

	// With the store back each of them joins.
	store.set(false)
	r.join(t, "g", "a")
	r.join848(t, "c", "n1", "a")
	if got := r.classicJoinSeat(t, "mixed", first.memberID, rigClassicSession, rigClassicSession, "a"); got.errorCode != 0 {
		t.Fatalf("classic join to a consumer group: %+v", got)
	}
}

// The store is told a group has members once, when it gets its first: not at every join
// and not at every heartbeat.
func TestRecordingThatAGroupIsLiveIsOneWrite(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	store := &flakyStore{Store: r.m}
	r.b.meta = store
	r.join848(t, "c", "n1", "a")
	r.join848(t, "c", "n2", "a")
	r.join848(t, "c", "n3", "a")
	for i := 0; i < 5; i++ {
		r.hb848("c", "n1", 0, nil)
	}
	if store.writes != 1 {
		t.Fatalf("%d writes for one group getting members, want 1", store.writes)
	}
}

// A group held by something that is not a protocol (it is being deleted, or forgotten)
// is not looked at.
func TestOffsetRetentionDoesNotLookAtAGroupBeingDeleted(t *testing.T) {
	for _, holder := range []string{"deleting", "expiring"} {
		r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
		r.commit(t, "g", "a", 1)
		r.advance(3 * testRetention)
		if !r.b.claims.claim("g", holder) {
			t.Fatal("could not hold the id")
		}
		r.exp.sweep()
		if !r.has("g", "a") {
			t.Errorf("an offset was expired from a group that is %s", holder)
		}
		r.b.claims.release("g", holder)
		r.exp.sweep()
		if r.has("g", "a") {
			t.Errorf("the offset was kept once the group was no longer %s", holder)
		}
	}
}

// What is known of a group and what the coordinator has must agree before anything is
// expired: members the record does not know of, or a record of members nobody can
// find, keep every offset.
func TestOffsetRetentionKeepsEverythingWhenTheRecordAndTheMembersDisagree(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a", "b")
	member, _ := r.join(t, "g", "a")
	r.commit(t, "g", "b", 2) // a topic it does not read: expires from its commit.
	r.advance(3 * testRetention)

	r.exp.mu.Lock()
	rec := r.exp.groups["g"]
	delete(r.exp.groups, "g")
	r.exp.mu.Unlock()
	r.exp.sweep()
	if !r.has("g", "b") {
		t.Fatal("expired from a group with members the record does not know of")
	}

	r.exp.mu.Lock()
	r.exp.groups["g"] = rec
	r.exp.mu.Unlock()
	r.b.groups.mu.Lock()
	r.b.groups.removeMember(r.b.groups.groups["g"], member) // gone, and nobody told the record.
	r.b.groups.claims.release("g", protoClassic)
	r.b.groups.mu.Unlock()
	r.exp.sweep()
	if !r.has("g", "b") {
		t.Fatal("expired from a group recorded as live whose members cannot be found")
	}
}

// The sweep and the coordinators at once. An old offset on the topic a group reads must
// survive any interleaving of the sweep with its only member leaving and coming back,
// and with the group moving between the two protocols: each of those used to leave a
// moment in which the group looked as if nobody had ever joined it. Run under -race.
func TestOffsetRetentionSweepRacesWithMembersComingAndGoing(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	r.withPartitions(t, "a", 2)
	// g churns its one classic member; m is a classic group that a consumer-protocol
	// member keeps joining and leaving, so it moves to the other coordinator and back.
	member, _ := r.join(t, "g", "a")
	r.holding(t, "m", "a", rigClassicSession, rigClassicSession, 0, 1)
	r.commit(t, "g", "a", 1)
	r.commit(t, "m", "a", 1)
	r.advance(3 * testRetention) // both offsets are far older than the retention.

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				r.exp.sweep()
			}
		}
	}()
	rounds := 300
	if testing.Short() {
		rounds = 50
	}
	for i := 0; i < rounds; i++ {
		r.leave(t, "g", member)
		member, _ = r.join(t, "g", "a")
		r.join848(t, "m", "n", "a")
		r.hb848("m", "n", -1, nil)
	}
	close(stop)
	wg.Wait()
	if !r.has("g", "a") {
		t.Error("the sweep expired an old offset of a group while its member was rejoining")
	}
	if !r.has("m", "a") {
		t.Error("the sweep expired an old offset of a group while it moved between the protocols")
	}
}
