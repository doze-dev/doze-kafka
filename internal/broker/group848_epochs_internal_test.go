package broker

import (
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// The KIP-848 coordinator's epochs, driven straight at the cg848Manager (interval 0, so
// targets are computed at every heartbeat). Wire-visible words are the ones the code
// cites as recorded (staleEpoch, staticRefusal).

func hb848(m *cg848Manager, group, member string, epoch int32, mut func(r *kmsg.ConsumerGroupHeartbeatRequest)) cg848Result {
	r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	r.Group, r.MemberID, r.MemberEpoch, r.RebalanceTimeoutMillis = group, member, epoch, 60_000
	if epoch == 0 {
		r.SubscribedTopicNames = []string{"t"}
		r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	}
	if mut != nil {
		mut(r)
	}
	return m.heartbeat(r, clientRef{})
}

func with848Topic(t *testing.T, m *cg848Manager, name string, partitions int32) meta.Topic {
	t.Helper()
	topic, err := m.meta.CreateTopic(name, partitions, nil)
	if err != nil {
		t.Fatal(err)
	}
	return topic
}

func TestKIP848ANewManagerWithNoSessionTimeoutUsesTheDefault(t *testing.T) {
	if cg848DefaultSessionTimeout != 45*time.Second {
		t.Fatalf("default session timeout %v, want 45s", cg848DefaultSessionTimeout)
	}
	for _, in := range []time.Duration{0, -time.Second} {
		if got := newCG848Manager(nil, nil, in, 0, nil).sessionTimeout; got != 45*time.Second {
			t.Errorf("session timeout %v given: %v, want 45s", in, got)
		}
	}
	if got := newCG848Manager(nil, nil, 9*time.Second, 0, nil).sessionTimeout; got != 9*time.Second {
		t.Errorf("9 s given: %v", got)
	}
}

// A member whose epoch is ahead of the group's is told so in Kafka's words (recorded:
// val-epoch-too-high), with the code that makes it rejoin.
func TestKIP848AnEpochAheadOfTheGroupsIsFencedInKafkasWords(t *testing.T) {
	m := new848Manager(t)
	with848Topic(t, m, "t", 2)
	join := hb848(m, "g", "a", 0, nil)
	if join.errorCode != 0 {
		t.Fatalf("join: %+v", join)
	}
	res := hb848(m, "g", "a", join.memberEpoch+90, nil)
	want := "The consumer group member has a greater member epoch (" + itoa(join.memberEpoch+90) + ") than the one known by the group coordinator (" + itoa(join.memberEpoch) + "). The member must abandon all its partitions and rejoin."
	if res.errorCode != kerr.FencedMemberEpoch.Code || res.errorMsg != want {
		t.Errorf("an epoch ahead: code %d message %q\nwant %q", res.errorCode, res.errorMsg, want)
	}
}

func itoa(n int32) string { return strconv.Itoa(int(n)) }

// A heartbeat that carries the epoch before the current one is a retry of one whose
// answer was lost: it gets the assignment again. Any other old epoch is fenced.
func TestKIP848ARetriedHeartbeatIsAnsweredAgainAndAnOlderOneIsFenced(t *testing.T) {
	m := new848Manager(t)
	with848Topic(t, m, "t", 2)
	join := hb848(m, "g", "a", 0, nil)
	g := m.groups["g"]
	mem := g.members["a"]
	m.mu.Lock()
	// As after a step to epoch+5 whose answer was lost: it was at epoch+2 before.
	mem.epoch, g.epoch, g.targetEpoch = join.memberEpoch+5, join.memberEpoch+5, join.memberEpoch+5
	mem.prevEpoch = join.memberEpoch + 2
	m.mu.Unlock()

	retry := hb848(m, "g", "a", mem.prevEpoch, nil)
	if retry.errorCode != 0 || retry.memberEpoch != mem.epoch || retry.assignment == nil {
		t.Errorf("the retried epoch: %+v, want the current epoch and the assignment again", retry)
	}
	for _, old := range []int32{mem.prevEpoch - 1, mem.prevEpoch + 1} {
		if res := hb848(m, "g", "a", old, nil); res.errorCode != kerr.FencedMemberEpoch.Code {
			t.Errorf("epoch %d (current %d, previous %d): code %d, want FENCED_MEMBER_EPOCH", old, mem.epoch, mem.prevEpoch, res.errorCode)
		}
	}
	// At the current epoch with nothing changed it is a keepalive: no assignment.
	if res := hb848(m, "g", "a", mem.epoch, nil); res.errorCode != 0 || res.assignment != nil {
		t.Errorf("a plain heartbeat after the assignment was sent: %+v, want none", res)
	}
}

// A known member that joins again at epoch 0 has dropped what it held: the group moves on an epoch
// and it is sent everything again.
func TestKIP848ARejoinAtEpochZeroMovesTheGroupOn(t *testing.T) {
	m := new848Manager(t)
	with848Topic(t, m, "t", 2)
	join := hb848(m, "g", "a", 0, nil)
	if join.assignment == nil {
		t.Fatal("the first answer carries the assignment")
	}
	g := m.groups["g"]
	before := g.epoch
	again := hb848(m, "g", "a", 0, nil)
	if again.errorCode != 0 || g.epoch != before+1 || again.assignment == nil {
		t.Errorf("rejoin: %+v, group epoch %d -> %d, want one more and the assignment in full", again, before, g.epoch)
	}
}

// A member that names another assignor moves the group on an epoch; naming its own again does not.
func TestKIP848ChangingTheServerAssignorMovesTheGroupOn(t *testing.T) {
	m := new848Manager(t)
	with848Topic(t, m, "t", 2)
	assignor := func(name string) func(r *kmsg.ConsumerGroupHeartbeatRequest) {
		return func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.ServerAssignor = &name }
	}
	join := hb848(m, "g", "a", 0, assignor("uniform"))
	g := m.groups["g"]
	cur := join.memberEpoch
	before := g.epoch
	if res := hb848(m, "g", "a", cur, assignor("uniform")); res.errorCode != 0 || g.epoch != before {
		t.Fatalf("the same assignor again: %+v, group epoch %d -> %d, want unchanged", res, before, g.epoch)
	}
	if res := hb848(m, "g", "a", cur, assignor("range")); res.errorCode != 0 || g.epoch != before+1 {
		t.Fatalf("another assignor: %+v, group epoch %d -> %d, want one more", res, before, g.epoch)
	}
	if g.members["a"].assignor != "range" {
		t.Errorf("the member's assignor is %q", g.members["a"].assignor)
	}
}

// A topic created after a member subscribed to it moves the group on an epoch, once
// targets exist for it.
func TestKIP848ATopicCreatedLaterMovesTheGroupOn(t *testing.T) {
	m := new848Manager(t)
	join := hb848(m, "g", "a", 0, func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.SubscribedTopicNames = []string{"later"} })
	if join.errorCode != 0 {
		t.Fatalf("join: %+v", join)
	}
	g := m.groups["g"]
	before := g.epoch
	with848Topic(t, m, "later", 3)
	m.topicChanged("later")
	if g.epoch != before+1 || g.targetEpoch != g.epoch || len(g.members["a"].target) != 3 {
		t.Errorf("after the topic: epoch %d -> %d, target epoch %d, %d partitions targeted; want one more epoch and 3 partitions", before, g.epoch, g.targetEpoch, len(g.members["a"].target))
	}
	// A topic nobody wants changes nothing.
	m.topicChanged("unwanted")
	if g.epoch != before+1 {
		t.Errorf("a topic nobody subscribed to moved the epoch to %d", g.epoch)
	}
}

// The targets are due only when the group is behind its epoch.
func TestKIP848TargetsAreNotRecomputedForAnEpochTheyAreAlreadyAt(t *testing.T) {
	m := new848Manager(t)
	with848Topic(t, m, "t", 1)
	hb848(m, "g", "a", 0, nil)
	g := m.groups["g"]
	m.mu.Lock()
	defer m.mu.Unlock()
	if g.targetEpoch != g.epoch {
		t.Fatalf("setup: target epoch %d, epoch %d", g.targetEpoch, g.epoch)
	}
	at := g.assignedAt
	if m.assignIfDue(g) || !g.assignedAt.Equal(at) {
		t.Error("targets were computed again for the epoch they were computed for")
	}
	g.epoch++
	if !m.assignIfDue(g) || g.targetEpoch != g.epoch {
		t.Error("targets were not computed for a new epoch")
	}
}

// An OffsetCommit that names a member is checked against the group; one that names none is
// allowed only while the group has no members (recorded: group-requests-refused).
func TestKIP848CommitChecks(t *testing.T) {
	m := new848Manager(t)
	with848Topic(t, m, "t", 1)
	if code := m.memberCheck("none", "", -1); code != errNone {
		t.Errorf("no group, no member, epoch -1: %d", code)
	}
	if code := m.memberCheck("none", "", 0); code != kerr.UnknownMemberID.Code {
		t.Errorf("no group, no member, epoch 0: %d, want UNKNOWN_MEMBER_ID", code)
	}
	if code := m.memberCheck("none", "x", -1); code != kerr.UnknownMemberID.Code {
		t.Errorf("no group, a member, epoch -1: %d, want UNKNOWN_MEMBER_ID", code)
	}

	join := hb848(m, "g", "a", 0, nil)
	g := m.groups["g"]
	g.members["c"] = &cg848Member{id: "c", classic: &classicSeat{}, epoch: 4, target: tpSet{}, assigned: tpSet{}}
	// Before v9 a consumer-protocol member cannot commit; a classic member in the group can.
	if code := m.validateCommitAt("g", "a", join.memberEpoch, 8); code != kerr.UnsupportedVersion.Code {
		t.Errorf("v8 from a consumer-protocol member: %d, want UNSUPPORTED_VERSION", code)
	}
	if code := m.validateCommitAt("g", "c", 4, 8); code != errNone {
		t.Errorf("v8 from a classic member: %d, want none", code)
	}
	if code := m.validateCommitAt("g", "stranger", 4, 8); code != kerr.UnknownMemberID.Code {
		t.Errorf("v8 from nobody the group has: %d, want UNKNOWN_MEMBER_ID", code)
	}
	if code := m.validateCommitAt("g", "a", join.memberEpoch, 9); code != errNone {
		t.Errorf("v9 at the member's epoch: %d", code)
	}
	if code := m.validateCommitAt("g", "a", join.memberEpoch+1, 9); code != kerr.StaleMemberEpoch.Code {
		t.Errorf("v9 at another epoch: %d, want STALE_MEMBER_EPOCH", code)
	}
	if code := m.validateCommitAt("g", "c", 5, 9); code != kerr.IllegalGeneration.Code {
		t.Errorf("a classic member at another epoch: %d, want ILLEGAL_GENERATION", code)
	}
}

// A heartbeat that names a static instance the sender does not hold is refused in
// Kafka's words (recorded: cs-heartbeat-wrong-instance, cs-old-member-heartbeat).
func TestKIP848StaticInstanceRefusalsSpeakKafkasWords(t *testing.T) {
	m := new848Manager(t)
	with848Topic(t, m, "t", 1)
	instance := func(name string) func(r *kmsg.ConsumerGroupHeartbeatRequest) {
		return func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.InstanceID = &name }
	}
	join := hb848(m, "g", "a", 0, instance("i1"))
	if join.errorCode != 0 {
		t.Fatalf("join: %+v", join)
	}
	res := hb848(m, "g", "b", join.memberEpoch, instance("ghost"))
	if res.errorCode != kerr.UnknownMemberID.Code || res.errorMsg != "Instance id ghost is unknown." {
		t.Errorf("an instance nobody holds: %d %q", res.errorCode, res.errorMsg)
	}
	res = hb848(m, "g", "b", join.memberEpoch, instance("i1"))
	if res.errorCode != kerr.FencedInstanceID.Code || res.errorMsg != "Static member b with instance id i1 was fenced by member a." {
		t.Errorf("an instance another member holds: %d %q", res.errorCode, res.errorMsg)
	}
}

// A static member that leaves for a restart (epoch -2) is shown at epoch -2 while its place is held.
func TestKIP848AStaticMemberAwayForARestartIsShownAtEpochMinusTwo(t *testing.T) {
	m := new848Manager(t)
	with848Topic(t, m, "t", 1)
	name := "i1"
	join := hb848(m, "g", "a", 0, func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.InstanceID = &name })
	left := hb848(m, "g", "a", -2, func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.InstanceID = &name })
	if left.errorCode != 0 || left.memberEpoch != -2 {
		t.Fatalf("leave for a restart: %+v (joined at %d)", left, join.memberEpoch)
	}
	_, _, _, _, members, ok := m.describe("g")
	if !ok || len(members) != 1 || members[0].epoch != -2 {
		t.Errorf("described: %+v ok %v, want the one member at epoch -2", members, ok)
	}
}

// A group put back at start is not put over one that is there, and with no assignor is uniform.
func TestKIP848RestoreEmptyOnlyCreatesAndDefaultsToUniform(t *testing.T) {
	m := new848Manager(t)
	m.restoreEmpty("g", 6, 5, "")
	g := m.groups["g"]
	if g == nil || g.epoch != 6 || g.targetEpoch != 5 || g.assignor != assignorUniform || len(g.members) != 0 {
		t.Fatalf("restored: %+v", g)
	}
	m.restoreEmpty("g", 9, 9, assignorRange)
	if m.groups["g"] != g || g.epoch != 6 || g.assignor != assignorUniform {
		t.Errorf("a group already there was changed: %+v", g)
	}
	m.restoreEmpty("h", 2, 2, assignorRange)
	if m.groups["h"].assignor != assignorRange {
		t.Errorf("an assignor given was not kept: %q", m.groups["h"].assignor)
	}
}

// A member whose rebalance timeout is zero is given its session timeout to give up
// what it is told to, not none at all (zero is a rebalance timeout, recorded:
// ch-rebalance-timeout-0).
func TestKIP848ARevocationWaitsAMemberWithNoRebalanceTimeoutForItsSession(t *testing.T) {
	m := new848Manager(t)
	topic := with848Topic(t, m, "t", 2)
	zero := func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.RebalanceTimeoutMillis = 0 }
	join := hb848(m, "g", "a", 0, zero)
	if join.errorCode != 0 || join.assignment == nil {
		t.Fatalf("a joins: %+v", join)
	}
	// b joins; a's next heartbeat, owning both partitions, is told to give one up.
	hb848(m, "g", "b", 0, nil)
	owned := func(r *kmsg.ConsumerGroupHeartbeatRequest) {
		rt := kmsg.NewConsumerGroupHeartbeatRequestTopic()
		rt.TopicID, rt.Partitions = topic.ID, []int32{0, 1}
		r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{rt}
	}
	res := hb848(m, "g", "a", join.memberEpoch, owned)
	if res.errorCode != 0 {
		t.Fatalf("a's heartbeat: %+v", res)
	}
	m.mu.Lock()
	revoking := len(m.groups["g"].members["a"].revoking)
	m.mu.Unlock()
	if revoking != 1 {
		t.Fatalf("a was not told to give anything up (revoking %d)", revoking)
	}
	time.Sleep(150 * time.Millisecond)
	m.mu.Lock()
	still := m.groups["g"].members["a"] != nil
	m.mu.Unlock()
	if !still {
		t.Error("a member with no rebalance timeout was removed at once for not giving up what it was told to")
	}
}
