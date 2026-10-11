package broker

import (
	"context"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The mixed-group coordinator driven directly, on timers short enough to watch: the
// wire-level flows are in conformance (what Kafka answers) and group_kip848_edges_test.go
// (a real connection); these are what happens when nobody speaks.

func shortSessions(t *testing.T) {
	t.Helper()
	oldMin := minSessionTimeout
	minSessionTimeout = 10 * time.Millisecond
	t.Cleanup(func() { minSessionTimeout = oldMin })
	noDelay(t)
}

// withPartitions makes the rig's topic have n partitions and returns its id.
func (r *expiryRig) withPartitions(t *testing.T, topic string, n int32) [16]byte {
	t.Helper()
	if n > 1 {
		if _, err := r.m.SetPartitions(topic, n); err != nil {
			t.Fatal(err)
		}
	}
	tp, ok, err := r.m.Topic(topic)
	if err != nil || !ok {
		t.Fatalf("topic %q: %v %v", topic, ok, err)
	}
	return tp.ID
}

func (r *expiryRig) join848(t *testing.T, group, member, topic string) cg848Result {
	t.Helper()
	req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req.Group, req.MemberID, req.MemberEpoch = group, member, 0
	req.RebalanceTimeoutMillis = 60_000
	req.SubscribedTopicNames = []string{topic}
	req.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	res := r.b.cg848.heartbeat(req, clientRef{"c", "h"})
	if res.errorCode != 0 {
		t.Fatalf("848 join: %+v", res)
	}
	return res
}

func (r *expiryRig) hb848(group, member string, epoch int32, owned map[[16]byte][]int32) cg848Result {
	req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req.Group, req.MemberID, req.MemberEpoch = group, member, epoch
	if owned != nil {
		req.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
		for id, ps := range owned {
			rt := kmsg.NewConsumerGroupHeartbeatRequestTopic()
			rt.TopicID, rt.Partitions = id, ps
			req.Topics = append(req.Topics, rt)
		}
	}
	return r.b.cg848.heartbeat(req, clientRef{"c", "h"})
}

func joinReqSeat(group, member string, session, rebalance time.Duration, ptype string, meta []byte) *kmsg.JoinGroupRequest {
	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(7)
	req.Group, req.MemberID, req.ProtocolType = group, member, ptype
	req.SessionTimeoutMillis, req.RebalanceTimeoutMillis = int32(session/time.Millisecond), int32(rebalance/time.Millisecond)
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name, p.Metadata = "range", meta
	req.Protocols = append(req.Protocols, p)
	return req
}

func (r *expiryRig) classicJoinSeat(t *testing.T, group, member string, session, rebalance time.Duration, topic string) joinResult {
	t.Helper()
	res, handled := r.b.cg848.classicJoin(joinReqSeat(group, member, session, rebalance, "consumer", subscriptionBytes(topic)), clientRef{"c", "h"})
	if !handled {
		t.Fatal("the consumer group did not take the classic join")
	}
	return res
}

// holding establishes a classic group of one member whose leader gave it these
// partitions of topic, and returns the member and its generation.
func (r *expiryRig) holding(t *testing.T, group, topic string, session, rebalance time.Duration, partitions ...int32) (string, int32) {
	t.Helper()
	ctx := context.Background()
	first, err := r.b.groups.join(ctx, joinReqSeat(group, "", session, rebalance, "consumer", subscriptionBytes(topic)), clientRef{"c", "h"})
	if err != nil || first.errorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("first join: %v %+v", err, first)
	}
	res, err := r.b.groups.join(ctx, joinReqSeat(group, first.memberID, session, rebalance, "consumer", subscriptionBytes(topic)), clientRef{"c", "h"})
	if err != nil || res.errorCode != 0 {
		t.Fatalf("join: %v %+v", err, res)
	}
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.MemberID, sync.Generation = group, res.memberID, res.generation
	a := kmsg.NewSyncGroupRequestGroupAssignment()
	a.MemberID, a.MemberAssignment = res.memberID, encodeClassicAssignment(map[string][]int32{topic: partitions})
	sync.GroupAssignment = append(sync.GroupAssignment, a)
	if sr, err := r.b.groups.sync(ctx, sync); err != nil || sr.errorCode != 0 {
		t.Fatalf("sync: %v %+v", err, sr)
	}
	return res.memberID, res.generation
}

func (r *expiryRig) classicHeartbeat(group, member string, gen int32) (int16, bool) {
	h := kmsg.NewPtrHeartbeatRequest()
	h.Group, h.MemberID, h.Generation = group, member, gen
	return r.b.cg848.classicHeartbeat(h)
}

func (r *expiryRig) members(group string) map[string]bool {
	r.b.cg848.mu.Lock()
	defer r.b.cg848.mu.Unlock()
	out := map[string]bool{}
	if g := r.b.cg848.groups[group]; g != nil {
		for id, m := range g.members {
			out[id] = m.classic != nil
		}
	}
	return out
}

// Every refusal a classic request to a consumer group can get.
func TestMixedGroupRefusals(t *testing.T) {
	shortSessions(t)
	r := newExpiryRig(t, t.TempDir(), time.Now(), "t")
	r.join848(t, "g", "n", "t")

	req := func(session time.Duration, ptype, member string, meta []byte) joinResult {
		res, handled := r.b.cg848.classicJoin(joinReqSeat("g", member, session, time.Second, ptype, meta), clientRef{})
		if !handled {
			t.Fatal("not handled")
		}
		return res
	}
	if res := req(time.Millisecond, "consumer", "", subscriptionBytes("t")); res.errorCode != kerr.InvalidSessionTimeout.Code {
		t.Errorf("a session below the minimum: %d", res.errorCode)
	}
	if res := req(time.Second, "connect", "", subscriptionBytes("t")); res.errorCode != kerr.InconsistentGroupProtocol.Code {
		t.Errorf("another protocol type: %d", res.errorCode)
	}
	if res := req(time.Second, "consumer", "", []byte{9}); res.errorCode != kerr.InconsistentGroupProtocol.Code {
		t.Errorf("a subscription that is not the consumer protocol's: %d", res.errorCode)
	}
	if res := req(time.Second, "consumer", "n", subscriptionBytes("t")); res.errorCode != kerr.UnknownMemberID.Code {
		t.Errorf("a consumer-protocol member's id: %d", res.errorCode)
	}
	if res := req(time.Second, "consumer", "never-issued", subscriptionBytes("t")); res.errorCode != kerr.UnknownMemberID.Code {
		t.Errorf("an id nobody was given: %d", res.errorCode)
	}
	first := req(time.Second, "consumer", "", subscriptionBytes("t"))
	if first.errorCode != kerr.MemberIDRequired.Code || first.memberID == "" {
		t.Fatalf("a first join: %+v", first)
	}
	joined := r.classicJoinSeat(t, "g", first.memberID, time.Second, time.Second, "t")
	if joined.errorCode != 0 || joined.leaderID != "" || len(joined.members) != 0 {
		t.Fatalf("the join with the id: %+v", joined)
	}
	if res := req(time.Second, "consumer", first.memberID, subscriptionBytes("t")); res.errorCode != 0 {
		t.Errorf("the same member rejoining: %d", res.errorCode)
	}
	// Old clients (JoinGroup below v4) are given an id and joined in one step.
	old := joinReqSeat("g", "", time.Second, time.Second, "consumer", subscriptionBytes("t"))
	old.SetVersion(3)
	if res, handled := r.b.cg848.classicJoin(old, clientRef{}); !handled || res.errorCode != 0 || res.memberID == "" {
		t.Errorf("a v3 join: %v %+v", handled, res)
	}

	if code, _ := r.classicHeartbeat("g", "nobody", 1); code != kerr.UnknownMemberID.Code {
		t.Errorf("heartbeat of an unknown member: %d", code)
	}
	if code, _ := r.classicHeartbeat("g", "n", 1); code != kerr.UnknownMemberID.Code {
		t.Errorf("heartbeat of a consumer-protocol member by the classic API: %d", code)
	}
	if code, _ := r.classicHeartbeat("g", first.memberID, 999); code != kerr.IllegalGeneration.Code {
		t.Errorf("heartbeat of an old generation: %d", code)
	}
	if _, handled := r.classicHeartbeat("no-such-group", "x", 1); handled {
		t.Error("a heartbeat for a group the consumer coordinator does not have was handled")
	}

	sync := func(member string, gen int32) int16 {
		s := kmsg.NewPtrSyncGroupRequest()
		s.Group, s.MemberID, s.Generation = "g", member, gen
		_, code, handled := r.b.cg848.classicSync(s)
		if !handled {
			t.Fatal("not handled")
		}
		return code
	}
	if code := sync("nobody", 1); code != kerr.UnknownMemberID.Code {
		t.Errorf("sync of an unknown member: %d", code)
	}
	if code := sync(first.memberID, 999); code != kerr.IllegalGeneration.Code {
		t.Errorf("sync of an old generation: %d", code)
	}
	bad := kmsg.NewPtrSyncGroupRequest()
	bad.Group = "no-such-group"
	if _, _, handled := r.b.cg848.classicSync(bad); handled {
		t.Error("a sync for a group the consumer coordinator does not have was handled")
	}

	lv := kmsg.NewPtrLeaveGroupRequest()
	lv.Group = "g"
	for _, id := range []string{"nobody", first.memberID} {
		m := kmsg.NewLeaveGroupRequestMember()
		m.MemberID = id
		lv.Members = append(lv.Members, m)
	}
	results, handled := r.b.cg848.classicLeave(lv)
	if !handled || len(results) != 2 || results[0].errorCode != kerr.UnknownMemberID.Code || results[1].errorCode != 0 {
		t.Fatalf("leave: %v %+v", handled, results)
	}
	gone := kmsg.NewPtrLeaveGroupRequest()
	gone.Group = "no-such-group"
	if _, handled := r.b.cg848.classicLeave(gone); handled {
		t.Error("a leave for a group the consumer coordinator does not have was handled")
	}
}

// A classic member that holds a partition it is no longer to have gives it up by
// rejoining, and one that does not within its rebalance timeout is removed, so the new
// owner is not kept waiting for ever.
func TestMixedGroupClassicMemberThatNeverRejoinsIsRemoved(t *testing.T) {
	shortSessions(t)
	r := newExpiryRig(t, t.TempDir(), time.Now(), "t")
	tid := r.withPartitions(t, "t", 2)

	member, gen := r.holding(t, "g", "t", 30*time.Second, 150*time.Millisecond, 0, 1)

	n := r.join848(t, "g", "n", "t")
	if code, handled := r.classicHeartbeat("g", member, gen); !handled || code != kerr.RebalanceInProgress.Code {
		t.Fatalf("the classic member's heartbeat: %d %v, want REBALANCE_IN_PROGRESS", code, handled)
	}
	epoch := n.memberEpoch
	held := func(res cg848Result) int {
		if res.assignment == nil {
			return -1
		}
		c := 0
		for _, tt := range res.assignment.Topics {
			c += len(tt.Partitions)
		}
		return c
	}
	first := r.hb848("g", "n", epoch, nil)
	if first.errorCode != 0 || held(first) > 0 {
		t.Fatalf("the new member while the classic one holds both: %+v", first)
	}

	// It does not rejoin; its rebalance timeout runs out, and the new member gets both.
	deadline := time.Now().Add(4 * time.Second)
	got := 0
	for time.Now().Before(deadline) && got != 2 {
		time.Sleep(40 * time.Millisecond)
		res := r.hb848("g", "n", epoch, map[[16]byte][]int32{})
		if res.errorCode != 0 {
			t.Fatalf("heartbeat: %+v", res)
		}
		epoch = res.memberEpoch
		if c := held(res); c >= 0 {
			got = c
		}
	}
	_ = tid
	if got != 2 {
		t.Errorf("the member that stayed holds %d partitions, want both once the classic one is gone", got)
	}
	if m := r.members("g"); len(m) != 1 || m[member] {
		t.Errorf("members after the timeout: %v, want only the consumer-protocol one", m)
	}
}

// A classic member that stops heartbeating is expired by its own session, and when the
// last consumer-protocol member leaves, what is left is a classic group.
func TestMixedGroupSessionExpiryAndDowngrade(t *testing.T) {
	shortSessions(t)
	r := newExpiryRig(t, t.TempDir(), time.Now(), "t")
	r.withPartitions(t, "t", 2)

	silent, _ := r.holding(t, "g", "t", 150*time.Millisecond, time.Second, 0)
	n := r.join848(t, "g", "n", "t")
	if m := r.members("g"); len(m) != 2 || !m[silent] {
		t.Fatalf("after the upgrade: %v", m)
	}
	// The silent one's own session is what removes it (the other keeps heartbeating).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(r.members("g")) != 1 {
		time.Sleep(30 * time.Millisecond)
		r.hb848("g", "n", n.memberEpoch, nil)
	}
	if m := r.members("g"); len(m) != 1 || m[silent] {
		t.Fatalf("after the silent member's session: %v", m)
	}

	// A classic member joins what is a consumer group, then the consumer member leaves.
	first := r.classicJoinSeat(t, "g", "", time.Minute, time.Minute, "t")
	b := r.classicJoinSeat(t, "g", first.memberID, time.Minute, time.Minute, "t")
	if b.errorCode != 0 {
		t.Fatalf("classic join: %+v", b)
	}
	if res := r.hb848("g", "n", -1, nil); res.errorCode != 0 {
		t.Fatalf("leave: %+v", res)
	}
	if owner := r.b.claims.ownerOf("g"); owner != protoClassic {
		t.Fatalf("the group is owned by %q, want classic", owner)
	}
	r.b.groups.mu.Lock()
	g := r.b.groups.groups["g"]
	state, members := stateEmpty, 0
	if g != nil {
		state, members = g.state, len(g.members)
	}
	r.b.groups.mu.Unlock()
	if g == nil || state != statePreparingRebalance || members != 1 {
		t.Fatalf("the classic group: %v state %v members %d", g != nil, state, members)
	}
	if code := r.b.groups.heartbeat(func() *kmsg.HeartbeatRequest {
		h := kmsg.NewPtrHeartbeatRequest()
		h.Group, h.MemberID, h.Generation = "g", first.memberID, b.generation
		return h
	}()); code != kerr.IllegalGeneration.Code {
		t.Errorf("the member's heartbeat of the old generation: %d, want ILLEGAL_GENERATION", code)
	}
	if m := r.members("g"); len(m) != 0 {
		t.Errorf("the consumer coordinator still has %v", m)
	}
}

// A group of classic members that is not of protocol type "consumer", or whose members
// do not subscribe as consumers, is not moved: the consumer-protocol join is refused.
func TestMixedGroupOnlyConsumerGroupsAreTaken(t *testing.T) {
	shortSessions(t)
	r := newExpiryRig(t, t.TempDir(), time.Now(), "t")
	ctx := context.Background()
	for _, tc := range []struct{ group, ptype string }{{"connect-group", "connect"}} {
		first, err := r.b.groups.join(ctx, joinReqSeat(tc.group, "", time.Second, time.Second, tc.ptype, subscriptionBytes("t")), clientRef{})
		if err != nil || first.errorCode != kerr.MemberIDRequired.Code {
			t.Fatalf("%v %+v", err, first)
		}
		if res, err := r.b.groups.join(ctx, joinReqSeat(tc.group, first.memberID, time.Second, time.Second, tc.ptype, subscriptionBytes("t")), clientRef{}); err != nil || res.errorCode != 0 {
			t.Fatalf("%v %+v", err, res)
		}
		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group, req.MemberID, req.RebalanceTimeoutMillis = tc.group, "n", 1000
		req.SubscribedTopicNames = []string{"t"}
		req.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
		res := r.b.cg848.heartbeat(req, clientRef{})
		if res.errorCode != kerr.GroupIDNotFound.Code || res.errorMsg == "" {
			t.Errorf("%s: join into a %s group: %+v, want GROUP_ID_NOT_FOUND with the reason", tc.group, tc.ptype, res)
		}
	}
	if owner := r.b.claims.ownerOf("connect-group"); owner != protoClassic {
		t.Errorf("a refused join changed the owner to %q", owner)
	}
}
