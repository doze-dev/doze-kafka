package conformance

import (
	"encoding/binary"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Classic members of a consumer group that are not plain eager, dynamic consumers
// (GRP-8): one that keeps its partitions across a rejoin, and a static one that restarts.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "mixed-protocol-cooperative", Run: mixedProtocolCooperative, Known: mixedCooperativeKnown()},
		Scenario{Name: "mixed-protocol-static", Run: mixedProtocolStatic, Known: mixedStaticKnown()},
	)
}

// What these scenarios differ in is what every mixed group does (scenarios_groups.go):
// the interval a consumer-protocol member is asked to heartbeat at.

func mixedCooperativeKnown() []Known {
	k := knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "co-n-join", "co-n-heartbeat-1", "v3-n-join",
		"co-n-heartbeat-after-1a", "co-n-heartbeat-after-1b", "co-n-heartbeat-after-2a", "co-n-heartbeat-after-2b",
		"co-n-heartbeat-after-3a", "co-n-heartbeat-after-3b")
	return k
}

func mixedStaticKnown() []Known {
	k := knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "sd-n-join", "sd-n-heartbeat", "su-n-join", "su-n-heartbeat", "sh-n-join-static")
	return k
}

// consumerSubscriptionOwning is the classic consumer protocol's subscription at version
// 1: the topic, no user data, and the partitions of it the member says it still owns,
// which is how a cooperative consumer keeps them across a rejoin.
func consumerSubscriptionOwning(topic string, owned ...int32) []byte {
	b := binary.BigEndian.AppendUint16(nil, 1)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint16(b, uint16(len(topic)))
	b = append(b, topic...)
	b = binary.BigEndian.AppendUint32(b, 0xffffffff)
	if len(owned) == 0 {
		return binary.BigEndian.AppendUint32(b, 0)
	}
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint16(b, uint16(len(topic)))
	b = append(b, topic...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(owned)))
	for _, p := range owned {
		b = binary.BigEndian.AppendUint32(b, uint32(p))
	}
	return b
}

// consumerSubscriptionV3 is the same at version 3, owning nothing: version 2 added the
// generation the member was last in (-1: none) and version 3 its rack (null).
func consumerSubscriptionV3(topic string) []byte {
	b := consumerSubscriptionOwning(topic)
	binary.BigEndian.PutUint16(b, 3)
	b = binary.BigEndian.AppendUint32(b, 0xffffffff)
	return binary.BigEndian.AppendUint16(b, 0xffff)
}

// assignedPartitions reads the partitions of the one topic in a classic assignment.
func assignedPartitions(b []byte) []int32 {
	if len(b) < 6 || binary.BigEndian.Uint32(b[2:]) == 0 {
		return nil
	}
	b = b[6:]
	if len(b) < 2 {
		return nil
	}
	l := int(binary.BigEndian.Uint16(b))
	if len(b) < 2+l+4 {
		return nil
	}
	b = b[2+l:]
	n := int(binary.BigEndian.Uint32(b))
	b = b[4:]
	var out []int32
	for i := 0; i < n && len(b) >= 4; i++ {
		out = append(out, int32(binary.BigEndian.Uint32(b)))
		b = b[4:]
	}
	return out
}

// mixedProtocolCooperative is a classic member that keeps its partitions across a rejoin,
// as a cooperative-sticky consumer does (its JoinGroup says which it still owns), in a
// group with a consumer-protocol member: when the partitions it is to lose are free for
// the other member.
func mixedProtocolCooperative(e *Env) {
	e.Do("create", createTopics(false, newTopic("mc", 4, 1), newTopic("mc3", 1, 1)), 7)
	e.findCoordinators("mc-coop", "mc-v3")
	const group, proto = "mc-coop", "cooperative-sticky"
	spec := joinSpec{group: group, protocol: proto, metadata: consumerSubscriptionOwning("mc")}
	spec.member = e.join("co-a-join-needs-id", spec).MemberID
	a := spec.member
	j := e.join("co-a-join", spec)
	e.Do("co-a-sync", classicSyncRequest(group, a, "", j.Generation, proto, consumerAssignment("mc", 0, 1, 2, 3)), 5)
	owned := []int32{0, 1, 2, 3}

	n := e.consumerHeartbeat("co-n-join", group, "", 0, "mc")
	nid, epoch := deref(n.MemberID), n.MemberEpoch
	nbeat := func(label string) {
		if h := e.consumerHeartbeat(label, group, nid, epoch, "mc"); h.ErrorCode == 0 && h.MemberEpoch > epoch {
			epoch = h.MemberEpoch
		}
	}
	nbeat("co-n-heartbeat-1")
	e.Do("co-a-heartbeat-1", classicHeartbeatRequest(group, a, "", j.Generation), 4)

	// Three rounds of what a cooperative client does when it is told to rejoin: it joins
	// saying what it owns, and owns from then on what its SyncGroup gave it.
	for _, round := range []string{"1", "2", "3"} {
		spec.metadata = consumerSubscriptionOwning("mc", owned...)
		j = e.join("co-a-rejoin-"+round, spec)
		s := e.Do("co-a-resync-"+round, classicSyncRequest(group, a, "", j.Generation, proto, nil), 5).(*kmsg.SyncGroupResponse)
		if s.ErrorCode == 0 {
			owned = assignedPartitions(s.MemberAssignment)
		}
		nbeat("co-n-heartbeat-after-" + round + "a")
		nbeat("co-n-heartbeat-after-" + round + "b")
		e.Do("co-a-heartbeat-after-"+round, classicHeartbeatRequest(group, a, "", j.Generation), 4)
		e.describeConsumer("co-describe-after-"+round, group)
	}
	// And a group nobody has ever used, for the words of "no such group".
	e.describeConsumer("co-describe-nobody", "mc-nobody")

	// Which version the assignment a classic member is given is written at: one member
	// whose subscription is at version 3, in a consumer group whose other member holds
	// every partition, so that the assignment is the empty one.
	e.consumerHeartbeat("v3-n-join", "mc-v3", "", 0, "mc3")
	v3 := joinSpec{group: "mc-v3", protocol: proto, metadata: consumerSubscriptionV3("mc3")}
	v3.member = e.join("v3-c-join-needs-id", v3).MemberID
	jv := e.join("v3-c-join", v3)
	e.Do("v3-c-sync", classicSyncRequest("mc-v3", v3.member, "", jv.Generation, proto, nil), 5)
}

// mixedProtocolStatic is a classic static member (group.instance.id) of a consumer group
// that restarts: it joins again with its instance id and no member id.
//
//	ms-down   a consumer-protocol member first, then the static classic one.
//	ms-up     the static classic member first, then a consumer-protocol member.
func mixedProtocolStatic(e *Env) {
	e.Do("create", createTopics(false, newTopic("ms", 2, 1)), 7)
	e.findCoordinators("ms-down", "ms-up", "ms-held")
	sub := consumerSubscription("ms")

	// joinStatic joins instance i1 with no member id, as a starting client does.
	joinStatic := func(prefix, group string) (string, int32) {
		spec := joinSpec{group: group, instance: "i1", metadata: sub}
		j := e.join(prefix+"-join", spec)
		if j.ErrorCode == 79 {
			spec.member = j.MemberID
			j = e.join(prefix+"-join-with-id", spec)
		}
		return j.MemberID, j.Generation
	}
	restart := func(prefix, group, old string, oldGen int32) {
		id, gen := joinStatic(prefix+"-s2", group)
		e.Do(prefix+"-s2-sync", classicSyncRequest(group, id, "i1", gen, "range", nil), 5)
		e.Do(prefix+"-s2-heartbeat", classicHeartbeatRequest(group, id, "i1", gen), 4)
		e.Do(prefix+"-s1-heartbeat-old-id", classicHeartbeatRequest(group, old, "i1", oldGen), 4)
		e.Do(prefix+"-s1-heartbeat-old-id-no-instance", classicHeartbeatRequest(group, old, "", oldGen), 4)
		e.describeConsumer(prefix+"-describe-after-restart", group)
		// What else the old incarnation may still send, with the instance id it has.
		commit := kmsg.NewPtrOffsetCommitRequest()
		commit.Group, commit.MemberID, commit.Generation, commit.InstanceID = group, old, oldGen, str("i1")
		ct := kmsg.NewOffsetCommitRequestTopic()
		ct.Topic = "ms"
		cp := kmsg.NewOffsetCommitRequestTopicPartition()
		cp.Partition, cp.Offset, cp.LeaderEpoch = 0, 1, -1
		ct.Partitions = append(ct.Partitions, cp)
		commit.Topics = append(commit.Topics, ct)
		e.Do(prefix+"-s1-commit-old-id", commit, 8)
		e.Do(prefix+"-s1-sync-old-id", classicSyncRequest(group, old, "i1", oldGen, "range", nil), 5)
		e.join(prefix+"-s1-join-old-id", joinSpec{group: group, member: old, instance: "i1", metadata: sub})
		e.describeConsumer(prefix+"-describe-after-old-id", group)
	}

	n := e.consumerHeartbeat("sd-n-join", "ms-down", "", 0, "ms")
	e.consumerHeartbeat("sd-n-heartbeat", "ms-down", deref(n.MemberID), n.MemberEpoch, "ms")
	s1, gen := joinStatic("sd-s1", "ms-down")
	e.Do("sd-s1-sync", classicSyncRequest("ms-down", s1, "i1", gen, "range", nil), 5)
	e.describeConsumer("sd-describe", "ms-down")
	restart("sd", "ms-down", s1, gen)

	s1, gen = joinStatic("su-s1", "ms-up")
	e.Do("su-s1-sync", classicSyncRequest("ms-up", s1, "i1", gen, "range", consumerAssignment("ms", 0, 1)), 5)
	n = e.consumerHeartbeat("su-n-join", "ms-up", "", 0, "ms")
	e.consumerHeartbeat("su-n-heartbeat", "ms-up", deref(n.MemberID), n.MemberEpoch, "ms")
	e.describeConsumer("su-describe", "ms-up")
	restart("su", "ms-up", s1, gen)

	// The instance leaves by its instance id, as an admin tool removes a static member.
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group = "ms-up"
	lm := kmsg.NewLeaveGroupRequestMember()
	lm.InstanceID = str("i1")
	leave.Members = append(leave.Members, lm)
	e.Do("su-leave-by-instance", leave, 4)
	e.describeConsumer("su-describe-after-leave", "ms-up")

	// An instance id held by a consumer-protocol member, and a classic client that
	// starts with the same one.
	hb := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb.Group, hb.InstanceID, hb.MemberEpoch, hb.RebalanceTimeoutMillis = "ms-held", str("j1"), 0, 30000
	hb.SubscribedTopicNames, hb.ServerAssignor = []string{"ms"}, str("uniform")
	hb.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	e.Do("sh-n-join-static", hb, 0)
	e.join("sh-classic-join-same-instance", joinSpec{group: "ms-held", instance: "j1", metadata: sub})
	e.describeConsumer("sh-describe", "ms-held")
}
