package conformance

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Group requests Kafka refuses, or answers in a way of its own, that a client gets wrong
// or a tool sends by mistake: an empty group id on every group API, a classic JoinGroup
// that names no protocol type, the checks a ConsumerGroupHeartbeat is put through, what a
// classic member of a consumer group may and may not send, and static membership.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "group-requests-refused", Run: groupRequestsRefused, Known: groupRefusedKnown()},
	)
}

func groupRefusedKnown() []Known {
	k := knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "ch-rebalance-timeout-0", "cm-n-join",
		"cm-n-heartbeat", "cm-classic-member-sends-consumer-heartbeat", "ca-n-join", "cs-join", "cs-heartbeat",
		"cs-rejoin-same-instance", "cs-rejoin-after-leave")
	return k
}

// consumerJoinRequest is a ConsumerGroupHeartbeat v0 joining group with topic, owning
// nothing, at a rebalance timeout of 30 s.
func consumerJoinRequest(group, instance, topic string) *kmsg.ConsumerGroupHeartbeatRequest {
	r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	r.Group, r.RebalanceTimeoutMillis = group, 30000
	if instance != "" {
		r.InstanceID = str(instance)
	}
	r.SubscribedTopicNames, r.ServerAssignor = []string{topic}, str("uniform")
	r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	return r
}

// consumerBeatRequest is a ConsumerGroupHeartbeat v0 of a member that has joined.
func consumerBeatRequest(group, member, instance string, epoch int32) *kmsg.ConsumerGroupHeartbeatRequest {
	r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	r.Group, r.MemberID, r.MemberEpoch = group, member, epoch
	if instance != "" {
		r.InstanceID = str(instance)
	}
	return r
}

func groupRequestsRefused(e *Env) {
	e.Do("create", createTopics(false, newTopic("gr", 2, 1)), 7)
	e.findCoordinators("gr-ept", "gr-c", "gr-mix", "gr-ca", "gr-st", "gr-cs", "gr-ei", "gr-eic")
	sub := consumerSubscription("gr")

	// --- An empty group id, on every group API.
	for _, v := range []int16{0, 4, 9} {
		r := classicJoinRequest(joinSpec{group: "", metadata: sub})
		e.Do("ej-join-v"+string(rune('0'+v)), r, v)
	}
	short := classicJoinRequest(joinSpec{group: "", session: 1000, metadata: sub})
	e.Do("ej-join-bad-session", short, 7) // which comes first, the group or the session.
	e.Do("ej-heartbeat", classicHeartbeatRequest("", "Qx7aNoSuchMember", "", 1), 4)
	e.Do("ej-sync", classicSyncRequest("", "Qx7aNoSuchMember", "", 1, "range", nil), 5)
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Members = append(leave.Members, kmsg.NewLeaveGroupRequestMember())
	e.Do("ej-leave-default", leave, 4)
	e.Do("ej-leave-v0", kmsg.NewPtrLeaveGroupRequest(), 0)
	e.Do("ej-offset-delete", offsetDeleteRequest("", "gr"), 0)

	// --- A classic JoinGroup with no protocol type.
	ept := classicJoinRequest(joinSpec{group: "gr-ept", metadata: sub})
	ept.ProtocolType = ""
	e.Do("ept-join", ept, 7)
	// And one with an empty instance id: static, or not?
	ei := classicJoinRequest(joinSpec{group: "gr-ei", metadata: sub})
	ei.InstanceID = str("")
	e.Do("ei-join-empty-instance", ei, 7)

	// --- What a ConsumerGroupHeartbeat is checked for.
	owned := consumerJoinRequest("gr-c", "", "gr")
	owned.Topics = nil
	e.Do("ch-owned-null-on-join", owned, 0)
	owned = consumerJoinRequest("gr-c", "", "gr")
	t := kmsg.NewConsumerGroupHeartbeatRequestTopic()
	t.Partitions = []int32{0}
	owned.Topics = append(owned.Topics, t)
	e.Do("ch-owned-nonempty-on-join", owned, 0)
	e.Do("ch-unknown-member-epoch-5", consumerBeatRequest("gr-nobody", "Qx7aNoSuchMember", "", 5), 0)
	e.Do("ch-static-leave-no-instance", consumerBeatRequest("gr-nobody", "Qx7aNoSuchMember", "", -2), 0)
	rt0 := consumerJoinRequest("gr-c", "", "gr")
	rt0.RebalanceTimeoutMillis = 0
	e.Do("ch-rebalance-timeout-0", rt0, 0)

	// --- A consumer group with one member of each protocol.
	n := e.Do("cm-n-join", consumerJoinRequest("gr-mix", "", "gr"), 0).(*kmsg.ConsumerGroupHeartbeatResponse)
	nid := deref(n.MemberID)
	e.Do("cm-n-heartbeat", consumerBeatRequest("gr-mix", nid, "", n.MemberEpoch), 0)
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group, commit.MemberID, commit.Generation = "gr-mix", nid, n.MemberEpoch
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = "gr"
	cp := kmsg.NewOffsetCommitRequestTopicPartition()
	cp.Offset, cp.LeaderEpoch = 0, -1
	ct.Partitions = append(ct.Partitions, cp)
	commit.Topics = append(commit.Topics, ct)
	e.Do("cm-commit-v8-consumer-member", commit, 8)
	c := e.join("cm-classic-join-needs-id", joinSpec{group: "gr-mix", metadata: sub})
	j := e.join("cm-classic-join", joinSpec{group: "gr-mix", member: c.MemberID, metadata: sub})
	// With a classic member there, a protocol it does not share.
	e.join("cm-classic-join-custom-assignor", joinSpec{group: "gr-mix", protocol: "my-own-assignor", metadata: sub})
	none := classicJoinRequest(joinSpec{group: "gr-mix", metadata: sub})
	none.Protocols = nil
	e.Do("cm-classic-join-no-protocols", none, 7)
	ept = classicJoinRequest(joinSpec{group: "gr-mix", metadata: sub})
	ept.ProtocolType = ""
	e.Do("cm-classic-join-empty-protocol-type", ept, 7)
	bad := e.join("cm-classic-join-unreadable-subscription", joinSpec{group: "gr-mix", metadata: []byte{0, 1, 2}})
	e.join("cm-classic-join-unreadable-subscription-with-id", joinSpec{group: "gr-mix", member: bad.MemberID, metadata: []byte{0, 1, 2}})
	wrongType := classicSyncRequest("gr-mix", c.MemberID, "", j.Generation, "range", nil)
	wrongType.ProtocolType = str("connect")
	e.Do("cm-sync-wrong-protocol-type", wrongType, 5)
	e.Do("cm-sync-wrong-protocol-name", classicSyncRequest("gr-mix", c.MemberID, "", j.Generation, "roundrobin", nil), 5)
	e.Do("cm-sync-unknown-member", classicSyncRequest("gr-mix", "Qx7cNotAMember", "", j.Generation, "range", nil), 5)
	e.Do("cm-sync", classicSyncRequest("gr-mix", c.MemberID, "", j.Generation, "range", nil), 5)
	e.Do("cm-classic-member-sends-consumer-heartbeat", consumerBeatRequest("gr-mix", c.MemberID, "", j.Generation), 0)
	// A member id this group never handed out.
	e.join("cm-classic-join-unknown-member-id", joinSpec{group: "gr-mix", member: "Qx7bNeverIssued", metadata: sub})
	lu := kmsg.NewPtrLeaveGroupRequest()
	lu.Group = "gr-mix"
	m := kmsg.NewLeaveGroupRequestMember()
	m.MemberID = "Qx7cNotAMember"
	lu.Members = append(lu.Members, m)
	e.Do("cm-leave-unknown-member", lu, 4)
	ln := kmsg.NewPtrLeaveGroupRequest()
	ln.Group = "gr-mix"
	m.MemberID = nid
	ln.Members = append(ln.Members, m)
	e.Do("cm-classic-leave-consumer-member", ln, 4)
	e.describeConsumer("cm-describe-after-leave", "gr-mix")

	// A custom assignor as the first classic member of a consumer group.
	e.Do("ca-n-join", consumerJoinRequest("gr-ca", "", "gr"), 0)
	ca := e.join("ca-classic-join-custom-assignor", joinSpec{group: "gr-ca", protocol: "my-own-assignor", metadata: sub})
	e.join("ca-classic-join-custom-assignor-with-id", joinSpec{group: "gr-ca", member: ca.MemberID, protocol: "my-own-assignor", metadata: sub})
	e.describeConsumer("ca-describe", "gr-ca")

	// --- Static membership: a member that names an instance it does not hold.
	s := e.join("st-join", joinSpec{group: "gr-st", instance: "i1", metadata: sub})
	e.Do("st-sync", classicSyncRequest("gr-st", s.MemberID, "i1", s.Generation, "range", consumerAssignment("gr", 0, 1)), 5)
	e.Do("st-heartbeat-wrong-instance", classicHeartbeatRequest("gr-st", s.MemberID, "i2", s.Generation), 4)
	e.Do("st-sync-wrong-instance", classicSyncRequest("gr-st", s.MemberID, "i2", s.Generation, "range", nil), 5)
	sc := kmsg.NewPtrOffsetCommitRequest()
	sc.Group, sc.MemberID, sc.Generation, sc.InstanceID = "gr-st", s.MemberID, s.Generation, str("i2")
	sc.Topics = commit.Topics
	e.Do("st-commit-wrong-instance", sc, 8)
	e.Do("st-heartbeat", classicHeartbeatRequest("gr-st", s.MemberID, "i1", s.Generation), 4)
	lui := kmsg.NewPtrLeaveGroupRequest()
	lui.Group = "gr-st"
	mi := kmsg.NewLeaveGroupRequestMember()
	mi.InstanceID = str("i-unknown")
	lui.Members = append(lui.Members, mi)
	e.Do("st-leave-unknown-instance", lui, 4)
	// The top-level code of a LeaveGroup: a member that is not there, and a group that is
	// not there, at the version that names one member and at one that names several.
	lv0 := kmsg.NewPtrLeaveGroupRequest()
	lv0.Group, lv0.MemberID = "gr-st", "Qx7cNotAMember"
	e.Do("lv-unknown-member-v0", lv0, 0)
	lv0 = kmsg.NewPtrLeaveGroupRequest()
	lv0.Group, lv0.MemberID = "gr-nobody", "Qx7cNotAMember"
	e.Do("lv-unknown-group-v0", lv0, 0)
	lv4 := kmsg.NewPtrLeaveGroupRequest()
	lv4.Group = "gr-nobody"
	m.MemberID = "Qx7cNotAMember"
	lv4.Members = append(lv4.Members, m)
	e.Do("lv-unknown-group-v4", lv4, 4)

	cs := e.Do("cs-join", consumerJoinRequest("gr-cs", "j1", "gr"), 0).(*kmsg.ConsumerGroupHeartbeatResponse)
	csid := deref(cs.MemberID)
	e.Do("cs-heartbeat-wrong-instance", consumerBeatRequest("gr-cs", csid, "j2", cs.MemberEpoch), 0)
	e.Do("cs-heartbeat", consumerBeatRequest("gr-cs", csid, "j1", cs.MemberEpoch), 0)
	e.Do("cs-static-leave", consumerBeatRequest("gr-cs", csid, "j1", -2), 0)
	re := e.Do("cs-rejoin-same-instance", consumerJoinRequest("gr-cs", "j1", "gr"), 0).(*kmsg.ConsumerGroupHeartbeatResponse)
	e.Do("cs-old-member-heartbeat", consumerBeatRequest("gr-cs", csid, "j1", cs.MemberEpoch), 0)
	e.Do("cs-leave", consumerBeatRequest("gr-cs", deref(re.MemberID), "j1", -1), 0)
	e.Do("cs-rejoin-after-leave", consumerJoinRequest("gr-cs", "j1", "gr"), 0)
	e.describeConsumer("cs-describe", "gr-cs")

	// An empty instance id on the consumer protocol.
	eic := consumerJoinRequest("gr-eic", "", "gr")
	eic.InstanceID = str("")
	e.Do("ei-c-join-empty-instance", eic, 0)
	e.describeConsumer("ei-c-describe", "gr-eic")
}
