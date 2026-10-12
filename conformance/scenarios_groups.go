package conformance

import (
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// Scenarios of the Groups stage (plan/stages/groups.md, decision D7): what a real
// Apache Kafka answers when the two group protocols meet on one group id (GRP-8), and
// when committed offsets expire (GRP-19).

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "mixed-protocol-groups", Run: mixedProtocolGroups, Known: mixedGroupsKnown()},
		Scenario{Name: "mixed-protocol-rolling", Run: mixedProtocolRolling, Known: mixedRollingKnown()},
		Scenario{Name: "offsets-retention", Run: offsetsRetention,
			// offsets.retention.minutes has a floor of one, so Kafka is recorded at
			// a minute and doze-kafka is replayed at a twentieth of that.
			KafkaEnv: []string{"KAFKA_OFFSETS_RETENTION_MINUTES=1", "KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS=1000"},
			Speed:    20,
			Replay:   func(cfg *config.Config) { cfg.OffsetsRetention = time.Minute / 20 },
			Known:    offsetsRetentionKnown(),
		},
	)
}

// classicJoin is a classic JoinGroup (v7) for group, protocol type "consumer" unless
// ptype says otherwise, subscribing to one topic.
func (e *Env) classicJoin(label, group, member, ptype, topic string) *kmsg.JoinGroupResponse {
	r := kmsg.NewPtrJoinGroupRequest()
	r.Group, r.MemberID = group, member
	r.SessionTimeoutMillis, r.RebalanceTimeoutMillis = 30000, 30000
	r.ProtocolType = ptype
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name, p.Metadata = "range", consumerSubscription(topic)
	r.Protocols = append(r.Protocols, p)
	return e.Do(label, r, 7).(*kmsg.JoinGroupResponse)
}

// classicSync is a SyncGroup (v5) in which the leader assigns partitions 0 and 1 of topic.
func (e *Env) classicSync(label, group, member string, gen int32, ptype, topic string) *kmsg.SyncGroupResponse {
	r := kmsg.NewPtrSyncGroupRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	r.ProtocolType, r.Protocol = str(ptype), str("range")
	a := kmsg.NewSyncGroupRequestGroupAssignment()
	a.MemberID, a.MemberAssignment = member, consumerAssignment(topic, 0, 1)
	r.GroupAssignment = append(r.GroupAssignment, a)
	return e.Do(label, r, 5).(*kmsg.SyncGroupResponse)
}

func (e *Env) classicHeartbeat(label, group, member string, gen int32) {
	r := kmsg.NewPtrHeartbeatRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	e.Do(label, r, 4)
}

func (e *Env) classicLeave(label, group, member string) {
	r := kmsg.NewPtrLeaveGroupRequest()
	r.Group = group
	m := kmsg.NewLeaveGroupRequestMember()
	m.MemberID = member
	r.Members = append(r.Members, m)
	e.Do(label, r, 4)
}

// commitOne commits one offset (OffsetCommit v8) for topic partition 0.
func (e *Env) commitOne(label, group, member string, gen int32, topic string, offset int64) {
	r := kmsg.NewPtrOffsetCommitRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	t := kmsg.NewOffsetCommitRequestTopic()
	t.Topic = topic
	p := kmsg.NewOffsetCommitRequestTopicPartition()
	p.Partition, p.Offset, p.LeaderEpoch = 0, offset, -1
	t.Partitions = append(t.Partitions, p)
	r.Topics = append(r.Topics, t)
	e.Do(label, r, 8)
}

// fetchAll is OffsetFetch v8 for every topic the group has offsets for.
func (e *Env) fetchAll(label, group string) {
	r := kmsg.NewPtrOffsetFetchRequest()
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group
	r.Groups = append(r.Groups, g)
	e.Do(label, r, 8)
}

// fetchTopics is OffsetFetch v8 for partition 0 of the topics named.
func (e *Env) fetchTopics(label, group string, topics ...string) {
	r := kmsg.NewPtrOffsetFetchRequest()
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group
	g.Topics = []kmsg.OffsetFetchRequestGroupTopic{}
	for _, t := range topics {
		rt := kmsg.NewOffsetFetchRequestGroupTopic()
		rt.Topic, rt.Partitions = t, []int32{0}
		g.Topics = append(g.Topics, rt)
	}
	r.Groups = append(r.Groups, g)
	e.Do(label, r, 8)
}

func (e *Env) findCoordinators(groups ...string) {
	fc := kmsg.NewPtrFindCoordinatorRequest()
	fc.CoordinatorKeys = groups
	e.Do("find-coordinators", fc, 4) // makes a real broker create its offsets topic.
}

// consumerHeartbeat is ConsumerGroupHeartbeat v0 (KIP-848).
func (e *Env) consumerHeartbeat(label, group, member string, epoch int32, topic string) *kmsg.ConsumerGroupHeartbeatResponse {
	r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	r.Group, r.MemberID, r.MemberEpoch = group, member, epoch
	if epoch == 0 {
		r.RebalanceTimeoutMillis = 30000
		r.SubscribedTopicNames = []string{topic}
		r.ServerAssignor = str("uniform")
		r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{} // empty, not null: a join owns nothing yet.
	}
	return e.Do(label, r, 0).(*kmsg.ConsumerGroupHeartbeatResponse)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (e *Env) describeClassic(label string, groups ...string) {
	r := kmsg.NewPtrDescribeGroupsRequest()
	r.Groups = groups
	e.Do(label, r, 5)
}

func (e *Env) describeConsumer(label string, groups ...string) {
	r := kmsg.NewPtrConsumerGroupDescribeRequest()
	r.Groups = groups
	e.Do(label, r, 0)
}

func (e *Env) listGroups(label string) {
	e.Do(label, kmsg.NewPtrListGroupsRequest(), 5)
}

// mixedProtocolGroups is GRP-8: classic and consumer-protocol members on one group id,
// in both orders, and a classic protocol type that is not "consumer". Kafka 4's default
// group.consumer.migration.policy is bidirectional, so what is recorded is what the
// default broker does with a rolling upgrade and a rolling downgrade.
func mixedProtocolGroups(e *Env) {
	e.Do("create", createTopics(false, newTopic("mp", 2, 1)), 7)
	e.findCoordinators("mp-classic-first", "mp-consumer-first", "mp-connect")

	// A classic group first; then an application on the new protocol joins it.
	first := e.classicJoin("c1-join-needs-id", "mp-classic-first", "", "consumer", "mp")
	m1 := first.MemberID
	j := e.classicJoin("c1-join", "mp-classic-first", m1, "consumer", "mp")
	e.classicSync("c1-sync", "mp-classic-first", m1, j.Generation, "consumer", "mp")
	e.commitOne("c1-commit", "mp-classic-first", m1, j.Generation, "mp", 0)
	n := e.consumerHeartbeat("c1-consumer-joins", "mp-classic-first", "", 0, "mp")
	e.describeClassic("c1-describe-classic", "mp-classic-first")
	e.describeConsumer("c1-describe-consumer", "mp-classic-first")
	e.classicHeartbeat("c1-classic-heartbeat", "mp-classic-first", m1, j.Generation)
	if id := deref(n.MemberID); id != "" {
		e.consumerHeartbeat("c1-consumer-heartbeat", "mp-classic-first", id, n.MemberEpoch, "mp")
	}
	e.listGroups("c1-list")
	e.fetchAll("c1-fetch", "mp-classic-first")

	// A consumer-protocol group first; then a classic client joins it.
	nj := e.consumerHeartbeat("n1-consumer-joins", "mp-consumer-first", "", 0, "mp")
	if id := deref(nj.MemberID); id != "" {
		e.consumerHeartbeat("n1-consumer-heartbeat", "mp-consumer-first", id, nj.MemberEpoch, "mp")
	}
	cj := e.classicJoin("n1-classic-join", "mp-consumer-first", "", "consumer", "mp")
	if cj.MemberID != "" && cj.ErrorCode == 79 {
		cj = e.classicJoin("n1-classic-join-with-id", "mp-consumer-first", cj.MemberID, "consumer", "mp")
	}
	e.describeClassic("n1-describe-classic", "mp-consumer-first")
	e.describeConsumer("n1-describe-consumer", "mp-consumer-first")
	e.listGroups("n1-list")

	// A classic group whose protocol type is not "consumer" (a Connect worker) is not a
	// consumer group: the new protocol has no claim on it.
	cc := e.classicJoin("x-connect-join-needs-id", "mp-connect", "", "connect", "mp")
	e.classicJoin("x-connect-join", "mp-connect", cc.MemberID, "connect", "mp")
	e.consumerHeartbeat("x-consumer-joins-a-connect-group", "mp-connect", "", 0, "mp")
	e.describeConsumer("x-describe-consumer", "mp-connect")
}

// consumerCommit is OffsetCommit v9 from a KIP-848 member: its member id and epoch.
func (e *Env) consumerCommit(label, group, member string, epoch int32, topic string, offset int64) {
	r := kmsg.NewPtrOffsetCommitRequest()
	r.Group, r.MemberID, r.Generation = group, member, epoch
	t := kmsg.NewOffsetCommitRequestTopic()
	t.Topic = topic
	p := kmsg.NewOffsetCommitRequestTopicPartition()
	p.Partition, p.Offset, p.LeaderEpoch = 0, offset, -1
	t.Partitions = append(t.Partitions, p)
	r.Topics = append(r.Topics, t)
	e.Do(label, r, 9)
}

// offsetsRetention is GRP-19 / D7: when a group's committed offsets expire, for each way
// a group can be at rest. offsets.retention.minutes is one minute (its floor) and the
// sweep runs every second; the script reads every group's offsets at 5 s, 75 s and 115 s,
// with members that must stay alive heartbeating in between.
//
//	g-empty       classic, one member commits at 0 and leaves at 0.
//	g-late        classic, commits at 0, member stays and leaves at 40.
//	g-recommit    classic, commits at 0 and leaves; a commit with no member at 40.
//	g-standalone  never had a member: one commit at 0 (no protocol type).
//	g-active      classic, subscribed to ret-a, commits ret-a and ret-b at 0, stays.
//	g-c-empty     consumer protocol, commits at 0 and leaves at 0.
//	g-c-active    consumer protocol, subscribed to ret-a, commits ret-a and ret-b, stays.
func offsetsRetention(e *Env) {
	e.Do("create", createTopics(false, newTopic("ret-a", 1, 1), newTopic("ret-b", 1, 1)), 7)
	groups := []string{"g-empty", "g-late", "g-recommit", "g-standalone", "g-active", "g-c-empty", "g-c-active"}
	e.findCoordinators(groups...)

	classic := func(group string) (member string, gen int32) {
		first := e.classicJoin("join-needs-id-"+group, group, "", "consumer", "ret-a")
		j := e.classicJoin("join-"+group, group, first.MemberID, "consumer", "ret-a")
		e.classicSync("sync-"+group, group, first.MemberID, j.Generation, "consumer", "ret-a")
		return first.MemberID, j.Generation
	}
	consumer := func(group string) (member string, epoch int32) {
		h := e.consumerHeartbeat("consumer-join-"+group, group, "", 0, "ret-a")
		member, epoch = deref(h.MemberID), h.MemberEpoch
		// A second heartbeat takes the assignment, as a client's poll loop does.
		if h2 := e.consumerHeartbeat("consumer-settle-"+group, group, member, epoch, "ret-a"); h2.MemberEpoch > 0 {
			epoch = h2.MemberEpoch
		}
		return member, epoch
	}

	mEmpty, gEmpty := classic("g-empty")
	e.commitOne("commit-g-empty", "g-empty", mEmpty, gEmpty, "ret-a", 10)
	e.classicLeave("leave-g-empty", "g-empty", mEmpty)

	mLate, gLate := classic("g-late")
	e.commitOne("commit-g-late", "g-late", mLate, gLate, "ret-a", 20)

	mRe, gRe := classic("g-recommit")
	e.commitOne("commit-g-recommit", "g-recommit", mRe, gRe, "ret-a", 30)
	e.classicLeave("leave-g-recommit", "g-recommit", mRe)

	e.commitOne("commit-g-standalone", "g-standalone", "", -1, "ret-a", 40)

	mAct, gAct := classic("g-active")
	e.commitOne("commit-g-active-a", "g-active", mAct, gAct, "ret-a", 50)
	e.commitOneOn("commit-g-active-b", "g-active", mAct, gAct, "ret-b", 51)

	mCE, eCE := consumer("g-c-empty")
	e.consumerCommit("commit-g-c-empty", "g-c-empty", mCE, eCE, "ret-a", 60)
	e.consumerHeartbeat("leave-g-c-empty", "g-c-empty", mCE, -1, "ret-a")

	mCA, eCA := consumer("g-c-active")
	e.consumerCommit("commit-g-c-active-a", "g-c-active", mCA, eCA, "ret-a", 70)
	e.consumerCommitOn("commit-g-c-active-b", "g-c-active", mCA, eCA, "ret-b", 71)

	start := time.Now()
	// elapsed is the script's own seconds: real ones against Kafka, sped up against
	// doze-kafka.
	elapsed := func() time.Duration { return time.Duration(float64(time.Since(start)) * e.speed) }
	keepAlive := func(lateAlive bool) {
		hb := kmsg.NewPtrHeartbeatRequest()
		hb.Group, hb.MemberID, hb.Generation = "g-active", mAct, gAct
		e.Quiet(hb, 4)
		if lateAlive {
			hl := kmsg.NewPtrHeartbeatRequest()
			hl.Group, hl.MemberID, hl.Generation = "g-late", mLate, gLate
			e.Quiet(hl, 4)
		}
		cb := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		cb.Group, cb.MemberID, cb.MemberEpoch = "g-c-active", mCA, eCA
		e.Quiet(cb, 0)
	}
	until := func(at time.Duration, lateAlive bool) {
		for elapsed() < at {
			keepAlive(lateAlive)
			e.Wait(2 * time.Second)
		}
	}
	checkpoint := func(label string) {
		// The topics named, so the answer's order is the request's and not a hash table's.
		for _, g := range groups {
			e.fetchTopics(label+"-fetch-"+g, g, "ret-a", "ret-b")
		}
		e.listGroups(label + "-list")
		e.describeClassic(label+"-describe-g-empty", "g-empty")
	}

	checkpoint("t5")

	until(40*time.Second, true)
	e.classicLeave("leave-g-late-at-40", "g-late", mLate)
	e.commitOne("commit-g-recommit-at-40", "g-recommit", "", -1, "ret-a", 31)

	until(75*time.Second, false)
	checkpoint("t75")

	until(115*time.Second, false)
	checkpoint("t115")
}

// commitOneOn is commitOne for a topic other than the first, on partition 0.
func (e *Env) commitOneOn(label, group, member string, gen int32, topic string, offset int64) {
	e.commitOne(label, group, member, gen, topic, offset)
}

// consumerCommitOn is consumerCommit, named for the topic it is about.
func (e *Env) consumerCommitOn(label, group, member string, epoch int32, topic string, offset int64) {
	e.consumerCommit(label, group, member, epoch, topic, offset)
}

// offsetsRetentionKnown is the KIP-848 difference the scenario steps over on the way to
// what it is about: the interval a member is asked to heartbeat at.
func offsetsRetentionKnown() []Known {
	var k []Known
	for _, g := range []string{"g-c-empty", "g-c-active"} {
		k = append(k, knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "consumer-join-"+g, "consumer-settle-"+g)...)
	}
	return k
}

const limHeartbeatInterval = "A consumer-protocol member is asked to heartbeat every second"

// What a migrating group differs in is the same few things, and each is written down in
// docs/limitations.md. Every request a client makes is answered with the code Kafka
// answers with; what differs is the interval a member is asked to heartbeat at, the
// numbers of epochs and generations (Kafka computes the target assignment after it has
// answered, so a joiner is told the epoch before its own join), and the states and
// targets a describe taken between shows.
const (
	limAssignAfter  = "A consumer group's target assignment is computed before the request that causes it"
	pathGroups      = "Groups"
	pathRequest     = "request"
	pathGeneration  = "Generation"
	pathMemberEpoch = "MemberEpoch"
	pathAssignment  = "Assignment"
)

// knownSteps is one Known entry for each step.
func knownSteps(limitation, path string, steps ...string) []Known {
	out := make([]Known, 0, len(steps))
	for _, s := range steps {
		out = append(out, Known{Step: s, Path: path, Limitation: limitation})
	}
	return out
}

func mixedGroupsKnown() []Known {
	var k []Known
	k = append(k, knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis",
		"c1-consumer-joins", "c1-consumer-heartbeat", "n1-consumer-joins", "n1-consumer-heartbeat")...)
	k = append(k, knownSteps(limAssignAfter, pathGeneration, "n1-classic-join-with-id")...)
	k = append(k, knownSteps(limAssignAfter, pathGroups, "n1-describe-consumer", "n1-list")...)
	return k
}

func mixedRollingKnown() []Known {
	var k []Known
	k = append(k, knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis",
		"up-n-join", "up-n-heartbeat-1", "up-n-heartbeat-2", "up-n-heartbeat-3", "up-n-heartbeat-4",
		"down-n-join", "down-n-heartbeat-1", "down-n-heartbeat-2", "down-n-heartbeat-3",
		"emptyup-n-join", "emptydown-n-join")...)
	k = append(k, knownSteps(limAssignAfter, pathGroups, "up-describe-after-leave", "up-list", "down-describe", "down-list",
		"down-list-after-leave", "emptydown-list")...)
	k = append(k, knownSteps(limAssignAfter, pathGeneration, "down-c-join", "down-c-rejoin")...)
	k = append(k, knownSteps(limAssignAfter, pathRequest,
		"down-c-sync", "down-c-heartbeat-1", "down-c-heartbeat-2", "down-c-resync", "down-c-heartbeat-3")...)
	k = append(k, knownSteps(limAssignAfter, pathAssignment, "up-n-heartbeat-4", "down-n-heartbeat-2")...)
	k = append(k, knownSteps(limAssignAfter, pathMemberEpoch, "up-n-heartbeat-4")...)
	return k
}

// mixedProtocolRolling is GRP-8 continued: what a rolling upgrade and a rolling downgrade
// of an application look like on the wire, one instance at a time, with every request an
// instance makes after the other protocol has joined its group. And the two cases
// where the group is empty and moves protocol whole.
func mixedProtocolRolling(e *Env) {
	e.Do("create", createTopics(false, newTopic("mr", 2, 1)), 7)
	e.findCoordinators("mr-up", "mr-down", "mr-empty-up", "mr-empty-down")

	// Upgrade: a classic instance is running; one instance restarts on the new protocol,
	// then the classic one rejoins what it finds, then it leaves.
	a := e.classicJoin("up-a-join-needs-id", "mr-up", "", "consumer", "mr")
	ja := e.classicJoin("up-a-join", "mr-up", a.MemberID, "consumer", "mr")
	e.classicSync("up-a-sync", "mr-up", a.MemberID, ja.Generation, "consumer", "mr")
	e.commitOne("up-a-commit", "mr-up", a.MemberID, ja.Generation, "mr", 3)
	n := e.consumerHeartbeat("up-n-join", "mr-up", "", 0, "mr")
	nid := deref(n.MemberID)
	e.classicHeartbeat("up-a-heartbeat-1", "mr-up", a.MemberID, ja.Generation)
	e.consumerHeartbeat("up-n-heartbeat-1", "mr-up", nid, n.MemberEpoch, "mr")
	ra := e.classicJoin("up-a-rejoin", "mr-up", a.MemberID, "consumer", "mr")
	e.classicSync("up-a-resync", "mr-up", a.MemberID, ra.Generation, "consumer", "mr")
	e.classicHeartbeat("up-a-heartbeat-2", "mr-up", a.MemberID, ra.Generation)
	h := e.consumerHeartbeat("up-n-heartbeat-2", "mr-up", nid, n.MemberEpoch, "mr")
	e.consumerHeartbeat("up-n-heartbeat-3", "mr-up", nid, h.MemberEpoch, "mr")
	e.describeConsumer("up-describe", "mr-up")
	e.commitOne("up-a-commit-2", "mr-up", a.MemberID, ra.Generation, "mr", 4)
	e.consumerCommit("up-n-commit", "mr-up", nid, h.MemberEpoch, "mr", 5)
	e.classicLeave("up-a-leave", "mr-up", a.MemberID)
	e.consumerHeartbeat("up-n-heartbeat-4", "mr-up", nid, h.MemberEpoch, "mr")
	e.describeConsumer("up-describe-after-leave", "mr-up")
	e.listGroups("up-list")

	// Downgrade: a consumer-protocol instance is running; a classic instance joins; the
	// consumer-protocol one leaves; the classic one is alone.
	m := e.consumerHeartbeat("down-n-join", "mr-down", "", 0, "mr")
	mid := deref(m.MemberID)
	m2 := e.consumerHeartbeat("down-n-heartbeat-1", "mr-down", mid, m.MemberEpoch, "mr")
	c := e.classicJoin("down-c-join-needs-id", "mr-down", "", "consumer", "mr")
	jc := e.classicJoin("down-c-join", "mr-down", c.MemberID, "consumer", "mr")
	e.classicSync("down-c-sync", "mr-down", c.MemberID, jc.Generation, "consumer", "mr")
	e.classicHeartbeat("down-c-heartbeat-1", "mr-down", c.MemberID, jc.Generation)
	epoch := m.MemberEpoch
	if m2.MemberEpoch > epoch {
		epoch = m2.MemberEpoch
	}
	h1 := e.consumerHeartbeat("down-n-heartbeat-2", "mr-down", mid, epoch, "mr")
	e.consumerHeartbeat("down-n-heartbeat-3", "mr-down", mid, h1.MemberEpoch, "mr")
	e.classicHeartbeat("down-c-heartbeat-2", "mr-down", c.MemberID, jc.Generation)
	rc := e.classicJoin("down-c-rejoin", "mr-down", c.MemberID, "consumer", "mr")
	e.classicSync("down-c-resync", "mr-down", c.MemberID, rc.Generation, "consumer", "mr")
	e.describeConsumer("down-describe", "mr-down")
	e.consumerHeartbeat("down-n-leaves", "mr-down", mid, -1, "mr")
	e.classicHeartbeat("down-c-heartbeat-3", "mr-down", c.MemberID, rc.Generation)
	e.describeClassic("down-describe-classic", "mr-down")
	e.describeConsumer("down-describe-consumer", "mr-down")
	e.listGroups("down-list")
	e.classicLeave("down-c-leave", "mr-down", c.MemberID)
	e.listGroups("down-list-after-leave")

	// An emptied group moves protocol whole, and keeps its offsets.
	ea := e.classicJoin("emptyup-a-join-needs-id", "mr-empty-up", "", "consumer", "mr")
	eja := e.classicJoin("emptyup-a-join", "mr-empty-up", ea.MemberID, "consumer", "mr")
	e.classicSync("emptyup-a-sync", "mr-empty-up", ea.MemberID, eja.Generation, "consumer", "mr")
	e.commitOne("emptyup-a-commit", "mr-empty-up", ea.MemberID, eja.Generation, "mr", 7)
	e.classicLeave("emptyup-a-leave", "mr-empty-up", ea.MemberID)
	en := e.consumerHeartbeat("emptyup-n-join", "mr-empty-up", "", 0, "mr")
	e.fetchAll("emptyup-fetch", "mr-empty-up")
	e.describeConsumer("emptyup-describe", "mr-empty-up")
	e.consumerHeartbeat("emptyup-n-leaves", "mr-empty-up", deref(en.MemberID), -1, "mr")

	nn := e.consumerHeartbeat("emptydown-n-join", "mr-empty-down", "", 0, "mr")
	e.consumerCommit("emptydown-n-commit", "mr-empty-down", deref(nn.MemberID), nn.MemberEpoch, "mr", 8)
	e.consumerHeartbeat("emptydown-n-leaves", "mr-empty-down", deref(nn.MemberID), -1, "mr")
	ec := e.classicJoin("emptydown-c-join-needs-id", "mr-empty-down", "", "consumer", "mr")
	ejc := e.classicJoin("emptydown-c-join", "mr-empty-down", ec.MemberID, "consumer", "mr")
	e.classicSync("emptydown-c-sync", "mr-empty-down", ec.MemberID, ejc.Generation, "consumer", "mr")
	e.fetchAll("emptydown-fetch", "mr-empty-down")
	e.describeClassic("emptydown-describe-classic", "mr-empty-down")
	e.listGroups("emptydown-list")
}
