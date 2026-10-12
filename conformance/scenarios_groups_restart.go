package conformance

import (
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// What a restart of the broker does to the committed offsets of a group that was live
// when it stopped (plan/stages/groups.md, GRP-19), and the request builders the later
// Groups scenarios share.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "offsets-retention-restart", Run: offsetsRetentionRestart,
			// A minute of retention is Kafka's floor. Sessions are six seconds: the
			// classic one is what the JoinGroup asks for (and the shortest either
			// broker takes), the consumer protocol's is the broker's setting.
			KafkaEnv: []string{"KAFKA_OFFSETS_RETENTION_MINUTES=1", "KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS=1000",
				"KAFKA_GROUP_CONSUMER_SESSION_TIMEOUT_MS=6000", "KAFKA_GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS=6000",
				"KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS=2000", "KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS=2000"},
			Speed: 4,
			Replay: func(cfg *config.Config) {
				cfg.OffsetsRetention = time.Minute / 4
				cfg.ConsumerSessionTimeout = 6 * time.Second / 4
			},
			Known: offsetsRestartKnown(),
		},
	)
}

// offsetsRestartKnown is what a restart shows of this broker not keeping members: the
// returning members' heartbeats, and what is listed and described of the groups until a
// real broker's members have run out of session. What the scenario is about, when each
// offset is gone, has no entry here.
func offsetsRestartKnown() []Known {
	var k []Known
	for _, g := range []string{"gc-gone", "gc-back"} {
		k = append(k, knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "consumer-join-"+g, "consumer-settle-"+g)...)
	}
	k = append(k, knownSteps(limMembersNotKept, "", "back-heartbeat-g-back", "back-heartbeat-gc-back")...)
	k = append(k, knownSteps(limMembersNotKept, pathGroups,
		"r3-describe-g-gone", "r3-describe-gc-gone", "r3-list",
		"r35-describe-g-gone", "r35-list", "r55-describe-g-gone", "r55-list")...)
	return k
}

const limMembersNotKept = "Group membership is not kept across a restart"

// joinSpec is a classic JoinGroup (v7) said in full.
type joinSpec struct {
	group, member string
	instance      string // a static member's group.instance.id; "" is none.
	session       int32  // ms; 30000 when 0.
	protocol      string // "range" when "".
	metadata      []byte
}

func classicJoinRequest(s joinSpec) *kmsg.JoinGroupRequest {
	r := kmsg.NewPtrJoinGroupRequest()
	r.Group, r.MemberID = s.group, s.member
	if s.instance != "" {
		r.InstanceID = str(s.instance)
	}
	r.SessionTimeoutMillis, r.RebalanceTimeoutMillis = 30000, 30000
	if s.session != 0 {
		r.SessionTimeoutMillis = s.session
	}
	r.ProtocolType = "consumer"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name, p.Metadata = "range", s.metadata
	if s.protocol != "" {
		p.Name = s.protocol
	}
	r.Protocols = append(r.Protocols, p)
	return r
}

func (e *Env) join(label string, s joinSpec) *kmsg.JoinGroupResponse {
	return e.Do(label, classicJoinRequest(s), 7).(*kmsg.JoinGroupResponse)
}

// syncRequest is a SyncGroup (v5); assignment, when not nil, is what the member gives
// itself as the leader.
func classicSyncRequest(group, member, instance string, gen int32, protocol string, assignment []byte) *kmsg.SyncGroupRequest {
	r := kmsg.NewPtrSyncGroupRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	if instance != "" {
		r.InstanceID = str(instance)
	}
	r.ProtocolType, r.Protocol = str("consumer"), str(protocol)
	if assignment != nil {
		a := kmsg.NewSyncGroupRequestGroupAssignment()
		a.MemberID, a.MemberAssignment = member, assignment
		r.GroupAssignment = append(r.GroupAssignment, a)
	}
	return r
}

func classicHeartbeatRequest(group, member, instance string, gen int32) *kmsg.HeartbeatRequest {
	r := kmsg.NewPtrHeartbeatRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	if instance != "" {
		r.InstanceID = str(instance)
	}
	return r
}

// quiet sends req and returns the answer without recording a step.
func (e *Env) quiet(req kmsg.Request, version int16) kmsg.Response {
	for attempt := 0; ; attempt++ {
		resp, err := e.w.roundTrip(req, version)
		if err != nil {
			e.fail("unrecorded %s: %v", kmsg.NameForKey(req.Key()), err)
			return req.ResponseKind()
		}
		n, _ := e.norm.normalize(resp)
		if attempt >= 100 || !coordinatorSettling(n) {
			return resp
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// offsetsRetentionRestart is what a restart of the broker does to the offsets of a group
// that was live when it stopped. Every group commits ret-a at 0; the broker restarts at
// 64 s, when that offset is older than the retention, and the script reads at 3, 35, 55
// and 100 s after it is back.
//
//	g-gone     classic, subscribed to ret-a, heartbeats until the restart, never returns.
//	g-back     classic, the same, and heartbeats again as soon as the broker is back.
//	gc-gone    consumer protocol, never returns.
//	gc-back    consumer protocol, heartbeats again.
//	g-alone    never had a member: one commit at 50 (read at 3 and 55 s after, not at 35:
//	           how old it is then depends on how long the restart took).
func offsetsRetentionRestart(e *Env) {
	e.Do("create", createTopics(false, newTopic("ret-a", 1, 1)), 7)
	e.findCoordinators("g-gone", "g-back", "gc-gone", "gc-back", "g-alone")

	const session = 6000
	type classicMember struct {
		id  string
		gen int32
	}
	classic := func(group string, record bool) classicMember {
		spec := joinSpec{group: group, session: session, metadata: consumerSubscription("ret-a")}
		if !record {
			first := e.quiet(classicJoinRequest(spec), 7).(*kmsg.JoinGroupResponse)
			spec.member = first.MemberID
			j := e.quiet(classicJoinRequest(spec), 7).(*kmsg.JoinGroupResponse)
			e.quiet(classicSyncRequest(group, spec.member, "", j.Generation, "range", consumerAssignment("ret-a", 0)), 5)
			return classicMember{spec.member, j.Generation}
		}
		first := e.join("join-needs-id-"+group, spec)
		spec.member = first.MemberID
		j := e.join("join-"+group, spec)
		e.Do("sync-"+group, classicSyncRequest(group, spec.member, "", j.Generation, "range", consumerAssignment("ret-a", 0)), 5)
		return classicMember{spec.member, j.Generation}
	}
	type consumerMember struct {
		id    string
		epoch int32
	}
	consumer := func(group string) consumerMember {
		h := e.consumerHeartbeat("consumer-join-"+group, group, "", 0, "ret-a")
		m := consumerMember{deref(h.MemberID), h.MemberEpoch}
		if h2 := e.consumerHeartbeat("consumer-settle-"+group, group, m.id, m.epoch, "ret-a"); h2.MemberEpoch > 0 {
			m.epoch = h2.MemberEpoch
		}
		return m
	}

	gone := classic("g-gone", true)
	e.commitOne("commit-g-gone", "g-gone", gone.id, gone.gen, "ret-a", 10)
	back := classic("g-back", true)
	e.commitOne("commit-g-back", "g-back", back.id, back.gen, "ret-a", 20)
	cGone := consumer("gc-gone")
	e.consumerCommit("commit-gc-gone", "gc-gone", cGone.id, cGone.epoch, "ret-a", 30)
	cBack := consumer("gc-back")
	e.consumerCommit("commit-gc-back", "gc-back", cBack.id, cBack.epoch, "ret-a", 40)

	start := time.Now()
	elapsed := func() time.Duration { return time.Duration(float64(time.Since(start)) * e.speed) }
	keepAlive := func(all bool) {
		e.Quiet(classicHeartbeatRequest("g-back", back.id, "", back.gen), 4)
		cb := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		cb.Group, cb.MemberID, cb.MemberEpoch = "gc-back", cBack.id, cBack.epoch
		e.Quiet(cb, 0)
		if all {
			e.Quiet(classicHeartbeatRequest("g-gone", gone.id, "", gone.gen), 4)
			cg := kmsg.NewPtrConsumerGroupHeartbeatRequest()
			cg.Group, cg.MemberID, cg.MemberEpoch = "gc-gone", cGone.id, cGone.epoch
			e.Quiet(cg, 0)
		}
	}
	until := func(at time.Duration, all bool) {
		for elapsed() < at {
			keepAlive(all)
			e.Wait(time.Second)
		}
	}
	checkpoint := func(label string, groups ...string) {
		for _, g := range groups {
			e.fetchTopics(label+"-fetch-"+g, g, "ret-a")
		}
		e.describeClassic(label+"-describe-g-gone", "g-gone")
		e.describeConsumer(label+"-describe-gc-gone", "gc-gone")
		e.listGroups(label + "-list")
	}

	until(50*time.Second, true)
	e.commitOne("commit-g-alone-at-50", "g-alone", "", -1, "ret-a", 50)
	until(64*time.Second, true)
	checkpoint("before", "g-gone", "g-back", "gc-gone", "gc-back", "g-alone")

	e.Restart("restart")
	start = time.Now()
	// The first request waits for the coordinator to have loaded.
	e.fetchTopics("back-settle", "g-back", "ret-a")
	// The members that return do what a client does: heartbeat with what they had.
	hb := e.Do("back-heartbeat-g-back", classicHeartbeatRequest("g-back", back.id, "", back.gen), 4).(*kmsg.HeartbeatResponse)
	if hb.ErrorCode != 0 {
		back = classic("g-back", false)
	}
	ch := e.consumerHeartbeat("back-heartbeat-gc-back", "gc-back", cBack.id, cBack.epoch, "ret-a")
	if ch.ErrorCode != 0 {
		r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		r.Group, r.MemberEpoch, r.RebalanceTimeoutMillis = "gc-back", 0, 30000
		r.SubscribedTopicNames, r.ServerAssignor = []string{"ret-a"}, str("uniform")
		r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
		h := e.quiet(r, 0).(*kmsg.ConsumerGroupHeartbeatResponse)
		cBack = consumerMember{deref(h.MemberID), h.MemberEpoch}
	}

	until(3*time.Second, false)
	checkpoint("r3", "g-gone", "g-back", "gc-gone", "gc-back", "g-alone")
	until(35*time.Second, false)
	checkpoint("r35", "g-gone", "g-back", "gc-gone", "gc-back")
	until(55*time.Second, false)
	checkpoint("r55", "g-gone", "g-back", "gc-gone", "gc-back", "g-alone")
	until(100*time.Second, false)
	checkpoint("r100", "g-gone", "g-back", "gc-gone", "gc-back", "g-alone")
}
