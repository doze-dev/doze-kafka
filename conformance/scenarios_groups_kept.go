package conformance

import (
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// Which offsets of a classic group with members the retention looks at, when it cannot
// tell what the group reads: in a rebalance, a protocol type other than "consumer", a
// subscription it cannot read (GRP-19).

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "offsets-retention-members", Run: offsetsRetentionMembers,
			KafkaEnv: []string{"KAFKA_OFFSETS_RETENTION_MINUTES=1", "KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS=1000"},
			Speed:    10,
			Replay: func(cfg *config.Config) {
				cfg.OffsetsRetention = time.Minute / 10
				cfg.OffsetsRetentionCheckInterval = time.Hour // Env.CheckOffsets: see offsets-retention-recreate.
			},
		},
	)
}

// offsetsRetentionMembers: four classic groups, each with a member that commits ret-a and
// ret-b at 0 and subscribes to ret-a (or says nothing Kafka can read); ret-b is what
// the retention may take at 60. Read at 75 and at 115.
//
//	g-stable      a stable consumer group: the control.
//	g-rebal       a second member's JoinGroup at 0 waits for the first to rejoin, which
//	              it does at 80 (its rebalance timeout is 300 s): in a rebalance until
//	              then, stable after.
//	g-connect     protocol type "connect", with metadata that reads as a subscription to ret-a.
//	g-unreadable  protocol type "consumer", a subscription that is not one.
func offsetsRetentionMembers(e *Env) {
	e.Do("create", createTopics(false, newTopic("ret-a", 1, 1), newTopic("ret-b", 1, 1)), 7)
	groups := []string{"g-stable", "g-rebal", "g-connect", "g-unreadable"}
	e.findCoordinators(groups...)
	e.Open("b", "", "")

	type member struct {
		group, id string
		gen       int32
	}
	join := func(label, group, ptype string, metadata []byte, rebalance int32) *member {
		s := joinSpec{group: group, metadata: metadata}
		first := e.Do(label+"-join-needs-id", withType(classicJoinRequest(s), ptype, rebalance), 7).(*kmsg.JoinGroupResponse)
		s.member = first.MemberID
		j := e.Do(label+"-join", withType(classicJoinRequest(s), ptype, rebalance), 7).(*kmsg.JoinGroupResponse)
		sync := classicSyncRequest(group, s.member, "", j.Generation, "range", consumerAssignment("ret-a", 0))
		sync.ProtocolType = str(ptype)
		e.Do(label+"-sync", sync, 5)
		m := &member{group: group, id: s.member, gen: j.Generation}
		e.commitOne(label+"-commit-a", group, m.id, m.gen, "ret-a", 10)
		e.commitOneOn(label+"-commit-b", group, m.id, m.gen, "ret-b", 11)
		return m
	}
	sub := consumerSubscription("ret-a")
	start := time.Now()
	stable := join("stable", "g-stable", "consumer", sub, 30000)
	rebal := join("rebal", "g-rebal", "consumer", sub, 300000)
	connect := join("connect", "g-connect", "connect", sub, 30000) // a consumer's subscription, under another protocol type.
	unreadable := join("unreadable", "g-unreadable", "consumer", []byte{0xff, 0xff, 0xff}, 30000)

	e.Use("b")
	sb := joinSpec{group: "g-rebal", metadata: sub}
	sb.member = e.join("rebal-b-join-needs-id", sb).MemberID
	pb := e.StartHeld("rebal-b-join", withType(classicJoinRequest(sb), "consumer", 300000), 7, 90*time.Second)
	e.Use("")
	// B's join came on another connection: the describe waits until it has arrived.
	dg := kmsg.NewPtrDescribeGroupsRequest()
	dg.Groups = []string{"g-rebal"}
	e.DoUntil("rebal-describe-at-once", dg, 5, func(r kmsg.Response) bool {
		g := r.(*kmsg.DescribeGroupsResponse).Groups
		return len(g) == 1 && len(g[0].Members) == 2
	})

	elapsed := func() time.Duration { return time.Duration(float64(time.Since(start)) * e.speed) }
	until := func(at time.Duration) {
		for elapsed() < at {
			for _, m := range []*member{stable, rebal, connect, unreadable} {
				e.Quiet(classicHeartbeatRequest(m.group, m.id, "", m.gen), 4)
			}
			e.Wait(time.Second)
		}
	}
	checkpoint := func(label string) {
		for _, g := range groups {
			e.fetchTopics(label+"-fetch-"+g, g, "ret-a", "ret-b")
		}
		e.describeClassic(label+"-describe-g-rebal", "g-rebal")
	}

	until(75 * time.Second)
	e.CheckOffsets()
	checkpoint("t75")

	until(80 * time.Second)
	again := e.join("rebal-a-rejoin-at-80", joinSpec{group: "g-rebal", member: rebal.id, metadata: sub})
	b := e.Finish(pb).(*kmsg.JoinGroupResponse)
	rebal.gen = again.Generation
	e.Do("rebal-a-sync-at-80", classicSyncRequest("g-rebal", rebal.id, "", again.Generation, "range", consumerAssignment("ret-a", 0)), 5)
	e.Use("b")
	e.Do("rebal-b-sync-at-80", classicSyncRequest("g-rebal", b.MemberID, "", b.Generation, "range", nil), 5)
	e.Use("")
	bm := &member{group: "g-rebal", id: b.MemberID, gen: b.Generation}
	for elapsed() < 115*time.Second {
		for _, m := range []*member{stable, rebal, bm, connect, unreadable} {
			e.Quiet(classicHeartbeatRequest(m.group, m.id, "", m.gen), 4)
		}
		e.Wait(time.Second)
	}
	e.CheckOffsets()
	checkpoint("t115")
}

// withType is a classic JoinGroup with its protocol type and rebalance timeout set.
func withType(r *kmsg.JoinGroupRequest, ptype string, rebalance int32) *kmsg.JoinGroupRequest {
	r.ProtocolType, r.RebalanceTimeoutMillis = ptype, rebalance
	return r
}
