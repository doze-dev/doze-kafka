package conformance

import (
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// A classic group upgraded to a consumer group in the middle of a rebalance (GRP-8).

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "mixed-protocol-parked", Run: mixedProtocolParked, Known: mixedParkedKnown()},
	)
}

// mixedParkedKnown is the interval a consumer-protocol member is asked to heartbeat at:
// every other answer of the scenario is Kafka's.
func mixedParkedKnown() []Known {
	return knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "pj-n-join", "pj-n-heartbeat", "ps-n-join")
}

// Answered reports whether the answer to Start has come within d.
func (e *Env) Answered(p *Pending, d time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(d):
		return false
	}
}

// mixedProtocolParked is a classic group upgraded in the middle of a rebalance: what the
// requests it had parked are answered, and what becomes of the members that sent them.
//
//	mk-join   A is stable with four partitions; B's JoinGroup waits for A to rejoin;
//	          a consumer-protocol member joins.
//	mk-sync   A and B have both joined a new generation; B's SyncGroup waits for the
//	          leader's; a consumer-protocol member joins.
//
// B reads a topic of its own, so that which partitions each member is to have does not
// depend on the order of two random member ids.
func mixedProtocolParked(e *Env) {
	e.Do("create", createTopics(false, newTopic("mk", 4, 1), newTopic("mk-b", 1, 1)), 7)
	e.findCoordinators("mk-join", "mk-sync")
	e.Open("b", "", "")
	sub, subB := consumerSubscription("mk"), consumerSubscription("mk-b")

	stable := func(prefix, group string) (string, int32) {
		spec := joinSpec{group: group, metadata: sub}
		first := e.join(prefix+"-a-join-needs-id", spec)
		spec.member = first.MemberID
		j := e.join(prefix+"-a-join", spec)
		e.Do(prefix+"-a-sync", classicSyncRequest(group, spec.member, "", j.Generation, "range", consumerAssignment("mk", 0, 1, 2, 3)), 5)
		return spec.member, j.Generation
	}

	// A JoinGroup parked.
	a, gen := stable("pj", "mk-join")
	e.Use("b")
	bspec := joinSpec{group: "mk-join", metadata: subB}
	bspec.member = e.join("pj-b-join-needs-id", bspec).MemberID
	parked := e.Start("pj-b-join-parked", classicJoinRequest(bspec), 7)
	e.Use("")
	e.Wait(time.Second) // B's request is in before the upgrade.
	n := e.consumerHeartbeat("pj-n-join", "mk-join", "", 0, "mk")
	bj, _ := e.Finish(parked).(*kmsg.JoinGroupResponse)
	e.describeConsumer("pj-describe", "mk-join")
	e.describeClassic("pj-describe-classic", "mk-join")
	// B does what a client does with the answer it got: with its id if it may keep it.
	e.Use("b")
	if bj != nil && bj.ErrorCode == 25 {
		bspec.member = ""
		bspec.member = e.join("pj-b-join-again-needs-id", bspec).MemberID
	}
	bj = e.join("pj-b-rejoin", bspec)
	e.Do("pj-b-sync", classicSyncRequest("mk-join", bspec.member, "", bj.Generation, "range", nil), 5)
	e.Do("pj-b-heartbeat", classicHeartbeatRequest("mk-join", bspec.member, "", bj.Generation), 4)
	e.Use("")
	e.Do("pj-a-heartbeat", classicHeartbeatRequest("mk-join", a, "", gen), 4)
	nid := deref(n.MemberID)
	e.consumerHeartbeat("pj-n-heartbeat", "mk-join", nid, n.MemberEpoch, "mk")
	e.describeConsumer("pj-describe-after", "mk-join")

	// A SyncGroup parked.
	a, _ = stable("ps", "mk-sync")
	e.Use("b")
	bspec = joinSpec{group: "mk-sync", metadata: subB}
	bspec.member = e.join("ps-b-join-needs-id", bspec).MemberID
	parked = e.Start("ps-b-join", classicJoinRequest(bspec), 7)
	e.Use("")
	e.Wait(time.Second)
	aj := e.join("ps-a-rejoin", joinSpec{group: "mk-sync", member: a, metadata: sub})
	bj, _ = e.Finish(parked).(*kmsg.JoinGroupResponse)
	e.Use("b")
	parked = e.Start("ps-b-sync-parked", classicSyncRequest("mk-sync", bspec.member, "", bj.Generation, "range", nil), 5)
	e.Use("")
	e.Wait(time.Second)
	e.consumerHeartbeat("ps-n-join", "mk-sync", "", 0, "mk")
	leaderSync := classicSyncRequest("mk-sync", a, "", aj.Generation, "range", consumerAssignment("mk", 0, 1))
	if e.Answered(parked, 3*time.Second) {
		e.Finish(parked)
		e.Do("ps-a-leader-sync", leaderSync, 5)
	} else {
		e.Do("ps-a-leader-sync", leaderSync, 5)
		e.Finish(parked)
	}
	e.describeConsumer("ps-describe", "mk-sync")
	e.describeClassic("ps-describe-classic", "mk-sync")
	e.Do("ps-a-heartbeat", classicHeartbeatRequest("mk-sync", a, "", aj.Generation), 4)
	e.Use("b")
	e.Do("ps-b-heartbeat", classicHeartbeatRequest("mk-sync", bspec.member, "", bj.Generation), 4)
	e.Use("")
}
