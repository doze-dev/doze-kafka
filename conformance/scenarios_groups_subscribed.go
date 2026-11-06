package conformance

import "github.com/twmb/franz-go/pkg/kmsg"

// OffsetDelete of a group that has a member (GRP-4): which topics Kafka refuses with
// GROUP_SUBSCRIBED_TO_TOPIC (86). The member reads od-a; od-b is a topic the group has an
// offset for and no member reads. Classic and consumer protocol.

func init() {
	Scenarios = append(Scenarios, Scenario{Name: "offset-delete-subscribed", Run: offsetDeleteSubscribed,
		Known: knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "k-join")})
}

func offsetDeleteSubscribed(e *Env) {
	e.Do("create", createTopics(false, newTopic("od-a", 1, 1), newTopic("od-b", 1, 1)), 7)
	e.findCoordinators("od-classic", "od-consumer")

	// A classic member reading od-a.
	sub := consumerSubscription("od-a")
	m := e.join("c-join-needs-id", joinSpec{group: "od-classic", metadata: sub}).MemberID
	j := e.join("c-join", joinSpec{group: "od-classic", member: m, metadata: sub})
	e.Do("c-sync", classicSyncRequest("od-classic", m, "", j.Generation, "range", consumerAssignment("od-a", 0)), 5)
	e.commitOne("c-commit-a", "od-classic", m, j.Generation, "od-a", 3)
	e.commitOne("c-commit-b", "od-classic", m, j.Generation, "od-b", 4)
	e.Do("c-delete-b", offsetDeleteRequest("od-classic", "od-b"), 0)
	e.Do("c-delete-a", offsetDeleteRequest("od-classic", "od-a"), 0)
	e.fetchTopics("c-fetch", "od-classic", "od-a", "od-b")

	// A consumer-protocol member reading od-a.
	hb := e.Do("k-join", consumerJoinRequest("od-consumer", "", "od-a"), 0).(*kmsg.ConsumerGroupHeartbeatResponse)
	member, epoch := *hb.MemberID, hb.MemberEpoch
	e.commitOne("k-commit-a", "od-consumer", member, epoch, "od-a", 3)
	e.commitOne("k-commit-b", "od-consumer", member, epoch, "od-b", 4)
	e.Do("k-delete-b", offsetDeleteRequest("od-consumer", "od-b"), 0)
	e.Do("k-delete-a", offsetDeleteRequest("od-consumer", "od-a"), 0)
	e.fetchTopics("k-fetch", "od-consumer", "od-a", "od-b")
}
