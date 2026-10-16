package conformance

import "github.com/twmb/franz-go/pkg/kmsg"

func init() {
	Scenarios = append(Scenarios, Scenario{Name: "unreadable-subscription", Run: unreadableSubscription,
		Known: knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "us-n-join")})
}

// unreadableSubscription is a classic JoinGroup into a consumer group whose
// subscription is not one: with the member id it was given, as a static member with
// none, and at a version (3) that joins in one step. Then what the group has.
func unreadableSubscription(e *Env) {
	e.Do("create", createTopics(false, newTopic("us", 1, 1)), 7)
	e.findCoordinators("us-g")
	e.consumerHeartbeat("us-n-join", "us-g", "", 0, "us")
	bad := []byte{0, 1, 2}
	first := e.Do("us-join-needs-id", classicJoinRequest(joinSpec{group: "us-g", metadata: bad}), 7).(*kmsg.JoinGroupResponse)
	e.Do("us-join-with-id", classicJoinRequest(joinSpec{group: "us-g", member: first.MemberID, metadata: bad}), 7)
	e.Do("us-join-static", classicJoinRequest(joinSpec{group: "us-g", instance: "us-i", metadata: bad}), 7)
	e.Do("us-join-v3", classicJoinRequest(joinSpec{group: "us-g", metadata: bad}), 3)
	e.describeConsumer("us-describe", "us-g")
}
