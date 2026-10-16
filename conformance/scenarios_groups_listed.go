package conformance

import (
	"time"
)

func init() {
	Scenarios = append(Scenarios, Scenario{Name: "groups-listed-after-restart", Run: groupsListedAfterRestart,
		Known: knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "c-join")})
}

// groupsListedAfterRestart is how empty groups with committed offsets are listed and
// described before a restart of the broker and after it: a classic group of protocol
// type "consumer", one of type "connect", and a consumer-protocol group.
func groupsListedAfterRestart(e *Env) {
	e.Do("create", createTopics(false, newTopic("lr", 1, 1)), 7)
	e.findCoordinators("lr-classic", "lr-connect", "lr-consumer")
	for _, ptype := range []string{"consumer", "connect"} {
		group := "lr-classic"
		if ptype == "connect" {
			group = "lr-connect"
		}
		first := e.classicJoin(ptype+"-join-needs-id", group, "", ptype, "lr")
		j := e.classicJoin(ptype+"-join", group, first.MemberID, ptype, "lr")
		e.classicSync(ptype+"-sync", group, first.MemberID, j.Generation, ptype, "lr")
		e.commitOne(ptype+"-commit", group, first.MemberID, j.Generation, "lr", 5)
		e.classicLeave(ptype+"-leave", group, first.MemberID)
	}
	n := e.consumerHeartbeat("c-join", "lr-consumer", "", 0, "lr")
	e.consumerCommit("c-commit", "lr-consumer", deref(n.MemberID), n.MemberEpoch, "lr", 7)
	e.consumerHeartbeat("c-leave", "lr-consumer", deref(n.MemberID), -1, "lr")

	look := func(label string) {
		e.describeClassic(label+"-describe-classic", "lr-classic")
		e.describeClassic(label+"-describe-connect", "lr-connect")
		e.describeConsumer(label+"-describe-consumer", "lr-consumer")
		e.listGroups(label + "-list")
	}
	look("before")
	e.Restart("restart")
	e.Wait(5 * time.Second) // a real broker lists nothing until it has loaded its groups.
	look("after")
}
