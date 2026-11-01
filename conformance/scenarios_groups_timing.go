package conformance

import (
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "consumer-assignment-interval", Run: consumerAssignmentInterval,
			Known: knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis",
				"a-joins", "b-joins", "a-heartbeat-300ms", "a-heartbeat-3s", "b-heartbeat", "a-heartbeat-c-1500ms", "d-joins", "a-heartbeat-d-1500ms", "a-heartbeat-last")},
		Scenario{Name: "consumer-assignment-expiry", Run: consumerAssignmentExpiry,
			KafkaEnv: []string{"KAFKA_GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS=6000", "KAFKA_GROUP_CONSUMER_SESSION_TIMEOUT_MS=6000"},
			Doze:     func(c *config.Config) { c.ConsumerSessionTimeout = 6 * time.Second },
			Known: knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis",
				"a-joins", "x-joins-1500ms", "a-heartbeat-2s", "a-heartbeat-4s", "a-heartbeat-8s")},
	)
}

// member848 is a consumer-protocol member of a script, its id and epoch kept from the
// answers it is given.
type member848 struct {
	group, id string
	epoch     int32
}

func (e *Env) beat(label string, m *member848, topic string) *kmsg.ConsumerGroupHeartbeatResponse {
	r := e.consumerHeartbeat(label, m.group, m.id, m.epoch, topic)
	if r.ErrorCode == 0 {
		if m.id == "" {
			m.id = deref(r.MemberID)
		}
		if m.epoch >= 0 {
			m.epoch = r.MemberEpoch
		}
	}
	return r
}

// consumerAssignmentInterval is when a consumer group's target assignment is computed
// (group.consumer.assignment.interval.ms, 1 s by default): at once for a change at
// least that long after the last one, otherwise at the first heartbeat after that long.
// Until then the group is Assigning with the old targets, nothing happens without a
// heartbeat however long it is, and a member that has joined is at the epoch of the
// targets, not of the group. Every wait leaves a margin of a third of a second or more
// either side of the interval.
func consumerAssignmentInterval(e *Env) {
	e.Do("create", createTopics(false, newTopic("ai", 2, 1)), 7)
	e.findCoordinators("ai-g")

	a := &member848{group: "ai-g"}
	b := &member848{group: "ai-g"}
	e.beat("a-joins", a, "ai")
	e.beat("b-joins", b, "ai") // a moment after the first targets.
	e.describeConsumer("describe-at-once", "ai-g")
	e.Wait(300 * time.Millisecond)
	e.beat("a-heartbeat-300ms", a, "ai")
	e.describeConsumer("describe-300ms", "ai-g")
	e.Wait(2700 * time.Millisecond)
	e.describeConsumer("describe-3s-no-heartbeats", "ai-g")
	e.beat("a-heartbeat-3s", a, "ai") // computes them.
	e.describeConsumer("describe-after-a-heartbeat", "ai-g")
	e.beat("b-heartbeat", b, "ai")

	// A classic member joins a moment after: its generation is the targets' epoch.
	first := e.classicJoin("c-join-needs-id", "ai-g", "", "consumer", "ai")
	c := e.classicJoin("c-join", "ai-g", first.MemberID, "consumer", "ai")
	e.describeConsumer("describe-after-c-join", "ai-g")
	e.Do("c-sync", classicSyncRequest("ai-g", c.MemberID, "", c.Generation, "range", nil), 5)
	e.Wait(1500 * time.Millisecond)
	e.Do("c-heartbeat-1500ms", classicHeartbeatRequest("ai-g", c.MemberID, "", c.Generation), 4)
	e.describeConsumer("describe-after-c-heartbeat", "ai-g")
	e.beat("a-heartbeat-c-1500ms", a, "ai")
	e.describeConsumer("describe-after-a-heartbeat-2", "ai-g")

	// Long after the last targets, a join computes them at once; a leave a moment
	// after that waits again.
	e.Wait(1500 * time.Millisecond)
	d := &member848{group: "ai-g"}
	e.beat("d-joins", d, "ai")
	e.describeConsumer("describe-after-d-joins", "ai-g")
	d.epoch = -1
	e.beat("d-leaves", d, "ai")
	e.describeConsumer("describe-after-d-leaves", "ai-g")

	// A leave long after the last targets does not compute them either; nor does a
	// classic member's JoinGroup; a heartbeat does.
	e.Wait(1500 * time.Millisecond)
	e.beat("a-heartbeat-d-1500ms", a, "ai")
	e.describeConsumer("describe-after-a-heartbeat-3", "ai-g")
	e.Wait(1500 * time.Millisecond)
	b.epoch = -1
	e.beat("b-leaves-1500ms", b, "ai")
	e.describeConsumer("describe-after-b-leaves", "ai-g")
	e.Wait(1500 * time.Millisecond)
	again := e.classicJoin("c-rejoin-1500ms", "ai-g", c.MemberID, "consumer", "ai")
	e.describeConsumer("describe-after-c-rejoin", "ai-g")
	e.Do("c-resync", classicSyncRequest("ai-g", c.MemberID, "", again.Generation, "range", nil), 5)
	e.beat("a-heartbeat-last", a, "ai")
	e.describeConsumer("describe-after-a-heartbeat-last", "ai-g")
}

// consumerAssignmentExpiry is a member that runs out of session (6 s here) long after
// the last targets: like a leave, its going does not compute new ones; the next
// heartbeat does.
func consumerAssignmentExpiry(e *Env) {
	e.Do("create", createTopics(false, newTopic("ae", 2, 1)), 7)
	e.findCoordinators("ae-g")
	a := &member848{group: "ae-g"}
	x := &member848{group: "ae-g"}
	e.beat("a-joins", a, "ae")
	e.Wait(1500 * time.Millisecond)
	e.beat("x-joins-1500ms", x, "ae")
	e.describeConsumer("describe-after-x-joins", "ae-g")
	// a keeps its session; x sends nothing more and is gone at about 6 s.
	for _, l := range []string{"a-heartbeat-2s", "a-heartbeat-4s"} {
		e.Wait(2 * time.Second)
		e.beat(l, a, "ae")
	}
	e.Wait(4 * time.Second)
	e.describeConsumer("describe-8s-x-expired", "ae-g")
	e.beat("a-heartbeat-8s", a, "ae")
	e.describeConsumer("describe-after-a-heartbeat", "ae-g")
}
