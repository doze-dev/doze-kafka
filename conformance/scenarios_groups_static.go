package conformance

import (
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// A static consumer-protocol member that leaves for a restart (epoch -2), and how long a
// classic rebalance waits for a member that does not rejoin.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "consumer-static-leave", Run: consumerStaticLeave,
			KafkaEnv: []string{"KAFKA_GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS=6000", "KAFKA_GROUP_CONSUMER_SESSION_TIMEOUT_MS=6000"},
			Doze:     func(c *config.Config) { c.ConsumerSessionTimeout = 6 * time.Second },
			Known: knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis",
				"a-joins", "b-joins", "a-heartbeat-1", "a-heartbeat-2", "b-heartbeat-1", "b-heartbeat-after-a-left",
				"a-rejoins", "b-heartbeat-after-a-rejoined", "b-heartbeat-2s", "b-heartbeat-4s", "b-heartbeat-7500ms", "b-heartbeat-10s")},
		Scenario{Name: "classic-rebalance-timeout", Run: classicRebalanceTimeout},
	)
}

// consumerStaticLeave: A (instance s1) and B share two partitions. A leaves with epoch
// -2: B, heartbeating past the assignment interval, is not given A's partition, and A,
// rejoining with its instance id and no member id, has it at once. A leaves again and
// stays away: once its session (6 s here) has run out, B's next heartbeat has both.
func consumerStaticLeave(e *Env) {
	e.Do("create", createTopics(false, newTopic("sl", 2, 1)), 7)
	e.findCoordinators("sl-g")
	a := &member848{group: "sl-g"}
	b := &member848{group: "sl-g"}
	join := func(label string, m *member848, instance string) *kmsg.ConsumerGroupHeartbeatResponse {
		r := consumerJoinRequest("sl-g", instance, "sl")
		resp := e.Do(label, r, 0).(*kmsg.ConsumerGroupHeartbeatResponse)
		m.id, m.epoch = deref(resp.MemberID), resp.MemberEpoch
		return resp
	}
	beat := func(label string, m *member848, instance string, owned ...int32) *kmsg.ConsumerGroupHeartbeatResponse {
		r := consumerBeatRequest("sl-g", m.id, instance, m.epoch)
		if owned != nil {
			r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
			t := kmsg.NewConsumerGroupHeartbeatRequestTopic()
			t.TopicID, t.Partitions = e.topicID("sl"), owned
			r.Topics = append(r.Topics, t)
		}
		resp := e.Do(label, r, 0).(*kmsg.ConsumerGroupHeartbeatResponse)
		if resp.ErrorCode == 0 && m.epoch >= 0 {
			m.epoch = resp.MemberEpoch
		}
		return resp
	}
	join("a-joins", a, "s1")
	e.Wait(1500 * time.Millisecond)
	join("b-joins", b, "")
	beat("a-heartbeat-1", a, "s1")    // told to give one up.
	beat("a-heartbeat-2", a, "s1", 0) // has.
	beat("b-heartbeat-1", b, "")      // takes it.
	e.describeConsumer("describe-before", "sl-g")

	e.Wait(1500 * time.Millisecond)
	a.epoch = -2
	beat("a-static-leave", a, "s1")
	e.describeConsumer("describe-after-a-left", "sl-g")
	e.Wait(1500 * time.Millisecond)
	beat("b-heartbeat-after-a-left", b, "", 1)
	e.describeConsumer("describe-after-b-heartbeat", "sl-g")
	join("a-rejoins", a, "s1")
	beat("b-heartbeat-after-a-rejoined", b, "", 1)
	e.describeConsumer("describe-after-a-rejoined", "sl-g")

	e.Wait(1500 * time.Millisecond)
	a.epoch = -2
	beat("a-static-leave-again", a, "s1")
	// The session runs out at 6 s: heartbeats at 2 and 4 s, then at 7.5 and 10 s.
	beatAt := func(label string, wait time.Duration) {
		e.Wait(wait)
		beat(label, b, "", 1)
	}
	beatAt("b-heartbeat-2s", 2*time.Second)
	beatAt("b-heartbeat-4s", 2*time.Second)
	e.Wait(3 * time.Second)
	e.describeConsumer("describe-7s-after-a-left", "sl-g")
	beatAt("b-heartbeat-7500ms", 500*time.Millisecond)
	e.describeConsumer("describe-after-b-heartbeat-7500ms", "sl-g")
	beatAt("b-heartbeat-10s", 2500*time.Millisecond)
}

// topicID is a topic's id, from Metadata (not a step).
func (e *Env) topicID(topic string) [16]byte {
	m := e.quiet(metadata(false, topic), 12).(*kmsg.MetadataResponse)
	return m.Topics[0].TopicID
}

// classicRebalanceTimeout is how long a rebalance waits for a stable member that does
// not rejoin: A (session 30 s) is stable; B joins at 0, and A never rejoins. Described
// at 10 s and 18 s.
//
//	rt-longest  A's rebalance timeout is 15 s, B's JoinGroup (v5) says 6 s: the
//	            rebalance waits the longest any member asked for, not the joiner's.
//	rt-v0       A's is 6 s; B's JoinGroup is v0, which has no rebalance timeout, and a
//	            session of 15 s: that is taken for its rebalance timeout.
func classicRebalanceTimeout(e *Env) {
	e.Do("create", createTopics(false, newTopic("rt", 1, 1)), 7)
	e.findCoordinators("rt-longest", "rt-v0")
	stable := func(group string, rebalance int32) {
		s := joinSpec{group: group, metadata: consumerSubscription("rt")}
		s.member = e.join(group+"-a-join-needs-id", s).MemberID
		r := classicJoinRequest(s)
		r.RebalanceTimeoutMillis = rebalance
		j := e.Do(group+"-a-join", r, 7).(*kmsg.JoinGroupResponse)
		e.Do(group+"-a-sync", classicSyncRequest(group, s.member, "", j.Generation, "range", consumerAssignment("rt", 0)), 5)
	}
	stable("rt-longest", 15000)
	stable("rt-v0", 6000)

	e.Open("b1", "", "")
	e.Open("b2", "", "")
	e.Use("b1")
	s := joinSpec{group: "rt-longest", metadata: consumerSubscription("rt")}
	s.member = e.join("rt-longest-b-join-needs-id", s).MemberID
	r := classicJoinRequest(s)
	r.RebalanceTimeoutMillis = 6000
	start := time.Now()
	p1 := e.StartHeld("rt-longest-b-join", r, 7, 20*time.Second)
	e.Use("b2")
	v0 := kmsg.NewPtrJoinGroupRequest()
	v0.Group, v0.SessionTimeoutMillis, v0.ProtocolType = "rt-v0", 15000, "consumer"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name, p.Metadata = "range", consumerSubscription("rt")
	v0.Protocols = append(v0.Protocols, p)
	p2 := e.StartHeld("rt-v0-b-join", v0, 0, 20*time.Second)
	e.Use("")

	at := func(d time.Duration) { time.Sleep(time.Until(start.Add(d))) }
	at(10 * time.Second)
	e.describeClassic("rt-longest-describe-10s", "rt-longest")
	e.describeClassic("rt-v0-describe-10s", "rt-v0")
	at(18 * time.Second)
	e.describeClassic("rt-longest-describe-18s", "rt-longest")
	e.describeClassic("rt-v0-describe-18s", "rt-v0")
	e.Finish(p1)
	e.Finish(p2)
}
