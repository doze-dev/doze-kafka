package conformance

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Scenarios of the "Bad days" stage (plan/stages/bad-days.md): what a real
// Apache Kafka answers when the session timeout is out of bounds (Q5) and when
// the data disk is full (Q2).

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "joingroup-session-timeout", Run: joinGroupSessionTimeout},
		Scenario{Name: "produce-acks-zero", Run: produceAcksZero},
	)
}

// joinGroupSessionTimeout is Q5: JoinGroup with session.timeout.ms on both
// sides of group.min.session.timeout.ms (6000) and group.max.session.timeout.ms
// (1800000), at the oldest and newest versions, and with rebalance timeouts
// that are not bounded at all.
func joinGroupSessionTimeout(e *Env) {
	join := func(label, group string, version int16, session, rebalance int32) {
		r := kmsg.NewPtrJoinGroupRequest()
		r.Group = group
		r.SessionTimeoutMillis, r.RebalanceTimeoutMillis = session, rebalance
		r.ProtocolType = "consumer"
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name, p.Metadata = "range", consumerSubscription("st")
		r.Protocols = append(r.Protocols, p)
		e.Do(label, r, version)
	}
	fc := kmsg.NewPtrFindCoordinatorRequest()
	fc.CoordinatorKeys = []string{"st-1"}
	e.Do("find-coordinator", fc, 4) // makes a real broker create its offsets topic.

	join("session-0", "st-0", 7, 0, 30000)
	join("session-1000", "st-1", 7, 1000, 30000)
	join("session-5999", "st-2", 7, 5999, 30000)
	join("session-6000", "st-3", 7, 6000, 30000)
	join("session-30000", "st-4", 7, 30000, 30000)
	join("session-1800000", "st-5", 7, 1800000, 30000)
	join("session-1800001", "st-6", 7, 1800001, 30000)
	join("session-negative", "st-7", 7, -1, 30000)
	join("v0-session-1000", "st-8", 0, 1000, 0)
	join("v0-session-1800001", "st-9", 0, 1800001, 0)
	join("v9-session-1000", "st-10", 9, 1000, 30000)
	join("v9-session-6000", "st-11", 9, 6000, 30000)
	// The rebalance timeout has no bounds of its own.
	join("rebalance-1", "st-12", 7, 30000, 1)
	join("rebalance-huge", "st-13", 7, 30000, 2000000000)
}

// produceAcksZero is Q9: an acks=0 Produce has no response, so whether the
// broker tells the producer it refused the data is whether it closes the
// connection. Each acks=0 request is followed by a Metadata request on the same
// connection: answered if the connection is open, recorded as closed if not.
func produceAcksZero(e *Env) {
	e.Do("create", createTopics(false, newTopic("z", 1, 1, "max.message.bytes", "2000")), 7)

	zero := func(label, topic string, partition int32, records []byte) {
		r := kmsg.NewPtrProduceRequest()
		r.Acks, r.TimeoutMillis = 0, 5000
		t := kmsg.NewProduceRequestTopic()
		t.Topic = topic
		p := kmsg.NewProduceRequestTopicPartition()
		p.Partition, p.Records = partition, records
		t.Partitions = append(t.Partitions, p)
		r.Topics = append(r.Topics, t)
		if err := e.w.send(r, 7); err != nil {
			e.fail("%s: %v", label, err)
		}
		e.Do(label+"-then-metadata", metadata(false, "z"), 9)
	}
	ok := encodeBatch([]ProduceRecord{{TimestampMs: 1000, Key: "k", Value: "v"}})
	zero("accepted", "z", 0, ok)
	zero("unknown-topic", "no-such-topic", 0, ok)
	zero("unknown-partition", "z", 5, ok)
	zero("too-large", "z", 0, encodeBatch([]ProduceRecord{{TimestampMs: 1000, Key: "k", Value: string(make([]byte, 5000))}}))
	corrupt := append([]byte(nil), ok...)
	corrupt[len(corrupt)-1] ^= 0xff
	zero("corrupt", "z", 0, corrupt)
}
