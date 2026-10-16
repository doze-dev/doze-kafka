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
		Scenario{Name: "produce-decompressed-size", Run: produceDecompressedSize, Known: []Known{
			// A refusal's text is doze-kafka's own; Kafka's response carries none.
			{Step: "keep-uncompressed-over", Path: "Topics[0].Partitions[0].ErrorMessage", Limitation: limMessages},
			{Step: "keep-gzip-compressed-over", Path: "Topics[0].Partitions[0].ErrorMessage", Limitation: limMessages},
			{Step: "plain-gzip-small-inflates-50k", Path: "Topics[0].Partitions[0].ErrorMessage", Limitation: limMessages},
		}},
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

// produceDecompressedSize is the Q10 recording: which size does a real Kafka hold a
// compressed batch to, the batch as it came (compressed) or what it inflates to? The
// topics have max.message.bytes=2000; every payload inflates to far more than that and
// the compressible ones weigh a few hundred bytes. Each answer is then followed by a
// ListOffsets for the partition's end (the step "<label> then end"), so a success is
// shown to have stored the batch and a refusal not to have. (Not a Fetch: the
// batch Kafka stores for the recompress topic is its own gzip output, whose bytes
// are no claim.)
func produceDecompressedSize(e *Env) {
	cfg := func(name, compression string) kmsg.CreateTopicsRequestTopic {
		return newTopic(name, 1, 1, "max.message.bytes", "2000", "compression.type", compression)
	}
	e.Do("create", createTopics(false,
		cfg("keep", "producer"), cfg("plain", "uncompressed"), cfg("recompress", "gzip"),
		newTopic("compact", 1, 1, "max.message.bytes", "2000", "cleanup.policy", "compact"),
		newTopic("default", 1, 1)), 7)

	zeros := func(n int) []ProduceRecord {
		return []ProduceRecord{{TimestampMs: 1000, Key: "k", Value: string(make([]byte, n))}}
	}
	noise := func(n int) []ProduceRecord {
		b := make([]byte, n)
		x := uint32(2463534242)
		for i := range b {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			b[i] = byte(x)
		}
		return []ProduceRecord{{TimestampMs: 1000, Key: "k", Value: string(b)}}
	}
	send := func(label, topic string, records []byte) {
		r := kmsg.NewPtrProduceRequest()
		r.Acks, r.TimeoutMillis = -1, 5000
		t := kmsg.NewProduceRequestTopic()
		t.Topic = topic
		p := kmsg.NewProduceRequestTopicPartition()
		p.Records = records
		t.Partitions = append(t.Partitions, p)
		r.Topics = append(r.Topics, t)
		// A topic only just created has no leader for a moment (NOT_LEADER_OR_FOLLOWER);
		// ask again until it has one, and record the answer that is about the batch.
		e.DoUntil(label, r, 9, func(resp kmsg.Response) bool {
			for _, t := range resp.(*kmsg.ProduceResponse).Topics {
				for _, p := range t.Partitions {
					if p.ErrorCode == 6 {
						return false
					}
				}
			}
			return true
		})
		lo := kmsg.NewPtrListOffsetsRequest()
		lo.ReplicaID = -1
		lt := kmsg.NewListOffsetsRequestTopic()
		lt.Topic = topic
		lp := kmsg.NewListOffsetsRequestTopicPartition()
		lp.Timestamp = -1 // the end.
		lt.Partitions = append(lt.Partitions, lp)
		lo.Topics = append(lo.Topics, lt)
		e.Do(label+" then end", lo, 7)
	}
	send("keep-uncompressed-over", "keep", encodeBatch(zeros(5000)))
	send("keep-gzip-small-inflates-50k", "keep", encodeBatchCodec(zeros(50000), 1))
	send("keep-zstd-small-inflates-50k", "keep", encodeBatchCodec(zeros(50000), 4))
	send("keep-gzip-compressed-over", "keep", encodeBatchCodec(noise(5000), 1))
	send("plain-gzip-small-inflates-50k", "plain", encodeBatchCodec(zeros(50000), 1))
	send("plain-gzip-small-inflates-1k", "plain", encodeBatchCodec(zeros(1000), 1))
	send("recompress-zstd-small-inflates-50k", "recompress", encodeBatchCodec(zeros(50000), 4))
	send("compact-gzip-small-inflates-50k", "compact", encodeBatchCodec(zeros(50000), 1))
	send("default-gzip-inflates-20m", "default", encodeBatchCodec(zeros(20<<20), 1))
}
