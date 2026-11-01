package broker

import (
	"context"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// A partition that is not there, of a topic that is or that is not, is answered
// UNKNOWN_TOPIC_OR_PARTITION with base offset -1, as Apache Kafka answers it
// (conformance/recordings/produce-mixed-partition-errors.json); the partition that is
// there is written in the same request.
func TestProduceUnknownPartitionsAnswerMinusOne(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(8)
	req.Acks, req.TimeoutMillis = -1, 1000
	for _, c := range []struct {
		topic string
		parts []int32
	}{{"t", []int32{0, 7, -1}}, {"no-such-topic", []int32{0}}} {
		pt := kmsg.NewProduceRequestTopic()
		pt.Topic = c.topic
		for _, p := range c.parts {
			pp := kmsg.NewProduceRequestTopicPartition()
			pp.Partition, pp.Records = p, plainBatch()
			pt.Partitions = append(pt.Partitions, pp)
		}
		req.Topics = append(req.Topics, pt)
	}
	out, err := b.handleProduce(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.Produce), APIVersion: 8, CorrelationID: 1},
		Body:   req.AppendTo(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(8)
	if err := resp.ReadFrom(out[4:]); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		code   int16
		offset int64
	}
	var have []answer
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			have = append(have, answer{p.ErrorCode, p.BaseOffset})
		}
	}
	unknown := answer{kerr.UnknownTopicOrPartition.Code, -1}
	want := []answer{{0, 0}, unknown, unknown, unknown}
	if len(have) != len(want) {
		t.Fatalf("answers: %+v", have)
	}
	for i := range want {
		if have[i] != want[i] {
			t.Errorf("answer %d: %+v, want %+v", i, have[i], want[i])
		}
	}
	if part, err := logs.Partition("t", 0); err != nil || part.LogEndOffset() != 1 {
		t.Error("the partition that exists was not written")
	}
}
