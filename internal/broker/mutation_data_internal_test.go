package broker

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// Tests that pin the exact answers of the data-path handlers: the sentinel offsets
// and times an error carries, which a client reads.

// produceTo sends one batch to topic/partition through the handler and returns the
// partition's answer.
func produceTo(t *testing.T, b *Broker, topic string, partition int32, acks int16, records []byte) kmsg.ProduceResponseTopicPartition {
	t.Helper()
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(8)
	req.Acks = acks
	req.TimeoutMillis = 1000
	pt := kmsg.NewProduceRequestTopic()
	pt.Topic = topic
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Partition, pp.Records = partition, records
	pt.Partitions = append(pt.Partitions, pp)
	req.Topics = append(req.Topics, pt)
	out, err := b.handleProduce(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.Produce), APIVersion: 8, CorrelationID: 1},
		Body:   req.AppendTo(nil),
	})
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(8)
	if err := resp.ReadFrom(out[4:]); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp.Topics[0].Partitions[0]
}

// withBaseOffset is a batch whose base offset a producer did not leave at 0.
func withBaseOffset(batch []byte, base int64) []byte {
	c := append([]byte{}, batch...)
	binary.BigEndian.PutUint64(c[0:], uint64(base))
	return c
}

func TestProduceToAPartitionThatIsNotThereHasNoOffset(t *testing.T) {
	var failSync bool
	b, _ := newTestBroker(t, &failSync)
	for _, tc := range []struct {
		name      string
		topic     string
		partition int32
	}{
		{"the first partition past the last", "t", 1},
		{"a negative partition", "t", -1},
		{"a topic that does not exist", "nosuch", 0},
	} {
		p := produceTo(t, b, tc.topic, tc.partition, 1, plainBatch())
		if p.ErrorCode != kerr.UnknownTopicOrPartition.Code || p.BaseOffset != -1 {
			t.Errorf("%s: code %d base offset %d, want UNKNOWN_TOPIC_OR_PARTITION and -1 (claim: traffic R7)", tc.name, p.ErrorCode, p.BaseOffset)
		}
	}
	if p := produceTo(t, b, "t", 0, 1, plainBatch()); p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Errorf("the last partition: code %d base offset %d, want 0 and 0", p.ErrorCode, p.BaseOffset)
	}
}

// A stored batch under CreateTime has no append time (-1), and the answer carries
// the partition's log start offset.
func TestProduceSuccessCarriesNoAppendTime(t *testing.T) {
	var failSync bool
	b, _ := newTestBroker(t, &failSync)
	plain := produceTo(t, b, "t", 0, 1, plainBatch())
	if plain.ErrorCode != 0 || plain.LogAppendTime != -1 || plain.LogStartOffset != 0 {
		t.Errorf("a plain batch: code %d append time %d log start %d, want 0, -1, 0", plain.ErrorCode, plain.LogAppendTime, plain.LogStartOffset)
	}
	idem := produceTo(t, b, "t", 0, 1, idemBatch(5, 0, 0, 1, 0))
	if idem.ErrorCode != 0 || idem.BaseOffset != 1 || idem.LogAppendTime != -1 || idem.LogStartOffset != 0 {
		t.Errorf("an idempotent batch: code %d base %d append time %d log start %d, want 0, 1, -1, 0", idem.ErrorCode, idem.BaseOffset, idem.LogAppendTime, idem.LogStartOffset)
	}
}

// A batch refused for its sequence has no offset (-1) and no time, and carries the
// partition's log start offset, wherever that now is.
func TestProduceRefusedForItsSequenceCarriesTheLogStart(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)
	if p := produceTo(t, b, "t", 0, 1, idemBatch(9, 0, 0, 3, 0)); p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Fatalf("seq 0-2: code %d offset %d", p.ErrorCode, p.BaseOffset)
	}
	if start, err := logs.DeleteBefore("t", 0, 2); err != nil || start != 2 {
		t.Fatalf("DeleteBefore: %d %v", start, err)
	}
	for _, tc := range []struct {
		name  string
		batch []byte
		code  int16
	}{
		{"a gap", idemBatch(9, 0, 7, 1, 0), kerr.OutOfOrderSequenceNumber.Code},
	} {
		p := produceTo(t, b, "t", 0, 1, tc.batch)
		if p.ErrorCode != tc.code || p.BaseOffset != -1 || p.LogAppendTime != -1 || p.LogStartOffset != 2 {
			t.Errorf("%s: code %d base %d append time %d log start %d, want %d, -1, -1, 2", tc.name, p.ErrorCode, p.BaseOffset, p.LogAppendTime, p.LogStartOffset, tc.code)
		}
	}
	// A new epoch fences the old one.
	if p := produceTo(t, b, "t", 0, 1, idemBatch(9, 1, 0, 1, 0)); p.ErrorCode != 0 {
		t.Fatalf("epoch 1: code %d", p.ErrorCode)
	}
	p := produceTo(t, b, "t", 0, 1, idemBatch(9, 0, 3, 1, 0))
	if p.ErrorCode != kerr.InvalidProducerEpoch.Code || p.BaseOffset != -1 || p.LogAppendTime != -1 || p.LogStartOffset != 2 {
		t.Errorf("a fenced epoch: code %d base %d append time %d log start %d, want INVALID_PRODUCER_EPOCH, -1, -1, 2", p.ErrorCode, p.BaseOffset, p.LogAppendTime, p.LogStartOffset)
	}
}

// A refusal made before Kafka asked the log anything has no log start offset (-1);
// one made after carries it. Kafka's text rides along where it has one (recorded:
// produce-validation).
func TestProduceRefusalsCarryWhatKafkaCarries(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)
	if p := produceTo(t, b, "t", 0, 1, plainBatch()); p.ErrorCode != 0 {
		t.Fatal(p.ErrorCode)
	}
	if p := produceTo(t, b, "t", 0, 1, plainBatch()); p.ErrorCode != 0 {
		t.Fatal(p.ErrorCode)
	}
	if start, err := logs.DeleteBefore("t", 0, 1); err != nil || start != 1 {
		t.Fatalf("DeleteBefore: %d %v", start, err)
	}

	badMagic := plainBatch()
	badMagic[16] = 3

	for _, tc := range []struct {
		name     string
		records  []byte
		code     int16
		logStart int64
		message  string // "" = none.
	}{
		{"a base offset a producer does not write", withBaseOffset(plainBatch(), 5), kerr.InvalidRecord.Code, 1, ""},
		{"a format version nothing has", badMagic, kerr.CorruptMessage.Code, -1, "Invalid magic found in record: 3"},
	} {
		p := produceTo(t, b, "t", 0, 1, tc.records)
		if p.ErrorCode != tc.code || p.BaseOffset != -1 || p.LogAppendTime != -1 || p.LogStartOffset != tc.logStart {
			t.Errorf("%s: code %d base %d append time %d log start %d, want %d, -1, -1, %d", tc.name, p.ErrorCode, p.BaseOffset, p.LogAppendTime, p.LogStartOffset, tc.code, tc.logStart)
		}
		switch {
		case tc.message == "" && p.ErrorMessage != nil:
			t.Errorf("%s: message %q, want none", tc.name, *p.ErrorMessage)
		case tc.message != "" && (p.ErrorMessage == nil || *p.ErrorMessage != tc.message):
			t.Errorf("%s: message %v, want %q", tc.name, p.ErrorMessage, tc.message)
		}
	}
}

// listOffsetsAt sends one ListOffsets entry through the handler at a version and
// returns the partition's answer.
func listOffsetsAt(t *testing.T, b *Broker, version int16, isolation int8, topic string, partition int32, timestamp int64, leaderEpoch int32) kmsg.ListOffsetsResponseTopicPartition {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	req.SetVersion(version)
	req.ReplicaID, req.IsolationLevel = -1, isolation
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition, rp.Timestamp, rp.CurrentLeaderEpoch = partition, timestamp, leaderEpoch
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	body := req.AppendTo(nil)
	if req.IsFlexible() {
		body = req.AppendTo([]byte{0})
	}
	out, err := typed(b.handleListOffsets)(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.ListOffsets), APIVersion: version, CorrelationID: 1},
		Body:   body,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := kmsg.NewPtrListOffsetsResponse()
	resp.SetVersion(version)
	skip := 4
	if resp.IsFlexible() {
		skip = 5
	}
	if err := resp.ReadFrom(out[skip:]); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp.Topics[0].Partitions[0]
}

// ListOffsets by time: the first record at or after the timestamp, with its time and
// the leader epoch it was found under; nothing found is -1 (the log end for v0, which
// has no way to say none), with no time and no epoch. Records of plainBatch carry
// timestamp 0.
func TestListOffsetsByTimeAnswersWhatItFoundOrNothing(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)

	// An empty partition has nothing at any time, and asking opens nothing: an open
	// partition holds descriptors for the life of the broker. Its latest is offset 0
	// under epoch 0.
	empty := listOffsetsAt(t, b, 5, 0, "t", 0, 1, -1)
	if empty.ErrorCode != 0 || empty.Offset != -1 || empty.Timestamp != -1 || empty.LeaderEpoch != -1 {
		t.Errorf("by time, nothing stored: %+v, want offset -1, time -1, epoch -1", empty)
	}
	if n := logs.OpenPartitions(); n != 0 {
		t.Errorf("a by-time lookup in an empty partition opened %d partitions", n)
	}
	if latest := listOffsetsAt(t, b, 5, 0, "t", 0, listOffsetsLatest, -1); latest.Offset != 0 || latest.LeaderEpoch != 0 {
		t.Errorf("latest of an empty partition: offset %d epoch %d, want 0 and 0", latest.Offset, latest.LeaderEpoch)
	}

	for range 2 {
		if p := produceTo(t, b, "t", 0, 1, plainBatch()); p.ErrorCode != 0 {
			t.Fatal(p.ErrorCode)
		}
	}
	found := listOffsetsAt(t, b, 5, 0, "t", 0, 0, -1)
	if found.ErrorCode != 0 || found.Offset != 0 || found.Timestamp != 0 || found.LeaderEpoch != 0 {
		t.Errorf("by time 0: %+v, want offset 0, time 0, epoch 0", found)
	}
	none := listOffsetsAt(t, b, 5, 0, "t", 0, 1, -1)
	if none.ErrorCode != 0 || none.Offset != -1 || none.Timestamp != -1 || none.LeaderEpoch != -1 {
		t.Errorf("by time after every record: %+v, want offset -1, time -1, epoch -1", none)
	}
	v0 := listOffsetsAt(t, b, 0, 0, "t", 0, 1, -1)
	if v0.ErrorCode != 0 || len(v0.OldStyleOffsets) != 1 || v0.OldStyleOffsets[0] != 2 {
		t.Errorf("v0 by time after every record: %+v, want the log end 2 as its one offset", v0)
	}
	v0found := listOffsetsAt(t, b, 0, 0, "t", 0, 0, -1)
	if len(v0found.OldStyleOffsets) != 1 || v0found.OldStyleOffsets[0] != 0 {
		t.Errorf("v0 by time 0: %+v, want offset 0", v0found)
	}
}

// Every partition-addressed API answers UNKNOWN_TOPIC_OR_PARTITION for the first
// partition past the last and for a negative one, and serves the last.
func TestListOffsetsRefusesThePartitionPastTheLast(t *testing.T) {
	var failSync bool
	b, _ := newTestBroker(t, &failSync)
	for _, tc := range []struct {
		partition int32
		code      int16
	}{
		{0, 0},
		{1, kerr.UnknownTopicOrPartition.Code},
		{-1, kerr.UnknownTopicOrPartition.Code},
	} {
		if p := listOffsetsAt(t, b, 5, 0, "t", tc.partition, listOffsetsLatest, -1); p.ErrorCode != tc.code {
			t.Errorf("partition %d: code %d, want %d", tc.partition, p.ErrorCode, tc.code)
		}
	}
	if p := listOffsetsAt(t, b, 5, 0, "nosuch", 0, listOffsetsLatest, -1); p.ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("an unknown topic: code %d", p.ErrorCode)
	}
}
