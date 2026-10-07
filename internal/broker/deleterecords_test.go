package broker_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// deleteRecords asks to move partition's start to offset and returns the answer.
func deleteRecords(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, partition int32, offset int64) kmsg.DeleteRecordsResponseTopicPartition {
	t.Helper()
	req := kmsg.NewPtrDeleteRecordsRequest()
	rt := kmsg.NewDeleteRecordsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewDeleteRecordsRequestTopicPartition()
	rp.Partition, rp.Offset = partition, offset
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("DeleteRecords: %v %+v", err, resp)
	}
	return resp.Topics[0].Partitions[0]
}

func writeRecords(t *testing.T, ctx context.Context, addr, topic string, n int) {
	t.Helper()
	p, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := range n {
		if err := p.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("r" + strconv.Itoa(i))}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// fetchErr is the error code of a fetch at offset (the shared fetchCode helper asks
// by name at a version every client library can still send).
func fetchErr(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, offset int64) int16 {
	t.Helper()
	code, _ := fetchCode(t, ctx, cl, topic, offset)
	return code
}

// DeleteRecords is what Kafka Streams and test suites use to clear a topic. What is
// below the new start is gone to fetches and to ListOffsets, what is above is not
// moved, and an offset that cannot be honoured says so rather than being guessed at.
func TestDeleteRecordsMovesTheStart(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	fc := nameFetchClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, cl, "dr", 2)
	writeRecords(t, ctx, addr, "dr", 10)

	if got := deleteRecords(t, ctx, cl, "dr", 0, 4); got.ErrorCode != 0 || got.LowWatermark != 4 {
		t.Fatalf("delete to 4: %+v", got)
	}
	if start := listOffset(t, ctx, cl, "dr", 0, -2); start != 4 {
		t.Errorf("earliest offset = %d, want 4", start)
	}
	if end := listOffset(t, ctx, cl, "dr", 0, -1); end != 10 {
		t.Errorf("latest offset = %d, want 10: records above the start must not move", end)
	}
	if c := fetchErr(t, ctx, fc, "dr", 3); c != kerr.OffsetOutOfRange.Code {
		t.Errorf("fetch below the new start: code %d, want OFFSET_OUT_OF_RANGE", c)
	}
	if c := fetchErr(t, ctx, fc, "dr", 4); c != 0 {
		t.Errorf("fetch at the new start: code %d", c)
	}

	// Below the start is a no-op that reports the start; above the end is refused.
	if got := deleteRecords(t, ctx, cl, "dr", 0, 2); got.ErrorCode != 0 || got.LowWatermark != 4 {
		t.Errorf("delete below the start: %+v, want a no-op at 4", got)
	}
	if got := deleteRecords(t, ctx, cl, "dr", 0, 11); got.ErrorCode != kerr.OffsetOutOfRange.Code {
		t.Errorf("delete above the end: code %d", got.ErrorCode)
	}
	// The client library refuses these itself (it shards by partition), so they go raw.
	for _, tc := range []struct {
		topic string
		part  int32
	}{{"dr", 5}, {"dr", -1}, {"nope", 0}} {
		if code := rawDeleteRecords(t, addr, tc.topic, tc.part, 1); code != kerr.UnknownTopicOrPartition.Code {
			t.Errorf("DeleteRecords %s/%d: code %d, want UNKNOWN_TOPIC_OR_PARTITION", tc.topic, tc.part, code)
		}
	}
	if start := listOffset(t, ctx, cl, "dr", 0, -2); start != 4 {
		t.Errorf("refused requests moved the start to %d", start)
	}

	// -1 is the high watermark: everything goes, and the other partition is untouched.
	if got := deleteRecords(t, ctx, cl, "dr", 0, -1); got.ErrorCode != 0 || got.LowWatermark != 10 {
		t.Errorf("delete to the high watermark: %+v", got)
	}
	if start, end := listOffset(t, ctx, cl, "dr", 0, -2), listOffset(t, ctx, cl, "dr", 0, -1); start != 10 || end != 10 {
		t.Errorf("after emptying: start %d end %d, want 10 and 10", start, end)
	}
	if end := listOffset(t, ctx, cl, "dr", 1, -1); end != 0 {
		t.Errorf("partition 1's end = %d", end)
	}
}

// The start is recorded: a restart finds it, and records written afterwards go on
// from the end.
func TestDeleteRecordsSurvivesARestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	addr := "127.0.0.1:" + strconv.Itoa(freePort(t))
	stop := runBrokerAt(t, addr, dir, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	mustCreateTopic(t, ctx, cl, "keep", 1)
	writeRecords(t, ctx, addr, "keep", 6)
	if got := deleteRecords(t, ctx, cl, "keep", 0, 3); got.ErrorCode != 0 {
		t.Fatal(got)
	}
	cl.Close()
	stop()

	stop = runBrokerAt(t, addr, dir, 4)
	defer stop()
	cl = newClient(t, addr)
	defer cl.Close()
	if start := listOffset(t, ctx, cl, "keep", 0, -2); start != 3 {
		t.Fatalf("after a restart the start is %d, want 3", start)
	}
	if c := fetchErr(t, ctx, nameFetchClient(t, addr), "keep", 2); c != kerr.OffsetOutOfRange.Code {
		t.Errorf("fetch below the recorded start after a restart: code %d", c)
	}
	writeRecords(t, ctx, addr, "keep", 1)
	if end := listOffset(t, ctx, cl, "keep", 0, -1); end != 7 {
		t.Errorf("end after one more record = %d, want 7", end)
	}
}

// Clearing a topic this way keeps the topic and what its consumers committed, which
// deleting and recreating it did not: a group that had read everything carries on with
// the next record instead of starting over from its reset policy.
func TestDeleteRecordsKeepsTheTopicAndItsGroupsPlace(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, cl, "clear", 1)
	writeRecords(t, ctx, addr, "clear", 5)
	if got := groupConsume(t, ctx, addr, "g", "clear", 5, true); len(got) != 5 {
		t.Fatalf("first read: %v", got)
	}

	before := topicID(t, ctx, cl, "clear")
	if got := deleteRecords(t, ctx, cl, "clear", 0, -1); got.ErrorCode != 0 {
		t.Fatal(got)
	}
	if after := topicID(t, ctx, cl, "clear"); after != before {
		t.Errorf("the topic id changed from %v to %v", before, after)
	}
	writeRecords(t, ctx, addr, "clear", 2)
	got := groupConsume(t, ctx, addr, "g", "clear", 2, true)
	if len(got) != 2 || got[0] != "r0" {
		t.Errorf("the group read %v after the clear, want the two new records", got)
	}
	if start := listOffset(t, ctx, cl, "clear", 0, -2); start != 5 {
		t.Errorf("start = %d, want 5", start)
	}
}

func topicID(t *testing.T, ctx context.Context, cl *kgo.Client, topic string) [16]byte {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(10)
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &topic
	req.Topics = append(req.Topics, mt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	mtp, _ := findTopic(resp, topic)
	return mtp.TopicID
}

// rawDeleteRecords sends one DeleteRecords on a bare connection and returns the
// partition's error code.
func rawDeleteRecords(t *testing.T, addr, topic string, partition int32, offset int64) int16 {
	t.Helper()
	req := kmsg.NewPtrDeleteRecordsRequest()
	req.SetVersion(2)
	rt := kmsg.NewDeleteRecordsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewDeleteRecordsRequestTopicPartition()
	rp.Partition, rp.Offset = partition, offset
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	var resp kmsg.DeleteRecordsResponse
	resp.SetVersion(2) // rawConn reads the header by the response's own version.
	if !openRaw(t, addr).do(req, &resp) || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("DeleteRecords %s/%d: no usable answer: %+v", topic, partition, resp)
	}
	return resp.Topics[0].Partitions[0].ErrorCode
}
