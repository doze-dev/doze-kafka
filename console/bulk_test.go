package console

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// rawClient is a Kafka client of the test's own, producing to the partition it is
// told to.
func rawClient(t testing.TB, addr string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// bulkProduce writes n records to one partition, value(i) each, fast and in order.
// at, if not zero, is the first record's timestamp, a millisecond more for each after.
func bulkProduce(t testing.TB, cl *kgo.Client, topic string, partition int32, n int, value func(i int) string, at time.Time) {
	t.Helper()
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value(i))}
		if !at.IsZero() {
			recs[i].Timestamp = at.Add(time.Duration(i) * time.Millisecond)
		}
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// deleteBefore moves a partition's log start to off (DeleteRecords).
func deleteBefore(t testing.TB, cl *kgo.Client, topic string, partition int32, off int64) {
	t.Helper()
	req := kmsg.NewPtrDeleteRecordsRequest()
	rt := kmsg.NewDeleteRecordsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewDeleteRecordsRequestTopicPartition()
	rp.Partition, rp.Offset = partition, off
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			if p.ErrorCode != 0 {
				t.Fatalf("DeleteRecords: error code %d", p.ErrorCode)
			}
		}
	}
}
