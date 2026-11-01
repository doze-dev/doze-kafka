package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// listOffsetAnswer is the whole answer of ListOffsets for one partition.
func listOffsetAnswer(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, ts int64, iso int8) kmsg.ListOffsetsResponseTopicPartition {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID, req.IsolationLevel = -1, iso
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Timestamp = ts
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	return resp.Topics[0].Partitions[0]
}

// A read-committed consumer's by-time lookup finds only what it may read: nothing
// at or above the last stable offset exists for it yet, however it is timed. A
// read-uncommitted one finds the open transaction's record.
func TestListOffsetsByTimeUnderReadCommittedStopsAtTheLastStableOffset(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	mustCreateTopic(t, ctx, admin, "lso", 1)
	p := txProducer(t, addr, "lso-p")

	stamp := func(cl *kgo.Client, ms int64, v string) {
		t.Helper()
		r := &kgo.Record{Topic: "lso", Partition: 0, Value: []byte(v), Timestamp: time.UnixMilli(ms)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	// Offset 0 is a plain record. The record at offset 1 is in a transaction left
	// open, so the last stable offset is 1.
	stamp(admin, 1000, "committed")
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	stamp(p, 2000, "open")

	const ru, rc = int8(0), int8(1)
	if a := listOffsetAnswer(t, ctx, admin, "lso", 1500, ru); a.ErrorCode != 0 || a.Offset != 1 || a.Timestamp != 2000 || a.LeaderEpoch != 0 {
		t.Errorf("read-uncommitted by time 1500: %+v, want the open record: offset 1, time 2000, epoch 0", a)
	}
	if a := listOffsetAnswer(t, ctx, admin, "lso", 1500, rc); a.ErrorCode != 0 || a.Offset != -1 || a.Timestamp != -1 || a.LeaderEpoch != -1 {
		t.Errorf("read-committed by time 1500: %+v, want nothing: offset -1, time -1, epoch -1", a)
	}
	if a := listOffsetAnswer(t, ctx, admin, "lso", 500, rc); a.Offset != 0 || a.Timestamp != 1000 {
		t.Errorf("read-committed by time 500: %+v, want the committed record at offset 0, time 1000", a)
	}
	if a := listOffsetAnswer(t, ctx, admin, "lso", -1, ru); a.Offset != 2 {
		t.Errorf("read-uncommitted latest: %d, want the log end 2", a.Offset)
	}
	if a := listOffsetAnswer(t, ctx, admin, "lso", -1, rc); a.Offset != 1 {
		t.Errorf("read-committed latest: %d, want the last stable offset 1", a.Offset)
	}
	// A lookup that lands exactly on the last stable offset is not found either.
	if a := listOffsetAnswer(t, ctx, admin, "lso", 2000, rc); a.Offset != -1 {
		t.Errorf("read-committed by time 2000, the open record's own: offset %d, want -1", a.Offset)
	}

	commit(t, ctx, p, true) // the record at 1 and the marker at 2.
	if a := listOffsetAnswer(t, ctx, admin, "lso", 1500, rc); a.Offset != 1 || a.Timestamp != 2000 {
		t.Errorf("read-committed by time 1500 once it is committed: %+v, want offset 1, time 2000", a)
	}
}
