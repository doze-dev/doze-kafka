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

// listAt asks ListOffsets (at the given request version) about one partition and
// returns the whole answer.
func listAt(t *testing.T, ctx context.Context, cl *kgo.Client, version int16, topic string, partition int32, timestamp int64) kmsg.ListOffsetsResponseTopicPartition {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	req.SetVersion(version)
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition, rp.Timestamp = partition, timestamp
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("ListOffsets: %v %+v", err, resp)
	}
	return resp.Topics[0].Partitions[0]
}

// writeStamped produces one record per timestamp to partition 0, each its own
// batch, with the timestamp the producer chose.
func writeStamped(t *testing.T, ctx context.Context, addr, topic string, stamps ...time.Time) {
	t.Helper()
	p, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.DisableIdempotentWrite())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i, ts := range stamps {
		r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v" + strconv.Itoa(i)), Timestamp: ts}
		if err := p.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// "Replay from ten minutes ago", offsetsForTimes and start-from-timestamp used to get
// the earliest offset whatever the time asked for, silently.
func TestListOffsetsByTimestampIsExact(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, cl, "ts", 1)
	mustCreateTopic(t, ctx, cl, "empty", 1)

	base := time.UnixMilli(1_700_000_000_000)
	var stamps []time.Time
	for i := range 5 {
		stamps = append(stamps, base.Add(time.Duration(i)*time.Minute)) // offsets 0..4, a minute apart.
	}
	writeStamped(t, ctx, addr, "ts", stamps...)
	ms := func(d time.Duration) int64 { return base.Add(d).UnixMilli() }

	for _, tc := range []struct {
		name   string
		target int64
		offset int64
		ts     int64
	}{
		{"before every record", ms(-time.Hour), 0, ms(0)},
		{"exactly the first", ms(0), 0, ms(0)},
		{"just after the first", ms(0) + 1, 1, ms(time.Minute)},
		{"exactly the third", ms(2 * time.Minute), 2, ms(2 * time.Minute)},
		{"between the third and fourth", ms(2*time.Minute) + 30_000, 3, ms(3 * time.Minute)},
		{"exactly the last", ms(4 * time.Minute), 4, ms(4 * time.Minute)},
		{"after every record", ms(4*time.Minute) + 1, -1, -1},
	} {
		got := listAt(t, ctx, cl, 7, "ts", 0, tc.target)
		if got.ErrorCode != 0 || got.Offset != tc.offset || got.Timestamp != tc.ts {
			t.Errorf("%s: %+v, want offset %d timestamp %d", tc.name, got, tc.offset, tc.ts)
		}
	}

	// -3: the record with the largest timestamp.
	if got := listAt(t, ctx, cl, 7, "ts", 0, -3); got.Offset != 4 || got.Timestamp != ms(4*time.Minute) {
		t.Errorf("max timestamp: %+v", got)
	}
	// Earliest and latest are what they were.
	if got := listAt(t, ctx, cl, 7, "ts", 0, -2); got.Offset != 0 {
		t.Errorf("earliest: %+v", got)
	}
	if got := listAt(t, ctx, cl, 7, "ts", 0, -1); got.Offset != 5 {
		t.Errorf("latest: %+v", got)
	}

	// Nothing to find in an empty partition, for a time or for the max.
	for _, ts := range []int64{ms(0), -3} {
		if got := listAt(t, ctx, cl, 7, "empty", 0, ts); got.ErrorCode != 0 || got.Offset != -1 || got.Timestamp != -1 {
			t.Errorf("empty partition at %d: %+v, want -1 and -1", ts, got)
		}
	}

	// v0 cannot say "none": it gets the log end, as it always did for a time past the data.
	// (Raw: the client library would send the newest version it can.)
	got := rawListAt(t, addr, 0, "ts", 0, ms(time.Hour))
	if len(got.OldStyleOffsets) != 1 || got.OldStyleOffsets[0] != 5 {
		t.Errorf("v0 past the data: %+v, want the log end 5", got)
	}
	got = rawListAt(t, addr, 0, "ts", 0, ms(2*time.Minute))
	if len(got.OldStyleOffsets) != 1 || got.OldStyleOffsets[0] != 2 {
		t.Errorf("v0 at the third: %+v", got)
	}
	if got := rawListAt(t, addr, 7, "ts", 3, ms(0)); got.ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("unknown partition: %+v", got)
	}
}

// Lookups see what is visible: records deleted below the start are not answers, and a
// restart changes nothing.
func TestListOffsetsByTimestampAfterDeleteAndRestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	addr, stop := bootAt(t, "127.0.0.1:0", dir, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	mustCreateTopic(t, ctx, cl, "r", 1)
	base := time.UnixMilli(1_700_000_000_000)
	var stamps []time.Time
	for i := range 6 {
		stamps = append(stamps, base.Add(time.Duration(i)*time.Second))
	}
	writeStamped(t, ctx, addr, "r", stamps...)
	if got := deleteRecords(t, ctx, cl, "r", 0, 3); got.ErrorCode != 0 {
		t.Fatal(got)
	}
	cl.Close()
	stop()

	stop = runBrokerAt(t, addr, dir, 4)
	defer stop()
	cl = newClient(t, addr)
	defer cl.Close()
	// Asking for a time of a deleted record gives the first one that is still there.
	if got := listAt(t, ctx, cl, 7, "r", 0, base.UnixMilli()); got.Offset != 3 || got.Timestamp != base.Add(3*time.Second).UnixMilli() {
		t.Errorf("a time before the start after a delete and restart: %+v, want offset 3", got)
	}
	if got := listAt(t, ctx, cl, 7, "r", 0, base.Add(4*time.Second).UnixMilli()); got.Offset != 4 {
		t.Errorf("a time inside the retained records: %+v", got)
	}
}

// A topic that stamps records with the broker's clock is searched by it.
func TestListOffsetsByTimestampOnALogAppendTimeTopic(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = "lat", 1, 1
	v := "LogAppendTime"
	cfg := kmsg.NewCreateTopicsRequestTopicConfig()
	cfg.Name, cfg.Value = "message.timestamp.type", &v
	ct.Configs = append(ct.Configs, cfg)
	req.Topics = append(req.Topics, ct)
	if resp, err := req.RequestWith(ctx, cl); err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create: %v %+v", err, resp)
	}
	before := time.Now().UnixMilli()
	// The producer's own timestamp (a day ago) is ignored by the topic.
	writeStamped(t, ctx, addr, "lat", time.Now().Add(-24*time.Hour), time.Now().Add(-24*time.Hour))
	after := time.Now().UnixMilli()

	if got := listAt(t, ctx, cl, 7, "lat", 0, before-1000); got.Offset != 0 || got.Timestamp < before || got.Timestamp > after {
		t.Errorf("by the broker's clock: %+v, want offset 0 stamped between %d and %d", got, before, after)
	}
	if got := listAt(t, ctx, cl, 7, "lat", 0, after+60_000); got.Offset != -1 {
		t.Errorf("a minute from now: %+v, want none", got)
	}
}

// rawListAt is listAt on a bare connection, at exactly the version asked for.
func rawListAt(t *testing.T, addr string, version int16, topic string, partition int32, timestamp int64) kmsg.ListOffsetsResponseTopicPartition {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	req.SetVersion(version)
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition, rp.Timestamp = partition, timestamp
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	var resp kmsg.ListOffsetsResponse
	resp.SetVersion(version)
	if !openRaw(t, addr).do(req, &resp) || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("ListOffsets v%d: no usable answer: %+v", version, resp)
	}
	return resp.Topics[0].Partitions[0]
}

// What a client does with it: a consumer told to start at a time begins at the first
// record from then on, rather than at the beginning of the log.
func TestAConsumerCanStartAtATime(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, cl, "from", 1)
	base := time.UnixMilli(1_700_000_000_000)
	var stamps []time.Time
	for i := range 6 {
		stamps = append(stamps, base.Add(time.Duration(i)*time.Minute))
	}
	writeStamped(t, ctx, addr, "from", stamps...)

	c, err := kgo.NewClient(kgo.SeedBrokers(addr),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"from": {0: kgo.NewOffset().AfterMilli(base.Add(2 * time.Minute).UnixMilli())}}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var got []string
	for len(got) < 3 {
		fs := c.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read only %v", got)
		}
		fs.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	if got[0] != "v2" || got[len(got)-1] != "v5" {
		t.Errorf("a consumer starting at the third minute read %v, want v2 through v5", got)
	}
}
