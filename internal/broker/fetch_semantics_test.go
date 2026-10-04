package broker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/broker"
)

// fetchReq builds a Fetch v11 by topic name for the given partitions.
func fetchReq(topic string, maxWaitMs, minBytes, maxBytes int32, parts map[int32][2]int64) *kmsg.FetchRequest {
	fr := kmsg.NewPtrFetchRequest()
	fr.SetVersion(11)
	fr.ReplicaID = -1
	fr.MaxWaitMillis, fr.MinBytes, fr.MaxBytes = maxWaitMs, minBytes, maxBytes
	fr.SessionEpoch = -1
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic = topic
	for p := int32(0); p < int32(len(parts)); p++ {
		v, ok := parts[p]
		if !ok {
			continue
		}
		rp := kmsg.NewFetchRequestTopicPartition()
		rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes = p, v[0], int32(v[1])
		rp.CurrentLeaderEpoch = -1
		rt.Partitions = append(rt.Partitions, rp)
	}
	fr.Topics = append(fr.Topics, rt)
	return fr
}

func produceBytes(t *testing.T, cl *kgo.Client, topic string, partition int32, n, size int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		r := &kgo.Record{Topic: topic, Partition: partition, Value: make([]byte, size)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// An error is an answer. A fetch whose partition is out of range, unknown, or
// deleted while it waits used to sit out the whole MaxWait first.
func TestFetchWithAnErrorAnswersImmediately(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	ctx := context.Background()
	mustCreateTopic(t, ctx, cl, "t", 2)

	fc := nameFetchClient(t, addr) // Fetch by topic name (v12 and below).
	timed := func(fr *kmsg.FetchRequest) (*kmsg.FetchResponse, time.Duration) {
		start := time.Now()
		resp, err := fr.RequestWith(ctx, fc)
		if err != nil {
			t.Fatal(err)
		}
		return resp, time.Since(start)
	}
	for name, fr := range map[string]*kmsg.FetchRequest{
		"offset above the log end": fetchReq("t", 5000, 1, 1<<20, map[int32][2]int64{0: {99, 1 << 20}}),
		"unknown topic":            fetchReq("nope", 5000, 1, 1<<20, map[int32][2]int64{0: {0, 1 << 20}}),
		"one bad partition index":  fetchReq("t", 5000, 1, 1<<20, map[int32][2]int64{0: {0, 1 << 20}, 1: {0, 1 << 20}, 2: {0, 1 << 20}}),
	} {
		if name == "one bad partition index" {
			fr.Topics[0].Partitions[2].Partition = 9
		}
		resp, took := timed(fr)
		var codes []int16
		for _, p := range resp.Topics[0].Partitions {
			codes = append(codes, p.ErrorCode)
		}
		want := map[string]int16{"offset above the log end": 1, "unknown topic": 3, "one bad partition index": 3}[name]
		if codes[len(codes)-1] != want {
			t.Errorf("%s: partition codes %v, want the last to be %d", name, codes, want)
		}
		if took > 2*time.Second {
			t.Errorf("%s: answered after %v of a 5s MaxWait; an error must not wait", name, took.Round(time.Millisecond))
		}
	}

	// Deleted while a fetch is parked on it: woken and answered.
	done := make(chan time.Duration, 1)
	go func() {
		_, took := timed(fetchReq("t", 8000, 1, 1<<20, map[int32][2]int64{0: {0, 1 << 20}}))
		done <- took
	}()
	time.Sleep(300 * time.Millisecond)
	del := kmsg.NewPtrDeleteTopicsRequest()
	del.TopicNames = []string{"t"} // v0-5
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.Topic = kmsg.StringPtr("t") // v6+
	del.Topics = append(del.Topics, dt)
	del.TimeoutMillis = 5000
	if _, err := del.RequestWith(ctx, newClient(t, addr)); err != nil {
		t.Fatal(err)
	}
	select {
	case took := <-done:
		if took > 3*time.Second {
			t.Errorf("a fetch parked on a topic that was then deleted answered after %v", took.Round(time.Millisecond))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the parked fetch never returned")
	}
}

// The request's MaxBytes bounds the whole response (bar one oversize first
// batch), and a partition later in the request still makes progress while an
// earlier one has a little data each time.
func TestFetchBudgetIsSharedAcrossPartitions(t *testing.T) {
	addr := startBroker(t, 4)
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	mustCreateTopic(t, ctx, cl, "t", 2)
	produceBytes(t, cl, "t", 0, 100, 100) // p0: 100 batches of ~170 bytes.
	produceBytes(t, cl, "t", 1, 400, 100) // p1: a backlog that outlasts the test.

	sizes := func(resp *kmsg.FetchResponse) (p0, p1 int) {
		for _, p := range resp.Topics[0].Partitions {
			if p.Partition == 0 {
				p0 = len(p.RecordBatches)
			} else {
				p1 = len(p.RecordBatches)
			}
		}
		return
	}
	both := map[int32][2]int64{0: {0, 1 << 20}, 1: {0, 1 << 20}}
	fc := nameFetchClient(t, addr)

	resp, err := fetchReq("t", 0, 1, 1000, both).RequestWith(ctx, fc)
	if err != nil {
		t.Fatal(err)
	}
	if p0, p1 := sizes(resp); p0 == 0 || p0+p1 > 1000 {
		t.Errorf("MaxBytes=1000: p0=%d bytes, p1=%d bytes; want some data and at most 1000 in total", p0, p1)
	}

	// p0 is caught up except for one small batch per fetch; p1 has a backlog. p1
	// must get the rest of the budget every time, not nothing.
	next0, next1, got1 := int64(100), int64(0), 0
	for i := 0; i < 10; i++ {
		produceBytes(t, cl, "t", 0, 1, 100)
		resp, err := fetchReq("t", 0, 1, 4096, map[int32][2]int64{0: {next0, 4096}, 1: {next1, 4096}}).RequestWith(ctx, fc)
		if err != nil {
			t.Fatal(err)
		}
		p0, p1 := sizes(resp)
		if p0+p1 > 4096 {
			t.Fatalf("fetch %d returned %d bytes for MaxBytes=4096", i, p0+p1)
		}
		next0++
		if p1 > 0 {
			got1++
			for _, p := range resp.Topics[0].Partitions {
				if p.Partition == 1 {
					next1 += countRecords(p.RecordBatches)
				}
			}
		}
	}
	if got1 < 9 {
		t.Errorf("the second partition got data in %d of 10 fetches while the first trickled; it is being starved", got1)
	}
}

// countRecords sums the record counts of the batches in raw.
func countRecords(raw []byte) int64 {
	var n int64
	for len(raw) >= 61 {
		size := 12 + int(uint32(raw[8])<<24|uint32(raw[9])<<16|uint32(raw[10])<<8|uint32(raw[11]))
		n += int64(uint32(raw[57])<<24 | uint32(raw[58])<<16 | uint32(raw[59])<<8 | uint32(raw[60]))
		if size > len(raw) {
			break
		}
		raw = raw[size:]
	}
	return n
}

// A long-poll is capped, so a client that asks to wait for weeks and goes away
// does not leave a goroutine parked for weeks. And a session the broker never
// created is refused rather than answered as an empty success.
func TestFetchWaitIsCappedAndUnknownSessionsAreRefused(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	ctx := context.Background()
	mustCreateTopic(t, ctx, cl, "t", 1)

	fr := fetchReq("", 0, 1, 1<<20, nil)
	fr.Topics = nil
	fr.SessionID, fr.SessionEpoch = 4242, 7
	resp, err := fr.RequestWith(ctx, nameFetchClient(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 70 {
		t.Errorf("continuing a session that does not exist: code %d, want FETCH_SESSION_ID_NOT_FOUND (70)", resp.ErrorCode)
	}
}

// A fetch that asks to wait far longer than the cap is answered at the cap.
func TestFetchLongPollIsCapped(t *testing.T) {
	old := broker.SetMaxFetchWaitForTest(400 * time.Millisecond)
	defer broker.SetMaxFetchWaitForTest(old)
	addr := startBroker(t, 4)
	ctx := context.Background()
	mustCreateTopic(t, ctx, newClient(t, addr), "t", 1)
	start := time.Now()
	if _, err := fetchReq("t", 20_000, 1<<20, 1<<20, map[int32][2]int64{0: {0, 1 << 20}}).RequestWith(ctx, nameFetchClient(t, addr)); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("a 20s long-poll with the cap at 400ms took %v", took.Round(time.Millisecond))
	}
}

// A produce that lands after a fetch has read its partitions and before it starts
// waiting must wake it. Subscribing only after the read would miss that signal and
// sleep the whole MaxWait with the data already there.
func TestFetchIsNotLateForAProduceThatLandsBetweenReadAndWait(t *testing.T) {
	addr := startBroker(t, 4)
	ctx := context.Background()
	cl := newClient(t, addr)
	mustCreateTopic(t, ctx, cl, "t", 1)

	var once sync.Once
	restore := broker.SetAfterFetchReadForTest(func() {
		once.Do(func() { produceBytes(t, cl, "t", 0, 1, 10) })
	})
	defer restore()

	start := time.Now()
	resp, err := fetchReq("t", 6000, 1, 1<<20, map[int32][2]int64{0: {0, 1 << 20}}).RequestWith(ctx, nameFetchClient(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 3*time.Second || len(resp.Topics[0].Partitions[0].RecordBatches) == 0 {
		t.Errorf("the fetch took %v and returned %d bytes; the produce between its read and its wait was missed",
			took.Round(time.Millisecond), len(resp.Topics[0].Partitions[0].RecordBatches))
	}
}
