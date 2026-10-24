package broker_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// produceNoise produces n records of size incompressible bytes, one batch each, so
// that a batch's size is the same whatever the client compresses with.
func produceNoise(t *testing.T, addr string, topic string, partition int32, n, size int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		v := make([]byte, size)
		for j := range v {
			v[j] = byte(rand.IntN(256))
		}
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: partition, Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// Tests of how a Fetch is bounded and when it answers. The batches are produced one
// at a time, so each is a batch of its own and a limit counts them.

// fetchBytes returns the record bytes of each partition of a fetch of topic.
func fetchBytes(t *testing.T, fr *kmsg.FetchRequest, addr string) []int {
	t.Helper()
	resp, err := fr.RequestWith(context.Background(), nameFetchClient(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 0 || len(resp.Topics) != 1 {
		t.Fatalf("fetch: code %d, %d topics", resp.ErrorCode, len(resp.Topics))
	}
	var sizes []int
	for _, p := range resp.Topics[0].Partitions {
		if p.ErrorCode != 0 {
			t.Fatalf("partition %d: code %d", p.Partition, p.ErrorCode)
		}
		sizes = append(sizes, len(p.RecordBatches))
	}
	return sizes
}

// oneBatchSize is the bytes of one produced batch of a 1000-byte value.
func oneBatchSize(t *testing.T, addr, topic string) int {
	t.Helper()
	sizes := fetchBytes(t, fetchReq(topic, 0, 1, 1<<20, map[int32][2]int64{0: {0, 1 << 20}}), addr)
	// Exactly one batch is in partition 0 of the topic it is asked of.
	return sizes[0]
}

// The request's MaxBytes is shared by the partitions in the order they are asked
// for. The first batch of the first partition with data is returned whatever the
// limit; after that a partition's batch is returned only if it fits what is left.
func TestFetchSharesMaxBytesAcrossPartitions(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	ctx := context.Background()
	mustCreateTopic(t, ctx, cl, "one", 1)
	mustCreateTopic(t, ctx, cl, "b", 3)
	produceNoise(t, addr, "one", 0, 1, 1000)
	size := oneBatchSize(t, addr, "one")
	if size < 1000 {
		t.Fatalf("a batch of a 1000-byte value is %d bytes", size)
	}
	for p := int32(0); p < 3; p++ {
		produceNoise(t, addr, "b", p, 1, 1000)
	}
	all := map[int32][2]int64{0: {0, 1 << 20}, 1: {0, 1 << 20}, 2: {0, 1 << 20}}

	for _, tc := range []struct {
		name     string
		maxBytes int32
		want     []int
	}{
		{"room for exactly two batches", int32(2 * size), []int{size, size, 0}},
		{"room for two batches and some", int32(2*size + 10), []int{size, size, 0}},
		{"less than one batch: the first still comes", int32(size - 1), []int{size, 0, 0}},
		{"room for all", int32(3 * size), []int{size, size, size}},
		{"no limit at all", 0, []int{size, size, size}},
	} {
		got := fetchBytes(t, fetchReq("b", 0, 1, tc.maxBytes, all), addr)
		if len(got) != 3 || got[0] != tc.want[0] || got[1] != tc.want[1] || got[2] != tc.want[2] {
			t.Errorf("%s: record bytes %v, want %v", tc.name, got, tc.want)
		}
	}

	// A partition with nothing in it has not used the allowance of one batch over
	// the limit: the next partition's first batch still comes.
	mustCreateTopic(t, ctx, cl, "gap", 2)
	produceNoise(t, addr, "gap", 1, 1, 1000)
	got := fetchBytes(t, fetchReq("gap", 0, 1, int32(size/2), map[int32][2]int64{0: {0, 1 << 20}, 1: {0, 1 << 20}}), addr)
	if len(got) != 2 || got[0] != 0 || got[1] != size {
		t.Errorf("an empty partition first: record bytes %v, want [0 %d]", got, size)
	}
}

// A partition's own limit, else the request's, else 1 MiB: each is a limit on how
// many whole batches one partition returns (the first always comes).
func TestFetchPartitionLimitFallsBackToTheRequestsThenTheDefault(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	ctx := context.Background()
	mustCreateTopic(t, ctx, cl, "one", 1)
	mustCreateTopic(t, ctx, cl, "many", 1)
	produceNoise(t, addr, "one", 0, 1, 1000)
	size := oneBatchSize(t, addr, "one")
	produceNoise(t, addr, "many", 0, 4, 1000)

	for _, tc := range []struct {
		name        string
		partMax     int64
		requestMax  int32
		wantBatches int
	}{
		{"the partition's own limit", int64(2*size + 10), 1 << 20, 2},
		{"the request's when the partition sets none", 0, int32(3*size + 10), 3},
		{"the default when neither is set", 0, 0, 4},
	} {
		fr := fetchReq("many", 0, 1, tc.requestMax, map[int32][2]int64{0: {0, tc.partMax}})
		got := fetchBytes(t, fr, addr)
		if len(got) != 1 || got[0] != tc.wantBatches*size {
			t.Errorf("%s: record bytes %v, want %d batches of %d", tc.name, got, tc.wantBatches, size)
		}
	}
}

// A fetch is answered as soon as it holds MinBytes, and not before it has them
// unless MaxWait runs out: exactly MinBytes is enough.
func TestFetchAnswersAtMinBytesAndNotBefore(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	ctx := context.Background()
	mustCreateTopic(t, ctx, cl, "one", 1)
	produceNoise(t, addr, "one", 0, 1, 1000)
	size := oneBatchSize(t, addr, "one")
	fc := nameFetchClient(t, addr)

	timed := func(minBytes int32, maxWait int32) time.Duration {
		start := time.Now()
		resp, err := fetchReq("one", maxWait, minBytes, 1<<20, map[int32][2]int64{0: {0, 1 << 20}}).RequestWith(ctx, fc)
		if err != nil || resp.ErrorCode != 0 {
			t.Fatalf("fetch: %v %+v", err, resp)
		}
		return time.Since(start)
	}
	if d := timed(int32(size), 20_000); d > 5*time.Second {
		t.Errorf("a fetch with MinBytes equal to the %d bytes held took %v of its 20s MaxWait", size, d.Round(time.Millisecond))
	}
	if d := timed(int32(size+1), 600); d < 500*time.Millisecond {
		t.Errorf("a fetch wanting one byte more than the %d held took %v of its 600ms MaxWait", size, d.Round(time.Millisecond))
	}
}

// A session id the broker does not keep is refused only when the request goes on
// with it: epoch 0 is a full fetch, and session 0 is no session.
func TestFetchSessionsAreRefusedOnlyWhenContinued(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	ctx := context.Background()
	mustCreateTopic(t, ctx, cl, "t", 1)
	fc := nameFetchClient(t, addr)
	for _, tc := range []struct {
		name         string
		id, epoch    int32
		wantNotFound bool
	}{
		{"a continuation", 4242, 1, true},
		{"a later continuation", 4242, 9, true},
		{"a full fetch that names a session", 4242, 0, false},
		{"a full fetch with no session", 0, 0, false},
		{"no session, whatever the epoch", 0, 5, false},
		{"a session closed by the epoch -1", 4242, -1, false},
	} {
		fr := fetchReq("t", 0, 1, 1<<20, map[int32][2]int64{0: {0, 1 << 20}})
		fr.SessionID, fr.SessionEpoch = tc.id, tc.epoch
		resp, err := fr.RequestWith(ctx, fc)
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.ErrorCode == 70; got != tc.wantNotFound || (!got && resp.ErrorCode != 0) {
			t.Errorf("%s: code %d, want FETCH_SESSION_ID_NOT_FOUND(70)=%v", tc.name, resp.ErrorCode, tc.wantNotFound)
		}
	}
}
