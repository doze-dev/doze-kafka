package console

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// listOffsetsSince is the ListOffsets requests that reached the broker after the first
// `skip` of them: the ones a call just made.
func listOffsetsSince(sn *sniffer, skip int) []*kmsg.ListOffsetsRequest {
	var out []*kmsg.ListOffsetsRequest
	for _, r := range sn.sent(2)[skip:] {
		out = append(out, r.(*kmsg.ListOffsetsRequest))
	}
	return out
}

// timestampsAsked is, per partition, the timestamps a ListOffsets request asks for.
func timestampsAsked(r *kmsg.ListOffsetsRequest) map[string][]int64 {
	out := map[string][]int64{}
	for _, t := range r.Topics {
		for _, p := range t.Partitions {
			k := fmt.Sprintf("%s/%d", t.Topic, p.Partition)
			out[k] = append(out[k], p.Timestamp)
		}
	}
	return out
}

func equalInts(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// What the backend asks the broker for when it wants offsets: a consumer's view (replica
// -1), the earliest and latest of every partition, and for the bounds also the newest
// timestamp; the stable offset is asked read-committed.
func TestBackendAsksTheBrokerForOffsetsAsAConsumer(t *testing.T) {
	c, sn := newSniffedConsole(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "lo", 2, nil); err != nil {
		t.Fatal(err)
	}
	check := func(what string, skip int, want map[string][]int64, isolation int8) int {
		t.Helper()
		rs := listOffsetsSince(sn, skip)
		if len(rs) != 1 {
			t.Fatalf("%s sent %d ListOffsets requests, want 1", what, len(rs))
		}
		r := rs[0]
		if r.ReplicaID != -1 || r.IsolationLevel != isolation {
			t.Errorf("%s: replica %d isolation %d, want -1 and %d", what, r.ReplicaID, r.IsolationLevel, isolation)
		}
		got := timestampsAsked(r)
		if len(got) != len(want) {
			t.Errorf("%s asked about %v, want %v", what, got, want)
		}
		for k, w := range want {
			if !equalInts(got[k], w) {
				t.Errorf("%s asked %s for %v, want %v", what, k, got[k], w)
			}
		}
		return skip + 1
	}
	n := 0
	if _, err := c.be.ListTopics(ctx); err != nil {
		t.Fatal(err)
	}
	n = check("ListTopics", n, map[string][]int64{"lo/0": {-2, -1}, "lo/1": {-2, -1}}, 0)
	if _, err := c.be.PartitionBounds(ctx, "lo", 2); err != nil {
		t.Fatal(err)
	}
	n = check("PartitionBounds", n, map[string][]int64{"lo/0": {-2, -1, -3}, "lo/1": {-2, -1, -3}}, 0)
	if _, _, err := c.be.PartitionOffsets(ctx, "lo", 1); err != nil {
		t.Fatal(err)
	}
	n = check("PartitionOffsets", n, map[string][]int64{"lo/1": {-2, -1}}, 0)
	if _, err := c.be.StableOffset(ctx, "lo", 0); err != nil {
		t.Fatal(err)
	}
	check("StableOffset", n, map[string][]int64{"lo/0": {-1}}, 1)
}

// With no topics there is nothing to count and nothing is asked.
func TestListTopicsOfAnEmptyBrokerAsksForNoOffsets(t *testing.T) {
	c, sn := newSniffedConsole(t)
	got, err := c.be.ListTopics(context.Background())
	if err != nil || len(got) != 0 {
		t.Fatalf("%v %v", got, err)
	}
	if n := len(sn.sent(2)); n != 0 {
		t.Errorf("%d ListOffsets requests for a broker without topics", n)
	}
}

// The topics list: sorted by name, and each count is what is on disk now, from the
// log start, over every partition.
func TestListTopicsSortsAndCountsFromTheLogStart(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for _, n := range []struct {
		name  string
		parts int32
	}{{"zeta", 1}, {"alpha", 2}, {"mid", 1}} {
		if err := c.be.CreateTopic(ctx, n.name, n.parts, nil); err != nil {
			t.Fatal(err)
		}
	}
	bulkProduce(t, cl, "zeta", 0, 10, vals("z"), time.Time{})
	deleteBefore(t, cl, "zeta", 0, 4)
	bulkProduce(t, cl, "alpha", 0, 3, vals("a"), time.Time{})
	bulkProduce(t, cl, "alpha", 1, 2, vals("a"), time.Time{})
	deleteBefore(t, cl, "alpha", 1, 1)
	got, err := c.be.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	counts := map[string]int64{}
	for _, tp := range got {
		names = append(names, tp.Name)
		counts[tp.Name] = tp.Msgs
	}
	if strings.Join(names, ",") != "alpha,mid,zeta" {
		t.Errorf("order %v", names)
	}
	if counts["zeta"] != 6 || counts["alpha"] != 4 || counts["mid"] != 0 {
		t.Errorf("counts %v, want zeta 6, alpha 4 (3+1), mid 0", counts)
	}
}

// A partition's newest timestamp is shown when the broker has one; a record stamped at
// the epoch has none to show.
func TestPartitionBoundsNewestTimestamp(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "nb", 3, nil); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	bulkProduce(t, cl, "nb", 0, 2, vals("a"), at)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "nb", Partition: 1, Value: []byte("epoch"), Timestamp: time.UnixMilli(0)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	bs, err := c.be.PartitionBounds(ctx, "nb", 3)
	if err != nil || len(bs) != 3 {
		t.Fatalf("%v %v", bs, err)
	}
	if want := at.Add(time.Millisecond); !bs[0].Newest.Equal(want) || bs[0].Earliest != 0 || bs[0].Latest != 2 || bs[0].Count() != 2 {
		t.Errorf("partition 0: %+v, want newest %v", bs[0], want)
	}
	if !bs[1].Newest.IsZero() || bs[1].Latest != 1 {
		t.Errorf("a record stamped at the epoch shows a newest time: %+v", bs[1])
	}
	if !bs[2].Newest.IsZero() || bs[2].Latest != 0 {
		t.Errorf("an empty partition shows a newest time: %+v", bs[2])
	}
}

// Reading a window of a partition.
func TestFetchWindowBounds(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "fw", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "fw", 0, 6, vals("m"), time.Time{})
	for _, limit := range []int{0, -5} {
		ms, err := c.be.FetchWindow(ctx, "fw", 0, 2, limit)
		if err != nil || len(ms) != 1 || ms[0].Offset != 2 {
			t.Errorf("limit %d: %d records, %v", limit, len(ms), err)
		}
	}
	// Asking at the end of the log answers at once, with nothing.
	start := time.Now()
	ms, err := c.be.FetchWindow(ctx, "fw", 0, 6, 3)
	if err != nil || len(ms) != 0 || time.Since(start) > 2*time.Second {
		t.Errorf("a window at the log end: %d records, %v, after %v", len(ms), err, time.Since(start))
	}
	ms, err = c.be.FetchWindow(ctx, "fw", 0, 5, 3)
	if err != nil || len(ms) != 1 || ms[0].Offset != 5 {
		t.Errorf("the last record: %d records, %v", len(ms), err)
	}
	ms, err = c.be.FetchWindow(ctx, "fw", 0, 1, 3)
	if err != nil || len(ms) != 3 || ms[0].Offset != 1 || ms[2].Offset != 3 {
		t.Errorf("a window of three: %v %v", ms, err)
	}
	// A window that ends at the log end returns as soon as it has its last record,
	// without waiting out the wait for more.
	best := time.Hour
	for i := 0; i < 3; i++ {
		start := time.Now()
		if ms, err := c.be.FetchWindow(ctx, "fw", 0, 5, 1); err != nil || len(ms) != 1 {
			t.Fatalf("%v %v", ms, err)
		}
		if d := time.Since(start); d < best {
			best = d
		}
	}
	if best > 350*time.Millisecond {
		t.Errorf("a window ending at the log end took %v; it waits for records after its last", best)
	}
}

// A window that ends in a transaction marker waits briefly for more, then stops, and
// the committed view leaves the aborted records out.
func TestFetchWindowWaitsForRecordsBeforeAMarkerOnly(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "mk", 1, nil); err != nil {
		t.Fatal(err)
	}
	p := txnProducer(t, addr, "marker")
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	txnWrite(t, ctx, p, "mk", "kept")
	if err := p.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	txnWrite(t, ctx, p, "mk", "aborted")
	if err := p.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	// Offsets: 0 kept, 1 commit marker, 2 aborted, 3 abort marker.
	start := time.Now()
	ms, err := c.be.FetchWindow(ctx, "mk", 0, 0, 4)
	d := time.Since(start)
	if err != nil || len(ms) != 2 || string(ms[0].Value) != "kept" || string(ms[1].Value) != "aborted" {
		t.Fatalf("uncommitted view: %v %v", ms, err)
	}
	if d < txnTail-20*time.Millisecond || d > 3*time.Second {
		t.Errorf("a window ending in a marker took %v, want about %v", d, txnTail)
	}
	ms, err = c.be.FetchWindowCommitted(ctx, "mk", 0, 0, 4)
	if err != nil || len(ms) != 1 || string(ms[0].Value) != "kept" {
		t.Errorf("committed view: %v %v", ms, err)
	}
}

// Records that are there but never arrive are a failure, not an empty topic.
func TestFetchWindowThatNeverAnswersIsAnError(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a 5 s timeout")
	}
	c, sn := newSniffedConsole(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "gone", 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.be.Produce(ctx, "gone", 0, nil, []byte("v"), nil, "1"); err != nil {
		t.Fatal(err)
	}
	sn.blackhole(1) // Fetch
	ms, err := c.be.FetchWindow(ctx, "gone", 0, 0, 1)
	if err == nil || len(ms) != 0 || !strings.Contains(err.Error(), "fetching gone partition 0 from offset 0") {
		t.Errorf("%v %v", ms, err)
	}
	// A caller that gave up is not told the broker was slow.
	cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := c.be.FetchWindow(cctx, "gone", 0, 0, 1); err != nil && strings.Contains(err.Error(), "fetching gone partition") {
		t.Errorf("a cancelled caller was told %v", err)
	}
}

func TestProduceWithoutAcksReportsNoOffset(t *testing.T) {
	c := newTestConsole(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "ack", 3, nil); err != nil {
		t.Fatal(err)
	}
	for _, acks := range []string{"1", "all"} {
		res, err := c.be.Produce(ctx, "ack", 2, nil, []byte("v"), nil, acks)
		if err != nil || res.Partition != 2 || res.Offset < 0 || !res.Confirmed {
			t.Errorf("acks %s: %+v %v", acks, res, err)
		}
	}
	res, err := c.be.Produce(ctx, "ack", 1, nil, []byte("v"), nil, "0")
	if err != nil || res != (ProduceResult{Partition: 1, Offset: -1, Confirmed: false}) {
		t.Errorf("acks 0: %+v %v", res, err)
	}
}

func TestPickPartitionRoundRobinsWithoutAKey(t *testing.T) {
	c := newTestConsole(t)
	var got []int32
	for i := 0; i < 7; i++ {
		got = append(got, c.be.PickPartition("rr", nil, 3))
	}
	if fmt.Sprint(got) != "[0 1 2 0 1 2 0]" {
		t.Errorf("round robin: %v", got)
	}
	if p := c.be.PickPartition("rr", nil, 1); p != 0 {
		t.Errorf("one partition: %d", p)
	}
	if a, b := c.be.PickPartition("rr", []byte("k"), 5), c.be.PickPartition("rr", []byte("k"), 5); a != b {
		t.Errorf("the same key went to %d and %d", a, b)
	}
}

// An unsupported SASL mechanism is refused where a client is built, not later.
func TestUnsupportedSASLMechanismIsRefused(t *testing.T) {
	if _, err := New(Options{Addr: "127.0.0.1:1", SASLUser: "u", SASLMechanism: "KERBEROS"}); err == nil || !strings.Contains(err.Error(), "unsupported SASL mechanism") {
		t.Errorf("New: %v", err)
	}
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "sa", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "sa", 0, 2, vals("m"), time.Time{})
	c.be.opts.SASLUser, c.be.opts.SASLMechanism = "u", "KERBEROS"
	if _, err := c.be.FetchWindow(ctx, "sa", 0, 0, 2); err == nil || !strings.Contains(err.Error(), "unsupported SASL mechanism") {
		t.Errorf("FetchWindow: %v", err)
	}
	if _, err := c.be.newReader(nil); err == nil || !strings.Contains(err.Error(), "unsupported SASL mechanism") {
		t.Errorf("newReader: %v", err)
	}
	c.be.opts.SASLUser = "" // so the cleanup does not trip on it.
}
