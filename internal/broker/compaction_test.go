package broker_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// createCompacted makes a topic with cleanup.policy and a small segment, so a handful of
// records roll the log and give the cleaner something sealed to work on.
func createCompacted(t *testing.T, ctx context.Context, cl *kgo.Client, name, policy string, extra map[string]string) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = name, 1, 1
	cfg := map[string]string{"cleanup.policy": policy, "segment.bytes": "1024"}
	for k, v := range extra {
		cfg[k] = v
	}
	for k, v := range cfg {
		v := v
		c := kmsg.NewCreateTopicsRequestTopicConfig()
		c.Name, c.Value = k, &v
		ct.Configs = append(ct.Configs, c)
	}
	req.Topics = append(req.Topics, ct)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create %s: %v %+v", name, err, resp)
	}
}

type kvRec struct {
	off     int64
	key, vl string
}

// readAllRecords reads partition 0 from the start to its end.
func readAllRecords(t *testing.T, addr, topic string, committed bool) []kvRec {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	defer admin.Close()
	end := listOffset(t, ctx, admin, topic, 0, -1)
	cl := reader(t, addr, topic, committed)
	var out []kvRec
	idle := 0
	for ctx.Err() == nil && idle < 3 {
		pctx, c := context.WithTimeout(ctx, 600*time.Millisecond)
		fs := cl.PollFetches(pctx)
		c()
		n := 0
		fs.EachRecord(func(r *kgo.Record) {
			n++
			k, v := "<nil>", "<nil>"
			if r.Key != nil {
				k = string(r.Key)
			}
			if r.Value != nil {
				v = string(r.Value)
			}
			out = append(out, kvRec{r.Offset, k, v})
		})
		if n == 0 {
			idle++
		} else {
			idle = 0
		}
		if len(out) > 0 && out[len(out)-1].off >= end-1 {
			break
		}
	}
	return out
}

func putKV(t *testing.T, ctx context.Context, cl *kgo.Client, topic, key, value string) {
	t.Helper()
	r := &kgo.Record{Topic: topic, Partition: 0, Key: []byte(key)}
	if value != "<nil>" {
		r.Value = []byte(value)
	}
	if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatalf("produce %s=%s: %v", key, value, err)
	}
}

func manualProducer(t *testing.T, addr string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.DialTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func perKey(rows []kvRec) map[string][]string {
	out := map[string][]string{}
	for _, r := range rows {
		out[r.key] = append(out[r.key], r.vl)
	}
	return out
}

// A compacted topic ends up with the latest value of each key, the older ones gone, at
// the offsets they had, while a consumer reads on as if nothing happened.
func TestACompactedTopicKeepsTheLatestValueOfEachKey(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	createCompacted(t, ctx, admin, "cfg", "compact", nil)
	p := manualProducer(t, addr)
	pad := strings.Repeat("x", 120)
	for round := 0; round < 4; round++ {
		for _, k := range []string{"a", "b", "c"} {
			putKV(t, ctx, p, "cfg", k, fmt.Sprintf("%s-r%d-%s", k, round, pad))
		}
	}
	for i := 0; i < 12; i++ { // push the keyed records out of the active segment.
		putKV(t, ctx, p, "cfg", fmt.Sprintf("filler-%d", i), pad)
	}
	before := readAllRecords(t, addr, "cfg", false)
	if got := len(perKey(before)["a"]); got != 4 {
		t.Fatalf("setup: key a has %d records, want 4", got)
	}
	end := listOffset(t, ctx, admin, "cfg", 0, -1)

	eventually(t, 30*time.Second, "the cleaner to remove the older values", func() bool {
		m := perKey(readAllRecords(t, addr, "cfg", false))
		return len(m["a"]) == 1 && len(m["b"]) == 1 && len(m["c"]) == 1
	})
	after := readAllRecords(t, addr, "cfg", false)
	m := perKey(after)
	for _, k := range []string{"a", "b", "c"} {
		if len(m[k]) != 1 || !strings.HasPrefix(m[k][0], k+"-r3-") {
			t.Errorf("key %s = %v, want only its latest value", k, m[k])
		}
	}
	for i := 0; i < 12; i++ {
		if len(m[fmt.Sprintf("filler-%d", i)]) != 1 {
			t.Errorf("filler-%d was lost", i)
		}
	}
	byOff := map[int64]kvRec{}
	for _, r := range before {
		byOff[r.off] = r
	}
	for _, r := range after {
		if o := byOff[r.off]; o.key != r.key || o.vl != r.vl {
			t.Errorf("the record at offset %d changed", r.off)
		}
	}
	if got := listOffset(t, ctx, admin, "cfg", 0, -1); got != end {
		t.Errorf("log end moved from %d to %d", end, got)
	}
	if start := listOffset(t, ctx, admin, "cfg", 0, -2); start != 0 {
		t.Errorf("log start moved to %d", start)
	}
}

// A consumer that had read part of the topic and committed keeps its place through a
// clean, and carries on with what came after.
func TestAConsumerKeepsItsPlaceThroughCompaction(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	createCompacted(t, ctx, admin, "place", "compact", nil)
	p := manualProducer(t, addr)
	pad := strings.Repeat("x", 120)
	for i := 0; i < 30; i++ {
		putKV(t, ctx, p, "place", fmt.Sprintf("k%d", i%5), fmt.Sprintf("v%d-%s", i, pad))
	}
	got := groupConsume(t, ctx, addr, "g-place", "place", 12, true)
	if len(got) < 12 {
		t.Fatalf("first read: %d", len(got))
	}
	committed := committedOffset(t, ctx, admin, "g-place", "place")
	eventually(t, 30*time.Second, "compaction", func() bool {
		return len(perKey(readAllRecords(t, addr, "place", false))["k0"]) <= 2
	})
	putKV(t, ctx, p, "place", "fresh", "after-the-clean")
	rest := groupConsume(t, ctx, addr, "g-place", "place", 1, false)
	if len(rest) == 0 {
		t.Fatalf("the group, committed at %d, read nothing after the clean", committed)
	}
	last := listOffset(t, ctx, admin, "place", 0, -1)
	if committedOffset(t, ctx, admin, "g-place", "place") != committed || last <= committed {
		t.Errorf("committed offset or log end is wrong: committed %d, end %d", committed, last)
	}
}

// Kafka refuses a record with no key on a compacted topic, and so does this.
func TestACompactedTopicRefusesARecordWithNoKey(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	createCompacted(t, ctx, admin, "keyed", "compact", nil)
	mustCreateTopic(t, ctx, admin, "plain", 1)
	p := manualProducer(t, addr)
	err := p.ProduceSync(ctx, &kgo.Record{Topic: "keyed", Partition: 0, Value: []byte("no key")}).FirstErr()
	if err == nil || !strings.Contains(err.Error(), kerr.InvalidRecord.Message) && !strings.Contains(err.Error(), "INVALID_RECORD") {
		t.Errorf("a keyless record on a compacted topic: %v, want INVALID_RECORD", err)
	}
	if err := p.ProduceSync(ctx, &kgo.Record{Topic: "plain", Partition: 0, Value: []byte("fine")}).FirstErr(); err != nil {
		t.Errorf("a keyless record on an ordinary topic: %v", err)
	}
	putKV(t, ctx, p, "keyed", "k", "ok")
}

// A tombstone hides its key, is kept for delete.retention.ms after the clean, and then
// goes; an unrelated key is untouched.
func TestATombstoneIsKeptThenRemoved(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	createCompacted(t, ctx, admin, "tomb", "compact", map[string]string{"delete.retention.ms": "2000"})
	p := manualProducer(t, addr)
	pad := strings.Repeat("x", 120)
	putKV(t, ctx, p, "tomb", "gone", "v1-"+pad)
	putKV(t, ctx, p, "tomb", "stays", "s1-"+pad)
	putKV(t, ctx, p, "tomb", "gone", "<nil>")
	for i := 0; i < 14; i++ {
		putKV(t, ctx, p, "tomb", fmt.Sprintf("filler-%d", i), pad)
	}
	eventually(t, 30*time.Second, "the value under the tombstone to go", func() bool {
		m := perKey(readAllRecords(t, addr, "tomb", false))
		return len(m["gone"]) == 1 && m["gone"][0] == "<nil>"
	})
	// More data seals the tombstone's segment again; once it is older than the retention
	// the tombstone itself is removed.
	deadline := time.Now().Add(40 * time.Second)
	for i := 0; ; i++ {
		if _, there := perKey(readAllRecords(t, addr, "tomb", false))["gone"]; !there {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the tombstone was never removed after delete.retention.ms")
		}
		putKV(t, ctx, p, "tomb", fmt.Sprintf("more-%d", i), pad)
		time.Sleep(700 * time.Millisecond)
	}
	if got := perKey(readAllRecords(t, addr, "tomb", false))["stays"]; len(got) != 1 {
		t.Errorf("an unrelated key has %v", got)
	}
}

// Records of an aborted transaction are removed by the cleaner and those of a committed
// one are not, and a read-committed consumer sees the same before and after.
func TestCompactionRemovesAbortedTransactionRecords(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	createCompacted(t, ctx, admin, "txc", "compact", nil)
	tp := txProducer(t, addr, "compactor")
	pad := strings.Repeat("x", 120)
	for _, step := range []struct {
		key, value string
		ok         bool
	}{{"a", "aborted-" + pad, false}, {"a", "committed-" + pad, true}, {"b", "b1-" + pad, true}} {
		if err := tp.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		r := &kgo.Record{Topic: "txc", Partition: 0, Key: []byte(step.key), Value: []byte(step.value)}
		if err := tp.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
		commit(t, ctx, tp, step.ok)
	}
	p := manualProducer(t, addr)
	for i := 0; i < 14; i++ {
		putKV(t, ctx, p, "txc", fmt.Sprintf("filler-%d", i), pad)
	}
	seen := func() map[string][]string { return perKey(readAllRecords(t, addr, "txc", true)) }
	want := seen()
	eventually(t, 30*time.Second, "the aborted record to be removed", func() bool {
		for _, v := range perKey(readAllRecords(t, addr, "txc", false))["a"] {
			if strings.HasPrefix(v, "aborted-") {
				return false
			}
		}
		return true
	})
	got := seen()
	if fmt.Sprint(got["a"]) != fmt.Sprint(want["a"]) || fmt.Sprint(got["b"]) != fmt.Sprint(want["b"]) {
		t.Errorf("a read-committed consumer sees something different after cleaning:\n before %v\n after  %v", want, got)
	}
	if len(got["a"]) != 1 || !strings.HasPrefix(got["a"][0], "committed-") {
		t.Errorf("key a = %v, want only the committed value", got["a"])
	}
}

// compact,delete does both, compaction first: the sealed log is deduped while it is young,
// and later retention removes it by age.
func TestCompactAndDeleteRunBoth(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	createCompacted(t, ctx, admin, "both", "compact,delete", map[string]string{"retention.ms": "8000"})
	p := manualProducer(t, addr)
	pad := strings.Repeat("x", 120)
	const total = 60
	for i := 0; i < total; i++ {
		putKV(t, ctx, p, "both", "same", fmt.Sprintf("v%d-%s", i, pad))
	}
	// First, compaction: far fewer than 60 records of the one key remain, and nothing has
	// been deleted by age yet.
	eventually(t, 6*time.Second, "compaction to dedupe the sealed log", func() bool {
		return len(perKey(readAllRecords(t, addr, "both", false))["same"]) <= 15
	})
	if start := listOffset(t, ctx, admin, "both", 0, -2); start != 0 {
		t.Fatalf("the log start is %d already: retention ran before there was anything to compact", start)
	}
	// Then retention: once the segments are older than retention.ms they are deleted.
	eventually(t, 30*time.Second, "retention to move the log start", func() bool {
		return listOffset(t, ctx, admin, "both", 0, -2) > 0
	})
}

// What the cleaner did survives a restart: the same records, the same offsets.
func TestACleanedTopicSurvivesARestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	addr, stop := bootAt(t, "127.0.0.1:0", dir, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	createCompacted(t, ctx, admin, "durable", "compact", nil)
	p := manualProducer(t, addr)
	pad := strings.Repeat("x", 120)
	for i := 0; i < 24; i++ {
		putKV(t, ctx, p, "durable", fmt.Sprintf("k%d", i%3), fmt.Sprintf("v%d-%s", i, pad))
	}
	for i := 0; i < 12; i++ {
		putKV(t, ctx, p, "durable", fmt.Sprintf("filler-%d", i), pad)
	}
	eventually(t, 30*time.Second, "compaction", func() bool {
		return len(perKey(readAllRecords(t, addr, "durable", false))["k0"]) == 1
	})
	want := readAllRecords(t, addr, "durable", false)
	end := listOffset(t, ctx, admin, "durable", 0, -1)
	stop()
	stop = runBrokerAt(t, addr, dir, 4)
	defer stop()
	got := readAllRecords(t, addr, "durable", false)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("after a restart:\n got  %v\n want %v", got, want)
	}
	admin = newClient(t, addr)
	if e := listOffset(t, ctx, admin, "durable", 0, -1); e != end {
		t.Errorf("log end %d after the restart, want %d", e, end)
	}
}
