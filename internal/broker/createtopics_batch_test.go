package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// listCounter is a metadata store that counts how often every topic is listed.
type listCounter struct {
	meta.Store
	lists atomic.Int64
}

func (c *listCounter) ListTopics() ([]meta.Topic, error) {
	c.lists.Add(1)
	return c.Store.ListTopics()
}

// batchBroker is a broker over a store that counts, with nothing listening.
func batchBroker(tb testing.TB) (*Broker, *listCounter) {
	tb.Helper()
	dir := tb.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { store.Close() })
	logs, err := log.OpenManager(filepath.Join(dir, "data"), log.ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { logs.Close() })
	cfg := config.Default()
	cfg.FsyncInterval = 0
	counted := &listCounter{Store: store}
	b, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, counted)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(b.Stop)
	return b, counted
}

// createBody is a CreateTopics v7 request for names, one partition each, as a
// handler is given it (v7 is flexible: the header's tagged fields come first).
func createBody(validateOnly bool, names ...string) []byte {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.SetVersion(7)
	req.TimeoutMillis = 5000
	req.ValidateOnly = validateOnly
	for _, n := range names {
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic, t.NumPartitions, t.ReplicationFactor = n, 1, 1
		req.Topics = append(req.Topics, t)
	}
	return req.AppendTo([]byte{0})
}

// createTopicsCall sends names through the handler and returns each one's answer.
func createTopicsCall(tb testing.TB, b *Broker, validateOnly bool, names ...string) map[string]kmsg.CreateTopicsResponseTopic {
	tb.Helper()
	out, err := typed(b.handleCreateTopics)(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.CreateTopics), APIVersion: 7, CorrelationID: 1},
		Body:   createBody(validateOnly, names...),
	})
	if err != nil {
		tb.Fatal(err)
	}
	resp := kmsg.NewPtrCreateTopicsResponse()
	resp.SetVersion(7)
	if err := resp.ReadFrom(out[5:]); err != nil { // the correlation id and the header's tagged fields.
		tb.Fatalf("decode: %v", err)
	}
	got := map[string]kmsg.CreateTopicsResponseTopic{}
	for _, t := range resp.Topics {
		got[t.Topic] = t
	}
	return got
}

// A request for n topics used to list every topic and compare the new name with
// each, once per topic, and the store compared it with each again when it created
// it: n squared for a batch. Both now ask an index, and the list is never read.
func TestCreateTopicsNeverListsEveryTopic(t *testing.T) {
	b, counted := batchBroker(t)
	var names []string
	for i := range 40 {
		names = append(names, fmt.Sprintf("batch-%d", i))
	}
	before := counted.lists.Load()
	for name, rt := range createTopicsCall(t, b, false, names...) {
		if rt.ErrorCode != 0 {
			t.Fatalf("%s: error %d", name, rt.ErrorCode)
		}
	}
	createTopicsCall(t, b, true, "one-more", "and.another", "and_another")
	if n := counted.lists.Load() - before; n != 0 {
		t.Errorf("CreateTopics listed every topic %d times for 43 names; it should ask the store's index and never list", n)
	}
}

// Which names are held against which is Apache Kafka's, recorded against
// apache/kafka:4.3.1 (conformance/recordings/createtopics-collisions.json): a name
// is refused for a topic that existed before the request, and not for one the same
// request creates. Names that differ only in letter case are this broker's own rule
// (one directory on a case-insensitive file system) and are refused either way.
func TestCreateTopicsCollisionsWithinARequestAndAcrossRequests(t *testing.T) {
	b, _ := batchBroker(t)
	code := func(got map[string]kmsg.CreateTopicsResponseTopic, name string) int16 {
		t.Helper()
		rt, ok := got[name]
		if !ok {
			t.Fatalf("no answer for %q in %v", name, got)
		}
		return rt.ErrorCode
	}
	const invalidTopic, exists = 17, 36

	got := createTopicsCall(t, b, false, "pair.one", "pair_one")
	if code(got, "pair.one") != 0 || code(got, "pair_one") != 0 {
		t.Errorf("pair.one and pair_one in one request: %d and %d, want both created as Kafka creates them", code(got, "pair.one"), code(got, "pair_one"))
	}
	got = createTopicsCall(t, b, false, "tri.a.b", "tri_a.b", "tri.a_b")
	for name, rt := range got {
		if rt.ErrorCode != 0 {
			t.Errorf("three spellings in one request: %s is error %d", name, rt.ErrorCode)
		}
	}
	// A later request is held against what that one created.
	got = createTopicsCall(t, b, false, "tri_a_b", "fine")
	if code(got, "tri_a_b") != invalidTopic || code(got, "fine") != 0 {
		t.Errorf("a fourth spelling in a later request: %d (want %d), and the name beside it %d (want 0)", code(got, "tri_a_b"), invalidTopic, code(got, "fine"))
	}
	if msg := got["tri_a_b"].ErrorMessage; msg == nil || *msg == "" {
		t.Error("the refusal names no topic it collides with")
	}
	// validate_only: nothing is created, so the two are not held against each other,
	// and each is still held against what exists.
	got = createTopicsCall(t, b, true, "dry.one", "dry_one", "pair.one", "fine_", "pair_one_")
	if code(got, "dry.one") != 0 || code(got, "dry_one") != 0 {
		t.Errorf("validate_only of dry.one and dry_one: %d and %d, want 0 and 0", code(got, "dry.one"), code(got, "dry_one"))
	}
	if code(got, "pair.one") != exists {
		t.Errorf("validate_only of a name that exists: %d, want %d", code(got, "pair.one"), exists)
	}
	if _, ok, _ := b.meta.Topic("dry.one"); ok {
		t.Error("validate_only created a topic")
	}
	got = createTopicsCall(t, b, true, "pair_one", "pair__one", "tri_a_b")
	if code(got, "tri_a_b") != invalidTopic || code(got, "pair__one") != 0 {
		t.Errorf("validate_only against existing topics: tri_a_b %d (want %d), pair__one %d (want 0)", code(got, "tri_a_b"), invalidTopic, code(got, "pair__one"))
	}

	// Letter case: refused in the same request too, whatever else differs.
	got = createTopicsCall(t, b, false, "Orders", "orders", "ORDERS")
	if code(got, "Orders") != 0 || code(got, "orders") != invalidTopic || code(got, "ORDERS") != invalidTopic {
		t.Errorf("Orders, orders, ORDERS in one request: %d, %d, %d; want 0, %d, %d", code(got, "Orders"), code(got, "orders"), code(got, "ORDERS"), invalidTopic, invalidTopic)
	}
	// Both differences at once are neither rule's business: Kafka compares the
	// spelling with '.' as '_' exactly, and the directories differ.
	got = createTopicsCall(t, b, false, "Mixed.Up", "mixed_up")
	if code(got, "Mixed.Up") != 0 || code(got, "mixed_up") != 0 {
		t.Errorf("Mixed.Up and mixed_up: %d and %d, want both created", code(got, "Mixed.Up"), code(got, "mixed_up"))
	}

	// Deleting one of a pair leaves the other in the way of its twin.
	if err := b.meta.DeleteTopic("pair.one"); err != nil {
		t.Fatal(err)
	}
	got = createTopicsCall(t, b, false, "pair.one")
	if code(got, "pair.one") != invalidTopic {
		t.Errorf("pair.one again, with pair_one still there: %d, want %d", code(got, "pair.one"), invalidTopic)
	}
	if err := b.meta.DeleteTopic("pair_one"); err != nil {
		t.Fatal(err)
	}
	got = createTopicsCall(t, b, false, "pair.one")
	if code(got, "pair.one") != 0 {
		t.Errorf("pair.one with both gone: %d, want 0", code(got, "pair.one"))
	}
}

// What one more name costs CreateTopics does not depend on how many topics there
// are. Nothing is created (validate_only), so what is measured is the checks, not
// the metadata database's fsync. Before the index each name was compared with every
// topic, twice: eight times the topics cost each name about eight times as much.
func BenchmarkCreateTopicsValidateOnly(b *testing.B) {
	for _, existing := range []int{100, 800} {
		// Outside b.Run, which calls its function again for every b.N it tries: each
		// existing topic is a transaction of the metadata database, with its fsync.
		br, _ := batchBroker(b)
		for i := range existing {
			if _, err := br.meta.CreateTopic(fmt.Sprintf("existing.topic-%d", i), 1, nil); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(fmt.Sprintf("existing=%d", existing), func(b *testing.B) {
			const batch = 100
			names := make([]string, batch)
			for i := range names {
				names[i] = fmt.Sprintf("wanted_topic.%d", i)
			}
			req := &protocol.Request{
				Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.CreateTopics), APIVersion: 7, CorrelationID: 1},
				Body:   createBody(true, names...),
			}
			h := typed(br.handleCreateTopics)
			b.ResetTimer()
			for range b.N {
				if _, err := h(context.Background(), req); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/batch, "ns/topic")
		})
	}
}
