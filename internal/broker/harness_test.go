package broker_test

// The shared test harness. Every integration test boots its broker through
// boot (directly, or via startBroker / runBroker / runBrokerAt), which goes
// through server.NewFromConfig — the same wiring the CLI and the embed API use,
// including fsync cadence and retention — so what the tests exercise is what
// ships, rather than a hand-assembled copy of it.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/server"
)

// boot starts a broker from a full config and returns the address it bound and
// a stop func. cfg.ListenAddr may be ":0"-style (the broker advertises the bound
// port) or fixed, for tests that restart on the same address. Serve flushes and
// closes storage when stop returns.
func boot(t *testing.T, cfg config.Config) (addr string, stop func()) {
	t.Helper()
	skipShort(t)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}
	srv, err := server.NewFromConfig(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("server.NewFromConfig: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	bound, err := srv.Bind(ctx)
	if err != nil {
		cancel()
		t.Fatalf("bind: %v", err)
	}
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	return bound.String(), func() {
		cancel()
		<-done
	}
}

// startBroker boots a broker on a free loopback port over a fresh data dir and
// returns its address. It shuts down when the test ends.
func startBroker(t *testing.T, version int) string {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Version = version
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	addr, stop := boot(t, cfg)
	t.Cleanup(stop)
	return addr
}

// runBrokerAt boots a broker on a fixed address over the given data dir and
// returns a stop func. Unlike startBroker it takes an explicit dir so a test can
// stop and restart against the same on-disk state.
func runBrokerAt(t *testing.T, addr, dir string, version int) func() {
	t.Helper()
	cfg := config.Default()
	cfg.Version = version
	cfg.ListenAddr = addr
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	return runBroker(t, cfg)
}

// runBroker boots a broker from a full config and returns a stop func.
func runBroker(t *testing.T, cfg config.Config) func() {
	t.Helper()
	_, stop := boot(t, cfg)
	return stop
}

// skipShort skips integration tests under `go test -short`, leaving a precise
// unit-only run. Every integration test boots a broker through startBroker,
// runBroker, or buildBinary, so gating those three covers them all.
func skipShort(t *testing.T) {
	if testing.Short() {
		t.Helper()
		t.Skip("integration test; omit -short (or run `go tool task test:integration`)")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func newClient(t *testing.T, addr string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	return cl
}

func findTopic(resp *kmsg.MetadataResponse, name string) (kmsg.MetadataResponseTopic, bool) {
	for _, t := range resp.Topics {
		if t.Topic != nil && *t.Topic == name {
			return t, true
		}
	}
	return kmsg.MetadataResponseTopic{}, false
}

func mustCreateTopic(t *testing.T, ctx context.Context, cl *kgo.Client, name string, partitions int32) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = name
	ct.NumPartitions = partitions
	ct.ReplicationFactor = 1
	req.Topics = append(req.Topics, ct)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic %s: %+v", name, resp.Topics)
	}
}

// consumeFromStart reads exactly n records from a topic's start (no consumer
// group — direct partition consumption) and returns their values in order.
func consumeFromStart(t *testing.T, ctx context.Context, addr, topic string, n int) []string {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	var got []string
	for len(got) < n {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
					t.Fatalf("timed out consuming (%d/%d): %v", len(got), n, e.Err)
				}
				t.Fatalf("fetch error: %v", e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	return got
}

// listOffset queries a partition's earliest (-2) or latest (-1) offset via
// ListOffsets, driving the same path a real consumer uses to resolve AtStart/AtEnd.
func listOffset(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, partition int32, timestamp int64) int64 {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = partition
	rp.Timestamp = timestamp
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("ListOffsets: %v", err)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("ListOffsets: malformed response %+v", resp.Topics)
	}
	pr := resp.Topics[0].Partitions[0]
	if pr.ErrorCode != 0 {
		t.Fatalf("ListOffsets: error code %d", pr.ErrorCode)
	}
	return pr.Offset
}

// waitStableEarliest polls the earliest offset until it stops advancing (two equal
// readings ~150ms apart) or the deadline passes, then returns it. Once production
// stops, background retention reaches a fixed point quickly, so this pins down a
// stable log-start offset without racing the reaper.
func waitStableEarliest(t *testing.T, ctx context.Context, cl *kgo.Client, topic string) int64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	prev := int64(-1)
	for time.Now().Before(deadline) {
		cur := listOffset(t, ctx, cl, topic, 0, -2)
		if cur == prev && cur > 0 {
			return cur
		}
		prev = cur
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("earliest offset never stabilised above 0 (retention did not trim)")
	return 0
}
