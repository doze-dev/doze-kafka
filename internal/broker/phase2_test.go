package broker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/broker"
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

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

// runBroker boots a broker from a full config and returns a stop func. It wires the
// log manager exactly as cmd/doze-kafka does — including the fsync cadence and
// retention policy — so durability and retention behaviour under test matches
// production.
func runBroker(t *testing.T, cfg config.Config) func() {
	t.Helper()
	skipShort(t)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metaStore, err := meta.Open(cfg.MetaDB)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	logs, err := log.OpenManager(cfg.DataDir, log.ManagerOptions{
		SegmentBytes:       cfg.SegmentBytes,
		IndexIntervalBytes: cfg.IndexIntervalBytes,
		FsyncInterval:      cfg.FsyncInterval,
		Retention:          cfg.Retention(),
		Logger:             logger,
	})
	if err != nil {
		t.Fatalf("log.OpenManager: %v", err)
	}

	b, err := broker.New(cfg, logger, logs, metaStore)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	router := protocol.NewRouter(logger)
	b.Register(router)

	srv := protocol.NewServer(cfg.ListenAddr, router, logger)
	if cfg.TLSEnabled() {
		tlsCfg, err := cfg.TLSConfig()
		if err != nil {
			t.Fatalf("tls config: %v", err)
		}
		srv.UseTLS(tlsCfg)
	}
	if cfg.AuthEnabled() {
		srv.RequireAuth()
	}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := srv.Bind(ctx); err != nil {
		t.Fatalf("bind: %v", err)
	}
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()

	return func() {
		cancel()
		<-done
		logs.Close()
		metaStore.Close()
	}
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

// TestPhase2_CreateTopicPersistsAcrossRestart is the Phase 2 acceptance gate at
// the protocol level: create a topic via franz-go, restart the broker over the
// same data dir, and confirm the topic + its partitions survive.
func TestPhase2_CreateTopicPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	stop := runBrokerAt(t, addr, dir, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newClient(t, addr)

	// Create "orders" with 3 partitions.
	creq := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = "orders"
	ct.NumPartitions = 3
	ct.ReplicationFactor = 1
	creq.Topics = append(creq.Topics, ct)

	cresp, err := creq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("create topics: %v", err)
	}
	if len(cresp.Topics) != 1 || cresp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create response: %+v", cresp.Topics)
	}

	// Metadata now lists it with 3 partitions.
	mresp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topic, ok := findTopic(mresp, "orders")
	if !ok {
		t.Fatalf("orders not in metadata: %+v", mresp.Topics)
	}
	if len(topic.Partitions) != 3 {
		t.Fatalf("orders has %d partitions, want 3", len(topic.Partitions))
	}
	cl.Close()
	stop()

	// Restart over the same dir; the topic must survive.
	stop2 := runBrokerAt(t, addr, dir, 4)
	defer stop2()

	cl2 := newClient(t, addr)
	defer cl2.Close()

	mresp2, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl2)
	if err != nil {
		t.Fatalf("metadata after restart: %v", err)
	}
	topic2, ok := findTopic(mresp2, "orders")
	if !ok {
		t.Fatalf("orders missing after restart: %+v", mresp2.Topics)
	}
	if len(topic2.Partitions) != 3 {
		t.Fatalf("orders has %d partitions after restart, want 3", len(topic2.Partitions))
	}

	// Partition directories are on disk.
	if _, err := os.Stat(filepath.Join(dir, "data", "orders-0")); err != nil {
		t.Fatalf("partition dir orders-0 missing: %v", err)
	}
}
