package broker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
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

// skipShort skips integration tests under `go test -short`, leaving a precise
// unit-only run. Every integration test boots a broker through startBroker,
// runBroker, or buildBinary, so gating those three covers them all.
func skipShort(t *testing.T) {
	if testing.Short() {
		t.Helper()
		t.Skip("integration test; omit -short (or run `go tool task test:integration`)")
	}
}

// startBroker boots a broker on a free loopback port and returns its address.
// It shuts down when the test ends.
func startBroker(t *testing.T, version int) string {
	t.Helper()
	skipShort(t)

	port := freePort(t)
	dir := t.TempDir()

	cfg := config.Default()
	cfg.Version = version
	cfg.ListenAddr = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	metaStore, err := meta.Open(cfg.MetaDB)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { metaStore.Close() })

	logs, err := log.OpenManager(cfg.DataDir, log.ManagerOptions{
		SegmentBytes:       cfg.SegmentBytes,
		IndexIntervalBytes: cfg.IndexIntervalBytes,
	})
	if err != nil {
		t.Fatalf("log.OpenManager: %v", err)
	}
	t.Cleanup(func() { logs.Close() })

	b, err := broker.New(cfg, logger, logs, metaStore)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	router := protocol.NewRouter(logger)
	b.Register(router)

	srv := protocol.NewServer(cfg.ListenAddr, router, logger)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if _, err := srv.Bind(ctx); err != nil {
		t.Fatalf("bind: %v", err)
	}
	go srv.Serve(ctx)

	return cfg.ListenAddr
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

// TestPhase1_MetadataViaFranzGo is the Phase 1 acceptance gate: an unmodified
// franz-go client completes the ApiVersions handshake and a Metadata request
// against a fresh broker, and sees the single broker advertised.
func TestPhase1_MetadataViaFranzGo(t *testing.T) {
	addr := startBroker(t, 4)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DialTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A metadata request implicitly drives the ApiVersions handshake first; if
	// that failed, this would error.
	resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("metadata request: %v", err)
	}

	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}
	got := resp.Brokers[0]
	if got.Host != "localhost" {
		t.Errorf("broker host = %q, want localhost", got.Host)
	}
	_, portStr, _ := net.SplitHostPort(addr)
	if fmt.Sprintf("%d", got.Port) != portStr {
		t.Errorf("broker port = %d, want %s", got.Port, portStr)
	}
	if resp.ControllerID != got.NodeID {
		t.Errorf("controller id = %d, want %d (self)", resp.ControllerID, got.NodeID)
	}
	if len(resp.Topics) != 0 {
		t.Errorf("expected 0 topics on a fresh broker, got %d", len(resp.Topics))
	}
}

// TestPhase1_ClassicProfileBoots checks a lower version profile also handshakes.
func TestPhase1_ClassicProfileBoots(t *testing.T) {
	addr := startBroker(t, 1)

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("metadata request (v1 profile): %v", err)
	}
	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}
}
