package server_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/doze-dev/doze-kafka/server"
)

// TestEmbedProduceConsume boots an embedded broker via the public server API
// and round-trips a record with a real franz-go client — proving the embed
// surface wires the same broker the CLI does.
func TestEmbedProduceConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker integration test in -short mode")
	}
	srv, err := server.New(server.Options{
		Version: 4,
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	addr, err := srv.Bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr.String()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := cl.ProduceSync(pctx, &kgo.Record{Topic: "embed", Value: []byte("hello")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	cl.AddConsumeTopics("embed")
	fetches := cl.PollFetches(pctx)
	if err := fetches.Err(); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var got string
	fetches.EachRecord(func(r *kgo.Record) { got = string(r.Value) })
	if got != "hello" {
		t.Fatalf("record value = %q", got)
	}
}

// TestServeListener proves ServeListener accepts a caller-provided listener —
// the doze module path (a unix socket here).
func TestServeListener(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker integration test in -short mode")
	}
	sock := t.TempDir() + "/kafka.sock"
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.ServeListener(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// A unix-dialing franz client speaks the protocol over the socket.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("kafka.local:9092"),
		kgo.Dialer(func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := cl.ProduceSync(pctx, &kgo.Record{Topic: "sock", Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce over unix: %v", err)
	}
}

// TestEphemeralPortIsAdvertised is the regression for ":0": the broker bound a
// free port but Metadata still told clients the configured string's port
// (falling back to 9092), so every client was sent somewhere nothing listened.
// The client here connects to the bound address, is handed the advertised one
// by Metadata, and must be able to use it.
func TestEphemeralPortIsAdvertised(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker integration test in -short mode")
	}
	srv, err := server.New(server.Options{DataDir: t.TempDir(), ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }() // Serve binds: Addr must be set by it.
	t.Cleanup(func() {
		cancel()
		<-done
	})

	var addr net.Addr
	for i := 0; i < 100 && addr == nil; i++ {
		if addr = srv.Addr(); addr == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if addr == nil {
		t.Fatal("Addr() still nil after Serve started")
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr.String()), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	// Metadata tells the client where to produce; with the wrong advertised
	// port this times out instead of returning.
	if err := cl.ProduceSync(pctx, &kgo.Record{Topic: "eph", Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce via advertised address: %v", err)
	}
}

// TestNoListenAddrIsLoopbackEphemeral pins the embedder default: no address
// asked for means a free loopback port, never the :9092 wildcard.
func TestNoListenAddrIsLoopbackEphemeral(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker integration test in -short mode")
	}
	srv, err := server.New(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, err := srv.Bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ta := addr.(*net.TCPAddr)
	if !ta.IP.IsLoopback() || ta.Port == 9092 {
		t.Fatalf("default bind = %v, want loopback on a free port", addr)
	}
	if srv.Addr() == nil {
		t.Fatal("Addr() nil after Bind")
	}
	cancel()
	_ = srv.Serve(ctx)
}

// The package's own doc example used a data dir that did not exist yet and
// failed with "no such file or directory": bbolt creates the file, not its
// directory, and the log manager made the data dir only after the metadata store
// was open.
func TestNewCreatesMissingDirectories(t *testing.T) {
	root := t.TempDir()
	srv, err := server.New(server.Options{
		DataDir: root + "/does/not/exist/yet/data",
		MetaDB:  root + "/elsewhere/also/new/meta.db",
	})
	if err != nil {
		t.Fatalf("New with missing parent directories: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.Serve(ctx); err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

// syncBuffer is a bytes.Buffer safe to write from the broker's goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// One log line per request is a firehose at the default level (a consumer polls
// several times a second) and used to drown everything else. It is a debug line.
func TestRequestsAreLoggedAtDebugNotInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker integration test in -short mode")
	}
	for _, tc := range []struct {
		level slog.Level
		want  bool
	}{{slog.LevelInfo, false}, {slog.LevelDebug, true}} {
		var logs syncBuffer
		srv, err := server.New(server.Options{DataDir: t.TempDir(),
			Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: tc.level}))})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		addr, err := srv.Bind(ctx)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- srv.Serve(ctx) }()
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr.String()), kgo.AllowAutoTopicCreation())
		if err != nil {
			t.Fatal(err)
		}
		pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := cl.ProduceSync(pctx, &kgo.Record{Topic: "t", Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		pcancel()
		cl.Close()
		cancel()
		<-done
		if got := strings.Contains(logs.String(), "msg=request"); got != tc.want {
			t.Errorf("level %v: request lines present = %v, want %v\n%s", tc.level, got, tc.want, logs.String())
		}
	}
}

// A config with no metadata path keeps it with the data, so a data directory is
// self-contained: delete it and the topics and committed offsets go with it.
func TestMetadataDefaultsToInsideTheDataDir(t *testing.T) {
	dir := t.TempDir()
	srv, err := server.New(server.Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := srv.Config().MetaDB, filepath.Join(dir, "meta.db"); got != want {
		t.Fatalf("MetaDB = %q, want %q", got, want)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	srv.Serve(ctx) // closes storage on the way out.
	if _, err := os.Stat(filepath.Join(dir, "meta.db")); err != nil {
		t.Errorf("no meta.db inside the data dir: %v", err)
	}
}
