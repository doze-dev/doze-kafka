package server_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/doze-dev/doze-kafka/internal/config"
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

// A second listener serves the same broker. The zone uses it to answer on
// kafka.doze's address when the main listener is loopback.
func TestAlsoListenServesTheSameBroker(t *testing.T) {
	srv, err := server.New(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	main, err := srv.Bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	other, err := srv.AlsoListen(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if other.String() == main.String() {
		t.Fatalf("both listeners are %s", main)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()

	for _, addr := range []net.Addr{main, other} {
		c, err := net.DialTimeout("tcp", addr.String(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		// ApiVersions v0: length, api key 18, version 0, correlation 7, null client id.
		req := []byte{0, 0, 0, 10, 0, 18, 0, 0, 0, 0, 0, 7, 0xff, 0xff}
		c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := c.Write(req); err != nil {
			t.Fatal(err)
		}
		resp := make([]byte, 8)
		if _, err := io.ReadFull(c, resp); err != nil {
			t.Fatalf("%s did not answer ApiVersions: %v", addr, err)
		}
		if resp[4] != 0 || resp[7] != 7 {
			t.Errorf("%s: response header %v, want correlation id 7", addr, resp)
		}
		c.Close()
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Serve: %v", err)
	}
}

// Close frees a server that never served, twice over, without error.
func TestCloseWithoutServeIsSafeAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	srv, err := server.New(server.Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// The metadata lock is released: the same directory opens again.
	again, err := server.New(server.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopening after Close: %v", err)
	}
	again.Close()
}

// Ready is how an embedder waits for the listener without sleeping, and it never
// hangs: it is also closed when Serve returns without ever binding.
func TestReadyAndConfig(t *testing.T) {
	dir := t.TempDir()
	srv, err := server.New(server.Options{DataDir: dir, Version: 3, SegmentBytes: 1 << 20, IndexIntervalBytes: 1024,
		ConsumerSessionTimeout: 7 * time.Second, FsyncInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-srv.Ready():
		t.Fatal("Ready is closed before anything is bound")
	default:
	}
	if srv.Addr() != nil {
		t.Fatal("Addr is set before Bind")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("Ready never closed")
	}
	if srv.Addr() == nil {
		t.Fatal("Ready is closed but Addr is nil")
	}
	cfg := srv.Config()
	if cfg.KafkaVersion != 3 || cfg.DataDir != dir || cfg.MetaDB != filepath.Join(dir, "meta.db") || cfg.SASL || cfg.TLS ||
		cfg.ListenAddr != "127.0.0.1:0" {
		t.Errorf("Config() = %+v", cfg)
	}
	cancel()
	<-done

	// A server that fails to bind still closes Ready.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	srv2, err := server.New(server.Options{DataDir: t.TempDir(), ListenAddr: busy.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv2.Serve(context.Background()); err == nil {
		t.Fatal("Serve on a taken port should fail")
	}
	select {
	case <-srv2.Ready():
	default:
		t.Error("Ready should be closed after a failed Serve, so waiters do not hang")
	}
	if srv2.Addr() != nil {
		t.Error("Addr should stay nil when binding failed")
	}
}

// The tuning options reach the broker: a tiny segment size rolls segments after a
// few records, which is how an embedder tests retention.
func TestSegmentOptionRolls(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker integration test in -short mode")
	}
	dir := t.TempDir()
	srv, err := server.New(server.Options{DataDir: dir, SegmentBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	<-srv.Ready()
	cl, err := kgo.NewClient(kgo.SeedBrokers(srv.Addr().String()), kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.NoCompression())) // a run of "x" would compress to nothing and never fill a segment.
	if err != nil {
		t.Fatal(err)
	}
	pctx, pcancel := context.WithTimeout(ctx, 20*time.Second)
	payload := bytes.Repeat([]byte("x"), 1000)
	for i := 0; i < 20; i++ {
		if err := cl.ProduceSync(pctx, &kgo.Record{Topic: "roll", Value: payload}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	pcancel()
	cl.Close()
	cancel()
	<-done
	logs, _ := filepath.Glob(filepath.Join(dir, "roll-0", "*.log"))
	if len(logs) < 3 {
		t.Errorf("20 x 1 KB records with SegmentBytes=4096 left %d segment file(s), want several", len(logs))
	}
}

// What clients asked is remembered only for a console to show. An embedded broker has
// none, and so pays nothing per request; the command line's, with its console on, does.
func TestOnlyABrokerWithAConsoleKeepsATrafficRecord(t *testing.T) {
	embedded, err := server.New(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer embedded.Close()
	if embedded.Traffic() != nil {
		t.Error("an embedded broker keeps a traffic record nobody will read")
	}

	cfg := config.Default()
	cfg.DataDir, cfg.ListenAddr = t.TempDir(), "127.0.0.1:0"
	withConsole, err := server.NewFromConfig(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer withConsole.Close()
	if withConsole.Traffic() == nil {
		t.Error("a broker with a console keeps no traffic record")
	}
	cfg.Console, cfg.DataDir = false, t.TempDir()
	without, err := server.NewFromConfig(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer without.Close()
	if without.Traffic() != nil {
		t.Error("--console=false still keeps a traffic record")
	}
}
