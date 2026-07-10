package server_test

import (
	"context"
	"net"
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
