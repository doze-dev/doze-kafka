package server_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/server"
)

// run starts a server and returns its address and a stop func.
func run(t *testing.T, opts server.Options) (string, func()) {
	t.Helper()
	srv, err := server.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	<-srv.Ready()
	return srv.Addr().String(), func() { cancel(); <-done }
}

func produceN(t *testing.T, addr, topic string, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func endOffset(t *testing.T, addr, topic string) int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Timestamp = -1
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	return resp.Topics[0].Partitions[0].Offset
}

// Partition data whose topic the registry does not know (the metadata database
// was lost, or the broker pointed at another one) is reported at start-up, and is
// not handed to a new topic of the same name as its first records.
func TestLeftoverPartitionDataIsReportedAndNotAdopted(t *testing.T) {
	if testing.Short() {
		t.Skip("boots a broker")
	}
	dir := t.TempDir()
	addr, stop := run(t, server.Options{DataDir: dir})
	produceN(t, addr, "orders", 5)
	stop()
	// Lose the registry, keep the logs.
	if err := os.Remove(filepath.Join(dir, "meta.db")); err != nil {
		t.Fatal(err)
	}

	var logs syncBuffer
	addr, stop = run(t, server.Options{DataDir: dir, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	defer stop()
	if out := logs.String(); !strings.Contains(out, "topics the metadata database does not know") || !strings.Contains(out, "orders-0") {
		t.Errorf("start-up said nothing about the leftover data:\n%s", out)
	}
	produceN(t, addr, "orders", 2)
	if n := endOffset(t, addr, "orders"); n != 2 {
		t.Errorf("the re-created topic has %d records, want the 2 just produced (it adopted old data)", n)
	}
}

// One broker per data directory, even when the metadata databases differ.
func TestSecondBrokerOnTheSameDataDirIsRefused(t *testing.T) {
	dir := t.TempDir()
	first, err := server.New(server.Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = server.New(server.Options{DataDir: dir, MetaDB: filepath.Join(t.TempDir(), "other.db")})
	if err == nil || !strings.Contains(err.Error(), "in use by another doze-kafka") {
		t.Fatalf("a second broker on the same data dir = %v, want a refusal that says why", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the refusal took %v", took)
	}
	first.Close()
	// Released on Close.
	again, err := server.New(server.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("after the first closed: %v", err)
	}
	again.Close()
}

// A topic that exists in the registry keeps working after a restart even if one of
// its partitions was never written to (and so was never opened).
func TestRegisteredTopicsGetTheirDirectoriesAtStartup(t *testing.T) {
	if testing.Short() {
		t.Skip("boots a broker")
	}
	dir := t.TempDir()
	addr, stop := run(t, server.Options{DataDir: dir, DefaultPartitions: 3})
	produceN(t, addr, "t", 1)
	stop()
	for _, p := range []string{"t-0", "t-1", "t-2"} {
		os.RemoveAll(filepath.Join(dir, p)) // as an older layout, or a careless cleanup, would leave it.
	}
	addr, stop = run(t, server.Options{DataDir: dir, DefaultPartitions: 3})
	defer stop()
	produceN(t, addr, "t", 3)
}
