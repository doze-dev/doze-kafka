package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// An acks=all write that was acknowledged is on disk: the process is killed
// (SIGKILL, no flush, no shutdown hook) the moment the acknowledgement arrives,
// and the record is there when the same directory is opened again. The timer
// fsync is off so nothing but the write's own sync can be what saved it.
func TestCrash_AcknowledgedAcksAllWriteSurvivesSIGKILL(t *testing.T) {
	n := startNode(t, nodeSubprocess(), nodeFsync("0"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin := newClient(t, n.Addr)
	mustCreateTopic(t, ctx, admin, "durable", 1)
	admin.Close()

	producer, err := kgo.NewClient(kgo.SeedBrokers(n.Addr), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite())
	if err != nil {
		t.Fatal(err)
	}
	rec := &kgo.Record{Topic: "durable", Key: []byte("k"), Value: []byte("acked")}
	if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	producer.Close()

	n.Kill()
	n.Start()

	got := consumeRecords(t, ctx, n.Addr, "durable", 1)
	if len(got) != 1 || string(got[0].Value) != "acked" || got[0].Offset != 0 {
		t.Fatalf("after SIGKILL the acknowledged record is %+v", got)
	}
}

// A frozen broker answers nothing and loses nothing: the request waits, and is
// answered when the process continues.
func TestCrash_PausedBrokerAnswersAfterResume(t *testing.T) {
	n := startNode(t, nodeSubprocess())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cl := newClient(t, n.Addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "frozen", 1)

	n.Pause()
	done := make(chan error, 1)
	go func() {
		done <- cl.ProduceSync(ctx, &kgo.Record{Topic: "frozen", Value: []byte("late")}).FirstErr()
	}()
	select {
	case err := <-done:
		t.Fatalf("a paused broker answered a produce: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	n.Resume()
	if err := <-done; err != nil {
		t.Fatalf("produce after resume: %v", err)
	}
}
