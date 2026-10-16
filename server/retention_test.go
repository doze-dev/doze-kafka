package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/server"
)

// OffsetsRetention reaches the broker: a short one expires a standalone committer's
// offset, a negative one keeps it, and zero is the default (7 days), as the zero value of
// every other duration in Options is.
func TestOffsetsRetentionOption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker integration test in -short mode")
	}
	for _, c := range []struct {
		name      string
		retention time.Duration
		kept      bool
	}{
		{"short", time.Second, false},
		{"negative keeps for ever", -1, true},
		{"zero is the default", 0, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv, err := server.New(server.Options{DataDir: t.TempDir(), OffsetsRetention: c.retention})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			addr, err := srv.Bind(ctx)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- srv.Serve(ctx) }()
			t.Cleanup(func() { cancel(); <-done })

			cl, err := kgo.NewClient(kgo.SeedBrokers(addr.String()), kgo.AllowAutoTopicCreation())
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "kept", Value: []byte("x")}).FirstErr(); err != nil {
				t.Fatal(err)
			}
			commit := kmsg.NewPtrOffsetCommitRequest()
			commit.Group, commit.Generation = "standalone", -1
			ct := kmsg.NewOffsetCommitRequestTopic()
			ct.Topic = "kept"
			cp := kmsg.NewOffsetCommitRequestTopicPartition()
			cp.Offset = 1
			ct.Partitions = append(ct.Partitions, cp)
			commit.Topics = append(commit.Topics, ct)
			if res, err := commit.RequestWith(ctx, cl); err != nil || res.Topics[0].Partitions[0].ErrorCode != 0 {
				t.Fatalf("commit: %v %+v", err, res)
			}
			time.Sleep(3 * time.Second) // the retention, and a sweep every 50 ms.
			fetch := kmsg.NewPtrOffsetFetchRequest()
			fetch.Group = "standalone"
			ft := kmsg.NewOffsetFetchRequestTopic()
			ft.Topic, ft.Partitions = "kept", []int32{0}
			fetch.Topics = append(fetch.Topics, ft)
			res, err := fetch.RequestWith(ctx, cl)
			if err != nil {
				t.Fatal(err)
			}
			if got := res.Topics[0].Partitions[0].Offset; (got == 1) != c.kept {
				t.Errorf("offset = %d, want it kept: %v", got, c.kept)
			}
		})
	}
}
