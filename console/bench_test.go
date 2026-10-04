package console

import (
	"context"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// What a tape refresh or a drawer open costs the console backend. Run with:
//
//	go test ./console/ -run '^$' -bench . -benchtime 100x
func BenchmarkFetchWindow(b *testing.B) {
	c := newTestConsole(b)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "bench", 1, nil); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if _, err := c.be.Produce(ctx, "bench", 0, nil, []byte("value"), nil, "1"); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for range b.N {
		msgs, err := c.be.FetchWindow(ctx, "bench", 0, 0, 60)
		if err != nil || len(msgs) != 60 {
			b.Fatalf("FetchWindow = %d msgs, %v", len(msgs), err)
		}
	}
}

// The group list used to cost 1 + 2N round trips for N groups (each describe
// re-listed every group to learn its protocol).
func BenchmarkListGroups100(b *testing.B) {
	c, addr := newTestConsoleAddr(b)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "t", 1, nil); err != nil {
		b.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		b.Fatal(err)
	}
	defer cl.Close()
	for i := 0; i < 100; i++ { // groups known only by a committed offset.
		oc := kmsg.NewPtrOffsetCommitRequest()
		oc.Group, oc.Generation = fmt.Sprintf("group-%03d", i), -1
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = "t"
		rt.Partitions = []kmsg.OffsetCommitRequestTopicPartition{kmsg.NewOffsetCommitRequestTopicPartition()}
		oc.Topics = append(oc.Topics, rt)
		if _, err := oc.RequestWith(ctx, cl); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for range b.N {
		if _, err := c.be.ListGroups(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// The rail's counts, polled every few seconds by every open tab.
func BenchmarkCounts(b *testing.B) {
	c := newTestConsole(b)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := c.be.CreateTopic(ctx, "t"+string(rune('a'+i)), 6, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for range b.N {
		if _, _, err := c.be.Counts(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
