package console

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// producePartition writes n records of framed(i) to one partition.
func producePartition(t *testing.T, cl *kgo.Client, topic string, partition int32, n int, value func(i int) []byte) {
	t.Helper()
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: topic, Partition: partition, Value: value(i)}
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// What the scan leaves unread it counts out of what the partition holds, from where the
// log starts; a partition read whole says nothing is unread.
func TestTheSchemaScanCountsWhatItLeftUnreadFromTheLogStart(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "big", 1, nil); err != nil {
		t.Fatal(err)
	}
	producePartition(t, cl, "big", 0, 13, func(int) []byte { return framedValue(3) })
	deleteBefore(t, cl, "big", 0, 3) // the log now holds offsets 3 to 12: ten records.
	defer func(n int) { schemaScanMaxRecords = n }(schemaScanMaxRecords)
	schemaScanMaxRecords = 4
	s, err := c.scanSchemaIDs(ctx, "big")
	if err != nil {
		t.Fatal(err)
	}
	want := "big partition 0: the oldest 6 of 10 records"
	if len(s.Unread) != 1 || s.Unread[0] != want || s.Scanned != 4 {
		t.Errorf("scanned %d, unread %q, want 4 and %q", s.Scanned, s.Unread, want)
	}

	// Three partitions of one record each, a budget of two: the scan stops at two, and
	// the oldest-records note is only for a partition read from past its start.
	if err := c.be.CreateTopic(ctx, "three", 3, nil); err != nil {
		t.Fatal(err)
	}
	for p := int32(0); p < 3; p++ {
		producePartition(t, cl, "three", p, 1, func(int) []byte { return framedValue(5) })
	}
	schemaScanMaxRecords = 2
	s, err = c.scanSchemaIDs(ctx, "three")
	if err != nil {
		t.Fatal(err)
	}
	if s.Scanned != 2 || len(s.Unread) != 0 {
		t.Errorf("a budget of two over three records: scanned %d, unread %q", s.Scanned, s.Unread)
	}
}

// A topic whose first records carry no schema id is given up on at the twentieth, not
// before.
func TestTheSchemaScanGivesUpAtTheTwentiethRecordWithNoId(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "text", 1, nil); err != nil {
		t.Fatal(err)
	}
	producePartition(t, cl, "text", 0, 30, func(i int) []byte { return []byte(fmt.Sprintf("plain %d", i)) })
	s, err := c.scanSchemaIDs(ctx, "text")
	if err != nil {
		t.Fatal(err)
	}
	if !s.GaveUp || s.Scanned != schemaScanProbe || s.NotFramed != schemaScanProbe {
		t.Errorf("a topic of plain text: %+v, want it given up on at record %d", s, schemaScanProbe)
	}
}

// A partition with nothing in it is not waited for.
func TestTheSchemaScanDoesNotWaitForAnEmptyPartition(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "lopsided", 2, nil); err != nil {
		t.Fatal(err)
	}
	producePartition(t, cl, "lopsided", 0, 3, func(int) []byte { return framedValue(2) })
	defer func(d time.Duration) { schemaScanMaxTime = d }(schemaScanMaxTime)
	schemaScanMaxTime = 5 * time.Second
	s, err := c.scanSchemaIDs(ctx, "lopsided")
	if err != nil {
		t.Fatal(err)
	}
	if s.TimedOut || s.Scanned != 3 {
		t.Errorf("one partition of three records and one empty: %+v", s)
	}
}
