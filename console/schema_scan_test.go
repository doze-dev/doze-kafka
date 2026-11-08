package console

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// framedValue is a registry-framed value for id with a body that need not decode.
func framedValue(id int) []byte {
	return []byte{0, byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id), 2, 'h', 'i'}
}

// The scan counts the schema ids a topic's values carry, with where each was first and
// last seen, what is not framed, and what its budget left unread; a topic whose first
// records are none of them framed is given up on early. (SCH-25)
func TestTopicScanCountsSchemaIDs(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	topicWith(t, c, "orders", 2, 0)
	topicWith(t, c, "plain", 1, 0)
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	produce := func(topic string, part int32, v []byte) {
		t.Helper()
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: part, Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		produce("orders", 0, framedValue(7))
	}
	produce("orders", 0, framedValue(4))
	produce("orders", 1, framedValue(1))
	produce("orders", 1, []byte("not framed"))
	produce("orders", 1, framedValue(9))
	produce("orders", 1, nil) // a tombstone
	for i := 0; i < 30; i++ {
		produce("plain", 0, []byte(`{"json":"text"}`))
	}
	produce("plain", 0, framedValue(7)) // past the point the scan gives up

	s, err := c.scanSchemaIDs(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if s.Scanned != 10 || s.NotFramed != 1 || s.Tombstones != 1 || s.TimedOut || s.GaveUp {
		t.Fatalf("orders: %+v", s)
	}
	want := map[int]int{1: 1, 4: 1, 7: 5, 9: 1}
	if len(s.IDs) != len(want) {
		t.Fatalf("ids: %+v", s.IDs)
	}
	for i, u := range s.IDs {
		if want[u.ID] != u.Count || (i > 0 && s.IDs[i-1].ID >= u.ID) {
			t.Errorf("id %d counted %d, want %d (ids in order: %+v)", u.ID, u.Count, want[u.ID], s.IDs)
		}
	}
	if u := s.idUse(7); u == nil || u.First.Partition != 0 || u.First.Offset != 0 || u.Last.Offset != 4 {
		t.Errorf("id 7 first and last: %+v", u)
	}

	p, err := c.scanSchemaIDs(ctx, "plain")
	if err != nil {
		t.Fatal(err)
	}
	if !p.GaveUp || len(p.IDs) != 0 || p.Scanned > schemaScanProbe {
		t.Errorf("a topic of plain JSON: %+v, want given up after %d records", p, schemaScanProbe)
	}

	// The budget: a scan that runs out says what it left unread.
	defer func(n int) { schemaScanMaxRecords = n }(schemaScanMaxRecords)
	schemaScanMaxRecords = 3
	b, err := c.scanSchemaIDs(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if b.Scanned > 3 || len(b.Unread) == 0 {
		t.Errorf("a scan over its budget: %+v, want it to stop and say what is unread", b)
	}
}
