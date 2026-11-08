package console

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The merged view orders records written at the same moment by partition, then newest
// offset first, and marks the newest three for the live highlight.
func TestTheMergedViewOrdersRecordsOfOneMomentByPartitionThenNewestOffset(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "mg", 2, nil); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	var recs []*kgo.Record
	for p, n := range []int{2, 3} {
		for i := 0; i < n; i++ {
			recs = append(recs, &kgo.Record{Topic: "mg", Partition: int32(p), Timestamp: at, Value: []byte(fmt.Sprintf("p%d-%d", p, i))})
		}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	bounds, err := c.be.PartitionBounds(ctx, "mg", 2)
	if err != nil {
		t.Fatal(err)
	}
	d, err := c.tapeAllData(ctx, "mg", "id", 2, bounds)
	if err != nil {
		t.Fatal(err)
	}

	var order []string
	for _, v := range d["Rows"].([]msgView) {
		order = append(order, fmt.Sprintf("p%d@%d", v.Partition, v.Offset))
	}
	if got := strings.Join(order, " "); got != "p0@1 p0@0 p1@2 p1@1 p1@0" {
		t.Errorf("newest first, ties by partition then newest offset: %s", got)
	}

	// The strip runs oldest to newest, and the last three are hot.
	var hot []string
	for _, v := range d["Messages"].([]msgView) {
		hot = append(hot, fmt.Sprintf("p%d@%d=%v", v.Partition, v.Offset, v.Hot))
	}
	if got := strings.Join(hot, " "); got != "p1@0=false p1@1=false p1@2=true p0@0=true p0@1=true" {
		t.Errorf("the newest three are hot: %s", got)
	}

	if got := d["MergedNote"]; got != "5 shown, newest first across partitions" {
		t.Errorf("note %q: a topic read whole, with every partition read, says only how many", got)
	}
	if d["PartSelect"] != false {
		t.Errorf("two partitions need no select: %v", d["PartSelect"])
	}
	if d["Copied"] != int64(-1) || d["HL"] != int64(-1) {
		t.Errorf("nothing is copied or highlighted in the merged view: Copied %v, HL %v", d["Copied"], d["HL"])
	}
}

// The merged view reads the newest 30 of each partition and shows the newest sixty
// of those; it says "older records are elsewhere" only when it left some out.
func TestTheMergedViewSaysWhenItLeftRecordsOutAndOnlyThen(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	merged := func(topic string, n int32) string {
		t.Helper()
		bounds, err := c.be.PartitionBounds(ctx, topic, n)
		if err != nil {
			t.Fatal(err)
		}
		d, err := c.tapeAllData(ctx, topic, "id", n, bounds)
		if err != nil {
			t.Fatal(err)
		}
		return d["MergedNote"].(string)
	}

	// Thirty on each of two partitions is sixty: exactly the window, nothing left out.
	if err := c.be.CreateTopic(ctx, "exact", 2, nil); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-time.Hour)
	bulkProduce(t, cl, "exact", 0, 30, vals("a"), at)
	bulkProduce(t, cl, "exact", 1, 30, vals("b"), at)
	if got := merged("exact", 2); got != "60 shown, newest first across partitions" {
		t.Errorf("thirty on each of two partitions: %q", got)
	}

	// Seventy on one: forty of them are not read.
	if err := c.be.CreateTopic(ctx, "deep", 2, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "deep", 0, 70, vals("a"), at)
	bulkProduce(t, cl, "deep", 1, 3, vals("b"), at)
	want := "33 shown, newest first across partitions; the newest 30 of each partition were read, so older records are on the partitions' own pages"
	if got := merged("deep", 2); got != want {
		t.Errorf("seventy on one partition:\n got %q\nwant %q", got, want)
	}

	// Thirty on each of three partitions is ninety: the window of sixty cuts thirty.
	if err := c.be.CreateTopic(ctx, "over", 3, nil); err != nil {
		t.Fatal(err)
	}
	for p := int32(0); p < 3; p++ {
		bulkProduce(t, cl, "over", p, 30, vals("a"), at)
	}
	want = "60 shown, newest first across partitions; the newest 30 of each partition were read, so older records are on the partitions' own pages"
	if got := merged("over", 3); got != want {
		t.Errorf("thirty on each of three partitions:\n got %q\nwant %q", got, want)
	}
}

// A topic of more partitions than the merged view reads says how many it read, and a
// topic of exactly the limit has no partition select.
func TestTheMergedViewOfAWideTopicSaysHowManyPartitionsItRead(t *testing.T) {
	c, _ := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "wide", 24, nil); err != nil {
		t.Fatal(err)
	}
	bounds, err := c.be.PartitionBounds(ctx, "wide", 24)
	if err != nil {
		t.Fatal(err)
	}
	d, err := c.tapeAllData(ctx, "wide", "id", 24, bounds)
	if err != nil {
		t.Fatal(err)
	}
	if got := d["MergedNote"]; got != "0 shown, newest first across partitions; the topic has 24 partitions and the first 12 were read" {
		t.Errorf("note %q", got)
	}
	if d["PartSelect"] != false {
		t.Errorf("24 partitions still get a tab each: %v", d["PartSelect"])
	}
	if lanes := d["Lanes"].([]tapeLane); len(lanes) != 12 {
		t.Errorf("%d lanes, want the 12 partitions read", len(lanes))
	}
}

// A jump past the end says which offsets the partition holds, and a tape shows nothing
// as copied or highlighted until it is asked to.
func TestAJumpPastTheEndNamesTheOffsetsThePartitionHolds(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "jp", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "jp", 0, 5, vals("v"), time.Time{})
	f := tapeFixture{c: c, addr: addr, id: "i"}
	d := f.tape(t, "jp", 0, 1, -1, -1, 99, false)
	jv, _ := d["Jump"].(*jumpView)
	if jv == nil || jv.Note != "p0 holds offsets 0 to 4 now. Showing the newest records." {
		t.Errorf("jump: %+v", jv)
	}
	if d["Copied"] != int64(-1) || d["HL"] != int64(-1) {
		t.Errorf("Copied %v, HL %v: nothing copied, nothing to highlight", d["Copied"], d["HL"])
	}
	plain := f.tape(t, "jp", 0, 1, -1, -1, -1, false)
	if plain["Copied"] != int64(-1) || plain["HL"] != int64(-1) {
		t.Errorf("a plain tape: Copied %v, HL %v", plain["Copied"], plain["HL"])
	}
}

// An older window of a plain topic offers no committed-only toggle: the records after
// the window are not part of what it shows.
func TestAnOlderWindowOfAPlainTopicHasNoTransactions(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "older", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "older", 0, 200, vals("v"), time.Time{})
	f := tapeFixture{c: c, addr: addr, id: "i"}
	d := f.tape(t, "older", 0, 1, 60, -1, -1, false)
	if d["HasTxn"] != false {
		t.Errorf("a window with no transaction in it reports HasTxn %v", d["HasTxn"])
	}
	rows := d["Rows"].([]msgView)
	if len(rows) != 60 || rows[0].Offset != 119 || rows[59].Offset != 60 {
		t.Errorf("window %d rows, %d down to %d", len(rows), rows[0].Offset, rows[len(rows)-1].Offset)
	}
	for _, r := range rows {
		if r.Txn != "" {
			t.Fatalf("offset %d is called %q", r.Offset, r.Txn)
		}
	}
}

// A window that starts after the first records of two transactions shows both their
// markers and the one record that is left: more markers than records is fine.
func TestAWindowWithMoreMarkersThanRecordsShowsThemAll(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "mm", 1, nil); err != nil {
		t.Fatal(err)
	}
	p := txnProducer(t, addr, "twice")
	for i := 0; i < 2; i++ {
		if err := p.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		txnWrite(t, ctx, p, "mm", fmt.Sprint("v", i))
		if err := p.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}
	}
	// Offsets: 0 record, 1 marker, 2 record, 3 marker. Retention takes the first record.
	deleteBefore(t, rawClient(t, addr), "mm", 0, 1)
	f := tapeFixture{c: c, addr: addr, id: "i"}
	d := f.tape(t, "mm", 0, 1, -1, -1, -1, false)
	var got []string
	for _, v := range d["Rows"].([]msgView) {
		got = append(got, fmt.Sprintf("%d:marker=%v", v.Offset, v.Marker))
	}
	if strings.Join(got, " ") != "3:marker=true 2:marker=false 1:marker=true" {
		t.Errorf("rows %v", got)
	}
}
