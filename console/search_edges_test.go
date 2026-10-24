package console

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func vals(prefix string) func(int) string {
	return func(i int) string { return fmt.Sprintf("%s%d", prefix, i) }
}

// How much one partition's scan reads: the budget is a cap on records, a log that
// starts above zero is read from where it starts, and before=0 means nothing is
// before the start.
func TestScanPartitionBudgetAndBounds(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "sb", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "sb", 0, 3000, vals("r"), time.Time{})

	_, from, to, earliest, err := c.scanPartition(ctx, "sb", 0, []byte("zzz"), -1, 1500)
	if err != nil || from != 1500 || to != 3000 || earliest != 0 {
		t.Errorf("a budget of 1500 over 3000 records read [%d,%d) from a log starting at %d (%v)", from, to, earliest, err)
	}
	_, from, to, _, err = c.scanPartition(ctx, "sb", 0, []byte("zzz"), 0, 1500)
	if err != nil || from != 0 || to != 0 {
		t.Errorf("before=0 read [%d,%d) (%v), want nothing", from, to, err)
	}
	_, from, to, _, err = c.scanPartition(ctx, "sb", 0, []byte("zzz"), 3000, 1500)
	if err != nil || from != 1500 || to != 3000 {
		t.Errorf("before=the end read [%d,%d) (%v), want [1500,3000)", from, to, err)
	}

	deleteBefore(t, cl, "sb", 0, 1200)
	hits, from, to, earliest, err := c.scanPartition(ctx, "sb", 0, []byte("r2999"), -1, 5000)
	if err != nil || from != 1200 || to != 3000 || earliest != 1200 || len(hits) != 1 || hits[0].Offset != 2999 {
		t.Errorf("a log starting at 1200: read [%d,%d) of a log starting at %d, %d hits (%v)", from, to, earliest, len(hits), err)
	}
	// before below the start of the log is the start.
	_, from, to, _, err = c.scanPartition(ctx, "sb", 0, []byte("zzz"), 10, 5000)
	if err != nil || from != 1200 || to != 1200 {
		t.Errorf("before below the log start read [%d,%d) (%v)", from, to, err)
	}
}

// A search of one partition says what it read, and offers to go further back exactly
// when there is further back to go.
func TestSearchOnePartitionNoteAndOlderLink(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for _, n := range []string{"small", "exact", "big"} {
		if err := c.be.CreateTopic(ctx, n, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	bulkProduce(t, cl, "small", 0, 250, vals("v"), time.Time{})
	bulkProduce(t, cl, "exact", 0, 100, vals("x"), time.Time{})
	bulkProduce(t, cl, "big", 0, 5200, vals("r"), time.Time{})
	search := func(topic, q string, before int64) *searchView {
		sv, err := c.searchTopic(ctx, topic, 0, 1, q, false, before)
		if err != nil {
			t.Fatal(err)
		}
		return sv
	}
	base := c.topicURL("small") + "?partition=0"

	// Everything read, nothing left, no cap reached: no link back.
	sv := search("small", "v24", -1)
	if sv.Note != "Read offsets 0–249 of partition 0 (250 records): 11 matches." || sv.OlderURL != "" || len(sv.Rows) != 11 {
		t.Errorf("a whole small log: %q, older %q, %d rows", sv.Note, sv.OlderURL, len(sv.Rows))
	}
	if sv.ClearURL != base {
		t.Errorf("clear link %q, want %q", sv.ClearURL, base)
	}
	// More matches than are shown: the newest hundred, and a link from below the oldest shown.
	sv = search("small", "v1", -1)
	if len(sv.Rows) != 100 || sv.Rows[0].Offset != 199 || sv.Rows[99].Offset != 100 ||
		sv.OlderURL != base+"&q=v1&before=100" || !strings.Contains(sv.Note, ": 100 matches.") {
		t.Errorf("111 matches: %d rows, newest %d, older %q, %q", len(sv.Rows), sv.Rows[0].Offset, sv.OlderURL, sv.Note)
	}
	// Exactly a hundred matches is also "full": there may be more before.
	sv = search("exact", "x", -1)
	if len(sv.Rows) != 100 || sv.OlderURL != c.topicURL("exact")+"?partition=0&q=x&before=0" {
		t.Errorf("100 matches: %d rows, older %q", len(sv.Rows), sv.OlderURL)
	}
	// The budget is spent before the log is: the link continues from where the read stopped.
	sv = search("big", "zzz", -1)
	if sv.Note != "Read offsets 200–5199 of partition 0 (5000 records): 0 matches." ||
		sv.OlderURL != c.topicURL("big")+"?partition=0&q=zzz&before=200" {
		t.Errorf("a 5200-record log: %q, older %q", sv.Note, sv.OlderURL)
	}
	// ...and follows it.
	sv = search("big", "zzz", 200)
	if sv.Note != "Read offsets 0–199 of partition 0 (200 records): 0 matches." || sv.OlderURL != "" {
		t.Errorf("the rest of the 5200-record log: %q, older %q", sv.Note, sv.OlderURL)
	}
	if sv = search("big", "zzz", 0); sv.Note != "Partition 0 has no records in that range." {
		t.Errorf("before=0: %q", sv.Note)
	}
}

// Over every partition: how much of each is read, and which order the matches come in.
func TestSearchAllPartitions(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for name, parts := range map[string]int32{"wide": 2, "few": 2, "ties": 2, "many": 2, "fifty": 51, "exactly": 2} {
		if err := c.be.CreateTopic(ctx, name, parts, nil); err != nil {
			t.Fatal(err)
		}
	}
	search := func(topic string, parts int32, q string) *searchView {
		sv, err := c.searchTopic(ctx, topic, 0, parts, q, true, -1)
		if err != nil {
			t.Fatal(err)
		}
		return sv
	}

	bulkProduce(t, cl, "wide", 0, 3000, vals("a"), time.Time{})
	bulkProduce(t, cl, "wide", 1, 3000, vals("b"), time.Time{})
	sv := search("wide", 2, "zzz")
	if want := "Read 5000 records across 2 partitions: 0 matches, newest first. Only the newest 2500 of each partition were read; search one partition to go further back."; sv.Note != want {
		t.Errorf("two 3000-record partitions:\n got %q\nwant %q", sv.Note, want)
	}

	bulkProduce(t, cl, "few", 0, 3, vals("a"), time.Time{})
	bulkProduce(t, cl, "few", 1, 3, vals("b"), time.Time{})
	sv = search("few", 2, "")
	if sv.Note != "Read 6 records across 2 partitions: 6 records, newest first." || len(sv.Rows) != 6 {
		t.Errorf("two small partitions: %q, %d rows", sv.Note, len(sv.Rows))
	}

	// Same timestamp: partition ascending, then the newest offset first.
	at := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	var recs []*kgo.Record
	for _, p := range []int32{1, 0} {
		for i := 0; i < 2; i++ {
			recs = append(recs, &kgo.Record{Topic: "ties", Partition: p, Value: []byte("t"), Timestamp: at})
		}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	sv = search("ties", 2, "t")
	var order []string
	for _, r := range sv.Rows {
		order = append(order, fmt.Sprintf("p%do%d", r.Partition, r.Offset))
	}
	if got := strings.Join(order, " "); got != "p0o1 p0o0 p1o1 p1o0" {
		t.Errorf("records with one timestamp came in the order %s", got)
	}

	// The shown matches are capped at a hundred, and the note says so only when it cut.
	bulkProduce(t, cl, "exactly", 0, 50, vals("m"), time.Time{})
	bulkProduce(t, cl, "exactly", 1, 50, vals("m"), time.Time{})
	sv = search("exactly", 2, "m")
	if len(sv.Rows) != 100 || strings.Contains(sv.Note, "Showing the newest") || !strings.Contains(sv.Note, "100 matches") {
		t.Errorf("exactly 100 matches: %d rows, %q", len(sv.Rows), sv.Note)
	}
	bulkProduce(t, cl, "many", 0, 60, vals("m"), time.Time{})
	bulkProduce(t, cl, "many", 1, 60, vals("m"), time.Time{})
	sv = search("many", 2, "m")
	if len(sv.Rows) != 100 || !strings.HasSuffix(sv.Note, " Showing the newest 100.") || !strings.Contains(sv.Note, "120 matches") {
		t.Errorf("120 matches: %d rows, %q", len(sv.Rows), sv.Note)
	}

	// A topic of more partitions than one search covers.
	sv = search("fifty", 51, "")
	if !strings.Contains(sv.Note, "across 50 partitions") || !strings.HasSuffix(sv.Note, " The topic has 51 partitions; the first 50 were searched.") {
		t.Errorf("51 partitions: %q", sv.Note)
	}
	if strings.Contains(search("few", 2, "").Note, "The topic has") {
		t.Error("a topic of two partitions says some were left out")
	}
}

func TestSearchParams(t *testing.T) {
	for _, tc := range []struct {
		query  string
		q      string
		all    bool
		before int64
		asked  bool
	}{
		{"", "", false, -1, false},
		{"q=+Foo+", "Foo", false, -1, true},
		{"all=1", "", true, -1, true},
		{"all=2", "", false, -1, false},
		{"q=x&before=0", "x", false, 0, true},
		{"q=x&before=7", "x", false, 7, true},
		{"q=x&before=-3", "x", false, -1, true},
		{"q=x&before=zz", "x", false, -1, true},
	} {
		q, all, before, asked := searchParams(httptest.NewRequest("GET", "/x?"+tc.query, nil))
		if q != tc.q || all != tc.all || before != tc.before || asked != tc.asked {
			t.Errorf("%q: got %q %v %d %v; want %q %v %d %v", tc.query, q, all, before, asked, tc.q, tc.all, tc.before, tc.asked)
		}
	}
}
