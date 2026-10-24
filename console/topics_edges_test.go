package console

import (
	"context"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func topicsHome(t *testing.T, c *Console) map[string]any {
	t.Helper()
	data, err := c.topicsHomeData(httptest.NewRequest("GET", "/_console/topics", nil))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// The Topics page's totals: what it counts, and the dead-letter topics it names.
func TestTopicsHomeCountsAndNamesWhatNeedsALook(t *testing.T) {
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "r1", Seq: 1, API: "Produce", ClientID: "writer-a", Topics: []string{"plain"}},
		{ID: "r2", Seq: 2, API: "Produce", ClientID: "writer-a", Topics: []string{"plain"}},
		{ID: "r3", Seq: 3, API: "Produce", Remote: "10.0.0.7:5555", Topics: []string{"plain"}},
		{ID: "r4", Seq: 4, API: "Produce", ClientID: "no-topics"},
		{ID: "r5", Seq: 5, API: "Fetch", ClientID: "reader", Topics: []string{"plain"}},
	}}
	c, addr := newTestConsoleAddr(t, func(o *Options) {
		o.Traffic = src
		o.Facts.KafkaMajor = 4
		o.Facts.AutoCreateTopics = true
		o.Facts.DefaultPartitions = 3
	})
	ctx := context.Background()
	cl := rawClient(t, addr)
	if data := topicsHome(t, c); data["Facts"].(connectFacts).Bootstrap == "" || data["Command"] == "" {
		t.Errorf("an empty broker shows no create command: %v", data["Command"])
	}
	for name, cfg := range map[string]map[string]string{"c1": {"cleanup.policy": "compact"}, "c2": {"cleanup.policy": "compact"}, "orders.dlq": nil, "events.dlq": nil, "plain": nil} {
		if err := c.be.CreateTopic(ctx, name, 2, cfg); err != nil {
			t.Fatal(err)
		}
	}
	bulkProduce(t, cl, "orders.dlq", 0, 3, vals("x"), time.Time{})
	bulkProduce(t, cl, "events.dlq", 1, 2, vals("x"), time.Time{})
	bulkProduce(t, cl, "plain", 0, 4, vals("x"), time.Time{})
	commitFor(t, addr, "dlq-reader", "events.dlq", map[int32]int64{0: 0})
	commitFor(t, addr, "plain-reader", "plain", map[int32]int64{0: 0})

	data := topicsHome(t, c)
	if data["Partitions"] != 10 || data["Messages"].(int64) != 9 || data["ReaderCount"] != 2 {
		t.Errorf("partitions %v, messages %v, readers %v; want 10, 9, 2", data["Partitions"], data["Messages"], data["ReaderCount"])
	}
	if data["Compacted"] != 2 || data["DeadLetters"] != 2 {
		t.Errorf("compacted %v, dead letters %v; want 2 and 2", data["Compacted"], data["DeadLetters"])
	}
	if got := strings.Join(data["DeadNames"].([]string), ","); got != "events.dlq,orders.dlq" {
		t.Errorf("dead-letter topics %q", got)
	}
	if got := strings.Join(data["DeadUnread"].([]string), ","); got != "orders.dlq" {
		t.Errorf("unread dead-letter topics %q (events.dlq has a reader)", got)
	}
	if data["Writers"] != 2 || data["HasWriters"] != true {
		t.Errorf("writers %v: the producer named twice, the one with no name, and the one with no topic and the fetcher left out", data["Writers"])
	}
	if data["KnowsAuto"] != true || data["AutoCreate"] != true || data["AutoParts"] != 3 {
		t.Errorf("auto-create: %v %v %v", data["KnowsAuto"], data["AutoCreate"], data["AutoParts"])
	}
	if f := data["Facts"].(connectFacts); f.Bootstrap != "" || f.DataDir == "x" {
		t.Errorf("a broker with topics carries connection facts: %+v", f)
	}
	if _, ok := data["Command"]; ok {
		t.Error("a broker with topics shows the first create command")
	}

	// A console without a recorder or the Kafka major says neither.
	plain := newTestConsole(t)
	if d := topicsHome(t, plain); d["KnowsAuto"] != false || d["HasWriters"] != nil {
		t.Errorf("without facts: KnowsAuto %v, HasWriters %v", d["KnowsAuto"], d["HasWriters"])
	}
}

func TestTopicTabLinks(t *testing.T) {
	c := newTestConsole(t)
	tabs := c.topicTabLinks("orders", 0, "messages", false, 3, 2)
	var got []string
	for _, tb := range tabs {
		got = append(got, tb.Key+"|"+tb.URL+"|"+tb.Count+"|"+map[bool]string{true: "on", false: "off"}[tb.On])
	}
	want := "messages|/_console/topics/orders||on partitions|/_console/topics/orders?tab=partitions|3|off readers|/_console/topics/orders?tab=readers|2|off config|/_console/topics/orders?tab=config||off"
	if strings.Join(got, " ") != want {
		t.Errorf("tabs\n got %s\nwant %s", strings.Join(got, " "), want)
	}
	tabs = c.topicTabLinks("orders", 2, "readers", true, 3, 2)
	if len(tabs) != 5 || tabs[4].Key != "schema" || tabs[0].URL != "/_console/topics/orders?partition=2" || tabs[0].On || !tabs[2].On {
		t.Errorf("on partition 2 with a schema: %+v", tabs)
	}
}

// Which tab a topic's page opens on.
func TestTopicPageChoosesItsTab(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "tabbed", 2, 1)
	for query, want := range map[string]string{"": "messages", "?tab=partitions": "partitions", "?tab=readers": "readers", "?tab=config": "config", "?tab=schema": "messages", "?tab=nope": "messages"} {
		body := req(t, c, "GET", "/_console/topics/tabbed"+query, nil).Body.String()
		var on []string
		for _, m := range tabOnRe.FindAllStringSubmatch(between(body, `aria-label="Topic views"`, "</nav>"), -1) {
			on = append(on, m[1])
		}
		if len(on) != 1 || !strings.EqualFold(on[0], want) {
			t.Errorf("%q: tab on %v, want %s", query, on, want)
		}
	}
}

// The Partitions tab: where each log starts, ends and may be read to.
func TestPartitionRowsShowTheStableOffset(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "pr", 3, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "pr", 0, 5, vals("x"), time.Time{})
	deleteBefore(t, cl, "pr", 0, 2)
	p := txnProducer(t, addr, "holder")
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := p.ProduceSync(ctx, &kgo.Record{Topic: "pr", Partition: 1, Value: []byte("open")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	bounds, err := c.be.PartitionBounds(ctx, "pr", 3)
	if err != nil {
		t.Fatal(err)
	}
	rows := c.partitionRows(ctx, "pr", bounds)
	if len(rows) != 3 {
		t.Fatalf("%d rows", len(rows))
	}
	if r := rows[0]; r.Start != 2 || r.End != 5 || r.Count != 3 || r.Stable != 5 || r.Held || !r.HasNewest {
		t.Errorf("partition 0: %+v", r)
	}
	if r := rows[1]; r.End != 1 || r.Stable != 0 || !r.Held || r.Count != 1 {
		t.Errorf("partition 1 holds an open transaction: %+v", r)
	}
	if r := rows[2]; r.End != 0 || r.Stable != 0 || r.Held || r.HasNewest {
		t.Errorf("empty partition 2: %+v", r)
	}
}

// The Readers tab: each group's lag on each partition, -1 where it has no offset.
func TestTopicReadersLagPerPartition(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "rd", 3, nil); err != nil {
		t.Fatal(err)
	}
	for p := int32(0); p < 3; p++ {
		bulkProduce(t, cl, "rd", p, 10, vals("x"), time.Time{})
	}
	commitFor(t, addr, "a", "rd", map[int32]int64{0: 4, 2: 10})
	commitFor(t, addr, "past", "rd", map[int32]int64{1: 40})
	if err := c.be.CreateTopic(ctx, "other", 1, nil); err != nil {
		t.Fatal(err)
	}
	commitFor(t, addr, "elsewhere", "other", map[int32]int64{0: 1})
	bounds, err := c.be.PartitionBounds(ctx, "rd", 3)
	if err != nil {
		t.Fatal(err)
	}
	rows := c.topicReaders(ctx, "rd", bounds)
	by := map[string]readerRow{}
	for _, r := range rows {
		by[r.Group] = r
	}
	if len(rows) != 2 {
		t.Fatalf("readers %+v", rows)
	}
	if r := by["a"]; r.Total != 6 || len(r.PartLag) != 3 || r.PartLag[0] != 6 || r.PartLag[1] != -1 || r.PartLag[2] != 0 || r.Protocol != "classic" || r.URL != "/_console/groups/a" {
		t.Errorf("a: %+v", r)
	}
	if r := by["past"]; r.Total != 0 || r.PartLag[1] != 0 || r.PartLag[0] != -1 {
		t.Errorf("a group past the end: %+v", r)
	}
}

func TestTopicSummaryRetentionAndNewest(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for name, cfg := range map[string]map[string]string{
		"s-compact": {"cleanup.policy": "compact"}, "s-both": {"cleanup.policy": "compact,delete", "retention.ms": "7200000"}, "s-plain": {"retention.ms": "604800000"},
	} {
		if err := c.be.CreateTopic(ctx, name, 2, cfg); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	bulkProduce(t, cl, "s-plain", 0, 3, vals("x"), at)
	bulkProduce(t, cl, "s-plain", 1, 2, vals("y"), at.Add(10*time.Second))
	summary := func(name string) topicFactsView {
		d, err := c.be.DescribeTopic(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		bounds, err := c.be.PartitionBounds(ctx, name, d.Partitions)
		if err != nil {
			t.Fatal(err)
		}
		return c.topicSummary(ctx, d, bounds)
	}
	if f := summary("s-compact"); !f.Compact || f.Retention != "per key" || f.Messages != 0 || f.HasNewest || f.Partitions != 2 {
		t.Errorf("compact: %+v", f)
	}
	if f := summary("s-both"); !f.Compact || f.Retention != "2 h" {
		t.Errorf("compact and delete: %+v", f)
	}
	f := summary("s-plain")
	if f.Compact || f.Retention != "7 d" || f.Messages != 5 || !f.HasNewest || !f.Newest.Equal(at.Add(10*time.Second+time.Millisecond)) {
		t.Errorf("plain: %+v (newest %v, want %v)", f, f.Newest, at.Add(10*time.Second+time.Millisecond))
	}
}

var tabOnRe = regexp.MustCompile(`<a class="tab on" href="[^"]*" aria-current="page">([A-Za-z]+)`)
