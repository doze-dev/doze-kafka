package console

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A recorder that holds an unbounded number of requests says so in words.
type unboundedTraffic struct{ *fakeTraffic }

func (unboundedTraffic) Capacity() int { return 0 }

func TestRingTextWithoutAKnownCapacity(t *testing.T) {
	now := time.Now()
	src := unboundedTraffic{&fakeTraffic{events: []TrafficEvent{{ID: "r", Seq: 1, First: now.Add(-5 * time.Second), Time: now, Count: 1, API: "Fetch"}}}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	_, long := c.ringText(now)
	if !strings.Contains(long, "counted from the recent requests the broker keeps in memory.") || strings.Contains(long, "the last 0 requests") {
		t.Errorf("%s", long)
	}
	d := trafficDataOf(t, c, "")
	if d["KeptText"] != "few thousand requests" {
		t.Errorf("kept %v", d["KeptText"])
	}
	with, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = &fakeTraffic{} })
	if d = trafficDataOf(t, with, ""); d["KeptText"] != "2,000" {
		t.Errorf("kept %v", d["KeptText"])
	}
}

func TestRefusalsOfOrdersManyClientsByCountThenName(t *testing.T) {
	t0 := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	var rows []TrafficEvent
	for i, n := range []int{1, 5, 3, 4, 2, 6} {
		rows = append(rows, TrafficEvent{ID: fmt.Sprint(i), Error: "E", Class: "refused", Time: t0, ClientID: fmt.Sprintf("client-%d", n), Count: n})
	}
	got := refusalsOf(rows)
	var order []int
	for _, c := range got[0].Clients {
		order = append(order, c.Count)
	}
	if fmt.Sprint(order) != "[6 5 4 3 2 1]" {
		t.Errorf("clients by count: %v", order)
	}
	if got[0].Count != 21 {
		t.Errorf("count %d", got[0].Count)
	}
	// Two groups of refusals: the failed ones are counted separately.
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "a", Seq: 1, First: t0, Time: t0, Count: 1, API: "Fetch", Class: "failed", Error: "F1", ClientID: "x"},
		{ID: "b", Seq: 2, First: t0, Time: t0, Count: 1, API: "Fetch", Class: "failed", Error: "F2", ClientID: "x"},
		{ID: "c", Seq: 3, First: t0, Time: t0, Count: 1, API: "Fetch", Class: "refused", Error: "R1", ClientID: "x"},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	d := trafficDataOf(t, c, "")
	if d["FailedCount"] != 2 || d["RefusalCount"] != 3 {
		t.Errorf("failed %v of %v", d["FailedCount"], d["RefusalCount"])
	}
}

func TestTrafficDataClientMetricsFlags(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{clients: []TrafficClient{
		{Conn: 1, ClientID: "reports", Remote: "r:1", Opened: now, Last: now},
		{Conn: 2, ClientID: "silent", Remote: "r:2", Opened: now, Last: now},
	}}
	metrics := fakeMetrics{instances: []ClientInstance{{ID: "i", ClientID: "reports", Conn: 1, Pushes: 2, First: now, Last: now}}}
	with, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src; o.ClientMetrics = metrics })
	d := trafficDataOf(t, with, "")
	if d["HasClientMetrics"] != true || d["MetricsOn"] != true {
		t.Errorf("flags %v %v", d["HasClientMetrics"], d["MetricsOn"])
	}
	flags := map[string]bool{}
	for _, r := range d["Clients"].([]clientRow) {
		flags[r.Who] = r.Metrics
	}
	if !flags["reports"] || flags["silent"] {
		t.Errorf("metrics flags %v", flags)
	}
	without, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	d = trafficDataOf(t, without, "")
	if d["HasClientMetrics"] != false || d["MetricsOn"] != false {
		t.Errorf("flags %v %v", d["HasClientMetrics"], d["MetricsOn"])
	}
	for _, r := range d["Clients"].([]clientRow) {
		if r.Metrics {
			t.Errorf("%s reports with no source", r.Who)
		}
	}
}

func TestTrafficRowTopicParts(t *testing.T) {
	c := newTestConsole(t)
	for name, tc := range map[string]struct {
		e    TrafficEvent
		view trafficView
		want string
	}{
		"one topic with partitions":  {TrafficEvent{Topics: []string{"t"}, Parts: "p0–p2"}, trafficView{}, "t p0–p2"},
		"one topic without":          {TrafficEvent{Topics: []string{"t"}}, trafficView{}, "t"},
		"two topics with partitions": {TrafficEvent{Topics: []string{"a", "b"}, Parts: "p0"}, trafficView{}, "a, b p0"},
		"two topics without":         {TrafficEvent{Topics: []string{"a", "b"}}, trafficView{}, "a, b"},
		"no topic":                   {TrafficEvent{Parts: "p3"}, trafficView{}, "p3"},
	} {
		if got := c.trafficRow(tc.e, tc.view).TopicParts; got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
	// Beside a topic filter, a request that names no topic is still about what it is about.
	row := c.trafficRow(TrafficEvent{API: "Heartbeat", Group: "g"}, trafficView{TrafficFilter: TrafficFilter{Topic: "t"}})
	if row.Target != "g" {
		t.Errorf("target %q", row.Target)
	}
}

func TestSpanBoardAndShortDurationBoundaries(t *testing.T) {
	if got := spanBoard(time.Hour - time.Nanosecond); got != "59 m 59 s" {
		t.Errorf("%q", got)
	}
	if got := spanBoard(time.Hour); got != "1 h 00 m" {
		t.Errorf("%q", got)
	}
	if got := shortDuration(0); got != "0 ms" {
		t.Errorf("%q", got)
	}
}

func TestErrorSummaryWithoutAKafkaMajorAndWithoutTopics(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "invoices", 1, 0)
	ctx := context.Background()
	v := c.errorSummary(ctx, []TrafficRefusal{{Code: 3, Name: "UNKNOWN_TOPIC_OR_PARTITION", Class: "refused", API: "Produce", Topics: []string{"invoicez"}, Clients: []TrafficClientCount{{"a", 1}}}})
	if strings.Contains(v.Fix, "auto-create is") {
		t.Errorf("a broker that did not say what it declares is told about auto-create: %q", v.Fix)
	}
	c.facts.KafkaMajor = 4
	if v = c.errorSummary(ctx, []TrafficRefusal{{Code: 3, Name: "UNKNOWN_TOPIC_OR_PARTITION", Class: "refused", API: "Produce", Topics: []string{"invoicez"}}}); !strings.Contains(v.Fix, "auto-create is off") {
		t.Errorf("%q", v.Fix)
	}
	// An unknown topic named in no row: nothing to create, and nothing to index.
	v = c.errorSummary(ctx, []TrafficRefusal{{Code: 3, Name: "UNKNOWN_TOPIC_OR_PARTITION", Class: "refused", API: "Produce"}})
	if v.Said != "a client sent Produce." || v.Create != "" || v.CreateURL != "" || v.TopicCount != 0 {
		t.Errorf("%+v", v)
	}
	v = c.errorSummary(ctx, []TrafficRefusal{{Code: 3, Name: "UNKNOWN_TOPIC_OR_PARTITION", Class: "refused", API: "Fetch"}})
	if v.Said != "a client sent Fetch." || v.Create != "" {
		t.Errorf("%+v", v)
	}
	// The first of equally near names wins.
	if got, ok := nearestTopic("aac", []TopicSummary{{Name: "aaa"}, {Name: "aab"}, {Name: "aad"}}); !ok || got != "aaa" {
		t.Errorf("nearest %q %v", got, ok)
	}
}

// A record right after the end of a range, with a transaction's marker between, is not in the range.
func TestScanRangesStopsAtTheRangeEndAcrossAMarker(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "mk", 1, nil); err != nil {
		t.Fatal(err)
	}
	p := txnProducer(t, addr, "scan-marker")
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := p.ProduceSync(ctx, &kgo.Record{Topic: "mk", Partition: 0, Value: []byte("in"), Headers: tp(traceA)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := p.EndTransaction(ctx, kgo.TryCommit); err != nil { // offsets 0 (record) and 1 (marker).
		t.Fatal(err)
	}
	cl := rawClient(t, addr)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "mk", Partition: 0, Value: []byte("after"), Headers: tp(traceA)}).FirstErr(); err != nil { // offset 2.
		t.Fatal(err)
	}
	hits, _, _, err := c.scanRanges(ctx, &traceJob{trace: traceA}, []partRange{{tpKey{"mk", 0}, 0, 2}})
	if err != nil || len(hits) != 1 || hits[0].Offset != 0 {
		t.Errorf("%d hits (%v), want only offset 0: %v", len(hits), hits, err)
	}
}

// A partition read to its end is not listed as unread when the time runs out elsewhere.
func TestAFullyReadPartitionIsNotUnreadAtATimeout(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for _, n := range []string{"done", "gap"} {
		if err := c.be.CreateTopic(ctx, n, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	recs := func(topic string, n int) []*kgo.Record {
		out := make([]*kgo.Record, n)
		for i := range out {
			out[i] = &kgo.Record{Topic: topic, Value: []byte("v"), Headers: tp(traceA)}
		}
		return out
	}
	if err := cl.ProduceSync(ctx, append(recs("done", 1), recs("gap", 5)...)...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	old := traceMaxTime
	traceMaxTime = 900 * time.Millisecond
	defer func() { traceMaxTime = old }()
	_, timedOut, unread, err := c.scanRanges(ctx, &traceJob{trace: traceA}, []partRange{{tpKey{"done", 0}, 0, 1}, {tpKey{"gap", 0}, 0, 100}})
	if err != nil || !timedOut {
		t.Fatalf("timedOut %v err %v", timedOut, err)
	}
	if len(unread) != 1 || unread[0] != "gap partition 0: 95 of 100 records" {
		t.Errorf("unread %q", unread)
	}
}

func TestOffsetsAtAsksAsAConsumer(t *testing.T) {
	c, sn := newSniffedConsole(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "oa", 3, nil); err != nil {
		t.Fatal(err)
	}
	topics := []TopicSummary{{Name: "oa", Partitions: 3}}
	got, err := c.be.offsetsAt(ctx, topics, -1)
	if err != nil || len(got) != 3 {
		t.Fatalf("%v %v", got, err)
	}
	rs := listOffsetsSince(sn, 0)
	last := rs[len(rs)-1]
	if last.ReplicaID != -1 || len(timestampsAsked(last)) != 3 {
		t.Errorf("replica %d, asked %v", last.ReplicaID, timestampsAsked(last))
	}
	for k, ts := range timestampsAsked(last) {
		if len(ts) != 1 || ts[0] != -1 {
			t.Errorf("%s asked for %v", k, ts)
		}
	}
}

func TestTraceDataBudgetAndTheTraceHomeFollowHint(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	d := c.traceData(&traceJob{id: "j", phase: "done"})
	if d["MaxSeconds"] != 8 || d["MaxRecords"] != "50,000" {
		t.Errorf("%v %v", d["MaxSeconds"], d["MaxRecords"])
	}
	topicWith(t, c, "untraced", 1, 3)
	if page := readable(req(t, c, "GET", "/_console/trace", nil).Body.String()); strings.Contains(page, "follow") {
		t.Errorf("records without a trace id offer to follow one")
	}
	if err := c.be.CreateTopic(context.Background(), "traced", 1, nil); err != nil {
		t.Fatal(err)
	}
	writeAll(t, addr, []sent{{"traced", time.Now(), tp(traceA), "k"}})
	if page := readable(req(t, c, "GET", "/_console/trace", nil).Body.String()); !strings.Contains(page, "follow") {
		t.Errorf("a record with a trace id does not offer to follow it")
	}
}

func TestTraceRecentIsTheNewestFewOfEachPartition(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	for _, n := range []string{"tr-a", "tr-empty"} {
		if err := c.be.CreateTopic(ctx, n, 2, nil); err != nil {
			t.Fatal(err)
		}
	}
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	var recs []sent
	for i := 0; i < 20; i++ {
		var h []kgo.RecordHeader
		if i%2 == 0 {
			h = tp(traceA)
		}
		recs = append(recs, sent{"tr-a", base.Add(time.Duration(i) * time.Second), h, fmt.Sprintf("k%02d", i)})
	}
	writeAll(t, addr, recs)
	rows, err := c.traceRecent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("%d rows, want the newest 5 of the one partition with records", len(rows))
	}
	for i, r := range rows {
		if r.Offset != int64(19-i) || r.Topic != "tr-a" {
			t.Errorf("row %d is %s/%d, want offset %d, newest first", i, r.Topic, r.Offset, 19-i)
		}
		if traced := r.Offset%2 == 0; traced != (r.Trace != "") || traced != (r.URL != "") {
			t.Errorf("offset %d: trace %q url %q", r.Offset, r.Trace, r.URL)
		}
	}
	// Eight at most, newest of all of them.
	many := 12
	var more []sent
	for i := 0; i < many; i++ {
		more = append(more, sent{"tr-empty", base.Add(time.Duration(100+i) * time.Second), nil, "x"})
	}
	writeAll(t, addr, more)
	rows, err = c.traceRecent(ctx)
	if err != nil || len(rows) != 8 || rows[0].Topic != "tr-empty" {
		t.Errorf("%d rows (%v), first %v", len(rows), err, rows[0])
	}
}

func TestTracePageSaysHowWideItLooked(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders", 1, 0)
	now := time.Now().Truncate(time.Millisecond)
	writeAll(t, addr, []sent{{"orders", now.Add(-time.Second), tp(traceA), "k"}})
	for window, label := range map[string]string{"1m": "1 minute", "5m": "5 minutes", "30m": "30 minutes", "3h": "3 hours"} {
		body := readable(waitTrace(t, c, traceQuery(traceA, now, map[string][]string{"window": {window}})))
		if !strings.Contains(body, "written within "+label+" of") {
			t.Errorf("window %s:\n%s", window, between(body, "Every record", "."))
		}
	}
}

func TestClusterRailAndMetaSize(t *testing.T) {
	dir := t.TempDir()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Facts.DataDir = dir })
	rail := between(req(t, c, "GET", "/_console/cluster", nil).Body.String(), `aria-label="Cluster pages"`, "</aside>")
	if strings.Contains(rail, "open</span>") {
		t.Errorf("a transaction count with none open:\n%s", rail)
	}
	page := func() string { return readable(req(t, c, "GET", "/_console/cluster", nil).Body.String()) }
	if strings.Contains(page(), "meta.db") {
		t.Error("meta.db without a file")
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.db"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if p := page(); !strings.Contains(p, "meta.db 0 B") {
		t.Errorf("an empty meta.db:\n%s", between(p, "Data directory", "Console"))
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.db"), make([]byte, 2048), 0o644); err != nil {
		t.Fatal(err)
	}
	if p := page(); !strings.Contains(p, "meta.db 2 KiB") {
		t.Errorf("meta.db of 2 KiB:\n%s", between(p, "Data directory", "Console"))
	}
}

func TestMemberWithoutAClientIDIsNamedByItsID(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "anon", 1, nil); err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup("anon-g"), kgo.ConsumeTopics("anon"), kgo.ClientID(""))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	go func() {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		cl.PollFetches(pctx)
	}()
	deadline := time.Now().Add(8 * time.Second)
	for {
		d, err := c.be.DescribeGroup(ctx, "anon-g")
		if err == nil && len(d.Members) == 1 {
			m := d.Members[0]
			if m.ClientID != "" {
				t.Skipf("the client sent the id %q anyway", m.ClientID)
			}
			label := shortID(m.ID)
			body := readable(req(t, c, "GET", "/_console/inspect/member?key="+joinKey("anon-g", m.ID), nil).Body.String())
			if !strings.Contains(body, label) {
				t.Errorf("the member is not named by its short id %q:\n%s", label, body)
			}
			data := groupPage(t, c, "anon-g", "")
			if ms := data["Members"].([]memberView); len(ms) != 1 || ms[0].Label != label {
				t.Errorf("group page labels %+v", ms)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the member never showed")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
