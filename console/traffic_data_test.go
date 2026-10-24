package console

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func trafficDataOf(t *testing.T, c *Console, query string) map[string]any {
	t.Helper()
	return c.trafficData(httptest.NewRequest("GET", "/_console/traffic?"+query, nil))
}

func TestRefusalsOfGroupsErrorsMostOftenFirst(t *testing.T) {
	t0 := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	rows := []TrafficEvent{
		{ID: "a", Error: "OLD", Class: "refused", ErrorCode: 1, Time: t0, ClientID: "x", Count: 1, API: "Fetch", Topics: []string{"t1"}},
		{ID: "b", Error: "MANY", Class: "failed", ErrorCode: 2, Time: t0.Add(time.Second), ClientID: "x", Count: 3, API: "Produce"},
		{ID: "c", Error: "MANY", Class: "failed", ErrorCode: 2, Time: t0.Add(-time.Minute), Remote: "r:1", API: "Fetch"},
		{ID: "d", Error: "NEW", Class: "denied", ErrorCode: 3, Time: t0.Add(2 * time.Second), ClientID: "y", API: "Metadata", Topics: []string{"t2"}},
		{ID: "e", Error: "NEW", Class: "denied", ErrorCode: 3, Time: t0.Add(time.Second), ClientID: "z", Count: 0},
		{ID: "f", Error: "SKIPPED", Class: "expected", Time: t0},
		{ID: "g", Error: "", Class: "refused", Time: t0},
		{ID: "h", Error: "NOCLASS", Class: "", Time: t0},
	}
	got := refusalsOf(rows)
	if len(got) != 3 {
		t.Fatalf("%d groups: %+v", len(got), got)
	}
	// MANY has 4 rows' worth; NEW and OLD have 2 and 1.
	if got[0].Name != "MANY" || got[0].Count != 4 || got[1].Name != "NEW" || got[1].Count != 2 || got[2].Name != "OLD" || got[2].Count != 1 {
		t.Errorf("order: %s %d, %s %d, %s %d", got[0].Name, got[0].Count, got[1].Name, got[1].Count, got[2].Name, got[2].Count)
	}
	many := got[0]
	if !many.First.Equal(t0.Add(-time.Minute)) || !many.Last.Equal(t0.Add(time.Second)) || many.Latest != "b" || many.API != "Produce" || many.Code != 2 || many.Class != "failed" {
		t.Errorf("MANY: %+v", many)
	}
	if len(many.Clients) != 2 || many.Clients[0].Client != "x" || many.Clients[0].Count != 3 || many.Clients[1].Client != "r:1" || many.Clients[1].Count != 1 {
		t.Errorf("MANY clients: %+v", many.Clients)
	}
	if nw := got[1]; nw.Latest != "d" || len(nw.Topics) != 1 || nw.Topics[0] != "t2" || nw.Clients[0].Client != "y" || nw.Clients[1].Client != "z" {
		t.Errorf("NEW (clients tied on count go alphabetically): %+v", nw)
	}
	// Equal counts: the newest first.
	tied := refusalsOf([]TrafficEvent{
		{ID: "1", Error: "A", Class: "refused", Time: t0}, {ID: "2", Error: "B", Class: "refused", Time: t0.Add(time.Hour)},
	})
	if tied[0].Name != "B" || tied[1].Name != "A" {
		t.Errorf("a tie: %s, %s", tied[0].Name, tied[1].Name)
	}
}

func TestClientsNoteNamesTheOneWithErrorsThatNeedsNoLook(t *testing.T) {
	base := "Error rate is the requests refused, denied, not served or failed over all requests in the last 5 minutes."
	mk := func(who string, err, req int, look bool, last string) clientRow {
		return clientRow{Who: who, NeedsALook: look, TrafficClient: TrafficClient{Err5m: err, Req5m: req, LastError: last}}
	}
	if got := clientsNote(nil); got != base {
		t.Errorf("%q", got)
	}
	got := clientsNote([]clientRow{mk("clean", 0, 50, false, ""), mk("raised", 9, 10, true, "X"), mk("one", 1, 1500, false, "UNKNOWN_TOPIC"), mk("second", 2, 10, false, "")})
	if want := base + " one shows 1 error among 1,500 (the latest UNKNOWN_TOPIC)."; got != want {
		t.Errorf("\n got %q\nwant %q", got, want)
	}
	if got = clientsNote([]clientRow{mk("a", 2, 10, false, "")}); got != base+" a shows 2 errors among 10." {
		t.Errorf("%q", got)
	}
}

func TestCountErrorsLeavesOutTheExpectedOnes(t *testing.T) {
	c := newTestConsole(t)
	rows := []trafficRow{{TrafficEvent: TrafficEvent{Error: "A", Class: "refused"}}, {TrafficEvent: TrafficEvent{Error: "B", Class: "expected"}}, {TrafficEvent: TrafficEvent{}}, {TrafficEvent: TrafficEvent{Error: "C", Class: "failed"}}}
	if n := c.countErrors(rows); n != 2 {
		t.Errorf("%d", n)
	}
}

func TestMsTextIsMillisecondsToTheMicrosecond(t *testing.T) {
	for in, want := range map[int64]string{1792148375593043: "1792148375593.043", 1000000: "1000.000", 1000: "1.000", 5: "0.005", 0: "0.000", 123456789: "123456.789"} {
		if got := msText(time.UnixMicro(in)); got != want {
			t.Errorf("msText(%d) = %q, want %q", in, got, want)
		}
	}
}

// The Traffic page's numbers and links for the requests view.
func TestTrafficDataRequestsView(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{
		events: []TrafficEvent{
			{ID: "r3", Seq: 3, Conn: 1, First: now, Time: now, Count: 4, ClientID: "a", API: "Produce", Topics: []string{"orders"}},
			{ID: "r2", Seq: 2, Conn: 9, First: now.Add(-time.Second), Time: now.Add(-time.Second), Count: 1, ClientID: "b", API: "Fetch", Topics: []string{"orders"}, Class: "failed", Error: "BOOM", ErrorCode: 5},
			{ID: "r1", Seq: 1, Conn: 1, First: now.Add(-2 * time.Second), Time: now.Add(-2 * time.Second), Count: 1, ClientID: "a", API: "Fetch", Class: "refused", Error: "NOPE", ErrorCode: 6},
			{ID: "r0", Seq: 0, Conn: 1, First: now.Add(-3 * time.Second), Time: now.Add(-3 * time.Second), Count: 1, ClientID: "a", API: "Heartbeat", Class: "expected", Error: "REBALANCE_IN_PROGRESS"},
		},
		clients: []TrafficClient{
			{Conn: 1, ClientID: "a", Remote: "r:1", Opened: now.Add(-time.Hour), Last: now, ClientStreak: 3, ClientLastClass: "refused", Requests: 6},
			{Conn: 2, ClientID: "a", Remote: "r:2", Opened: now.Add(-time.Hour), Last: now},
			{Conn: 3, ClientID: "c", Remote: "r:3", Opened: now.Add(-time.Hour), Last: now},
		},
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	d := trafficDataOf(t, c, "")
	if d["ClientCount"] != 2 || d["ConnCount"] != 3 || d["NeedLook"] != 1 {
		t.Errorf("clients %v conns %v need a look %v; want 2, 3, 1", d["ClientCount"], d["ConnCount"], d["NeedLook"])
	}
	rows := d["Rows"].([]trafficRow)
	if len(rows) != 4 || d["Shown"] != 4 || d["RowCount"] != 5 || d["Cut"] != false || d["MatchedRows"] != 4 || d["Matched"] != 7 || d["TotalRequests"] != 7 {
		t.Errorf("rows %d shown %v rowcount %v cut %v matched %v/%v of %v", len(rows), d["Shown"], d["RowCount"], d["Cut"], d["MatchedRows"], d["Matched"], d["TotalRequests"])
	}
	if rows[0].ConnKey != "1" || rows[1].ConnKey != "" || rows[2].ConnKey != "1" {
		t.Errorf("a row's connection is kept only while it is open: %q %q %q", rows[0].ConnKey, rows[1].ConnKey, rows[2].ConnKey)
	}
	if d["RefusalCount"] != 2 || d["FailedCount"] != 1 || d["RefusedMatching"] != 2 {
		t.Errorf("refusals %v failed %v matching %v", d["RefusalCount"], d["FailedCount"], d["RefusedMatching"])
	}
	if d["InspDefault"] != "traffic" || d["FootPad"] != true || d["Filtered"] != false || d["ClearURL"] != "/_console/traffic" || d["FieldValue"] != "" || d["KindWord"] != "all" || d["HasPills"] != false {
		t.Errorf("unfiltered: %v %v %v %v %v", d["InspDefault"], d["FootPad"], d["Filtered"], d["ClearURL"], d["KindWord"])
	}
	if d["KeptText"] != "2,000" || d["TView"] != "requests" || d["TabRequests"] != "/_console/traffic" || d["TabClients"] != "/_console/traffic?view=clients" {
		t.Errorf("kept %v view %v", d["KeptText"], d["TView"])
	}
	if !d["Newest"].(time.Time).Equal(now) || d["NewestMs"] != msText(now) {
		t.Errorf("newest %v", d["Newest"])
	}
	h1 := d["Hash"]
	src.events = src.events[1:]
	if trafficDataOf(t, c, "")["Hash"] == h1 {
		t.Error("the hash does not move when a row goes")
	}
}

func TestTrafficDataFiltersAndTheirInspectorDefault(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "r2", Seq: 2, Conn: 1, First: now, Time: now, Count: 1, ClientID: "a", API: "Fetch", Topics: []string{"orders"}, Group: "g", Class: "failed", Error: "BOOM"},
		{ID: "r1", Seq: 1, Conn: 1, First: now, Time: now, Count: 1, ClientID: "a", API: "OffsetCommit", Group: "g", Topics: []string{"orders"}},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	for _, tc := range []struct {
		query       string
		insp        string
		footpad     bool
		clear       string
		field       string
		filtered    bool
		kindWord    string
		flat, story bool
	}{
		{"", "traffic", true, "/_console/traffic", "", false, "all", false, false},
		{"topic=orders", "touched", true, "/_console/traffic", "orders", true, "all", false, false},
		{"group=g", "story", false, "/_console/traffic", "g", true, "all", false, true},
		{"group=g&flat=1", "traffic", true, "/_console/traffic", "g", true, "all", true, false},
		{"kind=errors", "errors", false, "/_console/traffic?kind=errors", "", true, "errors", false, false},
		{"errors=1", "errors", false, "/_console/traffic", "", true, "errors", false, false},
		{"kind=fetch", "traffic", true, "/_console/traffic?kind=fetch", "", true, "fetch", false, false},
		{"kind=produce&group=g", "story", false, "/_console/traffic", "g", true, "produce", false, true},
		{"kind=produce&group=g&flat=1", "traffic", true, "/_console/traffic?kind=produce", "g", true, "produce", true, false},
		{"client=a", "traffic", true, "/_console/traffic", "", true, "all", false, false},
		{"q=boom", "traffic", true, "/_console/traffic", "boom", true, "all", false, false},
	} {
		d := trafficDataOf(t, c, tc.query)
		if d["InspDefault"] != tc.insp || d["FootPad"] != tc.footpad || d["ClearURL"] != tc.clear || d["FieldValue"] != tc.field || d["Filtered"] != tc.filtered || d["KindWord"] != tc.kindWord {
			t.Errorf("%q: insp %v footpad %v clear %v field %v filtered %v kind %v", tc.query, d["InspDefault"], d["FootPad"], d["ClearURL"], d["FieldValue"], d["Filtered"], d["KindWord"])
		}
		_, hasFlat := d["FlatURL"]
		if _, hasStory := d["Story"]; hasStory != tc.story || hasFlat != tc.story {
			t.Errorf("%q: story %v flat link %v, want %v", tc.query, hasStory, hasFlat, tc.story)
		}
	}
	d := trafficDataOf(t, c, "group=g&topic=orders&kind=fetch")
	if d["FlatURL"] != "/_console/traffic?topic=orders&group=g&kind=fetch&flat=1" {
		t.Errorf("flat link %v", d["FlatURL"])
	}
}

func TestTrafficDataIsCutAtTwoHundredRows(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{}
	for i := 250; i >= 1; i-- {
		src.events = append(src.events, TrafficEvent{ID: fmt.Sprintf("r%d", i), Seq: uint64(i), First: now, Time: now, Count: 1, API: "Fetch"})
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	d := trafficDataOf(t, c, "")
	if d["Shown"] != 200 || d["MatchedRows"] != 250 || d["Cut"] != true || d["RowCount"] != 201 {
		t.Errorf("shown %v matched %v cut %v rowcount %v", d["Shown"], d["MatchedRows"], d["Cut"], d["RowCount"])
	}
	src.events = src.events[:200]
	if d = trafficDataOf(t, c, ""); d["Shown"] != 200 || d["Cut"] != false {
		t.Errorf("exactly 200: cut %v", d["Cut"])
	}
}

func TestTrafficKindsAndPills(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = &fakeTraffic{} })
	d := trafficDataOf(t, c, "topic=orders&client=a&api=Fetch&kind=fetch&q=zz")
	kinds := d["Kinds"].([]map[string]any)
	var on []string
	for _, k := range kinds {
		if k["On"] == true {
			on = append(on, k["Key"].(string))
		}
	}
	if len(kinds) != 6 || len(on) != 1 || on[0] != "fetch" {
		t.Errorf("kinds on: %v of %d", on, len(kinds))
	}
	for _, k := range kinds {
		u := k["URL"].(string)
		if k["Key"] == "" && strings.Contains(u, "kind=") || k["Key"] != "" && !strings.Contains(u, "kind="+k["Key"].(string)) {
			t.Errorf("kind %q links to %s", k["Key"], u)
		}
		if !strings.Contains(u, "topic=orders") || !strings.Contains(u, "q=zz") {
			t.Errorf("kind %q forgets the filter: %s", k["Key"], u)
		}
	}
	if d["HasPills"] != true || d["KindWord"] != "fetch" {
		t.Errorf("pills %v kind %v", d["HasPills"], d["KindWord"])
	}
	pills := fmt.Sprint(d["Pills"])
	for _, want := range []string{"topic orders", "client a", "request Fetch"} {
		if !strings.Contains(pills, want) {
			t.Errorf("no pill %q in %s", want, pills)
		}
	}
	// A pill removes itself and leaves the rest.
	if !strings.Contains(pills, "/_console/traffic?client=a&api=Fetch&q=zz&kind=fetch") || !strings.Contains(pills, "/_console/traffic?api=Fetch&topic=orders&q=zz&kind=fetch") && !strings.Contains(pills, "client=a&topic=orders") {
		t.Errorf("pills do not remove themselves: %s", pills)
	}
	// Errors-only on its own lights the errors kind and is not carried into the others.
	d = trafficDataOf(t, c, "errors=1")
	for _, k := range d["Kinds"].([]map[string]any) {
		if k["Key"] == "errors" && k["On"] != true || strings.Contains(k["URL"].(string), "errors=1") {
			t.Errorf("errors=1: kind %v on=%v url=%v", k["Key"], k["On"], k["URL"])
		}
	}
	// A group's story is its group requests.
	d = trafficDataOf(t, c, "group=g")
	for _, k := range d["Kinds"].([]map[string]any) {
		if (k["Key"] == "groups") != (k["On"] == true) {
			t.Errorf("group filter: kind %v on=%v", k["Key"], k["On"])
		}
	}
	d = trafficDataOf(t, c, "group=g&flat=1")
	for _, k := range d["Kinds"].([]map[string]any) {
		if (k["Key"] == "") != (k["On"] == true) {
			t.Errorf("flat group filter: kind %v on=%v", k["Key"], k["On"])
		}
	}
	d = trafficDataOf(t, c, "")
	if d["HasPills"] != false || fmt.Sprint(d["Pills"]) != "[]" {
		t.Errorf("pills with no filter: %v", d["Pills"])
	}
}
