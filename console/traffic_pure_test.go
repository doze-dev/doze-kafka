package console

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDurationTexts(t *testing.T) {
	ms := time.Millisecond
	us := time.Microsecond
	for d, want := range map[time.Duration]string{
		-5 * ms: "0 ms", 0: "0 ms", 400 * us: "0 ms", 500 * us: "1 ms", 2 * ms: "2 ms", 999400 * us: "999 ms", 999500 * us: "1.0 s",
		time.Second: "1.0 s", 3 * time.Second: "3.0 s", 59900 * ms: "59.9 s", 59950 * ms: "1 m 0 s", time.Minute: "1 m 0 s",
		130 * time.Second: "2 m 10 s", time.Hour: "60 m 0 s",
	} {
		if got := shortDuration(d); got != want {
			t.Errorf("shortDuration(%v) = %q, want %q", d, got, want)
		}
	}
	for d, want := range map[time.Duration]string{
		-5 * us: "took 0 µs", 42 * us: "took 42 µs", 99 * us: "took 99 µs", 100 * us: "took 0.1 ms", 22800 * us: "took 22.8 ms",
		999 * ms: "took 999.0 ms", time.Second: "took 1.000 s", 2500 * ms: "took 2.500 s",
	} {
		if got := exactDuration(d); got != want {
			t.Errorf("exactDuration(%v) = %q, want %q", d, got, want)
		}
	}
	for d, want := range map[time.Duration]string{
		0: "just now", 1999 * ms: "just now", 2 * time.Second: "2s", 59 * time.Second: "59s", time.Minute: "1m 00s", 3599 * time.Second: "59m 59s",
		time.Hour: "1h 00m", 47*time.Hour + 59*time.Minute: "47h 59m", 48 * time.Hour: "2d 00h", 100 * time.Hour: "4d 04h",
	} {
		if got := spanText(d); got != want {
			t.Errorf("spanText(%v) = %q, want %q", d, got, want)
		}
	}
	for d, want := range map[time.Duration]string{
		-5 * time.Second: "0 s", 0: "0 s", 9 * time.Second: "9 s", 59 * time.Second: "59 s", time.Minute: "1 m 00 s", 250 * time.Second: "4 m 10 s",
		time.Hour: "1 h 00 m", time.Hour + 2*time.Minute: "1 h 02 m", 47 * time.Hour: "47 h 00 m", 48 * time.Hour: "2 d 00 h", 77 * time.Hour: "3 d 05 h",
	} {
		if got := spanBoard(d); got != want {
			t.Errorf("spanBoard(%v) = %q, want %q", d, got, want)
		}
	}
	for d, want := range map[time.Duration]string{
		0: "0 ms", 500 * ms: "500 ms", 999 * ms: "999 ms", time.Second: "1 s", 1040 * ms: "1 s", 1500 * ms: "1.5 s", 3 * time.Second: "3 s",
		9400 * ms: "9.4 s", 10400 * ms: "10 s", 59 * time.Second: "59 s", time.Minute: "1 min", 90 * time.Second: "2 min", 5 * time.Minute: "5 min",
	} {
		if got := everyText(d); got != want {
			t.Errorf("everyText(%v) = %q, want %q", d, got, want)
		}
	}
	for n, want := range map[int]string{0: "0 B", 1023: "1023 B", 1024: "1.0 KiB", 1536: "1.5 KiB", 1<<20 - 1: "1024.0 KiB", 1 << 20: "1.0 MiB", 5 << 20: "5.0 MiB"} {
		if got := kibText(n); got != want {
			t.Errorf("kibText(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestResultWordsAndQueryEscaping(t *testing.T) {
	for class, want := range map[string][2]string{"": {"ok", ""}, "expected": {"deny", "Expected"}, "refused": {"deny", "Refused"}, "denied": {"deny", "Denied"},
		"not-implemented": {"warn", "Not implemented"}, "failed": {"bad", "Failed"}, "other": {"deny", ""}} {
		if got := resultTone(class); got != want[0] {
			t.Errorf("resultTone(%q) = %q", class, got)
		}
		if got := classWord(class); got != want[1] {
			t.Errorf("classWord(%q) = %q", class, got)
		}
	}
	for in, want := range map[string]string{
		"": "", "az09-_.~": "az09-_.~", "AZ": "AZ", "a b/c": "a%20b%2Fc", "é": "%C3%A9", "`{@[": "%60%7B%40%5B", "/:": "%2F%3A", "a+b&c=d": "a%2Bb%26c%3Dd",
	} {
		if got := queryEscape(in); got != want {
			t.Errorf("queryEscape(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTrafficViewFromAndItsQuery(t *testing.T) {
	view := func(q string) trafficView {
		return trafficViewFrom(httptest.NewRequest("GET", "/_console/traffic?"+q, nil))
	}
	v := view("")
	if !v.empty() || v.View != "requests" || v.Kind != "" || v.Flat || v.query() != "" || v.source() != (TrafficFilter{}) {
		t.Errorf("empty: %+v", v)
	}
	v = view("client=%20a%20&api=Fetch&topic=t&group=g&errors=1&q=%20x%20&kind=fetch&view=clients&flat=1")
	if v.Client != "a" || v.API != "Fetch" || v.Topic != "t" || v.Group != "g" || !v.ErrorsOnly || v.Q != "x" || v.Kind != "fetch" || v.View != "clients" || !v.Flat || v.empty() {
		t.Errorf("full: %+v", v)
	}
	if got, want := v.query(), "client=a&api=Fetch&topic=t&group=g&errors=1&q=x&kind=fetch&view=clients&flat=1"; got != want {
		t.Errorf("query\n got %s\nwant %s", got, want)
	}
	if f := v.source(); f.Any != "x" || f.Kind != "fetch" || !f.ErrorsOnly || f.Client != "a" {
		t.Errorf("source: %+v", f)
	}
	for _, k := range []string{"errors", "produce", "fetch", "groups", "admin"} {
		if got := view("kind=" + k).Kind; got != k {
			t.Errorf("kind %s read as %q", k, got)
		}
	}
	if got := view("kind=bogus").Kind; got != "" {
		t.Errorf("an unknown kind: %q", got)
	}
	if f := view("kind=errors").source(); !f.ErrorsOnly || f.Kind != "" {
		t.Errorf("errors kind: %+v", f)
	}
	if f := view("kind=admin").source(); f.ErrorsOnly || f.Kind != "admin" {
		t.Errorf("admin kind: %+v", f)
	}
	// The field repeating a pill is that pill, not a second filter.
	if v = view("topic=orders&q=orders"); v.Q != "" || v.Topic != "orders" {
		t.Errorf("q repeating the topic: %+v", v)
	}
	if v = view("group=g&q=g"); v.Q != "" {
		t.Errorf("q repeating the group: %+v", v)
	}
	if v = view("topic=orders&q=ord"); v.Q != "ord" {
		t.Errorf("q different from the topic: %+v", v)
	}
	if view("view=other").View != "requests" || view("errors=").ErrorsOnly {
		t.Error("view and errors defaults")
	}
	if view("q=a%20b&topic=t%20u").query() != "topic=t%20u&q=a%20b" {
		t.Errorf("escaping: %s", view("q=a%20b&topic=t%20u").query())
	}
	if (trafficView{Kind: "errors"}).empty() || (trafficView{Q: "x"}).empty() || (trafficView{TrafficFilter: TrafficFilter{Group: "g"}}).empty() || !(trafficView{View: "clients"}).empty() {
		t.Error("empty()")
	}
}

func TestClientNeedsALookRule(t *testing.T) {
	for _, tc := range []struct {
		c    TrafficClient
		want bool
	}{
		{TrafficClient{}, false},
		{TrafficClient{ClientStreak: 2}, false},
		{TrafficClient{ClientStreak: 3}, true},
		{TrafficClient{Err5m: 3, Req5m: 6}, true},
		{TrafficClient{Err5m: 3, Req5m: 7}, false},
		{TrafficClient{Err5m: 2, Req5m: 2}, false},
		{TrafficClient{Err5m: 5, Req5m: 10}, true},
		{TrafficClient{Err5m: 4, Req5m: 10}, false},
		{TrafficClient{Err5m: 3, Req5m: 0}, false},
		{TrafficClient{Err5m: 3, Req5m: 1}, true},
	} {
		if got := needsALook(tc.c); got != tc.want {
			t.Errorf("needsALook(%+v) = %v", tc.c, got)
		}
	}
	for class, want := range map[string]string{"denied": "denied", "failed": "failed", "not-implemented": "not served", "refused": "refused", "": "refused"} {
		if got := noWord(class); got != want {
			t.Errorf("noWord(%q) = %q", class, got)
		}
	}
	head, note := lookWords(TrafficClient{ClientStreak: 4, ClientLastClass: "denied"})
	if head != "has been denied 4 times in a row" || note != "last 4 requests denied" {
		t.Errorf("%q / %q", head, note)
	}
	head, note = lookWords(TrafficClient{ClientStreak: 3, ClientLastClass: "failed"})
	if head != "has failed 3 times in a row" || note != "last 3 requests failed" {
		t.Errorf("%q / %q", head, note)
	}
	head, note = lookWords(TrafficClient{ClientStreak: 2, Err5m: 3, Req5m: 5})
	if head != "had 3 errors among its 5 requests in the last five minutes" || note != "3 of 5 requests were errors in the last 5 minutes" {
		t.Errorf("%q / %q", head, note)
	}
}

func TestClientRowRatesAndWhatItDoes(t *testing.T) {
	c := newTestConsole(t)
	now := time.Now()
	row := func(cl TrafficClient) clientRow {
		cl.Opened, cl.Last = now.Add(-2*time.Hour-14*time.Minute), now.Add(-38*time.Second)
		return c.clientRow(cl, now)
	}
	for _, tc := range []struct {
		err, req  int
		rate, ton string
	}{{0, 0, "—", "mut"}, {0, 20, "0 %", "ok"}, {1, 20, "5.0 %", ""}, {1, 11, "9.1 %", ""}, {1, 10, "10 %", "warn"}, {3, 20, "15 %", "warn"}, {1, 8, "13 %", "warn"}, {1, 7, "14 %", "warn"}, {5, 5, "100 %", "warn"}} {
		r := row(TrafficClient{Err5m: tc.err, Req5m: tc.req})
		if r.Rate != tc.rate || r.RateTon != tc.ton {
			t.Errorf("%d of %d: %q %q, want %q %q", tc.err, tc.req, r.Rate, r.RateTon, tc.rate, tc.ton)
		}
	}
	if r := row(TrafficClient{Err5m: 1, Req5m: 4}); r.RateTitle != "1 of 4 requests in the last 5 minutes were errors" {
		t.Errorf("title %q", r.RateTitle)
	}
	if r := row(TrafficClient{}); r.RateTitle != "No request in the last 5 minutes" {
		t.Errorf("title %q", r.RateTitle)
	}
	r := row(TrafficClient{ClientID: "svc", Remote: "10.0.0.1:99"})
	if r.Who != "svc" || r.Age != "2 h 14 m" || r.Idle != "38 s" || r.Filter != "client=svc" || string(r.Href) != "/_console/traffic?client=svc" || r.Groups != "none" || r.Doing != "" {
		t.Errorf("%+v", r)
	}
	if r = row(TrafficClient{Remote: "10.0.0.1:99"}); r.Who != "10.0.0.1:99" || r.Filter != "client=10.0.0.1%3A99" {
		t.Errorf("by address: %q %q", r.Who, r.Filter)
	}
	if r = row(TrafficClient{Produces: true, TxnID: "tx-1"}); r.Doing != "producer · tx tx-1" || r.Groups != "none (producer)" {
		t.Errorf("transactional producer: %q %q", r.Doing, r.Groups)
	}
	if r = row(TrafficClient{Produces: true}); r.Doing != "producer" {
		t.Errorf("producer: %q", r.Doing)
	}
	if r = row(TrafficClient{Fetches: true}); r.Doing != "consumer" {
		t.Errorf("consumer: %q", r.Doing)
	}
	if r = row(TrafficClient{Fetches: true, Groups: []string{"g1", "g2"}}); r.Doing != "g1 · g2" || r.Groups != "g1, g2" {
		t.Errorf("in groups: %q %q", r.Doing, r.Groups)
	}
	if r = row(TrafficClient{Groups: []string{"g1"}, ClientStreak: 3, ClientLastClass: "denied"}); r.Groups != "g1, denied" || !r.NeedsALook {
		t.Errorf("refused in a group: %q", r.Groups)
	}
	if r = row(TrafficClient{Groups: []string{"g1"}, ClientStreak: 2}); r.Groups != "g1" || r.NeedsALook {
		t.Errorf("two refusals: %q", r.Groups)
	}
	if clientCount([]TrafficClient{{ClientID: "a"}, {ClientID: "a"}, {Remote: "r1"}, {Remote: "r2"}, {ClientID: "b", Remote: "r1"}}) != 4 {
		t.Error("clientCount")
	}
}

func TestGroupClientsFoldsConnections(t *testing.T) {
	c := newTestConsole(t)
	now := time.Now()
	cls := []TrafficClient{
		{Conn: 1, ClientID: "app", Remote: "a:1", Opened: now.Add(-time.Hour), Last: now.Add(-50 * time.Second), Requests: 10, Principal: "User:u1", Groups: []string{"g1"}, Produces: true,
			ClientReq1m: 7, ClientReq5m: 20, ClientErr5m: 2, Software: ""},
		{Conn: 2, ClientID: "app", Remote: "a:2", Opened: now.Add(-3 * time.Hour), Last: now.Add(-5 * time.Second), Requests: 5, Principal: "User:u2", Groups: []string{"g1", "g2"}, Fetches: true,
			ClientReq1m: 7, ClientReq5m: 20, ClientErr5m: 2, Software: "kafka-java", TxnID: "t1"},
		{Conn: 3, Remote: "b:9", Opened: now.Add(-time.Minute), Last: now, Requests: 1, ClientReq5m: 1},
		{Conn: 4, ClientID: "solo", Remote: "c:1", Opened: now.Add(-time.Minute), Last: now, Requests: 2, Req5m: 99, ClientReq5m: 4},
	}
	rows := c.groupClients(cls, now)
	if len(rows) != 3 || rows[0].Who != "app" || rows[1].Who != "b:9" || rows[2].Who != "solo" {
		t.Fatalf("%d rows, first %q", len(rows), rows[0].Who)
	}
	app := rows[0]
	if app.Conns != 2 || app.More != 1 || len(app.Members) != 2 || app.Keys != "client:1 client:2" {
		t.Errorf("app: conns %d more %d members %d keys %q", app.Conns, app.More, len(app.Members), app.Keys)
	}
	if app.Requests != 15 || app.Principal != "User:u1, User:u2" || strings.Join(app.Groups2(), ",") != "g1,g2" || !app.Produces || !app.Fetches ||
		app.Software != "kafka-java" || app.TxnID != "t1" || app.Age != "3 h 00 m" || app.Idle != "5 s" || app.Req5m != 20 || app.Err5m != 2 {
		t.Errorf("app: %+v", app)
	}
	if solo := rows[2]; solo.Conns != 1 || solo.Keys != "client:4" || solo.Req5m != 4 || len(solo.Members) != 1 {
		t.Errorf("solo: %+v", solo)
	}
}

func (r clientRow) Groups2() []string { return r.TrafficClient.Groups }
