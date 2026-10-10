package console

import (
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTraffic is a TrafficSource with what a test puts in it.
type fakeTraffic struct {
	mu      sync.Mutex
	events  []TrafficEvent
	clients []TrafficClient
	groups  map[string][]TrafficGroupEvent
	asked   []TrafficFilter
}

func (f *fakeTraffic) Events(flt TrafficFilter, limit int) []TrafficEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, flt)
	var out []TrafficEvent
	for _, e := range f.events {
		if flt.Topic != "" && !slices.Contains(e.Topics, flt.Topic) {
			continue
		}
		if flt.ErrorsOnly && e.Error == "" {
			continue
		}
		out = append(out, e)
	}
	return out
}
func (f *fakeTraffic) Clients() []TrafficClient { return f.clients }
func (f *fakeTraffic) GroupEvents(g string, limit int) []TrafficGroupEvent {
	return f.groups[g]
}

var tt0 = time.Date(2026, 10, 10, 12, 30, 15, 250_000_000, time.Local)

func sampleTraffic() *fakeTraffic {
	return &fakeTraffic{
		events: []TrafficEvent{
			{Seq: 9, First: tt0, Time: tt0.Add(5 * time.Second), Count: 4, ClientID: "billing", Remote: "127.0.0.1:50001", Principal: "User:ANONYMOUS",
				API: "Heartbeat", Version: 4, Group: "billing-workers", Duration: 800 * time.Microsecond, ReqBytes: 60, RespBytes: 12},
			{Seq: 8, First: tt0, Time: tt0, Count: 1, ClientID: "billing", Software: "apache-kafka-java 3.7.0", Remote: "127.0.0.1:50001", Principal: "User:ANONYMOUS",
				API: "Produce", Version: 9, Topics: []string{"orders"}, Detail: "1 partition, 2.0 KiB", Duration: 12 * time.Millisecond, ReqBytes: 2100, RespBytes: 80},
			{Seq: 7, First: tt0, Time: tt0, Count: 1, ClientID: "audit", Remote: "127.0.0.1:50002", Principal: "User:ANONYMOUS",
				API: "Fetch", Version: 13, Topics: []string{"payments"}, Error: "UNKNOWN_TOPIC_OR_PARTITION", Duration: 2 * time.Millisecond, ReqBytes: 90, RespBytes: 50},
		},
		clients: []TrafficClient{{Conn: 1, Remote: "127.0.0.1:50001", ClientID: "billing", Software: "apache-kafka-java 3.7.0", Principal: "User:ANONYMOUS",
			Opened: time.Now().Add(-time.Minute), Last: time.Now().Add(-2 * time.Second), Requests: 41, Groups: []string{"billing-workers"}}},
		groups: map[string][]TrafficGroupEvent{
			"billing-workers": {
				{Seq: 1, Time: tt0, Kind: "joined", Member: "member-aaaaaaaaaaaaaaaaaaaa", Detail: "client billing"},
				{Seq: 2, Time: tt0.Add(time.Second), Kind: "generation", Member: "member-aaaaaaaaaaaaaaaaaaaa", Generation: 1, Detail: "1 member, leader member-a, protocol range"},
				{Seq: 3, Time: tt0.Add(40 * time.Second), Kind: "expired", Member: "member-aaaaaaaaaaaaaaaaaaaa", Detail: "client billing sent no heartbeat within its session timeout"},
			},
		},
	}
}

func TestTrafficPageShowsRequestsAndClients(t *testing.T) {
	src := sampleTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

	rec := req(t, c, "GET", "/_console/traffic", nil)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"Heartbeat", "v4", "×4", "billing-workers", // a folded row, with its count and its group.
		"Produce", "orders", "1 partition, 2.0 KiB", "12 ms",
		"UNKNOWN_TOPIC_OR_PARTITION", // the failed fetch, named.
		"apache-kafka-java 3.7.0",    // in the clients table.
		`id="clients-table"`, `id="traffic-table"`,
		`data-live="/_console/traffic/live"`, // the tail polls itself.
		`hx-get="/_console/traffic/request?seq=8"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the page does not show %q", want)
		}
	}
	// The rail offers it and marks it current.
	if !strings.Contains(body, `href="/_console/traffic" class="ri on"`) {
		t.Error("the rail does not mark Traffic as the current section")
	}
}

func TestTrafficFiltersReachTheSourceAndSurviveThePoll(t *testing.T) {
	src := sampleTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

	rec := req(t, c, "GET", "/_console/traffic?topic=orders&errors=1&client=bill+ing", nil)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	last := src.asked[len(src.asked)-1]
	if last.Topic != "orders" || !last.ErrorsOnly || last.Client != "bill ing" {
		t.Errorf("the source was asked for %+v", last)
	}
	body := rec.Body.String()
	// The poll URL carries the filter, or the first refresh would undo it.
	if !strings.Contains(body, `data-live="/_console/traffic/live?client=bill%20ing&amp;topic=orders&amp;errors=1"`) {
		t.Errorf("the live URL loses the filter:\n%s", between(body, `data-live=`, ` data-hash`))
	}
	// Errors-only over this fake leaves nothing matching topic=orders.
	if !strings.Contains(body, "No request matches the filter") {
		t.Error("an empty result for a filter does not say so")
	}
}

func between(s, a, b string) string {
	i := strings.Index(s, a)
	if i < 0 {
		return ""
	}
	j := strings.Index(s[i:], b)
	if j < 0 {
		return s[i:]
	}
	return s[i : i+j]
}

// The poll answers 204 while nothing moved, and a full region once something did.
func TestTrafficLiveOnlyAnswersWhenSomethingChanged(t *testing.T) {
	src := sampleTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

	first := req(t, c, "GET", "/_console/traffic/live", nil)
	if first.Code != 200 || !strings.Contains(first.Body.String(), `id="traffic-live"`) {
		t.Fatalf("live: %d %s", first.Code, first.Body)
	}
	hash := first.Header().Get("HX-Live-Hash")
	if hash == "" {
		t.Fatal("no hash to poll with")
	}
	if rec := req(t, c, "GET", "/_console/traffic/live?h="+hash, nil); rec.Code != 204 {
		t.Errorf("nothing changed, but the poll answered %d", rec.Code)
	}
	src.mu.Lock()
	src.events = append([]TrafficEvent{{Seq: 10, Time: tt0, Count: 1, ClientID: "billing", API: "Metadata", Version: 12, Duration: time.Millisecond}}, src.events...)
	src.mu.Unlock()
	rec := req(t, c, "GET", "/_console/traffic/live?h="+hash, nil)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Metadata") {
		t.Errorf("a new request did not refresh the region: %d", rec.Code)
	}
	// A folded row that gained a repeat is also a change: same row, new count.
	src.mu.Lock()
	src.events[0].Count = 11
	src.events[0].Seq = 11
	src.mu.Unlock()
	h2 := rec.Header().Get("HX-Live-Hash")
	if rec := req(t, c, "GET", "/_console/traffic/live?h="+h2, nil); rec.Code != 200 {
		t.Errorf("a repeat folded into a row did not refresh the region: %d", rec.Code)
	}
}

func TestTrafficDrawerShowsOneRequestAndSaysWhenItMovedOn(t *testing.T) {
	src := sampleTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

	rec := req(t, c, "GET", "/_console/traffic/request?seq=8", nil)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{"Produce v9", "billing", "apache-kafka-java 3.7.0", "127.0.0.1:50001", "User:ANONYMOUS", "orders", "1 partition, 2.0 KiB", "12 ms", "2.1 KB in"} {
		if !strings.Contains(body, want) {
			t.Errorf("the drawer is missing %q:\n%s", want, body)
		}
	}
	if gone := req(t, c, "GET", "/_console/traffic/request?seq=999", nil); gone.Code != 200 || !strings.Contains(gone.Body.String(), "changed or aged out") {
		t.Errorf("a vanished row: %d %s", gone.Code, gone.Body)
	}
	if bad := req(t, c, "GET", "/_console/traffic/request?seq=abc", nil); bad.Code != 400 {
		t.Errorf("a non-numeric seq answered %d, want 400", bad.Code)
	}
}

func TestGroupPageShowsItsTimelineNewestFirst(t *testing.T) {
	src := sampleTraffic()
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 1, 1)
	commitFor(t, addr, "billing-workers", "orders", map[int32]int64{0: 1})

	rec := req(t, c, "GET", "/_console/groups/billing-workers", nil)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{`id="group-timeline"`, "session expired", "new generation", "gen 1", "no heartbeat within its session timeout"} {
		if !strings.Contains(body, want) {
			t.Errorf("the group page's timeline is missing %q", want)
		}
	}
	expired, generation, joined := strings.Index(body, "session expired"), strings.Index(body, "new generation"), strings.Index(body, ">joined<")
	if !(expired > 0 && expired < generation && generation < joined) {
		t.Errorf("not newest first: expired@%d generation@%d joined@%d", expired, generation, joined)
	}

	// A group the coordinator has said nothing about says so, rather than showing a blank.
	commitFor(t, addr, "quiet", "orders", map[int32]int64{0: 1})
	if q := req(t, c, "GET", "/_console/groups/quiet", nil); !strings.Contains(q.Body.String(), "Nothing has happened in this group since the broker started") {
		t.Error("a group with no events does not say so")
	}
}

// With nothing to show it from, the console does not pretend: no rail entry, no page.
func TestWithoutATrafficSourceThereIsNoTrafficSection(t *testing.T) {
	c := newTestConsole(t)
	home := req(t, c, "GET", "/_console/topics", nil)
	if strings.Contains(home.Body.String(), "/_console/traffic") {
		t.Error("the rail offers Traffic with nothing behind it")
	}
	for _, p := range []string{"/_console/traffic", "/_console/traffic/live", "/_console/traffic/request?seq=1"} {
		if rec := req(t, c, "GET", p, nil); rec.Code != 404 {
			t.Errorf("GET %s = %d, want 404", p, rec.Code)
		}
	}
	if rec := req(t, c, "GET", "/_console/api/palette", nil); strings.Contains(rec.Body.String(), "Traffic") {
		t.Error("the palette offers Traffic with nothing behind it")
	}
}

func TestShortDuration(t *testing.T) {
	for in, want := range map[time.Duration]string{
		400 * time.Microsecond: "0.4 ms", 12 * time.Millisecond: "12 ms", 2500 * time.Millisecond: "2.5 s",
		190 * time.Second: "3m10s", 0: "0.0 ms",
	} {
		if got := shortDuration(in); got != want {
			t.Errorf("shortDuration(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestSpanText(t *testing.T) {
	for in, want := range map[time.Duration]string{
		0: "just now", time.Second: "just now", 12 * time.Second: "12s", 190 * time.Second: "3m 10s", 125 * time.Minute: "2h 05m",
	} {
		if got := spanText(in); got != want {
			t.Errorf("spanText(%v) = %q, want %q", in, got, want)
		}
	}
}
