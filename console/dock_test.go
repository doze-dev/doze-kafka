package console

import (
	"html"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDock is a DockSource with what a test puts in it: events of several
// resources, of which a dock must show only its own.
type fakeDock struct {
	mu     sync.Mutex
	events []fakeDockEvent
	asked  []DockFilter
}

type fakeDockEvent struct {
	f DockFilter
	e DockEvent
}

func (d *fakeDock) DockEvents(f DockFilter, limit int, after string) ([]DockEvent, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.asked = append(d.asked, f)
	var out []DockEvent
	skipping := after != ""
	for _, fe := range d.events {
		if fe.f != f {
			continue
		}
		if skipping {
			skipping = fe.e.ID != after
			continue
		}
		if len(out) == limit {
			return out, out[len(out)-1].ID
		}
		out = append(out, fe.e)
	}
	return out, ""
}

func dockEvent(id, client, req, target string, source string) DockEvent {
	return DockEvent{ID: id, Time: time.Date(2026, 10, 11, 9, 30, 0, 0, time.UTC), Source: source, Client: client, Request: req, Target: target, Kind: KindRequest}
}

func sampleDock() *fakeDock {
	return &fakeDock{events: []fakeDockEvent{
		{DockFilter{Topic: "orders"}, dockEvent("o1", "checkout-api", "Produce v9", "orders", "kafka")},
		{DockFilter{Topic: "orders"}, dockEvent("o2", "g-validator", "Fetch v13", "orders", "kafka")},
		{DockFilter{Topic: "payments"}, dockEvent("p1", "ledger-sync", "Produce v9", "payments", "kafka")},
		{DockFilter{Group: "billing"}, dockEvent("g1", "billing-1", "Heartbeat v4", "billing", "kafka")},
		{DockFilter{Subject: "orders-value"}, dockEvent("s1", "checkout-api", "GET /subjects/orders-value/versions/latest", "orders-value", "registry")},
	}}
}

// The dock shows the requests of its own resource and no other's; it polls itself,
// has the pause control, and is the same rows when it is asked again.
func TestTheWireDockShowsOnlyRowsForItsResource(t *testing.T) {
	src := sampleDock()
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Dock = src })
	topicWith(t, c, "orders", 1, 1)
	topicWith(t, c, "payments", 1, 1)
	commitFor(t, addr, "billing", "orders", map[int32]int64{0: 1})

	page := req(t, c, "GET", "/_console/topics/orders", nil).Body.String()
	for _, want := range []string{`id="wire-dock"`, `data-live="/_console/dock?topic=orders"`, "data-live-toggle", "checkout-api", "g-validator", `hx-get="/_console/inspect/request?key=o1"`} {
		if !strings.Contains(page, want) {
			t.Errorf("the topic's dock is missing %q", want)
		}
	}
	for _, other := range []string{"ledger-sync", "billing-1", "orders-value"} {
		if strings.Contains(between(page, `id="wire-dock"`, `</section>`), other) {
			t.Errorf("the orders dock shows a row for another resource: %q", other)
		}
	}
	if pay := req(t, c, "GET", "/_console/topics/payments", nil).Body.String(); !strings.Contains(pay, "ledger-sync") || strings.Contains(between(pay, `id="wire-dock"`, `</section>`), "checkout-api") {
		t.Error("the payments dock should show payments' rows only")
	}
	grp := req(t, c, "GET", "/_console/groups/billing", nil).Body.String()
	dock := between(grp, `id="wire-dock"`, `</section>`)
	if !strings.Contains(dock, "billing-1") || strings.Contains(dock, "checkout-api") {
		t.Errorf("the group's dock should show the group's rows only:\n%s", dock)
	}
	last := src.asked[len(src.asked)-1]
	if last != (DockFilter{Group: "billing"}) {
		t.Errorf("the group page asked the source for %+v", last)
	}

	// Its poll: the same component, and 204 once nothing has changed.
	rec := req(t, c, "GET", "/_console/dock?topic=orders", nil)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "checkout-api") || strings.Contains(rec.Body.String(), "ledger-sync") {
		t.Fatalf("the poll: %d %s", rec.Code, rec.Body)
	}
	hash := rec.Header().Get("HX-Live-Hash")
	if hash == "" {
		t.Fatal("the poll does not say what it showed")
	}
	if again := req(t, c, "GET", "/_console/dock?topic=orders&h="+url.QueryEscape(hash), nil); again.Code != 204 {
		t.Errorf("an unchanged dock answered %d, want 204", again.Code)
	}
	if bad := req(t, c, "GET", "/_console/dock", nil); bad.Code != 400 {
		t.Errorf("a dock about nothing answered %d", bad.Code)
	}
}

// A subject's dock accepts a source that is not Kafka's (E4): the registry's own
// requests, by their REST path.
func TestADockTakesARegistrySource(t *testing.T) {
	src := sampleDock()
	c, _, _ := newRegistryConsole(t, func(o *Options) { o.Dock = src })
	d := c.dockData(DockFilter{Subject: "orders-value"})
	rows := d["DockRows"].([]dockRow)
	if len(rows) != 1 || rows[0].Source != "registry" || !strings.Contains(rows[0].Request, "GET /subjects/orders-value") {
		t.Errorf("subject dock rows = %+v", rows)
	}
	rec := req(t, c, "GET", "/_console/dock?subject=orders-value", nil)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "GET /subjects/orders-value") {
		t.Errorf("subject poll: %d %s", rec.Code, rec.Body)
	}
}

// With the traffic source the console already has, a topic's dock is built from
// it, newest first and with a cursor.
func TestTheDockIsBuiltFromTrafficWhenNothingBetterIsGiven(t *testing.T) {
	traffic := sampleTraffic()
	d := trafficDock{traffic}
	events, next := d.DockEvents(DockFilter{Topic: "orders"}, 10, "")
	if len(events) != 1 || events[0].Request != "Produce v9" || next != "" {
		t.Fatalf("events = %+v next = %q", events, next)
	}
	all, _ := d.DockEvents(DockFilter{}, 2, "")
	if len(all) != 2 {
		t.Fatalf("limit not honoured: %d", len(all))
	}
	_, next = d.DockEvents(DockFilter{}, 2, "")
	rest, _ := d.DockEvents(DockFilter{}, 2, next)
	if len(rest) != 1 || rest[0].ID == all[0].ID || rest[0].ID == all[1].ID {
		t.Errorf("the cursor did not continue after %q: %+v", next, rest)
	}
	if sub, _ := d.DockEvents(DockFilter{Subject: "x"}, 5, ""); sub != nil {
		t.Error("the traffic source has nothing for a subject")
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = traffic })
	topicWith(t, c, "orders", 1, 1)
	page := req(t, c, "GET", "/_console/topics/orders", nil).Body.String()
	if !strings.Contains(between(page, `id="wire-dock"`, `</section>`), "Produce v9") {
		t.Error("the dock built from traffic shows no Produce")
	}
	// Neither a dock source nor a traffic source: no dock, and not an empty one.
	c2 := newTestConsole(t)
	topicWith(t, c2, "orders", 1, 1)
	if strings.Contains(req(t, c2, "GET", "/_console/topics/orders", nil).Body.String(), "wire-dock") {
		t.Error("a console with nothing to read offers a dock")
	}
}

// The dock's link to the Traffic page carries its filter, once: a query string
// written into the href by the template was escaped a second time
// ("/traffic?topic%3dorders"), which the server reads as one key and no filter.
func TestTheDocksTrafficLinkCarriesItsFilter(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    DockFilter
		rows int // matching rows the Traffic page must show
	}{
		{"a plain topic", DockFilter{Topic: "orders"}, 9},
		{"a topic that needs escaping", DockFilter{Topic: "a&b=c d%2F+é"}, 9},
		{"a group that needs escaping", DockFilter{Group: "g 1&topic=orders#x?y"}, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			src := &fakeTraffic{}
			// Nine rows for the resource (one more than a dock shows, so it offers the
			// link) and one for another, which the filtered page must not show.
			for i := 0; i < 9; i++ {
				e := TrafficEvent{ID: "m" + itoa(i), Seq: uint64(10 + i), First: now, Time: now, Count: 1, ClientID: "mine-" + itoa(i), Remote: "r", API: "Produce", Version: 9}
				if tc.f.Topic != "" {
					e.Topics = []string{tc.f.Topic}
				} else {
					e.API, e.Group = "Heartbeat", tc.f.Group
				}
				src.events = append(src.events, e)
			}
			src.events = append(src.events, TrafficEvent{ID: "x", Seq: 1, First: now, Time: now, Count: 1, ClientID: "someone-else", Remote: "r", API: "Produce", Version: 9, Topics: []string{"orders-not"}, Group: "another"})
			c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

			dock := req(t, c, "GET", "/_console/dock?"+url.Values{"topic": {tc.f.Topic}, "group": {tc.f.Group}}.Encode(), nil).Body.String()
			href := after(dock, `Older requests are on the <a href="`, `"`)
			if href == "" {
				t.Fatalf("the dock offers no link to the Traffic page:\n%s", dock)
			}
			u, err := url.Parse(html.UnescapeString(href))
			if err != nil {
				t.Fatal(err)
			}
			want := url.Values{}
			if tc.f.Topic != "" {
				want.Set("topic", tc.f.Topic)
			} else {
				want.Set("group", tc.f.Group)
			}
			if got := u.Query(); got.Encode() != want.Encode() || u.Path != "/_console/traffic" {
				t.Fatalf("href %q is read as path %q query %v, want %v", href, u.Path, got, want)
			}
			// Following it shows that resource's rows and not the other's.
			// (A group's page is its rebalance story; flat=1 is that page's own "as a
			// plain list".)
			follow := u.RequestURI()
			if tc.f.Group != "" {
				follow += "&flat=1"
			}
			page := req(t, c, "GET", follow, nil).Body.String()
			if n := strings.Count(page, `>mine-`); n < tc.rows || strings.Contains(page, "someone-else") {
				t.Errorf("following %q: %d of the resource's rows, another's shown = %v", href, n, strings.Contains(page, "someone-else"))
			}
			// The poll address names the same resource.
			live, err := url.Parse(html.UnescapeString(after(dock, `data-live="`, `"`)))
			if err != nil || live.Query().Get("topic") != tc.f.Topic || live.Query().Get("group") != tc.f.Group {
				t.Errorf("the dock polls %v (%v)", live, err)
			}
		})
	}
}

// after is what follows marker in s, up to end.
func after(s, marker, end string) string {
	_, rest, ok := strings.Cut(s, marker)
	if !ok {
		return ""
	}
	out, _, _ := strings.Cut(rest, end)
	return out
}

// The group page's dock carries the pause control, and its poll must ask for the
// same component or the control would vanish at the first refresh.
func TestTheGroupPagesDockPollKeepsItsPauseControl(t *testing.T) {
	src := sampleDock()
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Dock = src })
	topicWith(t, c, "orders", 1, 1)
	commitFor(t, addr, "bill ing&x=1", "orders", map[int32]int64{0: 1})
	src.events = append(src.events, fakeDockEvent{DockFilter{Group: "bill ing&x=1"}, dockEvent("b1", "billing-1", "Heartbeat v4", "bill ing&x=1", "kafka")})
	page := req(t, c, "GET", "/_console/groups/"+url.PathEscape("bill ing&x=1"), nil)
	if page.Code != 200 {
		t.Fatalf("the group page: %d", page.Code)
	}
	addrLive := html.UnescapeString(after(after(page.Body.String(), `id="wire-dock"`, `data-hash`), `data-live="`, `"`))
	u, err := url.Parse(addrLive)
	if err != nil || u.Path != "/_console/dock" || u.Query().Get("group") != "bill ing&x=1" || u.Query().Get("t") != "1" || len(u.Query()) != 2 {
		t.Fatalf("the group page's dock polls %q (%v)", addrLive, err)
	}
	poll := req(t, c, "GET", u.RequestURI(), nil).Body.String()
	if !strings.Contains(poll, "data-live-toggle") || !strings.Contains(poll, "billing-1") {
		t.Errorf("the poll lost the pause control or the rows:\n%s", poll)
	}
	if again := html.UnescapeString(after(poll, `data-live="`, `"`)); again != addrLive {
		t.Errorf("the poll's own address is %q, the page's %q", again, addrLive)
	}
}

// A subject has no filter on the Traffic page, so its dock does not link there as if
// it had.
func TestASubjectsDockDoesNotLinkToAnUnfilteredTrafficPage(t *testing.T) {
	src := &fakeDock{}
	for i := 0; i < 9; i++ {
		src.events = append(src.events, fakeDockEvent{DockFilter{Subject: "orders-value"}, dockEvent("s"+itoa(i), "checkout-api", "GET /subjects/orders-value", "orders-value", "registry")})
	}
	c, _, _ := newRegistryConsole(t, func(o *Options) { o.Dock = src })
	dock := req(t, c, "GET", "/_console/dock?subject=orders-value", nil).Body.String()
	if strings.Contains(dock, "/traffic") || !strings.Contains(dock, "Only the latest 8 are shown.") {
		t.Errorf("a subject's dock:\n%s", dock)
	}
}
