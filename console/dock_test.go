package console

import (
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
