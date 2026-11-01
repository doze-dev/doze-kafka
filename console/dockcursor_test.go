package console

import (
	"html"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"
)

// dockIDs are the ids of the rows a dock response shows, in order.
func dockIDs(body string) []string {
	var out []string
	for _, m := range regexp.MustCompile(`id="wd-([^"]+)"`).FindAllStringSubmatch(body, -1) {
		out = append(out, m[1])
	}
	return out
}

// twentyOrders is a traffic source with twenty requests for "orders", r20 the newest.
func twentyOrders() *fakeTraffic {
	now := time.Now()
	src := &fakeTraffic{}
	for i := 20; i >= 1; i-- {
		src.events = append(src.events, TrafficEvent{ID: "r" + itoa(i), Seq: uint64(i), First: now, Time: now.Add(time.Duration(i) * time.Second), Count: 1,
			ClientID: "app", Remote: "r", API: "Produce", Version: 9, Topics: []string{"orders"}})
	}
	return src
}

// The dock's cursor is the contract's (A4, E4: events for one resource, newest
// first, at most limit, those after the cursor). The source has always taken one and
// the endpoint always passed "": the half of DockEvents that pages was never run by
// the console. /dock?after=<row id> is that page, the dock offers it ("Older"), and
// the page it lands on polls the same window, not the head.
func TestTheDockPagesByItsCursor(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = twentyOrders() })
	get := func(addr string) (string, []string) {
		rec := req(t, c, "GET", addr, nil)
		if rec.Code != 200 {
			t.Fatalf("GET %s: %d %s", addr, rec.Code, rec.Body)
		}
		return rec.Body.String(), dockIDs(rec.Body.String())
	}
	older := func(body string) string {
		return html.UnescapeString(after(after(body, `data-dock-older`, `>`), `hx-get="`, `"`))
	}

	first, ids := get("/_console/dock?topic=orders")
	if strings.Join(ids, " ") != "r20 r19 r18 r17 r16 r15 r14 r13" {
		t.Fatalf("the first page is %v", ids)
	}
	if strings.Contains(first, "data-dock-newest") {
		t.Error("the newest page offers a way back to itself")
	}
	next := older(first)
	u, err := url.Parse(next)
	if err != nil || u.Path != "/_console/dock" || u.Query().Get("topic") != "orders" || u.Query().Get("after") != "r13" {
		t.Fatalf("Older asks for %q (%v), want /_console/dock?topic=orders&after=r13", next, err)
	}

	second, ids := get(u.RequestURI())
	if strings.Join(ids, " ") != "r12 r11 r10 r9 r8 r7 r6 r5" {
		t.Fatalf("the page after r13 is %v", ids)
	}
	// It polls the window it shows.
	live, _ := url.Parse(html.UnescapeString(after(second, `data-live="`, `"`)))
	if live.Query().Get("after") != "r13" || live.Query().Get("topic") != "orders" {
		t.Errorf("the older page polls %v: the next refresh would put the newest rows back", live)
	}
	hash := req(t, c, "GET", u.RequestURI(), nil).Header().Get("HX-Live-Hash")
	if again := req(t, c, "GET", live.RequestURI()+"&h="+url.QueryEscape(hash), nil); again.Code != 204 {
		t.Errorf("an unchanged older page answered %d, want 204", again.Code)
	}
	// It says it is not the newest, and has the way back.
	newest, _ := url.Parse(html.UnescapeString(after(after(second, `data-dock-newest`, `>`), `hx-get="`, `"`)))
	if newest.Path != "/_console/dock" || newest.Query().Get("topic") != "orders" || newest.Query().Has("after") {
		t.Errorf("Newest asks for %v", newest)
	}
	if !strings.Contains(second, "older requests") {
		t.Errorf("the older page does not say what it shows:\n%s", between(second, `class="dh"`, "</div>"))
	}

	third, ids := get(html.UnescapeString(older(second)))
	if strings.Join(ids, " ") != "r4 r3 r2 r1" {
		t.Fatalf("the last page is %v", ids)
	}
	if strings.Contains(third, "data-dock-older") {
		t.Error("the last page offers an older one")
	}

	// The pause control of a page that has none of its own goes with the cursor.
	paged, _ := get("/_console/dock?topic=orders&t=1&after=r13")
	if l, _ := url.Parse(html.UnescapeString(after(paged, `data-live="`, `"`))); l.Query().Get("t") != "1" || l.Query().Get("after") != "r13" || !strings.Contains(paged, "data-live-toggle") {
		t.Errorf("the paged dock with a pause control polls %v", l)
	}
	if o, _ := url.Parse(older(paged)); o.Query().Get("t") != "1" || o.Query().Get("after") != "r5" {
		t.Errorf("its Older asks for %v", o)
	}

	// A cursor whose row the recorder no longer holds: everything kept is newer.
	gone, ids := get("/_console/dock?topic=orders&after=aged-out")
	if len(ids) != 0 || !strings.Contains(gone, "Nothing older is kept.") || !strings.Contains(gone, "data-dock-newest") {
		t.Errorf("an aged-out cursor shows %v:\n%s", ids, gone)
	}
	// A cursor is a row id, and a row id is short.
	if bad := req(t, c, "GET", "/_console/dock?topic=orders&after="+strings.Repeat("x", 200), nil); bad.Code != 400 {
		t.Errorf("a 200-byte cursor answered %d, want 400", bad.Code)
	}
}

// A page ends with the newest rows: the dock on a topic's page is never a paged one.
func TestAPagesDockIsTheNewestRows(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = twentyOrders() })
	topicWith(t, c, "orders", 1, 1)
	page := req(t, c, "GET", "/_console/topics/orders?after=r13", nil).Body.String()
	dock := between(page, `id="wire-dock"`, `</section>`)
	if ids := dockIDs(dock); len(ids) != 8 || ids[0] != "r20" {
		t.Errorf("the page's dock shows %v", ids)
	}
	if !strings.Contains(dock, "data-dock-older") || !strings.Contains(dock, `/_console/traffic?topic=orders"`) {
		t.Errorf("the page's dock offers neither Older nor the Traffic page:\n%s", dock)
	}
}

// A source that is not Kafka's (the registry's requests, by subject) pages the same
// way; it has no Traffic page to send anyone to, so Older is all it offers.
func TestASubjectsDockPagesToo(t *testing.T) {
	src := &fakeDock{}
	for i := 9; i >= 1; i-- {
		src.events = append(src.events, fakeDockEvent{DockFilter{Subject: "orders-value"}, dockEvent("s"+itoa(i), "checkout-api", "GET /subjects/orders-value", "orders-value", "registry")})
	}
	c, _, _ := newRegistryConsole(t, func(o *Options) { o.Dock = src })
	first := req(t, c, "GET", "/_console/dock?subject=orders-value", nil).Body.String()
	next := html.UnescapeString(after(after(first, `data-dock-older`, `>`), `hx-get="`, `"`))
	if !strings.Contains(next, "after=s2") || strings.Contains(first, "/traffic") {
		t.Fatalf("Older asks for %q:\n%s", next, between(first, `dock-more`, "</div>"))
	}
	if ids := dockIDs(req(t, c, "GET", next, nil).Body.String()); len(ids) != 1 || ids[0] != "s1" {
		t.Errorf("the subject's second page is %v", ids)
	}
	if got := src.asked[len(src.asked)-1]; got != (DockFilter{Subject: "orders-value"}) {
		t.Errorf("the source was asked for %+v", got)
	}
}
