package console

import (
	"strings"
	"testing"
	"time"
)

// The paused page asks how many requests came after the one it stopped at, to the microsecond.
func TestTrafficNewCountsAfterTheCursor(t *testing.T) {
	T := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "r3", Seq: 3, First: T.Add(2 * time.Microsecond), Time: T.Add(2 * time.Microsecond), Count: 3, API: "Fetch"},
		{ID: "r2", Seq: 2, First: T.Add(time.Microsecond), Time: T.Add(time.Microsecond), Count: 1, API: "Fetch"},
		{ID: "r1", Seq: 1, First: T.Add(500 * time.Nanosecond), Time: T.Add(500 * time.Nanosecond), Count: 1, API: "Fetch"},
		{ID: "r0", Seq: 0, First: T, Time: T, Count: 1, API: "Fetch"},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	ask := func(since string) (int, string, string) {
		rec := req(t, c, "GET", "/_console/traffic/new?since="+since, nil)
		return rec.Code, rec.Body.String(), rec.Header().Get("Cache-Control")
	}
	for since, want := range map[string]string{
		msText(T.Add(-time.Microsecond)):    `{"n":6,"atLeast":false}`,
		msText(T):                           `{"n":4,"atLeast":false}`,
		msText(T.Add(time.Microsecond)):     `{"n":3,"atLeast":false}`,
		msText(T.Add(2 * time.Microsecond)): `{"n":0,"atLeast":false}`,
	} {
		code, body, cache := ask(since)
		if code != 200 || body != want || cache != "no-store" {
			t.Errorf("since %s: %d %s (%s), want %s", since, code, body, cache, want)
		}
	}
	for _, bad := range []string{"", "x", "NaN", "Inf", "-Inf", "1e999"} {
		if code, _, _ := ask(bad); code != 400 {
			t.Errorf("since=%q: %d", bad, code)
		}
	}
	plain := newTestConsole(t)
	if rec := req(t, plain, "GET", "/_console/traffic/new?since=1", nil); rec.Code != 404 {
		t.Errorf("without a recorder: %d", rec.Code)
	}
}

func TestTrafficPageAndPollWithAndWithoutARecorder(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{{ID: "r1", Seq: 1, First: now, Time: now, Count: 1, ClientID: "svc", API: "Fetch", Topics: []string{"t"}}}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	page := req(t, c, "GET", "/_console/traffic", nil)
	if page.Code != 200 || !strings.Contains(page.Body.String(), "Fetch") || !strings.Contains(page.Body.String(), "<title>Traffic") {
		t.Errorf("page: %d", page.Code)
	}
	clients := req(t, c, "GET", "/_console/traffic?view=clients", nil).Body.String()
	if !strings.Contains(clients, "<title>Traffic, clients") {
		t.Errorf("the clients view is titled as such")
	}
	live := req(t, c, "GET", "/_console/traffic/live", nil)
	if live.Code != 200 || live.Header().Get("HX-Live-Hash") == "" {
		t.Errorf("live: %d", live.Code)
	}
	h := live.Header().Get("HX-Live-Hash")
	if again := req(t, c, "GET", "/_console/traffic/live?h="+h, nil); again.Code != 204 {
		t.Errorf("nothing changed: %d", again.Code)
	}
	src.events = append(src.events, TrafficEvent{ID: "r2", Seq: 2, First: now, Time: now, Count: 1, ClientID: "svc", API: "Produce", Topics: []string{"t"}})
	if moved := req(t, c, "GET", "/_console/traffic/live?h="+h, nil); moved.Code != 200 {
		t.Errorf("something changed: %d", moved.Code)
	}
	// First run: nothing on the broker and nobody has talked to it.
	empty, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = &fakeTraffic{} })
	if e := req(t, empty, "GET", "/_console/traffic", nil).Body.String(); !strings.Contains(e, "Connect an app") && !strings.Contains(e, "connect") {
		t.Errorf("the empty page does not say how to connect")
	}
	plain := newTestConsole(t)
	for _, path := range []string{"/_console/traffic", "/_console/traffic/live"} {
		if rec := req(t, plain, "GET", path, nil); rec.Code != 404 {
			t.Errorf("%s without a recorder: %d", path, rec.Code)
		}
	}
}
