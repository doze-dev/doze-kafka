package console

import (
	"strings"
	"testing"
	"time"
)

// Drawing the Traffic page, or polling it, never asks the recorder for everything it
// holds. The page lists the newest rows and counts the rest, and the recorder can
// count without handing rows over: asking for all of them copies the whole ring under
// the lock every client's request waits on, and the page did that twice a poll (and the
// home page and a topic's "written by" once each).
func TestTheTrafficPageNeverAsksForEverythingHeld(t *testing.T) {
	src := sampleTraffic()
	// Enough rows that the table is cut: the heading still counts them all.
	for i := range 300 {
		src.events = append(src.events, TrafficEvent{ID: "x" + strings.Repeat("i", i%7) + string(rune('a'+i%26)) + string(rune('a'+i/26)),
			Seq: uint64(100 + i), First: tt0.Add(-time.Hour), Time: tt0.Add(time.Duration(i) * time.Millisecond), Count: 2,
			ClientID: "bulk", API: "Metadata", Remote: "127.0.0.1:50009", Topics: []string{"bulk-topic"}})
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	for _, path := range []string{
		"/_console/traffic", "/_console/traffic/live", "/_console/traffic?q=bill", "/_console/traffic?kind=errors",
		"/_console/traffic?topic=orders", "/_console/traffic?view=clients", "/_console/",
	} {
		rec := req(t, c, "GET", path, nil)
		if rec.Code != 200 {
			t.Fatalf("%s: status %d", path, rec.Code)
		}
		if src.whole != 0 {
			t.Errorf("%s asked the recorder for everything it holds %d times", path, src.whole)
			src.whole = 0
		}
	}

	// The counts the page prints come from the one question, and are of everything.
	body := req(t, c, "GET", "/_console/traffic?topic=bulk-topic", nil).Body.String()
	if !strings.Contains(body, `<span class="b">600</span> of 606 requests match`) {
		t.Errorf("the heading does not count what matched: %q", between(body, `id="match-count"`, `</span></span>`))
	}
	if n := strings.Count(body, `id="rq-`); n > trafficRows+1 {
		t.Errorf("%d rows drawn, more than the %d the table shows", n, trafficRows)
	}
}

// How far back "written by" reaches is the start of the oldest request held.
func TestWrittenByReachesBackToTheOldestRequestHeld(t *testing.T) {
	src := sampleTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	short, long := c.ringText(tt0.Add(90 * time.Second))
	if short != "from the last 1m 30s of requests" {
		t.Errorf("short = %q", short)
	}
	if !strings.Contains(long, "the last 2,000 requests") || !strings.Contains(long, "They reach back 1m 30s") {
		t.Errorf("long = %q", long)
	}
	if src.whole != 0 {
		t.Errorf("it asked for everything held %d times", src.whole)
	}
	src.events = nil
	if short, _ := c.ringText(tt0); short != "from recent requests; none are kept yet" {
		t.Errorf("with nothing held: %q", short)
	}
}
