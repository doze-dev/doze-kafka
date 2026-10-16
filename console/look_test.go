package console

import (
	"strings"
	"testing"
	"time"
)

// A client is also raised when most of what it asked in five minutes was refused,
// though its latest request went well. Then its streak is 0, and the page must say
// what is true (how many of how many), not "refused 0 times in a row".
func TestClientMostlyRefusedSaysHowManyOfHowMany(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{
		events: []TrafficEvent{
			{ID: "r2", Seq: 2, First: now, Time: now, Count: 1, Conn: 1, ClientID: "a", Remote: "r1", API: "CreateTopics", Error: "TOPIC_ALREADY_EXISTS", Class: "refused"},
		},
		// Five requests, three refused, the latest one fine.
		clients: []TrafficClient{{Conn: 1, Remote: "r1", ClientID: "a", Opened: now.Add(-time.Hour), Last: now, Requests: 5, Streak: 0, Req5m: 5, Err5m: 3}},
	}
	if !needsALook(src.clients[0]) {
		t.Fatal("the rule no longer raises three refusals of five")
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	view := req(t, c, "GET", "/_console/traffic?view=clients", nil).Body.String()
	callout := between(view, `id="attention"`, "</div>")
	for _, want := range []string{"1 client needs a look", "a was refused 3 of its 5 requests in the last five minutes", "See the refusals"} {
		if !strings.Contains(view, want) {
			t.Errorf("the clients view does not say %q:\n%s", want, callout)
		}
	}
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	list := between(body, `id="clients-list"`, "</aside>")
	if !strings.Contains(list, "3 of 5 requests refused in the last 5 minutes") {
		t.Errorf("the clients list does not say how many:\n%s", list)
	}
	for _, page := range []string{view, body} {
		for _, lie := range []string{"0 times in a row", "last 0 requests refused", "times in a row"} {
			if strings.Contains(page, lie) {
				t.Errorf("the page says %q of a client whose latest request went well", lie)
			}
		}
	}
}

// The sentence for each way a client comes to need a look, and for a client that is
// several connections: the streak is one connection's, the count is all of them.
func TestLookWords(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cl         TrafficClient
		conns      int
		head, note string
	}{
		{"a streak, as the board", TrafficClient{Streak: 4, Req5m: 4, Err5m: 4}, 1, "has been refused 4 times in a row", "last 4 requests refused"},
		{"a streak wins over the ratio", TrafficClient{Streak: 3, Req5m: 3, Err5m: 3}, 1, "has been refused 3 times in a row", "last 3 requests refused"},
		{"the ratio, latest request fine", TrafficClient{Streak: 0, Req5m: 5, Err5m: 3}, 1, "was refused 3 of its 5 requests in the last five minutes", "3 of 5 requests refused in the last 5 minutes"},
		{"the ratio, a short streak", TrafficClient{Streak: 2, Req5m: 6, Err5m: 4}, 1, "was refused 4 of its 6 requests in the last five minutes", "4 of 6 requests refused in the last 5 minutes"},
		{"a streak on one of several connections", TrafficClient{Streak: 3, Req5m: 40, Err5m: 3}, 2, "has been refused 3 times in a row on one of its 2 connections", "last 3 requests refused on one connection"},
		{"several connections, one mostly refused", TrafficClient{Streak: 1, Req5m: 104, Err5m: 3}, 2, "was refused 3 of its 104 requests in the last five minutes", "3 of 104 requests refused in the last 5 minutes"},
	} {
		if head, note := lookWords(tc.cl, tc.conns); head != tc.head || note != tc.note {
			t.Errorf("%s: %q / %q, want %q / %q", tc.name, head, note, tc.head, tc.note)
		}
	}
}

// A client of two connections, one of them mostly refused with its latest request
// fine: the row is raised by that connection, and the sentence is the client's count.
func TestAClientOfSeveralConnectionsSaysWhatIsTrue(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{clients: []TrafficClient{
		{Conn: 1, Remote: "r1", ClientID: "app", Opened: now.Add(-time.Hour), Last: now.Add(-time.Second), Requests: 4, Streak: 0, Req5m: 4, Err5m: 3},
		{Conn: 2, Remote: "r2", ClientID: "app", Opened: now.Add(-time.Hour), Last: now, Requests: 100, Streak: 0, Req5m: 100, Err5m: 0},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	view := req(t, c, "GET", "/_console/traffic?view=clients", nil).Body.String()
	if want := "app was refused 3 of its 104 requests in the last five minutes"; !strings.Contains(view, want) {
		t.Errorf("the clients view does not say %q:\n%s", want, between(view, `id="attention"`, "</div>"))
	}
	if strings.Contains(view, "in a row") {
		t.Error("the page speaks of a streak no connection has")
	}
	// A streak on one of the two is said to be one connection's.
	src.clients[0].Streak, src.clients[0].Err5m, src.clients[0].LastError = 3, 3, "TOPIC_AUTHORIZATION_FAILED"
	view = req(t, c, "GET", "/_console/traffic?view=clients", nil).Body.String()
	if want := "app has been refused 3 times in a row on one of its 2 connections"; !strings.Contains(view, want) {
		t.Errorf("the clients view does not say %q:\n%s", want, between(view, `id="attention"`, "</div>"))
	}
}
