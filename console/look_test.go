package console

import (
	"strings"
	"testing"
	"time"
)

// A client is also raised when most of what it asked in five minutes was an error,
// though its latest request went well. Then its streak is 0, and the page must say
// what is true (how many of how many), not "refused 0 times in a row".
func TestClientMostlyRefusedSaysHowManyOfHowMany(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{
		events: []TrafficEvent{
			{ID: "r2", Seq: 2, First: now, Time: now, Count: 1, Conn: 1, ClientID: "a", Remote: "r1", API: "CreateTopics", Error: "TOPIC_ALREADY_EXISTS", Class: "refused"},
		},
		// Five requests, three refused, the latest one fine.
		clients: []TrafficClient{{Conn: 1, Remote: "r1", ClientID: "a", Opened: now.Add(-time.Hour), Last: now, Requests: 5, Req5m: 5, Err5m: 3, ClientReq5m: 5, ClientErr5m: 3}},
	}
	if !needsALook(src.clients[0]) {
		t.Fatal("the rule no longer raises three errors of five")
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	view := req(t, c, "GET", "/_console/traffic?view=clients", nil).Body.String()
	callout := between(view, `id="attention"`, "</div>")
	for _, want := range []string{"1 client needs a look", "a had 3 errors among its 5 requests in the last five minutes", "See the refusals"} {
		if !strings.Contains(view, want) {
			t.Errorf("the clients view does not say %q:\n%s", want, callout)
		}
	}
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	list := between(body, `id="clients-list"`, "</aside>")
	if !strings.Contains(list, "3 of 5 requests were errors in the last 5 minutes") {
		t.Errorf("the clients list does not say how many:\n%s", list)
	}
	for _, page := range []string{view, body} {
		for _, lie := range []string{"0 times in a row", "last 0 requests", "times in a row"} {
			if strings.Contains(page, lie) {
				t.Errorf("the page says %q of a client whose latest request went well", lie)
			}
		}
	}
}

// The sentence for each way a client comes to need a look. The streak is the client's
// own (the recorder's ClientStreak), and its word is the class of the no: an ACL's is
// "denied" (A2 of the foundation; the board's sample says "refused" of one).
func TestLookWords(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cl         TrafficClient
		head, note string
	}{
		{"a streak of refusals", TrafficClient{ClientStreak: 4, ClientLastClass: "refused", Req5m: 4, Err5m: 4}, "has been refused 4 times in a row", "last 4 requests refused"},
		{"a streak of denials", TrafficClient{ClientStreak: 4, ClientLastClass: "denied", Req5m: 4, Err5m: 4}, "has been denied 4 times in a row", "last 4 requests denied"},
		{"a streak of failures", TrafficClient{ClientStreak: 3, ClientLastClass: "failed", Req5m: 3, Err5m: 3}, "has failed 3 times in a row", "last 3 requests failed"},
		{"the ratio, latest request fine", TrafficClient{Req5m: 5, Err5m: 3}, "had 3 errors among its 5 requests in the last five minutes", "3 of 5 requests were errors in the last 5 minutes"},
		{"the ratio, a short streak", TrafficClient{ClientStreak: 2, Req5m: 6, Err5m: 4}, "had 4 errors among its 6 requests in the last five minutes", "4 of 6 requests were errors in the last 5 minutes"},
	} {
		if head, note := lookWords(tc.cl); head != tc.head || note != tc.note {
			t.Errorf("%s: %q / %q, want %q / %q", tc.name, head, note, tc.head, tc.note)
		}
	}
}

// A2: a client of two connections whose one connection has three errors in a row and
// whose other has gone on fine is not "refused 3 times in a row": the client's streak
// is its own requests in order, which the recorder counts (ClientStreak), not the
// largest of its connections'.
func TestAClientOfSeveralConnectionsSaysWhatIsTrue(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{clients: []TrafficClient{
		{Conn: 1, Remote: "r1", ClientID: "app", Opened: now.Add(-time.Hour), Last: now.Add(-time.Second), Requests: 4, Streak: 3, LastError: "TOPIC_AUTHORIZATION_FAILED", Req5m: 4, Err5m: 3, ClientReq5m: 104, ClientErr5m: 3},
		{Conn: 2, Remote: "r2", ClientID: "app", Opened: now.Add(-time.Hour), Last: now, Requests: 100, Req5m: 100, Err5m: 0, ClientReq5m: 104, ClientErr5m: 3},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	view := req(t, c, "GET", "/_console/traffic?view=clients", nil).Body.String()
	if strings.Contains(view, "in a row") || strings.Contains(view, `id="attention"`) {
		t.Errorf("the page raises a client whose latest request went well:\n%s", between(view, `id="attention"`, "</div>"))
	}
	// The client's own streak, the same on each of its connections.
	for i := range src.clients {
		src.clients[i].ClientStreak, src.clients[i].ClientLastError, src.clients[i].ClientLastClass = 3, "TOPIC_AUTHORIZATION_FAILED", "denied"
	}
	view = req(t, c, "GET", "/_console/traffic?view=clients", nil).Body.String()
	if want := "app has been denied 3 times in a row"; !strings.Contains(view, want) {
		t.Errorf("the clients view does not say %q:\n%s", want, between(view, `id="attention"`, "</div>"))
	}
}
