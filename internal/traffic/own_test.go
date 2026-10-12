package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// own marks an observation as coming over one of the process's own connections.
func own(o *protocol.Observation) { o.Own = true }

// The console's client ids, as a client that is not the console may also call itself.
var consoleNames = []string{"doze-kafka-console", "doze-kafka-console-fetch", "doze-kafka-console-trace"}

// What is left out of Traffic is what came over the process's own connections, which
// the connection says and a client cannot. A client that names itself as the console
// does is a client like any other: it has rows, it is listed, it is counted. (It was
// invisible: the console was known by its client id, and anyone can send one.)
func TestAClientThatCallsItselfTheConsoleIsListed(t *testing.T) {
	r := New(50, nil)
	for i, id := range consoleNames {
		conn := uint64(i + 1)
		r.ConnOpened(conn, "203.0.113.9:4000")
		r.Request(obs(conn, id, kmsg.Produce, time.Duration(i)*time.Second, func(o *protocol.Observation) {
			o.Topics, o.Kind = []string{"orders"}, "produce"
		}))
	}
	rows := r.Events(Filter{}, 0)
	if len(rows) != len(consoleNames) {
		t.Fatalf("%d rows, want one for each of %v", len(rows), consoleNames)
	}
	clients := r.Clients()
	if len(clients) != len(consoleNames) {
		t.Fatalf("%d clients, want %d", len(clients), len(consoleNames))
	}
	for i, c := range clients {
		if c.ClientID != consoleNames[i] || !c.Produces || c.Requests != 1 {
			t.Errorf("client %d = %+v", i, c)
		}
	}
	if got := r.Requests(); got != uint64(len(consoleNames)) {
		t.Errorf("counted %d requests, want %d", got, len(consoleNames))
	}
}

// And the process's own requests are left out whatever they are called: no row, no
// client, no count, no description.
func TestTheProcessesOwnRequestsAreLeftOutWhateverTheyAreCalled(t *testing.T) {
	r := New(50, nil)
	described := 0
	r.describe = func(q kmsg.Request, resp kmsg.Response, resolve func([16]byte) string) protocol.Facts {
		described++
		return protocol.DescribeFactsNamed(q, resp, resolve)
	}
	for i, id := range append([]string{"billing", "", "anything"}, consoleNames...) {
		conn := uint64(i + 1)
		r.ConnOpened(conn, "127.0.0.1:5000")
		r.Request(obs(conn, id, kmsg.Fetch, time.Duration(i)*time.Second, own, func(o *protocol.Observation) {
			o.Req, o.Group, o.Kind = kmsg.NewPtrFetchRequest(), "g", "fetch"
		}))
	}
	if rows := r.Events(Filter{}, 0); len(rows) != 0 {
		t.Errorf("rows = %+v, want none", rows)
	}
	if clients := r.Clients(); len(clients) != 0 {
		t.Errorf("clients = %+v, want none", clients)
	}
	if r.Requests() != 0 || described != 0 {
		t.Errorf("counted %d, described %d; want 0 and 0", r.Requests(), described)
	}
}
