package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// The first of a run of idle requests is described with the lock let go. If the same
// connection's next one arrives meanwhile and starts the row, the first folds into it
// when it takes the lock again: one row, counted twice, never two rows for one run.
func TestARowMadeWhileAnotherWasBeingDescribedIsFoldedInto(t *testing.T) {
	r := New(50, nil)
	beat := kmsg.NewPtrHeartbeatRequest()
	second := obs(1, "app", kmsg.Heartbeat, time.Second, func(o *protocol.Observation) { o.Idle, o.Group = true, "g" })
	nested := false
	r.describe = func(q kmsg.Request, resp kmsg.Response, resolve func([16]byte) string) protocol.Facts {
		if !nested {
			nested = true
			r.Request(second) // it has no decoded request, so it is put on the list at once.
		}
		return protocol.DescribeFactsNamed(q, resp, resolve)
	}
	r.Request(obs(1, "app", kmsg.Heartbeat, 2*time.Second, func(o *protocol.Observation) { o.Idle, o.Group, o.Req = true, "g", beat }))
	got := r.Query(Query{})
	if got.Total != 1 || got.Requests != 2 || got.Held != 2 || got.Rows[0].Count != 2 || len(got.Rows[0].Ticks) != 2 {
		t.Fatalf("rows = %d, requests = %d, held = %d: %+v", got.Total, got.Requests, got.Held, got.Rows)
	}
	if cs := r.Clients(); len(cs) != 1 || cs[0].Requests != 2 {
		t.Errorf("clients = %+v", cs)
	}
}

// A request that closed its connection and came with no class is a failure of the
// broker's, and is counted as an error.
func TestAClosedConnectionWithNoClassIsAFailure(t *testing.T) {
	r := New(50, nil)
	r.Request(obs(1, "app", kmsg.Produce, 0, func(o *protocol.Observation) { o.Closed = "the broker's handler panicked" }))
	got := r.Events(Filter{ErrorsOnly: true}, 0)
	if len(got) != 1 || got[0].Class != protocol.ClassFailed || got[0].Error != "CONNECTION_CLOSED" {
		t.Fatalf("rows = %+v", got)
	}
	if cs := r.Clients(); cs[0].Streak != 1 || cs[0].LastError != "CONNECTION_CLOSED" {
		t.Errorf("client = %+v", cs[0])
	}
	// One that says its class keeps it.
	r.Request(obs(1, "app", kmsg.Produce, time.Second, func(o *protocol.Observation) {
		o.Closed, o.Class = "the request came before authentication", protocol.ClassDenied
	}))
	if got := r.Events(Filter{ErrorsOnly: true}, 1); got[0].Class != protocol.ClassDenied {
		t.Errorf("class = %q", got[0].Class)
	}
}
