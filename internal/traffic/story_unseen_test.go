package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// A generation the recorder never saw begin has no block: a request that names it is one
// of the rows before the oldest rebalance. A detail that names no usable generation is
// placed by time like any other.
func TestARequestNamingAnUnseenGenerationIsBefore(t *testing.T) {
	r := New(100, nil)
	now := t0
	r.now = func() time.Time { return now }
	r.ConnOpened(1, "a")
	grp := func(o *protocol.Observation) { o.Group, o.Kind = "g", "groups" }
	det := func(s string) func(*protocol.Observation) { return func(o *protocol.Observation) { o.Detail = s } }

	now = t0.Add(time.Second)
	r.GroupEvent("g", "generation", "m1", 5, "1 member", 1, "") // no rebalance before it.
	r.Request(obs(1, "b", kmsg.JoinGroup, 10*time.Second, grp, det("generation 5")))
	r.Request(obs(1, "b", kmsg.JoinGroup, 11*time.Second, grp, det("generation ")))
	r.Request(obs(1, "b", kmsg.SyncGroup, 12*time.Second, grp, det("generation 0")))
	now = t0.Add(20 * time.Second)
	r.GroupEvent("g", "rebalance", "m2", 5, "a member joined", 1, "a member joined")
	now = t0.Add(25 * time.Second)
	r.GroupEvent("g", "generation", "m1", 6, "2 members", 2, "")

	s := r.Story("g")
	if len(s.Rebalances) != 1 || len(s.Rebalances[0].Rows) != 0 {
		t.Fatalf("rebalances = %+v", s.Rebalances)
	}
	if len(s.Before) != 3 {
		t.Errorf("before = %d rows, want the three requests", len(s.Before))
	}
}
