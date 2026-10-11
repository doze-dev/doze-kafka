package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// Two rebalances that begin within joinLead of each other (a cooperative-sticky member
// joins, then rejoins after the revoke): the requests that brought the first one about
// are under the first, not under the later one.
func TestTriggerRequestsOfTwoCloseRebalancesStayUnderTheFirst(t *testing.T) {
	r := New(100, nil)
	now := t0
	r.now = func() time.Time { return now }
	at := func(d time.Duration) { now = t0.Add(d) }
	r.ConnOpened(1, "a")
	grp := func(o *protocol.Observation) { o.Group, o.Kind = "g", "groups" }

	at(0)
	r.GroupEvent("g", "generation", "rb-b", 1, "1 member", 1, "")
	T := 20 * time.Second
	r.Request(obs(1, "rb-a", kmsg.FindCoordinator, T, grp))
	r.Request(obs(1, "rb-a", kmsg.JoinGroup, T, grp, func(o *protocol.Observation) {
		o.ErrorCode, o.Class = 79, protocol.ClassExpected
	}))
	r.Request(obs(1, "rb-a", kmsg.JoinGroup, T, grp))
	at(T)
	r.GroupEvent("g", "rebalance", "rb-a", 1, "a member joined", 1, "a member joined")
	at(T + time.Second)
	r.GroupEvent("g", "generation", "rb-b", 2, "2 members", 2, "")
	at(T + 1500*time.Millisecond)
	r.GroupEvent("g", "rebalance", "rb-a", 2, "a member joined", 2, "a member joined")
	at(T + 3*time.Second)
	r.GroupEvent("g", "generation", "rb-b", 3, "2 members", 2, "")

	s := r.Story("g")
	if len(s.Rebalances) != 2 {
		t.Fatalf("%d rebalances: %+v", len(s.Rebalances), s.Rebalances)
	}
	first, second := s.Rebalances[1], s.Rebalances[0] // newest first.
	if first.From != 1 || first.To != 2 || second.From != 2 || second.To != 3 {
		t.Fatalf("rebalances: %+v", s.Rebalances)
	}
	if len(first.Rows) != 3 || len(second.Rows) != 0 {
		t.Errorf("1->2 has %d rows, 2->3 has %d; want 3 and 0", len(first.Rows), len(second.Rows))
	}
}
