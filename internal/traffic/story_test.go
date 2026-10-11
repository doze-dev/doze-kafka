package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// A group's requests go under the rebalance they belong to, and the rows before it are
// the generation before.
func TestStoryNestsRequestsUnderTheirRebalance(t *testing.T) {
	r := New(100, nil)
	now := t0
	r.now = func() time.Time { return now }
	at := func(d time.Duration) { now = t0.Add(d) }
	r.ConnOpened(1, "a")
	grp := func(o *protocol.Observation) { o.Group, o.Kind = "g-depot", "groups" }

	// Generation 6: two members, steady beats.
	at(0)
	r.GroupEvent("g-depot", "generation", "m1", 6, "2 members", 2, "")
	for i := 1; i <= 3; i++ {
		r.Request(obs(1, "depot", kmsg.Heartbeat, time.Duration(i)*3*time.Second, idle("g-depot"), grp))
	}
	// A member joins at +30 s: the rebalance, the join and sync, the new generation 6 s later.
	at(30 * time.Second)
	r.GroupEvent("g-depot", "rebalance", "m3", 6, "a member joined", 2, "a member joined")
	r.Request(obs(1, "depot", kmsg.JoinGroup, 30*time.Second, grp))
	at(36 * time.Second)
	r.GroupEvent("g-depot", "generation", "m1", 7, "3 members, leader m1", 3, "")
	r.Request(obs(1, "depot", kmsg.SyncGroup, 36*time.Second+400*time.Millisecond, grp))
	r.Request(obs(1, "depot", kmsg.OffsetCommit, 40*time.Second, grp))
	r.Request(obs(1, "depot", kmsg.Fetch, 41*time.Second, func(o *protocol.Observation) { o.Topics, o.Kind = []string{"dispatch"}, "fetch" }))

	s := r.Story("g-depot")
	if s.Protocol != "classic" || len(s.Rebalances) != 1 {
		t.Fatalf("protocol %q, %d rebalances: %+v", s.Protocol, len(s.Rebalances), s.Rebalances)
	}
	latest := s.Rebalances[0]
	if !latest.Done || latest.From != 6 || latest.To != 7 || latest.Members != 3 || latest.MembersBefore != 2 ||
		latest.Cause != "a member joined" || latest.End.Sub(latest.Start) != 6*time.Second {
		t.Errorf("the rebalance = %+v", latest)
	}
	var apis []string
	for _, e := range latest.Rows {
		apis = append(apis, e.API)
	}
	if len(apis) != 4 || apis[0] != "Fetch" || apis[3] != "JoinGroup" {
		t.Errorf("rows under the rebalance (newest first) = %v, want Fetch (a member's), OffsetCommit, SyncGroup, JoinGroup", apis)
	}
	// Generation 6's own opening, and its beats, are the generation before.
	if len(s.Before) != 1 || s.Before[0].API != "Heartbeat" || s.Before[0].Count != 3 {
		t.Errorf("before = %+v, want one folded row of three heartbeats", s.Before)
	}
	if s.Requests != 7 || s.Folded != 2 {
		t.Errorf("%d requests, %d folded, want 7 and 2", s.Requests, s.Folded)
	}
	if len(s.Generations) != 2 || s.Generations[1].Gen != 7 || s.Generations[1].Cause != "a member joined" || s.Generations[1].Members != 3 {
		t.Errorf("generations = %+v", s.Generations)
	}
	if len(s.Topics) != 1 || s.Topics[0] != "dispatch" {
		t.Errorf("topics = %v", s.Topics)
	}
}

func TestStoryOfARebalanceInProgressAndOfEpochs(t *testing.T) {
	r := New(10, nil)
	r.GroupEvent("g", "generation", "m", 1, "", 1, "")
	r.GroupEvent("g", "rebalance", "m2", 1, "a member joined", 1, "a member joined")
	s := r.Story("g")
	if len(s.Rebalances) != 1 || s.Rebalances[0].Done || s.Rebalances[0].Cause != "a member joined" || s.Rebalances[0].From != 1 {
		t.Errorf("rebalances = %+v, want one in progress, after generation 1", s.Rebalances)
	}
	if len(s.Generations) != 1 {
		t.Errorf("generations = %+v, want the first only: the second has not opened", s.Generations)
	}

	k := New(10, nil)
	k.GroupEvent("kg", "joined", "m", 1, "client c", 1, "")
	k.GroupEvent("kg", "epoch", "", 2, "a member joined; 1 member reassigned", 1, "a member joined")
	ks := k.Story("kg")
	if ks.Protocol != "848" || len(ks.Rebalances) != 1 || !ks.Rebalances[0].Done || ks.Rebalances[0].To != 2 || ks.Rebalances[0].Cause != "a member joined" {
		t.Errorf("KIP-848 story = %+v", ks)
	}
	if empty := New(10, nil).Story("none"); len(empty.Rebalances) != 0 || empty.Protocol != "" {
		t.Errorf("a group nobody has heard of has a story: %+v", empty)
	}
}
