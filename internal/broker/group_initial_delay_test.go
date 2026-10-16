package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// The first rebalance of an empty classic group waits the initial delay, and waits
// another when a member joined during it, as Kafka does (recorded:
// classic-initial-rebalance-delay): with a 500 ms delay and a second member at 250 ms,
// the group is still forming at 750 ms and has formed by 1.4 s, with the first member
// as its leader.
func TestGroupClassic_InitialDelayWaitsAgainForAMemberThatJoinedDuringIt(t *testing.T) {
	addr := startBrokerWith(t, 4, func(c *config.Config) { c.GroupInitialRebalanceDelay = 500 * time.Millisecond })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	defer admin.Close()

	clA, clB := newClient(t, addr), newClient(t, addr)
	defer clA.Close()
	defer clB.Close()
	join := func(cl *kgo.Client, member string) (*kmsg.JoinGroupResponse, error) {
		r := kmsg.NewPtrJoinGroupRequest()
		r.Group, r.MemberID, r.ProtocolType = "delay-g", member, "consumer"
		r.SessionTimeoutMillis, r.RebalanceTimeoutMillis = 30000, 30000
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name, p.Metadata = "range", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		r.Protocols = append(r.Protocols, p)
		return r.RequestWith(ctx, cl)
	}
	memberID := func() string {
		r, err := join(admin, "")
		if err != nil || r.ErrorCode != 79 {
			t.Fatalf("join without an id: %v %+v", err, r)
		}
		return r.MemberID
	}
	state := func() string {
		r := kmsg.NewPtrDescribeGroupsRequest()
		r.Groups = []string{"delay-g"}
		resp, err := r.RequestWith(ctx, admin)
		if err != nil || len(resp.Groups) != 1 {
			t.Fatalf("describe: %v %+v", err, resp)
		}
		return resp.Groups[0].State
	}
	type answer struct {
		r   *kmsg.JoinGroupResponse
		err error
	}
	a, b := memberID(), memberID()
	start := time.Now()
	at := func(d time.Duration) { time.Sleep(time.Until(start.Add(d))) }
	answers := make(chan answer, 2)
	go func() { r, err := join(clA, a); answers <- answer{r, err} }()
	at(250 * time.Millisecond)
	go func() { r, err := join(clB, b); answers <- answer{r, err} }()
	at(750 * time.Millisecond)
	if s := state(); s != "PreparingRebalance" {
		t.Errorf("at 750 ms, a member having joined during the first 500 ms: %s, want PreparingRebalance", s)
	}
	at(1400 * time.Millisecond)
	if s := state(); s != "CompletingRebalance" {
		t.Errorf("at 1.4 s: %s, want CompletingRebalance", s)
	}
	for range 2 {
		ans := <-answers
		if ans.err != nil || ans.r.ErrorCode != 0 || ans.r.LeaderID != a {
			t.Errorf("join: %v %+v, want the first member (%s) as leader", ans.err, ans.r, a)
		}
	}
}
