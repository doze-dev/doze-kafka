package broker_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// Raw ConsumerGroupHeartbeat tests: a real client's reconciliation loop hides
// the exact window these bugs lived in, so the member's side is played by hand.

type cgMember struct {
	cl    *kgo.Client
	group string
	id    string
	epoch int32
	held  map[[16]byte][]int32 // what this member currently believes it owns.
}

func topicIDOf(t *testing.T, ctx context.Context, cl *kgo.Client, name string) [16]byte {
	t.Helper()
	n := name
	req := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &n
	req.Topics = append(req.Topics, mt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("metadata for %q: %v %+v", name, err, resp)
	}
	return resp.Topics[0].TopicID
}

// heartbeat sends one ConsumerGroupHeartbeat as a member that reports what it
// holds, applies any assignment the broker returns, and returns the response.
func (m *cgMember) heartbeat(t *testing.T, ctx context.Context, subscribe []string, reportOwned bool) *kmsg.ConsumerGroupHeartbeatResponse {
	t.Helper()
	r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	r.Group, r.MemberID, r.MemberEpoch = m.group, m.id, m.epoch
	r.RebalanceTimeoutMillis = 60_000
	r.ServerAssignor = kmsg.StringPtr("uniform")
	if subscribe != nil {
		r.SubscribedTopicNames = subscribe
	}
	if reportOwned {
		r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
		for id, ps := range m.held {
			if len(ps) == 0 {
				continue
			}
			rt := kmsg.NewConsumerGroupHeartbeatRequestTopic()
			rt.TopicID, rt.Partitions = id, slices.Clone(ps)
			r.Topics = append(r.Topics, rt)
		}
	}
	resp, err := r.RequestWith(ctx, m.cl)
	if err != nil {
		t.Fatalf("ConsumerGroupHeartbeat: %v", err)
	}
	if resp.ErrorCode != 0 {
		return resp
	}
	if resp.MemberID != nil {
		m.id = *resp.MemberID
	}
	m.epoch = resp.MemberEpoch
	if resp.Assignment != nil { // the full set this member should now own.
		m.held = map[[16]byte][]int32{}
		for _, at := range resp.Assignment.Topics {
			ps := slices.Clone(at.Partitions)
			slices.Sort(ps)
			m.held[at.TopicID] = ps
		}
	}
	return resp
}

func (m *cgMember) holds(topic [16]byte) []int32 { return m.held[topic] }

func newCGMember(t *testing.T, addr, group, id string) *cgMember {
	t.Helper()
	cl := newClient(t, addr)
	t.Cleanup(cl.Close)
	return &cgMember{cl: cl, group: group, id: id, held: map[[16]byte][]int32{}}
}

func overlap(a, b []int32) []int32 {
	var out []int32
	for _, p := range a {
		if slices.Contains(b, p) {
			out = append(out, p)
		}
	}
	return out
}

// A member that does not name itself would share the id "" with every other such
// member and be handed the same partitions. The broker mints one.
func TestGroupKIP848_BrokerMintsAMemberIDWhenNoneIsGiven(t *testing.T) {
	c := openRaw(t, startBroker(t, 4))
	join := func(version int16) *kmsg.ConsumerGroupHeartbeatResponse {
		r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		r.SetVersion(version)
		r.Group, r.RebalanceTimeoutMillis = "mint", 60_000
		r.SubscribedTopicNames = []string{"t"}
		var resp kmsg.ConsumerGroupHeartbeatResponse
		if !c.do(r, &resp) {
			t.Fatal("connection closed")
		}
		return &resp
	}
	// v0: the broker assigns the id.
	ra, rb := join(0), join(0)
	if ra.ErrorCode != 0 || rb.ErrorCode != 0 {
		t.Fatalf("join errors: %d %d", ra.ErrorCode, rb.ErrorCode)
	}
	if ra.MemberID == nil || rb.MemberID == nil || *ra.MemberID == "" || *ra.MemberID == *rb.MemberID {
		t.Fatalf("minted ids = %v and %v; want two distinct non-empty ids", ra.MemberID, rb.MemberID)
	}
	// v1: the client generates it; an empty one is a malformed request.
	if r := join(1); r.ErrorCode != 42 {
		t.Errorf("a v1 join with no member id: code %d, want INVALID_REQUEST (42)", r.ErrorCode)
	}
}

// The double-ownership window. A is told it owns every partition; B joins before
// A has had a chance to report anything. Judging ownership by A's report alone
// (still empty) handed B partitions A believed it owned. Playing both members by
// hand, no partition may ever be in both hands.
func TestGroupKIP848_NoPartitionIsHeldByTwoMembers(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	defer admin.Close()
	mustCreateTopic(t, ctx, admin, "t", 4)
	tid := topicIDOf(t, ctx, admin, "t")

	a, b := newCGMember(t, addr, "split", "member-a"), newCGMember(t, addr, "split", "member-b")
	check := func(step string) {
		t.Helper()
		if both := overlap(a.holds(tid), b.holds(tid)); len(both) > 0 {
			t.Fatalf("after %s: partitions %v are held by both members (A=%v B=%v)", step, both, a.holds(tid), b.holds(tid))
		}
	}

	a.heartbeat(t, ctx, []string{"t"}, false)
	if got := a.holds(tid); !slices.Equal(got, []int32{0, 1, 2, 3}) {
		t.Fatalf("a lone member should be given every partition, got %v", got)
	}
	check("A joins")

	b.heartbeat(t, ctx, []string{"t"}, false) // B joins BEFORE A has reported anything.
	check("B joins")
	if got := b.holds(tid); len(got) != 0 {
		t.Fatalf("B was handed %v while A believes it owns every partition", got)
	}

	for round := 1; round <= 8; round++ {
		a.heartbeat(t, ctx, nil, true)
		check("A heartbeat")
		b.heartbeat(t, ctx, nil, true)
		check("B heartbeat")
	}
	// Two each, all four covered. Which two is the assignor's business (it keeps
	// what A already had where it can).
	all := append(slices.Clone(a.holds(tid)), b.holds(tid)...)
	slices.Sort(all)
	if len(a.holds(tid)) != 2 || len(b.holds(tid)) != 2 || !slices.Equal(all, []int32{0, 1, 2, 3}) {
		t.Fatalf("the split never converged: A=%v B=%v, want two each covering 0-3", a.holds(tid), b.holds(tid))
	}
}

// Start the consumer, then create the topic — the usual order in local
// development. The assignor ran only when membership changed, so a member
// subscribed to a topic that did not exist yet idled forever.
func TestGroupKIP848_TopicCreatedAfterSubscribingIsAssigned(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	defer admin.Close()

	m := newCGMember(t, addr, "late", "member-late")
	m.heartbeat(t, ctx, []string{"late"}, false)
	if len(m.held) != 0 {
		t.Fatalf("assigned %v for a topic that does not exist", m.held)
	}

	mustCreateTopic(t, ctx, admin, "late", 2)
	tid := topicIDOf(t, ctx, admin, "late")
	for i := 0; i < 5 && len(m.holds(tid)) == 0; i++ {
		m.heartbeat(t, ctx, nil, true)
	}
	if got := m.holds(tid); !slices.Equal(got, []int32{0, 1}) {
		t.Fatalf("after the topic was created the member holds %v, want [0 1]", got)
	}

	// And when it is deleted the member is told to let go.
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{"late"}
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.Topic = kmsg.StringPtr("late")
	req.Topics = append(req.Topics, dt)
	if _, err := req.RequestWith(ctx, admin); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5 && len(m.holds(tid)) > 0; i++ {
		m.heartbeat(t, ctx, nil, true)
	}
	if got := m.holds(tid); len(got) != 0 {
		t.Fatalf("the member still holds %v of a deleted topic", got)
	}
}

// A group id is one protocol or the other; sharing it gave every partition to
// both sets of members and listed the group twice.
func TestGroupCoordinators_ShareNoGroupID(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()

	// A live classic group refuses a KIP-848 member.
	stableGroupOfOne(t, ctx, cl, "classic-first", testSessionMs, testRebalanceMsDefault)
	m := newCGMember(t, addr, "classic-first", "new-protocol-member")
	if resp := m.heartbeat(t, ctx, []string{"t"}, false); resp.ErrorCode != int16(protocol.ErrGroupIDNotFound) {
		t.Errorf("KIP-848 join into a live classic group: code %d, want GROUP_ID_NOT_FOUND (%d)", resp.ErrorCode, protocol.ErrGroupIDNotFound)
	}

	// A live KIP-848 group refuses a classic member.
	m2 := newCGMember(t, addr, "consumer-first", "member-x")
	if resp := m2.heartbeat(t, ctx, []string{"t"}, false); resp.ErrorCode != 0 {
		t.Fatalf("KIP-848 join: code %d", resp.ErrorCode)
	}
	resp, err := joinRequest("consumer-first", "classic-member", testSessionMs, testRebalanceMsDefault).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != int16(protocol.ErrInconsistentGroupProtocol) {
		t.Errorf("classic join into a live KIP-848 group: code %d, want INCONSISTENT_GROUP_PROTOCOL (%d)", resp.ErrorCode, protocol.ErrInconsistentGroupProtocol)
	}

	// Neither id is listed twice.
	seen := map[string]int{}
	lr, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range lr.Groups {
		seen[g.Group]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("group %q is listed %d times", id, n)
		}
	}
}
