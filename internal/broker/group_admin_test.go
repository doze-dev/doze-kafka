package broker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// Committed offsets are the durable consumer state, but the group objects that
// listings are built from live in memory. After a restart a group with 40
// committed offsets vanished from every listing even though its consumers would
// resume from them.
func TestGroupAdmin_GroupsWithOffsetsSurviveARestart(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	stop := runBrokerAt(t, addr, dir, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	mustCreateTopic(t, ctx, cl, "t", 1)
	commitOffset(t, ctx, cl, "billing", "t", 40)
	cl.Close()
	stop()

	stop = runBrokerAt(t, addr, dir, 4)
	defer stop()
	cl = newClient(t, addr)
	defer cl.Close()

	if got := listedGroups(t, ctx, cl); got["billing"] != "Empty" || len(got) != 1 {
		t.Fatalf("after a restart ListGroups = %v, want just billing: Empty", got)
	}
	dg := kmsg.NewPtrDescribeGroupsRequest()
	dg.Groups = []string{"billing", "never-existed"}
	resp, err := dg.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for _, g := range resp.Groups {
		states[g.Group] = g.State
	}
	if states["billing"] != "Empty" || states["never-existed"] != "Dead" {
		t.Fatalf("DescribeGroups states = %v, want billing Empty (it has offsets) and never-existed Dead", states)
	}
}

func consumerGroupState(t *testing.T, ctx context.Context, m *cgMember) string {
	t.Helper()
	r := kmsg.NewPtrConsumerGroupDescribeRequest()
	r.Groups = []string{m.group}
	resp, err := r.RequestWith(ctx, m.cl)
	if err != nil || len(resp.Groups) != 1 || resp.Groups[0].ErrorCode != 0 {
		t.Fatalf("ConsumerGroupDescribe: %v %+v", err, resp)
	}
	return resp.Groups[0].State
}

// The state used to be hard-coded "Stable". It is Reconciling while partitions
// are still moving between members — which is exactly when someone is looking.
func TestGroupAdmin_ConsumerGroupStateIsReal(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	defer admin.Close()
	mustCreateTopic(t, ctx, admin, "t", 4)

	a, b := newCGMember(t, addr, "states", "member-a"), newCGMember(t, addr, "states", "member-b")
	a.heartbeat(t, ctx, []string{"t"}, false)
	if s := consumerGroupState(t, ctx, a); s != "Reconciling" {
		t.Errorf("right after joining, before the member has reported what it holds: state %q, want Reconciling", s)
	}
	a.heartbeat(t, ctx, nil, true)
	if s := consumerGroupState(t, ctx, a); s != "Stable" {
		t.Errorf("a lone member holding its whole target: state %q, want Stable", s)
	}

	b.heartbeat(t, ctx, []string{"t"}, false)
	if s := consumerGroupState(t, ctx, a); s != "Reconciling" {
		t.Errorf("a second member joined and partitions must move: state %q, want Reconciling", s)
	}
	for range 8 {
		a.heartbeat(t, ctx, nil, true)
		b.heartbeat(t, ctx, nil, true)
	}
	if s := consumerGroupState(t, ctx, a); s != "Stable" {
		t.Errorf("after the handoff completed: state %q, want Stable", s)
	}

	// ListGroups agrees.
	if got := listedGroups(t, ctx, admin); got["states"] != "Stable" {
		t.Errorf("ListGroups state = %q, want Stable", got["states"])
	}
}

func TestGroupAdmin_DeleteGroups(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "t", 1)

	commitOffset(t, ctx, cl, "idle", "t", 5) // offsets only: deletable.
	stableGroupOfOne(t, ctx, cl, "busy", testSessionMs, testRebalanceMsDefault)

	req := kmsg.NewPtrDeleteGroupsRequest()
	req.Groups = []string{"idle", "busy", "nonexistent"}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	codes := map[string]int16{}
	for _, g := range resp.Groups {
		codes[g.Group] = g.ErrorCode
	}
	if codes["idle"] != 0 {
		t.Errorf("deleting an idle group with offsets: code %d, want 0", codes["idle"])
	}
	if codes["busy"] != int16(protocol.ErrNonEmptyGroup) {
		t.Errorf("deleting a group with a live member: code %d, want NON_EMPTY_GROUP (%d)", codes["busy"], protocol.ErrNonEmptyGroup)
	}
	if codes["nonexistent"] != int16(protocol.ErrGroupIDNotFound) {
		t.Errorf("deleting an unknown group: code %d, want GROUP_ID_NOT_FOUND (%d)", codes["nonexistent"], protocol.ErrGroupIDNotFound)
	}

	got := listedGroups(t, ctx, cl)
	if _, ok := got["idle"]; ok {
		t.Errorf("the deleted group is still listed: %v", got)
	}
	if _, ok := got["busy"]; !ok {
		t.Errorf("the refused group should still be listed: %v", got)
	}
	if off := fetchCommitted(t, ctx, cl, "idle", "t"); off != -1 {
		t.Errorf("a deleted group kept its committed offset %d", off)
	}
}

func TestGroupAdmin_OffsetDelete(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "t", 2)

	commitOffset(t, ctx, cl, "g", "t", 9)
	od := func(group, topic string, partitions ...int32) *kmsg.OffsetDeleteResponse {
		r := kmsg.NewPtrOffsetDeleteRequest()
		r.Group = group
		rt := kmsg.NewOffsetDeleteRequestTopic()
		rt.Topic = topic
		for _, p := range partitions {
			rp := kmsg.NewOffsetDeleteRequestTopicPartition()
			rp.Partition = p
			rt.Partitions = append(rt.Partitions, rp)
		}
		r.Topics = append(r.Topics, rt)
		resp, err := r.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	if r := od("nobody", "t", 0); r.ErrorCode != int16(protocol.ErrGroupIDNotFound) {
		t.Errorf("unknown group: code %d, want GROUP_ID_NOT_FOUND", r.ErrorCode)
	}
	r := od("g", "t", 0, 7) // 7 is not a partition of t.
	if r.ErrorCode != 0 {
		t.Fatalf("top-level code %d", r.ErrorCode)
	}
	got := map[int32]int16{}
	for _, p := range r.Topics[0].Partitions {
		got[p.Partition] = p.ErrorCode
	}
	if got[0] != 0 || got[7] != int16(protocol.ErrUnknownTopicOrPart) {
		t.Errorf("per-partition codes = %v, want 0 for a real partition and UNKNOWN_TOPIC_OR_PARTITION for 7", got)
	}
	if off := fetchCommitted(t, ctx, cl, "g", "t"); off != -1 {
		t.Errorf("the offset survived OffsetDelete: %d", off)
	}

	// With a live member the group is treated as subscribed.
	stableGroupOfOne(t, ctx, cl, "live", testSessionMs, testRebalanceMsDefault)
	commitOffset(t, ctx, cl, "live", "t", 3)
	r = od("live", "t", 0)
	if len(r.Topics) == 0 || r.Topics[0].Partitions[0].ErrorCode != int16(protocol.ErrGroupSubscribedToTopic) {
		t.Errorf("OffsetDelete on a group with live members: %+v, want GROUP_SUBSCRIBED_TO_TOPIC", r)
	}
}
