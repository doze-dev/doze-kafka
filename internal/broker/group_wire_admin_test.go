package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// OffsetDelete names partitions of a topic: one that exists but holds no offset is
// deleted (nothing to do, code 0), one past the last or below the first is unknown.
func TestGroupAdmin_OffsetDeleteTellsAPartitionWithNoOffsetFromOneThatDoesNotExist(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	mustCreateTopic(t, ctx, cl, "two", 2)

	c := kmsg.NewPtrOffsetCommitRequest()
	c.Group, c.Generation = "h", -1
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = "two"
	cp := kmsg.NewOffsetCommitRequestTopicPartition()
	cp.Partition, cp.Offset, cp.LeaderEpoch = 1, 7, -1
	ct.Partitions = append(ct.Partitions, cp)
	c.Topics = append(c.Topics, ct)
	if r, err := c.RequestWith(ctx, cl); err != nil || r.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit: %v %+v", err, r)
	}

	r := kmsg.NewPtrOffsetDeleteRequest()
	r.Group = "h"
	rt := kmsg.NewOffsetDeleteRequestTopic()
	rt.Topic = "two"
	for _, p := range []int32{-1, 0, 1, 2} {
		rp := kmsg.NewOffsetDeleteRequestTopicPartition()
		rp.Partition = p
		rt.Partitions = append(rt.Partitions, rp)
	}
	r.Topics = append(r.Topics, rt)
	resp, err := r.RequestWith(ctx, cl)
	if err != nil || resp.ErrorCode != 0 {
		t.Fatalf("OffsetDelete: %v %+v", err, resp)
	}
	got := map[int32]int16{}
	for _, p := range resp.Topics[0].Partitions {
		got[p.Partition] = p.ErrorCode
	}
	want := map[int32]int16{-1: kerr.UnknownTopicOrPartition.Code, 0: 0, 1: 0, 2: kerr.UnknownTopicOrPartition.Code}
	for p, code := range want {
		if got[p] != code {
			t.Errorf("partition %d: code %d, want %d", p, got[p], code)
		}
	}
}

// DescribeGroups shows a group's protocol and its members' subscription metadata
// only while it is Stable: in CompletingRebalance, after the join and before the
// leader's sync, both are left out (doze's own contract, groupadmin.go; the claim
// behind it is groups R23c).
func TestGroupAdmin_DescribeGroupsShowsProtocolAndMetadataOnlyWhenStable(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)

	const g = "described"
	describe := func() kmsg.DescribeGroupsResponseGroup {
		t.Helper()
		r := kmsg.NewPtrDescribeGroupsRequest()
		r.Groups = []string{g}
		resp, err := r.RequestWith(ctx, cl)
		if err != nil || len(resp.Groups) != 1 || len(resp.Groups[0].Members) != 1 {
			t.Fatalf("DescribeGroups: %v %+v", err, resp)
		}
		return resp.Groups[0]
	}
	id := acquireID(t, ctx, cl, g)
	join := mustJoin(t, await(t, joinAsync(ctx, cl, g, id, testSessionMs, testRebalanceMsDefault), 5*time.Second, "join"), "join")

	d := describe()
	if d.State != "CompletingRebalance" || d.Protocol != "" || len(d.Members[0].ProtocolMetadata) != 0 {
		t.Errorf("after the join: state %q protocol %q metadata %q, want CompletingRebalance with neither", d.State, d.Protocol, d.Members[0].ProtocolMetadata)
	}

	s := await(t, syncAsync(ctx, cl, g, id, join.Generation, map[string]string{id: "assign-" + id}), 5*time.Second, "sync")
	if s.err != nil || s.resp.ErrorCode != 0 {
		t.Fatalf("sync: %v %+v", s.err, s.resp)
	}
	d = describe()
	if d.State != "Stable" || d.Protocol != "range" || string(d.Members[0].ProtocolMetadata) != "subscription-of-"+id || string(d.Members[0].MemberAssignment) != "assign-"+id {
		t.Errorf("after the sync: state %q protocol %q metadata %q assignment %q", d.State, d.Protocol, d.Members[0].ProtocolMetadata, d.Members[0].MemberAssignment)
	}
}
