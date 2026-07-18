package console

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// GroupSummary is one row in the consumer groups list.
type GroupSummary struct {
	ID       string
	Protocol string // "classic" or "848" (KIP-848, broker-driven)
	State    string
	Members  int
}

// GroupMember is one member of a group, with its assigned partitions.
type GroupMember struct {
	ID       string
	Assigned map[string][]int32 // topic -> partitions
}

// GroupDetail is a group's identity plus its members.
type GroupDetail struct {
	ID       string
	Protocol string
	State    string
	Assignor string // KIP-848 only ("uniform"/"range"); empty for classic
	Members  []GroupMember
}

// GroupOffset is one committed offset a group holds.
type GroupOffset struct {
	Topic     string
	Partition int32
	Committed int64
}

func normalizeProtocol(groupType string) string {
	if groupType == "classic" {
		return "classic"
	}
	return "848" // "consumer" (KIP-848) — doze-kafka's only other group type
}

// ListGroups returns every group, sorted by ID.
func (b *backend) ListGroups(ctx context.Context) ([]GroupSummary, error) {
	req := kmsg.NewPtrListGroupsRequest()
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(resp.ErrorCode)
	}
	out := make([]GroupSummary, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		out = append(out, GroupSummary{ID: g.Group, Protocol: normalizeProtocol(g.GroupType), State: g.GroupState})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	// Member counts need a per-group describe; ListGroups doesn't carry them.
	// Fine for a dev console's list pane — group counts are small.
	for i := range out {
		if d, err := b.DescribeGroup(ctx, out[i].ID); err == nil {
			out[i].Members = len(d.Members)
		}
	}
	return out, nil
}

// DescribeGroup returns a group's members and their assigned partitions,
// decoding whichever protocol the group actually uses.
func (b *backend) DescribeGroup(ctx context.Context, id string) (*GroupDetail, error) {
	lreq := kmsg.NewPtrListGroupsRequest()
	lresp, err := lreq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	protocol := ""
	for _, g := range lresp.Groups {
		if g.Group == id {
			protocol = normalizeProtocol(g.GroupType)
			break
		}
	}
	if protocol == "" {
		return nil, fmt.Errorf("group %q not found", id)
	}
	if protocol == "848" {
		return b.describeGroup848(ctx, id)
	}
	return b.describeGroupClassic(ctx, id)
}

func (b *backend) describeGroupClassic(ctx context.Context, id string) (*GroupDetail, error) {
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{id}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if len(resp.Groups) == 0 {
		return nil, fmt.Errorf("group %q not found", id)
	}
	g := resp.Groups[0]
	if g.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(g.ErrorCode)
	}
	members := make([]GroupMember, len(g.Members))
	for i, m := range g.Members {
		assigned := map[string][]int32{}
		var a kmsg.ConsumerMemberAssignment
		if len(m.MemberAssignment) > 0 && a.ReadFrom(m.MemberAssignment) == nil {
			for _, t := range a.Topics {
				assigned[t.Topic] = t.Partitions
			}
		}
		members[i] = GroupMember{ID: m.MemberID, Assigned: assigned}
	}
	return &GroupDetail{ID: id, Protocol: "classic", State: g.State, Members: members}, nil
}

func (b *backend) describeGroup848(ctx context.Context, id string) (*GroupDetail, error) {
	req := kmsg.NewPtrConsumerGroupDescribeRequest()
	req.Groups = []string{id}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if len(resp.Groups) == 0 {
		return nil, fmt.Errorf("group %q not found", id)
	}
	g := resp.Groups[0]
	if g.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(g.ErrorCode)
	}
	members := make([]GroupMember, len(g.Members))
	for i, m := range g.Members {
		assigned := map[string][]int32{}
		for _, tp := range m.Assignment.TopicPartitions {
			assigned[tp.Topic] = tp.Partitions
		}
		members[i] = GroupMember{ID: m.MemberID, Assigned: assigned}
	}
	return &GroupDetail{ID: id, Protocol: "848", State: g.State, Assignor: g.AssignorName, Members: members}, nil
}

// GroupOffsets returns every committed offset a group holds, across all
// topics — the same shape kafka-consumer-groups.sh reads to compute lag.
func (b *backend) GroupOffsets(ctx context.Context, id string) ([]GroupOffset, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = id
	req.Topics = nil // null = every topic the group has committed offsets for
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(resp.ErrorCode)
	}
	var out []GroupOffset
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 || p.Offset < 0 {
				continue // no committed offset for this partition
			}
			out = append(out, GroupOffset{Topic: t.Topic, Partition: p.Partition, Committed: p.Offset})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out, nil
}
