package broker

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// This file holds the group admin APIs — ListGroups and DescribeGroups — that back
// `kafka-consumer-groups --list` / `--describe` and equivalent admin clients. They
// read across both group managers: the classic relay (groupManager) and the
// KIP-848 broker-driven coordinator (cg848Manager).

// adminGroupListing is one row of ListGroups: a group id, its state, and whether it
// is a classic or a KIP-848 (consumer) group.
type adminGroupListing struct {
	id        string
	state     string
	groupType string // "classic" or "consumer".
}

// list returns every classic group. Called under no lock; takes gm.mu itself.
func (gm *groupManager) list() []adminGroupListing {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	out := make([]adminGroupListing, 0, len(gm.groups))
	for id, g := range gm.groups {
		out = append(out, adminGroupListing{id: id, state: g.state.String(), groupType: "classic"})
	}
	return out
}

// list returns every KIP-848 group.
func (m *cg848Manager) list() []adminGroupListing {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]adminGroupListing, 0, len(m.groups))
	for id := range m.groups {
		out = append(out, adminGroupListing{id: id, state: "Stable", groupType: "consumer"})
	}
	return out
}

// classicGroupDesc is a classic group's DescribeGroups view: its state, selected
// assignor, and members (with the opaque subscription/assignment bytes we relay).
type classicGroupDesc struct {
	state    string
	protocol string
	members  []classicMemberDesc
}

type classicMemberDesc struct {
	id         string
	instanceID *string
	metadata   []byte // subscription from JoinGroup, for the selected protocol.
	assignment []byte // from the leader's SyncGroup.
}

func (gm *groupManager) describe(id string) (classicGroupDesc, bool) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[id]
	if g == nil {
		return classicGroupDesc{}, false
	}
	d := classicGroupDesc{state: g.state.String(), protocol: g.protocol}
	for _, mid := range sortedMemberIDs(g) {
		m := g.members[mid]
		d.members = append(d.members, classicMemberDesc{
			id:         m.id,
			instanceID: m.instanceID,
			metadata:   m.metadataFor(g.protocol),
			assignment: m.assignment,
		})
	}
	return d, true
}

// handleListGroups lists groups across both protocols, applying the optional
// state/type filters (v4+/v5+). Single-node: every group is coordinated here.
func (b *Broker) handleListGroups(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	lr := kreq.(*kmsg.ListGroupsRequest)

	stateFilter := toSet(lr.StatesFilter)
	typeFilter := toSet(lr.TypesFilter)

	resp := kmsg.NewPtrListGroupsResponse()
	resp.SetVersion(req.Header.APIVersion)

	groups := append(b.groups.list(), b.cg848.list()...)
	for _, g := range groups {
		if len(stateFilter) > 0 && !stateFilter[g.state] {
			continue
		}
		if len(typeFilter) > 0 && !typeFilter[g.groupType] {
			continue
		}
		rg := kmsg.NewListGroupsResponseGroup()
		rg.Group = g.id
		rg.ProtocolType = "consumer"
		rg.GroupState = g.state
		rg.GroupType = g.groupType
		resp.Groups = append(resp.Groups, rg)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// handleDescribeGroups describes classic groups (the API predates KIP-848; new-
// protocol groups are described via ConsumerGroupDescribe). An unknown group is
// reported as Dead with no members — Kafka's behaviour, not an error.
func (b *Broker) handleDescribeGroups(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	dr := kreq.(*kmsg.DescribeGroupsRequest)

	resp := kmsg.NewPtrDescribeGroupsResponse()
	resp.SetVersion(req.Header.APIVersion)

	for _, id := range dr.Groups {
		rg := kmsg.NewDescribeGroupsResponseGroup()
		rg.Group = id
		rg.ProtocolType = "consumer"

		d, ok := b.groups.describe(id)
		if !ok {
			rg.State = "Dead"
			resp.Groups = append(resp.Groups, rg)
			continue
		}
		rg.State = d.state
		rg.Protocol = d.protocol
		for _, m := range d.members {
			rm := kmsg.NewDescribeGroupsResponseGroupMember()
			rm.MemberID = m.id
			rm.InstanceID = m.instanceID
			rm.ProtocolMetadata = m.metadata
			rm.MemberAssignment = m.assignment
			rg.Members = append(rg.Members, rm)
		}
		resp.Groups = append(resp.Groups, rg)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

func toSet(vals []string) map[string]bool {
	if len(vals) == 0 {
		return nil
	}
	m := make(map[string]bool, len(vals))
	for _, v := range vals {
		m[v] = true
	}
	return m
}
