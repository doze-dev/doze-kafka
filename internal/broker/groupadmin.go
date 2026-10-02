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
	for id, g := range m.groups {
		out = append(out, adminGroupListing{id: id, state: g.stateName(), groupType: "consumer"})
	}
	return out
}

// stateName is the group's state as ConsumerGroupDescribe / ListGroups report it.
// A group is Stable once every member has been told, and reports it holds, exactly
// its target assignment; until then partitions are still moving between members
// and the honest state is Reconciling. (Empty groups are removed, so none is Empty.)
func (g *cg848Group) stateName() string {
	for _, mem := range g.members {
		if mem.lastSent == nil || !assignmentsEqual(mem.lastSent, mem.target) || !assignmentsEqual(mem.owned, mem.target) {
			return "Reconciling"
		}
	}
	return "Stable"
}

// listAllGroups is every group a client could ask about: the live ones from both
// coordinators, plus any group that exists only as committed offsets. Offsets
// outlive the members (they are the durable state), and the in-memory group
// objects do not survive a restart — so without this a group with 40 committed
// offsets vanishes from every listing the moment the broker restarts, even
// though its consumers will resume from them.
func (b *Broker) listAllGroups() []adminGroupListing {
	groups := append(b.groups.list(), b.cg848.list()...)
	known := make(map[string]bool, len(groups))
	for _, g := range groups {
		known[g.id] = true
	}
	ids, err := b.meta.OffsetGroups()
	if err != nil {
		b.log.Warn("list groups: could not read committed offsets", "err", err)
		return groups
	}
	for _, id := range ids {
		if !known[id] {
			groups = append(groups, adminGroupListing{id: id, state: "Empty", groupType: "classic"})
		}
	}
	return groups
}

// groupHasOffsets reports whether a group id holds committed offsets.
func (b *Broker) groupHasOffsets(id string) bool {
	entries, err := b.meta.ListOffsets(id)
	return err == nil && len(entries) > 0
}

// classicGroupDesc is a classic group's DescribeGroups view: its state, selected
// assignor, and members (with the opaque subscription/assignment bytes we relay).
type classicGroupDesc struct {
	state    string
	protocol string
	members  []classicMemberDesc
}

type classicMemberDesc struct {
	client     clientRef
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
			client:     m.client,
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

	for _, g := range b.listAllGroups() {
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
			// Known only by its committed offsets: Empty, not Dead. Dead is a
			// group Kafka has never heard of.
			rg.State = "Dead"
			if b.groupHasOffsets(id) {
				rg.State = "Empty"
			}
			resp.Groups = append(resp.Groups, rg)
			continue
		}
		rg.State = d.state
		rg.Protocol = d.protocol
		for _, m := range d.members {
			rm := kmsg.NewDescribeGroupsResponseGroupMember()
			rm.MemberID = m.id
			rm.ClientID = m.client.clientID
			rm.ClientHost = m.client.host
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
