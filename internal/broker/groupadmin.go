package broker

import (
	"context"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// This file holds the group admin APIs — ListGroups and DescribeGroups — that back
// `kafka-consumer-groups --list` / `--describe` and equivalent admin clients. They
// read across both group managers: the classic relay (groupManager) and the
// KIP-848 broker-driven coordinator (cg848Manager).

// adminGroupListing is one row of ListGroups: a group id, its state, and whether it
// is a classic or a KIP-848 (consumer) group.
type adminGroupListing struct {
	id           string
	state        string
	groupType    string // "classic" or "consumer".
	protocolType string // what the members are; "consumer" for a consumer group.
	members      int
}

// list returns every classic group. Called under no lock; takes gm.mu itself.
func (gm *groupManager) list() []adminGroupListing {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	out := make([]adminGroupListing, 0, len(gm.groups))
	for id, g := range gm.groups {
		pt := g.protocolType
		if pt == "" {
			pt = "consumer"
		}
		out = append(out, adminGroupListing{id: id, state: g.state.String(), groupType: "classic", protocolType: pt, members: len(g.members)})
	}
	return out
}

// list returns every KIP-848 group.
func (m *cg848Manager) list() []adminGroupListing {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]adminGroupListing, 0, len(m.groups))
	for id, g := range m.groups {
		out = append(out, adminGroupListing{id: id, state: g.stateName(), groupType: "consumer", protocolType: "consumer", members: len(g.members)})
	}
	return out
}

// listAllGroups is every group a client could ask about: the live ones from both
// coordinators, plus any group that exists only as committed offsets. Offsets
// outlive the members (they are the durable state), and the in-memory group
// objects do not survive a restart — so without this a group with 40 committed
// offsets vanishes from every listing the moment the broker restarts, even
// though its consumers will resume from them.
func (b *Broker) listAllGroups() []adminGroupListing {
	// A group id belongs to one protocol at a time. A classic group object that
	// has no members but is kept for its offsets must not be listed beside the
	// KIP-848 group now running under the same id.
	var groups []adminGroupListing
	known := map[string]bool{}
	for _, g := range b.cg848.list() {
		// An emptied KIP-848 group gives way to a classic group now using its id.
		if g.members == 0 && b.claims.ownerOf(g.id) == protoClassic {
			continue
		}
		groups = append(groups, g)
		known[g.id] = true
	}
	for _, g := range b.groups.list() {
		if known[g.id] && g.members == 0 {
			continue
		}
		groups = append(groups, g)
		known[g.id] = true
	}
	ids, err := b.meta.OffsetGroups()
	if err != nil {
		b.log.Warn("list groups: could not read committed offsets", "err", err)
		return groups
	}
	for _, id := range ids {
		if !known[id] {
			groups = append(groups, adminGroupListing{id: id, state: "Empty", groupType: "classic", protocolType: "consumer"})
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
	state        string
	protocolType string
	protocol     string
	members      []classicMemberDesc
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
	d := classicGroupDesc{state: g.state.String(), protocol: g.protocol, protocolType: g.protocolType}
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
func (b *Broker) handleListGroups(ctx context.Context, req *protocol.Request, lr *kmsg.ListGroupsRequest, resp *kmsg.ListGroupsResponse) error {
	// Both filters match without regard to case, as Kafka's do: the Java admin
	// client sends "Classic" and "Consumer", and a case-sensitive match returned
	// nothing for `kafka-consumer-groups --list --type classic`.
	stateFilter := toSet(lr.StatesFilter)
	typeFilter := toSet(lr.TypesFilter)

	for _, g := range b.listAllGroups() {
		if len(stateFilter) > 0 && !stateFilter[strings.ToLower(g.state)] {
			continue
		}
		if len(typeFilter) > 0 && !typeFilter[strings.ToLower(g.groupType)] {
			continue
		}
		// Groups the principal may describe (Describe on the group, or on the cluster).
		if !b.canGroup(req.Conn, g.id, opDescribe) && !b.canCluster(req.Conn, opDescribe) {
			continue
		}
		rg := kmsg.NewListGroupsResponseGroup()
		rg.Group = g.id
		rg.ProtocolType = g.protocolType
		if rg.ProtocolType == "" {
			rg.ProtocolType = "consumer"
		}
		rg.GroupState = g.state
		rg.GroupType = g.groupType
		resp.Groups = append(resp.Groups, rg)
	}
	return nil
}

// handleDescribeGroups describes classic groups (the API predates KIP-848; new-
// protocol groups are described via ConsumerGroupDescribe). An unknown group is
// reported as Dead with no members — Kafka's behaviour, not an error.
func (b *Broker) handleDescribeGroups(ctx context.Context, req *protocol.Request, dr *kmsg.DescribeGroupsRequest, resp *kmsg.DescribeGroupsResponse) error {
	for _, id := range dr.Groups {
		rg := kmsg.NewDescribeGroupsResponseGroup()
		rg.Group = id
		rg.ProtocolType = "consumer"
		if !b.canGroup(req.Conn, id, opDescribe) {
			rg.ErrorCode = kerr.GroupAuthorizationFailed.Code
			resp.Groups = append(resp.Groups, rg)
			continue
		}

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
		if d.protocolType != "" {
			rg.ProtocolType = d.protocolType
		}
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
	return nil
}

func toSet(vals []string) map[string]bool {
	if len(vals) == 0 {
		return nil
	}
	m := make(map[string]bool, len(vals))
	for _, v := range vals {
		m[strings.ToLower(v)] = true
	}
	return m
}
