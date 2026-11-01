package broker

import (
	"sort"
	"time"
)

// GroupSnapshot is a read-only copy of one group's coordinator state, for the console
// (GRP-22). It holds what the broker keeps in memory and no Kafka API returns: where
// each member connected from, when it joined and last heartbeated, the timeouts it
// asked for, the protocols it offered and who leads. It shares nothing with the
// coordinators; a restart loses it with the members (an empty snapshot, not an error).
//
// A field a coordinator does not have is left empty and is not invented: for a
// KIP-848 ("consumer") group there is no Leader, no ProtocolName, no Generation (the
// group has an Epoch instead), and its members have no offered Protocols and no
// SessionTimeout of their own (the session timeout is the broker's setting, not
// something a ConsumerGroupHeartbeat states). A classic-protocol member that joined a
// consumer group keeps the Protocols, SessionTimeout and RebalanceTimeout it sent.
type GroupSnapshot struct {
	ID string
	// Kind is "classic" or "consumer" (KIP-848), as ListGroups reports a group's type.
	Kind string
	// State is the name DescribeGroups gives it ("Stable", "PreparingRebalance"...).
	State        string
	ProtocolType string
	// ProtocolName is the assignor a classic group selected; empty for a consumer group.
	ProtocolName string
	// Generation is a classic group's generation; zero for a consumer group.
	Generation int32
	// Epoch is a consumer group's epoch; zero for a classic group.
	Epoch int32
	// Leader is the member id leading a classic group; empty for a consumer group.
	Leader string
	// Members are ordered by member id.
	Members []MemberSnapshot
	// Taken is when the snapshot was made.
	Taken time.Time
}

// MemberSnapshot is one member of a GroupSnapshot.
type MemberSnapshot struct {
	ID         string
	InstanceID string // the static group.instance.id; empty for a dynamic member.
	ClientID   string
	// Addr is the connection's remote address with its port ("127.0.0.1:53712"), Host
	// the same without the port as DescribeGroups reports it ("/127.0.0.1").
	Addr string
	Host string
	// Software is the "name version" the client announced in ApiVersions; empty if it
	// sent none. Principal is who the connection authenticated as.
	Software  string
	Principal string
	// Joined is when this broker first saw the member in its present incarnation and
	// LastHeartbeat the last time it refreshed its session (a join, a sync or a
	// heartbeat).
	Joined        time.Time
	LastHeartbeat time.Time
	// SessionTimeout and RebalanceTimeout are what the member asked for; zero when it
	// did not say (JoinGroup v0 states no rebalance timeout; a consumer-protocol member
	// states no session timeout).
	SessionTimeout   time.Duration
	RebalanceTimeout time.Duration
	// Protocols are the assignor names a classic member offered, in its order.
	Protocols []string
	// Leader says the member leads its (classic) group.
	Leader bool
	// Epoch is the member epoch of a consumer-protocol member; zero for classic.
	Epoch int32
}

// GroupInspector is what the console reads group snapshots through.
type GroupInspector interface {
	InspectGroup(id string) (GroupSnapshot, bool)
	InspectGroups() []GroupSnapshot
}

// InspectGroup is the snapshot of one group, if either coordinator holds it.
func (b *Broker) InspectGroup(id string) (GroupSnapshot, bool) {
	for _, g := range b.InspectGroups() {
		if g.ID == id {
			return g, true
		}
	}
	return GroupSnapshot{}, false
}

// InspectGroups is a snapshot of every group either coordinator holds in memory,
// ordered by id. Each is copied under its coordinator's lock.
func (b *Broker) InspectGroups() []GroupSnapshot {
	now := time.Now()
	out := append(b.cg848.inspect(now), b.groups.inspect(now)...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (gm *groupManager) inspect(now time.Time) []GroupSnapshot {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	out := make([]GroupSnapshot, 0, len(gm.groups))
	for id, g := range gm.groups {
		s := GroupSnapshot{ID: id, Kind: "classic", State: g.state.String(), ProtocolType: g.protocolType,
			ProtocolName: g.protocol, Generation: g.generation, Leader: g.leader, Taken: now}
		for _, mid := range sortedMemberIDs(g) {
			m := g.members[mid]
			ms := MemberSnapshot{ID: m.id, ClientID: m.client.clientID, Addr: m.client.addr, Host: m.client.host,
				Software: m.client.software, Principal: m.client.principal, Joined: m.joinedAt, LastHeartbeat: m.lastHeartbeat,
				SessionTimeout: m.sessionTimeout, RebalanceTimeout: m.rebalanceTimeout, Leader: m.id == g.leader}
			if m.instanceID != nil {
				ms.InstanceID = *m.instanceID
			}
			for _, p := range m.protocols {
				ms.Protocols = append(ms.Protocols, p.name)
			}
			s.Members = append(s.Members, ms)
		}
		out = append(out, s)
	}
	return out
}

func (m *cg848Manager) inspect(now time.Time) []GroupSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]GroupSnapshot, 0, len(m.groups))
	for id, g := range m.groups {
		s := GroupSnapshot{ID: id, Kind: "consumer", State: g.stateName(), ProtocolType: "consumer", Epoch: g.epoch, Taken: now}
		for _, mid := range sortedKeys(g.members) {
			mem := g.members[mid]
			ms := MemberSnapshot{ID: mem.id, ClientID: mem.client.clientID, Addr: mem.client.addr, Host: mem.client.host,
				Software: mem.client.software, Principal: mem.client.principal, Joined: mem.joinedAt, LastHeartbeat: mem.lastHeartbeat,
				RebalanceTimeout: mem.rebalanceTimeout, Epoch: mem.epoch}
			if mem.instanceID != nil {
				ms.InstanceID = *mem.instanceID
			}
			if mem.classic != nil {
				ms.SessionTimeout = mem.classic.session
				for _, p := range mem.classic.protocols {
					ms.Protocols = append(ms.Protocols, p.name)
				}
			}
			s.Members = append(s.Members, ms)
		}
		out = append(out, s)
	}
	return out
}
