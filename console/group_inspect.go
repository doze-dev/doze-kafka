package console

import "time"

// The group inspector (GRP-22): what the broker's group coordinators hold in memory
// and no Kafka API returns. Like TrafficSource it is implemented where the broker is
// (internal/consolefeed over internal/broker); a console running apart from the broker
// has none, and says so (InspectUnavailable) instead of showing an empty group.

// GroupInspector is a read-only view of the coordinators' memory.
type GroupInspector interface {
	// InspectGroup is one group's snapshot, if a coordinator holds it.
	InspectGroup(id string) (GroupSnapshot, bool)
	// InspectGroups is every group a coordinator holds, ordered by id.
	InspectGroups() []GroupSnapshot
}

// GroupSnapshot is an immutable copy of one group. A field a coordinator does not have
// is empty: a consumer-protocol (KIP-848) group has an Epoch and no Generation, Leader
// or ProtocolName, and its members have no offered Protocols; only a classic-protocol
// member of it keeps the Protocols and timeouts its JoinGroup sent.
type GroupSnapshot struct {
	ID           string
	Kind         string // "classic" or "consumer".
	State        string
	ProtocolType string
	ProtocolName string
	Generation   int32
	Epoch        int32
	Leader       string
	Members      []MemberSnapshot
	Taken        time.Time
}

// MemberSnapshot is one member of a GroupSnapshot.
type MemberSnapshot struct {
	ID               string
	InstanceID       string
	ClientID         string
	Addr             string // host:port of its connection.
	Host             string // as DescribeGroups reports it, without the port.
	Software         string
	Principal        string
	Joined           time.Time
	LastHeartbeat    time.Time
	SessionTimeout   time.Duration
	RebalanceTimeout time.Duration
	Protocols        []string
	Leader           bool
	Epoch            int32
}

// InspectState says whether a snapshot could be had and whether it holds the group.
type InspectState int

const (
	// InspectUnavailable: this console cannot see the broker's memory. The page says so.
	InspectUnavailable InspectState = iota
	// InspectNone: it can, and no coordinator holds the group (it is empty, or the
	// broker restarted: members are in memory only).
	InspectNone
	// InspectOK: Group is the snapshot.
	InspectOK
)

// GroupInspection is the answer to inspectGroup.
type GroupInspection struct {
	State InspectState
	Group GroupSnapshot
}

// inspectGroup reads one group's snapshot, or says why there is none.
func (c *Console) inspectGroup(id string) GroupInspection {
	if c.inspector == nil {
		return GroupInspection{State: InspectUnavailable}
	}
	g, ok := c.inspector.InspectGroup(id)
	if !ok {
		return GroupInspection{State: InspectNone}
	}
	return GroupInspection{State: InspectOK, Group: g}
}

// hasGroupInspector says whether the console can see the broker's group memory.
func (c *Console) hasGroupInspector() bool { return c.inspector != nil }
