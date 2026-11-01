package consolefeed

import (
	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/internal/broker"
)

type groupInspector struct{ in broker.GroupInspector }

// Groups is the console's view of the group coordinators' memory, or nil when there is
// no broker to ask.
func Groups(in broker.GroupInspector) console.GroupInspector {
	if in == nil {
		return nil
	}
	return groupInspector{in}
}

func (g groupInspector) InspectGroup(id string) (console.GroupSnapshot, bool) {
	s, ok := g.in.InspectGroup(id)
	return groupOf(s), ok
}

func (g groupInspector) InspectGroups() []console.GroupSnapshot {
	all := g.in.InspectGroups()
	out := make([]console.GroupSnapshot, 0, len(all))
	for _, s := range all {
		out = append(out, groupOf(s))
	}
	return out
}

func groupOf(s broker.GroupSnapshot) console.GroupSnapshot {
	out := console.GroupSnapshot{ID: s.ID, Kind: s.Kind, State: s.State, ProtocolType: s.ProtocolType, ProtocolName: s.ProtocolName,
		Generation: s.Generation, Epoch: s.Epoch, Leader: s.Leader, Taken: s.Taken, TargetEpoch: s.TargetEpoch,
		RebalanceTimeout: s.RebalanceTimeout, RebalanceStarted: s.RebalanceStarted, RebalanceDeadline: s.RebalanceDeadline, Holding: s.Holding}
	for _, m := range s.Members {
		out.Members = append(out.Members, console.MemberSnapshot{ID: m.ID, InstanceID: m.InstanceID, ClientID: m.ClientID, Addr: m.Addr,
			Host: m.Host, Software: m.Software, Principal: m.Principal, Joined: m.Joined, LastHeartbeat: m.LastHeartbeat,
			SessionTimeout: m.SessionTimeout, RebalanceTimeout: m.RebalanceTimeout, Protocols: m.Protocols, Leader: m.Leader, Epoch: m.Epoch,
			Rejoined: m.Rejoined, RejoinedAt: m.RejoinedAt, State: m.State, PrevEpoch: m.PrevEpoch,
			Target: partsOf(m.Target), Current: partsOf(m.Current), Revoking: partsOf(m.Revoking)})
	}
	return out
}

func partsOf(in []broker.TopicParts) []console.TopicParts {
	var out []console.TopicParts
	for _, t := range in {
		out = append(out, console.TopicParts(t))
	}
	return out
}

// OffsetExpiry is when the group's committed offsets expire, if the broker behind the
// inspector can say (console.OffsetExpirer).
func (g groupInspector) OffsetExpiry(group string) (console.OffsetExpiry, bool) {
	x, ok := g.in.(interface {
		OffsetExpiry(string) broker.OffsetExpiry
	})
	if !ok {
		return console.OffsetExpiry{}, false
	}
	e := x.OffsetExpiry(group)
	out := console.OffsetExpiry{Retention: e.Retention, EmptySince: e.EmptySince, Interval: e.Interval}
	for _, o := range e.Offsets {
		out.Offsets = append(out.Offsets, console.OffsetExpiryAt(o))
	}
	return out, true
}
