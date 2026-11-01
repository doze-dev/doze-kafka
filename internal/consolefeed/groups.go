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
		Generation: s.Generation, Epoch: s.Epoch, Leader: s.Leader, Taken: s.Taken}
	for _, m := range s.Members {
		out.Members = append(out.Members, console.MemberSnapshot(m))
	}
	return out
}
