package broker

// What the group coordinators did, for the console's group timeline.
//
// "Why does my consumer keep rebalancing?" is answered by the order things happened
// in: a member joined, a generation began, a session expired. The coordinators say so
// here, to a sink the embedder may set; with none set, saying so costs a nil check.

// GroupEventSink is told what the group coordinators do.
type GroupEventSink interface {
	// GroupEvent reports one thing that happened in a group. kind is a short word
	// (joined, left, expired, rebalance, generation, assigned, dropped, epoch);
	// generation is the classic generation or the KIP-848 group epoch. members is how
	// many members the group has when it happened, and cause is why a rebalance or an
	// epoch began, as a short phrase ("a member joined"), where there is one: fields,
	// so the console does not have to read them out of detail.
	GroupEvent(group, kind, member string, generation int32, detail string, members int, cause string)
}

// SetGroupEvents makes sink hear what both group coordinators do. Call before serving.
func (b *Broker) SetGroupEvents(sink GroupEventSink) {
	b.groups.mu.Lock()
	b.groups.sink = sink
	b.groups.mu.Unlock()
	b.cg848.mu.Lock()
	b.cg848.sink = sink
	b.cg848.mu.Unlock()
}

func (gm *groupManager) emit(g *group, kind, member, detail, cause string) {
	if gm.sink != nil {
		gm.sink.GroupEvent(g.id, kind, member, g.generation, detail, len(g.members), cause)
	}
}

func (m *cg848Manager) emit(g *cg848Group, kind, member, detail, cause string) {
	if m.sink != nil {
		m.sink.GroupEvent(g.id, kind, member, g.epoch, detail, len(g.members), cause)
	}
}
