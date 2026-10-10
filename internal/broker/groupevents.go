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
	// generation is the classic generation or the KIP-848 group epoch.
	GroupEvent(group, kind, member string, generation int32, detail string)
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

func (gm *groupManager) emit(group, kind, member string, generation int32, detail string) {
	if gm.sink != nil {
		gm.sink.GroupEvent(group, kind, member, generation, detail)
	}
}

func (m *cg848Manager) emit(group, kind, member string, generation int32, detail string) {
	if m.sink != nil {
		m.sink.GroupEvent(group, kind, member, generation, detail)
	}
}
