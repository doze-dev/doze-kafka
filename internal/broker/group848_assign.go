package broker

import (
	"slices"
	"sort"

	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// The KIP-848 assignors: given who subscribes to what, decide each member's
// target. The coordinator (group848.go) then moves members towards it.

// topicInfo is a subscribed topic as the assignor sees it.
type topicInfo struct {
	name        string
	id          meta.TopicID
	partitions  int32
	subscribers []string // member ids, sorted.
}

// subscribedTopics resolves every topic the group's members want that exists.
func (m *cg848Manager) subscribedTopics(g *cg848Group) []topicInfo {
	names := map[string]struct{}{}
	anyRegex := false
	for _, mem := range g.members {
		for _, t := range mem.subscribed {
			names[t] = struct{}{}
		}
		anyRegex = anyRegex || mem.regex != nil
	}
	if anyRegex {
		if all, err := m.meta.ListTopics(); err == nil {
			for _, t := range all {
				for _, mem := range g.members {
					if mem.regex != nil && mem.regex.MatchString(t.Name) {
						names[t.Name] = struct{}{}
					}
				}
			}
		}
	}
	out := make([]topicInfo, 0, len(names))
	for name := range names {
		t, ok, err := m.meta.Topic(name)
		if err != nil || !ok || t.Partitions <= 0 {
			continue
		}
		info := topicInfo{name: name, id: t.ID, partitions: t.Partitions}
		for id, mem := range g.members {
			if mem.subscribes(name) {
				info.subscribers = append(info.subscribers, id)
			}
		}
		if len(info.subscribers) == 0 {
			continue
		}
		slices.Sort(info.subscribers)
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// assign runs the server-side assignor, filling each member's target.
func (m *cg848Manager) assign(g *cg848Group) {
	// The assignor is the one most members asked for; with no preference, uniform.
	votes := map[string]int{}
	for _, mem := range g.members {
		if mem.assignor != "" {
			votes[mem.assignor]++
		}
	}
	g.assignor = assignorUniform
	if votes[assignorRange] > votes[assignorUniform] {
		g.assignor = assignorRange
	}

	topics := m.subscribedTopics(g)
	previous := make(map[string]tpSet, len(g.members))
	for id, mem := range g.members {
		previous[id] = mem.target
		mem.target = tpSet{}
	}
	if g.assignor == assignorRange {
		for _, t := range topics {
			assignRange(g, t)
		}
		return
	}
	assignUniform(g, topics, previous)
}

// assignRange gives each subscriber a contiguous slice of the topic's partitions;
// the first (n mod k) members get one extra. Applied topic by topic, so topics with
// the same partition count line up (co-partitioning), as Kafka's range does.
func assignRange(g *cg848Group, t topicInfo) {
	k := int32(len(t.subscribers))
	base, extra := t.partitions/k, t.partitions%k
	var start int32
	for i, id := range t.subscribers {
		count := base
		if int32(i) < extra {
			count++
		}
		for p := start; p < start+count; p++ {
			g.members[id].target[tp{t.id, p}] = struct{}{}
		}
		start += count
	}
}

// assignUniform spreads all partitions of all topics evenly over the members
// subscribed to them, and keeps a member's partitions where it can (sticky).
//
// It used to deal each topic round-robin on its own: six single-partition topics
// and one of three partitions gave three members 7, 1 and 1 partitions, and adding
// a member moved partitions between members that had no reason to change.
func assignUniform(g *cg848Group, topics []topicInfo, previous map[string]tpSet) {
	eligible := map[tp][]string{} // partition -> members subscribed to its topic.
	var all []tp
	for _, t := range topics {
		for p := int32(0); p < t.partitions; p++ {
			part := tp{t.id, p}
			eligible[part] = t.subscribers
			all = append(all, part)
		}
	}
	owner := map[tp]string{}
	count := map[string]int{}
	for id := range g.members {
		count[id] = 0
	}

	// Sticky: keep what each member had, in a stable order, as long as the
	// partition still exists and the member still wants its topic.
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		for _, part := range sortedParts(previous[id]) {
			if _, taken := owner[part]; taken {
				continue
			}
			if subs, ok := eligible[part]; ok && slices.Contains(subs, id) {
				owner[part] = id
				count[id]++
			}
		}
	}
	// Unowned partitions go to the eligible member with the fewest.
	least := func(subs []string) string {
		best := subs[0]
		for _, id := range subs[1:] {
			if count[id] < count[best] {
				best = id
			}
		}
		return best
	}
	for _, part := range all {
		if _, taken := owner[part]; !taken {
			id := least(eligible[part])
			owner[part] = id
			count[id]++
		}
	}
	// Even out: move a partition from a member to an eligible member that has at
	// least two fewer, until no such move exists.
	for moved := true; moved; {
		moved = false
		for i := len(all) - 1; i >= 0; i-- {
			part := all[i]
			from := owner[part]
			to := least(eligible[part])
			if count[from]-count[to] >= 2 {
				owner[part] = to
				count[from]--
				count[to]++
				moved = true
			}
		}
	}
	for part, id := range owner {
		g.members[id].target[part] = struct{}{}
	}
}

func sortedParts(s tpSet) []tp {
	out := make([]tp, 0, len(s))
	for p := range s {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].topic != out[j].topic {
			return string(out[i].topic[:]) < string(out[j].topic[:])
		}
		return out[i].part < out[j].part
	})
	return out
}
