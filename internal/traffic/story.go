package traffic

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

// The story of a group (Traf-12): its requests, nested under the rebalance they belong
// to, with how each rebalance began and ended. "Why does my consumer keep rebalancing?"
// is answered by the order things happened in, so the coordinators' events (a member
// joined, a generation opened) and the group's requests (JoinGroup, SyncGroup, the
// heartbeats that carry the new generation) are put on one line.

// Generation is one row of the generations table.
type Generation struct {
	Gen     int32
	Cause   string
	Members int
	At      time.Time
}

// Rebalance is the span from a coordinator beginning a rebalance to the generation it
// opened, and the requests the group made from then until the next one began.
type Rebalance struct {
	Start, End time.Time
	// Done says the generation it was opening has opened.
	Done bool
	// From and To are the generations before and after (To is 0 until it is Done).
	From, To int32
	// Cause is why, as a phrase ("a member joined"); Members is how many members the
	// new generation has and MembersBefore how many the one before had (0 when not
	// known).
	Cause                  string
	Members, MembersBefore int
	// Rows are the requests in this span, oldest first: the order they happened in, as
	// the board draws them under their rebalance.
	Rows   []Event
	unseen bool
}

// joinLead is how long before the coordinator notes a rebalance the requests that lead
// to it can have begun: the client finds the coordinator, is told to come back with a
// member id (KIP-394), and joins. A request of these goes under the earliest rebalance
// that began within this after it.
const joinLead = 5 * time.Second

// joinsAGroup are the requests that bring a member in, and so begin a rebalance. A
// SyncGroup is not one of them: it ends a rebalance.
// A LeaveGroup is the cause of the rebalance that follows it, so it is placed the
// same way: under the rebalance it triggers, not the generation it leaves.
var joinsAGroup = map[string]bool{"FindCoordinator": true, "JoinGroup": true, "LeaveGroup": true}

// namedGeneration is the generation a JoinGroup's answer or a SyncGroup's request names
// in its detail ("generation 7 · ..."), or 0.
func namedGeneration(e Event) int32 {
	if e.API != "JoinGroup" && e.API != "SyncGroup" {
		return 0
	}
	rest, ok := strings.CutPrefix(e.Detail, "generation ")
	if !ok {
		return 0
	}
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	n, err := strconv.Atoi(rest[:end])
	if err != nil || n <= 0 {
		return 0
	}
	return int32(n)
}

// Story is one group's recent past.
type Story struct {
	Group string
	// Protocol is "classic" or "848": events of the KIP-848 coordinator are epochs.
	Protocol string
	// Rebalances are newest first. Before holds the requests that came before the
	// oldest rebalance the recorder knows of (the generation before it).
	Rebalances  []Rebalance
	Before      []Event
	Generations []Generation
	// Requests counts every request the rows stand for, and Folded how many of those
	// are repeats folded into another row.
	Requests, Folded int
	// Topics are the topics the group's rows name, in first-seen order.
	Topics []string
}

// Story builds a group's story from the events and rows the recorder holds.
func (r *Recorder) Story(group string) Story {
	s := Story{Group: group}
	events := r.GroupEvents(group, 0)
	rows := r.Events(Filter{Group: group}, 0) // newest first.

	var cur *Rebalance
	var lastGen int32
	var lastMembers int
	var spans []Rebalance
	finish := func(gen int32, members int, at time.Time, cause string) {
		if cur == nil {
			// A generation whose rebalance the recorder did not see (it started late, or
			// the group's older events were pushed out): it is in the table, but there is
			// no span to nest requests under.
			cur = &Rebalance{Start: at, unseen: true}
		}
		cur.End, cur.Done, cur.To, cur.Members = at, true, gen, members
		cur.From, cur.MembersBefore = lastGen, lastMembers
		if cur.Cause == "" {
			cur.Cause = cause
		}
		if !cur.unseen {
			spans = append(spans, *cur)
		}
		s.Generations = append(s.Generations, Generation{Gen: gen, Cause: cur.Cause, Members: members, At: at})
		lastGen, lastMembers = gen, members
		cur = nil
	}
	for _, e := range events {
		switch e.Kind {
		case "rebalance":
			if cur == nil {
				cur = &Rebalance{Start: e.Time, Cause: e.Cause}
				if cur.Cause == "" {
					cur.Cause = e.Detail
				}
			}
		case "generation":
			s.Protocol = "classic"
			finish(e.Generation, e.Members, e.Time, "")
		case "epoch":
			s.Protocol = "848"
			cur = &Rebalance{Start: e.Time, Cause: e.Cause}
			finish(e.Generation, e.Members, e.Time, e.Detail)
		}
	}
	if cur != nil { // a rebalance that has not finished.
		cur.From, cur.MembersBefore = lastGen, lastMembers
		spans = append(spans, *cur)
	}

	// Each row goes under the last rebalance that began before it. A row is placed by
	// when it was last seen (a folded heartbeat that ran across a rebalance sits under
	// the generation its last beat was in, not the one that had already ended), and the
	// requests that bring a member to a group are placed under the rebalance that began
	// within joinLead after them: the
	// coordinator notes the rebalance when it handles the JoinGroup, a moment after the
	// client sent the FindCoordinator and the first JoinGroup that led to it.
	for i := len(rows) - 1; i >= 0; i-- { // oldest first.
		e := rows[i]
		at := e.Time
		s.Requests += max(e.Count, 1)
		s.Folded += max(e.Count-1, 0)
		for _, tp := range e.Topics {
			if !slices.Contains(s.Topics, tp) {
				s.Topics = append(s.Topics, tp)
			}
		}
		idx := -1
		if gen := namedGeneration(e); gen > 0 {
			// The response (or the sync) names the generation: it belongs to the
			// rebalance that produced it, whenever the request began.
			for j := range spans {
				if spans[j].Done && spans[j].To == gen {
					idx = j
					break
				}
			}
			if idx < 0 && slices.ContainsFunc(s.Generations, func(g Generation) bool { return g.Gen == gen }) {
				// A generation whose rebalance the recorder did not see.
				s.Before = append(s.Before, e)
				continue
			}
		}
		if idx < 0 && joinsAGroup[e.API] {
			// One that arrives while a rebalance is in progress is part of it.
			for j := range spans {
				if !at.Before(spans[j].Start) && (!spans[j].Done || !at.After(spans[j].End)) {
					idx = j
					break
				}
			}
		}
		if idx < 0 && joinsAGroup[e.API] {
			// The earliest rebalance that began at or just after the request: the one
			// it led to, not a later one that began within joinLead of it too.
			for j := range spans {
				if !spans[j].Start.Before(at) && spans[j].Start.Sub(at) <= joinLead {
					idx = j
					break
				}
			}
		}
		if idx < 0 {
			for j := len(spans) - 1; j >= 0; j-- {
				if !at.Before(spans[j].Start) {
					idx = j
					break
				}
			}
		}
		if idx < 0 {
			s.Before = append(s.Before, e)
			continue
		}
		spans[idx].Rows = append(spans[idx].Rows, e)
	}
	// Rebalances newest first, as the page lists them; inside one, the requests in the
	// order they happened.
	slices.Reverse(spans)
	for i := range spans {
		slices.SortStableFunc(spans[i].Rows, func(a, b Event) int {
			return a.Time.Compare(b.Time) // stable: rows seen at one instant stay in the order seen.
		})
	}
	slices.Reverse(s.Before)
	s.Rebalances = spans
	return s
}
