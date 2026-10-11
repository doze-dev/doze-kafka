package traffic

import (
	"slices"
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
	// Rows are the requests in this span, newest first.
	Rows   []Event
	unseen bool
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

	// Each row goes under the last rebalance that began before it.
	for i := len(rows) - 1; i >= 0; i-- { // oldest first.
		e := rows[i]
		s.Requests += max(e.Count, 1)
		s.Folded += max(e.Count-1, 0)
		for _, tp := range e.Topics {
			if !slices.Contains(s.Topics, tp) {
				s.Topics = append(s.Topics, tp)
			}
		}
		idx := -1
		for j := len(spans) - 1; j >= 0; j-- {
			if !e.First.Before(spans[j].Start) {
				idx = j
				break
			}
		}
		if idx < 0 {
			s.Before = append(s.Before, e)
			continue
		}
		spans[idx].Rows = append(spans[idx].Rows, e)
	}
	// Newest first, as the page lists them.
	slices.Reverse(spans)
	for i := range spans {
		slices.Reverse(spans[i].Rows)
	}
	slices.Reverse(s.Before)
	s.Rebalances = spans
	return s
}
