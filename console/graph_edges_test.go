package console

import (
	"strings"
	"testing"
	"time"
)

func phrases(g *Graph, kind NodeKind, name string) string {
	var out []string
	for _, p := range g.Neighbours(kind, name) {
		var names []string
		for _, n := range p.Nodes {
			names = append(names, n.Name)
		}
		out = append(out, p.Verb+": "+strings.Join(names, ","))
	}
	return strings.Join(out, " | ")
}

func TestGraphAddMergesRepeats(t *testing.T) {
	g := NewGraph()
	t0 := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	e := Edge{From: Node{NodeClient, "c"}, To: Node{NodeTopic, "t"}, Kind: EdgeWrites, Count: 2, Last: t0}
	g.Add(e)
	e.Count, e.Last = 3, t0.Add(time.Hour)
	g.Add(e)
	e.Count, e.Last = 4, t0.Add(-time.Hour)
	g.Add(e)
	got := g.Edges()
	if len(got) != 1 || got[0].Count != 9 || !got[0].Last.Equal(t0.Add(time.Hour)) {
		t.Errorf("%+v", got)
	}
	// Another kind between the same nodes is another edge.
	g.Add(Edge{From: Node{NodeClient, "c"}, To: Node{NodeTopic, "t"}, Kind: EdgeReads})
	if len(g.Edges()) != 2 {
		t.Errorf("%d edges", len(g.Edges()))
	}
}

func TestGraphFromTrafficGroupsAndSubjects(t *testing.T) {
	g := NewGraph()
	g.AddTraffic([]TrafficEvent{
		{API: "Produce", ClientID: "p1", Topics: []string{"orders", "audit"}, Count: 3},
		{API: "Produce", ClientID: "p1", Topics: []string{"orders"}},
		{API: "Produce", Remote: "10.0.0.7:5", Topics: []string{"orders"}},
		{API: "Produce", Topics: []string{"orders"}}, // no one to name.
		{API: "Fetch", ClientID: "r1", Topics: []string{"orders"}, Error: "X"},
		{API: "Fetch", ClientID: "r2", Topics: []string{"orders"}},
		{API: "Metadata", ClientID: "m", Topics: []string{"orders"}},
	})
	g.AddGroupReads("grp", []string{"orders"})
	g.AddGroupMembers("grp", []string{"r2", "", "p1"})
	g.AddSubjects([]string{"orders-value", "orders-key", "other-value", "-value", "plainsubject", "audit-key"}, []string{"orders", "audit"})
	if got := phrases(g, NodeTopic, "orders"); got != "Written by: 10.0.0.7:5,p1 | read by: grp,r2 | schema subjects: orders-key,orders-value" {
		t.Errorf("orders: %s", got)
	}
	if got := phrases(g, NodeTopic, "audit"); got != "Written by: p1 | schema subject: audit-key" {
		t.Errorf("audit: %s", got)
	}
	if got := phrases(g, NodeClient, "p1"); got != "Writes: audit,orders" {
		t.Errorf("p1: %s", got)
	}
	if got := phrases(g, NodeClient, "r2"); got != "Reads: orders" {
		t.Errorf("r2: %s", got)
	}
	if got := phrases(g, NodeGroup, "grp"); got != "Reads: orders | members: p1,r2 | schema subjects: orders-key,orders-value" {
		t.Errorf("grp: %s", got)
	}
	if got := phrases(g, NodeSubject, "orders-value"); got != "Describes topic: orders" {
		t.Errorf("subject: %s", got)
	}
	if got := phrases(g, NodeTopic, "nobody"); got != "" {
		t.Errorf("a node with no edges: %s", got)
	}
	var count int
	for _, e := range g.Edges() {
		if e.From.Name == "p1" && e.To.Name == "orders" {
			count = e.Count
		}
	}
	if count != 4 {
		t.Errorf("p1 wrote orders %d times, want 3+1", count)
	}
	// A group that reads a topic with exactly one subject.
	g2 := NewGraph()
	g2.AddGroupReads("g", []string{"t"})
	g2.AddSubjects([]string{"t-value"}, []string{"t"})
	if got := phrases(g2, NodeGroup, "g"); got != "Reads: t | schema subject: t-value" {
		t.Errorf("%s", got)
	}
}

func TestSubjectTopicAndUpperFirst(t *testing.T) {
	for in, want := range map[string]string{"orders-value": "orders", "orders-key": "orders", "a-b-value": "a-b", "-value": "", "-key": "", "plain": "", "value": "", "x-valuex": "", "orders-value-key": "orders-value"} {
		if got := subjectTopic(in); got != want {
			t.Errorf("subjectTopic(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{"": "", "a": "A", "abc": "Abc", "Abc": "Abc", "1a": "1a"} {
		if got := upperFirst(in); got != want {
			t.Errorf("upperFirst(%q) = %q", in, got)
		}
	}
}
