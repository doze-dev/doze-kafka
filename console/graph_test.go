package console

import (
	"strings"
	"testing"
	"time"
)

func fixtureGraph() *Graph {
	t0 := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)
	g := NewGraph()
	g.AddTraffic([]TrafficEvent{
		{API: "Produce", ClientID: "checkout-api", Topics: []string{"orders"}, Time: t0, Count: 1},
		{API: "Produce", ClientID: "checkout-api", Topics: []string{"orders"}, Time: t0.Add(time.Minute), Count: 3},
		{API: "Produce", ClientID: "importer", Topics: []string{"orders", "payments"}, Time: t0},
		{API: "Produce", ClientID: "broken", Topics: []string{"orders"}, Error: "TOPIC_AUTHORIZATION_FAILED"}, // refused: not an edge.
		{API: "Fetch", ClientID: "audit", Topics: []string{"orders"}, Time: t0},
		{API: "Heartbeat", ClientID: "audit", Group: "g-audit"}, // not a read or a write.
		{API: "Produce", Remote: "127.0.0.1:50000", Topics: []string{"payments"}},
	})
	g.AddGroupReads("g-order-validator", []string{"orders"})
	g.AddGroupReads("g-ledger", []string{"payments"})
	g.AddSubjects([]string{"orders-value", "orders-key", "payments-value", "lonely-value", "nodash"}, []string{"orders", "payments"})
	return g
}

// The graph is built from fixtures of the three sources and holds the edges they
// say, once each, and none they do not.
func TestTheNeighboursGraphIsBuiltFromItsSources(t *testing.T) {
	g := fixtureGraph()
	type e struct {
		from, kind, to, source string
	}
	var got []e
	for _, ed := range g.Edges() {
		got = append(got, e{string(ed.From.Kind) + ":" + ed.From.Name, string(ed.Kind), string(ed.To.Kind) + ":" + ed.To.Name, ed.Source})
	}
	want := []e{
		{"client:127.0.0.1:50000", "writes", "topic:payments", "traffic"},
		{"client:audit", "reads", "topic:orders", "traffic"},
		{"client:checkout-api", "writes", "topic:orders", "traffic"},
		{"client:importer", "writes", "topic:orders", "traffic"},
		{"client:importer", "writes", "topic:payments", "traffic"},
		{"group:g-ledger", "reads", "topic:payments", "groups"},
		{"group:g-order-validator", "reads", "topic:orders", "groups"},
		{"subject:orders-key", "describes", "topic:orders", "registry"},
		{"subject:orders-value", "describes", "topic:orders", "registry"},
		{"subject:payments-value", "describes", "topic:payments", "registry"},
	}
	if len(got) != len(want) {
		t.Fatalf("edges = %v\nwant %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("edge %d = %v, want %v", i, got[i], want[i])
		}
	}
	// Repeats are one edge with their counts added and their latest time.
	for _, ed := range g.Edges() {
		if ed.From.Name == "checkout-api" {
			if ed.Count != 4 || !ed.Last.Equal(time.Date(2026, 10, 11, 9, 1, 0, 0, time.UTC)) {
				t.Errorf("checkout-api edge = %+v", ed)
			}
		}
	}
}

func TestNeighboursSayHowANodeIsConnected(t *testing.T) {
	g := fixtureGraph()
	say := func(kind NodeKind, name string) string {
		var parts []string
		for _, p := range g.Neighbours(kind, name) {
			var names []string
			for _, n := range p.Nodes {
				names = append(names, n.Name)
			}
			parts = append(parts, p.Verb+" "+strings.Join(names, "+"))
		}
		return strings.Join(parts, " | ")
	}
	for _, tc := range []struct {
		kind       NodeKind
		name, want string
	}{
		{NodeTopic, "orders", "Written by checkout-api+importer | read by g-order-validator+audit | schema subjects orders-key+orders-value"},
		{NodeTopic, "payments", "Written by 127.0.0.1:50000+importer | read by g-ledger | schema subject payments-value"},
		{NodeGroup, "g-ledger", "Reads payments | schema subject payments-value"},
		{NodeClient, "importer", "Writes orders+payments"},
		{NodeSubject, "orders-value", "Describes topic orders"},
		{NodeTopic, "nothing-here", ""},
	} {
		if got := say(tc.kind, tc.name); got != tc.want {
			t.Errorf("%s %s: %q, want %q", tc.kind, tc.name, got, tc.want)
		}
	}
}

// The sentence component renders from the graph: its words, its links, and a
// subject that opens in the inspector.
func TestTheNeighboursSentenceRendersFromTheGraph(t *testing.T) {
	src := sampleTraffic()
	c, addr, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 1, 2)
	register(t, c, "orders-value", userV1)
	commitFor(t, addr, "g-order-validator", "orders", map[int32]int64{0: 1})

	page := req(t, c, "GET", "/_console/topics/orders", nil).Body.String()
	sentence := between(page, `id="neighbours"`, `<nav class="tabs"`)
	for _, want := range []string{
		"written by", `href="/_console/traffic?client=billing"`, ">billing</span>",
		"read by", `href="/_console/groups/g-order-validator"`,
		`hx-get="/_console/inspect/subject?key=orders-value"`, `data-inspector-open`, `href="/_console/schemas/orders-value"`,
	} {
		if !strings.Contains(sentence, want) {
			t.Errorf("the sentence is missing %q:\n%s", want, sentence)
		}
	}
	// A topic nobody touches has no sentence, not an empty one.
	topicWith(t, c, "quiet", 1, 1)
	if q := req(t, c, "GET", "/_console/topics/quiet", nil).Body.String(); strings.Contains(q, `id="neighbours"`) {
		t.Error("a topic with no neighbours should have no sentence")
	}
	// A group says what it reads, and a subject which topic it describes.
	grp := req(t, c, "GET", "/_console/groups/g-order-validator", nil).Body.String()
	if s := between(grp, `id="neighbours"`, `</div>`); !strings.Contains(s, "reads") || !strings.Contains(s, `href="/_console/topics/orders"`) {
		t.Errorf("the group's sentence: %s", s)
	}
	sub := req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String()
	if s := between(sub, `id="neighbours"`, `</div>`); !strings.Contains(s, "describes topic") || !strings.Contains(s, `href="/_console/topics/orders"`) {
		t.Errorf("the subject's sentence: %s", s)
	}
}
