package console

// The neighbours graph: who writes a topic, who reads it, which schema subject
// describes it. It is a small graph, derived from what the recorder, the
// coordinators and the registry already know, and nothing is stored: it is built
// from those sources when a page asks.
//
//	nodes   client, topic, group, subject
//	edges   writes    client  -> topic    a Produce the recorder saw
//	        reads     client  -> topic    a Fetch the recorder saw
//	        reads     group   -> topic    an offset the group has committed there
//	        describes subject -> topic    the registry's naming (<topic>-key, <topic>-value)
//
// The sentence on a topic's page ("Written by checkout-api, read by g-order-
// validator, schema subject orders-value") renders from Neighbours. The map stage
// draws the same Graph, with lag on the edges.

import (
	"sort"
	"strings"
	"time"
)

// NodeKind is what a node is.
type NodeKind string

// EdgeKind is what an edge says.
type EdgeKind string

const (
	NodeClient  NodeKind = "client"
	NodeTopic   NodeKind = "topic"
	NodeGroup   NodeKind = "group"
	NodeSubject NodeKind = "subject"

	EdgeWrites    EdgeKind = "writes"
	EdgeReads     EdgeKind = "reads"
	EdgeDescribes EdgeKind = "describes"
	EdgeMember    EdgeKind = "member" // client -> group: a client id the coordinator holds as a member.
)

// Edge sources: where an edge was learned.
const (
	FromTraffic  = "traffic"  // the recorder: a request the broker answered.
	FromGroups   = "groups"   // a coordinator: an offset a group committed.
	FromRegistry = "registry" // the registry: a subject named for a topic.
	FromMembers  = "members"  // a coordinator: who is a member of a group now.
)

// Node is a client, topic, group or subject, by name.
type Node struct {
	Kind NodeKind
	Name string
}

// Edge is one relation. Count and Last are what the source knew (a recorder row's
// repeats and last time); an edge from a source with neither leaves them zero.
type Edge struct {
	From, To Node
	Kind     EdgeKind
	Source   string
	Count    int
	Last     time.Time
}

type edgeKey struct {
	from, to Node
	kind     EdgeKind
}

// Graph is a set of edges, one per (from, kind, to).
type Graph struct {
	edges map[edgeKey]*Edge
}

// NewGraph is an empty graph.
func NewGraph() *Graph { return &Graph{edges: map[edgeKey]*Edge{}} }

// Add records an edge; one already there takes the larger count and the later time.
func (g *Graph) Add(e Edge) {
	k := edgeKey{e.From, e.To, e.Kind}
	if old, ok := g.edges[k]; ok {
		old.Count += e.Count
		if e.Last.After(old.Last) {
			old.Last = e.Last
		}
		return
	}
	cp := e
	g.edges[k] = &cp
}

// Edges is every edge, in a stable order.
func (g *Graph) Edges() []Edge {
	out := make([]Edge, 0, len(g.edges))
	for _, e := range g.edges {
		out = append(out, *e)
	}
	label := func(n Node) string { return string(n.Kind) + ":" + n.Name }
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.From != b.From {
			return label(a.From) < label(b.From)
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return label(a.To) < label(b.To)
	})
	return out
}

// AddTraffic adds what the recorder saw: a client that produced to a topic writes
// it, and one that fetched from it reads it. A request that failed is not an edge:
// it did not write or read anything.
func (g *Graph) AddTraffic(events []TrafficEvent) {
	for _, e := range events {
		if e.Error != "" {
			continue
		}
		var kind EdgeKind
		switch e.API {
		case "Produce":
			kind = EdgeWrites
		case "Fetch":
			kind = EdgeReads
		default:
			continue
		}
		who := e.ClientID
		if who == "" {
			who = e.Remote
		}
		if who == "" {
			continue
		}
		n := max(e.Count, 1)
		for _, t := range e.Topics {
			g.Add(Edge{From: Node{NodeClient, who}, To: Node{NodeTopic, t}, Kind: kind, Source: FromTraffic, Count: n, Last: e.Time})
		}
	}
}

// AddGroupReads adds that a group reads the topics it has committed offsets on.
func (g *Graph) AddGroupReads(group string, topics []string) {
	for _, t := range topics {
		g.Add(Edge{From: Node{NodeGroup, group}, To: Node{NodeTopic, t}, Kind: EdgeReads, Source: FromGroups})
	}
}

// AddGroupMembers adds that each client id is a member of the group now.
func (g *Graph) AddGroupMembers(group string, clients []string) {
	for _, c := range clients {
		if c != "" {
			g.Add(Edge{From: Node{NodeClient, c}, To: Node{NodeGroup, group}, Kind: EdgeMember, Source: FromMembers})
		}
	}
}

// subjectTopic is the topic a subject describes by the registry's usual naming
// (TopicNameStrategy: <topic>-key and <topic>-value), or "" when it does not follow it.
func subjectTopic(subject string) string {
	for _, suffix := range []string{"-value", "-key"} {
		if t, ok := strings.CutSuffix(subject, suffix); ok && t != "" {
			return t
		}
	}
	return ""
}

// AddSubjects adds that a subject describes the topic it is named for, when that
// topic is one of topics.
func (g *Graph) AddSubjects(subjects, topics []string) {
	have := map[string]bool{}
	for _, t := range topics {
		have[t] = true
	}
	for _, s := range subjects {
		if t := subjectTopic(s); t != "" && have[t] {
			g.Add(Edge{From: Node{NodeSubject, s}, To: Node{NodeTopic, t}, Kind: EdgeDescribes, Source: FromRegistry})
		}
	}
}

// Phrase is one clause of the sentence: a verb and the nodes it applies to.
type Phrase struct {
	Verb  string
	Nodes []Node
}

// Neighbours is the clauses that say how one node is connected: for a topic, who
// writes it, who reads it and which subjects describe it; for a group, what it
// reads; for a subject, which topic it describes; for a client, what it writes and
// reads. A node with no edges has no clauses.
func (g *Graph) Neighbours(kind NodeKind, name string) []Phrase {
	self := Node{kind, name}
	collect := func(match func(e Edge) (Node, bool)) []Node {
		var out []Node
		for _, e := range g.Edges() {
			if n, ok := match(e); ok {
				out = append(out, n)
			}
		}
		return out
	}
	var phrases []Phrase
	add := func(verb string, nodes []Node) {
		if len(nodes) > 0 {
			phrases = append(phrases, Phrase{Verb: verb, Nodes: nodes})
		}
	}
	incoming := func(k EdgeKind, from NodeKind) []Node {
		return collect(func(e Edge) (Node, bool) { return e.From, e.To == self && e.Kind == k && e.From.Kind == from })
	}
	outgoing := func(k EdgeKind, to NodeKind) []Node {
		return collect(func(e Edge) (Node, bool) { return e.To, e.From == self && e.Kind == k && e.To.Kind == to })
	}
	switch kind {
	case NodeTopic:
		add("Written by", incoming(EdgeWrites, NodeClient))
		readers := append(incoming(EdgeReads, NodeGroup), incoming(EdgeReads, NodeClient)...)
		add("read by", readers)
		subjects := incoming(EdgeDescribes, NodeSubject)
		if len(subjects) == 1 {
			add("schema subject", subjects)
		} else {
			add("schema subjects", subjects)
		}
	case NodeGroup, NodeClient:
		add("Writes", outgoing(EdgeWrites, NodeTopic))
		read := outgoing(EdgeReads, NodeTopic)
		add("reads", read)
		if kind == NodeGroup {
			add("members", incoming(EdgeMember, NodeClient))
			// The schema of what it reads: the subjects that describe those topics.
			reads := map[Node]bool{}
			for _, t := range read {
				reads[t] = true
			}
			subjects := collect(func(e Edge) (Node, bool) {
				return e.From, e.Kind == EdgeDescribes && e.From.Kind == NodeSubject && reads[e.To]
			})
			if len(subjects) == 1 {
				add("schema subject", subjects)
			} else {
				add("schema subjects", subjects)
			}
		}
	case NodeSubject:
		add("Describes topic", outgoing(EdgeDescribes, NodeTopic))
	}
	if len(phrases) > 0 {
		// The first clause starts the sentence; the rest follow it.
		phrases[0].Verb = upperFirst(phrases[0].Verb)
	}
	return phrases
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
