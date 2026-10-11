package console

import "context"

// neighbourLink is one name in the neighbours sentence, with the words that join it
// to the one before.
type neighbourLink struct {
	Name string
	URL  string
	// Open, for a subject, is where the inspector fetches it from: the link opens
	// the subject in place, and URL is where it goes without scripts.
	Open string
	Sep  string
}

// neighbourClause is one clause of the sentence.
type neighbourClause struct {
	Verb  string
	Links []neighbourLink
}

// neighbourClauses renders a node's phrases as clauses of links: a topic, group
// or subject opens its page, a client its requests, and a subject opens in the
// inspector. Names are joined "a, b and c".
func (c *Console) neighbourClauses(g *Graph, kind NodeKind, name string) []neighbourClause {
	var out []neighbourClause
	for _, ph := range g.Neighbours(kind, name) {
		cl := neighbourClause{Verb: ph.Verb}
		for i, n := range ph.Nodes {
			l := neighbourLink{Name: n.Name}
			switch {
			case i == 0:
			case i == len(ph.Nodes)-1:
				l.Sep = " and "
			default:
				l.Sep = ", "
			}
			switch n.Kind {
			case NodeTopic:
				l.URL = c.topicURL(n.Name)
			case NodeGroup:
				l.URL = c.groupURL(n.Name)
			case NodeClient:
				l.URL = c.prefix + "/traffic?" + TrafficFilter{Client: n.Name}.query()
			case NodeSubject:
				l.URL = c.schemaURL(n.Name)
				l.Open = c.inspectURL(KindSubject, n.Name)
			}
			cl.Links = append(cl.Links, l)
		}
		out = append(out, cl)
	}
	return out
}

// topicGraph is the graph around one topic: the clients the recorder saw write and
// read it, the groups that have committed offsets on it (already known to the page
// that asks) and the subjects the registry names for it.
func (c *Console) topicGraph(ctx context.Context, topic string, groups []consumerRow) *Graph {
	g := NewGraph()
	if c.traffic != nil {
		g.AddTraffic(c.traffic.Events(TrafficFilter{Topic: topic}, 0))
	}
	for _, gr := range groups {
		g.AddGroupReads(gr.Group, []string{topic})
	}
	if c.reg != nil {
		g.AddSubjects(c.reg.SubjectNames(ctx), []string{topic})
	}
	return g
}
