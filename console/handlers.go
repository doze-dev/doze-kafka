package console

import (
	"encoding/json"
	"net/http"
)

// liveUnchanged replies 204 when the poller's hash matches current content.
func liveUnchanged(w http.ResponseWriter, r *http.Request, hash string) bool {
	// Echo the current hash in a header so the client can update data-hash
	// even when a morph swap leaves the root element's attributes untouched
	// — otherwise the poll never sees its own hash advance and 200-loops.
	w.Header().Set("HX-Live-Hash", hash)
	if q := r.URL.Query().Get("h"); q != "" && q == hash {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

// paletteEntry is one row of the command palette: S is the section (topics,
// groups, views), N the label, U the link, K what to call it.
type paletteEntry struct {
	S string `json:"s"`
	N string `json:"n"`
	U string `json:"u"`
	K string `json:"k"`
}

// apiPalette is the whole palette catalogue: the console's own views and actions
// (defined here, once) followed by every topic and group on the broker.
func (c *Console) apiPalette(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	out := []paletteEntry{
		{"views", "Topics", c.prefix + "/topics", "view"},
		{"views", "Consumer groups", c.prefix + "/groups", "view"},
		{"views", "Cluster", c.prefix + "/cluster", "view"},
		{"views", "Access", c.prefix + "/access", "view"},
		{"topics", "Create topic", c.newTopicURL(), "action"},
	}
	if c.reg != nil {
		out = append(out, paletteEntry{"views", "Schemas", schemasPath(c.prefix), "view"}, paletteEntry{"schemas", "Register schema", newSchemaPath(c.prefix), "action"})
		if subjects, err := c.reg.Subjects(ctx); err == nil {
			for _, s := range subjects {
				out = append(out, paletteEntry{"schemas", s.Name, c.schemaURL(s.Name), "schema"})
			}
		}
	}
	if topics, err := c.be.ListTopics(ctx); err == nil {
		for _, t := range topics {
			out = append(out, paletteEntry{"topics", t.Name, c.topicURL(t.Name), "topic"})
		}
	}
	if groups, err := c.be.ListGroups(ctx); err == nil {
		for _, g := range groups {
			out = append(out, paletteEntry{"groups", g.ID, c.groupURL(g.ID), "group"})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// apiCounts feeds the rail's live counts.
func (c *Console) apiCounts(w http.ResponseWriter, r *http.Request) {
	counts := map[string]int{}
	if topics, groups, err := c.be.Counts(r.Context()); err == nil {
		counts["topics"], counts["groups"] = topics, groups
	}
	if c.reg != nil {
		if names := c.reg.SubjectNames(r.Context()); names != nil {
			counts["schemas"] = len(names)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(counts)
}
