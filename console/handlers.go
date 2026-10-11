package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

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
	U string `json:"u,omitempty"`
	K string `json:"k"`
	// A names something the console does in the browser (a theme, the rail, pause,
	// help) instead of going to an address; shell.js runs it.
	A string `json:"a,omitempty"`
	// D is what a resource's row says beside its name (Sy-05): "3 partitions · 14,407
	// messages", "Stable · 1 member · lag 0", "Avro · v3". H is an action's hint: the
	// key that does it, or "asks first".
	D string `json:"d,omitempty"`
	H string `json:"h,omitempty"`
}

// apiPalette is the whole palette catalogue: the console's own views and actions
// (defined here, once) followed by every topic and group on the broker.
func (c *Console) apiPalette(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	out := []paletteEntry{
		{S: "views", N: "Topics", U: c.prefix + "/topics", K: "view"},
		{S: "views", N: "Groups", U: c.prefix + "/groups", K: "view"},
		{S: "views", N: "Trace", U: c.prefix + "/trace", K: "view"},
		{S: "views", N: "Cluster", U: c.prefix + "/cluster", K: "view"},
		{S: "views", N: "Access", U: c.prefix + "/access", K: "view"},
		{S: "views", N: "Connect an app", U: c.prefix + "/connect", K: "view"},
		{S: "topics", N: "Create topic", U: c.newTopicURL(), K: "action", H: "c"},
		{S: "views", N: "Follow a message by trace id", U: c.prefix + "/trace", K: "action"},
	}
	// What the console does in the browser, declared once with the keys (keys.go).
	for _, a := range clientActions {
		out = append(out, paletteEntry{S: "views", N: a.Label, K: "action", A: a.ID})
	}
	if c.traffic != nil {
		out = append(out, paletteEntry{S: "views", N: "Traffic", U: c.prefix + "/traffic", K: "view"})
	}
	if c.reg != nil {
		out = append(out, paletteEntry{S: "views", N: "Schemas", U: schemasPath(c.prefix), K: "view"}, paletteEntry{S: "schemas", N: "Register schema", U: newSchemaPath(c.prefix), K: "action"})
		if subjects, err := c.reg.Subjects(ctx); err == nil {
			for _, s := range subjects {
				out = append(out, paletteEntry{S: "schemas", N: s.Name, U: c.schemaURL(s.Name), K: "schema", D: fmt.Sprintf("%s · v%d", s.TypeLabel, s.Latest)})
			}
		}
	}
	if topics, err := c.be.ListTopics(ctx); err == nil {
		for _, t := range topics {
			out = append(out, paletteEntry{S: "topics", N: t.Name, U: c.topicURL(t.Name), K: "topic",
				D: fmt.Sprintf("%d partition%s · %s message%s", t.Partitions, pluralS(int(t.Partitions)), num(t.Msgs), pluralS(int(t.Msgs)))})
		}
	}
	if groups, err := c.be.ListGroups(ctx); err == nil {
		// A group's lag takes a request per group, so it is told only where the broker
		// has a few: the palette is opened often.
		var rows []groupRow
		if len(groups) <= 25 {
			rows = c.groupRows(ctx, groups)
		}
		for i, g := range groups {
			d := fmt.Sprintf("%s · %d member%s", g.State, g.Members, pluralS(g.Members))
			if rows != nil && rows[i].HasOffsets {
				d += " · lag " + num(rows[i].Lag)
			}
			out = append(out, paletteEntry{S: "groups", N: g.ID, U: c.groupURL(g.ID), K: "group", D: d})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// railCounts are the numbers the rail shows beside its items: how many topics,
// groups and subjects there are and how many client connections are open. An item
// whose number could not be read has none (the rail draws nothing, not a zero).
func (c *Console) railCounts(ctx context.Context) map[string]int {
	counts := map[string]int{}
	if topics, groups, err := c.be.Counts(ctx); err == nil {
		counts["topics"], counts["groups"] = topics, groups
	}
	if c.reg != nil {
		if names := c.reg.SubjectNames(ctx); names != nil {
			counts["schemas"] = len(names)
		}
	}
	if c.traffic != nil {
		counts["traffic"] = len(c.traffic.Clients())
	}
	return counts
}

// apiCounts feeds the rail's live counts.
func (c *Console) apiCounts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(c.railCounts(r.Context()))
}
