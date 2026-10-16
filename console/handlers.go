package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
)

// pluralWord is a noun's plural: fetch, fetches; heartbeat, heartbeats.
func pluralWord(w string) string {
	for _, end := range []string{"ch", "sh", "s", "x", "z"} {
		if strings.HasSuffix(w, end) {
			return w + "es"
		}
	}
	return w + "s"
}

func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// livePartial answers a polled region: the template name draws the region's element
// (with data-hash="{{.LiveHash}}") around name+"_in", its content. 204 when the
// content is what the poller already has (the hash of its rendered bytes), else the
// region with its new hash.
func (c *Console) livePartial(w http.ResponseWriter, r *http.Request, name string, data map[string]any) {
	data["Prefix"] = c.prefix
	h, err := c.liveHash(name, data)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if liveUnchanged(w, r, h) {
		return
	}
	data["LiveHash"] = h
	var buf bytes.Buffer
	if err := c.templates().ExecuteTemplate(&buf, name, data); err != nil {
		c.fail(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(buf.Bytes())
}

// liveHash is the hash of a polled region's content, which the page is drawn with
// (data-hash) so that its first poll is answered 204 when nothing changed.
func (c *Console) liveHash(name string, data map[string]any) (string, error) {
	data["Prefix"] = c.prefix
	var buf bytes.Buffer
	if err := c.templates().ExecuteTemplate(&buf, name+"_in", data); err != nil {
		return "", err
	}
	h := fnv.New64a()
	h.Write(buf.Bytes())
	return fmt.Sprintf("%x", h.Sum64()), nil
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

// paletteEntry is one row of the command palette: S is the group it sits under
// (topics, groups, schemas, actions: Sy-05 has those four and no "Go to", the
// sections being reached by the rail and the g chords), N the label, U the link,
// K what to call it.
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
		{S: "topics", N: "Create topic", U: c.newTopicURL(), K: "action", H: "c"},
		{S: "actions", N: "Follow a message by trace id", U: c.prefix + "/trace", K: "action"},
	}
	if c.traffic != nil {
		out = append(out, paletteEntry{S: "actions", N: "Show errors", U: c.prefix + "/traffic?kind=errors", K: "action", H: "e"})
	}
	// What the console does in the browser, declared once with the keys (keys.go).
	for _, a := range clientActions {
		out = append(out, paletteEntry{S: "actions", N: a.Label, K: "action", A: a.ID})
	}
	if c.reg != nil {
		out = append(out, paletteEntry{S: "schemas", N: "Register schema", U: newSchemaPath(c.prefix), K: "action"})
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
		counts["traffic"] = clientCount(c.traffic.Clients())
	}
	return counts
}

// apiCounts feeds the rail's live counts, and is the shell's heartbeat (A7): every
// page asks it every few seconds, and "broker" says whether the broker answered (1)
// or not (0) within the rail's deadline. A page with nothing live of its own learns
// from it that the broker is gone; when the console itself is gone the request fails.
func (c *Console) apiCounts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), railCountsTimeout)
	defer cancel()
	counts := c.railCounts(ctx)
	counts["broker"] = 0
	if _, ok := counts["topics"]; ok {
		counts["broker"] = 1
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(counts)
}

// listNumber is one row of the list pane as the poll redraws it: the number beside
// the name, its tone, and (for a group) its state dot.
type listNumber struct {
	Name string `json:"name"`
	N    string `json:"n"`
	Tone string `json:"tone"`
	Dot  string `json:"dot,omitempty"`
}

// apiList answers the list pane's poll: the numbers beside each topic (records) or
// group (lag), drawn as the pane draws them. The rail's count and the list beside it
// move together; a list that differs in its names is fetched again as a page.
func (c *Console) apiList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var out []listNumber
	switch r.PathValue("section") {
	case "topics":
		list, err := c.be.ListTopics(ctx)
		if err != nil {
			c.fail(w, r, err)
			return
		}
		for _, t := range list {
			tone := "mut"
			if isDeadLetterName(t.Name) && t.Msgs > 0 {
				tone = "warn"
			}
			out = append(out, listNumber{Name: t.Name, N: num(t.Msgs), Tone: tone})
		}
	case "groups":
		list, err := c.be.ListGroups(ctx)
		if err != nil {
			c.fail(w, r, err)
			return
		}
		for _, g := range c.groupRows(ctx, list) {
			tone := "mut"
			if g.Behind {
				tone = "warn"
			}
			out = append(out, listNumber{Name: g.ID, N: num(g.Lag), Tone: tone, Dot: groupDot(g.State)})
		}
	default:
		c.fail(w, r, fmt.Errorf("%w: there is no list %q", errNotFound, r.PathValue("section")))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(out)
}

// groupDot is the list's state dot for a group: green when stable, grey when empty,
// red when dead, amber otherwise (rebalancing).
func groupDot(state string) string {
	switch state {
	case "Stable":
		return ""
	case "Empty":
		return "off"
	case "Dead":
		return "bad"
	}
	return "warn"
}
