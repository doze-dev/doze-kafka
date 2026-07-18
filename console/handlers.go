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

// apiResources feeds the command palette.
func (c *Console) apiResources(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	type res struct {
		S string `json:"s"`
		N string `json:"n"`
		U string `json:"u"`
	}
	var out []res
	if topics, err := c.be.ListTopics(ctx); err == nil {
		for _, t := range topics {
			out = append(out, res{S: "topics", N: t.Name, U: c.prefix + "/topics/" + t.Name})
		}
	}
	if groups, err := c.be.ListGroups(ctx); err == nil {
		for _, g := range groups {
			out = append(out, res{S: "groups", N: g.ID, U: c.prefix + "/groups/" + g.ID})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// apiCounts feeds the rail's live counts.
func (c *Console) apiCounts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	counts := map[string]int{}
	if v, err := c.be.ListTopics(ctx); err == nil {
		counts["topics"] = len(v)
	}
	if v, err := c.be.ListGroups(ctx); err == nil {
		counts["groups"] = len(v)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(counts)
}
