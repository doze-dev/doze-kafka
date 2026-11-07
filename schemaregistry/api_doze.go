package schemaregistry

import (
	"net/http"
	"strconv"
	"unicode/utf8"
)

// The extension routes, under /_doze/: not Confluent paths, and listed as such in
// docs/schema-registry.md. They ask for the same credentials as every route.

// maxAgentLen bounds the User-Agent a version keeps.
const maxAgentLen = 256

// originOf is who a request came from, as the registry keeps it with a version.
func originOf(r *http.Request) Origin {
	user, _, _ := r.BasicAuth()
	agent := r.UserAgent()
	if len(agent) > maxAgentLen {
		agent = agent[:maxAgentLen]
		for !utf8.ValidString(agent) {
			agent = agent[:len(agent)-1]
		}
	}
	return Origin{User: user, From: clientAddr(r), Agent: agent}
}

func (a *API) dozeRoutes() {
	a.mux.HandleFunc("GET /_doze/overview", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, a.reg.Overview())
	})
	// A check at any level, the subject's left as it is, with each reason taken apart.
	a.mux.HandleFunc("POST /_doze/subjects/{subject}/check", func(w http.ResponseWriter, r *http.Request) {
		in, e := readInput(r)
		if e != nil {
			writeErr(w, e)
			return
		}
		version := r.URL.Query().Get("version")
		against, found, e := a.reg.checkAt(r.PathValue("subject"), version, in, boolParam(r, "normalize"), r.URL.Query().Get("level"))
		if e != nil {
			writeErr(w, e)
			return
		}
		type finding struct {
			Version int            `json:"version"`
			Level   string         `json:"level"`
			Reasons []CompatReason `json:"reasons"`
		}
		out := struct {
			Compatible bool      `json:"compatible"`
			Level      string    `json:"level"`
			Compared   []int     `json:"compared"`
			Findings   []finding `json:"findings"`
		}{Compatible: len(found) == 0, Level: a.reg.levelFor(r.PathValue("subject"), r.URL.Query().Get("level")), Compared: []int{}, Findings: []finding{}}
		for _, v := range against {
			out.Compared = append(out.Compared, v.Version)
		}
		for _, f := range found {
			out.Findings = append(out.Findings, finding{Version: f.Version, Level: f.Level, Reasons: f.Details})
		}
		writeJSON(w, 200, out)
	})
	a.mux.HandleFunc("GET /_doze/subjects/{subject}/versions/{version}/resolved", func(w http.ResponseWriter, r *http.Request) {
		v, e := a.reg.Resolved(r.PathValue("subject"), r.PathValue("version"))
		if e != nil {
			writeErr(w, e)
			return
		}
		writeJSON(w, 200, v)
	})
	a.mux.HandleFunc("GET /_doze/requests", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		before, _ := strconv.ParseUint(q.Get("before"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))
		writeJSON(w, 200, a.Requests(RequestFilter{Subject: q.Get("subject"), Before: before, Limit: limit}))
	})
	a.mux.HandleFunc("GET /_doze/subjects/{subject}/versions/{version}", func(w http.ResponseWriter, r *http.Request) {
		v, e := a.reg.VersionInfo(r.PathValue("subject"), r.PathValue("version"))
		if e != nil {
			writeErr(w, e)
			return
		}
		writeJSON(w, 200, v)
	})
}
