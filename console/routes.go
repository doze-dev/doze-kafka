package console

import (
	"fmt"
	"net/http"
	"strings"
)

// The route table is the one list of what the console answers. routes() in
// console.go registers from it and nothing else, the browser suite's route gate
// (e2e/route-gate.ts) reads it through Routes, and a test in this package holds it
// to the handlers and to the specs that drive it: a route cannot exist without a
// row, and a row says whether it mutates and which spec drives it.

// routeRow is one row.
type routeRow struct {
	// Method is GET or POST.
	Method string
	// Path is relative to the console's prefix, with mux wildcards. "" is the
	// prefix itself.
	Path string
	// Mutates is true for a route that changes state (every POST), and for a GET
	// that does work beyond reading (none today).
	Mutates bool
	// Test is the browser spec (under e2e/tests/) that drives it.
	Test string
	// Note says what it is, in a few words.
	Note string
	// atHostRoot puts the route at the host's root, not under the prefix.
	atHostRoot bool
	// underPrefixOnly drops the route when the console is mounted at the root,
	// where another row already answers for it.
	underPrefixOnly bool
	handler         http.HandlerFunc
}

// RouteInfo is a row as the route gate reads it.
type RouteInfo struct {
	Method  string `json:"method"`
	Path    string `json:"path"` // relative to the prefix; {name} is one segment.
	Mutates bool   `json:"mutates"`
	Test    string `json:"test"`
	Note    string `json:"note"`
}

// Routes is the table without handlers: what the browser suite's gate is held to.
func Routes() []RouteInfo {
	var c *Console // the rows bind methods; none is called here.
	var out []RouteInfo
	for _, r := range c.routeTable() {
		if r.atHostRoot {
			continue
		}
		out = append(out, RouteInfo{Method: r.Method, Path: r.Path, Mutates: r.Mutates, Test: r.Test, Note: r.Note})
	}
	return out
}

const (
	specTopics   = "topics.spec.ts"
	specMessages = "messages.spec.ts"
	specCompose  = "compose.spec.ts"
	specGroups   = "groups.spec.ts"
	specSchemas  = "schemas.spec.ts"
	specTrace    = "trace.spec.ts"
	specTraffic  = "traffic.spec.ts"
	specAccess   = "access.spec.ts"
	specShell    = "shell.spec.ts"
	specSmoke    = "smoke.spec.ts"
)

// routeTable lists every route, in the order they are registered.
func (c *Console) routeTable() []routeRow {
	get := func(path, test, note string, h http.HandlerFunc) routeRow {
		return routeRow{Method: "GET", Path: path, Test: test, Note: note, handler: h}
	}
	post := func(path, test, note string, h http.HandlerFunc) routeRow {
		return routeRow{Method: "POST", Path: path, Mutates: true, Test: test, Note: note, handler: h}
	}
	static := get("/static/", specShell, "the embedded assets", func(w http.ResponseWriter, r *http.Request) {
		http.StripPrefix(c.prefix+"/", c.staticHandler()).ServeHTTP(w, r)
	})
	redirect := get("/{$}", specSmoke, "the host's bare root points at the console", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, c.prefix, http.StatusFound)
	})
	redirect.atHostRoot, redirect.underPrefixOnly = true, true
	home := get("/", specTopics, "Topics is the home surface; a mistyped address under the prefix is a 404", func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimSuffix(r.URL.Path, "/") != strings.TrimSuffix(c.prefix, "/") {
			c.fail(w, r, fmt.Errorf("%w: there is no page at %s", errNotFound, r.URL.Path))
			return
		}
		c.topics(w, r)
	})
	bare := get("", specTopics, "the prefix without its slash", c.topics)
	bare.underPrefixOnly = true

	return []routeRow{
		static,
		redirect,
		home,
		bare,

		// Topics.
		get("/topics", specTopics, "the list", c.topics),
		get("/new/topic", specTopics, "the create form", c.topicCreatePage),
		post("/new/topic", specTopics, "create a topic", c.topicCreate),
		get("/topics/{name}", specTopics, "one topic", c.topicDetail),
		post("/topics/{name}/delete", specTopics, "delete the topic", c.topicDelete),
		post("/topics/{name}/purge", specTopics, "delete its records", c.topicPurge),
		post("/topics/{name}/partitions", specTopics, "grow it; never shrinks", c.topicPartitions),
		post("/topics/{name}/config", specTopics, "its own retention and segment size", c.topicConfig),
		get("/topics/{name}/tape", specMessages, "HTMX partial, polled", c.topicTape),
		post("/topics/{name}/produce", specCompose, "send a record", c.topicProduce),
		post("/topics/{name}/resend", specMessages, "an exact copy of a stored record", c.topicResend),
		get("/topics/{name}/message", specMessages, "inspector partial", c.topicMessage),
		get("/topics/{name}/message/raw", specMessages, "the stored bytes", c.topicMessageRaw),

		// Lineage: where else did the messages of one trace go.
		get("/trace", specTrace, "the page", c.tracePage),
		get("/trace/live", specTrace, "HTMX partial, polled", c.traceLive),

		// Traffic: what clients asked the broker.
		get("/traffic", specTraffic, "the page", c.trafficPage),
		get("/traffic/live", specTraffic, "HTMX partial, polled", c.trafficLive),
		get("/traffic/request", specTraffic, "inspector partial", c.trafficRequest),

		// Schemas: the built-in registry.
		get("/schemas", specSchemas, "the list", c.schemas),
		get("/new/schema", specSchemas, "the register form", c.schemaCreatePage),
		post("/new/schema", specSchemas, "register a first version", c.schemaCreate),
		get("/schemas/{subject}", specSchemas, "one subject", c.schemaDetail),
		post("/schemas/{subject}/check", specSchemas, "try a version against the level", c.schemaCheck),
		post("/schemas/{subject}/register", specSchemas, "register a new version", c.schemaRegister),
		post("/schemas/{subject}/config", specSchemas, "set the compatibility level", c.schemaConfig),
		post("/schemas/{subject}/delete", specSchemas, "delete the subject", c.schemaDelete),

		// Consumer groups.
		get("/groups", specGroups, "the list", c.groups),
		get("/groups/{id}", specGroups, "one group", c.groupDetail),
		get("/groups/{id}/live", specGroups, "HTMX partial, polled", c.groupLive),
		post("/groups/{id}/reset", specGroups, "move the group's offsets", c.groupReset),
		post("/groups/{id}/offset", specGroups, "one partition's committed offset", c.groupOffsetSet),
		post("/groups/{id}/offset/delete", specGroups, "remove one committed offset", c.groupOffsetDelete),
		post("/groups/{id}/delete", specGroups, "delete the group", c.groupDelete),

		// Cluster and access.
		get("/cluster", specShell, "the cluster page", c.cluster),
		get("/access", specAccess, "stored ACLs, read-only", c.access),

		// The shell: the inspector, the wire dock and a confirm's sentence.
		get("/inspect/{kind}", "inspector.spec.ts", "the inspector's body for one selection", c.inspectRoute),
		get("/dock", specTopics, "the wire dock's rows for one topic or group", c.dockLive),
		get("/confirm/{id}", specTopics, "a confirm's sentence, measured when the button is pressed", c.confirmLive),

		// The resource index for the command palette, and the rail's counts.
		get("/api/palette", specShell, "the palette's catalogue", c.apiPalette),
		get("/api/counts", specShell, "the rail's counts", c.apiCounts),
	}
}

// register puts every row on a mux, under the prefix.
func (c *Console) register(m *http.ServeMux) {
	for _, r := range c.routeTable() {
		if r.underPrefixOnly && c.prefix == "" {
			continue
		}
		pattern := r.Path
		if !r.atHostRoot {
			pattern = c.prefix + r.Path
		}
		m.HandleFunc(r.Method+" "+pattern, r.handler)
	}
}
