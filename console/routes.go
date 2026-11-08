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
	specTopicsB  = "topics-boards2.spec.ts"
	specMessages = "messages.spec.ts"
	specCompose  = "compose.spec.ts"
	specGroups   = "groups.spec.ts"
	specSchemas  = "schemas.spec.ts"
	specTrace    = "trace.spec.ts"
	specTraffic  = "traffic.spec.ts"
	specAccess   = "access.spec.ts"
	specShell    = "shell.spec.ts"
	specSmoke    = "smoke.spec.ts"
	specDesign   = "design.spec.ts"
)

// routeTable lists every route, in the order they are registered.
func (c *Console) routeTable() []routeRow {
	get := func(path, test, note string, h http.HandlerFunc) routeRow {
		return routeRow{Method: "GET", Path: path, Test: test, Note: note, handler: h}
	}
	post := func(path, test, note string, h http.HandlerFunc) routeRow {
		return routeRow{Method: "POST", Path: path, Mutates: true, Test: test, Note: note, handler: h}
	}
	// withoutMutating marks a POST that carries a form too big for an address and
	// changes nothing (the Groups reset's preview).
	withoutMutating := func(r routeRow) routeRow { r.Mutates = false; return r }
	static := get("/static/", specShell, "the embedded assets", func(w http.ResponseWriter, r *http.Request) {
		http.StripPrefix(c.prefix+"/", c.staticHandler()).ServeHTTP(w, r)
	})
	redirect := get("/{$}", specSmoke, "the host's bare root points at the console", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, c.prefix, http.StatusFound)
	})
	redirect.atHostRoot, redirect.underPrefixOnly = true, true
	home := get("/", specDesign, "Traffic is the home surface (the first-run page on a broker with nothing in it; Topics where no traffic is recorded); a mistyped address under the prefix is a 404", func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimSuffix(r.URL.Path, "/") != strings.TrimSuffix(c.prefix, "/") {
			c.fail(w, r, fmt.Errorf("%w: there is no page at %s", errNotFound, r.URL.Path))
			return
		}
		c.home(w, r)
	})
	bare := get("", specDesign, "the prefix without its slash", c.home)
	bare.underPrefixOnly = true

	return []routeRow{
		static,
		redirect,
		home,
		bare,

		// Topics.
		get("/topics", specTopics, "the list", c.topics),
		get("/live/topics", specTopics, "Tp-Home's table, polled", c.topicsLive),
		get("/new/topic", specTopics, "the create form", c.topicCreatePage),
		get("/new/topic/shell", specDesign, "the create form's shell command, as typed", c.topicShell),
		post("/new/topic", specTopics, "create a topic", c.topicCreate),
		get("/topics/{name}", specTopics, "one topic", c.topicDetail),
		post("/topics/{name}/delete", specTopics, "delete the topic", c.topicDelete),
		post("/topics/{name}/purge", specTopics, "delete its records", c.topicPurge),
		post("/topics/{name}/partitions", specTopics, "grow it; never shrinks", c.topicPartitions),
		post("/topics/{name}/config", specTopics, "its own retention and segment size", c.topicConfig),
		get("/topics/{name}/config/{key}", specTopicsB, "Tp-19's Before, After and What it affects, as a value is typed", c.topicConfigPreview),
		post("/topics/{name}/config/{key}", specTopicsB, "set one key, or reset it to the broker's default; the broker's refusal is shown as it said it (Tp-20)", c.topicConfigKey),
		get("/topics/{name}/new", specTopicsB, "how many records arrived since a paused view stopped (Tp-26)", c.topicNew),
		get("/topics/{name}/tape", specMessages, "HTMX partial, polled", c.topicTape),
		post("/topics/{name}/produce", specCompose, "send a record", c.topicProduce),
		post("/topics/{name}/compose/preview", specCompose, "what a send would do, checked as it is typed; it is a POST for the size of a value, and writes nothing", c.topicComposePreview),
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
		get("/traffic/new", specTraffic, "how many requests a paused page has missed", c.trafficNew),

		// Schemas: the built-in registry.
		get("/schemas", specSchemas, "the list", c.schemas),
		get("/new/schema", specSchemas, "the register form", c.schemaCreatePage),
		post("/new/schema", specSchemas, "register a version: a new subject's first, or the next", c.schemaCreate),
		withoutMutating(post("/new/schema/check", specSchemas, "whether the draft parses and may follow the subject's versions: asks the registry, and sends nothing it stores", c.schemaRegisterCheck)),
		withoutMutating(post("/new/schema/fix", specSchemas, "the draft with a finding's fix applied; sends nothing", c.schemaRegisterFix)),
		get("/schemas/{subject}", specSchemas, "one subject", c.schemaDetail),
		get("/schemas/{subject}/diff", specSchemas, "two versions compared", c.schemaDiff),
		get("/schemas/{subject}/open-id", specSchemas, "the newest message on the subject's topics that carries an id", c.schemaOpenID),
		withoutMutating(post("/schemas/{subject}/try", specSchemas, "encode or check a JSON value with a version; sends nothing to the registry", c.schemaTry)),
		post("/schemas/{subject}/config", specSchemas, "set the compatibility level and the mode, of the subject or the registry", c.schemaConfig),
		post("/schemas/{subject}/mode", specSchemas, "set the subject's mode", c.schemaMode),
		withoutMutating(post("/schemas/{subject}/level-check", specSchemas, "the latest version checked at a level, which is not saved: sends nothing that changes anything", c.schemaLevelCheck)),
		post("/schemas/{subject}/delete", specSchemas, "delete the subject", c.schemaDelete),

		// Consumer groups.
		get("/groups", specGroups, "the list", c.groups),
		get("/live/groups", specGroups, "Gr-Home's table, polled", c.groupsLive),
		get("/groups/{id}", specGroups, "one group", c.groupDetail),
		get("/groups/{id}/live", specGroups, "HTMX partial, polled", c.groupLive),
		post("/groups/{id}/reset", specGroups, "move the group's offsets", c.groupReset),
		withoutMutating(post("/groups/{id}/reset/preview", specGroups, "the reset as it would be: committed now, after, lag after; sends nothing", c.groupResetPreview)),
		post("/groups/{id}/offset", specGroups, "one partition's committed offset", c.groupOffsetSet),
		post("/groups/{id}/offset/delete", specGroups, "remove one committed offset", c.groupOffsetDelete),
		post("/groups/{id}/delete", specGroups, "delete the group", c.groupDelete),

		// Connect an app: the settings a client needs, from how the broker is running.
		get("/connect", specDesign, "the settings for a client, filled in", c.connectPage),

		// Cluster and access.
		get("/cluster", specShell, "the cluster page", c.cluster),
		get("/access", specAccess, "stored ACLs, the checker card and the wire of ACL requests", c.access),
		get("/access/check", specAccess, "could a principal do this, by the broker's rules (Ac-05)", c.accessCheck),
		get("/new/acl", specAccess, "the create form (Ac-03)", c.aclCreatePage),
		get("/new/acl/preview", specAccess, "the create form's command and what it will do, as typed", c.aclCreatePreview),
		post("/new/acl", specAccess, "create an ACL (CreateAcls)", c.aclCreate),

		// The shell: the inspector, the wire dock and a confirm's sentence.
		get("/inspect/{kind}", "inspector.spec.ts", "the inspector's body for one selection", c.inspectRoute),
		get("/dock", specTopics, "the wire dock's rows for one topic or group", c.dockLive),
		get("/confirm/{id}", specTopics, "a confirm's sentence, measured when the button is pressed", c.confirmLive),

		// The resource index for the command palette, and the rail's counts.
		get("/api/palette", specShell, "the palette's catalogue", c.apiPalette),
		get("/api/counts", specShell, "the rail's counts, and the heartbeat: whether the broker answers", c.apiCounts),
		get("/api/list/{section}", specShell, "the list pane's numbers, polled", c.apiList),
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
