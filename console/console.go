// Package console is a lightweight, server-rendered web UI for inspecting a
// running doze-kafka broker — topics, live messages, consumer groups, and
// cluster info. It is a real Kafka client (franz-go over loopback TCP), not
// a bypass: doze-kafka has no trusted-internal-caller path, so the console
// authenticates like any other client when SASL is configured. HTMX
// (vendored, embedded) drives partial updates; there is no SPA build step
// and the whole thing ships inside the Go binary.
package console

import (
	"context"
	"crypto/tls"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

// defaultPrefix is where the console is mounted when nothing says otherwise.
const defaultPrefix = "/_console"

// Console is the web-UI http.Handler. Mount it on its own listener — Kafka's
// wire protocol can't share a port with HTTP.
type Console struct {
	be          *backend
	reg         *registryClient     // nil when there is no schema registry to talk to.
	traffic     TrafficSource       // nil when the broker's traffic is not available to this console.
	metrics     ClientMetricsSource // nil when clients' own metrics are not available.
	dock        DockSource          // nil: the dock is built from traffic, or not offered.
	traces      traceJobs           // message-lineage scans, running and recent.
	traceHeader string              // an extra record header that holds a trace id (Options.TraceHeader).
	mux         *http.ServeMux
	tmplOnce    sync.Once // the templates are parsed on the first request, not at start (idle heap is a budget).
	tmpl        *template.Template
	dev         string // DOZE_KAFKA_CONSOLE_DEV: read templates and static files from this directory, live.
	prefix      string
	hosts       []string // extra Host names to answer to (see hostguard.go).
	facts       BrokerFacts
}

// Options configures the console.
type Options struct {
	// Addr is the broker's own listen address (e.g. "127.0.0.1:9092") — the
	// console dials it as a real client, same as any Kafka SDK would.
	Addr string
	// SASL credentials the console authenticates with, when the broker has
	// SASL enabled. Left zero when auth is disabled.
	SASLUser, SASLPass, SASLMechanism string
	// TLSConfig, if set, dials the broker over TLS with this trust config.
	TLSConfig *tls.Config
	// OnDial, if set, is called with every TCP connection the console opens to the
	// broker, after it is made and before anything is sent on it. A broker in the same
	// process uses it to know the console's connections for its own, so that the
	// Traffic page can leave the console's requests out without trusting a client id.
	// Its error is not the console's business: the connection is used either way.
	OnDial func(ctx context.Context, conn net.Conn) error
	// Prefix is the URL path the console is mounted under; defaults to
	// "/_console" (or DOZE_KAFKA_CONSOLE_PREFIX, when set).
	Prefix string
	// DisplayAddr is the bootstrap address pages SHOW clients (copy chips
	// etc.) when Addr itself is not dialable by them — the embedded topology
	// dials a unix socket, but apps connect via the supervisor's host:port.
	// Empty = show Addr.
	DisplayAddr string
	// AllowedHosts are hostnames, beyond the always-allowed set (IP literals,
	// localhost, and *.doze names), that the console answers to. "*" turns the
	// DNS-rebinding guard off, for an embedder that mounts the console under
	// names it does not control.
	AllowedHosts []string
	// RegistryURL is the schema registry's base address. Set, the console grows a
	// Schemas section, links a topic to its subjects, shows registry-encoded payloads
	// as the JSON they mean and can produce them. Empty, none of that appears.
	RegistryURL string
	// RegistryUser and RegistryPass are HTTP basic credentials for the registry, when
	// it requires them.
	RegistryUser, RegistryPass string
	// Traffic is what the broker remembers of its clients' requests and its groups'
	// membership changes. Nil, the Traffic section is not offered: the console cannot
	// see the wire of a broker it is not part of.
	Traffic TrafficSource
	// Dock is what the wire dock under a topic, group or subject reads. Nil, it is
	// built from Traffic (which can answer for a topic or a group); with neither, no
	// dock is shown.
	Dock DockSource
	// TraceHeader is the name of a record header whose whole value is a trace id (a
	// request id, a correlation id), found in addition to W3C traceparent and B3.
	TraceHeader string
	// ClientMetrics is what clients have reported about themselves (KIP-714). Nil, the
	// Traffic page has no client metrics section.
	ClientMetrics ClientMetricsSource
	// Facts are what the broker knows about itself that the console cannot read off
	// the wire: its name in the top bar, the Kafka major it declares, where its data
	// is and which SASL mechanisms it serves. The zero value is honest: the pages
	// leave out what they are not told.
	Facts BrokerFacts
}

// BrokerFacts is what the broker's own configuration says about it. The console
// shows these in the top bar, on the Connect page and on a first run; it never
// invents a value for one that was not given.
type BrokerFacts struct {
	// Workspace labels the broker in the top bar: the instance's name (the one it
	// answers to as kafka.<name>.doze), so two brokers on one machine can be told apart.
	Workspace string
	// KafkaMajor is the Kafka major version the broker declares ("Kafka 4.0 compat").
	KafkaMajor int
	// DataDir is the data directory, as the person should read it.
	DataDir string
	// SASLMechanisms are the mechanisms the listener serves; empty when it is open.
	SASLMechanisms []string
	// Version is this doze-kafka's version, and Started when it began serving.
	Version string
	Started time.Time
	// RegistryURL is where clients reach the schema registry, which is not always
	// where the console does (a name the zone routes, against the loopback port).
	RegistryURL string
	// AutoCreateTopics says whether a producer that names a topic that does not exist
	// makes it, with DefaultPartitions partitions.
	AutoCreateTopics  bool
	DefaultPartitions int
	// MaxPartitions is the most partitions one topic may have (--max-partitions); zero
	// when the console was not told, and the forms then name no cap.
	MaxPartitions int
}

// New builds a console with a real Kafka client dialed at opts.Addr.
func New(opts Options) (*Console, error) {
	prefix := opts.Prefix
	if prefix == "" {
		// DOZE_KAFKA_CONSOLE_PREFIX mounts the console elsewhere without a code
		// change: the suites run once under a prefix that is not the default, to show
		// nothing assumes it (like DOZE_KAFKA_CONSOLE_DEV below, a developer's hook).
		prefix = os.Getenv("DOZE_KAFKA_CONSOLE_PREFIX")
	}
	if prefix == "" {
		prefix = defaultPrefix
	}
	prefix = "/" + strings.Trim(prefix, "/")
	if prefix == "/" {
		prefix = "" // mounted at the root: routes are "/topics", not "//topics".
	}

	// Developing the console itself: DOZE_KAFKA_CONSOLE_DEV=./console reads the
	// templates and static files from disk on every request, so an edit shows on
	// reload without rebuilding the binary (`task dev` sets it).
	dev := os.Getenv("DOZE_KAFKA_CONSOLE_DEV")
	c := &Console{prefix: prefix, hosts: opts.AllowedHosts, dev: dev, facts: opts.Facts}
	be, err := newBackend(opts)
	if err != nil {
		return nil, err
	}
	c.be = be
	c.reg = newRegistryClient(opts.RegistryURL, opts.RegistryUser, opts.RegistryPass)
	c.traffic = opts.Traffic
	c.metrics = opts.ClientMetrics
	c.traceHeader = strings.TrimSpace(opts.TraceHeader)
	c.dock = opts.Dock
	c.routes()
	return c, nil
}

// parseTemplates loads the page templates: embedded in the binary, or from disk in
// dev mode.
func (c *Console) parseTemplates() (*template.Template, error) {
	t := template.New("").Funcs(templateFuncs(c.prefix, c.teach))
	if c.dev != "" {
		return t.ParseFS(os.DirFS(c.dev), "templates/*.html")
	}
	return t.ParseFS(templateFS, "templates/*.html")
}

// templates is the template set to render with. In dev mode it is re-read each
// time (a template that does not parse is reported on the page, not swallowed).
func (c *Console) templates() *template.Template {
	c.tmplOnce.Do(func() {
		t, err := c.parseTemplates()
		if err != nil {
			// The embedded set is checked by the suite; a dev tree can be mid-edit.
			log.Printf("console: templates: %v", err)
			t = template.New("")
		}
		c.tmpl = t
	})
	if c.dev == "" {
		return c.tmpl
	}
	t, err := c.parseTemplates()
	if err != nil {
		log.Printf("console dev mode: %v", err)
		return c.tmpl
	}
	return t
}

// Close releases the console's Kafka client.
func (c *Console) Close() { c.be.close() }

func (c *Console) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// DNS rebinding: refuse any Host name we were not expecting, for every
	// method — a rebound GET reads the whole console too (hostguard.go).
	if !hostAllowed(r.Host, c.hosts) {
		refuseHost(w, r.Host)
		return
	}
	// CSRF: a state-changing request must originate from the console itself,
	// which stops a page on another site from submitting to it. This checks
	// Origin against Host; it does NOT stop DNS rebinding (both would name the
	// attacker) — that is the Host guard above.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		if origin := r.Header.Get("Origin"); origin != "" && !originMatchesHost(origin, r.Host) {
			c.refuse(w, Refusal{Kind: Denied, Source: SourceConsole, Status: http.StatusForbidden,
				Message: "cross-origin request refused", Rule: "Refused by the console: a change must come from the console's own pages."})
			return
		}
		// Browsers say where a request came from; a state change coming from
		// another site is refused even if it somehow carries no Origin.
		if site := r.Header.Get("Sec-Fetch-Site"); site == "cross-site" {
			c.refuse(w, Refusal{Kind: Denied, Source: SourceConsole, Status: http.StatusForbidden,
				Message: "cross-site request refused", Rule: "Refused by the console: a change must come from the console's own pages."})
			return
		}
	}
	// The console is never meant to be shown inside another page: framed, its
	// buttons could be clicked through by a page the user thinks they are using.
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// Every request gets a deadline. The console talks to the broker through a
	// Kafka client that retries a dead connection for a long time, and with no
	// bound a page, a poll or a button hangs for as long as that takes.
	if !strings.HasPrefix(r.URL.Path, c.prefix+"/static/") {
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		r = r.WithContext(ctx)
	}
	c.mux.ServeHTTP(w, r)
}

// requestTimeout bounds one console request, including every broker call it makes
// (a variable only so a test need not wait it out).
var requestTimeout = 15 * time.Second

// railCountsTimeout bounds the broker calls that draw the rail's numbers.
const railCountsTimeout = 2 * time.Second

func originMatchesHost(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	return u.Host == host
}

// routes registers the route table (routes.go): the one list of what the
// console answers.
func (c *Console) routes() {
	m := http.NewServeMux()
	c.register(m)
	c.mux = m
}

// render writes a full page (layout + named content template).
func (c *Console) render(w http.ResponseWriter, r *http.Request, page string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	// The handler says which rail section a page belongs to (a topic's detail page
	// is "topics"); only fall back to the template name when it did not. Overwriting
	// it made the rail's server-rendered highlight wrong on every detail page.
	if _, ok := data["Page"]; !ok {
		data["Page"] = page
	}
	c.shellData(r, data)
	if f := r.URL.Query().Get("flash"); f != "" {
		data["Flash"] = f
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := c.templates().ExecuteTemplate(w, page, data); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

// shellData adds what every full page needs of the shell around it: the prefix, the
// broker's address, which sections exist, the rail's numbers and the top bar's
// labels. An error page is a full page too, and calls this as well.
func (c *Console) shellData(r *http.Request, data map[string]any) {
	data["Prefix"] = c.prefix
	data["Endpoint"] = c.be.displayAddr()
	data["HasRegistry"] = c.reg != nil
	// The rail's numbers are drawn with the page, so the first paint has them; the
	// script keeps them current. Bounded: a broker that is not answering must not hold
	// every page for a request's whole deadline just to draw a count.
	cctx, cancel := context.WithTimeout(r.Context(), railCountsTimeout)
	counts := map[string]string{}
	for k, n := range c.railCounts(cctx) {
		counts[k] = strconv.Itoa(n)
	}
	cancel()
	data["Counts"] = counts
	data["Home"] = c.homeSection()
	data["Workspace"] = c.facts.Workspace
	if m := c.facts.KafkaMajor; m > 0 {
		data["KafkaCompat"] = fmt.Sprintf("Kafka %d.0 compat", m)
	}
	data["HasTraffic"] = c.traffic != nil
	// What the keys strip at the foot of the page says: the page's own hints, by its
	// view (a topic's page is "topic", though its section is "topics").
	if _, ok := data["Hints"]; !ok {
		view, _ := data["View"].(string)
		if view == "" {
			view, _ = data["Page"].(string)
		}
		data["Hints"] = pageHints(view)
	}
}

// partial renders a single named template (for HTMX swaps).
func (c *Console) partial(w http.ResponseWriter, name string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["Prefix"] = c.prefix
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := c.templates().ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

// redirect sends the browser to `to` with an optional flash banner — via
// HX-Redirect for htmx requests, 303 See Other for plain forms.
func (c *Console) redirect(w http.ResponseWriter, r *http.Request, to, flash string) {
	if flash != "" {
		sep := "?"
		if strings.Contains(to, "?") {
			sep = "&"
		}
		to += sep + "flash=" + url.QueryEscape(flash)
	}
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", to)
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, to, http.StatusSeeOther)
}

// toast asks the client to show a transient notification. htmx turns the
// HX-Trigger header into a "toast" event the layout's shell.js listens for.
// Call before writing the body.
func toast(w http.ResponseWriter, msg string) {
	// QuoteToASCII (not Quote): HTTP header values are latin-1, so any
	// non-ASCII rune must be backslash-u escaped to survive the header.
	w.Header().Set("HX-Trigger", `{"toast":`+strconv.QuoteToASCII(msg)+`}`)
}

// wrapName escapes s and lets it break after each underscore.
func wrapName(s string) template.HTML {
	return template.HTML(strings.ReplaceAll(template.HTMLEscapeString(s), "_", "_<wbr>"))
}

func templateFuncs(prefix string, teach func(kind, name string) string) template.FuncMap {
	return template.FuncMap{
		"prefix": func() string { return prefix },
		// A name that may wrap, at its underscores: UNKNOWN_TOPIC_OR_PARTITION is three
		// words, not a run to be cut anywhere (AD5).
		"wrapName": wrapName,
		"icon":     icon,
		// Links to resources. Names are data: never concatenate them into a URL
		// in a template, use these (see resourceurl.go).
		"topicURL": func(name string) string { return topicPath(prefix, name) },
		// The address the inspector fetches a selection of one kind from.
		"inspectHref": func(kind, key string) string { return prefix + "/inspect/" + kind + "?key=" + url.QueryEscape(key) },
		"groupURL":    func(id string) string { return groupPath(prefix, id) },
		"newTopicURL": func() string { return newTopicPath(prefix) },
		"schemaURL":   func(subject string) string { return schemaPath(prefix, subject) },
		"newSchemaURL": func(subject string) string {
			u := newSchemaPath(prefix)
			if subject != "" {
				u += "?subject=" + url.QueryEscape(subject)
			}
			return u
		},
		"t": uiText,
		// keyCaps splits how a key is drawn into its caps: "g r" is two keys.
		"keyCaps": func(show string) []string { return strings.Fields(show) },
		// num writes a count with thousands separators (14,407), as the boards do.
		"num": num,
		// needsLook is whether a topic's number is coloured in the list: a dead-letter
		// topic that has records in it.
		"needsLook": func(name string, msgs int64) bool { return isDeadLetterName(name) && msgs > 0 },
		// teach is the line under a field, read from the settings table by the
		// record's kind and name (teaching.go).
		"teach": teach,
		// confirmOffset is the confirm of one row's Remove (confirms.go).
		"confirmOffset": offsetDeleteConfirm,
		"confirmURL":    func(id string, kv ...any) string { return confirmURLFor(prefix, id, kv...) },
		// Keys of the inspector's selections, escaped for a query string.
		"memberKey": func(group, member string) string { return url.QueryEscape(joinKey(group, member)) },
		"eventKey": func(group string, seq uint64) string {
			return url.QueryEscape(joinKey(group, strconv.FormatUint(seq, 10)))
		},
		"txnKey": func(id string) string { return url.QueryEscape(id) },
		"dict": func(kv ...any) map[string]any {
			m := make(map[string]any, len(kv)/2)
			for i := 0; i+1 < len(kv); i += 2 {
				if k, ok := kv[i].(string); ok {
					m[k] = kv[i+1]
				}
			}
			return m
		},
		"list": func(items ...any) []any { return items },
		"sub":  func(a, b int) int { return a - b },
		// when is the one formatter for a moment in time (timefmt.go); whenText is
		// its plain text, for an attribute.
		"when": when,
		// the declared key map, for the help overlay and for shell.js (keys.go).
		"keyGroups":  keyGroups,
		"keymapJSON": keymapJSON,
		"whenText":   whenText,
		"hasPrefix":  strings.HasPrefix,
		"hasSuffix":  strings.HasSuffix,
		"refsJSON": func(refs any) string {
			b, _ := json.Marshal(refs)
			return string(b)
		},
		"lower": strings.ToLower,
		// capital and upper write a word the way a column of the boards does: Topic, Write,
		// ALLOW. The stored value stays as the broker names it.
		"capital":  upperFirst,
		"upper":    strings.ToUpper,
		"jsonTint": jsonTint,
		"stateBadge": func(state string) string {
			switch state {
			case "Stable":
				return "state-stable"
			case "Empty":
				return "state-empty"
			case "Dead":
				return "state-dead"
			default: // PreparingRebalance, CompletingRebalance
				return "state-rebal"
			}
		},
		"protoBadge": func(p string) string {
			if p == "848" {
				return "proto-848"
			}
			return "proto-classic"
		},
		"protoLabel": func(p string) string {
			if p == "848" {
				return "KIP-848"
			}
			return "classic"
		},
	}
}

// humanBytes renders a byte count the way a person reads it: 512 B, 2.1 KB, 3 MB.
func humanBytes(n int) string {
	const u = "BKMGT"
	f, i := float64(n), 0
	for f >= 1024 && i < len(u)-1 {
		f /= 1024
		i++
	}
	if i == 0 {
		return strconv.Itoa(n) + " B"
	}
	return trimFloat(f) + " " + string(u[i]) + "B"
}

func trimFloat(f float64) string {
	s := strconv.FormatFloat(f, 'f', 1, 64)
	return strings.TrimSuffix(s, ".0")
}
