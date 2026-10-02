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
	"html/template"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

// Console is the web-UI http.Handler. Mount it on its own listener — Kafka's
// wire protocol can't share a port with HTTP.
type Console struct {
	be     *backend
	mux    *http.ServeMux
	tmpl   *template.Template
	dev    string // DOZE_KAFKA_CONSOLE_DEV: read templates and static files from this directory, live.
	prefix string
	hosts  []string // extra Host names to answer to (see hostguard.go).
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
	// Prefix is the URL path the console is mounted under; defaults to
	// "/_console".
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
}

// New builds a console with a real Kafka client dialed at opts.Addr.
func New(opts Options) (*Console, error) {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "/_console"
	}
	prefix = "/" + strings.Trim(prefix, "/")

	// Developing the console itself: DOZE_KAFKA_CONSOLE_DEV=./console reads the
	// templates and static files from disk on every request, so an edit shows on
	// reload without rebuilding the binary (`task dev` sets it).
	dev := os.Getenv("DOZE_KAFKA_CONSOLE_DEV")
	c := &Console{prefix: prefix, hosts: opts.AllowedHosts, dev: dev}
	tmpl, err := c.parseTemplates()
	if err != nil {
		return nil, err
	}
	be, err := newBackend(opts)
	if err != nil {
		return nil, err
	}
	c.be, c.tmpl = be, tmpl
	c.routes()
	return c, nil
}

// parseTemplates loads the page templates: embedded in the binary, or from disk in
// dev mode.
func (c *Console) parseTemplates() (*template.Template, error) {
	t := template.New("").Funcs(templateFuncs(c.prefix))
	if c.dev != "" {
		return t.ParseFS(os.DirFS(c.dev), "templates/*.html")
	}
	return t.ParseFS(templateFS, "templates/*.html")
}

// templates is the template set to render with. In dev mode it is re-read each
// time (a template that does not parse is reported on the page, not swallowed).
func (c *Console) templates() *template.Template {
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
			http.Error(w, "cross-origin request refused", http.StatusForbidden)
			return
		}
	}
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

func originMatchesHost(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	return u.Host == host
}

func (c *Console) routes() {
	m := http.NewServeMux()
	p := c.prefix

	m.Handle("GET "+p+"/static/", http.StripPrefix(p+"/", c.staticHandler()))

	// The bare root points at the console, so http://host:port works as a URL.
	if p != "" && p != "/" {
		m.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, p, http.StatusFound)
		})
	}

	// Topics is the home surface.
	m.HandleFunc("GET "+p+"/", c.topics)
	m.HandleFunc("GET "+p, c.topics)
	m.HandleFunc("GET "+p+"/topics", c.topics)
	m.HandleFunc("GET "+newTopicPath(p), c.topicCreatePage)
	m.HandleFunc("POST "+newTopicPath(p), c.topicCreate)
	m.HandleFunc("GET "+p+"/topics/{name}", c.topicDetail)
	m.HandleFunc("POST "+p+"/topics/{name}/delete", c.topicDelete)
	m.HandleFunc("POST "+p+"/topics/{name}/purge", c.topicPurge)
	m.HandleFunc("GET "+p+"/topics/{name}/tape", c.topicTape) // HTMX partial (polled)
	m.HandleFunc("POST "+p+"/topics/{name}/produce", c.topicProduce)
	m.HandleFunc("GET "+p+"/topics/{name}/groups", c.topicGroups)   // "who consumes this" strip
	m.HandleFunc("GET "+p+"/topics/{name}/message", c.topicMessage) // drawer partial
	m.HandleFunc("GET "+p+"/topics/{name}/message/raw", c.topicMessageRaw)

	// Consumer groups.
	m.HandleFunc("GET "+p+"/groups", c.groups)
	m.HandleFunc("GET "+p+"/groups/{id}", c.groupDetail)
	m.HandleFunc("GET "+p+"/groups/{id}/live", c.groupLive) // HTMX partial (polled)
	m.HandleFunc("POST "+p+"/groups/{id}/reset", c.groupReset)
	m.HandleFunc("POST "+p+"/groups/{id}/delete", c.groupDelete)

	// Cluster.
	m.HandleFunc("GET "+p+"/cluster", c.cluster)

	// Resource index for the command palette.
	m.HandleFunc("GET "+p+"/api/palette", c.apiPalette)
	m.HandleFunc("GET "+p+"/api/counts", c.apiCounts)

	c.mux = m
}

// render writes a full page (layout + named content template).
func (c *Console) render(w http.ResponseWriter, r *http.Request, page string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["Prefix"] = c.prefix
	// The handler says which rail section a page belongs to (a topic's detail page
	// is "topics"); only fall back to the template name when it did not. Overwriting
	// it made the rail's server-rendered highlight wrong on every detail page.
	if _, ok := data["Page"]; !ok {
		data["Page"] = page
	}
	data["Endpoint"] = c.be.displayAddr()
	if f := r.URL.Query().Get("flash"); f != "" {
		data["Flash"] = f
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := c.templates().ExecuteTemplate(w, page, data); err != nil {
		http.Error(w, err.Error(), 500)
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

func templateFuncs(prefix string) template.FuncMap {
	return template.FuncMap{
		"prefix": func() string { return prefix },
		"icon":   icon,
		// Links to resources. Names are data: never concatenate them into a URL
		// in a template, use these (see resourceurl.go).
		"topicURL":    func(name string) string { return topicPath(prefix, name) },
		"groupURL":    func(id string) string { return groupPath(prefix, id) },
		"newTopicURL": func() string { return newTopicPath(prefix) },
		"t":           uiText,
		"dict": func(kv ...any) map[string]any {
			m := make(map[string]any, len(kv)/2)
			for i := 0; i+1 < len(kv); i += 2 {
				if k, ok := kv[i].(string); ok {
					m[k] = kv[i+1]
				}
			}
			return m
		},
		"list":      func(items ...any) []any { return items },
		"sub":       func(a, b int) int { return a - b },
		"ago":       ago,
		"hasPrefix": strings.HasPrefix,
		"jsonTint":  jsonTint,
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

// ago renders a compact relative time ("3m ago", "just now").
func ago(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return strconv.Itoa(int(d.Minutes())) + "m ago"
	case d < 24*time.Hour:
		return strconv.Itoa(int(d.Hours())) + "h ago"
	default:
		return strconv.Itoa(int(d.Hours()/24)) + "d ago"
	}
}
