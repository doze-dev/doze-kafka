package schemaregistry

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/hostguard"
)

// contentType is what Confluent's registry answers with, and what its clients send.
const contentType = "application/vnd.schemaregistry.v1+json"

// Options configures the HTTP API.
type Options struct {
	// Auth, when set, is asked to approve the HTTP basic credentials of every request
	// (the same users as the broker's SASL). Nil means no authentication.
	Auth func(user, pass string) bool
	// AllowedHosts are the host names, beyond the always-allowed set (IP literals,
	// localhost, *.doze), that a request's Host header may name: the host clients
	// are told to dial, and whatever the operator listed. "*" allows every name.
	// A request for any other host is refused before anything else is looked at
	// (see ServeHTTP): that is the defence against DNS rebinding.
	AllowedHosts []string
	Logger       *slog.Logger
}

// API is the registry's REST interface.
type API struct {
	reg  *Registry
	opts Options
	mux  *http.ServeMux
	log  *slog.Logger

	failMu sync.Mutex
	fails  map[string]int      // failed sign-ins in a row, by client address
	sleep  func(time.Duration) // time.Sleep; a test's own
}

// NewAPI builds the HTTP handler over a registry.
func NewAPI(reg *Registry, opts Options) *API {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	a := &API{reg: reg, opts: opts, mux: http.NewServeMux(), log: log, fails: map[string]int{}, sleep: time.Sleep}
	a.routes()
	return a
}

// Failed sign-ins from one address are answered at once up to freeFailures in a row;
// after that each waits failureStep longer than the last, up to maxFailureWait, so
// passwords cannot be tried at the speed of HTTP. A success clears the count.
const (
	freeFailures   = 5
	failureStep    = 100 * time.Millisecond
	maxFailureWait = 2 * time.Second
	maxFailAddrs   = 4096 // addresses remembered; past this the counts start again
)

// failedSignIn counts a failure from addr and waits as long as its count says.
func (a *API) failedSignIn(addr string) int {
	a.failMu.Lock()
	if len(a.fails) >= maxFailAddrs {
		clear(a.fails)
	}
	a.fails[addr]++
	n := a.fails[addr]
	a.failMu.Unlock()
	if n > freeFailures {
		a.sleep(min(time.Duration(n-freeFailures)*failureStep, maxFailureWait))
	}
	return n
}

func (a *API) signedIn(addr string) {
	a.failMu.Lock()
	delete(a.fails, addr)
	a.failMu.Unlock()
}

// clientAddr is the address a request came from, without its port.
func clientAddr(r *http.Request) string {
	if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return h
	}
	return r.RemoteAddr
}

// statusWriter remembers the status a handler wrote.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Registry is the store behind the API.
func (a *API) Registry() *Registry { return a.reg }

// ServeHTTP answers one request. The Host header is judged first, before the
// credentials and before the route: a request that names a host this registry is
// not served under came through a name somebody else controls (DNS rebinding from a
// web page: internal/hostguard), and it learns nothing, not even whether the
// registry wants a password. The 403 carries the registry's error shape with the
// HTTP status as its error_code, as the 401, 404 and 405 do.
//
//fuzz:parser class=registry-rest FuzzAPIBody
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	sw := &statusWriter{ResponseWriter: w}
	a.serve(sw, r)
	a.logRequest(r, sw.status, time.Since(start))
}

// logRequest writes one record per request: a server failure as a warning, a refused
// sign-in, host or incompatible schema at info, anything else at debug. The user is
// the one the request named; a password is never logged.
func (a *API) logRequest(r *http.Request, status int, took time.Duration) {
	level := slog.LevelDebug
	switch {
	case status >= 500:
		level = slog.LevelWarn
	case status == http.StatusUnauthorized, status == http.StatusForbidden, status == http.StatusConflict:
		level = slog.LevelInfo
	}
	if !a.log.Enabled(r.Context(), level) {
		return
	}
	user, _, _ := r.BasicAuth()
	a.log.LogAttrs(r.Context(), level, "schema registry request",
		slog.String("method", r.Method), slog.String("path", r.URL.Path), slog.Int("status", status),
		slog.String("user", user), slog.String("from", clientAddr(r)), slog.Duration("took", took))
}

func (a *API) serve(w http.ResponseWriter, r *http.Request) {
	if !hostguard.Allowed(r.Host, a.opts.AllowedHosts) {
		writeErr(w, newErr(403, codeForbidden, "This schema registry does not answer to the host name %s. If that is a name you chose, start doze-kafka with --console-allow-host=%s (the registry answers to the same names as the console, and to --advertised-host).",
			hostguard.Quote(r.Host), hostguard.Name(r.Host)))
		return
	}
	if a.opts.Auth != nil {
		u, p, ok := r.BasicAuth()
		if !ok || !a.opts.Auth(u, p) {
			a.failedSignIn(clientAddr(r))
			w.Header().Set("WWW-Authenticate", `Basic realm="doze-kafka schema registry"`)
			writeErr(w, newErr(401, codeUnauthorized, "Unauthorized"))
			return
		}
		a.signedIn(clientAddr(r))
	}
	if (r.Method == http.MethodPost || r.Method == http.MethodPut) && !acceptedBodyType(r.Header.Get("Content-Type")) {
		writeErr(w, newErr(http.StatusUnsupportedMediaType, codeUnsupportedMediaType, "HTTP 415 Unsupported Media Type"))
		return
	}
	limit := int64(maxSchemaBody)
	if strings.HasPrefix(r.URL.Path, "/config") || strings.HasPrefix(r.URL.Path, "/mode") {
		limit = maxSettingBody
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	a.mux.ServeHTTP(&statusFixer{ResponseWriter: w}, r)
}

// acceptedBodyType tells whether a POST or PUT body of this Content-Type is read, as
// Confluent's registry decides (recorded: content-types): no type, JSON, the
// registry's own types and application/octet-stream are; text/plain, XML, form and
// multipart bodies get 415. The three a web page may send to another site without
// asking first (text/plain, form-urlencoded, multipart) are among those refused, so a
// page cannot register schemas in a developer's registry.
func acceptedBodyType(ct string) bool {
	if strings.TrimSpace(ct) == "" {
		return true
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	switch mt {
	case "application/json", "application/octet-stream",
		"application/vnd.schemaregistry+json", "application/vnd.schemaregistry.v1+json":
		return true
	}
	return false
}

// Request bodies are read up to a limit. A schema with its references may be as large
// as Confluent's registry takes (recorded: large-bodies, a 9 MiB schema registered;
// its JSON parser refuses a string past 20,000,000 characters). A level or a mode is
// a few bytes, where Confluent reads 20 MiB of padding and ignores it: here that is
// refused (413), so nobody's memory goes to a body nothing reads.
const (
	maxSchemaBody  = 20 << 20
	maxSettingBody = 64 << 10
)

// tooLarge is the answer to a body past its limit, or nil for any other read error.
func tooLarge(err error) *Error {
	var mb *http.MaxBytesError
	if errors.As(err, &mb) {
		return newErr(http.StatusRequestEntityTooLarge, http.StatusRequestEntityTooLarge, "The request body is larger than %d bytes", mb.Limit)
	}
	return nil
}

// decodeSetting reads the small JSON body of a config or mode change.
//
//fuzz:parser class=registry-rest FuzzAPIBody
func decodeSetting(r *http.Request, v any) *Error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		if e := tooLarge(err); e != nil {
			return e
		}
		return newErr(422, codeInvalidRequest, "Unrecognized token in the request body: %v", err)
	}
	return nil
}

// statusFixer turns the mux's own plain-text 404 and 405 into the registry's JSON.
type statusFixer struct {
	http.ResponseWriter
	wrote bool
}

func (s *statusFixer) WriteHeader(code int) {
	if !s.wrote && (code == 404 || code == 405) && s.Header().Get("Content-Type") != contentType {
		s.wrote = true
		s.Header().Set("Content-Type", contentType)
		s.ResponseWriter.WriteHeader(code)
		msg := "HTTP 404 Not Found"
		if code == 405 {
			msg = "HTTP 405 Method Not Allowed"
		}
		json.NewEncoder(s.ResponseWriter).Encode(Error{Code: code, Message: msg})
		return
	}
	s.wrote = true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusFixer) Write(b []byte) (int, error) {
	if s.wrote && s.Header().Get("Content-Type") == contentType {
		// Body of a rewritten 404/405 was already written; swallow the mux's text.
		if strings.HasPrefix(string(b), "404 page not found") || strings.HasPrefix(string(b), "Method Not Allowed") {
			return len(b), nil
		}
	}
	s.wrote = true
	return s.ResponseWriter.Write(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, e *Error) {
	writeJSON(w, e.Status, e)
}

// page applies the offset and limit parameters to a list, as Confluent does for
// GET /schemas (recorded: limit-and-offset; it ignores them on /subjects and on a
// subject's versions, and so does this registry). A missing, negative or unreadable
// offset is 0; a limit that is not positive is no limit.
func page[T any](all []T, r *http.Request) []T {
	q := r.URL.Query()
	if off, err := strconv.Atoi(q.Get("offset")); err == nil && off > 0 {
		all = all[min(off, len(all)):]
	}
	if lim, err := strconv.Atoi(q.Get("limit")); err == nil && lim > 0 && lim < len(all) {
		all = all[:lim]
	}
	return all
}

func boolParam(r *http.Request, name string) bool {
	b, _ := strconv.ParseBool(r.URL.Query().Get(name))
	return b
}

func prefixParam(r *http.Request) string {
	p := r.URL.Query().Get("subjectPrefix")
	if p == ":*:" {
		return ""
	}
	return p
}

//fuzz:parser class=registry-rest FuzzAPIBody
func readInput(r *http.Request) (SchemaInput, *Error) {
	var in SchemaInput
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if e := tooLarge(err); e != nil {
			return in, e
		}
		return in, newErr(422, codeInvalidRequest, "Could not read the request: %v", err)
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return in, newErr(400, 400, "Unrecognized token in the request body: %v", err)
	}
	return in, nil
}

func (a *API) routes() {
	m := a.mux
	// handle wraps a handler that returns an error as the registry's JSON error.
	handle := func(pattern string, h func(w http.ResponseWriter, r *http.Request) *Error) {
		m.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			if e := h(w, r); e != nil {
				writeErr(w, e)
			}
		})
	}

	handle("GET /{$}", func(w http.ResponseWriter, r *http.Request) *Error {
		writeJSON(w, 200, map[string]any{})
		return nil
	})

	handle("GET /subjects", func(w http.ResponseWriter, r *http.Request) *Error {
		writeJSON(w, 200, a.reg.Subjects(prefixParam(r), boolParam(r, "deleted"), boolParam(r, "deletedOnly")))
		return nil
	})
	handle("GET /subjects/{subject}/versions", func(w http.ResponseWriter, r *http.Request) *Error {
		v, err := a.reg.Versions(r.PathValue("subject"), boolParam(r, "deleted"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	handle("POST /subjects/{subject}/versions", func(w http.ResponseWriter, r *http.Request) *Error {
		in, e := readInput(r)
		if e != nil {
			return e
		}
		res, e := a.reg.Register(r.PathValue("subject"), in, boolParam(r, "normalize"))
		if e != nil {
			return e
		}
		if res.Latest != nil {
			latest := *res.Latest
			latest.Subject = ""
			writeJSON(w, 200, latest)
			return nil
		}
		writeJSON(w, 200, map[string]int{"id": res.ID})
		return nil
	})
	handle("POST /subjects/{subject}", func(w http.ResponseWriter, r *http.Request) *Error {
		in, e := readInput(r)
		if e != nil {
			return e
		}
		v, e := a.reg.Lookup(r.PathValue("subject"), in, boolParam(r, "normalize"), boolParam(r, "deleted"))
		if e != nil {
			return e
		}
		writeJSON(w, 200, v)
		return nil
	})
	handle("DELETE /subjects/{subject}", func(w http.ResponseWriter, r *http.Request) *Error {
		v, e := a.reg.DeleteSubject(r.PathValue("subject"), boolParam(r, "permanent"))
		if e != nil {
			return e
		}
		writeJSON(w, 200, v)
		return nil
	})
	handle("GET /subjects/{subject}/versions/{version}", func(w http.ResponseWriter, r *http.Request) *Error {
		v, e := a.reg.Version(r.PathValue("subject"), r.PathValue("version"), boolParam(r, "deleted"))
		if e != nil {
			return e
		}
		writeJSON(w, 200, v)
		return nil
	})
	handle("GET /subjects/{subject}/versions/{version}/schema", func(w http.ResponseWriter, r *http.Request) *Error {
		v, e := a.reg.Version(r.PathValue("subject"), r.PathValue("version"), boolParam(r, "deleted"))
		if e != nil {
			return e
		}
		w.Header().Set("Content-Type", contentType)
		io.WriteString(w, v.Schema)
		return nil
	})
	handle("GET /subjects/{subject}/versions/{version}/referencedby", func(w http.ResponseWriter, r *http.Request) *Error {
		ids, e := a.reg.ReferencedBy(r.PathValue("subject"), r.PathValue("version"))
		if e != nil {
			return e
		}
		writeJSON(w, 200, ids)
		return nil
	})
	handle("DELETE /subjects/{subject}/versions/{version}", func(w http.ResponseWriter, r *http.Request) *Error {
		v, e := a.reg.DeleteVersion(r.PathValue("subject"), r.PathValue("version"), boolParam(r, "permanent"))
		if e != nil {
			return e
		}
		writeJSON(w, 200, v)
		return nil
	})

	handle("GET /schemas", func(w http.ResponseWriter, r *http.Request) *Error {
		all := a.reg.AllSchemas(prefixParam(r), boolParam(r, "latestOnly"), boolParam(r, "deleted"))
		writeJSON(w, 200, page(all, r))
		return nil
	})
	handle("GET /schemas/types", func(w http.ResponseWriter, r *http.Request) *Error {
		writeJSON(w, 200, []string{TypeJSON, TypeProtobuf, TypeAvro})
		return nil
	})
	schemaID := func(r *http.Request) (int, *Error) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			return 0, newErr(404, codeNotFound, "HTTP 404 Not Found")
		}
		return id, nil
	}
	handle("GET /schemas/ids/{id}", func(w http.ResponseWriter, r *http.Request) *Error {
		id, e := schemaID(r)
		if e != nil {
			return e
		}
		v, e := a.reg.SchemaByID(id)
		if e != nil {
			return e
		}
		if boolParam(r, "fetchMaxId") {
			writeJSON(w, 200, struct {
				SchemaView
				MaxID int `json:"maxId"`
			}{v, a.reg.MaxID()})
			return nil
		}
		writeJSON(w, 200, v)
		return nil
	})
	handle("GET /schemas/ids/{id}/schema", func(w http.ResponseWriter, r *http.Request) *Error {
		id, e := schemaID(r)
		if e != nil {
			return e
		}
		v, e := a.reg.SchemaByID(id)
		if e != nil {
			return e
		}
		w.Header().Set("Content-Type", contentType)
		io.WriteString(w, v.Schema)
		return nil
	})
	handle("GET /schemas/ids/{id}/subjects", func(w http.ResponseWriter, r *http.Request) *Error {
		id, e := schemaID(r)
		if e != nil {
			return e
		}
		vs, e := a.reg.IDSubjects(id, boolParam(r, "deleted"))
		if e != nil {
			return e
		}
		seen := map[string]bool{}
		names := []string{}
		for _, v := range vs {
			if !seen[v.Subject] {
				seen[v.Subject] = true
				names = append(names, v.Subject)
			}
		}
		writeJSON(w, 200, names)
		return nil
	})
	handle("GET /schemas/ids/{id}/versions", func(w http.ResponseWriter, r *http.Request) *Error {
		id, e := schemaID(r)
		if e != nil {
			return e
		}
		vs, e := a.reg.IDSubjects(id, boolParam(r, "deleted"))
		if e != nil {
			return e
		}
		out := []map[string]any{}
		for _, v := range vs {
			out = append(out, map[string]any{"subject": v.Subject, "version": v.Version})
		}
		writeJSON(w, 200, out)
		return nil
	})

	compat := func(w http.ResponseWriter, r *http.Request) *Error {
		in, e := readInput(r)
		if e != nil {
			return e
		}
		found, e := a.reg.CheckCompatible(r.PathValue("subject"), r.PathValue("version"), in, boolParam(r, "normalize"))
		if e != nil {
			return e
		}
		out := map[string]any{"is_compatible": len(found) == 0}
		if boolParam(r, "verbose") {
			msgs := []string{} // an empty list when it fits, as Confluent sends it.
			for _, f := range found {
				msgs = append(msgs, f.Messages()...)
			}
			out["messages"] = msgs
		}
		writeJSON(w, 200, out)
		return nil
	}
	handle("POST /compatibility/subjects/{subject}/versions/{version}", compat)
	handle("POST /compatibility/subjects/{subject}/versions", compat)

	handle("GET /config", func(w http.ResponseWriter, r *http.Request) *Error {
		l, e := a.reg.Compat("", true)
		if e != nil {
			return e
		}
		writeJSON(w, 200, map[string]string{"compatibilityLevel": l})
		return nil
	})
	handle("GET /config/{subject}", func(w http.ResponseWriter, r *http.Request) *Error {
		l, e := a.reg.Compat(r.PathValue("subject"), boolParam(r, "defaultToGlobal"))
		if e != nil {
			return e
		}
		writeJSON(w, 200, map[string]string{"compatibilityLevel": l})
		return nil
	})
	putConfig := func(subject func(*http.Request) string) func(http.ResponseWriter, *http.Request) *Error {
		return func(w http.ResponseWriter, r *http.Request) *Error {
			var body struct {
				Compatibility string `json:"compatibility"`
			}
			if e := decodeSetting(r, &body); e != nil {
				return e
			}
			if body.Compatibility == "" {
				writeJSON(w, 200, map[string]string{}) // as Confluent: nothing to set, nothing changed.
				return nil
			}
			if e := a.reg.SetCompat(subject(r), body.Compatibility); e != nil {
				return e
			}
			writeJSON(w, 200, map[string]string{"compatibility": body.Compatibility})
			return nil
		}
	}
	handle("PUT /config", putConfig(func(*http.Request) string { return "" }))
	handle("PUT /config/{subject}", putConfig(func(r *http.Request) string { return r.PathValue("subject") }))
	delConfig := func(subject func(*http.Request) string) func(http.ResponseWriter, *http.Request) *Error {
		return func(w http.ResponseWriter, r *http.Request) *Error {
			l, e := a.reg.DeleteCompat(subject(r))
			if e != nil {
				return e
			}
			writeJSON(w, 200, map[string]string{"compatibilityLevel": l})
			return nil
		}
	}
	handle("DELETE /config", delConfig(func(*http.Request) string { return "" }))
	handle("DELETE /config/{subject}", delConfig(func(r *http.Request) string { return r.PathValue("subject") }))

	handle("GET /mode", func(w http.ResponseWriter, r *http.Request) *Error {
		mode, e := a.reg.Mode("", true)
		if e != nil {
			return e
		}
		writeJSON(w, 200, map[string]string{"mode": mode})
		return nil
	})
	handle("GET /mode/{subject}", func(w http.ResponseWriter, r *http.Request) *Error {
		mode, e := a.reg.Mode(r.PathValue("subject"), boolParam(r, "defaultToGlobal"))
		if e != nil {
			return e
		}
		writeJSON(w, 200, map[string]string{"mode": mode})
		return nil
	})
	putMode := func(subject func(*http.Request) string) func(http.ResponseWriter, *http.Request) *Error {
		return func(w http.ResponseWriter, r *http.Request) *Error {
			var body struct {
				Mode string `json:"mode"`
			}
			if e := decodeSetting(r, &body); e != nil {
				return e
			}
			if e := a.reg.SetMode(subject(r), body.Mode); e != nil {
				return e
			}
			writeJSON(w, 200, map[string]string{"mode": strings.ToUpper(body.Mode)})
			return nil
		}
	}
	handle("PUT /mode", putMode(func(*http.Request) string { return "" }))
	handle("PUT /mode/{subject}", putMode(func(r *http.Request) string { return r.PathValue("subject") }))
	handle("DELETE /mode/{subject}", func(w http.ResponseWriter, r *http.Request) *Error {
		prev, e := a.reg.DeleteMode(r.PathValue("subject"))
		if e != nil {
			return e
		}
		writeJSON(w, 200, map[string]string{"mode": prev})
		return nil
	})
}
