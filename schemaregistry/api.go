package schemaregistry

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

// contentType is what Confluent's registry answers with, and what its clients send.
const contentType = "application/vnd.schemaregistry.v1+json"

// Options configures the HTTP API.
type Options struct {
	// Auth, when set, is asked to approve the HTTP basic credentials of every request
	// (the same users as the broker's SASL). Nil means no authentication.
	Auth   func(user, pass string) bool
	Logger *slog.Logger
}

// API is the registry's REST interface.
type API struct {
	reg  *Registry
	opts Options
	mux  *http.ServeMux
}

// NewAPI builds the HTTP handler over a registry.
func NewAPI(reg *Registry, opts Options) *API {
	a := &API{reg: reg, opts: opts, mux: http.NewServeMux()}
	a.routes()
	return a
}

// Registry is the store behind the API.
func (a *API) Registry() *Registry { return a.reg }

//fuzz:parser class=registry-rest FuzzAPIBody
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a.opts.Auth != nil {
		u, p, ok := r.BasicAuth()
		if !ok || !a.opts.Auth(u, p) {
			w.Header().Set("WWW-Authenticate", `Basic realm="doze-kafka schema registry"`)
			writeErr(w, newErr(401, codeUnauthorized, "Unauthorized"))
			return
		}
	}
	a.mux.ServeHTTP(&statusFixer{ResponseWriter: w}, r)
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
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
		writeJSON(w, 200, a.reg.AllSchemas(prefixParam(r), boolParam(r, "latestOnly"), boolParam(r, "deleted")))
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
		if boolParam(r, "verbose") && len(found) > 0 {
			var msgs []string
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
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				return newErr(422, codeInvalidRequest, "Unrecognized token in the request body: %v", err)
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
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				return newErr(422, codeInvalidRequest, "Unrecognized token in the request body: %v", err)
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
