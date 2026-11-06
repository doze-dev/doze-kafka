package schemaregistry

// Conformance against Confluent's own registry.
//
// Each scenario is a script of requests. TestRecordConformance (run with
// SR_RECORD_URL pointing at a real Confluent Schema Registry) plays every script
// against it and writes what came back to testdata/conformance. TestConformance plays
// the same scripts against this registry and requires the same answers. Nothing here
// is written by hand from memory of what Confluent does: the expected answers are
// Confluent's.
//
// Schema ids are global and keep counting, so a script cannot know them in advance.
// Each run first registers a throwaway schema to learn the next id (the "base") and
// removes it; the script writes @N for "base+N", and recorded answers hold ids as
// offsets from the base.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

type scriptStep struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Body   string `json:"body,omitempty"`
	// ContentType is the header a step sends with its body: empty for the registry's
	// own type, "none" for no header at all.
	ContentType string `json:"contentType,omitempty"`
	// Auth is the user:password a step sends as basic authentication: empty for the
	// scenario's own, "none" for no Authorization header.
	Auth string `json:"auth,omitempty"`
}

type recordedStep struct {
	scriptStep
	Status   int             `json:"status"`
	Response json.RawMessage `json:"response"` // JSON, or a JSON string holding a non-JSON body.
	// Challenge is the WWW-Authenticate header's scheme, lower-cased, when there is one.
	Challenge string `json:"challenge,omitempty"`
}

type scenario struct {
	name  string
	steps []scriptStep
	// cleanup are requests run after the script (not recorded) to leave the registry empty.
	cleanup []scriptStep
	// auth is the user:password every request sends unless a step says otherwise; a
	// scenario with one is recorded against SR_RECORD_AUTH_URL, a registry that
	// requires it, and replayed against one that does.
	auth string
}

func get(path string) scriptStep { return scriptStep{Method: "GET", Path: path} }
func del(path string) scriptStep { return scriptStep{Method: "DELETE", Path: path} }
func post(path, body string) scriptStep {
	return scriptStep{Method: "POST", Path: path, Body: body}
}
func put(path, body string) scriptStep { return scriptStep{Method: "PUT", Path: path, Body: body} }

// withType is a step that sends its body under another Content-Type ("none": no header).
func withType(st scriptStep, ct string) scriptStep { st.ContentType = ct; return st }

// padRef in a body is replaced, when the step is sent, by that many "x": a large body
// without a large script or recording.
var padRef = regexp.MustCompile(`@PAD\((\d+)\)`)

func expandPad(body string) string {
	return padRef.ReplaceAllStringFunc(body, func(m string) string {
		n, _ := strconv.Atoi(padRef.FindStringSubmatch(m)[1])
		return strings.Repeat("x", n)
	})
}

// sendAuth sets the basic authentication a step asks for on req.
func sendAuth(req *http.Request, auth string) {
	if auth == "" || auth == "none" {
		return
	}
	u, p, _ := strings.Cut(auth, ":")
	req.SetBasicAuth(u, p)
}

// challengeOf is the scheme of a WWW-Authenticate header, lower-cased: what a client
// keys on. The realm is the server's own name and is not compared.
func challengeOf(h http.Header) string {
	scheme, _, _ := strings.Cut(h.Get("WWW-Authenticate"), " ")
	return strings.ToLower(scheme)
}

// sendType sets the Content-Type a step asks for on req.
func sendType(req *http.Request, body, ct string) {
	switch {
	case ct == "none":
	case ct != "":
		req.Header.Set("Content-Type", ct)
	case body != "":
		req.Header.Set("Content-Type", contentType)
	}
}

// maxRecordedBody bounds a recorded response: an answer that echoes a large body is
// kept as its status, its error code and its first and last bytes.
const maxRecordedBody = 4096

func schemaBody(schema string) string {
	b, _ := json.Marshal(map[string]string{"schema": schema})
	return string(b)
}

func typedBody(typ, schema string) string {
	b, _ := json.Marshal(map[string]string{"schema": schema, "schemaType": typ})
	return string(b)
}

func refBody(typ, schema string, refs ...Reference) string {
	b, _ := json.Marshal(map[string]any{"schema": schema, "schemaType": typ, "references": refs})
	return string(b)
}

// doer sends a request to some registry and returns the status and body.
type doer func(method, path, body, ct string) (int, string)

// authDoer is a doer that also sends credentials and reports the challenge.
type authDoer func(method, path, body, ct, auth string) (int, string, string)

func (d authDoer) as(auth string) doer {
	return func(method, path, body, ct string) (int, string) {
		st, b, _ := d(method, path, body, ct, auth)
		return st, b
	}
}

func httpDoer(base string) doer { return httpAuthDoer(base).as("") }

func httpAuthDoer(base string) authDoer {
	return func(method, path, body, ct, auth string) (int, string, string) {
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		sendType(req, body, ct)
		sendAuth(req, auth)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, err.Error(), ""
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), challengeOf(resp.Header)
	}
}

func recorderDoer(h http.Handler) doer {
	return recorderAuthDoer(h).as("")
}

func recorderAuthDoer(h http.Handler) authDoer {
	return func(method, path, body, ct, auth string) (int, string, string) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		sendType(req, body, ct)
		sendAuth(req, auth)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String(), challengeOf(rec.Header())
	}
}

var atRef = regexp.MustCompile(`@(\d+)`)

// probe learns the next schema id by registering a throwaway schema, then removes it.
func probe(do doer, n int) int {
	subject := "zz-probe"
	body := schemaBody(fmt.Sprintf(`{"type":"record","name":"Probe%d","fields":[]}`, n))
	status, resp := do("POST", "/subjects/"+subject+"/versions", body, "")
	if status != 200 {
		panic(fmt.Sprintf("probe failed: %d %s", status, resp))
	}
	var out struct{ ID int }
	json.Unmarshal([]byte(resp), &out)
	do("DELETE", "/subjects/"+subject, "", "")
	do("DELETE", "/subjects/"+subject+"?permanent=true", "", "")
	return out.ID
}

// run plays a scenario and returns what came back, with ids made relative to the base.
func run(ad authDoer, sc scenario, probeN int) []recordedStep {
	do := ad.as(sc.auth)
	base := probe(do, probeN)
	sub := func(s string) string {
		return atRef.ReplaceAllStringFunc(s, func(m string) string {
			n, _ := strconv.Atoi(m[1:])
			return strconv.Itoa(base + n)
		})
	}
	var out []recordedStep
	for _, st := range sc.steps {
		path, body := sub(st.Path), expandPad(sub(st.Body))
		auth := st.Auth
		if auth == "" {
			auth = sc.auth
		}
		status, resp, challenge := ad(st.Method, path, body, st.ContentType, auth)
		out = append(out, recordedStep{scriptStep: st, Status: status, Response: relativize(path, clip(resp), base), Challenge: challenge})
	}
	for _, st := range sc.cleanup {
		do(st.Method, sub(st.Path), expandPad(sub(st.Body)), st.ContentType)
	}
	return out
}

// clip keeps a long answer's error code and its ends, so a reply that echoes a
// megabyte body is recorded, and compared, as its shape.
func clip(body string) string {
	if len(body) <= maxRecordedBody {
		return body
	}
	var e struct {
		Code *int `json:"error_code"`
	}
	json.Unmarshal([]byte(body), &e)
	b, _ := json.Marshal(map[string]any{"clipped": len(body), "error_code": e.Code, "head": body[:200], "tail": body[len(body)-200:]})
	return string(b)
}

var (
	idInMessage = regexp.MustCompile(`(Schema )(\d+)( not found)`)
	idField     = regexp.MustCompile(`("(?:id|maxId)":)(\d+)`)
)

// relativize turns a response body into JSON with ids expressed as offsets from base.
func relativize(path, body string, base int) json.RawMessage {
	body = strings.TrimSpace(body)
	if strings.HasSuffix(path, "/referencedby") && strings.HasPrefix(body, "[") {
		var ids []int
		if json.Unmarshal([]byte(body), &ids) == nil {
			for i := range ids {
				ids[i] -= base
			}
			b, _ := json.Marshal(ids)
			body = string(b)
		}
	}
	body = idField.ReplaceAllStringFunc(body, func(m string) string {
		sm := idField.FindStringSubmatch(m)
		n, _ := strconv.Atoi(sm[2])
		return sm[1] + strconv.Itoa(n-base)
	})
	body = idInMessage.ReplaceAllStringFunc(body, func(m string) string {
		sm := idInMessage.FindStringSubmatch(m)
		n, _ := strconv.Atoi(sm[2])
		return sm[1] + "@" + strconv.Itoa(n-base) + sm[3]
	})
	if json.Valid([]byte(body)) {
		return json.RawMessage(body)
	}
	b, _ := json.Marshal(body)
	return b
}

const conformanceDir = "testdata/conformance"

func TestRecordConformance(t *testing.T) {
	url := os.Getenv("SR_RECORD_URL")
	if url == "" {
		t.Skip("set SR_RECORD_URL to a Confluent Schema Registry to record the conformance scripts")
	}
	authURL := os.Getenv("SR_RECORD_AUTH_URL")
	if err := os.MkdirAll(conformanceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	only := map[string]bool{}
	for _, n := range strings.Split(os.Getenv("SR_RECORD_ONLY"), ",") {
		if n != "" {
			only[n] = true
		}
	}
	for i, sc := range scenarios {
		if len(only) > 0 && !only[sc.name] {
			continue
		}
		target := url
		if sc.auth != "" {
			if authURL == "" {
				t.Logf("%s needs SR_RECORD_AUTH_URL, a registry that requires %s: not recorded", sc.name, sc.auth)
				continue
			}
			target = authURL
		}
		got := run(httpAuthDoer(target), sc, 1000+i)
		b, _ := json.MarshalIndent(got, "", "  ")
		if err := os.WriteFile(filepath.Join(conformanceDir, sc.name+".json"), append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// knownDifferences lists the answers where this registry differs from Confluent's on
// purpose, keyed "scenario/step index". Each carries the reason; docs/schema-registry.md
// repeats them.
var knownDifferences = map[string]string{
	"modes/9": "IMPORT mode is not supported, so the list of valid modes does not name it.",

	"readonly-subject/12": "IMPORT mode is not supported: refused as an invalid mode (42204), where Confluent refuses it while subjects exist (42205).",
	"readonly-subject/13": "IMPORT mode is not supported, forced or not.",
	"readonly-subject/14": "READONLY_OVERRIDE mode is not supported.",
	"readonly-subject/15": "Follows from steps 13 and 14: the mode stayed READONLY.",
	"readonly-subject/16": "IMPORT mode is not supported: refused as an invalid mode.",
	"readonly-subject/17": "IMPORT mode is not supported, forced or not.",
	"readonly-subject/18": "Follows from step 17: the global mode stayed READWRITE.",

	"subject-names/3":  "Confluent's HTTP server refuses a path past about 8 KiB (414) before its registry sees it; this one takes a subject up to 16 KiB.",
	"subject-names/7":  "Contexts are not supported: :.ctx:orders is a subject of that name in the one registry, so its id is the registry's next and not the context's first.",
	"subject-names/9":  "Contexts are not supported: :*: is a subject of that name.",
	"subject-names/12": "Contexts are not supported: GET /contexts is 404.",

	"large-bodies/2": "A body past 20 MiB is refused as too large (413); Confluent's JSON parser refuses a string past 20,000,000 characters (400).",
	"large-bodies/3": "A config or mode body is at most 64 KiB (413): Confluent reads 20 MiB of padding and ignores it.",
	"large-bodies/4": "A config or mode body is at most 64 KiB (413).",
	"large-bodies/5": "A config or mode body is at most 64 KiB (413).",

	"unserved-routes-and-parameters/1":  "Contexts are not supported: GET /contexts is 404.",
	"unserved-routes-and-parameters/2":  "The metadata endpoints are not served: there is no Kafka cluster id or registry cluster to name.",
	"unserved-routes-and-parameters/3":  "The metadata endpoints are not served.",
	"unserved-routes-and-parameters/8":  "format=serialized on an id is a 500 in Confluent; here the parameter is ignored and the schema is answered.",
	"unserved-routes-and-parameters/12": "A lookup in a soft-deleted subject is a 500 in Confluent; here it is the subject's 404.",

	"basic-authentication/8": "Without credentials a PUT is 401 with an empty body in Confluent (its HTTP server's answer); here the 401 carries the registry's JSON error, as for every other method.",
	"basic-authentication/9": "Without credentials a DELETE is 401 with an empty body in Confluent; here the JSON error.",

	"unknown-id/1": "GET /schemas/ids/{unknown}/schema is a 500 in Confluent; here it is 404 with 40403.",
}

func TestConformance(t *testing.T) {
	for i, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(conformanceDir, sc.name+".json"))
			if err != nil {
				t.Fatalf("no recording for %s (run TestRecordConformance against Confluent): %v", sc.name, err)
			}
			var want []recordedStep
			if err := json.Unmarshal(raw, &want); err != nil {
				t.Fatal(err)
			}
			reg, err := Open(NewMemoryStorage())
			if err != nil {
				t.Fatal(err)
			}
			opts := Options{AllowedHosts: testHosts}
			if sc.auth != "" {
				wantUser, wantPass, _ := strings.Cut(sc.auth, ":")
				opts.Auth = func(u, p string) bool { return u == wantUser && p == wantPass }
			}
			got := run(recorderAuthDoer(NewAPI(reg, opts)), sc, 1000+i)
			if len(got) != len(want) {
				t.Fatalf("%d steps played, %d recorded", len(got), len(want))
			}
			for n := range want {
				if reason, ok := knownDifferences[fmt.Sprintf("%s/%d", sc.name, n)]; ok {
					t.Logf("step %d differs on purpose: %s", n, reason)
					continue
				}
				if msg := differ(want[n], got[n]); msg != "" {
					t.Errorf("step %d  %s %s %s\n%s", n, want[n].Method, want[n].Path, want[n].Body, msg)
				}
			}
		})
	}
}

// differ compares a recorded answer with ours. Error messages that carry the parser's
// own wording after "details:" are compared only up to that point.
func differ(want, got recordedStep) string {
	if want.Challenge != got.Challenge {
		return fmt.Sprintf("  WWW-Authenticate %q, Confluent sent %q", got.Challenge, want.Challenge)
	}
	if want.Status != got.Status {
		return fmt.Sprintf("  status %d, Confluent answered %d\n  ours:      %s\n  Confluent: %s", got.Status, want.Status, got.Response, want.Response)
	}
	var w, g any
	json.Unmarshal(want.Response, &w)
	json.Unmarshal(got.Response, &g)
	if wm, ok := w.(map[string]any); ok {
		if gm, ok := g.(map[string]any); ok {
			if _, isErr := wm["error_code"]; isErr {
				if wm["error_code"] != gm["error_code"] {
					return fmt.Sprintf("  error_code %v, Confluent answered %v\n  ours:      %s\n  Confluent: %s", gm["error_code"], wm["error_code"], got.Response, want.Response)
				}
				if wm["error_code"] == float64(400) {
					return "" // the wording is the JSON parser's own.
				}
				wmsg, _ := wm["message"].(string)
				gmsg, _ := gm["message"].(string)
				full := wmsg
				if i := strings.Index(wmsg, ", details:"); i >= 0 {
					wmsg = wmsg[:i]
					if j := strings.Index(gmsg, ", details:"); j >= 0 {
						gmsg = gmsg[:j]
					}
				}
				if strings.Contains(full, "Invalid schema") {
					wmsg, gmsg = "Invalid schema", gmsgPrefix(gmsg, "Invalid schema")
				}
				if wmsg != gmsg {
					return fmt.Sprintf("  message differs\n  ours:      %s\n  Confluent: %s", gmsg, wmsg)
				}
				return ""
			}
		}
	}
	// A verbose compatibility check lists why in the checker's own words, then three
	// fixed lines (the version compared, its text, the level); only those are compared.
	if wm, ok := w.(map[string]any); ok {
		if gm, ok := g.(map[string]any); ok {
			wmsgs, _ := wm["messages"].([]any)
			gmsgs, _ := gm["messages"].([]any)
			if len(wmsgs) >= 3 && len(gmsgs) >= 3 {
				if len(gmsgs) < 4 {
					return "  no reason given with the verdict"
				}
				wm["messages"], gm["messages"] = wmsgs[len(wmsgs)-3:], gmsgs[len(gmsgs)-3:]
			}
		}
	}
	if !reflect.DeepEqual(w, g) {
		var a, b bytes.Buffer
		json.Indent(&a, got.Response, "    ", " ")
		json.Indent(&b, want.Response, "    ", " ")
		return "  ours:      " + a.String() + "\n  Confluent: " + b.String()
	}
	return ""
}

func gmsgPrefix(s, p string) string {
	if strings.HasPrefix(s, p) {
		return p
	}
	return s
}
