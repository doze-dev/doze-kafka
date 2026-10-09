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
}

type recordedStep struct {
	scriptStep
	Status   int             `json:"status"`
	Response json.RawMessage `json:"response"` // JSON, or a JSON string holding a non-JSON body.
}

type scenario struct {
	name  string
	steps []scriptStep
	// cleanup are requests run after the script (not recorded) to leave the registry empty.
	cleanup []scriptStep
}

func get(path string) scriptStep { return scriptStep{"GET", path, ""} }
func del(path string) scriptStep { return scriptStep{"DELETE", path, ""} }
func post(path, body string) scriptStep {
	return scriptStep{"POST", path, body}
}
func put(path, body string) scriptStep { return scriptStep{"PUT", path, body} }

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
type doer func(method, path, body string) (int, string)

func httpDoer(base string) doer {
	return func(method, path, body string) (int, string) {
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
}

func recorderDoer(h http.Handler) doer {
	return func(method, path, body string) (int, string) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", contentType)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
}

var atRef = regexp.MustCompile(`@(\d+)`)

// probe learns the next schema id by registering a throwaway schema, then removes it.
func probe(do doer, n int) int {
	subject := "zz-probe"
	body := schemaBody(fmt.Sprintf(`{"type":"record","name":"Probe%d","fields":[]}`, n))
	status, resp := do("POST", "/subjects/"+subject+"/versions", body)
	if status != 200 {
		panic(fmt.Sprintf("probe failed: %d %s", status, resp))
	}
	var out struct{ ID int }
	json.Unmarshal([]byte(resp), &out)
	do("DELETE", "/subjects/"+subject, "")
	do("DELETE", "/subjects/"+subject+"?permanent=true", "")
	return out.ID
}

// run plays a scenario and returns what came back, with ids made relative to the base.
func run(do doer, sc scenario, probeN int) []recordedStep {
	base := probe(do, probeN)
	sub := func(s string) string {
		return atRef.ReplaceAllStringFunc(s, func(m string) string {
			n, _ := strconv.Atoi(m[1:])
			return strconv.Itoa(base + n)
		})
	}
	var out []recordedStep
	for _, st := range sc.steps {
		path, body := sub(st.Path), sub(st.Body)
		status, resp := do(st.Method, path, body)
		out = append(out, recordedStep{scriptStep: st, Status: status, Response: relativize(path, resp, base)})
	}
	for _, st := range sc.cleanup {
		do(st.Method, sub(st.Path), sub(st.Body))
	}
	return out
}

var (
	idInMessage = regexp.MustCompile(`(Schema )(\d+)( not found)`)
	idField     = regexp.MustCompile(`("id":)(\d+)`)
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
	do := httpDoer(url)
	if err := os.MkdirAll(conformanceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i, sc := range scenarios {
		got := run(do, sc, 1000+i)
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
			reg, err := Open(newMemStore())
			if err != nil {
				t.Fatal(err)
			}
			got := run(recorderDoer(NewAPI(reg, Options{})), sc, 1000+i)
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
				if i := strings.Index(wmsg, ", details:"); i >= 0 {
					wmsg = wmsg[:i]
					if j := strings.Index(gmsg, ", details:"); j >= 0 {
						gmsg = gmsg[:j]
					}
				}
				if strings.Contains(wm["message"].(string), "Invalid schema") {
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
