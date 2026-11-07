package schemaregistry

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

// The small things the rest leaves unasked.
func TestAccessorsSayWhatTheyHold(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts})
	if api.Registry() != r {
		t.Error("API.Registry is not the registry it was built over")
	}
	if got := (&Error{Code: 409, Message: "no"}).Error(); got != "409: no" {
		t.Errorf("Error.Error = %q", got)
	}
	for _, typ := range []string{TypeAvro, TypeJSON, TypeProtobuf} {
		schema := map[string]string{TypeAvro: `"string"`, TypeJSON: `{"type":"string"}`, TypeProtobuf: `syntax = "proto3"; message M { string a = 1; }`}[typ]
		c, err := NewCodec(typ, schema, nil)
		if err != nil || c.Type() != typ {
			t.Errorf("a %s codec: %v, type %q", typ, err, c.Type())
		}
	}
	if tooLarge(errors.New("connection reset")) != nil {
		t.Error("an error that is not a size limit was answered as 413")
	}
	if got := r.levelFor("x", "full"); got != CompatFull {
		t.Errorf("a level asked for: %q", got)
	}
}

// A proto2 extension is printed as it was declared.
func TestProtoExtendIsPrinted(t *testing.T) {
	r, _ := openReg(t)
	id := mustRegister(t, r, "ext", SchemaInput{SchemaType: TypeProtobuf,
		Schema: `syntax = "proto2"; package p; message M { extensions 100 to 199; } extend M { optional string note = 100; }`})
	got := r.schemas[id].Schema
	if !strings.Contains(got, "extend M {") || !strings.Contains(got, "optional string note = 100;") {
		t.Errorf("the stored schema:\n%s", got)
	}
}

// changesOf is every change the JSON Schema diff names going from old to new,
// compatible ones included.
func changesOf(t *testing.T, old, new string) []string {
	t.Helper()
	o, err := parseSchema(TypeJSON, old, nil)
	if err != nil {
		t.Fatal(err)
	}
	n, err := parseSchema(TypeJSON, new, nil)
	if err != nil {
		t.Fatal(err)
	}
	var d jdiff
	d.compare(o.(*jsonSchema).node, n.(*jsonSchema).node, "#")
	var out []string
	for _, c := range d.changes {
		out = append(out, c.typ)
	}
	return out
}

func TestJSONAdditionalItemsAndDependencies(t *testing.T) {
	tuple := func(additional string) string {
		return `{"type":"array","items":[{"type":"string"}],"additionalItems":` + additional + `}`
	}
	for _, c := range []struct{ old, new, want string }{
		{tuple("false"), tuple(`{"type":"string"}`), "ADDITIONAL_ITEMS_ADDED"},
		{tuple(`{"type":"string"}`), tuple("false"), "ADDITIONAL_ITEMS_REMOVED"},
		{tuple(`{"type":"integer"}`), tuple(`{"type":"number"}`), "ADDITIONAL_ITEMS_EXTENDED"},
		{tuple(`{"type":"number"}`), tuple(`{"type":"integer"}`), "ADDITIONAL_ITEMS_NARROWED"},
		{`{"dependencies":{"a":["b"]}}`, `{"dependencies":{"a":["b","c"]}}`, "DEPENDENCY_ARRAY_EXTENDED"},
		{`{"dependencies":{"a":["b","c"]}}`, `{"dependencies":{"a":["b"]}}`, "DEPENDENCY_ARRAY_NARROWED"},
		{`{"dependencies":{"a":["b"]}}`, `{"dependencies":{"a":["c"]}}`, "DEPENDENCY_ARRAY_EXTENDED"},
		{`{"dependencies":{}}`, `{"dependencies":{"a":["b"]}}`, "DEPENDENCY_ARRAY_ADDED"},
		{`{"dependencies":{"a":["b"]}}`, `{"dependencies":{}}`, "DEPENDENCY_ARRAY_REMOVED"},
	} {
		got := changesOf(t, c.old, c.new)
		if len(got) == 0 || got[len(got)-1] != c.want {
			t.Errorf("%s -> %s: %v, want %s", c.old, c.new, got, c.want)
		}
	}
	if got := changesOf(t, `{"dependencies":{"a":["b","c"]}}`, `{"dependencies":{"a":["b","c"]}}`); len(got) != 0 {
		t.Errorf("the same dependencies: %v", got)
	}
}

// A User-Agent past 256 bytes is kept cut short on a character boundary.
func TestOriginKeepsAShortAgent(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("User-Agent", strings.Repeat("a", 255)+"é"+strings.Repeat("b", 100))
	req.RemoteAddr = "not-an-address"
	o := originOf(req)
	if len(o.Agent) > maxAgentLen || !utf8.ValidString(o.Agent) || o.From != "not-an-address" {
		t.Errorf("origin: %d bytes, valid %v, from %q", len(o.Agent), utf8.ValidString(o.Agent), o.From)
	}
}

// The extension routes over HTTP, answers and refusals.
func TestExtensionRoutesOverHTTP(t *testing.T) {
	r, _ := openReg(t)
	mustRegister(t, r, "x", avroIn(avroUser1))
	api := NewAPI(r, Options{AllowedHosts: testHosts})
	do := func(method, path, body string) (int, map[string]any) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", contentType)
		}
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		var m map[string]any
		json.Unmarshal(rec.Body.Bytes(), &m)
		return rec.Code, m
	}
	if code, m := do("GET", "/_doze/overview", ""); code != 200 || m["nextId"] != float64(2) {
		t.Errorf("overview: %d %v", code, m)
	}
	for _, path := range []string{"/_doze/subjects/nope/versions/1", "/_doze/subjects/nope/versions/1/resolved"} {
		if code, m := do("GET", path, ""); code != http.StatusNotFound || m["error_code"] != float64(codeSubjectNotFound) {
			t.Errorf("GET %s: %d %v", path, code, m)
		}
	}
	if code, m := do("POST", "/_doze/subjects/x/check?level=SIDEWAYS", schemaBody(avroUser2)); code != 422 || m["error_code"] != float64(codeInvalidCompatibility) {
		t.Errorf("a check at a made-up level: %d %v", code, m)
	}
	if code, _ := do("POST", "/_doze/subjects/x/check", "{not json"); code != 400 {
		t.Errorf("a check with a broken body: %d", code)
	}
	if code, m := do("POST", "/_doze/subjects/x/check?version=7", schemaBody(avroUser2)); code != 404 || m["error_code"] != float64(codeVersionNotFound) {
		t.Errorf("a check against a missing version: %d %v", code, m)
	}
	if code, m := do("POST", "/_doze/subjects/x/check", schemaBody(avroUser2)); code != 200 || m["compatible"] != true || m["level"] != CompatBackward {
		t.Errorf("a check at the subject's own level: %d %v", code, m)
	}
	if code, m := do("GET", "/_doze/requests?subject=x&limit=1", ""); code != 200 || m["max"] != float64(maxRequests) {
		t.Errorf("requests: %d %v", code, m)
	}
	if code, _ := do("GET", "/subjects?subjectPrefix=:*:", ""); code != 200 {
		t.Errorf("the all-subjects prefix: %d", code)
	}
}

// Named types of every kind a reference defines are written out where first used:
// an enum, a fixed, and a record inside an array and a map.
func TestInlineAvroEveryNamedKind(t *testing.T) {
	refs := []resolvedRef{{Reference: Reference{Name: "lib"}, Text: `{"type":"record","name":"Lib","namespace":"l","fields":[
		{"name":"c","type":{"type":"enum","name":"Colour","symbols":["RED"]}},
		{"name":"h","type":{"type":"fixed","name":"Hash","size":4}},
		{"name":"xs","type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"n","type":"int"}]}}},
		{"name":"m","type":{"type":"map","values":{"type":"record","name":"Val","fields":[{"name":"v","type":"long"}]}}},
		{"name":"u","type":["null",{"type":"record","name":"InUnion","fields":[]}]}]}`}}
	main := `{"type":"record","name":"Main","fields":[{"name":"a","type":"l.Colour"},{"name":"b","type":"l.Hash"},{"name":"c","type":{"type":"array","items":"l.Item"}},{"name":"d","type":{"type":"map","values":"l.Val"}},{"name":"e","type":"l.InUnion"},{"name":"f","type":"l.Colour"}]}`
	got, err := avroInline(main, refs)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"name":"l.Colour"`, `"name":"l.Hash"`, `"name":"l.Item"`, `"name":"l.Val"`, `"name":"l.InUnion"`, `{"name":"f","type":"l.Colour"}`} {
		if !strings.Contains(got, want) {
			t.Errorf("the inlined schema lacks %s:\n%s", want, got)
		}
	}
	if _, err := parseSchema(TypeAvro, got, nil); err != nil {
		t.Errorf("the inlined schema does not parse alone: %v", err)
	}
	if _, err := avroInline(main, []resolvedRef{{Reference: Reference{Name: "bad"}, Text: "{"}}); err == nil {
		t.Error("a reference that is not JSON was inlined")
	}
}

// References in a refusal are written as Confluent writes them, one after another.
func TestRefsStringIsConfluents(t *testing.T) {
	if got := refsString(nil); got != "[]" {
		t.Errorf("no references: %q", got)
	}
	got := refsString([]Reference{{Name: "a.json", Subject: "a", Version: 1}, {Name: "b.json", Subject: "b", Version: 2}})
	want := "[io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference@0{name=a.json, subject=a, version=1}, " +
		"io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference@0{name=b.json, subject=b, version=2}]"
	if got != want {
		t.Errorf("two references:\n got %s\nwant %s", got, want)
	}
}
