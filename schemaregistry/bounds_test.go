package schemaregistry

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// slack widens a wall-clock bound under -race, which slows this code about tenfold.
func slack(d time.Duration) time.Duration {
	if raceOn {
		return 10 * d
	}
	return d
}

// A schema that names the same subject twice at every level is a diamond, not a
// tree: each (subject, version) is resolved once, so a deep diamond costs what its
// levels cost and not two to the power of its depth. The registry's lock is held
// while it resolves, so a slow resolve stalls every other client. (SCH-1)
func TestReferenceGraphIsLinear(t *testing.T) {
	r, _ := openReg(t)
	const levels = 26
	mustRegister(t, r, "l0", SchemaInput{SchemaType: TypeJSON, Schema: `{"type":"string"}`})
	var took time.Duration
	for i := 1; i <= levels; i++ {
		in := SchemaInput{SchemaType: TypeJSON,
			Schema: fmt.Sprintf(`{"type":"object","properties":{"a":{"$ref":"a%d.json"},"b":{"$ref":"b%d.json"}}}`, i, i),
			References: []Reference{
				{Name: fmt.Sprintf("a%d.json", i), Subject: fmt.Sprintf("l%d", i-1), Version: 1},
				{Name: fmt.Sprintf("b%d.json", i), Subject: fmt.Sprintf("l%d", i-1), Version: 1},
			}}
		start := time.Now()
		mustRegister(t, r, fmt.Sprintf("l%d", i), in)
		took = time.Since(start)
	}
	if bound := slack(200 * time.Millisecond); took > bound {
		t.Errorf("registering level %d of a diamond took %s, want under %s", levels, took, bound)
	}
}

// nestedAvro is a record whose one field is an array nested depth times around leaf.
func nestedAvro(depth int, leaf string) string {
	typ := `"` + leaf + `"`
	for range depth {
		typ = `{"type":"array","items":` + typ + `}`
	}
	return `{"type":"record","name":"Deep","fields":[{"name":"f","type":` + typ + `}]}`
}

// A deeply nested schema is either refused as invalid or answered quickly: comparing
// it with the subject's previous version must not hold the lock for seconds. (SCH-2)
func TestNestingIsBounded(t *testing.T) {
	r, _ := openReg(t)
	if _, e := r.Register("deep", avroIn(nestedAvro(3000, "int")), false); e != nil && e.Code != codeInvalidSchema {
		t.Fatalf("a 3,000-deep schema: %v, want accepted or %d", e, codeInvalidSchema)
	}
	start := time.Now()
	_, e := r.Register("deep", avroIn(nestedAvro(3000, "string")), false)
	took := time.Since(start)
	if e != nil && e.Code != codeInvalidSchema && e.Code != codeIncompatible {
		t.Errorf("the second 3,000-deep schema: %v, want refused as invalid or incompatible", e)
	}
	if bound := slack(time.Second); took > bound {
		t.Errorf("the second 3,000-deep schema was answered in %s, want under %s", took, bound)
	}

	deepJSON := `{"type":"integer"}`
	for range 3000 {
		deepJSON = `{"type":"array","items":` + deepJSON + `}`
	}
	start = time.Now()
	_, e = r.Register("deep-json", SchemaInput{SchemaType: TypeJSON, Schema: deepJSON}, false)
	took = time.Since(start)
	if e == nil || e.Code != codeInvalidSchema {
		t.Errorf("a 3,000-deep JSON Schema: %v, want %d", e, codeInvalidSchema)
	} else if len(e.Message) > 200 {
		t.Errorf("the refusal is %d bytes: it echoes the schema", len(e.Message))
	}
	if bound := slack(time.Second); took > bound {
		t.Errorf("a 3,000-deep JSON Schema was answered in %s, want under %s", took, bound)
	}

	// The limit is on nesting, not on brackets inside names and docs, and a schema
	// at the limit is ordinary.
	doc := `{"type":"record","name":"R","doc":"` + strings.Repeat("[{", 400) + `\"","fields":[{"name":"f","type":"int"}]}`
	mustRegister(t, r, "brackets-in-doc", avroIn(doc))
	mustRegister(t, r, "at-the-limit", avroIn(nestedAvro(maxSchemaDepth/2-2, "int")))
}

// Every request body is read up to a limit: the config and mode bodies are a few
// bytes, and a 20 MiB one is refused before it is all read. (SCH-3)
func TestBodiesAreBounded(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts})
	big := `{"compatibility":"FULL","mode":"READWRITE","pad":"` + strings.Repeat("x", 20<<20) + `"}`
	for _, path := range []string{"/config", "/config/x", "/mode", "/mode/x"} {
		req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(big))
		req.Header.Set("Content-Type", contentType)
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("PUT %s with a 20 MiB body: %d %s, want 413", path, rec.Code, rec.Body)
		}
	}
	// A schema body past the 20 MiB limit is refused as too large, not reported as
	// broken JSON.
	huge := `{"schema":"` + strings.Repeat("x", 21<<20) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/subjects/x/versions", strings.NewReader(huge))
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("POST a 21 MiB schema: %d %.200s, want 413", rec.Code, rec.Body)
	}
}

// A subject too long to store is refused before any work, as a bad request and not a
// store failure, and the answer does not echo it back. (SCH-18; the exact code
// follows recording R7.)
func TestSubjectLengthIsBounded(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts})
	long := strings.Repeat("s", 40000)
	req := httptest.NewRequest(http.MethodPost, "/subjects/"+url.PathEscape(long)+"/versions", strings.NewReader(`{"schema":"\"string\""}`))
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code < 400 || rec.Code >= 500 {
		t.Errorf("a 40,000-character subject: %d, want a 4xx refusal", rec.Code)
	}
	if rec.Body.Len() > 1000 {
		t.Errorf("the refusal is %d bytes: it echoes the subject", rec.Body.Len())
	}
	// Nor can a level or a mode be stored under one.
	if e := r.SetCompat(long, "FULL"); e == nil || e.Code != codeInvalidSubject {
		t.Errorf("a level for a 40,000-character subject: %v, want %d", e, codeInvalidSubject)
	}
	if e := r.SetMode(long, "READONLY"); e == nil || e.Code != codeInvalidSubject {
		t.Errorf("a mode for a 40,000-character subject: %v, want %d", e, codeInvalidSubject)
	}
	// A 255-character subject is ordinary.
	mustRegister(t, r, strings.Repeat("s", 255), avroIn(`"string"`))
}

// A schema stored before the canonical form changed is found again when the same text
// is registered after the change: the same id, no new version. (The Avro unnamed-type
// attributes and the Protobuf field options were printed differently before.)
func TestOldCanonicalFormsAreStillFound(t *testing.T) {
	st := NewMemoryStorage()
	old := map[string][]byte{
		schemaKey(1):       []byte(`{"id":1,"type":"AVRO","schema":"{\"type\":\"string\",\"doc\":\"d\"}"}`),
		versionKey("a", 1): []byte(`{"subject":"a","version":1,"id":1}`),
		schemaKey(2):       []byte(`{"id":2,"type":"PROTOBUF","schema":"syntax = \"proto2\";\npackage t;\n\nmessage M {\n  optional string b = 2 [default = \"hi\", deprecated = true];\n}\n"}`),
		versionKey("p", 1): []byte(`{"subject":"p","version":1,"id":2}`),
		keyNextID:          []byte("3"),
	}
	if err := st.Apply(old, nil); err != nil {
		t.Fatal(err)
	}
	r, err := Open(st)
	if err != nil {
		t.Fatal(err)
	}
	if id := mustRegister(t, r, "a", avroIn(`{"type":"string","doc":"d"}`)); id != 1 {
		t.Errorf("the stored Avro schema registered again got id %d, want 1", id)
	}
	if id := mustRegister(t, r, "p", SchemaInput{SchemaType: TypeProtobuf, Schema: `syntax = "proto2"; package t; message M { optional string b = 2 [default = "hi", deprecated = true]; }`}); id != 2 {
		t.Errorf("the stored Protobuf schema registered again got id %d, want 2", id)
	}
	for _, s := range []string{"a", "p"} {
		if vs := r.live(s); len(vs) != 1 {
			t.Errorf("subject %s has %d versions, want 1", s, len(vs))
		}
	}
	// Deleted for good, the schema is gone under both forms: registered again it is new.
	if _, e := r.DeleteSubject("a", false); e != nil {
		t.Fatal(e)
	}
	if _, e := r.DeleteSubject("a", true); e != nil {
		t.Fatal(e)
	}
	if id := mustRegister(t, r, "a", avroIn(`{"type":"string","doc":"d"}`)); id == 1 {
		t.Errorf("after a permanent delete the schema came back as id 1")
	}
}

// A record the registry cannot read stops it opening, and the error names the record,
// so whoever starts it can say which one. (SCH-13; the broker carries on without the
// registry: cmd/doze-kafka TestBrokerStartsWithAnUnreadableRegistry.)
func TestOpenWithABadRecord(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.Apply(map[string][]byte{schemaKey(1): []byte("not json")}, nil); err != nil {
		t.Fatal(err)
	}
	_, err := Open(st)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%q", schemaKey(1))) {
		t.Errorf("Open with an unreadable record: %v, want an error naming %q", err, schemaKey(1))
	}
}
