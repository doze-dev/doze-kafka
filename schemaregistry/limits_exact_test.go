package schemaregistry

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

// Each limit is held at the limit itself: what is exactly at it is allowed, one past
// it is not. (These kill the mutants that move a limit by one; scripts/mutate.)
func TestLimitsHoldExactlyAtTheLimit(t *testing.T) {
	r, _ := openReg(t)

	// A subject of exactly the longest length is stored; one byte more is refused.
	at := strings.Repeat("s", maxSubjectLen)
	if _, e := r.Register(at, avroIn(`"string"`), false); e != nil {
		t.Errorf("a subject of exactly %d bytes: %v", maxSubjectLen, e)
	}
	if _, e := r.Register(at+"s", avroIn(`"string"`), false); e == nil || e.Code != codeInvalidSubject {
		t.Errorf("a subject one byte over: %v, want %d", e, codeInvalidSubject)
	}
	// A control character is refused wherever it is, the first byte included.
	for _, s := range []string{"\x01orders", "ord\x1fers", "orders\x7f"} {
		if _, e := r.Register(s, avroIn(`"string"`), false); e == nil || e.Code != codeInvalidSubject {
			t.Errorf("subject %q: %v, want %d", s, e, codeInvalidSubject)
		}
	}

	// Text nested exactly maxSchemaDepth deep is read; one level more is refused before
	// any parser sees it, for Avro and JSON Schema alike. (JSON Schema's own compiler
	// refuses nesting well short of the limit, so the exact edge is held with Avro.)
	k := 0
	for jsonDepth(nestedAvro(k+1, "int")) <= maxSchemaDepth {
		k++
	}
	if d := jsonDepth(nestedAvro(k, "int")); d != maxSchemaDepth {
		t.Fatalf("the fixture nests %d, want %d", d, maxSchemaDepth)
	}
	if _, e := r.Register("avro-at", avroIn(nestedAvro(k, "int")), false); e != nil && strings.Contains(e.Message, "nested more than") {
		t.Errorf("Avro nested exactly %d deep is refused by the depth limit: %v", maxSchemaDepth, e)
	}
	if _, e := r.Register("avro-over", avroIn(nestedAvro(k+1, "int")), false); e == nil || !strings.Contains(e.Message, "nested more than") {
		t.Errorf("Avro nested one past the limit: %v, want refused as nested too deep", e)
	}
	deep := `{"type":"integer"}`
	for jsonDepth(deep) <= maxSchemaDepth {
		deep = `{"type":"array","items":` + deep + `}`
	}
	if _, e := r.Register("json-over", SchemaInput{SchemaType: TypeJSON, Schema: deep}, false); e == nil || !strings.Contains(e.Message, "nested more than") {
		t.Errorf("JSON Schema one level past the limit: %v", e)
	}
	// Depth is nesting, not the count of brackets: a wide schema with many more than
	// maxSchemaDepth objects side by side is ordinary.
	var props []string
	for i := range maxSchemaDepth * 2 {
		props = append(props, fmt.Sprintf(`"p%d":{"type":"string"}`, i))
	}
	if _, e := r.Register("wide", SchemaInput{SchemaType: TypeJSON, Schema: `{"type":"object","properties":{` + strings.Join(props, ",") + `}}`}, false); e != nil {
		t.Errorf("a wide, shallow schema: %v", e)
	}
}

// The request log holds exactly maxRequests requests, not one more.
func TestTheRequestRingHoldsExactlyItsBound(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts})
	for i := range maxRequests + 5 {
		api.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", fmt.Sprintf("/subjects/s%d/versions", i), nil))
	}
	if n := len(api.reqs.buf); n != maxRequests {
		t.Errorf("the ring holds %d, want %d", n, maxRequests)
	}
}

// The overview gives the ids in use as runs: 1 to 3 and 5, not four runs or one.
func TestOverviewIDsAreRuns(t *testing.T) {
	r, _ := openReg(t)
	for i, s := range []string{`"string"`, `"int"`, `"long"`, `"double"`, `"float"`} {
		mustRegister(t, r, fmt.Sprintf("s%d", i), avroIn(s))
	}
	if _, e := r.DeleteSubject("s3", false); e != nil {
		t.Fatal(e)
	}
	if _, e := r.DeleteSubject("s3", true); e != nil {
		t.Fatal(e)
	}
	if got := fmt.Sprint(r.Overview().IDsInUse); got != "[[1 3] [5 5]]" {
		t.Errorf("ids in use = %s, want [[1 3] [5 5]]", got)
	}
	// A soft-deleted version's id still resolves, and is not live.
	if _, e := r.DeleteVersion("s1", "1", false); e != nil {
		t.Fatal(e)
	}
	o := r.Overview()
	if got := fmt.Sprint(o.IDsInUse, o.LiveIDs); got != "[[1 3] [5 5]] [[1 1] [3 3] [5 5]]" {
		t.Errorf("ids in use, live = %s", got)
	}
}

// A schema with no references is not "inlined": there was nothing to write out.
func TestResolvedWithoutReferencesIsNotInlined(t *testing.T) {
	r, _ := openReg(t)
	mustRegister(t, r, "plain", avroIn(avroUser1))
	got, e := r.Resolved("plain", "1")
	if e != nil || got.Inlined || len(got.References) != 0 {
		t.Errorf("resolved = %+v, %v; want not inlined, no references", got, e)
	}
}

// A limit of 0 is no limit, as recorded (R6); only a positive limit cuts the page.
func TestALimitOfZeroIsNoLimit(t *testing.T) {
	r, _ := openReg(t)
	for i, s := range []string{`"string"`, `"int"`, `"long"`} {
		mustRegister(t, r, fmt.Sprintf("s%d", i), avroIn(s))
	}
	api := NewAPI(r, Options{AllowedHosts: testHosts})
	for q, want := range map[string]int{"limit=0": 3, "limit=-1": 3, "limit=2": 2, "limit=3": 3, "offset=0&limit=1": 1} {
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, httptest.NewRequest("GET", "/schemas?"+q, nil))
		if n := strings.Count(rec.Body.String(), `"subject"`); n != want {
			t.Errorf("GET /schemas?%s: %d schemas, want %d", q, n, want)
		}
	}
}
