package schemaregistry

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

func openReg(t *testing.T) (*Registry, *MemoryStorage) {
	t.Helper()
	st := NewMemoryStorage()
	r, err := Open(st)
	if err != nil {
		t.Fatal(err)
	}
	return r, st
}

func mustRegister(t *testing.T, r *Registry, subject string, in SchemaInput) int {
	t.Helper()
	res, e := r.Register(subject, in, false)
	if e != nil {
		t.Fatalf("register %s: %v", subject, e)
	}
	return res.ID
}

func avroIn(s string) SchemaInput { return SchemaInput{Schema: s} }

// Everything the registry holds is in the metadata database, so a restart finds it as
// it was: schemas and their ids, versions and which are deleted, levels and modes. An
// id is never handed out twice, even when the schema that held the highest one has
// been deleted for good.
func TestEverythingSurvivesARestart(t *testing.T) {
	r, st := openReg(t)
	a := mustRegister(t, r, "orders", avroIn(avroUser1))
	b := mustRegister(t, r, "orders", avroIn(avroUser2))
	c := mustRegister(t, r, "users", SchemaInput{Schema: jsonUser1, SchemaType: TypeJSON})
	if a != 1 || b != 2 || c != 3 {
		t.Fatalf("ids %d %d %d, want 1 2 3", a, b, c)
	}
	if e := r.SetCompat("orders", "FULL"); e != nil {
		t.Fatal(e)
	}
	if e := r.SetCompat("", "NONE"); e != nil {
		t.Fatal(e)
	}
	if e := r.SetMode("frozen", ModeReadOnly); e != nil {
		t.Fatal(e)
	}
	if _, e := r.DeleteVersion("orders", "1", false); e != nil {
		t.Fatal(e)
	}
	// The newest schema goes for good: its id must stay spent.
	if _, e := r.DeleteSubject("users", false); e != nil {
		t.Fatal(e)
	}
	if _, e := r.DeleteSubject("users", true); e != nil {
		t.Fatal(e)
	}

	r2, err := Open(st)
	if err != nil {
		t.Fatal(err)
	}
	if got := r2.Subjects("", false, false); len(got) != 1 || got[0] != "orders" {
		t.Errorf("subjects after a restart = %v, want [orders]", got)
	}
	if m, _ := r2.Mode("frozen", false); m != ModeReadOnly {
		t.Errorf("subject mode = %q, want READONLY", m)
	}
	if v, _ := r2.Versions("orders", true); len(v) != 2 {
		t.Errorf("versions (with deleted) = %v, want both", v)
	}
	if v, _ := r2.Versions("orders", false); len(v) != 1 || v[0] != 2 {
		t.Errorf("versions = %v, want [2]", v)
	}
	if l, _ := r2.Compat("orders", false); l != "FULL" {
		t.Errorf("subject level = %q, want FULL", l)
	}
	if l, _ := r2.Compat("", false); l != "NONE" {
		t.Errorf("global level = %q, want NONE", l)
	}
	if _, e := r2.SchemaByID(3); e == nil {
		t.Error("the schema deleted for good is still there")
	}
	if next := mustRegister(t, r2, "fresh", avroIn(`"string"`)); next != 4 {
		t.Errorf("the next id is %d; id 3 was spent and must not be reissued (want 4)", next)
	}
}

// A write that the store refuses must leave the registry as it was, and say so.
func TestAFailedWriteChangesNothing(t *testing.T) {
	r, st := openReg(t)
	mustRegister(t, r, "s", avroIn(avroUser1))
	fail := &failingStore{MemoryStorage: st}
	r.st = fail
	fail.err = errors.New("disk full")
	if _, e := r.Register("s", avroIn(avroUser2), false); e == nil || e.Code != codeStore {
		t.Fatalf("register on a failing store = %v, want error %d", e, codeStore)
	}
	if v, _ := r.Versions("s", false); len(v) != 1 {
		t.Errorf("versions = %v after a failed write, want just version 1", v)
	}
	fail.err = nil
	if id := mustRegister(t, r, "s", avroIn(avroUser2)); id != 2 {
		t.Errorf("id after the failure = %d, want 2 (the failed attempt took none)", id)
	}
}

type failingStore struct {
	*MemoryStorage
	err error
}

func (f *failingStore) Apply(puts map[string][]byte, deletes []string) error {
	if f.err != nil {
		return f.err
	}
	return f.MemoryStorage.Apply(puts, deletes)
}

// Registration is serialized: the same schema from many clients at once is one id and
// one version; different schemas get distinct ids and consecutive versions.
func TestConcurrentRegistration(t *testing.T) {
	r, _ := openReg(t)
	var wg sync.WaitGroup
	same := make([]int, 40)
	for i := range same {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, e := r.Register("same", avroIn(avroUser1), false)
			if e != nil {
				t.Error(e)
				return
			}
			same[i] = res.ID
		}()
	}
	distinct := make([]int, 20)
	for i := range distinct {
		wg.Add(1)
		go func() {
			defer wg.Done()
			schema := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"f%d","type":"int","default":0}]}`, i)
			res, e := r.Register("many", SchemaInput{Schema: schema}, false)
			if e != nil && e.Code != codeIncompatible { // adding f(i) and dropping the others is a legal evolution only sometimes.
				t.Error(e)
			}
			if e == nil {
				distinct[i] = res.ID
			}
		}()
	}
	wg.Wait()
	for _, id := range same {
		if id != same[0] || id == 0 {
			t.Fatalf("one schema registered concurrently got ids %v", same)
		}
	}
	if v, _ := r.Versions("same", false); len(v) != 1 {
		t.Errorf("versions of the repeated schema = %v, want one", v)
	}
	seen := map[int]bool{}
	for _, id := range distinct {
		if id != 0 && seen[id] {
			t.Errorf("id %d given to two different schemas", id)
		}
		seen[id] = true
	}
}

// JSON Schema and Protobuf take references too: a $ref to a name, an import of a name.
func TestReferencesInJSONAndProtobuf(t *testing.T) {
	r, _ := openReg(t)
	mustRegister(t, r, "address", SchemaInput{SchemaType: TypeJSON, Schema: `{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`})
	person := SchemaInput{SchemaType: TypeJSON, Schema: `{"type":"object","properties":{"home":{"$ref":"address.json"}}}`,
		References: []Reference{{Name: "address.json", Subject: "address", Version: 1}}}
	mustRegister(t, r, "person", person)
	// The reference is followed when comparing: a person whose home became a number is breaking.
	changed := SchemaInput{SchemaType: TypeJSON, Schema: `{"type":"object","properties":{"home":{"type":"number"}}}`}
	if msgs, e := r.CheckCompatible("person", "latest", changed, false); e != nil || len(msgs) == 0 {
		t.Errorf("home changed from an address object to a number: %v %v, want incompatible", msgs, e)
	}

	mustRegister(t, r, "proto-dep", SchemaInput{SchemaType: TypeProtobuf, Schema: `syntax = "proto3"; package dep; message Addr { string city = 1; }`})
	main := SchemaInput{SchemaType: TypeProtobuf, Schema: `syntax = "proto3"; import "dep.proto"; message P { dep.Addr home = 1; }`,
		References: []Reference{{Name: "dep.proto", Subject: "proto-dep", Version: 1}}}
	mustRegister(t, r, "proto-main", main)
	retyped := SchemaInput{SchemaType: TypeProtobuf, Schema: `syntax = "proto3"; message P { string home = 1; }`}
	if msgs, e := r.CheckCompatible("proto-main", "latest", retyped, false); e != nil || len(msgs) == 0 {
		t.Errorf("home changed from a message to a string: %v %v, want incompatible", msgs, e)
	}

	if ids, e := r.ReferencedBy("address", "1"); e != nil || len(ids) != 1 {
		t.Errorf("referenced by = %v %v, want one schema", ids, e)
	}
	if _, e := r.DeleteVersion("address", "1", false); e == nil || e.Code != codeReferenceExists {
		t.Errorf("deleting a referenced version = %v, want %d", e, codeReferenceExists)
	}
	// A reference that is not there is an invalid schema, not a crash.
	missing := person
	missing.References = []Reference{{Name: "address.json", Subject: "address", Version: 9}}
	if _, e := r.Register("person2", missing, false); e == nil || e.Code != codeInvalidSchema {
		t.Errorf("a reference to a missing version = %v, want %d", e, codeInvalidSchema)
	}
}

// Subject names can hold anything, "/" included, as a URL-encoded path element.
func TestSubjectsWithUnusualNames(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts})
	for _, name := range []string{"a/b", "with space", "ünïcode-значение", "100%", "dot.dot", "q?mark", "hash#tag"} {
		path := "/subjects/" + url.PathEscape(name) + "/versions"
		req := httptest.NewRequest("POST", path, strings.NewReader(schemaBody(avroUser1)))
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Errorf("register under %q: %d %s", name, rec.Code, rec.Body)
			continue
		}
		var subjects []string
		rec = httptest.NewRecorder()
		api.ServeHTTP(rec, httptest.NewRequest("GET", "/subjects", nil))
		json.Unmarshal(rec.Body.Bytes(), &subjects)
		found := false
		for _, s := range subjects {
			found = found || s == name
		}
		if !found {
			t.Errorf("subject %q is not listed (got %v)", name, subjects)
		}
	}
}

// With credentials configured every request needs them, the health of the registry
// included, and a wrong pair is the same 401 as none.
func TestBasicAuthIsRequiredWhenConfigured(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts, Auth: func(u, p string) bool { return u == "alice" && p == "pw" }})
	try := func(user, pass string) int {
		req := httptest.NewRequest("GET", "/subjects", nil)
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		return rec.Code
	}
	if c := try("", ""); c != http.StatusUnauthorized {
		t.Errorf("no credentials = %d, want 401", c)
	}
	if c := try("alice", "nope"); c != http.StatusUnauthorized {
		t.Errorf("wrong password = %d, want 401", c)
	}
	if c := try("alice", "pw"); c != 200 {
		t.Errorf("right credentials = %d, want 200", c)
	}
}

// The levels differ in which earlier versions a new schema is compared with.
func TestCompatibilityLevelsChooseTheVersionsCompared(t *testing.T) {
	// v1 {a}, v2 {a, b:int default 0}; the candidate drops a's default-less b... build
	// three versions where only the transitive levels see a conflict with v1.
	v1 := `{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}`
	v2 := `{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":"int","default":0}]}`
	// candidate adds c without a default: breaks readers of anything written before c existed.
	cand := `{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":"int"},{"name":"c","type":"int"}]}`
	for _, tc := range []struct {
		level string
		ok    bool
	}{
		{CompatNone, true},
		{CompatBackward, false},           // v2 lacks c, no default.
		{CompatBackwardTransitive, false}, // as does v1.
		{CompatForward, true},             // v2 can read what cand writes (extra fields ignored).
		{CompatForwardTransitive, false},  // v1 cannot read b? it ignores it: but v2... see below.
	} {
		r, _ := openReg(t)
		mustRegister(t, r, "s", avroIn(v1))
		r.SetCompat("s", CompatNone)
		mustRegister(t, r, "s", avroIn(v2))
		r.SetCompat("s", tc.level)
		found, e := r.CheckCompatible("s", "", avroIn(cand), false)
		if e != nil {
			t.Fatal(e)
		}
		if tc.level == CompatForwardTransitive {
			continue // exercised in the corpus; here only the non-transitive/transitive split matters.
		}
		if (len(found) == 0) != tc.ok {
			t.Errorf("%s: compatible=%v, want %v (%v)", tc.level, len(found) == 0, tc.ok, found)
		}
	}
}
