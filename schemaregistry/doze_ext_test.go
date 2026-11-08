package schemaregistry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A version remembers when it was registered, by which user, from where and with what
// client, and a level and a mode when they were set; a restart keeps all of it, and a
// record from before these were kept reads as unknown. (SCH-20)
func TestVersionMetadataSurvivesRestart(t *testing.T) {
	st := NewMemoryStorage()
	r, err := Open(st)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 7, 14, 36, 15, 0, time.UTC)
	r.now = func() time.Time { return at }
	who := Origin{User: "alice", From: "10.0.0.7", Agent: "Java/21 KafkaAvroSerializer"}
	if _, e := r.RegisterFrom("orders-value", avroIn(avroUser1), false, who); e != nil {
		t.Fatal(e)
	}
	if e := r.SetCompat("orders-value", "FULL"); e != nil {
		t.Fatal(e)
	}
	if e := r.SetMode("orders-value", "READONLY"); e != nil {
		t.Fatal(e)
	}

	r2, err := Open(st)
	if err != nil {
		t.Fatal(err)
	}
	v, e := r2.VersionInfo("orders-value", "1")
	if e != nil {
		t.Fatal(e)
	}
	if !v.RegisteredAt.Equal(at) || v.RegisteredBy != "alice" || v.From != "10.0.0.7" || v.Agent != who.Agent {
		t.Errorf("after a restart, version 1 says %+v", v)
	}
	o := r2.Overview()
	s := o.subject("orders-value")
	if s == nil || s.LevelSetAt == nil || !s.LevelSetAt.Equal(at) || s.ModeSetAt == nil || !s.ModeSetAt.Equal(at) {
		t.Errorf("after a restart the level and mode were set at %+v, want %s", s, at)
	}
}

func TestOldRecordsWithoutMetadata(t *testing.T) {
	st := NewMemoryStorage()
	st.Apply(map[string][]byte{
		schemaKey(1):         []byte(`{"id":1,"type":"AVRO","schema":"\"string\""}`),
		versionKey("old", 1): []byte(`{"subject":"old","version":1,"id":1}`),
		keyConfig + "old":    []byte("FULL"),
		keyNextID:            []byte("2"),
	}, nil)
	r, err := Open(st)
	if err != nil {
		t.Fatal(err)
	}
	v, e := r.VersionInfo("old", "latest")
	if e != nil {
		t.Fatal(e)
	}
	if v.RegisteredAt != nil || v.RegisteredBy != "" || v.From != "" || v.Agent != "" {
		t.Errorf("a version stored before metadata was kept says %+v, want nothing known", v)
	}
	if s := r.Overview().subject("old"); s == nil || s.LevelSetAt != nil || s.Level != "FULL" || !s.LevelIsOwn {
		t.Errorf("a level stored before set-at was kept: %+v", s)
	}
}

// The HTTP API passes who asked to the store, and the extension route answers it.
func TestVersionMetadataOverHTTP(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts, Auth: func(u, p string) bool { return p == "pw" }})
	req := httptest.NewRequest("POST", "/subjects/x/versions", strings.NewReader(schemaBody(avroUser1)))
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", "confluent-kafka-python/2.6")
	req.RemoteAddr = "192.0.2.9:51234"
	req.SetBasicAuth("bob", "pw")
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("register: %d %s", rec.Code, rec.Body)
	}
	req = httptest.NewRequest("GET", "/_doze/subjects/x/versions/1", nil)
	req.SetBasicAuth("bob", "pw")
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	var got map[string]any
	json.Unmarshal(rec.Body.Bytes(), &got)
	if rec.Code != 200 || got["registeredBy"] != "bob" || got["from"] != "192.0.2.9" || got["agent"] != "confluent-kafka-python/2.6" || got["registeredAt"] == nil || got["id"] != float64(1) {
		t.Errorf("GET /_doze/subjects/x/versions/1: %d %s", rec.Code, rec.Body)
	}
	// It needs the same credentials as every route.
	req = httptest.NewRequest("GET", "/_doze/subjects/x/versions/1", nil)
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("the extension route without credentials: %d, want 401", rec.Code)
	}
}

// The overview's next id is what the next new schema gets, after a permanent delete of
// the highest id and after a restart too; the ids in use are the ones that resolve, as
// ranges; a soft-deleted subject is listed as such. (SCH-23)
func TestOverviewNextIDMatchesRegister(t *testing.T) {
	st := NewMemoryStorage()
	r, _ := Open(st)
	a := mustRegister(t, r, "a", avroIn(avroUser1))
	mustRegister(t, r, "a", avroIn(avroUser2))
	c := mustRegister(t, r, "c", avroIn(`"string"`))
	mustRegister(t, r, "gone", avroIn(`"long"`))
	if _, e := r.DeleteSubject("gone", false); e != nil {
		t.Fatal(e)
	}
	if _, e := r.DeleteSubject("c", false); e != nil {
		t.Fatal(e)
	}
	if _, e := r.DeleteSubject("c", true); e != nil { // the id of "string" goes for good
		t.Fatal(e)
	}
	r2, _ := Open(st)
	o := r2.Overview()
	next := mustRegister(t, r2, "n", avroIn(`"double"`))
	if o.NextID != next {
		t.Errorf("overview said the next id is %d, register gave %d", o.NextID, next)
	}
	if c <= a || o.IDsInUse[0][0] != a {
		t.Fatalf("ids: a=%d c=%d, in use %v", a, c, o.IDsInUse)
	}
	for _, rng := range o.IDsInUse {
		if c >= rng[0] && c <= rng[1] {
			t.Errorf("id %d was deleted for good but is listed in use: %v", c, o.IDsInUse)
		}
	}
	if s := o.subject("gone"); s == nil || !s.Deleted || s.Versions != 0 || s.DeletedVersions != 1 {
		t.Errorf("the soft-deleted subject: %+v", s)
	}
	if s := o.subject("a"); s == nil || s.Versions != 2 || s.LatestVersion != 2 || s.Level != "BACKWARD" || s.LevelIsOwn || s.Type != TypeAvro {
		t.Errorf("subject a: %+v", s)
	}
	if o.subject("c") != nil {
		t.Errorf("a subject deleted for good is listed")
	}
}

// The registry keeps its newest requests (SCH-21): 2,500 asked, the newest 2,000 kept,
// newest first, with a cursor; filtered by subject, which includes a request by id for
// a schema the subject holds; the query is kept with the path, no body is kept; the extension routes and requests marked
// as the console's own are not.
func TestRequestRingBounds(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts})
	do := func(method, path, body string, own bool) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if own {
			req.Header.Set(ownHeader, "1")
		}
		api.ServeHTTP(httptest.NewRecorder(), req)
	}
	do("POST", "/subjects/orders-value/versions", schemaBody(avroUser1), false)
	do("GET", "/schemas/ids/1?fetchMaxId=true", "", false)
	do("GET", "/subjects/other/versions", "", false)
	do("GET", "/subjects", "", true)
	do("GET", "/_doze/overview", "", false)

	got := api.Requests(RequestFilter{Subject: "orders-value"})
	if len(got.Requests) != 2 || got.Requests[0].Path != "/schemas/ids/1?fetchMaxId=true" || got.Requests[1].Method != "POST" || got.Requests[1].Status != 200 {
		t.Fatalf("orders-value's requests: %+v", got.Requests)
	}
	if all := api.Requests(RequestFilter{}); len(all.Requests) != 3 {
		t.Errorf("all requests: %d, want 3 (not the console's own, not /_doze/)", len(all.Requests))
	}
	for i := range 2500 {
		do("GET", fmt.Sprintf("/subjects/s%d/versions", i), "", false)
	}
	page := api.Requests(RequestFilter{Limit: 2500})
	if len(page.Requests) != 2000 || page.Requests[0].Subject != "s2499" {
		t.Fatalf("after 2,500 more: %d kept, newest %q", len(page.Requests), page.Requests[0].Subject)
	}
	first := api.Requests(RequestFilter{Limit: 10})
	next := api.Requests(RequestFilter{Limit: 10, Before: first.Next})
	if next.Requests[0].Seq != first.Requests[9].Seq-1 {
		t.Errorf("the cursor: page two starts at %d after %d", next.Requests[0].Seq, first.Requests[9].Seq)
	}
}

// A check can be asked at a level that is not the subject's, without changing it, and
// answers with each reason taken apart: type, path, sentence, info. (SCH-22)
func TestCheckAtLevel(t *testing.T) {
	r, _ := openReg(t)
	mustRegister(t, r, "x", avroIn(avroUser1)) // the subject is at BACKWARD
	// avroUserBad adds a required field: not BACKWARD, but FORWARD.
	at := func(level string) []CompatFinding {
		t.Helper()
		f, e := r.CheckAt("x", "", avroIn(avroUserBad), false, level)
		if e != nil {
			t.Fatal(e)
		}
		return f
	}
	if f := at(""); len(f) != 1 || f[0].Level != CompatBackward {
		t.Fatalf("at the subject's own level: %+v", f)
	}
	if f := at(CompatForward); len(f) != 0 {
		t.Errorf("at FORWARD: %+v, want compatible", f)
	}
	f := at(CompatFull)
	if len(f) != 1 || len(f[0].Details) != 1 {
		t.Fatalf("at FULL: %+v", f)
	}
	d := f[0].Details[0]
	if d.Type != "READER_FIELD_MISSING_DEFAULT_VALUE" || d.Path != "/fields/1" || d.Info != "email" || !strings.Contains(d.Description, "has no default value") {
		t.Errorf("the reason taken apart: %+v", d)
	}
	if lvl, _ := r.Compat("x", true); lvl != CompatBackward {
		t.Errorf("checking at another level changed the subject's to %s", lvl)
	}
	if _, e := r.CheckAt("x", "", avroIn(avroUserBad), false, "SIDEWAYS"); e == nil || e.Code != codeInvalidCompatibility {
		t.Errorf("an unknown level: %v, want %d", e, codeInvalidCompatibility)
	}

	// Over HTTP: POST /_doze/subjects/x/check?level=FULL.
	api := NewAPI(r, Options{AllowedHosts: testHosts})
	req := httptest.NewRequest("POST", "/_doze/subjects/x/check?level=FULL", strings.NewReader(schemaBody(avroUserBad)))
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	var got struct {
		Compatible bool   `json:"compatible"`
		Level      string `json:"level"`
		Compared   []int  `json:"compared"`
		Findings   []struct {
			Version int            `json:"version"`
			Reasons []CompatReason `json:"reasons"`
		} `json:"findings"`
	}
	json.Unmarshal(rec.Body.Bytes(), &got)
	if rec.Code != 200 || got.Compatible || got.Level != CompatFull || len(got.Compared) != 1 || got.Compared[0] != 1 ||
		len(got.Findings) != 1 || got.Findings[0].Reasons[0].Path != "/fields/1" {
		t.Errorf("POST /_doze/subjects/x/check?level=FULL: %d %s", rec.Code, rec.Body)
	}
}

// Every extension route asks for the same credentials as the Confluent routes.
func TestExtensionRoutesAreAuthenticated(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts, Auth: func(u, p string) bool { return p == "pw" }})
	for _, c := range [][2]string{{"GET", "/_doze/overview"}, {"GET", "/_doze/requests"}, {"GET", "/_doze/subjects/x/versions/1"}, {"GET", "/_doze/subjects/x/versions/1/resolved"}, {"POST", "/_doze/subjects/x/check"}} {
		req := httptest.NewRequest(c[0], c[1], strings.NewReader(schemaBody(avroUser1)))
		req.Header.Set("Content-Type", contentType)
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without credentials: %d, want 401", c[0], c[1], rec.Code)
		}
	}
}

// The resolved view writes a referenced type out where it is first used and names it
// after that, through a reference's own references, keeping docs and defaults; the
// result is the same schema (it parses, and is compatible both ways with the
// original). (SCH-22, board Sc-10)
func TestInlineAvro(t *testing.T) {
	r, _ := openReg(t)
	mustRegister(t, r, "geo", avroIn(`{"type":"record","name":"Geo","namespace":"com.acme.geo","fields":[{"name":"lat","type":"double"}]}`))
	mustRegister(t, r, "address", SchemaInput{Schema: `{"type":"record","name":"Address","namespace":"com.acme","doc":"where","fields":[{"name":"city","type":"string","default":"?"},{"name":"at","type":"com.acme.geo.Geo"}]}`,
		References: []Reference{{Name: "com.acme.geo.Geo", Subject: "geo", Version: 1}}})
	person := SchemaInput{Schema: `{"type":"record","name":"Person","namespace":"com.acme","fields":[{"name":"home","type":"Address"},{"name":"work","type":["null","com.acme.Address"],"default":null}]}`,
		References: []Reference{{Name: "com.acme.Address", Subject: "address", Version: 1}}}
	mustRegister(t, r, "person", person)

	got, e := r.Resolved("person", "latest")
	if e != nil {
		t.Fatal(e)
	}
	s := got.Schema
	if strings.Count(s, `"fields"`) != 3 || !strings.Contains(s, `"name":"com.acme.Address"`) || !strings.Contains(s, `"doc":"where"`) ||
		!strings.Contains(s, `"default":"?"`) || !strings.Contains(s, `"name":"com.acme.geo.Geo"`) || !strings.Contains(s, `["null","Address"]`) {
		t.Errorf("the resolved view: %s", s)
	}
	alone, err := parseSchema(TypeAvro, s, nil)
	if err != nil {
		t.Fatalf("the resolved view does not parse on its own: %v\n%s", err, s)
	}
	orig, _ := r.parseStored(mustRegister(t, r, "person", person))
	if rs := compare(alone, orig, CompatFull); len(rs) != 0 {
		t.Errorf("the resolved view is not the same schema: %v", rs)
	}
	if len(got.References) != 2 {
		t.Errorf("the references listed: %+v", got.References)
	}
}
