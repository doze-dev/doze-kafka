package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// noPanic runs f and fails the test, rather than the binary, if it panics.
func noPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", what, r)
		}
	}()
	f()
}

func registerFormReq(form url.Values) *http.Request {
	r := httptest.NewRequest("POST", "/_console/new/schema", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

func TestARegisterDraftSendsTheTypeAndReferencesOnlyWhenThereAreSome(t *testing.T) {
	if b := (regDraft{Schema: "x", Type: schemaregistry.TypeProtobuf}).body(); b.SchemaType != schemaregistry.TypeProtobuf {
		t.Errorf("a Protobuf draft sends type %q", b.SchemaType)
	}
	if b := (regDraft{Schema: "x", Type: schemaregistry.TypeJSON}).body(); b.SchemaType != schemaregistry.TypeJSON {
		t.Errorf("a JSON draft sends type %q", b.SchemaType)
	}
	for _, typ := range []string{"", schemaregistry.TypeAvro} {
		if b := (regDraft{Schema: "x", Type: typ}).body(); b.SchemaType != "" {
			t.Errorf("an Avro draft (type %q) sends type %q; Avro is the default and is left out", typ, b.SchemaType)
		}
	}
	b := regDraft{Schema: "x", Type: schemaregistry.TypeAvro}.body()
	if b.References != nil {
		t.Errorf("a draft with no references sends %#v", b.References)
	}
	b = regDraft{Schema: "x", Refs: []refRow{{Name: "n", Subject: "s", Version: 2}}}.body()
	refs, ok := b.References.([]map[string]any)
	if !ok || len(refs) != 1 || refs[0]["name"] != "n" || refs[0]["subject"] != "s" || refs[0]["version"] != 2 {
		t.Errorf("one reference sends %#v", b.References)
	}
}

func TestTheRegisterFormIsReadIntoADraft(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[]}`
	d, err := draftFromForm(registerFormReq(url.Values{"subject": {" s-value "}, "schema": {schema}, "normalize": {"on"}}))
	if err != nil || d.Subject != "s-value" || !d.Normalize || d.Type != schemaregistry.TypeAvro {
		t.Errorf("normalize on: %+v %v", d, err)
	}
	d, err = draftFromForm(registerFormReq(url.Values{"subject": {"s-value"}, "schema": {schema}}))
	if err != nil || d.Normalize {
		t.Errorf("normalize absent: %+v %v", d, err)
	}

	// References arrive as rows, one of each field per row; an empty row is skipped.
	d, err = draftFromForm(registerFormReq(url.Values{"subject": {"s"}, "schema": {schema},
		"ref_name": {" a ", "", "b"}, "ref_subject": {"sa", "", "sb"}, "ref_version": {"1", "", "3"}}))
	if err != nil || len(d.Refs) != 2 || d.Refs[0] != (refRow{"a", "sa", 1}) || d.Refs[1] != (refRow{"b", "sb", 3}) {
		t.Errorf("rows: %+v %v", d.Refs, err)
	}
	// A row short of a field is refused, not read past its end.
	for name, form := range map[string]url.Values{
		"a name alone":           {"ref_name": {"a"}},
		"a name and a subject":   {"ref_name": {"a"}, "ref_subject": {"sa"}},
		"version zero":           {"ref_name": {"a"}, "ref_subject": {"sa"}, "ref_version": {"0"}},
		"a version that is not":  {"ref_name": {"a"}, "ref_subject": {"sa"}, "ref_version": {"x"}},
		"a name with no subject": {"ref_name": {"a"}, "ref_subject": {""}, "ref_version": {"1"}},
		"a subject with no name": {"ref_name": {""}, "ref_subject": {"sa"}, "ref_version": {"1"}},
	} {
		form.Set("subject", "s")
		form.Set("schema", schema)
		noPanic(t, name, func() {
			if _, err := draftFromForm(registerFormReq(form)); err == nil {
				t.Errorf("%s: no refusal", name)
			}
		})
	}
	// Version 1 is the lowest there is.
	if d, err := draftFromForm(registerFormReq(url.Values{"schema": {schema}, "ref_name": {"a"}, "ref_subject": {"sa"}, "ref_version": {"1"}})); err != nil || len(d.Refs) != 1 {
		t.Errorf("version 1: %+v %v", d.Refs, err)
	}
}

func TestTheRegisterPageKnowsTheTopicASubjectIsNamedFor(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "orders", 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.be.CreateTopic(ctx, "other", 1, nil); err != nil {
		t.Fatal(err)
	}
	if p := c.regPage(ctx, regDraft{Subject: "orders-value"}); p.Topic != "orders" || !p.TopicOK {
		t.Errorf("orders-value: topic %q ok %v", p.Topic, p.TopicOK)
	}
	if p := c.regPage(ctx, regDraft{Subject: "other-key"}); p.Topic != "other" || !p.TopicOK {
		t.Errorf("other-key: topic %q ok %v", p.Topic, p.TopicOK)
	}
	if p := c.regPage(ctx, regDraft{Subject: "nowhere-value"}); p.Topic != "nowhere" || p.TopicOK {
		t.Errorf("a topic the broker lacks: topic %q ok %v", p.Topic, p.TopicOK)
	}
	if p := c.regPage(ctx, regDraft{Subject: "com.acme.Thing"}); p.Topic != "" || p.TopicOK {
		t.Errorf("a record-named subject: topic %q ok %v", p.Topic, p.TopicOK)
	}
}

func TestWhatARegisterSaidWhenTheVersionListsAreEmpty(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	ctx := context.Background()
	// The version lists could not be read after the call: it still registered.
	noPanic(t, "both lists empty", func() {
		if got := c.registeredSay(ctx, "s", 1, 0, nil, nil); got != "Registered v0, id 1" {
			t.Errorf("both empty: %q", got)
		}
	})
	noPanic(t, "the list after empty", func() {
		if got := c.registeredSay(ctx, "s", 1, 0, []int{1}, nil); got != "Registered v0, id 1" {
			t.Errorf("after empty: %q", got)
		}
	})
	register(t, c, "s", userV1)
	if got := c.registeredSay(ctx, "s", 1, 2, []int{1}, []int{1}); got != "Already registered as v1, id 1: nothing was created" {
		t.Errorf("the same schema again: %q", got)
	}
	// An id below the next free one, held by another subject.
	register(t, c, "t", userV1)
	if got := c.registeredSay(ctx, "t", 1, 2, []int{}, []int{1}); !strings.Contains(got, "the id this schema already has under s") {
		t.Errorf("an id another subject holds: %q", got)
	}
	// With the next id unknown nothing is said about it being reused.
	if got := c.registeredSay(ctx, "t", 1, 0, []int{}, []int{1}); got != "Registered v1, id 1" {
		t.Errorf("next id unknown: %q", got)
	}
}

func TestAChecksFindingsAreNumberedFromOne(t *testing.T) {
	c := &Console{}
	a := checkAnswer{Findings: []checkFinding{
		{Version: 1, Reasons: []schemaregistry.CompatReason{{Type: "TYPE_MISMATCH", Path: "/fields/0/type", Description: "a"}, {Type: "READER_FIELD_MISSING_DEFAULT_VALUE", Path: "/fields/1", Description: "b"}}},
		{Version: 2, Reasons: []schemaregistry.CompatReason{{Type: "NAME_MISMATCH", Description: "c"}}},
	}}
	d := regDraft{Type: schemaregistry.TypeAvro, Schema: `{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`}
	rows := c.regFindings(a, d)
	if len(rows) != 3 || rows[0].N != 1 || rows[1].N != 2 || rows[2].N != 3 {
		t.Fatalf("%+v", rows)
	}
	if rows[0].Fix != "" || rows[1].Fix != "Add default" || rows[1].FixLabel != `add "default": ""` {
		t.Errorf("fixes: %+v", rows)
	}
}

func TestTheDefaultAFieldGetsIsItsTypesZero(t *testing.T) {
	const rec = `{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`
	for path, want := range map[string]string{"/fields/0": "0", "/fields/1": `""`} {
		if got, ok := avroFieldZero(rec, path); !ok || got != want {
			t.Errorf("%s: %q %v, want %q", path, got, ok, want)
		}
	}
	for _, path := range []string{"/fields/2", "/fields/3", "/fields/-1", "/fields/x", "x"} {
		noPanic(t, path, func() {
			if got, ok := avroFieldZero(rec, path); ok {
				t.Errorf("%s: a default %q for a field that is not there", path, got)
			}
		})
	}
	if _, ok := avroFieldZero(`not json`, "/fields/0"); ok {
		t.Error("a schema that does not parse")
	}
	// An empty union has no first branch to take the zero of.
	noPanic(t, "an empty union", func() {
		if got, ok := avroZero([]byte(`[]`)); ok {
			t.Errorf("an empty union: %q", got)
		}
	})
	if got, ok := avroZero([]byte(`["null","string"]`)); !ok || got != "null" {
		t.Errorf("a union that opens with null: %q %v", got, ok)
	}
}

// A registry may refuse a register as incompatible (409) while its own check, asked
// right after, finds nothing wrong, or says why in a way the console can read. The
// refusal is still drawn, from the registry's words.
func TestARefusedRegisterIsDrawnWhetherOrNotTheCheckFindsReasons(t *testing.T) {
	armed := false
	c, _ := newProxiedRegistryConsole(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case !armed:
			return false
		case r.Method == "POST" && r.URL.Path == "/subjects/s-value/versions":
			w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"error_code":409,"message":"Schema being registered is incompatible with an earlier schema for subject \"s-value\""}`))
			return true
		case strings.HasPrefix(r.URL.Path, "/_doze/subjects/s-value/check"):
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"compatible":true,"level":"BACKWARD","compared":[1]}`))
			return true
		}
		return false
	})
	register(t, c, "s-value", userV1)
	armed = true
	noPanic(t, "a refusal with no findings", func() {
		rec := reqHX(t, c, "POST", "/_console/new/schema", url.Values{"subject": {"s-value"}, "schema": {userV2}})
		if rec.Code != 200 || !strings.Contains(rec.Body.String(), "incompatible with an earlier schema") {
			t.Errorf("no findings: %d %s", rec.Code, rec.Body)
		}
	})
}

// A real refusal is drawn with the reasons the registry's check takes apart.
func TestARefusedRegisterListsTheReasons(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "s-value", userV1)
	const needsDefault = `{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"email","type":"string"}]}`
	rec := reqHX(t, c, "POST", "/_console/new/schema", url.Values{"subject": {"s-value"}, "schema": {needsDefault}})
	body := readable(rec.Body.String())
	if rec.Code != 200 || !strings.Contains(body, "READER_FIELD_MISSING_DEFAULT_VALUE") || !strings.Contains(body, "/fields/1") || !strings.Contains(body, "Add default") {
		t.Errorf("%d %s", rec.Code, body)
	}
}

func TestTheRegisterPageShowsAResolvedViewOnlyForADraftWithReferences(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "s-value", userV1)
	page := req(t, c, "GET", "/_console/new/schema?subject=s-value", nil).Body.String()
	if !strings.Contains(page, "Register a new version of s-value") || strings.Contains(page, "Resolved view") {
		t.Errorf("a subject with no references: %s", readable(page))
	}
}
