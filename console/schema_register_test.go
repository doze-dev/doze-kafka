package console

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// The register page (Sc-02 to Sc-04, Sc-10) against a real registry.
func TestRegisterPage(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	htmx := func(path string, form url.Values) (int, string, http.Header) {
		rec := reqHX(t, c, "POST", path, form)
		return rec.Code, rec.Body.String(), rec.Header()
	}

	// Sc-02: a new subject. The list's first row is the subject not saved yet; nothing
	// is checked; v1 gets the next free id.
	page := readable(req(t, c, "GET", "/_console/new/schema", nil).Body.String())
	for _, want := range []string{"Register schema", "New subject", "not saved yet", "Not checked yet", "so Register will create v1", "next free id (1)", "None. A reference names"} {
		if !strings.Contains(page, want) {
			t.Errorf("Sc-02 is missing %q", want)
		}
	}
	code, body, _ := htmx("/_console/new/schema/check", url.Values{"subject": {"orders-value"}, "schema": {userV1}})
	if code != 200 || !strings.Contains(readable(body), "Parses") || !strings.Contains(readable(body), "Registering creates v1") {
		t.Errorf("checking a new subject: %d %s", code, readable(body))
	}
	if _, body, _ := htmx("/_console/new/schema/check", url.Values{"subject": {"orders-value"}, "schema": {"{nope"}}); !strings.Contains(body, "42201") {
		t.Errorf("a draft that does not parse: %s", readable(body))
	}
	if code, _, h := htmx("/_console/new/schema", url.Values{"subject": {"orders-value"}, "schema": {userV1}}); code != 200 || h.Get("HX-Redirect") == "" {
		t.Fatalf("register v1: %d %v", code, h)
	}

	// Sc-03: the next version starts from the latest; a compatible change says so.
	page = readable(req(t, c, "GET", "/_console/new/schema?subject=orders-value", nil).Body.String())
	for _, want := range []string{"Register a new version", "latest is v1, id 1", "level BACKWARD (global)", "Register v2", `"name": "id"`, "Check compares this with v1 at BACKWARD"} {
		if !strings.Contains(page, want) {
			t.Errorf("Sc-03 is missing %q", want)
		}
	}
	_, body, _ = htmx("/_console/new/schema/check", url.Values{"subject": {"orders-value"}, "schema": {userV2}})
	for _, want := range []string{"Compatible", "compatible with v1 (BACKWARD)", "POST /compatibility/subjects/orders-value/versions/latest", "Nothing is stored yet", "Check again", "Registering creates v2"} {
		if !strings.Contains(readable(body), want) {
			t.Errorf("the compatible answer is missing %q:\n%s", want, readable(body))
		}
	}

	// A field with no default: the check says not compatible, with the finding and its fix.
	breaking := `{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"channel","type":"string"}]}`
	_, body, _ = htmx("/_console/new/schema/check", url.Values{"subject": {"orders-value"}, "schema": {breaking}})
	for _, want := range []string{"Not compatible", "READER_FIELD_MISSING_DEFAULT_VALUE", "/fields/1", "Add default", `Fix: add "default": ""`} {
		if !strings.Contains(readable(body), want) {
			t.Errorf("the refused check is missing %q:\n%s", want, readable(body))
		}
	}
	// Sc-04: registering it anyway is refused, drawn where the answer goes, nothing stored.
	code, body, _ = htmx("/_console/new/schema", url.Values{"subject": {"orders-value"}, "schema": {breaking}})
	for _, want := range []string{"Refused", "409 Conflict", "error_code 409", `"error_code":409`, "READER_FIELD_MISSING_DEFAULT_VALUE", "Nothing was stored", "relax this subject to NONE"} {
		if code != 200 || !strings.Contains(readable(body), want) {
			t.Errorf("the refused register (%d) is missing %q:\n%s", code, want, readable(body))
		}
	}
	if vs, _ := c.reg.Versions(t.Context(), "orders-value"); len(vs) != 1 {
		t.Errorf("a refused register stored something: %v", vs)
	}
	// Without htmx the refusal is the foundation's.
	if rec := req(t, c, "POST", "/_console/new/schema", url.Values{"subject": {"orders-value"}, "schema": {breaking}}); rec.Code != 400 || !strings.Contains(rec.Body.String(), "409") {
		t.Errorf("a plain refused register: %d %s", rec.Code, readable(rec.Body.String()))
	}
	// The fix gives the field a default of its type's zero, and the draft then passes.
	_, body, _ = htmx("/_console/new/schema/fix", url.Values{"subject": {"orders-value"}, "schema": {breaking}, "path": {"/fields/1"}})
	if !strings.Contains(body, `&#34;default&#34;: &#34;&#34;`) {
		t.Errorf("the fix: %s", body)
	}
	fixed, _ := addAvroDefault(breaking, "/fields/1")
	if _, body, _ := htmx("/_console/new/schema/check", url.Values{"subject": {"orders-value"}, "schema": {fixed}}); !strings.Contains(readable(body), "Compatible") {
		t.Errorf("the fixed draft: %s", readable(body))
	}

	// Sc-10: a draft with a reference lists it and shows the resolved view.
	key := `{"type":"record","name":"OrderKey","namespace":"h","fields":[{"name":"orderId","type":"string"}]}`
	register(t, c, "orders-key", key)
	withRef := `{"type":"record","name":"Order","namespace":"h","fields":[{"name":"key","type":["null","h.OrderKey"],"default":null}]}`
	form := url.Values{"subject": {"refs-value"}, "schema": {withRef}, "ref_name": {"h.OrderKey", ""}, "ref_subject": {"orders-key", ""}, "ref_version": {"1", ""}}
	if _, body, _ := htmx("/_console/new/schema/check", form); !strings.Contains(readable(body), "Parses") {
		t.Errorf("a draft with a reference: %s", readable(body))
	}
	if code, _, _ := htmx("/_console/new/schema", form); code != 200 {
		t.Fatalf("register with a reference: %d", code)
	}
	page = readable(req(t, c, "GET", "/_console/new/schema?subject=refs-value", nil).Body.String())
	for _, want := range []string{"Name used in the schema", "h.OrderKey", "orders-key", "Resolved view", "orderId", "error 42206"} {
		if !strings.Contains(page, want) {
			t.Errorf("Sc-10 is missing %q", want)
		}
	}
	// A reference row half filled in is refused before anything is sent.
	if code, _, _ := htmx("/_console/new/schema/check", url.Values{"subject": {"x"}, "schema": {userV1}, "ref_name": {"a"}, "ref_subject": {""}, "ref_version": {"1"}}); code != 400 {
		t.Errorf("half a reference: %d", code)
	}
}

func TestAddAvroDefault(t *testing.T) {
	rec := `{"type":"record","name":"R","fields":[{"name":"s","type":"string"},{"name":"n","type":["null","int"]},{"name":"l","type":"long"},{"name":"b","type":"boolean"},{"name":"a","type":{"type":"array","items":"int"}},{"name":"m","type":{"type":"map","values":"int"}},{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}},{"name":"x","type":"bytes"},{"name":"z","type":"null"},{"name":"r","type":{"type":"record","name":"Q","fields":[]}}]}`
	for path, want := range map[string]string{
		"/fields/0": `"default": ""`, "/fields/1": `"default": null`, "/fields/2": `"default": 0`, "/fields/3": `"default": false`,
		"/fields/4": `"default": []`, "/fields/5": `"default": {}`, "/fields/7": `"default": ""`, "/fields/8": `"default": null`,
	} {
		got, ok := addAvroDefault(rec, path)
		if !ok || !strings.Contains(got, want) {
			t.Errorf("%s: %v\n%s", path, ok, got)
		}
	}
	for _, path := range []string{"/fields/6", "/fields/9", "/fields/10", "/fields/-1", "/other/0", "/fields/x"} {
		if _, ok := addAvroDefault(rec, path); ok {
			t.Errorf("%s has no default to add", path)
		}
	}
	if _, ok := addAvroDefault("{nope", "/fields/0"); ok {
		t.Error("a schema that does not parse")
	}
	if _, ok := addAvroDefault(`{"type":"record","fields":7}`, "/fields/0"); ok {
		t.Error("fields that are not a list")
	}
	for _, level := range []string{"BACKWARD", "FORWARD_TRANSITIVE", "FULL", "NONE"} {
		if levelRule(level) == "" {
			t.Error(level)
		}
	}
	for _, typ := range []string{"AVRO", "PROTOBUF", "JSON"} {
		if compatibleWhy(typ) == "" {
			t.Error(typ)
		}
	}
}

// reqHX is req as htmx sends it.
func reqHX(t *testing.T, c *Console, method, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("HX-Request", "true")
	r.Host = "127.0.0.1:9093"
	rec := httptest.NewRecorder()
	serveInto(t, c, rec, r)
	return rec
}
