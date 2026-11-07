package console

import (
	"context"
	"net/url"
	"strings"
	"testing"
)

func TestSchemaActionsRedirectsAndFlashes(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	post := func(path string, form url.Values) (int, string) {
		rec := req(t, c, "POST", path, form)
		return rec.Code, rec.Header().Get("Location")
	}
	if code, l := post("/_console/new/schema", url.Values{"subject": {" orders-value "}, "schema": {userV1}}); code != 303 || l != "/_console/schemas/orders-value?flash=Schema+registered" {
		t.Errorf("register: %d %s", code, l)
	}
	if code, l := post("/_console/new/schema", url.Values{"subject": {"orders-value"}, "schema": {userV2}}); code != 303 || l != "/_console/schemas/orders-value?flash=Schema+registered" {
		t.Errorf("register a version: %d %s", code, l)
	}
	if code, _ := post("/_console/new/schema", url.Values{"subject": {"  "}, "schema": {userV1}}); code != 400 {
		t.Errorf("no subject: %d", code)
	}
	if code, _ := post("/_console/new/schema", url.Values{"subject": {"x"}, "schema": {" "}}); code != 400 {
		t.Errorf("no schema: %d", code)
	}
	if code, _ := post("/_console/new/schema", url.Values{"subject": {"x"}, "schema": {"{not avro"}}); code != 400 {
		t.Errorf("a schema that does not parse: %d", code)
	}

	// Compatibility: set, then back to the registry's default (also when none was set).
	if code, l := post("/_console/schemas/orders-value/config", url.Values{"level": {"FULL"}}); code != 303 || !strings.HasSuffix(l, "?tab=compat&flash=Compatibility+updated") {
		t.Errorf("set: %d %s", code, l)
	}
	if lvl, own, _ := c.reg.Compat(context.Background(), "orders-value"); lvl != "FULL" || !own {
		t.Errorf("level %s own %v", lvl, own)
	}
	for _, level := range []string{"default", ""} {
		if code, _ := post("/_console/schemas/orders-value/config", url.Values{"level": {level}}); code != 303 {
			t.Errorf("clear with %q: %d", level, code)
		}
		if _, own, _ := c.reg.Compat(context.Background(), "orders-value"); own {
			t.Errorf("level %q left the subject's own level in place", level)
		}
	}
	if code, _ := post("/_console/schemas/orders-value/config", url.Values{"level": {"SIDEWAYS"}}); code != 400 {
		t.Errorf("an unknown level: %d", code)
	}

	// Deleting a version, the last version, and the subject.
	if code, l := post("/_console/schemas/orders-value/delete", url.Values{"version": {"x"}}); code != 400 {
		t.Errorf("a version that is not a number: %d %s", code, l)
	}
	if code, l := post("/_console/schemas/orders-value/delete", url.Values{"version": {"1"}}); code != 303 || l != "/_console/schemas/orders-value?flash=Version+1+soft-deleted%3B+its+id+still+resolves" {
		t.Errorf("one of two: %d %s", code, l)
	}
	if code, l := post("/_console/schemas/orders-value/delete", url.Values{"version": {"2"}}); code != 303 || l != "/_console/schemas?flash=Version+2+soft-deleted%3A+it+was+the+last+one%2C+so+the+subject+is+listed+under+Soft-deleted" {
		t.Errorf("the last: %d %s", code, l)
	}
	register(t, c, "kept-value", userV1)
	// For good: only with its name typed, then two calls (soft, permanent).
	if code, _ := post("/_console/schemas/kept-value/delete", nil); code != 400 {
		t.Errorf("a subject deleted without its name typed: %d", code)
	}
	if code, l := post("/_console/schemas/kept-value/delete", url.Values{"confirm": {"kept-value"}}); code != 303 || l != "/_console/schemas?flash=Subject+deleted+for+good" {
		t.Errorf("the subject: %d %s", code, l)
	}
	if got := c.reg.SubjectNames(context.Background()); len(got) != 0 {
		t.Errorf("subjects left: %v", got)
	}
	// orders-value was soft-deleted with its last version (SCH-17): the permanent call alone.
	if d, _ := c.reg.DeletedSubjects(context.Background()); len(d) != 1 || d[0] != "orders-value" {
		t.Errorf("soft-deleted: %v", d)
	}
	if code, _ := post("/_console/schemas/orders-value/delete", url.Values{"confirm": {"orders-value"}}); code != 303 {
		t.Errorf("a soft-deleted subject for good: %d", code)
	}
	if d, _ := c.reg.DeletedSubjects(context.Background()); len(d) != 0 {
		t.Errorf("still held: %v", d)
	}
	if code, _ := post("/_console/schemas/nobody/delete", url.Values{"confirm": {"nobody"}}); code != 404 {
		t.Errorf("a subject that is not there: %d", code)
	}
}

// Asking whether a schema may follow the versions of a subject.
func TestSchemaCheckAnswers(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	check := func(schema string) string {
		rec := req(t, c, "POST", "/_console/new/schema/check", url.Values{"subject": {"orders-value"}, "schema": {schema}})
		if rec.Code != 200 {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
		return readable(rec.Body.String())
	}
	if got := check(userV2); !strings.Contains(got, "BACKWARD") || strings.Contains(strings.ToLower(got), "not compatible") {
		t.Errorf("a compatible schema:\n%s", got)
	}
	incompatible := `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`
	if got := check(incompatible); !strings.Contains(strings.ToLower(got), "not compatible") && !strings.Contains(strings.ToLower(got), "incompatible") {
		t.Errorf("an incompatible schema:\n%s", got)
	}
	if got := check("{broken"); got == "" {
		t.Error("a schema that does not parse is an answer, not a blank")
	}
	if rec := req(t, c, "POST", "/_console/new/schema/check", url.Values{"subject": {"orders-value"}, "schema": {" "}}); rec.Code != 400 {
		t.Errorf("no schema: %d", rec.Code)
	}
	if rec := req(t, c, "GET", "/_console/new/schema?subject=a+b", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "a b") {
		t.Errorf("the register page prefilled: %d", rec.Code)
	}
}
