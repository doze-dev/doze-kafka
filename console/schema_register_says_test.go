package console

import (
	"net/url"
	"testing"
)

// SCH-33: what a register did is what it says. A schema the subject already holds
// creates nothing; a schema the registry holds under another subject is a new version
// with the id it already has; only a schema never seen takes the next free id.
func TestRegisterSaysWhatItCreated(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	flash := func(subject, schema string) string {
		t.Helper()
		rec := req(t, c, "POST", "/_console/new/schema", url.Values{"subject": {subject}, "schema": {schema}})
		if rec.Code != 303 {
			t.Fatalf("register %s: %d %s", subject, rec.Code, rec.Body)
		}
		u, _ := url.Parse(rec.Header().Get("Location"))
		return u.Query().Get("flash")
	}
	for _, step := range []struct{ subject, schema, want string }{
		{"orders-value", userV1, "Registered v1, id 1"},
		{"orders-value", userV1, "Already registered as v1, id 1: nothing was created"},
		{"audit-value", userV1, "Registered v1 with id 1, the id this schema already has under orders-value"},
		{"orders-value", userV2, "Registered v2, id 2"},
	} {
		if got := flash(step.subject, step.schema); got != step.want {
			t.Errorf("%s: %q, want %q", step.subject, got, step.want)
		}
	}
	// A soft-deleted version's schema registered again in its own subject keeps its id.
	if err := c.reg.SoftDeleteVersion(t.Context(), "orders-value", 2); err != nil {
		t.Fatal(err)
	}
	if got := flash("orders-value", userV2); got != "Registered v3 with id 2, the id this schema had in a deleted version" {
		t.Errorf("again after a soft delete: %q", got)
	}
}
