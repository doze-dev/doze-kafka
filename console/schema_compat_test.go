package console

import (
	"net/url"
	"strings"
	"testing"
)

// Sc-09 against a real registry: the levels, the scope, the mode and the check at
// a level, which saves nothing.
func TestCompatibilityTab(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	page := readable(req(t, c, "GET", "/_console/schemas/orders-value?tab=compat", nil).Body.String())
	for _, want := range []string{"Save level", "level BACKWARD, the global default", "global default BACKWARD", "mode READWRITE",
		"NONE", "BACKWARD now", "FULL_TRANSITIVE", "Both, against every earlier version.", "This subject", "Global default", "IMPORT is not supported here", "Test v2 against this level", `{"compatibility":"BACKWARD"}`, "choose level"} {
		if !strings.Contains(page, want) {
			t.Errorf("Sc-09 is missing %q", want)
		}
	}
	// The check at a level saves nothing; one that passes and one that would fail.
	if b := readable(reqHX(t, c, "POST", "/_console/schemas/orders-value/level-check", url.Values{"level": {"FULL_TRANSITIVE"}}).Body.String()); !strings.Contains(b, "Passes") || !strings.Contains(b, "v2 against v1 at FULL_TRANSITIVE") {
		t.Errorf("a level that passes: %s", b)
	}
	breaking := `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`
	if err := c.reg.SetCompat(t.Context(), "orders-value", "NONE"); err != nil {
		t.Fatal(err)
	}
	register(t, c, "orders-value", breaking)
	if b := readable(reqHX(t, c, "POST", "/_console/schemas/orders-value/level-check", url.Values{"level": {"BACKWARD"}}).Body.String()); !strings.Contains(b, "Would fail") || !strings.Contains(b, "against v2") {
		t.Errorf("a level that would fail: %s", b)
	}
	if lvl, own, _ := c.reg.Compat(t.Context(), "orders-value"); lvl != "NONE" || !own {
		t.Errorf("a check at a level changed the level: %s %v", lvl, own)
	}
	register(t, c, "alone-value", userV1)
	if b := readable(reqHX(t, c, "POST", "/_console/schemas/alone-value/level-check", url.Values{"level": {"FULL"}}).Body.String()); !strings.Contains(b, "the only version") {
		t.Errorf("one version: %s", b)
	}

	// Its own level shows, and Remove it hands it back.
	page = readable(req(t, c, "GET", "/_console/schemas/orders-value?tab=compat", nil).Body.String())
	if !strings.Contains(page, "level NONE, set on this subject") || !strings.Contains(page, "Remove it to follow the global level (BACKWARD) again") {
		t.Errorf("an own level: %s", between(page, "versions", "Compatibility"))
	}
	// Remove it, sent beside the checked level and the mode switch as the form sends it.
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/config", url.Values{"level": {"NONE", "default"}, "scope": {"subject"}, "mode": {"READWRITE"}}); rec.Code != 303 {
		t.Fatal(rec.Code)
	}
	if _, own, _ := c.reg.SubjectMode(t.Context(), "orders-value"); own {
		t.Error("saving gave the subject a mode of its own")
	}
	if _, own, _ := c.reg.Compat(t.Context(), "orders-value"); own {
		t.Error("Remove it left the subject's own level")
	}
	// The global default, and the mode with it.
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/config", url.Values{"level": {"FULL"}, "scope": {"global"}, "mode": {"READWRITE"}}); rec.Code != 303 {
		t.Fatal(rec.Code)
	}
	if g, _ := c.reg.GlobalCompat(t.Context()); g != "FULL" {
		t.Errorf("global level %s", g)
	}
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/config", url.Values{"level": {"BACKWARD"}, "scope": {"subject"}, "mode": {"READONLY"}}); rec.Code != 303 {
		t.Fatal(rec.Code)
	}
	if m, own, _ := c.reg.SubjectMode(t.Context(), "orders-value"); m != "READONLY" || !own {
		t.Errorf("mode %s %v", m, own)
	}

	// Sc-15: read-only. The page says so; a check warns; a register is refused with 42205.
	page = readable(req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String())
	if !strings.Contains(page, "READONLY") || !strings.Contains(page, "mode READONLY, set on this subject at") {
		t.Errorf("a read-only subject's page: %s", between(page, "orders-value", "Neighbours"))
	}
	reg := readable(req(t, c, "GET", "/_console/new/schema?subject=orders-value", nil).Body.String())
	if !strings.Contains(reg, "This subject is read-only") {
		t.Error("the register page does not warn of READONLY")
	}
	if b := readable(reqHX(t, c, "POST", "/_console/new/schema/check", url.Values{"subject": {"orders-value"}, "schema": {breaking}}).Body.String()); !strings.Contains(b, "Register will be refused with 42205") {
		t.Errorf("the check does not warn of READONLY: %s", b)
	}
	// A schema the subject does not hold (one it holds is answered with its id, READONLY or not).
	b := readable(reqHX(t, c, "POST", "/_console/new/schema", url.Values{"subject": {"orders-value"}, "schema": {`{"type":"record","name":"User","fields":[{"name":"id","type":"string"},{"name":"x","type":["null","int"],"default":null}]}`}}).Body.String())
	for _, want := range []string{"Refused", "42205", "HTTP 422", "read-only", "Set READWRITE for this subject", "Why would it be read-only"} {
		if !strings.Contains(b, want) {
			t.Errorf("Sc-15's refusal is missing %q:\n%s", want, b)
		}
	}
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/mode", url.Values{"mode": {"READWRITE"}}); rec.Code != 303 {
		t.Fatal(rec.Code)
	}
	if m, _, _ := c.reg.SubjectMode(t.Context(), "orders-value"); m != "READWRITE" {
		t.Errorf("mode after Set READWRITE: %s", m)
	}
	// The registry's mode from the tab, and the subject's own dropped.
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/mode", url.Values{"mode": {"default"}}); rec.Code != 303 {
		t.Fatal(rec.Code)
	}
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/mode", url.Values{"mode": {"READWRITE"}, "scope": {"global"}}); rec.Code != 303 {
		t.Fatal(rec.Code)
	}
}

// SCH-17: a subject a client soft-deleted is listed under Soft-deleted on the home,
// and is deleted for good with the permanent call alone.
func TestSoftDeletedSubjectIsListedAndRemovable(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "kept-value", userV1)
	register(t, c, "gone-value", userV1)
	register(t, c, "gone-value", userV2)
	if err := c.reg.SoftDeleteSubject(t.Context(), "gone-value"); err != nil {
		t.Fatal(err)
	}
	home := req(t, c, "GET", "/_console/schemas", nil).Body.String()
	section := between(home, `id="list-deleted"`, "</aside>")
	if !strings.Contains(section, "Soft-deleted") || !strings.Contains(section, "gone-value") || strings.Contains(section, "kept-value") || !strings.Contains(section, "Delete for good") {
		t.Errorf("the soft-deleted section: %s", readable(section))
	}
	_, got := confirmOf(t, c, "schema.delete?subject=gone-value")
	if !strings.Contains(got["text"].(string), "A client soft-deleted it already") || !strings.Contains(got["text"].(string), "All 2 versions") || !strings.Contains(got["text"].(string), "Ids 1 and 2") {
		t.Errorf("its confirm: %v", got["text"])
	}
	if rec := req(t, c, "POST", "/_console/schemas/gone-value/delete", url.Values{"confirm": {"gone-value"}}); rec.Code != 303 {
		t.Fatalf("delete for good: %d %s", rec.Code, rec.Body)
	}
	if d, _ := c.reg.DeletedSubjects(t.Context()); len(d) != 0 {
		t.Errorf("still held: %v", d)
	}
	if strings.Contains(req(t, c, "GET", "/_console/schemas", nil).Body.String(), `id="list-deleted"`) {
		t.Error("the section stays with nothing in it")
	}
}
