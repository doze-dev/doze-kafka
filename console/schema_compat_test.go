package console

import (
	"encoding/json"
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

// Sc-16: a subject the registry does not have, close names offered, a soft-deleted
// one said to be so, and the way to register it.
func TestSubjectNotFoundPage(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-key", userV1)
	rec := req(t, c, "GET", "/_console/schemas/order-value", nil)
	page := readable(rec.Body.String())
	for _, want := range []string{"No subject called order-value. The registry has 2 others.", "Open orders-value", "Not found", "40401", "HTTP 404",
		"Did you mean", "orders-value", "one letter short of orders", "Was it deleted?", "GET /subjects?deleted=true", "Register order-value", "open closest"} {
		if rec.Code != 404 || !strings.Contains(page, want) {
			t.Errorf("Sc-16 (%d) is missing %q", rec.Code, want)
		}
	}
	if !strings.Contains(rec.Body.String(), `href="/_console/new/schema?subject=order-value"`) {
		t.Error("no way to register it")
	}
	// A soft-deleted subject is said to be one.
	register(t, c, "gone-value", userV1)
	if err := c.reg.SoftDeleteSubject(t.Context(), "gone-value"); err != nil {
		t.Fatal(err)
	}
	if b := readable(req(t, c, "GET", "/_console/schemas/gone-value", nil).Body.String()); !strings.Contains(b, "gone-value is soft-deleted") {
		t.Errorf("a soft-deleted subject: %s", between(b, "Was it deleted?", "Or is it"))
	}
	// Nothing close: Register is the band's button.
	if b := readable(req(t, c, "GET", "/_console/schemas/zzzzzzzzzzzzzz", nil).Body.String()); strings.Contains(b, "Did you mean") || !strings.Contains(b, "Register zzzzzzzzzzzzzz") {
		t.Errorf("nothing close: %s", between(b, "Schemas", "Rule"))
	}
	for asked, want := range map[string]string{"orders": `"orders" is part of its name.`, "orders-value-x": `its name is part of "orders-value-x".`, "orders-valu": "one letter short of value.", "orders-valuee": "one letter more than value.", "ordrs-valeu": "3 letters differ.", "orders-valve": "one letter differs from value.", "order-value": "one letter short of orders."} {
		if got := closeWhy(asked, "orders-value"); got != want {
			t.Errorf("%s: %q, want %q", asked, got, want)
		}
	}
}

// SCH-32 (Sc-14): with no registry the rail keeps Schemas, marked off, and the page
// says why (switched off, or its records could not be read), with the command that
// turns it on, where it would listen, and what still works.
func TestRailKeepsSchemasWhenOff(t *testing.T) {
	for _, reason := range []string{"", "meta.db: a registry record does not parse"} {
		c, _ := newTestConsoleAddr(t, func(o *Options) {
			o.RegistryOffReason = reason
			o.Facts.DataDir = "/var/lib/doze kafka"
		})
		rec := req(t, c, "GET", "/_console/schemas", nil)
		page := readable(rec.Body.String())
		for _, want := range []string{"No registry to list", "Copy the command to turn it on", "Turn it on", "Where it listens when it is on", "What still works without it",
			"doze-kafka --data-dir '/var/lib/doze kafka' --schema-registry", ":8081", "copy command"} {
			if rec.Code != 200 || !strings.Contains(page, want) {
				t.Errorf("%q: Sc-14 (%d) is missing %q", reason, rec.Code, want)
			}
		}
		if reason == "" && (!strings.Contains(page, "switched off") || !strings.Contains(page, "--schema-registry=false")) {
			t.Error("switched off: not said")
		}
		if reason != "" && (!strings.Contains(page, "did not start") || !strings.Contains(page, reason)) {
			t.Errorf("did not start: %s", between(page, "Schemas", "Turn it on"))
		}
		if !strings.Contains(between(rec.Body.String(), `<nav class="rail"`, "</nav>"), `data-ct="schemas-off"`) {
			t.Error("the rail item is not marked off")
		}
	}
	if got := registryExample("broker.example:9092"); got != "broker.example:8081" {
		t.Error(got)
	}
	if got := registryExample("nonsense"); got != "host:8081" {
		t.Error(got)
	}
}

// Sc-11 and Sc-17: which of the subject's ids the topic's messages carry, and the
// messages that carry an id the registry does not have.
func TestUsedByTab(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	topicWith(t, c, "orders", 1, 0)
	register(t, c, "orders-value", userV1)
	produce := func(form url.Values) {
		t.Helper()
		if rec := req(t, c, "POST", "/_console/topics/orders/produce", form); rec.Code >= 400 {
			t.Fatalf("produce: %d %s", rec.Code, rec.Body)
		}
	}
	for i := 0; i < 3; i++ {
		produce(url.Values{"key": {"a"}, "value": {`{"id": 1}`}, "schema_subject": {"orders-value"}})
	}
	register(t, c, "orders-value", userV2)
	produce(url.Values{"key": {"b"}, "value": {`{"id": 2, "email": null}`}, "schema_subject": {"orders-value"}})
	// An id nothing registered: magic byte 0, id 99, then bytes.
	produce(url.Values{"key": {"orphan"}, "value": {"AAAAAGMCAgI="}, "value_enc": {"base64"}})

	page := readable(req(t, c, "GET", "/_console/schemas/orders-value?tab=used", nil).Body.String())
	for _, want := range []string{"ids 1, 2", "1 message carries id 99, unknown here", "4 of 5 decoded", "Shown raw, not decoded", "schema id 99",
		"This registry has ids 1 to 2 only", "Messages that could not be decoded", "orphan", "message, raw", "Bring the id back"} {
		if !strings.Contains(page, want) {
			t.Errorf("Sc-17 is missing %q:\n%s", want, between(page, "Used by", "The wire"))
		}
	}
	// Without the orphan the tab is Sc-11's: each id, its share and where to open it.
	c2, _, _ := newRegistryConsole(t)
	topicWith(t, c2, "orders", 1, 0)
	register(t, c2, "orders-value", userV1)
	for i := 0; i < 3; i++ {
		req(t, c2, "POST", "/_console/topics/orders/produce", url.Values{"key": {"a"}, "value": {`{"id": 1}`}, "schema_subject": {"orders-value"}})
	}
	register(t, c2, "orders-value", userV2)
	req(t, c2, "POST", "/_console/topics/orders/produce", url.Values{"key": {"b"}, "value": {`{"id": 2, "email": null}`}, "schema_subject": {"orders-value"}})
	page = readable(req(t, c2, "GET", "/_console/schemas/orders-value?tab=used", nil).Body.String())
	for _, want := range []string{"Which schema id the messages carry", "2 · v2", "25.0%", "1 · v1", "75.0%", "newest on this id", "offsets 0 to 2", "Why 3 are still on v1", "Open on orders"} {
		if !strings.Contains(page, want) {
			t.Errorf("Sc-11 is missing %q:\n%s", want, between(page, "Used by", "The wire"))
		}
	}
	// Try it: the newest message on an id, or a refusal.
	if rec := req(t, c2, "GET", "/_console/schemas/orders-value/open-id?id=1", nil); rec.Code != 303 || !strings.Contains(rec.Header().Get("Location"), "/topics/orders/message?partition=0&offset=2") {
		t.Errorf("open id 1: %d %s", rec.Code, rec.Header().Get("Location"))
	}
	if rec := req(t, c2, "GET", "/_console/schemas/orders-value/open-id?id=55", nil); rec.Code != 404 {
		t.Errorf("an id no message carries: %d", rec.Code)
	}
	for in, want := range map[string]string{"[[1,8],[10,10]]": "ids 1 to 8 and 10", "[[3,3]]": "id 3", "[]": "no ids", "[[1,2],[4,4],[6,9]]": "ids 1 to 2, 4 and 6 to 9"} {
		var rs [][2]int
		if err := json.Unmarshal([]byte(in), &rs); err != nil {
			t.Fatal(err)
		}
		if got := idRangesText(rs); got != want {
			t.Errorf("%s: %q, want %q", in, got, want)
		}
	}
}
