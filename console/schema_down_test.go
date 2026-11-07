package console

import (
	"net/url"
	"strings"
	"testing"
)

// The Schemas pages with the registry gone: every page answers with a page that says
// the registry is not answering, never a 500 or a blank, and nothing is sent; and on
// an empty registry each says there is nothing yet. (Stage 5 acceptance: the pages
// render with the registry down and with an empty data directory.)
func TestSchemasPagesWithTheRegistryDownOrEmpty(t *testing.T) {
	c, _, ts := newRegistryConsole(t)
	pages := []string{"/_console/schemas", "/_console/schemas/orders-value", "/_console/schemas/orders-value?tab=compat",
		"/_console/schemas/orders-value?tab=used", "/_console/schemas/orders-value?tab=raw", "/_console/schemas/orders-value/diff?from=1&to=2",
		"/_console/new/schema", "/_console/new/schema?subject=orders-value"}

	// Empty: nothing registered.
	if b := readable(req(t, c, "GET", "/_console/schemas", nil).Body.String()); !strings.Contains(b, "No subjects") {
		t.Errorf("the empty registry's home: %s", between(b, "Schemas", "Rule"))
	}
	if rec := req(t, c, "GET", "/_console/new/schema", nil); rec.Code != 200 {
		t.Errorf("the register page of an empty registry = %d", rec.Code)
	}

	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	ts.Close()
	for _, p := range pages {
		rec := req(t, c, "GET", p, nil)
		body := readable(rec.Body.String())
		if rec.Code >= 500 && rec.Code != 502 && rec.Code != 503 || rec.Code == 200 && !strings.Contains(body, "Schemas") {
			t.Errorf("%s with the registry down = %d", p, rec.Code)
		}
		if rec.Code != 200 && (!strings.Contains(body, "Can't reach the schema registry") || strings.Contains(body, "Can't reach the broker") || !strings.Contains(body, "The broker is not affected")) {
			t.Errorf("%s with the registry down (%d) does not say it is the registry: %.300s", p, rec.Code, strings.Join(strings.Fields(body), " "))
		}
	}
	for _, p := range []string{"/_console/new/schema/check", "/_console/schemas/orders-value/level-check", "/_console/schemas/orders-value/try"} {
		if rec := req(t, c, "POST", p, url.Values{"subject": {"orders-value"}, "schema": {userV2}, "level": {"FULL"}, "doc": {`{"id":1}`}}); rec.Code == 500 || rec.Code == 200 && rec.Body.Len() == 0 {
			t.Errorf("POST %s with the registry down = %d", p, rec.Code)
		}
	}
}
