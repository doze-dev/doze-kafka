package schemaregistry

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Every route the registry serves has a row, and a row for a route it does not
// serve is S with a reason; docs/api-support.md's table is generated from it.
// Regenerate the table: UPDATE_API_DOCS=1 go test ./schemaregistry -run TestEveryRegistryRouteHasALedgerRow
func TestEveryRegistryRouteHasALedgerRow(t *testing.T) {
	reg, err := Open(NewMemoryStorage())
	if err != nil {
		t.Fatal(err)
	}
	a := NewAPI(reg, Options{})
	rows := map[string]ledgerRow{}
	for _, r := range routeLedger {
		if _, dup := rows[r.Route]; dup {
			t.Errorf("%s has two rows", r.Route)
		}
		rows[r.Route] = r
		switch {
		case r.Tier == "S" && slices.Contains(a.patterns, r.Route):
			t.Errorf("%s is served and its row says S", r.Route)
		case r.Tier == "S" && r.Note == "":
			t.Errorf("%s is S and does not say why", r.Route)
		case r.Tier != "S" && !slices.Contains(a.patterns, r.Route):
			t.Errorf("%s has a %s row and is not served", r.Route, r.Tier)
		case r.Tier == "X" && !strings.Contains(r.Route, "/_doze/"):
			t.Errorf("%s is X and is not under /_doze/", r.Route)
		case r.Tier == "F" && strings.Contains(r.Route, "/_doze/"):
			t.Errorf("%s is under /_doze/, which is not a Confluent path: X", r.Route)
		}
	}
	for _, p := range a.patterns {
		if _, ok := rows[p]; !ok {
			t.Errorf("the registry serves %s and docs/api-support.md has no row for it: add one to routeLedger", p)
		}
	}

	path := filepath.Join("..", "docs", "api-support.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	const begin, end = "<!-- registry-routes:begin (generated from schemaregistry/ledger.go; do not edit by hand) -->\n", "<!-- registry-routes:end -->"
	i, j := strings.Index(doc, begin), strings.Index(doc, end)
	if i < 0 || j < i {
		t.Fatalf("%s has no generated table between the registry-routes markers", path)
	}
	var b strings.Builder
	b.WriteString("| Route | Tier | Note |\n|---|---|---|\n")
	for _, r := range routeLedger {
		b.WriteString("| `" + r.Route + "` | " + r.Tier + " | " + r.Note + " |\n")
	}
	if got := doc[i+len(begin) : j]; got != b.String() {
		if os.Getenv("UPDATE_API_DOCS") != "" {
			if err := os.WriteFile(path, []byte(doc[:i+len(begin)]+b.String()+doc[j:]), 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
		t.Errorf("docs/api-support.md's registry table is out of date. Regenerate it: UPDATE_API_DOCS=1 go test ./schemaregistry -run TestEveryRegistryRouteHasALedgerRow")
	}
}
