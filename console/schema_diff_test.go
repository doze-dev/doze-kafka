package console

import (
	"strings"
	"testing"
)

// Removed lines go beside the added lines that follow them, as changed lines; what
// is left over on either side is added or removed.
func TestPairDiff(t *testing.T) {
	rows, added, removed, changed := pairDiff(diffLines("a\nb\nc\nd", "a\nB\nc\nd\ne\nf"))
	if added != 2 || removed != 0 || changed != 1 || len(rows) != 6 {
		t.Fatalf("added %d removed %d changed %d rows %d", added, removed, changed, len(rows))
	}
	if r := rows[1]; !r.Changed || r.Old != "b" || r.New != "B" || r.OldN != 2 || r.NewN != 2 {
		t.Errorf("the changed line: %+v", r)
	}
	if r := rows[5]; r.OldOp != "" || r.NewOp != "add" || r.NewN != 6 || r.OldN != 0 {
		t.Errorf("an added line has no old side: %+v", r)
	}
	_, a, rm, ch := pairDiff(diffLines("x\ny\nz", "x"))
	if a != 0 || rm != 2 || ch != 0 {
		t.Errorf("removed only: %d %d %d", a, rm, ch)
	}
}

// Sc-06 against a real registry: the pair, the counts, what readers do, and the
// other levels, without changing the subject's level.
func TestSchemaDiffPage(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	rec := req(t, c, "GET", "/_console/schemas/orders-value/diff", nil)
	text := readable(rec.Body.String())
	if rec.Code != 200 {
		t.Fatalf("%d", rec.Code)
	}
	for _, want := range []string{"v1 to v2", "1 line added", "1 line changed", "the change is allowed at BACKWARD: the new field has a default",
		"A consumer on v2 reading a v1 message fills email from the default", "At BACKWARD, the subject's level, v2 passes.", "At FORWARD it would pass.", "At FULL it would pass.",
		"Side by side", "Unified", "older, newer pair", "back to versions"} {
		if !strings.Contains(text, want) {
			t.Errorf("the diff page is missing %q:\n%s", want, between(text, "v1 to v2", "The wire"))
		}
	}
	if !strings.Contains(rec.Body.String(), "dl-add") {
		t.Error("no added line is marked")
	}
	if lvl, own, _ := c.reg.Compat(t.Context(), "orders-value"); own || lvl != "BACKWARD" {
		t.Errorf("the checks at other levels changed the level: %s %v", lvl, own)
	}
	// Unified, and a pair named by its numbers, the wrong way round, falls back.
	u := readable(req(t, c, "GET", "/_console/schemas/orders-value/diff?from=1&to=2&layout=unified", nil).Body.String())
	if !strings.Contains(u, "+ ") || !strings.Contains(u, "v1 to v2") {
		t.Errorf("unified: %s", between(u, "Compare", "What this means"))
	}
	if b := readable(req(t, c, "GET", "/_console/schemas/orders-value/diff?from=2&to=2", nil).Body.String()); !strings.Contains(b, "v1 to v2") {
		t.Error("a pair of one version falls back to the one before it")
	}
	// A version that is not the subject's is the latest pair.
	if b := readable(req(t, c, "GET", "/_console/schemas/orders-value/diff?from=9&to=7", nil).Body.String()); !strings.Contains(b, "v1 to v2") {
		t.Error("versions that are not there")
	}
}

func TestDiffSentences(t *testing.T) {
	if got := allowedNote(checkAnswer{Level: "NONE", Compatible: true}, nil); !strings.Contains(got, "NONE") {
		t.Error(got)
	}
	refused := checkAnswer{Level: "BACKWARD", Findings: []checkFinding{{Reasons: nil}}}
	if got := allowedNote(refused, nil); got != "the change would be refused at BACKWARD" {
		t.Error(got)
	}
	two := []changePart{{Before: "added optional ", Name: "a", After: " (x, default null)"}, {Before: "added ", Name: "b", After: " with default 1"}}
	if got := allowedNote(checkAnswer{Level: "FULL", Compatible: true}, two); got != "the change is allowed at FULL: the new fields have defaults" {
		t.Error(got)
	}
	for _, c := range []struct {
		typ   string
		parts []changePart
		want  string
	}{
		{"AVRO", nil, "differ in their text only"},
		{"PROTOBUF", []changePart{{Before: "added ", Name: "carrier = 4"}}, "sees carrier = 4 unset"},
		{"AVRO", []changePart{{Before: "removed ", Name: "x"}}, "skips x"},
		{"AVRO", []changePart{{Before: "removed ", Name: "x"}, {Before: "added ", Name: "y"}}, "whether each reader copes"},
	} {
		if got := readersMeaning(c.typ, c.parts, 1, 2); !strings.Contains(got, c.want) {
			t.Errorf("%v: %q", c.parts, got)
		}
	}
	if otherLevelsSentence(map[string]checkAnswer{}, "BACKWARD", 2) != "" {
		t.Error("no verdict, no sentence")
	}
	v := map[string]checkAnswer{"": {Compatible: false}, "FORWARD": {Compatible: false}, "FULL": {Findings: []checkFinding{{Reasons: nil}}}}
	if got := otherLevelsSentence(v, "BACKWARD", 3); got != "At BACKWARD, the subject's level, v3 is refused. At FORWARD it would be refused. At FULL it would be refused." {
		t.Error(got)
	}
}
