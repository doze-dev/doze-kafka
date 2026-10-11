// Package conformance holds the evidence a stage rests on about how a real Apache
// Kafka behaves (foundation B1). This file defines and checks the claims format.
//
// A claims file is conformance/claims/<stage>.md: a Markdown table whose columns
// are, in order,
//
//	ID | Claim | Source | Evidence | Scenario
//
// Source is one of the four in plan/01-protocol.md:
//
//	spec      the wire protocol's own message definitions (the kmsg structs).
//	          Evidence names the type and field.
//	recorded  a scenario run against a real apache/kafka:4.0.0 and stored under
//	          conformance/recordings/. Evidence is the recording's path, which
//	          must exist.
//	doc       the Kafka documentation or a KIP. Evidence cites the page or number,
//	          and Scenario names the recording that would settle it.
//	memory    somebody recalls it. It is not acted on until it is doc or recorded:
//	          Scenario says what would record it.
//
// Every file may have any text around its table; only table rows with a header
// that starts "| ID | Claim | Source" are claims.
package conformance

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Claim is one row of a claims table.
type Claim struct {
	File, ID, Text, Source, Evidence, Scenario string
}

// Sources is the closed set of evidence sources.
var Sources = []string{"spec", "recorded", "doc", "memory"}

// ReadClaims parses every conformance/claims/*.md file.
func ReadClaims(dir string) ([]Claim, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.md"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	var out []Claim
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		in := false
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "|") {
				in = false
				continue
			}
			cells := strings.Split(strings.Trim(line, "|"), "|")
			for i := range cells {
				cells[i] = strings.TrimSpace(cells[i])
			}
			switch {
			case len(cells) >= 3 && cells[0] == "ID" && cells[1] == "Claim" && cells[2] == "Source":
				in = true
			case !in, strings.HasPrefix(cells[0], "---"):
			case len(cells) == 5:
				out = append(out, Claim{File: filepath.Base(f), ID: cells[0], Text: cells[1], Source: cells[2], Evidence: cells[3], Scenario: cells[4]})
			default:
				out = append(out, Claim{File: filepath.Base(f), ID: cells[0], Text: "malformed row: " + line})
			}
		}
	}
	return out, nil
}

// Problems lists what is wrong with a set of claims; root is the conformance
// directory (where recordings/ is).
func Problems(claims []Claim, root string) []string {
	var bad []string
	seen := map[string]bool{}
	for _, c := range claims {
		where := c.File + " " + c.ID
		switch {
		case c.ID == "" || strings.HasPrefix(c.Text, "malformed row"):
			bad = append(bad, where+": "+c.Text)
			continue
		case seen[c.File+"/"+c.ID]:
			bad = append(bad, where+": the id is used twice")
		case c.Text == "":
			bad = append(bad, where+": no claim")
		}
		seen[c.File+"/"+c.ID] = true
		switch c.Source {
		case "spec":
			if c.Evidence == "" {
				bad = append(bad, where+": a spec claim names the kmsg type and field in Evidence")
			}
		case "recorded":
			if !strings.HasPrefix(c.Evidence, "conformance/recordings/") {
				bad = append(bad, where+": a recorded claim points at a file under conformance/recordings/")
			} else if _, err := os.Stat(filepath.Join(root, "..", c.Evidence)); err != nil {
				bad = append(bad, where+": the recording "+c.Evidence+" does not exist")
			}
		case "doc":
			if c.Evidence == "" || c.Scenario == "" {
				bad = append(bad, where+": a doc claim cites the page or KIP and names the scenario that would record it")
			}
		case "memory":
			if c.Scenario == "" {
				bad = append(bad, where+": a memory claim says what scenario would record it")
			}
		default:
			bad = append(bad, where+": source "+c.Source+" is not one of spec, recorded, doc, memory")
		}
	}
	return bad
}

// Count is the number of claims per source, in the order of Sources.
func Count(claims []Claim) map[string]int {
	n := map[string]int{}
	for _, c := range claims {
		n[c.Source]++
	}
	return n
}

// TestClaimsCountPerSource prints the number of claims per source (run it with -v,
// or `task claims`) and fails on a claim that is not well formed.
func TestClaimsCountPerSource(t *testing.T) {
	claims, err := ReadClaims("claims")
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) == 0 {
		t.Fatal("no claims found under conformance/claims")
	}
	for _, p := range Problems(claims, ".") {
		t.Error(p)
	}
	files := map[string]int{}
	for _, c := range claims {
		files[c.File]++
	}
	n := Count(claims)
	var lines []string
	for _, s := range Sources {
		lines = append(lines, s+" "+itoa(n[s]))
	}
	t.Logf("claims per source: %s (total %d in %d files)", strings.Join(lines, ", "), len(claims), len(files))
	if files["topics.md"] == 0 {
		t.Error("the Topics stage's claims table is the worked example and must exist")
	}
}

func TestProblemsCatchesEachKindOfBadRow(t *testing.T) {
	good := Claim{File: "x.md", ID: "A", Text: "t", Source: "memory", Scenario: "s"}
	for name, c := range map[string]Claim{
		"unknown source":          {File: "x.md", ID: "B", Text: "t", Source: "hearsay", Scenario: "s"},
		"memory with no scenario": {File: "x.md", ID: "C", Text: "t", Source: "memory"},
		"doc with no citation":    {File: "x.md", ID: "D", Text: "t", Source: "doc", Scenario: "s"},
		"spec with no field":      {File: "x.md", ID: "E", Text: "t", Source: "spec"},
		"recorded, no file":       {File: "x.md", ID: "F", Text: "t", Source: "recorded", Evidence: "conformance/recordings/none.json"},
		"empty claim":             {File: "x.md", ID: "G", Source: "memory", Scenario: "s"},
	} {
		if p := Problems([]Claim{good, c}, "."); len(p) != 1 {
			t.Errorf("%s: want one problem, got %v", name, p)
		}
	}
	if p := Problems([]Claim{good, good}, "."); len(p) != 1 {
		t.Errorf("a repeated id: want one problem, got %v", p)
	}
	if p := Problems([]Claim{good}, "."); len(p) != 0 {
		t.Errorf("a good claim: %v", p)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}
