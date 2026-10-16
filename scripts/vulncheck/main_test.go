package main

import (
	"strings"
	"testing"
)

// scanOutput is govulncheck's -format json stream, cut down to what is read: three
// advisories, one called with no fix, one called with a fix, one only required.
const scanOutput = `
{"config":{"protocol_version":"v1.0.0","scanner_name":"govulncheck"}}
{"progress":{"message":"Checking the code against the vulnerabilities..."}}
{"osv":{"id":"GO-1-0001","summary":"loops for ever"}}
{"osv":{"id":"GO-1-0002","summary":"reads past the end"}}
{"osv":{"id":"GO-1-0003","summary":"never called"}}
{"finding":{"osv":"GO-1-0001","trace":[{"module":"example.com/avro","version":"v2.31.0"}]}}
{"finding":{"osv":"GO-1-0001","trace":[{"module":"example.com/avro","version":"v2.31.0","package":"example.com/avro"}]}}
{"finding":{"osv":"GO-1-0001","trace":[{"module":"example.com/avro","version":"v2.31.0","package":"example.com/avro","function":"Unmarshal"},{"module":"me","package":"me/reg","function":"Decode","position":{"filename":"reg/codec.go","line":88}}]}}
{"finding":{"osv":"GO-1-0001","trace":[{"module":"example.com/avro","version":"v2.31.0","package":"example.com/avro","function":"Parse"},{"module":"me","package":"me/reg","function":"parse","position":{"filename":"reg/avro.go","line":28}}]}}
{"finding":{"osv":"GO-1-0002","fixed_version":"v1.2.3","trace":[{"module":"example.com/zip","version":"v1.2.0","package":"example.com/zip","function":"Open"},{"module":"me","package":"me","function":"main","position":{"filename":"main.go","line":7}}]}}
{"finding":{"osv":"GO-1-0003","fixed_version":"v9.0.0","trace":[{"module":"example.com/other","version":"v8.0.0"}]}}
`

func judged(t *testing.T, accepted string, tests ...string) (problems, notes []string) {
	t.Helper()
	found, err := ParseScan(strings.NewReader(scanOutput))
	if err != nil {
		t.Fatal(err)
	}
	acc, err := ReadAccepted(strings.NewReader(accepted))
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, name := range tests {
		have[name] = true
	}
	return Judge(found, acc, func(name string) bool { return have[name] })
}

func has(lines []string, parts ...string) bool {
	for _, l := range lines {
		all := true
		for _, p := range parts {
			all = all && strings.Contains(l, p)
		}
		if all {
			return true
		}
	}
	return false
}

func TestScanIsRead(t *testing.T) {
	found, err := ParseScan(strings.NewReader(scanOutput))
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 3 {
		t.Fatalf("%d findings, want 3: %+v", len(found), found)
	}
	want := []Finding{
		{ID: "GO-1-0001", Summary: "loops for ever", Module: "example.com/avro@v2.31.0", Called: true, Example: "reg/codec.go:88"},
		{ID: "GO-1-0002", Summary: "reads past the end", Module: "example.com/zip@v1.2.0", Fixed: "v1.2.3", Called: true, Example: "main.go:7"},
		{ID: "GO-1-0003", Summary: "never called", Module: "example.com/other@v8.0.0", Fixed: "v9.0.0"},
	}
	for i, w := range want {
		if found[i] != w {
			t.Errorf("finding %d = %+v, want %+v", i, found[i], w)
		}
	}
	if _, err := ParseScan(strings.NewReader(`{"finding": `)); err == nil {
		t.Error("output cut short was read as a scan")
	}
}

// Nothing accepted: both reachable vulnerabilities fail, each told what to do; the one
// nothing calls is a note.
func TestAReachableVulnerabilityFails(t *testing.T) {
	problems, notes := judged(t, "")
	if len(problems) != 2 {
		t.Fatalf("problems = %q", problems)
	}
	if !has(problems, "GO-1-0001", "No fixed version", "reg/codec.go:88") {
		t.Errorf("the one with no fix: %q", problems)
	}
	if !has(problems, "GO-1-0002", "Fixed in v1.2.3: upgrade") {
		t.Errorf("the one with a fix: %q", problems)
	}
	if !has(notes, "GO-1-0003", "nothing here calls") {
		t.Errorf("notes = %q", notes)
	}
}

// Accepted with its guard: it passes, and is still printed.
func TestAnAcceptedVulnerabilityPassesAndIsShown(t *testing.T) {
	problems, notes := judged(t, "# why\nGO-1-0001 reg.TestHostile the library is archived\n", "reg.TestHostile")
	if len(problems) != 1 || !has(problems, "GO-1-0002") {
		t.Fatalf("problems = %q, want only the one with a fix", problems)
	}
	if !has(notes, "GO-1-0001", "ACCEPTED", "reg.TestHostile", "the library is archived") {
		t.Errorf("notes = %q", notes)
	}
}

// What acceptance cannot do.
func TestAcceptanceIsNotAWayToLookAway(t *testing.T) {
	// The guard test is gone.
	if problems, _ := judged(t, "GO-1-0001 reg.TestHostile archived\n"); !has(problems, "GO-1-0001", "no such test") {
		t.Errorf("a missing guard test: %q", problems)
	}
	// A fix exists: accepting it is refused.
	if problems, _ := judged(t, "GO-1-0002 x.TestY cannot upgrade\n", "x.TestY"); !has(problems, "GO-1-0002", "now fixed in v1.2.3") {
		t.Errorf("accepted with a fix available: %q", problems)
	}
	// Not reported any more (or never called): the entry is stale.
	if problems, _ := judged(t, "GO-1-0009 x.TestY gone\nGO-1-0003 x.TestY not called\n", "x.TestY"); !has(problems, "GO-1-0009", "delete the entry") || !has(problems, "GO-1-0003", "delete the entry") {
		t.Errorf("stale entries: %q", problems)
	}
	// Twice.
	if problems, _ := judged(t, "GO-1-0001 reg.TestHostile a\nGO-1-0001 reg.TestHostile b\n", "reg.TestHostile"); !has(problems, "accepted twice") {
		t.Errorf("a repeated entry: %q", problems)
	}
	for _, bad := range []string{"GO-1-0001", "GO-1-0001 reason only here", "CVE-1 x.TestY reason", "GO-1-0001 notatest reason"} {
		if _, err := ReadAccepted(strings.NewReader(bad)); err == nil {
			t.Errorf("%q was read as an entry", bad)
		}
	}
}

// The accepted file in this repository names tests that exist.
func TestTheAcceptedFileNamesTestsThatExist(t *testing.T) {
	tests, err := testsIn("../..")
	if err != nil {
		t.Fatal(err)
	}
	if !tests["scripts/vulncheck.TestTheAcceptedFileNamesTestsThatExist"] {
		t.Fatalf("the walk does not find this very test among %d", len(tests))
	}
	raw := mustRead(t, "../../"+acceptedFile)
	accepted, err := ReadAccepted(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range accepted {
		if !tests[a.Test] {
			t.Errorf("%s is accepted because %s guards it, and there is no such test", a.ID, a.Test)
		}
	}
}
