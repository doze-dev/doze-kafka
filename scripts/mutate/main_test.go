package main

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Every non-test file of a split package is in exactly one group, so a new file
// cannot silently fall out of the baseline.
func TestFileGroupsPartitionTheirPackage(t *testing.T) {
	byDir := map[string]map[string]string{} // dir -> file -> unit
	for _, u := range Units {
		if len(u.Only) == 0 {
			continue
		}
		if byDir[u.Dir] == nil {
			byDir[u.Dir] = map[string]string{}
		}
		for _, n := range u.Only {
			if prev, dup := byDir[u.Dir][n]; dup {
				t.Errorf("%s/%s.go is in both %s and %s", u.Dir, n, prev, u.Name)
			}
			byDir[u.Dir][n] = u.Name
		}
	}
	for dir, in := range byDir {
		files, err := filepath.Glob(filepath.Join("..", "..", dir, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("%s: %v (%d files)", dir, err, len(files))
		}
		seen := map[string]bool{}
		for _, f := range files {
			base := strings.TrimSuffix(filepath.Base(f), ".go")
			if strings.HasSuffix(base, "_test") {
				continue
			}
			seen[base] = true
			if _, ok := in[base]; !ok {
				t.Errorf("%s/%s.go is in no mutation group: add it to a unit in scripts/mutate/main.go", dir, base)
			}
		}
		for n := range in {
			if !seen[n] {
				t.Errorf("%s/%s.go is named in a mutation group and does not exist", dir, n)
			}
		}
	}
}

const sample = `{"go_module":"m","files":[{"file_name":"a.go","mutations":[
 {"type":"CONDITIONALS_NEGATION","status":"KILLED","line":3,"column":1},
 {"type":"ARITHMETIC_BASE","status":"LIVED","line":9,"column":2},
 {"type":"CONDITIONALS_BOUNDARY","status":"TIMED OUT","line":4,"column":2},
 {"type":"INCREMENT_DECREMENT","status":"NOT COVERED","line":7,"column":2}]}]}`

func TestParseCountsAndNamesSurvivors(t *testing.T) {
	got, err := Parse([]byte(sample), "pkg/x")
	if err != nil {
		t.Fatal(err)
	}
	want := Result{Killed: 1, TimedOut: 1, Lived: 1, NotCovered: 1, Survivors: []string{"pkg/x/a.go:9:ARITHMETIC_BASE"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestDiffFailsOnANewSurvivorAndOnMoreUncovered(t *testing.T) {
	base := Result{Survivors: []string{"p/a.go:9:ARITHMETIC_BASE"}, NotCovered: 2}
	if d := Diff(base, base); len(d) != 0 {
		t.Errorf("the baseline against itself: %v", d)
	}
	better := Result{NotCovered: 1}
	if d := Diff(base, better); len(d) != 0 {
		t.Errorf("fewer survivors is not a failure: %v", d)
	}
	worse := Result{Survivors: []string{"p/a.go:9:ARITHMETIC_BASE", "p/a.go:12:CONDITIONALS_NEGATION"}, NotCovered: 3}
	d := Diff(base, worse)
	if len(d) != 2 || !strings.Contains(d[0], "p/a.go:12:CONDITIONALS_NEGATION") {
		t.Errorf("want the new survivor and the uncovered count, got %v", d)
	}
}

func TestSelectTakesPackagesAndUnitNames(t *testing.T) {
	us, err := Select([]string{"./internal/broker/..."})
	if err != nil || len(us) != 4 {
		t.Fatalf("the broker has 4 units, got %d (%v)", len(us), err)
	}
	us, err = Select([]string{"internal/broker:data", "./console"})
	if err != nil || len(us) != 2 {
		t.Fatalf("got %d, %v", len(us), err)
	}
	if _, err := Select([]string{"./nope"}); err == nil {
		t.Error("an unknown package must be an error, not an empty run")
	}
}
