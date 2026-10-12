package docs

import (
	"strings"
	"testing"
	"testing/fstest"
)

// The marker check is itself checked: a small invented tree that passes, and the
// same tree with one fault planted at a time, each of which must be reported, by
// name, and alone. A check that cannot fail would say every parser has a fuzz
// target for ever.

const (
	fakeWire = `package wire

import "io"

// ParseThing reads a thing off the wire.
//
//fuzz:parser class=frame FuzzThing
func ParseThing(b []byte) int { return len(b) }

// readState reads what this program wrote.
//
//fuzz:not-a-parser reads the state file this program wrote itself
func readState(path string) string { return path }

type conn struct{}

//fuzz:parser FuzzThing
func (c *conn) ReadFrom(r io.Reader) error { return nil }

// helper takes text and is not named like a parser.
func helper(s string) string { return s }

// parseNothing is named like a parser and takes no bytes.
func parseNothing(n int) int { return n }
`
	// (Put together so that no line of this file starts a Fuzz function: the real
	// check reads this file too.)
	fakeWireTest = "package wire\n\nimport \"testing\"\n\n" + "func " + "FuzzThing(f *testing.F) {}\n"
	fakeWeb      = `package web

import "net/http"

type Handler struct{}

// ServeHTTP is where the forms arrive.
//
//fuzz:parser class=forms FuzzForm
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {}
`
	fakeWebTest = "package web_test\n\nimport \"testing\"\n\n" + "func " + "FuzzForm(f *testing.F) {}\n"
	fakeList    = "# every target\n./wire FuzzThing\n./web   FuzzForm\n"
)

func fakeFuzzTree() (fstest.MapFS, fuzzRules) {
	tree := fstest.MapFS{
		"testdata/fuzz-targets.txt": {Data: []byte(fakeList)},
		"wire/thing.go":             {Data: []byte(fakeWire)},
		"wire/thing_test.go":        {Data: []byte(fakeWireTest)},
		"web/form.go":               {Data: []byte(fakeWeb)},
		"web/form_test.go":          {Data: []byte(fakeWebTest)},
		// Not a package that faces the network: its parsers are not asked about.
		"tools/gen.go": {Data: []byte("package tools\n\nfunc ParseFlags(s string) {}\n")},
		// Never walked.
		".claude/worktrees/x/wire/thing.go": {Data: []byte("package wire\n\nfunc ParseOther(b []byte) {}\n")},
		"wire/testdata/src.go":              {Data: []byte("package x\n\nfunc ParseFixture(b []byte) {}\n")},
	}
	return tree, fuzzRules{List: "testdata/fuzz-targets.txt", Net: []string{"wire", "web"}, Classes: []string{"frame", "forms"}}
}

func TestFuzzMarkerCheckPassesACleanTree(t *testing.T) {
	tree, rules := fakeFuzzTree()
	rep := checkFuzzMarkers(tree, rules)
	for _, p := range rep.Problems {
		t.Errorf("a clean tree is faulted: %s", p)
	}
	if len(rep.Targets) != 2 || !rep.Targets["./wire FuzzThing"] || !rep.Targets["./web FuzzForm"] {
		t.Errorf("targets found = %v, want ./wire FuzzThing and ./web FuzzForm", rep.Targets)
	}
}

func TestFuzzMarkerCheckReportsEachPlantedFault(t *testing.T) {
	replace := func(file, old, new string) func(fstest.MapFS, *fuzzRules) {
		return func(tree fstest.MapFS, _ *fuzzRules) {
			src := string(tree[file].Data)
			if !strings.Contains(src, old) {
				panic("the fake tree has no " + old)
			}
			tree[file] = &fstest.MapFile{Data: []byte(strings.Replace(src, old, new, 1))}
		}
	}
	add := func(file, src string) func(fstest.MapFS, *fuzzRules) {
		return func(tree fstest.MapFS, _ *fuzzRules) {
			tree[file] = &fstest.MapFile{Data: []byte(string(tree[file].Data) + src)}
		}
	}
	for _, tc := range []struct {
		name  string
		plant func(fstest.MapFS, *fuzzRules)
		want  []string // every one of these is in the one problem reported.
	}{
		{"an unmarked ParseThing([]byte)",
			replace("wire/thing.go", "//\n//fuzz:parser class=frame FuzzThing\nfunc ParseThing", "func ParseThing"),
			[]string{"wire/thing.go:6: ParseThing", "neither a //fuzz:parser nor a //fuzz:not-a-parser"}},
		{"a new unmarked parser taking a string",
			add("wire/thing.go", "\nfunc decodeName(s string) string { return s }\n"),
			[]string{"wire/thing.go", "decodeName", "neither"}},
		{"a new unmarked method taking an io.Reader",
			add("wire/thing.go", "\nfunc (c *conn) ReadHeader(r io.Reader) {}\n"),
			[]string{"conn.ReadHeader", "neither"}},
		{"a new unmarked handler-side reader taking a request",
			add("web/form.go", "\nfunc readForm(r *http.Request) {}\n"),
			[]string{"web/form.go", "readForm", "neither"}},
		{"a new file of the package with an unmarked parser",
			func(tree fstest.MapFS, _ *fuzzRules) {
				tree["wire/more.go"] = &fstest.MapFile{Data: []byte("package wire\n\nfunc UnmarshalMore(b []byte) {}\n")}
			},
			[]string{"wire/more.go:3: UnmarshalMore", "neither"}},
		{"a marker naming a target that does not exist",
			replace("wire/thing.go", "//fuzz:parser FuzzThing", "//fuzz:parser FuzzGone"),
			[]string{"conn.ReadFrom", "FuzzGone", "no func FuzzGone(f *testing.F) exists"}},
		{"a marker naming a target that exists and is not in the list",
			func(tree fstest.MapFS, r *fuzzRules) {
				replace("wire/thing.go", "//fuzz:parser FuzzThing", "//fuzz:parser FuzzUnlisted")(tree, r)
				add("wire/thing_test.go", "\nfunc FuzzUnlisted(f *testing.F) {}\n")(tree, r)
			},
			[]string{"conn.ReadFrom", "its marker names FuzzUnlisted, which is not in testdata/fuzz-targets.txt"}},
		{"a class with no marked function",
			replace("web/form.go", "class=forms ", ""),
			[]string{"class=forms", "no function is marked"}},
		{"a class the plan lists and nothing claims",
			func(_ fstest.MapFS, r *fuzzRules) { r.Classes = append(r.Classes, "trace-headers") },
			[]string{"class=trace-headers", "no function is marked"}},
		{"a class nobody defined",
			replace("web/form.go", "class=forms", "class=forms class=froms"),
			[]string{"ServeHTTP", "class=froms is not one of the classes"}},
		{"a listed target that no marker names",
			func(tree fstest.MapFS, r *fuzzRules) {
				add("wire/thing_test.go", "\nfunc FuzzNothingInParticular(f *testing.F) {}\n")(tree, r)
				add("testdata/fuzz-targets.txt", "./wire FuzzNothingInParticular\n")(tree, r)
			},
			[]string{"./wire FuzzNothingInParticular", "no //fuzz:parser marker names it"}},
		{"a listed target with no Fuzz function",
			add("testdata/fuzz-targets.txt", "./wire FuzzRemoved\n"),
			[]string{"./wire FuzzRemoved", "no such Fuzz function exists"}},
		{"not-a-parser with no reason",
			replace("wire/thing.go", "//fuzz:not-a-parser reads the state file this program wrote itself", "//fuzz:not-a-parser"),
			[]string{"readState", "needs a reason of at least 10 characters"}},
		{"not-a-parser with a reason too short to be one",
			replace("wire/thing.go", "//fuzz:not-a-parser reads the state file this program wrote itself", "//fuzz:not-a-parser trusted"),
			[]string{"readState", "needs a reason of at least 10 characters"}},
		{"a parser marker with no target",
			replace("wire/thing.go", "//fuzz:parser FuzzThing", "//fuzz:parser class=frame"),
			[]string{"conn.ReadFrom", "names no fuzz target"}},
		{"a marker of an unknown kind",
			replace("wire/thing.go", "//fuzz:parser FuzzThing", "//fuzz:parsed FuzzThing"),
			[]string{"conn.ReadFrom", "unknown marker"}},
		{"a marker that is not in a function's doc comment",
			replace("wire/thing.go", "//fuzz:parser FuzzThing\nfunc (c *conn) ReadFrom", "//fuzz:parser FuzzThing\n\nfunc (c *conn) ReadFrom"),
			nil}, // two problems: the stray marker, and ReadFrom now unmarked. Checked below.
		{"a target name that two packages have, used from a third",
			func(tree fstest.MapFS, r *fuzzRules) {
				add("web/form_test.go", "\nfunc FuzzThing(f *testing.F) {}\n")(tree, r)
				add("testdata/fuzz-targets.txt", "./web FuzzThing\n")(tree, r)
				add("web/form.go", "\n//fuzz:parser FuzzThing\nfunc local() {}\n")(tree, r)
				tree["other/x.go"] = &fstest.MapFile{Data: []byte("package other\n\n//fuzz:parser FuzzThing\nfunc X() {}\n")}
			},
			[]string{"other/x.go", "X", "which one?"}},
		{"an ownedElsewhere entry for a function that is not there",
			func(_ fstest.MapFS, r *fuzzRules) {
				r.Owned = []fuzzOwned{{File: "wire/thing.go", Func: "ParseGone", Targets: []string{"FuzzThing"}}}
			},
			[]string{"ownedElsewhere", "ParseGone", "no such function"}},
		{"an ownedElsewhere entry for a function that has its own marker now",
			func(_ fstest.MapFS, r *fuzzRules) {
				r.Owned = []fuzzOwned{{File: "wire/thing.go", Func: "ParseThing", Targets: []string{"FuzzThing"}, Class: "frame"}}
			},
			[]string{"ParseThing", "take it out of the ownedElsewhere table"}},
		{"an ownedElsewhere entry naming a target that does not exist",
			func(tree fstest.MapFS, r *fuzzRules) {
				add("wire/thing.go", "\nfunc scanBlock(b []byte) {}\n")(tree, r)
				r.Owned = []fuzzOwned{{File: "wire/thing.go", Func: "scanBlock", Targets: []string{"FuzzBlock"}}}
			},
			[]string{"scanBlock", "FuzzBlock", "no func FuzzBlock"}},
		{"an ownedElsewhere not-a-parser entry with no real reason",
			func(tree fstest.MapFS, r *fuzzRules) {
				add("wire/thing.go", "\nfunc scanBlock(b []byte) {}\n")(tree, r)
				r.Owned = []fuzzOwned{{File: "wire/thing.go", Func: "scanBlock", NotParser: "no"}}
			},
			[]string{"scanBlock", "needs a reason"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tree, rules := fakeFuzzTree()
			tc.plant(tree, &rules)
			got := checkFuzzMarkers(tree, rules).Problems
			if tc.want == nil {
				if len(got) != 2 || !strings.Contains(strings.Join(got, "\n"), "not in the doc comment of a function") || !strings.Contains(strings.Join(got, "\n"), "conn.ReadFrom looks like a parser") {
					t.Fatalf("want the stray marker and the unmarked ReadFrom, got:\n%s", strings.Join(got, "\n"))
				}
				return
			}
			var hit []string
			for _, p := range got {
				all := true
				for _, w := range tc.want {
					all = all && strings.Contains(p, w)
				}
				if all {
					hit = append(hit, p)
				}
			}
			if len(hit) != 1 {
				t.Fatalf("want one problem holding %q, got:\n%s", tc.want, strings.Join(got, "\n"))
			}
			// The fault is reported alone, except where planting it makes a second
			// thing true, which must then be reported as well.
			also := map[string]string{
				// The class the marker stood for is then empty too (a later case has that alone).
				"an unmarked ParseThing([]byte)":                              "no function is marked //fuzz:parser class=frame",
				"a marker naming a target that exists and is not in the list": "is a Fuzz function and is not in testdata/fuzz-targets.txt: ./wire FuzzUnlisted",
			}[tc.name]
			alsoSeen := also == ""
			for _, p := range got {
				switch {
				case p == hit[0]:
				case also != "" && strings.Contains(p, also):
					alsoSeen = true
				default:
					t.Errorf("an unrelated problem: %s", p)
				}
			}
			if !alsoSeen {
				t.Errorf("want %q reported as well, got:\n%s", also, strings.Join(got, "\n"))
			}
		})
	}
}

// An ownedElsewhere entry does what the marker would: the function passes, its
// class is filled and its target is named.
func TestFuzzMarkerCheckTakesAnOwnedEntryForAMarker(t *testing.T) {
	tree, rules := fakeFuzzTree()
	src := strings.Replace(string(tree["wire/thing.go"].Data), "//\n//fuzz:parser class=frame FuzzThing\nfunc ParseThing", "func ParseThing", 1)
	src = strings.Replace(src, "//fuzz:parser FuzzThing\nfunc (c *conn) ReadFrom", "func (c *conn) ReadFrom", 1)
	tree["wire/thing.go"] = &fstest.MapFile{Data: []byte(src)}
	if got := checkFuzzMarkers(tree, rules).Problems; len(got) != 4 {
		t.Fatalf("with both markers gone: want ParseThing, conn.ReadFrom, the class and the target reported, got:\n%s", strings.Join(got, "\n"))
	}
	rules.Owned = []fuzzOwned{
		{File: "wire/thing.go", Func: "ParseThing", Targets: []string{"FuzzThing"}, Class: "frame"},
		{File: "wire/thing.go", Func: "conn.ReadFrom", NotParser: "a stand-in reason for the test"},
	}
	if got := checkFuzzMarkers(tree, rules).Problems; len(got) != 0 {
		t.Fatalf("with the table standing in for the markers:\n%s", strings.Join(got, "\n"))
	}
}

// The real table and the real rules are well formed (the rest is the real run).
func TestRepoFuzzRulesAreWellFormed(t *testing.T) {
	rules := repoFuzzRules()
	seen := map[string]bool{}
	for _, o := range rules.Owned {
		k := o.File + " " + o.Func
		if seen[k] {
			t.Errorf("ownedElsewhere lists %s twice", k)
		}
		seen[k] = true
		if !strings.HasPrefix(o.File, "internal/storage/log/") && !strings.HasPrefix(o.File, "console/traffic") {
			t.Errorf("ownedElsewhere holds %s, whose file can carry its own marker", k)
		}
		if (len(o.Targets) == 0) == (o.NotParser == "") {
			t.Errorf("ownedElsewhere: %s needs targets or a not-a-parser reason, and not both", k)
		}
		if len(o.Targets) > 0 && o.Reason == "" {
			t.Errorf("ownedElsewhere: %s does not say how its target reaches it", k)
		}
	}
	if len(rules.Classes) != 14 || len(rules.Net) != 8 {
		t.Errorf("%d classes and %d packages; the plan lists 14 and 8", len(rules.Classes), len(rules.Net))
	}
}
