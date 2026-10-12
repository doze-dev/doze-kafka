package docs

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
)

// The half of B12 that a list of Fuzz functions cannot check: "every function that
// parses untrusted bytes has a fuzz target". The link between a parser and its
// target is a marker in the parser's own doc comment (the syntax is at the top of
// testdata/fuzz-targets.txt):
//
//	//fuzz:parser [class=<class>]... FuzzTarget[, FuzzTarget]...
//	//fuzz:not-a-parser <why, in at least ten characters>
//
// checkFuzzMarkers reads a source tree and says what is wrong with it. It takes
// the tree as an fs.FS so that a test can hand it a small invented one and watch
// each rule fail (fuzzmarkers_selftest_test.go).

// fuzzOwned stands in for a marker on a function whose file this work may not
// edit. It is checked like a marker, and it must go once the function has one.
type fuzzOwned struct {
	File      string   // slash path from the root of the tree
	Func      string   // "name", or "Type.name" for a method
	Targets   []string // the fuzz targets that reach it; empty with NotParser
	Class     string   // the B12 class it stands for, if any
	NotParser string   // or: why it is not a parser of untrusted bytes
	Reason    string   // how the target reaches it (for the reader)
}

type fuzzRules struct {
	List    string      // the target list, e.g. "testdata/fuzz-targets.txt"
	Net     []string    // the directories whose parser-shaped functions must all be marked
	Classes []string    // every class must have a marked function
	Owned   []fuzzOwned // markers kept here for files owned by somebody else
}

type fuzzReport struct {
	Targets  map[string]bool // "./pkg FuzzName", every Fuzz function in the tree
	Listed   map[string]bool // the same form, from the list
	Problems []string
}

var (
	fuzzFuncRE    = regexp.MustCompile(`(?m)^func (Fuzz\w+)\(\w+ \*testing\.F\)`)
	fuzzNameRE    = regexp.MustCompile(`^Fuzz\w+$`)
	parserShapeRE = regexp.MustCompile(`^(Parse|Decode|Unmarshal|Read|Scan|parse|decode|unmarshal|read|scan)`)
)

const fuzzMinReason = 10

// fuzzMark is one function's marker, from its doc comment or from the Owned table.
type fuzzMark struct {
	where     string // "file:line: func"
	dir       string
	targets   []string
	classes   []string
	notParser bool
}

func skipFuzzDir(name string, isRoot bool) bool {
	if !isRoot && strings.HasPrefix(name, ".") {
		return true // .git, .claude (agent worktrees), .audit, .mutate, .embeddoc-*
	}
	switch name {
	case "node_modules", "kmsg", "module", "e2e", "testdata":
		return true
	}
	return false
}

func checkFuzzMarkers(tree fs.FS, rules fuzzRules) fuzzReport {
	rep := fuzzReport{Targets: map[string]bool{}, Listed: map[string]bool{}}
	bad := func(format string, a ...any) { rep.Problems = append(rep.Problems, fmt.Sprintf(format, a...)) }

	net := map[string]bool{}
	for _, d := range rules.Net {
		net[d] = true
	}
	classes := map[string]int{}
	for _, c := range rules.Classes {
		classes[c] = 0
	}
	owned := map[string]*fuzzOwned{} // "file Func"
	ownedSeen := map[string]bool{}
	for i := range rules.Owned {
		o := &rules.Owned[i]
		owned[o.File+" "+o.Func] = o
	}

	var marks []fuzzMark
	byName := map[string][]string{} // FuzzName -> the packages that have one

	err := fs.WalkDir(tree, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipFuzzDir(d.Name(), p == ".") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		raw, err := fs.ReadFile(tree, p)
		if err != nil {
			return err
		}
		dir := path.Dir(p)
		if strings.HasSuffix(p, "_test.go") {
			for _, m := range fuzzFuncRE.FindAllSubmatch(raw, -1) {
				name := string(m[1])
				rep.Targets["./"+dir+" "+name] = true
				byName[name] = append(byName[name], "./"+dir)
			}
			return nil
		}
		if !net[dir] && !bytes.Contains(raw, []byte("//fuzz:")) {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, p, raw, parser.ParseComments)
		if err != nil {
			bad("cannot be parsed, so its parsers cannot be checked: %v", err)
			return nil
		}
		inDoc := map[*ast.Comment]bool{}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			name := funcDisplayName(fn)
			where := fmt.Sprintf("%s:%d: %s", p, fset.Position(fn.Pos()).Line, name)
			var lines []string
			if fn.Doc != nil {
				for _, c := range fn.Doc.List {
					if strings.HasPrefix(c.Text, "//fuzz:") {
						inDoc[c] = true
						lines = append(lines, c.Text)
					}
				}
			}
			o := owned[p+" "+name]
			if o != nil {
				ownedSeen[p+" "+name] = true
			}
			switch {
			case len(lines) > 1:
				bad("%s has %d //fuzz: markers; a function has one", where, len(lines))
			case len(lines) == 1:
				if o != nil {
					bad("%s has a //fuzz: marker now; take it out of the ownedElsewhere table", where)
				}
				if m, msg := parseFuzzMarker(lines[0]); msg != "" {
					bad("%s: %s", where, msg)
				} else {
					m.where, m.dir = where, dir
					marks = append(marks, m)
				}
			case o != nil:
				m := fuzzMark{where: where + " (ownedElsewhere)", dir: dir, targets: o.Targets}
				if o.Class != "" {
					m.classes = []string{o.Class}
				}
				switch {
				case o.NotParser != "" && len(o.Targets) > 0:
					bad("%s: is both a parser and not a parser", m.where)
				case o.NotParser != "":
					m.notParser = true
					if len(strings.TrimSpace(o.NotParser)) < fuzzMinReason {
						bad("%s: not-a-parser needs a reason of at least %d characters", m.where, fuzzMinReason)
					}
				case len(o.Targets) == 0:
					bad("%s: names no fuzz target", m.where)
				}
				marks = append(marks, m)
			case net[dir] && parserShaped(fn):
				bad("%s looks like a parser of outside bytes (its name and a []byte, string, io.Reader or *http.Request parameter) and has neither a //fuzz:parser nor a //fuzz:not-a-parser marker", where)
			}
		}
		for _, cg := range file.Comments {
			for _, c := range cg.List {
				if strings.HasPrefix(c.Text, "//fuzz:") && !inDoc[c] {
					bad("%s:%d: a //fuzz: marker that is not in the doc comment of a function marks nothing", p, fset.Position(c.Pos()).Line)
				}
			}
		}
		return nil
	})
	if err != nil {
		bad("walking the tree: %v", err)
	}

	for k, o := range owned {
		if !ownedSeen[k] {
			bad("ownedElsewhere names %s in %s, and there is no such function", o.Func, o.File)
		}
	}

	raw, err := fs.ReadFile(tree, rules.List)
	if err != nil {
		bad("reading the target list: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rep.Listed[strings.Join(strings.Fields(line), " ")] = true
	}
	for k := range rep.Targets {
		if !rep.Listed[k] {
			bad("is a Fuzz function and is not in %s: %s", rules.List, k)
		}
	}
	for k := range rep.Listed {
		if !rep.Targets[k] {
			bad("is in %s and no such Fuzz function exists: %s", rules.List, k)
		}
	}

	// (i) every marker names targets that exist and are run.
	named := map[string]bool{}
	for _, m := range marks {
		for _, c := range m.classes {
			if _, ok := classes[c]; !ok {
				bad("%s: class=%s is not one of the classes (%s)", m.where, c, strings.Join(rules.Classes, ", "))
				continue
			}
			if !m.notParser {
				classes[c]++
			}
		}
		for _, t := range m.targets {
			key := "./" + m.dir + " " + t
			if !rep.Targets[key] {
				switch pkgs := byName[t]; len(pkgs) {
				case 0:
					bad("%s: its marker names %s, and no func %s(f *testing.F) exists", m.where, t, t)
					continue
				case 1:
					key = pkgs[0] + " " + t
				default:
					sort.Strings(pkgs)
					bad("%s: its marker names %s, which is in %s and not in its own package; which one?", m.where, t, strings.Join(pkgs, " and "))
					continue
				}
			}
			if !rep.Listed[key] {
				bad("%s: its marker names %s, which is not in %s", m.where, t, rules.List)
			}
			named[key] = true
		}
	}
	// (ii) every target in the list fuzzes something in particular.
	for k := range rep.Listed {
		if rep.Targets[k] && !named[k] {
			bad("is in %s and no //fuzz:parser marker names it, so nothing says what it fuzzes: %s", rules.List, k)
		}
	}
	// (iii) every class of parser the plan lists has a marked function.
	for _, c := range rules.Classes {
		if classes[c] == 0 {
			bad("no function is marked //fuzz:parser class=%s: that class of untrusted bytes has no fuzz target", c)
		}
	}
	sort.Strings(rep.Problems)
	return rep
}

func funcDisplayName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	t := fn.Recv.List[0].Type
	for {
		switch x := t.(type) {
		case *ast.StarExpr:
			t = x.X
			continue
		case *ast.IndexExpr:
			t = x.X
			continue
		case *ast.IndexListExpr:
			t = x.X
			continue
		case *ast.Ident:
			return x.Name + "." + fn.Name.Name
		}
		return fn.Name.Name
	}
}

// parseFuzzMarker reads one marker line. msg says what is wrong with it, if anything.
func parseFuzzMarker(line string) (m fuzzMark, msg string) {
	switch {
	case strings.HasPrefix(line, "//fuzz:not-a-parser"):
		rest := strings.TrimPrefix(line, "//fuzz:not-a-parser")
		if rest != "" && rest[0] != ' ' {
			return m, "unknown marker " + line
		}
		if len(strings.TrimSpace(rest)) < fuzzMinReason {
			return m, fmt.Sprintf("//fuzz:not-a-parser needs a reason of at least %d characters", fuzzMinReason)
		}
		m.notParser = true
		return m, ""
	case strings.HasPrefix(line, "//fuzz:parser ") || line == "//fuzz:parser":
		for _, w := range strings.Fields(strings.ReplaceAll(strings.TrimPrefix(line, "//fuzz:parser"), ",", " ")) {
			switch {
			case strings.HasPrefix(w, "class="):
				m.classes = append(m.classes, strings.TrimPrefix(w, "class="))
			case fuzzNameRE.MatchString(w):
				m.targets = append(m.targets, w)
			default:
				return m, fmt.Sprintf("//fuzz:parser takes class=<class> and FuzzTarget names, not %q", w)
			}
		}
		if len(m.targets) == 0 {
			return m, "//fuzz:parser names no fuzz target"
		}
		return m, ""
	}
	return m, "unknown marker " + line
}

// parserShaped is the net for a parser nobody marked: a function named like one
// that takes bytes, text, a reader or a request.
func parserShaped(fn *ast.FuncDecl) bool {
	if !parserShapeRE.MatchString(fn.Name.Name) || fn.Type.Params == nil {
		return false
	}
	for _, f := range fn.Type.Params.List {
		t := f.Type
		if e, ok := t.(*ast.Ellipsis); ok {
			t = e.Elt
		}
		switch x := t.(type) {
		case *ast.Ident:
			if x.Name == "string" {
				return true
			}
		case *ast.ArrayType:
			if id, ok := x.Elt.(*ast.Ident); ok && x.Len == nil && id.Name == "byte" {
				return true
			}
		case *ast.SelectorExpr:
			if isSel(x, "io", "Reader") {
				return true
			}
		case *ast.StarExpr:
			if s, ok := x.X.(*ast.SelectorExpr); ok && isSel(s, "http", "Request") {
				return true
			}
		}
	}
	return false
}

func isSel(s *ast.SelectorExpr, pkg, name string) bool {
	id, ok := s.X.(*ast.Ident)
	return ok && id.Name == pkg && s.Sel.Name == name
}
