package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// A Mutant is one token of the source swapped for another.
type Mutant struct {
	File   string `json:"file"` // slash path relative to the module root.
	Line   int    `json:"line"`
	Col    int    `json:"col"`
	Type   string `json:"type"`
	From   string `json:"from"`
	To     string `json:"to"`
	Func   string `json:"func"`   // the enclosing declaration: "Name", "(*T).Name" or "<package>".
	Ord    int    `json:"ord"`    // which mutant of this Type in Func, from 1, in source order.
	Source string `json:"source"` // the line, trimmed.

	offset int // byte offset of the token in the file.

	Status Status   `json:"status"`
	Detail string   `json:"detail,omitempty"` // the test that failed, or what the run printed.
	Tests  []string `json:"tests,omitempty"`  // the tests that reach the line (selection mode).
}

// At is how a mutant is written for a person: file:line:col:TYPE.
func (m Mutant) At() string { return fmt.Sprintf("%s:%d:%d:%s", m.File, m.Line, m.Col, m.Type) }

// Key identifies a mutant across edits elsewhere in its file: the file, the
// declaration it is in, the kind of mutation and which one of that kind it is
// there. A line number would make every edit above it a "new" survivor.
func (m Mutant) Key() string { return fmt.Sprintf("%s:%s:%s#%d", m.File, m.Func, m.Type, m.Ord) }

// Status is what happened to a mutant.
type Status string

const (
	// Killed: the tests ran and one failed (or the test process crashed after a
	// test had started).
	Killed Status = "killed"
	// Lived: the tests ran and all passed with the code wrong.
	Lived Status = "lived"
	// NotCovered: no test executes the mutated line, so no test was run.
	NotCovered Status = "notCovered"
	// TimedOut: the tests did not finish in the time allowed, twice.
	TimedOut Status = "timedOut"
	// NotViable: the mutant does not compile (a `+` on strings made a `-`). It
	// says nothing about the tests.
	NotViable Status = "notViable"
	// SetupFailed: the test process failed before any test ran. That is a broken
	// run, never a kill.
	SetupFailed Status = "setupFailed"
)

// The mutations: the five gremlins applies by default, so the numbers compare
// with the earlier baselines. A binary minus is ARITHMETIC_BASE only and a unary
// minus INVERT_NEGATIVES only (gremlins counted each under both, twice the same
// mutant).
var binaryMutations = map[token.Token][]struct {
	typ string
	to  token.Token
}{
	token.ADD: {{"ARITHMETIC_BASE", token.SUB}},
	token.SUB: {{"ARITHMETIC_BASE", token.ADD}},
	token.MUL: {{"ARITHMETIC_BASE", token.QUO}},
	token.QUO: {{"ARITHMETIC_BASE", token.MUL}},
	token.REM: {{"ARITHMETIC_BASE", token.MUL}},
	token.GTR: {{"CONDITIONALS_BOUNDARY", token.GEQ}, {"CONDITIONALS_NEGATION", token.LEQ}},
	token.GEQ: {{"CONDITIONALS_BOUNDARY", token.GTR}, {"CONDITIONALS_NEGATION", token.LSS}},
	token.LSS: {{"CONDITIONALS_BOUNDARY", token.LEQ}, {"CONDITIONALS_NEGATION", token.GEQ}},
	token.LEQ: {{"CONDITIONALS_BOUNDARY", token.LSS}, {"CONDITIONALS_NEGATION", token.GTR}},
	token.EQL: {{"CONDITIONALS_NEGATION", token.NEQ}},
	token.NEQ: {{"CONDITIONALS_NEGATION", token.EQL}},
}

// Discover lists every mutant in the non-test Go files of dir (relative to root)
// that are part of the build on this platform. only, when not empty, names the
// files (without .go) to take.
func Discover(root, dir string, only []string) ([]Mutant, error) {
	abs := filepath.Join(root, filepath.FromSlash(dir))
	entries, err := os.ReadDir(abs)
	if err != nil {
		return nil, err
	}
	keep := map[string]bool{}
	for _, n := range only {
		keep[n] = true
	}
	var out []Mutant
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if len(keep) > 0 && !keep[strings.TrimSuffix(name, ".go")] {
			continue
		}
		// A file for another platform (or behind a tag) is not compiled here: its
		// mutants could only ever read "not covered".
		if ok, err := build.Default.MatchFile(abs, name); err != nil || !ok {
			continue
		}
		src, err := os.ReadFile(filepath.Join(abs, name))
		if err != nil {
			return nil, err
		}
		ms, err := discoverFile(dir+"/"+name, src)
		if err != nil {
			return nil, err
		}
		out = append(out, ms...)
	}
	return out, nil
}

func discoverFile(rel string, src []byte) ([]Mutant, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, rel, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	if ast.IsGenerated(file) {
		return nil, nil
	}
	lines := bytes.Split(src, []byte("\n"))
	var out []Mutant
	add := func(fn string, pos token.Pos, typ string, from, to token.Token) {
		p := fset.Position(pos)
		out = append(out, Mutant{
			File: rel, Line: p.Line, Col: p.Column, Type: typ, From: from.String(), To: to.String(),
			Func: fn, Source: strings.TrimSpace(string(lines[p.Line-1])), offset: p.Offset,
		})
	}
	for _, decl := range file.Decls {
		fn := "<package>"
		if fd, ok := decl.(*ast.FuncDecl); ok {
			fn = funcName(fd)
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.BinaryExpr:
				for _, m := range binaryMutations[n.Op] {
					add(fn, n.OpPos, m.typ, n.Op, m.to)
				}
			case *ast.IncDecStmt:
				to := token.DEC
				if n.Tok == token.DEC {
					to = token.INC
				}
				add(fn, n.TokPos, "INCREMENT_DECREMENT", n.Tok, to)
			case *ast.UnaryExpr:
				if n.Op == token.SUB {
					add(fn, n.OpPos, "INVERT_NEGATIVES", token.SUB, token.ADD)
				}
			}
			return true
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].offset != out[j].offset {
			return out[i].offset < out[j].offset
		}
		return out[i].Type < out[j].Type
	})
	seen := map[string]int{}
	for i := range out {
		k := out[i].Func + "\x00" + out[i].Type
		seen[k]++
		out[i].Ord = seen[k]
	}
	return out, nil
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	return "(" + typeText(fd.Recv.List[0].Type) + ")." + fd.Name.Name
}

func typeText(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.StarExpr:
		return "*" + typeText(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.IndexExpr:
		return typeText(e.X)
	case *ast.IndexListExpr:
		return typeText(e.X)
	}
	return "?"
}

// Apply returns src with the mutant's token swapped.
func (m Mutant) Apply(src []byte) ([]byte, error) {
	end := m.offset + len(m.From)
	if m.offset < 0 || end > len(src) || string(src[m.offset:end]) != m.From {
		return nil, fmt.Errorf("%s: the file no longer has %q there", m.At(), m.From)
	}
	out := make([]byte, 0, len(src)+1)
	out = append(out, src[:m.offset]...)
	out = append(out, m.To...)
	out = append(out, src[end:]...)
	return out, nil
}
