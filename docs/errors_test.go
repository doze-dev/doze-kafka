package docs

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// What the documents say the program prints, logs or answers must be what it does
// (foundation E14). Three kinds of claim are found and held to the code:
//
//  1. An error name in code (`KAFKA_STORAGE_ERROR`): every upper-case, underscored
//     code span in a user document is an error name Kafka defines (protocol.ErrorName
//     knows it), or a string the program's source has (an environment variable it
//     reads or prints, a security protocol it parses).
//  2. A message in code in a sentence about what the program says (`did you mean
//     --listen?` after "an error"): a code span of three words or more, in the same
//     sentence as and after one of prints, logs, says, answers, refuses, fails with,
//     error, message, warns, asks, is in a string literal of the program's source (a
//     %-verb there, and a <placeholder> in the document, stand for anything).
//  3. A log line in a fence (level=... msg=...): its msg is in the source.
//
// The strings of the source are its string literals outside tests, and the text of
// the console's templates.

var (
	upperSnake = regexp.MustCompile(`^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$`)
	// Only the forms that say what the program does: "print with --x" is an
	// instruction to the reader, not a claim about output.
	sayingVerb = regexp.MustCompile(`(?i)\b(prints|printed|logs|logged|says|said|answers|answered|refuses|refused|fails with|failed with|errors?|messages?|warns|warned|asks|asked|reports|reported)\b`)
	// An upper-case span is an error name when its sentence is about an error.
	errorVerb   = regexp.MustCompile(`(?i)\b(answers?|answered|errors?|refus\w*|fails?|failed|returns?|returned|gets|rejects?|rejected|code)\b`)
	logMsg      = regexp.MustCompile(`\blevel=[A-Z]+\s+msg=("(?:[^"\\]|\\.)*"|\S+)`)
	placeholder = regexp.MustCompile(`<[^<>\s]+>`)
	formatVerb  = regexp.MustCompile(`%[-+# 0-9.*]*[a-zA-Z]`)
)

// sourceStrings is every string literal in the program's non-test Go source, and the
// text of every console template.
var sourceStrings = sync.OnceValues(func() ([]string, error) {
	root, err := filepath.Abs("..")
	if err != nil {
		return nil, err
	}
	var out []string
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		if e.IsDir() {
			if rel != "." && (strings.HasPrefix(e.Name(), ".") || e.Name() == "node_modules" || e.Name() == "testdata" || isNotDocument(filepath.ToSlash(rel)+"/x")) {
				return filepath.SkipDir
			}
			return nil
		}
		switch {
		case strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go"):
			f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(f, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if s, err := strconv.Unquote(lit.Value); err == nil {
						out = append(out, s)
					}
				}
				return true
			})
		case strings.HasPrefix(filepath.ToSlash(rel), "console/templates/"), strings.HasSuffix(p, ".sh"):
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			out = append(out, string(b))
		}
		return nil
	})
	return out, err
})

// inSource reports whether claimed text is in one of the source's strings: whether
// some text the program can print contains it. In the claim a <placeholder> stands
// for anything; in a source string a %-verb does (what the program fills in).
func inSource(claim string, strs []string) bool {
	claim = strings.Join(strings.Fields(claim), " ")
	// The longest run of the claim that is not a placeholder: a string that has none
	// of it, nor a %-verb, cannot hold the claim.
	longest := ""
	for _, p := range placeholder.Split(claim, -1) {
		if len(p) > len(longest) {
			longest = p
		}
	}
	for _, s := range strs {
		flat := strings.Join(strings.Fields(s), " ")
		if !strings.Contains(flat, longest) && !strings.Contains(flat, "%") {
			continue
		}
		if matchesWithin(claim, flat) {
			return true
		}
	}
	return false
}

// matchesWithin reports whether claim matches a contiguous part of some expansion of
// format: a %-verb in format matches one or more characters of the claim, a
// <placeholder> in the claim matches any run of format.
func matchesWithin(claim, format string) bool {
	type tok struct {
		c    byte
		verb bool
	}
	var f []tok
	for i := 0; i < len(format); {
		if loc := formatVerb.FindStringIndex(format[i:]); loc != nil && loc[0] == 0 && format[i:i+2] != "%%" {
			f = append(f, tok{verb: true})
			i += loc[1]
			continue
		}
		f = append(f, tok{c: format[i]})
		if strings.HasPrefix(format[i:], "%%") {
			i++
		}
		i++
	}
	var c []tok // verb here means a placeholder of the claim
	for i := 0; i < len(claim); {
		if loc := placeholder.FindStringIndex(claim[i:]); loc != nil && loc[0] == 0 {
			c = append(c, tok{verb: true})
			i += loc[1]
			continue
		}
		c = append(c, tok{c: claim[i]})
		i++
	}
	// best(a, b) is the most characters of claim[a:] that can be matched to literal
	// characters of the format from token b on, or -1 when it cannot be matched there.
	// A verb can stand for a word of the claim, not for the claim: what was matched
	// literally must be most of it (see below).
	memo := map[[2]int]int{}
	var best func(a, b int) int
	best = func(a, b int) int {
		if a == len(c) {
			return 0 // the claim is used up: it is inside this expansion.
		}
		if b == len(f) {
			return -1
		}
		k := [2]int{a, b}
		if v, ok := memo[k]; ok {
			return v
		}
		r := -1
		switch {
		case c[a].verb:
			for b2 := b + 1; b2 <= len(f); b2++ {
				if a+1 == len(c) {
					r = max(r, 0)
					break
				}
				r = max(r, best(a+1, b2))
			}
		case f[b].verb:
			for a2 := a + 1; a2 <= len(c); a2++ {
				if a2 == len(c) {
					r = max(r, 0)
				} else {
					r = max(r, best(a2, b+1))
				}
			}
		case c[a].c == f[b].c:
			if v := best(a+1, b+1); v >= 0 {
				r = v + 1
			}
		}
		memo[k] = r
		return r
	}
	literal := 0
	for _, t := range c {
		if !t.verb {
			literal++
		}
	}
	for b := 0; b < len(f); b++ {
		if v := best(0, b); v >= 0 && v*10 >= literal*6 {
			return true
		}
	}
	return false
}

// A claim is one thing a document says the program prints or answers.
type claim struct {
	doc  string
	line int
	kind string // "error name", "message", "log line"
	text string
}

// claims finds the claims of one document.
func claims(name, md string) []claim {
	var out []claim
	prose := blankFences(md)
	for _, s := range codeSpans(prose) {
		switch {
		case upperSnake.MatchString(s.text):
			if errorVerb.MatchString(sentenceBefore(prose, s.start)) {
				out = append(out, claim{name, s.line, "error name", s.text})
			}
		case len(strings.Fields(s.text)) >= 3 && !strings.HasPrefix(s.text, "-") && !strings.ContainsAny(s.text[:1], "$./{[(") && !isCommand(s.text) &&
			sayingVerb.MatchString(sentenceBefore(prose, s.start)):
			out = append(out, claim{name, s.line, "message", s.text})
		}
	}
	for _, b := range fencedBlocks(md) {
		for i, l := range b.lines {
			if m := logMsg.FindStringSubmatch(l); m != nil {
				msg := m[1]
				if u, err := strconv.Unquote(msg); err == nil {
					msg = u
				}
				out = append(out, claim{name, b.line + 1 + i, "log line", msg})
			}
		}
	}
	return out
}

// isCommand reports whether a span is a command for the reader to type (`config
// print --show-secrets`), not something the program says.
func isCommand(s string) bool {
	switch strings.Fields(s)[0] {
	case "doze-kafka", "./doze-kafka", "config", "env", "doctor", "dns-setup", "kcat", "go", "curl", "docker":
		return true
	}
	return false
}

// sentenceBefore is the text of the sentence a position is in, up to the position.
func sentenceBefore(prose string, at int) string {
	start := strings.LastIndexAny(prose[:at], ".!?\n")
	// A line break inside a paragraph does not end a sentence; a blank line does.
	if p := strings.LastIndex(prose[:at], "\n\n"); p > start {
		start = p
	} else if start >= 0 && prose[start] == '\n' {
		if q := strings.LastIndexAny(prose[:start], ".!?"); q > strings.LastIndex(prose[:start], "\n\n") {
			start = q
		} else {
			start = strings.LastIndex(prose[:start], "\n\n")
		}
	}
	if start < 0 {
		start = 0
	}
	return prose[start:at]
}

// errorNames is every error name Kafka defines, as the program names them.
func errorNames() map[string]bool {
	names := map[string]bool{}
	for code := int16(-1); code < 200; code++ {
		if n := protocol.ErrorName(code); n != "" && !strings.HasPrefix(n, "UNKNOWN_ERROR_CODE") {
			names[n] = true
		}
	}
	return names
}

// checkClaims reports each claim the program does not make.
func checkClaims(cs []claim, errs map[string]bool, strs []string) []string {
	var bad []string
	for _, c := range cs {
		ok := false
		switch c.kind {
		case "error name":
			ok = errs[c.text] || inSource(c.text, strs)
		default:
			ok = inSource(c.text, strs)
		}
		if !ok {
			bad = append(bad, fmt.Sprintf("%s:%d: the %s %q is not one the program has (no error name, and in no string of its source)", c.doc, c.line, c.kind, c.text))
		}
	}
	return bad
}

func TestWhatTheDocumentsSayTheProgramPrintsIsWhatItPrints(t *testing.T) {
	strs, err := sourceStrings()
	if err != nil {
		t.Fatal(err)
	}
	errs := errorNames()
	if !errs["KAFKA_STORAGE_ERROR"] || !errs["PRODUCER_FENCED"] || len(errs) < 100 {
		t.Fatalf("protocol.ErrorName knows %d names: the list is broken", len(errs))
	}
	docs := userDocs(t)
	kinds := map[string]int{}
	for _, name := range sortedKeys(docs) {
		cs := claims(name, docs[name])
		for _, c := range cs {
			kinds[c.kind]++
		}
		for _, b := range checkClaims(cs, errs, strs) {
			t.Error(b)
		}
	}
	t.Logf("claims checked: %v", kinds)
	if kinds["error name"] < 12 || kinds["message"] < 4 || kinds["log line"] < 1 {
		t.Errorf("found too few claims (%v): the finder is broken", kinds)
	}
}

// The check's own test: a document with a true and a false claim of each kind.
func TestTheClaimCheckFindsAFalseClaimOfEachKind(t *testing.T) {
	strs := []string{"unknown flag %q: did you mean --listen?", "shutting down: flushing logs to disk", "KAFKA_BOOTSTRAP_SERVERS"}
	errs := map[string]bool{"KAFKA_STORAGE_ERROR": true}
	md := strings.Join([]string{
		"A full disk answers every write `KAFKA_STORAGE_ERROR`, and `doze-kafka env` sets `KAFKA_BOOTSTRAP_SERVERS`.",
		"A full disk answers `KAFKA_STORAGE_EROR` in the old build.",
		"A mistyped flag is an error: `did you mean --listen?`.",
		"A mistyped flag is an error: `did you mean to type --listen?`.",
		"The flag `--data-dir path to use` is not a claim: no saying verb is in its sentence.",
		"",
		"```",
		`level=INFO msg="shutting down: flushing logs to disk"`,
		`level=INFO msg="shutting down for good"`,
		"```",
	}, "\n")
	bad := checkClaims(claims("t.md", md), errs, strs)
	want := []string{"KAFKA_STORAGE_EROR", "did you mean to type --listen?", "shutting down for good"}
	if len(bad) != len(want) {
		t.Errorf("want %d false claims, got %d: %q", len(want), len(bad), bad)
	}
	for _, w := range want {
		found := false
		for _, b := range bad {
			found = found || strings.Contains(b, w)
		}
		if !found {
			t.Errorf("the false claim %q was not reported: %q", w, bad)
		}
	}
}
