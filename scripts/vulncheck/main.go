// Command vulncheck runs govulncheck over this module (and any other module directory
// named, such as module/) and fails on every vulnerability the code can reach, except
// the ones accepted in testdata/vulncheck-accepted.txt, each of which names the test
// that keeps hostile input away from the vulnerable code and says why there is nothing
// to upgrade to.
//
//	go run ./scripts/vulncheck . module
//
// An accepted entry is not a way to look away:
//
//   - it stops being accepted the day the advisory has a fixed version (upgrade);
//   - it fails when govulncheck no longer reports it (delete the entry);
//   - it fails when the test it names is no longer in the tree.
//
// So the list is exactly the reachable vulnerabilities that cannot be fixed by an
// upgrade today, and every one of them is held by a test.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Scanner is the govulncheck that is run: a pinned release, fetched and verified by the
// go command, and not a dependency of this module (it would add its own to the list
// scripts/lightness.sh counts).
const Scanner = "golang.org/x/vuln/cmd/govulncheck@v1.9.0"

const acceptedFile = "testdata/vulncheck-accepted.txt"

// Accepted is one line of the accepted file.
type Accepted struct {
	ID     string // GO-2026-5046
	Test   string // the test that guards it, as package-directory.TestName
	Reason string
}

// ReadAccepted parses the accepted file: "ID test reason...", '#' starts a comment.
func ReadAccepted(r io.Reader) ([]Accepted, error) {
	var out []Accepted
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 3 || !strings.HasPrefix(f[0], "GO-") || !strings.Contains(f[1], ".Test") {
			return nil, fmt.Errorf("line %d: want \"GO-yyyy-nnnn dir.TestName the reason\", got %q", n, line)
		}
		out = append(out, Accepted{ID: f[0], Test: f[1], Reason: strings.Join(f[2:], " ")})
	}
	return out, sc.Err()
}

// Finding is what govulncheck says about one advisory in one scan.
type Finding struct {
	ID      string
	Summary string
	Module  string // the module the vulnerable code is in, with its version.
	Fixed   string // the version that fixes it, "" when there is none.
	Called  bool   // this module's code reaches the vulnerable package's code.
	Example string // one place it is called from.
}

// ParseScan reads govulncheck's -format json output.
func ParseScan(r io.Reader) ([]Finding, error) {
	type frame struct {
		Module, Version, Package, Function, Receiver string
		Position                                     *struct {
			Filename string
			Line     int
		}
	}
	type message struct {
		OSV *struct {
			ID      string `json:"id"`
			Summary string `json:"summary"`
		} `json:"osv"`
		Finding *struct {
			OSV          string  `json:"osv"`
			FixedVersion string  `json:"fixed_version"`
			Trace        []frame `json:"trace"`
		} `json:"finding"`
	}
	summaries := map[string]string{}
	byID := map[string]*Finding{}
	dec := json.NewDecoder(r)
	for {
		var m message
		if err := dec.Decode(&m); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("govulncheck's output is not the JSON it documents: %w", err)
		}
		if m.OSV != nil {
			summaries[m.OSV.ID] = m.OSV.Summary
		}
		if m.Finding == nil || len(m.Finding.Trace) == 0 {
			continue
		}
		f := byID[m.Finding.OSV]
		if f == nil {
			f = &Finding{ID: m.Finding.OSV}
			byID[f.ID] = f
		}
		top := m.Finding.Trace[0]
		f.Module = strings.TrimSuffix(top.Module+"@"+top.Version, "@")
		f.Fixed = m.Finding.FixedVersion
		// A trace that begins in a function is a call path from this module's code; one
		// that names only a module or a package says it is required or imported.
		if top.Function != "" && !f.Called {
			f.Called = true
			last := m.Finding.Trace[len(m.Finding.Trace)-1]
			if last.Position != nil {
				f.Example = fmt.Sprintf("%s:%d", last.Position.Filename, last.Position.Line)
			}
		}
	}
	var out []Finding
	for _, f := range byID {
		f.Summary = summaries[f.ID]
		out = append(out, *f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Judge is the verdict on a scan: what fails it, and what is accepted (to be printed,
// every time, so an accepted vulnerability is never out of sight).
func Judge(found []Finding, accepted []Accepted, testExists func(string) bool) (problems, notes []string) {
	by := map[string]Accepted{}
	for _, a := range accepted {
		if _, dup := by[a.ID]; dup {
			problems = append(problems, fmt.Sprintf("%s is accepted twice in %s", a.ID, acceptedFile))
		}
		by[a.ID] = a
	}
	seen := map[string]bool{}
	for _, f := range found {
		if !f.Called {
			notes = append(notes, fmt.Sprintf("%s in %s: required, and nothing here calls the vulnerable package (%s)", f.ID, f.Module, f.Summary))
			continue
		}
		seen[f.ID] = true
		a, ok := by[f.ID]
		switch {
		case !ok && f.Fixed != "":
			problems = append(problems, fmt.Sprintf("%s in %s, reached from %s: %s. Fixed in %s: upgrade.", f.ID, f.Module, f.Example, f.Summary, f.Fixed))
		case !ok:
			problems = append(problems, fmt.Sprintf("%s in %s, reached from %s: %s. No fixed version: keep hostile input away from it, with a test, and accept it in %s.",
				f.ID, f.Module, f.Example, f.Summary, acceptedFile))
		case f.Fixed != "":
			problems = append(problems, fmt.Sprintf("%s in %s is accepted, and is now fixed in %s: upgrade and delete the entry.", f.ID, f.Module, f.Fixed))
		case !testExists(a.Test):
			problems = append(problems, fmt.Sprintf("%s is accepted because %s guards it, and there is no such test.", f.ID, a.Test))
		default:
			notes = append(notes, fmt.Sprintf("%s in %s: ACCEPTED, no fixed version. Guarded by %s. %s", f.ID, f.Module, a.Test, a.Reason))
		}
	}
	for _, a := range accepted {
		if !seen[a.ID] {
			problems = append(problems, fmt.Sprintf("%s is accepted in %s and govulncheck no longer reports it as reachable: delete the entry.", a.ID, acceptedFile))
		}
	}
	return problems, notes
}

var testFunc = regexp.MustCompile(`(?m)^func (Test\w+)\(`)

// testsIn lists dir.TestName for every test function under root.
func testsIn(root string) (map[string]bool, error) {
	out := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (strings.HasPrefix(d.Name(), ".") && path != root || d.Name() == "node_modules") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		dir, _ := filepath.Rel(root, filepath.Dir(path))
		for _, m := range testFunc.FindAllSubmatch(src, -1) {
			out[filepath.ToSlash(dir)+"."+string(m[1])] = true
		}
		return nil
	})
	return out, err
}

func scan(dir string) ([]Finding, error) {
	cmd := exec.Command("go", "run", Scanner, "-format", "json", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		// The database could not be reached, or the code does not build: not a pass.
		return nil, fmt.Errorf("govulncheck in %s: %w\n%s", dir, err, stderr.String())
	}
	return ParseScan(bytes.NewReader(out))
}

func main() {
	dirs := os.Args[1:]
	if len(dirs) == 0 {
		dirs = []string{"."}
	}
	raw, err := os.ReadFile(acceptedFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	accepted, err := ReadAccepted(bytes.NewReader(raw))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", acceptedFile, err)
		os.Exit(2)
	}
	tests, err := testsIn(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	// One verdict over every directory: an advisory reached from any of them is found.
	merged := map[string]Finding{}
	for _, dir := range dirs {
		found, err := scan(dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		for _, f := range found {
			if old, ok := merged[f.ID]; !ok || (f.Called && !old.Called) {
				merged[f.ID] = f
			}
		}
		fmt.Printf("scanned  %s (%s)\n", dir, Scanner)
	}
	var all []Finding
	for _, f := range merged {
		all = append(all, f)
	}
	slices.SortFunc(all, func(a, b Finding) int { return strings.Compare(a.ID, b.ID) })
	problems, notes := Judge(all, accepted, func(name string) bool { return tests[name] })
	for _, n := range notes {
		fmt.Println("note     " + n)
	}
	for _, p := range problems {
		fmt.Fprintln(os.Stderr, "FAIL     "+p)
	}
	if len(problems) > 0 {
		os.Exit(1)
	}
	fmt.Printf("ok       no reachable vulnerability outside the %d accepted\n", len(accepted))
}
