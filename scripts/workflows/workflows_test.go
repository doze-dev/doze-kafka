// Package workflows holds the GitHub workflows to the rules that were found
// broken in them before they had ever run: a floating tag that does not exist
// (astral-sh/setup-uv@v10), a shallow checkout under a gate that reads history,
// a `| tee` that hid the exit status of what it piped, and Playwright retries
// that turned a flaky test green. Nothing here runs a workflow; it reads them.
package workflows

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func workflows(t *testing.T) map[string]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", ".github", "workflows", "*.yml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no workflows found: %v", err)
	}
	out := map[string]string{}
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		out[filepath.Base(p)] = string(raw)
	}
	return out
}

var usesLine = regexp.MustCompile(`^\s*(?:-\s+)?uses:\s*(\S+)(.*)$`)
var pinned = regexp.MustCompile(`^[\w.-]+/[\w./-]+@[0-9a-f]{40}$`)
var versionComment = regexp.MustCompile(`^\s+#\s+v\d+\.\d+\.\d+\s*$`)

// Every action is pinned to a commit, with the release it is in a comment. A tag
// moves (or, as setup-uv@v10 did, never existed); a commit does not.
func TestEveryActionIsPinnedToACommit(t *testing.T) {
	for name, body := range workflows(t) {
		for i, line := range strings.Split(body, "\n") {
			m := usesLine.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			if strings.HasPrefix(m[1], "./") {
				continue // a local action is this commit's own.
			}
			if !pinned.MatchString(m[1]) || !versionComment.MatchString(m[2]) {
				t.Errorf("%s:%d: %q is not <owner>/<repo>@<40-hex commit> # vX.Y.Z", name, i+1, strings.TrimSpace(line))
			}
		}
	}
}

// The steps that run a gate reading git history (the CHANGELOG rule of
// scripts/api.sh, the coverage floors, the lightness raises) fail in a shallow
// clone, so each job that runs one checks out the whole history.
func TestGatesThatReadHistoryHaveIt(t *testing.T) {
	gates := []string{"scripts/api.sh", "scripts/coverage.sh", "scripts/lightness.sh"}
	for name, body := range workflows(t) {
		for _, job := range jobs(body) {
			for _, g := range gates {
				if strings.Contains(job, g) && !strings.Contains(job, "fetch-depth: 0") {
					t.Errorf("%s: a job runs %s without fetch-depth: 0", name, g)
				}
			}
		}
	}
}

// A step whose run pipes into tee must run under bash with pipefail (GitHub's
// `shell: bash` is `bash -eo pipefail`; the default shell is `bash -e`, under which
// the step's status is tee's).
func TestATeedStepKeepsTheStatusOfWhatItPiped(t *testing.T) {
	for name, body := range workflows(t) {
		for _, step := range steps(body) {
			if strings.Contains(step, "| tee") && !strings.Contains(step, "shell: bash") {
				t.Errorf("%s: a step pipes into tee without shell: bash:\n%s", name, step)
			}
		}
	}
}

// Playwright in CI: a test that passes only on a retry fails the run, and the
// route gate cannot be switched off.
func TestBrowserSuiteNeitherRetriesToGreenNorSkipsTheRouteGate(t *testing.T) {
	cfg, err := os.ReadFile(filepath.Join("..", "..", "e2e", "playwright.config.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cfg), "failOnFlakyTests: !!process.env.CI") {
		t.Error("e2e/playwright.config.ts: retries in CI without failOnFlakyTests let a flaky test pass")
	}
	teardown, err := os.ReadFile(filepath.Join("..", "..", "e2e", "global-teardown.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(teardown), "E2E_ROUTE_GATE=0 is set in CI") {
		t.Error("e2e/global-teardown.ts: E2E_ROUTE_GATE=0 is not refused in CI")
	}
	e2e := workflows(t)["e2e.yml"]
	runs := strings.Count(e2e, "bunx playwright test")
	gated := strings.Count(e2e, "grep -q 'route gate: every console route was visited or driven'")
	if runs == 0 || runs != gated {
		t.Errorf("e2e.yml: %d Playwright passes, %d of them checked for the route gate's line", runs, gated)
	}
	if regexp.MustCompile(`E2E_ROUTE_GATE\s*[:=]`).MatchString(e2e) {
		t.Error("e2e.yml sets E2E_ROUTE_GATE")
	}
}

// Every module in the repository is tidy, in CI.
func TestCIChecksEveryModuleIsTidy(t *testing.T) {
	if !strings.Contains(workflows(t)["ci.yml"], "go mod tidy -diff") {
		t.Error("ci.yml does not run go mod tidy -diff")
	}
}

// jobs splits a workflow into its jobs' text (each from its two-space key to the next).
func jobs(body string) []string {
	i := strings.Index(body, "\njobs:\n")
	if i < 0 {
		return nil
	}
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(body[i+len("\njobs:\n"):], "\n") {
		if len(line) > 2 && line[:2] == "  " && line[2] != ' ' && line[2] != '#' {
			if cur.Len() > 0 {
				out = append(out, cur.String())
			}
			cur.Reset()
		}
		cur.WriteString(line + "\n")
	}
	return append(out, cur.String())
}

// steps splits a workflow into its steps' text (each from its "- " to the next).
func steps(body string) []string {
	var out []string
	var cur strings.Builder
	indent := -1
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimLeft(line, " ")
		lead := len(line) - len(trimmed)
		if strings.HasPrefix(trimmed, "- ") && (indent < 0 || lead <= indent) {
			if cur.Len() > 0 {
				out = append(out, cur.String())
			}
			cur.Reset()
			indent = lead
		} else if indent >= 0 && trimmed != "" && lead < indent {
			if cur.Len() > 0 {
				out = append(out, cur.String())
			}
			cur.Reset()
			indent = -1
		}
		if indent >= 0 {
			cur.WriteString(line + "\n")
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}
