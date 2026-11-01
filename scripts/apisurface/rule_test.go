package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The CHANGELOG rule of `task api` (scripts/api-rule.sh), run in throwaway
// repositories. The fault it is for: CI checked out two commits and the rule
// read the last commit that changed the surface; at the edge of a shallow clone
// that commit's diff is the whole CHANGELOG, so any old "server API" line read
// as added and a surface change with no line passed.
func TestChangelogRule(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the rule is a bash script; the gate runs on Linux and macOS")
	}
	for _, tool := range []string{"git", "bash"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed", tool)
		}
	}
	rule, err := filepath.Abs(filepath.Join("..", "api-rule.sh"))
	if err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	run := func(dir, name string, args ...string) (string, error) {
		cmd := exec.Command(name, args...)
		cmd.Dir = dir
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	git := func(dir string, args ...string) {
		t.Helper()
		if out, err := run(dir, "git", args...); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	write := func(dir, rel, body string) {
		t.Helper()
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A repository whose first commit pins the surface with its line, as ours did.
	fresh := func() string {
		dir := t.TempDir()
		git(dir, "init", "-q", "-b", "main")
		write(dir, "testdata/api/server.txt", "func New() *Server\n")
		write(dir, "CHANGELOG.md", "# Changelog\n\n- server API: the exported surface is now pinned.\n")
		git(dir, "add", "-A")
		git(dir, "commit", "-q", "-m", "pin the surface")
		return dir
	}
	commit := func(dir, surface, changelogLine, msg string) {
		t.Helper()
		if surface != "" {
			write(dir, "testdata/api/server.txt", surface)
		}
		if changelogLine != "" {
			old, _ := os.ReadFile(filepath.Join(dir, "CHANGELOG.md"))
			write(dir, "CHANGELOG.md", string(old)+changelogLine+"\n")
		}
		git(dir, "add", "-A")
		git(dir, "commit", "-q", "--allow-empty", "-m", msg)
	}
	expect := func(name, dir string, pass bool, say string) {
		t.Helper()
		out, err := run(dir, "bash", rule)
		if pass != (err == nil) || !strings.Contains(out, say) {
			t.Errorf("%s: pass=%v, want pass=%v and output containing %q:\n%s", name, err == nil, pass, say, out)
		}
	}

	t.Run("a change with its line passes", func(t *testing.T) {
		dir := fresh()
		commit(dir, "func New() *Server\nfunc Open() *Server\n", "- server API: `Open` is new.", "add Open")
		expect("committed", dir, true, "each of the 2 commits")
	})

	t.Run("a change without a line fails, in a full and in a shallow clone", func(t *testing.T) {
		dir := fresh()
		commit(dir, "func New() *Server\nfunc Open() *Server\n", "", "add Open, say nothing")
		expect("full history", dir, false, "without adding a CHANGELOG.md line")

		// What CI had: the last two commits, and then only the last one.
		for _, depth := range []string{"2", "1"} {
			clone := filepath.Join(t.TempDir(), "clone")
			git(dir, "clone", "-q", "--depth", depth, "file://"+dir, clone)
			expect("clone --depth "+depth, clone, false, "shallow")
		}
	})

	t.Run("a later line does not excuse an earlier change", func(t *testing.T) {
		dir := fresh()
		commit(dir, "func New() *Server\nfunc Open() *Server\n", "", "add Open, say nothing")
		commit(dir, "func New() *Server\nfunc Open() *Server\nfunc Close()\n", "- server API: `Close` is new.", "add Close")
		expect("two commits", dir, false, "without adding a CHANGELOG.md line")
	})

	t.Run("a line about something else is not the line", func(t *testing.T) {
		dir := fresh()
		commit(dir, "func New() *Server\nfunc Open() *Server\n", "- Console: a new button.", "add Open, wrong line")
		expect("wrong line", dir, false, "without adding a CHANGELOG.md line")
	})

	t.Run("the working tree is held to the same rule", func(t *testing.T) {
		dir := fresh()
		write(dir, "testdata/api/server.txt", "func New() *Server\nfunc Open() *Server\n")
		expect("uncommitted, no line", dir, false, "is modified but CHANGELOG.md has no added line")
		old, _ := os.ReadFile(filepath.Join(dir, "CHANGELOG.md"))
		write(dir, "CHANGELOG.md", string(old)+"- server API: `Open` is new.\n")
		expect("uncommitted, with its line", dir, true, "ok")
	})

	t.Run("a commit that does not touch the surface needs no line", func(t *testing.T) {
		dir := fresh()
		commit(dir, "", "- Console: a new button.", "console")
		expect("unrelated", dir, true, "each of the 1 commits")
	})
}
