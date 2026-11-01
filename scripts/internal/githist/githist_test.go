package githist

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestChangesReadsEveryStepAMergeIncludedAndRefusesAShallowClone(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	dir := t.TempDir()
	git := func(in string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = in
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
			"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	write := func(body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(dir, "testdata"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "testdata", "f.txt"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	steps := func() string {
		t.Helper()
		cs, err := Changes(dir, "testdata/f.txt")
		if err != nil {
			t.Fatal(err)
		}
		var s []string
		for _, c := range cs {
			where := "commit"
			if c.Commit == "" {
				where = "tree"
			}
			s = append(s, where+":"+strings.TrimSpace(string(c.Old))+">"+strings.TrimSpace(string(c.New)))
		}
		return strings.Join(s, " ")
	}

	git(dir, "init", "-q", "-b", "main")
	if got := steps(); got != "" {
		t.Errorf("a repository with no commit has no history: %q", got)
	}
	write("90\n")
	git(dir, "add", "-A")
	git(dir, "commit", "-q", "-m", "one")
	// A side branch lowers it; main raises it; the merge takes the side's.
	git(dir, "checkout", "-q", "-b", "side")
	write("80\n")
	git(dir, "commit", "-q", "-am", "side")
	git(dir, "checkout", "-q", "main")
	write("95\n")
	git(dir, "commit", "-q", "-am", "main")
	git(dir, "merge", "-q", "-s", "ours", "--no-commit", "side")
	write("80\n")
	git(dir, "commit", "-q", "-am", "merge, taking the side's value")
	write("70\n") // and the working tree goes lower still.

	got := steps()
	for _, want := range []string{"commit:>90", "commit:90>80", "commit:90>95", "commit:95>80", "tree:80>70"} {
		if !strings.Contains(got, want) {
			t.Errorf("history %q has no step %q", got, want)
		}
	}
	if strings.Count(got, ":") != 5 {
		t.Errorf("want 5 steps (the merge only against the parent it differs from), got %q", got)
	}

	git(dir, "commit", "-q", "-am", "lower")
	clone := filepath.Join(t.TempDir(), "clone")
	git(dir, "clone", "-q", "--depth", "1", "file://"+dir, clone)
	if _, err := Changes(clone, "testdata/f.txt"); err == nil || !strings.Contains(err.Error(), "shallow") {
		t.Errorf("a shallow clone must be an error that says so, got %v", err)
	}
	if _, err := Changes(t.TempDir(), "testdata/f.txt"); err == nil {
		t.Error("a directory that is not a repository must be an error")
	}
}
