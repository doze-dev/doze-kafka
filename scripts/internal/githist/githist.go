// Package githist reads every change a file went through in git, for the gates
// whose baseline may only move one way without a written reason (coverage
// floors, lightness budgets). A baseline that can be edited by hand to make the
// gate pass is no gate; these gates therefore hold the file's own history
// against the reasons the file gives.
package githist

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Change is one step in the file's history: what it was in a parent commit and
// what the commit made it. Commit is "" for the working tree against HEAD.
type Change struct {
	Commit string // abbreviated; "" means the working tree.
	Date   string // YYYY-MM-DD of the commit; "" for the working tree.
	Old    []byte // nil when the parent did not have the file.
	New    []byte // nil when the commit deleted it.
}

// Where names a change for a message.
func (c Change) Where() string {
	if c.Commit == "" {
		return "the working tree"
	}
	return "commit " + c.Commit
}

func git(repo string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// Changes returns every change of file (a slash path relative to repo), oldest
// first, the working tree last: for each commit that has the file different
// from one of its parents, one Change per such parent. A merge is held against
// each parent, so a floor that one side had and the merge dropped is seen.
//
// A shallow clone is an error: its history ends at a commit whose parents are
// missing, and a gate that reads "nothing was ever lowered" from that would
// pass on what it cannot see.
func Changes(repo, file string) ([]Change, error) {
	if _, err := git(repo, "rev-parse", "--git-dir"); err != nil {
		return nil, fmt.Errorf("not a git repository (%v): the history of %s cannot be read", err, file)
	}
	shallow, err := git(repo, "rev-parse", "--is-shallow-repository")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(shallow)) == "true" {
		return nil, fmt.Errorf("this clone is shallow: the history of %s cannot be read. Fetch it (git fetch --unshallow; in a workflow, actions/checkout with fetch-depth: 0)", file)
	}
	var out []Change
	if _, err := git(repo, "rev-parse", "--verify", "-q", "HEAD"); err == nil {
		log, err := git(repo, "log", "--full-history", "--reverse", "--topo-order", "--date=short", "--format=%h %ad %P", "--", file)
		if err != nil {
			return nil, err
		}
		blob := func(rev string) []byte {
			b, err := git(repo, "show", rev+":"+file)
			if err != nil {
				return nil
			}
			return b
		}
		for _, line := range strings.Split(strings.TrimSpace(string(log)), "\n") {
			f := strings.Fields(line)
			if len(f) < 2 {
				continue
			}
			commit, date, parents := f[0], f[1], f[2:]
			now := blob(commit)
			if len(parents) == 0 {
				out = append(out, Change{Commit: commit, Date: date, New: now})
				continue
			}
			for _, p := range parents {
				old := blob(p)
				if !bytes.Equal(old, now) {
					out = append(out, Change{Commit: commit, Date: date, Old: old, New: now})
				}
			}
		}
		head := blob("HEAD")
		work, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(file)))
		if err != nil {
			work = nil
		}
		if !bytes.Equal(head, work) {
			out = append(out, Change{Old: head, New: work})
		}
	}
	return out, nil
}
