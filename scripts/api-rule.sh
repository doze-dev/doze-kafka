#!/usr/bin/env bash
# The CHANGELOG rule of `task api`, on its own so it can be tested in a
# throwaway repository (scripts/apisurface/rule_test.go):
#
#   every commit that changes the surface file also adds a line to CHANGELOG.md
#   that contains "server API"; and so does a working tree that changes it.
#
#   scripts/api-rule.sh [surface-file]     run in the repository to check
#
# Every commit, not the last one: a later commit with a line does not excuse an
# earlier one without. That needs the history, so a shallow clone FAILS here
# rather than passing on what it cannot see (in a shallow clone the boundary
# commit's diff is the whole file, CHANGELOG included, and the rule would read
# every old line as added). CI checks out with fetch-depth: 0.
set -euo pipefail
surface=${1:-testdata/api/server.txt}
has_line() { grep -qi '^+.*server API'; }

git rev-parse --git-dir >/dev/null 2>&1 || { echo "not a git repository: the CHANGELOG rule needs the history of $surface" >&2; exit 1; }
if [ "$(git rev-parse --is-shallow-repository)" = true ]; then
  echo "this clone is shallow: the CHANGELOG rule reads every commit that changed $surface and cannot." >&2
  echo "Fetch the history (git fetch --unshallow; in a workflow, actions/checkout with fetch-depth: 0)." >&2
  exit 1
fi

fail=0
if [ -n "$(git status --porcelain -- "$surface")" ]; then
  # Uncommitted (new or modified): the same working tree must add the line.
  git diff HEAD -- CHANGELOG.md | has_line || {
    echo "$surface is modified but CHANGELOG.md has no added line containing \"server API\"." >&2; fail=1; }
fi
empty_tree=$(git hash-object -t tree /dev/null)
n=0
for c in $(git log --format=%H -- "$surface"); do
  n=$((n + 1))
  parent=$(git rev-parse -q --verify "$c^" 2>/dev/null || echo "$empty_tree")
  git diff "$parent" "$c" -- CHANGELOG.md | has_line || {
    echo "commit ${c:0:9} changed $surface without adding a CHANGELOG.md line containing \"server API\"." >&2; fail=1; }
done
[ "$fail" = 0 ] || exit 1
echo "ok       CHANGELOG rule: each of the $n commits that changed $surface added a \"server API\" line"
