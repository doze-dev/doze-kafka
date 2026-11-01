#!/usr/bin/env bash
# Run every fuzz target in testdata/fuzz-targets.txt for FUZZ_TIME each (default 30s).
#
#   scripts/fuzz.sh [fuzztime] [name-regexp]
#
# go test refuses -fuzz when it matches more than one target, so one run per
# target. Every target is run even after one fails, and the exit code says if any did.
set -uo pipefail
cd "$(dirname "$0")/.."
time=${1:-${FUZZ_TIME:-30s}}
only=${2:-.}
export GOWORK=off
fail=0
n=0
while read -r pkg target; do
  case "$pkg" in ''|'#'*) continue ;; esac
  echo "$target" | grep -Eq "$only" || continue
  n=$((n + 1))
  echo "== $pkg $target ($time)"
  go test "$pkg" -run '^$' -fuzz "^${target}\$" -fuzztime "$time" || { echo "FAILED $pkg $target" >&2; fail=1; }
done < testdata/fuzz-targets.txt
[ "$n" -gt 0 ] || { echo "no target matches $only" >&2; exit 2; }
exit $fail
