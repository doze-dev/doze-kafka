#!/usr/bin/env bash
# Statement coverage per package against the committed floor (testdata/coverage.txt).
#
#   scripts/coverage.sh          fail if a package fell more than 0.1 point below its floor
#   scripts/coverage.sh record   write the current numbers as the new floor
#
# A floor only goes up: `record` refuses to lower any package unless
# ALLOW_LOWER=1 is set (and the commit message then says why).
set -euo pipefail
cd "$(dirname "$0")/.."
floor=testdata/coverage.txt
mode=${1:-check}

now=$(mktemp)
trap 'rm -f "$now"' EXIT
# -short is not used: the floor is for the full suite. Packages with no test
# files print "[no test files]" and are skipped.
go test -cover ./... 2>&1 | awk '
  /^ok / && /coverage:/ {
    for (i = 1; i <= NF; i++) if ($i == "coverage:") { pct = $(i+1); sub(/%/, "", pct) }
    if (pct !~ /^[0-9.]+$/) next   # "[no statements]": a package that is only tests or a build-tagged seam.
    pkg = $2; sub(/^github.com\/doze-dev\/doze-kafka\/?/, "", pkg); if (pkg == "") pkg = "."
    printf "%s %s\n", pkg, pct
  }' | sort > "$now"

if [ ! -s "$now" ]; then echo "no coverage numbers produced (did the tests fail?)" >&2; exit 1; fi

if [ "$mode" = record ]; then
  if [ -f "$floor" ] && [ "${ALLOW_LOWER:-0}" != 1 ]; then
    lowered=$(join <(sort "$floor") "$now" | awk '$3 + 0 < $2 + 0 - 0.1 { printf "%s %s -> %s\n", $1, $2, $3 }')
    if [ -n "$lowered" ]; then
      echo "refusing to lower the floor (ALLOW_LOWER=1 to override):" >&2; echo "$lowered" >&2; exit 1
    fi
  fi
  mkdir -p testdata
  cp "$now" "$floor"
  echo "recorded $(wc -l < "$floor") packages in $floor"
  exit 0
fi

fail=0
while read -r pkg want; do
  got=$(awk -v p="$pkg" '$1 == p { print $2 }' "$now")
  if [ -z "$got" ]; then echo "MISSING  $pkg (floor $want%): no coverage reported" >&2; fail=1; continue; fi
  if awk -v g="$got" -v w="$want" 'BEGIN { exit !(g + 0 < w - 0.1) }'; then
    echo "FELL     $pkg: $got% < floor $want%" >&2; fail=1
  else
    echo "ok       $pkg: $got% (floor $want%)"
  fi
done < "$floor"
# A package with tests and no floor entry is new: say so, do not fail.
while read -r pkg got; do
  grep -q "^$pkg " "$floor" || echo "NEW      $pkg: $got% (run: task coverage:record)"
done < "$now"
exit $fail
