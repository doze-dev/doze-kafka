#!/usr/bin/env bash
# Run every gate and write what each answered to a dated file under plan/evidence/,
# so a stage's close commit carries its gate results by tool, not by hand
# (plan/README.md: results are "recorded in the last commit").
#
#   scripts/gate-report.sh                 the push gates (minutes)
#   scripts/gate-report.sh --slow          ... and the mutation check and the compat
#                                          matrix (hours; Docker and every toolchain)
#   scripts/gate-report.sh --out FILE      write FILE instead of the dated one
#
# The file names the commit (and says when the tree had uncommitted changes), the
# platform and toolchain, and for each gate its command, exit status, time and the
# last lines it printed. The script exits non-zero when any gate failed; the file is
# written either way, because a failed gate is a result too.
#
# GATE_REPORT_GATES=<file> replaces the list (one "name<TAB>command" per line): the
# script's own test (scripts/gatereport) uses it to plant a failing gate.
set -uo pipefail
cd "$(dirname "$0")/.."
slow=0 out=""
while [ $# -gt 0 ]; do
  case $1 in
    --slow) slow=1 ;;
    --out) out=$2; shift ;;
    *) echo "usage: scripts/gate-report.sh [--slow] [--out FILE]" >&2; exit 2 ;;
  esac
  shift
done

gates=$(mktemp)
trap 'rm -f "$gates"' EXIT
if [ -n "${GATE_REPORT_GATES:-}" ]; then
  cp "$GATE_REPORT_GATES" "$gates"
else
  cat > "$gates" <<'EOF'
gofmt	test -z "$(gofmt -l $(git ls-files --cached --others --exclude-standard -- '*.go'))"
go vet	go vet ./...
go vet (windows)	GOOS=windows GOARCH=amd64 go vet ./...
go mod tidy -diff	for m in $(git ls-files '*go.mod'); do (cd "$(dirname "$m")" && go mod tidy -diff) || exit 1; done
task check	go tool task check
public surface (task api)	scripts/api.sh
coverage floors	scripts/coverage.sh
lightness budget	scripts/lightness.sh
documents (task docs:check)	scripts/docs-check.sh
workflows	go test -count=1 ./scripts/workflows/
mutation gate self-test	go test -count=1 -run SelfTest ./scripts/mutate/
compat runner self-test	go test -count=1 ./compat/
EOF
  if [ "$slow" = 1 ]; then
    printf '%s\t%s\n' "mutation check (every unit)" "go run ./scripts/mutate check" >> "$gates"
    printf '%s\t%s\n' "compat matrix" "compat/run.sh" >> "$gates"
  fi
fi

today=$(date +%F)
[ -n "$out" ] || out="plan/evidence/gates-$today.md"
mkdir -p "$(dirname "$out")"
commit=$(git rev-parse --short HEAD 2>/dev/null || echo "no git")
dirty=""
[ -n "$(git status --porcelain --untracked-files=no 2>/dev/null)" ] && dirty=" (with uncommitted changes: this is not the commit's result alone)"
logs=$(mktemp -d)
failed=0
rows=""
n=0
while IFS=$'\t' read -r name cmd; do
  [ -n "$name" ] || continue
  n=$((n + 1))
  echo "== $name"
  start=$(date +%s)
  bash -c "$cmd" > "$logs/$n" 2>&1
  rc=$?
  secs=$(( $(date +%s) - start ))
  verdict=PASS
  if [ "$rc" != 0 ]; then verdict=FAIL; failed=1; fi
  echo "   $verdict (exit $rc, ${secs}s)"
  rows+="| $name | $verdict | $rc | ${secs}s |"$'\n'
  printf '%s\t%s\t%s\t%s\t%s\n' "$n" "$name" "$cmd" "$verdict" "$rc" >> "$logs/index"
done < "$gates"

{
  echo "# Gate results, $today"
  echo
  echo "Written by \`scripts/gate-report.sh\`; do not edit by hand."
  echo
  echo "- commit: \`$commit\`$dirty"
  echo "- platform: $(go env GOOS)/$(go env GOARCH), $(go env GOVERSION)"
  echo "- run: $( [ "$slow" = 1 ] && echo "every gate, the slow ones included" || echo "the push gates (the mutation check and the compat matrix need --slow)")"
  echo
  echo "| Gate | Result | Exit | Time |"
  echo "|---|---|---|---|"
  printf '%s' "$rows"
  while IFS=$'\t' read -r i name cmd verdict rc; do
    echo
    echo "## $name: $verdict"
    echo
    echo '```'
    echo "\$ $cmd"
    tail -n 25 "$logs/$i"
    echo '```'
  done < "$logs/index"
} > "$out"
rm -rf "$logs"
echo "wrote $out"
[ "$failed" = 0 ] || { echo "a gate failed: see $out" >&2; exit 1; }
