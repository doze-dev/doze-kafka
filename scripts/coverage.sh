#!/usr/bin/env bash
# Statement coverage per package (and per file, where a stage set a target)
# against the floors in testdata/coverage.txt. The gate is scripts/coverage (Go);
# its package comment says what fails and why a lowered floor needs a written line.
#
#   scripts/coverage.sh                        check (what CI runs)
#   scripts/coverage.sh record                 raise this platform's floors to today's numbers
#   scripts/coverage.sh record -lower "why"    ... and lower those that fell, with the reason
#   scripts/coverage.sh history                only the history rule (no tests run)
set -euo pipefail
cd "$(dirname "$0")/.."
exec env GOWORK=off go run ./scripts/coverage "$@"
