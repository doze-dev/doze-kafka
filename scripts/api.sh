#!/usr/bin/env bash
# The embedding API is the exported surface of server/. This writes it and fails
# when it changed without a CHANGELOG line.
#
#   scripts/api.sh          check: the committed surface equals the code, and
#                           every commit that changed the surface also added a
#                           CHANGELOG.md line containing "server API"
#                           (scripts/api-rule.sh; it needs the whole history)
#   scripts/api.sh record   write testdata/api/server.txt from the code
#
# Changing the surface is therefore two edits in one commit: `task api:record`,
# and a line in CHANGELOG.md that says what changed ("server API: ...").
set -euo pipefail
cd "$(dirname "$0")/.."
surface=testdata/api/server.txt
now=$(mktemp)
trap 'rm -f "$now"' EXIT
GOWORK=off go run ./scripts/apisurface ./server > "$now"

if [ "${1:-check}" = record ]; then
  mkdir -p "$(dirname "$surface")"
  cp "$now" "$surface"
  echo "recorded $(wc -l < "$surface" | tr -d ' ') declarations in $surface"
  exit 0
fi

if ! diff -u "$surface" "$now"; then
  echo >&2
  echo "The exported surface of server/ changed. If that is intended: task api:record," >&2
  echo "and add a line containing \"server API\" to CHANGELOG.md in the same commit." >&2
  exit 1
fi

# The surface file may only change together with a CHANGELOG line: every commit
# that changed it, and the working tree. A shallow clone fails (api-rule.sh).
scripts/api-rule.sh "$surface"
echo "ok       server API surface ($(wc -l < "$surface" | tr -d ' ') declarations)"
