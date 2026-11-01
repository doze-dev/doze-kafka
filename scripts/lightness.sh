#!/usr/bin/env bash
# Weight is a feature: binary size, idle heap and module count, per platform,
# against testdata/lightness.json. The gate is scripts/lightness (Go); see its
# package comment for what is measured and why there is no slack percentage.
#
#   scripts/lightness.sh                       check this platform (what CI runs)
#   scripts/lightness.sh record                record this platform; a number only goes down
#   scripts/lightness.sh record -reason "why"  ... or up, with the reason written into the file
#   scripts/lightness.sh measure -runs 20      print the numbers and their spread
set -euo pipefail
cd "$(dirname "$0")/.."
exec env GOWORK=off go run ./scripts/lightness "$@"
