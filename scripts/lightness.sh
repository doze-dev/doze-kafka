#!/usr/bin/env bash
# Weight is a feature: binary size, idle heap after start, and the module list
# are budgets in testdata/lightness.json. Growth beyond the slack fails.
#
#   scripts/lightness.sh          check against testdata/lightness.json
#   scripts/lightness.sh record   write today's numbers (the commit must say why they grew)
set -euo pipefail
cd "$(dirname "$0")/.."
mode=${1:-check}
budget=testdata/lightness.json
tmp=$(mktemp -d)
trap 'kill "${pid:-0}" 2>/dev/null || true; rm -rf "$tmp"' EXIT

GOWORK=off go build -ldflags='-s -w' -o "$tmp/doze-kafka" ./cmd/doze-kafka
size=$(wc -c < "$tmp/doze-kafka" | tr -d ' ')
mods=$(GOWORK=off go list -m all | wc -l | tr -d ' ')

port=$((20000 + RANDOM % 20000))
"$tmp/doze-kafka" --listen 127.0.0.1:0 --console-addr 127.0.0.1:0 --schema-registry-addr 127.0.0.1:0 \
  --data-dir "$tmp/data" --pprof-addr "127.0.0.1:$port" >"$tmp/log" 2>&1 &
pid=$!
for _ in $(seq 1 100); do curl -sf "http://127.0.0.1:$port/debug/pprof/" >/dev/null 2>&1 && break; sleep 0.1; done
sleep 1
# Force a collection so HeapAlloc is live data, not garbage waiting for a cycle.
heap=$(curl -sf "http://127.0.0.1:$port/debug/pprof/heap?gc=1&debug=1" | awk '/^# HeapAlloc =/ { print $4 }')
[ -n "$heap" ] || { echo "no heap figure from pprof" >&2; cat "$tmp/log" >&2; exit 1; }
echo "binary $size bytes, idle heap $heap bytes, $mods modules"

if [ "$mode" = record ]; then
  mkdir -p testdata
  printf '{\n  "binaryBytes": %s,\n  "idleHeapBytes": %s,\n  "modules": %s,\n  "slackPercent": 5\n}\n' "$size" "$heap" "$mods" > "$budget"
  echo "recorded $budget"; exit 0
fi

read_json() { sed -n "s/.*\"$1\": *\([0-9]*\).*/\1/p" "$budget"; }
slack=$(read_json slackPercent)
fail=0
for pair in "binaryBytes:$size" "idleHeapBytes:$heap" "modules:$mods"; do
  k=${pair%%:*}; v=${pair##*:}; b=$(read_json "$k")
  if awk -v v="$v" -v b="$b" -v s="$slack" 'BEGIN { exit !(v + 0 > b * (1 + s / 100)) }'; then
    echo "OVER     $k: $v > budget $b (+$slack%)" >&2; fail=1
  else echo "ok       $k: $v (budget $b)"; fi
done
exit $fail
