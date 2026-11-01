#!/usr/bin/env bash
# librdkafka 2.5+ pushes its metrics (KIP-714) by default once the broker advertises the
# two APIs. A producer left connected for a few report intervals appears on the Traffic
# page with its client id and its producer metrics.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/../lib-telemetry.sh"
KCAT=${KCAT:-$(command -v kcat || command -v kafkacat || true)}
[ -n "$KCAT" ] || { echo "kcat is not installed"; exit 77; }
VER=$("$KCAT" -V 2>&1 | sed -n 's/.*librdkafka \([0-9]*\)\.\([0-9]*\).*/\1 \2/p' | head -1)
set -- $VER
if [ -z "${1:-}" ] || [ "$1" -lt 2 ] || { [ "$1" -eq 2 ] && [ "${2:-0}" -lt 5 ]; }; then
  echo "kcat's librdkafka is older than 2.5, which has no client telemetry"; exit 77
fi
ID=compat-kcat-telemetry
A=(-b "$BOOTSTRAP" -X client.id=$ID)
if [ "$SECURITY" = scram-tls ]; then
  A+=(-X security.protocol=SASL_SSL -X sasl.mechanism="$SASL_MECHANISM" -X sasl.username="$SASL_USER" -X sasl.password="$SASL_PASS" -X ssl.ca.location="$CA_CERT")
fi
T=telemetry-$RANDOM
# One message, then the producer stays connected: kcat -P exits at end of input, so the
# input is held open past the push interval.
( echo hello; sleep 20 ) | "$KCAT" "${A[@]}" -P -t "$T" >/dev/null 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT
if wait_metrics "$ID" 18; then
  echo "ok   $ID reported its metrics and the console shows them"
else
  echo "FAIL $ID did not appear with metrics on the Traffic page within 18 s"
  exit 1
fi
