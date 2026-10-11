#!/usr/bin/env bash
# The Java client (3.7+) pushes its metrics (KIP-714) by default once the broker
# advertises the two APIs: Apache Kafka's own console producer, left connected past a
# few report intervals, appears on the Traffic page with its client id.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/../lib.sh"
. "$HERE/../lib-telemetry.sh"
need_docker
ID=compat-java-telemetry
T=telemetry-$RANDOM
# The input is held open past the push interval; the producer sends one message so it
# has a connection to report on.
( echo hello; sleep 25 ) | kafka_tool kafka-console-producer.sh --topic "$T" --producer-property client.id=$ID >/dev/null 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT
if wait_metrics "$ID" 22; then
  ok "$ID reported its metrics and the console shows them"
else
  fail "$ID did not appear with metrics on the Traffic page within 22 s"
fi
