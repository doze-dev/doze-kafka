#!/usr/bin/env bash
# kcat (librdkafka) against the real binary: the client most scripts and CI jobs
# reach for, and an implementation that shares nothing with the Go clients the
# rest of the tests use. Checks metadata, produce with a key and headers, consume,
# a consumer group that commits and resumes, and the same over SASL/PLAIN.
#
#   scripts/kcat-check.sh path/to/doze-kafka
#
# Needs kcat (apt install kcat / brew install kcat). Exits non-zero on the first thing
# that is wrong, saying what.
set -euo pipefail

BIN=${1:?usage: kcat-check.sh path/to/doze-kafka}
KCAT=${KCAT:-$(command -v kcat || command -v kafkacat || true)}
[ -n "$KCAT" ] || { echo "kcat is not installed" >&2; exit 2; }

WORK=$(mktemp -d)
PIDS=()
cleanup() {
  for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "ok   $*"; }

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# start_broker NAME [extra flags...] -> sets ADDR
start_broker() {
  local name=$1; shift
  local port; port=$(free_port)
  ADDR=127.0.0.1:$port
  "$BIN" --listen "$ADDR" --console=false --zone=false --data-dir "$WORK/$name" "$@" >"$WORK/$name.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 100); do
    (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null && return 0
    sleep 0.1
  done
  cat "$WORK/$name.log" >&2
  fail "the broker ($name) did not come up"
}

run_checks() {
  local label=$1; shift
  local topic="kc-$label" group="g-$label"
  local kc=("$KCAT" -b "$ADDR" "$@")

  # Metadata.
  local meta; meta=$("${kc[@]}" -L -J)
  case $meta in *'"brokers"'*) ;; *) fail "$label: -L printed no brokers: $meta" ;; esac
  pass "$label: metadata"

  # Produce: a key, a header, and a value with a space and a newline-free tail.
  printf 'k1:first value\nk2:second\nk3:third\n' |
    "${kc[@]}" -P -t "$topic" -K: -H "source=kcat" -p 0 || fail "$label: produce"
  local got; got=$("${kc[@]}" -C -t "$topic" -p 0 -o beginning -e -q -f '%k|%s|%h\n')
  [ "$got" = "k1|first value|source=kcat
k2|second|source=kcat
k3|third|source=kcat" ] || fail "$label: consumed: $got"
  pass "$label: produce and consume, with keys and headers"

  # A group: read two records and commit, then a second run resumes after them.
  # (kcat takes everything after -G as topics, so the options come first.)
  local first; first=$("${kc[@]}" -e -q -X auto.offset.reset=earliest -X enable.auto.commit=true -c 2 -f '%s\n' -G "$group" "$topic")
  [ "$first" = "first value
second" ] || fail "$label: the group's first read: $first"
  sleep 1
  local second; second=$("${kc[@]}" -e -q -X auto.offset.reset=earliest -f '%s\n' -G "$group" "$topic")
  [ "$second" = "third" ] || fail "$label: the group did not resume after its commit; it read: $second"
  pass "$label: a consumer group commits and resumes"

  # Many records, in order, to a topic with several partitions created by the broker.
  seq 1 500 | "${kc[@]}" -P -t "$topic-many" || fail "$label: bulk produce"
  local n; n=$("${kc[@]}" -C -t "$topic-many" -o beginning -e -q | wc -l | tr -d ' ')
  [ "$n" = 500 ] || fail "$label: bulk consume read $n of 500"
  pass "$label: 500 records round-trip"
}

start_broker plain
run_checks plain

start_broker sasl --sasl-mechanisms PLAIN --sasl-user kc:secret
run_checks sasl -X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN -X sasl.username=kc -X sasl.password=secret

# Wrong credentials are refused, not hung.
if "$KCAT" -b "$ADDR" -X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN -X sasl.username=kc -X sasl.password=wrong -L -m 5 >/dev/null 2>&1; then
  fail "sasl: a wrong password was accepted"
fi
pass "sasl: a wrong password is refused"

echo "all kcat checks passed"
