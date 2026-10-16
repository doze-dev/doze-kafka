#!/usr/bin/env bash
# KafkaJS: produce, consume in a group, admin; then the kafkajs snippet in
# docs/clients.md, run as written (its broker address replaced by $BOOTSTRAP).
set -euo pipefail
command -v bun >/dev/null || command -v node >/dev/null || { echo "neither bun nor node is installed"; exit 77; }
RUN=node; command -v bun >/dev/null && RUN=bun
[ -d node_modules ] || { command -v bun >/dev/null && bun install --silent || npm install --silent; }
$RUN check.mjs
if [ "$SECURITY" != scram-tls ]; then # the snippet is the plaintext one.
  SNIPPET=$(awk '/^```js$/{on=1;next} /^```$/{on=0} on' ../../docs/clients.md)
  [ -n "$SNIPPET" ] || { echo "FAIL docs/clients.md has no js snippet"; exit 1; }
  {
    echo "import { Kafka } from 'kafkajs';"
    echo "${SNIPPET//\"kafka.myproject.doze:9092\"/process.env.BOOTSTRAP}"
    echo "await producer.disconnect();"
  } > .doc-snippet.mjs
  trap 'rm -f .doc-snippet.mjs' EXIT
  $RUN .doc-snippet.mjs >/dev/null 2>&1 || { echo "FAIL the kafkajs snippet in docs/clients.md"; exit 1; }
  echo "ok   the docs/clients.md snippet, as written"
fi
