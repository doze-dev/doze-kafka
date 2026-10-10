#!/usr/bin/env bash
# KafkaJS: produce, consume in a group, admin.
set -euo pipefail
command -v bun >/dev/null || command -v node >/dev/null || { echo "neither bun nor node is installed"; exit 77; }
RUN=node; command -v bun >/dev/null && RUN=bun
[ -d node_modules ] || { command -v bun >/dev/null && bun install --silent || npm install --silent; }
exec $RUN check.mjs
