#!/usr/bin/env bash
# confluent-kafka (librdkafka) through its Python API: produce, consume, a group that
# commits and resumes, admin (create, describe, grow, delete), idempotence and a
# transaction.
set -euo pipefail
export PROTO_UV_VERSION=${PROTO_UV_VERSION:-$(ls ~/.proto/tools/uv 2>/dev/null | grep -E '^[0-9]' | tail -1)}
command -v uv >/dev/null || { echo "uv is not installed"; exit 77; }
uv run --quiet --with confluent-kafka python check.py
# The serializers need the registry, which the runner gives both ways.
[ -z "${REGISTRY_URL:-}" ] || uv run --quiet --with "confluent-kafka[avro,json,protobuf,schemaregistry]" python serdes.py
