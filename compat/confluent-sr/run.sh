#!/usr/bin/env bash
# Confluent's own Schema Registry, for a team that prefers its own: it starts against
# doze-kafka (keeping its state in the _schemas topic, on a compacted topic this broker
# must create and honour) and completes an Avro round trip.
set -euo pipefail
. ../lib.sh
need_docker; trap_cleanup
CONTAINER=compat-sr-$$
ENVS=(-e SCHEMA_REGISTRY_HOST_NAME=sr -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081)
MOUNT=()
if [ "$SECURITY" = scram-tls ]; then
  ENVS+=(-e "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=SASL_SSL://$BOOTSTRAP"
         -e SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL=SASL_SSL
         -e SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM="$SASL_MECHANISM"
         -e "SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG=$(jaas)"
         -e SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_TYPE=PEM
         -e SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION=/ca.pem)
  MOUNT=(-v "$CA_CERT:/ca.pem:ro")
else
  ENVS+=(-e "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://$BOOTSTRAP")
fi
# shellcheck disable=SC2086
docker run -d --name "$CONTAINER" ${HOSTS_FLAG:-} -p 127.0.0.1::8081 "${ENVS[@]}" ${MOUNT[@]+"${MOUNT[@]}"} confluentinc/cp-schema-registry:7.7.1 >/dev/null
PORT=$(publish_port "$CONTAINER" 8081)
wait_http "http://127.0.0.1:$PORT/subjects" 150 || fail "Confluent's Schema Registry did not come up against doze-kafka"
ok "Confluent Schema Registry started against doze-kafka and created its _schemas topic"

SCHEMA='{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"}'
ID=$(curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' -d "$SCHEMA" "http://127.0.0.1:$PORT/subjects/orders-value/versions" | sed 's/.*"id":\([0-9]*\).*/\1/')
[ -n "$ID" ] || fail "registering a schema returned no id"
curl -fsS "http://127.0.0.1:$PORT/schemas/ids/$ID" | grep -q Order || fail "reading the schema back by id"
curl -fsS "http://127.0.0.1:$PORT/subjects" | grep -q orders-value || fail "the subject is not listed"
ok "Avro round trip: register, read by id, list"
# A restart of the registry rebuilds its state from the _schemas topic this broker holds.
docker restart "$CONTAINER" >/dev/null
# Docker publishes a new host port when a container restarts.
PORT=$(publish_port "$CONTAINER" 8081)
wait_http "http://127.0.0.1:$PORT/subjects" 150 || fail "the registry did not come back"
curl -fsS "http://127.0.0.1:$PORT/subjects" | grep -q orders-value || fail "after a restart the registry lost its schemas: _schemas was not replayed"
ok "after a restart, the registry replays its schemas from the _schemas topic"
