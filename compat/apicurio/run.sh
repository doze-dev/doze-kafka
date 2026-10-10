#!/usr/bin/env bash
# Apicurio Registry with Kafka SQL storage, which keeps its journal in a Kafka topic:
# it starts against doze-kafka, completes an Avro round trip, and replays after a restart.
set -euo pipefail
. ../lib.sh
need_docker; trap_cleanup
CONTAINER=compat-apicurio-$$
ENVS=(-e APICURIO_STORAGE_KIND=kafkasql -e "APICURIO_KAFKASQL_BOOTSTRAP_SERVERS=$BOOTSTRAP")
MOUNT=()
if [ "$SECURITY" = scram-tls ]; then
  # Apicurio hands apicurio.kafka.common.* to its Kafka producer, consumer and admin client.
  ENVS+=(-e APICURIO_KAFKA_COMMON_SECURITY_PROTOCOL=SASL_SSL -e APICURIO_KAFKA_COMMON_SASL_MECHANISM="$SASL_MECHANISM"
         -e "APICURIO_KAFKA_COMMON_SASL_JAAS_CONFIG=$(jaas)" -e APICURIO_KAFKA_COMMON_SSL_TRUSTSTORE_TYPE=PEM
         -e APICURIO_KAFKA_COMMON_SSL_TRUSTSTORE_LOCATION=/ca.pem)
  MOUNT=(-v "$CA_CERT:/ca.pem:ro")
fi
IMAGE=${APICURIO_IMAGE:-apicurio/apicurio-registry:latest-release}
# shellcheck disable=SC2086
docker run -d --name "$CONTAINER" ${HOSTS_FLAG:-} -p 127.0.0.1::8080 "${ENVS[@]}" ${MOUNT[@]+"${MOUNT[@]}"} "$IMAGE" >/dev/null
PORT=$(publish_port "$CONTAINER" 8080)
wait_http "http://127.0.0.1:$PORT/apis/registry/v2/system/info" 150 || wait_http "http://127.0.0.1:$PORT/apis/registry/v3/system/info" 20 || fail "Apicurio did not come up against doze-kafka"
ok "Apicurio Registry (kafkasql) started against doze-kafka"
API=v3; curl -fsS "http://127.0.0.1:$PORT/apis/registry/v3/system/info" >/dev/null 2>&1 || API=v2
if [ $API = v3 ]; then
  curl -fsS -X POST -H 'Content-Type: application/json' "http://127.0.0.1:$PORT/apis/registry/v3/groups/default/artifacts" \
    -d '{"artifactId":"orders","artifactType":"AVRO","firstVersion":{"version":"1","content":{"content":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}","contentType":"application/json"}}}' >/dev/null || fail "creating an artifact"
  curl -fsS "http://127.0.0.1:$PORT/apis/registry/v3/groups/default/artifacts/orders/versions/1/content" | grep -q Order || fail "reading the artifact back"
else
  curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Registry-ArtifactId: orders' -H 'X-Registry-ArtifactType: AVRO' \
    "http://127.0.0.1:$PORT/apis/registry/v2/groups/default/artifacts" -d '{"type":"record","name":"Order","fields":[{"name":"id","type":"long"}]}' >/dev/null || fail "creating an artifact"
  curl -fsS "http://127.0.0.1:$PORT/apis/registry/v2/groups/default/artifacts/orders" | grep -q Order || fail "reading the artifact back"
fi
ok "Avro round trip: create an artifact, read it back"
docker restart "$CONTAINER" >/dev/null
# Docker publishes a new host port when a container restarts.
PORT=$(publish_port "$CONTAINER" 8080)
wait_http "http://127.0.0.1:$PORT/apis/registry/$API/system/info" 150 || fail "Apicurio did not come back"
if [ $API = v3 ]; then curl -fsS "http://127.0.0.1:$PORT/apis/registry/v3/groups/default/artifacts/orders/versions/1/content" | grep -q Order || fail "after a restart the artifact is gone: the journal was not replayed"
else curl -fsS "http://127.0.0.1:$PORT/apis/registry/v2/groups/default/artifacts/orders" | grep -q Order || fail "after a restart the artifact is gone"; fi
ok "after a restart, Apicurio replays its journal from the Kafka topic"
