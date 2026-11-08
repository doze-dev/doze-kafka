#!/usr/bin/env bash
# Redpanda Console, pointed at a Kafka broker: it connects and lists topics and consumer groups,
# and the built-in registry's subjects, and shows an Avro record decoded through it.
set -euo pipefail
. ../lib.sh
need_docker; trap_cleanup
T=rp-$$
printf 'a\nb\nc\n' | kafka_tool kafka-console-producer.sh --topic "$T" >/dev/null 2>&1 || fail "seeding a topic"
kafka_tool kafka-console-consumer.sh --topic "$T" --group "g-$T" --from-beginning --max-messages 3 >/dev/null 2>&1 || fail "seeding a consumer group"

# A subject in the built-in registry (registered from the host, on loopback), for the
# UI's schema pages (Stage 5).
HOST_REG=${REGISTRY_URL/host.docker.internal/127.0.0.1}; REG_AUTH=()
[ -z "${REGISTRY_USER:-}" ] || REG_AUTH=(-u "$REGISTRY_USER:$REGISTRY_PASS")
curl -fsS ${REG_AUTH[@]+"${REG_AUTH[@]}"} -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"text\",\"type\":\"string\"}]}"}' \
  "$HOST_REG/subjects/$T-value/versions" >/dev/null || fail "registering a schema"

AVRO_ID=$(avro_record "$T-avro") || fail "writing an Avro record"
CONTAINER=compat-redpanda-$$
ENVS=(-e "KAFKA_BROKERS=$BOOTSTRAP" -e SCHEMAREGISTRY_ENABLED=true -e "SCHEMAREGISTRY_URLS=$REGISTRY_URL")
[ -z "${REGISTRY_USER:-}" ] || ENVS+=(-e SCHEMAREGISTRY_AUTHENTICATION_BASIC_ENABLED=true -e "SCHEMAREGISTRY_AUTHENTICATION_BASIC_USERNAME=$REGISTRY_USER" -e "SCHEMAREGISTRY_AUTHENTICATION_BASIC_PASSWORD=$REGISTRY_PASS")
MOUNT=()
if [ "$SECURITY" = scram-tls ]; then
  ENVS+=(-e KAFKA_TLS_ENABLED=true -e KAFKA_TLS_CAFILEPATH=/ca.pem -e KAFKA_SASL_ENABLED=true
         -e KAFKA_SASL_MECHANISM="$SASL_MECHANISM" -e KAFKA_SASL_USERNAME="$SASL_USER" -e KAFKA_SASL_PASSWORD="$SASL_PASS")
  MOUNT=(-v "$CA_CERT:/ca.pem:ro")
fi
# shellcheck disable=SC2086
docker run -d --name "$CONTAINER" ${HOSTS_FLAG:-} -p 127.0.0.1::8080 "${ENVS[@]}" ${MOUNT[@]+"${MOUNT[@]}"} redpandadata/console:latest >/dev/null
PORT=$(publish_port "$CONTAINER" 8080)
URL=http://127.0.0.1:$PORT
wait_http "$URL/admin/health" 90 || fail "Redpanda Console did not start"
for _ in $(seq 1 30); do curl -fsS "$URL/api/topics" 2>/dev/null | grep -q "\"topicName\":\"$T\"" && break; sleep 1; done
curl -fsS "$URL/api/topics" | grep -q "\"topicName\":\"$T\"" || fail "the topic list lacks $T: $(curl -sS "$URL/api/topics" | head -c 400)"
ok "the console connected and lists the topic"
curl -fsS "$URL/api/consumer-groups" | grep -q "g-$T" || fail "the consumer group list lacks g-$T: $(curl -sS "$URL/api/consumer-groups" | head -c 400)"
ok "the consumer group list shows the group"
curl -fsS "$URL/api/cluster/overview" 2>/dev/null | grep -q -i 'broker\|kafka' || curl -fsS "$URL/api/cluster" | grep -q -i 'broker\|kafka' || fail "the cluster overview did not load"
ok "the cluster overview loads"
curl -fsS "$URL/api/schema-registry/subjects" | grep -q "\"$T-value\"" || fail "the schema registry's subject list lacks $T-value: $(curl -sS "$URL/api/schema-registry/subjects" | head -c 400)"
curl -fsS "$URL/api/schema-registry/subjects/$T-value/versions/latest" | grep -q 'Greeting' || fail "the subject's latest schema did not load: $(curl -sS "$URL/api/schema-registry/subjects/$T-value/versions/latest" | head -c 400)"
ok "the schema registry page lists the registry's subject, and its latest version loads"
# The Avro record, read through the registry. Messages are a Connect RPC stream: a
# request is one envelope (a flags byte, four bytes of length, the JSON), and the
# decoded value comes back base64 in normalizedPayload ({"text":"greets"}).
REQ='{"topic":"'"$T-avro"'","startOffset":"-2","partitionId":-1,"maxResults":5}'
LEN=$(printf %s "$REQ" | wc -c | tr -d ' ')
{ printf '\x00'; printf "\\x$(printf %02x $((LEN >> 24 & 255)))\\x$(printf %02x $((LEN >> 16 & 255)))\\x$(printf %02x $((LEN >> 8 & 255)))\\x$(printf %02x $((LEN & 255)))"; printf %s "$REQ"; } >"$WORK/rp-list-$$.bin"
list() { curl -sS --max-time 20 -H 'Content-Type: application/connect+json' -H 'Connect-Protocol-Version: 1' --data-binary @"$WORK/rp-list-$$.bin" "$URL/redpanda.api.console.v1alpha1.ConsoleService/ListMessages" | LC_ALL=C tr -c '[:print:]' '.'; }
for _ in $(seq 1 15); do list | grep -q '"normalizedPayload":"eyJ0ZXh0IjoiZ3JlZXRzIn0="' && break; sleep 1; done
OUT=$(list)
echo "$OUT" | grep -q '"normalizedPayload":"eyJ0ZXh0IjoiZ3JlZXRzIn0="' && echo "$OUT" | grep -q 'PAYLOAD_ENCODING_AVRO' || fail "the console did not decode the Avro record (id $AVRO_ID): $(echo "$OUT" | head -c 400)"
ok "the topic's Avro record is shown decoded through the registry (id $AVRO_ID)"
