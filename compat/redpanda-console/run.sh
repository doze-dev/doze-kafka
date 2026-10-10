#!/usr/bin/env bash
# Redpanda Console, pointed at a Kafka broker: it connects and lists topics and consumer groups.
set -euo pipefail
. ../lib.sh
need_docker; trap_cleanup
T=rp-$$
printf 'a\nb\nc\n' | kafka_tool kafka-console-producer.sh --topic "$T" >/dev/null 2>&1 || fail "seeding a topic"
kafka_tool kafka-console-consumer.sh --topic "$T" --group "g-$T" --from-beginning --max-messages 3 >/dev/null 2>&1 || fail "seeding a consumer group"

CONTAINER=compat-redpanda-$$
ENVS=(-e "KAFKA_BROKERS=$BOOTSTRAP")
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
