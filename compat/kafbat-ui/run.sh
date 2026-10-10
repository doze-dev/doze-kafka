#!/usr/bin/env bash
# Kafbat UI (the maintained fork of Kafka UI): it connects, and lists the cluster's topics and
# consumer groups without tripping on an API the broker does not have.
set -euo pipefail
. ../lib.sh
need_docker; trap_cleanup
T=ui-$$
printf 'a\nb\nc\n' | kafka_tool kafka-console-producer.sh --topic "$T" >/dev/null 2>&1 || fail "seeding a topic"
kafka_tool kafka-console-consumer.sh --topic "$T" --group "g-$T" --from-beginning --max-messages 3 >/dev/null 2>&1 || fail "seeding a consumer group"

CONTAINER=compat-kafbat-$$
ENVS=(-e KAFKA_CLUSTERS_0_NAME=local -e "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=$BOOTSTRAP" -e DYNAMIC_CONFIG_ENABLED=false)
MOUNT=()
if [ "$SECURITY" = scram-tls ]; then
  ENVS+=(-e KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL -e KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM="$SASL_MECHANISM"
         -e "KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG=$(jaas)" -e KAFKA_CLUSTERS_0_PROPERTIES_SSL_TRUSTSTORE_TYPE=PEM
         -e KAFKA_CLUSTERS_0_PROPERTIES_SSL_TRUSTSTORE_LOCATION=/ca.pem)
  MOUNT=(-v "$CA_CERT:/ca.pem:ro")
fi
# shellcheck disable=SC2086
docker run -d --name "$CONTAINER" ${HOSTS_FLAG:-} -p 127.0.0.1::8080 "${ENVS[@]}" ${MOUNT[@]+"${MOUNT[@]}"} ghcr.io/kafbat/kafka-ui:latest >/dev/null
PORT=$(publish_port "$CONTAINER" 8080)
URL=http://127.0.0.1:$PORT
wait_http "$URL/actuator/health" 90 || fail "Kafbat UI did not start"
for _ in $(seq 1 40); do curl -fsS "$URL/api/clusters" | grep -q -i "\"status\":\"online\"" && break; sleep 1; done
curl -fsS "$URL/api/clusters" | grep -q -i "\"status\":\"online\"" || fail "the UI does not see the cluster as online: $(curl -sS "$URL/api/clusters")"
ok "the UI connected: the cluster is online"
curl -fsS "$URL/api/clusters/local/topics" | grep -q "\"name\":\"$T\"" || fail "the topic list lacks $T: $(curl -sS "$URL/api/clusters/local/topics" | head -c 400)"
ok "the topic list shows the topic"
for _ in $(seq 1 15); do curl -fsS "$URL/api/clusters/local/consumer-groups/paged" | grep -q "g-$T" && break; sleep 1; done
curl -fsS "$URL/api/clusters/local/consumer-groups/paged" | grep -q "g-$T" || fail "the consumer group list lacks g-$T"
ok "the consumer group list shows the group"
curl -fsS "$URL/api/clusters/local/brokers" | grep -q '"id"' || fail "the broker list is empty"
curl -fsS "$URL/api/clusters/local/topics/$T" | grep -q "\"name\":\"$T\"" || fail "the topic's detail did not load"
ok "the broker list and a topic's detail load"
