#!/usr/bin/env bash
# Kafka Connect in distributed mode: the worker creates and verifies its three internal
# topics (which must be compacted), runs a file source into a topic and a file sink out
# of it (the plugin path holds only the file connectors: scanning all of libs/ takes minutes on a loaded machine and has nothing to do with the broker), and after a restart picks its connectors and its offsets back up from those topics.
set -euo pipefail
. ../lib.sh
need_docker; trap_cleanup
CONTAINER=compat-connect-$$
P=$$
SEC=""; MOUNT=()
if [ "$SECURITY" = scram-tls ]; then
  for pre in "" "producer." "consumer." "admin."; do
    SEC+="${pre}security.protocol=SASL_SSL
${pre}sasl.mechanism=$SASL_MECHANISM
${pre}sasl.jaas.config=$(jaas)
${pre}ssl.truststore.type=PEM
${pre}ssl.truststore.location=/ca.pem
"
  done
  MOUNT=(-v "$CA_CERT:/ca.pem:ro")
fi
# The worker's settings are docs/frameworks.md's, as a reader copies them: the broker's
# address for this run, and a per-run suffix on the group and the three topics (the
# document says so), are the only changes. A setting the document lacks fails here.
DOC=$(doc_properties "Kafka Connect") || exit 1
CONF=$(printf '%s\n' "$DOC" | sed -E \
  -e "s#^bootstrap\.servers=.*#bootstrap.servers=$BOOTSTRAP#" \
  -e "s#^group\.id=(.*)#group.id=\1-$P#" \
  -e "s#^((offset|config|status)\.storage\.topic)=(.*)#\1=\3-$P#")
topic_of() { printf '%s\n' "$CONF" | sed -n "s|^$1\.storage\.topic=||p"; }
for k in offset config status; do [ -n "$(topic_of "$k")" ] || fail "docs/frameworks.md's Connect settings have no $k.storage.topic"; done
CONF="$CONF
listeners=http://0.0.0.0:8083
plugin.path=/tmp/plugins
$SEC"
# shellcheck disable=SC2086
docker run -d --name "$CONTAINER" ${HOSTS_FLAG:-} -p 127.0.0.1::8083 ${MOUNT[@]+"${MOUNT[@]}"} -e CONF="$CONF" --entrypoint sh apache/kafka:4.3.1 \
  -c 'mkdir /tmp/plugins && cp /opt/kafka/libs/connect-file-*.jar /tmp/plugins/; printf "%s\n" "$CONF" > /tmp/worker.properties; exec /opt/kafka/bin/connect-distributed.sh /tmp/worker.properties' >/dev/null
PORT=$(publish_port "$CONTAINER" 8083)
URL=http://127.0.0.1:$PORT
wait_http "$URL/connectors" 180 || fail "the Connect worker did not start against doze-kafka"
ok "the worker started: its offset, config and status topics were created and verified as compacted"

docker exec "$CONTAINER" sh -c 'printf "one\ntwo\nthree\n" > /tmp/in.txt'
curl -fsS -X POST -H 'Content-Type: application/json' "$URL/connectors" -d "{\"name\":\"src\",\"config\":{\"connector.class\":\"org.apache.kafka.connect.file.FileStreamSourceConnector\",\"tasks.max\":\"1\",\"file\":\"/tmp/in.txt\",\"topic\":\"connect-data-$P\"}}" >/dev/null || fail "creating the source connector"
curl -fsS -X POST -H 'Content-Type: application/json' "$URL/connectors" -d "{\"name\":\"snk\",\"config\":{\"connector.class\":\"org.apache.kafka.connect.file.FileStreamSinkConnector\",\"tasks.max\":\"1\",\"file\":\"/tmp/out.txt\",\"topics\":\"connect-data-$P\"}}" >/dev/null || fail "creating the sink connector"
for _ in $(seq 1 60); do
  [ "$(docker exec "$CONTAINER" sh -c 'cat /tmp/out.txt 2>/dev/null | wc -l')" -ge 3 ] && break; sleep 1
done
[ "$(docker exec "$CONTAINER" cat /tmp/out.txt)" = "$(printf 'one\ntwo\nthree')" ] || fail "the sink file is not what the source read: $(docker exec "$CONTAINER" cat /tmp/out.txt 2>&1)"
ok "file source -> topic -> file sink moved three lines"
for c in src snk; do
  [ "$(curl -fsS "$URL/connectors/$c/status" | sed 's/.*"connector":{"state":"\([A-Z]*\)".*/\1/')" = RUNNING ] || fail "$c is not RUNNING"
done
ok "both connectors report RUNNING"

# Restart the worker: its connectors come back from the config topic and the source resumes from
# the offset stored in the offsets topic, so new lines arrive once and old ones are not repeated.
docker restart "$CONTAINER" >/dev/null
# Docker publishes a new host port when a container restarts.
PORT=$(publish_port "$CONTAINER" 8083)
URL=http://127.0.0.1:$PORT
wait_http "$URL/connectors" 180 || fail "the worker did not come back"
curl -fsS "$URL/connectors" | grep -q src || fail "after a restart the connectors are gone: the config topic was not read back"
docker exec "$CONTAINER" sh -c 'printf "four\n" >> /tmp/in.txt'
for _ in $(seq 1 60); do
  [ "$(docker exec "$CONTAINER" sh -c 'cat /tmp/out.txt | wc -l')" -ge 4 ] && break; sleep 1
done
[ "$(docker exec "$CONTAINER" cat /tmp/out.txt)" = "$(printf 'one\ntwo\nthree\nfour')" ] || fail "after a restart the sink holds: $(docker exec "$CONTAINER" cat /tmp/out.txt)"
ok "after a restart: connectors restored from the config topic, source resumed from its stored offset"

# The three internal topics are what Connect requires of them.
for t in "$(topic_of offset)" "$(topic_of config)" "$(topic_of status)"; do
  kafka_tool kafka-configs.sh --describe --entity-type topics --entity-name "$t" 2>/dev/null | grep -q "cleanup.policy=compact" || fail "$t is not a compacted topic"
done
ok "the offsets, configs and status topics are compacted"
