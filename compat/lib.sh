# Helpers the Docker-based clients share. Source it: . ../lib.sh
need_docker() { command -v docker >/dev/null && docker info >/dev/null 2>&1 || { echo "docker is not available"; exit 77; }; }
ok() { echo "ok   $*"; }
fail() { echo "FAIL $*"; docker logs --tail 40 "${CONTAINER:-none}" 2>&1 | sed 's/^/     | /' || true; exit 1; }
# publish_port CONTAINER PORT -> the host port Docker chose
publish_port() { docker port "$1" "$2/tcp" | head -1 | sed 's/.*://'; }
# wait_http URL SECONDS
wait_http() { local end=$((SECONDS + $2)); until curl -fsS -o /dev/null "$1" 2>/dev/null; do [ $SECONDS -lt $end ] || return 1; sleep 1; done; }
trap_cleanup() { trap 'docker rm -f "${CONTAINER:-none}" >/dev/null 2>&1; [ -z "${EXTRA_CONTAINERS:-}" ] || docker rm -f $EXTRA_CONTAINERS >/dev/null 2>&1' EXIT; }
# Kafka client settings for a Java-based tool, as environment-variable-friendly pieces.
jaas() { echo "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$SASL_USER\" password=\"$SASL_PASS\";"; }

# kafka_tool TOOL ARGS... runs one of Apache Kafka's own command-line tools (from the official
# image) against the broker under test, with the connection settings for this run.
kafka_tool() {
  local tool=$1; shift
  local cfg="$WORK/tools-$$.properties" mount=() cfgarg=()
  if [ "$SECURITY" = scram-tls ]; then
    {
      echo "security.protocol=SASL_SSL"; echo "sasl.mechanism=$SASL_MECHANISM"; echo "sasl.jaas.config=$(jaas)"
      echo "ssl.truststore.type=PEM"; echo "ssl.truststore.location=/ca.pem"
    } > "$cfg"
    mount=(-v "$cfg:/tools.properties:ro" -v "$CA_CERT:/ca.pem:ro"); cfgarg=(--command-config /tools.properties)
    [ "$tool" = kafka-console-producer.sh ] && cfgarg=(--producer.config /tools.properties)
    [ "$tool" = kafka-console-consumer.sh ] && cfgarg=(--consumer.config /tools.properties)
  fi
  # shellcheck disable=SC2086
  docker run --rm -i ${HOSTS_FLAG:-} ${mount[@]+"${mount[@]}"} apache/kafka:4.3.1 "/opt/kafka/bin/$tool" --bootstrap-server "$BOOTSTRAP" ${cfgarg[@]+"${cfgarg[@]}"} "$@"
}

# doc_properties HEADING prints the ```properties block under "## HEADING" in
# docs/frameworks.md: the snippet a reader copies. The Spring and Connect cells run
# with it, so a snippet that does not work fails its cell. It fails when there is none.
doc_properties() {
  local doc out
  doc="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/docs/frameworks.md"
  out=$(awk -v h="## $1" '$0 == h { f = 1; next } f && /^## / { exit } f && /^```properties/ { p = 1; next } p && /^```/ { exit } p { print }' "$doc")
  [ -n "$out" ] || { echo "FAIL docs/frameworks.md has no \`\`\`properties block under \"## $1\"" >&2; return 1; }
  printf '%s\n' "$out"
}

# avro_record TOPIC registers an Avro schema of one string field ("text") and writes one
# record framed with its id, the value "greets", through the Kafka container's console
# producer (a client in Docker reaches the broker by the name it advertises). That
# producer passes ASCII through as it is but splits on a newline, so an id with a
# byte of 0x0a or 0x0d, or one past 0x7f, is skipped: the schema is registered again
# under another name until the id is one it can carry. Prints the id.
avro_record() {
  local t=$1 id n=0 b good reg=${REGISTRY_URL/host.docker.internal/127.0.0.1} auth=()
  [ -z "${REGISTRY_USER:-}" ] || auth=(-u "$REGISTRY_USER:$REGISTRY_PASS")
  while :; do
    id=$(curl -fsS ${auth[@]+"${auth[@]}"} -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      -d "{\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Greeting$n\\\",\\\"fields\\\":[{\\\"name\\\":\\\"text\\\",\\\"type\\\":\\\"string\\\"}]}\"}" \
      "$reg/subjects/$t-g$n-value/versions" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
    [ -n "$id" ] || return 1
    good=1
    for b in $((id >> 24 & 255)) $((id >> 16 & 255)) $((id >> 8 & 255)) $((id & 255)); do
      if [ "$b" -eq 10 ] || [ "$b" -eq 13 ] || [ "$b" -gt 127 ]; then good=0; fi
    done
    [ $good = 1 ] && break
    n=$((n + 1)); [ $n -lt 20 ] || return 1
  done
  # Magic byte 0, the id in four bytes, then Avro: "greets" is length 6 (zigzag 0x0c).
  printf "\\x00\\x$(printf %02x $((id >> 24 & 255)))\\x$(printf %02x $((id >> 16 & 255)))\\x$(printf %02x $((id >> 8 & 255)))\\x$(printf %02x $((id & 255)))\\x0cgreets\n" |
    kafka_tool kafka-console-producer.sh --topic "$t" >/dev/null 2>&1 || return 1
  echo "$id"
}
