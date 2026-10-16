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
