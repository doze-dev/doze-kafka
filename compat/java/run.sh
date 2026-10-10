#!/usr/bin/env bash
# The Java clients and Kafka Streams, in a container (there is no JDK to assume on the host).
#   KAFKA_VERSION  the kafka-clients release to test (default: the one in pom.xml)
set -euo pipefail
command -v docker >/dev/null && docker info >/dev/null 2>&1 || { echo "docker is not available"; exit 77; }
IMAGE=${JAVA_IMAGE:-maven:3.9-eclipse-temurin-21}
mkdir -p "$HOME/.m2"
EXTRA=()
[ -n "${KAFKA_VERSION:-}" ] && EXTRA+=(-Dkafka.version="$KAFKA_VERSION")
CA=""
if [ "$SECURITY" = scram-tls ]; then CA="-v $CA_CERT:/ca.pem:ro"; fi
# shellcheck disable=SC2086
exec docker run --rm ${HOSTS_FLAG:-} $CA -v "$PWD":/work -v "$HOME/.m2":/root/.m2 -w /work \
  -e BOOTSTRAP -e SECURITY -e SASL_USER -e SASL_PASS -e SASL_MECHANISM -e CA_CERT=/ca.pem -e REGISTRY_URL -e REGISTRY_USER -e REGISTRY_PASS \
  "$IMAGE" mvn -q -B ${EXTRA[@]+"${EXTRA[@]}"} compile exec:java -Dexec.args="${COMPAT_SECTIONS:-clients streams serdes}"
