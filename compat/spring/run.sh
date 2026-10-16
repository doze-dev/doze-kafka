#!/usr/bin/env bash
# Spring Kafka (a Spring Boot app) in a container: KafkaAdmin and NewTopic beans, including a
# partition-count bump on the second start, and a transactional listener.
set -euo pipefail
command -v docker >/dev/null && docker info >/dev/null 2>&1 || { echo "docker is not available"; exit 77; }
IMAGE=${JAVA_IMAGE:-maven:3.9-eclipse-temurin-21}
mkdir -p "$HOME/.m2"
CA=""
if [ "$SECURITY" = scram-tls ]; then CA="-v $CA_CERT:/ca.pem:ro"; fi
# The app's Kafka settings are docs/frameworks.md's Spring block as a reader copies it
# (Check.java imports the file; the address comes in as KAFKA_BOOTSTRAP_SERVERS, as the
# block expects), so a snippet that does not work fails this cell.
. ../lib.sh
DOC_PROPS="$WORK/spring-doc-$$.properties"
doc_properties "Spring Kafka" > "$DOC_PROPS" || exit 1
# shellcheck disable=SC2086
exec docker run --rm ${HOSTS_FLAG:-} $CA -v "$PWD":/work -v "$HOME/.m2":/root/.m2 -v "$DOC_PROPS:/doc.properties:ro" -w /work \
  -e BOOTSTRAP -e KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP" -e SECURITY -e SASL_USER -e SASL_PASS -e SASL_MECHANISM -e CA_CERT=/ca.pem -e REGISTRY_URL \
  "$IMAGE" mvn -q -B compile exec:java
