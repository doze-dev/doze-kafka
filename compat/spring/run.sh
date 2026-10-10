#!/usr/bin/env bash
# Spring Kafka (a Spring Boot app) in a container: KafkaAdmin and NewTopic beans, including a
# partition-count bump on the second start, and a transactional listener.
set -euo pipefail
command -v docker >/dev/null && docker info >/dev/null 2>&1 || { echo "docker is not available"; exit 77; }
IMAGE=${JAVA_IMAGE:-maven:3.9-eclipse-temurin-21}
mkdir -p "$HOME/.m2"
CA=""
if [ "$SECURITY" = scram-tls ]; then CA="-v $CA_CERT:/ca.pem:ro"; fi
# shellcheck disable=SC2086
exec docker run --rm ${HOSTS_FLAG:-} $CA -v "$PWD":/work -v "$HOME/.m2":/root/.m2 -w /work \
  -e BOOTSTRAP -e SECURITY -e SASL_USER -e SASL_PASS -e SASL_MECHANISM -e CA_CERT=/ca.pem -e REGISTRY_URL \
  "$IMAGE" mvn -q -B compile exec:java
