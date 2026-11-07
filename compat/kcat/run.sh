#!/usr/bin/env bash
# kcat (librdkafka): metadata, produce with a key and headers, consume, a group that
# resumes. (scripts/kcat-check.sh is the deeper, broker-booting version CI runs.)
set -euo pipefail
KCAT=${KCAT:-$(command -v kcat || command -v kafkacat || true)}
[ -n "$KCAT" ] || { echo "kcat is not installed"; exit 77; }
A=(-b "$BOOTSTRAP" -X client.id=compat-kcat)
if [ "$SECURITY" = scram-tls ]; then
  A+=(-X security.protocol=SASL_SSL -X sasl.mechanism="$SASL_MECHANISM" -X sasl.username="$SASL_USER" -X sasl.password="$SASL_PASS" -X ssl.ca.location="$CA_CERT")
fi
# kcat is the one installed (apt's in CI), not a pinned version: say which ran.
echo "ok   kcat $("$KCAT" -V 2>&1 | sed -n 's/^Version \([^ ]*\) .*librdkafka \([^ ]*\).*/\1, librdkafka \2/p')"
T=kcat-$RANDOM
"$KCAT" "${A[@]}" -L -t "$T" 2>&1 | grep -q "broker" || { echo "FAIL metadata"; exit 1; }
printf 'k1:one\nk2:two\nk3:three\n' | "$KCAT" "${A[@]}" -P -t "$T" -K: -H trace=abc 2>&1 | grep -v -E "^%[0-9]" || true
OUT=$("$KCAT" "${A[@]}" -C -t "$T" -o beginning -e -f '%k=%s %h\n' 2>/dev/null)
[ "$(echo "$OUT" | wc -l | tr -d ' ')" = 3 ] && echo "$OUT" | grep -q "k2=two trace=abc" || { echo "FAIL consume: $OUT"; exit 1; }
echo "ok   produce with key and header, consume from the start"
G=g-$RANDOM
"$KCAT" "${A[@]}" -o beginning -e -f '%s\n' -G "$G" "$T" 2>/dev/null | grep -c . | grep -q 3 || { echo "FAIL group consume"; exit 1; }
N=$("$KCAT" "${A[@]}" -e -f '%s\n' -G "$G" "$T" 2>/dev/null | grep -c . || true)
[ "$N" = 0 ] || { echo "FAIL a resumed group re-read $N records"; exit 1; }
echo "ok   a consumer group commits and resumes"

# Avro through the registry (Stage 5): a record framed with a registered schema's id
# (magic byte 0, the id in four bytes, then Avro's binary encoding) is decoded by kcat
# with -s value=avro, which asks the registry for the schema by id.
"$KCAT" -V 2>&1 | grep -q 'Avro' || { echo "FAIL this kcat was built without Avro (libserdes): scripts/install-kcat.sh builds it with"; exit 1; }
REG=$REGISTRY_URL; CURL_AUTH=()
if [ -n "${REGISTRY_USER:-}" ]; then
  REG=${REGISTRY_URL/:\/\//://$REGISTRY_USER:$REGISTRY_PASS@}; CURL_AUTH=(-u "$REGISTRY_USER:$REGISTRY_PASS")
fi
AT=kcat-avro-$RANDOM
ID=$(curl -fsS ${CURL_AUTH[@]+"${CURL_AUTH[@]}"} -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"n\",\"type\":\"int\"}]}"}' \
  "$REGISTRY_URL/subjects/$AT-value/versions" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
[ -n "$ID" ] || { echo "FAIL registering the Avro schema"; exit 1; }
# text "hello" (length 5, zigzag 0x0a) and n 21 (zigzag 0x2a).
printf "\\x00\\x$(printf %02x $((ID >> 24 & 255)))\\x$(printf %02x $((ID >> 16 & 255)))\\x$(printf %02x $((ID >> 8 & 255)))\\x$(printf %02x $((ID & 255)))\\x0ahello\\x2a" >"$WORK/kcat-avro.bin"
"$KCAT" "${A[@]}" -P -t "$AT" "$WORK/kcat-avro.bin" 2>&1 | grep -v -E "^%[0-9]" || true
OUT=$("$KCAT" "${A[@]}" -C -t "$AT" -o beginning -e -s value=avro -r "$REG" -f '%s\n' 2>&1)
echo "$OUT" | grep -q '"text": *"hello"' && echo "$OUT" | grep -q '"n": *21' || { echo "FAIL kcat's Avro decode of id $ID: $OUT"; exit 1; }
echo "ok   kcat decodes an Avro record through the registry (id $ID)"
