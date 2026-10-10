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
