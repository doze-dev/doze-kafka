# Shared by the telemetry cells (KIP-714): a client that pushes its own metrics shows
# them on the Traffic page. Source it: . ../lib-telemetry.sh
#
# wait_metrics CLIENT_ID SECONDS: pass once the console lists a card of that client with
# at least one standard client metric (the broker strips the "org.apache.kafka." prefix).
wait_metrics() {
  local id=$1 end=$((SECONDS + $2)) page=""
  [ -n "${CONSOLE_URL:-}" ] || { echo "CONSOLE_URL is not set: run this through compat/run.sh"; exit 2; }
  while [ $SECONDS -lt $end ]; do
    page=$(curl -fsS "$CONSOLE_URL/traffic" 2>/dev/null || true)
    if printf '%s' "$page" | grep -q "Metrics from $id" && printf '%s' "$page" | grep -Eq 'producer\.|consumer\.|connection\.|request\.'; then
      return 0
    fi
    sleep 1
  done
  return 1
}
