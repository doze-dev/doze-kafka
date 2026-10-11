# Shared by the telemetry cells (KIP-714): a client that pushes its own metrics shows
# them on its page in Traffic. Source it: . ../lib-telemetry.sh
#
# wait_metrics CLIENT_ID SECONDS: pass once the console's Clients view lists a
# connection of that client whose inspector says what it reports, with at least one
# standard client metric (the broker strips the "org.apache.kafka." prefix).
wait_metrics() {
  local id=$1 end=$((SECONDS + $2)) clients="" conn="" insp=""
  [ -n "${CONSOLE_URL:-}" ] || { echo "CONSOLE_URL is not set: run this through compat/run.sh"; exit 2; }
  while [ $SECONDS -lt $end ]; do
    clients=$(curl -fsS "$CONSOLE_URL/traffic?view=clients" 2>/dev/null | tr '\n' ' ' || true)
    # A client is a row (its connection count follows its name): id="cl-N" ... <td class="m b nw">CLIENT</td>.
    for conn in $(printf '%s' "$clients" | grep -o "id=\"cl-[0-9]*\"[^>]*>[^<]*<td class=\"m b nw cell-client\"><a [^>]*>$id<" | grep -o 'cl-[0-9]*' | sed 's/cl-//'); do
      insp=$(curl -fsS "$CONSOLE_URL/inspect/client?key=$conn" 2>/dev/null | tr '\n' ' ' || true)
      if printf '%s' "$insp" | grep -q 'Metrics reported by the client itself' && printf '%s' "$insp" | grep -Eq 'producer\.|consumer\.|connection\.|request\.|Request '; then
        return 0
      fi
    done
    sleep 1
  done
  return 1
}
