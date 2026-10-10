#!/usr/bin/env bash
# The client-compatibility matrix: real clients and frameworks against the real binary.
#
#   compat/run.sh [-b path/to/doze-kafka] [client ...]
#
# Each directory under compat/ is one client: a self-contained project with a `run.sh`
# that takes its configuration from the environment and exits 0 (passed), 77 (the
# toolchain it needs is not installed: SKIPPED, which is not a pass) or anything else
# (failed). This runner boots the binary on free ports over fresh data directories, once
# without authentication and once with SASL/SCRAM over TLS, and runs every client against
# each, then prints what passed, what was skipped and what failed.
#
# Environment the client scripts get:
#   BOOTSTRAP         host:port of the broker
#   SECURITY          plain | scram-tls
#   SASL_USER, SASL_PASS, SASL_MECHANISM   (scram-tls) SCRAM-SHA-512 credentials
#   CA_CERT           (scram-tls) PEM file that signed the broker's certificate
#   REGISTRY_URL      the built-in schema registry (plain only)
#   BROKER_BIN        the binary under test, for scripts that restart it
#   WORK              a scratch directory the script may use
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(dirname "$HERE")
BIN=""
while getopts "b:" o; do case $o in b) BIN=$OPTARG ;; *) exit 2 ;; esac; done
shift $((OPTIND-1))
if [ -z "$BIN" ]; then
  BIN=$(mktemp -d)/doze-kafka
  (cd "$ROOT" && go build -o "$BIN" ./cmd/doze-kafka) || { echo "build failed" >&2; exit 2; }
fi
CLIENTS=("$@")
if [ ${#CLIENTS[@]} -eq 0 ]; then
  for d in "$HERE"/*/; do [ -f "$d/run.sh" ] && CLIENTS+=("$(basename "$d")"); done
fi

# Under $HOME, not /tmp: Docker (Colima, Docker Desktop) shares the home directory with its VM and not the system temp directory, and a client in a container needs the CA file.
WORK=$(mktemp -d "$HOME/.doze-compat.XXXXXX")
PIDS=()
cleanup() { for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "$p" 2>/dev/null; done; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
wait_port() { for _ in $(seq 1 100); do (echo >/dev/tcp/127.0.0.1/"$1") 2>/dev/null && return 0; sleep 0.1; done; return 1; }

# A self-signed certificate for 127.0.0.1, localhost and host.docker.internal.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1" >/dev/null 2>&1 || { echo "openssl failed" >&2; exit 2; }

# start NAME FLAGS... -> BOOTSTRAP, REGISTRY_URL
#
# A client in a container cannot reach 127.0.0.1 on the host, and the broker tells every
# client to reconnect to the address it advertises. Clients that run in Docker (those
# with a `docker` file in their directory) therefore get a broker of their own that
# listens on every interface and advertises host.docker.internal.
HOST=127.0.0.1
start() {
  local name=$1; shift
  local kp rp cp; kp=$(free_port); rp=$(free_port); cp=$(free_port)
  local listen=127.0.0.1 extra=()
  if [ "$HOST" != 127.0.0.1 ]; then listen=0.0.0.0; extra=(--advertised-host "$HOST"); fi
  BOOTSTRAP=$HOST:$kp; REGISTRY_URL=http://$HOST:$rp
  "$BIN" --listen "$listen:$kp" --console-addr "127.0.0.1:$cp" --schema-registry-addr "$listen:$rp" \
    ${extra[@]+"${extra[@]}"} --data-dir "$WORK/$name" "$@" >"$WORK/$name.log" 2>&1 &
  PIDS+=($!)
  wait_port "$kp" || { echo "broker $name did not start:"; cat "$WORK/$name.log"; exit 2; }
}

declare -a RESULTS
run_all() { # security docker(yes|no)
  local sec=$1 dock=$2
  for c in "${CLIENTS[@]}"; do
    local isdock=no; [ -f "$HERE/$c/docker" ] && isdock=yes
    [ "$isdock" = "$dock" ] || continue
    [ -f "$HERE/$c/run.sh" ] || { echo "no such client: $c" >&2; exit 2; }
    local log="$WORK/$c-$sec.log"
    echo "== $c ($sec)"
    ( cd "$HERE/$c" && SECURITY=$sec BOOTSTRAP=$BOOTSTRAP REGISTRY_URL=$REGISTRY_URL BROKER_BIN=$BIN WORK=$WORK \
        SASL_USER=${SASL_USER:-} SASL_PASS=${SASL_PASS:-} SASL_MECHANISM=${SASL_MECHANISM:-} CA_CERT=${CA_CERT:-} HOSTS_FLAG=${HOSTS_FLAG:-} \
        bash ./run.sh ) 2>&1 | tee "$log" | sed 's/^/   /'
    local code=${PIPESTATUS[0]}
    case $code in
      0) RESULTS+=("PASS  $c ($sec)") ;;
      77) RESULTS+=("SKIP  $c ($sec): $(tail -1 "$log")") ;;
      *) RESULTS+=("FAIL  $c ($sec)") ;;
    esac
  done
}

export SASL_USER=dev SASL_PASS=dev-pass SASL_MECHANISM=SCRAM-SHA-512 CA_CERT=$WORK/cert.pem
SECURE=(--sasl-mechanisms SCRAM-SHA-512 --sasl-user "$SASL_USER:$SASL_PASS" --tls-cert "$WORK/cert.pem" --tls-key "$WORK/key.pem")
for dock in no yes; do
  if [ $dock = yes ]; then
    HOST=host.docker.internal
    command -v docker >/dev/null && docker info >/dev/null 2>&1 || { for c in "${CLIENTS[@]}"; do [ -f "$HERE/$c/docker" ] && RESULTS+=("SKIP  $c: docker is not available"); done; continue; }
    # The broker is told its own name; the host's /etc/hosts may not have it.
    HOSTS_FLAG="--add-host=host.docker.internal:host-gateway"; export HOSTS_FLAG
    # On the host the name must resolve for the readiness probe: use loopback for that.
  fi
  any=no; for c in "${CLIENTS[@]}"; do [ -f "$HERE/$c/docker" ] && [ $dock = yes ] && any=yes; [ ! -f "$HERE/$c/docker" ] && [ $dock = no ] && any=yes; done
  [ $any = yes ] || continue
  start plain-$dock; REG=$REGISTRY_URL; run_all plain $dock
  start secure-$dock "${SECURE[@]}"; REGISTRY_URL=""; run_all scram-tls $dock
done

echo; echo "---- matrix"
fail=0
for r in "${RESULTS[@]}"; do echo "$r"; case $r in FAIL*) fail=1 ;; esac; done
exit $fail
