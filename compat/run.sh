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
# The matrix is every client asked for (all of them when none is named) times the two
# securities: a cell for each, and the run is green only when every cell PASSED. A
# skipped cell fails the run. A machine that lacks a toolchain says so on purpose:
#
#   ALLOW_SKIP="kcat-telemetry spring" compat/run.sh     these clients may skip
#
# and that is refused when CI is set: in CI nothing may skip. The runner checks its own
# result against the list of cells it set out to run, so a cell that was never run (and
# so is neither PASS, SKIP nor FAIL) fails it too.
#
# COMPAT_CLIENTS_DIR (default: this directory) is where the client directories are; the
# runner's own test points it at fakes (compat/runner_test.go).
#
# Environment the client scripts get:
#   BOOTSTRAP         host:port of the broker
#   SECURITY          plain | scram-tls
#   SASL_USER, SASL_PASS, SASL_MECHANISM   (scram-tls) SCRAM-SHA-512 credentials
#   CA_CERT           (scram-tls) PEM file that signed the broker's certificate
#   REGISTRY_URL      the built-in schema registry (REGISTRY_USER and REGISTRY_PASS under scram-tls)
#   CONSOLE_URL       the console, for a cell that asserts on what it shows (http://127.0.0.1:PORT/_console)
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
HERE=${COMPAT_CLIENTS_DIR:-$HERE}
CLIENTS=("$@")
if [ ${#CLIENTS[@]} -eq 0 ]; then
  for d in "$HERE"/*/; do [ -f "$d/run.sh" ] && CLIENTS+=("$(basename "$d")"); done
fi
[ ${#CLIENTS[@]} -gt 0 ] || { echo "no client directory with a run.sh under $HERE" >&2; exit 2; }
for c in "${CLIENTS[@]}"; do [ -f "$HERE/$c/run.sh" ] || { echo "no such client: $c" >&2; exit 2; }; done

# Who may skip. Nobody in CI.
ALLOWED=()
for c in $(printf '%s' "${ALLOW_SKIP:-}" | tr ',' ' '); do
  [ -f "$HERE/$c/run.sh" ] || { echo "ALLOW_SKIP names $c, which is not a client" >&2; exit 2; }
  ALLOWED+=("$c")
done
if [ -n "${CI:-}" ] && [ ${#ALLOWED[@]} -gt 0 ]; then
  echo "ALLOW_SKIP is set (${ALLOWED[*]}) and so is CI: in CI every cell runs, or the job fails" >&2; exit 2
fi
may_skip() { local c; for c in ${ALLOWED[@]+"${ALLOWED[@]}"}; do [ "$c" = "$1" ] && return 0; done; return 1; }

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
  # The console is on the host's loopback for every client: a script reads the Traffic
  # page from there (the telemetry cells do).
  CONSOLE_URL=http://127.0.0.1:$cp/_console
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
    local log="$WORK/$c-$sec.log"
    echo "== $c ($sec)"
    ( cd "$HERE/$c" && SECURITY=$sec BOOTSTRAP=$BOOTSTRAP REGISTRY_URL=$REGISTRY_URL CONSOLE_URL=$CONSOLE_URL BROKER_BIN=$BIN WORK=$WORK \
        REGISTRY_USER=${SASL_USER:-} REGISTRY_PASS=${SASL_PASS:-} SASL_USER=${SASL_USER:-} SASL_PASS=${SASL_PASS:-} SASL_MECHANISM=${SASL_MECHANISM:-} CA_CERT=${CA_CERT:-} HOSTS_FLAG=${HOSTS_FLAG:-} \
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
# The registry is kept on plain HTTP here (--schema-registry-tls=false): every cell
# reads REGISTRY_URL as http:// and none is given the CA for it yet. The broker serves
# it over TLS by default; cmd/doze-kafka's registry_tls_test.go is what holds that.
SECURE=(--sasl-mechanisms SCRAM-SHA-512 --sasl-user "$SASL_USER:$SASL_PASS" --tls-cert "$WORK/cert.pem" --tls-key "$WORK/key.pem" --schema-registry-tls=false)
for dock in no yes; do
  if [ $dock = yes ]; then
    HOST=host.docker.internal
    command -v docker >/dev/null && docker info >/dev/null 2>&1 || { for c in "${CLIENTS[@]}"; do [ -f "$HERE/$c/docker" ] && RESULTS+=("SKIP  $c (plain): docker is not available" "SKIP  $c (scram-tls): docker is not available"); done; continue; }
    # The broker is told its own name; the host's /etc/hosts may not have it.
    HOSTS_FLAG="--add-host=host.docker.internal:host-gateway"; export HOSTS_FLAG
    # On the host the name must resolve for the readiness probe: use loopback for that.
  fi
  any=no; for c in "${CLIENTS[@]}"; do [ -f "$HERE/$c/docker" ] && [ $dock = yes ] && any=yes; [ ! -f "$HERE/$c/docker" ] && [ $dock = no ] && any=yes; done
  [ $any = yes ] || continue
  start plain-$dock; REG=$REGISTRY_URL; run_all plain $dock
  # The registry takes the broker's SASL credentials as HTTP basic auth.
  start secure-$dock "${SECURE[@]}"; run_all scram-tls $dock
done

echo; echo "---- matrix"
for r in ${RESULTS[@]+"${RESULTS[@]}"}; do echo "$r"; done

# The verdict, against the cells this run set out to run: each must be there once, and
# be a PASS (or the SKIP of a client that was allowed one).
fail=0; passed=0; skipped=0; expected=0
for c in "${CLIENTS[@]}"; do
  for sec in plain scram-tls; do
    expected=$((expected + 1))
    found=""
    for r in ${RESULTS[@]+"${RESULTS[@]}"}; do
      case $r in
        "PASS  $c ($sec)") found="$found PASS" ;;
        "SKIP  $c ($sec)"*) found="$found SKIP" ;;
        "FAIL  $c ($sec)") found="$found FAIL" ;;
      esac
    done
    case $found in
      " PASS") passed=$((passed + 1)) ;;
      " SKIP")
        skipped=$((skipped + 1))
        if may_skip "$c"; then echo "allowed to skip (ALLOW_SKIP): $c ($sec)"
        else echo "SKIPPED, which is not a pass: $c ($sec). Install what it needs, or name it in ALLOW_SKIP (never in CI)" >&2; fail=1; fi ;;
      " FAIL") fail=1 ;;
      "") echo "NOT RUN: $c ($sec) has no result" >&2; fail=1 ;;
      *) echo "RUN MORE THAN ONCE: $c ($sec):$found" >&2; fail=1 ;;
    esac
  done
done
echo "$passed of $expected cells passed, $skipped skipped (${#CLIENTS[@]} clients, plain and scram-tls)"
[ "$fail" = 0 ] && [ $((passed + skipped)) -eq "$expected" ] || { echo "the matrix is NOT green" >&2; exit 1; }
exit 0
