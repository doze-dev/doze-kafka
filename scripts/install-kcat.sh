#!/usr/bin/env bash
# Build kcat against a librdkafka that has client telemetry (KIP-714 needs
# librdkafka 2.5 or newer), from pinned sources, and install both under a prefix.
#
#   scripts/install-kcat.sh [prefix]     default prefix: /usr/local (uses sudo when it must)
#
# Why not the distribution's package: Ubuntu 24.04's kcat links librdkafka 2.3,
# so compat/kcat-telemetry skipped itself in CI and the cell never ran. The
# versions here are the ones the matrix passes with on a developer's machine
# (kcat 1.7 on librdkafka 2.15.1), pinned in compat/versions.env. Both are fetched
# by commit, so what is built cannot change under the same file.
#
# Needs: a C toolchain, pkg-config, and the headers of zlib, zstd, lz4, OpenSSL,
# SASL, curl and yajl. On Debian or Ubuntu:
#   apt-get install -y build-essential pkg-config zlib1g-dev libzstd-dev liblz4-dev \
#     libssl-dev libsasl2-dev libcurl4-openssl-dev libyajl-dev curl ca-certificates
set -euo pipefail

# The pins are compat/versions.env's (docs/clients.md's table is held to the same file).
# shellcheck source=../compat/versions.env
. "$(cd "$(dirname "$0")/.." && pwd)/compat/versions.env"
: "${KCAT_VERSION:?}" "${KCAT_COMMIT:?}" "${LIBRDKAFKA_VERSION:?}" "${LIBRDKAFKA_COMMIT:?}"

prefix=${1:-/usr/local}
sudo=""
if [ ! -w "$prefix" ] && [ "$(id -u)" != 0 ]; then sudo=sudo; fi
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)
# Build logs go beside the sources, never into them: mklove reads every
# ./configure.<name> as a module, so a "configure.log" there breaks configure.
logs=$work/logs
mkdir -p "$logs"

fetch() { # repo commit dir
  mkdir -p "$3"
  curl -fsSL "https://github.com/$1/archive/$2.tar.gz" | tar -xz -C "$3" --strip-components 1
}

echo "== librdkafka v$LIBRDKAFKA_VERSION ($LIBRDKAFKA_COMMIT)"
fetch confluentinc/librdkafka "$LIBRDKAFKA_COMMIT" "$work/librdkafka"
name=librdkafka
( cd "$work/librdkafka" && ./configure --prefix="$prefix" >"$logs/$name-configure" 2>&1 || { tail -40 "$logs/$name-configure"; exit 1; }
  make -j"$jobs" libs >"$logs/$name-make" 2>&1 || { tail -60 "$logs/$name-make"; exit 1; }
  $sudo make -C src install >"$logs/$name-install" 2>&1 || { tail -40 "$logs/$name-install"; exit 1; } )

echo "== kcat $KCAT_VERSION ($KCAT_COMMIT)"
fetch edenhill/kcat "$KCAT_COMMIT" "$work/kcat"
name=kcat
( cd "$work/kcat"
  export PKG_CONFIG_PATH="$prefix/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
  ./configure --prefix="$prefix" >"$logs/$name-configure" 2>&1 || { tail -40 "$logs/$name-configure"; exit 1; }
  make -j"$jobs" >"$logs/$name-make" 2>&1 || { tail -60 "$logs/$name-make"; exit 1; }
  $sudo make install >"$logs/$name-install" 2>&1 || { tail -40 "$logs/$name-install"; exit 1; } )
# The shared library is under the prefix; tell the loader (Linux).
if command -v ldconfig >/dev/null 2>&1; then $sudo ldconfig 2>/dev/null || true; fi

# The point of all this: say what was built, and fail if it is not what the cell needs.
out=$("$prefix/bin/kcat" -V 2>&1) || { echo "$out"; echo "the kcat just built does not run" >&2; exit 1; }
echo "$out" | sed -n 's/^Version/kcat version/p'
ver=$(echo "$out" | sed -n 's/.*librdkafka \([0-9]*\.[0-9]*\.[0-9]*\).*/\1/p' | head -1)
[ "$ver" = "$LIBRDKAFKA_VERSION" ] || { echo "kcat links librdkafka '$ver', wanted $LIBRDKAFKA_VERSION" >&2; exit 1; }
echo "ok       $prefix/bin/kcat links librdkafka $ver"
