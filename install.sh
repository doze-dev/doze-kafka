#!/usr/bin/env bash
#
# doze-kafka installer — downloads the prebuilt binary for your OS/arch from the
# GitHub releases, verifies its checksum, and installs it to ~/.doze-kafka/bin.
#
#   curl -o- https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | bash
#   wget -qO- https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | bash
#
# Pin a version by using its tag in the URL (.../doze-kafka/v1.0.0/install.sh) or by
# setting DOZE_KAFKA_VERSION=v1.0.0. Override the install location with
# DOZE_KAFKA_DIR (default: ~/.doze-kafka).

set -euo pipefail

REPO="doze-dev/doze-kafka"
INSTALL_DIR="${DOZE_KAFKA_DIR:-$HOME/.doze-kafka}"
BIN_DIR="$INSTALL_DIR/bin"

die() {
	echo "install: $*" >&2
	exit 1
}

have() { command -v "$1" >/dev/null 2>&1; }

# --- detect platform ------------------------------------------------------------

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$os" in
linux) os=linux ;;
darwin) os=darwin ;;
*) die "unsupported OS '$os'. This installer supports Linux and macOS; grab a binary from https://github.com/$REPO/releases for anything else." ;;
esac

arch="$(uname -m)"
case "$arch" in
x86_64 | amd64) arch=amd64 ;;
arm64 | aarch64) arch=arm64 ;;
*) die "unsupported architecture '$arch'." ;;
esac

# --- download tool --------------------------------------------------------------

if have curl; then
	download() { curl -fsSL "$1" -o "$2"; }
	fetch() { curl -fsSL "$1"; }
elif have wget; then
	download() { wget -qO "$2" "$1"; }
	fetch() { wget -qO- "$1"; }
else
	die "need curl or wget to download."
fi

# --- resolve version ------------------------------------------------------------

version="${DOZE_KAFKA_VERSION:-}"
if [ -z "$version" ]; then
	echo "Resolving the latest release..."
	version="$(fetch "https://api.github.com/repos/$REPO/releases/latest" |
		grep -m1 '"tag_name"' | cut -d'"' -f4 || true)"
fi
[ -n "$version" ] || die "could not resolve a release version. Set DOZE_KAFKA_VERSION=vX.Y.Z."

ver="${version#v}" # asset names omit the leading 'v'
asset="doze-kafka_${ver}_${os}_${arch}.tar.gz"
base="https://github.com/$REPO/releases/download/$version"

# --- download + verify ----------------------------------------------------------

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "Downloading $asset ($version)..."
download "$base/$asset" "$tmp/$asset" || die "could not download $asset. Does a release exist for $version on $os/$arch?"
download "$base/checksums.txt" "$tmp/checksums.txt" || die "could not download checksums.txt for $version."

echo "Verifying checksum..."
expected="$(grep " ${asset}\$" "$tmp/checksums.txt" | awk '{print $1}' || true)"
[ -n "$expected" ] || die "no checksum found for $asset."
if have sha256sum; then
	actual="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
elif have shasum; then
	actual="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
else
	die "need sha256sum or shasum to verify the download."
fi
[ "$expected" = "$actual" ] || die "checksum mismatch for $asset (expected $expected, got $actual)."

# --- install --------------------------------------------------------------------

mkdir -p "$BIN_DIR"
tar -xzf "$tmp/$asset" -C "$tmp"
install -m 0755 "$tmp/doze-kafka" "$BIN_DIR/doze-kafka"
echo "Installed doze-kafka $version to $BIN_DIR/doze-kafka"

# --- PATH -----------------------------------------------------------------------

add_path_line="export PATH=\"$BIN_DIR:\$PATH\""

profile=""
case "${SHELL:-}" in
*/zsh) profile="$HOME/.zshrc" ;;
*/bash) [ -f "$HOME/.bashrc" ] && profile="$HOME/.bashrc" || profile="$HOME/.bash_profile" ;;
esac

if [ -n "$profile" ] && ! grep -qs "$BIN_DIR" "$profile" 2>/dev/null; then
	{
		echo ""
		echo "# doze-kafka"
		echo "$add_path_line"
	} >>"$profile"
	echo "Added $BIN_DIR to your PATH in $profile — open a new shell or run:"
	echo "  $add_path_line"
else
	echo "Add $BIN_DIR to your PATH if it isn't already:"
	echo "  $add_path_line"
fi

echo ""
echo "Done. Start a broker with:  doze-kafka"
