#!/bin/sh
#
# doze-kafka installer — POSIX sh (runs under dash, busybox ash, bash, zsh, ksh).
# Downloads the prebuilt binary for your OS/arch from the GitHub releases, verifies
# its SHA-256 checksum, and installs it to ~/.doze-kafka/bin.
#
#   curl -fsSL https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh
#   wget -qO-  https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh
#
# Configuration (all optional, via environment):
#   DOZE_KAFKA_VERSION         pin a version, e.g. v1.0.0 (default: latest release)
#   DOZE_KAFKA_DIR             install prefix (default: ~/.doze-kafka; binary in bin/)
#   DOZE_KAFKA_NO_MODIFY_PATH  set to 1 to leave your shell profile untouched
#
# You can also pin a version by putting its tag in the URL:
#   .../doze-dev/doze-kafka/v1.0.0/install.sh

set -eu

REPO="doze-dev/doze-kafka"
INSTALL_DIR="${DOZE_KAFKA_DIR:-$HOME/.doze-kafka}"
BIN_DIR="$INSTALL_DIR/bin"

die() {
	echo "install: $*" >&2
	exit 1
}

have() { command -v "$1" >/dev/null 2>&1; }
need() { have "$1" || die "required command not found: $1"; }

# --- preflight ------------------------------------------------------------------

for c in uname mktemp tar mkdir chmod cp tr grep awk cut; do
	need "$c"
done

# --- detect platform ------------------------------------------------------------

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$os" in
linux) os=linux ;;
darwin) os=darwin ;;
*) die "unsupported OS '$os'. This installer covers Linux and macOS; for anything else (Windows included) grab a binary from https://github.com/$REPO/releases." ;;
esac

arch="$(uname -m)"
case "$arch" in
x86_64 | amd64) arch=amd64 ;;
arm64 | aarch64) arch=arm64 ;;
*) die "unsupported architecture '$arch'. See https://github.com/$REPO/releases." ;;
esac

# --- download + checksum tools --------------------------------------------------

if have curl; then
	# --proto '=https' and --tlsv1.2 refuse anything but modern HTTPS; --retry rides
	# out flaky networks.
	download() { curl --proto '=https' --tlsv1.2 --retry 3 -fsSL "$1" -o "$2"; }
	fetch() { curl --proto '=https' --tlsv1.2 --retry 3 -fsSL "$1"; }
elif have wget; then
	download() { wget -qO "$2" "$1"; }
	fetch() { wget -qO- "$1"; }
else
	die "need curl or wget to download."
fi

if have sha256sum; then
	sha256() { sha256sum "$1" | awk '{print $1}'; }
elif have shasum; then
	sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
else
	die "need sha256sum or shasum to verify the download."
fi

# --- resolve version ------------------------------------------------------------

version="${DOZE_KAFKA_VERSION:-}"
if [ -z "$version" ]; then
	echo "Resolving the latest release..."
	version="$(fetch "https://api.github.com/repos/$REPO/releases/latest" |
		grep '"tag_name"' | head -n 1 | cut -d'"' -f4 || true)"
fi
[ -n "$version" ] || die "could not resolve a release version. Set DOZE_KAFKA_VERSION=vX.Y.Z."

ver="${version#v}" # asset names omit the leading 'v'
asset="doze-kafka_${ver}_${os}_${arch}.tar.gz"
base="https://github.com/$REPO/releases/download/$version"

# --- download + verify ----------------------------------------------------------

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

echo "Downloading $asset ($version)..."
download "$base/$asset" "$tmp/$asset" || die "could not download $asset. Does a release exist for $version on $os/$arch?"
download "$base/checksums.txt" "$tmp/checksums.txt" || die "could not download checksums.txt for $version."

echo "Verifying checksum..."
expected="$(awk -v f="$asset" '$2 == f {print $1}' "$tmp/checksums.txt" || true)"
[ -n "$expected" ] || die "no checksum listed for $asset in checksums.txt."
actual="$(sha256 "$tmp/$asset")"
[ "$expected" = "$actual" ] || die "checksum mismatch for $asset (expected $expected, got $actual)."

# --- install --------------------------------------------------------------------

mkdir -p "$BIN_DIR"
tar -xzf "$tmp/$asset" -C "$tmp"
cp "$tmp/doze-kafka" "$BIN_DIR/doze-kafka"
chmod 0755 "$BIN_DIR/doze-kafka"
echo "Installed doze-kafka $version to $BIN_DIR/doze-kafka"

# Non-fatal smoke check: the binary is already in place, but a bad OS/arch match
# would surface here rather than the first time the user runs it.
if ! "$BIN_DIR/doze-kafka" version >/dev/null 2>&1; then
	echo "warning: the installed binary did not run cleanly — check your OS/arch." >&2
fi

# --- PATH -----------------------------------------------------------------------

add_path_line="export PATH=\"$BIN_DIR:\$PATH\""

profile=""
if [ "${DOZE_KAFKA_NO_MODIFY_PATH:-}" != "1" ]; then
	case "${SHELL:-}" in
	*/zsh) profile="$HOME/.zshrc" ;;
	*/bash)
		if [ -f "$HOME/.bashrc" ]; then profile="$HOME/.bashrc"; else profile="$HOME/.bash_profile"; fi
		;;
	esac
fi

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
