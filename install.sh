#!/bin/sh
#
# doze-kafka installer — POSIX sh (runs under dash, busybox ash, bash, zsh, ksh).
# Downloads the prebuilt binary for your OS/arch from the GitHub releases, verifies
# its SHA-256 checksum, and installs it to ~/.doze-kafka/bin.
#
#   curl -fsSL https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh
#   wget -qO-  https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh
#
# Install a specific version by passing --version through the pipe:
#   curl -fsSL .../install.sh | sh -s -- --version v1.0.0
#
# Uninstall (removes the binary and the PATH entry; leaves your data alone):
#   curl -fsSL .../install.sh | sh -s -- --uninstall
#
# Options (see --help): --version <tag>, --dir <path>, --no-modify-path, --uninstall.
# Each has an equivalent environment variable (the flag wins if both are set):
#   DOZE_KAFKA_VERSION         pin a version, e.g. v1.0.0 (default: latest release)
#   DOZE_KAFKA_DIR             install prefix (default: ~/.doze-kafka; binary in bin/)
#   DOZE_KAFKA_NO_MODIFY_PATH  set to 1 to leave your shell profile untouched
#
# You can also pin a version by putting its tag in the URL:
#   .../doze-dev/doze-kafka/v1.0.0/install.sh

set -eu

REPO="doze-dev/doze-kafka"

die() {
	echo "install: $*" >&2
	exit 1
}

have() { command -v "$1" >/dev/null 2>&1; }
need() { have "$1" || die "required command not found: $1"; }

usage() {
	cat <<'EOF'
doze-kafka installer — download the prebuilt broker binary for your OS/arch.

Usage:
  install.sh [options]
  curl -fsSL .../install.sh | sh -s -- [options]

Options:
  -v, --version <tag>   install a specific release, e.g. v1.0.0 (default: latest)
  -d, --dir <path>      install prefix (default: ~/.doze-kafka; binary in bin/)
      --no-modify-path  leave your shell profile untouched
      --uninstall       remove the binary and PATH entry (your data is left alone)
  -h, --help            show this help

Each option also has an environment variable (the flag wins if both are set):
  DOZE_KAFKA_VERSION, DOZE_KAFKA_DIR, DOZE_KAFKA_NO_MODIFY_PATH
EOF
}

# uninstall removes the installed binary and the PATH entry the installer added. It
# deliberately does NOT touch broker data (data-dir/meta-db live in your working
# directories, not here).
uninstall() {
	removed=0
	if [ -d "$INSTALL_DIR" ]; then
		rm -rf "$INSTALL_DIR"
		echo "Removed $INSTALL_DIR"
		removed=1
	fi
	for p in "$HOME/.zshrc" "$HOME/.bashrc" "$HOME/.bash_profile"; do
		[ -f "$p" ] || continue
		grep -qs -F "$BIN_DIR" "$p" 2>/dev/null || continue
		tmp="$(mktemp)"
		awk -v bindir="$BIN_DIR" '
			index($0, bindir) { next }   # the "export PATH=..." line we added
			$0 == "# doze-kafka" { next } # our marker comment
			{ print }
		' "$p" >"$tmp" && cat "$tmp" >"$p"
		rm -f "$tmp"
		echo "Removed the PATH entry from $p"
		removed=1
	done
	if [ "$removed" = 0 ]; then
		echo "Nothing to uninstall (looked in $INSTALL_DIR and your shell profiles)."
	else
		echo "Uninstalled. Open a new shell to refresh your PATH."
		echo "Your data (data-dir/meta-db, by default ./data and ./meta.db) was left untouched."
	fi
}

# --- arguments ------------------------------------------------------------------

version="${DOZE_KAFKA_VERSION:-}"
install_dir="${DOZE_KAFKA_DIR:-}"
no_modify_path="${DOZE_KAFKA_NO_MODIFY_PATH:-}"
action=install

while [ $# -gt 0 ]; do
	case "$1" in
	-v | --version)
		[ $# -ge 2 ] || die "--version needs a value, e.g. --version v1.0.0"
		version="$2"
		shift 2
		;;
	--version=*)
		version="${1#*=}"
		shift
		;;
	-d | --dir)
		[ $# -ge 2 ] || die "--dir needs a value"
		install_dir="$2"
		shift 2
		;;
	--dir=*)
		install_dir="${1#*=}"
		shift
		;;
	--no-modify-path)
		no_modify_path=1
		shift
		;;
	--uninstall)
		action=uninstall
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		die "unknown option: $1 (try --help)"
		;;
	esac
done

INSTALL_DIR="${install_dir:-$HOME/.doze-kafka}"
BIN_DIR="$INSTALL_DIR/bin"

if [ "$action" = uninstall ]; then
	uninstall
	exit 0
fi

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
if [ "$no_modify_path" != "1" ]; then
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
