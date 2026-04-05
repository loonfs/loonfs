#!/bin/sh
set -eu

REPO_SLUG="prequel-co/loonfs"
ASSET_NAME="loon-darwin-arm64.tar.gz"
BIN_DIR="${HOME:-}/.loonfs/bin"
VERSION=""

usage() {
  cat <<'EOF'
usage: install.sh [--version <tag>] [--bin-dir <path>] [--help]

Installs loon and loond for macOS arm64 into ~/.loonfs/bin by default.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      shift
      [ "$#" -gt 0 ] || die "missing value for --version"
      VERSION=$1
      ;;
    --bin-dir)
      shift
      [ "$#" -gt 0 ] || die "missing value for --bin-dir"
      BIN_DIR=$1
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
  shift
done

[ -n "${HOME:-}" ] || die "HOME is not set"

for cmd in curl tar shasum mktemp install awk basename uname mkdir cp; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    die "required command not found: $cmd"
  fi
done

TARGET_OS=$(uname -s)
TARGET_ARCH=$(uname -m)
[ "$TARGET_OS" = "Darwin" ] || die "this installer currently supports macOS arm64 only"
[ "$TARGET_ARCH" = "arm64" ] || die "this installer currently supports macOS arm64 only"

# LOONFS_RELEASE_BASE_URL lets CI exercise the real installer flow against locally staged assets.
if [ -n "${LOONFS_RELEASE_BASE_URL:-}" ]; then
  RELEASE_BASE_URL=${LOONFS_RELEASE_BASE_URL%/}
elif [ -n "$VERSION" ]; then
  RELEASE_BASE_URL="https://github.com/$REPO_SLUG/releases/download/$VERSION"
else
  RELEASE_BASE_URL="https://github.com/$REPO_SLUG/releases/latest/download"
fi

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM

ARCHIVE_PATH="$TMP_DIR/$ASSET_NAME"
CHECKSUMS_PATH="$TMP_DIR/SHA256SUMS"
EXTRACT_DIR="$TMP_DIR/extract"

curl -L --output "$ARCHIVE_PATH" "$RELEASE_BASE_URL/$ASSET_NAME"
curl -L --output "$CHECKSUMS_PATH" "$RELEASE_BASE_URL/SHA256SUMS"

EXPECTED_SHA=$(awk '$2 == "'"$ASSET_NAME"'" { print $1 }' "$CHECKSUMS_PATH")
[ -n "$EXPECTED_SHA" ] || die "missing checksum entry for $ASSET_NAME"
ACTUAL_SHA=$(shasum -a 256 "$ARCHIVE_PATH" | awk '{ print $1 }')
[ "$EXPECTED_SHA" = "$ACTUAL_SHA" ] || die "checksum verification failed for $ASSET_NAME"

mkdir -p "$EXTRACT_DIR"
tar -xzf "$ARCHIVE_PATH" -C "$EXTRACT_DIR"

CONFIG_ROOT="$HOME/.config/loonfs"
LOON_CONFIG_DIR="$CONFIG_ROOT/loon"
LOOND_CONFIG_DIR="$CONFIG_ROOT/loond"
LOOND_EXAMPLES_DIR="$LOOND_CONFIG_DIR/examples"

# Create the managed loon config root and the operator-owned loond config roots separately.
mkdir -p "$BIN_DIR" "$LOON_CONFIG_DIR" "$LOOND_CONFIG_DIR" "$LOOND_EXAMPLES_DIR"

install -m 0755 "$EXTRACT_DIR/bin/loon" "$BIN_DIR/loon"
install -m 0755 "$EXTRACT_DIR/bin/loond" "$BIN_DIR/loond"

# Example loond configs are safe to refresh on reinstall because users copy them before editing.
for example in "$EXTRACT_DIR"/examples/*.toml; do
  [ -e "$example" ] || continue
  cp "$example" "$LOOND_EXAMPLES_DIR/$(basename "$example")"
done

echo "Installed:"
echo "  $BIN_DIR/loon"
echo "  $BIN_DIR/loond"
echo
echo "Prepared:"
echo "  $LOON_CONFIG_DIR/"
echo "  $LOOND_CONFIG_DIR/"
echo "  $LOOND_EXAMPLES_DIR/"
echo

case ":${PATH:-}:" in
  *:"$BIN_DIR":*) ;;
  *)
    # The installer stays non-invasive: it prints the PATH export instead of editing shell rc files.
    echo "Add to PATH:"
    echo "  export PATH=\"$BIN_DIR:\$PATH\""
    echo
    ;;
esac

echo "Next (local mode):"
echo "  cp ~/.config/loonfs/loond/examples/loond.cloudflare-r2.example.toml \\"
echo "     ~/.config/loonfs/loond/home.toml"
echo "  \$EDITOR ~/.config/loonfs/loond/home.toml"
echo "  loon profile add local home --server-config ~/.config/loonfs/loond/home.toml"
echo "  loon --profile home local up"
echo
echo "Remote mode:"
echo "  coming soon"
echo "  the first public install path is local mode against a user-managed loond config"
