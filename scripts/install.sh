#!/bin/sh
set -eu

REPO_SLUG="prequel-co/loonfs"
BIN_DIR="${HOME:-}/.loonfs/bin"
VERSION=""

usage() {
  cat <<'EOF'
usage: install.sh [--version <tag>] [--bin-dir <path>] [--help]

Installs the loon CLI into ~/.loonfs/bin by default.

  curl -fsSL https://install.loonfs.com | sh
  curl -fsSL https://install.loonfs.com | sh -s -- --version v0.2.0
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

for cmd in curl tar mktemp install awk uname mkdir; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    die "required command not found: $cmd"
  fi
done

# --- platform detection ---

detect_platform() {
  OS=$(uname -s)
  ARCH=$(uname -m)

  case "$OS" in
    Darwin) OS_NAME="darwin" ;;
    Linux)  OS_NAME="linux" ;;
    *)      die "unsupported OS: $OS (macOS and Linux are supported)" ;;
  esac

  case "$ARCH" in
    x86_64|amd64)  ARCH_NAME="x86_64" ;;
    arm64|aarch64)  ARCH_NAME="aarch64" ;;
    *)              die "unsupported architecture: $ARCH (x86_64 and arm64/aarch64 are supported)" ;;
  esac

  ASSET_NAME="loon-${OS_NAME}-${ARCH_NAME}.tar.gz"
}

detect_platform

# --- checksum tool detection ---

if command -v sha256sum >/dev/null 2>&1; then
  SHA256_CMD="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  SHA256_CMD="shasum -a 256"
else
  die "neither sha256sum nor shasum found — cannot verify download"
fi

# --- download ---

# LOONFS_RELEASE_BASE_URL lets CI exercise the installer against locally staged assets.
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

echo "Downloading $ASSET_NAME..."
curl -fSL --output "$ARCHIVE_PATH" "$RELEASE_BASE_URL/$ASSET_NAME"
curl -fSL --output "$CHECKSUMS_PATH" "$RELEASE_BASE_URL/SHA256SUMS"

# --- verify checksum ---

EXPECTED=$(awk -v asset="$ASSET_NAME" '$2 == asset || $2 == "./"asset { print $1 }' "$CHECKSUMS_PATH")
[ -n "$EXPECTED" ] || die "missing checksum entry for $ASSET_NAME"
ACTUAL=$($SHA256_CMD "$ARCHIVE_PATH" | awk '{ print $1 }')
[ "$EXPECTED" = "$ACTUAL" ] || die "checksum verification failed for $ASSET_NAME (expected $EXPECTED, got $ACTUAL)"

# --- install ---

EXTRACT_DIR="$TMP_DIR/extract"
mkdir -p "$EXTRACT_DIR"
tar -xzf "$ARCHIVE_PATH" -C "$EXTRACT_DIR"

mkdir -p "$BIN_DIR"
install -m 0755 "$EXTRACT_DIR/bin/loon" "$BIN_DIR/loon"

echo "Installed loon to $BIN_DIR/loon"

# --- PATH hint ---

case ":${PATH:-}:" in
  *:"$BIN_DIR":*)
    ;;
  *)
    echo
    echo "Add loon to your PATH:"
    echo
    if [ -f "$HOME/.zshrc" ] || [ "$(basename "${SHELL:-}")" = "zsh" ]; then
      echo "  echo 'export PATH=\"$BIN_DIR:\$PATH\"' >> ~/.zshrc && source ~/.zshrc"
    elif [ -f "$HOME/.bashrc" ] || [ "$(basename "${SHELL:-}")" = "bash" ]; then
      echo "  echo 'export PATH=\"$BIN_DIR:\$PATH\"' >> ~/.bashrc && source ~/.bashrc"
    else
      echo "  export PATH=\"$BIN_DIR:\$PATH\""
    fi
    echo
    ;;
esac

# --- next steps ---

echo "Get started:"
echo "  loon --help"
