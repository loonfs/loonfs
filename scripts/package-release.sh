#!/bin/sh
set -eu

TARGET_OS="$(uname -s)"
TARGET_ARCH="$(uname -m)"

if [ "$TARGET_OS" != "Darwin" ] || [ "$TARGET_ARCH" != "arm64" ]; then
  echo "error: package-release.sh currently supports macOS arm64 only" >&2
  exit 1
fi

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <output-dir>" >&2
  exit 1
fi

ROOT_DIR=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
OUTPUT_DIR=$1
ASSET_NAME="loon-darwin-arm64.tar.gz"

for cmd in cargo shasum tar mktemp install; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: required command not found: $cmd" >&2
    exit 1
  fi
done

mkdir -p "$OUTPUT_DIR"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM

cd "$ROOT_DIR"
cargo build --release -p loon-cli -p loon-server

PACKAGE_DIR="$TMP_DIR/package"
# Match the installer's expected archive layout exactly.
mkdir -p "$PACKAGE_DIR/bin" "$PACKAGE_DIR/examples"

install -m 0755 "target/release/loon" "$PACKAGE_DIR/bin/loon"
install -m 0755 "target/release/loond" "$PACKAGE_DIR/bin/loond"
cp "configs/loond.local-fs.example.toml" "$PACKAGE_DIR/examples/loond.local-fs.example.toml"
cp "configs/loond.aws-s3.example.toml" "$PACKAGE_DIR/examples/loond.aws-s3.example.toml"
cp "configs/loond.cloudflare-r2.example.toml" "$PACKAGE_DIR/examples/loond.cloudflare-r2.example.toml"

ARCHIVE_PATH="$OUTPUT_DIR/$ASSET_NAME"
tar -C "$PACKAGE_DIR" -czf "$ARCHIVE_PATH" .

(
  cd "$OUTPUT_DIR"
  # Keep the checksum file format simple so install.sh can validate with awk + shasum only.
  shasum -a 256 "$ASSET_NAME" > SHA256SUMS
)

echo "wrote $ARCHIVE_PATH"
echo "wrote $OUTPUT_DIR/SHA256SUMS"
