#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
SAMPLE_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(CDPATH= cd -- "$SAMPLE_DIR/../../.." && pwd)"

CARGO_BIN="${CARGO:-}"
if [ -z "$CARGO_BIN" ] && command -v cargo >/dev/null 2>&1; then
    CARGO_BIN="$(command -v cargo)"
fi
if [ -z "$CARGO_BIN" ] && [ -x "${HOME:-}/.cargo/bin/cargo" ]; then
    CARGO_BIN="${HOME}/.cargo/bin/cargo"
fi
if [ -z "$CARGO_BIN" ] && [ -x "/opt/homebrew/bin/cargo" ]; then
    CARGO_BIN="/opt/homebrew/bin/cargo"
fi
if [ -z "$CARGO_BIN" ] && [ -x "/usr/local/bin/cargo" ]; then
    CARGO_BIN="/usr/local/bin/cargo"
fi
if [ -z "$CARGO_BIN" ]; then
    echo "Could not find cargo. Launch Xcode from a shell with Rust on PATH, or set CARGO to your cargo binary." >&2
    exit 1
fi
export PATH="$(dirname "$CARGO_BIN"):$PATH"

ARCH="${CURRENT_ARCH:-}"
if [ -z "$ARCH" ] || [ "$ARCH" = "undefined_arch" ]; then
    ARCH="${NATIVE_ARCH_ACTUAL:-}"
fi
if [ -z "$ARCH" ] || [ "$ARCH" = "undefined_arch" ]; then
    ARCH="${ARCHS:-}"
fi
if [ -z "$ARCH" ] || [ "$ARCH" = "undefined_arch" ]; then
    ARCH="$(uname -m)"
fi
case "$ARCH" in
    *" "*)
        set -- $ARCH
        ARCH="$1"
        ;;
esac
case "$ARCH" in
    arm64)
        RUST_TARGET="aarch64-apple-darwin"
        ;;
    x86_64)
        RUST_TARGET="x86_64-apple-darwin"
        ;;
    *)
        echo "Unsupported Xcode architecture: $ARCH" >&2
        exit 1
        ;;
esac

PROFILE="debug"
RELEASE_FLAG=""
if [ "${CONFIGURATION:-Debug}" = "Release" ]; then
    PROFILE="release"
    RELEASE_FLAG="--release"
fi

echo "Building loon-macos for $RUST_TARGET ($PROFILE)"
cd "$REPO_ROOT"
"$CARGO_BIN" build -p loon-macos --target "$RUST_TARGET" $RELEASE_FLAG

LIB_SOURCE="$REPO_ROOT/target/$RUST_TARGET/$PROFILE/libloon_macos.a"
LIB_DEST="$SAMPLE_DIR/Vendor/libloon_macos.a"

if [ ! -f "$LIB_SOURCE" ]; then
    echo "Expected Rust static library at $LIB_SOURCE" >&2
    exit 1
fi

mkdir -p "$SAMPLE_DIR/Vendor"
cp "$LIB_SOURCE" "$LIB_DEST"
echo "Copied $LIB_DEST"
