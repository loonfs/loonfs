#!/bin/sh

set -eu

REPO_SLUG="${LOON_REPO_SLUG:-loonfs/loonfs}"
INSTALL_DIR="${LOON_INSTALL_DIR:-$HOME/.local/bin}"
VERSION=""

usage() {
    cat <<'EOF'
Usage: install-loon.sh [--version <tag>] [--install-dir <path>]
EOF
}

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "required command not found: $1" >&2
        exit 1
    fi
}

download() {
    url="$1"
    destination="$2"

    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "$destination"
        return
    fi

    if command -v wget >/dev/null 2>&1; then
        wget -qO "$destination" "$url"
        return
    fi

    echo "install requires curl or wget" >&2
    exit 1
}

detect_target() {
    os="${LOON_INSTALL_OS:-$(uname -s)}"
    arch="${LOON_INSTALL_ARCH:-$(uname -m)}"

    case "$os/$arch" in
        Darwin/arm64)
            printf '%s\n' "aarch64-apple-darwin"
            ;;
        Darwin/x86_64)
            printf '%s\n' "x86_64-apple-darwin"
            ;;
        Linux/x86_64)
            printf '%s\n' "x86_64-unknown-linux-gnu"
            ;;
        Linux/aarch64)
            printf '%s\n' "aarch64-unknown-linux-gnu"
            ;;
        *)
            echo "unsupported platform: $os $arch" >&2
            echo "supported targets: macOS arm64, macOS x86_64, Linux arm64 GNU, Linux x86_64 GNU" >&2
            exit 1
            ;;
    esac
}

checksum_file() {
    archive_path="$1"
    sums_path="$2"

    if command -v shasum >/dev/null 2>&1; then
        expected=$(awk -v name="$(basename "$archive_path")" '$2 == name { print $1 }' "$sums_path")
        actual=$(shasum -a 256 "$archive_path" | awk '{ print $1 }')
    elif command -v sha256sum >/dev/null 2>&1; then
        expected=$(awk -v name="$(basename "$archive_path")" '$2 == name { print $1 }' "$sums_path")
        actual=$(sha256sum "$archive_path" | awk '{ print $1 }')
    else
        echo "install requires shasum or sha256sum" >&2
        exit 1
    fi

    if [ -z "$expected" ]; then
        echo "checksum entry missing for $(basename "$archive_path")" >&2
        exit 1
    fi

    if [ "$expected" != "$actual" ]; then
        echo "checksum verification failed for $(basename "$archive_path")" >&2
        exit 1
    fi
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --version)
            VERSION="${2:-}"
            shift 2
            ;;
        --install-dir)
            INSTALL_DIR="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

need_cmd uname
need_cmd tar
need_cmd mktemp

target=$(detect_target)
archive_name="loon-$target.tar.gz"

if [ -n "$VERSION" ]; then
    base_url="${LOON_RELEASE_URL_ROOT:-https://github.com/$REPO_SLUG/releases}/download/$VERSION"
else
    base_url="${LOON_RELEASE_URL_ROOT:-https://github.com/$REPO_SLUG/releases}/latest/download"
fi

tmpdir=$(mktemp -d)
cleanup() {
    rm -rf "$tmpdir"
}
trap cleanup EXIT INT TERM

archive_path="$tmpdir/$archive_name"
sums_path="$tmpdir/SHA256SUMS"

download "$base_url/$archive_name" "$archive_path"
download "$base_url/SHA256SUMS" "$sums_path"
checksum_file "$archive_path" "$sums_path"

mkdir -p "$INSTALL_DIR"
tar -xzf "$archive_path" -C "$tmpdir"
install_path="$INSTALL_DIR/loon"
cp "$tmpdir/loon" "$install_path"
chmod 755 "$install_path"

printf 'installed loon to %s\n' "$install_path"
"$install_path" version

case ":$PATH:" in
    *:"$INSTALL_DIR":*)
        ;;
    *)
        printf 'add %s to PATH to run `loon` directly\n' "$INSTALL_DIR"
        ;;
esac
