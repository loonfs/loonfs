#!/bin/sh

set -eu

usage() {
    cat <<'EOF'
Usage: package-loonfs-release.sh --target <target> --version <version> --artifact-dir <dir>
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

target=""
version=""
artifact_dir=""
stage_dir=""
archive_tmp=""

cleanup() {
    if [ -n "$stage_dir" ] && [ -d "$stage_dir" ]; then
        rm -rf "$stage_dir"
    fi
    if [ -n "$archive_tmp" ] && [ -f "$archive_tmp" ]; then
        rm -f "$archive_tmp"
    fi
}

trap cleanup EXIT
trap 'exit 1' HUP INT TERM

while [ "$#" -gt 0 ]; do
    case "$1" in
        --target)
            [ "$#" -ge 2 ] || die "--target requires a value"
            target="$2"
            shift 2
            ;;
        --version)
            [ "$#" -ge 2 ] || die "--version requires a value"
            version="$2"
            shift 2
            ;;
        --artifact-dir)
            [ "$#" -ge 2 ] || die "--artifact-dir requires a value"
            artifact_dir="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            die "unknown argument: $1"
            ;;
    esac
done

if [ -z "$target" ] || [ -z "$version" ] || [ -z "$artifact_dir" ]; then
    usage >&2
    exit 1
fi

case "$target" in
    aarch64-apple-darwin | x86_64-apple-darwin | aarch64-unknown-linux-gnu | x86_64-unknown-linux-gnu)
        ;;
    *)
        die "unsupported release target: $target"
        ;;
esac

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
binary_path="$repo_root/target/$target/release/loonfs"
archive_name="loonfs-$target.tar.gz"

if [ ! -x "$binary_path" ]; then
    die "missing compiled loonfs binary at $binary_path"
fi

binary_output=$("$binary_path" version)
binary_version=${binary_output%% *}
if [ "$binary_version" != "$version" ]; then
    die "compiled loonfs reports version $binary_version, expected $version"
fi

mkdir -p "$artifact_dir"
artifact_dir=$(CDPATH= cd -- "$artifact_dir" && pwd)
stage_dir=$(mktemp -d "${TMPDIR:-/tmp}/loonfs-release.XXXXXX")
archive_tmp=$(mktemp "$artifact_dir/.${archive_name}.XXXXXX")
archive_path="$artifact_dir/$archive_name"

cp "$binary_path" "$stage_dir/loonfs"
cp "$repo_root/README.md" "$stage_dir/README.md"
cp "$repo_root/LICENSE" "$stage_dir/LICENSE"
printf '%s\n' "$version" > "$stage_dir/VERSION"

COPYFILE_DISABLE=1 tar -C "$stage_dir" -czf "$archive_tmp" loonfs README.md LICENSE VERSION
mv "$archive_tmp" "$archive_path"
archive_tmp=""

printf '%s\n' "$archive_path"
