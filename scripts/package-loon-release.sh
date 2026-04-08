#!/bin/sh

set -eu

usage() {
    cat <<'EOF'
Usage: package-loon-release.sh --target <target> --version <version> --artifact-dir <dir>
EOF
}

target=""
version=""
artifact_dir=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --target)
            target="${2:-}"
            shift 2
            ;;
        --version)
            version="${2:-}"
            shift 2
            ;;
        --artifact-dir)
            artifact_dir="${2:-}"
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

if [ -z "$target" ] || [ -z "$version" ] || [ -z "$artifact_dir" ]; then
    usage >&2
    exit 1
fi

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
binary_path="$repo_root/target/$target/release/loon"
archive_name="loon-$target.tar.gz"
stage_dir="$artifact_dir/package/$target"

if [ ! -x "$binary_path" ]; then
    echo "missing compiled loon binary at $binary_path" >&2
    exit 1
fi

mkdir -p "$stage_dir"
rm -rf "$stage_dir"/*

cp "$binary_path" "$stage_dir/loon"
cp "$repo_root/README.md" "$stage_dir/README.md"
cp "$repo_root/LICENSE" "$stage_dir/LICENSE"
printf '%s\n' "$version" > "$stage_dir/VERSION"

mkdir -p "$artifact_dir"
tar -C "$stage_dir" -czf "$artifact_dir/$archive_name" .

printf '%s\n' "$artifact_dir/$archive_name"
