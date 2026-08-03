#!/bin/sh

set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
version=$(cargo pkgid --manifest-path "$repo_root/Cargo.toml" -p loonfs-cli | sed 's/.*#//')
target="${LOONFS_TEST_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}"
expected_version="$version"

tmpdir=$(mktemp -d)
cleanup() {
    rm -rf "$tmpdir"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

expect_failure() {
    description="$1"
    shift

    set +e
    "$@"
    status=$?
    set -e

    if [ "$status" -eq 0 ]; then
        echo "expected $description" >&2
        exit 1
    fi
}

write_checksum() {
    archive_path="$1"
    sums_path="$2"
    archive_dir=$(dirname "$archive_path")
    archive_name=$(basename "$archive_path")

    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$archive_dir" && sha256sum "$archive_name") > "$sums_path"
    elif command -v shasum >/dev/null 2>&1; then
        (cd "$archive_dir" && shasum -a 256 "$archive_name") > "$sums_path"
    else
        echo "test requires sha256sum or shasum" >&2
        exit 1
    fi
}

artifact_dir="$tmpdir/artifacts"
latest_dir="$tmpdir/releases/latest/download"
pinned_dir="$tmpdir/releases/download/v$version"

cargo build --manifest-path "$repo_root/Cargo.toml" --release -p loonfs-cli --target "$target"

expect_failure "an unsupported release target" \
    "$repo_root/scripts/package-loonfs-release.sh" \
    --target "../escape" --version "$version" --artifact-dir "$artifact_dir"
expect_failure "a mismatched package version" \
    "$repo_root/scripts/package-loonfs-release.sh" \
    --target "$target" --version "0.0.0-mismatch" --artifact-dir "$artifact_dir"

"$repo_root/scripts/package-loonfs-release.sh" --target "$target" --version "$version" --artifact-dir "$artifact_dir"

archive_contents=$(tar -tzf "$artifact_dir/loonfs-$target.tar.gz" | sort)
expected_contents=$(printf '%s\n' LICENSE README.md VERSION loonfs | sort)
if [ "$archive_contents" != "$expected_contents" ]; then
    echo "release archive contained unexpected files:" >&2
    printf '%s\n' "$archive_contents" >&2
    exit 1
fi
archive_version=$(tar -xOf "$artifact_dir/loonfs-$target.tar.gz" VERSION)
if [ "$archive_version" != "$version" ]; then
    echo "release archive reported version $archive_version, expected $version" >&2
    exit 1
fi

mkdir -p "$latest_dir" "$pinned_dir"
cp "$artifact_dir/loonfs-$target.tar.gz" "$latest_dir/"
cp "$artifact_dir/loonfs-$target.tar.gz" "$pinned_dir/"

write_checksum "$artifact_dir/loonfs-$target.tar.gz" "$artifact_dir/SHA256SUMS"

cp "$artifact_dir/SHA256SUMS" "$latest_dir/"
cp "$artifact_dir/SHA256SUMS" "$pinned_dir/"

latest_install_dir="$tmpdir/install-latest"
pinned_install_dir="$tmpdir/install-pinned"
LOONFS_RELEASE_URL_ROOT="file://$tmpdir/releases" "$repo_root/scripts/install-loonfs.sh" --install-dir "$latest_install_dir"
LOONFS_RELEASE_URL_ROOT="file://$tmpdir/releases" "$repo_root/scripts/install-loonfs.sh" --version "v$version" --install-dir "$pinned_install_dir"

"$latest_install_dir/loonfs" version | grep -Fx "$expected_version" >/dev/null
"$pinned_install_dir/loonfs" version | grep -Fx "$expected_version" >/dev/null

printf '0000000000000000000000000000000000000000000000000000000000000000  loonfs-%s.tar.gz\n' "$target" > "$pinned_dir/SHA256SUMS"
expect_failure "checksum failure" \
    env LOONFS_RELEASE_URL_ROOT="file://$tmpdir/releases" \
    "$repo_root/scripts/install-loonfs.sh" --version "v$version" --install-dir "$tmpdir/install-bad"

expect_failure "unsupported platform failure" \
    env LOONFS_INSTALL_OS="Linux" LOONFS_INSTALL_ARCH="riscv64" \
    "$repo_root/scripts/install-loonfs.sh" --install-dir "$tmpdir/install-unsupported"
