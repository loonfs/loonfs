#!/bin/sh

set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
version=$(cargo pkgid -p loonfs-cli | sed 's/.*#//')
target="${LOONFS_TEST_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}"
expected_version="$version ($(git -C "$repo_root" rev-parse --short=12 HEAD) $(git -C "$repo_root" show -s --format=%cs HEAD))"

tmpdir=$(mktemp -d)
cleanup() {
    rm -rf "$tmpdir"
}
trap cleanup EXIT INT TERM

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

artifact_dir="$tmpdir/artifacts"
latest_dir="$tmpdir/releases/latest/download"
pinned_dir="$tmpdir/releases/download/v$version"

cargo build --release -p loonfs-cli --target "$target"
"$repo_root/scripts/package-loonfs-release.sh" --target "$target" --version "$version" --artifact-dir "$artifact_dir"

tar -tzf "$artifact_dir/loonfs-$target.tar.gz" | grep -Fx "./LICENSE" >/dev/null

mkdir -p "$latest_dir" "$pinned_dir"
cp "$artifact_dir/loonfs-$target.tar.gz" "$latest_dir/"
cp "$artifact_dir/loonfs-$target.tar.gz" "$pinned_dir/"

(
    cd "$artifact_dir"
    sha256sum "loonfs-$target.tar.gz" > SHA256SUMS
)

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
