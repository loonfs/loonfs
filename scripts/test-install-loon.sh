#!/bin/sh

set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
version=$(cargo pkgid -p loonfs-cli | sed 's/.*#//')
target="${LOONFS_TEST_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}"

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
"$repo_root/scripts/package-loon-release.sh" --target "$target" --version "$version" --artifact-dir "$artifact_dir"

tar -tzf "$artifact_dir/loon-$target.tar.gz" | grep -Fx "./LICENSE" >/dev/null

mkdir -p "$latest_dir" "$pinned_dir"
cp "$artifact_dir/loon-$target.tar.gz" "$latest_dir/"
cp "$artifact_dir/loon-$target.tar.gz" "$pinned_dir/"

(
    cd "$artifact_dir"
    sha256sum "loon-$target.tar.gz" > SHA256SUMS
)

cp "$artifact_dir/SHA256SUMS" "$latest_dir/"
cp "$artifact_dir/SHA256SUMS" "$pinned_dir/"

latest_install_dir="$tmpdir/install-latest"
pinned_install_dir="$tmpdir/install-pinned"
LOONFS_RELEASE_URL_ROOT="file://$tmpdir/releases" "$repo_root/scripts/install-loon.sh" --install-dir "$latest_install_dir"
LOONFS_RELEASE_URL_ROOT="file://$tmpdir/releases" "$repo_root/scripts/install-loon.sh" --version "v$version" --install-dir "$pinned_install_dir"

"$latest_install_dir/loon" version | grep -Fx "$version" >/dev/null
"$pinned_install_dir/loon" version | grep -Fx "$version" >/dev/null

printf '0000000000000000000000000000000000000000000000000000000000000000  loon-%s.tar.gz\n' "$target" > "$pinned_dir/SHA256SUMS"
expect_failure "checksum failure" \
    env LOONFS_RELEASE_URL_ROOT="file://$tmpdir/releases" \
    "$repo_root/scripts/install-loon.sh" --version "v$version" --install-dir "$tmpdir/install-bad"

expect_failure "unsupported platform failure" \
    env LOONFS_INSTALL_OS="Linux" LOONFS_INSTALL_ARCH="riscv64" \
    "$repo_root/scripts/install-loon.sh" --install-dir "$tmpdir/install-unsupported"
