#!/bin/sh

set -eu

usage() {
    cat <<'EOF'
Usage: bump-homebrew-tap.sh --version <version> [--tap-dir <path>]

Updates the LoonFS formula in the Homebrew tap for an existing GitHub
release. The script downloads SHA256SUMS and updates the two macOS URLs,
their checksums. It does not commit or
push the changes.

The tap checkout defaults to ../homebrew-tap next to this repository.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

version=""
tap_dir=""
sums_tmp=""

cleanup() {
    if [ -n "$sums_tmp" ] && [ -f "$sums_tmp" ]; then
        rm -f "$sums_tmp"
    fi
}

trap cleanup EXIT
trap 'exit 1' HUP INT TERM

while [ "$#" -gt 0 ]; do
    case "$1" in
        --version)
            [ "$#" -ge 2 ] || die "--version requires a value"
            version="$2"
            shift 2
            ;;
        --tap-dir)
            [ "$#" -ge 2 ] || die "--tap-dir requires a value"
            tap_dir="$2"
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

if [ -z "$version" ]; then
    usage >&2
    exit 1
fi

version="${version#v}"
case "$version" in
    [0-9]*.[0-9]*.[0-9]*) ;;
    *) die "version must look like X.Y.Z, got: $version" ;;
esac

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
if [ -z "$tap_dir" ]; then
    tap_dir="$repo_root/../homebrew-tap"
fi
[ -d "$tap_dir" ] || die "no tap checkout at $tap_dir"
tap_dir=$(CDPATH= cd -- "$tap_dir" && pwd)
formula="$tap_dir/Formula/loonfs.rb"
[ -f "$formula" ] || die "no formula at $formula"

command -v gh >/dev/null || die "gh is required to read the release checksums"

if [ -n "$(git -C "$tap_dir" status --porcelain)" ]; then
    die "the tap checkout at $tap_dir has local changes"
fi

sums_tmp=$(mktemp "${TMPDIR:-/tmp}/loonfs-sha256sums.XXXXXX")
gh release download "v$version" --repo loonfs/loonfs \
    --pattern SHA256SUMS --output "$sums_tmp" --clobber

arm_sha=$(awk '$2 == "loonfs-aarch64-apple-darwin.tar.gz" { print $1 }' "$sums_tmp")
intel_sha=$(awk '$2 == "loonfs-x86_64-apple-darwin.tar.gz" { print $1 }' "$sums_tmp")
echo "$arm_sha" | grep -Eq '^[0-9a-f]{64}$' \
    || die "release v$version lists no checksum for the aarch64 macOS archive"
echo "$intel_sha" | grep -Eq '^[0-9a-f]{64}$' \
    || die "release v$version lists no checksum for the x86_64 macOS archive"

# Each archive URL is followed by its sha256 value. Update only these values.
awk -v version="$version" -v arm_sha="$arm_sha" -v intel_sha="$intel_sha" '
    function indent_of(line) {
        match(line, /^ */)
        return substr(line, 1, RLENGTH)
    }
    /url ".*loonfs-aarch64-apple-darwin\.tar\.gz"/ {
        print indent_of($0) "url \"https://github.com/loonfs/loonfs/releases/download/v" version "/loonfs-aarch64-apple-darwin.tar.gz\""
        pending = arm_sha
        next
    }
    /url ".*loonfs-x86_64-apple-darwin\.tar\.gz"/ {
        print indent_of($0) "url \"https://github.com/loonfs/loonfs/releases/download/v" version "/loonfs-x86_64-apple-darwin.tar.gz\""
        pending = intel_sha
        next
    }
    /^ *sha256 "/ && pending != "" {
        print indent_of($0) "sha256 \"" pending "\""
        pending = ""
        next
    }
    { print }
' "$formula" > "$formula.tmp"
mv "$formula.tmp" "$formula"

[ "$(grep -c "download/v$version/" "$formula")" -eq 2 ] \
    || die "the rewritten formula does not name two v$version archives"
grep -q "$arm_sha" "$formula" && grep -q "$intel_sha" "$formula" \
    || die "the rewritten formula is missing a release checksum"
if command -v ruby >/dev/null; then
    ruby -c "$formula" >/dev/null || die "the rewritten formula is not valid Ruby"
fi

echo
git -C "$tap_dir" --no-pager diff
echo
echo "next, in $tap_dir:"
echo "  git add Formula/loonfs.rb"
echo "  git commit -m \"chore: update to v$version\""
echo "  git push"
