#!/bin/sh

set -eu

usage() {
    cat <<'EOF'
Usage: prepare-release.sh --version <version>

Updates the workspace version references, regenerates the OpenAPI
specification, and refreshes Cargo.lock. It then runs the version and
OpenAPI checks used by the release workflow:

  Cargo.toml               workspace.package.version and the pinned
                           versions of the published workspace crates
  Chart.yaml               version and appVersion of the server chart
  docs/specs/openapi.json  regenerated from the server
  Cargo.lock               refreshed by the regeneration build

Run it on a clean branch based on main after CI passes. Commit the result
as "chore(release): prepare v<version>", then follow RELEASING.md.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

version=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --version)
            [ "$#" -ge 2 ] || die "--version requires a value"
            version="$2"
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

case "$version" in
    [0-9]*.[0-9]*.[0-9]*) ;;
    *) die "version must look like X.Y.Z, got: $version" ;;
esac

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"

chart="crates/loonfs-server/deploy/helm/loonfs-server/Chart.yaml"
spec="docs/specs/openapi.json"

git diff --quiet && git diff --cached --quiet \
    || die "the working tree has changes; prepare a release from a clean checkout"

if git rev-parse -q --verify "refs/tags/v$version" >/dev/null; then
    die "tag v$version already exists"
fi

current=$(awk '
    /^\[workspace\.package\]$/ { in_section = 1; next }
    /^\[/ { in_section = 0 }
    in_section && /^version = / { gsub(/"/, ""); print $3; exit }
' Cargo.toml)
[ -n "$current" ] || die "could not read workspace.package.version from Cargo.toml"
[ "$current" != "$version" ] || die "the workspace is already at $version"

echo "bumping $current -> $version"

# Update the workspace version and the pinned registry versions of the
# published workspace crates. RELEASING.md explains why both are required.
sed "/^\[workspace\.package\]$/,/^\[/ s/^version = \"$current\"\$/version = \"$version\"/
     s/\(^loonfs[a-z-]* = { path = \"crates\/[a-z-]*\", version = \"\)$current\(\" }\)\$/\1$version\2/" \
    Cargo.toml > Cargo.toml.tmp
mv Cargo.toml.tmp Cargo.toml

bumped=$(grep -c "version = \"$version\"" Cargo.toml) || true
if [ "$bumped" -ne 7 ]; then
    die "expected 7 versions in Cargo.toml to read $version (workspace.package plus 6 pinned crates), found $bumped"
fi

sed "s/^version: .*\$/version: $version/
     s/^appVersion: .*\$/appVersion: \"$version\"/" \
    "$chart" > "$chart.tmp"
mv "$chart.tmp" "$chart"

# Regenerate the specification because it includes the release version.
# Cargo also refreshes Cargo.lock while building the OpenAPI generator.
cargo run -p loonfs-server --features openapi --bin loonfs-openapi -- "$spec"

# Run the version checks used by the release workflow.
resolved=$(cargo pkgid -p loonfs-cli | sed 's/.*#//')
[ "$resolved" = "$version" ] \
    || die "cargo resolves loonfs-cli to $resolved, expected $version"

chart_version=$(sed -n 's/^version: *//p' "$chart" | tr -d '"')
app_version=$(sed -n 's/^appVersion: *//p' "$chart" | tr -d '"')
[ "$chart_version" = "$version" ] \
    || die "chart version is $chart_version, expected $version"
[ "$app_version" = "$version" ] \
    || die "chart appVersion is $app_version, expected $version"

grep -q "\"version\": \"$version\"" "$spec" \
    || die "regenerated spec does not carry version $version"

# Run the OpenAPI specification test with the same options used in CI.
cargo test -p loonfs-server --features openapi --locked openapi

echo
echo "prepared v$version:"
git status --short
echo
echo "next: commit as \"chore(release): prepare v$version\" and follow RELEASING.md"
