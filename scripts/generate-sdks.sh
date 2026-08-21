#!/bin/sh
# Generates SDKs from the checked-in OpenAPI documents using pinned Fern versions.
# Requires Docker. Output is written to the untracked sdk/generated directory.
#
# Usage: scripts/generate-sdks.sh [go|python|typescript|typescript-client]
# With no argument, validates both Fern APIs and generates every SDK.
# The browser client uses the proxy document and the browser Fern API.
# Handwritten files under sdk/transfers/<language> are copied into each SDK
# after generation.

set -eu

FERN_CLI_VERSION="5.98.3"
cd "$(dirname "$0")/../sdk"

npx --yes "fern-api@${FERN_CLI_VERSION}" check

overlay_handwritten() {
    if [ -d "transfers/$1" ]; then
        cp -R "transfers/$1/." "generated/$1/"
    fi
    # TypeScript ships the proxy as a separate package.
    if [ "$1" != "typescript" ] && [ -d "proxy/$1" ]; then
        cp -R "proxy/$1/." "generated/$1/"
    fi
}

prune_generated() {
    case "$1" in
    go)
        # Fern cannot omit these generator-level model tests.
        for name in admin filesystem inodes namespaces query types uploads; do
            rm "generated/go/${name}_test.go"
        done
        ;;
    python)
        # LoonFS does not use server-sent events.
        rm -r generated/python/core/http_sse
        python3 - <<'PY'
import pathlib
path = pathlib.Path("generated/python/core/pydantic_utilities.py")
source = path.read_text()
import_block = 'if TYPE_CHECKING:\n    from .http_sse._models import ServerSentEvent\n\n'
assert source.count(import_block) == 1, "SSE type-checking import not found"
source = source.replace(import_block, "")
start = source.index("def parse_sse_obj(")
end = source.index("_type_adapter_cache")
path.write_text(source[:start] + source[end:])
PY
        ;;
    esac
}

generate_group() {
    api=server
    [ "$1" = "typescript-client" ] && api=browser
    rm -rf "generated/$1"
    npx --yes "fern-api@${FERN_CLI_VERSION}" generate --force --local --api "$api" --group "$1"
    prune_generated "$1"
    overlay_handwritten "$1"
}

if [ "$#" -ge 1 ]; then
    generate_group "$1"
    exit 0
fi

for group in go python typescript typescript-client; do
    generate_group "$group"
done
