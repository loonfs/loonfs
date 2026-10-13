#!/bin/sh
# Generates SDKs from the checked-in OpenAPI documents using pinned Fern versions.
# Requires Docker. Output is written to the untracked sdk/generated directory.
#
# Usage: scripts/generate-sdks.sh [go|python|typescript|typescript-client]
# With no argument, validates both Fern APIs and generates every SDK.
# The browser client uses the proxy document and the browser Fern API.
# Handwritten transfer helpers and release overlays are copied into each SDK
# after generation.

set -eu

cd "$(dirname "$0")/../sdk"
FERN_CLI_VERSION=$(python3 ../scripts/read-json-field.py version < fern/fern.config.json)

npx --yes "fern-api@${FERN_CLI_VERSION}" check

overlay_handwritten() {
    case "$1" in
        typescript|typescript-client) cp transfers/typescript-shared/*.ts "generated/$1/" ;;
    esac
    if [ -d "transfers/$1" ]; then
        cp -R "transfers/$1/." "generated/$1/"
    fi
    if [ -d "overlays/$1" ]; then
        cp -R "overlays/$1/." "generated/$1/"
    fi
    if [ "$1" = "typescript" ] && [ -f "proxy/typescript/proxy.ts" ]; then
        mkdir -p generated/typescript/proxy/src
        cp proxy/typescript/proxy.ts generated/typescript/proxy/src/proxy.ts
    elif [ -d "proxy/$1" ]; then
        cp -R "proxy/$1/." "generated/$1/"
    fi
}

prune_generated() {
    case "$1" in
    go)
        # Fern cannot omit these generator-level model tests.
        for name in capabilities changes commits files inodes namespaces snapshots trash types uploads \
            maintenance/checkpoints maintenance/diagnostics maintenance/grep_index maintenance/runs; do
            rm "generated/go/${name}_test.go"
        done
        rm -r generated/go/*/*_test generated/go/*/*/*_test
        # The generator's retrier test pins the generator's retry policy; the
        # overlay replaces it with one that pins the Retry-After contract.
        rm generated/go/internal/retrier_test.go
        python3 ../scripts/patch-go-sdk.py
        ;;
    python)
        # LoonFS does not use server-sent events.
        rm -r generated/python/core/http_sse
        python3 ../scripts/patch-python-sdk.py
        ;;
    typescript|typescript-client)
        python3 ../scripts/patch-typescript-sdk.py "$1"
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
    python3 ../scripts/verify-sdk-retry-safety.py "$1"
}

if [ "$#" -ge 1 ]; then
    generate_group "$1"
    exit 0
fi

for group in go python typescript typescript-client; do
    generate_group "$group"
done
