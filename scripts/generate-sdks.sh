#!/bin/sh
# Generates SDKs from the checked-in OpenAPI document using pinned Fern versions.
# Requires Docker. Output is written to the untracked sdk/generated directory.
#
# Usage: scripts/generate-sdks.sh [go|python|typescript]
# With no argument, validates the Fern API and generates every SDK.
# Handwritten transfer helpers and release overlays are copied into each SDK
# after generation.

set -eu

cd "$(dirname "$0")/../sdk"
FERN_CLI_VERSION=$(python3 ../scripts/read-json-field.py version < fern/fern.config.json)

npx --yes "fern-api@${FERN_CLI_VERSION}" check

overlay_handwritten() {
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
    typescript)
        python3 ../scripts/patch-typescript-sdk.py
        ;;
    esac
}

generate_group() {
    rm -rf "generated/$1"
    npx --yes "fern-api@${FERN_CLI_VERSION}" generate --force --local --api server --group "$1"
    prune_generated "$1"
    overlay_handwritten "$1"
    python3 ../scripts/verify-sdk-retry-safety.py "$1"
}

if [ "$#" -ge 1 ]; then
    generate_group "$1"
    exit 0
fi

for group in go python typescript; do
    generate_group "$group"
done
