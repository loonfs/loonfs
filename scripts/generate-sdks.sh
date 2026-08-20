#!/bin/sh
# Generates SDKs from the checked-in OpenAPI documents using pinned Fern versions.
# Requires Docker. Output is written to the untracked sdk/generated directory.
#
# Usage: scripts/generate-sdks.sh [go|python|typescript|typescript-client]
# With no argument, validates both Fern workspaces and generates every SDK.
# The browser client uses the proxy document and the sdk/fern-client workspace.
# Handwritten files under sdk/transfers/<language> are copied into each SDK
# after generation.

set -eu

FERN_CLI_VERSION="5.98.3"
cd "$(dirname "$0")/../sdk"

npx --yes "fern-api@${FERN_CLI_VERSION}" check
(cd fern-client && npx --yes "fern-api@${FERN_CLI_VERSION}" check)

overlay_handwritten() {
    if [ -d "transfers/$1" ]; then
        cp -R "transfers/$1/." "generated/$1/"
    fi
    # The TypeScript proxy is a standalone package beside the generated SDK.
    if [ "$1" != "typescript" ] && [ -d "proxy/$1" ]; then
        cp -R "proxy/$1/." "generated/$1/"
    fi
}

generate_group() {
    rm -rf "generated/$1"
    if [ "$1" = "typescript-client" ]; then
        (cd fern-client && npx --yes "fern-api@${FERN_CLI_VERSION}" generate --force --local --group "$1")
    else
        npx --yes "fern-api@${FERN_CLI_VERSION}" generate --force --local --group "$1"
    fi
    overlay_handwritten "$1"
}

if [ "$#" -ge 1 ]; then
    generate_group "$1"
    exit 0
fi

for group in go python typescript typescript-client; do
    generate_group "$group"
done
