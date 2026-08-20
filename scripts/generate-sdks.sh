#!/bin/sh
# Generates SDKs from docs/specs/openapi.json with the pinned Fern versions.
# Requires Docker. Output is written to the untracked sdk/generated directory.
#
# Usage: scripts/generate-sdks.sh [go|python|typescript|typescript-client]
# With no argument, validates the documents and generates every SDK.
# The typescript-client group is the browser client, generated from the proxy
# document through its own workspace in sdk/fern-client.
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
