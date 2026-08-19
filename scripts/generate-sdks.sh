#!/bin/sh
# Generates SDKs from docs/specs/openapi.json with the pinned Fern versions.
# Requires Docker. Output is written to the untracked sdk/generated directory.
#
# Usage: scripts/generate-sdks.sh [go|python|typescript]
# With no argument, validates the document and generates all three SDKs.

set -eu

FERN_CLI_VERSION="5.98.3"
cd "$(dirname "$0")/../sdk"

npx --yes "fern-api@${FERN_CLI_VERSION}" check

if [ "$#" -ge 1 ]; then
    rm -rf "generated/$1"
    npx --yes "fern-api@${FERN_CLI_VERSION}" generate --force --local --group "$1"
    exit 0
fi

for group in go python typescript; do
    rm -rf "generated/${group}"
    npx --yes "fern-api@${FERN_CLI_VERSION}" generate --force --local --group "$group"
done
