#!/bin/sh
# Generates the three SDKs locally from docs/specs/openapi.json using the
# pinned Fern CLI and generator versions in fern/generators.yml.
# Requires docker. Output lands under fern/generated/ (not tracked).
#
# Usage: scripts/generate-sdks.sh [go|python|typescript]
# With no argument, validates the spec and generates all three.

set -eu

FERN_CLI_VERSION="5.98.3"
cd "$(dirname "$0")/.."

npx --yes "fern-api@${FERN_CLI_VERSION}" check

if [ "$#" -ge 1 ]; then
    npx --yes "fern-api@${FERN_CLI_VERSION}" generate --local --group "$1"
    exit 0
fi

for group in go python typescript; do
    npx --yes "fern-api@${FERN_CLI_VERSION}" generate --local --group "$group"
done
