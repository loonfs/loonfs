#!/bin/sh
# Runs a generated SDK against a local server using the shared test cases.
#
# Usage: scripts/run-sdk-conformance.sh <go|python|typescript>

set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: scripts/run-sdk-conformance.sh <go|python|typescript>" >&2
    exit 2
fi

LANGUAGE="$1"
case "$LANGUAGE" in
    go|python|typescript) ;;
    *)
        echo "usage: scripts/run-sdk-conformance.sh <go|python|typescript>" >&2
        exit 2
        ;;
esac

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
if [ ! -d "$REPO_ROOT/sdk/generated/$LANGUAGE" ]; then
    "$REPO_ROOT/scripts/generate-sdks.sh" "$LANGUAGE"
fi

cd "$REPO_ROOT"
cargo build -p loonfs-conformance --bin conformance-server

RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/loonfs-conformance.XXXXXX")
SERVER_INPUT="$RUN_DIR/server-input"
SERVER_OUTPUT="$RUN_DIR/server-output"
mkfifo "$SERVER_INPUT" "$SERVER_OUTPUT"

SERVER_PID=
cleanup() {
    status=$?
    trap - 0
    if [ -n "$SERVER_PID" ]; then
        exec 3>&-
        attempts=0
        while kill -0 "$SERVER_PID" 2>/dev/null && [ "$attempts" -lt 50 ]; do
            sleep 0.1
            attempts=$((attempts + 1))
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill "$SERVER_PID" 2>/dev/null || :
        fi
        wait "$SERVER_PID" 2>/dev/null || :
    fi
    rm -r "$RUN_DIR"
    exit "$status"
}
trap cleanup 0

exec 3<>"$SERVER_INPUT"
"$REPO_ROOT/target/debug/conformance-server" \
    <"$SERVER_INPUT" >"$SERVER_OUTPUT" 3>&- &
SERVER_PID=$!
IFS= read -r SERVER_LINE <"$SERVER_OUTPUT"

LOONFS_CONFORMANCE_URL=$(printf '%s\n' "$SERVER_LINE" | \
    python3 -c 'import json, sys; print(json.load(sys.stdin)["base_url"])')
LOONFS_CONFORMANCE_TOKEN=$(printf '%s\n' "$SERVER_LINE" | \
    python3 -c 'import json, sys; print(json.load(sys.stdin)["token"])')
LOONFS_CONFORMANCE_CASES="$REPO_ROOT/crates/loonfs-conformance/cases"
LOONFS_PROXY_DOCUMENT="$REPO_ROOT/docs/specs/openapi-proxy.json"
export LOONFS_CONFORMANCE_URL LOONFS_CONFORMANCE_TOKEN LOONFS_CONFORMANCE_CASES LOONFS_PROXY_DOCUMENT

case "$LANGUAGE" in
    go)
        if (cd "$REPO_ROOT/sdk/conformance/go" && go test ./...); then
            HARNESS_STATUS=0
        else
            HARNESS_STATUS=$?
        fi
        ;;
    python)
        PYTHON_HARNESS="$REPO_ROOT/sdk/conformance/python"
        if [ ! -d "$PYTHON_HARNESS/.venv" ]; then
            python3 -m venv "$PYTHON_HARNESS/.venv"
        fi
        "$PYTHON_HARNESS/.venv/bin/pip" install -q -r "$PYTHON_HARNESS/requirements.txt"
        if PYTHONPATH="$PYTHON_HARNESS" \
            "$PYTHON_HARNESS/.venv/bin/python" -m pytest "$PYTHON_HARNESS/test_conformance.py"; then
            HARNESS_STATUS=0
        else
            HARNESS_STATUS=$?
        fi
        ;;
    typescript)
        TYPESCRIPT_HARNESS="$REPO_ROOT/sdk/conformance/typescript"
        if [ ! -d "$TYPESCRIPT_HARNESS/node_modules" ]; then
            (cd "$TYPESCRIPT_HARNESS" && npm install --no-fund --no-audit)
        fi
        if (cd "$TYPESCRIPT_HARNESS" && npm run build && npm test); then
            HARNESS_STATUS=0
        else
            HARNESS_STATUS=$?
        fi
        ;;
esac

exit "$HARNESS_STATUS"
