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

FERN_CLI_VERSION="5.98.3"
cd "$(dirname "$0")/../sdk"

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
            admin/checkpoints admin/diagnostics admin/grep_index admin/maintenance; do
            rm "generated/go/${name}_test.go"
        done
        python3 - <<'PY'
import pathlib

module_root = pathlib.Path("generated/go")
server_package = module_root / "server"
(module_root / "client").replace(server_package)

for source_path in [*module_root.rglob("*.go"), *module_root.rglob("*.md")]:
    source = source_path.read_text()
    source = source.replace(
        "github.com/loonfs/loonfs-sdk-go/client",
        "github.com/loonfs/loonfs-sdk-go/server",
    )
    source = source.replace(
        'client "github.com/loonfs/loonfs-sdk-go/server"',
        'server "github.com/loonfs/loonfs-sdk-go/server"',
    )
    # Only files that import the root server package construct the root client;
    # server/client.go builds the nested admin client through its own `client` alias.
    if '"github.com/loonfs/loonfs-sdk-go/server"' in source:
        source = source.replace("client.NewClient", "server.NewClient")
    if source_path.parent == server_package:
        source = source.replace("package client", "package server")
    source_path.write_text(source)

test_path = module_root / "internal/explicit_fields_test.go"
source = test_path.read_text()
start = source.index("// Test for backwards compatibility")
end = source.index("// Helper functions", start)
test_path.write_text(source[:start] + source[end:])
PY
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

request_options_path = pathlib.Path("generated/python/core/request_options.py")
source = request_options_path.read_text()
source = source.replace(
    "        - timeout_in_seconds: int. Deprecated alias for `timeout`; both are in seconds. Prefer `timeout`.\n\n",
    "",
)
source = source.replace("    timeout_in_seconds: NotRequired[int]\n", "")
request_options_path.write_text(source)

http_client_path = pathlib.Path("generated/python/core/http_client.py")
source = http_client_path.read_text()
source = source.replace(
    '            else request_options.get("timeout_in_seconds")\n'
    '            if request_options is not None and request_options.get("timeout_in_seconds") is not None\n',
    "",
)
http_client_path.write_text(source)

package_root = pathlib.Path("generated/python")
server_module = package_root / "server.py"
(package_root / "__init__.py").replace(server_module)
(package_root / "__init__.py").write_text(
    '"""Explicit server and proxy entry points for the LoonFS SDK."""\n'
)

for example_path in [*package_root.rglob("*.py"), package_root / "reference.md"]:
    source = example_path.read_text()
    example_path.write_text(
        source.replace("from loonfs import", "from loonfs.server import")
    )
PY
        ;;
    typescript|typescript-client)
        python3 - "$1" <<'PY'
import pathlib
import sys

package_root = pathlib.Path("generated") / sys.argv[1]

response_path = package_root / "core/fetcher/APIResponse.ts"
source = response_path.read_text()
source = source.replace(
    '    /**\n     * @deprecated Use `rawResponse` instead\n     */\n'
    "    headers?: Record<string, any>;\n",
    "",
)
response_path.write_text(source)

fetcher_path = package_root / "core/fetcher/Fetcher.ts"
source = fetcher_path.read_text()
source = source.replace('import { createRequestUrl } from "./createRequestUrl.js";\n', "")
source = source.replace(
    'import { redactUrl, SENSITIVE_QUERY_PARAMS } from "./redactUrl.js";',
    'import { redactUrl } from "./redactUrl.js";',
)
source = source.replace(
    '        /**\n'
    '         * @deprecated Prefer `queryString` (produced by `core.url.queryBuilder()`).\n'
    '         * Retained for backwards compatibility with custom fetchers and callers that\n'
    '         * still construct request args with a query-parameter object.\n'
    '         */\n'
    '        queryParameters?: Record<string, unknown>;\n',
    "",
)
start = source.index("function redactQueryParameters(")
end = source.index("async function getHeaders(", start)
source = source[:start] + source[end:]
source = source.replace(
    '    } else {\n        url = createRequestUrl(args.url, args.queryParameters);\n',
    "",
)
source = source.replace(
    "            queryParameters: redactQueryParameters(args.queryParameters),\n",
    "",
)
source = source.replace("                headers: response.headers,\n", "")
fetcher_path.write_text(source)

(package_root / "core/fetcher/createRequestUrl.ts").unlink()
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
    python3 ../scripts/verify-sdk-retry-safety.py "$1"
}

if [ "$#" -ge 1 ]; then
    generate_group "$1"
    exit 0
fi

for group in go python typescript typescript-client; do
    generate_group "$group"
done
