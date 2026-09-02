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


def remove_exact(source, snippet, expected_count, label):
    actual_count = source.count(snippet)
    assert actual_count == expected_count, (
        f"expected {expected_count} {label}, found {actual_count}"
    )
    return source.replace(snippet, "")

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

option_path = module_root / "option/request_option.go"
source = option_path.read_text()
source = remove_exact(
    source,
    "// WithMaxStreamBufSize configures the maximum buffer size for streaming responses.\n"
    "// This controls the maximum size of a single message (in bytes) that the stream\n"
    "// can process. By default, this is set to 1MB.\n"
    "func WithMaxStreamBufSize(size int) *core.MaxBufSizeOption {\n"
    "\treturn &core.MaxBufSizeOption{\n"
    "\t\tMaxBufSize: size,\n"
    "\t}\n"
    "}\n\n",
    1,
    "WithMaxStreamBufSize option",
)
source = remove_exact(
    source,
    "// WithMaxStreamReconnectAttempts caps the number of transparent mid-stream\n"
    "// reconnect attempts on streaming endpoints that support resumption. The\n"
    "// reconnect loop honors Last-Event-ID and any server-sent `retry:` directives.\n"
    "// Has no effect on endpoints that don't support resumption.\n"
    "func WithMaxStreamReconnectAttempts(attempts uint) *core.MaxStreamReconnectAttemptsOption {\n"
    "\treturn &core.MaxStreamReconnectAttemptsOption{\n"
    "\t\tMaxStreamReconnectAttempts: attempts,\n"
    "\t}\n"
    "}\n\n",
    1,
    "WithMaxStreamReconnectAttempts option",
)
source = remove_exact(
    source,
    "// WithoutStreamReconnection disables transparent mid-stream reconnection on\n"
    "// resumable SSE endpoints. Has no effect on non-resumable endpoints.\n"
    "func WithoutStreamReconnection() *core.WithoutStreamReconnectionOption {\n"
    "\treturn &core.WithoutStreamReconnectionOption{}\n"
    "}\n\n",
    1,
    "WithoutStreamReconnection option",
)
option_path.write_text(source)

core_option_path = module_root / "core/request_option.go"
source = core_option_path.read_text()
source = remove_exact(
    source,
    "\tMaxBufSize                 int\n"
    "\tMaxStreamReconnectAttempts uint\n"
    "\tDisableStreamReconnection  bool\n",
    1,
    "streaming RequestOptions fields",
)
source = remove_exact(
    source,
    "// MaxBufSizeOption implements the RequestOption interface.\n"
    "type MaxBufSizeOption struct {\n"
    "\tMaxBufSize int\n"
    "}\n\n"
    "func (m *MaxBufSizeOption) applyRequestOptions(opts *RequestOptions) {\n"
    "\topts.MaxBufSize = m.MaxBufSize\n"
    "}\n\n",
    1,
    "MaxBufSizeOption type",
)
source = remove_exact(
    source,
    "// MaxStreamReconnectAttemptsOption implements the RequestOption interface.\n"
    "type MaxStreamReconnectAttemptsOption struct {\n"
    "\tMaxStreamReconnectAttempts uint\n"
    "}\n\n"
    "func (m *MaxStreamReconnectAttemptsOption) applyRequestOptions(opts *RequestOptions) {\n"
    "\topts.MaxStreamReconnectAttempts = m.MaxStreamReconnectAttempts\n"
    "}\n\n",
    1,
    "MaxStreamReconnectAttemptsOption type",
)
source = remove_exact(
    source,
    "// WithoutStreamReconnectionOption implements the RequestOption interface.\n"
    "type WithoutStreamReconnectionOption struct{}\n\n"
    "func (w *WithoutStreamReconnectionOption) applyRequestOptions(opts *RequestOptions) {\n"
    "\topts.DisableStreamReconnection = true\n"
    "}\n\n",
    1,
    "WithoutStreamReconnectionOption type",
)
core_option_path.write_text(source)

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


def remove_exact(source, snippet, expected_count, label):
    actual_count = source.count(snippet)
    assert actual_count == expected_count, (
        f"expected {expected_count} {label}, found {actual_count}"
    )
    return source.replace(snippet, "")


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
source = remove_exact(
    source,
    "    stream_reconnection_enabled: NotRequired[bool]\n",
    1,
    "request stream_reconnection_enabled option",
)
source = remove_exact(
    source,
    "    max_stream_reconnection_attempts: NotRequired[int]\n",
    1,
    "request max_stream_reconnection_attempts option",
)
request_options_path.write_text(source)

http_client_path = pathlib.Path("generated/python/core/http_client.py")
source = http_client_path.read_text()
source = source.replace(
    '            else request_options.get("timeout_in_seconds")\n'
    '            if request_options is not None and request_options.get("timeout_in_seconds") is not None\n',
    "",
)
http_client_path.write_text(source)

client_path = pathlib.Path("generated/python/client.py")
source = client_path.read_text()
source = remove_exact(
    source,
    "    stream_reconnection_enabled : typing.Optional[bool]\n"
    "        Whether to automatically reconnect on stream disconnection for resumable streaming endpoints. Defaults to True. Per-request `stream_reconnection_enabled` in `request_options` takes precedence over this value.\n\n",
    2,
    "client stream_reconnection_enabled documentation blocks",
)
source = remove_exact(
    source,
    "    max_stream_reconnection_attempts : typing.Optional[int]\n"
    "        The maximum number of reconnection attempts for resumable streaming endpoints. Defaults to no limit. Per-request `max_stream_reconnection_attempts` in `request_options` takes precedence over this value.\n\n",
    2,
    "client max_stream_reconnection_attempts documentation blocks",
)
source = remove_exact(
    source,
    "        stream_reconnection_enabled: typing.Optional[bool] = None,\n",
    2,
    "client stream_reconnection_enabled parameters",
)
source = remove_exact(
    source,
    "        max_stream_reconnection_attempts: typing.Optional[int] = None,\n",
    2,
    "client max_stream_reconnection_attempts parameters",
)
source = remove_exact(
    source,
    "            stream_reconnection_enabled=stream_reconnection_enabled,\n",
    2,
    "client stream_reconnection_enabled pass-throughs",
)
source = remove_exact(
    source,
    "            max_stream_reconnection_attempts=max_stream_reconnection_attempts,\n",
    2,
    "client max_stream_reconnection_attempts pass-throughs",
)
client_path.write_text(source)

wrapper_path = pathlib.Path("generated/python/core/client_wrapper.py")
source = wrapper_path.read_text()
source = remove_exact(
    source,
    "        stream_reconnection_enabled: typing.Optional[bool] = None,\n",
    3,
    "client wrapper stream_reconnection_enabled parameters",
)
source = remove_exact(
    source,
    "        max_stream_reconnection_attempts: typing.Optional[int] = None,\n",
    3,
    "client wrapper max_stream_reconnection_attempts parameters",
)
source = remove_exact(
    source,
    "        self._stream_reconnection_enabled = stream_reconnection_enabled\n",
    1,
    "client wrapper stream_reconnection_enabled attribute",
)
source = remove_exact(
    source,
    "        self._max_stream_reconnection_attempts = max_stream_reconnection_attempts\n",
    1,
    "client wrapper max_stream_reconnection_attempts attribute",
)
source = remove_exact(
    source,
    "    def get_stream_reconnection_enabled(self) -> bool:\n"
    "        return self._stream_reconnection_enabled if self._stream_reconnection_enabled is not None else True\n\n",
    1,
    "client wrapper stream_reconnection_enabled getter",
)
source = remove_exact(
    source,
    "    def get_max_stream_reconnection_attempts(self) -> typing.Optional[int]:\n"
    "        return self._max_stream_reconnection_attempts\n\n",
    1,
    "client wrapper max_stream_reconnection_attempts getter",
)
source = remove_exact(
    source,
    "            stream_reconnection_enabled=stream_reconnection_enabled,\n",
    2,
    "client wrapper stream_reconnection_enabled pass-throughs",
)
source = remove_exact(
    source,
    "            max_stream_reconnection_attempts=max_stream_reconnection_attempts,\n",
    2,
    "client wrapper max_stream_reconnection_attempts pass-throughs",
)
wrapper_path.write_text(source)

package_root = pathlib.Path("generated/python")
server_module = package_root / "server.py"
(package_root / "__init__.py").replace(server_module)
(package_root / "__init__.py").write_text(
    '"""Explicit server and proxy entry points for the LoonFS SDK."""\n'
)

source = server_module.read_text()
assert '"ApiError"' not in source, "generated server.py unexpectedly exports ApiError"
generated_import = "    from .client import AsyncLoonFS, LoonFS\n"
assert source.count(generated_import) == 1, "generated server.py client import not found"
source = source.replace(
    generated_import,
    "    from .client import AsyncLoonFS\n"
    "    from .core.api_error import ApiError\n"
    "    from .transfers import FileDownloadResult, FileUploadResult, LoonFS\n",
)
generated_mapping = '    "LoonFS": ".client",\n'
assert source.count(generated_mapping) == 1, "generated server.py client mapping not found"
source = source.replace(generated_mapping, '    "LoonFS": ".transfers",\n')
for anchor, insertion, label in (
    ('    "AsyncLoonFS": ".client",\n', '    "ApiError": ".core.api_error",\n', "ApiError mapping"),
    ('    "FileRevision": ".types",\n', '    "FileDownloadResult": ".transfers",\n', "FileDownloadResult mapping"),
    ('    "FilesystemChange": ".types",\n', '    "FileUploadResult": ".transfers",\n', "FileUploadResult mapping"),
):
    assert source.count(anchor) == 1, f"generated server.py anchor for {label} not found exactly once"
    source = source.replace(anchor, insertion + anchor)
for anchor, insertion, label in (
    ('    "AsyncLoonFS",\n', '    "ApiError",\n', "ApiError __all__ entry"),
    ('    "FileRevision",\n', '    "FileDownloadResult",\n', "FileDownloadResult __all__ entry"),
    ('    "FilesystemChange",\n', '    "FileUploadResult",\n', "FileUploadResult __all__ entry"),
):
    assert source.count(anchor) == 1, f"generated server.py anchor for {label} not found exactly once"
    source = source.replace(anchor, insertion + anchor)
server_module.write_text(source)

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

base_client_path = package_root / "BaseClient.ts"
source = base_client_path.read_text()
client_stream_options = (
    "    /** Default options for SSE stream reconnection behavior. Has no effect on non-resumable endpoints. */\n"
    "    stream?: { reconnectionEnabled?: boolean; maxReconnectionAttempts?: number };\n"
)
assert source.count(client_stream_options) == 1, "generated BaseClient.ts client stream options not found"
source = source.replace(client_stream_options, "")
request_stream_options = (
    "    /** Options for SSE stream reconnection behavior. Has no effect on non-resumable endpoints. */\n"
    "    stream?: { reconnectionEnabled?: boolean; maxReconnectionAttempts?: number };\n"
)
assert source.count(request_stream_options) == 1, "generated BaseClient.ts request stream options not found"
source = source.replace(request_stream_options, "")
base_client_path.write_text(source)

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

index_path = package_root / "index.ts"
source = index_path.read_text()
generated_export = 'export { LoonFSClient } from "./Client.js";\n'
assert source.count(generated_export) == 1, "generated index.ts client export not found"
source = source.replace(
    generated_export,
    'export { LoonFSClient } from "./transfers.js";\n'
    'export type {\n'
    '    FileDownloadInput,\n'
    '    FileDownloadResult,\n'
    '    FileUploadInput,\n'
    '    FileUploadResult,\n'
    '} from "./transfers.js";\n',
)
index_path.write_text(source)
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
