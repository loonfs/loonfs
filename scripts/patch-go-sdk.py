#!/usr/bin/env python3
"""Moves the generated Go client into the server package, removes its streaming options, and applies the LoonFS retry rules."""

import json
import pathlib
import re

module_root = pathlib.Path("generated/go")
server_package = module_root / "server"
(module_root / "client").replace(server_package)


def remove_exact(source, snippet, expected_count, label):
    actual_count = source.count(snippet)
    assert actual_count == expected_count, (
        f"expected {expected_count} {label}, found {actual_count}"
    )
    return source.replace(snippet, "")


def replace_once(source, old, new):
    actual_count = source.count(old)
    assert actual_count == 1, f"expected exactly one match for {old!r}, found {actual_count}"
    return source.replace(old, new)


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
    # server/client.go builds the nested maintenance client through its own `client` alias.
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

retrier_path = module_root / "internal/retrier.go"
source = retrier_path.read_text()
source = replace_once(
    source,
    "type Retrier struct {\n\tattempts uint\n}",
    "type Retrier struct {\n\tattempts uint\n\tdisabled bool\n}",
)
source = replace_once(
    source,
    "\t\tattempts: attempts,\n",
    "\t\tattempts: attempts,\n\t\tdisabled: options.disabled,\n",
)
source = replace_once(source, "\tif options.disabled {", "\tif r.disabled || options.disabled {")
source = replace_once(
    source,
    "\tif r.shouldRetry(response) {",
    "\tif retryAttempt+1 < maxRetryAttempts && r.shouldRetry(response) {",
)
source = replace_once(
    source,
    "\t\ttime.Sleep(delay)",
    "\t\ttimer := time.NewTimer(delay)\n"
    "\t\tdefer timer.Stop()\n"
    "\t\tselect {\n"
    "\t\tcase <-request.Context().Done():\n"
    "\t\t\treturn nil, request.Context().Err()\n"
    "\t\tcase <-timer.C:\n"
    "\t\t}",
)
source = replace_once(
    source,
    "// shouldRetry returns true if the request should be retried based on the given\n"
    "// response status code.\n"
    "func (r *Retrier) shouldRetry(response *http.Response) bool {\n"
    "\treturn response.StatusCode == http.StatusTooManyRequests ||\n"
    "\t\tresponse.StatusCode == http.StatusRequestTimeout ||\n"
    "\t\tresponse.StatusCode >= http.StatusInternalServerError\n"
    "}\n",
    "func (r *Retrier) shouldRetry(response *http.Response) bool {\n"
    '\treturn response.Header.Get("Retry-After") != ""\n'
    "}\n",
)
retrier_path.write_text(source)

spec = json.loads(pathlib.Path("../docs/specs/openapi.json").read_text())
for path_item in spec["paths"].values():
    for method, operation in path_item.items():
        if method not in {"get", "post", "put", "patch", "delete", "options", "head"}:
            continue
        if operation.get("x-fern-retries") != {"disabled": True}:
            continue
        operation_id = operation["operationId"]
        raw_client_path = module_root.joinpath(
            *(segment.lower() for segment in operation["x-fern-sdk-group-name"]),
            "raw_client.go",
        )
        method_name = "".join(
            word[:1].upper() + word[1:]
            for word in operation["x-fern-sdk-method-name"].split("_")
        )
        source = raw_client_path.read_text()
        functions = list(re.finditer(
            rf"^func \(r \*RawClient\) {re.escape(method_name)}\(\n.*?^\}}",
            source,
            re.MULTILINE | re.DOTALL,
        ))
        assert len(functions) == 1, (
            f"expected one raw-client function for {operation_id} in {raw_client_path}, "
            f"found {len(functions)}"
        )
        function = functions[0]
        body, count = re.subn(
            r"^(\s*DisableRetries:[ \t]+)options\.DisableRetries,",
            r"\g<1>true,",
            function.group(),
            flags=re.MULTILINE,
        )
        assert count == 1, (
            f"expected one DisableRetries assignment for {operation_id} in {raw_client_path}, "
            f"found {count}"
        )
        raw_client_path.write_text(source[:function.start()] + body + source[function.end():])
