#!/usr/bin/env python3
"""Removes the streaming and deprecated options from a generated TypeScript SDK, applies the LoonFS retry and timeout rules, and exports the transfer helpers."""

import pathlib
import sys


def replace_once(source, old, new):
    actual_count = source.count(old)
    assert actual_count == 1, f"expected exactly one match for {old!r}, found {actual_count}"
    return source.replace(old, new)

package_root = pathlib.Path("generated") / sys.argv[1]

signals_path = package_root / "core/fetcher/signals.ts"
source = signals_path.read_text()
source = replace_once(source, 'const TIMEOUT = "timeout";', 'export class RequestTimeoutError extends Error {}')
source = replace_once(source, 'controller.abort(TIMEOUT)', 'controller.abort(new RequestTimeoutError("Request timed out"))')
signals_path.write_text(source)

request_path = package_root / "core/fetcher/makeRequest.ts"
source = request_path.read_text()
source = replace_once(
    source,
    '''    const response = await fetchFn(url, {
        method: method,
        headers,
        body: requestBody,
        signal: newSignals,
        credentials: withCredentials ? "include" : undefined,
        // @ts-ignore
        duplex,
        ...(disableCache && isCacheNoStoreSupported() ? { cache: "no-store" as RequestCache } : {}),
    });

    if (timeoutAbortId != null) {
        clearTimeout(timeoutAbortId);
    }

    return response;''',
    '''    try {
        return await fetchFn(url, {
            method: method,
            headers,
            body: requestBody,
            signal: newSignals,
            credentials: withCredentials ? "include" : undefined,
            // @ts-ignore
            duplex,
            ...(disableCache && isCacheNoStoreSupported() ? { cache: "no-store" as RequestCache } : {}),
        });
    } finally {
        if (timeoutAbortId != null) {
            clearTimeout(timeoutAbortId);
        }
    }''',
)
request_path.write_text(source)

retry_path = package_root / "core/fetcher/requestWithRetries.ts"
source = retry_path.read_text()
source = replace_once(
    source,
    "function isRetryableStatusCode(statusCode: number): boolean {\n"
    "    return [408, 429].includes(statusCode) || statusCode >= 500;\n"
    "}\n\n",
    "",
)
source = replace_once(
    source,
    "        if (isRetryableStatusCode(response.status)) {\n",
    '        if (response.headers.has("retry-after")) {\n',
)
retry_path.write_text(source)

base_client_path = package_root / "BaseClient.ts"
source = base_client_path.read_text()
client_stream_options = (
    "    /** Default options for SSE stream reconnection behavior. Has no effect on non-resumable endpoints. */\n"
    "    stream?: { reconnectionEnabled?: boolean; maxReconnectionAttempts?: number };\n"
)
assert source.count(client_stream_options) == 1, "generated BaseClient.ts client stream options not found"
source = replace_once(source, client_stream_options, "")
request_stream_options = (
    "    /** Options for SSE stream reconnection behavior. Has no effect on non-resumable endpoints. */\n"
    "    stream?: { reconnectionEnabled?: boolean; maxReconnectionAttempts?: number };\n"
)
assert source.count(request_stream_options) == 1, "generated BaseClient.ts request stream options not found"
source = replace_once(source, request_stream_options, "")
base_client_path.write_text(source)

response_path = package_root / "core/fetcher/APIResponse.ts"
source = response_path.read_text()
source = replace_once(
    source,
    '    /**\n     * @deprecated Use `rawResponse` instead\n     */\n'
    "    headers?: Record<string, any>;\n",
    "",
)
response_path.write_text(source)

fetcher_path = package_root / "core/fetcher/Fetcher.ts"
source = fetcher_path.read_text()
source = replace_once(
    source,
    'import { makeRequest } from "./makeRequest.js";',
    'import { makeRequest } from "./makeRequest.js";\nimport { RequestTimeoutError } from "./signals.js";',
)
source = replace_once(
    source,
    'error instanceof Error && error.name === "AbortError"',
    'error instanceof RequestTimeoutError',
)
source = replace_once(source, 'import { createRequestUrl } from "./createRequestUrl.js";\n', "")
source = replace_once(
    source,
    'import { redactUrl, SENSITIVE_QUERY_PARAMS } from "./redactUrl.js";',
    'import { redactUrl } from "./redactUrl.js";',
)
source = replace_once(
    source,
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
source = replace_once(
    source,
    '    } else {\n        url = createRequestUrl(args.url, args.queryParameters);\n',
    "",
)
source = replace_once(
    source,
    "            queryParameters: redactQueryParameters(args.queryParameters),\n",
    "",
)
source = replace_once(source, "                headers: response.headers,\n", "")
fetcher_path.write_text(source)

(package_root / "core/fetcher/createRequestUrl.ts").unlink()

index_path = package_root / "index.ts"
source = index_path.read_text()
generated_export = 'export { LoonFSClient } from "./Client.js";\n'
assert source.count(generated_export) == 1, "generated index.ts client export not found"
source = replace_once(
    source,
    generated_export,
    'export { LoonFSClient } from "./transfers.js";\n'
    'export type {\n'
    '    AppendInput,\n'
    '    DownloadInput,\n'
    '    DownloadResult,\n'
    '    DownloadStream,\n'
    '    UploadInput,\n'
    '    StreamUploadInput,\n'
    '    PrepareStreamInput,\n'
    '    PreparedContent,\n'
    '    InlinePreparedContent,\n'
    '    PreparedFile,\n'
    '    PreparedUploadInput,\n'
    '} from "./transfers.js";\n',
)
index_path.write_text(source)
