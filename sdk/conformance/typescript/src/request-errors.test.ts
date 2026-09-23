import { execFile } from "node:child_process";
import { promisify } from "node:util";
import test from "node:test";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const run = promisify(execFile);

// A fresh process catches timers that keep an otherwise finished script alive.
const script = `
import assert from "node:assert/strict";
import http from "node:http";
const { LoonFSClient, LoonFSError, LoonFSTimeoutError } = await import(process.argv[1]);
const mode = process.argv[2];
const controller = new AbortController();
const callerReason = new DOMException("caller stopped", "AbortError");
let requests = 0;
const server = http.createServer((req, res) => {
    requests++;
    if (mode === "success") res.end("{}");
    if (mode === "transport") req.socket.destroy();
    if (mode === "cancel") controller.abort(callerReason);
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
if (mode === "pre-abort") controller.abort(callerReason);
const client = new LoonFSClient({
    baseUrl: "http://127.0.0.1:" + server.address().port,
    token: "local-test",
    maxRetries: 0,
    timeoutInSeconds: mode === "timeout" ? 0.05 : 30,
});
try {
    const request = client.capabilities.retrieve({ abortSignal: controller.signal });
    if (mode === "success") await request;
    else await assert.rejects(request, error => {
        if (mode === "timeout") assert.ok(error instanceof LoonFSTimeoutError);
        else {
            assert.ok(error instanceof LoonFSError);
            assert.ok(!(error instanceof LoonFSTimeoutError));
            if (mode !== "transport") assert.equal(error.cause, callerReason);
        }
        return true;
    });
    assert.equal(requests, mode === "pre-abort" ? 0 : 1);
} finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
}
`;

for (const sdk of ["typescript", "typescript-client"]) {
    for (const mode of ["success", "pre-abort", "cancel", "timeout", "transport"]) {
        test(`${sdk} ${mode} preserves errors and releases request timers`, async () => {
            const entry = pathToFileURL(resolve(__dirname, `../../../generated/${sdk}/index.js`));
            await run(process.execPath, ["--input-type=module", "--eval", script, entry.href, mode], {
                timeout: 5_000,
            });
        });
    }
}
