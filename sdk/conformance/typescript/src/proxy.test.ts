import * as assert from "node:assert/strict";
import { test } from "node:test";
import { createProxyHandler } from "../../../proxy/typescript/proxy.js";

test("proxy uses upstream authority and replaces browser credentials", async () => {
    const originalFetch = globalThis.fetch;
    let calls = 0;
    globalThis.fetch = async (input, init) => {
        calls++;
        const request = new Request(input, init);
        assert.equal(request.url, "https://api.test/v0/namespaces/demo/filesystem/content?path=%2Ffile");
        for (const header of ["host", "cookie", "connection", "x-hop"])
            assert.equal(request.headers.has(header), false);
        assert.equal(request.headers.get("authorization"), "Bearer server-token");
        assert.equal(request.headers.get("loonfs-actor"), "app-user");
        assert.equal(request.headers.get("range"), "bytes=0-8");
        return new Response("123456789");
    };
    try {
        const proxy = createProxyHandler({
            serverBaseUrl: "https://api.test",
            token: "server-token",
            namespaceAliases: { files: "demo" },
            authorize: () => ({ actorId: "app-user" }),
        });
        const response = await proxy(
            new Request("https://app.test/v0/namespace-aliases/files/filesystem/content?path=%2Ffile", {
                headers: {
                    host: "app.test",
                    cookie: "session=private",
                    authorization: "Bearer browser-token",
                    "loonfs-actor": "browser-user",
                    connection: "x-hop",
                    "x-hop": "private",
                    range: "bytes=0-8",
                },
            }),
        );
        assert.equal(await response.text(), "123456789");
        assert.equal(calls, 1);
    } finally {
        globalThis.fetch = originalFetch;
    }
});
