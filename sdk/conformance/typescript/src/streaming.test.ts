import * as assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { LoonFSClient } from "../../../generated/typescript/transfers.js";
import { LoonFSClient as BrowserClient } from "../../../generated/typescript-client/transfers.js";
import {
    IncrementalChecksum,
    TransferScope,
    verifiedDownload,
} from "../../../generated/typescript/transfer-runtime.js";

type Fixture = {
    name: string;
    content: string;
    algorithm: "sha256" | "crc32c" | "crc64nvme";
    checksum: string;
    size_bytes: number;
    error: boolean;
    transport_error?: boolean;
};
const fixtures: Fixture[] = JSON.parse(
    readFileSync(join(__dirname, "../../../../../fixtures/streaming_downloads.json"), "utf8"),
);

function fakeFetch(fixture: Fixture, direct: boolean, body: ReadableStream<Uint8Array>): typeof fetch {
    const claim = {
        kind: "blob",
        content_id: "cnt_00000000000000000000000000000001",
        size_bytes: fixture.size_bytes,
        checksum: { algorithm: fixture.algorithm, value: fixture.checksum },
    };
    return (async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(input instanceof Request ? input.url : input.toString());
        const headers = new Headers(init?.headers);
        if (url.pathname === "/object") {
            assert.equal(headers.has("Authorization"), false);
            assert.equal(headers.has("X-Private"), false);
            assert.ok(init?.signal, "direct bytes must use the operation's cancellation signal");
            return new Response(body);
        }
        if (url.pathname.endsWith("/capabilities"))
            return Response.json({
                protocol_version: "v0",
                api_groups: ["filesystem/v0"],
                features: { "filesystem.downloads.direct_get": direct },
            });
        if (url.pathname.endsWith("/downloads"))
            return Response.json({
                namespace_id: "demo",
                namespace_alias: "demo",
                path: "/file",
                revision_no: 1,
                content_ref: claim,
                access: {
                    kind: "presigned_url",
                    method: "GET",
                    url: "http://objects.test/object",
                    expires_at_ms: 2000000000000,
                },
            });
        if (url.pathname.endsWith("/entry"))
            return Response.json({
                inode_kind: "file",
                namespace_id: "demo",
                path: "/file",
                revision_no: 1,
                content_ref: claim,
            });
        if (url.pathname.endsWith("/content")) {
            assert.equal(url.searchParams.get("revision_no"), "1");
            assert.ok(init?.signal, "proxy bytes must use the operation's cancellation signal");
            return new Response(body);
        }
        throw new Error(`unexpected request ${url}`);
    }) as typeof fetch;
}

for (const browser of [false, true])
    for (const direct of [false, true])
        for (const fixture of fixtures) {
            test(`verified streaming ${browser ? "browser" : "server"} ${direct ? "direct" : "proxy"} ${fixture.name}`, async () => {
                let reads = 0;
                const body = new ReadableStream<Uint8Array>(
                    {
                        pull(controller) {
                            reads++;
                            if (reads === 1) controller.enqueue(new TextEncoder().encode(fixture.content));
                            else if (fixture.transport_error) controller.error(new Error("body interrupted after its last byte"));
                        else controller.close();
                        },
                    },
                    { highWaterMark: 0 },
                );
                const options = {
                    baseUrl: "http://api.test",
                    token: "private-token",
                    headers: { "X-Private": "secret" },
                    fetch: fakeFetch(fixture, direct, body),
                };
                const stream = browser
                    ? await new BrowserClient(options).files.downloadStream({
                          namespace_alias: "demo",
                          path: "/file",
                      })
                    : await new LoonFSClient(options).files.downloadStream({
                          namespace_id: "demo",
                          path: "/file",
                      });
                assert.equal(reads, 0, "opening must not consume the response body");
                const collected = new Response(stream.content).text();
                if (fixture.error) await assert.rejects(collected);
                else assert.equal(await collected, fixture.content);
            });
        }

test("incremental SHA-256 agrees with the native implementation across block boundaries", () => {
    for (const length of [0, 1, 55, 56, 63, 64, 65, 127, 128, 129, 4096, 65537, 1000000]) {
        const bytes = Uint8Array.from({ length }, (_, i) => (i * 29) % 251);
        const expected = createHash("sha256").update(bytes).digest("hex");
        for (const stride of [1, 7, 63, 64, 65536]) {
            const digest = new IncrementalChecksum("sha256");
            for (let offset = 0; offset < bytes.length; offset += stride)
                digest.update(bytes.subarray(offset, offset + stride));
            assert.equal(digest.finish().value, expected, `length=${length} stride=${stride}`);
        }
    }
});

for (const direct of [false, true]) {
    test(`streaming ${direct ? "direct" : "proxy"} cancellation releases a stalled body`, async () => {
        let cancelled = false;
        const body = new ReadableStream<Uint8Array>(
            {
                cancel() {
                    cancelled = true;
                },
            },
            { highWaterMark: 0 },
        );
        const controller = new AbortController();
        const client = new LoonFSClient({
            baseUrl: "http://api.test",
            token: "private-token",
            fetch: fakeFetch(fixtures[0]!, direct, body),
        });
        const stream = await client.files.downloadStream(
            { namespace_id: "demo", path: "/file" },
            { abortSignal: controller.signal },
        );
        const read = stream.content.getReader().read();
        controller.abort(new Error("caller cancelled"));
        await assert.rejects(read, /caller cancelled/);
        assert.ok(cancelled);
    });
    test(`streaming ${direct ? "direct" : "proxy"} deadline remains active after headers`, async () => {
        let cancelled = false;
        const body = new ReadableStream<Uint8Array>(
            {
                cancel() {
                    cancelled = true;
                },
            },
            { highWaterMark: 0 },
        );
        const client = new LoonFSClient({
            baseUrl: "http://api.test",
            token: "private-token",
            fetch: fakeFetch(fixtures[0]!, direct, body),
        });
        const stream = await client.files.downloadStream(
            { namespace_id: "demo", path: "/file" },
            { timeoutInSeconds: 0.05 },
        );
        await assert.rejects(stream.content.getReader().read(), { name: "TimeoutError" });
        assert.ok(cancelled);
    });
}

test("verified streams bound chunks and stop reading when the consumer closes", async () => {
    const bytes = new Uint8Array(3 * 65536);
    const digest = new IncrementalChecksum("sha256");
    digest.update(bytes);
    let reads = 0,
        cancelled = false;
    const body = new ReadableStream<Uint8Array>(
        {
            pull(controller) {
                reads++;
                controller.enqueue(bytes);
            },
            cancel() {
                cancelled = true;
            },
        },
        { highWaterMark: 0 },
    );
    const stream = verifiedDownload(
        body,
        { size_bytes: bytes.length, checksum: digest.finish() },
        new TransferScope(),
    );
    const reader = stream.getReader();
    assert.equal((await reader.read()).value?.length, 65536);
    assert.equal(reads, 1);
    await reader.cancel();
    assert.ok(cancelled);
    assert.equal(reads, 1);
});
