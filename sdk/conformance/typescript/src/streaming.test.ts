import * as assert from "node:assert/strict";
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
    algorithm: "crc32c" | "crc64nvme";
    checksum: string;
    size_bytes: number;
    range: string | null;
    transport_error?: boolean;
    error_message: string | null;
    direct_error_message: string | null;
    object_requests: string[];
    ranges?: {
        start_offset: number;
        length: number;
        range: string;
        content: string;
    }[];
};
const fixtures: Fixture[] = JSON.parse(
    readFileSync(join(__dirname, "../../../../../fixtures/streaming_downloads.json"), "utf8"),
);

function fakeFetch(
    fixture: Fixture,
    direct: boolean,
    body: ReadableStream<Uint8Array>,
    requests: string[] = [],
): typeof fetch {
    const claim = {
        kind: "blob_v1",
        owner_namespace_id: "demo",
        content_id: "con_00000000000000000000000000000001",
        size_bytes: fixture.size_bytes,
        checksum: { algorithm: fixture.algorithm, value: fixture.checksum },
    };
    return (async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(input instanceof Request ? input.url : input.toString());
        requests.push(url.pathname);
        const headers = new Headers(init?.headers);
        if (url.pathname.startsWith("/object/")) {
            const part = fixture.ranges![Number(url.pathname.split("/").pop())]!;
            assert.equal(headers.has("Authorization"), false);
            assert.equal(headers.has("X-Private"), false);
            assert.equal(headers.get("range"), part.range);
            return new Response(part.content);
        }
        if (url.pathname === "/object") {
            assert.equal(headers.has("Authorization"), false);
            assert.equal(headers.has("X-Private"), false);
            assert.ok(fixture.size_bytes > 0, "a grant of zero bytes needs no request");
            assert.equal(headers.get("range"), fixture.range);
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
                ranges: fixture.ranges?.map((part, index) => ({
                    start_offset: part.start_offset,
                    length: part.length,
                    access: {
                        kind: "presigned_url",
                        method: "GET",
                        url: `http://objects.test/object/${index}`,
                        headers: { range: part.range },
                        expires_at_ms: 2000000000000,
                    },
                })) ?? [
                    {
                        start_offset: 0,
                        length: fixture.size_bytes,
                        access: {
                            kind: "presigned_url",
                            method: "GET",
                            url: "http://objects.test/object",
                            ...(fixture.range === null ? {} : { headers: { range: fixture.range } }),
                            expires_at_ms: 2000000000000,
                        },
                    },
                ],
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
            assert.equal(headers.has("range"), false);
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
                            else if (fixture.transport_error)
                                controller.error(new Error("body interrupted after its last byte"));
                            else controller.close();
                        },
                    },
                    { highWaterMark: 0 },
                );
                const requests: string[] = [];
                const options = {
                    baseUrl: "http://api.test",
                    token: "private-token",
                    headers: { "X-Private": "secret" },
                    fetch: fakeFetch(fixture, direct, body, requests),
                };
                const collect = async () => {
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
                    return new Response(stream.content).text();
                };
                const expectedError = direct ? fixture.direct_error_message : fixture.error_message;
                if (expectedError) await assert.rejects(collect(), { message: expectedError });
                else assert.equal(await collect(), fixture.content);
                if (direct)
                    assert.deepEqual(
                        requests.filter((path) => path.startsWith("/object")),
                        fixture.object_requests,
                    );
                else
                    assert.deepEqual(
                        requests.map((path) => path.split("/").pop()),
                        ["capabilities", "entry", "content"],
                    );
            });
        }

test("incremental CRCs match the catalog values across chunks", () => {
    const bytes = new TextEncoder().encode("123456789");
    for (const [algorithm, expected] of [
        ["crc32c", "e3069283"],
        ["crc64nvme", "ae8b14860a799888"],
    ] as const) {
        for (const stride of [1, 3, 7, 9]) {
            const digest = new IncrementalChecksum(algorithm);
            for (let offset = 0; offset < bytes.length; offset += stride)
                digest.update(bytes.subarray(offset, offset + stride));
            assert.equal(digest.finish().value, expected);
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
        await assert.rejects(stream.content.getReader().read(), {
            name: "TimeoutError",
        });
        assert.ok(cancelled);
    });
}

test("verified streams bound chunks and stop reading when the consumer closes", async () => {
    const bytes = new Uint8Array(3 * 65536);
    const digest = new IncrementalChecksum("crc64nvme");
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
        {
            kind: "blob_v1",
            owner_namespace_id: "demo",
            content_id: "con_00000000000000000000000000000001",
            size_bytes: bytes.length,
            checksum: digest.finish(),
        },
        new TransferScope(),
    );
    const reader = stream.getReader();
    assert.equal((await reader.read()).value?.length, 65536);
    assert.equal(reads, 1);
    await reader.cancel();
    assert.ok(cancelled);
    assert.equal(reads, 1);
});
