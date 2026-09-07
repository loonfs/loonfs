import * as assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { LoonFSClient } from "../../../generated/typescript/transfers.js";
import { LoonFSClient as BrowserClient } from "../../../generated/typescript-client/transfers.js";
import { UploadSource, TransferScope } from "../../../generated/typescript/transfer-runtime.js";

type Fixture = {
    name: string;
    content: string;
    algorithm: string;
    checksum: string;
    size_bytes: number;
    size: number | null;
    mode: string;
    fault?: string;
    error: boolean;
};
const cases: Fixture[] = JSON.parse(
    readFileSync(join(__dirname, "../../../../../fixtures/streaming_uploads.json"), "utf8"),
);
for (const browser of [false, true])
    for (const fixture of cases) {
        test(`streaming upload ${browser ? "browser" : "server"} ${fixture.name}`, async () => {
            let position = 0,
                eof = false,
                closed = false;
            const content = new TextEncoder().encode(fixture.content);
            const source = {
                async *[Symbol.asyncIterator]() {
                    try {
                        while (position < content.length) {
                            const start = position;
                            position += Math.min(2, content.length - position);
                            yield content.subarray(start, position);
                        }
                        eof = true;
                        if (fixture.fault === "source_error")
                            throw new Error("source failed after its last byte");
                    } finally {
                        closed = true;
                    }
                },
            };
            const counts = { payload: 0, abort: 0, complete: 0 };
            const bodies: Uint8Array[] = [];
            const claim = {
                kind: "blob",
                content_id: "cnt_00000000000000000000000000000001",
                size_bytes: fixture.size_bytes,
                checksum: { algorithm: fixture.algorithm, value: fixture.checksum },
            };
            const session = {
                namespace_id: "demo",
                namespace_alias: "demo",
                upload_id: "upl_test",
                mode: fixture.mode,
            };
            const access = {
                kind: "presigned_url",
                method: "PUT",
                url: "http://objects.test/object",
                expires_at_ms: 2000000000000,
            };
            const send: typeof fetch = async (input, init) => {
                const url = new URL(input instanceof Request ? input.url : input.toString());
                const path = url.pathname,
                    headers = new Headers(init?.headers);
                assert.ok(init?.signal, "every transport needs a signal");
                if (path === "/object") {
                    assert.equal(headers.has("authorization"), false);
                    assert.equal(headers.has("X-Private"), false);
                } else {
                    if (!browser) assert.equal(headers.get("authorization"), "Bearer private-token");
                    if (!path.endsWith("/capabilities"))
                        assert.ok(
                            path.startsWith(browser ? "/v0/namespace-aliases/demo/" : "/v0/namespaces/demo/"),
                        );
                }
                // Construct a real Request: Node rejects a stream if duplex was lost.
                const request = new Request(url, init);
                let value: unknown;
                if (path.endsWith("/capabilities"))
                    value = {
                        protocol_version: "v0",
                        api_groups: ["filesystem/v0"],
                        features: {
                            "filesystem.uploads.direct_put": fixture.mode === "direct_put",
                            "filesystem.uploads.direct_multipart": fixture.mode === "direct_multipart",
                        },
                        limits: fixture.mode === "direct_put" ? { "upload.max_content_bytes": 0 } : {},
                    };
                else if (path.endsWith("/uploads")) {
                    assert.equal((await request.json()).mode, fixture.mode);
                    value = { ...session, checksum_algorithm: fixture.algorithm, part_size_bytes: 4, access };
                } else if (path.endsWith("/parts")) {
                    const { parts } = await request.json();
                    assert.equal(parts.length, 1);
                    assert.ok(position <= bodies.length * 4 + 4, "send a part before reading the next");
                    value = { ...session, parts: [{ part_number: parts[0].part_number, access }] };
                } else if (path === "/object" || path.endsWith("/content")) {
                    counts.payload++;
                    if (fixture.size !== null)
                        assert.ok(
                            !(init?.body instanceof ReadableStream),
                            "small known bodies must remain portable in browsers",
                        );
                    bodies.push(new Uint8Array(await request.arrayBuffer()));
                    if (fixture.fault === "timeout")
                        return new Promise<Response>((_, reject) => {
                            if (init!.signal!.aborted) reject(init!.signal!.reason);
                            else
                                init!.signal!.addEventListener("abort", () => reject(init!.signal!.reason), {
                                    once: true,
                                });
                        });
                    if (fixture.fault === "payload_error") return new Response(null, { status: 503 });
                    return Response.json(
                        { ...session, content_ref: claim },
                        { headers: { ETag: "test-etag" } },
                    );
                } else if (path.endsWith("/abort")) {
                    counts.abort++;
                    value = { ...session, status: "aborted", aborted_at_ms: 0 };
                } else if (path.endsWith("/complete")) {
                    counts.complete++;
                    assert.ok(eof);
                    const completion = await request.json();
                    if (fixture.mode !== "service_proxied")
                        assert.deepEqual(completion.content, {
                            size_bytes: fixture.size_bytes,
                            checksum: claim.checksum,
                        });
                    if (fixture.fault === "completion_error") return new Response(null, { status: 503 });
                    value = {
                        ...session,
                        status: "completed",
                        completed_at_ms: 0,
                        content_ref: claim,
                        content_token: { token: "retained-token", content_ref: claim },
                    };
                } else throw new Error(`unexpected ${url}`);
                return Response.json(value);
            };
            const options = {
                baseUrl: "http://api.test",
                token: "private-token",
                headers: { "X-Private": "secret" },
                fetch: send,
            };
            const requestOptions = {
                timeoutInSeconds: fixture.fault === "timeout" ? 0.02 : 7,
                maxRetries: 3,
            };
            const result = browser
                ? new BrowserClient(options).files.prepareFileStream(
                      { namespace_alias: "demo", content: source, size_bytes: fixture.size ?? undefined },
                      requestOptions,
                  )
                : new LoonFSClient(options).files.prepareFileStream(
                      { namespace_id: "demo", content: source, size_bytes: fixture.size ?? undefined },
                      requestOptions,
                  );
            if (fixture.error) {
                await assert.rejects(result, (error) => !(error instanceof assert.AssertionError));
                assert.equal(counts.abort, fixture.fault === "completion_error" ? 0 : 1);
                assert.equal(counts.complete, fixture.fault === "completion_error" ? 1 : 0);
                if (fixture.fault === "payload_error") assert.equal(counts.payload, 1);
            } else {
                const prepared = await result;
                assert.equal(prepared.contentToken?.token, "retained-token");
                assert.equal(Buffer.concat(bodies).toString(), fixture.content);
                assert.equal(counts.complete, 1);
                assert.equal(counts.abort, 0);
            }
            assert.ok(closed, "SDK consumes or returns the source");
        });
    }

test("upload cancellation interrupts a stalled source without waiting for its cleanup", async () => {
    const scope = new TransferScope({ timeoutInSeconds: 0.02 });
    const source = new UploadSource(
        {
            async *[Symbol.asyncIterator]() {
                await new Promise(() => {});
                yield new Uint8Array();
            },
        },
        scope,
    );
    try {
        await assert.rejects(source.read());
    } finally {
        source.close();
        scope.close();
    }
});

test("upload bounds large caller chunks and validates before locking a stream", async () => {
    const scope = new TransferScope({});
    const stream = new ReadableStream<Uint8Array>();
    assert.throws(() => new UploadSource(stream, scope, -1));
    assert.equal(stream.locked, false);
    const source = new UploadSource(
        {
            async *[Symbol.asyncIterator]() {
                yield new Uint8Array(1024 * 1024);
            },
        },
        scope,
    );
    try {
        assert.equal((await source.read())?.length, 65536);
    } finally {
        source.close();
        scope.close();
    }
});
