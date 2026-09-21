import * as assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { LoonFSClient } from "../../../generated/typescript/transfers.js";
import { LoonFSClient as BrowserClient } from "../../../generated/typescript-client/transfers.js";
import { TransferScope, UploadSource } from "../../../generated/typescript/transfer-runtime.js";

type Fixture = {
    name: string;
    bytes: number;
    size: number | null;
    limit?: number | null;
    feature?: boolean;
    inline?: boolean;
    error?: boolean;
    fault?: boolean;
};
const cases: Fixture[] = JSON.parse(
    readFileSync(join(__dirname, "../../../../../fixtures/inline_uploads.json"), "utf8"),
);

for (const browser of [false, true])
    for (const fixture of cases) {
        test(`inline ${browser ? "browser" : "server"} ${fixture.name}`, async () => {
            const content = Uint8Array.from({ length: fixture.bytes }, (_, i) => i % 256);
            let position = 0,
                closed = false;
            const source = {
                async *[Symbol.asyncIterator]() {
                    // Reusing this chunk catches callers retaining mutable source buffers.
                    const chunk = new Uint8Array(1024);
                    try {
                        while (position < content.length) {
                            const size = Math.min(chunk.length, content.length - position);
                            chunk.set(content.subarray(position, position + size));
                            position += size;
                            yield chunk.subarray(0, size);
                        }
                        if (fixture.fault) throw new Error("source failed at EOF");
                    } finally {
                        closed = true;
                    }
                },
            };
            const paths: string[] = [],
                commits: unknown[] = [];
            const session = {
                namespace_id: "demo",
                namespace_alias: "demo",
                upload_id: "upl_test",
                mode: "service_proxied",
            };
            const claim = {
                kind: "blob_v1",
                owner_namespace_id: "demo",
                content_id: "con_test",
                size_bytes: content.length,
                checksum: { algorithm: "sha256", value: "0".repeat(64) },
            };
            const send: typeof fetch = async (input, init) => {
                const request = new Request(input, init);
                const path = new URL(request.url).pathname;
                paths.push(path);
                let value: unknown;
                if (path.endsWith("/capabilities")) {
                    assert.equal(paths.length, 1, "publication must not reselect transport");
                    value = {
                        protocol_version: "v0",
                        api_groups: ["filesystem/v0"],
                        features: { "filesystem.commits.inline_content": fixture.feature ?? true },
                        limits:
                            fixture.limit === null ? {} : { "commit.max_inline_content_bytes": fixture.limit ?? 65536 },
                    };
                } else if (path.endsWith("/uploads")) {
                    assert.ok(!fixture.inline && !fixture.error, "inline preparation starts no upload");
                    if (fixture.feature !== false && fixture.limit !== null)
                        assert.ok(
                            position <= Math.min(fixture.limit ?? 65536, 65536) + 1024,
                            "bounded lookahead plus one caller chunk",
                        );
                    assert.equal((await request.json()).mode, "service_proxied");
                    value = { ...session, status: "open", expires_at_ms: 2000000000000 };
                } else if (path.endsWith("/content")) {
                    assert.deepEqual(
                        new Uint8Array(await request.arrayBuffer()),
                        content,
                        "fallback must preserve every byte",
                    );
                    value = { ...session, status: "open", expires_at_ms: 2000000000000, content_ref: claim };
                } else if (path.endsWith("/complete")) {
                    value = {
                        ...session,
                        status: "completed",
                        completed_at_ms: 0,
                        content_ref: claim,
                        content_token: { content_ref: claim, token: "retained" },
                    };
                } else if (path.endsWith("/commits")) {
                    const body = await request.json();
                    commits.push(body);
                    const op = body.operations[0];
                    assert.equal(body.commit_id, "stable");
                    assert.equal(body.message, "original");
                    assert.equal(op.path, "/file");
                    assert.equal(op.behavior, "replace");
                    assert.equal(op.expected_inode_id, "ino_1");
                    assert.equal(op.expected_revision_no, 1);
                    if (!browser) assert.equal(request.headers.get("Loonfs-Actor"), "writer");
                    if (fixture.inline) {
                        assert.deepEqual(new Uint8Array(Buffer.from(op.inline_content, "base64")), content);
                        assert.equal(op.content_ref, undefined);
                        assert.equal((body.content_tokens ?? []).length, 0);
                    } else {
                        assert.equal(op.inline_content, undefined);
                        assert.deepEqual(op.content_ref, claim);
                    }
                    if (commits.length === 1)
                        return Response.json({ code: "deadline_exceeded", message: "reply lost" }, { status: 503 });
                    assert.deepEqual(body, commits[0], "retry must retain the exact request");
                    value = {
                        namespace_id: "demo",
                        namespace_alias: "demo",
                        commit_id: "stable",
                        committed_seq: 1,
                        committed_at_ms: 0,
                        committed_by: "writer",
                        events: [],
                    };
                } else throw new Error(`unexpected ${path}`);
                return Response.json(value);
            };
            const options = { baseUrl: "http://api.test", token: "secret", fetch: send };
            const client = new LoonFSClient(options),
                browserClient = new BrowserClient(options);
            const requestOptions = { maxRetries: 0, headers: { "Loonfs-Actor": "writer" } };
            const prepare = browser
                ? browserClient.files.prepareStream({
                      namespace_alias: "demo",
                      content: source,
                      size_bytes: fixture.size ?? undefined,
                  })
                : client.files.prepareStream({
                      namespace_id: "demo",
                      content: source,
                      size_bytes: fixture.size ?? undefined,
                  });
            if (fixture.error) {
                await assert.rejects(prepare, (error) => !(error instanceof assert.AssertionError));
                assert.deepEqual(paths, ["/v0/capabilities"]);
            } else {
                const prepared = await prepare;
                assert.equal("inlineContent" in prepared, fixture.inline);
                const publication = {
                    path: "/file",
                    prepared,
                    commit_id: "stable",
                    message: "original",
                    behavior: "replace" as const,
                    expected_inode_id: "ino_1",
                    expected_revision_no: 1,
                };
                const publish = () =>
                    browser
                        ? browserClient.files.uploadPrepared(
                              { namespace_alias: "demo", ...publication },
                              requestOptions,
                          )
                        : client.files.uploadPrepared({ namespace_id: "demo", ...publication }, requestOptions);
                await assert.rejects(publish(), (error) => !(error instanceof assert.AssertionError));
                assert.equal((await publish()).committed_seq, 1);
                assert.equal(commits.length, 2);
            }
            assert.ok(closed, "SDK consumes or releases its source");
        });
    }

test("inline lookahead honours cancellation", async () => {
    const scope = new TransferScope({ timeoutInSeconds: 0.02 });
    const source = new UploadSource(
        {
            async *[Symbol.asyncIterator]() {
                yield new Uint8Array([1]);
                await new Promise(() => {});
            },
        },
        scope,
    );
    try {
        await assert.rejects(source.tryInline(65536));
    } finally {
        source.close();
        scope.close();
    }
});

test("inline preparation copies a caller-owned buffer", async () => {
    const content = new Uint8Array([0, 128, 255]);
    const client = new LoonFSClient({
        baseUrl: "http://api.test",
        token: "secret",
        fetch: async () =>
            Response.json({
                protocol_version: "v0",
                api_groups: [],
                features: { "filesystem.commits.inline_content": true },
                limits: { "commit.max_inline_content_bytes": 65536 },
            }),
    });
    const prepared = await client.files.prepare({ namespace_id: "demo", content });
    content.fill(1);
    assert.ok("inlineContent" in prepared);
    assert.equal(prepared.inlineContent, "AID/");
});
