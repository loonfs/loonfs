import * as assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { LoonFSClient } from "../../../generated/typescript/transfers.js";
import { LoonFSClient as BrowserClient } from "../../../generated/typescript-client/transfers.js";

type Fixture = { name: string; bytes: number; sent: boolean };
const cases: Fixture[] = JSON.parse(
    readFileSync(join(__dirname, "../../../../../fixtures/appends.json"), "utf8"),
);

for (const browser of [false, true])
    for (const fixture of cases) {
        test(`append ${browser ? "browser" : "server"} ${fixture.name}`, async () => {
            const content = Uint8Array.from({ length: fixture.bytes }, (_, i) => i % 256);
            const paths: string[] = [];
            const send: typeof fetch = async (input, init) => {
                const request = new Request(input, init);
                const path = new URL(request.url).pathname;
                paths.push(path);
                assert.equal(request.method, "POST");
                assert.equal(
                    path,
                    browser ? "/v0/namespace-aliases/demo/commits" : "/v0/namespaces/demo/commits",
                );
                if (!browser) assert.equal(request.headers.get("Loonfs-Actor"), "writer");
                const body = await request.json();
                assert.equal(body.commit_id, "stable");
                assert.equal(body.message, "original");
                assert.equal((body.content_tokens ?? []).length, 0);
                assert.equal(body.operations.length, 1);
                const [op] = body.operations;
                assert.equal(op.kind, "append_file");
                assert.equal(op.path, "/file");
                assert.equal(op.expected_inode_id, "ino_1");
                assert.equal(op.expected_revision_no, 1);
                assert.equal(op.behavior, undefined);
                assert.deepEqual(new Uint8Array(Buffer.from(op.inline_content, "base64")), content);
                return Response.json({
                    namespace_id: "demo",
                    namespace_alias: "demo",
                    commit_id: "stable",
                    committed_seq: 2,
                    committed_at_ms: 0,
                    committed_by: "writer",
                    events: [],
                });
            };
            const options = { baseUrl: "http://api.test", token: "secret", fetch: send };
            const input = {
                path: "/file",
                content,
                commit_id: "stable",
                message: "original",
                expected_inode_id: "ino_1",
                expected_revision_no: 1,
            };
            const requestOptions = { maxRetries: 0, headers: { "Loonfs-Actor": "writer" } };
            const append = browser
                ? new BrowserClient(options).files.append({ namespace_alias: "demo", ...input }, requestOptions)
                : new LoonFSClient(options).files.append({ namespace_id: "demo", ...input }, requestOptions);
            if (fixture.sent) {
                assert.equal((await append).committed_seq, 2);
                assert.equal(paths.length, 1);
            } else {
                await assert.rejects(append, (error) => !(error instanceof assert.AssertionError));
                assert.deepEqual(paths, [], "a refused append must send nothing");
            }
        });
    }
