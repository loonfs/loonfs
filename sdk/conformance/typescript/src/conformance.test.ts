import * as assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { basename, extname, join } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { test } from "node:test";

import { LoonFS, LoonFSClient } from "../../../generated/typescript/index.js";
import { getFile, putFile } from "../../../generated/typescript/transfers.js";
import {
    LoonFS as BrowserLoonFS,
    LoonFSClient as BrowserLoonFSClient,
} from "../../../generated/typescript-client/index.js";
import {
    getFile as getBrowserFile,
    putFile as putBrowserFile,
} from "../../../generated/typescript-client/transfers.js";
import { createProxyHandler } from "../../../proxy/typescript/proxy.js";


const RUNNER_SKIP = "run scripts/run-sdk-conformance.sh typescript";
const CRC64_NVME_POLYNOMIAL = 0x9a6c9329ac4bc9b5n;
const CRC64_MASK = 0xffffffffffffffffn;
const BROWSER_MULTIPART_MIN_BYTES = 8 * 1024 * 1024;
const PROXY_UPLOAD_MAX_BYTES = "upload.max_content_bytes";
const CASE_FIELDS = ["expected", "intent", "name", "request"];
const EXPECTED_CASES = [
    "changes",
    "children_by_inode",
    "commit_replay",
    "download",
    "end_to_end",
    "error_contract",
    "inode_mutations",
    "pagination",
    "proxy",
    "snapshots",
    "upload_abort",
    "upload_direct_put",
    "upload_multipart",
] as const;
const CRC64_NVME_TABLE = makeCrc64NvmeTable();
type JsonObject = Record<string, unknown>;
type ActorKind = "user" | "service" | "system";

interface ActorValue {
    id: string;
    kind: ActorKind;
}

interface ConformanceCase {
    name: string;
    request: JsonObject;
    expected: JsonObject;
}

interface ErrorStatusExpected {
    status: number;
    code: string;
}

interface ErrorContractRequest {
    namespace_id: string;
}

interface ErrorContractExpected {
    unauthenticated: ErrorStatusExpected;
}

interface CommitReplayRequest {
    namespace_id: string;
    commit_id: string;
    actor: ActorValue;
    message: string;
    path: string;
}

interface CommitReplayExpected {
    committed_seq: number;
}

interface PaginationRequest {
    namespace_id: string;
    directory: string;
    actor: ActorValue;
    entry_names: string[];
    page_size: number;
    resume_after_page: number;
}

interface PaginationExpected {
    entry_count: number;
    minimum_page_count: number;
    head_seq: number;
}

interface ChildrenByInodeRequest {
    namespace_id: string;
    directory: string;
    renamed_directory: string;
    rename_commit_id: string;
    actor: ActorValue;
    entry_names: string[];
    page_size: number;
    rename_after_page: number;
    resume_after_page: number;
}

interface ChildrenByInodeExpected {
    entry_count: number;
    minimum_page_count: number;
    initial_head_seq: number;
    renamed_head_seq: number;
}

interface InodeMutationsRequest {
    namespace_id: string;
    directory: string;
    actor: ActorValue;
    path_directory_name: string;
    path_file_name: string;
    inode_directory_name: string;
    inode_file_name: string;
    renamed_file_name: string;
    moved_file_name: string;
    content_utf8: string;
    revised_content_utf8: string;
    malformed_binding_generation: string;
}

interface InodeMutationsExpected {
    entry_names: string[];
    revised_revision_no: number;
    moved_committed_seq: number;
    deleted_committed_seq: number;
    stale_binding_generation: ErrorStatusExpected;
    malformed_binding_generation: ErrorStatusExpected;
}

interface SnapshotsRequest {
    namespace_id: string;
    directory: string;
    actor: ActorValue;
    snapshot_name: string;
    replaced_file_name: string;
    deleted_file_name: string;
    added_file_name: string;
    captured_content_utf8: string;
    current_content_utf8: string;
    deleted_content_utf8: string;
    added_content_utf8: string;
    create_ttl_ms: number;
    extend_ttl_ms: number;
    unknown_snapshot_id: string;
}

interface SnapshotsExpected {
    snapshot_head_seq: number;
    captured_revision_no: number;
    captured_entry_names: string[];
    current_revision_no: number;
    current_entry_names: string[];
    snapshot_change_seqs: number[];
    snapshot_gone: ErrorStatusExpected;
    snapshot_not_found: ErrorStatusExpected;
    revision_with_snapshot: ErrorStatusExpected;
    zero_ttl: ErrorStatusExpected;
}

interface ChangesRequest {
    namespace_id: string;
    path: string;
    commit_id: string;
    actor: ActorValue;
    after_seq: number;
}

interface ChangesExpected {
    committed_seq: number;
    change_count: number;
}

interface DirectPutRequest {
    namespace_id: string;
    path: string;
    commit_id: string;
    actor: ActorValue;
    content_utf8: string;
}

interface DirectPutExpected {
    mode: string;
    size_bytes: number;
    checksum_algorithm: string;
    committed_seq: number;
}

interface BytePattern {
    length: number;
    modulus: number;
}

interface MultipartRequest {
    namespace_id: string;
    path: string;
    commit_id: string;
    actor: ActorValue;
    part_size_bytes: number;
    content_pattern: BytePattern;
}

interface MultipartExpected {
    mode: string;
    part_count: number;
    size_bytes: number;
    checksum_algorithm: string;
    committed_seq: number;
}

interface AbortRequest {
    namespace_id: string;
}

interface AbortExpected {
    mode: string;
    status: string;
}

interface DownloadRequest {
    namespace_id: string;
    path: string;
    commit_id: string;
    actor: ActorValue;
    content_utf8: string;
}

interface DownloadExpected {
    size_bytes: number;
    checksum_algorithm: string;
    committed_seq: number;
}

interface EndToEndCommitIds {
    mkdir: string;
    upload: string;
    move: string;
    remove: string;
}

interface EndToEndRequest {
    namespace_id: string;
    directory: string;
    upload_path: string;
    moved_path: string;
    actor: ActorValue;
    content_utf8: string;
    commit_ids: EndToEndCommitIds;
}

interface EndToEndExpected {
    mkdir_committed_seq: number;
    upload_committed_seq: number;
    move_committed_seq: number;
    remove_committed_seq: number;
    size_bytes: number;
    revision_count: number;
    change_count: number;
}

interface ProxyCommitIds {
    directory: string;
    proxied: string;
    direct: string;
}

interface ProxyRequest {
    namespace_alias: string;
    namespace_id: string;
    unknown_namespace_alias: string;
    actor: ActorValue;
    directory: string;
    proxied_path: string;
    direct_path: string;
    commit_ids: ProxyCommitIds;
    content_utf8: string;
    disallowed_path_suffix: string;
}

interface ProxyExpected {
    mkdir_committed_seq: number;
    proxied_committed_seq: number;
    direct_committed_seq: number;
    entry_count: number;
    unknown_namespace_alias_status: number;
    disallowed_route_status: number;
}

interface Harness {
    client: LoonFSClient;
    unauthenticated: LoonFSClient;
    serverBaseUrl: string;
    token: string;
}

interface RunningProxy {
    baseUrl: string;
    close: () => Promise<void>;
}

interface RunningRecordingServer extends RunningProxy {
    requests: string[];
}

type StreamingRequestInit = RequestInit & { duplex?: "half" };

type CompletedUpload = Extract<LoonFS.UploadSession, { status: "completed" }>;
type AbortedUpload = Extract<LoonFS.UploadSession, { status: "aborted" }>;
type FileEntry = Extract<LoonFS.PathEntry, { inode_kind: "file" }>;


function jsonObject(value: unknown, label: string): JsonObject {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new Error(`${label} must be a JSON object`);
    }
    return value as JsonObject;
}

function strictObject(value: unknown, fields: readonly string[], label: string): JsonObject {
    const data = jsonObject(value, label);
    const actual = Object.keys(data).sort();
    const expected = [...fields].sort();
    if (actual.length !== expected.length || actual.some((field, index) => field !== expected[index])) {
        const unknown = actual.filter((field) => !expected.includes(field));
        const missing = expected.filter((field) => !actual.includes(field));
        throw new Error(`${label} fields differ: unknown=${unknown.join(",")}, missing=${missing.join(",")}`);
    }
    return data;
}

const ERROR_CONTRACT_REQUEST_FIELDS = ["namespace_id"] as const;
const ERROR_CONTRACT_EXPECTED_FIELDS = ["unauthenticated"] as const;
const COMMIT_REPLAY_REQUEST_FIELDS = [
    "namespace_id",
    "commit_id",
    "actor",
    "message",
    "path",
] as const;
const COMMIT_REPLAY_EXPECTED_FIELDS = ["committed_seq"] as const;
const PAGINATION_REQUEST_FIELDS = [
    "namespace_id",
    "directory",
    "actor",
    "entry_names",
    "page_size",
    "resume_after_page",
] as const;
const PAGINATION_EXPECTED_FIELDS = ["entry_count", "minimum_page_count", "head_seq"] as const;
const CHILDREN_BY_INODE_REQUEST_FIELDS = [
    "namespace_id",
    "directory",
    "renamed_directory",
    "rename_commit_id",
    "actor",
    "entry_names",
    "page_size",
    "rename_after_page",
    "resume_after_page",
] as const;
const CHILDREN_BY_INODE_EXPECTED_FIELDS = [
    "entry_count",
    "minimum_page_count",
    "initial_head_seq",
    "renamed_head_seq",
] as const;
const INODE_MUTATIONS_REQUEST_FIELDS = [
    "namespace_id",
    "directory",
    "actor",
    "path_directory_name",
    "path_file_name",
    "inode_directory_name",
    "inode_file_name",
    "renamed_file_name",
    "moved_file_name",
    "content_utf8",
    "revised_content_utf8",
    "malformed_binding_generation",
] as const;
const INODE_MUTATIONS_EXPECTED_FIELDS = [
    "entry_names",
    "revised_revision_no",
    "moved_committed_seq",
    "deleted_committed_seq",
    "stale_binding_generation",
    "malformed_binding_generation",
] as const;
const SNAPSHOTS_REQUEST_FIELDS = [
    "namespace_id",
    "directory",
    "actor",
    "snapshot_name",
    "replaced_file_name",
    "deleted_file_name",
    "added_file_name",
    "captured_content_utf8",
    "current_content_utf8",
    "deleted_content_utf8",
    "added_content_utf8",
    "create_ttl_ms",
    "extend_ttl_ms",
    "unknown_snapshot_id",
] as const;
const SNAPSHOTS_EXPECTED_FIELDS = [
    "snapshot_head_seq",
    "captured_revision_no",
    "captured_entry_names",
    "current_revision_no",
    "current_entry_names",
    "snapshot_change_seqs",
    "snapshot_gone",
    "snapshot_not_found",
    "revision_with_snapshot",
    "zero_ttl",
] as const;
const CHANGES_REQUEST_FIELDS = [
    "namespace_id",
    "path",
    "commit_id",
    "actor",
    "after_seq",
] as const;
const CHANGES_EXPECTED_FIELDS = ["committed_seq", "change_count"] as const;
const DIRECT_PUT_REQUEST_FIELDS = [
    "namespace_id",
    "path",
    "commit_id",
    "actor",
    "content_utf8",
] as const;
const DIRECT_PUT_EXPECTED_FIELDS = [
    "mode",
    "size_bytes",
    "checksum_algorithm",
    "committed_seq",
] as const;
const MULTIPART_REQUEST_FIELDS = [
    "namespace_id",
    "path",
    "commit_id",
    "actor",
    "part_size_bytes",
    "content_pattern",
] as const;
const MULTIPART_EXPECTED_FIELDS = [
    "mode",
    "part_count",
    "size_bytes",
    "checksum_algorithm",
    "committed_seq",
] as const;
const ABORT_REQUEST_FIELDS = ["namespace_id"] as const;
const ABORT_EXPECTED_FIELDS = ["mode", "status"] as const;
const DOWNLOAD_REQUEST_FIELDS = [
    "namespace_id",
    "path",
    "commit_id",
    "actor",
    "content_utf8",
] as const;
const DOWNLOAD_EXPECTED_FIELDS = ["size_bytes", "checksum_algorithm", "committed_seq"] as const;
const END_TO_END_REQUEST_FIELDS = [
    "namespace_id",
    "directory",
    "upload_path",
    "moved_path",
    "actor",
    "content_utf8",
    "commit_ids",
] as const;
const END_TO_END_EXPECTED_FIELDS = [
    "mkdir_committed_seq",
    "upload_committed_seq",
    "move_committed_seq",
    "remove_committed_seq",
    "size_bytes",
    "revision_count",
    "change_count",
] as const;
const PROXY_REQUEST_FIELDS = [
    "namespace_alias",
    "namespace_id",
    "unknown_namespace_alias",
    "actor",
    "directory",
    "proxied_path",
    "direct_path",
    "commit_ids",
    "content_utf8",
    "disallowed_path_suffix",
] as const;
const PROXY_EXPECTED_FIELDS = [
    "mkdir_committed_seq",
    "proxied_committed_seq",
    "direct_committed_seq",
    "entry_count",
    "unknown_namespace_alias_status",
    "disallowed_route_status",
] as const;

function decodeErrorContract(
    testCase: ConformanceCase,
): [ErrorContractRequest, ErrorContractExpected] {
    strictObject(testCase.request, ERROR_CONTRACT_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, ERROR_CONTRACT_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as ErrorContractRequest,
        testCase.expected as unknown as ErrorContractExpected,
    ];
}

function decodeCommitReplay(
    testCase: ConformanceCase,
): [CommitReplayRequest, CommitReplayExpected] {
    strictObject(testCase.request, COMMIT_REPLAY_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, COMMIT_REPLAY_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as CommitReplayRequest,
        testCase.expected as unknown as CommitReplayExpected,
    ];
}

function decodePagination(
    testCase: ConformanceCase,
): [PaginationRequest, PaginationExpected] {
    strictObject(testCase.request, PAGINATION_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, PAGINATION_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as PaginationRequest,
        testCase.expected as unknown as PaginationExpected,
    ];
}

function decodeChildrenByInode(
    testCase: ConformanceCase,
): [ChildrenByInodeRequest, ChildrenByInodeExpected] {
    strictObject(
        testCase.request,
        CHILDREN_BY_INODE_REQUEST_FIELDS,
        `${testCase.name} request`,
    );
    strictObject(
        testCase.expected,
        CHILDREN_BY_INODE_EXPECTED_FIELDS,
        `${testCase.name} expected`,
    );
    return [
        testCase.request as unknown as ChildrenByInodeRequest,
        testCase.expected as unknown as ChildrenByInodeExpected,
    ];
}

function decodeInodeMutations(
    testCase: ConformanceCase,
): [InodeMutationsRequest, InodeMutationsExpected] {
    strictObject(testCase.request, INODE_MUTATIONS_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, INODE_MUTATIONS_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as InodeMutationsRequest,
        testCase.expected as unknown as InodeMutationsExpected,
    ];
}

function decodeSnapshots(
    testCase: ConformanceCase,
): [SnapshotsRequest, SnapshotsExpected] {
    strictObject(testCase.request, SNAPSHOTS_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, SNAPSHOTS_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as SnapshotsRequest,
        testCase.expected as unknown as SnapshotsExpected,
    ];
}

function decodeChanges(testCase: ConformanceCase): [ChangesRequest, ChangesExpected] {
    strictObject(testCase.request, CHANGES_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, CHANGES_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as ChangesRequest,
        testCase.expected as unknown as ChangesExpected,
    ];
}

function decodeDirectPut(testCase: ConformanceCase): [DirectPutRequest, DirectPutExpected] {
    strictObject(testCase.request, DIRECT_PUT_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, DIRECT_PUT_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as DirectPutRequest,
        testCase.expected as unknown as DirectPutExpected,
    ];
}

function decodeMultipart(testCase: ConformanceCase): [MultipartRequest, MultipartExpected] {
    strictObject(testCase.request, MULTIPART_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, MULTIPART_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as MultipartRequest,
        testCase.expected as unknown as MultipartExpected,
    ];
}

function decodeAbort(testCase: ConformanceCase): [AbortRequest, AbortExpected] {
    strictObject(testCase.request, ABORT_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, ABORT_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as AbortRequest,
        testCase.expected as unknown as AbortExpected,
    ];
}

function decodeDownload(testCase: ConformanceCase): [DownloadRequest, DownloadExpected] {
    strictObject(testCase.request, DOWNLOAD_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, DOWNLOAD_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as DownloadRequest,
        testCase.expected as unknown as DownloadExpected,
    ];
}

function decodeEndToEnd(testCase: ConformanceCase): [EndToEndRequest, EndToEndExpected] {
    strictObject(testCase.request, END_TO_END_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, END_TO_END_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as EndToEndRequest,
        testCase.expected as unknown as EndToEndExpected,
    ];
}

function decodeProxy(testCase: ConformanceCase): [ProxyRequest, ProxyExpected] {
    strictObject(testCase.request, PROXY_REQUEST_FIELDS, `${testCase.name} request`);
    strictObject(testCase.expected, PROXY_EXPECTED_FIELDS, `${testCase.name} expected`);
    return [
        testCase.request as unknown as ProxyRequest,
        testCase.expected as unknown as ProxyExpected,
    ];
}

function loadCases(directory: string): Map<string, ConformanceCase> {
    const cases = readdirSync(directory, { withFileTypes: true })
        .filter((entry) => entry.isFile() && extname(entry.name) === ".json")
        .sort((left, right) => left.name.localeCompare(right.name))
        .map((entry): ConformanceCase => {
            const path = join(directory, entry.name);
            const root = strictObject(
                JSON.parse(readFileSync(path, "utf8")),
                CASE_FIELDS,
                path,
            ) as unknown as {
                name: string;
                intent: string;
                request: unknown;
                expected: unknown;
            };
            const { name, intent } = root;
            const stem = basename(path, extname(path));
            if (name !== stem) {
                throw new Error(`invalid fixture ${path}: name is ${name}, expected ${stem}`);
            }
            if (intent.trim() === "") {
                throw new Error(`invalid fixture ${path}: intent must not be empty`);
            }
            return {
                name,
                request: jsonObject(root.request, `${path} request`),
                expected: jsonObject(root.expected, `${path} expected`),
            };
        });

    const inventory = cases.map((testCase) => testCase.name);
    assert.deepEqual(inventory, EXPECTED_CASES, "fixture inventory differs");
    return new Map(cases.map((testCase) => [testCase.name, testCase]));
}

function requiredEnvironment(name: string): string {
    const value = process.env[name];
    if (value == null || value === "") {
        throw new Error(`${name} is not set`);
    }
    return value;
}

function caseNamed(cases: Map<string, ConformanceCase>, name: string): ConformanceCase {
    const testCase = cases.get(name);
    if (testCase == null) {
        throw new Error(`conformance case ${name} is missing`);
    }
    return testCase;
}

function directoryCommit(
    namespaceId: string,
    commitId: string,
    actor: ActorValue,
    path: string,
    message?: string,
): LoonFS.CommitRequest {
    const request: LoonFS.CommitRequest = {
        namespace_id: namespaceId,
        actor,
        commit_id: commitId,
        operations: [{ kind: "create_directory", parents: false, path }],
    };
    if (message !== undefined) {
        request.message = message;
    }
    return request;
}

function fileCommit(
    namespaceId: string,
    commitId: string,
    actor: ActorValue,
    path: string,
    contentRef: LoonFS.ContentRef,
    contentToken?: LoonFS.ContentToken,
): LoonFS.CommitRequest {
    const request: LoonFS.CommitRequest = {
        namespace_id: namespaceId,
        actor,
        commit_id: commitId,
        operations: [
            {
                kind: "put_file",
                path,
                content_ref: contentRef,
                behavior: "no_replace",
            },
        ],
    };
    if (contentToken !== undefined) {
        request.content_tokens = [contentToken];
    }
    return request;
}

function namespaceAliasDirectoryCommit(
    commitId: string,
    actor: ActorValue,
    path: string,
): Omit<LoonFS.CommitRequest, "namespace_id"> {
    return {
        actor,
        commit_id: commitId,
        operations: [{ kind: "create_directory", parents: false, path }],
    };
}

function namespaceAliasFileCommit(
    commitId: string,
    actor: ActorValue,
    path: string,
    completed: CompletedUpload,
): Omit<LoonFS.CommitRequest, "namespace_id"> {
    const request: Omit<LoonFS.CommitRequest, "namespace_id"> = {
        actor,
        commit_id: commitId,
        operations: [
            {
                kind: "put_file",
                path,
                content_ref: completed.content_ref,
                behavior: "no_replace",
            },
        ],
    };
    if (completed.content_token !== undefined) {
        request.content_tokens = [completed.content_token];
    }
    return request;
}

function moveCommit(
    namespaceId: string,
    commitId: string,
    actor: ActorValue,
    fromPath: string,
    toPath: string,
): LoonFS.CommitRequest {
    return {
        namespace_id: namespaceId,
        actor,
        commit_id: commitId,
        operations: [
            {
                kind: "move_path",
                from_path: fromPath,
                to_path: toPath,
                behavior: "no_replace",
            },
        ],
    };
}

function deleteCommit(
    namespaceId: string,
    commitId: string,
    actor: ActorValue,
    path: string,
): LoonFS.CommitRequest {
    return {
        namespace_id: namespaceId,
        actor,
        commit_id: commitId,
        operations: [{ kind: "delete_path", path, behavior: "non_recursive" }],
    };
}

function bytePattern(pattern: BytePattern): Uint8Array {
    if (pattern.length < 0 || pattern.modulus === 0) {
        throw new Error("invalid byte pattern");
    }
    return Uint8Array.from({ length: pattern.length }, (_, offset) => offset % pattern.modulus);
}

function splitBytes(bytes: Uint8Array, partSize: number): Uint8Array[] {
    const parts: Uint8Array[] = [];
    for (let offset = 0; offset < bytes.byteLength; offset += partSize) {
        parts.push(bytes.subarray(offset, Math.min(offset + partSize, bytes.byteLength)));
    }
    return parts;
}

async function uploadPresigned(
    access: LoonFS.ObjectTransferAccess,
    bytes: Uint8Array,
    label: string,
): Promise<Response> {
    assert.equal(access.method, "PUT", `${label} presigned method`);
    const response = await fetch(access.url, {
        method: access.method,
        headers: access.headers,
        body: arrayBuffer(bytes),
    });
    assert.ok(response.ok, `${label} failed with HTTP ${response.status}`);
    return response;
}

async function readProxied(
    client: LoonFSClient,
    namespaceId: string,
    path: string,
    snapshotId?: string,
    revisionNo?: number,
): Promise<Uint8Array> {
    const response = await client.files.content({
        namespace_id: namespaceId,
        path,
        snapshot_id: snapshotId,
        revision_no: revisionNo,
    });
    return new Uint8Array(await response.arrayBuffer());
}

async function assertBrowserTransfer(
    client: BrowserLoonFSClient,
    namespaceAlias: string,
    path: string,
    bytes: Uint8Array,
    actor: ActorValue,
    commitId: string,
    label: string,
): Promise<void> {
    const committed = await putBrowserFile(client, {
        namespace_alias: namespaceAlias,
        path,
        bytes,
        actor,
        commit_id: commitId,
    });
    assert.ok(committed.committed_seq > 0, `${label} commit sequence is not positive`);

    const readback = await getBrowserFile(client, {
        namespace_alias: namespaceAlias,
        path,
    });
    assert.deepEqual(readback.bytes, bytes);
    assert.equal(readback.content_ref.size_bytes, bytes.byteLength);
    const contentChecksum = readback.content_ref.checksum;
    if (contentChecksum !== undefined) {
        assert.deepEqual(checksum(contentChecksum.algorithm, bytes), contentChecksum);
    }
}

async function stageContent(
    client: LoonFSClient,
    namespaceId: string,
    bytes: Uint8Array,
): Promise<CompletedUpload> {
    const begin = await client.uploads.create({
        namespace_id: namespaceId,
        body: { mode: "service_proxied" },
    });
    await client.uploads.putContent(arrayBuffer(bytes), namespaceId, begin.upload_id);
    return completedUpload(
        await client.uploads.complete({
            namespace_id: namespaceId,
            upload_id: begin.upload_id,
            body: { mode: "service_proxied" },
        }),
    );
}

function stagedCommit(
    namespaceId: string,
    commitId: string,
    actor: ActorValue,
    operation: LoonFS.FilesystemOperation,
    staged: CompletedUpload,
): LoonFS.CommitRequest {
    const request: LoonFS.CommitRequest = {
        namespace_id: namespaceId,
        actor,
        commit_id: commitId,
        operations: [operation],
    };
    if (staged.content_token !== undefined) {
        request.content_tokens = [staged.content_token];
    }
    return request;
}

function completedUpload(response: LoonFS.UploadSession): CompletedUpload {
    if (response.status !== "completed") {
        throw new Error(`upload ${response.upload_id} is ${response.status}, not completed`);
    }
    return response;
}

function abortedUpload(response: LoonFS.UploadSession): AbortedUpload {
    if (response.status !== "aborted") {
        throw new Error(`upload ${response.upload_id} is ${response.status}, not aborted`);
    }
    return response;
}

function fileEntry(entry: LoonFS.PathEntry): FileEntry {
    if (entry.inode_kind !== "file") {
        throw new Error(`path ${entry.path} is a ${entry.inode_kind}, not a file`);
    }
    return entry;
}

function checksum(algorithm: LoonFS.ChecksumAlgorithm, bytes: Uint8Array): LoonFS.Checksum {
    switch (algorithm) {
        case "sha256":
            return { algorithm, value: createHash("sha256").update(bytes).digest("hex") };
        case "crc64nvme":
            return { algorithm, value: crc64Nvme(bytes).toString(16).padStart(16, "0") };
        default:
            throw new Error(`unsupported checksum algorithm ${algorithm}`);
    }
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
    const copy = new Uint8Array(bytes.byteLength);
    copy.set(bytes);
    return copy.buffer;
}

function streamedBytes(bytes: Uint8Array): ReadableStream<Uint8Array> {
    const split = Math.floor(bytes.byteLength / 2);
    const chunks = [bytes.subarray(0, split), bytes.subarray(split)].filter(
        (chunk) => chunk.byteLength > 0,
    );
    let index = 0;
    return new ReadableStream<Uint8Array>({
        pull(controller) {
            const chunk = chunks[index];
            if (chunk === undefined) {
                controller.close();
                return;
            }
            controller.enqueue(chunk);
            index += 1;
        },
    });
}

function makeCrc64NvmeTable(): bigint[] {
    const table: bigint[] = [];
    for (let index = 0; index < 256; index += 1) {
        let value = BigInt(index);
        for (let bit = 0; bit < 8; bit += 1) {
            value = (value >> 1n) ^ ((value & 1n) === 0n ? 0n : CRC64_NVME_POLYNOMIAL);
        }
        table.push(value & CRC64_MASK);
    }
    return table;
}

function crc64Nvme(bytes: Uint8Array): bigint {
    let value = CRC64_MASK;
    for (const byte of bytes) {
        const index = Number((value ^ BigInt(byte)) & 0xffn);
        value = CRC64_NVME_TABLE[index]! ^ (value >> 8n);
    }
    return (value ^ CRC64_MASK) & CRC64_MASK;
}

function listedNames(entries: LoonFS.PathEntry[]): string[] {
    return entries.map((entry) => {
        const name = entry.display_name;
        assert.ok(name != null, "listed entry has no display_name");
        return name;
    });
}

async function startProxyServer(
    handler: (request: Request) => Promise<Response>,
): Promise<RunningProxy> {
    return startServer((incoming, outgoing) => {
        void serveProxyRequest(handler, incoming, outgoing).catch(() => {
            if (!outgoing.headersSent) {
                outgoing.writeHead(500);
            }
            outgoing.end();
        });
    });
}

async function startServer(
    listener: (request: IncomingMessage, response: ServerResponse) => void,
): Promise<RunningProxy> {
    const server = createServer(listener);
    await new Promise<void>((resolve, reject) => {
        const onError = (error: Error): void => reject(error);
        server.once("error", onError);
        server.listen(0, "127.0.0.1", () => {
            server.off("error", onError);
            resolve();
        });
    });
    const address = server.address();
    assert.ok(address !== null && typeof address !== "string");
    return {
        baseUrl: `http://127.0.0.1:${address.port}`,
        close: () =>
            new Promise<void>((resolve, reject) => {
                server.close((error) => {
                    if (error === undefined) {
                        resolve();
                    } else {
                        reject(error);
                    }
                });
                server.closeIdleConnections();
            }),
    };
}

async function startRecordingServer(): Promise<RunningRecordingServer> {
    const requests: string[] = [];
    const handler = (incoming: IncomingMessage, outgoing: ServerResponse): void => {
        const method = incoming.method ?? "GET";
        const path = new URL(incoming.url ?? "/", "http://127.0.0.1").pathname;
        requests.push(`${method} ${path}`);
        incoming.resume();
        outgoing.writeHead(200, {
            "content-type": "application/json",
            "content-length": "2",
        });
        outgoing.end("{}");
    };
    return { ...(await startServer(handler)), requests };
}

async function serveProxyRequest(
    handler: (request: Request) => Promise<Response>,
    incoming: IncomingMessage,
    outgoing: ServerResponse,
): Promise<void> {
    const headers = new Headers();
    for (let index = 0; index < incoming.rawHeaders.length; index += 2) {
        headers.append(incoming.rawHeaders[index]!, incoming.rawHeaders[index + 1]!);
    }
    const method = incoming.method ?? "GET";
    const init: StreamingRequestInit = { method, headers };
    if (method !== "GET" && method !== "HEAD") {
        init.body = Readable.toWeb(incoming) as unknown as ReadableStream<Uint8Array>;
        init.duplex = "half";
    }
    const host = incoming.headers.host ?? "127.0.0.1";
    const response = await handler(new Request(`http://${host}${incoming.url ?? "/"}`, init));

    outgoing.statusCode = response.status;
    if (response.statusText !== "") {
        outgoing.statusMessage = response.statusText;
    }
    response.headers.forEach((value, name) => outgoing.setHeader(name, value));
    if (response.body === null) {
        outgoing.end();
        return;
    }
    const body = Readable.fromWeb(
        response.body as unknown as import("node:stream/web").ReadableStream<Uint8Array>,
    );
    await pipeline(body, outgoing);
}

async function fetchThroughProxy(
    url: string | URL,
    init: StreamingRequestInit = {},
): Promise<Response> {
    const headers = new Headers(init.headers);
    headers.set("authorization", "Bearer browser-credential");
    return fetch(url, { ...init, headers });
}

async function proxyJson<T>(
    url: string | URL,
    init: StreamingRequestInit,
    label: string,
): Promise<T> {
    const response = await fetchThroughProxy(url, init);
    await requireSuccessfulResponse(response, label);
    return (await response.json()) as T;
}

async function requireSuccessfulResponse(response: Response, label: string): Promise<void> {
    if (!response.ok) {
        const body = await response.text();
        assert.fail(`${label} failed with HTTP ${response.status}: ${body}`);
    }
}


const baseUrl = process.env.LOONFS_CONFORMANCE_URL;
const environmentSkip = baseUrl == null || baseUrl === "" ? RUNNER_SKIP : undefined;
let harness: Harness | undefined;
let cases: Map<string, ConformanceCase> | undefined;

if (environmentSkip === undefined) {
    const configuredBaseUrl = requiredEnvironment("LOONFS_CONFORMANCE_URL");
    const token = requiredEnvironment("LOONFS_CONFORMANCE_TOKEN");
    cases = loadCases(requiredEnvironment("LOONFS_CONFORMANCE_CASES"));
    harness = {
        client: new LoonFSClient({ environment: configuredBaseUrl, token }),
        unauthenticated: new LoonFSClient({ environment: configuredBaseUrl, token, auth: false }),
        serverBaseUrl: configuredBaseUrl,
        token,
    };
}

function conformanceTest(
    name: string,
    run: (harness: Harness, testCase: ConformanceCase) => Promise<void>,
): void {
    test(name, { skip: environmentSkip }, async () => {
        assert.ok(harness != null);
        assert.ok(cases != null);
        await run(harness, caseNamed(cases, name));
    });
}


conformanceTest("error_contract", async (activeHarness, testCase) => {
    const [request, expected] = decodeErrorContract(testCase);
    let caught: unknown;
    try {
        await activeHarness.unauthenticated.namespaces.retrieve({ namespace_id: request.namespace_id });
    } catch (error) {
        caught = error;
    }

    assert.ok(caught instanceof LoonFS.UnauthorizedError);
    assert.equal(caught.statusCode, expected.unauthenticated.status);
    assert.equal(caught.body.code, expected.unauthenticated.code);
    assert.ok(caught.body.request_id != null);
});

conformanceTest("commit_replay", async (activeHarness, testCase) => {
    const [request, expected] = decodeCommitReplay(testCase);
    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    const commit = directoryCommit(
        request.namespace_id,
        request.commit_id,
        request.actor,
        request.path,
        request.message,
    );
    const first = await activeHarness.client.commits.create(commit);
    const replayed = await activeHarness.client.commits.create(commit);

    assert.equal(first.committed_seq, expected.committed_seq);
    assert.equal(first.commit_id, request.commit_id);
    assert.equal(replayed.committed_seq, first.committed_seq);
    assert.deepEqual(replayed, first);
});

conformanceTest("pagination", async (activeHarness, testCase) => {
    const [request, expected] = decodePagination(testCase);
    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    await activeHarness.client.commits.create(
        directoryCommit(
            request.namespace_id,
            "conf-pagination-directory",
            request.actor,
            request.directory,
        ),
    );
    for (const [index, name] of request.entry_names.entries()) {
        await activeHarness.client.commits.create(
            directoryCommit(
                request.namespace_id,
                `conf-pagination-entry-${index.toString().padStart(2, "0")}`,
                request.actor,
                `${request.directory}/${name}`,
            ),
        );
    }

    const observed: string[] = [];
    let pageCount = 0;
    let savedCursor: string | undefined;
    let resumeOffset: number | undefined;
    let page = await activeHarness.client.files.list({
        namespace_id: request.namespace_id,
        path: request.directory,
        limit: request.page_size,
    });
    let cursor: string | undefined;
    while (true) {
        pageCount += 1;
        assert.equal(page.response.head_seq, expected.head_seq);
        observed.push(...listedNames(page.data));
        cursor = page.response.next_cursor ?? undefined;
        if (pageCount === request.resume_after_page) {
            savedCursor = cursor;
            resumeOffset = observed.length;
        }
        if (cursor === undefined) {
            break;
        }
        await page.getNextPage();
    }

    assert.equal(observed.length, expected.entry_count);
    assert.ok(pageCount >= expected.minimum_page_count);
    assert.equal(cursor, undefined);
    assert.ok(savedCursor !== undefined, "resume cursor was not recorded");
    assert.ok(resumeOffset !== undefined, "resume position was not recorded");

    const resumed: string[] = [];
    page = await activeHarness.client.files.list({
        namespace_id: request.namespace_id,
        path: request.directory,
        limit: request.page_size,
        cursor: savedCursor,
    });
    while (true) {
        resumed.push(...listedNames(page.data));
        cursor = page.response.next_cursor ?? undefined;
        if (cursor === undefined) {
            break;
        }
        await page.getNextPage();
    }

    assert.equal(new Set(observed).size, observed.length, "pagination returned an entry more than once");
    assert.deepEqual(observed, request.entry_names);
    assert.ok(resumeOffset <= request.entry_names.length);
    assert.deepEqual(resumed, request.entry_names.slice(resumeOffset));
});

conformanceTest("children_by_inode", async (activeHarness, testCase) => {
    const [request, expected] = decodeChildrenByInode(testCase);
    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    await activeHarness.client.commits.create(
        directoryCommit(
            request.namespace_id,
            "conf-children-by-inode-directory",
            request.actor,
            request.directory,
        ),
    );
    for (const [index, name] of [...request.entry_names].reverse().entries()) {
        await activeHarness.client.commits.create(
            directoryCommit(
                request.namespace_id,
                `conf-children-by-inode-entry-${index.toString().padStart(2, "0")}`,
                request.actor,
                `${request.directory}/${name}`,
            ),
        );
    }

    const parent = await activeHarness.client.files.retrieve({
        namespace_id: request.namespace_id,
        path: request.directory,
    });
    const parentInodeId = parent.inode_id;
    const observed: string[] = [];
    let pageCount = 0;
    let savedCursor: string | undefined;
    let resumeOffset: number | undefined;
    let page = await activeHarness.client.inodes.listChildren({
        namespace_id: request.namespace_id,
        inode_id: parentInodeId,
        limit: request.page_size,
    });
    let cursor: string | undefined;
    while (true) {
        pageCount += 1;
        assert.equal(page.response.namespace_id, request.namespace_id);
        assert.equal(page.response.parent_inode_id, parentInodeId);
        const expectedHeadSeq =
            pageCount <= request.rename_after_page
                ? expected.initial_head_seq
                : expected.renamed_head_seq;
        assert.equal(page.response.head_seq, expectedHeadSeq);
        observed.push(...listedNames(page.data));
        cursor = page.response.next_cursor ?? undefined;
        if (pageCount === request.resume_after_page) {
            savedCursor = cursor;
            resumeOffset = observed.length;
        }
        if (pageCount === request.rename_after_page) {
            const renamed = await activeHarness.client.commits.create(
                moveCommit(
                    request.namespace_id,
                    request.rename_commit_id,
                    request.actor,
                    request.directory,
                    request.renamed_directory,
                ),
            );
            assert.equal(renamed.committed_seq, expected.renamed_head_seq);
            const renamedParent = await activeHarness.client.files.retrieve({
                namespace_id: request.namespace_id,
                path: request.renamed_directory,
            });
            assert.equal(renamedParent.inode_id, parentInodeId);
        }
        if (cursor === undefined) {
            break;
        }
        await page.getNextPage();
    }

    assert.equal(observed.length, expected.entry_count);
    assert.ok(pageCount >= expected.minimum_page_count);
    assert.ok(savedCursor !== undefined, "resume cursor was not recorded");
    assert.ok(resumeOffset !== undefined, "resume position was not recorded");

    const resumed: string[] = [];
    page = await activeHarness.client.inodes.listChildren({
        namespace_id: request.namespace_id,
        inode_id: parentInodeId,
        limit: request.page_size,
        cursor: savedCursor,
    });
    while (true) {
        assert.equal(page.response.namespace_id, request.namespace_id);
        assert.equal(page.response.parent_inode_id, parentInodeId);
        assert.equal(page.response.head_seq, expected.renamed_head_seq);
        resumed.push(...listedNames(page.data));
        cursor = page.response.next_cursor ?? undefined;
        if (cursor === undefined) {
            break;
        }
        await page.getNextPage();
    }

    assert.equal(
        new Set(observed).size,
        observed.length,
        "children-by-inode pagination returned an entry more than once",
    );
    assert.deepEqual(observed, request.entry_names);
    assert.ok(resumeOffset <= request.entry_names.length);
    assert.deepEqual(resumed, request.entry_names.slice(resumeOffset));
});

conformanceTest("inode_mutations", async (activeHarness, testCase) => {
    const [request, expected] = decodeInodeMutations(testCase);
    const client = activeHarness.client;
    const namespaceId = request.namespace_id;
    const childPath = (name: string): string => `${request.directory}/${name}`;
    await client.namespaces.create({ namespace_id: namespaceId });
    await client.commits.create(
        directoryCommit(
            namespaceId,
            "conf-inode-mutations-directory",
            request.actor,
            request.directory,
        ),
    );
    await client.commits.create(
        directoryCommit(
            namespaceId,
            "conf-inode-mutations-path-directory",
            request.actor,
            childPath(request.path_directory_name),
        ),
    );
    await putFile(client, {
        namespace_id: namespaceId,
        path: childPath(request.path_file_name),
        bytes: new TextEncoder().encode(request.content_utf8),
        actor: request.actor,
        commit_id: "conf-inode-mutations-path-file",
    });

    const parent = await client.files.retrieve({
        namespace_id: namespaceId,
        path: request.directory,
    });
    await client.commits.create({
        namespace_id: namespaceId,
        actor: request.actor,
        commit_id: "conf-inode-mutations-inode-directory",
        operations: [
            {
                kind: "create_directory_by_inode",
                parent_inode_id: parent.inode_id,
                display_name: request.inode_directory_name,
            },
        ],
    });
    let staged = await stageContent(
        client,
        namespaceId,
        new TextEncoder().encode(request.content_utf8),
    );
    await client.commits.create(
        stagedCommit(
            namespaceId,
            "conf-inode-mutations-inode-file",
            request.actor,
            {
                kind: "put_file_by_inode",
                parent_inode_id: parent.inode_id,
                display_name: request.inode_file_name,
                content_ref: staged.content_ref,
            },
            staged,
        ),
    );

    const listing = await client.files.list({
        namespace_id: namespaceId,
        path: request.directory,
    });
    const entries = listing.data;
    assert.deepEqual(listedNames(entries), expected.entry_names);
    const generations = new Set(
        entries.map((entry) => {
            assert.ok(entry.binding_generation != null, "listed entry has no binding_generation");
            return entry.binding_generation;
        }),
    );
    assert.equal(generations.size, entries.length);
    const entryNamed = (name: string): LoonFS.PathEntry => {
        const entry = entries.find((candidate) => candidate.display_name === name);
        assert.ok(entry != null, `listed entry ${name} is missing`);
        return entry;
    };
    assert.equal(
        entryNamed(request.inode_directory_name).inode_kind,
        entryNamed(request.path_directory_name).inode_kind,
    );
    const inodeFile = fileEntry(entryNamed(request.inode_file_name));
    assert.equal(inodeFile.size_bytes, fileEntry(entryNamed(request.path_file_name)).size_bytes);
    assert.equal(inodeFile.parent_inode_id, parent.inode_id);

    staged = await stageContent(
        client,
        namespaceId,
        new TextEncoder().encode(request.revised_content_utf8),
    );
    await client.commits.create(
        stagedCommit(
            namespaceId,
            "conf-inode-mutations-revision",
            request.actor,
            {
                kind: "put_file_revision_by_inode",
                inode_id: inodeFile.inode_id,
                content_ref: staged.content_ref,
                expected_revision_no: inodeFile.revision_no,
            },
            staged,
        ),
    );
    const revised = fileEntry(
        await client.files.retrieve({
            namespace_id: namespaceId,
            path: childPath(request.inode_file_name),
        }),
    );
    assert.equal(revised.revision_no, expected.revised_revision_no);
    assert.deepEqual(
        await readProxied(client, namespaceId, childPath(request.inode_file_name)),
        new TextEncoder().encode(request.revised_content_utf8),
    );

    await client.commits.create(
        moveCommit(
            namespaceId,
            "conf-inode-mutations-rename",
            request.actor,
            childPath(request.inode_file_name),
            childPath(request.renamed_file_name),
        ),
    );
    const moveByInode = (commitId: string, generation: string): LoonFS.CommitRequest => ({
        namespace_id: namespaceId,
        actor: request.actor,
        commit_id: commitId,
        operations: [
            {
                kind: "move_by_inode",
                inode_id: inodeFile.inode_id,
                expected_binding_generation: generation,
                to_parent_inode_id: entryNamed(request.inode_directory_name).inode_id,
                to_display_name: request.moved_file_name,
                behavior: "no_replace",
            },
        ],
    });

    assert.ok(revised.binding_generation != null, "revised entry has no binding_generation");
    await assert.rejects(
        client.commits.create(
            moveByInode("conf-inode-mutations-stale-move", revised.binding_generation),
        ),
        (error: unknown) => {
            assert.ok(error instanceof LoonFS.ConflictError);
            assert.equal(error.statusCode, expected.stale_binding_generation.status);
            assert.equal(error.body.code, expected.stale_binding_generation.code);
            return true;
        },
    );
    await assert.rejects(
        client.commits.create(
            moveByInode(
                "conf-inode-mutations-malformed-move",
                request.malformed_binding_generation,
            ),
        ),
        (error: unknown) => {
            assert.ok(error instanceof LoonFS.BadRequestError);
            assert.equal(error.statusCode, expected.malformed_binding_generation.status);
            assert.equal(error.body.code, expected.malformed_binding_generation.code);
            return true;
        },
    );

    const renamed = await client.files.retrieve({
        namespace_id: namespaceId,
        path: childPath(request.renamed_file_name),
    });
    assert.ok(renamed.binding_generation != null, "renamed entry has no binding_generation");
    const moved = await client.commits.create(
        moveByInode("conf-inode-mutations-move", renamed.binding_generation),
    );
    assert.equal(moved.committed_seq, expected.moved_committed_seq);
    const movedEntry = await client.files.retrieve({
        namespace_id: namespaceId,
        path: `${childPath(request.inode_directory_name)}/${request.moved_file_name}`,
    });
    assert.equal(movedEntry.inode_id, inodeFile.inode_id);
    assert.ok(movedEntry.binding_generation != null, "moved entry has no binding_generation");
    assert.notEqual(movedEntry.binding_generation, renamed.binding_generation);

    const feed = await client.changes.list({
        namespace_id: namespaceId,
        after_seq: expected.moved_committed_seq - 1,
        limit: 1,
    });
    assert.equal(feed.changes.length, 1);
    const events = feed.changes[0]?.events ?? [];
    assert.equal(events.length, 1);
    const movedEvent = events[0];
    assert.ok(movedEvent?.kind === "moved");
    assert.equal(movedEvent.binding_generation, movedEntry.binding_generation);

    const deleted = await client.commits.create({
        namespace_id: namespaceId,
        actor: request.actor,
        commit_id: "conf-inode-mutations-delete",
        operations: [
            {
                kind: "delete_by_inode",
                inode_id: inodeFile.inode_id,
                expected_binding_generation: movedEntry.binding_generation,
                behavior: "non_recursive",
            },
        ],
    });
    assert.equal(deleted.committed_seq, expected.deleted_committed_seq);
});

conformanceTest("snapshots", async (activeHarness, testCase) => {
    const [request, expected] = decodeSnapshots(testCase);
    const client = activeHarness.client;
    const namespaceId = request.namespace_id;
    const childPath = (name: string): string => `${request.directory}/${name}`;
    const capturedBytes = new TextEncoder().encode(request.captured_content_utf8);
    const currentBytes = new TextEncoder().encode(request.current_content_utf8);

    await client.namespaces.create({ namespace_id: namespaceId });
    await client.commits.create(
        directoryCommit(
            namespaceId,
            "conf-snapshots-create-directory",
            request.actor,
            request.directory,
        ),
    );
    await putFile(client, {
        namespace_id: namespaceId,
        path: childPath(request.replaced_file_name),
        bytes: capturedBytes,
        actor: request.actor,
        commit_id: "conf-snapshots-create-replaced",
    });
    await putFile(client, {
        namespace_id: namespaceId,
        path: childPath(request.deleted_file_name),
        bytes: new TextEncoder().encode(request.deleted_content_utf8),
        actor: request.actor,
        commit_id: "conf-snapshots-create-deleted",
    });

    const snapshot = await client.snapshots.create({
        namespace_id: namespaceId,
        name: request.snapshot_name,
        ttl_ms: request.create_ttl_ms,
    });
    assert.equal(snapshot.namespace_id, namespaceId);
    assert.equal(snapshot.name, request.snapshot_name);
    assert.equal(snapshot.head_seq, expected.snapshot_head_seq);
    assert.ok(snapshot.expires_at_ms > snapshot.created_at_ms);

    await putFile(client, {
        namespace_id: namespaceId,
        path: childPath(request.replaced_file_name),
        bytes: currentBytes,
        actor: request.actor,
        commit_id: "conf-snapshots-replace-file",
        behavior: "replace",
    });
    await putFile(client, {
        namespace_id: namespaceId,
        path: childPath(request.added_file_name),
        bytes: new TextEncoder().encode(request.added_content_utf8),
        actor: request.actor,
        commit_id: "conf-snapshots-add-file",
    });
    await client.commits.create(
        deleteCommit(
            namespaceId,
            "conf-snapshots-delete-file",
            request.actor,
            childPath(request.deleted_file_name),
        ),
    );

    const capturedEntry = fileEntry(
        await client.files.retrieve({
            namespace_id: namespaceId,
            path: childPath(request.replaced_file_name),
            snapshot_id: snapshot.snapshot_id,
        }),
    );
    assert.equal(capturedEntry.revision_no, expected.captured_revision_no);
    const currentEntry = fileEntry(
        await client.files.retrieve({
            namespace_id: namespaceId,
            path: childPath(request.replaced_file_name),
        }),
    );
    assert.equal(currentEntry.revision_no, expected.current_revision_no);

    const capturedListing = await client.files.list({
        namespace_id: namespaceId,
        path: request.directory,
        snapshot_id: snapshot.snapshot_id,
    });
    assert.equal(capturedListing.response.head_seq, expected.snapshot_head_seq);
    assert.deepEqual(listedNames(capturedListing.data), expected.captured_entry_names);
    const currentListing = await client.files.list({
        namespace_id: namespaceId,
        path: request.directory,
    });
    assert.deepEqual(listedNames(currentListing.data), expected.current_entry_names);

    assert.deepEqual(
        await readProxied(
            client,
            namespaceId,
            childPath(request.replaced_file_name),
            snapshot.snapshot_id,
        ),
        capturedBytes,
    );
    assert.deepEqual(
        await readProxied(client, namespaceId, childPath(request.replaced_file_name)),
        currentBytes,
    );

    const feed = await client.changes.list({
        namespace_id: namespaceId,
        after_seq: 0,
        limit: 100,
        snapshot_id: snapshot.snapshot_id,
    });
    assert.equal(feed.through_seq, expected.snapshot_head_seq);
    assert.equal(feed.next_after_seq, undefined);
    assert.deepEqual(
        feed.changes.map((change) => change.committed_seq),
        expected.snapshot_change_seqs,
    );

    const extended = await client.snapshots.extend({
        namespace_id: namespaceId,
        snapshot_id: snapshot.snapshot_id,
        ttl_ms: request.extend_ttl_ms,
    });
    assert.equal(extended.snapshot_id, snapshot.snapshot_id);
    assert.equal(extended.head_seq, expected.snapshot_head_seq);
    assert.equal(extended.name, request.snapshot_name);
    assert.ok(extended.expires_at_ms > snapshot.expires_at_ms);

    const listed = await client.snapshots.list({ namespace_id: namespaceId });
    assert.equal(listed.response.namespace_id, namespaceId);
    assert.equal(listed.response.next_cursor, undefined);
    assert.equal(listed.data.length, 1);
    assert.equal(listed.data[0]?.snapshot_id, snapshot.snapshot_id);

    const releaseRequest = {
        namespace_id: namespaceId,
        snapshot_id: snapshot.snapshot_id,
    };
    const firstRelease = await client.snapshots.release(releaseRequest);
    assert.equal(firstRelease.namespace_id, namespaceId);
    assert.equal(firstRelease.snapshot_id, snapshot.snapshot_id);
    const secondRelease = await client.snapshots.release(releaseRequest);
    assert.equal(secondRelease.namespace_id, namespaceId);
    assert.equal(secondRelease.snapshot_id, snapshot.snapshot_id);

    await assert.rejects(
        client.files.retrieve({
            namespace_id: namespaceId,
            path: childPath(request.replaced_file_name),
            snapshot_id: snapshot.snapshot_id,
        }),
        (error: unknown) => {
            assert.ok(error instanceof LoonFS.GoneError);
            assert.equal(error.statusCode, expected.snapshot_gone.status);
            assert.equal(error.body.code, expected.snapshot_gone.code);
            return true;
        },
    );
    await assert.rejects(
        client.snapshots.extend({
            namespace_id: namespaceId,
            snapshot_id: snapshot.snapshot_id,
            ttl_ms: request.extend_ttl_ms,
        }),
        (error: unknown) => {
            assert.ok(error instanceof LoonFS.GoneError);
            assert.equal(error.statusCode, expected.snapshot_gone.status);
            assert.equal(error.body.code, expected.snapshot_gone.code);
            return true;
        },
    );
    await assert.rejects(
        client.files.retrieve({
            namespace_id: namespaceId,
            path: childPath(request.replaced_file_name),
            snapshot_id: request.unknown_snapshot_id,
        }),
        (error: unknown) => {
            assert.ok(error instanceof LoonFS.NotFoundError);
            assert.equal(error.statusCode, expected.snapshot_not_found.status);
            assert.equal(error.body.code, expected.snapshot_not_found.code);
            return true;
        },
    );
    await assert.rejects(
        readProxied(
            client,
            namespaceId,
            childPath(request.replaced_file_name),
            snapshot.snapshot_id,
            expected.captured_revision_no,
        ),
        (error: unknown) => {
            assert.ok(error instanceof LoonFS.BadRequestError);
            assert.equal(error.statusCode, expected.revision_with_snapshot.status);
            assert.equal(error.body.code, expected.revision_with_snapshot.code);
            return true;
        },
    );
    await assert.rejects(
        client.snapshots.create({
            namespace_id: namespaceId,
            name: request.snapshot_name,
            ttl_ms: 0,
        }),
        (error: unknown) => {
            assert.ok(error instanceof LoonFS.BadRequestError);
            assert.equal(error.statusCode, expected.zero_ttl.status);
            assert.equal(error.body.code, expected.zero_ttl.code);
            return true;
        },
    );
});

test("proxy", { skip: environmentSkip }, async (context) => {
    assert.ok(harness != null);
    assert.ok(cases != null);
    const activeHarness = harness;
    const [request, expected] = decodeProxy(caseNamed(cases, "proxy"));
    const proxyHandler = createProxyHandler({
        serverBaseUrl: activeHarness.serverBaseUrl,
        token: activeHarness.token,
        namespaceAliases: { [request.namespace_alias]: request.namespace_id },
    });
    const beginPath = `/v0/namespace-aliases/${encodeURIComponent(request.namespace_alias)}/uploads`;
    const beginModes: string[] = [];
    const handler = async (incoming: Request): Promise<Response> => {
        if (incoming.method === "POST" && new URL(incoming.url).pathname === beginPath) {
            const body = (await incoming.clone().json()) as { mode?: unknown };
            if (typeof body.mode === "string") {
                beginModes.push(body.mode);
            }
        }
        return proxyHandler(incoming);
    };
    const proxy = await startProxyServer(handler);
    context.after(() => proxy.close());

    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    const namespaceAliasBase =
        `${proxy.baseUrl}/v0/namespace-aliases/` +
        encodeURIComponent(request.namespace_alias);
    const payload = new TextEncoder().encode(request.content_utf8);

    const mkdir = await proxyJson<LoonFS.CommitResponse>(
        `${namespaceAliasBase}/commits`,
        {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(
                namespaceAliasDirectoryCommit(
                    request.commit_ids.directory,
                    request.actor,
                    request.directory,
                ),
            ),
        },
        "proxy directory commit",
    );
    assert.equal(mkdir.committed_seq, expected.mkdir_committed_seq);

    const proxiedBegin = await proxyJson<LoonFS.BeginUploadResponse>(
        `${namespaceAliasBase}/uploads`,
        {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ mode: "service_proxied" }),
        },
        "proxy service upload begin",
    );
    assert.equal(proxiedBegin.mode, "service_proxied");
    const contentResponse = await fetchThroughProxy(
        `${namespaceAliasBase}/uploads/${encodeURIComponent(proxiedBegin.upload_id)}/content`,
        {
            method: "PUT",
            headers: { "content-type": "application/octet-stream" },
            body: streamedBytes(payload),
            duplex: "half",
        },
    );
    await requireSuccessfulResponse(contentResponse, "proxy content upload");
    const uploadedContent = (await contentResponse.json()) as LoonFS.UploadContentResponse;
    assert.equal(uploadedContent.content_ref.size_bytes, payload.byteLength);
    const proxiedCompleted = completedUpload(
        await proxyJson<LoonFS.UploadSession>(
            `${namespaceAliasBase}/uploads/${encodeURIComponent(proxiedBegin.upload_id)}/complete`,
            {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ mode: "service_proxied" }),
            },
            "proxy service upload completion",
        ),
    );
    assert.deepEqual(proxiedCompleted.content_ref, uploadedContent.content_ref);
    const proxiedCommit = await proxyJson<LoonFS.CommitResponse>(
        `${namespaceAliasBase}/commits`,
        {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(
                namespaceAliasFileCommit(
                    request.commit_ids.proxied,
                    request.actor,
                    request.proxied_path,
                    proxiedCompleted,
                ),
            ),
        },
        "proxy service upload commit",
    );
    assert.equal(proxiedCommit.committed_seq, expected.proxied_committed_seq);

    const directBegin = await proxyJson<LoonFS.BeginUploadResponse>(
        `${namespaceAliasBase}/uploads`,
        {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ mode: "direct_put", size_bytes: payload.byteLength }),
        },
        "proxy direct upload begin",
    );
    assert.equal(directBegin.mode, "direct_put");
    await uploadPresigned(directBegin.access, payload, "proxy direct PUT");
    const directClaim: LoonFS.UploadContentClaim = {
        size_bytes: payload.byteLength,
        checksum: checksum(directBegin.checksum_algorithm, payload),
    };
    const directCompleted = completedUpload(
        await proxyJson<LoonFS.UploadSession>(
            `${namespaceAliasBase}/uploads/${encodeURIComponent(directBegin.upload_id)}/complete`,
            {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ mode: "direct_put", content: directClaim }),
            },
            "proxy direct upload completion",
        ),
    );
    assert.deepEqual(directCompleted.content_ref.checksum, directClaim.checksum);
    const directCommit = await proxyJson<LoonFS.CommitResponse>(
        `${namespaceAliasBase}/commits`,
        {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(
                namespaceAliasFileCommit(
                    request.commit_ids.direct,
                    request.actor,
                    request.direct_path,
                    directCompleted,
                ),
            ),
        },
        "proxy direct upload commit",
    );
    assert.equal(directCommit.committed_seq, expected.direct_committed_seq);

    const listUrl = new URL(`${namespaceAliasBase}/filesystem/entries`);
    listUrl.searchParams.set("path", request.directory);
    const listing = await proxyJson<LoonFS.ListPathEntriesResponse>(
        listUrl,
        { method: "GET" },
        "proxy directory listing",
    );
    assert.equal(listing.entries.length, expected.entry_count);

    const readUrl = new URL(`${namespaceAliasBase}/filesystem/content`);
    readUrl.searchParams.set("path", request.proxied_path);
    const read = await fetchThroughProxy(readUrl);
    await requireSuccessfulResponse(read, "proxy file read");
    assert.deepEqual(new Uint8Array(await read.arrayBuffer()), payload);

    const unknownNamespaceAliasUrl = new URL(
        `/v0/namespace-aliases/${encodeURIComponent(request.unknown_namespace_alias)}/filesystem/entries`,
        proxy.baseUrl,
    );
    unknownNamespaceAliasUrl.searchParams.set("path", request.directory);
    const unknownNamespaceAlias = await fetchThroughProxy(unknownNamespaceAliasUrl);
    assert.equal(unknownNamespaceAlias.status, expected.unknown_namespace_alias_status);
    assert.equal((await unknownNamespaceAlias.arrayBuffer()).byteLength, 0);

    const disallowedRoute = await fetchThroughProxy(
        `${namespaceAliasBase}${request.disallowed_path_suffix}`,
    );
    assert.equal(disallowedRoute.status, expected.disallowed_route_status);
    assert.equal((await disallowedRoute.arrayBuffer()).byteLength, 0);

    beginModes.length = 0;
    const browserClient = new BrowserLoonFSClient({ environment: proxy.baseUrl });
    const browserPath = `${request.proxied_path}-browser`;
    await assertBrowserTransfer(
        browserClient,
        request.namespace_alias,
        browserPath,
        payload,
        request.actor,
        `${request.commit_ids.proxied}-browser`,
        "browser service-proxied transfer",
    );
    assert.deepEqual(beginModes, ["service_proxied"]);

    const capabilities = await browserClient.capabilities.retrieve();
    const proxyUploadMaxBytes = capabilities.limits?.[PROXY_UPLOAD_MAX_BYTES];
    assert.ok(proxyUploadMaxBytes !== undefined, "browser proxy upload limit is not advertised");
    assert.ok(Number.isSafeInteger(proxyUploadMaxBytes) && proxyUploadMaxBytes >= 0);
    const directPutLength = proxyUploadMaxBytes + 1;
    assert.ok(directPutLength < BROWSER_MULTIPART_MIN_BYTES);
    const directPutBytes = bytePattern({ length: directPutLength, modulus: 251 });
    await assertBrowserTransfer(
        browserClient,
        request.namespace_alias,
        `${request.direct_path}-browser`,
        directPutBytes,
        request.actor,
        `${request.commit_ids.direct}-browser`,
        "browser direct-PUT transfer",
    );
    assert.deepEqual(beginModes, ["service_proxied", "direct_put"]);

    const multipartBytes = bytePattern({
        length: BROWSER_MULTIPART_MIN_BYTES + 1,
        modulus: 251,
    });
    await assertBrowserTransfer(
        browserClient,
        request.namespace_alias,
        `${request.direct_path}-browser-multipart`,
        multipartBytes,
        request.actor,
        `${request.commit_ids.direct}-browser-multipart`,
        "browser multipart transfer",
    );
    assert.deepEqual(beginModes, ["service_proxied", "direct_put", "direct_multipart"]);

    // The rig fails only at begin. No session exists then, so mid-flow cleanup is not covered.
    await assert.rejects(
        putBrowserFile(browserClient, {
            namespace_alias: request.unknown_namespace_alias,
            path: `${request.proxied_path}-browser-failure`,
            bytes: payload,
            actor: request.actor,
            commit_id: `${request.commit_ids.proxied}-browser-failure`,
        }),
        (error: unknown) => {
            assert.ok(error instanceof BrowserLoonFS.NotFoundError);
            assert.equal(error.statusCode, expected.unknown_namespace_alias_status);
            return true;
        },
    );
});

conformanceTest("changes", async (activeHarness, testCase) => {
    const [request, expected] = decodeChanges(testCase);
    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    const committed = await activeHarness.client.commits.create(
        directoryCommit(request.namespace_id, request.commit_id, request.actor, request.path),
    );
    assert.equal(committed.committed_seq, expected.committed_seq);

    const feed = await activeHarness.client.changes.list({
        namespace_id: request.namespace_id,
        after_seq: request.after_seq,
    });
    assert.equal(feed.changes.length, expected.change_count);
    assert.ok(feed.changes.length > 0, "change feed is empty");
    const change = feed.changes[0];
    assert.equal(change.commit_id, request.commit_id);
    assert.deepEqual(change.committed_by, request.actor);
    assert.equal(change.events.length, 1);
    assert.equal(change.events[0]?.kind, "directory_created");
});

conformanceTest("upload_direct_put", async (activeHarness, testCase) => {
    const [request, expected] = decodeDirectPut(testCase);
    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    const payload = new TextEncoder().encode(request.content_utf8);
    const begin = await activeHarness.client.uploads.create({
        namespace_id: request.namespace_id,
        body: { mode: "direct_put", size_bytes: payload.byteLength },
    });
    assert.equal(begin.mode, expected.mode);
    const directPut = begin as LoonFS.BeginUploadResponse.DirectPut;
    assert.equal(directPut.checksum_algorithm, expected.checksum_algorithm);

    await uploadPresigned(directPut.access, payload, "direct PUT");
    const claim: LoonFS.UploadContentClaim = {
        size_bytes: payload.byteLength,
        checksum: checksum(directPut.checksum_algorithm, payload),
    };
    const completed = completedUpload(
        await activeHarness.client.uploads.complete({
            namespace_id: request.namespace_id,
            upload_id: begin.upload_id,
            body: { mode: "direct_put", content: claim },
        }),
    );
    assert.equal(completed.content_ref.size_bytes, expected.size_bytes);
    assert.equal(completed.content_ref.checksum.algorithm, expected.checksum_algorithm);
    assert.deepEqual(completed.content_ref.checksum, claim.checksum);
    assert.deepEqual(
        completed.content_ref.checksum,
        checksum(completed.content_ref.checksum.algorithm, payload),
    );

    const committed = await activeHarness.client.commits.create(
        fileCommit(
            request.namespace_id,
            request.commit_id,
            request.actor,
            request.path,
            completed.content_ref,
            completed.content_token,
        ),
    );
    assert.equal(committed.committed_seq, expected.committed_seq);
    const stat = fileEntry(
        await activeHarness.client.files.retrieve({
            namespace_id: request.namespace_id,
            path: request.path,
        }),
    );
    assert.deepEqual(stat.content_ref, completed.content_ref);
    assert.deepEqual(
        await readProxied(activeHarness.client, request.namespace_id, request.path),
        payload,
    );
});

conformanceTest("upload_multipart", async (activeHarness, testCase) => {
    const [request, expected] = decodeMultipart(testCase);
    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    const payload = bytePattern(request.content_pattern);
    const begin = await activeHarness.client.uploads.create({
        namespace_id: request.namespace_id,
        body: {
            mode: "direct_multipart",
            part_size_bytes: request.part_size_bytes,
        },
    });
    assert.equal(begin.mode, expected.mode);
    const multipart = begin as LoonFS.BeginUploadResponse.DirectMultipart;
    assert.equal(multipart.part_size_bytes, request.part_size_bytes);
    assert.equal(multipart.checksum_algorithm, expected.checksum_algorithm);

    const chunks = splitBytes(payload, multipart.part_size_bytes);
    assert.equal(chunks.length, expected.part_count);
    const claims: LoonFS.UploadPartChecksumClaim[] = chunks.map((chunk, index) => ({
        part_number: index + 1,
        checksum: checksum(multipart.checksum_algorithm, chunk),
    }));
    const signed = await activeHarness.client.uploads.signParts({
        namespace_id: request.namespace_id,
        upload_id: begin.upload_id,
        parts: claims,
    });
    assert.equal(signed.parts.length, expected.part_count);

    const completedParts: LoonFS.CompletedUploadPart[] = [];
    for (const signedPart of signed.parts) {
        const index = signedPart.part_number - 1;
        const chunk = chunks[index];
        const claim = claims[index];
        assert.ok(chunk !== undefined, `signed part ${signedPart.part_number} has no payload`);
        assert.ok(claim !== undefined, `signed part ${signedPart.part_number} has no claim`);
        const response = await uploadPresigned(
            signedPart.access,
            chunk,
            `multipart part ${signedPart.part_number}`,
        );
        const etag = response.headers.get("etag");
        assert.ok(etag !== null, `multipart part ${signedPart.part_number} returned no etag`);
        completedParts.push({
            part_number: signedPart.part_number,
            checksum: claim.checksum,
            etag,
        });
    }
    completedParts.sort((left, right) => left.part_number - right.part_number);
    const wholeChecksum = checksum(multipart.checksum_algorithm, payload);
    const completion: LoonFS.CompleteUploadRequest = {
        mode: "direct_multipart",
        content: {
            size_bytes: payload.byteLength,
            checksum: wholeChecksum,
        },
        parts: completedParts,
    };
    const first = completedUpload(
        await activeHarness.client.uploads.complete({
            namespace_id: request.namespace_id,
            upload_id: begin.upload_id,
            body: completion,
        }),
    );
    const replayed = completedUpload(
        await activeHarness.client.uploads.complete({
            namespace_id: request.namespace_id,
            upload_id: begin.upload_id,
            body: completion,
        }),
    );
    assert.equal(replayed.namespace_id, first.namespace_id);
    assert.equal(replayed.upload_id, first.upload_id);
    assert.equal(replayed.mode, first.mode);
    assert.deepEqual(replayed.content_ref, first.content_ref);
    assert.equal(replayed.completed_at_ms, first.completed_at_ms);
    assert.equal(first.content_ref.size_bytes, expected.size_bytes);
    assert.deepEqual(first.content_ref.checksum, wholeChecksum);
    assert.deepEqual(checksum(first.content_ref.checksum.algorithm, payload), wholeChecksum);

    const committed = await activeHarness.client.commits.create(
        fileCommit(
            request.namespace_id,
            request.commit_id,
            request.actor,
            request.path,
            first.content_ref,
            replayed.content_token,
        ),
    );
    assert.equal(committed.committed_seq, expected.committed_seq);
    assert.deepEqual(
        await readProxied(activeHarness.client, request.namespace_id, request.path),
        payload,
    );

    // The same content through the high-level helper: the payload exceeds the
    // part size, so this exercises putFile's multipart branch.
    const helperPath = `${request.path}-helper`;
    const helperCommit = await putFile(activeHarness.client, {
        namespace_id: request.namespace_id,
        path: helperPath,
        bytes: payload,
        actor: request.actor,
        commit_id: `${request.commit_id}-helper`,
    });
    assert.ok(helperCommit.committed_seq > 0, "helper multipart put reported no committed_seq");
    const helperRead = await getFile(activeHarness.client, {
        namespace_id: request.namespace_id,
        path: helperPath,
    });
    assert.deepEqual(helperRead.bytes, payload);
    // Content ids are random per upload and the helper may choose a different
    // checksum algorithm; the comparable content fact is the size.
    assert.equal(helperRead.content_ref.size_bytes, first.content_ref.size_bytes);
});

conformanceTest("upload_abort", async (activeHarness, testCase) => {
    const [request, expected] = decodeAbort(testCase);
    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    const begin = await activeHarness.client.uploads.create({
        namespace_id: request.namespace_id,
        body: { mode: "service_proxied" },
    });
    const first = abortedUpload(
        await activeHarness.client.uploads.abort({
            namespace_id: request.namespace_id,
            upload_id: begin.upload_id,
        }),
    );
    const replayed = abortedUpload(
        await activeHarness.client.uploads.abort({
            namespace_id: request.namespace_id,
            upload_id: begin.upload_id,
        }),
    );
    assert.equal(first.mode, expected.mode);
    assert.equal(first.status, expected.status);
    assert.deepEqual(replayed, first);
    assert.equal(replayed.aborted_at_ms, first.aborted_at_ms);
});

conformanceTest("download", async (activeHarness, testCase) => {
    const [request, expected] = decodeDownload(testCase);
    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    const payload = new TextEncoder().encode(request.content_utf8);
    const committed = await putFile(activeHarness.client, {
        namespace_id: request.namespace_id,
        path: request.path,
        bytes: payload,
        actor: request.actor,
        commit_id: request.commit_id,
    });
    assert.equal(committed.committed_seq, expected.committed_seq);
    const stat = fileEntry(
        await activeHarness.client.files.retrieve({
            namespace_id: request.namespace_id,
            path: request.path,
        }),
    );
    const download = await getFile(activeHarness.client, {
        namespace_id: request.namespace_id,
        path: request.path,
    });
    assert.deepEqual(stat.content_ref, download.content_ref);
    assert.equal(download.content_ref.size_bytes, expected.size_bytes);
    assert.equal(download.content_ref.checksum.algorithm, expected.checksum_algorithm);
    assert.equal(download.bytes.byteLength, download.content_ref.size_bytes);
    assert.deepEqual(
        checksum(download.content_ref.checksum.algorithm, download.bytes),
        download.content_ref.checksum,
    );
    assert.deepEqual(download.bytes, payload);
});

conformanceTest("end_to_end", async (activeHarness, testCase) => {
    const [request, expected] = decodeEndToEnd(testCase);
    await activeHarness.client.namespaces.create({ namespace_id: request.namespace_id });
    const mkdir = await activeHarness.client.commits.create(
        directoryCommit(
            request.namespace_id,
            request.commit_ids.mkdir,
            request.actor,
            request.directory,
        ),
    );
    assert.equal(mkdir.committed_seq, expected.mkdir_committed_seq);

    const payload = new TextEncoder().encode(request.content_utf8);
    const upload = await putFile(activeHarness.client, {
        namespace_id: request.namespace_id,
        path: request.upload_path,
        bytes: payload,
        actor: request.actor,
        commit_id: request.commit_ids.upload,
    });
    assert.equal(upload.committed_seq, expected.upload_committed_seq);
    const stat = fileEntry(
        await activeHarness.client.files.retrieve({
            namespace_id: request.namespace_id,
            path: request.upload_path,
        }),
    );
    assert.equal(stat.size_bytes, expected.size_bytes);
    const uploadedInode = stat.inode_id;

    const initialListing = await activeHarness.client.files.list({
        namespace_id: request.namespace_id,
        path: request.directory,
    });
    assert.ok(initialListing.data.some((entry) => entry.path === request.upload_path));

    const downloaded = await getFile(activeHarness.client, {
        namespace_id: request.namespace_id,
        path: request.upload_path,
    });
    assert.deepEqual(downloaded.bytes, payload);

    const moved = await activeHarness.client.commits.create(
        moveCommit(
            request.namespace_id,
            request.commit_ids.move,
            request.actor,
            request.upload_path,
            request.moved_path,
        ),
    );
    assert.equal(moved.committed_seq, expected.move_committed_seq);
    const movedListing = await activeHarness.client.files.list({
        namespace_id: request.namespace_id,
        path: request.directory,
    });
    assert.ok(movedListing.data.some((entry) => entry.path === request.moved_path));

    const revisions = await activeHarness.client.files.listRevisions({
        namespace_id: request.namespace_id,
        path: request.moved_path,
    });
    assert.equal(revisions.data.length, expected.revision_count);
    assert.equal(revisions.data[0]?.commit_id, request.commit_ids.upload);

    let changes = await activeHarness.client.changes.list({
        namespace_id: request.namespace_id,
        after_seq: 0,
    });
    assert.equal(changes.changes.length, expected.change_count - 1);
    const removed = await activeHarness.client.commits.create(
        deleteCommit(
            request.namespace_id,
            request.commit_ids.remove,
            request.actor,
            request.moved_path,
        ),
    );
    assert.equal(removed.committed_seq, expected.remove_committed_seq);

    changes = await activeHarness.client.changes.list({
        namespace_id: request.namespace_id,
        after_seq: 0,
    });
    assert.equal(changes.changes.length, expected.change_count);
    assert.deepEqual(
        changes.changes.map((change) => change.commit_id),
        [
            request.commit_ids.mkdir,
            request.commit_ids.upload,
            request.commit_ids.move,
            request.commit_ids.remove,
        ],
    );
    for (const change of changes.changes) {
        assert.deepEqual(change.committed_by, request.actor);
    }

    const trash = await activeHarness.client.trash.list({
        namespace_id: request.namespace_id,
    });
    const removedEntry = trash.data.find((entry) => entry.inode_id === uploadedInode);
    assert.ok(removedEntry !== undefined, "removed inode is missing from trash");
    assert.equal(removedEntry.deletion_seq, removed.committed_seq);
});

// Every proxy route must reach the server.
// Every excluded server route must stop at the proxy.
test("proxy forwards every documented route", { skip: environmentSkip }, async (context) => {
    assert.ok(cases != null);
    const [fixture] = decodeProxy(caseNamed(cases, "proxy"));
    const documentPath = process.env.LOONFS_PROXY_DOCUMENT;
    assert.ok(documentPath, "LOONFS_PROXY_DOCUMENT is not set");
    const proxyDocument = JSON.parse(readFileSync(documentPath, "utf8")) as {
        paths: Record<string, Record<string, unknown>>;
    };
    const serverDocumentPath = process.env.LOONFS_SERVER_DOCUMENT;
    assert.ok(serverDocumentPath, "LOONFS_SERVER_DOCUMENT is not set");
    const serverDocument = JSON.parse(readFileSync(serverDocumentPath, "utf8")) as {
        paths: Record<string, Record<string, unknown>>;
    };
    const proxyRoutes = new Set<string>();
    for (const [template, item] of Object.entries(proxyDocument.paths)) {
        for (const documentedMethod of Object.keys(item)) {
            proxyRoutes.add(`${documentedMethod.toUpperCase()} ${template}`);
        }
    }

    const stub = await startRecordingServer();
    context.after(() => stub.close());
    const handler = createProxyHandler({
        serverBaseUrl: stub.baseUrl,
        token: "recording-stub-token",
        namespaceAliases: { [fixture.namespace_alias]: fixture.namespace_id },
    });
    const proxy = await startProxyServer(handler);
    context.after(() => proxy.close());

    const instantiate = (template: string): string =>
        template.replace(/\{([^/{}]+)\}/g, (_placeholder, name: string) =>
            name === "namespace_alias" ? fixture.namespace_alias : "x",
        );
    const proxyTemplateForServer = (template: string): string => {
        const serverNamespacePrefix = "/v0/namespaces/{namespace_id}";
        if (
            template === serverNamespacePrefix ||
            template.startsWith(`${serverNamespacePrefix}/`)
        ) {
            return template.replace(
                serverNamespacePrefix,
                "/v0/namespace-aliases/{namespace_alias}",
            );
        }
        return template;
    };

    const expected: string[] = [];
    for (const [template, item] of Object.entries(proxyDocument.paths)) {
        for (const documentedMethod of Object.keys(item)) {
            const method = documentedMethod.toUpperCase();
            const path = instantiate(template);
            const forwardedTemplate = template.replace(
                "/v0/namespace-aliases/{namespace_alias}",
                `/v0/namespaces/${fixture.namespace_id}`,
            );
            expected.push(`${method} ${instantiate(forwardedTemplate)}`);
            const response = await fetchThroughProxy(`${proxy.baseUrl}${path}`, { method });
            assert.equal(response.status, 200);
            await response.arrayBuffer();
        }
    }
    assert.deepEqual([...stub.requests].sort(), expected.sort());

    const observedBefore = [...stub.requests];
    for (const [serverTemplate, item] of Object.entries(serverDocument.paths)) {
        const proxyTemplate = proxyTemplateForServer(serverTemplate);
        for (const documentedMethod of Object.keys(item)) {
            const method = documentedMethod.toUpperCase();
            if (proxyRoutes.has(`${method} ${proxyTemplate}`)) {
                continue;
            }
            const path = instantiate(proxyTemplate);
            const response = await fetchThroughProxy(`${proxy.baseUrl}${path}`, { method });
            assert.equal(response.status, 404, `${method} ${path}`);
            await response.arrayBuffer();
        }
    }
    assert.deepEqual(stub.requests, observedBefore);
});
