import * as assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { basename, extname, join } from "node:path";
import { test } from "node:test";

import { LoonFS, LoonFSClient } from "../../../generated/typescript/index.js";
import { getFile, putFile } from "../../../generated/typescript/transfers.js";


const FIXTURE_VERSION = 1;
const RUNNER_SKIP = "run scripts/run-sdk-conformance.sh typescript";
const CRC32C_POLYNOMIAL = 0x82f63b78;
const CRC64_NVME_POLYNOMIAL = 0x9a6c9329ac4bc9b5n;
const CRC64_MASK = 0xffffffffffffffffn;
const CASE_FIELDS = ["expected", "family", "intent", "name", "request", "version"];
const EXPECTED_CASES = [
    ["changes", "changes"],
    ["commit_replay", "commit_replay"],
    ["download", "download"],
    ["end_to_end", "end_to_end"],
    ["error_contract", "error_contract"],
    ["pagination", "pagination"],
    ["upload_abort", "upload_abort"],
    ["upload_direct_put", "upload_direct_put"],
    ["upload_multipart", "upload_multipart"],
] as const;
const CRC32C_TABLE = makeCrc32cTable();
const CRC64_NVME_TABLE = makeCrc64NvmeTable();
type JsonObject = Record<string, unknown>;
type ActorKind = "user" | "service" | "system";

interface ActorValue {
    id: string;
    kind: ActorKind;
}

interface ConformanceCase {
    name: string;
    family: string;
    request: JsonObject;
    expected: JsonObject;
}

interface ErrorStatusExpected {
    status: number;
    code: string;
}

interface ErrorOutcome {
    status: number;
    code: string;
    param: string;
}

interface ErrorContractRequest {
    namespaceId: string;
    malformedBody: JsonObject;
    invalidAfterSeq: string;
}

interface ErrorContractExpected {
    unauthenticated: ErrorStatusExpected;
    malformedBody: ErrorOutcome;
    invalidQuery: ErrorOutcome;
}

interface CommitReplayRequest {
    namespaceId: string;
    commitId: string;
    actor: ActorValue;
    message: string;
    path: string;
}

interface CommitReplayExpected {
    committedSeq: number;
}

interface PaginationRequest {
    namespaceId: string;
    directory: string;
    actor: ActorValue;
    entryNames: string[];
    pageSize: number;
    resumeAfterPage: number;
}

interface PaginationExpected {
    entryCount: number;
    minimumPageCount: number;
    headSeq: number;
}

interface ChangesRequest {
    namespaceId: string;
    path: string;
    commitId: string;
    actor: ActorValue;
    afterSeq: number;
}

interface ChangesExpected {
    committedSeq: number;
    changeCount: number;
    eventKind: string;
}

interface DirectPutRequest {
    namespaceId: string;
    path: string;
    commitId: string;
    actor: ActorValue;
    contentUtf8: string;
}

interface DirectPutExpected {
    mode: string;
    sizeBytes: number;
    checksumAlgorithm: string;
    committedSeq: number;
}

interface BytePattern {
    length: number;
    modulus: number;
}

interface MultipartRequest {
    namespaceId: string;
    path: string;
    commitId: string;
    actor: ActorValue;
    partSizeBytes: number;
    contentPattern: BytePattern;
}

interface MultipartExpected {
    mode: string;
    partCount: number;
    sizeBytes: number;
    checksumAlgorithm: string;
    committedSeq: number;
}

interface AbortRequest {
    namespaceId: string;
}

interface AbortExpected {
    mode: string;
    status: string;
}

interface DownloadRequest {
    namespaceId: string;
    path: string;
    commitId: string;
    actor: ActorValue;
    contentUtf8: string;
}

interface DownloadExpected {
    sizeBytes: number;
    checksumAlgorithm: string;
    committedSeq: number;
}

interface EndToEndCommitIds {
    mkdir: string;
    upload: string;
    move: string;
    remove: string;
}

interface EndToEndRequest {
    namespaceId: string;
    directory: string;
    uploadPath: string;
    movedPath: string;
    actor: ActorValue;
    contentUtf8: string;
    commitIds: EndToEndCommitIds;
}

interface EndToEndExpected {
    mkdirCommittedSeq: number;
    uploadCommittedSeq: number;
    moveCommittedSeq: number;
    removeCommittedSeq: number;
    sizeBytes: number;
    revisionCount: number;
    changeCount: number;
}

interface Harness {
    client: LoonFSClient;
    unauthenticated: LoonFSClient;
}

type CompletedUpload = LoonFS.UploadSessionResponse & LoonFS.UploadSessionStatus.Completed;
type AbortedUpload = LoonFS.UploadSessionResponse & LoonFS.UploadSessionStatus.Aborted;
type FileEntry = LoonFS.AuthoritativePathEntry &
    LoonFS.AuthoritativePathEntryFile & { kind: "file" };


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

function stringValue(value: unknown, label: string): string {
    if (typeof value !== "string") {
        throw new Error(`${label} must be a string`);
    }
    return value;
}

function integerValue(value: unknown, label: string): number {
    if (typeof value !== "number" || !Number.isSafeInteger(value)) {
        throw new Error(`${label} must be an integer`);
    }
    return value;
}

function byteValue(value: unknown, label: string): number {
    const byte = integerValue(value, label);
    if (byte < 0 || byte > 255) {
        throw new Error(`${label} must fit in one byte`);
    }
    return byte;
}

function stringArray(value: unknown, label: string): string[] {
    if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
        throw new Error(`${label} must be an array of strings`);
    }
    return value;
}

function actorValue(value: unknown, label: string): ActorValue {
    const actor = strictObject(value, ["id", "kind"], label);
    const kind = stringValue(actor.kind, `${label}.kind`);
    if (kind !== "user" && kind !== "service" && kind !== "system") {
        throw new Error(`${label}.kind is not a known actor kind`);
    }
    return {
        id: stringValue(actor.id, `${label}.id`),
        kind,
    };
}

function errorStatus(value: unknown, label: string): ErrorStatusExpected {
    const data = strictObject(value, ["status", "code"], label);
    return {
        status: integerValue(data.status, `${label}.status`),
        code: stringValue(data.code, `${label}.code`),
    };
}

function errorOutcome(value: unknown, label: string): ErrorOutcome {
    const data = strictObject(value, ["status", "code", "param"], label);
    return {
        status: integerValue(data.status, `${label}.status`),
        code: stringValue(data.code, `${label}.code`),
        param: stringValue(data.param, `${label}.param`),
    };
}

function decodeErrorContract(
    testCase: ConformanceCase,
): [ErrorContractRequest, ErrorContractExpected] {
    const request = strictObject(
        testCase.request,
        ["namespace_id", "malformed_body", "invalid_after_seq"],
        `${testCase.name} request`,
    );
    const expected = strictObject(
        testCase.expected,
        ["unauthenticated", "malformed_body", "invalid_query"],
        `${testCase.name} expected`,
    );
    return [
        {
            namespaceId: stringValue(request.namespace_id, "error_contract request.namespace_id"),
            malformedBody: jsonObject(request.malformed_body, "error_contract request.malformed_body"),
            invalidAfterSeq: stringValue(
                request.invalid_after_seq,
                "error_contract request.invalid_after_seq",
            ),
        },
        {
            unauthenticated: errorStatus(
                expected.unauthenticated,
                "error_contract expected.unauthenticated",
            ),
            malformedBody: errorOutcome(
                expected.malformed_body,
                "error_contract expected.malformed_body",
            ),
            invalidQuery: errorOutcome(
                expected.invalid_query,
                "error_contract expected.invalid_query",
            ),
        },
    ];
}

function decodeCommitReplay(
    testCase: ConformanceCase,
): [CommitReplayRequest, CommitReplayExpected] {
    const request = strictObject(
        testCase.request,
        ["namespace_id", "commit_id", "actor", "message", "path"],
        `${testCase.name} request`,
    );
    const expected = strictObject(testCase.expected, ["committed_seq"], `${testCase.name} expected`);
    return [
        {
            namespaceId: stringValue(request.namespace_id, "commit_replay request.namespace_id"),
            commitId: stringValue(request.commit_id, "commit_replay request.commit_id"),
            actor: actorValue(request.actor, "commit_replay request.actor"),
            message: stringValue(request.message, "commit_replay request.message"),
            path: stringValue(request.path, "commit_replay request.path"),
        },
        {
            committedSeq: integerValue(expected.committed_seq, "commit_replay expected.committed_seq"),
        },
    ];
}

function decodePagination(
    testCase: ConformanceCase,
): [PaginationRequest, PaginationExpected] {
    const request = strictObject(
        testCase.request,
        ["namespace_id", "directory", "actor", "entry_names", "page_size", "resume_after_page"],
        `${testCase.name} request`,
    );
    const expected = strictObject(
        testCase.expected,
        ["entry_count", "minimum_page_count", "head_seq"],
        `${testCase.name} expected`,
    );
    return [
        {
            namespaceId: stringValue(request.namespace_id, "pagination request.namespace_id"),
            directory: stringValue(request.directory, "pagination request.directory"),
            actor: actorValue(request.actor, "pagination request.actor"),
            entryNames: stringArray(request.entry_names, "pagination request.entry_names"),
            pageSize: integerValue(request.page_size, "pagination request.page_size"),
            resumeAfterPage: integerValue(
                request.resume_after_page,
                "pagination request.resume_after_page",
            ),
        },
        {
            entryCount: integerValue(expected.entry_count, "pagination expected.entry_count"),
            minimumPageCount: integerValue(
                expected.minimum_page_count,
                "pagination expected.minimum_page_count",
            ),
            headSeq: integerValue(expected.head_seq, "pagination expected.head_seq"),
        },
    ];
}

function decodeChanges(testCase: ConformanceCase): [ChangesRequest, ChangesExpected] {
    const request = strictObject(
        testCase.request,
        ["namespace_id", "path", "commit_id", "actor", "after_seq"],
        `${testCase.name} request`,
    );
    const expected = strictObject(
        testCase.expected,
        ["committed_seq", "change_count", "event_kind"],
        `${testCase.name} expected`,
    );
    return [
        {
            namespaceId: stringValue(request.namespace_id, "changes request.namespace_id"),
            path: stringValue(request.path, "changes request.path"),
            commitId: stringValue(request.commit_id, "changes request.commit_id"),
            actor: actorValue(request.actor, "changes request.actor"),
            afterSeq: integerValue(request.after_seq, "changes request.after_seq"),
        },
        {
            committedSeq: integerValue(expected.committed_seq, "changes expected.committed_seq"),
            changeCount: integerValue(expected.change_count, "changes expected.change_count"),
            eventKind: stringValue(expected.event_kind, "changes expected.event_kind"),
        },
    ];
}

function decodeDirectPut(testCase: ConformanceCase): [DirectPutRequest, DirectPutExpected] {
    const request = strictObject(
        testCase.request,
        ["namespace_id", "path", "commit_id", "actor", "content_utf8"],
        `${testCase.name} request`,
    );
    const expected = strictObject(
        testCase.expected,
        ["mode", "size_bytes", "checksum_algorithm", "committed_seq"],
        `${testCase.name} expected`,
    );
    return [
        {
            namespaceId: stringValue(request.namespace_id, "upload_direct_put request.namespace_id"),
            path: stringValue(request.path, "upload_direct_put request.path"),
            commitId: stringValue(request.commit_id, "upload_direct_put request.commit_id"),
            actor: actorValue(request.actor, "upload_direct_put request.actor"),
            contentUtf8: stringValue(request.content_utf8, "upload_direct_put request.content_utf8"),
        },
        {
            mode: stringValue(expected.mode, "upload_direct_put expected.mode"),
            sizeBytes: integerValue(expected.size_bytes, "upload_direct_put expected.size_bytes"),
            checksumAlgorithm: stringValue(
                expected.checksum_algorithm,
                "upload_direct_put expected.checksum_algorithm",
            ),
            committedSeq: integerValue(
                expected.committed_seq,
                "upload_direct_put expected.committed_seq",
            ),
        },
    ];
}

function bytePatternValue(value: unknown, label: string): BytePattern {
    const pattern = strictObject(value, ["length", "modulus"], label);
    return {
        length: integerValue(pattern.length, `${label}.length`),
        modulus: byteValue(pattern.modulus, `${label}.modulus`),
    };
}

function decodeMultipart(testCase: ConformanceCase): [MultipartRequest, MultipartExpected] {
    const request = strictObject(
        testCase.request,
        ["namespace_id", "path", "commit_id", "actor", "part_size_bytes", "content_pattern"],
        `${testCase.name} request`,
    );
    const expected = strictObject(
        testCase.expected,
        ["mode", "part_count", "size_bytes", "checksum_algorithm", "committed_seq"],
        `${testCase.name} expected`,
    );
    return [
        {
            namespaceId: stringValue(request.namespace_id, "upload_multipart request.namespace_id"),
            path: stringValue(request.path, "upload_multipart request.path"),
            commitId: stringValue(request.commit_id, "upload_multipart request.commit_id"),
            actor: actorValue(request.actor, "upload_multipart request.actor"),
            partSizeBytes: integerValue(
                request.part_size_bytes,
                "upload_multipart request.part_size_bytes",
            ),
            contentPattern: bytePatternValue(
                request.content_pattern,
                "upload_multipart request.content_pattern",
            ),
        },
        {
            mode: stringValue(expected.mode, "upload_multipart expected.mode"),
            partCount: integerValue(expected.part_count, "upload_multipart expected.part_count"),
            sizeBytes: integerValue(expected.size_bytes, "upload_multipart expected.size_bytes"),
            checksumAlgorithm: stringValue(
                expected.checksum_algorithm,
                "upload_multipart expected.checksum_algorithm",
            ),
            committedSeq: integerValue(
                expected.committed_seq,
                "upload_multipart expected.committed_seq",
            ),
        },
    ];
}

function decodeAbort(testCase: ConformanceCase): [AbortRequest, AbortExpected] {
    const request = strictObject(testCase.request, ["namespace_id"], `${testCase.name} request`);
    const expected = strictObject(testCase.expected, ["mode", "status"], `${testCase.name} expected`);
    return [
        {
            namespaceId: stringValue(request.namespace_id, "upload_abort request.namespace_id"),
        },
        {
            mode: stringValue(expected.mode, "upload_abort expected.mode"),
            status: stringValue(expected.status, "upload_abort expected.status"),
        },
    ];
}

function decodeDownload(testCase: ConformanceCase): [DownloadRequest, DownloadExpected] {
    const request = strictObject(
        testCase.request,
        ["namespace_id", "path", "commit_id", "actor", "content_utf8"],
        `${testCase.name} request`,
    );
    const expected = strictObject(
        testCase.expected,
        ["size_bytes", "checksum_algorithm", "committed_seq"],
        `${testCase.name} expected`,
    );
    return [
        {
            namespaceId: stringValue(request.namespace_id, "download request.namespace_id"),
            path: stringValue(request.path, "download request.path"),
            commitId: stringValue(request.commit_id, "download request.commit_id"),
            actor: actorValue(request.actor, "download request.actor"),
            contentUtf8: stringValue(request.content_utf8, "download request.content_utf8"),
        },
        {
            sizeBytes: integerValue(expected.size_bytes, "download expected.size_bytes"),
            checksumAlgorithm: stringValue(
                expected.checksum_algorithm,
                "download expected.checksum_algorithm",
            ),
            committedSeq: integerValue(expected.committed_seq, "download expected.committed_seq"),
        },
    ];
}

function decodeEndToEnd(testCase: ConformanceCase): [EndToEndRequest, EndToEndExpected] {
    const request = strictObject(
        testCase.request,
        ["namespace_id", "directory", "upload_path", "moved_path", "actor", "content_utf8", "commit_ids"],
        `${testCase.name} request`,
    );
    const commitIds = strictObject(
        request.commit_ids,
        ["mkdir", "upload", "move", "remove"],
        "end_to_end request.commit_ids",
    );
    const expected = strictObject(
        testCase.expected,
        [
            "mkdir_committed_seq",
            "upload_committed_seq",
            "move_committed_seq",
            "remove_committed_seq",
            "size_bytes",
            "revision_count",
            "change_count",
        ],
        `${testCase.name} expected`,
    );
    return [
        {
            namespaceId: stringValue(request.namespace_id, "end_to_end request.namespace_id"),
            directory: stringValue(request.directory, "end_to_end request.directory"),
            uploadPath: stringValue(request.upload_path, "end_to_end request.upload_path"),
            movedPath: stringValue(request.moved_path, "end_to_end request.moved_path"),
            actor: actorValue(request.actor, "end_to_end request.actor"),
            contentUtf8: stringValue(request.content_utf8, "end_to_end request.content_utf8"),
            commitIds: {
                mkdir: stringValue(commitIds.mkdir, "end_to_end request.commit_ids.mkdir"),
                upload: stringValue(commitIds.upload, "end_to_end request.commit_ids.upload"),
                move: stringValue(commitIds.move, "end_to_end request.commit_ids.move"),
                remove: stringValue(commitIds.remove, "end_to_end request.commit_ids.remove"),
            },
        },
        {
            mkdirCommittedSeq: integerValue(
                expected.mkdir_committed_seq,
                "end_to_end expected.mkdir_committed_seq",
            ),
            uploadCommittedSeq: integerValue(
                expected.upload_committed_seq,
                "end_to_end expected.upload_committed_seq",
            ),
            moveCommittedSeq: integerValue(
                expected.move_committed_seq,
                "end_to_end expected.move_committed_seq",
            ),
            removeCommittedSeq: integerValue(
                expected.remove_committed_seq,
                "end_to_end expected.remove_committed_seq",
            ),
            sizeBytes: integerValue(expected.size_bytes, "end_to_end expected.size_bytes"),
            revisionCount: integerValue(
                expected.revision_count,
                "end_to_end expected.revision_count",
            ),
            changeCount: integerValue(expected.change_count, "end_to_end expected.change_count"),
        },
    ];
}

function loadCases(directory: string): Map<string, ConformanceCase> {
    const cases = readdirSync(directory, { withFileTypes: true })
        .filter((entry) => entry.isFile() && extname(entry.name) === ".json")
        .sort((left, right) => left.name.localeCompare(right.name))
        .map((entry): ConformanceCase => {
            const path = join(directory, entry.name);
            const root = strictObject(JSON.parse(readFileSync(path, "utf8")), CASE_FIELDS, path);
            const version = integerValue(root.version, `${path} version`);
            if (version !== FIXTURE_VERSION) {
                throw new Error(
                    `invalid fixture ${path}: version must be ${FIXTURE_VERSION}, found ${version}`,
                );
            }
            const name = stringValue(root.name, `${path} name`);
            const stem = basename(path, extname(path));
            if (name !== stem) {
                throw new Error(`invalid fixture ${path}: name is ${name}, expected ${stem}`);
            }
            const intent = stringValue(root.intent, `${path} intent`);
            if (intent.trim() === "") {
                throw new Error(`invalid fixture ${path}: intent must not be empty`);
            }
            return {
                name,
                family: stringValue(root.family, `${path} family`),
                request: jsonObject(root.request, `${path} request`),
                expected: jsonObject(root.expected, `${path} expected`),
            };
        });

    const inventory = cases.map((testCase) => [testCase.name, testCase.family]);
    assert.deepEqual(inventory, EXPECTED_CASES, "fixture version 1 inventory differs");
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
): Promise<Uint8Array> {
    const response = await client.filesystem.getFileBytes({
        namespace_id: namespaceId,
        path,
    });
    return new Uint8Array(await response.arrayBuffer());
}

function completedUpload(response: LoonFS.UploadSessionResponse): CompletedUpload {
    const completed = response as CompletedUpload;
    assert.equal(completed.status, "completed");
    return completed;
}

function abortedUpload(response: LoonFS.UploadSessionResponse): AbortedUpload {
    const aborted = response as AbortedUpload;
    assert.equal(aborted.status, "aborted");
    return aborted;
}

function checksum(algorithm: LoonFS.ChecksumAlgorithm, bytes: Uint8Array): LoonFS.Checksum {
    switch (algorithm) {
        case "sha256":
            return { algorithm, value: createHash("sha256").update(bytes).digest("hex") };
        case "crc32c":
            return { algorithm, value: crc32c(bytes).toString(16).padStart(8, "0") };
        case "crc64nvme":
            return { algorithm, value: crc64Nvme(bytes).toString(16).padStart(16, "0") };
    }
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
    const copy = new Uint8Array(bytes.byteLength);
    copy.set(bytes);
    return copy.buffer;
}

function makeCrc32cTable(): Uint32Array {
    const table = new Uint32Array(256);
    for (let index = 0; index < table.length; index += 1) {
        let value = index;
        for (let bit = 0; bit < 8; bit += 1) {
            value = (value >>> 1) ^ ((value & 1) === 0 ? 0 : CRC32C_POLYNOMIAL);
        }
        table[index] = value >>> 0;
    }
    return table;
}

function crc32c(bytes: Uint8Array): number {
    let value = 0xffffffff;
    for (const byte of bytes) {
        value = CRC32C_TABLE[(value ^ byte) & 0xff]! ^ (value >>> 8);
    }
    return (value ^ 0xffffffff) >>> 0;
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

function listedNames(entries: LoonFS.AuthoritativePathEntry[]): string[] {
    return entries.map((entry) => {
        const name = entry.display_name;
        assert.ok(name != null, "listed entry has no display_name");
        return name;
    });
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
        await activeHarness.unauthenticated.namespaces.getNamespace({ namespace_id: request.namespaceId });
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
    await activeHarness.client.namespaces.createNamespace({ namespace_id: request.namespaceId });
    const commit = directoryCommit(
        request.namespaceId,
        request.commitId,
        request.actor,
        request.path,
        request.message,
    );
    const first = await activeHarness.client.filesystem.applyCommit(commit);
    const replayed = await activeHarness.client.filesystem.applyCommit(commit);

    assert.equal(first.committed_seq, expected.committedSeq);
    assert.equal(first.commit_id, request.commitId);
    assert.equal(replayed.committed_seq, first.committed_seq);
    assert.deepEqual(replayed, first);
});

conformanceTest("pagination", async (activeHarness, testCase) => {
    const [request, expected] = decodePagination(testCase);
    await activeHarness.client.namespaces.createNamespace({ namespace_id: request.namespaceId });
    await activeHarness.client.filesystem.applyCommit(
        directoryCommit(
            request.namespaceId,
            "conf-pagination-directory",
            request.actor,
            request.directory,
        ),
    );
    for (const [index, name] of request.entryNames.entries()) {
        await activeHarness.client.filesystem.applyCommit(
            directoryCommit(
                request.namespaceId,
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
    let page = await activeHarness.client.filesystem.listPathEntries({
        namespace_id: request.namespaceId,
        path: request.directory,
        limit: request.pageSize,
    });
    let cursor: string | undefined;
    while (true) {
        pageCount += 1;
        assert.equal(page.response.head_seq, expected.headSeq);
        observed.push(...listedNames(page.data));
        cursor = page.response.next_cursor ?? undefined;
        if (pageCount === request.resumeAfterPage) {
            savedCursor = cursor;
            resumeOffset = observed.length;
        }
        if (cursor === undefined) {
            break;
        }
        await page.getNextPage();
    }

    assert.equal(observed.length, expected.entryCount);
    assert.ok(pageCount >= expected.minimumPageCount);
    assert.equal(cursor, undefined);
    assert.ok(savedCursor !== undefined, "resume cursor was not recorded");
    assert.ok(resumeOffset !== undefined, "resume position was not recorded");

    const resumed: string[] = [];
    page = await activeHarness.client.filesystem.listPathEntries({
        namespace_id: request.namespaceId,
        path: request.directory,
        limit: request.pageSize,
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
    assert.deepEqual(observed, request.entryNames);
    assert.ok(resumeOffset <= request.entryNames.length);
    assert.deepEqual(resumed, request.entryNames.slice(resumeOffset));
});

conformanceTest("changes", async (activeHarness, testCase) => {
    const [request, expected] = decodeChanges(testCase);
    await activeHarness.client.namespaces.createNamespace({ namespace_id: request.namespaceId });
    const committed = await activeHarness.client.filesystem.applyCommit(
        directoryCommit(request.namespaceId, request.commitId, request.actor, request.path),
    );
    assert.equal(committed.committed_seq, expected.committedSeq);

    const feed = await activeHarness.client.filesystem.listChanges({
        namespace_id: request.namespaceId,
        after_seq: request.afterSeq,
    });
    assert.equal(feed.changes.length, expected.changeCount);
    assert.ok(feed.changes.length > 0, "change feed is empty");
    const change = feed.changes[0];
    assert.equal(change.commit_id, request.commitId);
    assert.deepEqual(change.committed_by, request.actor);
    assert.equal(expected.eventKind, "directory_created");
    assert.equal(change.events.length, 1);
    assert.equal(change.events[0]?.kind, "directory_created");
});

conformanceTest("upload_direct_put", async (activeHarness, testCase) => {
    const [request, expected] = decodeDirectPut(testCase);
    await activeHarness.client.namespaces.createNamespace({ namespace_id: request.namespaceId });
    const payload = new TextEncoder().encode(request.contentUtf8);
    const begin = await activeHarness.client.uploads.beginUpload({
        namespace_id: request.namespaceId,
        body: { mode: "direct_put", size_bytes: payload.byteLength },
    });
    assert.equal(begin.mode, "direct_put");
    assert.equal(expected.mode, "direct_put");
    assert.equal(begin.direct_put.checksum_algorithm, expected.checksumAlgorithm);

    await uploadPresigned(begin.direct_put.access, payload, "direct PUT");
    const claim: LoonFS.UploadContentClaim = {
        size_bytes: payload.byteLength,
        checksum: checksum(begin.direct_put.checksum_algorithm, payload),
    };
    const completed = completedUpload(
        await activeHarness.client.uploads.completeUpload({
            namespace_id: request.namespaceId,
            upload_id: begin.upload_id,
            body: { mode: "direct_put", content: claim },
        }),
    );
    assert.equal(completed.content_ref.size_bytes, expected.sizeBytes);
    assert.equal(completed.content_ref.checksum.algorithm, expected.checksumAlgorithm);
    assert.deepEqual(completed.content_ref.checksum, claim.checksum);
    assert.deepEqual(
        completed.content_ref.checksum,
        checksum(completed.content_ref.checksum.algorithm, payload),
    );

    const committed = await activeHarness.client.filesystem.applyCommit(
        fileCommit(
            request.namespaceId,
            request.commitId,
            request.actor,
            request.path,
            completed.content_ref,
            completed.content_token,
        ),
    );
    assert.equal(committed.committed_seq, expected.committedSeq);
    const stat = (await activeHarness.client.filesystem.statPath({
        namespace_id: request.namespaceId,
        path: request.path,
    })) as FileEntry;
    assert.deepEqual(stat.content_ref, completed.content_ref);
    assert.deepEqual(
        await readProxied(activeHarness.client, request.namespaceId, request.path),
        payload,
    );
});

conformanceTest("upload_multipart", async (activeHarness, testCase) => {
    const [request, expected] = decodeMultipart(testCase);
    await activeHarness.client.namespaces.createNamespace({ namespace_id: request.namespaceId });
    const payload = bytePattern(request.contentPattern);
    const begin = await activeHarness.client.uploads.beginUpload({
        namespace_id: request.namespaceId,
        body: {
            mode: "direct_multipart",
            multipart: { part_size_bytes: request.partSizeBytes },
        },
    });
    assert.equal(begin.mode, "direct_multipart");
    assert.equal(expected.mode, "direct_multipart");
    assert.equal(begin.direct_multipart.part_size_bytes, request.partSizeBytes);
    assert.equal(begin.direct_multipart.checksum_algorithm, expected.checksumAlgorithm);

    const chunks = splitBytes(payload, begin.direct_multipart.part_size_bytes);
    assert.equal(chunks.length, expected.partCount);
    const claims: LoonFS.UploadPartChecksumClaim[] = chunks.map((chunk, index) => ({
        part_number: index + 1,
        checksum: checksum(begin.direct_multipart.checksum_algorithm, chunk),
    }));
    const signed = await activeHarness.client.uploads.signUploadParts({
        namespace_id: request.namespaceId,
        upload_id: begin.upload_id,
        parts: claims,
    });
    assert.equal(signed.parts.length, expected.partCount);

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
    const wholeChecksum = checksum(begin.direct_multipart.checksum_algorithm, payload);
    const completion: LoonFS.CompleteUploadRequest = {
        mode: "direct_multipart",
        content: {
            size_bytes: payload.byteLength,
            checksum: wholeChecksum,
        },
        parts: completedParts,
    };
    const first = completedUpload(
        await activeHarness.client.uploads.completeUpload({
            namespace_id: request.namespaceId,
            upload_id: begin.upload_id,
            body: completion,
        }),
    );
    const replayed = completedUpload(
        await activeHarness.client.uploads.completeUpload({
            namespace_id: request.namespaceId,
            upload_id: begin.upload_id,
            body: completion,
        }),
    );
    assert.equal(replayed.namespace_id, first.namespace_id);
    assert.equal(replayed.upload_id, first.upload_id);
    assert.equal(replayed.mode, first.mode);
    assert.deepEqual(replayed.content_ref, first.content_ref);
    assert.equal(replayed.completed_at_ms, first.completed_at_ms);
    assert.equal(first.content_ref.size_bytes, expected.sizeBytes);
    assert.deepEqual(first.content_ref.checksum, wholeChecksum);
    assert.deepEqual(checksum(first.content_ref.checksum.algorithm, payload), wholeChecksum);

    const committed = await activeHarness.client.filesystem.applyCommit(
        fileCommit(
            request.namespaceId,
            request.commitId,
            request.actor,
            request.path,
            first.content_ref,
            replayed.content_token,
        ),
    );
    assert.equal(committed.committed_seq, expected.committedSeq);
    assert.deepEqual(
        await readProxied(activeHarness.client, request.namespaceId, request.path),
        payload,
    );

    // The same content through the high-level helper: the payload exceeds the
    // part size, so this exercises putFile's multipart branch.
    const helperPath = `${request.path}-helper`;
    const helperCommit = await putFile(activeHarness.client, {
        namespace_id: request.namespaceId,
        path: helperPath,
        bytes: payload,
        actor: request.actor,
        commit_id: `${request.commitId}-helper`,
    });
    assert.ok(helperCommit.committed_seq > 0, "helper multipart put reported no committed_seq");
    const helperRead = await getFile(activeHarness.client, {
        namespace_id: request.namespaceId,
        path: helperPath,
    });
    assert.deepEqual(helperRead.bytes, payload);
    // Content ids are random per upload and the helper may choose a different
    // checksum algorithm; the comparable content fact is the size.
    assert.equal(helperRead.content_ref.size_bytes, first.content_ref.size_bytes);
});

conformanceTest("upload_abort", async (activeHarness, testCase) => {
    const [request, expected] = decodeAbort(testCase);
    await activeHarness.client.namespaces.createNamespace({ namespace_id: request.namespaceId });
    const begin = await activeHarness.client.uploads.beginUpload({
        namespace_id: request.namespaceId,
        body: { mode: "service_proxied" },
    });
    const first = abortedUpload(
        await activeHarness.client.uploads.abortUpload({
            namespace_id: request.namespaceId,
            upload_id: begin.upload_id,
        }),
    );
    const replayed = abortedUpload(
        await activeHarness.client.uploads.abortUpload({
            namespace_id: request.namespaceId,
            upload_id: begin.upload_id,
        }),
    );
    assert.equal(expected.mode, "service_proxied");
    assert.equal(expected.status, "aborted");
    assert.equal(first.mode, expected.mode);
    assert.equal(first.status, expected.status);
    assert.deepEqual(replayed, first);
    assert.equal(replayed.aborted_at_ms, first.aborted_at_ms);
});

conformanceTest("download", async (activeHarness, testCase) => {
    const [request, expected] = decodeDownload(testCase);
    await activeHarness.client.namespaces.createNamespace({ namespace_id: request.namespaceId });
    const payload = new TextEncoder().encode(request.contentUtf8);
    const committed = await putFile(activeHarness.client, {
        namespace_id: request.namespaceId,
        path: request.path,
        bytes: payload,
        actor: request.actor,
        commit_id: request.commitId,
    });
    assert.equal(committed.committed_seq, expected.committedSeq);
    const stat = (await activeHarness.client.filesystem.statPath({
        namespace_id: request.namespaceId,
        path: request.path,
    })) as FileEntry;
    const download = await getFile(activeHarness.client, {
        namespace_id: request.namespaceId,
        path: request.path,
    });
    assert.deepEqual(stat.content_ref, download.content_ref);
    assert.equal(download.content_ref.size_bytes, expected.sizeBytes);
    assert.equal(download.content_ref.checksum.algorithm, expected.checksumAlgorithm);
    assert.equal(download.bytes.byteLength, download.content_ref.size_bytes);
    assert.deepEqual(
        checksum(download.content_ref.checksum.algorithm, download.bytes),
        download.content_ref.checksum,
    );
    assert.deepEqual(download.bytes, payload);
});

conformanceTest("end_to_end", async (activeHarness, testCase) => {
    const [request, expected] = decodeEndToEnd(testCase);
    await activeHarness.client.namespaces.createNamespace({ namespace_id: request.namespaceId });
    const mkdir = await activeHarness.client.filesystem.applyCommit(
        directoryCommit(
            request.namespaceId,
            request.commitIds.mkdir,
            request.actor,
            request.directory,
        ),
    );
    assert.equal(mkdir.committed_seq, expected.mkdirCommittedSeq);

    const payload = new TextEncoder().encode(request.contentUtf8);
    const upload = await putFile(activeHarness.client, {
        namespace_id: request.namespaceId,
        path: request.uploadPath,
        bytes: payload,
        actor: request.actor,
        commit_id: request.commitIds.upload,
    });
    assert.equal(upload.committed_seq, expected.uploadCommittedSeq);
    const stat = (await activeHarness.client.filesystem.statPath({
        namespace_id: request.namespaceId,
        path: request.uploadPath,
    })) as FileEntry;
    assert.equal(stat.size_bytes, expected.sizeBytes);
    const uploadedInode = stat.inode_id;

    const initialListing = await activeHarness.client.filesystem.listPathEntries({
        namespace_id: request.namespaceId,
        path: request.directory,
    });
    assert.ok(initialListing.data.some((entry) => entry.path === request.uploadPath));

    const downloaded = await getFile(activeHarness.client, {
        namespace_id: request.namespaceId,
        path: request.uploadPath,
    });
    assert.deepEqual(downloaded.bytes, payload);

    const moved = await activeHarness.client.filesystem.applyCommit(
        moveCommit(
            request.namespaceId,
            request.commitIds.move,
            request.actor,
            request.uploadPath,
            request.movedPath,
        ),
    );
    assert.equal(moved.committed_seq, expected.moveCommittedSeq);
    const movedListing = await activeHarness.client.filesystem.listPathEntries({
        namespace_id: request.namespaceId,
        path: request.directory,
    });
    assert.ok(movedListing.data.some((entry) => entry.path === request.movedPath));

    const revisions = await activeHarness.client.filesystem.listFileRevisions({
        namespace_id: request.namespaceId,
        path: request.movedPath,
    });
    assert.equal(revisions.data.length, expected.revisionCount);
    assert.equal(revisions.data[0]?.commit_id, request.commitIds.upload);

    let changes = await activeHarness.client.filesystem.listChanges({
        namespace_id: request.namespaceId,
        after_seq: 0,
    });
    assert.equal(changes.changes.length, expected.changeCount - 1);
    const removed = await activeHarness.client.filesystem.applyCommit(
        deleteCommit(
            request.namespaceId,
            request.commitIds.remove,
            request.actor,
            request.movedPath,
        ),
    );
    assert.equal(removed.committed_seq, expected.removeCommittedSeq);

    changes = await activeHarness.client.filesystem.listChanges({
        namespace_id: request.namespaceId,
        after_seq: 0,
    });
    assert.equal(changes.changes.length, expected.changeCount);
    assert.deepEqual(
        changes.changes.map((change) => change.commit_id),
        [
            request.commitIds.mkdir,
            request.commitIds.upload,
            request.commitIds.move,
            request.commitIds.remove,
        ],
    );
    for (const change of changes.changes) {
        assert.deepEqual(change.committed_by, request.actor);
    }

    const trash = await activeHarness.client.filesystem.listTrash({
        namespace_id: request.namespaceId,
    });
    const removedEntry = trash.data.find((entry) => entry.inode_id === uploadedInode);
    assert.ok(removedEntry !== undefined, "removed inode is missing from trash");
    assert.equal(removedEntry.deletion_seq, removed.committed_seq);
});
