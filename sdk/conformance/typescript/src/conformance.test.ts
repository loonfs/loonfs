import * as assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { basename, extname, join } from "node:path";
import { test } from "node:test";

import { LoonFS, LoonFSClient } from "../../../generated/typescript/index.js";


const FIXTURE_VERSION = 1;
const RUNNER_SKIP = "run scripts/run-sdk-conformance.sh typescript";
const TRANSFER_SKIP = "file transfer cases are not implemented in the TypeScript harness yet";
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
const TRANSFER_CASES = [
    "download",
    "end_to_end",
    "upload_abort",
    "upload_direct_put",
    "upload_multipart",
] as const;


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

interface Harness {
    client: LoonFSClient;
    unauthenticated: LoonFSClient;
}


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

for (const caseName of TRANSFER_CASES) {
    test(caseName, { skip: environmentSkip ?? TRANSFER_SKIP }, () => {});
}
