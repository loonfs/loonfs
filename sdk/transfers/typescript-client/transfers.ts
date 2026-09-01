import { LoonFSClient } from "./Client.js";
import type * as LoonFS from "./api/index.js";

const DIRECT_GET_FEATURE = "core.downloads.direct_get";
const DIRECT_MULTIPART_FEATURE = "core.uploads.direct_multipart";
const DIRECT_PUT_FEATURE = "core.uploads.direct_put";
const DIRECT_PUT_MAX_BYTES = "upload.direct_put_max_content_bytes";
const PROXY_UPLOAD_MAX_BYTES = "upload.max_content_bytes";
const MULTIPART_MIN_BYTES = 8 * 1024 * 1024;

// The CRC constants, tables, and functions are copied from
// sdk/transfers/typescript/transfers.ts.
const CRC32C_POLYNOMIAL = 0x82f63b78;
const CRC64_NVME_POLYNOMIAL = 0x9a6c9329ac4bc9b5n;
const CRC64_MASK = 0xffffffffffffffffn;

const CRC32C_TABLE = makeCrc32cTable();
const CRC64_NVME_TABLE = makeCrc64NvmeTable();

export interface PutFileInput {
    namespace_alias: string;
    path: LoonFS.AbsolutePath;
    bytes: Uint8Array;
    actor: LoonFS.ActorRef;
    commit_id: LoonFS.CommitId;
    message?: string | null;
    behavior?: LoonFS.DestinationBehavior;
    expected_revision_no?: LoonFS.RevisionNo;
}

export interface PutFileResult {
    namespace_id: LoonFS.NamespaceId;
    commit_id: LoonFS.CommitId;
    committed_seq: LoonFS.ChangeSeq;
}

export interface GetFileInput {
    namespace_alias: string;
    path: LoonFS.AbsolutePath;
    revision_no?: LoonFS.RevisionNo;
}

export interface GetFileResult {
    namespace_alias: string;
    path: LoonFS.AbsolutePath;
    revision_no: LoonFS.RevisionNo;
    content_ref: LoonFS.ContentRef;
    bytes: Uint8Array;
}

interface StagedContent {
    contentRef: LoonFS.ContentRef;
    contentToken?: LoonFS.ContentToken;
}

interface DirectPutBody {
    body: ArrayBuffer;
    content: LoonFS.UploadContentClaim;
}

/**
 * Uploads in-memory bytes and commits them at one namespace alias path.
 * Streaming and resume are follow-ups.
 */
export async function putFile(client: LoonFSClient, input: PutFileInput): Promise<PutFileResult> {
    const staged = await stageBytes(client, input.namespace_alias, input.bytes);
    const request: LoonFS.CommitRequest = {
        namespace_alias: input.namespace_alias,
        actor: input.actor,
        commit_id: input.commit_id,
        content_tokens: staged.contentToken === undefined ? [] : [staged.contentToken],
        operations: [
            {
                kind: "put_file",
                path: input.path,
                content_ref: staged.contentRef,
                behavior: input.behavior ?? "no_replace",
                expected_revision_no: input.expected_revision_no,
            },
        ],
    };
    if (input.message !== undefined) {
        request.message = input.message;
    }
    return client.commits.create(request);
}

/**
 * Downloads one revision into memory and verifies its content claim.
 * Streaming and resume are follow-ups.
 */
export async function getFile(client: LoonFSClient, input: GetFileInput): Promise<GetFileResult> {
    const capabilities = await client.capabilities.retrieve();
    if ((capabilities.features ?? {})[DIRECT_GET_FEATURE] !== true) {
        return getFileProxied(client, input);
    }
    const grant = await client.files.createDownload(input);
    requirePresignedMethod(grant.access, "GET", "download");
    const response = await fetch(grant.access.url, {
        redirect: "error",
        method: grant.access.method,
        headers: grant.access.headers,
    });
    requireSuccessfulResponse(response, "download");
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength !== grant.content_ref.size_bytes) {
        throw new Error(
            `download returned ${bytes.byteLength} bytes, expected ${grant.content_ref.size_bytes}`,
        );
    }
    const actual = await checksum(grant.content_ref.checksum.algorithm, bytes);
    if (actual.value !== grant.content_ref.checksum.value) {
        throw new Error(`download checksum did not match ${grant.content_ref.checksum.algorithm} claim`);
    }
    return {
        namespace_alias: input.namespace_alias,
        path: grant.path,
        revision_no: grant.revision_no,
        content_ref: grant.content_ref,
        bytes,
    };
}

// Reads through LoonFS when direct reads are unavailable. It loads the content
// reference first, then requests the exact revision so the reference and
// returned bytes describe the same file version.
async function getFileProxied(client: LoonFSClient, input: GetFileInput): Promise<GetFileResult> {
    let revisionNo = input.revision_no;
    let claim: LoonFS.ContentRef | undefined;
    if (revisionNo === undefined) {
        const entry = await client.files.retrieve({
            namespace_alias: input.namespace_alias,
            path: input.path,
        });
        if (entry.inode_kind !== "file") {
            throw new Error(`path ${input.path} is a ${entry.inode_kind}, not a file`);
        }
        claim = entry.content_ref;
        revisionNo = entry.revision_no;
    } else {
        const page = await client.files.listRevisions({
            namespace_alias: input.namespace_alias,
            path: input.path,
        });
        for await (const revision of page) {
            if (revision.revision_no === revisionNo) {
                claim = revision.content_ref;
                break;
            }
        }
        if (claim === undefined) {
            throw new Error(`revision ${revisionNo} not found for ${input.path}`);
        }
    }
    const body = await client.files.content({
        namespace_alias: input.namespace_alias,
        path: input.path,
        revision_no: revisionNo,
    });
    const bytes = new Uint8Array(await body.arrayBuffer());
    if (bytes.byteLength !== claim.size_bytes) {
        throw new Error(`proxied read returned ${bytes.byteLength} bytes, expected ${claim.size_bytes}`);
    }
    const actual = await checksum(claim.checksum.algorithm, bytes);
    if (actual.value !== claim.checksum.value) {
        throw new Error(`proxied read checksum did not match ${claim.checksum.algorithm} claim`);
    }
    return { namespace_alias: input.namespace_alias, path: input.path, revision_no: revisionNo, content_ref: claim, bytes };
}

async function stageBytes(
    client: LoonFSClient,
    namespaceAlias: string,
    bytes: Uint8Array,
): Promise<StagedContent> {
    const capabilities = await client.capabilities.retrieve();
    const beginRequest = selectBeginRequest(capabilities, bytes);
    const begin = await client.uploads.create({
        namespace_alias: namespaceAlias,
        body: beginRequest,
    });

    switch (begin.mode) {
        case "direct_put":
            return stageDirectPut(client, namespaceAlias, bytes, begin);
        case "direct_multipart":
            return stageMultipart(client, namespaceAlias, bytes, begin);
        case "service_proxied":
            return stageServiceProxied(client, namespaceAlias, bytes, begin);
    }
}

function selectBeginRequest(
    capabilities: LoonFS.CapabilityDocument,
    bytes: Uint8Array,
): LoonFS.BeginUploadRequest {
    const features = capabilities.features ?? {};
    const limits = capabilities.limits ?? {};
    const supportsMultipart = features[DIRECT_MULTIPART_FEATURE] === true;
    const worthCutting = bytes.byteLength >= MULTIPART_MIN_BYTES;
    if (worthCutting && supportsMultipart) {
        return { mode: "direct_multipart" };
    }

    const proxyLimit = limits[PROXY_UPLOAD_MAX_BYTES];
    const fitsProxy = proxyLimit === undefined || bytes.byteLength <= proxyLimit;
    const directPutLimit = limits[DIRECT_PUT_MAX_BYTES];
    const fitsDirectPut = directPutLimit === undefined || bytes.byteLength <= directPutLimit;
    if (
        features[DIRECT_PUT_FEATURE] === true &&
        fitsDirectPut &&
        (worthCutting || !fitsProxy)
    ) {
        return {
            mode: "direct_put",
            size_bytes: bytes.byteLength,
        };
    }
    if (fitsProxy) {
        return { mode: "service_proxied" };
    }
    throw new Error(`${bytes.byteLength} bytes fit no advertised upload transport`);
}

async function stageServiceProxied(
    client: LoonFSClient,
    namespaceAlias: string,
    bytes: Uint8Array,
    begin: LoonFS.BeginUploadResponse.ServiceProxied,
): Promise<StagedContent> {
    try {
        await client.uploads.putContent(arrayBuffer(bytes), namespaceAlias, begin.upload_id);
        return stagedContent(
            await client.uploads.complete({
                namespace_alias: namespaceAlias,
                upload_id: begin.upload_id,
                body: { mode: "service_proxied" },
            }),
        );
    } catch (error) {
        await abortQuietly(client, namespaceAlias, begin.upload_id);
        throw error;
    }
}

async function stageDirectPut(
    client: LoonFSClient,
    namespaceAlias: string,
    bytes: Uint8Array,
    begin: LoonFS.BeginUploadResponse.DirectPut,
): Promise<StagedContent> {
    let upload: DirectPutBody;
    try {
        requirePresignedMethod(begin.access, "PUT", "direct PUT");
        upload = await directPutBody(begin.checksum_algorithm, bytes);
        const response = await fetch(begin.access.url, {
            redirect: "error",
            method: begin.access.method,
            headers: begin.access.headers,
            body: upload.body,
        });
        requireSuccessfulResponse(response, "direct PUT");
    } catch (error) {
        await abortQuietly(client, namespaceAlias, begin.upload_id);
        throw error;
    }
    return stagedContent(
        await client.uploads.complete({
            namespace_alias: namespaceAlias,
            upload_id: begin.upload_id,
            body: { mode: "direct_put", content: upload.content },
        }),
    );
}

async function stageMultipart(
    client: LoonFSClient,
    namespaceAlias: string,
    bytes: Uint8Array,
    begin: LoonFS.BeginUploadResponse.DirectMultipart,
): Promise<StagedContent> {
    const { checksum_algorithm: algorithm, part_size_bytes: partSize } = begin;
    if (!Number.isSafeInteger(partSize) || partSize <= 0) {
        throw new Error(`invalid multipart part size ${partSize}`);
    }
    if (bytes.byteLength === 0) {
        throw new Error("direct multipart upload cannot carry an empty payload");
    }

    const chunks = splitBytes(bytes, partSize);
    const claims: LoonFS.UploadPartChecksumClaim[] = [];
    for (const [index, chunk] of chunks.entries()) {
        claims.push({
            part_number: index + 1,
            checksum: await checksum(algorithm, chunk),
        });
    }
    const completedParts: LoonFS.CompletedUploadPart[] = [];
    try {
        const signed = await client.uploads.signParts({
            namespace_alias: namespaceAlias,
            upload_id: begin.upload_id,
            parts: claims,
        });
        const accessByPart = new Map(signed.parts.map((part) => [part.part_number, part.access]));
        for (const [index, claim] of claims.entries()) {
            const access = accessByPart.get(claim.part_number);
            if (access === undefined) {
                throw new Error(`server did not sign part ${claim.part_number}`);
            }
            requirePresignedMethod(access, "PUT", `part ${claim.part_number}`);
            const response = await fetch(access.url, {
                redirect: "error",
                method: access.method,
                headers: access.headers,
                body: arrayBuffer(chunks[index]!),
            });
            requireSuccessfulResponse(response, `part ${claim.part_number}`);
            const etag = response.headers.get("etag");
            if (etag === null) {
                throw new Error(`part ${claim.part_number} upload returned no etag`);
            }
            completedParts.push({
                part_number: claim.part_number,
                checksum: claim.checksum,
                etag,
            });
        }
    } catch (error) {
        await abortQuietly(client, namespaceAlias, begin.upload_id);
        throw error;
    }

    return stagedContent(
        await client.uploads.complete({
            namespace_alias: namespaceAlias,
            upload_id: begin.upload_id,
            body: {
                mode: "direct_multipart",
                content: {
                    size_bytes: bytes.byteLength,
                    checksum: await checksum(algorithm, bytes),
                },
                parts: completedParts,
            },
        }),
    );
}

function splitBytes(bytes: Uint8Array, partSize: number): Uint8Array[] {
    const parts: Uint8Array[] = [];
    for (let offset = 0; offset < bytes.byteLength; offset += partSize) {
        parts.push(bytes.subarray(offset, Math.min(offset + partSize, bytes.byteLength)));
    }
    return parts;
}

function stagedContent(response: LoonFS.UploadSession): StagedContent {
    if (response.status !== "completed") {
        throw new Error(`upload ${response.upload_id} completed with status ${response.status}`);
    }
    return {
        contentRef: response.content_ref,
        contentToken: response.content_token,
    };
}

async function abortQuietly(
    client: LoonFSClient,
    namespaceAlias: string,
    uploadId: LoonFS.UploadId,
): Promise<void> {
    try {
        await client.uploads.abort({
            namespace_alias: namespaceAlias,
            upload_id: uploadId,
        });
    } catch {
        // Preserve the transfer error.
    }
}

function requirePresignedMethod(
    access: LoonFS.ObjectTransferAccess,
    expected: "GET" | "PUT",
    operation: string,
): void {
    if (access.method !== expected) {
        throw new Error(`${operation} received unsupported presigned method ${access.method}`);
    }
}

function requireSuccessfulResponse(response: Response, operation: string): void {
    if (!response.ok) {
        throw new Error(`${operation} failed with HTTP ${response.status}`);
    }
}

async function directPutBody(
    algorithm: LoonFS.ChecksumAlgorithm,
    bytes: Uint8Array,
): Promise<DirectPutBody> {
    const body = arrayBuffer(bytes);
    const sentBytes = new Uint8Array(body);
    return {
        body,
        content: {
            size_bytes: sentBytes.byteLength,
            checksum: await checksum(algorithm, sentBytes),
        },
    };
}

async function checksum(
    algorithm: LoonFS.ChecksumAlgorithm,
    bytes: Uint8Array,
): Promise<LoonFS.Checksum> {
    switch (algorithm) {
        case "sha256": {
            const digest = await globalThis.crypto.subtle.digest("SHA-256", arrayBuffer(bytes));
            return { algorithm, value: hex(new Uint8Array(digest)) };
        }
        case "crc32c":
            return { algorithm, value: crc32c(bytes).toString(16).padStart(8, "0") };
        case "crc64nvme":
            return { algorithm, value: crc64Nvme(bytes).toString(16).padStart(16, "0") };
    }
}

function hex(bytes: Uint8Array): string {
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
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
