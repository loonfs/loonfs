import { LoonFSClient } from "./Client.js";
import type * as LoonFS from "./api/index.js";

const DIRECT_MULTIPART_FEATURE = "core.uploads.direct_multipart";
const DIRECT_PUT_FEATURE = "core.uploads.direct_put";
const DIRECT_PUT_MAX_BYTES = "upload.direct_put_max_content_bytes";
const PROXY_UPLOAD_MAX_BYTES = "upload.max_content_bytes";
const MULTIPART_MIN_BYTES = 8 * 1024 * 1024;
const CRC32C_POLYNOMIAL = 0x82f63b78;
const CRC64_NVME_POLYNOMIAL = 0x9a6c9329ac4bc9b5n;
const CRC64_MASK = 0xffffffffffffffffn;

const DIRECT_PUT_CHECKSUM_FEATURES: readonly [string, LoonFS.ChecksumAlgorithm][] = [
    ["core.uploads.direct_put.checksum.sha256", "sha256"],
    ["core.uploads.direct_put.checksum.crc64nvme", "crc64nvme"],
    ["core.uploads.direct_put.checksum.crc32c", "crc32c"],
];

const CRC32C_TABLE = makeCrc32cTable();
const CRC64_NVME_TABLE = makeCrc64NvmeTable();

export interface PutFileInput {
    namespace_id: LoonFS.NamespaceId;
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
    namespace_id: LoonFS.NamespaceId;
    path: LoonFS.AbsolutePath;
    revision_no?: LoonFS.RevisionNo;
}

export interface GetFileResult extends LoonFS.BeginDownloadResponse {
    bytes: Uint8Array;
}

type CompletedUpload = LoonFS.UploadSessionResponse & LoonFS.UploadSessionStatus.Completed;

interface StagedContent {
    contentRef: LoonFS.ContentRef;
    contentToken?: LoonFS.ContentToken;
}

/**
 * Uploads in-memory bytes and commits them at one path.
 * Streaming and resume are follow-ups.
 */
export async function putFile(client: LoonFSClient, input: PutFileInput): Promise<PutFileResult> {
    const staged = await stageBytes(client, input.namespace_id, input.bytes);
    const request: LoonFS.CommitRequest = {
        namespace_id: input.namespace_id,
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
    return client.filesystem.applyCommit(request);
}

/**
 * Downloads one revision into memory and verifies its content claim.
 * Streaming and resume are follow-ups.
 */
export async function getFile(client: LoonFSClient, input: GetFileInput): Promise<GetFileResult> {
    const grant = await client.filesystem.beginDownload(input);
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
    return { ...grant, bytes };
}

async function stageBytes(
    client: LoonFSClient,
    namespaceId: LoonFS.NamespaceId,
    bytes: Uint8Array,
): Promise<StagedContent> {
    const capabilities = await client.capabilities();
    const beginRequest = await selectBeginRequest(capabilities, bytes);
    const begin = await client.uploads.beginUpload({
        namespace_id: namespaceId,
        body: beginRequest,
    });

    switch (begin.mode) {
        case "direct_put":
            return stageDirectPut(client, namespaceId, bytes, begin);
        case "direct_multipart":
            return stageMultipart(client, namespaceId, bytes, begin);
        case "service_proxied":
            return stageServiceProxied(client, namespaceId, bytes, begin);
    }
}

async function selectBeginRequest(
    capabilities: LoonFS.CapabilityDocument,
    bytes: Uint8Array,
): Promise<LoonFS.BeginUploadRequest> {
    const features = capabilities.features ?? {};
    const limits = capabilities.limits ?? {};
    const supportsMultipart = features[DIRECT_MULTIPART_FEATURE] === true;
    const worthCutting = bytes.byteLength >= MULTIPART_MIN_BYTES;
    if (worthCutting && supportsMultipart) {
        return { mode: "direct_multipart" };
    }

    const directPutAlgorithm = features[DIRECT_PUT_FEATURE]
        ? DIRECT_PUT_CHECKSUM_FEATURES.find(([feature]) => features[feature] === true)?.[1]
        : undefined;
    const proxyLimit = limits[PROXY_UPLOAD_MAX_BYTES];
    const fitsProxy = proxyLimit === undefined || bytes.byteLength <= proxyLimit;
    const directPutLimit = limits[DIRECT_PUT_MAX_BYTES];
    const fitsDirectPut = directPutLimit === undefined || bytes.byteLength <= directPutLimit;
    if (
        directPutAlgorithm !== undefined &&
        fitsDirectPut &&
        (worthCutting || !fitsProxy)
    ) {
        return {
            mode: "direct_put",
            content: {
                size_bytes: bytes.byteLength,
                checksum: await checksum(directPutAlgorithm, bytes),
            },
        };
    }
    if (fitsProxy) {
        return { mode: "service_proxied" };
    }
    throw new Error(`${bytes.byteLength} bytes fit no advertised upload transport`);
}

async function stageServiceProxied(
    client: LoonFSClient,
    namespaceId: LoonFS.NamespaceId,
    bytes: Uint8Array,
    begin: LoonFS.BeginUploadResponse.ServiceProxied,
): Promise<StagedContent> {
    try {
        await client.uploads.uploadContent(arrayBuffer(bytes), namespaceId, begin.upload_id);
        return stagedContent(
            await client.uploads.completeUpload({
                namespace_id: namespaceId,
                upload_id: begin.upload_id,
                body: { mode: "service_proxied" },
            }),
        );
    } catch (error) {
        await abortQuietly(client, namespaceId, begin.upload_id);
        throw error;
    }
}

async function stageDirectPut(
    client: LoonFSClient,
    namespaceId: LoonFS.NamespaceId,
    bytes: Uint8Array,
    begin: LoonFS.BeginUploadResponse.DirectPut,
): Promise<StagedContent> {
    const expected = begin.direct_put.content_ref;
    if (expected.size_bytes !== bytes.byteLength) {
        throw new Error("direct PUT response did not describe the requested size");
    }
    const actual = await checksum(expected.checksum.algorithm, bytes);
    if (actual.value !== expected.checksum.value) {
        throw new Error("direct PUT response did not describe the requested checksum");
    }
    try {
        requirePresignedMethod(begin.direct_put.access, "PUT", "direct PUT");
        const response = await fetch(begin.direct_put.access.url, {
            redirect: "error",
            method: begin.direct_put.access.method,
            headers: begin.direct_put.access.headers,
            body: arrayBuffer(bytes),
        });
        requireSuccessfulResponse(response, "direct PUT");
    } catch (error) {
        await abortQuietly(client, namespaceId, begin.upload_id);
        throw error;
    }
    const completed = completedUpload(
        await client.uploads.completeUpload({
            namespace_id: namespaceId,
            upload_id: begin.upload_id,
            body: { mode: "direct_put" },
        }),
    );
    if (!sameContentRef(completed.content_ref, expected)) {
        throw new Error("direct PUT completion returned a different content reference");
    }
    return stagedContent(completed);
}

async function stageMultipart(
    client: LoonFSClient,
    namespaceId: LoonFS.NamespaceId,
    bytes: Uint8Array,
    begin: LoonFS.BeginUploadResponse.DirectMultipart,
): Promise<StagedContent> {
    const { checksum_algorithm: algorithm, part_size_bytes: partSize } = begin.direct_multipart;
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
        const signed = await client.uploads.signUploadParts({
            namespace_id: namespaceId,
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
        await abortQuietly(client, namespaceId, begin.upload_id);
        throw error;
    }

    return stagedContent(
        await client.uploads.completeUpload({
            namespace_id: namespaceId,
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

function completedUpload(response: LoonFS.UploadSessionResponse): CompletedUpload {
    const completed = response as CompletedUpload;
    if (completed.status !== "completed") {
        throw new Error(`upload ${response.upload_id} completed with status ${completed.status}`);
    }
    return completed;
}

function stagedContent(response: LoonFS.UploadSessionResponse): StagedContent {
    const completed = completedUpload(response);
    return {
        contentRef: completed.content_ref,
        contentToken: completed.content_token,
    };
}

async function abortQuietly(
    client: LoonFSClient,
    namespaceId: LoonFS.NamespaceId,
    uploadId: LoonFS.UploadId,
): Promise<void> {
    try {
        await client.uploads.abortUpload({ namespace_id: namespaceId, upload_id: uploadId });
    } catch {
        // Preserve the transfer error.
    }
}

function sameContentRef(left: LoonFS.ContentRef, right: LoonFS.ContentRef): boolean {
    return (
        left.content_id === right.content_id &&
        left.kind === right.kind &&
        left.size_bytes === right.size_bytes &&
        left.checksum.algorithm === right.checksum.algorithm &&
        left.checksum.value === right.checksum.value
    );
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
