import {
    TransferScope,
    verifiedDownload,
    UploadSource,
    bytesSource,
    IncrementalChecksum,
    type UploadContent,
    streamingFetch,
    uploadBody,
} from "./transfer-runtime.js";
import { LoonFSClient as GeneratedLoonFSClient } from "./Client.js";
import { FilesClient as GeneratedFilesClient } from "./api/resources/files/client/Client.js";
import * as core from "./core/index.js";
import type * as LoonFS from "./api/index.js";

const DIRECT_GET_FEATURE = "filesystem.downloads.direct_get";
const DIRECT_MULTIPART_FEATURE = "filesystem.uploads.direct_multipart";
const DIRECT_PUT_FEATURE = "filesystem.uploads.direct_put";
const DIRECT_PUT_MAX_BYTES = "upload.direct_put_max_content_bytes";
const PROXY_UPLOAD_MAX_BYTES = "upload.max_content_bytes";
const MULTIPART_MIN_BYTES = 8 * 1024 * 1024;

export interface FileUploadInput {
    namespace_alias: string;
    path: LoonFS.AbsolutePath;
    content: Uint8Array;
    actor: LoonFS.ActorId;
    commit_id: LoonFS.CommitId;
    message?: string | null;
    behavior?: LoonFS.DestinationBehavior;
    expected_inode_id?: string;
    expected_revision_no?: LoonFS.RevisionNo;
}

export interface FileStreamUploadInput extends Omit<FileUploadInput, "content"> {
    content: UploadContent;
    size_bytes?: number;
}

export interface PrepareFileStreamInput {
    namespace_alias: string;
    content: UploadContent;
    size_bytes?: number;
}

export interface PreparedFileUploadInput extends Omit<FileUploadInput, "content"> {
    prepared: PreparedFileContent;
}

export interface FileUploadResult {
    namespace_id: LoonFS.NamespaceId;
    commit_id: LoonFS.CommitId;
    committed_seq: LoonFS.ChangeSeq;
}

export interface FileDownloadInput {
    namespace_alias: string;
    path: LoonFS.AbsolutePath;
    revision_no?: LoonFS.RevisionNo;
}

export interface FileDownloadResult {
    namespace_alias: string;
    path: LoonFS.AbsolutePath;
    revision_no: LoonFS.RevisionNo;
    content_ref: LoonFS.ContentRef;
    content: Uint8Array;
}

/** A live stream; consume through successful EOF to verify the content. */
export interface FileDownloadStream extends Omit<FileDownloadResult, "content"> {
    content: ReadableStream<Uint8Array>;
}

/** Completed content; preparation does not publish or extend the upload lifetime. */
export interface PreparedFileContent {
    readonly contentRef: LoonFS.ContentRef;
    readonly contentToken?: LoonFS.ContentToken;
}

export declare namespace LoonFSClient {
    /** The generated client options with `baseUrl` in place of `environment`. */
    export type Options = Omit<GeneratedLoonFSClient.Options, "environment"> & {
        /** Origin of the LoonFS proxy. */
        baseUrl: core.Supplier<string>;
    };
    export interface RequestOptions extends GeneratedLoonFSClient.RequestOptions {}
}

export declare namespace FilesClient {
    export type Options = GeneratedFilesClient.Options;
    export interface RequestOptions extends GeneratedFilesClient.RequestOptions {}
}

/** The files group plus streaming and buffered transfers. */
export class FilesClient extends GeneratedFilesClient {
    constructor(
        options: GeneratedFilesClient.Options,
        private readonly root: LoonFSClient,
    ) {
        super(options);
    }

    /** Upload bytes through the same streaming path. */
    public async upload(
        input: FileUploadInput,
        requestOptions: FilesClient.RequestOptions = {},
    ): Promise<FileUploadResult> {
        return this.uploadStream(
            { ...input, content: bytesSource(input.content), size_bytes: input.content.length },
            requestOptions,
        );
    }

    /** Consume a source once and publish it. For publication retries, prepare separately. */
    public async uploadStream(
        input: FileStreamUploadInput,
        requestOptions: FilesClient.RequestOptions = {},
    ): Promise<FileUploadResult> {
        const scope = new TransferScope(requestOptions, this._options.timeoutInSeconds);
        const options = { ...requestOptions, abortSignal: scope.signal };
        try {
            const prepared = await this.prepareFileStream(
                {
                    namespace_alias: input.namespace_alias,
                    content: input.content,
                    size_bytes: input.size_bytes,
                },
                options,
            );
            const { content, size_bytes, ...publication } = input;
            return await this.putFilePrepared({ ...publication, prepared }, options);
        } finally {
            scope.close();
        }
    }

    public async prepareFileBytes(
        input: Pick<FileUploadInput, "namespace_alias" | "content">,
        requestOptions: FilesClient.RequestOptions = {},
    ): Promise<PreparedFileContent> {
        return this.prepareFileStream(
            { ...input, content: bytesSource(input.content), size_bytes: input.content.length },
            requestOptions,
        );
    }

    /** Stage a source once with bounded memory; retain the result for publication retries. */
    public async prepareFileStream(
        input: PrepareFileStreamInput,
        requestOptions: FilesClient.RequestOptions = {},
    ): Promise<PreparedFileContent> {
        const scope = new TransferScope(requestOptions, this._options.timeoutInSeconds);
        let source: UploadSource | undefined;
        try {
            source = new UploadSource(input.content, scope, input.size_bytes);
            return await stageStream(
                this.root,
                input.namespace_alias,
                source,
                scope,
                { ...requestOptions, abortSignal: scope.signal, maxRetries: 0 },
                this._options.fetch ?? fetch,
            );
        } finally {
            source?.close();
            scope.close();
        }
    }

    /** Reuse prepared content and identical publication inputs to retry the same commit. */
    public async putFilePrepared(
        input: PreparedFileUploadInput,
        requestOptions: FilesClient.RequestOptions = {},
    ): Promise<FileUploadResult> {
        const request: LoonFS.CommitRequest = {
            namespace_alias: input.namespace_alias,
            actor: input.actor,
            commit_id: input.commit_id,
            content_tokens: input.prepared.contentToken === undefined ? [] : [input.prepared.contentToken],
            operations: [
                {
                    kind: "put_file",
                    path: input.path,
                    content_ref: input.prepared.contentRef,
                    behavior: input.behavior ?? "no_replace",
                    expected_inode_id: input.expected_inode_id,
                    expected_revision_no: input.expected_revision_no,
                },
            ],
        };
        if (input.message !== undefined) {
            request.message = input.message;
        }
        return this.root.commits.create(request, requestOptions);
    }

    /** Opens a verified stream; cancel its reader to release an unfinished download. */
    public async downloadStream(
        input: FileDownloadInput,
        requestOptions: FilesClient.RequestOptions = {},
    ): Promise<FileDownloadStream> {
        const scope = new TransferScope(requestOptions, this._options.timeoutInSeconds);
        const options = { ...requestOptions, abortSignal: scope.signal };
        let body: ReadableStream<Uint8Array> | null | undefined;
        try {
            scope.check();
            const capabilities = await this.root.capabilities.retrieve(options);
            if ((capabilities.features ?? {})[DIRECT_GET_FEATURE] !== true) {
                const result = await downloadProxied(this.root, input, options);
                body = result.content;
                return { ...result, content: verifiedDownload(body, result.content_ref, scope) };
            }
            const grant = await this.createDownload(input, options);
            requirePresignedMethod(grant.access, "GET", "download");
            const response = await (this._options.fetch ?? fetch)(grant.access.url, {
                redirect: "error",
                method: grant.access.method,
                headers: grant.access.headers,
                signal: scope.signal,
            });
            body = response.body;
            requireSuccessfulResponse(response, "download");
            return {
                namespace_alias: input.namespace_alias,
                path: grant.path,
                revision_no: grant.revision_no,
                content_ref: grant.content_ref,
                content: verifiedDownload(body, grant.content_ref, scope),
            };
        } catch (error) {
            scope.close();
            await body?.cancel(error).catch(() => {});
            throw error;
        }
    }

    /** Collects downloadStream for callers that want all bytes in memory. */
    public async download(
        input: FileDownloadInput,
        requestOptions: FilesClient.RequestOptions = {},
    ): Promise<FileDownloadResult> {
        const stream = await this.downloadStream(input, requestOptions);
        const content = new Uint8Array(await new Response(stream.content).arrayBuffer());
        return { ...stream, content };
    }
}

/** The generated client with streaming and buffered transfer helpers. */
export class LoonFSClient extends GeneratedLoonFSClient {
    private _transferFiles: FilesClient | undefined;

    constructor(options: LoonFSClient.Options) {
        super({
            ...options,
            environment: options.baseUrl,
            fetch: streamingFetch(options.fetch ?? globalThis.fetch.bind(globalThis)),
        });
    }

    public override get files(): FilesClient {
        return (this._transferFiles ??= new FilesClient(this._options, this));
    }
}

// Reads through LoonFS when direct reads are unavailable. It loads the content
// reference first, then requests the exact revision so the reference and
// returned bytes describe the same file version.
async function downloadProxied(
    client: GeneratedLoonFSClient,
    input: FileDownloadInput,
    requestOptions: FilesClient.RequestOptions,
): Promise<FileDownloadStream> {
    let revisionNo = input.revision_no;
    let claim: LoonFS.ContentRef | undefined;
    if (revisionNo === undefined) {
        const entry = await client.files.retrieve(
            {
                namespace_alias: input.namespace_alias,
                path: input.path,
            },
            requestOptions,
        );
        if (entry.inode_kind !== "file") {
            throw new Error(`path ${input.path} is a ${entry.inode_kind}, not a file`);
        }
        claim = entry.content_ref;
        revisionNo = entry.revision_no;
    } else {
        const page = await client.files.listRevisions(
            {
                namespace_alias: input.namespace_alias,
                path: input.path,
            },
            requestOptions,
        );
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
    const body = await client.files.content(
        {
            namespace_alias: input.namespace_alias,
            path: input.path,
            revision_no: revisionNo,
        },
        requestOptions,
    );
    return {
        namespace_alias: input.namespace_alias,
        path: input.path,
        revision_no: revisionNo,
        content_ref: claim,
        content:
            body.stream() ??
            new ReadableStream({
                start(controller) {
                    controller.error(new Error("download response has no body"));
                },
            }),
    };
}

async function stageStream(
    client: GeneratedLoonFSClient,
    namespace: string,
    source: UploadSource,
    scope: TransferScope,
    options: FilesClient.RequestOptions,
    send: typeof fetch,
): Promise<PreparedFileContent> {
    scope.check();
    const capabilities = await client.capabilities.retrieve(options);
    const size = source.expected ?? ((await source.empty()) ? 0 : undefined);
    const features = capabilities.features ?? {},
        limits = capabilities.limits ?? {};
    let request: LoonFS.BeginUploadRequest;
    if ((size === undefined || size >= MULTIPART_MIN_BYTES) && features[DIRECT_MULTIPART_FEATURE]) {
        request = { mode: "direct_multipart" };
    } else {
        const fitsProxy =
            size === undefined ||
            limits[PROXY_UPLOAD_MAX_BYTES] === undefined ||
            size <= limits[PROXY_UPLOAD_MAX_BYTES]!;
        const fitsDirect =
            size !== undefined &&
            (limits[DIRECT_PUT_MAX_BYTES] === undefined || size <= limits[DIRECT_PUT_MAX_BYTES]!);
        if (features[DIRECT_PUT_FEATURE] && fitsDirect && (size! >= MULTIPART_MIN_BYTES || !fitsProxy))
            request = { mode: "direct_put", size_bytes: size };
        else if (fitsProxy) request = { mode: "service_proxied" };
        else throw new Error("source fits no advertised upload transport");
    }
    const begin = await client.uploads.create({ namespace_alias: namespace, body: request }, options);
    let completion: LoonFS.UploadCompletion;
    try {
        if (begin.mode === "service_proxied") {
            source.limit = limits[PROXY_UPLOAD_MAX_BYTES];
            const response = await client.fetch(
                `v0/namespace-aliases/${encodeURIComponent(namespace)}/uploads/${encodeURIComponent(begin.upload_id)}/content`,
                {
                    method: "PUT",
                    headers: { "Content-Type": "application/octet-stream" },
                    body: await uploadBody(source),
                    duplex: "half",
                } as RequestInit,
                {
                    timeoutInSeconds: options.timeoutInSeconds,
                    maxRetries: 0,
                    abortSignal: options.abortSignal,
                    headers: await resolvedHeaders(options.headers),
                },
            );
            requireSuccessfulResponse(response, "proxied upload");
            await response.body?.cancel();
            completion = { mode: "service_proxied" };
        } else if (begin.mode === "direct_put") {
            source.digest = new IncrementalChecksum(begin.checksum_algorithm);
            await putStream(send, begin.access, await uploadBody(source), scope);
            completion = {
                mode: "direct_put",
                content: { size_bytes: source.count, checksum: source.digest.finish() },
            };
        } else {
            const partSize = begin.part_size_bytes;
            if (!Number.isSafeInteger(partSize) || partSize <= 0)
                throw new Error("invalid multipart part size");
            source.digest = new IncrementalChecksum(begin.checksum_algorithm);
            const parts: LoonFS.CompletedUploadPart[] = [];
            while (true) {
                const buffer = new Uint8Array(partSize);
                let length = 0;
                while (length < partSize) {
                    const chunk = await source.read(partSize - length);
                    if (!chunk) break;
                    buffer.set(chunk, length);
                    length += chunk.length;
                }
                if (!length) break;
                if (parts.length === 10000) throw new Error("multipart upload exceeds 10000 parts");
                const part = buffer.subarray(0, length),
                    partNumber = parts.length + 1;
                const digest = new IncrementalChecksum(begin.checksum_algorithm);
                digest.update(part);
                const checksum = digest.finish();
                const signed = await client.uploads.signParts(
                    {
                        namespace_alias: namespace,
                        upload_id: begin.upload_id,
                        parts: [{ part_number: partNumber, checksum }],
                    },
                    options,
                );
                if (signed.parts.length !== 1 || signed.parts[0]!.part_number !== partNumber)
                    throw new Error(`server did not sign requested part ${partNumber}`);
                const etag = await putStream(send, signed.parts[0]!.access, part, scope);
                if (!etag) throw new Error(`part ${partNumber} returned no ETag`);
                parts.push({ part_number: partNumber, checksum, etag });
            }
            completion = {
                mode: "direct_multipart",
                content: { size_bytes: source.count, checksum: source.digest.finish() },
                parts,
            };
        }
        source.finish();
    } catch (error) {
        try {
            await client.uploads.abort(
                { namespace_alias: namespace, upload_id: begin.upload_id },
                { timeoutInSeconds: 5, maxRetries: 0, abortSignal: AbortSignal.timeout(5000) },
            );
        } catch {
            /* Preserve the transfer error. */
        }
        throw error;
    }
    // A lost completion response may still have completed the session.
    const completed = await client.uploads.complete(
        { namespace_alias: namespace, upload_id: begin.upload_id, body: completion },
        options,
    );
    if (completed.status !== "completed") throw new Error(`upload is ${completed.status}, not completed`);
    if (completed.content_ref.size_bytes !== source.count) throw new Error("completed upload size mismatch");
    return { contentRef: completed.content_ref, contentToken: completed.content_token ?? undefined };
}

async function putStream(
    send: typeof fetch,
    access: LoonFS.ObjectTransferAccess,
    body: BodyInit,
    scope: TransferScope,
): Promise<string | null> {
    requirePresignedMethod(access, "PUT", "upload");
    scope.check();
    const response = await send(access.url, {
        redirect: "error",
        method: "PUT",
        headers: access.headers,
        body,
        signal: scope.signal,
        duplex: "half",
    } as RequestInit);
    try {
        requireSuccessfulResponse(response, "upload");
        return response.headers.get("etag");
    } finally {
        await response.body?.cancel();
    }
}

function requirePresignedMethod(
    access: LoonFS.ObjectTransferAccess,
    expected: "GET" | "PUT",
    operation: string,
): void {
    if (access.method !== expected)
        throw new Error(`${operation} received unsupported presigned method ${access.method}`);
}

function requireSuccessfulResponse(response: Response, operation: string): void {
    if (!response.ok) throw new Error(`${operation} failed with HTTP ${response.status}`);
}

async function resolvedHeaders(
    headers: FilesClient.RequestOptions["headers"],
): Promise<Record<string, string>> {
    const result: Record<string, string> = {};
    for (const [name, supplier] of Object.entries(headers ?? {})) {
        const value = await core.Supplier.get(supplier);
        if (value != null) result[name] = value;
    }
    return result;
}
