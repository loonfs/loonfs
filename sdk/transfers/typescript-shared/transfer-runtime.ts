import type { ChecksumAlgorithm, ContentRef, DownloadRange } from "./api/index.js";
const TRANSFER_CHUNK_BYTES = 64 * 1024;
const MAX_INLINE_BYTES = 64 * 1024;
const MAX_APPEND_BYTES = 256 * 1024;

interface TransferRequestOptions {
    timeoutInSeconds?: number;
    abortSignal?: AbortSignal;
}

export class TransferScope {
    readonly controller = new AbortController();
    readonly signal = this.controller.signal;
    private readonly timer: ReturnType<typeof setTimeout>;
    private readonly parent?: AbortSignal;
    private readonly onParentAbort = () => this.controller.abort(this.parent?.reason);

    constructor(options: TransferRequestOptions = {}, defaultTimeout = 60) {
        const seconds = options.timeoutInSeconds ?? defaultTimeout;
        if (!Number.isFinite(seconds) || seconds <= 0)
            throw new Error("transfer timeout must be positive and finite");
        this.parent = options.abortSignal;
        if (this.parent?.aborted) this.onParentAbort();
        else
            this.parent?.addEventListener("abort", this.onParentAbort, {
                once: true,
            });
        this.timer = setTimeout(
            () => this.controller.abort(new DOMException("transfer timed out", "TimeoutError")),
            seconds * 1000,
        );
    }

    check(): void {
        if (this.signal.aborted) throw this.signal.reason;
    }
    close(): void {
        clearTimeout(this.timer);
        this.parent?.removeEventListener("abort", this.onParentAbort);
    }
}

function crcTable(polynomial: bigint): bigint[] {
    return Array.from({ length: 256 }, (_, byte) => {
        let value = BigInt(byte);
        for (let bit = 0; bit < 8; bit++) value = (value >> 1n) ^ (value & 1n ? polynomial : 0n);
        return value;
    });
}
const CRC32 = crcTable(0x82f63b78n).map(Number);
const CRC64 = crcTable(0x9a6c9329ac4bc9b5n);

export class IncrementalChecksum {
    private crc32 = 0xffffffff;
    private crc64 = 0xffffffffffffffffn;
    constructor(readonly algorithm: ChecksumAlgorithm) {
        if (algorithm !== "crc32c" && algorithm !== "crc64nvme")
            throw new Error(`unsupported checksum algorithm ${algorithm}`);
    }
    update(bytes: Uint8Array): void {
        if (this.algorithm === "crc32c") {
            for (const byte of bytes) this.crc32 = CRC32[(this.crc32 ^ byte) & 255]! ^ (this.crc32 >>> 8);
        } else {
            for (const byte of bytes)
                this.crc64 = CRC64[Number((this.crc64 ^ BigInt(byte)) & 255n)]! ^ (this.crc64 >> 8n);
        }
    }
    finish(): { algorithm: ChecksumAlgorithm; value: string } {
        const value =
            this.algorithm === "crc32c"
                ? ((this.crc32 ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0")
                : (this.crc64 ^ 0xffffffffffffffffn).toString(16).padStart(16, "0");
        return { algorithm: this.algorithm, value };
    }
}

export function downloadRanges(
    ranges: DownloadRange[],
    sizeBytes: number,
    send: typeof fetch,
    scope: TransferScope,
): ReadableStream<Uint8Array> {
    let offset = 0;
    if (ranges.length === 0 || (sizeBytes === 0 && ranges.length !== 1))
        throw new Error("download grant has invalid ranges");
    for (const range of ranges) {
        if (
            range.start_offset !== offset ||
            !Number.isSafeInteger(range.length) ||
            range.length < 0 ||
            (range.length === 0 && sizeBytes !== 0) ||
            range.length > sizeBytes - offset
        )
            throw new Error("download grant has invalid ranges");
        if (range.access.method !== "GET") throw new Error("download grant must use GET");
        offset += range.length;
    }
    if (!Number.isSafeInteger(offset) || offset !== sizeBytes)
        throw new Error("download grant has invalid ranges");
    let index = 0;
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let remaining = 0;
    let cancelled = false;
    const openNext = async (): Promise<boolean> => {
        while (index < ranges.length && !cancelled) {
            scope.check();
            const range = ranges[index++]!;
            if (range.length === 0) continue;
            const response = await send(range.access.url, {
                redirect: "error",
                method: range.access.method,
                headers: range.access.headers,
                signal: scope.signal,
            });
            if (cancelled) {
                await response.body?.cancel();
                return false;
            }
            if (!response.ok) {
                await response.body?.cancel();
                throw new Error(`presigned request failed with HTTP ${response.status}`);
            }
            if (!response.body) throw new Error("download response has no body");
            reader = response.body.getReader();
            remaining = range.length;
            return true;
        }
        return false;
    };
    return new ReadableStream<Uint8Array>(
        {
            async pull(controller) {
                try {
                    while (!cancelled) {
                        scope.check();
                        if (!reader && !(await openNext())) {
                            if (!cancelled) controller.close();
                            return;
                        }
                        const next = await reader!.read();
                        if (cancelled) return;
                        if (next.done) {
                            reader!.releaseLock();
                            reader = undefined;
                            if (remaining !== 0)
                                throw new Error("download range ended before its declared length");
                            continue;
                        }
                        remaining -= next.value.length;
                        if (remaining < 0) throw new Error("download range exceeded its declared length");
                        controller.enqueue(next.value);
                        return;
                    }
                } catch (error) {
                    await reader?.cancel(error).catch(() => {});
                    reader?.releaseLock();
                    reader = undefined;
                    if (!cancelled) controller.error(error);
                }
            },
            async cancel(reason) {
                cancelled = true;
                scope.controller.abort(reason);
                await reader?.cancel(reason).catch(() => {});
                reader?.releaseLock();
                reader = undefined;
            },
        },
        { highWaterMark: 0 },
    );
}

export function verifiedDownload(
    body: ReadableStream<Uint8Array> | null,
    claim: ContentRef,
    scope: TransferScope,
): ReadableStream<Uint8Array> {
    if (!body) throw new Error("download response has no body");
    const expectedSize = claim.size_bytes,
        expectedChecksum = claim.checksum.value;
    if (!Number.isSafeInteger(expectedSize) || expectedSize < 0) throw new Error("invalid download size");
    const digest = new IncrementalChecksum(claim.checksum.algorithm);
    const reader = body.getReader();
    let count = 0,
        offset = 0,
        closed = false;
    let pending: Uint8Array | undefined;
    let onAbort: () => void;
    const cleanup = () => {
        scope.signal.removeEventListener("abort", onAbort);
        scope.close();
        reader.releaseLock();
        pending = undefined;
    };
    return new ReadableStream<Uint8Array>(
        {
            start(controller) {
                onAbort = () => {
                    if (closed) return;
                    closed = true;
                    controller.error(scope.signal.reason);
                    void reader
                        .cancel(scope.signal.reason)
                        .catch(() => {})
                        .finally(cleanup);
                };
                scope.signal.addEventListener("abort", onAbort, { once: true });
                if (scope.signal.aborted) onAbort();
            },
            async pull(controller) {
                if (closed) return;
                try {
                    scope.check();
                    while (!pending || offset === pending.length) {
                        const next = await reader.read();
                        scope.check();
                        if (next.done) {
                            if (count !== expectedSize)
                                throw new Error(`download returned ${count} bytes, expected ${expectedSize}`);
                            if (digest.finish().value !== expectedChecksum)
                                throw new Error("download checksum mismatch");
                            closed = true;
                            controller.close();
                            cleanup();
                            return;
                        }
                        pending = next.value;
                        offset = 0;
                    }
                    const chunk = pending.subarray(offset, offset + TRANSFER_CHUNK_BYTES);
                    offset += chunk.length;
                    count += chunk.length;
                    if (count > expectedSize)
                        throw new Error(`download exceeded expected size ${expectedSize}`);
                    digest.update(chunk);
                    controller.enqueue(chunk);
                } catch (error) {
                    if (closed) return;
                    closed = true;
                    controller.error(error);
                    try {
                        await reader.cancel(error);
                    } finally {
                        cleanup();
                    }
                }
            },
            async cancel(reason) {
                if (closed) return;
                closed = true;
                try {
                    await reader.cancel(reason);
                } finally {
                    cleanup();
                }
            },
        },
        { highWaterMark: 0 },
    );
}

export type UploadContent = Blob | ReadableStream<Uint8Array> | AsyncIterable<Uint8Array>;

function streamIterator(stream: ReadableStream<Uint8Array>): AsyncIterator<Uint8Array> {
    const reader = stream.getReader();
    let closed = false;
    return {
        async next() {
            const next = await reader.read();
            if (next.done) {
                closed = true;
                reader.releaseLock();
            }
            return next.done ? { done: true, value: undefined } : next;
        },
        async return() {
            if (!closed) {
                closed = true;
                try {
                    await reader.cancel();
                } finally {
                    reader.releaseLock();
                }
            }
            return { done: true, value: undefined };
        },
    };
}

async function withAbort<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
    if (signal.aborted) throw signal.reason;
    let abort: () => void;
    const cancelled = new Promise<never>((_, reject) => {
        abort = () => reject(signal.reason);
        signal.addEventListener("abort", abort, { once: true });
    });
    try {
        return await Promise.race([promise, cancelled]);
    } finally {
        signal.removeEventListener("abort", abort!);
    }
}

export class UploadSource {
    readonly expected?: number;
    private readonly iterator: AsyncIterator<Uint8Array>;
    private pending?: Uint8Array;
    private prefix = new Uint8Array(0);
    private offset = 0;
    private closed = false;
    count = 0;
    ended = false;
    digest?: IncrementalChecksum;
    limit?: number;

    constructor(
        content: UploadContent,
        private readonly scope: TransferScope,
        size?: number,
    ) {
        this.expected = size ?? ("size" in content ? content.size : undefined);
        if (this.expected !== undefined && (!Number.isSafeInteger(this.expected) || this.expected < 0))
            throw new Error("invalid upload size");
        this.iterator =
            "size" in content && "stream" in content
                ? streamIterator(content.stream())
                : "getReader" in content
                  ? streamIterator(content)
                  : content[Symbol.asyncIterator]();
    }

    async tryInline(limit: number): Promise<string | undefined> {
        const bytes = new Uint8Array(limit + 1);
        let length = 0;
        while (length < bytes.length) {
            const chunk = await this.read(bytes.length - length);
            if (chunk === undefined) return base64(bytes.subarray(0, length));
            bytes.set(chunk, length);
            length += chunk.length;
        }
        this.prefix = bytes;
        this.count = 0;
        return undefined;
    }

    async empty(): Promise<boolean> {
        if (this.prefix.length) return false;
        await this.fill();
        return this.ended;
    }

    private async fill(): Promise<void> {
        while (!this.ended && (!this.pending || this.offset === this.pending.length)) {
            this.scope.check();
            const next = await withAbort(Promise.resolve(this.iterator.next()), this.scope.signal);
            if (next.done) {
                this.ended = true;
                if (this.expected !== undefined && this.count !== this.expected)
                    throw new Error("source does not match declared size");
            } else {
                if (!(next.value instanceof Uint8Array))
                    throw new Error("upload source must yield Uint8Array chunks");
                this.pending = next.value;
                this.offset = 0;
            }
        }
    }

    async read(maximum = TRANSFER_CHUNK_BYTES): Promise<Uint8Array | undefined> {
        this.scope.check();
        let chunk: Uint8Array;
        if (this.prefix.length) {
            chunk = this.prefix.subarray(0, Math.min(maximum, TRANSFER_CHUNK_BYTES));
            this.prefix = this.prefix.subarray(chunk.length);
        } else {
            await this.fill();
            if (this.ended) return undefined;
            chunk = this.pending!.subarray(
                this.offset,
                this.offset + Math.min(maximum, TRANSFER_CHUNK_BYTES),
            );
            this.offset += chunk.length;
        }
        this.count += chunk.length;
        if (this.expected !== undefined && this.count > this.expected)
            throw new Error("source does not match declared size");
        if (this.limit !== undefined && this.count > this.limit)
            throw new Error("source exceeds advertised proxy upload limit");
        this.digest?.update(chunk);
        return chunk;
    }

    stream(): ReadableStream<Uint8Array> {
        return new ReadableStream(
            {
                pull: async (controller) => {
                    try {
                        const chunk = await this.read();
                        if (chunk) controller.enqueue(chunk);
                        else controller.close();
                    } catch (error) {
                        controller.error(error);
                    }
                },
                cancel: () => this.close(),
            },
            { highWaterMark: 0 },
        );
    }

    finish(): void {
        if (!this.ended) throw new Error("successful response before upload source reached EOF");
    }
    close(): void {
        if (this.closed) return;
        this.closed = true;
        // A caller-owned async iterator may itself be stalled; its cleanup
        // must not hold up transport cancellation indefinitely.
        void this.iterator.return?.().catch(() => {});
        this.pending = undefined;
        this.prefix = new Uint8Array(0);
    }
}

function base64(bytes: Uint8Array): string {
    let binary = "";
    for (const byte of bytes) binary += String.fromCharCode(byte);
    return btoa(binary);
}

export function appendContent(bytes: Uint8Array): string {
    if (bytes.length === 0) throw new Error("append content is empty");
    if (bytes.length > MAX_APPEND_BYTES)
        throw new Error(`${bytes.length}-byte append is larger than the ${MAX_APPEND_BYTES}-byte limit`);
    return base64(bytes);
}

export function bytesSource(bytes: Uint8Array): UploadContent {
    return {
        async *[Symbol.asyncIterator]() {
            yield bytes;
        },
    };
}

export function streamingFetch(send: typeof fetch): typeof fetch {
    return (input, init) =>
        send(
            input,
            init?.body instanceof ReadableStream ? ({ ...init, duplex: "half" } as RequestInit) : init,
        );
}

export async function uploadBody(source: UploadSource): Promise<BodyInit> {
    if (source.expected === undefined || source.expected > 8 * 1024 * 1024) return source.stream();
    const bytes = new Uint8Array(source.expected);
    let offset = 0;
    for (;;) {
        const chunk = await source.read();
        if (!chunk) return bytes;
        bytes.set(chunk, offset);
        offset += chunk.length;
    }
}

export function inlineContentLimit(capabilities: {
    features?: Record<string, boolean>;
    limits?: Record<string, number>;
}): number | undefined {
    const limit = capabilities.limits?.["commit.max_inline_content_bytes_per_operation"];
    if (
        !capabilities.features?.["filesystem.commits.inline_content"] ||
        limit === undefined ||
        !Number.isSafeInteger(limit) ||
        limit < 0
    )
        return undefined;
    return Math.min(limit, MAX_INLINE_BYTES);
}
