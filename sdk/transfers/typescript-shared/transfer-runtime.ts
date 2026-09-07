/** Shared by server and browser transfers; no Node-only APIs or whole-file hashing. */
export type ChecksumAlgorithm = "sha256" | "crc32c" | "crc64nvme";
export const TRANSFER_CHUNK_BYTES = 64 * 1024;

export interface TransferRequestOptions {
    timeoutInSeconds?: number;
    abortSignal?: AbortSignal;
}

/** One deadline and cancellation signal, retained until the body is finished. */
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
        else this.parent?.addEventListener("abort", this.onParentAbort, { once: true });
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

const SHA256_K = new Uint32Array([
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);
const rotate = (value: number, bits: number) => (value >>> bits) | (value << (32 - bits));

/** SHA-256 compression and padding with a single retained 64-byte block. */
class Sha256 {
    private readonly state = new Uint32Array([
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ]);
    private readonly words = new Uint32Array(64);
    private readonly block = new Uint8Array(64);
    private used = 0;
    private bytes = 0;
    update(input: Uint8Array): void {
        this.bytes += input.length;
        let offset = 0;
        while (offset < input.length) {
            const count = Math.min(64 - this.used, input.length - offset);
            this.block.set(input.subarray(offset, offset + count), this.used);
            offset += count;
            this.used += count;
            if (this.used === 64) {
                this.compress(this.block);
                this.used = 0;
            }
        }
    }
    private compress(block: Uint8Array): void {
        const w = this.words;
        const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
        for (let i = 0; i < 16; i++) w[i] = view.getUint32(i * 4);
        for (let i = 16; i < 64; i++) {
            const x = w[i - 15]!,
                y = w[i - 2]!;
            w[i] =
                (w[i - 16]! +
                    (rotate(x, 7) ^ rotate(x, 18) ^ (x >>> 3)) +
                    w[i - 7]! +
                    (rotate(y, 17) ^ rotate(y, 19) ^ (y >>> 10))) >>>
                0;
        }
        let [a, b, c, d, e, f, g, h] = Array.from(this.state) as [
            number,
            number,
            number,
            number,
            number,
            number,
            number,
            number,
        ];
        for (let i = 0; i < 64; i++) {
            const t1 =
                (h +
                    (rotate(e, 6) ^ rotate(e, 11) ^ rotate(e, 25)) +
                    ((e & f) ^ (~e & g)) +
                    SHA256_K[i]! +
                    w[i]!) >>>
                0;
            const t2 = ((rotate(a, 2) ^ rotate(a, 13) ^ rotate(a, 22)) + ((a & b) ^ (a & c) ^ (b & c))) >>> 0;
            h = g;
            g = f;
            f = e;
            e = (d + t1) >>> 0;
            d = c;
            c = b;
            b = a;
            a = (t1 + t2) >>> 0;
        }
        [a, b, c, d, e, f, g, h].forEach((value, i) => {
            this.state[i] = (this.state[i]! + value) >>> 0;
        });
    }
    finish(): string {
        const padding = new Uint8Array(this.used < 56 ? 64 : 128);
        padding.set(this.block.subarray(0, this.used));
        padding[this.used] = 0x80;
        new DataView(padding.buffer).setBigUint64(padding.length - 8, BigInt(this.bytes) * 8n);
        this.compress(padding.subarray(0, 64));
        if (padding.length === 128) this.compress(padding.subarray(64));
        return Array.from(this.state, (value) => value.toString(16).padStart(8, "0")).join("");
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
    private readonly sha?: Sha256;
    private crc32 = 0xffffffff;
    private crc64 = 0xffffffffffffffffn;
    private result?: string;
    constructor(readonly algorithm: ChecksumAlgorithm) {
        if (algorithm === "sha256") this.sha = new Sha256();
        else if (algorithm !== "crc32c" && algorithm !== "crc64nvme")
            throw new Error(`unsupported checksum algorithm ${algorithm}`);
    }
    update(bytes: Uint8Array): void {
        if (this.result !== undefined) throw new Error("checksum already finalized");
        if (this.sha) this.sha.update(bytes);
        else if (this.algorithm === "crc32c") {
            for (const byte of bytes) this.crc32 = CRC32[(this.crc32 ^ byte) & 255]! ^ (this.crc32 >>> 8);
        } else {
            for (const byte of bytes)
                this.crc64 = CRC64[Number((this.crc64 ^ BigInt(byte)) & 255n)]! ^ (this.crc64 >> 8n);
        }
    }
    finish(): { algorithm: ChecksumAlgorithm; value: string } {
        this.result ??= this.sha
            ? this.sha.finish()
            : this.algorithm === "crc32c"
              ? ((this.crc32 ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0")
              : (this.crc64 ^ 0xffffffffffffffffn).toString(16).padStart(16, "0");
        return { algorithm: this.algorithm, value: this.result };
    }
}

/** Verification belongs to EOF, on the same stream as the bytes it checks. */
export function verifiedDownload(
    body: ReadableStream<Uint8Array> | null,
    claim: { size_bytes: number; checksum: { algorithm: ChecksumAlgorithm; value: string } },
    scope: TransferScope,
): ReadableStream<Uint8Array> {
    if (!body) throw new Error("download response has no body");
    const digest = new IncrementalChecksum(claim.checksum.algorithm);
    const expectedSize = claim.size_bytes,
        expectedChecksum = claim.checksum.value;
    if (!Number.isSafeInteger(expectedSize) || expectedSize < 0) throw new Error("invalid download size");
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
