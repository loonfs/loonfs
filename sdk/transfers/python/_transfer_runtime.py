from __future__ import annotations

import asyncio
import contextvars
import typing

import httpx

from .types import Checksum, ChecksumAlgorithm, ContentRef

_TRANSFER_CHUNK_BYTES = 64 * 1024


def _crc_table(polynomial: int, mask: int) -> tuple[int, ...]:
    values = []
    for byte in range(256):
        value = byte
        for _ in range(8):
            value = (value >> 1) ^ polynomial if value & 1 else value >> 1
        values.append(value & mask)
    return tuple(values)


_CRC64_NVME_MASK = (1 << 64) - 1
_CRC32C_MASK = (1 << 32) - 1
_CRC64_NVME_TABLE = _crc_table(0x9A6C9329AC4BC9B5, _CRC64_NVME_MASK)
_CRC32C_TABLE = _crc_table(0x82F63B78, _CRC32C_MASK)


class _IncrementalChecksum:
    def __init__(self, algorithm: ChecksumAlgorithm):
        self.algorithm = algorithm
        if algorithm == "crc32c":
            self.value, self.table, self.mask = (
                _CRC32C_MASK,
                _CRC32C_TABLE,
                _CRC32C_MASK,
            )
        elif algorithm == "crc64nvme":
            self.value, self.table, self.mask = (
                _CRC64_NVME_MASK,
                _CRC64_NVME_TABLE,
                _CRC64_NVME_MASK,
            )
        else:
            raise ValueError(f"unsupported checksum algorithm {algorithm}")

    def update(self, content: bytes) -> None:
        for byte in content:
            self.value = self.table[(self.value ^ byte) & 0xFF] ^ (self.value >> 8)

    def finish(self) -> Checksum:
        value = format(
            self.value ^ self.mask, "08x" if self.algorithm == "crc32c" else "016x"
        )
        return Checksum(algorithm=self.algorithm, value=value)


def _checksum(algorithm: ChecksumAlgorithm, content: bytes) -> Checksum:
    digest = _IncrementalChecksum(algorithm)
    digest.update(content)
    return digest.finish()


class _DownloadVerification:
    def __init__(self, content_ref: ContentRef):
        self.expected_size = content_ref.size_bytes
        if self.expected_size < 0:
            raise ValueError("invalid download size")
        self.expected_checksum = content_ref.checksum.value
        self.digest = _IncrementalChecksum(content_ref.checksum.algorithm)
        self.count = 0

    def update(self, chunk: bytes) -> None:
        self.count += len(chunk)
        if self.count > self.expected_size:
            raise RuntimeError(f"download exceeded expected size {self.expected_size}")
        self.digest.update(chunk)

    def finish(self) -> None:
        if self.count != self.expected_size:
            raise RuntimeError(
                f"download returned {self.count} bytes, expected {self.expected_size}"
            )
        if self.digest.finish().value != self.expected_checksum:
            raise RuntimeError("download checksum mismatch")


def _validate_download_ranges(ranges, size_bytes):
    if not ranges or (size_bytes == 0 and len(ranges) != 1):
        raise RuntimeError("download grant has invalid ranges")
    offset = 0
    for part in ranges:
        if (
            part.start_offset != offset
            or part.length < 0
            or part.length > size_bytes - offset
            or (part.length == 0 and size_bytes != 0)
        ):
            raise RuntimeError("download grant has invalid ranges")
        if part.access.method != "GET":
            raise RuntimeError("download grant must use GET")
        offset += part.length
    if offset != size_bytes:
        raise RuntimeError("download grant has invalid ranges")


def _download_range_request(part, timeout):
    return httpx.Request(
        "GET",
        part.access.url,
        headers=part.access.headers or {},
        extensions={"timeout": httpx.Timeout(timeout).as_dict()},
    )


def _download_ranges(client, ranges, timeout):
    for part in ranges:
        if part.length == 0:
            continue
        response = client.send(
            _download_range_request(part, timeout),
            stream=True,
            auth=None,
            follow_redirects=False,
        )
        try:
            _require_success(response)
            count = 0
            for chunk in response.iter_bytes(chunk_size=_TRANSFER_CHUNK_BYTES):
                count += len(chunk)
                if count > part.length:
                    raise RuntimeError("download range exceeded its declared length")
                yield chunk
            if count != part.length:
                raise RuntimeError("download range ended before its declared length")
        finally:
            response.close()


async def _async_download_ranges(client, ranges, timeout):
    for part in ranges:
        if part.length == 0:
            continue
        response = await client.send(
            _download_range_request(part, timeout),
            stream=True,
            auth=None,
            follow_redirects=False,
        )
        try:
            _require_success(response)
            count = 0
            async for chunk in response.aiter_bytes(chunk_size=_TRANSFER_CHUNK_BYTES):
                count += len(chunk)
                if count > part.length:
                    raise RuntimeError("download range exceeded its declared length")
                yield chunk
            if count != part.length:
                raise RuntimeError("download range ended before its declared length")
        finally:
            await response.aclose()


class _AsyncReader:
    def __init__(self, content: typing.AsyncIterator[bytes] | typing.BinaryIO):
        self._content = content
        self._iterator = content.__aiter__() if hasattr(content, "__aiter__") else None
        self._pending = memoryview(b"")

    async def read(self, size: int) -> bytes:
        size = min(size, _TRANSFER_CHUNK_BYTES)
        if self._iterator is None:
            return await asyncio.get_running_loop().run_in_executor(
                None, contextvars.copy_context().run, self._content.read, size
            )
        while not self._pending:
            try:
                chunk = await self._iterator.__anext__()
            except StopAsyncIteration:
                return b""
            if not isinstance(chunk, bytes):
                raise TypeError("upload source must return bytes")
            self._pending = memoryview(chunk)
        chunk = bytes(self._pending[:size])
        self._pending = self._pending[size:]
        return chunk


def _send_stream_presigned(client, access, content, timeout, size_bytes=None):
    request = _upload_request(access, content, timeout, size_bytes)
    response = client.send(request, stream=True, auth=None, follow_redirects=False)
    try:
        _require_success(response)
        return response.headers.get("etag")
    finally:
        response.close()


async def _async_send_stream_presigned(
    client, access, content, timeout, size_bytes=None
):
    request = _upload_request(access, content, timeout, size_bytes)
    response = await client.send(
        request, stream=True, auth=None, follow_redirects=False
    )
    try:
        _require_success(response)
        return response.headers.get("etag")
    finally:
        await response.aclose()


class _UploadState:
    def __init__(self, reader, prefix, expected):
        self.reader, self.prefix, self.expected = reader, prefix, expected
        self.count = 0
        self.ended = False
        self.limit = None
        self.digest = None

    def accept(self, chunk):
        if not isinstance(chunk, bytes):
            raise TypeError("upload source must return bytes")
        self.count += len(chunk)
        if self.expected is not None and (
            self.count > self.expected or (not chunk and self.count != self.expected)
        ):
            raise ValueError("source does not match declared size")
        if self.limit is not None and self.count > self.limit:
            raise ValueError("source exceeds advertised proxy upload limit")
        if self.digest is not None:
            self.digest.update(chunk)
        self.ended = not chunk
        return chunk

    def finish(self):
        if not self.ended:
            raise RuntimeError("successful response before upload source reached EOF")


class _UploadSource(_UploadState):
    def read(self, size):
        if self.ended:
            return b""
        size = min(size, _TRANSFER_CHUNK_BYTES)
        if self.prefix:
            chunk, self.prefix = self.prefix[:size], self.prefix[size:]
        else:
            chunk = self.reader.read(size)
        return self.accept(chunk)

    def chunks(self):
        while True:
            chunk = self.read(_TRANSFER_CHUNK_BYTES)
            if not chunk:
                return
            yield chunk


class _AsyncUploadSource(_UploadState):
    async def read(self, size):
        if self.ended:
            return b""
        size = min(size, _TRANSFER_CHUNK_BYTES)
        if self.prefix:
            chunk, self.prefix = self.prefix[:size], self.prefix[size:]
        else:
            chunk = await self.reader.read(size)
        return self.accept(chunk)

    async def chunks(self):
        while True:
            chunk = await self.read(_TRANSFER_CHUNK_BYTES)
            if not chunk:
                return
            yield chunk


def _upload_request(access, content, timeout, size_bytes):
    if access.method != "PUT":
        raise RuntimeError("upload grant must use PUT")
    headers = httpx.Headers(access.headers or {})
    if size_bytes is not None:
        headers["Content-Length"] = str(size_bytes)
    return httpx.Request(
        "PUT",
        access.url,
        headers=headers,
        content=content,
        extensions={"timeout": httpx.Timeout(timeout).as_dict()},
    )


def _require_success(response):
    if not response.is_success:
        raise httpx.HTTPStatusError(
            f"presigned request failed with HTTP {response.status_code}",
            request=response.request,
            response=response,
        )
