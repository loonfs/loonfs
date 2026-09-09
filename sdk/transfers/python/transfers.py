"""Synchronous streaming transfers and buffered convenience methods."""

from __future__ import annotations

import hashlib
import io
import typing
from dataclasses import dataclass

import httpx

from .client import LoonFS as _GeneratedLoonFS
from .files.client import FilesClient as _GeneratedFilesClient
from .core.request_options import RequestOptions
from .types import (
    ActorId,
    BeginUploadRequest_DirectMultipart,
    BeginUploadRequest_DirectPut,
    BeginUploadRequest_ServiceProxied,
    Checksum,
    CompletedUploadPart,
    ContentRef,
    ContentToken,
    DestinationBehavior,
    FilesystemOperation_PutFile,
    ObjectTransferAccess,
    RevisionNo,
    UploadCompletion_DirectMultipart,
    UploadCompletion_DirectPut,
    UploadCompletion_ServiceProxied,
    UploadContentClaim,
    UploadPartChecksumClaim,
    UploadSession,
)

_MULTIPART_MIN_BYTES = 8 * 1024 * 1024
_DIRECT_GET_FEATURE = "filesystem.downloads.direct_get"
_DIRECT_MULTIPART_FEATURE = "filesystem.uploads.direct_multipart"
_DIRECT_PUT_FEATURE = "filesystem.uploads.direct_put"
_PROXY_UPLOAD_LIMIT = "upload.max_content_bytes"
_DIRECT_PUT_LIMIT = "upload.direct_put_max_content_bytes"


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


@dataclass(frozen=True)
class FileUploadResult:
    """The identity and sequence of the commit that stored the file."""

    namespace_id: str
    commit_id: str
    committed_seq: int


@dataclass(frozen=True)
class FileDownloadResult:
    """Downloaded bytes and the immutable revision facts from its grant."""

    content: bytes
    namespace_id: str
    path: str
    revision_no: int
    content_ref: ContentRef


@dataclass(frozen=True)
class PreparedFileContent:
    """Completed content retained for repeated publication of the same request.

    Preparation does not publish a file or extend the upload lifetime.
    Treat the content reference and token as immutable.
    """

    content_ref: ContentRef
    content_token: ContentToken | None


_TRANSFER_CHUNK_BYTES = 64 * 1024


class _IncrementalChecksum:
    def __init__(self, algorithm: str):
        self.algorithm = algorithm
        self.sha = hashlib.sha256() if algorithm == "sha256" else None
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
        elif algorithm != "sha256":
            raise ValueError(f"unsupported checksum algorithm {algorithm!r}")

    def update(self, content: bytes) -> None:
        if self.sha is not None:
            self.sha.update(content)
        else:
            for byte in content:
                self.value = self.table[(self.value ^ byte) & 0xFF] ^ (self.value >> 8)

    def finish(self) -> Checksum:
        value = (
            self.sha.hexdigest()
            if self.sha is not None
            else format(
                self.value ^ self.mask, "08x" if self.algorithm == "crc32c" else "016x"
            )
        )
        return Checksum(algorithm=self.algorithm, value=value)


class FileDownloadStream(typing.Iterator[bytes]):
    """A single-use verified iterator. Close or leave its with block to cancel.

    A caller that stops early has not verified the complete file. Bytes already
    consumed cannot be recalled if a checksum or transport error occurs later.
    """

    def __init__(self, chunks, close, namespace_id, path, revision_no, content_ref):
        self._chunks, self._close = iter(chunks), close
        self.namespace_id, self.path, self.revision_no = namespace_id, path, revision_no
        self.content_ref = content_ref
        self._checksum = _IncrementalChecksum(content_ref.checksum.algorithm)
        self._expected_checksum = content_ref.checksum.value
        self._expected_size = content_ref.size_bytes
        self._count = 0
        self._closed = False
        self._verified = False
        self._terminal_error = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        if self._terminal_error is not None:
            raise self._terminal_error
        if self._closed:
            if self._verified:
                raise StopIteration
            raise ValueError("download stream was closed before verification")
        try:
            chunk = next(self._chunks)
            self._count += len(chunk)
            if self._count > self._expected_size:
                raise RuntimeError(
                    f"download exceeded expected size {self._expected_size}"
                )
            self._checksum.update(chunk)
            return chunk
        except StopIteration:
            try:
                if self._count != self._expected_size:
                    raise RuntimeError(
                        f"download returned {self._count} bytes, expected {self._expected_size}"
                    )
                if self._checksum.finish().value != self._expected_checksum:
                    raise RuntimeError(
                        "download checksum did not match its content reference"
                    )
                self._verified = True
            except BaseException as error:
                self._terminal_error = error
                raise
            finally:
                self.close()
            raise
        except BaseException as error:
            self._terminal_error = error
            self.close()
            raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._close()


class FilesClient(_GeneratedFilesClient):
    """The files group plus streaming and buffered transfers."""

    def __init__(self, *, client_wrapper, root: "LoonFS") -> None:
        super().__init__(client_wrapper=client_wrapper)
        self._root = root

    def upload(
        self,
        namespace_id: str,
        *,
        path: str,
        content: bytes,
        actor: ActorId,
        commit_id: str,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: str | None = None,
        expected_revision_no: RevisionNo | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> FileUploadResult:
        """Upload fresh bytes through upload_stream."""
        return self.upload_stream(
            namespace_id,
            path=path,
            content=io.BytesIO(content),
            size_bytes=len(content),
            actor=actor,
            commit_id=commit_id,
            message=message,
            behavior=behavior,
            expected_inode_id=expected_inode_id,
            expected_revision_no=expected_revision_no,
            http_client=http_client,
            request_options=request_options,
        )

    def upload_stream(
        self,
        namespace_id: str,
        *,
        path: str,
        content: typing.BinaryIO,
        size_bytes: int | None = None,
        actor: ActorId,
        commit_id: str,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: str | None = None,
        expected_revision_no: RevisionNo | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> FileUploadResult:
        """Consume a source once and publish it; the caller owns the source."""
        prepared = self.prepare_file_stream(
            namespace_id,
            content=content,
            size_bytes=size_bytes,
            http_client=http_client,
            request_options=request_options,
        )
        return self.put_file_prepared(
            namespace_id,
            path=path,
            prepared=prepared,
            actor=actor,
            commit_id=commit_id,
            message=message,
            behavior=behavior,
            expected_inode_id=expected_inode_id,
            expected_revision_no=expected_revision_no,
            request_options=request_options,
        )

    def prepare_file_bytes(
        self,
        namespace_id: str,
        *,
        content: bytes,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> PreparedFileContent:
        """Stage the streaming path for an existing byte buffer."""
        return self.prepare_file_stream(
            namespace_id,
            content=io.BytesIO(content),
            size_bytes=len(content),
            http_client=http_client,
            request_options=request_options,
        )

    def prepare_file_stream(
        self,
        namespace_id: str,
        *,
        content: typing.BinaryIO,
        size_bytes: int | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> PreparedFileContent:
        """Stage once with bounded memory; retain the result for publication retries.

        The caller owns content. Payload requests are never retried. A known
        size validates the source and selects the usual small-file transport.
        """
        if size_bytes is not None and size_bytes < 0:
            raise ValueError("size_bytes must be nonnegative")
        capabilities = self._root.capabilities.retrieve(request_options=request_options)
        first = b""
        if size_bytes is None:
            first = content.read(1)
            if not first:
                size_bytes = 0
        source = _UploadSource(content, first, size_bytes)
        begin = _create_upload(
            self._root, namespace_id, capabilities, size_bytes, request_options
        )
        client = http_client or self._root._client_wrapper.httpx_client.httpx_client
        timeout = (request_options or {}).get(
            "timeout", self._root._client_wrapper.get_timeout()
        )
        options = {**(request_options or {}), "max_retries": 0}
        try:
            if begin.mode == "service_proxied":
                source.limit = (capabilities.limits or {}).get(_PROXY_UPLOAD_LIMIT)
                self._root.uploads.put_content(
                    namespace_id,
                    begin.upload_id,
                    request=source.chunks(),
                    request_options=options,
                )
                completion = UploadCompletion_ServiceProxied()
            elif begin.mode == "direct_put":
                source.digest = _IncrementalChecksum(begin.checksum_algorithm)
                _send_stream_presigned(
                    client, begin.access, source.chunks(), timeout, size_bytes
                )
                completion = UploadCompletion_DirectPut(
                    content=UploadContentClaim(
                        size_bytes=source.count, checksum=source.digest.finish()
                    )
                )
            elif begin.mode == "direct_multipart":
                completion = _stream_multipart(
                    self._root, client, namespace_id, begin, source, timeout, options
                )
            else:
                raise RuntimeError(f"unsupported upload mode {begin.mode!r}")
            source.finish()
        except BaseException:
            _abort_quietly(self._root, namespace_id, begin.upload_id)
            raise
        # Keep a session whose completion response was lost available for inspection.
        completed = self._root.uploads.complete(
            namespace_id, begin.upload_id, request=completion, request_options=options
        )
        result = _completed_content(completed)
        if result.content_ref.size_bytes != source.count:
            raise RuntimeError("completed upload size mismatch")
        return result

    def put_file_prepared(
        self,
        namespace_id: str,
        *,
        path: str,
        prepared: PreparedFileContent,
        actor: ActorId,
        commit_id: str,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: str | None = None,
        expected_revision_no: RevisionNo | None = None,
        request_options: RequestOptions | None = None,
    ) -> FileUploadResult:
        """Publish retained content; reuse it with identical inputs to retry safely."""
        operation_arguments = {"path": path, "content_ref": prepared.content_ref}
        if behavior is not None:
            operation_arguments["behavior"] = behavior
        if expected_inode_id is not None:
            operation_arguments["expected_inode_id"] = expected_inode_id
        if expected_revision_no is not None:
            operation_arguments["expected_revision_no"] = expected_revision_no
        operation = FilesystemOperation_PutFile(**operation_arguments)
        commit_arguments = {
            "actor": actor,
            "commit_id": commit_id,
            "operations": [operation],
            "content_tokens": [prepared.content_token]
            if prepared.content_token is not None
            else [],
        }
        if message is not None:
            commit_arguments["message"] = message
        committed = self._root.commits.create(
            namespace_id, request_options=request_options, **commit_arguments
        )
        return FileUploadResult(
            namespace_id=committed.namespace_id,
            commit_id=committed.commit_id,
            committed_seq=committed.committed_seq,
        )

    def download_stream(
        self,
        namespace_id: str,
        *,
        path: str,
        revision_no: RevisionNo | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> FileDownloadStream:
        """Open a verified stream; use a with block to close on early exit.

        Size and checksum verification complete only at successful exhaustion.
        Request timeouts apply to metadata and payload I/O on either transport.
        """
        capabilities = self._root.capabilities.retrieve(request_options=request_options)
        if not (capabilities.features or {}).get(_DIRECT_GET_FEATURE, False):
            claim, revision_no = _proxied_claim(
                self._root, namespace_id, path, revision_no, request_options
            )
            chunks = self._root.files.content(
                namespace_id,
                path=path,
                revision_no=revision_no,
                request_options={
                    **(request_options or {}),
                    "chunk_size": _TRANSFER_CHUNK_BYTES,
                },
            )
            return FileDownloadStream(
                chunks, chunks.close, namespace_id, path, revision_no, claim
            )
        grant = self.create_download(
            namespace_id,
            path=path,
            revision_no=revision_no,
            request_options=request_options,
        )
        if grant.access.method.upper() != "GET":
            raise RuntimeError("download grant must use GET")
        client = http_client or self._root._client_wrapper.httpx_client.httpx_client
        timeout = (request_options or {}).get(
            "timeout", self._root._client_wrapper.get_timeout()
        )
        # Construct a fresh request: SDK authorization, cookies and custom
        # API headers must never be forwarded to the object-store capability.
        request = httpx.Request(
            "GET",
            grant.access.url,
            headers=grant.access.headers or {},
            extensions={"timeout": httpx.Timeout(timeout).as_dict()},
        )
        response = client.send(request, stream=True, auth=None, follow_redirects=False)
        try:
            response.raise_for_status()
            return FileDownloadStream(
                response.iter_bytes(chunk_size=_TRANSFER_CHUNK_BYTES),
                response.close,
                grant.namespace_id,
                grant.path,
                grant.revision_no,
                grant.content_ref,
            )
        except BaseException:
            response.close()
            raise

    def download(
        self,
        namespace_id: str,
        *,
        path: str,
        revision_no: RevisionNo | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> FileDownloadResult:
        """Collect download_stream for callers that want all bytes in memory."""
        with self.download_stream(
            namespace_id,
            path=path,
            revision_no=revision_no,
            http_client=http_client,
            request_options=request_options,
        ) as stream:
            content = b"".join(stream)
            return FileDownloadResult(
                content=content,
                namespace_id=stream.namespace_id,
                path=stream.path,
                revision_no=stream.revision_no,
                content_ref=stream.content_ref,
            )


class LoonFS(_GeneratedLoonFS):
    """The generated client with ``files.upload`` and ``files.download``."""

    _transfer_files: typing.Optional[FilesClient] = None

    @property
    def files(self) -> FilesClient:
        if self._transfer_files is None:
            self._transfer_files = FilesClient(
                client_wrapper=self._client_wrapper, root=self
            )
        return self._transfer_files


__all__ = [
    "FileDownloadResult",
    "FileDownloadStream",
    "FileUploadResult",
    "PreparedFileContent",
    "FilesClient",
    "LoonFS",
]


def _proxied_claim(client, namespace_id, path, revision_no, request_options):
    if revision_no is None:
        entry = client.files.retrieve(
            namespace_id, path=path, request_options=request_options
        )
        if entry.inode_kind != "file":
            raise RuntimeError(f"path {path!r} is a {entry.inode_kind}, not a file")
        return entry.content_ref, entry.revision_no
    cursor = None
    while True:
        page = client.files.list_revisions(
            namespace_id, path=path, cursor=cursor, request_options=request_options
        )
        for revision in page.revisions:
            if revision.revision_no == revision_no:
                return revision.content_ref, revision_no
        if page.next_cursor is None:
            raise RuntimeError(f"revision {revision_no} not found for {path!r}")
        cursor = page.next_cursor


class _UploadSource:
    def __init__(self, reader, prefix, expected):
        self.reader, self.prefix, self.expected = reader, prefix, expected
        self.count = 0
        self.ended = False
        self.limit = None
        self.digest = None

    def read(self, size):
        if self.ended:
            return b""
        size = min(size, _TRANSFER_CHUNK_BYTES)
        if self.prefix:
            chunk, self.prefix = self.prefix[:size], self.prefix[size:]
        else:
            chunk = self.reader.read(size)
        if not isinstance(chunk, bytes):
            raise TypeError("upload source must return bytes")
        self.count += len(chunk)
        if self.limit is not None and self.count > self.limit:
            raise ValueError("source exceeds advertised proxy upload limit")
        if self.expected is not None and (
            self.count > self.expected or (not chunk and self.count != self.expected)
        ):
            raise ValueError("source does not match declared size")
        if self.digest is not None:
            self.digest.update(chunk)
        if not chunk:
            self.ended = True
        return chunk

    def chunks(self):
        while True:
            chunk = self.read(_TRANSFER_CHUNK_BYTES)
            if not chunk:
                return
            yield chunk

    def finish(self):
        if not self.ended:
            raise RuntimeError("successful response before upload source reached EOF")


def _create_upload(client, namespace_id, capabilities, size_bytes, request_options):
    features, limits = capabilities.features or {}, capabilities.limits or {}
    if (size_bytes is None or size_bytes >= _MULTIPART_MIN_BYTES) and features.get(
        _DIRECT_MULTIPART_FEATURE, False
    ):
        request = BeginUploadRequest_DirectMultipart()
    else:
        proxy_limit = limits.get(_PROXY_UPLOAD_LIMIT)
        fits_proxy = (
            size_bytes is None or proxy_limit is None or size_bytes <= proxy_limit
        )
        direct_limit = limits.get(_DIRECT_PUT_LIMIT)
        fits_direct = size_bytes is not None and (
            direct_limit is None or size_bytes <= direct_limit
        )
        if (
            features.get(_DIRECT_PUT_FEATURE, False)
            and fits_direct
            and (size_bytes >= _MULTIPART_MIN_BYTES or not fits_proxy)
        ):
            request = BeginUploadRequest_DirectPut(size_bytes=size_bytes)
        elif fits_proxy:
            request = BeginUploadRequest_ServiceProxied()
        else:
            raise ValueError("source fits no advertised upload transport")
    return client.uploads.create(
        namespace_id, request=request, request_options=request_options
    )


def _send_stream_presigned(client, access, content, timeout, size_bytes=None):
    if access.method.upper() != "PUT":
        raise RuntimeError("upload grant must use PUT")
    headers = httpx.Headers(access.headers or {})
    if size_bytes is not None:
        headers["Content-Length"] = str(size_bytes)
    request = httpx.Request(
        "PUT",
        access.url,
        headers=headers,
        content=content,
        extensions={"timeout": httpx.Timeout(timeout).as_dict()},
    )
    response = client.send(request, stream=True, auth=None, follow_redirects=False)
    try:
        response.raise_for_status()
        return response.headers.get("etag")
    finally:
        response.close()


def _stream_multipart(
    client, http, namespace_id, begin, source, timeout, request_options
):
    part_size = begin.part_size_bytes
    if part_size <= 0:
        raise RuntimeError("multipart part size must be positive")
    source.digest = _IncrementalChecksum(begin.checksum_algorithm)
    completed_parts = []
    while True:
        part = bytearray()
        while len(part) < part_size:
            chunk = source.read(part_size - len(part))
            if not chunk:
                break
            part.extend(chunk)
        if not part:
            break
        if len(completed_parts) == 10000:
            raise ValueError("multipart upload exceeds 10000 parts")
        number = len(completed_parts) + 1
        checksum = _checksum(begin.checksum_algorithm, part)
        signed = client.uploads.sign_parts(
            namespace_id,
            begin.upload_id,
            parts=[UploadPartChecksumClaim(part_number=number, checksum=checksum)],
            request_options=request_options,
        )
        if len(signed.parts) != 1 or signed.parts[0].part_number != number:
            raise RuntimeError(f"server did not sign requested part {number}")
        etag = _send_stream_presigned(
            http, signed.parts[0].access, bytes(part), timeout
        )
        if not etag:
            raise RuntimeError(f"part {number} returned no ETag")
        completed_parts.append(
            CompletedUploadPart(part_number=number, etag=etag, checksum=checksum)
        )
    return UploadCompletion_DirectMultipart(
        content=UploadContentClaim(
            size_bytes=source.count, checksum=source.digest.finish()
        ),
        parts=completed_parts,
    )


def _completed_content(response: UploadSession) -> PreparedFileContent:
    if response.status != "completed":
        raise RuntimeError(
            f"upload {response.upload_id!r} completed with status {response.status!r}"
        )
    return PreparedFileContent(
        content_ref=response.content_ref,
        content_token=response.content_token,
    )


def _abort_quietly(client, namespace_id, upload_id):
    try:
        client.uploads.abort(
            namespace_id, upload_id, request_options={"timeout": 5, "max_retries": 0}
        )
    except Exception:
        pass


def _checksum(algorithm: str, content: bytes) -> Checksum:
    digest = _IncrementalChecksum(algorithm)
    digest.update(content)
    return digest.finish()
