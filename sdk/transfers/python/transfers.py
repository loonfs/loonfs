"""Synchronous file transfers with verified streaming downloads."""

from __future__ import annotations

import hashlib
import typing
from dataclasses import dataclass

import httpx

from .client import LoonFS as _GeneratedLoonFS
from .files.client import FilesClient as _GeneratedFilesClient
from .core.request_options import RequestOptions
from .types import (
    ActorRef,
    BeginUploadRequest_DirectMultipart,
    BeginUploadRequest_DirectPut,
    BeginUploadRequest_ServiceProxied,
    Checksum,
    CompletedUploadPart,
    ContentRef,
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
    content_token: str | None


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
    """The files group plus whole-file transfers."""

    def __init__(self, *, client_wrapper, root: "LoonFS") -> None:
        super().__init__(client_wrapper=client_wrapper)
        self._root = root

    def upload(
        self,
        namespace_id: str,
        *,
        path: str,
        content: bytes,
        actor: ActorRef,
        commit_id: str,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: str | None = None,
        expected_revision_no: RevisionNo | None = None,
        http_client: httpx.Client | None = None,
    ) -> FileUploadResult:
        """Upload fresh content and publish it.

        For publication retries, retain prepare_file_bytes() output and call
        put_file_prepared() with unchanged inputs.
        """

        prepared = self.prepare_file_bytes(
            namespace_id, content=content, http_client=http_client
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
        )

    def prepare_file_bytes(
        self,
        namespace_id: str,
        *,
        content: bytes,
        http_client: httpx.Client | None = None,
    ) -> PreparedFileContent:
        """Upload bytes once without publishing; retain the result for retries."""
        begin = _create_upload(self._root, namespace_id, content)
        if http_client is None:
            with httpx.Client() as transfer_client:
                staged = _stage_upload(
                    self._root, transfer_client, namespace_id, begin, content
                )
        else:
            staged = _stage_upload(
                self._root, http_client, namespace_id, begin, content
            )

        return staged

    def put_file_prepared(
        self,
        namespace_id: str,
        *,
        path: str,
        prepared: PreparedFileContent,
        actor: ActorRef,
        commit_id: str,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: str | None = None,
        expected_revision_no: RevisionNo | None = None,
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
        committed = self._root.commits.create(namespace_id, **commit_arguments)
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


def _create_upload(client: _GeneratedLoonFS, namespace_id: str, content: bytes):
    capabilities = client.capabilities.retrieve()
    features = capabilities.features or {}
    limits = capabilities.limits or {}
    size_bytes = len(content)
    if size_bytes >= _MULTIPART_MIN_BYTES and features.get(
        _DIRECT_MULTIPART_FEATURE, False
    ):
        request = BeginUploadRequest_DirectMultipart()
    else:
        proxy_limit = limits.get(_PROXY_UPLOAD_LIMIT)
        fits_proxy = proxy_limit is None or size_bytes <= proxy_limit
        direct_put_limit = limits.get(_DIRECT_PUT_LIMIT)
        fits_direct_put = direct_put_limit is None or size_bytes <= direct_put_limit
        if (
            features.get(_DIRECT_PUT_FEATURE, False)
            and fits_direct_put
            and (size_bytes >= _MULTIPART_MIN_BYTES or not fits_proxy)
        ):
            request = BeginUploadRequest_DirectPut(size_bytes=size_bytes)
        elif fits_proxy:
            request = BeginUploadRequest_ServiceProxied()
        else:
            raise ValueError(
                f"{size_bytes} bytes exceed the advertised proxy and direct PUT limits, "
                "and direct multipart is unavailable"
            )
    return client.uploads.create(namespace_id, request=request)


def _stage_upload(
    client: _GeneratedLoonFS,
    transfer_client: httpx.Client,
    namespace_id: str,
    begin,
    content: bytes,
) -> PreparedFileContent:
    if begin.mode == "service_proxied":
        try:
            client.uploads.put_content(namespace_id, begin.upload_id, request=content)
            completion = client.uploads.complete(
                namespace_id,
                begin.upload_id,
                request=UploadCompletion_ServiceProxied(),
            )
        except Exception:
            _abort_quietly(client, namespace_id, begin.upload_id)
            raise
        return _completed_content(completion)
    if begin.mode == "direct_put":
        try:
            _send_presigned(
                transfer_client,
                begin.access,
                "PUT",
                content=content,
            )
        except Exception:
            _abort_quietly(client, namespace_id, begin.upload_id)
            raise
        completion = client.uploads.complete(
            namespace_id,
            begin.upload_id,
            request=UploadCompletion_DirectPut(
                content=UploadContentClaim(
                    size_bytes=len(content),
                    checksum=_checksum(begin.checksum_algorithm, content),
                )
            ),
        )
        return _completed_content(completion)
    if begin.mode == "direct_multipart":
        return _stage_multipart(
            client,
            transfer_client,
            namespace_id,
            begin.upload_id,
            begin.part_size_bytes,
            begin.checksum_algorithm,
            content,
        )
    raise RuntimeError(f"unsupported upload mode {begin.mode!r}")


def _stage_multipart(
    client: _GeneratedLoonFS,
    transfer_client: httpx.Client,
    namespace_id: str,
    upload_id: str,
    part_size_bytes: int,
    checksum_algorithm: str,
    content: bytes,
) -> PreparedFileContent:
    if part_size_bytes <= 0:
        raise RuntimeError("multipart response returned a non-positive part size")
    parts = [
        content[offset : offset + part_size_bytes]
        for offset in range(0, len(content), part_size_bytes)
    ]
    claims = [
        UploadPartChecksumClaim(
            part_number=index,
            checksum=_checksum(checksum_algorithm, part),
        )
        for index, part in enumerate(parts, start=1)
    ]
    try:
        signed = client.uploads.sign_parts(
            namespace_id,
            upload_id,
            parts=claims,
        )
        access_by_part = {part.part_number: part.access for part in signed.parts}
        completed_parts = []
        for claim, part in zip(claims, parts):
            access = access_by_part.get(claim.part_number)
            if access is None:
                raise RuntimeError(
                    f"server did not sign multipart part {claim.part_number}"
                )
            response = _send_presigned(
                transfer_client,
                access,
                "PUT",
                content=part,
            )
            etag = response.headers.get("etag")
            if etag is None:
                raise RuntimeError(
                    f"multipart part {claim.part_number} returned no ETag"
                )
            completed_parts.append(
                CompletedUploadPart(
                    part_number=claim.part_number,
                    etag=etag,
                    checksum=claim.checksum,
                )
            )
    except Exception:
        _abort_quietly(client, namespace_id, upload_id)
        raise
    completion = client.uploads.complete(
        namespace_id,
        upload_id,
        request=UploadCompletion_DirectMultipart(
            content=UploadContentClaim(
                size_bytes=len(content),
                checksum=_checksum(checksum_algorithm, content),
            ),
            parts=completed_parts,
        ),
    )
    return _completed_content(completion)


def _send_presigned(
    client: httpx.Client,
    access: ObjectTransferAccess,
    expected_method: str,
    *,
    content: bytes | None = None,
) -> httpx.Response:
    if access.method.upper() != expected_method:
        raise RuntimeError(
            f"presigned access uses {access.method!r}, expected {expected_method!r}"
        )
    headers = httpx.Headers(access.headers or {})
    response = client.request(
        expected_method,
        access.url,
        headers=headers,
        content=content,
    )
    response.raise_for_status()
    return response


def _completed_content(response: UploadSession) -> PreparedFileContent:
    if response.status != "completed":
        raise RuntimeError(
            f"upload {response.upload_id!r} completed with status {response.status!r}"
        )
    return PreparedFileContent(
        content_ref=response.content_ref,
        content_token=response.content_token,
    )


def _abort_quietly(client: _GeneratedLoonFS, namespace_id: str, upload_id: str) -> None:
    try:
        client.uploads.abort(namespace_id, upload_id)
    except Exception:
        pass


def _checksum(algorithm: str, content: bytes) -> Checksum:
    if algorithm == "sha256":
        return Checksum(algorithm=algorithm, value=hashlib.sha256(content).hexdigest())
    if algorithm == "crc64nvme":
        table, mask, width = _CRC64_NVME_TABLE, _CRC64_NVME_MASK, 16
    elif algorithm == "crc32c":
        table, mask, width = _CRC32C_TABLE, _CRC32C_MASK, 8
    else:
        raise RuntimeError(f"unsupported checksum algorithm {algorithm!r}")
    value = mask
    for byte in content:
        value = table[(value ^ byte) & 0xFF] ^ (value >> 8)
    return Checksum(algorithm=algorithm, value=f"{value ^ mask:0{width}x}")
