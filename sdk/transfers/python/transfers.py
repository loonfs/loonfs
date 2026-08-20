"""Synchronous, in-memory file transfer orchestration.

Streaming and resume are follow-ups. AsyncLoonFS support is a follow-up.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass

import httpx

from .client import LoonFS
from .types import (
    ActorRef,
    BeginUploadRequest_DirectMultipart,
    BeginUploadRequest_DirectPut,
    BeginUploadRequest_ServiceProxied,
    Checksum,
    CompleteUploadRequest_DirectMultipart,
    CompleteUploadRequest_DirectPut,
    CompleteUploadRequest_ServiceProxied,
    CompletedUploadPart,
    ContentRef,
    DestinationBehavior,
    DirectMultipartUploadOptions,
    FilesystemOperation_PutFile,
    ObjectTransferAccess,
    RevisionNo,
    UploadContentClaim,
    UploadPartChecksumClaim,
)

__all__ = ["GetFileResult", "PutFileResult", "get_file", "put_file"]


_MULTIPART_MIN_BYTES = 8 * 1024 * 1024
_DIRECT_MULTIPART_FEATURE = "core.uploads.direct_multipart"
_DIRECT_PUT_FEATURE = "core.uploads.direct_put"
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


class _ChecksumAccumulator:
    def __init__(self, algorithm: str) -> None:
        self._algorithm = algorithm
        self._sha256 = hashlib.sha256() if algorithm == "sha256" else None
        if algorithm == "crc64nvme":
            self._crc_table = _CRC64_NVME_TABLE
            self._crc_mask = _CRC64_NVME_MASK
        elif algorithm == "crc32c":
            self._crc_table = _CRC32C_TABLE
            self._crc_mask = _CRC32C_MASK
        elif algorithm == "sha256":
            self._crc_table = None
            self._crc_mask = None
        else:
            raise RuntimeError(f"unsupported checksum algorithm {algorithm!r}")
        self._crc_value = self._crc_mask

    def update(self, content: bytes) -> None:
        if self._sha256 is not None:
            self._sha256.update(content)
            return
        assert self._crc_table is not None
        assert self._crc_value is not None
        for byte in content:
            self._crc_value = (
                self._crc_table[(self._crc_value ^ byte) & 0xFF]
                ^ (self._crc_value >> 8)
            )

    def finish(self) -> Checksum:
        if self._sha256 is not None:
            value = self._sha256.hexdigest()
        else:
            assert self._crc_mask is not None
            assert self._crc_value is not None
            width = 16 if self._crc_mask == _CRC64_NVME_MASK else 8
            value = f"{self._crc_value ^ self._crc_mask:0{width}x}"
        return Checksum(algorithm=self._algorithm, value=value)


class _UploadBody:
    def __init__(self, algorithm: str, content: bytes) -> None:
        self._content = content
        self._checksum = _ChecksumAccumulator(algorithm)
        self._size_bytes = 0
        self._started = False

    def __iter__(self) -> Iterator[bytes]:
        if self._started:
            raise RuntimeError("direct PUT body cannot be replayed")
        self._started = True
        self._checksum.update(self._content)
        self._size_bytes += len(self._content)
        yield self._content

    def claim(self) -> UploadContentClaim:
        return UploadContentClaim(
            size_bytes=self._size_bytes,
            checksum=self._checksum.finish(),
        )


@dataclass(frozen=True)
class PutFileResult:
    """The identity and sequence of the commit that stored the file."""

    namespace_id: str
    commit_id: str
    committed_seq: int


@dataclass(frozen=True)
class GetFileResult:
    """Downloaded bytes and the immutable revision facts from its grant."""

    content: bytes
    namespace_id: str
    path: str
    revision_no: int
    content_ref: ContentRef


@dataclass(frozen=True)
class _StagedContent:
    content_ref: ContentRef
    content_token: str | None


def put_file(
    client: LoonFS,
    *,
    namespace_id: str,
    path: str,
    content: bytes,
    actor: ActorRef,
    commit_id: str,
    message: str | None = None,
    behavior: DestinationBehavior | None = None,
    expected_revision_no: RevisionNo | None = None,
    http_client: httpx.Client | None = None,
) -> PutFileResult:
    """Upload in-memory bytes, complete the upload, and commit the file."""

    begin = _begin_upload(client, namespace_id, content)
    if http_client is None:
        with httpx.Client() as transfer_client:
            staged = _stage_upload(
                client, transfer_client, namespace_id, begin, content
            )
    else:
        staged = _stage_upload(client, http_client, namespace_id, begin, content)

    operation_arguments = {"path": path, "content_ref": staged.content_ref}
    if behavior is not None:
        operation_arguments["behavior"] = behavior
    if expected_revision_no is not None:
        operation_arguments["expected_revision_no"] = expected_revision_no
    operation = FilesystemOperation_PutFile(**operation_arguments)
    commit_arguments = {
        "actor": actor,
        "commit_id": commit_id,
        "operations": [operation],
        "content_tokens": [staged.content_token]
        if staged.content_token is not None
        else [],
    }
    if message is not None:
        commit_arguments["message"] = message
    committed = client.filesystem.apply_commit(namespace_id, **commit_arguments)
    return PutFileResult(
        namespace_id=committed.namespace_id,
        commit_id=committed.commit_id,
        committed_seq=committed.committed_seq,
    )


def get_file(
    client: LoonFS,
    *,
    namespace_id: str,
    path: str,
    revision_no: RevisionNo | None = None,
    http_client: httpx.Client | None = None,
) -> GetFileResult:
    """Download one file revision into memory and verify its checksum."""

    if revision_no is None:
        grant = client.filesystem.begin_download(namespace_id, path=path)
    else:
        grant = client.filesystem.begin_download(
            namespace_id,
            path=path,
            revision_no=revision_no,
        )
    if http_client is None:
        with httpx.Client() as transfer_client:
            response = _send_presigned(transfer_client, grant.access, "GET")
    else:
        response = _send_presigned(http_client, grant.access, "GET")
    content = response.content
    if len(content) != grant.content_ref.size_bytes:
        raise RuntimeError(
            f"download returned {len(content)} bytes, expected {grant.content_ref.size_bytes}"
        )
    if (
        _checksum(grant.content_ref.checksum.algorithm, content)
        != grant.content_ref.checksum
    ):
        raise RuntimeError("download checksum did not match its content reference")
    return GetFileResult(
        content=content,
        namespace_id=grant.namespace_id,
        path=grant.path,
        revision_no=grant.revision_no,
        content_ref=grant.content_ref,
    )


def _begin_upload(client: LoonFS, namespace_id: str, content: bytes):
    capabilities = client.capabilities()
    features = capabilities.features or {}
    limits = capabilities.limits or {}
    size_bytes = len(content)
    if size_bytes >= _MULTIPART_MIN_BYTES and features.get(
        _DIRECT_MULTIPART_FEATURE, False
    ):
        request = BeginUploadRequest_DirectMultipart(
            multipart=DirectMultipartUploadOptions()
        )
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
    return client.uploads.begin_upload(namespace_id, request=request)


def _stage_upload(
    client: LoonFS,
    transfer_client: httpx.Client,
    namespace_id: str,
    begin,
    content: bytes,
) -> _StagedContent:
    if begin.mode == "service_proxied":
        try:
            client.uploads.upload_content(namespace_id, begin.upload_id, request=content)
            completion = client.uploads.complete_upload(
                namespace_id,
                begin.upload_id,
                request=CompleteUploadRequest_ServiceProxied(),
            )
        except Exception:
            _abort_quietly(client, namespace_id, begin.upload_id)
            raise
        return _completed_content(completion)
    if begin.mode == "direct_put":
        upload_body = _UploadBody(begin.direct_put.checksum_algorithm, content)
        try:
            _send_presigned(
                transfer_client,
                begin.direct_put.access,
                "PUT",
                content=upload_body,
                content_length=len(content),
            )
        except Exception:
            _abort_quietly(client, namespace_id, begin.upload_id)
            raise
        completion = client.uploads.complete_upload(
            namespace_id,
            begin.upload_id,
            request=CompleteUploadRequest_DirectPut(content=upload_body.claim()),
        )
        return _completed_content(completion)
    if begin.mode == "direct_multipart":
        return _stage_multipart(
            client,
            transfer_client,
            namespace_id,
            begin.upload_id,
            begin.direct_multipart.part_size_bytes,
            begin.direct_multipart.checksum_algorithm,
            content,
        )
    raise RuntimeError(f"unsupported upload mode {begin.mode!r}")


def _stage_multipart(
    client: LoonFS,
    transfer_client: httpx.Client,
    namespace_id: str,
    upload_id: str,
    part_size_bytes: int,
    checksum_algorithm: str,
    content: bytes,
) -> _StagedContent:
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
        signed = client.uploads.sign_upload_parts(
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
    completion = client.uploads.complete_upload(
        namespace_id,
        upload_id,
        request=CompleteUploadRequest_DirectMultipart(
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
    content: bytes | Iterable[bytes] | None = None,
    content_length: int | None = None,
) -> httpx.Response:
    if access.method.upper() != expected_method:
        raise RuntimeError(
            f"presigned access uses {access.method!r}, expected {expected_method!r}"
        )
    headers = httpx.Headers(access.headers or {})
    if content_length is not None and "content-length" not in headers:
        headers["content-length"] = str(content_length)
    response = client.request(
        expected_method,
        access.url,
        headers=headers,
        content=content,
    )
    response.raise_for_status()
    return response


def _completed_content(response) -> _StagedContent:
    if getattr(response, "status", None) != "completed":
        raise RuntimeError(
            f"upload {response.upload_id!r} completed with status "
            f"{getattr(response, 'status', None)!r}"
        )
    return _StagedContent(
        content_ref=_content_ref(getattr(response, "content_ref", None)),
        content_token=getattr(response, "content_token", None),
    )


def _content_ref(value) -> ContentRef:
    if isinstance(value, ContentRef):
        return value
    if isinstance(value, Mapping):
        return ContentRef(**value)
    raise RuntimeError("completed upload returned no content reference")


def _abort_quietly(client: LoonFS, namespace_id: str, upload_id: str) -> None:
    try:
        client.uploads.abort_upload(namespace_id, upload_id)
    except Exception:
        pass


def _checksum(algorithm: str, content: bytes) -> Checksum:
    accumulator = _ChecksumAccumulator(algorithm)
    accumulator.update(content)
    return accumulator.finish()
