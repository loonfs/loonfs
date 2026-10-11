"""Transfer helpers for both clients. See the package README for their contract."""

from __future__ import annotations

import asyncio
import base64
import io
import typing
import uuid
from dataclasses import dataclass

import httpx

from ._transfer_runtime import (
    _AsyncReader,
    _AsyncUploadSource,
    _DownloadVerification,
    _IncrementalChecksum,
    _TRANSFER_CHUNK_BYTES,
    _UploadSource,
    _async_download_ranges,
    _async_send_stream_presigned,
    _checksum,
    _download_ranges,
    _send_stream_presigned,
    _validate_download_ranges,
)

from .client import AsyncLoonFS as _GeneratedAsyncLoonFS
from .client import LoonFS as _GeneratedLoonFS
from .files.client import AsyncFilesClient as _GeneratedAsyncFilesClient
from .files.client import FilesClient as _GeneratedFilesClient
from .core.request_options import RequestOptions
from .types import (
    AbsolutePath,
    NamespaceId,
    Commit,
    CommitId,
    InodeId,
    CreateUploadBody_DirectMultipart,
    CreateUploadBody_DirectPut,
    CreateUploadBody_ServiceProxied,
    CompletedUploadPart,
    ContentRef,
    ContentToken,
    DestinationBehavior,
    FilesystemOperation_AppendFile,
    FilesystemOperation_PutFile,
    RevisionNo,
    CompleteUploadBody_DirectMultipart,
    CompleteUploadBody_DirectPut,
    CompleteUploadBody_ServiceProxied,
    UploadContentClaim,
    UploadSession_Open,
    UploadPartChecksumClaim,
    UploadSession,
)

_INLINE_FEATURE = "filesystem.commits.inline_content"
_INLINE_LIMIT = "commit.max_inline_content_bytes_per_operation"
_MAX_INLINE_BYTES = 64 * 1024
_MAX_APPEND_BYTES = 256 * 1024

_MULTIPART_MIN_BYTES = 8 * 1024 * 1024
_DIRECT_GET_FEATURE = "filesystem.downloads.direct_get"
_DIRECT_MULTIPART_FEATURE = "filesystem.uploads.direct_multipart"
_DIRECT_PUT_FEATURE = "filesystem.uploads.direct_put"
_PROXY_UPLOAD_LIMIT = "upload.service_proxied.max_content_bytes"
_DIRECT_PUT_LIMIT = "upload.direct_put.max_content_bytes"


@dataclass(frozen=True)
class DownloadResult:
    content: bytes
    namespace_id: NamespaceId
    path: AbsolutePath
    revision_no: RevisionNo
    content_ref: ContentRef


@dataclass(frozen=True)
class PreparedContent:
    content_ref: ContentRef
    content_token: ContentToken | None


@dataclass(frozen=True)
class InlinePreparedContent:
    content: bytes

    def __post_init__(self) -> None:
        object.__setattr__(self, "content", bytes(self.content))


PreparedFile = typing.Union[PreparedContent, InlinePreparedContent]


class DownloadStream(typing.Iterator[bytes]):
    def __init__(self, chunks, close, namespace_id, path, revision_no, content_ref):
        self._chunks, self._close = iter(chunks), close
        self.namespace_id, self.path, self.revision_no = namespace_id, path, revision_no
        self.content_ref = content_ref
        self._verification = _DownloadVerification(content_ref)
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
            self._verification.update(chunk)
            return chunk
        except StopIteration:
            try:
                self._verification.finish()
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
    def __init__(self, *, client_wrapper, root: "LoonFS") -> None:
        super().__init__(client_wrapper=client_wrapper)
        self._root = root

    def upload(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        content: bytes,
        commit_id: CommitId | None = None,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: InodeId | None = None,
        expected_revision_no: RevisionNo | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> Commit:
        return self.upload_stream(
            namespace_id,
            path=path,
            content=io.BytesIO(content),
            size_bytes=len(content),
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
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        content: typing.BinaryIO,
        size_bytes: int | None = None,
        commit_id: CommitId | None = None,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: InodeId | None = None,
        expected_revision_no: RevisionNo | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> Commit:
        commit_id = _publication_id(commit_id)
        prepared = self.prepare_stream(
            namespace_id,
            content=content,
            size_bytes=size_bytes,
            http_client=http_client,
            request_options=request_options,
        )
        return self.upload_prepared(
            namespace_id,
            path=path,
            prepared=prepared,
            commit_id=commit_id,
            message=message,
            behavior=behavior,
            expected_inode_id=expected_inode_id,
            expected_revision_no=expected_revision_no,
            request_options=request_options,
        )

    def prepare(
        self,
        namespace_id: NamespaceId,
        *,
        content: bytes,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> PreparedFile:
        return self.prepare_stream(
            namespace_id,
            content=io.BytesIO(content),
            size_bytes=len(content),
            http_client=http_client,
            request_options=request_options,
        )

    def prepare_stream(
        self,
        namespace_id: NamespaceId,
        *,
        content: typing.BinaryIO,
        size_bytes: int | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> PreparedFile:
        if size_bytes is not None and size_bytes < 0:
            raise ValueError("invalid upload size")
        capabilities = self._root.capabilities.retrieve(request_options=request_options)
        first = b""
        if size_bytes is None:
            first = content.read(1)
            if not first:
                size_bytes = 0
        source = _UploadSource(content, first, size_bytes)
        limit = _inline_limit(capabilities)
        if limit is not None:
            prefix = bytearray()
            while len(prefix) <= limit:
                chunk = source.read(limit + 1 - len(prefix))
                if not chunk:
                    return InlinePreparedContent(bytes(prefix))
                prefix.extend(chunk)
            # The source is one-pass: replay only the bounded prefix into staging.
            source = _UploadSource(content, bytes(prefix), size_bytes)
        begin = _create_upload(
            self._root, namespace_id, capabilities, size_bytes, request_options
        )
        client = http_client or self._root._client_wrapper.httpx_client.httpx_client
        timeout = (request_options or {}).get(
            "timeout", self._root._client_wrapper.get_timeout()
        )
        options = {**(request_options or {}), "max_retries": 0}
        if not isinstance(begin, UploadSession_Open):
            raise RuntimeError("created upload session is not open")
        try:
            if begin.mode == "service_proxied":
                source.limit = (capabilities.limits or {}).get(_PROXY_UPLOAD_LIMIT)
                self._root.uploads.put_content(
                    namespace_id,
                    begin.upload_id,
                    request=source.chunks(),
                    request_options=options,
                )
                completion = CompleteUploadBody_ServiceProxied()
            elif begin.mode == "direct_put":
                if begin.access is None:
                    raise RuntimeError("direct_put session lacks access")
                source.digest = _IncrementalChecksum(begin.checksum_algorithm)
                _send_stream_presigned(
                    client, begin.access, source.chunks(), timeout, size_bytes
                )
                completion = CompleteUploadBody_DirectPut(
                    content=UploadContentClaim(
                        size_bytes=source.count, checksum=source.digest.finish()
                    )
                )
            elif begin.mode == "direct_multipart":
                completion = _stream_multipart(
                    self._root, client, namespace_id, begin, source, timeout, options
                )
            else:
                raise RuntimeError(f"unsupported upload mode {begin.mode}")
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

    def upload_prepared(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        prepared: PreparedFile,
        commit_id: CommitId | None = None,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: InodeId | None = None,
        expected_revision_no: RevisionNo | None = None,
        request_options: RequestOptions | None = None,
    ) -> Commit:
        commit_id = _publication_id(commit_id)
        commit_arguments = _upload_commit(
            path, prepared, message, behavior, expected_inode_id, expected_revision_no
        )
        return self._root.commits.create(
            namespace_id,
            commit_id=commit_id,
            request_options=request_options,
            **commit_arguments,
        )

    def append(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        content: bytes,
        commit_id: CommitId | None = None,
        message: str | None = None,
        expected_inode_id: InodeId | None = None,
        expected_revision_no: RevisionNo | None = None,
        request_options: RequestOptions | None = None,
    ) -> Commit:
        commit_arguments = _append_commit(
            path, content, message, expected_inode_id, expected_revision_no
        )
        return self._root.commits.create(
            namespace_id,
            commit_id=_publication_id(commit_id),
            request_options=request_options,
            **commit_arguments,
        )

    def download_stream(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        revision_no: RevisionNo | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> DownloadStream:
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
            return DownloadStream(
                chunks, chunks.close, namespace_id, path, revision_no, claim
            )
        grant = self.create_download(
            namespace_id,
            path=path,
            revision_no=revision_no,
            request_options=request_options,
        )
        _validate_download_ranges(grant.ranges, grant.content_ref.size_bytes)
        client = http_client or self._root._client_wrapper.httpx_client.httpx_client
        timeout = (request_options or {}).get(
            "timeout", self._root._client_wrapper.get_timeout()
        )
        chunks = _download_ranges(client, grant.ranges, timeout)
        return DownloadStream(
            chunks,
            chunks.close,
            grant.namespace_id,
            grant.path,
            grant.revision_no,
            grant.content_ref,
        )

    def download(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        revision_no: RevisionNo | None = None,
        http_client: httpx.Client | None = None,
        request_options: RequestOptions | None = None,
    ) -> DownloadResult:
        with self.download_stream(
            namespace_id,
            path=path,
            revision_no=revision_no,
            http_client=http_client,
            request_options=request_options,
        ) as stream:
            content = b"".join(stream)
            return DownloadResult(
                content=content,
                namespace_id=stream.namespace_id,
                path=stream.path,
                revision_no=stream.revision_no,
                content_ref=stream.content_ref,
            )


def _validate_subject_context(options: typing.Mapping[str, typing.Any]) -> None:
    if (options.get("principal_scope") is None) != (options.get("principals") is None):
        raise ValueError("principal_scope and principals must be configured together")


class LoonFS(_GeneratedLoonFS):
    def __init__(self, **kwargs: typing.Any) -> None:
        _validate_subject_context(kwargs)
        super().__init__(**kwargs)
        self._transfer_files: FilesClient | None = None

    @property
    def files(self) -> FilesClient:
        if self._transfer_files is None:
            self._transfer_files = FilesClient(
                client_wrapper=self._client_wrapper, root=self
            )
        return self._transfer_files


class AsyncDownloadStream(typing.AsyncIterator[bytes]):
    def __init__(self, chunks, close, namespace_id, path, revision_no, content_ref):
        self._chunks, self._close = chunks.__aiter__(), close
        self.namespace_id, self.path, self.revision_no = namespace_id, path, revision_no
        self.content_ref = content_ref
        self._verification = _DownloadVerification(content_ref)
        self._closed = False
        self._verified = False
        self._terminal_error = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if self._terminal_error is not None:
            raise self._terminal_error
        if self._closed:
            if self._verified:
                raise StopAsyncIteration
            raise ValueError("download stream was closed before verification")
        try:
            chunk = await self._chunks.__anext__()
            self._verification.update(chunk)
            return chunk
        except StopAsyncIteration:
            try:
                self._verification.finish()
                self._verified = True
            except BaseException as error:
                self._terminal_error = error
                raise
            finally:
                await self.aclose()
            raise
        except BaseException as error:
            self._terminal_error = error
            await self.aclose()
            raise

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._close()


class AsyncFilesClient(_GeneratedAsyncFilesClient):
    def __init__(self, *, client_wrapper, root: "AsyncLoonFS") -> None:
        super().__init__(client_wrapper=client_wrapper)
        self._root = root

    async def upload(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        content: bytes,
        commit_id: CommitId | None = None,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: InodeId | None = None,
        expected_revision_no: RevisionNo | None = None,
        http_client: httpx.AsyncClient | None = None,
        request_options: RequestOptions | None = None,
    ) -> Commit:
        return await self.upload_stream(
            namespace_id,
            path=path,
            content=io.BytesIO(content),
            size_bytes=len(content),
            commit_id=commit_id,
            message=message,
            behavior=behavior,
            expected_inode_id=expected_inode_id,
            expected_revision_no=expected_revision_no,
            http_client=http_client,
            request_options=request_options,
        )

    async def upload_stream(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        content: typing.AsyncIterator[bytes] | typing.BinaryIO,
        size_bytes: int | None = None,
        commit_id: CommitId | None = None,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: InodeId | None = None,
        expected_revision_no: RevisionNo | None = None,
        http_client: httpx.AsyncClient | None = None,
        request_options: RequestOptions | None = None,
    ) -> Commit:
        commit_id = _publication_id(commit_id)
        prepared = await self.prepare_stream(
            namespace_id,
            content=content,
            size_bytes=size_bytes,
            http_client=http_client,
            request_options=request_options,
        )
        return await self.upload_prepared(
            namespace_id,
            path=path,
            prepared=prepared,
            commit_id=commit_id,
            message=message,
            behavior=behavior,
            expected_inode_id=expected_inode_id,
            expected_revision_no=expected_revision_no,
            request_options=request_options,
        )

    async def prepare(
        self,
        namespace_id: NamespaceId,
        *,
        content: bytes,
        http_client: httpx.AsyncClient | None = None,
        request_options: RequestOptions | None = None,
    ) -> PreparedFile:
        return await self.prepare_stream(
            namespace_id,
            content=io.BytesIO(content),
            size_bytes=len(content),
            http_client=http_client,
            request_options=request_options,
        )

    async def prepare_stream(
        self,
        namespace_id: NamespaceId,
        *,
        content: typing.AsyncIterator[bytes] | typing.BinaryIO,
        size_bytes: int | None = None,
        http_client: httpx.AsyncClient | None = None,
        request_options: RequestOptions | None = None,
    ) -> PreparedFile:
        if size_bytes is not None and size_bytes < 0:
            raise ValueError("invalid upload size")
        capabilities = await self._root.capabilities.retrieve(
            request_options=request_options
        )
        reader = _AsyncReader(content)
        first = b""
        if size_bytes is None:
            first = await reader.read(1)
            if not first:
                size_bytes = 0
        source = _AsyncUploadSource(reader, first, size_bytes)
        limit = _inline_limit(capabilities)
        if limit is not None:
            prefix = bytearray()
            while len(prefix) <= limit:
                chunk = await source.read(limit + 1 - len(prefix))
                if not chunk:
                    return InlinePreparedContent(bytes(prefix))
                prefix.extend(chunk)
            # The source is one-pass: replay only the bounded prefix into staging.
            source = _AsyncUploadSource(reader, bytes(prefix), size_bytes)
        begin = await _create_upload(
            self._root, namespace_id, capabilities, size_bytes, request_options
        )
        client = http_client or self._root._client_wrapper.httpx_client.httpx_client
        timeout = (request_options or {}).get(
            "timeout", self._root._client_wrapper.get_timeout()
        )
        options = {**(request_options or {}), "max_retries": 0}
        if not isinstance(begin, UploadSession_Open):
            raise RuntimeError("created upload session is not open")
        try:
            if begin.mode == "service_proxied":
                source.limit = (capabilities.limits or {}).get(_PROXY_UPLOAD_LIMIT)
                await self._root.uploads.put_content(
                    namespace_id,
                    begin.upload_id,
                    request=source.chunks(),
                    request_options=options,
                )
                completion = CompleteUploadBody_ServiceProxied()
            elif begin.mode == "direct_put":
                if begin.access is None:
                    raise RuntimeError("direct_put session lacks access")
                source.digest = _IncrementalChecksum(begin.checksum_algorithm)
                await _async_send_stream_presigned(
                    client, begin.access, source.chunks(), timeout, size_bytes
                )
                completion = CompleteUploadBody_DirectPut(
                    content=UploadContentClaim(
                        size_bytes=source.count, checksum=source.digest.finish()
                    )
                )
            elif begin.mode == "direct_multipart":
                completion = await _async_stream_multipart(
                    self._root, client, namespace_id, begin, source, timeout, options
                )
            else:
                raise RuntimeError(f"unsupported upload mode {begin.mode}")
            source.finish()
        except BaseException:
            await _async_abort_quietly(self._root, namespace_id, begin.upload_id)
            raise
        # Keep a session whose completion response was lost available for inspection.
        completed = await self._root.uploads.complete(
            namespace_id, begin.upload_id, request=completion, request_options=options
        )
        result = _completed_content(completed)
        if result.content_ref.size_bytes != source.count:
            raise RuntimeError("completed upload size mismatch")
        return result

    async def upload_prepared(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        prepared: PreparedFile,
        commit_id: CommitId | None = None,
        message: str | None = None,
        behavior: DestinationBehavior | None = None,
        expected_inode_id: InodeId | None = None,
        expected_revision_no: RevisionNo | None = None,
        request_options: RequestOptions | None = None,
    ) -> Commit:
        commit_id = _publication_id(commit_id)
        commit_arguments = _upload_commit(
            path, prepared, message, behavior, expected_inode_id, expected_revision_no
        )
        return await self._root.commits.create(
            namespace_id,
            commit_id=commit_id,
            request_options=request_options,
            **commit_arguments,
        )

    async def append(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        content: bytes,
        commit_id: CommitId | None = None,
        message: str | None = None,
        expected_inode_id: InodeId | None = None,
        expected_revision_no: RevisionNo | None = None,
        request_options: RequestOptions | None = None,
    ) -> Commit:
        commit_arguments = _append_commit(
            path, content, message, expected_inode_id, expected_revision_no
        )
        return await self._root.commits.create(
            namespace_id,
            commit_id=_publication_id(commit_id),
            request_options=request_options,
            **commit_arguments,
        )

    async def download_stream(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        revision_no: RevisionNo | None = None,
        http_client: httpx.AsyncClient | None = None,
        request_options: RequestOptions | None = None,
    ) -> AsyncDownloadStream:
        capabilities = await self._root.capabilities.retrieve(
            request_options=request_options
        )
        if not (capabilities.features or {}).get(_DIRECT_GET_FEATURE, False):
            claim, revision_no = await _async_proxied_claim(
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
            return AsyncDownloadStream(
                chunks, chunks.aclose, namespace_id, path, revision_no, claim
            )
        grant = await self.create_download(
            namespace_id,
            path=path,
            revision_no=revision_no,
            request_options=request_options,
        )
        _validate_download_ranges(grant.ranges, grant.content_ref.size_bytes)
        client = http_client or self._root._client_wrapper.httpx_client.httpx_client
        timeout = (request_options or {}).get(
            "timeout", self._root._client_wrapper.get_timeout()
        )
        chunks = _async_download_ranges(client, grant.ranges, timeout)
        return AsyncDownloadStream(
            chunks,
            chunks.aclose,
            grant.namespace_id,
            grant.path,
            grant.revision_no,
            grant.content_ref,
        )

    async def download(
        self,
        namespace_id: NamespaceId,
        *,
        path: AbsolutePath,
        revision_no: RevisionNo | None = None,
        http_client: httpx.AsyncClient | None = None,
        request_options: RequestOptions | None = None,
    ) -> DownloadResult:
        async with await self.download_stream(
            namespace_id,
            path=path,
            revision_no=revision_no,
            http_client=http_client,
            request_options=request_options,
        ) as stream:
            content = b"".join([chunk async for chunk in stream])
            return DownloadResult(
                content=content,
                namespace_id=stream.namespace_id,
                path=stream.path,
                revision_no=stream.revision_no,
                content_ref=stream.content_ref,
            )


class AsyncLoonFS(_GeneratedAsyncLoonFS):
    def __init__(self, **kwargs: typing.Any) -> None:
        _validate_subject_context(kwargs)
        super().__init__(**kwargs)
        self._transfer_files: AsyncFilesClient | None = None

    @property
    def files(self) -> AsyncFilesClient:
        if self._transfer_files is None:
            self._transfer_files = AsyncFilesClient(
                client_wrapper=self._client_wrapper, root=self
            )
        return self._transfer_files


__all__ = [
    "AsyncDownloadStream",
    "AsyncFilesClient",
    "AsyncLoonFS",
    "DownloadResult",
    "DownloadStream",
    "PreparedContent",
    "InlinePreparedContent",
    "PreparedFile",
    "FilesClient",
    "LoonFS",
]


def _proxied_claim(client, namespace_id, path, revision_no, request_options):
    if revision_no is None:
        entry = client.files.retrieve(
            namespace_id, path=path, request_options=request_options
        )
        if entry.inode_kind != "file":
            raise RuntimeError(f"path is a {entry.inode_kind}, not a file")
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
            raise RuntimeError(f"revision {revision_no} not found for {path}")
        cursor = page.next_cursor


def _inline_limit(capabilities):
    limit = (capabilities.limits or {}).get(_INLINE_LIMIT)
    if (
        (capabilities.features or {}).get(_INLINE_FEATURE)
        and type(limit) is int
        and limit >= 0
    ):
        return min(limit, _MAX_INLINE_BYTES)
    return None


def _append_commit(path, content, message, expected_inode_id, expected_revision_no):
    if not content:
        raise ValueError("append content is empty")
    if len(content) > _MAX_APPEND_BYTES:
        raise ValueError(
            f"{len(content)}-byte append is larger than the {_MAX_APPEND_BYTES}-byte limit"
        )
    operation_arguments = {
        "path": path,
        "inline_content": base64.b64encode(content).decode("ascii"),
    }
    if expected_inode_id is not None:
        operation_arguments["expected_inode_id"] = expected_inode_id
    if expected_revision_no is not None:
        operation_arguments["expected_revision_no"] = expected_revision_no
    commit_arguments = {
        "operations": [FilesystemOperation_AppendFile(**operation_arguments)]
    }
    if message is not None:
        commit_arguments["message"] = message
    return commit_arguments


def _create_upload(client, namespace_id, capabilities, size_bytes, request_options):
    features, limits = capabilities.features or {}, capabilities.limits or {}
    if (size_bytes is None or size_bytes >= _MULTIPART_MIN_BYTES) and features.get(
        _DIRECT_MULTIPART_FEATURE, False
    ):
        request = CreateUploadBody_DirectMultipart()
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
            request = CreateUploadBody_DirectPut(size_bytes=size_bytes)
        elif fits_proxy:
            request = CreateUploadBody_ServiceProxied()
        else:
            raise ValueError("source fits no advertised upload transport")
    return client.uploads.create(
        namespace_id,
        request=request,
        request_options={**(request_options or {}), "max_retries": 0},
    )


def _stream_multipart(
    client, http, namespace_id, begin, source, timeout, request_options
):
    part_size = begin.part_size_bytes
    if part_size is None:
        raise RuntimeError("direct_multipart session lacks part_size_bytes")
    if part_size <= 0:
        raise RuntimeError("invalid multipart part size")
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
    return CompleteUploadBody_DirectMultipart(
        content=UploadContentClaim(
            size_bytes=source.count, checksum=source.digest.finish()
        ),
        parts=completed_parts,
    )


def _completed_content(response: UploadSession) -> PreparedContent:
    if response.status != "completed":
        raise RuntimeError(f"upload is {response.status}, not completed")
    return PreparedContent(
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


async def _async_proxied_claim(
    client, namespace_id, path, revision_no, request_options
):
    if revision_no is None:
        entry = await client.files.retrieve(
            namespace_id, path=path, request_options=request_options
        )
        if entry.inode_kind != "file":
            raise RuntimeError(f"path is a {entry.inode_kind}, not a file")
        return entry.content_ref, entry.revision_no
    cursor = None
    while True:
        page = await client.files.list_revisions(
            namespace_id, path=path, cursor=cursor, request_options=request_options
        )
        for revision in page.revisions:
            if revision.revision_no == revision_no:
                return revision.content_ref, revision_no
        if page.next_cursor is None:
            raise RuntimeError(f"revision {revision_no} not found for {path}")
        cursor = page.next_cursor


async def _async_stream_multipart(
    client, http, namespace_id, begin, source, timeout, request_options
):
    part_size = begin.part_size_bytes
    if part_size is None:
        raise RuntimeError("direct_multipart session lacks part_size_bytes")
    if part_size <= 0:
        raise RuntimeError("invalid multipart part size")
    source.digest = _IncrementalChecksum(begin.checksum_algorithm)
    completed_parts = []
    while True:
        part = bytearray()
        while len(part) < part_size:
            chunk = await source.read(part_size - len(part))
            if not chunk:
                break
            part.extend(chunk)
        if not part:
            break
        if len(completed_parts) == 10000:
            raise ValueError("multipart upload exceeds 10000 parts")
        number = len(completed_parts) + 1
        checksum = _checksum(begin.checksum_algorithm, part)
        signed = await client.uploads.sign_parts(
            namespace_id,
            begin.upload_id,
            parts=[UploadPartChecksumClaim(part_number=number, checksum=checksum)],
            request_options=request_options,
        )
        if len(signed.parts) != 1 or signed.parts[0].part_number != number:
            raise RuntimeError(f"server did not sign requested part {number}")
        etag = await _async_send_stream_presigned(
            http, signed.parts[0].access, bytes(part), timeout
        )
        if not etag:
            raise RuntimeError(f"part {number} returned no ETag")
        completed_parts.append(
            CompletedUploadPart(part_number=number, etag=etag, checksum=checksum)
        )
    return CompleteUploadBody_DirectMultipart(
        content=UploadContentClaim(
            size_bytes=source.count, checksum=source.digest.finish()
        ),
        parts=completed_parts,
    )


async def _async_abort_quietly(client, namespace_id, upload_id):
    try:
        await asyncio.wait_for(
            client.uploads.abort(
                namespace_id,
                upload_id,
                request_options={"timeout": 5, "max_retries": 0},
            ),
            timeout=5,
        )
    except (Exception, asyncio.CancelledError):
        pass


def _publication_id(commit_id: CommitId | None) -> CommitId:
    return commit_id if commit_id is not None else "c_" + uuid.uuid4().hex


def _upload_commit(
    path, prepared, message, behavior, expected_inode_id, expected_revision_no
):
    if isinstance(prepared, InlinePreparedContent):
        content = {"inline_content": base64.b64encode(prepared.content).decode("ascii")}
        tokens = []
    else:
        content = {"content_ref": prepared.content_ref}
        tokens = [prepared.content_token] if prepared.content_token is not None else []
    operation_arguments = {"path": path, **content}
    if behavior is not None:
        operation_arguments["behavior"] = behavior
    if expected_inode_id is not None:
        operation_arguments["expected_inode_id"] = expected_inode_id
    if expected_revision_no is not None:
        operation_arguments["expected_revision_no"] = expected_revision_no
    operation = FilesystemOperation_PutFile(**operation_arguments)
    commit_arguments = {
        "operations": [operation],
        "content_tokens": tokens,
    }
    if message is not None:
        commit_arguments["message"] = message
    return commit_arguments
