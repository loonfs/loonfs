from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import pydantic
import pytest
import uvicorn
from loonfs.server import (
    ActorRef,
    BadRequestError,
    BeginUploadRequest_DirectMultipart,
    BeginUploadRequest_DirectPut,
    BeginUploadRequest_ServiceProxied,
    BeginUploadResponse_DirectPut,
    BeginUploadResponse_ServiceProxied,
    Checksum,
    CommitResponse,
    CompleteUploadRequest_DirectMultipart,
    CompleteUploadRequest_DirectPut,
    CompleteUploadRequest_ServiceProxied,
    CompletedUploadPart,
    ConflictError,
    FilesystemOperation_CreateDirectory,
    FilesystemOperation_CreateDirectoryByInode,
    FilesystemOperation_DeleteByInode,
    FilesystemOperation_DeletePath,
    FilesystemOperation_MoveByInode,
    FilesystemOperation_MovePath,
    FilesystemOperation_PutFile,
    FilesystemOperation_PutFileByInode,
    FilesystemOperation_PutFileRevisionByInode,
    GoneError,
    ListPathEntriesResponse,
    LoonFS,
    NotFoundError,
    PathEntry,
    PathEntry_File,
    UnauthorizedError,
    UploadContentResponse,
    UploadContentClaim,
    UploadPartChecksumClaim,
    UploadSession,
    UploadSession_Aborted,
    UploadSession_Completed,
)
from loonfs.proxy import LoonFSProxy


RUNNER_SKIP = "run scripts/run-sdk-conformance.sh python"
EXPECTED_CASES = [
    "changes",
    "children_by_inode",
    "commit_replay",
    "download",
    "end_to_end",
    "error_contract",
    "inode_mutations",
    "pagination",
    "proxy",
    "snapshots",
    "upload_abort",
    "upload_direct_put",
    "upload_multipart",
]
pytestmark = pytest.mark.skipif(
    not os.environ.get("LOONFS_CONFORMANCE_URL"),
    reason=RUNNER_SKIP,
)


JsonObject = dict[str, object]


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ConformanceCase:
    name: str
    intent: str
    request: JsonObject
    expected: JsonObject


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ErrorStatusExpected:
    status: int
    code: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ErrorContractRequest:
    namespace_id: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ErrorContractExpected:
    unauthenticated: ErrorStatusExpected


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class CommitReplayRequest:
    namespace_id: str
    commit_id: str
    actor: ActorRef
    message: str
    path: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class CommitReplayExpected:
    committed_seq: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class DirectPutRequest:
    namespace_id: str
    path: str
    commit_id: str
    actor: ActorRef
    content_utf8: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class DirectPutExpected:
    mode: str
    size_bytes: int
    checksum_algorithm: str
    committed_seq: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class BytePattern:
    length: int
    modulus: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class MultipartRequest:
    namespace_id: str
    path: str
    commit_id: str
    actor: ActorRef
    part_size_bytes: int
    content_pattern: BytePattern


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class MultipartExpected:
    mode: str
    part_count: int
    size_bytes: int
    checksum_algorithm: str
    committed_seq: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class AbortRequest:
    namespace_id: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class AbortExpected:
    mode: str
    status: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class DownloadRequest:
    namespace_id: str
    path: str
    commit_id: str
    actor: ActorRef
    content_utf8: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class DownloadExpected:
    size_bytes: int
    checksum_algorithm: str
    committed_seq: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class EndToEndCommitIds:
    mkdir: str
    upload: str
    move: str
    remove: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class EndToEndRequest:
    namespace_id: str
    directory: str
    upload_path: str
    moved_path: str
    actor: ActorRef
    content_utf8: str
    commit_ids: EndToEndCommitIds


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class EndToEndExpected:
    mkdir_committed_seq: int
    upload_committed_seq: int
    move_committed_seq: int
    remove_committed_seq: int
    size_bytes: int
    revision_count: int
    change_count: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class PaginationRequest:
    namespace_id: str
    directory: str
    actor: ActorRef
    entry_names: list[str]
    page_size: int
    resume_after_page: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class PaginationExpected:
    entry_count: int
    minimum_page_count: int
    head_seq: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ChildrenByInodeRequest:
    namespace_id: str
    directory: str
    renamed_directory: str
    rename_commit_id: str
    actor: ActorRef
    entry_names: list[str]
    page_size: int
    rename_after_page: int
    resume_after_page: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ChildrenByInodeExpected:
    entry_count: int
    minimum_page_count: int
    initial_head_seq: int
    renamed_head_seq: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class InodeMutationsRequest:
    namespace_id: str
    directory: str
    actor: ActorRef
    path_directory_name: str
    path_file_name: str
    inode_directory_name: str
    inode_file_name: str
    renamed_file_name: str
    moved_file_name: str
    content_utf8: str
    revised_content_utf8: str
    malformed_binding_generation: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class InodeMutationsExpected:
    entry_names: list[str]
    revised_revision_no: int
    moved_committed_seq: int
    deleted_committed_seq: int
    stale_binding_generation: ErrorStatusExpected
    malformed_binding_generation: ErrorStatusExpected


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class SnapshotsRequest:
    namespace_id: str
    directory: str
    actor: ActorRef
    snapshot_name: str
    replaced_file_name: str
    deleted_file_name: str
    added_file_name: str
    captured_content_utf8: str
    current_content_utf8: str
    deleted_content_utf8: str
    added_content_utf8: str
    create_ttl_ms: int
    extend_ttl_ms: int
    unknown_snapshot_id: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class SnapshotsExpected:
    snapshot_head_seq: int
    captured_revision_no: int
    captured_entry_names: list[str]
    current_revision_no: int
    current_entry_names: list[str]
    snapshot_change_seqs: list[int]
    snapshot_gone: ErrorStatusExpected
    snapshot_not_found: ErrorStatusExpected
    revision_with_snapshot: ErrorStatusExpected
    zero_ttl: ErrorStatusExpected


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ProxyCommitIds:
    directory: str
    proxied: str
    direct: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ProxyRequest:
    namespace_alias: str
    namespace_id: str
    unknown_namespace_alias: str
    actor: ActorRef
    directory: str
    proxied_path: str
    direct_path: str
    commit_ids: ProxyCommitIds
    content_utf8: str
    disallowed_path_suffix: str


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ProxyExpected:
    mkdir_committed_seq: int
    proxied_committed_seq: int
    direct_committed_seq: int
    entry_count: int
    unknown_namespace_alias_status: int
    disallowed_route_status: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ChangesRequest:
    namespace_id: str
    path: str
    commit_id: str
    actor: ActorRef
    after_seq: int


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="forbid", strict=True), frozen=True)
class ChangesExpected:
    committed_seq: int
    change_count: int


@dataclass(frozen=True)
class Harness:
    client: LoonFS
    unauthenticated: LoonFS


def _decode(case: ConformanceCase, request_type: Any, expected_type: Any) -> tuple[Any, Any]:
    # Strict Pydantic dataclasses reject Python dictionaries, so validate JSON instead.
    return (
        pydantic.TypeAdapter(request_type).validate_json(json.dumps(case.request)),
        pydantic.TypeAdapter(expected_type).validate_json(json.dumps(case.expected)),
    )


def load_cases(directory: str) -> dict[str, ConformanceCase]:
    cases: list[ConformanceCase] = []
    for path in sorted(Path(directory).iterdir()):
        if not path.is_file() or path.suffix != ".json":
            continue
        test_case = pydantic.TypeAdapter(ConformanceCase).validate_json(path.read_text())
        if test_case.name != path.stem:
            raise AssertionError(
                f"invalid fixture {path}: name is {test_case.name!r}, expected {path.stem!r}"
            )
        if not test_case.intent.strip():
            raise AssertionError(f"invalid fixture {path}: intent must not be empty")
        cases.append(test_case)

    inventory = [test_case.name for test_case in cases]
    if inventory != EXPECTED_CASES:
        raise AssertionError(
            f"fixture inventory is {inventory!r}, expected {EXPECTED_CASES!r}"
        )
    return {test_case.name: test_case for test_case in cases}


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise AssertionError(f"{name} is not set")
    return value


@contextmanager
def _serve_asgi(app: Any, thread_name: str) -> Iterator[str]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    _, port = listener.getsockname()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            log_level="critical",
            access_log=False,
            lifespan="on",
        )
    )
    thread = threading.Thread(
        target=server.run,
        kwargs={"sockets": [listener]},
        name=thread_name,
        daemon=True,
    )
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not server.started:
            if not thread.is_alive():
                raise AssertionError("ASGI server stopped during startup")
            if time.monotonic() >= deadline:
                raise AssertionError("ASGI server did not start within five seconds")
            time.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        listener.close()
        if thread.is_alive():
            raise AssertionError("ASGI server did not stop within five seconds")


def _remove_authorization(request: httpx.Request) -> None:
    request.headers.pop("Authorization", None)


def _crc_table(polynomial: int, mask: int) -> tuple[int, ...]:
    values = []
    for byte in range(256):
        value = byte
        for _ in range(8):
            value = (value >> 1) ^ polynomial if value & 1 else value >> 1
        values.append(value & mask)
    return tuple(values)


_CRC64_NVME_MASK = (1 << 64) - 1
_CRC64_NVME_TABLE = _crc_table(0x9A6C9329AC4BC9B5, _CRC64_NVME_MASK)


@pytest.fixture(scope="session")
def cases() -> dict[str, ConformanceCase]:
    return load_cases(_required_environment("LOONFS_CONFORMANCE_CASES"))


@pytest.fixture(scope="session")
def harness() -> Iterator[Harness]:
    base_url = _required_environment("LOONFS_CONFORMANCE_URL")
    token = _required_environment("LOONFS_CONFORMANCE_TOKEN")
    unauthenticated_http = httpx.Client(
        event_hooks={"request": [_remove_authorization]}
    )
    try:
        yield Harness(
            client=LoonFS(base_url=base_url, token=token),
            unauthenticated=LoonFS(
                base_url=base_url,
                token="",
                httpx_client=unauthenticated_http,
            ),
        )
    finally:
        unauthenticated_http.close()


@pytest.fixture(scope="session")
def proxy_harness(
    cases: dict[str, ConformanceCase],
) -> Iterator[str]:
    request, _ = _decode(cases["proxy"], ProxyRequest, ProxyExpected)
    app = LoonFSProxy(
        _required_environment("LOONFS_CONFORMANCE_URL"),
        _required_environment("LOONFS_CONFORMANCE_TOKEN"),
        {request.namespace_alias: request.namespace_id},
    )
    with _serve_asgi(app, "loonfs-python-proxy") as base_url:
        yield base_url


def _apply(
    client: LoonFS,
    namespace_id: str,
    commit_id: str,
    actor: ActorRef,
    operation: Any,
    *,
    message: str | None = None,
    content_tokens: list[str] | None = None,
) -> Any:
    extra = {}
    if message is not None:
        extra["message"] = message
    if content_tokens is not None:
        extra["content_tokens"] = content_tokens
    return client.commits.create(
        namespace_id,
        actor=actor,
        commit_id=commit_id,
        operations=[operation],
        **extra,
    )


def _byte_pattern(pattern: BytePattern) -> bytes:
    if pattern.modulus == 0:
        raise AssertionError("byte pattern modulus must be greater than zero")
    return bytes(offset % pattern.modulus for offset in range(pattern.length))


def _checksum(algorithm: str, content: bytes) -> Checksum:
    if algorithm == "sha256":
        value = hashlib.sha256(content).hexdigest()
    elif algorithm == "crc64nvme":
        value = f"{_crc(content, _CRC64_NVME_TABLE, _CRC64_NVME_MASK):016x}"
    else:
        raise AssertionError(f"unsupported checksum algorithm {algorithm!r}")
    return Checksum(algorithm=algorithm, value=value)


def _crc(content: bytes, table: tuple[int, ...], mask: int) -> int:
    value = mask
    for byte in content:
        value = table[(value ^ byte) & 0xFF] ^ (value >> 8)
    return value ^ mask


def _put_presigned(access: Any, content: bytes) -> httpx.Response:
    assert access.method.upper() == "PUT"
    response = httpx.put(
        access.url,
        headers=access.headers or {},
        content=content,
    )
    response.raise_for_status()
    return response


def _completed_upload(response: UploadSession) -> UploadSession_Completed:
    assert isinstance(
        response, UploadSession_Completed
    ), f"upload {response.upload_id} is {response.status}, not completed"
    return response


def _stage_content(
    client: LoonFS, namespace_id: str, payload: bytes
) -> UploadSession_Completed:
    begin = client.uploads.create(
        namespace_id, request=BeginUploadRequest_ServiceProxied()
    )
    assert isinstance(begin, BeginUploadResponse_ServiceProxied)
    client.uploads.put_content(namespace_id, begin.upload_id, request=payload)
    return _completed_upload(
        client.uploads.complete(
            namespace_id,
            begin.upload_id,
            request=CompleteUploadRequest_ServiceProxied(),
        )
    )


def _aborted_upload(response: UploadSession) -> UploadSession_Aborted:
    assert isinstance(
        response, UploadSession_Aborted
    ), f"upload {response.upload_id} is {response.status}, not aborted"
    return response


def _file_entry(entry: PathEntry) -> PathEntry_File:
    assert isinstance(
        entry, PathEntry_File
    ), f"path {entry.path} is a {entry.inode_kind}, not a file"
    return entry


def _wire(model: pydantic.BaseModel) -> JsonObject:
    """Render one SDK model back into the JSON the proxy route accepts."""
    return model.model_dump(mode="json")


def _read_proxied(client: LoonFS, namespace_id: str, path: str) -> bytes:
    return b"".join(client.files.content(namespace_id, path=path))


def _proxy_response_json(response: httpx.Response, label: str) -> JsonObject:
    response.raise_for_status()
    value = response.json()
    if not isinstance(value, dict):
        raise AssertionError(f"{label} must be a JSON object")
    return value


def _proxy_create_commit(
    client: httpx.Client,
    request: ProxyRequest,
    commit_id: str,
    operation: JsonObject,
    content_token: JsonObject | None = None,
) -> CommitResponse:
    body: JsonObject = {
        "actor": {"id": request.actor.id, "kind": request.actor.kind},
        "commit_id": commit_id,
        "operations": [operation],
    }
    if content_token is not None:
        body["content_tokens"] = [content_token]
    response = client.post(
        f"/v0/namespace-aliases/{request.namespace_alias}/commits",
        json=body,
    )
    return CommitResponse(**_proxy_response_json(response, "proxy commit response"))




def _listed_names(entries: list[Any]) -> list[str]:
    names: list[str] = []
    for entry in entries:
        assert entry.display_name is not None, "listed entry has no display_name"
        names.append(entry.display_name)
    return names


def _list_path_entries(
    client: LoonFS,
    request: PaginationRequest,
    cursor: str | None,
) -> Any:
    if cursor is None:
        return client.files.list(
            request.namespace_id,
            path=request.directory,
            limit=request.page_size,
        )
    return client.files.list(
        request.namespace_id,
        path=request.directory,
        limit=request.page_size,
        cursor=cursor,
    )


def _list_inode_children(
    client: LoonFS,
    request: ChildrenByInodeRequest,
    parent_inode_id: str,
    cursor: str | None,
) -> Any:
    if cursor is None:
        return client.inodes.list_children(
            request.namespace_id,
            parent_inode_id,
            limit=request.page_size,
        )
    return client.inodes.list_children(
        request.namespace_id,
        parent_inode_id,
        limit=request.page_size,
        cursor=cursor,
    )


def test_error_contract(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["error_contract"], ErrorContractRequest, ErrorContractExpected
    )
    with pytest.raises(UnauthorizedError) as captured:
        harness.unauthenticated.namespaces.retrieve(request.namespace_id)

    error = captured.value
    assert error.status_code == expected.unauthenticated.status
    assert error.body.code == expected.unauthenticated.code
    assert error.body.request_id is not None


def test_commit_replay(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["commit_replay"], CommitReplayRequest, CommitReplayExpected
    )
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    first = _apply(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.path, parents=False),
        message=request.message,
    )
    replayed = _apply(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.path, parents=False),
        message=request.message,
    )

    assert first.committed_seq == expected.committed_seq
    assert first.commit_id == request.commit_id
    assert replayed.committed_seq == first.committed_seq
    assert replayed.commit_id == first.commit_id
    assert replayed.namespace_id == first.namespace_id


def test_pagination(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["pagination"], PaginationRequest, PaginationExpected
    )
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    _apply(
        harness.client,
        request.namespace_id,
        "conf-pagination-directory",
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.directory, parents=False),
    )
    for index, name in enumerate(request.entry_names):
        _apply(
            harness.client,
            request.namespace_id,
            f"conf-pagination-entry-{index:02d}",
            request.actor,
            FilesystemOperation_CreateDirectory(
                path=f"{request.directory}/{name}", parents=False
            ),
        )

    observed: list[str] = []
    cursor: str | None = None
    saved_cursor: str | None = None
    resume_offset: int | None = None
    page_count = 0
    while True:
        page = _list_path_entries(harness.client, request, cursor)
        page_count += 1
        assert page.head_seq == expected.head_seq
        observed.extend(_listed_names(page.entries))
        cursor = page.next_cursor
        if page_count == request.resume_after_page:
            saved_cursor = cursor
            resume_offset = len(observed)
        if cursor is None:
            break

    assert len(observed) == expected.entry_count
    assert page_count >= expected.minimum_page_count
    assert cursor is None
    assert saved_cursor is not None, "resume cursor was not recorded"
    assert resume_offset is not None, "resume position was not recorded"

    resumed: list[str] = []
    cursor = saved_cursor
    while True:
        page = _list_path_entries(harness.client, request, cursor)
        resumed.extend(_listed_names(page.entries))
        cursor = page.next_cursor
        if cursor is None:
            break

    assert len(set(observed)) == len(observed), (
        "pagination returned an entry more than once"
    )
    assert observed == request.entry_names
    assert resume_offset <= len(request.entry_names)
    assert resumed == request.entry_names[resume_offset:]


def test_children_by_inode(
    cases: dict[str, ConformanceCase], harness: Harness
) -> None:
    request, expected = _decode(
        cases["children_by_inode"], ChildrenByInodeRequest, ChildrenByInodeExpected
    )
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    _apply(
        harness.client,
        request.namespace_id,
        "conf-children-by-inode-directory",
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.directory, parents=False),
    )
    for index, name in reversed(list(enumerate(request.entry_names))):
        _apply(
            harness.client,
            request.namespace_id,
            f"conf-children-by-inode-entry-{index:02d}",
            request.actor,
            FilesystemOperation_CreateDirectory(
                path=f"{request.directory}/{name}", parents=False
            ),
        )

    parent_inode_id = harness.client.files.retrieve(
        request.namespace_id, path=request.directory
    ).inode_id
    observed: list[str] = []
    cursor: str | None = None
    saved_cursor: str | None = None
    resume_offset: int | None = None
    page_count = 0
    while True:
        page = _list_inode_children(
            harness.client, request, parent_inode_id, cursor
        )
        page_count += 1
        assert page.namespace_id == request.namespace_id
        assert page.parent_inode_id == parent_inode_id
        expected_head_seq = (
            expected.initial_head_seq
            if page_count <= request.rename_after_page
            else expected.renamed_head_seq
        )
        assert page.head_seq == expected_head_seq
        observed.extend(_listed_names(page.entries))
        cursor = page.next_cursor
        if page_count == request.resume_after_page:
            saved_cursor = cursor
            resume_offset = len(observed)
        if page_count == request.rename_after_page:
            renamed = _apply(
                harness.client,
                request.namespace_id,
                request.rename_commit_id,
                request.actor,
                FilesystemOperation_MovePath(
                    from_path=request.directory,
                    to_path=request.renamed_directory,
                ),
            )
            assert renamed.committed_seq == expected.renamed_head_seq
            renamed_inode_id = harness.client.files.retrieve(
                request.namespace_id, path=request.renamed_directory
            ).inode_id
            assert renamed_inode_id == parent_inode_id
        if cursor is None:
            break

    assert len(observed) == expected.entry_count
    assert page_count >= expected.minimum_page_count
    assert saved_cursor is not None, "resume cursor was not recorded"
    assert resume_offset is not None, "resume position was not recorded"

    resumed: list[str] = []
    cursor = saved_cursor
    while True:
        page = _list_inode_children(
            harness.client, request, parent_inode_id, cursor
        )
        assert page.namespace_id == request.namespace_id
        assert page.parent_inode_id == parent_inode_id
        assert page.head_seq == expected.renamed_head_seq
        resumed.extend(_listed_names(page.entries))
        cursor = page.next_cursor
        if cursor is None:
            break

    assert len(set(observed)) == len(observed), (
        "children-by-inode pagination returned an entry more than once"
    )
    assert observed == request.entry_names
    assert resume_offset <= len(request.entry_names)
    assert resumed == request.entry_names[resume_offset:]


def test_inode_mutations(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["inode_mutations"], InodeMutationsRequest, InodeMutationsExpected
    )
    client = harness.client
    namespace_id = request.namespace_id

    def child_path(name: str) -> str:
        return f"{request.directory}/{name}"

    client.namespaces.create(namespace_id=namespace_id)
    _apply(
        client,
        namespace_id,
        "conf-inode-mutations-directory",
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.directory, parents=False),
    )
    _apply(
        client,
        namespace_id,
        "conf-inode-mutations-path-directory",
        request.actor,
        FilesystemOperation_CreateDirectory(
            path=child_path(request.path_directory_name), parents=False
        ),
    )
    client.files.upload(
        namespace_id,
        path=child_path(request.path_file_name),
        content=request.content_utf8.encode(),
        actor=request.actor,
        commit_id="conf-inode-mutations-path-file",
    )

    parent_inode_id = client.files.retrieve(
        namespace_id, path=request.directory
    ).inode_id
    _apply(
        client,
        namespace_id,
        "conf-inode-mutations-inode-directory",
        request.actor,
        FilesystemOperation_CreateDirectoryByInode(
            parent_inode_id=parent_inode_id,
            display_name=request.inode_directory_name,
        ),
    )
    staged = _stage_content(client, namespace_id, request.content_utf8.encode())
    _apply(
        client,
        namespace_id,
        "conf-inode-mutations-inode-file",
        request.actor,
        FilesystemOperation_PutFileByInode(
            parent_inode_id=parent_inode_id,
            display_name=request.inode_file_name,
            content_ref=staged.content_ref,
        ),
        content_tokens=[staged.content_token] if staged.content_token else None,
    )

    entries = client.files.list(
        namespace_id, path=request.directory
    ).entries
    assert _listed_names(entries) == expected.entry_names
    generations = {entry.binding_generation for entry in entries}
    assert None not in generations, "listed entry has no binding_generation"
    assert len(generations) == len(entries)

    def entry_named(name: str) -> PathEntry:
        return next(entry for entry in entries if entry.display_name == name)

    assert (
        entry_named(request.inode_directory_name).inode_kind
        == entry_named(request.path_directory_name).inode_kind
    )
    inode_file = _file_entry(entry_named(request.inode_file_name))
    assert inode_file.size_bytes == _file_entry(
        entry_named(request.path_file_name)
    ).size_bytes
    assert inode_file.parent_inode_id == parent_inode_id

    staged = _stage_content(client, namespace_id, request.revised_content_utf8.encode())
    _apply(
        client,
        namespace_id,
        "conf-inode-mutations-revision",
        request.actor,
        FilesystemOperation_PutFileRevisionByInode(
            inode_id=inode_file.inode_id,
            content_ref=staged.content_ref,
            expected_revision_no=inode_file.revision_no,
        ),
        content_tokens=[staged.content_token] if staged.content_token else None,
    )
    revised = _file_entry(
        client.files.retrieve(
            namespace_id, path=child_path(request.inode_file_name)
        )
    )
    assert revised.revision_no == expected.revised_revision_no
    assert (
        _read_proxied(client, namespace_id, child_path(request.inode_file_name))
        == request.revised_content_utf8.encode()
    )

    _apply(
        client,
        namespace_id,
        "conf-inode-mutations-rename",
        request.actor,
        FilesystemOperation_MovePath(
            from_path=child_path(request.inode_file_name),
            to_path=child_path(request.renamed_file_name),
        ),
    )

    def move_by_inode(commit_id: str, generation: str) -> Any:
        return _apply(
            client,
            namespace_id,
            commit_id,
            request.actor,
            FilesystemOperation_MoveByInode(
                inode_id=inode_file.inode_id,
                expected_binding_generation=generation,
                to_parent_inode_id=entry_named(request.inode_directory_name).inode_id,
                to_display_name=request.moved_file_name,
            ),
        )

    with pytest.raises(ConflictError) as stale:
        move_by_inode("conf-inode-mutations-stale-move", revised.binding_generation)
    assert stale.value.status_code == expected.stale_binding_generation.status
    assert stale.value.body.code == expected.stale_binding_generation.code

    with pytest.raises(BadRequestError) as malformed:
        move_by_inode(
            "conf-inode-mutations-malformed-move",
            request.malformed_binding_generation,
        )
    assert malformed.value.status_code == expected.malformed_binding_generation.status
    assert malformed.value.body.code == expected.malformed_binding_generation.code

    fresh_generation = client.files.retrieve(
        namespace_id, path=child_path(request.renamed_file_name)
    ).binding_generation
    moved = move_by_inode("conf-inode-mutations-move", fresh_generation)
    assert moved.committed_seq == expected.moved_committed_seq
    moved_entry = client.files.retrieve(
        namespace_id,
        path=f"{child_path(request.inode_directory_name)}/{request.moved_file_name}",
    )
    assert moved_entry.inode_id == inode_file.inode_id
    assert moved_entry.binding_generation != fresh_generation

    feed = client.changes.list(
        namespace_id, after_seq=expected.moved_committed_seq - 1, limit=1
    )
    assert len(feed.changes) == 1
    events = feed.changes[0].events
    assert len(events) == 1
    assert events[0].kind == "moved"
    assert events[0].binding_generation == moved_entry.binding_generation

    deleted = _apply(
        client,
        namespace_id,
        "conf-inode-mutations-delete",
        request.actor,
        FilesystemOperation_DeleteByInode(
            inode_id=inode_file.inode_id,
            expected_binding_generation=moved_entry.binding_generation,
        ),
    )
    assert deleted.committed_seq == expected.deleted_committed_seq


def test_snapshots(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["snapshots"], SnapshotsRequest, SnapshotsExpected
    )
    client = harness.client
    namespace_id = request.namespace_id

    def child_path(name: str) -> str:
        return f"{request.directory}/{name}"

    client.namespaces.create(namespace_id=namespace_id)
    _apply(
        client,
        namespace_id,
        "conf-snapshots-create-directory",
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.directory, parents=False),
    )
    client.files.upload(
        namespace_id,
        path=child_path(request.replaced_file_name),
        content=request.captured_content_utf8.encode(),
        actor=request.actor,
        commit_id="conf-snapshots-create-replaced",
    )
    client.files.upload(
        namespace_id,
        path=child_path(request.deleted_file_name),
        content=request.deleted_content_utf8.encode(),
        actor=request.actor,
        commit_id="conf-snapshots-create-deleted",
    )

    snapshot = client.snapshots.create(
        namespace_id,
        name=request.snapshot_name,
        ttl_ms=request.create_ttl_ms,
    )
    assert snapshot.namespace_id == namespace_id
    assert snapshot.name == request.snapshot_name
    assert snapshot.head_seq == expected.snapshot_head_seq
    assert snapshot.expires_at_ms > snapshot.created_at_ms

    client.files.upload(
        namespace_id,
        path=child_path(request.replaced_file_name),
        content=request.current_content_utf8.encode(),
        actor=request.actor,
        commit_id="conf-snapshots-replace-file",
        behavior="replace",
    )
    client.files.upload(
        namespace_id,
        path=child_path(request.added_file_name),
        content=request.added_content_utf8.encode(),
        actor=request.actor,
        commit_id="conf-snapshots-add-file",
    )
    _apply(
        client,
        namespace_id,
        "conf-snapshots-delete-file",
        request.actor,
        FilesystemOperation_DeletePath(path=child_path(request.deleted_file_name)),
    )

    captured_entry = _file_entry(
        client.files.retrieve(
            namespace_id,
            path=child_path(request.replaced_file_name),
            snapshot_id=snapshot.snapshot_id,
        )
    )
    assert captured_entry.revision_no == expected.captured_revision_no
    current_entry = _file_entry(
        client.files.retrieve(
            namespace_id,
            path=child_path(request.replaced_file_name),
        )
    )
    assert current_entry.revision_no == expected.current_revision_no

    captured_listing = client.files.list(
        namespace_id,
        path=request.directory,
        snapshot_id=snapshot.snapshot_id,
    )
    assert captured_listing.head_seq == expected.snapshot_head_seq
    assert _listed_names(captured_listing.entries) == expected.captured_entry_names
    current_listing = client.files.list(
        namespace_id,
        path=request.directory,
    )
    assert _listed_names(current_listing.entries) == expected.current_entry_names

    captured_content = b"".join(
        client.files.content(
            namespace_id,
            path=child_path(request.replaced_file_name),
            snapshot_id=snapshot.snapshot_id,
        )
    )
    assert captured_content == request.captured_content_utf8.encode()
    current_content = b"".join(
        client.files.content(
            namespace_id,
            path=child_path(request.replaced_file_name),
        )
    )
    assert current_content == request.current_content_utf8.encode()

    feed = client.changes.list(
        namespace_id,
        after_seq=0,
        limit=100,
        snapshot_id=snapshot.snapshot_id,
    )
    assert feed.through_seq == expected.snapshot_head_seq
    assert feed.next_after_seq is None
    assert [change.committed_seq for change in feed.changes] == (
        expected.snapshot_change_seqs
    )

    extended = client.snapshots.extend(
        namespace_id,
        snapshot.snapshot_id,
        ttl_ms=request.extend_ttl_ms,
    )
    assert extended.snapshot_id == snapshot.snapshot_id
    assert extended.head_seq == expected.snapshot_head_seq
    assert extended.name == request.snapshot_name
    assert extended.expires_at_ms > snapshot.expires_at_ms

    listed = client.snapshots.list(namespace_id)
    assert listed.namespace_id == namespace_id
    assert listed.next_cursor is None
    assert len(listed.snapshots) == 1
    assert listed.snapshots[0].snapshot_id == snapshot.snapshot_id

    first_release = client.snapshots.release(
        namespace_id, snapshot.snapshot_id
    )
    assert first_release.namespace_id == namespace_id
    assert first_release.snapshot_id == snapshot.snapshot_id
    second_release = client.snapshots.release(
        namespace_id, snapshot.snapshot_id
    )
    assert second_release.namespace_id == namespace_id
    assert second_release.snapshot_id == snapshot.snapshot_id

    with pytest.raises(GoneError) as released_read:
        client.files.retrieve(
            namespace_id,
            path=child_path(request.replaced_file_name),
            snapshot_id=snapshot.snapshot_id,
        )
    assert released_read.value.status_code == expected.snapshot_gone.status
    assert released_read.value.body.code == expected.snapshot_gone.code
    with pytest.raises(GoneError) as released_extend:
        client.snapshots.extend(
            namespace_id,
            snapshot.snapshot_id,
            ttl_ms=request.extend_ttl_ms,
        )
    assert released_extend.value.status_code == expected.snapshot_gone.status
    assert released_extend.value.body.code == expected.snapshot_gone.code

    with pytest.raises(NotFoundError) as unknown_read:
        client.files.retrieve(
            namespace_id,
            path=child_path(request.replaced_file_name),
            snapshot_id=request.unknown_snapshot_id,
        )
    assert unknown_read.value.status_code == expected.snapshot_not_found.status
    assert unknown_read.value.body.code == expected.snapshot_not_found.code
    with pytest.raises(BadRequestError) as revision_with_snapshot:
        b"".join(
            client.files.content(
                namespace_id,
                path=child_path(request.replaced_file_name),
                revision_no=expected.captured_revision_no,
                snapshot_id=snapshot.snapshot_id,
            )
        )
    assert (
        revision_with_snapshot.value.status_code
        == expected.revision_with_snapshot.status
    )
    assert (
        revision_with_snapshot.value.body.code == expected.revision_with_snapshot.code
    )
    with pytest.raises(BadRequestError) as zero_ttl:
        client.snapshots.create(
            namespace_id,
            name=request.snapshot_name,
            ttl_ms=0,
        )
    assert zero_ttl.value.status_code == expected.zero_ttl.status
    assert zero_ttl.value.body.code == expected.zero_ttl.code


def test_proxy(
    cases: dict[str, ConformanceCase],
    harness: Harness,
    proxy_harness: str,
) -> None:
    request, expected = _decode(cases["proxy"], ProxyRequest, ProxyExpected)
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    payload = request.content_utf8.encode()
    namespace_alias_base = f"/v0/namespace-aliases/{request.namespace_alias}"

    with httpx.Client(
        base_url=proxy_harness,
        headers={"Authorization": "Bearer browser-token"},
    ) as client:
        mkdir = _proxy_create_commit(
            client,
            request,
            request.commit_ids.directory,
            {
                "kind": "create_directory",
                "parents": False,
                "path": request.directory,
            },
        )
        assert mkdir.committed_seq == expected.mkdir_committed_seq

        proxied_begin_response = client.post(
            f"{namespace_alias_base}/uploads",
            json={"mode": "service_proxied"},
        )
        proxied_begin = BeginUploadResponse_ServiceProxied(
            **_proxy_response_json(
                proxied_begin_response,
                "proxy service-proxied begin response",
            )
        )
        split = len(payload) // 2
        uploaded_response = client.put(
            f"{namespace_alias_base}/uploads/{proxied_begin.upload_id}/content",
            headers={"Content-Type": "application/octet-stream"},
            content=iter((payload[:split], payload[split:])),
        )
        uploaded = UploadContentResponse(
            **_proxy_response_json(
                uploaded_response,
                "proxy upload-content response",
            )
        )
        proxied_complete_response = client.post(
            f"{namespace_alias_base}/uploads/{proxied_begin.upload_id}/complete",
            json={"mode": "service_proxied"},
        )
        proxied_complete_data = _proxy_response_json(
            proxied_complete_response,
            "proxy service-proxied complete response",
        )
        proxied_complete = UploadSession_Completed(**proxied_complete_data)
        assert proxied_complete.content_ref == uploaded.content_ref
        assert proxied_complete.content_token is not None
        proxied_commit = _proxy_create_commit(
            client,
            request,
            request.commit_ids.proxied,
            {
                "kind": "put_file",
                "path": request.proxied_path,
                "content_ref": _wire(proxied_complete.content_ref),
            },
            _wire(proxied_complete.content_token),
        )
        assert proxied_commit.committed_seq == expected.proxied_committed_seq

        direct_begin_response = client.post(
            f"{namespace_alias_base}/uploads",
            json={"mode": "direct_put", "size_bytes": len(payload)},
        )
        direct_begin = BeginUploadResponse_DirectPut(
            **_proxy_response_json(
                direct_begin_response,
                "proxy direct-PUT begin response",
            )
        )
        direct_access = direct_begin.access
        assert direct_access.method.upper() == "PUT"
        direct_put_response = httpx.request(
            direct_access.method,
            direct_access.url,
            headers=direct_access.headers or {},
            content=payload,
        )
        direct_put_response.raise_for_status()
        direct_checksum = _checksum(
            direct_begin.checksum_algorithm,
            payload,
        )
        direct_complete_response = client.post(
            f"{namespace_alias_base}/uploads/{direct_begin.upload_id}/complete",
            json={
                "mode": "direct_put",
                "content": {
                    "size_bytes": len(payload),
                    "checksum": {
                        "algorithm": direct_checksum.algorithm,
                        "value": direct_checksum.value,
                    },
                },
            },
        )
        direct_complete_data = _proxy_response_json(
            direct_complete_response,
            "proxy direct-PUT complete response",
        )
        direct_complete = UploadSession_Completed(**direct_complete_data)
        assert direct_complete.content_ref.size_bytes == len(payload)
        assert direct_complete.content_token is not None
        direct_commit = _proxy_create_commit(
            client,
            request,
            request.commit_ids.direct,
            {
                "kind": "put_file",
                "path": request.direct_path,
                "content_ref": _wire(direct_complete.content_ref),
            },
            _wire(direct_complete.content_token),
        )
        assert direct_commit.committed_seq == expected.direct_committed_seq

        listing_response = client.get(
            f"{namespace_alias_base}/filesystem/entries",
            params={"path": request.directory},
        )
        listing = ListPathEntriesResponse(
            **_proxy_response_json(listing_response, "proxy list response")
        )
        assert len(listing.entries) == expected.entry_count

        with client.stream(
            "GET",
            f"{namespace_alias_base}/filesystem/content",
            params={"path": request.proxied_path},
        ) as read_response:
            read_response.raise_for_status()
            assert b"".join(read_response.iter_raw()) == payload

        unknown_namespace_alias = client.get(
            f"/v0/namespace-aliases/{request.unknown_namespace_alias}/filesystem/entries",
            params={"path": request.directory},
        )
        assert unknown_namespace_alias.status_code == expected.unknown_namespace_alias_status
        assert unknown_namespace_alias.content == b""

        disallowed_route = client.get(
            f"{namespace_alias_base}{request.disallowed_path_suffix}"
        )
        assert disallowed_route.status_code == expected.disallowed_route_status
        assert disallowed_route.content == b""


def test_changes(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(cases["changes"], ChangesRequest, ChangesExpected)
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    committed = _apply(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.path, parents=False),
    )
    assert committed.committed_seq == expected.committed_seq

    feed = harness.client.changes.list(
        request.namespace_id,
        after_seq=request.after_seq,
    )
    assert len(feed.changes) == expected.change_count
    assert feed.changes, "change feed is empty"
    change = feed.changes[0]
    assert change.commit_id == request.commit_id
    assert change.committed_by.id == request.actor.id
    assert change.committed_by.kind == request.actor.kind
    assert len(change.events) == 1
    assert change.events[0].kind == "directory_created"


def test_upload_direct_put(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["upload_direct_put"], DirectPutRequest, DirectPutExpected
    )
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    payload = request.content_utf8.encode()
    begin = harness.client.uploads.create(
        request.namespace_id,
        request=BeginUploadRequest_DirectPut(size_bytes=len(payload)),
    )

    assert begin.mode == expected.mode
    assert begin.checksum_algorithm == expected.checksum_algorithm

    _put_presigned(begin.access, payload)
    claim = UploadContentClaim(
        size_bytes=len(payload),
        checksum=_checksum(begin.checksum_algorithm, payload),
    )
    completed = harness.client.uploads.complete(
        request.namespace_id,
        begin.upload_id,
        request=CompleteUploadRequest_DirectPut(content=claim),
    )
    completed = _completed_upload(completed)
    content_ref = completed.content_ref
    content_token = completed.content_token
    assert content_ref.size_bytes == expected.size_bytes
    assert content_ref.checksum.algorithm == expected.checksum_algorithm
    assert content_ref.checksum == claim.checksum
    assert content_ref.checksum == _checksum(content_ref.checksum.algorithm, payload)

    committed = _apply(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        FilesystemOperation_PutFile(path=request.path, content_ref=content_ref),
        content_tokens=[content_token] if content_token is not None else None,
    )
    assert committed.committed_seq == expected.committed_seq
    stat = _file_entry(
        harness.client.files.retrieve(request.namespace_id, path=request.path)
    )
    assert stat.content_ref == content_ref
    assert _read_proxied(harness.client, request.namespace_id, request.path) == payload


def test_upload_multipart(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["upload_multipart"], MultipartRequest, MultipartExpected
    )
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    payload = _byte_pattern(request.content_pattern)
    begin = harness.client.uploads.create(
        request.namespace_id,
        request=BeginUploadRequest_DirectMultipart(
            part_size_bytes=request.part_size_bytes,
        ),
    )

    assert begin.mode == expected.mode
    assert begin.part_size_bytes == request.part_size_bytes
    assert begin.checksum_algorithm == expected.checksum_algorithm

    part_size = begin.part_size_bytes
    chunks = [
        payload[offset : offset + part_size]
        for offset in range(0, len(payload), part_size)
    ]
    assert len(chunks) == expected.part_count
    claims = [
        UploadPartChecksumClaim(
            part_number=index,
            checksum=_checksum(begin.checksum_algorithm, chunk),
        )
        for index, chunk in enumerate(chunks, start=1)
    ]
    signed = harness.client.uploads.sign_parts(
        request.namespace_id,
        begin.upload_id,
        parts=claims,
    )
    assert len(signed.parts) == expected.part_count

    completed_parts = []
    for signed_part in signed.parts:
        index = signed_part.part_number - 1
        response = _put_presigned(signed_part.access, chunks[index])
        etag = response.headers.get("etag")
        assert etag is not None, (
            f"part {signed_part.part_number} upload returned no ETag"
        )
        completed_parts.append(
            CompletedUploadPart(
                part_number=signed_part.part_number,
                etag=etag,
                checksum=claims[index].checksum,
            )
        )
    completed_parts.sort(key=lambda part: part.part_number)
    whole_checksum = _checksum(begin.checksum_algorithm, payload)
    completion_request = CompleteUploadRequest_DirectMultipart(
        content=UploadContentClaim(
            size_bytes=len(payload),
            checksum=whole_checksum,
        ),
        parts=completed_parts,
    )
    first = harness.client.uploads.complete(
        request.namespace_id,
        begin.upload_id,
        request=completion_request,
    )
    first_completed = _completed_upload(first)
    first_content_ref = first_completed.content_ref
    replayed = harness.client.uploads.complete(
        request.namespace_id,
        begin.upload_id,
        request=completion_request,
    )
    replayed_completed = _completed_upload(replayed)
    replayed_content_ref = replayed_completed.content_ref
    replayed_token = replayed_completed.content_token

    assert replayed.namespace_id == first.namespace_id
    assert replayed.upload_id == first.upload_id
    assert replayed.mode == first.mode
    assert replayed_content_ref == first_content_ref
    assert replayed_completed.completed_at_ms == first_completed.completed_at_ms
    assert first_content_ref.size_bytes == expected.size_bytes
    assert first_content_ref.checksum == whole_checksum
    assert first_content_ref.checksum == _checksum(
        first_content_ref.checksum.algorithm,
        payload,
    )

    committed = _apply(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        FilesystemOperation_PutFile(
            path=request.path, content_ref=first_content_ref
        ),
        content_tokens=[replayed_token] if replayed_token is not None else None,
    )
    assert committed.committed_seq == expected.committed_seq
    assert _read_proxied(harness.client, request.namespace_id, request.path) == payload

    # The same content through the high-level helper: the payload exceeds the
    # part size, so this exercises files.upload's multipart branch.
    helper_path = request.path + "-helper"
    helper_commit = harness.client.files.upload(
        request.namespace_id,
        path=helper_path,
        content=payload,
        actor=request.actor,
        commit_id=request.commit_id + "-helper",
    )
    assert helper_commit.committed_seq > 0
    helper_read = harness.client.files.download(
        request.namespace_id,
        path=helper_path,
    )
    assert helper_read.content == payload
    # Content ids are random per upload and the helper may choose a different
    # checksum algorithm; the comparable content fact is the size.
    assert helper_read.content_ref.size_bytes == first_content_ref.size_bytes


def test_upload_abort(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(cases["upload_abort"], AbortRequest, AbortExpected)
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    begin = harness.client.uploads.create(
        request.namespace_id,
        request=BeginUploadRequest_ServiceProxied(),
    )
    first = _aborted_upload(
        harness.client.uploads.abort(request.namespace_id, begin.upload_id)
    )
    replayed = _aborted_upload(
        harness.client.uploads.abort(request.namespace_id, begin.upload_id)
    )

    assert first.mode == expected.mode
    assert first.status == expected.status
    assert replayed == first
    assert replayed.aborted_at_ms == first.aborted_at_ms


def test_download(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["download"], DownloadRequest, DownloadExpected
    )
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    payload = request.content_utf8.encode()
    committed = harness.client.files.upload(
        request.namespace_id,
        path=request.path,
        content=payload,
        actor=request.actor,
        commit_id=request.commit_id,
    )
    assert committed.committed_seq == expected.committed_seq

    stat = _file_entry(
        harness.client.files.retrieve(request.namespace_id, path=request.path)
    )
    downloaded = harness.client.files.download(
        request.namespace_id,
        path=request.path,
    )
    assert stat.content_ref == downloaded.content_ref
    assert downloaded.content_ref.size_bytes == expected.size_bytes
    assert downloaded.content_ref.checksum.algorithm == expected.checksum_algorithm
    assert len(downloaded.content) == downloaded.content_ref.size_bytes
    assert downloaded.content_ref.checksum == _checksum(
        downloaded.content_ref.checksum.algorithm,
        downloaded.content,
    )
    assert downloaded.content == payload


def test_end_to_end(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["end_to_end"], EndToEndRequest, EndToEndExpected
    )
    harness.client.namespaces.create(namespace_id=request.namespace_id)
    mkdir = _apply(
        harness.client,
        request.namespace_id,
        request.commit_ids.mkdir,
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.directory, parents=False),
    )
    assert mkdir.committed_seq == expected.mkdir_committed_seq

    payload = request.content_utf8.encode()
    upload = harness.client.files.upload(
        request.namespace_id,
        path=request.upload_path,
        content=payload,
        actor=request.actor,
        commit_id=request.commit_ids.upload,
    )
    assert upload.committed_seq == expected.upload_committed_seq
    stat = harness.client.files.retrieve(
        request.namespace_id, path=request.upload_path
    )
    assert stat.size_bytes == expected.size_bytes
    uploaded_inode = stat.inode_id

    initial_listing = harness.client.files.list(
        request.namespace_id,
        path=request.directory,
    )
    assert any(entry.path == request.upload_path for entry in initial_listing.entries)

    downloaded = harness.client.files.download(
        request.namespace_id,
        path=request.upload_path,
    )
    assert downloaded.content == payload

    moved = _apply(
        harness.client,
        request.namespace_id,
        request.commit_ids.move,
        request.actor,
        FilesystemOperation_MovePath(
            from_path=request.upload_path,
            to_path=request.moved_path,
        ),
    )
    assert moved.committed_seq == expected.move_committed_seq
    moved_listing = harness.client.files.list(
        request.namespace_id,
        path=request.directory,
    )
    assert any(entry.path == request.moved_path for entry in moved_listing.entries)

    revisions = harness.client.files.list_revisions(
        request.namespace_id,
        path=request.moved_path,
    )
    assert len(revisions.revisions) == expected.revision_count
    assert revisions.revisions[0].commit_id == request.commit_ids.upload

    changes_before_remove = harness.client.changes.list(
        request.namespace_id,
        after_seq=0,
    )
    assert len(changes_before_remove.changes) == expected.change_count - 1

    removed = _apply(
        harness.client,
        request.namespace_id,
        request.commit_ids.remove,
        request.actor,
        FilesystemOperation_DeletePath(path=request.moved_path),
    )
    assert removed.committed_seq == expected.remove_committed_seq

    changes = harness.client.changes.list(
        request.namespace_id,
        after_seq=0,
    )
    assert len(changes.changes) == expected.change_count
    assert [change.commit_id for change in changes.changes] == [
        request.commit_ids.mkdir,
        request.commit_ids.upload,
        request.commit_ids.move,
        request.commit_ids.remove,
    ]
    assert all(
        change.committed_by.id == request.actor.id
        and change.committed_by.kind == request.actor.kind
        for change in changes.changes
    )

    trash = harness.client.trash.list(request.namespace_id)
    removed_entry = next(
        entry for entry in trash.entries if entry.inode_id == uploaded_inode
    )
    assert removed_entry.deletion_seq == removed.committed_seq


def test_proxy_forwards_every_documented_route(
    cases: dict[str, ConformanceCase],
) -> None:
    """Proxy routes must reach the server. Excluded server routes must not."""
    fixture, _ = _decode(cases["proxy"], ProxyRequest, ProxyExpected)
    with open(_required_environment("LOONFS_PROXY_DOCUMENT"), encoding="utf-8") as handle:
        proxy_document = json.load(handle)
    with open(_required_environment("LOONFS_SERVER_DOCUMENT"), encoding="utf-8") as handle:
        server_document = json.load(handle)

    proxy_routes = {
        (method.upper(), template)
        for template, item in proxy_document["paths"].items()
        for method in item
    }

    observed: list[tuple[str, str]] = []

    class RecordingHandler(BaseHTTPRequestHandler):
        def _record(self) -> None:
            observed.append((self.command, urlsplit(self.path).path))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        do_GET = _record
        do_DELETE = _record
        do_POST = _record
        do_PUT = _record

        def log_message(self, _format: str, *args: object) -> None:
            pass

    stub = HTTPServer(("127.0.0.1", 0), RecordingHandler)
    stub_thread = threading.Thread(
        target=stub.serve_forever,
        name="loonfs-python-recording-stub",
        daemon=True,
    )
    stub_thread.start()
    stub_host, stub_port = stub.server_address
    proxy = LoonFSProxy(
        f"http://{stub_host}:{stub_port}",
        "recording-stub-token",
        {fixture.namespace_alias: fixture.namespace_id},
    )

    def instantiate(template: str) -> str:
        return re.sub(
            r"\{([^/{}]+)\}",
            lambda match: fixture.namespace_alias
            if match.group(1) == "namespace_alias"
            else "x",
            template,
        )

    def proxy_template_for_server(template: str) -> str:
        server_namespace_prefix = "/v0/namespaces/{namespace_id}"
        if template == server_namespace_prefix or template.startswith(
            f"{server_namespace_prefix}/"
        ):
            return template.replace(
                server_namespace_prefix,
                "/v0/namespace-aliases/{namespace_alias}",
                1,
            )
        return template

    try:
        with _serve_asgi(proxy, "loonfs-python-drift-proxy") as proxy_base_url:
            expected: list[tuple[str, str]] = []
            with httpx.Client(base_url=proxy_base_url) as client:
                for template, item in proxy_document["paths"].items():
                    for documented_method in item:
                        method = documented_method.upper()
                        path = instantiate(template)
                        forwarded_template = template.replace(
                            "/v0/namespace-aliases/{namespace_alias}",
                            f"/v0/namespaces/{fixture.namespace_id}",
                        )
                        expected.append((method, instantiate(forwarded_template)))
                        response = client.request(method, path)
                        assert response.status_code == 200

                assert sorted(observed) == sorted(expected)

                observed_before = list(observed)
                for server_template, item in server_document["paths"].items():
                    proxy_template = proxy_template_for_server(server_template)
                    for documented_method in item:
                        method = documented_method.upper()
                        if (method, proxy_template) in proxy_routes:
                            continue
                        path = instantiate(proxy_template)
                        response = client.request(method, path)
                        assert response.status_code == 404, f"{method} {path}"
                assert observed == observed_before
    finally:
        stub.shutdown()
        stub.server_close()
        stub_thread.join(timeout=5)
        if stub_thread.is_alive():
            raise AssertionError("recording stub did not stop within five seconds")
