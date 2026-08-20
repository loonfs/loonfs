from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import threading
import time
from collections.abc import Iterator, Mapping
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
from loonfs_sdk import (
    ActorRef,
    BeginUploadRequest_DirectMultipart,
    BeginUploadRequest_DirectPut,
    BeginUploadRequest_ServiceProxied,
    BeginUploadResponse_DirectPut,
    BeginUploadResponse_ServiceProxied,
    Checksum,
    CommitResponse,
    CompleteUploadRequest_DirectMultipart,
    CompleteUploadRequest_DirectPut,
    CompletedUploadPart,
    ContentRef,
    DirectMultipartUploadOptions,
    FilesystemOperation_CreateDirectory,
    FilesystemOperation_DeletePath,
    FilesystemOperation_MovePath,
    FilesystemOperation_PutFile,
    ListPathEntriesResponse,
    LoonFS,
    UnauthorizedError,
    UploadContentResponse,
    UploadContentClaim,
    UploadPartChecksumClaim,
    UploadSessionResponse,
)
from loonfs_sdk.proxy import LoonFSProxy
from loonfs_sdk.transfers import get_file, put_file


RUNNER_SKIP = "run scripts/run-sdk-conformance.sh python"
EXPECTED_CASES = [
    "changes",
    "commit_replay",
    "download",
    "end_to_end",
    "error_contract",
    "pagination",
    "proxy",
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
    # Strict dataclasses accept fixture mappings through Pydantic's JSON validation path.
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
    return client.filesystem.apply_commit(
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


def _content_ref(value: object) -> ContentRef:
    if isinstance(value, ContentRef):
        return value
    if isinstance(value, Mapping):
        return ContentRef(**value)
    raise AssertionError("response has no content reference")


def _completed_content(response: Any) -> tuple[ContentRef, str | None, int]:
    assert response.status == "completed"
    return (
        _content_ref(response.content_ref),
        getattr(response, "content_token", None),
        response.completed_at_ms,
    )


def _read_proxied(client: LoonFS, namespace_id: str, path: str) -> bytes:
    return b"".join(client.filesystem.get_file_bytes(namespace_id, path=path))


def _proxy_response_json(response: httpx.Response, label: str) -> JsonObject:
    response.raise_for_status()
    value = response.json()
    if not isinstance(value, dict):
        raise AssertionError(f"{label} must be a JSON object")
    return value


def _proxy_apply_commit(
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
        return client.filesystem.list_path_entries(
            request.namespace_id,
            path=request.directory,
            limit=request.page_size,
        )
    return client.filesystem.list_path_entries(
        request.namespace_id,
        path=request.directory,
        limit=request.page_size,
        cursor=cursor,
    )


def test_error_contract(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["error_contract"], ErrorContractRequest, ErrorContractExpected
    )
    with pytest.raises(UnauthorizedError) as captured:
        harness.unauthenticated.namespaces.get_namespace(request.namespace_id)

    error = captured.value
    assert error.status_code == expected.unauthenticated.status
    assert error.body.code == expected.unauthenticated.code
    assert error.body.request_id is not None


def test_commit_replay(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["commit_replay"], CommitReplayRequest, CommitReplayExpected
    )
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
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
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
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


def test_proxy(
    cases: dict[str, ConformanceCase],
    harness: Harness,
    proxy_harness: str,
) -> None:
    request, expected = _decode(cases["proxy"], ProxyRequest, ProxyExpected)
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    payload = request.content_utf8.encode()
    namespace_alias_base = f"/v0/namespace-aliases/{request.namespace_alias}"

    with httpx.Client(
        base_url=proxy_harness,
        headers={"Authorization": "Bearer browser-token"},
    ) as client:
        mkdir = _proxy_apply_commit(
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
        proxied_complete = UploadSessionResponse(**proxied_complete_data)
        assert proxied_complete.status == "completed"
        proxied_content_ref = proxied_complete_data["content_ref"]
        assert isinstance(proxied_content_ref, dict)
        assert ContentRef(**proxied_content_ref) == uploaded.content_ref
        proxied_content_token = proxied_complete_data["content_token"]
        assert isinstance(proxied_content_token, dict)
        proxied_commit = _proxy_apply_commit(
            client,
            request,
            request.commit_ids.proxied,
            {
                "kind": "put_file",
                "path": request.proxied_path,
                "content_ref": proxied_content_ref,
            },
            proxied_content_token,
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
        direct_access = direct_begin.direct_put.access
        assert direct_access.method.upper() == "PUT"
        direct_put_response = httpx.request(
            direct_access.method,
            direct_access.url,
            headers=direct_access.headers or {},
            content=payload,
        )
        direct_put_response.raise_for_status()
        direct_checksum = _checksum(
            direct_begin.direct_put.checksum_algorithm,
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
        direct_complete = UploadSessionResponse(**direct_complete_data)
        assert direct_complete.status == "completed"
        direct_content_ref = direct_complete_data["content_ref"]
        assert isinstance(direct_content_ref, dict)
        direct_content_token = direct_complete_data["content_token"]
        assert isinstance(direct_content_token, dict)
        direct_commit = _proxy_apply_commit(
            client,
            request,
            request.commit_ids.direct,
            {
                "kind": "put_file",
                "path": request.direct_path,
                "content_ref": direct_content_ref,
            },
            direct_content_token,
        )
        assert direct_commit.committed_seq == expected.direct_committed_seq

        listing_response = client.get(
            f"{namespace_alias_base}/filesystem/list",
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
            f"/v0/namespace-aliases/{request.unknown_namespace_alias}/filesystem/list",
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
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    committed = _apply(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.path, parents=False),
    )
    assert committed.committed_seq == expected.committed_seq

    feed = harness.client.filesystem.list_changes(
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
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    payload = request.content_utf8.encode()
    begin = harness.client.uploads.begin_upload(
        request.namespace_id,
        request=BeginUploadRequest_DirectPut(size_bytes=len(payload)),
    )

    assert begin.mode == expected.mode
    assert begin.direct_put.checksum_algorithm == expected.checksum_algorithm

    _put_presigned(begin.direct_put.access, payload)
    claim = UploadContentClaim(
        size_bytes=len(payload),
        checksum=_checksum(begin.direct_put.checksum_algorithm, payload),
    )
    completed = harness.client.uploads.complete_upload(
        request.namespace_id,
        begin.upload_id,
        request=CompleteUploadRequest_DirectPut(content=claim),
    )
    content_ref, content_token, _ = _completed_content(completed)
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
    stat = harness.client.filesystem.stat_path(request.namespace_id, path=request.path)
    assert _content_ref(stat.content_ref) == content_ref
    assert _read_proxied(harness.client, request.namespace_id, request.path) == payload


def test_upload_multipart(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["upload_multipart"], MultipartRequest, MultipartExpected
    )
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    payload = _byte_pattern(request.content_pattern)
    begin = harness.client.uploads.begin_upload(
        request.namespace_id,
        request=BeginUploadRequest_DirectMultipart(
            multipart=DirectMultipartUploadOptions(
                part_size_bytes=request.part_size_bytes,
            )
        ),
    )

    assert begin.mode == expected.mode
    assert begin.direct_multipart.part_size_bytes == request.part_size_bytes
    assert begin.direct_multipart.checksum_algorithm == expected.checksum_algorithm

    part_size = begin.direct_multipart.part_size_bytes
    chunks = [
        payload[offset : offset + part_size]
        for offset in range(0, len(payload), part_size)
    ]
    assert len(chunks) == expected.part_count
    claims = [
        UploadPartChecksumClaim(
            part_number=index,
            checksum=_checksum(begin.direct_multipart.checksum_algorithm, chunk),
        )
        for index, chunk in enumerate(chunks, start=1)
    ]
    signed = harness.client.uploads.sign_upload_parts(
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
    whole_checksum = _checksum(begin.direct_multipart.checksum_algorithm, payload)
    completion_request = CompleteUploadRequest_DirectMultipart(
        content=UploadContentClaim(
            size_bytes=len(payload),
            checksum=whole_checksum,
        ),
        parts=completed_parts,
    )
    first = harness.client.uploads.complete_upload(
        request.namespace_id,
        begin.upload_id,
        request=completion_request,
    )
    first_content_ref, _, first_completed_at_ms = _completed_content(first)
    replayed = harness.client.uploads.complete_upload(
        request.namespace_id,
        begin.upload_id,
        request=completion_request,
    )
    replayed_content_ref, replayed_token, replayed_completed_at_ms = _completed_content(
        replayed
    )

    assert replayed.namespace_id == first.namespace_id
    assert replayed.upload_id == first.upload_id
    assert replayed.mode == first.mode
    assert replayed_content_ref == first_content_ref
    assert replayed_completed_at_ms == first_completed_at_ms
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
    # part size, so this exercises put_file's multipart branch.
    helper_path = request.path + "-helper"
    helper_commit = put_file(
        harness.client,
        namespace_id=request.namespace_id,
        path=helper_path,
        content=payload,
        actor=request.actor,
        commit_id=request.commit_id + "-helper",
    )
    assert helper_commit.committed_seq > 0
    helper_read = get_file(
        harness.client,
        namespace_id=request.namespace_id,
        path=helper_path,
    )
    assert helper_read.content == payload
    # Content ids are random per upload and the helper may choose a different
    # checksum algorithm; the comparable content fact is the size.
    assert helper_read.content_ref.size_bytes == first_content_ref.size_bytes


def test_upload_abort(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(cases["upload_abort"], AbortRequest, AbortExpected)
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    begin = harness.client.uploads.begin_upload(
        request.namespace_id,
        request=BeginUploadRequest_ServiceProxied(),
    )
    first = harness.client.uploads.abort_upload(request.namespace_id, begin.upload_id)
    replayed = harness.client.uploads.abort_upload(
        request.namespace_id, begin.upload_id
    )

    assert first.mode == expected.mode
    assert first.status == expected.status
    assert replayed == first
    assert replayed.aborted_at_ms == first.aborted_at_ms


def test_download(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode(
        cases["download"], DownloadRequest, DownloadExpected
    )
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    payload = request.content_utf8.encode()
    committed = put_file(
        harness.client,
        namespace_id=request.namespace_id,
        path=request.path,
        content=payload,
        actor=request.actor,
        commit_id=request.commit_id,
    )
    assert committed.committed_seq == expected.committed_seq

    stat = harness.client.filesystem.stat_path(request.namespace_id, path=request.path)
    downloaded = get_file(
        harness.client,
        namespace_id=request.namespace_id,
        path=request.path,
    )
    assert _content_ref(stat.content_ref) == downloaded.content_ref
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
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    mkdir = _apply(
        harness.client,
        request.namespace_id,
        request.commit_ids.mkdir,
        request.actor,
        FilesystemOperation_CreateDirectory(path=request.directory, parents=False),
    )
    assert mkdir.committed_seq == expected.mkdir_committed_seq

    payload = request.content_utf8.encode()
    upload = put_file(
        harness.client,
        namespace_id=request.namespace_id,
        path=request.upload_path,
        content=payload,
        actor=request.actor,
        commit_id=request.commit_ids.upload,
    )
    assert upload.committed_seq == expected.upload_committed_seq
    stat = harness.client.filesystem.stat_path(
        request.namespace_id, path=request.upload_path
    )
    assert stat.size_bytes == expected.size_bytes
    uploaded_inode = stat.inode_id

    initial_listing = harness.client.filesystem.list_path_entries(
        request.namespace_id,
        path=request.directory,
    )
    assert any(entry.path == request.upload_path for entry in initial_listing.entries)

    downloaded = get_file(
        harness.client,
        namespace_id=request.namespace_id,
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
    moved_listing = harness.client.filesystem.list_path_entries(
        request.namespace_id,
        path=request.directory,
    )
    assert any(entry.path == request.moved_path for entry in moved_listing.entries)

    revisions = harness.client.filesystem.list_file_revisions(
        request.namespace_id,
        path=request.moved_path,
    )
    assert len(revisions.revisions) == expected.revision_count
    assert revisions.revisions[0].commit_id == request.commit_ids.upload

    changes_before_remove = harness.client.filesystem.list_changes(
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

    changes = harness.client.filesystem.list_changes(
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

    trash = harness.client.filesystem.list_trash(request.namespace_id)
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
