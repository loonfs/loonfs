from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
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
from loonfs_sdk.proxy import ROUTES as PROXY_ROUTES, LoonFSProxy
from loonfs_sdk.transfers import get_file, put_file


FIXTURE_VERSION = 1
RUNNER_SKIP = "run scripts/run-sdk-conformance.sh python"
CASE_FIELDS = {"version", "name", "intent", "family", "request", "expected"}
EXPECTED_CASES = [
    ("changes", "changes"),
    ("commit_replay", "commit_replay"),
    ("download", "download"),
    ("end_to_end", "end_to_end"),
    ("error_contract", "error_contract"),
    ("pagination", "pagination"),
    ("proxy", "proxy"),
    ("upload_abort", "upload_abort"),
    ("upload_direct_put", "upload_direct_put"),
    ("upload_multipart", "upload_multipart"),
]
pytestmark = pytest.mark.skipif(
    not os.environ.get("LOONFS_CONFORMANCE_URL"),
    reason=RUNNER_SKIP,
)


JsonObject = dict[str, object]


@dataclass(frozen=True)
class ConformanceCase:
    name: str
    family: str
    request: JsonObject
    expected: JsonObject


@dataclass(frozen=True)
class ErrorStatusExpected:
    status: int
    code: str


@dataclass(frozen=True)
class ErrorOutcome:
    status: int
    code: str
    param: str


@dataclass(frozen=True)
class ErrorContractRequest:
    namespace_id: str
    malformed_body: JsonObject
    invalid_after_seq: str


@dataclass(frozen=True)
class ErrorContractExpected:
    unauthenticated: ErrorStatusExpected
    malformed_body: ErrorOutcome
    invalid_query: ErrorOutcome


@dataclass(frozen=True)
class CommitReplayRequest:
    namespace_id: str
    commit_id: str
    actor: ActorRef
    message: str
    path: str


@dataclass(frozen=True)
class CommitReplayExpected:
    committed_seq: int


@dataclass(frozen=True)
class DirectPutRequest:
    namespace_id: str
    path: str
    commit_id: str
    actor: ActorRef
    content_utf8: str


@dataclass(frozen=True)
class DirectPutExpected:
    mode: str
    size_bytes: int
    checksum_algorithm: str
    committed_seq: int


@dataclass(frozen=True)
class BytePattern:
    length: int
    modulus: int


@dataclass(frozen=True)
class MultipartRequest:
    namespace_id: str
    path: str
    commit_id: str
    actor: ActorRef
    part_size_bytes: int
    content_pattern: BytePattern


@dataclass(frozen=True)
class MultipartExpected:
    mode: str
    part_count: int
    size_bytes: int
    checksum_algorithm: str
    committed_seq: int


@dataclass(frozen=True)
class AbortRequest:
    namespace_id: str


@dataclass(frozen=True)
class AbortExpected:
    mode: str
    status: str


@dataclass(frozen=True)
class DownloadRequest:
    namespace_id: str
    path: str
    commit_id: str
    actor: ActorRef
    content_utf8: str


@dataclass(frozen=True)
class DownloadExpected:
    size_bytes: int
    checksum_algorithm: str
    committed_seq: int


@dataclass(frozen=True)
class EndToEndCommitIds:
    mkdir: str
    upload: str
    move: str
    remove: str


@dataclass(frozen=True)
class EndToEndRequest:
    namespace_id: str
    directory: str
    upload_path: str
    moved_path: str
    actor: ActorRef
    content_utf8: str
    commit_ids: EndToEndCommitIds


@dataclass(frozen=True)
class EndToEndExpected:
    mkdir_committed_seq: int
    upload_committed_seq: int
    move_committed_seq: int
    remove_committed_seq: int
    size_bytes: int
    revision_count: int
    change_count: int


@dataclass(frozen=True)
class PaginationRequest:
    namespace_id: str
    directory: str
    actor: ActorRef
    entry_names: list[str]
    page_size: int
    resume_after_page: int


@dataclass(frozen=True)
class PaginationExpected:
    entry_count: int
    minimum_page_count: int
    head_seq: int


@dataclass(frozen=True)
class ProxyCommitIds:
    directory: str
    proxied: str
    direct: str


@dataclass(frozen=True)
class ProxyRequest:
    mount: str
    namespace_id: str
    unknown_mount: str
    actor: ActorRef
    directory: str
    proxied_path: str
    direct_path: str
    commit_ids: ProxyCommitIds
    content_utf8: str
    disallowed_path_suffix: str


@dataclass(frozen=True)
class ProxyExpected:
    mkdir_committed_seq: int
    proxied_committed_seq: int
    direct_committed_seq: int
    entry_count: int
    unknown_mount_status: int
    disallowed_route_status: int


@dataclass(frozen=True)
class ChangesRequest:
    namespace_id: str
    path: str
    commit_id: str
    actor: ActorRef
    after_seq: int


@dataclass(frozen=True)
class ChangesExpected:
    committed_seq: int
    change_count: int
    event_kind: str


@dataclass(frozen=True)
class Harness:
    client: LoonFS
    unauthenticated: LoonFS


@dataclass(frozen=True)
class ProxyHarness:
    base_url: str


def _strict_object(value: object, fields: set[str], label: str) -> JsonObject:
    if not isinstance(value, dict):
        raise AssertionError(f"{label} must be a JSON object")
    if not all(isinstance(key, str) for key in value):
        raise AssertionError(f"{label} must use string keys")
    actual = set(value)
    if actual != fields:
        unknown = sorted(actual - fields)
        missing = sorted(fields - actual)
        raise AssertionError(
            f"{label} fields differ: unknown={unknown}, missing={missing}"
        )
    return value


def _json_object(value: object, label: str) -> JsonObject:
    if not isinstance(value, dict):
        raise AssertionError(f"{label} must be a JSON object")
    if not all(isinstance(key, str) for key in value):
        raise AssertionError(f"{label} must use string keys")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise AssertionError(f"{label} must be a string")
    return value


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise AssertionError(f"{label} must be an integer")
    return value


def _string_list(value: object, label: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise AssertionError(f"{label} must be an array of strings")
    return value


def _actor(value: object, label: str) -> ActorRef:
    actor = _strict_object(value, {"id", "kind"}, label)
    actor_id = _string(actor["id"], f"{label}.id")
    kind = _string(actor["kind"], f"{label}.kind")
    if kind not in {"user", "service", "system"}:
        raise AssertionError(f"{label}.kind is not a known actor kind")
    return ActorRef(id=actor_id, kind=kind)


def _error_status(value: object, label: str) -> ErrorStatusExpected:
    data = _strict_object(value, {"status", "code"}, label)
    return ErrorStatusExpected(
        status=_integer(data["status"], f"{label}.status"),
        code=_string(data["code"], f"{label}.code"),
    )


def _error_outcome(value: object, label: str) -> ErrorOutcome:
    data = _strict_object(value, {"status", "code", "param"}, label)
    return ErrorOutcome(
        status=_integer(data["status"], f"{label}.status"),
        code=_string(data["code"], f"{label}.code"),
        param=_string(data["param"], f"{label}.param"),
    )


def _decode_error_contract(
    test_case: ConformanceCase,
) -> tuple[ErrorContractRequest, ErrorContractExpected]:
    request = _strict_object(
        test_case.request,
        {"namespace_id", "malformed_body", "invalid_after_seq"},
        f"{test_case.name} request",
    )
    expected = _strict_object(
        test_case.expected,
        {"unauthenticated", "malformed_body", "invalid_query"},
        f"{test_case.name} expected",
    )
    return (
        ErrorContractRequest(
            namespace_id=_string(
                request["namespace_id"], "error_contract request.namespace_id"
            ),
            malformed_body=_json_object(
                request["malformed_body"], "error_contract request.malformed_body"
            ),
            invalid_after_seq=_string(
                request["invalid_after_seq"], "error_contract request.invalid_after_seq"
            ),
        ),
        ErrorContractExpected(
            unauthenticated=_error_status(
                expected["unauthenticated"], "error_contract expected.unauthenticated"
            ),
            malformed_body=_error_outcome(
                expected["malformed_body"], "error_contract expected.malformed_body"
            ),
            invalid_query=_error_outcome(
                expected["invalid_query"], "error_contract expected.invalid_query"
            ),
        ),
    )


def _decode_commit_replay(
    test_case: ConformanceCase,
) -> tuple[CommitReplayRequest, CommitReplayExpected]:
    request = _strict_object(
        test_case.request,
        {"namespace_id", "commit_id", "actor", "message", "path"},
        f"{test_case.name} request",
    )
    expected = _strict_object(
        test_case.expected,
        {"committed_seq"},
        f"{test_case.name} expected",
    )
    return (
        CommitReplayRequest(
            namespace_id=_string(
                request["namespace_id"], "commit_replay request.namespace_id"
            ),
            commit_id=_string(request["commit_id"], "commit_replay request.commit_id"),
            actor=_actor(request["actor"], "commit_replay request.actor"),
            message=_string(request["message"], "commit_replay request.message"),
            path=_string(request["path"], "commit_replay request.path"),
        ),
        CommitReplayExpected(
            committed_seq=_integer(
                expected["committed_seq"], "commit_replay expected.committed_seq"
            )
        ),
    )


def _decode_direct_put(
    test_case: ConformanceCase,
) -> tuple[DirectPutRequest, DirectPutExpected]:
    request = _strict_object(
        test_case.request,
        {"namespace_id", "path", "commit_id", "actor", "content_utf8"},
        f"{test_case.name} request",
    )
    expected = _strict_object(
        test_case.expected,
        {"mode", "size_bytes", "checksum_algorithm", "committed_seq"},
        f"{test_case.name} expected",
    )
    return (
        DirectPutRequest(
            namespace_id=_string(
                request["namespace_id"], "upload_direct_put request.namespace_id"
            ),
            path=_string(request["path"], "upload_direct_put request.path"),
            commit_id=_string(
                request["commit_id"], "upload_direct_put request.commit_id"
            ),
            actor=_actor(request["actor"], "upload_direct_put request.actor"),
            content_utf8=_string(
                request["content_utf8"], "upload_direct_put request.content_utf8"
            ),
        ),
        DirectPutExpected(
            mode=_string(expected["mode"], "upload_direct_put expected.mode"),
            size_bytes=_integer(
                expected["size_bytes"], "upload_direct_put expected.size_bytes"
            ),
            checksum_algorithm=_string(
                expected["checksum_algorithm"],
                "upload_direct_put expected.checksum_algorithm",
            ),
            committed_seq=_integer(
                expected["committed_seq"], "upload_direct_put expected.committed_seq"
            ),
        ),
    )


def _decode_multipart(
    test_case: ConformanceCase,
) -> tuple[MultipartRequest, MultipartExpected]:
    request = _strict_object(
        test_case.request,
        {
            "namespace_id",
            "path",
            "commit_id",
            "actor",
            "part_size_bytes",
            "content_pattern",
        },
        f"{test_case.name} request",
    )
    pattern = _strict_object(
        request["content_pattern"],
        {"length", "modulus"},
        "upload_multipart request.content_pattern",
    )
    expected = _strict_object(
        test_case.expected,
        {"mode", "part_count", "size_bytes", "checksum_algorithm", "committed_seq"},
        f"{test_case.name} expected",
    )
    return (
        MultipartRequest(
            namespace_id=_string(
                request["namespace_id"], "upload_multipart request.namespace_id"
            ),
            path=_string(request["path"], "upload_multipart request.path"),
            commit_id=_string(
                request["commit_id"], "upload_multipart request.commit_id"
            ),
            actor=_actor(request["actor"], "upload_multipart request.actor"),
            part_size_bytes=_integer(
                request["part_size_bytes"], "upload_multipart request.part_size_bytes"
            ),
            content_pattern=BytePattern(
                length=_integer(
                    pattern["length"], "upload_multipart request.content_pattern.length"
                ),
                modulus=_integer(
                    pattern["modulus"],
                    "upload_multipart request.content_pattern.modulus",
                ),
            ),
        ),
        MultipartExpected(
            mode=_string(expected["mode"], "upload_multipart expected.mode"),
            part_count=_integer(
                expected["part_count"], "upload_multipart expected.part_count"
            ),
            size_bytes=_integer(
                expected["size_bytes"], "upload_multipart expected.size_bytes"
            ),
            checksum_algorithm=_string(
                expected["checksum_algorithm"],
                "upload_multipart expected.checksum_algorithm",
            ),
            committed_seq=_integer(
                expected["committed_seq"], "upload_multipart expected.committed_seq"
            ),
        ),
    )


def _decode_abort(test_case: ConformanceCase) -> tuple[AbortRequest, AbortExpected]:
    request = _strict_object(
        test_case.request,
        {"namespace_id"},
        f"{test_case.name} request",
    )
    expected = _strict_object(
        test_case.expected,
        {"mode", "status"},
        f"{test_case.name} expected",
    )
    return (
        AbortRequest(
            namespace_id=_string(
                request["namespace_id"], "upload_abort request.namespace_id"
            )
        ),
        AbortExpected(
            mode=_string(expected["mode"], "upload_abort expected.mode"),
            status=_string(expected["status"], "upload_abort expected.status"),
        ),
    )


def _decode_download(
    test_case: ConformanceCase,
) -> tuple[DownloadRequest, DownloadExpected]:
    request = _strict_object(
        test_case.request,
        {"namespace_id", "path", "commit_id", "actor", "content_utf8"},
        f"{test_case.name} request",
    )
    expected = _strict_object(
        test_case.expected,
        {"size_bytes", "checksum_algorithm", "committed_seq"},
        f"{test_case.name} expected",
    )
    return (
        DownloadRequest(
            namespace_id=_string(
                request["namespace_id"], "download request.namespace_id"
            ),
            path=_string(request["path"], "download request.path"),
            commit_id=_string(request["commit_id"], "download request.commit_id"),
            actor=_actor(request["actor"], "download request.actor"),
            content_utf8=_string(
                request["content_utf8"], "download request.content_utf8"
            ),
        ),
        DownloadExpected(
            size_bytes=_integer(expected["size_bytes"], "download expected.size_bytes"),
            checksum_algorithm=_string(
                expected["checksum_algorithm"], "download expected.checksum_algorithm"
            ),
            committed_seq=_integer(
                expected["committed_seq"], "download expected.committed_seq"
            ),
        ),
    )


def _decode_end_to_end(
    test_case: ConformanceCase,
) -> tuple[EndToEndRequest, EndToEndExpected]:
    request = _strict_object(
        test_case.request,
        {
            "namespace_id",
            "directory",
            "upload_path",
            "moved_path",
            "actor",
            "content_utf8",
            "commit_ids",
        },
        f"{test_case.name} request",
    )
    commit_ids = _strict_object(
        request["commit_ids"],
        {"mkdir", "upload", "move", "remove"},
        "end_to_end request.commit_ids",
    )
    expected = _strict_object(
        test_case.expected,
        {
            "mkdir_committed_seq",
            "upload_committed_seq",
            "move_committed_seq",
            "remove_committed_seq",
            "size_bytes",
            "revision_count",
            "change_count",
        },
        f"{test_case.name} expected",
    )
    return (
        EndToEndRequest(
            namespace_id=_string(
                request["namespace_id"], "end_to_end request.namespace_id"
            ),
            directory=_string(request["directory"], "end_to_end request.directory"),
            upload_path=_string(
                request["upload_path"], "end_to_end request.upload_path"
            ),
            moved_path=_string(request["moved_path"], "end_to_end request.moved_path"),
            actor=_actor(request["actor"], "end_to_end request.actor"),
            content_utf8=_string(
                request["content_utf8"], "end_to_end request.content_utf8"
            ),
            commit_ids=EndToEndCommitIds(
                mkdir=_string(
                    commit_ids["mkdir"], "end_to_end request.commit_ids.mkdir"
                ),
                upload=_string(
                    commit_ids["upload"], "end_to_end request.commit_ids.upload"
                ),
                move=_string(commit_ids["move"], "end_to_end request.commit_ids.move"),
                remove=_string(
                    commit_ids["remove"], "end_to_end request.commit_ids.remove"
                ),
            ),
        ),
        EndToEndExpected(
            mkdir_committed_seq=_integer(
                expected["mkdir_committed_seq"],
                "end_to_end expected.mkdir_committed_seq",
            ),
            upload_committed_seq=_integer(
                expected["upload_committed_seq"],
                "end_to_end expected.upload_committed_seq",
            ),
            move_committed_seq=_integer(
                expected["move_committed_seq"], "end_to_end expected.move_committed_seq"
            ),
            remove_committed_seq=_integer(
                expected["remove_committed_seq"],
                "end_to_end expected.remove_committed_seq",
            ),
            size_bytes=_integer(
                expected["size_bytes"], "end_to_end expected.size_bytes"
            ),
            revision_count=_integer(
                expected["revision_count"], "end_to_end expected.revision_count"
            ),
            change_count=_integer(
                expected["change_count"], "end_to_end expected.change_count"
            ),
        ),
    )


def _decode_pagination(
    test_case: ConformanceCase,
) -> tuple[PaginationRequest, PaginationExpected]:
    request = _strict_object(
        test_case.request,
        {
            "namespace_id",
            "directory",
            "actor",
            "entry_names",
            "page_size",
            "resume_after_page",
        },
        f"{test_case.name} request",
    )
    expected = _strict_object(
        test_case.expected,
        {"entry_count", "minimum_page_count", "head_seq"},
        f"{test_case.name} expected",
    )
    return (
        PaginationRequest(
            namespace_id=_string(
                request["namespace_id"], "pagination request.namespace_id"
            ),
            directory=_string(request["directory"], "pagination request.directory"),
            actor=_actor(request["actor"], "pagination request.actor"),
            entry_names=_string_list(
                request["entry_names"], "pagination request.entry_names"
            ),
            page_size=_integer(request["page_size"], "pagination request.page_size"),
            resume_after_page=_integer(
                request["resume_after_page"], "pagination request.resume_after_page"
            ),
        ),
        PaginationExpected(
            entry_count=_integer(
                expected["entry_count"], "pagination expected.entry_count"
            ),
            minimum_page_count=_integer(
                expected["minimum_page_count"], "pagination expected.minimum_page_count"
            ),
            head_seq=_integer(expected["head_seq"], "pagination expected.head_seq"),
        ),
    )


def _decode_proxy(
    test_case: ConformanceCase,
) -> tuple[ProxyRequest, ProxyExpected]:
    request = _strict_object(
        test_case.request,
        {
            "mount",
            "namespace_id",
            "unknown_mount",
            "actor",
            "directory",
            "proxied_path",
            "direct_path",
            "commit_ids",
            "content_utf8",
            "disallowed_path_suffix",
        },
        f"{test_case.name} request",
    )
    commit_ids = _strict_object(
        request["commit_ids"],
        {"directory", "proxied", "direct"},
        "proxy request.commit_ids",
    )
    expected = _strict_object(
        test_case.expected,
        {
            "mkdir_committed_seq",
            "proxied_committed_seq",
            "direct_committed_seq",
            "entry_count",
            "unknown_mount_status",
            "disallowed_route_status",
        },
        f"{test_case.name} expected",
    )
    return (
        ProxyRequest(
            mount=_string(request["mount"], "proxy request.mount"),
            namespace_id=_string(
                request["namespace_id"], "proxy request.namespace_id"
            ),
            unknown_mount=_string(
                request["unknown_mount"], "proxy request.unknown_mount"
            ),
            actor=_actor(request["actor"], "proxy request.actor"),
            directory=_string(request["directory"], "proxy request.directory"),
            proxied_path=_string(
                request["proxied_path"], "proxy request.proxied_path"
            ),
            direct_path=_string(
                request["direct_path"], "proxy request.direct_path"
            ),
            commit_ids=ProxyCommitIds(
                directory=_string(
                    commit_ids["directory"], "proxy request.commit_ids.directory"
                ),
                proxied=_string(
                    commit_ids["proxied"], "proxy request.commit_ids.proxied"
                ),
                direct=_string(
                    commit_ids["direct"], "proxy request.commit_ids.direct"
                ),
            ),
            content_utf8=_string(
                request["content_utf8"], "proxy request.content_utf8"
            ),
            disallowed_path_suffix=_string(
                request["disallowed_path_suffix"],
                "proxy request.disallowed_path_suffix",
            ),
        ),
        ProxyExpected(
            mkdir_committed_seq=_integer(
                expected["mkdir_committed_seq"],
                "proxy expected.mkdir_committed_seq",
            ),
            proxied_committed_seq=_integer(
                expected["proxied_committed_seq"],
                "proxy expected.proxied_committed_seq",
            ),
            direct_committed_seq=_integer(
                expected["direct_committed_seq"],
                "proxy expected.direct_committed_seq",
            ),
            entry_count=_integer(
                expected["entry_count"], "proxy expected.entry_count"
            ),
            unknown_mount_status=_integer(
                expected["unknown_mount_status"],
                "proxy expected.unknown_mount_status",
            ),
            disallowed_route_status=_integer(
                expected["disallowed_route_status"],
                "proxy expected.disallowed_route_status",
            ),
        ),
    )


def _decode_changes(
    test_case: ConformanceCase,
) -> tuple[ChangesRequest, ChangesExpected]:
    request = _strict_object(
        test_case.request,
        {"namespace_id", "path", "commit_id", "actor", "after_seq"},
        f"{test_case.name} request",
    )
    expected = _strict_object(
        test_case.expected,
        {"committed_seq", "change_count", "event_kind"},
        f"{test_case.name} expected",
    )
    return (
        ChangesRequest(
            namespace_id=_string(
                request["namespace_id"], "changes request.namespace_id"
            ),
            path=_string(request["path"], "changes request.path"),
            commit_id=_string(request["commit_id"], "changes request.commit_id"),
            actor=_actor(request["actor"], "changes request.actor"),
            after_seq=_integer(request["after_seq"], "changes request.after_seq"),
        ),
        ChangesExpected(
            committed_seq=_integer(
                expected["committed_seq"], "changes expected.committed_seq"
            ),
            change_count=_integer(
                expected["change_count"], "changes expected.change_count"
            ),
            event_kind=_string(expected["event_kind"], "changes expected.event_kind"),
        ),
    )


def load_cases(directory: str) -> dict[str, ConformanceCase]:
    cases: list[ConformanceCase] = []
    for path in sorted(Path(directory).iterdir()):
        if not path.is_file() or path.suffix != ".json":
            continue
        root = _strict_object(json.loads(path.read_text()), CASE_FIELDS, str(path))
        version = _integer(root["version"], f"{path} version")
        if version != FIXTURE_VERSION:
            raise AssertionError(
                f"invalid fixture {path}: version must be {FIXTURE_VERSION}, found {version}"
            )
        name = _string(root["name"], f"{path} name")
        if name != path.stem:
            raise AssertionError(
                f"invalid fixture {path}: name is {name!r}, expected {path.stem!r}"
            )
        intent = _string(root["intent"], f"{path} intent")
        if not intent.strip():
            raise AssertionError(f"invalid fixture {path}: intent must not be empty")
        cases.append(
            ConformanceCase(
                name=name,
                family=_string(root["family"], f"{path} family"),
                request=_json_object(root["request"], f"{path} request"),
                expected=_json_object(root["expected"], f"{path} expected"),
            )
        )

    inventory = [(test_case.name, test_case.family) for test_case in cases]
    if inventory != EXPECTED_CASES:
        raise AssertionError(
            f"fixture version 1 inventory is {inventory!r}, expected {EXPECTED_CASES!r}"
        )
    return {test_case.name: test_case for test_case in cases}


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise AssertionError(f"{name} is not set")
    return value


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
_CRC32C_MASK = (1 << 32) - 1
_CRC64_NVME_TABLE = _crc_table(0x9A6C9329AC4BC9B5, _CRC64_NVME_MASK)
_CRC32C_TABLE = _crc_table(0x82F63B78, _CRC32C_MASK)


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
) -> Iterator[ProxyHarness]:
    request, _ = _decode_proxy(cases["proxy"])
    app = LoonFSProxy(
        _required_environment("LOONFS_CONFORMANCE_URL"),
        _required_environment("LOONFS_CONFORMANCE_TOKEN"),
        {request.mount: request.namespace_id},
    )
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
        name="loonfs-python-proxy",
        daemon=True,
    )
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not server.started:
            if not thread.is_alive():
                raise AssertionError("Python proxy stopped during startup")
            if time.monotonic() >= deadline:
                raise AssertionError("Python proxy did not start within five seconds")
            time.sleep(0.01)
        yield ProxyHarness(base_url=f"http://127.0.0.1:{port}")
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        listener.close()
        if thread.is_alive():
            raise AssertionError("Python proxy did not stop within five seconds")


def _apply_create_directory(
    client: LoonFS,
    namespace_id: str,
    commit_id: str,
    actor: ActorRef,
    path: str,
    message: str | None = None,
) -> Any:
    operation = FilesystemOperation_CreateDirectory(path=path, parents=False)
    if message is None:
        return client.filesystem.apply_commit(
            namespace_id,
            actor=actor,
            commit_id=commit_id,
            operations=[operation],
        )
    return client.filesystem.apply_commit(
        namespace_id,
        actor=actor,
        commit_id=commit_id,
        operations=[operation],
        message=message,
    )


def _apply_put_file(
    client: LoonFS,
    namespace_id: str,
    commit_id: str,
    actor: ActorRef,
    path: str,
    content_ref: ContentRef,
    content_token: str | None,
) -> Any:
    return client.filesystem.apply_commit(
        namespace_id,
        actor=actor,
        commit_id=commit_id,
        operations=[FilesystemOperation_PutFile(path=path, content_ref=content_ref)],
        content_tokens=[content_token] if content_token is not None else [],
    )


def _apply_operation(
    client: LoonFS,
    namespace_id: str,
    commit_id: str,
    actor: ActorRef,
    operation: Any,
) -> Any:
    return client.filesystem.apply_commit(
        namespace_id,
        actor=actor,
        commit_id=commit_id,
        operations=[operation],
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
    elif algorithm == "crc32c":
        value = f"{_crc(content, _CRC32C_TABLE, _CRC32C_MASK):08x}"
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
    return _json_object(response.json(), label)


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
        f"/v0/mounts/{request.mount}/commits",
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
    request, expected = _decode_error_contract(cases["error_contract"])
    with pytest.raises(UnauthorizedError) as captured:
        harness.unauthenticated.namespaces.get_namespace(request.namespace_id)

    error = captured.value
    assert error.status_code == expected.unauthenticated.status
    assert error.body.code == expected.unauthenticated.code
    assert error.body.request_id is not None


def test_commit_replay(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode_commit_replay(cases["commit_replay"])
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    first = _apply_create_directory(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        request.path,
        request.message,
    )
    replayed = _apply_create_directory(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        request.path,
        request.message,
    )

    assert first.committed_seq == expected.committed_seq
    assert first.commit_id == request.commit_id
    assert replayed.committed_seq == first.committed_seq
    assert replayed.commit_id == first.commit_id
    assert replayed.namespace_id == first.namespace_id


def test_pagination(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode_pagination(cases["pagination"])
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    _apply_create_directory(
        harness.client,
        request.namespace_id,
        "conf-pagination-directory",
        request.actor,
        request.directory,
    )
    for index, name in enumerate(request.entry_names):
        _apply_create_directory(
            harness.client,
            request.namespace_id,
            f"conf-pagination-entry-{index:02d}",
            request.actor,
            f"{request.directory}/{name}",
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
    proxy_harness: ProxyHarness,
) -> None:
    request, expected = _decode_proxy(cases["proxy"])
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    payload = request.content_utf8.encode()
    mount_base = f"/v0/mounts/{request.mount}"

    with httpx.Client(
        base_url=proxy_harness.base_url,
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
            f"{mount_base}/uploads",
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
            f"{mount_base}/uploads/{proxied_begin.upload_id}/content",
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
            f"{mount_base}/uploads/{proxied_begin.upload_id}/complete",
            json={"mode": "service_proxied"},
        )
        proxied_complete_data = _proxy_response_json(
            proxied_complete_response,
            "proxy service-proxied complete response",
        )
        proxied_complete = UploadSessionResponse(**proxied_complete_data)
        assert proxied_complete.status == "completed"
        proxied_content_ref = _json_object(
            proxied_complete_data["content_ref"],
            "proxy service-proxied content_ref",
        )
        assert ContentRef(**proxied_content_ref) == uploaded.content_ref
        proxied_content_token = _json_object(
            proxied_complete_data["content_token"],
            "proxy service-proxied content_token",
        )
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
            f"{mount_base}/uploads",
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
            f"{mount_base}/uploads/{direct_begin.upload_id}/complete",
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
        direct_content_ref = _json_object(
            direct_complete_data["content_ref"],
            "proxy direct-PUT content_ref",
        )
        direct_content_token = _json_object(
            direct_complete_data["content_token"],
            "proxy direct-PUT content_token",
        )
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
            f"{mount_base}/filesystem/list",
            params={"path": request.directory},
        )
        listing = ListPathEntriesResponse(
            **_proxy_response_json(listing_response, "proxy list response")
        )
        assert len(listing.entries) == expected.entry_count

        with client.stream(
            "GET",
            f"{mount_base}/filesystem/content",
            params={"path": request.proxied_path},
        ) as read_response:
            read_response.raise_for_status()
            assert b"".join(read_response.iter_raw()) == payload

        unknown_mount = client.get(
            f"/v0/mounts/{request.unknown_mount}/filesystem/list",
            params={"path": request.directory},
        )
        assert unknown_mount.status_code == expected.unknown_mount_status
        assert unknown_mount.content == b""

        disallowed_route = client.get(
            f"{mount_base}{request.disallowed_path_suffix}"
        )
        assert disallowed_route.status_code == expected.disallowed_route_status
        assert disallowed_route.content == b""


def test_changes(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode_changes(cases["changes"])
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    committed = _apply_create_directory(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        request.path,
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
    assert expected.event_kind == "directory_created"
    assert len(change.events) == 1
    assert change.events[0].kind == "directory_created"


def test_upload_direct_put(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode_direct_put(cases["upload_direct_put"])
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    payload = request.content_utf8.encode()
    begin = harness.client.uploads.begin_upload(
        request.namespace_id,
        request=BeginUploadRequest_DirectPut(size_bytes=len(payload)),
    )

    assert expected.mode == "direct_put"
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

    committed = _apply_put_file(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        request.path,
        content_ref,
        content_token,
    )
    assert committed.committed_seq == expected.committed_seq
    stat = harness.client.filesystem.stat_path(request.namespace_id, path=request.path)
    assert _content_ref(stat.content_ref) == content_ref
    assert _read_proxied(harness.client, request.namespace_id, request.path) == payload


def test_upload_multipart(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode_multipart(cases["upload_multipart"])
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

    assert expected.mode == "direct_multipart"
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

    committed = _apply_put_file(
        harness.client,
        request.namespace_id,
        request.commit_id,
        request.actor,
        request.path,
        first_content_ref,
        replayed_token,
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
    request, expected = _decode_abort(cases["upload_abort"])
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    begin = harness.client.uploads.begin_upload(
        request.namespace_id,
        request=BeginUploadRequest_ServiceProxied(),
    )
    first = harness.client.uploads.abort_upload(request.namespace_id, begin.upload_id)
    replayed = harness.client.uploads.abort_upload(
        request.namespace_id, begin.upload_id
    )

    assert expected.mode == "service_proxied"
    assert expected.status == "aborted"
    assert first.mode == expected.mode
    assert first.status == expected.status
    assert replayed == first
    assert replayed.aborted_at_ms == first.aborted_at_ms


def test_download(cases: dict[str, ConformanceCase], harness: Harness) -> None:
    request, expected = _decode_download(cases["download"])
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
    request, expected = _decode_end_to_end(cases["end_to_end"])
    harness.client.namespaces.create_namespace(namespace_id=request.namespace_id)
    mkdir = _apply_create_directory(
        harness.client,
        request.namespace_id,
        request.commit_ids.mkdir,
        request.actor,
        request.directory,
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

    moved = _apply_operation(
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

    removed = _apply_operation(
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


def test_proxy_route_table_matches_the_document() -> None:
    """The route table cannot drift from the proxy document unnoticed."""
    document_path = os.environ.get("LOONFS_PROXY_DOCUMENT")
    if not document_path:
        pytest.skip("run scripts/run-sdk-conformance.sh python")
    with open(document_path, encoding="utf-8") as handle:
        document = json.load(handle)
    documented = {
        f"{method.upper()} {path}"
        for path, item in document["paths"].items()
        for method in item
    }
    table = {f"{method} {template}" for _operation, method, template in PROXY_ROUTES}
    assert table == documented
