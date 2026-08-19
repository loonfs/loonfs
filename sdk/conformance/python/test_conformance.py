from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from loonfs_sdk import ActorRef, FilesystemOperation_CreateDirectory, LoonFS, UnauthorizedError


FIXTURE_VERSION = 1
RUNNER_SKIP = "run scripts/run-sdk-conformance.sh python"
TRANSFER_SKIP = "file transfer cases are not implemented in the Python harness yet"
CASE_FIELDS = {"version", "name", "intent", "family", "request", "expected"}
EXPECTED_CASES = [
    ("changes", "changes"),
    ("commit_replay", "commit_replay"),
    ("download", "download"),
    ("end_to_end", "end_to_end"),
    ("error_contract", "error_contract"),
    ("pagination", "pagination"),
    ("upload_abort", "upload_abort"),
    ("upload_direct_put", "upload_direct_put"),
    ("upload_multipart", "upload_multipart"),
]
TRANSFER_CASES = [
    "download",
    "end_to_end",
    "upload_abort",
    "upload_direct_put",
    "upload_multipart",
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


def _strict_object(value: object, fields: set[str], label: str) -> JsonObject:
    if not isinstance(value, dict):
        raise AssertionError(f"{label} must be a JSON object")
    if not all(isinstance(key, str) for key in value):
        raise AssertionError(f"{label} must use string keys")
    actual = set(value)
    if actual != fields:
        unknown = sorted(actual - fields)
        missing = sorted(fields - actual)
        raise AssertionError(f"{label} fields differ: unknown={unknown}, missing={missing}")
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
            namespace_id=_string(request["namespace_id"], "error_contract request.namespace_id"),
            malformed_body=_json_object(request["malformed_body"], "error_contract request.malformed_body"),
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
            namespace_id=_string(request["namespace_id"], "commit_replay request.namespace_id"),
            commit_id=_string(request["commit_id"], "commit_replay request.commit_id"),
            actor=_actor(request["actor"], "commit_replay request.actor"),
            message=_string(request["message"], "commit_replay request.message"),
            path=_string(request["path"], "commit_replay request.path"),
        ),
        CommitReplayExpected(
            committed_seq=_integer(expected["committed_seq"], "commit_replay expected.committed_seq")
        ),
    )


def _decode_pagination(
    test_case: ConformanceCase,
) -> tuple[PaginationRequest, PaginationExpected]:
    request = _strict_object(
        test_case.request,
        {"namespace_id", "directory", "actor", "entry_names", "page_size", "resume_after_page"},
        f"{test_case.name} request",
    )
    expected = _strict_object(
        test_case.expected,
        {"entry_count", "minimum_page_count", "head_seq"},
        f"{test_case.name} expected",
    )
    return (
        PaginationRequest(
            namespace_id=_string(request["namespace_id"], "pagination request.namespace_id"),
            directory=_string(request["directory"], "pagination request.directory"),
            actor=_actor(request["actor"], "pagination request.actor"),
            entry_names=_string_list(request["entry_names"], "pagination request.entry_names"),
            page_size=_integer(request["page_size"], "pagination request.page_size"),
            resume_after_page=_integer(
                request["resume_after_page"], "pagination request.resume_after_page"
            ),
        ),
        PaginationExpected(
            entry_count=_integer(expected["entry_count"], "pagination expected.entry_count"),
            minimum_page_count=_integer(
                expected["minimum_page_count"], "pagination expected.minimum_page_count"
            ),
            head_seq=_integer(expected["head_seq"], "pagination expected.head_seq"),
        ),
    )


def _decode_changes(test_case: ConformanceCase) -> tuple[ChangesRequest, ChangesExpected]:
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
            namespace_id=_string(request["namespace_id"], "changes request.namespace_id"),
            path=_string(request["path"], "changes request.path"),
            commit_id=_string(request["commit_id"], "changes request.commit_id"),
            actor=_actor(request["actor"], "changes request.actor"),
            after_seq=_integer(request["after_seq"], "changes request.after_seq"),
        ),
        ChangesExpected(
            committed_seq=_integer(expected["committed_seq"], "changes expected.committed_seq"),
            change_count=_integer(expected["change_count"], "changes expected.change_count"),
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
            raise AssertionError(f"invalid fixture {path}: name is {name!r}, expected {path.stem!r}")
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
        raise AssertionError(f"fixture version 1 inventory is {inventory!r}, expected {EXPECTED_CASES!r}")
    return {test_case.name: test_case for test_case in cases}


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise AssertionError(f"{name} is not set")
    return value


def _remove_authorization(request: httpx.Request) -> None:
    request.headers.pop("Authorization", None)


@pytest.fixture(scope="session")
def cases() -> dict[str, ConformanceCase]:
    return load_cases(_required_environment("LOONFS_CONFORMANCE_CASES"))


@pytest.fixture(scope="session")
def harness() -> Iterator[Harness]:
    base_url = _required_environment("LOONFS_CONFORMANCE_URL")
    token = _required_environment("LOONFS_CONFORMANCE_TOKEN")
    unauthenticated_http = httpx.Client(event_hooks={"request": [_remove_authorization]})
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

    assert len(set(observed)) == len(observed), "pagination returned an entry more than once"
    assert observed == request.entry_names
    assert resume_offset <= len(request.entry_names)
    assert resumed == request.entry_names[resume_offset:]


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


@pytest.mark.parametrize("case_name", TRANSFER_CASES)
def test_transfer_family_is_skipped(
    case_name: str,
    cases: dict[str, ConformanceCase],
) -> None:
    assert cases[case_name].family == case_name
    pytest.skip(TRANSFER_SKIP)
