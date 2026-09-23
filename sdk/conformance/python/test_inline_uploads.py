"""The same inline boundaries and retry contract for sync and async clients."""

import asyncio
import base64
import io
import json
from pathlib import Path

import httpx
import pytest
from loonfs.server import AsyncLoonFS, InlinePreparedContent, LoonFS, PreparedContent
from loonfs.core.api_error import ApiError

CASES = json.loads(
    (Path(__file__).parents[1] / "fixtures/inline_uploads.json").read_text()
)


class Source(io.BytesIO):
    def __init__(self, content, fault=False):
        super().__init__(content)
        self.fault = fault

    def read(self, size=-1):
        assert 0 < size <= 65536
        chunk = super().read(size)
        if not chunk and self.fault:
            raise OSError("source failed at EOF")
        return chunk


class Endpoint:
    def __init__(self, fixture, content, source, iterator=False):
        self.fixture, self.content, self.source = fixture, content, source
        self.iterator = iterator
        self.paths, self.commits = [], []
        self.staged = b""
        self.claim = dict(
            kind="blob_v1",
            owner_namespace_id="demo",
            owner_generation=1,
            content_id="con_test",
            size_bytes=len(content),
            checksum=dict(algorithm="sha256", value="0" * 64),
        )
        self.session = dict(
            namespace_id="demo", upload_id="upl_test", mode="service_proxied"
        )

    def handle(self, request):
        path = request.url.path
        self.paths.append(path)
        if path.endswith("/capabilities"):
            assert self.paths.count(path) == 1, (
                "publication must not reselect transport"
            )
            limit = self.fixture.get("limit", 65536)
            return httpx.Response(
                200,
                json=dict(
                    protocol_version="v0",
                    api_groups=["filesystem/v0"],
                    features={
                        "filesystem.commits.inline_content": self.fixture.get(
                            "feature", True
                        )
                    },
                    limits={}
                    if limit is None
                    else {"commit.max_inline_content_bytes_per_operation": limit},
                ),
            )
        if path.endswith("/uploads"):
            assert not self.fixture.get("inline") and not self.fixture.get("error")
            if (
                self.fixture.get("feature", True)
                and self.fixture.get("limit", 65536) is not None
            ):
                bound = min(self.fixture.get("limit", 65536), 65536) + 1
                if self.iterator:
                    assert bound <= self.source.tell() < bound + 65536
                else:
                    assert self.source.tell() == bound
            assert json.loads(request.content)["mode"] == "service_proxied"
            return httpx.Response(
                200,
                json={**self.session, "status": "open", "expires_at_ms": 2000000000000},
            )
        if path.endswith("/content"):
            self.staged = request.read()
            assert self.staged == self.content, (
                "fallback must preserve every prefix byte"
            )
            return httpx.Response(
                200,
                json={
                    **self.session,
                    "status": "open",
                    "expires_at_ms": 2000000000000,
                    "content_ref": self.claim,
                },
            )
        if path.endswith("/complete"):
            return httpx.Response(
                200,
                json={
                    **self.session,
                    "status": "completed",
                    "completed_at_ms": 0,
                    "content_ref": self.claim,
                    "content_token": {"content_ref": self.claim, "token": "retained"},
                },
            )
        if path.endswith("/commits"):
            body = json.loads(request.content)
            self.commits.append(body)
            assert request.headers["loonfs-actor"] == "writer"
            op = body["operations"][0]
            assert op["path"] == "/file" and op["behavior"] == "replace"
            assert (
                op["expected_inode_id"] == "ino_1" and op["expected_revision_no"] == 1
            )
            assert body["commit_id"] == "stable" and body["message"] == "original"
            if self.fixture["inline"]:
                assert (
                    base64.b64decode(op["inline_content"], validate=True)
                    == self.content
                )
                assert "content_ref" not in op and not body.get("content_tokens")
            else:
                assert "inline_content" not in op and op["content_ref"] == self.claim
                assert len(body["content_tokens"]) == 1
            if len(self.commits) == 1:
                return httpx.Response(
                    503, json={"code": "deadline_exceeded", "message": "reply lost"}
                )
            assert self.commits[0] == body
            return httpx.Response(
                200,
                json=dict(
                    namespace_id="demo",
                    commit_id="stable",
                    committed_seq=1,
                    committed_at_ms=0,
                    committed_by="writer",
                    events=[],
                ),
            )
        raise AssertionError(path)


def publication(prepared):
    return dict(
        path="/file",
        prepared=prepared,
        commit_id="stable",
        message="original",
        behavior="replace",
        expected_inode_id="ino_1",
        expected_revision_no=1,
        request_options={
            "max_retries": 0,
            "additional_headers": {"Loonfs-Actor": "writer"},
        },
    )


@pytest.mark.parametrize("fixture", CASES, ids=lambda f: f["name"])
@pytest.mark.parametrize("mode", ["sync", "async", "async_iterator"])
def test_inline_preparation_and_retry(fixture, mode):
    content = bytes(i % 256 for i in range(fixture["bytes"]))
    source = Source(content, fixture.get("fault", False))
    endpoint = Endpoint(fixture, content, source, iterator=mode == "async_iterator")

    async def chunks():
        while chunk := source.read(65536):
            yield chunk

    upload_content = chunks() if mode == "async_iterator" else source

    async def run_async():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(endpoint.handle)
        ) as http:
            client = AsyncLoonFS(
                base_url="http://api.test", token="secret", httpx_client=http
            )
            if fixture.get("error"):
                with pytest.raises((OSError, ValueError)):
                    await client.files.prepare_stream(
                        "demo", content=upload_content, size_bytes=fixture["size"]
                    )
                return
            prepared = await client.files.prepare_stream(
                "demo", content=upload_content, size_bytes=fixture["size"]
            )
            assert isinstance(
                prepared,
                InlinePreparedContent if fixture["inline"] else PreparedContent,
            )
            source.close()  # publication must never read the source again
            with pytest.raises(ApiError):
                await client.files.upload_prepared("demo", **publication(prepared))
            assert (
                await client.files.upload_prepared("demo", **publication(prepared))
            ).committed_seq == 1

    if mode != "sync":
        asyncio.run(run_async())
    else:
        with httpx.Client(transport=httpx.MockTransport(endpoint.handle)) as http:
            client = LoonFS(
                base_url="http://api.test", token="secret", httpx_client=http
            )
            if fixture.get("error"):
                with pytest.raises((OSError, ValueError)):
                    client.files.prepare_stream(
                        "demo", content=upload_content, size_bytes=fixture["size"]
                    )
            else:
                prepared = client.files.prepare_stream(
                    "demo", content=upload_content, size_bytes=fixture["size"]
                )
                assert isinstance(
                    prepared,
                    InlinePreparedContent if fixture["inline"] else PreparedContent,
                )
                source.close()
                with pytest.raises(ApiError):
                    client.files.upload_prepared("demo", **publication(prepared))
                assert (
                    client.files.upload_prepared(
                        "demo", **publication(prepared)
                    ).committed_seq
                    == 1
                )
    if fixture.get("error"):
        assert endpoint.paths == ["/v0/capabilities"], (
            "source failure must not start a mutation"
        )
        assert not source.closed, "caller owns the source"
    else:
        assert len(endpoint.commits) == 2


def test_inline_prepared_content_copies_mutable_input():
    content = bytearray(b"original")
    prepared = InlinePreparedContent(content)
    content[:] = b"changed!"
    assert prepared.content == b"original"


def test_async_inline_peek_cancellation_starts_no_upload():
    async def run():
        started = asyncio.Event()
        calls = []

        async def source():
            yield b"first"
            started.set()
            await asyncio.Event().wait()

        def handle(request):
            calls.append(request.url.path)
            assert request.url.path == "/v0/capabilities"
            return httpx.Response(
                200,
                json=dict(
                    protocol_version="v0",
                    api_groups=[],
                    features={"filesystem.commits.inline_content": True},
                    limits={"commit.max_inline_content_bytes_per_operation": 65536},
                ),
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            client = AsyncLoonFS(
                base_url="http://api.test", token="secret", httpx_client=http
            )
            task = asyncio.create_task(
                client.files.prepare_stream("demo", content=source())
            )
            await asyncio.wait_for(started.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert calls == ["/v0/capabilities"]

    asyncio.run(run())
