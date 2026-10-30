import asyncio
import json
import re
import threading
from pathlib import Path

import httpx
import pytest
from loonfs.server import AsyncLoonFS, LoonFS
from loonfs._transfer_runtime import _RANGE_RESPONSES_IN_FLIGHT, _checksum

CASES = json.loads(
    (Path(__file__).parents[1] / "fixtures/streaming_downloads.json").read_text()
)


class Chunks(httpx.SyncByteStream, httpx.AsyncByteStream):
    def __init__(self, chunks, transport_error=False):
        self.transport_error = transport_error
        self.chunks = chunks
        self.reads = 0
        self.closed = False

    def __iter__(self):
        for chunk in self.chunks:
            self.reads += 1
            yield chunk
        if self.transport_error:
            raise httpx.ReadError("body interrupted after its last byte")

    async def __aiter__(self):
        for chunk in self:
            yield chunk

    async def aclose(self):
        self.close()

    def close(self):
        self.closed = True


def stream_client(fixture, direct, body, asynchronous=False, range_body=None):
    claim = {
        "kind": "blob_v1",
        "owner_namespace_id": "demo",
        "content_id": "con_00000000000000000000000001",
        "size_bytes": fixture["size_bytes"],
        "checksum": {"algorithm": fixture["algorithm"], "value": fixture["checksum"]},
    }
    requests = []
    signed = [fixture["range"]] if fixture.get("range") else []

    def handle(request):
        requests.append(request)
        assert request.extensions["timeout"]["read"] == 7
        path = request.url.path
        if path.startswith("/object/"):
            index = int(path.rsplit("/", 1)[1])
            part = fixture["ranges"][index]
            assert "authorization" not in request.headers
            assert "x-private" not in request.headers
            assert "cookie" not in request.headers
            assert request.headers.get_list("range") == [part["range"]]
            if range_body is not None:
                return httpx.Response(200, stream=range_body(index))
            return httpx.Response(200, content=part["content"].encode())
        if path == "/object":
            assert "authorization" not in request.headers
            assert "x-private" not in request.headers
            assert "cookie" not in request.headers
            assert fixture["size_bytes"] > 0, "a grant of zero bytes needs no request"
            assert request.headers.get_list("range") == signed
            return httpx.Response(200, stream=body)
        assert request.headers["authorization"] == "Bearer private-token"
        if path.endswith("/capabilities"):
            value = {
                "protocol_version": "v0",
                "api_groups": ["filesystem/v0"],
                "features": {"filesystem.downloads.direct_get": direct},
            }
        elif path.endswith("/downloads"):
            access = {
                "kind": "presigned_url",
                "method": "GET",
                "url": "http://objects.test/object",
                "headers": {},
                "expires_at_ms": 2000000000000,
            }
            if signed:
                access["headers"] = {"range": signed[0]}
            value = {
                "namespace_id": "demo",
                "path": "/file",
                "revision_no": 1,
                "content_ref": claim,
                "ranges": [
                    {
                        "start_offset": 0,
                        "length": fixture["size_bytes"],
                        "access": access,
                    }
                ],
            }
            if "ranges" in fixture:
                value["ranges"] = [
                    {
                        "start_offset": part["start_offset"],
                        "length": part["length"],
                        "access": {
                            **access,
                            "url": f"http://objects.test/object/{index}",
                            "headers": {"range": part["range"]},
                        },
                    }
                    for index, part in enumerate(fixture["ranges"])
                ]
        elif path.endswith("/entry"):
            actor = "test"
            value = {
                "inode_kind": "file",
                "namespace_id": "demo",
                "path": "/file",
                "inode_id": "ino_00000000000000000000000002",
                "head_seq": 1,
                "created_at_ms": 0,
                "created_by": actor,
                "revision_committed_at_ms": 0,
                "revision_committed_by": actor,
                "revision_no": 1,
                "content_ref": claim,
                "size_bytes": claim["size_bytes"],
            }
        elif path.endswith("/content"):
            assert request.url.params["revision_no"] == "1"
            assert "range" not in request.headers
            return httpx.Response(200, stream=body)
        else:
            raise AssertionError(str(request.url))
        return httpx.Response(200, json=value)

    http_type = httpx.AsyncClient if asynchronous else httpx.Client
    client_type = AsyncLoonFS if asynchronous else LoonFS
    http = http_type(
        transport=httpx.MockTransport(handle),
        headers={"X-Private": "secret"},
        cookies={"private": "secret"},
    )
    return (
        client_type(
            base_url="http://api.test", token="private-token", httpx_client=http
        ),
        http,
        requests,
    )


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("direct", [False, True])
@pytest.mark.parametrize("fixture", CASES, ids=lambda case: case["name"])
def test_streaming_download_conformance(fixture, direct, asynchronous):
    body = Chunks([fixture["content"].encode()], fixture.get("transport_error", False))
    client, http, requests = stream_client(fixture, direct, body, asynchronous)

    def collect():
        with http:
            with client.files.download_stream(
                "demo", path="/file", request_options={"timeout": 7}
            ) as stream:
                assert body.reads == 0
                return b"".join(stream)

    async def collect_async():
        async with http:
            async with await client.files.download_stream(
                "demo", path="/file", request_options={"timeout": 7}
            ) as stream:
                assert body.reads == 0
                return b"".join([chunk async for chunk in stream])

    expected_error = fixture["direct_error_message" if direct else "error_message"]
    if expected_error:
        with pytest.raises(
            (RuntimeError, httpx.ReadError), match=f"^{re.escape(expected_error)}$"
        ):
            asyncio.run(collect_async()) if asynchronous else collect()
    else:
        result = asyncio.run(collect_async()) if asynchronous else collect()
        assert result == fixture["content"].encode()
    assert body.closed or (
        direct and (fixture["size_bytes"] == 0 or "ranges" in fixture)
    )
    if direct:
        assert sorted(object_requests(requests)) == fixture["object_requests"]
    else:
        assert [r.url.path.rsplit("/", 1)[1] for r in requests] == [
            "capabilities",
            "entry",
            "content",
        ]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("direct", [False, True])
def test_streaming_download_backpressure_and_early_close(direct, asynchronous):
    chunk = b"x" * 65536
    fixture = {
        "size_bytes": len(chunk) * 3,
        "algorithm": "crc64nvme",
        "checksum": _checksum("crc64nvme", chunk * 3).value,
        "range": "bytes=0-196607",
    }
    body = Chunks([chunk] * 3)
    client, http, _ = stream_client(fixture, direct, body, asynchronous)
    if asynchronous:

        async def read_one():
            async with http:
                async with await client.files.download_stream(
                    "demo", path="/file", request_options={"timeout": 7}
                ) as stream:
                    assert await stream.__anext__() == chunk
                    assert body.reads == 1

        asyncio.run(read_one())
    else:
        with http:
            with client.files.download_stream(
                "demo", path="/file", request_options={"timeout": 7}
            ) as stream:
                assert next(stream) == chunk
                assert body.reads == 1
    assert body.closed
    assert body.reads == 1


FIVE_RANGES = {
    "size_bytes": 9,
    "algorithm": "crc64nvme",
    "checksum": _checksum("crc64nvme", b"123456789").value,
    "range": None,
    "ranges": [
        {"start_offset": 0, "length": 2, "range": "bytes=4-5", "content": "12"},
        {"start_offset": 2, "length": 2, "range": "bytes=0-1", "content": "34"},
        {"start_offset": 4, "length": 2, "range": "bytes=8-9", "content": "56"},
        {"start_offset": 6, "length": 2, "range": "bytes=2-3", "content": "78"},
        {"start_offset": 8, "length": 1, "range": "bytes=6-6", "content": "9"},
    ],
}


def object_requests(requests):
    return [r.url.path for r in requests if r.url.host == "objects.test"]


def test_streaming_download_requests_a_window_of_ranges_while_one_is_read():
    bodies = [Chunks([part["content"].encode()]) for part in FIVE_RANGES["ranges"]]
    requested = [threading.Event() for _ in bodies]

    def range_body(index):
        requested[index].set()
        return bodies[index]

    client, http, requests = stream_client(
        FIVE_RANGES, True, None, range_body=range_body
    )
    with http:
        with client.files.download_stream(
            "demo", path="/file", request_options={"timeout": 7}
        ) as stream:
            assert next(stream) == b"12"
            assert all(event.wait(5) for event in requested[:_RANGE_RESPONSES_IN_FLIGHT])
            assert len(object_requests(requests)) == _RANGE_RESPONSES_IN_FLIGHT
            assert not requested[_RANGE_RESPONSES_IN_FLIGHT].is_set()
            assert next(stream) == b"34"
            assert requested[_RANGE_RESPONSES_IN_FLIGHT].wait(5)
            assert b"".join(stream) == b"56789"
    assert sorted(object_requests(requests)) == [f"/object/{i}" for i in range(5)]
    assert all(body.closed for body in bodies)


def test_async_streaming_download_requests_a_window_of_ranges_while_one_is_read():
    bodies = [Chunks([part["content"].encode()]) for part in FIVE_RANGES["ranges"]]

    async def read():
        requested = [asyncio.Event() for _ in bodies]

        def range_body(index):
            requested[index].set()
            return bodies[index]

        client, http, requests = stream_client(
            FIVE_RANGES, True, None, True, range_body=range_body
        )
        async with http:
            async with await client.files.download_stream(
                "demo", path="/file", request_options={"timeout": 7}
            ) as stream:
                assert await stream.__anext__() == b"12"
                for event in requested[:_RANGE_RESPONSES_IN_FLIGHT]:
                    await asyncio.wait_for(event.wait(), 5)
                assert len(object_requests(requests)) == _RANGE_RESPONSES_IN_FLIGHT
                assert not requested[_RANGE_RESPONSES_IN_FLIGHT].is_set()
                assert await stream.__anext__() == b"34"
                await asyncio.wait_for(requested[_RANGE_RESPONSES_IN_FLIGHT].wait(), 5)
                assert b"".join([chunk async for chunk in stream]) == b"56789"
        return requests

    requests = asyncio.run(read())
    assert sorted(object_requests(requests)) == [f"/object/{i}" for i in range(5)]
    assert all(body.closed for body in bodies)
