import hashlib
import json
from pathlib import Path

import httpx
import pytest
from loonfs.server import LoonFS

CASES = json.loads(
    (Path(__file__).parents[1] / "fixtures/streaming_downloads.json").read_text()
)


class Chunks(httpx.SyncByteStream):
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

    def close(self):
        self.closed = True


def stream_client(fixture, direct, body):
    claim = {
        "kind": "blob",
        "content_id": "cnt_00000000000000000000000000000001",
        "size_bytes": fixture["size_bytes"],
        "checksum": {"algorithm": fixture["algorithm"], "value": fixture["checksum"]},
    }
    requests = []

    def handle(request):
        requests.append(request)
        assert request.extensions["timeout"]["read"] == 7
        path = request.url.path
        if path == "/object":
            assert "authorization" not in request.headers
            assert "x-private" not in request.headers
            assert "cookie" not in request.headers
            return httpx.Response(200, stream=body)
        assert request.headers["authorization"] == "Bearer private-token"
        if path.endswith("/capabilities"):
            value = {
                "protocol_version": "v0",
                "api_groups": ["filesystem/v0"],
                "features": {"filesystem.downloads.direct_get": direct},
            }
        elif path.endswith("/downloads"):
            value = {
                "namespace_id": "demo",
                "path": "/file",
                "revision_no": 1,
                "content_ref": claim,
                "access": {
                    "kind": "presigned_url",
                    "method": "GET",
                    "url": "http://objects.test/object",
                    "expires_at_ms": 2000000000000,
                },
            }
        elif path.endswith("/entry"):
            actor = {"kind": "system", "id": "test"}
            value = {
                "inode_kind": "file",
                "namespace_id": "demo",
                "path": "/file",
                "inode_id": "ino_2",
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
            return httpx.Response(200, stream=body)
        else:
            raise AssertionError(str(request.url))
        return httpx.Response(200, json=value)

    http = httpx.Client(
        transport=httpx.MockTransport(handle),
        headers={"X-Private": "secret"},
        cookies={"private": "secret"},
    )
    return (
        LoonFS(base_url="http://api.test", token="private-token", httpx_client=http),
        http,
        requests,
    )


@pytest.mark.parametrize("direct", [False, True])
@pytest.mark.parametrize("fixture", CASES, ids=lambda case: case["name"])
def test_streaming_download_conformance(fixture, direct):
    body = Chunks([fixture["content"].encode()], fixture.get("transport_error", False))
    client, http, requests = stream_client(fixture, direct, body)
    with http:
        with client.files.download_stream(
            "demo", path="/file", request_options={"timeout": 7}
        ) as stream:
            assert body.reads == 0, "opening the download must not consume its body"
            if fixture["error"]:
                with pytest.raises((RuntimeError, httpx.ReadError)):
                    b"".join(stream)
            else:
                assert b"".join(stream) == fixture["content"].encode()
        assert body.closed


@pytest.mark.parametrize("direct", [False, True])
def test_streaming_download_backpressure_and_early_close(direct):
    chunk = b"x" * 65536
    fixture = {
        "size_bytes": len(chunk) * 3,
        "algorithm": "sha256",
        "checksum": hashlib.sha256(chunk * 3).hexdigest(),
    }
    body = Chunks([chunk] * 3)
    client, http, _ = stream_client(fixture, direct, body)
    with http:
        with client.files.download_stream(
            "demo", path="/file", request_options={"timeout": 7}
        ) as stream:
            assert next(stream) == chunk
            assert body.reads == 1, "the next chunk must wait for the consumer"
        assert body.closed
        assert body.reads == 1
