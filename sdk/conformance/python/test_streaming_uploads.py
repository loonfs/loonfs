import io
import json
from pathlib import Path

import httpx
import pytest
from loonfs.server import LoonFS
from loonfs.core.api_error import ApiError

CASES = json.loads(
    (Path(__file__).parents[1] / "fixtures/streaming_uploads.json").read_text()
)


class Source(io.BytesIO):
    def __init__(self, fixture):
        super().__init__(fixture["content"].encode())
        self.fixture = fixture
        self.eof = False

    def read(self, size=-1):
        assert 0 < size <= 65536, "source reads must stay bounded"
        result = super().read(size)
        if not result:
            self.eof = True
            if self.fixture.get("fault") == "source_error":
                raise OSError("source failed after its last byte")
        return result


@pytest.mark.parametrize("fixture", CASES, ids=lambda c: c["name"])
def test_streaming_uploads(fixture):
    source = Source(fixture)
    mode, fault = fixture["mode"], fixture.get("fault")
    counts = {"payload": 0, "abort": 0, "complete": 0}
    bodies = []
    claim = {
        "kind": "blob",
        "content_id": "cnt_00000000000000000000000000000001",
        "size_bytes": fixture["size_bytes"],
        "checksum": {"algorithm": fixture["algorithm"], "value": fixture["checksum"]},
    }
    session = {"namespace_id": "demo", "upload_id": "upl_test", "mode": mode}
    access = {
        "kind": "presigned_url",
        "method": "PUT",
        "url": "http://objects.test/object",
        "expires_at_ms": 2000000000000,
    }

    def handle(request):
        path = request.url.path
        assert request.extensions["timeout"]["read"] == (
            5 if path.endswith("/abort") else 7
        )
        if path == "/object":
            assert "authorization" not in request.headers
            assert "x-private" not in request.headers
            assert "cookie" not in request.headers
            if mode == "direct_put":
                assert int(request.headers["content-length"]) == fixture["size"]
        else:
            assert request.headers["authorization"] == "Bearer private-token"
        if path.endswith("/capabilities"):
            value = {
                "protocol_version": "v0",
                "api_groups": ["filesystem/v0"],
                "features": {
                    "filesystem.uploads.direct_put": mode == "direct_put",
                    "filesystem.uploads.direct_multipart": mode == "direct_multipart",
                },
                "limits": {"upload.max_content_bytes": 0}
                if mode == "direct_put"
                else {},
            }
        elif path.endswith("/uploads"):
            assert json.loads(request.content)["mode"] == mode
            value = {
                **session,
                "checksum_algorithm": fixture["algorithm"],
                "part_size_bytes": 4,
                "access": access,
            }
        elif path.endswith("/parts"):
            parts = json.loads(request.content)["parts"]
            assert len(parts) == 1
            assert source.tell() <= len(bodies) * 4 + 4, (
                "must send a part before reading the next"
            )
            value = {
                "namespace_id": "demo",
                "upload_id": "upl_test",
                "parts": [{"part_number": parts[0]["part_number"], "access": access}],
            }
        elif path == "/object" or path.endswith("/content"):
            counts["payload"] += 1
            bodies.append(request.read())
            if fault == "timeout":
                raise httpx.WriteTimeout("payload timed out")
            if fault == "payload_error":
                return httpx.Response(503)
            return httpx.Response(
                200,
                headers={"ETag": "test-etag"},
                json={**session, "content_ref": claim},
            )
        elif path.endswith("/abort"):
            counts["abort"] += 1
            value = {**session, "status": "aborted", "aborted_at_ms": 0}
        elif path.endswith("/complete"):
            counts["complete"] += 1
            assert source.eof
            completion = json.loads(request.content)
            if mode != "service_proxied":
                assert completion["content"] == {
                    "size_bytes": fixture["size_bytes"],
                    "checksum": claim["checksum"],
                }
            if fault == "completion_error":
                return httpx.Response(503)
            value = {
                **session,
                "status": "completed",
                "completed_at_ms": 0,
                "content_ref": claim,
                "content_token": {"token": "retained-token", "content_ref": claim},
            }
        else:
            raise AssertionError(str(request.url))
        return httpx.Response(200, json=value)

    with httpx.Client(
        transport=httpx.MockTransport(handle),
        headers={"X-Private": "secret"},
        cookies={"private": "secret"},
    ) as http:
        client = LoonFS(
            base_url="http://api.test", token="private-token", httpx_client=http
        )

        def prepare():
            return client.files.prepare_file_stream(
                "demo",
                content=source,
                size_bytes=fixture["size"],
                request_options={"timeout": 7, "max_retries": 3},
            )

        if fixture["error"]:
            with pytest.raises((OSError, ValueError, httpx.HTTPError, ApiError)):
                prepare()
            assert counts["abort"] == (0 if fault == "completion_error" else 1)
            assert counts["complete"] == (1 if fault == "completion_error" else 0)
            if fault == "payload_error":
                assert counts["payload"] == 1
        else:
            prepared = prepare()
            assert prepared.content_ref.size_bytes == len(fixture["content"])
            assert prepared.content_token.token == "retained-token"
            assert b"".join(bodies) == fixture["content"].encode()
            assert counts["complete"] == 1 and counts["abort"] == 0
        assert not source.closed, "callers own their source"
