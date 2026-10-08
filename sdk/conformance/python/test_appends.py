"""The append helper sends one commit and refuses content it cannot carry."""

import asyncio
import base64
import json
from pathlib import Path

import httpx
import pytest
from loonfs.server import AsyncLoonFS, LoonFS

CASES = json.loads((Path(__file__).parents[1] / "fixtures/appends.json").read_text())


def endpoint(content, requests):
    def handle(request):
        requests.append(request.url.path)
        assert request.method == "POST"
        assert request.url.path == "/v0/namespaces/demo/commits"
        assert request.headers["loonfs-actor"] == "writer"
        body = json.loads(request.content)
        assert body["commit_id"] == "stable" and body["message"] == "original"
        assert not body.get("content_tokens")
        [operation] = body["operations"]
        assert operation["kind"] == "append_file" and operation["path"] == "/file"
        assert operation["expected_inode_id"] == "ino_1"
        assert operation["expected_revision_no"] == 1
        assert "behavior" not in operation
        assert base64.b64decode(operation["inline_content"], validate=True) == content
        return httpx.Response(
            200,
            json=dict(
                namespace_id="demo",
                commit_id="stable",
                committed_seq=2,
                committed_at_ms=0,
                committed_by="writer",
                events=[],
            ),
        )

    return handle


def arguments(content):
    return dict(
        path="/file",
        content=content,
        commit_id="stable",
        message="original",
        expected_inode_id="ino_1",
        expected_revision_no=1,
        request_options={
            "max_retries": 0,
            "additional_headers": {"Loonfs-Actor": "writer"},
        },
    )


@pytest.mark.parametrize("fixture", CASES, ids=lambda f: f["name"])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_append_sends_one_commit(fixture, asynchronous):
    content = bytes(i % 256 for i in range(fixture["bytes"]))
    requests = []
    transport = httpx.MockTransport(endpoint(content, requests))

    async def append_async():
        async with httpx.AsyncClient(transport=transport) as http:
            client = AsyncLoonFS(
                base_url="http://api.test", token="secret", httpx_client=http
            )
            return await client.files.append("demo", **arguments(content))

    def append():
        if asynchronous:
            return asyncio.run(append_async())
        with httpx.Client(transport=transport) as http:
            client = LoonFS(
                base_url="http://api.test", token="secret", httpx_client=http
            )
            return client.files.append("demo", **arguments(content))

    if fixture["sent"]:
        assert append().committed_seq == 2
        assert len(requests) == 1
    else:
        with pytest.raises(ValueError):
            append()
        assert requests == [], "a refused append must send nothing"
