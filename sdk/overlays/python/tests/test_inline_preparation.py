import asyncio

import httpx
import pytest

from loonfs.server import AsyncLoonFS, InlinePreparedContent, LoonFS


@pytest.mark.parametrize("asynchronous", [False, True])
def test_prepare_small_file_from_capabilities(asynchronous):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "protocol_version": "v0",
                "api_groups": ["filesystem/v0"],
                "features": {"filesystem.commits.inline_content": True},
                "limits": {"commit.max_inline_content_bytes_per_operation": 65536},
            },
        )

    async def prepare_async():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            client = AsyncLoonFS(base_url="http://api.test", token="test", httpx_client=http)
            return await client.files.prepare("test", content=b"hello")

    if asynchronous:
        prepared = asyncio.run(prepare_async())
    else:
        with httpx.Client(transport=httpx.MockTransport(respond)) as http:
            client = LoonFS(base_url="http://api.test", token="test", httpx_client=http)
            prepared = client.files.prepare("test", content=b"hello")

    assert isinstance(prepared, InlinePreparedContent)
    assert prepared.content == b"hello"
    assert len(requests) == 1
    assert requests[0].url.path == "/v0/capabilities"
