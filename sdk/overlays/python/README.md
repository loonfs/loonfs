# LoonFS Python SDK

One package for LoonFS server and proxy applications. SDK v0.2.x targets LoonFS
API v0.3.x.

## Install

```sh
pip install loonfs
```

Choose the module that matches where your code runs.

## Server

```python
import os

from loonfs.server import LoonFS

client = LoonFS(
    base_url=os.environ["LOONFS_URL"],
    token=os.environ["LOONFS_AUTH_TOKEN"],
    actor_id="example-user",
)

capabilities = client.capabilities.retrieve()
commit = client.files.upload("demo", path="/hello.txt", content=b"hello")
print(commit.commit_id, commit.events)
```

Publishing helpers return the commit, including its events. Pass `commit_id`
explicitly if you may retry. A call's `actor_id` overrides the client default.

Use `client.files.download_stream` in a `with` block for bounded download memory:

```python
with client.files.download_stream("demo", path="/large.bin", request_options={"timeout": 60}) as download:
    for chunk in download:
        destination.write(chunk)
```

Successful exhaustion verifies both size and checksum. Leaving the block early
closes the response without claiming verification; bytes already consumed cannot
be recalled if a later check fails. The request's `timeout` (or the client default)
applies to metadata and payload HTTP I/O for direct and proxied reads. Python's
synchronous HTTPX timeout bounds I/O waits, not the time spent processing chunks.
Direct requests do not inherit API authorization, cookies or API headers, and
never follow redirects. Closing the stream or interrupting its `with` block is
the synchronous cancellation mechanism.

`client.files.download` collects the same verified stream into memory.
`client.files.upload` accepts in-memory bytes through the same transfer path.
Use `prepare_stream` to retain prepared content for publication retries:

```python
with open("large.bin", "rb") as source:
    prepared = client.files.prepare_stream(
        "demo", content=source, request_options={"timeout": 60}
    )
```

Pass `size_bytes` when known to validate the source and choose the usual transport.
Unknown nonempty sources use multipart when available; memory is bounded by a
provider-sized part. `upload_stream` prepares and publishes in one operation.
The HTTP I/O timeout applies to both transports. Source and payload failures abort
without replaying bytes. The caller owns the source and must interrupt any
blocking source read; an HTTP timeout cannot interrupt arbitrary Python code.

 `AsyncLoonFS` provides the same generated API for async applications; it does not have the transfer methods yet.

## Proxy

Use `loonfs.proxy` in your backend to forward client requests while keeping the
LoonFS credential on the server.

Set `authorize` to check each request and replace `actor_id` in commit bodies.
Here, `authorized_actor` checks the application's session and namespace access.

```python
from loonfs.proxy import LoonFSProxy, ProxyAuthorization, ProxyRefusal

async def authorize(scope, context):
    actor_id = await authorized_actor(scope, context.namespace_id)
    if actor_id is None:
        return ProxyRefusal(status=403)
    return ProxyAuthorization(actor_id=actor_id)

app = LoonFSProxy(
    os.environ["LOONFS_URL"],
    os.environ["LOONFS_AUTH_TOKEN"],
    {"team-files": "demo"},
    authorize=authorize,
)
```

See the [generated API reference](https://github.com/loonfs/loonfs-sdk-python/blob/main/reference.md).

## Retries

The SDK retries connection failures and responses that carry `Retry-After`,
and does not retry on status alone. It never retries operations that LoonFS
marks `not_idempotent`. Use the `max_retries` client or request option to
tune the retry count.

For publication retries, call `client.files.prepare(namespace_id,
content=payload)` once and retain its `PreparedContent`. Pass it to
`client.files.upload_prepared(namespace_id, path=path, prepared=prepared,
actor_id=actor_id, commit_id=commit_id)` on each attempt, keeping all publication
inputs identical. Preparation does not create a visible file or extend the
upload lifetime. Calling `upload` again starts a fresh upload and cannot replay
a previously committed ID.

## Generated code

This SDK is generated from the LoonFS OpenAPI specification. Please report SDK
issues in the [main LoonFS repository](https://github.com/loonfs/loonfs).

## License

Apache-2.0.
