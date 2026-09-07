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
)

capabilities = client.capabilities.retrieve()
```

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
Use `prepare_file_stream` to retain prepared content for publication retries:

```python
with open("large.bin", "rb") as source:
    prepared = client.files.prepare_file_stream("demo", content=source,
                                                request_options={"timeout": 60})
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

See the [generated API reference](https://github.com/loonfs/loonfs-sdk-python/blob/main/reference.md).

## Retries

The SDK retries transient failures on operations that are safe to repeat.
Operations that LoonFS classifies as non-idempotent are never retried
automatically. Use the `max_retries` client or request option to tune retries for
safe operations.

For publication retries, call `client.files.prepare_file_bytes(namespace_id,
content=payload)` once and retain its `PreparedFileContent`. Pass it to
`client.files.put_file_prepared(namespace_id, path=path, prepared=prepared,
actor=actor, commit_id=commit_id)` on each attempt, keeping all publication
inputs identical. Preparation does not create a visible file or extend the
upload lifetime. Calling `upload` again starts a fresh upload and cannot replay
a previously committed ID.

## Generated code

This SDK is generated from the LoonFS OpenAPI specification. Please report SDK
issues in the [main LoonFS repository](https://github.com/loonfs/loonfs).

## License

Apache-2.0.
