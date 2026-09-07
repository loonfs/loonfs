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

`client.files.upload` and `client.files.download` transfer whole files in memory. `AsyncLoonFS` provides the same generated API for async applications; it does not have the transfer methods yet.

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
