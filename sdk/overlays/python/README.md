# LoonFS Python SDK

Typed synchronous and asynchronous clients for the LoonFS HTTP API. SDK v0.1.x
targets LoonFS API v0.3.x.

## Install

```sh
pip install loonfs
```

## Usage

```python
import os

from loonfs import LoonFS

client = LoonFS(
    base_url=os.environ["LOONFS_URL"],
    token=os.environ["LOONFS_AUTH_TOKEN"],
)

capabilities = client.system.get_capabilities()
```

`AsyncLoonFS` provides the same API for async applications. Upload and download
helpers are available from `loonfs.transfers`.

See the [generated API reference](https://github.com/loonfs/loonfs-sdk-python/blob/main/reference.md).

## Retries

The SDK retries transient failures on operations that are safe to repeat.
Operations that LoonFS classifies as non-idempotent are never retried
automatically. Use the `max_retries` client or request option to tune retries for
safe operations.

## Generated code

This SDK is generated from the LoonFS OpenAPI specification. Please report SDK
issues in the [main LoonFS repository](https://github.com/loonfs/loonfs).

## License

Apache-2.0.
