# LoonFS TypeScript proxy

`@loonfs/sdk-proxy` provides a fetch-compatible handler for the LoonFS browser
API. It maps configured mount names to namespace ids. It replaces the browser's
authorization header with the server credential. It forwards only the routes
in the proxy OpenAPI document.

The package requires Node 18 or newer and has no runtime dependencies.

## Status

This package is pre-release. It is not yet published to npm.

## Install

After the first release, install it with:

```sh
npm install @loonfs/sdk-proxy
```

## Usage

```ts
import { createProxyHandler } from "@loonfs/sdk-proxy";

const handle = createProxyHandler({
    serverBaseUrl: "https://loonfs.example.com",
    token: process.env.LOONFS_TOKEN!,
    mounts: {
        "team-files": "namespace_123",
    },
});

const response = await handle(request);
```

`token` is the raw server token. The handler sends it as a Bearer credential.
Request and response bodies stay as web `ReadableStream` values. This includes
uploads and file reads. The handler does not retry, cache, or change response
bodies.

## License

Apache-2.0.
