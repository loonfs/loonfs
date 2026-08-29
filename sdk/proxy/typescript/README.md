# LoonFS TypeScript proxy

`@loonfs/sdk-proxy` provides a fetch-compatible handler for LoonFS browser
requests. It maps namespace aliases to namespaces, adds the server credential, and
forwards only the routes in the proxy API.

The package requires Node 18 or newer and has no runtime dependencies. SDK
v0.1.x targets LoonFS API v0.3.x.

## Install

```sh
npm install @loonfs/sdk-proxy
```

## Usage

```ts
import { createProxyHandler } from "@loonfs/sdk-proxy";

const handle = createProxyHandler({
    serverBaseUrl: "https://loonfs.example.com",
    token: process.env.LOONFS_TOKEN!,
    namespaceAliases: {
        "team-files": "namespace_123",
    },
});

const response = await handle(request);
```

Pass the raw server token as `token`. The handler sends it as a Bearer
credential. It streams uploads and downloads without retrying, caching, or
changing response bodies.

## License

Apache-2.0.
