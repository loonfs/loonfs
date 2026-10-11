# LoonFS browser client

Import `LoonFSClient` from `@loonfs/sdk/client`. Set `baseUrl` to the application
proxy. Requests use `namespace_alias`; the proxy maps it to a namespace and
supplies server credentials.

The browser and server clients share the
[TypeScript helper contract](https://github.com/loonfs/loonfs-sdk-typescript#transfer-helpers).
It documents ordered range reads, verification on resume, upload modes,
prepared publication, append, and proxy configuration.
