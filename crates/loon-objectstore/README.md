# loon-objectstore

Object-store abstraction, provider profiles, and the conformance surface that all durable LoonDB
behavior depends on.

This crate is the only layer that should know provider quirks.

`#![forbid(unsafe_code)]`

## Status

Implemented providers:

- **Local filesystem** (`fs`) — atomic writes via temp+rename, etag derived from size+mtime
- **AWS S3** (`s3`) — conditional headers for CAS, strong read-after-write consistency
- **Cloudflare R2** (`r2`) — S3-compatible via shared implementation with region="auto"

Conformance suite: 10 test cases covering create-if-absent, compare-and-swap, idempotent delete,
list visibility, sorted prefix listing, byte-range reads, key validation, and scoped prefixing.
LocalFS runs by default; S3/R2 require environment variables.

## Public API surface

- **`ObjectStore`** trait — 5 core methods: `head`, `get`, `put`, `delete`, `list_prefix`; plus
  convenience methods `put_overwrite`, `put_if_absent`, `compare_and_swap`
- **`ObjectMetadata`** — etag (opaque CAS token) and size
- **`PutMode`** — `Overwrite`, `CreateIfAbsent`, `CompareAndSwap`
- **`ByteRange`** — byte-range read support
- **`ObjectStoreError`** — stable error enum (`PreconditionFailed`, `InvalidKey`, etc.)
- **`ConfiguredObjectStore`** — unified wrapper that dispatches to the configured provider

## Provider contract

Provider profiles distinguish between:

- active contract fields that other crates may rely on now
- future capability flags that stay informational until the trait and conformance suite grow

Higher layers may depend on:

- the `ObjectStore` trait
- trait-level errors such as `PreconditionFailed`
- opaque compare tokens returned as `ObjectMetadata.etag`

Higher layers should not depend on:

- provider-specific HTTP status codes or header names
- SDK-specific error strings
- ETags as canonical content identity
- endpoint, path-style, or prefixing quirks
