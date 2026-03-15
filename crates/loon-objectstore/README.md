# loon-objectstore

Object-store abstraction, provider profiles, and the conformance surface that all durable LoonDB
behavior depends on.

This crate is the only layer that should know provider quirks.

Higher layers may depend on:

- the `ObjectStore` trait
- trait-level errors such as `PreconditionFailed`
- opaque compare tokens returned as `ObjectMetadata.etag`

Higher layers should not depend on:

- provider-specific HTTP status codes or header names
- SDK-specific error strings
- ETags as canonical content identity
- endpoint, path-style, or prefixing quirks
