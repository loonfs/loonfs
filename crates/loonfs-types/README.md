# loonfs-types

`loonfs-types` defines the types that the other LoonFS crates share: validated identifiers and paths, the HTTP API's request and response shapes, the codecs of the durable formats, and the per-operation options. The crate defines shapes, validation, and codecs, and it doesn't read or write objects, files, or network connections. The crates above it do the I/O. To read and write namespaces, use the embedded runtime, [`loonfs`](../loonfs), or the HTTP client, [`loonfs-client`](../loonfs-client). You'd add this crate beside `loonfs-client` for the types the client takes but doesn't re-export, such as `Context`, `ActorId`, and `NamespaceId`. The `loonfs` crate root re-exports the types that its own signatures use.

The crate has four parts:

- **Identifiers and paths**, at the crate root. Each kind of ID is its own newtype with a `parse` constructor that validates the input, so one kind of ID can't be passed as another. `AbsolutePath::parse` accepts only the canonical spelling of an absolute path, so it rejects `.` and `..` components and empty components such as `//`.
- **API shapes**, in `api::v0`, which the crate root also re-exports. With the `openapi` feature, these types also derive their OpenAPI schemas.
- **Durable formats**, in `format`: the codecs of manifests, control objects, WAL objects, segment blocks, and the envelope they share. The golden fixtures in `tests/golden` pin the bytes that each format writes.
- **Options**, in `options`: the per-operation options types, such as `PutFileOptions`. The embedded runtime and the HTTP client both use these types, so the two crates take the same fields.

```rust
use loonfs_types::options::PutFileOptions;
use loonfs_types::{AbsolutePath, DestinationBehavior, NamespaceId, PathError};

let namespace_id = NamespaceId::parse("demo").expect("namespace");
let path = AbsolutePath::parse("/docs/a.txt").expect("path");
assert!(matches!(
    AbsolutePath::parse("/docs/../a.txt"),
    Err(PathError::ParentComponent { .. })
));

let options = PutFileOptions {
    behavior: DestinationBehavior::Replace,
    ..Default::default()
};
```

A namespace ID is 1 to 128 lowercase ASCII letters, digits, dots, underscores, or hyphens, and it starts with a letter or digit. The `loonfs-` prefix is reserved for LoonFS system namespaces. A generated ID, such as a content ID or an upload ID, is a type prefix, an underscore, and 26 lowercase Crockford base 32 characters, such as `upl_3s0drmj8em84yb4mnetqkz2j0x`.

`ErrorCode` lists every error code that the HTTP API returns, and `ErrorCode::kind` groups the codes by what a caller does about one.

The [storage format spec](../../docs/specs/format.md) defines the durable formats, and [Numbered objects and generated IDs](../../docs/specs/format.md#23-numbered-objects-and-generated-ids) defines each kind of generated ID. The [API spec](../../docs/specs/api.md) defines the request and response shapes.
