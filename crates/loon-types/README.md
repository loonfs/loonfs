# loon-types

Shared IDs, enums, and plain data structures used across the LoonDB workspace.

This is the foundation crate — it has no internal dependencies and is imported by every other crate
in the workspace. All types here are pure data: serializable, cloneable, and side-effect free.

`#![forbid(unsafe_code)]`

## What this crate owns

- **Identity types** — `InodeId`, `NamespaceId`, `ChangeSeq`, `FenceToken`, `RevisionNo`, and other
  strongly-typed IDs used throughout the protocol
- **WAL types** — commit envelopes, operation payloads, and precondition definitions
- **Checkpoint types** — snapshot manifests, segment metadata, and table family descriptors
- **Content types** — content manifest envelopes, block descriptors, and digest utilities
- **Control types** — `HeadState`, `LeaseState`, and progress tracking structures
- **Client types** — mutation request/response envelopes for the client-server protocol
- **Conflict types** — conflict artifact definitions and resolution metadata

## What this crate does not own

- Protocol rules or validation logic (see `loon-core`)
- I/O, storage, or network operations (see `loon-objectstore`, `loon-client`, `loon-server`)

## Public API

All internal modules are private and re-exported at the crate root via `pub use`. Types are accessed
directly as `loon_types::InodeId`, `loon_types::ChangeSeq`, etc.
