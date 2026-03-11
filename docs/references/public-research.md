# Public research notes

These are external references that informed the current scaffold.

## Dropbox

- Testing sync at Dropbox / “Testing our new sync engine”
  - design away invalid states
  - use a deterministic randomized test harness
  - separate narrow semantic testing from broader concurrency simulation

## turbopuffer

- fast search on object storage
- distributed queue in a single JSON file on object storage
- design derived/background work around immutable outputs and a small mutable coordination object

## Quickwit

- Rust repo inspiration for Cargo workspace structure and crate boundaries
- object store abstraction and storage conformance testing
- dependency hygiene for a multi-crate server codebase

## AWS S3

- strong consistency for PUT/DELETE/LIST/HEAD
- conditional writes with `If-None-Match` and `If-Match`

## Cloudflare R2

- strong consistency for reads, writes, deletes, and listings via the S3 API
- S3-compatible API with `auto` region
- multipart and ETag caveats mean provider ETags must not become canonical content identity

## How to use this file

Keep this as a lightweight rationale index. Do not let it become the source of truth. The source of truth for repository behavior belongs in `docs/specs/` and `docs/adr/`.
