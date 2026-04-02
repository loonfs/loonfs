# loon-server

Thin server integration crate for the LoonDB mutation pipeline.

`#![forbid(unsafe_code)]`

## Status

**Active surface**: `mutation` — composes `loon-core` and related crates into the authoritative
create/replace execution path (lease acquisition, precondition validation, WAL write, head publish).

**Active surface**: `ops` — namespace bootstrapping and state summary loading.

**Quarantined**: The `loond` binary shell and HTTP transport layer are intentionally unavailable
during the semantic-core reset. The repo does not imply a fuller server surface than actually exists.

## Public modules

| Module | Purpose |
|--------|---------|
| `mutation` | Authoritative mutation execution: lease, validate, commit, publish |
| `ops` | Namespace bootstrap and state inspection |
