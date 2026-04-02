# ADR 0022: authoritative file writes resolve and commit under one server-side basis

Status: accepted

`loon file ...` now has two surfaces:

- read commands that project authoritative state by path
- write commands that mutate authoritative state by path

For writes, path resolution must happen inside one authoritative server-side mutation flow.

Rules:

- `loon-ops` and `loon-cli` do not resolve selectors and then call the existing inode-addressed
  mutation path as two separate authoritative steps
- the server-side write helper must:
  - acquire or renew the namespace lease
  - load one verified basis
  - resolve selectors against that basis
  - translate to existing inode-keyed mutation ops
  - reuse the existing commit/WAL/head-publish machinery
- `put` uploads durable content first, then enters that same leased path-mutation flow

Consequences:

- `loon file put/mkdir/rm/mv` remain product-facing commands without becoming a second semantics
  engine
- path drift between selector resolution and commit validation is prevented by construction
- the existing client/inode-addressed mutation path remains the shared commit substrate rather than
  being duplicated in a path-addressed shell
