# ADR 0019: CLI v0 uses server-backed local and remote profiles

Status: accepted

Supersedes: ADR 0018

## Decision

CLI v0 is a server-backed product surface.

This ADR describes the current path-oriented CLI implementation layer. It does not replace the
broader public surface defined by the imported LoonFS core spec family.

Rules:

- the public context noun is `profile`
- profile `mode` is `local` or `remote`
- both modes execute through `loond`
- `local` means the CLI points at or manages a local `loond`
- `remote` means the CLI points at an already-running `loond`
- `local` profiles store only a `loond` config path
- `remote` profiles store a server URL and optional bearer token
- the CLI never constructs object-store providers directly
- the public filesystem noun is `filesystem`
- namespace addressing is by namespace name only
- profiles do not carry a default namespace
- JSON output uses versioned envelopes with `kind` and `format_version`
- clap parsing, config resolution, runtime management, backend execution, and rendering stay separate
- filesystem mutations execute through `loon-client` over HTTP, not by calling core mutations in the CLI
- `mv` and `cp` use exact target-path semantics; no implicit “move or copy into directory”

## Consequences

- the runtime path for both local and remote execution is:
  `loon-cli -> loon-client -> loon-server -> loon-core -> loon-objectstore`
- this ADR defines one path-oriented client profile implementation, not the full public protocol
- `loon-core` remains the owner of overwrite policy, non-recursive delete checks, and file copy semantics
- `loon-server` and `loon-client` must expose the full CLI v0 mutation surface, including copy and create-only vs replace semantics for put
- CLI config uses `config_version = 1` for the current local/remote schema
- local server lifecycle is explicit through `loon local up|status|down`
- direct-store doctor flows are deferred until a later slice
