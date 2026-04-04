# ADR 0018: CLI v0 uses direct-to-store profiles

Status: superseded by ADR 0019

## Decision

CLI v0 is a direct-to-store product surface.

Rules:

- the public context noun is `profile`
- backend `mode` is `store` or `server`
- v0 executes only `store`
- `server` profiles are parsed and displayed, but execution returns
  `server_mode_not_yet_available`
- the public filesystem noun is `filesystem`
- namespace addressing is by namespace name only
- profiles do not carry a default namespace
- credentials come only from static values in the CLI config file
- no provider config or credential resolution uses env vars or keychain state
- JSON output uses versioned envelopes with `kind` and `format_version`
- clap parsing, config resolution, backend execution, and rendering stay separate
- filesystem mutations call shared authoritative library code, not client import/sync flows
- `mv` and `cp` use exact target-path semantics; no implicit “move or copy into directory”

## Consequences

- the CLI can operate directly against local-fs, AWS S3, or Cloudflare R2 without a server
- `loon-core` owns `put` overwrite policy, non-recursive delete checks, and file copy semantics
- `doctor` reuses production-safe object-store contract probes extracted from conformance logic
- namespace rename/delete and recursive filesystem operations remain out of scope for v0
- future server mode can be added without changing the top-level config schema
