# Proposal: CLI v0 narrowing for the spec-locked rewrite

Superseded by proposal 002.

## Type

`narrowing`

## Affected specs

- `docs/specs/070-cli-definition.md`
- `docs/specs/080-repo-and-delivery-plan.md`

## Problem

The locked CLI spec still describes a broader surface than the current rewrite can support without
reintroducing premature complexity. The active rewrite is using the CLI as a thin direct-to-store
product surface, not as a sync client or a server-backed control plane.

## Proposed change

For CLI v0:

- execute only direct-to-store `store` profiles
- reserve `server` mode in the config schema and reject it deterministically at runtime
- keep namespace operations to `create` and `list`
- defer namespace rename/delete
- keep filesystem operations non-recursive
- keep `cp` file-only and same-namespace
- require JSON versioned envelopes on CLI machine output

## Rewrite decision

The rewrite is following this proposal now.

## Consequences

- code stays centered on shared direct-to-store semantics in `loon-core`
- tests focus on local-fs CI coverage plus provider conformance, not server-mode behavior
- future server-mode work can add transport/auth on top of the existing profile model without a
  schema reset
