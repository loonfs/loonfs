# Proposal: Correct CLI v0 from direct-store to server-backed local and remote profiles

Superseded by proposal 003.

## Type

`superseding correction`

## Affected specs

- `docs/specs/060-interfaces-and-clients.md`

## Problem

The direct-store CLI v0 narrowed the implementation too far. It bypassed the intended server
boundary in local operation and made the CLI own object-store construction and core mutation
execution directly. The imported core spec family now makes the broader public surface explicit,
so this proposal is historical rather than current.

## Proposed change

For the historical CLI correction:

- replace `store` and `server` profile modes with `local` and `remote`
- route both modes through `loond`
- keep object-store credentials in `loond` config, not in CLI profile config
- add managed local server lifecycle commands: `local up`, `local status`, `local down`
- preserve the existing namespace and filesystem command surface, but execute it through HTTP
- remove the direct-store-only CLI `doctor` path from this milestone

## Rewrite decision

The rewrite is no longer following this proposal directly. Proposal 003 now describes how the
current branch relates the local/remote CLI model to the imported core spec family.

## Consequences

- the server-backed local/remote CLI path remains useful implementation history
- it is not the full public-contract story under the imported core spec family
