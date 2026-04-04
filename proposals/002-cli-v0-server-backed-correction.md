# Proposal: Correct CLI v0 from direct-store to server-backed local and remote profiles

## Type

`superseding correction`

## Affected specs

- `docs/specs/070-cli-definition.md`
- `docs/specs/080-repo-and-delivery-plan.md`

## Problem

The direct-store CLI v0 narrowed the implementation too far. It bypassed the intended server
boundary in local operation and made the CLI own object-store construction and core mutation
execution directly.

## Proposed change

For CLI v0:

- replace `store` and `server` profile modes with `local` and `remote`
- route both modes through `loond`
- keep object-store credentials in `loond` config, not in CLI profile config
- add managed local server lifecycle commands: `local up`, `local status`, `local down`
- preserve the existing namespace and filesystem command surface, but execute it through HTTP
- remove the direct-store-only CLI `doctor` path from this milestone

## Rewrite decision

The rewrite is following this proposal now.

## Consequences

- the CLI/runtime boundary matches remote and local operation
- `loon-client` and `loon-server` become required for all CLI execution
- the old pre-release direct-store CLI config is intentionally rejected instead of migrated
- direct-store docs, tests, and config examples are removed or superseded
