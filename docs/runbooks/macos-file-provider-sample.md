# Runbook: macOS File Provider sample contract

This runbook describes the expected contract for the out-of-tree native macOS File Provider sample.

## Purpose

The sample exists to prove Finder integration against the in-repo Rust bridge without forcing the
main repo to carry native app packaging yet.

## Sample inputs

The sample should point at:

- an existing `OpsConfig`
- a namespace allowlist

The sample owns its own thin wrapper configuration. The main repo does not add a second in-repo
config format for this spike.

## Expected Rust bridge calls

The sample should call into `loon-macos` for:

- root listing
- item lookup
- child listing
- targeted item materialization

The sample should treat `ProviderItemId` values as opaque.

## Expected sample behavior

- one Finder-visible account/root domain
- configured namespaces appear as top-level directories
- listings come from the current client SQLite snapshot only
- placeholder files and directories stay placeholders until Finder asks for local bytes
- file open/content fetch calls targeted materialization

## Explicit non-goals for the sample

The first sample should not:

- auto-import authoritative state
- auto-sync local state
- create, modify, delete, or rename items
- define a second local sync database
- infer item identity from paths

## Expected operational workflow

Freshness still comes from the current explicit CLI/operator flow outside the sample:

1. `loon ops bootstrap-namespace ...`
2. `loon ops import-remote-observations ...`
3. `loon ops sync-once ...` or `loon ops sync-until-idle ...`
4. enumerate or materialize through the File Provider sample
