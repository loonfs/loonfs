# Runbook: macOS File Provider sample contract

This runbook describes the expected contract for the out-of-tree native macOS File Provider sample.

## Purpose

The sample exists to prove Finder integration against the in-repo Rust bridge without forcing the
main repo to carry native app packaging yet.

## Sample inputs

The sample should point at:

- an existing `OpsConfig`
- a namespace allowlist

The sample owns its own thin wrapper configuration in an App Group container. The main repo does
not add a second in-repo config format for this spike.

Expected sample config fields:

- `ops_config_path`
- `exposed_namespaces`
- `domain_identifier`
- `domain_display_name`
- `app_group_identifier`

## Expected Rust bridge calls

Swift should call into `loon-macos` through the C ABI + JSON surface for:

- `open`
- `close`
- root listing
- item lookup
- child listing
- targeted item materialization
- `string_free`

The sample should treat bridge item ids as opaque encoded strings.

## App Group and process boundary

- the containing app and the File Provider extension share the sample wrapper config through an App
  Group container
- the containing app reads that config and registers or removes the File Provider domain
- the extension opens the Rust bridge from the shared config and serves Finder callbacks against the
  current SQLite snapshot

## Domain registration and reset flow

Containing app responsibilities:

1. read the App Group sample config
2. call `NSFileProviderManager.getDomainsWithCompletionHandler`
3. add the configured domain if missing
4. provide a local-testing reset path that removes the domain and lets the sample re-register it

This keeps domain lifecycle in the containing app instead of splitting it across app and extension
code.

## Expected sample behavior

- one Finder-visible account/root domain
- configured namespaces appear as top-level directories
- listings come from the current client SQLite snapshot only
- placeholder files and directories stay placeholders until Finder asks for local bytes
- file open/content fetch calls targeted materialization
- item identifiers are the opaque bridge ids returned by `loon-macos`
- the first sample uses full snapshot enumeration rather than incremental File Provider change
  tokens
- unsupported `symlink` and `mount` entries are omitted from Finder and logged as bridge warnings

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

The sample does not auto-import or auto-sync when Finder opens the domain.
