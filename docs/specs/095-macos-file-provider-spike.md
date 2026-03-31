# Spec 095: macOS File Provider bridge spike

## Purpose

The first File Provider slice proves that the current client truth model can back a Finder-visible,
iCloud-like macOS domain without inventing a second sync model.

This slice is intentionally narrow:

- read-only
- DB-backed only
- one account/root domain
- namespaces exposed as top-level directories
- native app and extension shell live in repo as a developer sample

## Bridge rule

The in-repo File Provider surface is a Rust bridge crate.

Rules:

- `loon-macos` projects visible provider items from the existing client SQLite state
- `loon-macos` reuses the current `OpsConfig` object-store/client config rather than defining a
  second in-repo config format
- the bridge must not invent a second durable local truth model
- the bridge must not auto-import authoritative state or auto-sync local state in this slice
- the bridge may call targeted client hydration helpers when Finder asks for local bytes

Why this rule exists:
the File Provider layer should be a platform adapter over the current client semantics, not a new
sync engine.

Failure modes prevented:

- Finder integration drifting from the mirror client's authoritative and local state semantics
- native shell code becoming the owner of item identity or hydration logic
- a read-only spike quietly widening into implicit refresh or sync behavior

## Domain shape rule

The first domain shape is one account/root that lists configured namespaces as top-level
directories.

Rules:

- the synthetic provider root lists only the namespaces explicitly allowed by configuration
- namespace display names are the namespace ids in this spike
- each namespace root is synthetic and enumerates that namespace's visible tree
- there is no in-repo account/profile abstraction in this slice

Why this rule exists:
the spike should prove cross-namespace Finder projection without adding account/profile product
surface yet.

Failure modes prevented:

- hard-coding one demo namespace and then needing to redesign item identity immediately
- introducing account/product configuration into the main repo before the bridge proves useful

## Item identity rule

Provider item ids must reuse the current client identity model.

Rules:

- root id is synthetic
- namespace-root ids are synthetic and keyed by `namespace_id`
- bound items use `(namespace_id, inode_id)`
- local-only items use `(namespace_id, client_file_id)`
- remote-only placeholders use canonical bound inode identity, not path-only identity
- provider ids are opaque to the out-of-tree native sample

Why this rule exists:
the bridge should preserve canonical inode identity and the existing temporary local-only identity
bridge.

Failure modes prevented:

- path-based provider ids changing after rename
- placeholder identity drifting from the mirror client's canonical inode ids

## Enumeration rule

Enumeration is snapshot-based and DB-backed only in this slice.

Rules:

- the bridge loads `ClientNamespaceStateSummary`
- the bridge loads local-only parent links
- the bridge builds `NamespacePathIndex`
- the bridge projects visible items in deterministic order from that snapshot
- the bridge does not define incremental File Provider change tokens in this slice
- deleted or tombstoned items are omitted
- unsupported `symlink` and `mount` items are omitted and returned as structured warnings

Projection order:

- bound/local items
- local-only items
- remote-only placeholders only when no visible local materialization exists for that identity

Why this rule exists:
the first spike should prove that the current durable state is enough to describe a Finder tree.

Failure modes prevented:

- enumeration code inferring visibility from mutable on-disk state instead of SQLite truth
- unsupported inode kinds silently masquerading as files or directories

## Materialization rule

Materialization is targeted and explicit.

Rules:

- the bridge must not use the global next-action scheduler
- a bound item already present on disk returns its existing local path
- a local-only item already present on disk returns its existing local path
- a remote-only directory reuses the existing directory materialization path into `mirror_root`
- a remote-only file uses a targeted file hydration path into `mirror_root`
- if a remote-only file depends on placeholder ancestor directories, the helper materializes those
  ancestors first in authoritative order
- if the item or a required ancestor has unresolved conflicting work, unresolved issues, or
  non-materialization planned work that makes hydration unsafe, the helper fails closed with a
  typed unavailable/busy error

Why this rule exists:
Finder-triggered hydration should be narrow and deterministic instead of becoming a hidden sync
loop.

Failure modes prevented:

- File Provider reads accidentally consuming unrelated queued client work
- materializing a file into a path whose parent hierarchy is still not safely usable
- bypassing conflict or waiting state because the bridge ran the generic scheduler

## Native sample rule

The first runnable Finder shell now lives in repo as a developer sample.

Rules:

- the containing app owns File Provider domain registration and reset
- the extension owns enumeration, item lookup, and targeted hydration
- the sample lives under `native/macos/LoonFileProviderSample/`
- the sample owns its own app bundle, extension bundle, plist, entitlements, and build files
- Swift must call `loon-macos` through a small C ABI rather than re-implementing provider logic
- C ABI payloads are UTF-8 JSON envelopes
- the sample points at an existing `OpsConfig` plus a namespace allowlist
- the sample calls the Rust bridge for:
  - root listing
  - item lookup
  - child listing
  - targeted materialization
- File Provider item identifiers in the sample are opaque bridge ids, not path-derived ids
- the sample does not support create/modify/delete/rename in this slice
- the sample does not auto-refresh authoritative state

Why this rule exists:
the repo now has enough bridge stability to keep the developer sample versioned next to the Rust
interop layer while still keeping native packaging clearly separated from product code.

Failure modes prevented:

- committing a Finder shell that re-implements client truth model decisions
- shipping native sample logic that drifts away from the Rust bridge as the crate evolves
- path-derived Finder item ids drifting after rename or local-only replacement
- app/extension code disagreeing about domain registration ownership or sample config shape

## Native interop rule

The first native callable surface is a C ABI over the Rust bridge.

Rules:

- `loon-macos` exports a small static-library C ABI:
  - `open`
  - `close`
  - `list_root`
  - `lookup_item`
  - `list_children`
  - `materialize_item`
  - `string_free`
- `open` accepts JSON containing:
  - `ops_config_path`
  - `exposed_namespaces`
- list/lookup/materialize calls return JSON envelopes containing either:
  - a success payload
  - a typed error code and message
- the native shell treats provider item ids as opaque encoded strings and passes them back to the
  bridge unchanged
- the bridge must not require callbacks, background threads, or an async runtime in this slice
- the first in-repo sample uses a checked-in C header rather than a generated binding step
- repo-safe native tests should live in a Swift package inside the sample directory, while Finder
  packaging remains in the Xcode project

Why this rule exists:
the first native sample needs a stable interop boundary that is easy to call from Swift without
copying bridge logic into native code.

Failure modes prevented:

- introducing a second typed native object model that drifts from the Rust bridge
- coupling the first sample to direct Rust enum layouts or path-derived identifiers
- turning the bridge into a callback-driven runtime before the Finder projection contract is proven
