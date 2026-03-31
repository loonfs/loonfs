# Spec 095: macOS File Provider bridge spike

## Purpose

The first File Provider slice proves that the current client truth model can back a Finder-visible,
iCloud-like macOS domain without inventing a second sync model.

This slice is intentionally narrow:

- read-only
- DB-backed only
- one account/root domain
- namespaces exposed as top-level directories
- native app and extension shell remain out of tree

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

The first runnable Finder shell stays out of tree.

Rules:

- the out-of-tree sample owns its own app bundle, extension bundle, plist, entitlements, and build
  files
- the sample points at an existing `OpsConfig` plus a namespace allowlist
- the sample calls the Rust bridge for:
  - root listing
  - item lookup
  - child listing
  - targeted materialization
- the sample does not support create/modify/delete/rename in this slice
- the sample does not auto-refresh authoritative state

Why this rule exists:
the repo should first prove the semantic bridge before it accumulates native shell packaging.

Failure modes prevented:

- committing a Finder shell that re-implements client truth model decisions
- widening the main repo's delivery surface before the Rust bridge contract is proven
