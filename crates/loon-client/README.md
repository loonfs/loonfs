# loon-client

Client-side daemon scaffolding for mirror sync, hydration, and local durability.

This crate implements the client half of the LoonDB sync protocol: observing remote namespace
changes, planning uploads and downloads, executing content transfers, and managing conflicts. Local
durable state is maintained in a SQLite database.

`#![forbid(unsafe_code)]`

## Architecture

The client runs a planner/executor loop:

1. **Observe** — detect remote namespace changes and local filesystem events
2. **Plan** — decide what needs to upload, download, or reconcile
3. **Execute** — transfer content blocks, materialize files, and apply metadata

## Key modules

| Module | Purpose |
|--------|---------|
| `state_db` | SQLite-backed local state (namespace tracking, sync progress, file registry) |
| `planner` | Action selection: what to upload, download, or reconcile next |
| `executor` | Content transfer execution and local state updates |
| `download` | Content block and manifest downloading |
| `upload` | Content block splitting, hashing, and uploading |
| `local_fs` | Filesystem event observation and path normalization |
| `provider` | Materialization of remote files into local mirror roots |
| `conflict` | Conflict artifact creation, archival, and restoration |
| `testing` | `FaultController` for deterministic fault injection in tests |

## Conflict resolution

Six conflict classes are handled: stale edits, path collisions, delete-vs-edit, rename-vs-edit,
subtree conflicts, and local-only binding ambiguity. Conflicts produce durable artifacts that can be
inspected, restored, or archived.
