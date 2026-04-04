# Architecture

## Current shape

The rewrite branch has one authoritative path:

1. `loon-cli` parses user commands.
2. `loon-client` sends HTTP requests.
3. `loon-server` authenticates, translates path-oriented requests, and calls the core.
4. `loon-core` acquires the lease, replays durable state, validates mutations, writes WAL, and
   publishes `head.json`.
5. `loon-objectstore` is the only place that understands provider mechanics.

## Durable objects

The rewrite keeps the same durable families described in the locked specs:

- `namespaces/{namespace}/head.json`
- `namespaces/{namespace}/lease.json`
- `namespaces/{namespace}/wal/...`
- `namespaces/{namespace}/blobs/...`
- `namespaces/{namespace}/manifests/...`

Checkpoints and queue/progress objects stay deferred in implementation, but their spec-defined
shape remains reserved.

## Crate boundaries

- `loon-api` owns ids, envelopes, WAL/control/content codecs, and HTTP DTOs.
- `loon-objectstore` owns the storage trait, key validation, provider adapters, and contract
  surface.
- `loon-core` owns metadata semantics, replay, path resolution, lease handling, content durability,
  and mutation publication.
- `loon-model` provides a pure metadata/WAL reference surface for differential testing.
- `loon-server` is a stateless HTTP adapter.
- `loon-client` is transport-only.
- `loon-cli` owns terminal UX only.

## Scope cuts

Not in scope for the current branch phase:

- SQLite client truth
- watcher/sync planner/executor behavior
- background queue/checkpoint builders
- native/macOS surfaces
- daemon orchestration beyond one stateless HTTP server

## Governance

`docs/specs/*` is immutable here. Any proposed change to the spec set belongs in `proposals/*`.
