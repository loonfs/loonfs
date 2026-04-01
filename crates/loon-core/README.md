# loon-core

Canonical metadata rules, commit planning, path resolution, and invariants for the LoonDB protocol.

This crate encodes the authoritative rules for how namespace state changes — what preconditions must
hold, how inode IDs are allocated, how WAL entries are serialized, and how checkpoints are built and
verified.

`#![forbid(unsafe_code)]`

## What this crate owns

- **Metadata state machine** — the four append-only record types (inodes, direntries, revisions,
  tombstones) and their visibility rules
- **Commit planning** — precondition validation, inode ID allocation, WAL entry construction
- **WAL serialization** — CBOR + Zstd encoding/decoding of commit payloads
- **Checkpoint logic** — snapshot building, segment serialization, and checkpoint verification
- **Path resolution** — derived path computation from inode-keyed metadata
- **Named invariants** — explicit, named invariants checked at each protocol step
- **Progress tracking** — background work high-water mark management

## What this crate does not own

- I/O or storage operations (see `loon-objectstore`)
- HTTP, RPC, or transport concerns (see `loon-server`)
- Client-side state or sync logic (see `loon-client`)

## Public modules

| Module | Purpose |
|--------|---------|
| `checkpoint` | Checkpoint building, loading, and verification |
| `commit` | Commit plan construction and validation |
| `content` | Content manifest handling and block management |
| `invariants` | Named invariant definitions and checking |
| `metadata` | Metadata state construction and record visibility |
| `namespace` | Namespace-level state loading (head + checkpoint + WAL replay) |
| `path` | Derived path resolution from inode-keyed records |
| `progress` | Background work progress tracking |
| `wal` | WAL entry serialization, deserialization, and replay |
