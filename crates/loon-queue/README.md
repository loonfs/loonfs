# loon-queue

Sharded, object-storage-backed background-work coordination.

This crate manages durable job queues for background work that must survive process restarts and
be distributed across workers. Queue state is stored as JSON objects in the object store, using
compare-and-swap for consistency.

`#![forbid(unsafe_code)]`

## Work classes

- **BuildSnapshot** — checkpoint/snapshot construction triggered by WAL growth

## Key modules

| Module | Purpose |
|--------|---------|
| `broker` | Broker lease acquisition and epoch management |
| `durable` | Durable shard state persistence and compare-and-swap updates |
| `repair` | Queue repair: enqueue missing work, promote stale follow-ups |
| `types` | Queue data types (shards, jobs, claims, payloads) |
| `worker` | Worker claim/complete/heartbeat lifecycle |
