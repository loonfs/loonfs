# Spec 050: background work

## What background work is for

Background work exists to build derived state after canonical metadata has already been committed.

It is for:

- snapshots
- change indices
- listing indices
- revision indices
- verification
- GC planning and sweep

It is not for:

- authoritative namespace commits
- required content uploads for the current request

## Queue design

We use sharded queue objects in object storage.

Each shard owns:

- broker lease state
- a set of queued jobs
- claim / heartbeat / timeout metadata

We do **not** use one global queue file.

Why not:
a single mutable object becomes a write hotspot.

## Job rule

Every job class must be idempotent.

Example:
`BuildSnapshot(namespace=abc, through_seq=420)` may run twice and still converge to the same durable outputs.

Failure mode prevented:
duplicate execution corrupting derived state.

## Lost enqueue rule

The queue is coordination, not truth.

If a post-commit enqueue is lost, repair logic must be able to recreate it by comparing namespace head seq to derived progress objects.

Failure mode prevented:
derived work permanently stalling because one enqueue was dropped.
