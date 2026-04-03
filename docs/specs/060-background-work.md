# Background work

Background work exists to improve performance and maintain replay guarantees. It must not become part of the correctness path for visible metadata.

## First principle

Background work is coordination, not truth.

If a background queue stalls or a job is lost, readers must still recover correct namespace state from durable canonical objects.

## What background work does

| Job family | Purpose |
| --- | --- |
| **BuildSnapshot** | Create a verified checkpoint so readers can start from a bounded metadata basis. |
| **BuildListingIndex** | Publish rebuildable read accelerations for listings or other hot queries. |
| **RetentionPolicy** | Authorize advancement of the retention floor after replay promises are still safe. |
| **Repair** | Recreate missed queue work by comparing canonical head state with durable progress objects. |

## Verified checkpoints

A checkpoint is usable only after:

1. every referenced segment object exists
2. the checkpoint manifest exists
3. the checkpoint has been verified against those segment objects
4. the namespace head advertises it as the current `snapshot_hint_seq`

A checkpoint is immutable. Publishing a newer checkpoint does not rewrite history; it creates a newer recovery starting point.

## Derived indices and progress

Every derived work class should publish two things:

1. immutable output objects, usually keyed by the `through_seq` they cover
2. one small mutable `progress.json` object that records the highest sequence boundary the output is safe for

A reader may use a derived index only when the matching progress object proves the index covers the reader’s requested boundary. Otherwise the reader must fall back to checkpoint plus WAL replay.

## Queue and worker rules

The core queue rules are small:

- jobs are idempotent
- queue state is durable but non-authoritative
- workers use leases and claim tokens to fence stale owners
- lost enqueues must be repairable from canonical head state and progress objects

The exact shard layout is an implementation detail as long as those rules remain true.

## Retention floor

The retention floor may advance only when all of the following are true:

- a verified checkpoint covers that boundary or later
- every required progress object covers that boundary or later
- the retention-policy gate also covers that boundary or later

This prevents the system from promising incremental replay from a point it can no longer reconstruct safely.

## The important separation

The commit path creates visible metadata.

Background work may later:

- publish faster replay starting points
- publish faster read projections
- allow older WAL history to age out safely

That separation is one of the main reasons the LoonFS spec stays understandable.
