# Background Jobs

## 1. Purpose

Background work reduces read cost, supports safe retention, and cleans up durable state. It does not create a second source of truth for the filesystem.

## 2. Required job classes

### 2.1 Checkpoint build and verification

A checkpoint summarizes namespace metadata at one chosen `seq`.

A checkpoint is useful only after it is verified. Readers must prefer verified checkpoints plus the WAL tail over unverified or partial snapshots.

### 2.2 Retention management

Retention management decides how far back incremental replay is still promised.

A retention floor may advance only when the system has enough verified material to support readers from the new floor forward.

### 2.3 Garbage collection

Delete is tombstone-first. Garbage collection is the separate process that eventually reclaims content or metadata that is no longer reachable and no longer protected by retention policy.

Garbage collection must be conservative. It may reclaim an object only when:

- no visible metadata references it;
- no retained historical metadata still needs it; and
- no active session, upload, or job still depends on it.

### 2.4 Expired control-object cleanup

Implementations may clean up expired sessions, uploads, leases, or jobs. This is control-plane maintenance, not namespace history.

## 3. Optional derived work

Derived structures such as search indexes, caches, or materialized summaries are optional. They may improve performance or higher-level features, but they are not authoritative. They must be rebuildable from authoritative state.
