# Read Snapshots

A read snapshot is a time-bounded, read-only lease on one namespace
state. An application creates one, reads the namespace as it stood at
that moment for as long as the lease lasts, and lets it expire. The
feature exists for work that needs a stable input: indexing, export,
training runs, and reproducible builds. This document describes the
concept, the API surface, the durable substrate it shares with
checkpoints, and the order the work should land in.

## One substrate, three surfaces

LoonFS already holds points in time two ways. An operator checkpoint
pins namespace material against retention and garbage collection, lives
in the admin plane, and may live indefinitely. A fork materializes the
current head as a new writable namespace, and protects its basis with a
checkpoint record it owns. Snapshots become the third surface over the
same substrate:

```
checkpoint  pins    (admin plane, operator, long-lived or indefinite)
snapshot    reads   (core plane, application, always time-bounded)
fork        writes  (core plane, application, new namespace at head)
```

All three are checkpoint records underneath. The record already carries
an owner: fork-owned records exist today, refuse manual release, and
are accounted separately by garbage collection. A snapshot is a
checkpoint record with a third owner value, a required expiry, and a
read surface. Checkpoint creation already accepts `ttl_ms`, so the
expiry machinery exists; snapshots make it mandatory.

This is a durable-format change: one new owner value. Pre-release
format rules apply as usual. A loader that does not know the value
rejects the record at load, and `format.md` gains the value in the
checkpoint record section. Nothing else in the record changes.

## What a snapshot promises

Creating a snapshot captures the namespace head sequence and the
metadata basis that serves it. Every read through the snapshot answers
the namespace exactly as it stood at that sequence: path resolution,
directory listings, inode resolution, and content selection all agree,
no matter what commits publish afterward. Content references are
immutable, and the underlying checkpoint record holds garbage
collection away from the captured basis and the content it references
while the snapshot lives.

The read path is `FsReadSnapshot`, which landed with the grep query
pinning work: a pinned metadata view whose reads share one head. Today
it is constructed only at the current head and lives for one request.
Snapshots add the second constructor: build the same view from a
snapshot record's captured basis, on any later request, until the
lease ends.

A snapshot is never writable, and nothing writable is reachable from
it. Restoring or promoting a snapshot is not part of this feature. The
writable materialization of a point in time remains the fork.

## Expiry is the safety property

A pin blocks cleanup. An application that can create indefinite pins
can, by accident or neglect, prevent a namespace from ever collecting
garbage. Snapshots are safe to hand to application service accounts
precisely because they cannot do that:

- `ttl_ms` is required at creation. The server rejects values above
  its configured maximum.
- A snapshot can be extended, but only up to a configured maximum
  total lifetime measured from creation. A job that outruns its lease
  extends it; nothing extends forever.
- Each namespace has a configured cap on live snapshots. Creation past
  the cap fails until one is released or expires.
- Expiry is enforced on the read path and reclaimed by garbage
  collection. A read through an expired or released snapshot fails
  with a terminal error. It never falls back to the current head,
  because silently answering from a different state than the caller
  pinned is the exact failure this feature exists to prevent.

Extension exists because the alternative breaks the core use case: a
fixed lease that expires mid-export cannot be replaced, since a new
snapshot captures a different sequence, and the export loses its
reproducibility. Extension within a hard cap keeps long jobs honest
and keeps the damage bounded.

The retention floor is unchanged by snapshots. The floor may advance
past a snapshot's sequence; the snapshot still serves its captured
state, because the record pins the material. Only the change feed
below the floor becomes unanswerable, which the feed already reports
honestly.

## API surface

Snapshots live in the core plane behind one capability, so a
deployment can front them with application-scoped credentials without
exposing the admin plane. LoonFS keeps its single-token model;
delegation is the deployment's concern, and the plane split is what
makes it possible.

| Operation | Route | Notes |
| --- | --- | --- |
| Create a snapshot | `POST /v0/namespaces/{ns}/snapshots` | Requires `ttl_ms`; accepts `name`. Not idempotent, one attempt, like checkpoint create. |
| List snapshots | `GET /v0/namespaces/{ns}/snapshots?limit=&cursor=` | Live records with id, name, `head_seq`, `created_at_ms`, `expires_at_ms`. |
| Extend a snapshot | `POST /v0/namespaces/{ns}/snapshots/{id}/extend` | Sets the remaining lease, clamped by the lifetime cap. Idempotent by outcome. |
| Release a snapshot | `POST /v0/namespaces/{ns}/snapshots/{id}/release` | Idempotent and one-way, like checkpoint release. |

Reads take the snapshot as an optional query parameter on the routes
that already exist, rather than as a parallel read tree:

- `stat` and directory listing accept `snapshot_id` and answer the
  captured state. Page cursors minted under a snapshot bind to it and
  resume against the same immutable view. This is pinned-snapshot
  pagination: the cursor caveat that pages may see different heads
  simply does not apply under a snapshot.
- Content reads and download grants accept `snapshot_id` and serve the
  revision the snapshot selects.
- The change feed accepts `snapshot_id` and truncates at the captured
  sequence, exactly as grep query pinning truncates its tail today.
  An indexer can walk changes to a stable boundary and know it saw
  everything up to it and nothing past it.
- Inode-addressed reads follow the same pattern as a fast follow once
  the path routes prove the shape.

Grep at a snapshot is deliberately out. The content index follows the
head; serving a query at an arbitrary pinned sequence would need index
time travel, which is its own design. The deferred list records it.

Error codes follow the registry's absence-versus-terminal split: an
unknown id answers `snapshot_not_found` (404); an expired or released
one answers `snapshot_gone` (410), with the message naming which. A
creation past the namespace cap answers `snapshot_quota_exceeded`
(409), resolvable by releasing or waiting out a lease.

## Configuration

Three server settings, all with modest defaults a deployment can
raise: the maximum `ttl_ms` a creation may request, the maximum total
lifetime an extension chain may reach, and the per-namespace cap on
live snapshots. Garbage collection accounting extends the existing
released-checkpoint counts with a snapshot bucket, and diagnostics
list live snapshots alongside checkpoints.

## Implementation order

1. Core read-at-basis: construct `FsReadSnapshot` from a checkpoint
   record's captured basis instead of the current head. Embedded API
   and tests only; no wire change.
2. Durable lifecycle: the snapshot owner value, required expiry for
   that owner, garbage collection accounting, and the `format.md`
   addition.
3. Wire lifecycle: the four routes, the capability, the error codes,
   and the `api.md` sections.
4. Snapshot reads: `snapshot_id` on stat, listing, content, download,
   and the change feed; cursor binding; tri-language conformance
   cases.
5. Polish: configuration, metrics, diagnostics, and documentation.

Steps 1 and 2 are independent of each other. Everything after builds
on both.

## Deferred

- Fork from a snapshot: `fork_namespace` accepting a snapshot id, so
  any pinned state can become writable. The natural next step, and the
  reason the substrate is shared.
- Grep at a snapshot, pending an index time-travel design.
- Restore or promotion of a snapshot over its source namespace. This
  remains on the product list, unbuilt.
- Snapshot-scoped credentials. Authorization stays with the
  deployment.
