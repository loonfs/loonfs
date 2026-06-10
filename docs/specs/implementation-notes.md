# LoonFS Implementation Notes

This document is **non-normative**. It collects guidance and worked patterns
for implementing LoonFS engines and servers. Nothing here is required for
conformance; the normative requirements live in `format.md` (mandatory) and
`api.md` (normative where implemented).

## 1. Scheduling maintenance

The format defines maintenance *effects* (`format.md`, "Maintenance
operations"); how work gets triggered is implementation freedom. Conforming
shapes include:

- **Manual triggers.** An embedded engine can expose maintenance as explicit
  calls (create a checkpoint, advance the retention floor, run a maintenance
  tick) that an operator or wrapping application invokes. The reference
  embedded runtime takes this shape: a maintenance tick checks the visible
  WAL tail length and publishes a checkpoint when it crosses a threshold.
- **Automatic scheduling.** A server can run the same work invisibly — on a
  timer, after N commits, or driven by its own job infrastructure — and hide
  the maintenance plane entirely.
- **Hybrid.** A server may both schedule maintenance and expose `admin/v0`
  triggers for operators.

An embedded engine is never required to ship a queue, scheduler, or worker
topology.

## 2. A work queue in object storage

A server that wants distributed background work can keep a work queue in the
same object store, using the contract's compare-and-swap and create-if-absent
primitives. One workable sketch:

- Shard the queue across small mutable objects, for example
  `queue/shards/{shard_index}.json` with a ten-digit zero-padded index so
  shard keys sort in index order.
- Each shard is a compare-and-swap-guarded list of pending work items;
  workers claim items by rewriting the shard, and lease-style fencing bounds
  stalled workers.
- Work items reference namespaces and work classes; completed work records
  its progress through the namespace's derived-progress objects so the queue
  itself never becomes authority.

This is one valid design, not a standard. Queue objects are server-private
state: they live outside the format's durable object families, they are not
interoperable, and another implementation is free to ignore them or schedule
work completely differently. Keep private control objects outside the
`namespaces/{namespace_id}/` families reserved by the format, or under a
clearly private root, so format tooling never confuses them with
interoperable state.

## 3. Caching

Correctness never requires a cache: every read can be answered from durable
objects alone. Caches are throughput and latency tools, and the one rule that
keeps them safe is that **no cache may serve state that has not been
revalidated against durable identity**.

Patterns from the reference runtime:

- **Verified-basis cache.** Cache the reconstructed metadata state per
  namespace, keyed by the head ETag it was built from; revalidate with a
  cheap head probe before reuse and rebuild on mismatch.
- **Control-object cache.** Cache small control objects together with the
  object identity (ETag) they were read at, and pair every use with an
  identity check.
- **Metadata table cache.** Cache decoded SST blocks keyed by the file's
  payload checksum; immutability makes invalidation unnecessary.

## 4. Batching writes

Per-namespace commit batching amortizes head compare-and-swap round trips:
collect concurrent commit requests, validate them in order against ephemeral
state, publish one WAL segment, and advance the head once. The format
explicitly supports this (one head update may publish multiple logical
commits) as long as per-commit idempotency, ordering, and change-feed
identity are preserved.

A coalescing delay (the reference server uses on the order of 100
milliseconds) trades a small latency floor for larger batches and fewer
object-store writes. Rejected requests fail individually; batching never
turns into an all-or-nothing transaction.

## 5. Multi-tenancy

- **Key-prefix scoping.** The object-store layer supports a configured key
  prefix; a server can give each tenant a disjoint prefix and rely on the
  contract's deterministic key scoping.
- **Capability masking.** A server may disable features per tenant — most
  commonly `core.namespaces.list` — and deny the corresponding requests with
  `permission_denied`. The capability document is per-connection truth, so
  different tenants may see different documents from the same server.
- **Admin-plane hiding.** A hosted deployment that runs maintenance
  automatically simply omits `admin/v0` from its advertised profiles.

## 6. Control-object hygiene

Expired upload sessions, leases, and similar control objects accumulate;
cleaning them up is control-plane maintenance with no namespace-history
effect. Cleanup must respect the format's conservative reclamation rules: an
object may be removed only when nothing — visible metadata, retained history,
checkpoints, pins, or in-flight sessions — still depends on it.
