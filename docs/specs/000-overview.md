# LoonFS overview

LoonFS is a filesystem and sync engine whose only durable dependency is object storage.

Its durable system of record is intentionally small:

- immutable file-content blocks
- immutable content manifests
- immutable namespace write-ahead log (WAL) entries
- immutable checkpoint objects
- small mutable control objects such as the namespace head, leases, and progress markers

Everything else is cache, coordination, or a rebuildable acceleration structure.

## Design goals

| Goal | Why it matters |
| --- | --- |
| Simple durable model | A new reader should be able to explain where truth lives and how it becomes visible. |
| Object-store portability | The system should run anywhere that satisfies a small storage contract. |
| Deterministic recovery | After a crash, readers and writers should reconstruct the same state from durable objects alone. |
| Clear concurrency rules | Races should fail explicitly rather than silently merging or overwriting. |
| Shared semantics across modes | Local server mode, hosted server mode, mirror clients, and later on-demand clients should all use the same core model. |

## The few rules that matter most

| Area | Rule |
| --- | --- |
| Identity | The canonical identity of an item is `(namespace_id, inode_id)`. |
| Naming | Paths are views built from directory bindings. They are not the mutation identity model. |
| Ordering | Each namespace has one total order of visible metadata commits, numbered by `seq`. |
| Visibility | A metadata change becomes visible only when `head.json` advances successfully. |
| Content safety | A file revision is visible only after its referenced content blocks and content manifest are already durable. |
| Replay | Readers reconstruct state from a verified checkpoint plus the WAL tail after that checkpoint. |
| Derived state | Background indices may improve performance, but correctness must not depend on them. |

## Plain-language shape of the system

```text
client uploads content  --->  object store
client sends commit     --->  authoritative mutation service
service writes WAL/head --->  object store
workers build indexes   --->  object store
readers recover state   --->  head + checkpoint + WAL
```

A typical file edit looks like this:

1. upload any missing content blocks
2. upload the content manifest
3. validate the write against the current namespace head
4. write one immutable WAL entry
5. advance the namespace head with compare-and-swap

The file becomes visible only after step 5.

## What this spec does not try to freeze

This core spec does **not** define:

- a repository layout
- milestone-specific feature boundaries
- a particular client database schema
- one internal queue implementation
- one platform integration strategy

Those can evolve without changing the public contract, as long as the durable rules in this spec still hold.

## Reading guide

Start here, then read the docs in this order:

1. `010-glossary`
2. `020-architecture-overview`
3. `030-object-store-contract`
4. `040-filesystem-and-storage-model`
5. `050-mutation-model`
6. `055-http-api-binding`
7. `060-background-work`
8. `070-client-requirements`
9. `080-versioning-and-conformance`
