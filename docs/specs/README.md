# LoonFS Core Specification

## 1. What LoonFS is

LoonFS is a filesystem built on top of object storage.

The durable state consists of:

- immutable content objects referenced by `content_ref`
- immutable metadata commits recorded in a write-ahead log (WAL)
- immutable namespace manifests and checkpoint records
- small mutable control objects such as the namespace head and leases

All other state is cache, coordination, or other rebuildable state.

LoonFS can be exposed as:

- an embedded/direct filesystem runtime with commands such as `ls`, `get`, `put`, `mv`, and `cp`
- an advanced writer surface for staged upload, explicit commit, and incremental change consumption
- a foundation for sync clients, batch writers, and operator tooling

This spec standardizes the durable model and the rules for interoperable implementations. It does not standardize local client databases, queue layouts, scheduler loops, or other implementation-specific mechanics.

The specification is two normative documents plus supporting material, all in this folder:

| Document | Force | Contents |
| --- | --- | --- |
| `format.md` | Normative, mandatory | The durable format: object-store contract, storage model, write/read protocol, encodings and versioning, namespace features, maintenance invariants. |
| `api.md` | Normative where implemented | Profiles (functional planes), capability discovery, the standard error contract, and the representative HTTP binding. |
| `operation-statefulness.md` | Normative where implemented | When an operation is single-request versus control-object-backed, and the client/server responsibility split. |
| `glossary.md` | Orientation | Shared vocabulary for every other document. |
| `architecture.md` | Orientation | How the durable pieces and the runtime fit together. |
| `object-storage-providers.md` | Non-normative reference | Provider limits and performance data points that inform the design. |

Two sorting questions place every future addition: behavior another implementation must interpret from bytes or metadata belongs in the format spec; a client-visible operation whose shape should be uniform belongs in the API spec inside a profile or named feature. How work gets done — queues, schedulers, caches — is implementation freedom and belongs in no specification document.

## 2. Design goals

The design goals are:

| Goal | Meaning |
| --- | --- |
| **Simple** | The durable model should fit in a small number of concepts: namespaces, inodes, revisions, content refs, logical commits, WAL segments, manifests, and checkpoints. |
| **Portable** | The only required durable dependency is object storage with a small set of well-defined guarantees. |
| **Safe** | Writes are never partially visible. Metadata never points to content that is not already durable. |
| **Readable** | A reader should be able to understand the system from a small public spec without reading client architecture or rollout plans. |
| **Extensible** | The core model should support direct filesystem operations, sync engines, and future clients without changing identity or visibility rules. |

## 3. Core decisions

| Topic | Decision |
| --- | --- |
| Durable dependency | Object storage is the only required durable dependency. |
| Unit of history | Each namespace has its own ordered metadata history. |
| Identity | The canonical identity of an item is `(namespace_id, inode_id)`. |
| Names | Paths are lookup views built from directory bindings. They are not the identity model. |
| Content publication | File content becomes visible only after the object named by its `content_ref` is already durable. |
| Commit visibility | A logical commit becomes visible only when the namespace head advances successfully to a `seq` at or beyond that commit. |
| Delete | Delete is logical first and creates tombstones. Physical reclamation is background garbage collection. |
| Recovery | Readers reconstruct state from a verified manifest, pinned by a checkpoint when available, plus the visible WAL segment chain after that boundary. |
| Writes | A path-oriented filesystem API and an explicit upload/commit/change-feed API are both part of the core model. |
| Access control | ACLs and shares are a separate control plane keyed by namespace or subtree identity, not by path text. |
| Long-running operations | Resumable uploads may use control-plane objects. |

## 4. System sketch

```text
            user-facing filesystem commands
      ls / stat / get / put / mkdir / mv / cp / rm
                         |
                         v
                authoritative LoonFS runtime
          path resolution, validation, commit, reads
             /                    |                 \
            /                     |                  \
           v                      v                   v
  object-storage content   metadata history      control objects,
  content ref objects      WAL segments, head,    shares, leases
                           manifests,
                           checkpoints
```

## 5. What this spec leaves to implementations

The following are intentionally outside the core spec:

- local client database schemas
- job schedulers and worker topologies
- queue shard layouts
- platform-specific file-watcher integrations
- exact binary encodings for large immutable metadata objects
- whether recursive workflows use purely client-side orchestration or implementation-specific coordinators
- whether one implementation uses a single process or several services internally

Those choices are implementation-specific and do not change the filesystem model.

## 6. Reading guide

Readers should start with:

1. the glossary (`glossary.md`)
2. the architecture overview (`architecture.md`)
3. the format specification (`format.md`)
4. the API specification (`api.md`)

The operation statefulness matrix and the provider reference support design
work around those two specifications.
