# LoonFS Core Specification

## 1. What LoonFS is

LoonFS is a filesystem built on top of object storage.

The durable state consists of:

- immutable content blocks
- immutable content manifests
- immutable metadata commits recorded in a write-ahead log (WAL)
- immutable checkpoints
- small mutable control objects such as the namespace head and leases

All other state is cache, coordination, or other rebuildable state.

LoonFS can be exposed as:

- a direct filesystem service with commands such as `ls`, `get`, `put`, `mv`, and `cp`
- an advanced writer surface for staged upload, explicit commit, and incremental change consumption
- a foundation for sync clients, batch writers, and operator tooling

This spec standardizes the durable model and the rules for interoperable implementations. It does not standardize local client databases, queue layouts, scheduler loops, or other implementation-specific mechanics.

## 2. Design goals

The design goals are:

| Goal | Meaning |
| --- | --- |
| **Simple** | The durable model should fit in a small number of concepts: namespaces, inodes, revisions, content manifests, logical commits, WAL segments, and checkpoints. |
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
| Content publication | File content becomes visible only after its blocks and content manifest are already durable. |
| Commit visibility | A logical commit becomes visible only when the namespace head advances successfully to a `seq` at or beyond that commit. |
| Delete | Delete is logical first and creates tombstones. Physical reclamation is background garbage collection. |
| Recovery | Readers reconstruct state from a verified checkpoint plus the visible WAL segment chain after that checkpoint. |
| Writes | A path-oriented filesystem API and an explicit upload/commit/change-feed API are both part of the core model. |
| Access control | ACLs and shares are a separate control plane keyed by namespace or subtree identity, not by path text. |
| Long-running operations | Resumable uploads may use control-plane objects. |

## 4. System sketch

```text
            user-facing filesystem commands
      ls / stat / get / put / mkdir / mv / cp / rm
                         |
                         v
                authoritative LoonFS service
          path resolution, validation, commit, reads
             /                    |                 \
            /                     |                  \
           v                      v                   v
  object-storage content   metadata history      control objects,
  blocks and manifests     WAL segments, head,    shares, leases
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

1. the glossary
2. the architecture overview
3. the object store contract
4. the filesystem and storage model
5. the mutation and visibility model
6. the interfaces and clients section
