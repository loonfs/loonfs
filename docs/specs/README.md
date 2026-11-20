# LoonFS specification

LoonFS is a filesystem built on object storage. Each [namespace](glossary.md#namespace) has a directory tree, per-file revision history, and an ordered log of metadata changes. Files keep their inode identity across renames and moves. A [fork](glossary.md#fork) starts from a point in another namespace's history and shares its existing stored objects, while keeping later commits separate.

An uploaded file is written in two steps: bytes are stored and verified first, and then a commit records the metadata referencing them. Small files can also be sent inline inside the commit itself. Successfully writing the next numbered write-ahead log (WAL) object commits every record inside it. [File contents and ownership](format.md#15-file-contents-and-ownership) defines when file content becomes durable. To read a namespace, a reader locates the current [manifest](glossary.md#manifest) and the [WAL tip](glossary.md#wal-tip), then layers uncommitted WAL tail records over the manifest's folded metadata.

Everything required for recovery is stored in object storage, including control records, metadata segments, and file content (a fork may depend on objects stored under an ancestor's prefix). Local caches and derived search indexes can be rebuilt from object storage, provided the required manifests, metadata segments, and snapshots are kept according to the format rules.

## Reading guide

| Document | What it covers |
| --- | --- |
| [Glossary](glossary.md) | Terms used across the specification. |
| [Architecture](architecture.md) | How reads, writes, forks, and maintenance fit together. |
| [Storage format](format.md) | Required object keys, stored records, encodings, publication protocols, and collection rules. |
| [API](api.md) | API groups, capability discovery, error contract, operations, and the HTTP API. |
| [Object-storage providers](object-storage-providers.md) | Provider limits, transfer behavior, bucket configuration, and adapter requirements. |
| [OpenAPI](openapi.json) | Generated schemas for the v0 HTTP API. |

The storage format is mandatory for any conforming implementation. API requirements apply to whichever API groups and features a deployment exposes. The glossary and architecture overview explain the model, while the format, API, and provider documents define normative requirements.

For a first pass, read [Architecture](architecture.md) first, then sections 1 through 6 of [Storage format](format.md) (from [The storage model](format.md#1-the-storage-model) through [Publishing a commit](format.md#6-publishing-a-commit)). Sections 7 through 12 cover folding, snapshots, namespace lifecycle, compaction, garbage collection, and versioning. The appendices specify exact fields, row keys, encodings, fingerprints, and timing bounds.

## Scope

The format specification defines what an implementation must preserve to read, write, or garbage-collect a shared store safely. The API specification defines client-visible operations. Process topology, request queues, cache policies, background schedulers, and client-side state are left to each implementation.

An implementation can expose direct filesystem commands, an embedded library, or an API built around uploads, commits, and the change feed. All of them share the same namespace identities, commit boundary, and retention rules.
