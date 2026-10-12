# LoonFS specification

LoonFS is a filesystem built on object storage. Each [namespace](glossary.md#namespace) has a directory tree, a revision history for each file, and an ordered log of metadata changes. A file keeps its inode identity when it's renamed or moved. A [fork](glossary.md#fork) starts from an existing namespace's state and shares that namespace's stored objects, but its later history is separate.

An uploaded file is written in two steps. First the bytes are stored and verified, and then the metadata that references them is committed. A small file's bytes can instead be included inline in the commit. Successfully writing the next numbered write-ahead log (WAL) object commits every record in it. [File contents and ownership](format.md#15-file-contents-and-ownership) defines when file content becomes durable. To read a namespace, a reader finds the current [manifest](glossary.md#manifest) and the [WAL tip](glossary.md#wal-tip), and then combines the materialized metadata with later commits.

All the state needed for recovery is stored in object storage, including control records, retained metadata, and file content. Some of the objects a fork depends on can be stored under an ancestor's prefix. Local caches and derived search indexes can be rebuilt, but the required manifests, metadata segments, and pins must be kept according to the format rules.

## Reading guide

This section lists the documents in the specification and suggests an order for reading them.

| Document | What it covers |
| --- | --- |
| [Glossary](glossary.md) | The terms used throughout the specification. |
| [Architecture](architecture.md) | How reads, writes, forks, and maintenance fit together. |
| [Storage format](format.md) | Required object keys, stored records, encodings, publication protocols, and collection rules. |
| [API](api.md) | API groups, capability discovery, errors, operations, and the HTTP binding. |
| [Object-storage providers](object-storage-providers.md) | Reference information about provider constraints and supported transfer behavior. |
| [OpenAPI](openapi.json) | Generated schemas for the v0 HTTP API. |
| [Browser-proxy OpenAPI](openapi-proxy.json) | Generated schemas for clients that address namespaces by alias. |

The storage format is mandatory for a conforming implementation. API requirements apply to the API groups and features that an implementation exposes. The glossary and the architecture overview explain the model, and the format and API documents define its requirements.

For a first read, start with the architecture overview, and then read the format's sections from [The storage model](format.md#1-the-storage-model) through [Publishing a commit](format.md#6-publishing-a-commit). The sections from [Materializing metadata](format.md#7-materializing-metadata) through [Encodings, versions, and extensions](format.md#12-encodings-versions-and-extensions) describe maintenance and lifecycle protocols. The appendices contain the exact fields, row keys, byte encodings, fingerprints, and timing relationships needed to implement the format.

## Scope

This section describes what the specification defines and what it leaves to each implementation.

The format specifies the behavior that another implementation must preserve to read, write, or garbage collect the same store safely. The API specifies the operations that clients can call. Process boundaries, queues, cache policies, schedulers, and client-side databases are implementation choices.

An implementation can expose direct filesystem commands, an embedded runtime, or an interface built on uploads, commits, and the change feed. All of them use the same namespace identities, commit boundary, and retention rules. Implementations that share the storage format can still have different runtime architectures.
