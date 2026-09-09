# LoonFS specification

LoonFS is a filesystem built on object storage. Each namespace has a directory tree, file revision history, and an ordered log of metadata changes. Files retain their inode identities when renamed or moved. Forks start from an existing namespace's state and share its stored objects while maintaining independent subsequent history.

A file write has two steps: store and verify the bytes, then commit the metadata that references them. Creating the next numbered write-ahead log (WAL) object commits its records. Readers discover the current manifest and WAL tip, then combine materialized metadata with later commits.

Object storage contains all required recovery state, including control records, retained metadata, and file content. Some of a fork's dependencies can be stored under an ancestor's prefix. Local caches and derived search indexes can be rebuilt, but required manifests, metadata segments, and checkpoint records must be retained according to the format rules.

## Reading guide

| Document | What it covers |
| --- | --- |
| [Glossary](glossary.md) | The terms used throughout the specification. |
| [Architecture](architecture.md) | How reads, writes, forks, and maintenance fit together. |
| [Storage format](format.md) | Required object keys, stored records, encodings, publication protocols, and collection rules. |
| [API](api.md) | API groups, capability discovery, errors, operations, and the HTTP binding. |
| [Object-storage providers](object-storage-providers.md) | Reference information about provider constraints and supported transfer behavior. |
| [OpenAPI](openapi.json) | Generated schemas for the v0 HTTP API. |
| [Browser-proxy OpenAPI](openapi-proxy.json) | Generated schemas for clients that address namespaces by alias. |

The storage format is mandatory for a conforming implementation. API requirements apply to the groups and features that an implementation exposes. The glossary and architecture overview explain the model; the format and API documents define its requirements.

For a first read, start with the architecture overview and sections 1–6 of the format. Sections 7–11 describe maintenance and lifecycle protocols. The appendices contain the exact fields, row keys, byte encodings, fingerprints, and timing relationships needed to implement the format.

## Scope

The format specifies behavior that another implementation must preserve to read, write, or collect the same store safely. The API specifies operations that clients can call. Process boundaries, queues, cache policies, schedulers, and client-side databases are implementation choices.

Implementations may expose direct filesystem commands, an embedded runtime, or an upload/commit/change-feed interface. All use the same namespace identities, commit boundary, and retention rules. Sharing a storage format does not require sharing a runtime architecture.
