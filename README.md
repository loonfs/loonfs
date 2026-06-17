![LoonFS Logo](assets/loonfs-wordmark-black.svg)

LoonFS is a durable filesystem built on object storage. It can be used to store, manage, index, and retrieve files and folders for a variety of use cases. Object storage is the only durable dependency from which LoonFS derives virtually unlimited storage and a high throughput ceiling. It is designed for use cases with many writers and readers, and can be used across sessions, agents, and teams as an embedded engine or through a remote server connection.

## Download

You can use the [install script](https://github.com/loonfs/loonfs/blob/main/scripts/install-loon.sh) by running
```bash
curl -fsSL https://install.loonfs.com | sh
```

If you use Homebrew as your package manager, you can also install it by running
```bash
brew install loonfs/tap/loon
```

Or compile directly from source by checking out this repository and running
```bash
cargo build -p loonfs-cli                     # compile from source
cp ./target/debug/loon ~/.local/bin/loon    # copy it to somewhere in your $PATH
```

## Quickstart

This example uses S3 in embedded mode, where the CLI talks directly to the bucket without a LoonFS server:

```bash
loon init default --no-input \
  --mode embedded \
  --store-kind aws-s3 \
  --bucket {bucket_name} \
  --region {aws_region} \
  --access-key-id {access_key_id} \
  --secret-access-key {secret_access_key}
loon namespace create {namespace_id}
loon use {namespace_id}
```

## Documentation

Visit loonfs.com/docs to learn more.


## Core concepts

- **Object storage is the only durable substrate.** LoonFS stores durable truth in object storage: immutable file content, immutable metadata history, materialized manifests/checkpoints, and a small number of mutable control objects. Caches, queues, workers, and local state are safely rebuildable from the object store.

- **Namespaces are independent filesystem histories.** A namespace is the unit of visibility, metadata history, recovery, retention, and forking. 

- **Inodes are identity, paths are views.** The identity of a filesystem item is `(namespace_id, inode_id)`. Paths are "views" derived from directory bindings and may change over time without changing the item’s identity.

- **Commits are the unit of transactional change.** File bytes are written to object storage before metadata can reference them. Metadata changes are recorded as logical commits, and a commit becomes visible only when the namespace head durably advances to include it.

- **Materialization and background work are derived, not authoritative.** Manifests, checkpoints, indexes, compaction, retention advancement, and garbage collection make the system faster, cheaper, or easier to recover, but they do not create a second source of truth.

## Design philosophy

LoonFS is built around a correctness-first protocol where the object store is the only source of truth.

- **Correctness is the primary feature.** LoonFS favors designs with fewer valid states, explicit invariants, named failure modes, and deterministic tests.

- **Durability and visibility are separate.** A write is not successful merely because bytes or WAL records exist. LoonFS distinguishes durable content, durable metadata, and visible committed state, with the namespace head serving as the visibility boundary.

- **Serialize commits, scale the rest.** Every change runs through one transactional core with an ordered WAL. LoonFS keeps expensive work off the write path so it can scale independently: moving content bytes, publishing metadata to readers, background maintenance, and rebuilding derived indexes.
