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

This example uses S3 in embedded mode, where the CLI talks directly to the bucket without a LoonFS server. Provider credentials are read from the standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` if you use one):

```bash
export AWS_ACCESS_KEY_ID={access_key_id}
export AWS_SECRET_ACCESS_KEY={secret_access_key}
loon init default --no-input \
  --mode embedded \
  --store-kind aws-s3 \
  --bucket {bucket_name} \
  --region {aws_region}
loon namespace create {namespace_id}
loon use {namespace_id}
```

## Documentation

Visit loonfs.com/docs to learn more.

## Server trust model

The v0 server is a single trust domain:

- **One bearer token grants everything.** A caller holding the server's `auth_token` can read and write every namespace and call every admin endpoint, including namespace deletion, retention advancement, and garbage collection. There are no per-namespace or per-action scopes in v0.
- **Namespaces isolate data, not callers.** Namespace boundaries exist for history, recovery, and forking — they are not a security boundary between clients of one server.
- **Run it accordingly.** Give the token only to clients you would trust with the whole store, and terminate TLS in front of the server (it serves plain HTTP). By default the server refuses to bind a non-loopback address without a token; `allow_unauthenticated_remote = true` overrides that check on purpose.

Multi-user authorization is future work; until it exists this contract is deliberate and documented rather than partially enforced.

## Moving file content

There are two ways to move file bytes through a deployment, and they are sized differently on purpose:

- **Service-proxied transfers are the bounded convenience path.** The server buffers each proxied upload body and each proxied content read fully in memory, capped per request (`max_upload_bytes`, `max_download_bytes`, advertised as `upload.max_content_bytes` and `download.max_content_bytes`) and capped in count (`max_concurrent_uploads`, `max_concurrent_downloads`, answering `server_busy` past the cap). Worst-case transfer memory is the product of the two caps in each direction.
- **`direct_put` is the scalable path for large content.** When the object store supports presigned URLs, the server hands the client a short-lived upload capability and the bytes never pass through the server. Clients discover it through the `core.uploads.direct_put` capability key.


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
