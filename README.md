<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/loonfs-wordmark-black.svg">
    <source media="(prefers-color-scheme: dark)" srcset="assets/loonfs-wordmark-white.svg">
    <img alt="LoonFS logo" src="assets/loonfs-wordmark-black.svg" height="100">
  </picture>
</div>
<br>

LoonFS is a durable filesystem built on object storage. It can be used to store, manage, index, and retrieve files and folders for a variety of use cases. Object storage is the only durable dependency from which LoonFS derives virtually unlimited storage and a high throughput ceiling. It uses a single-writer, multi-reader model and can be used across sessions and clients as an embedded engine or through a remote server connection.

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

## Running a server

Embedded mode gives one process at a time direct object-store access. Concurrent `loon` invocations against one namespace contend for the writer role: whichever acquires it last wins, and a command that loses fails with `writer_fenced`, naming both sessions. A fenced command commits nothing, so rerunning it is always safe — but fencing is a stop signal, not a retry loop, so put bulk work in one process or run the server for concurrent writers. To share a deployment across machines and let many clients write concurrently, run the reference server and point remote profiles at it:

```bash
cargo build -p loonfs-server
./target/debug/loonfs-server --config configs/loonfs-server.local-fs.example.toml
```

Commented example configs for every supported store live in [configs/](configs) (`local-fs`, `aws-s3`, `gcp-gcs`, `cloudflare-r2`, `azure-abs`). The required fields are `bind`, `writer_id`, and a `[store]` block; everything else defaults. The optional `[grep]` table selects `mode = "disabled" | "embedded" | "serve_only"` (default `embedded`) and the bounded per-step build/fold policy shown in the local-fs example. Embedded grep maintenance is event-driven per namespace and has no recurring timer or store enumeration. Connect a client:

```bash
export LOONFS_AUTH_TOKEN={auth_token}
loon init default --no-input --mode remote --server-url http://127.0.0.1:9400
```

## Documentation

Visit loonfs.com/docs to learn more.


## Core concepts

LoonFS is designed with a core set of foundational ideas.

- **Object storage is the only required durable substrate.** LoonFS stores durable truth in object storage: file content, immutable metadata history, materialized manifests/checkpoints, and a small number of mutable control objects. Caches, queues, workers, and local state are safely rebuildable from the object store.

- **A namespace is a self-contained filesystem.** Each namespace has it's own contents and history, and is managed independently of every other namespace.

- **Inodes are identity, paths are views.** The identity of a filesystem item is `(namespace_id, inode_id)`. Paths are "views" that point to inodes, and may change over time without changing the item’s identity.

- **Commits are the unit of transactional change.** File bytes are written to object storage before metadata can reference them. Metadata changes are recorded as logical commits, and a commit becomes visible only when the namespace head durably records it.

- **Materialization and background work are derived, not authoritative.** Manifests, checkpoints, indexes, compaction, retention advancement, and garbage collection make the system faster, cheaper, or easier to recover, but they should not create a second source of truth.

## Design philosophy

LoonFS is built around a correctness-first protocol where the object store is the only source of truth.

- **Correctness is the primary feature.** LoonFS favors designs with fewer valid states, explicit invariants, named failure modes, and deterministic tests. 

- **Durability and visibility are separate.** LoonFS may durably store file content and metadata before a change appears in the filesystem. Changes are acknowledged only once the namespace head advances to include its commit.

- **Serialize commits, scale everything else.** Every change is executed through a transactional core with an ordered WAL. LoonFS keeps expensive work (uploading and downloading, compaction, garbage collection, indexing) off the write path so it can scale independently.
