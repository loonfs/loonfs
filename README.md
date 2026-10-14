<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/loonfs-wordmark-black.svg">
    <source media="(prefers-color-scheme: dark)" srcset="assets/loonfs-wordmark-white.svg">
    <img alt="LoonFS logo" src="assets/loonfs-wordmark-black.svg" height="100">
  </picture>
</div>
<br>
<div align="center">
  <a href="LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-green?style=flat-square"></a>
</div>
<br>
<br>

## LoonFS

LoonFS is a durable filesystem built on object storage. It can be used to store, organize, index, search, and read files and directories. Object storage is the only durable dependency for LoonFS, so the capacity and throughput of namespaces is virtually unlimited. It uses a single-writer, multi-reader model, and applications can embed the LoonFS engine directly or connect to a LoonFS server over HTTP.

## Download

You can use the [install script](https://github.com/loonfs/loonfs/blob/main/scripts/install-loonfs.sh) by running
```bash
curl -fsSL https://raw.githubusercontent.com/loonfs/loonfs/main/scripts/install-loonfs.sh | sh
```

If you use Homebrew as your package manager, you can also install it by running
```bash
brew install loonfs/tap/loonfs
```

Or compile directly from source by checking out this repository and running
```bash
cargo build --release -p loonfs-cli               # compile from source
cp ./target/release/loonfs ~/.local/bin/loonfs    # copy it to somewhere in your $PATH
```

## Quickstart

This example uses S3 in embedded mode, where the CLI reads and writes directly to the bucket, without a LoonFS server. Provider credentials are read from the standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` if you use one):

```bash
export AWS_ACCESS_KEY_ID={access_key_id}
export AWS_SECRET_ACCESS_KEY={secret_access_key}
loonfs --no-input profile create s3 default \
  --bucket {bucket_name} \
  --region {aws_region}
loonfs namespace create {namespace_id}
loonfs use {namespace_id}
```

Then write a file and read it:

```bash
loonfs put ./notes.txt /notes.txt
loonfs ls /
loonfs cat /notes.txt
```

The CLI also works with Cloudflare R2 (`profile create r2`), Google Cloud Storage (`gcs`), and a LoonFS server (`remote`). The [CLI reference](crates/loonfs-cli/README.md) lists every command.

## SDKs and libraries

The Go, Python, and TypeScript SDKs talk to a LoonFS server over HTTP.

| Language | Package | Install |
| --- | --- | --- |
| Rust (embedded) | `loonfs` | `cargo add loonfs` |
| Rust (HTTP client) | `loonfs-client` | `cargo add loonfs-client` |
| Go | [`loonfs-sdk-go`](https://github.com/loonfs/loonfs-sdk-go) | `go get github.com/loonfs/loonfs-sdk-go@latest` |
| Python | [`loonfs`](https://github.com/loonfs/loonfs-sdk-python) | `pip install loonfs` |
| TypeScript | [`@loonfs/sdk`](https://github.com/loonfs/loonfs-sdk-typescript) | `npm install @loonfs/sdk` |

## Server deployment

See [Self-hosting LoonFS](crates/loonfs-server/docs/self-hosting.md) for the complete deployment guide.

## Documentation

See the [documentation](docs/specs/README.md) for the architecture, storage format, and API.

## Core concepts

- **Each namespace is an isolated filesystem with a serialized WAL and native version history.** Every change is recorded as a numbered commit in the namespace’s write-ahead log (WAL), every file change is recorded as a new revision, and the change feed replays every commit back to a retention floor.

- **All durable state is stored in object storage and only object storage.** LoonFS stores immutable file content, manifests, numbered WAL objects, and metadata segments in object storage. Caches, workers, and other local state can all be rebuilt from the object store.

- **An item's identity is inode based, and paths are mutable views.** A file or directory is identified by `(namespace_id, inode_id)`. A path points to an inode, so renaming or moving an item changes its path but not its inode.

- **A commit applies one or more changes atomically.** The operations in a commit are serialized in order, and preconditions add optimistic concurrency control. 

- **Forks and snapshots are copy-on-write.** Every stored object is immutable, so a fork shares the source namespace’s metadata segments and content objects instead of copying them. Within a namespace, a snapshot is a pin that keeps one manifest readable until the snapshot expires.


## Design philosophy

LoonFS is built around a correctness-first protocol where the object store is the only source of truth.

- **Correctness is the primary feature.** For system design decisions, LoonFS weighs correctness first, then simplicity, then performance. The [style guide](docs/STYLE.md) lists the full set of rules.