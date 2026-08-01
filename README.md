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
  &nbsp;&nbsp;
  <a href="https://loonfs.com"><img alt="Site: loonfs.com" src="https://img.shields.io/badge/site-loonfs.com-blue?style=flat-square"></a>
</div>
<br>
<br>

## LoonFS

LoonFS is a durable filesystem built on object storage. It can be used to store, manage, index, and retrieve files and folders for a variety of use cases. Object storage is the only durable dependency from which LoonFS derives virtually unlimited storage and a high throughput ceiling. It uses a single-writer, multi-reader model and can be used across sessions and clients as an embedded engine or through a remote server connection.

## Download

You can use the [install script](https://github.com/loonfs/loonfs/blob/main/scripts/install-loonfs.sh) by running
```bash
curl -fsSL https://install.loonfs.com | sh
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

This example uses S3 in embedded mode, where the CLI talks directly to the bucket without a LoonFS server. Provider credentials are read from the standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` if you use one):

```bash
export AWS_ACCESS_KEY_ID={access_key_id}
export AWS_SECRET_ACCESS_KEY={secret_access_key}
loonfs init default --no-input \
  --mode embedded \
  --store-kind aws-s3 \
  --bucket {bucket_name} \
  --region {aws_region}
loonfs namespace create {namespace_id}
loonfs use {namespace_id}
```

## Running a server

Embedded mode gives one process at a time direct object-store access. Concurrent `loonfs` invocations against one namespace contend for the writer role: whichever acquires it last wins, and a command that loses fails with `writer_fenced`, naming both sessions. A fenced command commits nothing, so rerunning it is always safe — but fencing is a stop signal, not a retry loop, so put bulk work in one process or run the server for concurrent writers. To share a deployment across machines and let many clients write concurrently, run the reference server and point remote profiles at it:

```bash
cargo build --release -p loonfs-server
./target/release/loonfs-server --config configs/loonfs-server.local-fs.example.toml
```

Commented example configs for every supported store live in [configs/](configs) (`local-fs`, `aws-s3`, `gcp-gcs`, `cloudflare-r2`, `azure-abs`). The required fields are `bind`, `writer_id`, and a `[store]` block; everything else defaults. Secrets need not live in the file: `auth_token` and `content_token_secret` fall back to `LOONFS_AUTH_TOKEN` and `LOONFS_CONTENT_TOKEN_SECRET`, and an `aws-s3` or `cloudflare-r2` store falls back to `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`. A value written in the file always wins. A server with no `[grep]` table composes no grep at all; the optional `[grep]` table selects `mode = "disabled" | "serve_only" | "maintain_only" | "serve_and_maintain"` (default `serve_and_maintain`) and the bounded per-step build/fold policy shown in the local-fs example. Grep index maintenance runs as one job under the writer's maintenance runner, driven by writes rather than by a timer or a store enumeration. Connect a client:

```bash
export LOONFS_AUTH_TOKEN={auth_token}
loonfs init default --no-input --mode remote --server-url http://127.0.0.1:9400
```

### Running a server in production

Two things travel over the connection that must not travel in the clear: the
bearer token on every request, and the presigned object-store URLs the
transfer routes return in response bodies. Each of those URLs is a capability
to write into or read out of your bucket, so anyone who can read the traffic
can both impersonate the client and reach objects directly. A server that
binds anything other than loopback therefore refuses to start unless it either
terminates TLS itself or says out loud that something else does.

Direct transfers go both ways, and that is a rule rather than a convenience:
a deployment must be able to serve back whatever it let a client create. On
S3 and R2 a client can upload an object of any size straight to the bucket,
while a proxied read buffers the whole file and refuses anything past
`max_download_bytes` (256 MiB by default) — so those deployments also hand
out short-lived presigned reads, and `loonfs get` uses one whenever a file is
larger than the server will proxy. Deployments that cannot presign proxy
everything, which is safe because they cannot presign an upload either.

Terminate TLS in the server by adding a `[tls]` table:

```toml
bind = "0.0.0.0:9400"

[tls]
cert_path = "/etc/loonfs/tls/server.crt"   # PEM chain, leaf first
key_path  = "/etc/loonfs/tls/server.key"   # PEM PKCS#8, RSA, or EC key
```

Both files are read once at startup, before the port is bound: a missing or
malformed one fails the process instead of quietly serving plaintext. Clients
then use an `https://` server URL. For a certificate a private CA issued,
point the client at the CA bundle with `--ca-cert-path` (or `ca_cert_path` in
the profile); it is added to the platform trust store rather than replacing
it.

If TLS terminates in front of LoonFS — a load balancer, an ingress
controller, a sidecar — leave `[tls]` out and declare that:

```toml
bind = "0.0.0.0:9400"
allow_remote_without_tls = true
```

A loopback bind never requires either, so local development is unchanged. The
same shape governs authentication: a non-loopback bind with no `auth_token`
refuses to start unless `allow_unauthenticated_remote = true`.

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
