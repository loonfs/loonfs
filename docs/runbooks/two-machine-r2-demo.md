# Two-Machine R2 Demo

This runbook proves the current CLI-first product shape against one shared `loond` process and
one Cloudflare R2 bucket.

It is documented here for the following milestone. It is not part of the current acceptance gate.

## Prerequisites

- one Cloudflare R2 bucket
- one reachable host that will run `loond`
- two machines with the repository checked out and Rust installed
- a shared bearer token for the server and both CLI clients

## Configs

1. On the server machine, copy
   [`configs/loond.cloudflare-r2.example.toml`](/Users/conormccarter/Code/loondb/configs/loond.cloudflare-r2.example.toml)
   to `configs/loond.cloudflare-r2.local.toml`, then fill in the real R2 bucket, account,
   endpoint, credentials, and any desired `key_prefix`.
2. On both client machines, copy
   [`configs/loon-client.r2.example.toml`](/Users/conormccarter/Code/loondb/configs/loon-client.r2.example.toml)
   to `configs/loon-client.r2.local.toml`, then point `server_url` at the server host and
   `auth_token` at the shared bearer token.

## Start The Server

On machine A, start `loond`:

```bash
cargo run -p loon-server --bin loond -- --config ./configs/loond.cloudflare-r2.local.toml
```

Wait for the server to answer `GET /healthz`.

## Machine A: Write Path

Create a namespace and upload a file:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.local.toml namespace create demo

cargo run -p loon-cli -- --config ./configs/loon-client.r2.local.toml \
  file put ./README.md demo:/docs/README.md

cargo run -p loon-cli -- --config ./configs/loon-client.r2.local.toml \
  file mv demo:/docs/README.md demo:/docs/README-renamed.md
```

## Machine B: Read Path

On machine B, inspect and download the same namespace through the same server:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.local.toml namespace list

cargo run -p loon-cli -- --config ./configs/loon-client.r2.local.toml \
  file stat demo:/docs/README-renamed.md

cargo run -p loon-cli -- --config ./configs/loon-client.r2.local.toml \
  file get demo:/docs/README-renamed.md ./tmp-readme.md
```

Verify that `./tmp-readme.md` matches the expected bytes.

## Machine A: Remove

Delete the file from machine A:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.local.toml \
  file rm demo:/docs/README-renamed.md
```

## Machine B: Verify Removal

Confirm that the file is gone:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.local.toml \
  file ls demo:/docs

cargo run -p loon-cli -- --config ./configs/loon-client.r2.local.toml \
  file stat demo:/docs/README-renamed.md
```

The final `file stat` should return a `path_not_found` API error.
