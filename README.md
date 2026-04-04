# LoonDB

LoonDB is now a spec-locked rewrite branch focused on one narrow product shape:

- `loon-cli -> loon-client -> loon-server -> loon-core -> loon-objectstore`
- one stateless HTTP server
- one thin HTTP client library
- one CLI for namespace and file operations
- one object-store-backed authority, starting with local-fs, AWS S3, and Cloudflare R2

The first milestone is not a sync engine. It is a readable, spec-shaped core plus a usable CLI
that can be run from two machines against the same server and bucket.

## Spec Lock

The files under [`docs/specs/`](docs/specs/) are read-only on this branch.

- do not edit `docs/specs/*`
- do not reinterpret those files silently in code
- record clarifications, divergences, or replacements under [`proposals/`](proposals/)

## Workspace

```text
crates/
  loon-api/          shared ids, envelopes, and HTTP DTOs
  loon-objectstore/  object-store trait, providers, key builders, conformance surface
  loon-core/         authoritative metadata, lease, WAL, path resolution, mutations
  loon-model/        pure metadata replay/model helpers for differential tests
  loon-server/       stateless HTTP shell and `loond` binary
  loon-client/       transport-only HTTP client
  loon-cli/          `loon` CLI
  loon-testkit/      minimal shared test helpers

xtask/               smoke automation
```

## Local Demo

1. Start the server with the local-fs example config.

```bash
cargo run -p loon-server --bin loond -- --config ./configs/loond.local-fs.example.toml
```

2. In another shell, point the CLI at the client config and create a namespace.

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.local.example.toml namespace create demo
```

3. Upload, inspect, and download a file.

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.local.example.toml \
  file put ./README.md demo:/docs/README.md

cargo run -p loon-cli -- --config ./configs/loon-client.local.example.toml \
  file ls demo:/docs

cargo run -p loon-cli -- --config ./configs/loon-client.local.example.toml \
  file get demo:/docs/README.md ./tmp-readme.md
```

## Commands

The current CLI surface is:

- `loon namespace create NAME [--json]`
- `loon namespace list [--json]`
- `loon file ls NAMESPACE:/path [--json]`
- `loon file stat NAMESPACE:/path [--json]`
- `loon file cat NAMESPACE:/path`
- `loon file get NAMESPACE:/path LOCAL_PATH [--json]`
- `loon file put LOCAL_PATH NAMESPACE:/path [--json]`
- `loon file rm NAMESPACE:/path [--json]`
- `loon file mv NAMESPACE:/from NAMESPACE:/to [--json]`

## Configs

Example configs live in [`configs/`](configs/):

- [`configs/loond.local-fs.example.toml`](configs/loond.local-fs.example.toml)
- [`configs/loond.aws-s3.example.toml`](configs/loond.aws-s3.example.toml)
- [`configs/loond.cloudflare-r2.example.toml`](configs/loond.cloudflare-r2.example.toml)
- [`configs/loon-client.local.example.toml`](configs/loon-client.local.example.toml)
- [`configs/loon-client.r2.example.toml`](configs/loon-client.r2.example.toml)

Checked-in `*.example.toml` files stay sanitized. For filled-in local credentials or machine-specific
values, copy the example to `configs/*.local.toml`. Those local config files are ignored by Git.

## Verification

Current local baseline:

```bash
cargo fmt --all
cargo check --workspace
cargo run -p xtask -- smoke \
  --server-config ./configs/loond.local-fs.example.toml \
  --client-config ./configs/loon-client.local.example.toml \
  --namespace demo
```

If `loond` is already running, point `xtask` at the live server only:

```bash
cargo run -p xtask -- smoke \
  --client-config ./configs/loon-client.local.example.toml \
  --namespace demo
```

## R2 Acceptance

The acceptance ladder for the current CLI-first milestone is:

1. direct Cloudflare R2 conformance
2. managed single-host R2 smoke
3. manual two-host lease-handoff demo

Cloudflare R2 validation stays manual and env-gated. The canonical conformance invocation is:

```bash
cargo test -p loon-objectstore --test objectstore_conformance \
  cloudflare_r2_real_provider_conformance -- --ignored --exact
```

The R2 conformance test reads these environment variables:

- `LOON_TEST_R2_BUCKET`
- `LOON_TEST_R2_ACCOUNT_ID`
- `LOON_TEST_R2_ENDPOINT`
- `LOON_TEST_R2_ACCESS_KEY_ID`
- `LOON_TEST_R2_SECRET_ACCESS_KEY`
- `LOON_TEST_R2_PREFIX` (optional)

The example values and variable names live in
[`crates/loon-objectstore/tests/provider-conformance.env.example`](crates/loon-objectstore/tests/provider-conformance.env.example).

For a managed smoke run against an R2-backed server config:

```bash
cargo run -p xtask -- smoke \
  --server-config ./configs/loond.cloudflare-r2.local.toml \
  --client-config ./configs/loon-client.r2.local.toml \
  --namespace demo
```

If the R2-backed server is already running:

```bash
cargo run -p xtask -- smoke \
  --client-config ./configs/loon-client.r2.local.toml \
  --namespace demo
```

## Two-Machine Demo

The two-host lease-handoff demo is now part of the acceptance gate after the direct conformance
command and the managed smoke flow. Use host-specific ignored local configs so both machines share
the same bucket, key prefix, and auth token while using distinct `bind`, `writer_id`, and
`server_url` values.

The full A/B sequence is documented in
[`docs/runbooks/two-machine-r2-demo.md`](docs/runbooks/two-machine-r2-demo.md).
