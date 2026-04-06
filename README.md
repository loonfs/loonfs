# LoonFS

LoonFS is an object-storage-backed filesystem and sync core where correctness, determinism, and
rebuildability are product features.

The durable source of truth is object storage. Paths are derived views over inode-keyed metadata.
Head, lease, WAL, content manifests, and snapshots are the durable basis for rebuilding namespace
state.

## Current Product Shape

The current implementation exposes two public surfaces over the same core model:

- a path-oriented user surface through `loon` and `loond`
- a lower-level `/v0` upload, explicit-commit, and ordered change-feed surface through `loond`
  and `loon-client`

The current runtime path is:

`loon -> loon-client -> loond -> loon-core -> loon-objectstore`

Current product rules:

- every CLI operation goes through `loond`, even in local mode
- profile `mode` is `local` or `remote`
- `local` means `loon` points at or manages a local `loond`
- `remote` means `loon` points at an already-running `loond`
- canonical sibling-name comparison uses `NamePolicy = nfc_casefold_v0`

Not implemented yet relative to the current spec set:

- long-running protocol state such as `ReadSession`, `CopyJob`, and `ImportJob`
- ACLs, shares, and mounts as public product behavior

## Spec Lock

The files under [`docs/specs/`](docs/specs/) are authoritative for this repository.

- implement to `docs/specs/*`
- do not create local contracts that disagree with the current spec
- if code and spec disagree, bring the code back into alignment or stop and escalate to the core
  team before changing behavior

## Workspace

```text
crates/
  loon-api/          shared ids, codecs, and HTTP DTOs
  loon-objectstore/  object-store trait, providers, key builders, conformance surface
  loon-core/         authoritative metadata, lease, replay, and mutation logic
  loon-model/        pure metadata replay/model helpers for differential tests
  loon-server/       `loond` HTTP server over core + object storage
  loon-client/       thin HTTP transport client
  loon-cli/          profile-based `loon` CLI
  loon-testkit/      shared test helpers
xtask/
  smoke             end-to-end acceptance helper
```

## Config Ownership

`loon` and `loond` do not share a config file.

- `loon` owns CLI-managed profile state
- `loond` owns operator-authored server configuration

`loon` default config path when `--config` is omitted:

- macOS: `~/Library/Application Support/loon/config.toml`
- other Unix: `~/.config/loon/config.toml`
- Windows: `%APPDATA%\\loon\\config.toml`

`loond` has no code-level default path. You pass a server config path explicitly when you start
`loond` or when you create a local `loon` profile.

Sanitized example configs live in [`configs/`](configs/):

- [`configs/loon.local.example.toml`](configs/loon.local.example.toml)
- [`configs/loon.remote.example.toml`](configs/loon.remote.example.toml)
- [`configs/loond.local-fs.example.toml`](configs/loond.local-fs.example.toml)
- [`configs/loond.aws-s3.example.toml`](configs/loond.aws-s3.example.toml)
- [`configs/loond.cloudflare-r2.example.toml`](configs/loond.cloudflare-r2.example.toml)

Copy example `loond` configs to a user-owned path outside the repository for real use.

The current CLI schema is `config_version = 1`. JSON command envelopes use `format_version = 1`.

## Install And Run

Until a packaged installer lands, the normal user path is to install the binaries from source:

```bash
cargo install --path crates/loon-cli
cargo install --path crates/loon-server
```

Example local flow:

```bash
cp ./configs/loond.local-fs.example.toml "$HOME/loond.local.toml"
$EDITOR "$HOME/loond.local.toml"

loon profile add local local \
  --server-config "$HOME/loond.local.toml"

loon --profile local local up
loon namespace create demo

printf 'hello\n' > /tmp/loonfs-demo.txt
loon filesystem put demo /tmp/loonfs-demo.txt /docs/hello.txt
loon filesystem ls demo /docs
loon filesystem stat demo /docs/hello.txt
loon filesystem get demo /docs/hello.txt /tmp/loonfs-downloaded.txt

loon --profile local local down
```

Example remote profile:

```bash
loon profile add remote prod \
  --server-url http://127.0.0.1:9400 \
  --auth-token dev-token
```

## CLI Surface

Current path-oriented CLI commands:

- `loon profile add local NAME --server-config <PATH>`
- `loon profile add remote NAME --server-url <URL> [--auth-token <TOKEN>]`
- `loon profile list`
- `loon profile use NAME`
- `loon profile show [NAME]`
- `loon profile remove NAME`
- `loon local up`
- `loon local status`
- `loon local down`
- `loon namespace create NAME`
- `loon namespace list`
- `loon filesystem ls NAMESPACE [PATH]`
- `loon filesystem stat NAMESPACE PATH`
- `loon filesystem cat NAMESPACE PATH`
- `loon filesystem get NAMESPACE REMOTE_PATH [LOCAL_DESTINATION]`
- `loon filesystem put NAMESPACE LOCAL_PATH [REMOTE_PATH] [--force]`
- `loon filesystem rm NAMESPACE REMOTE_PATH`
- `loon filesystem mv NAMESPACE SOURCE_PATH DEST_PATH`
- `loon filesystem cp NAMESPACE SOURCE_PATH DEST_PATH`
- `loon config path`
- `loon config show`
- `loon version`

Global flags:

- `--profile <NAME>`
- `--json`
- `--no-input`
- `--config <PATH>`

`filesystem cat` and `filesystem get ... -` stream raw bytes to stdout and reject `--json`.

The CLI is the current path-oriented client profile only. It does not expose first-class commands
for the lower-level staged upload, explicit commit, or ordered change-feed surface yet.

## Verification

Repository baseline:

```bash
cargo fmt --all
cargo test --workspace
```

Smoke acceptance:

```bash
cargo run -p xtask -- smoke local \
  --server-config ./configs/loond.local-fs.example.toml \
  --namespace demo
```

Live-provider object-store conformance:

```bash
cargo test -p loon-objectstore --test objectstore_conformance \
  cloudflare_r2_real_provider_conformance -- --ignored --exact
```

If you are iterating inside the repository rather than using installed binaries, `cargo run -p
loon-cli -- ...` and `cargo run -p loon-server -- ...` remain valid development workflows.

## Specs To Read

- [`docs/specs/000-overview.md`](docs/specs/000-overview.md)
- [`docs/specs/020-architecture-overview.md`](docs/specs/020-architecture-overview.md)
- [`docs/specs/030-object-store-contract.md`](docs/specs/030-object-store-contract.md)
- [`docs/specs/040-filesystem-and-storage-model.md`](docs/specs/040-filesystem-and-storage-model.md)
- [`docs/specs/050-write-read-protocol.md`](docs/specs/050-write-read-protocol.md)
- [`docs/specs/060-interfaces-and-clients.md`](docs/specs/060-interfaces-and-clients.md)
- [`docs/specs/080-background-jobs.md`](docs/specs/080-background-jobs.md)
- [`docs/specs/090-versioning-conformance-and-extensions.md`](docs/specs/090-versioning-conformance-and-extensions.md)
- [`docs/appendices/095-operation-statefulness-matrix.md`](docs/appendices/095-operation-statefulness-matrix.md)
