# LoonFS

LoonFS is an object-storage-backed filesystem and sync core where correctness, determinism, and
rebuildability are primary product features.

The durable source of truth is object storage. Paths are derived views over inode-keyed metadata.
Head, lease, WAL, content manifests, and snapshots are the durable basis for rebuilding visible
namespace state.

Current implementation shape:

- the imported `docs/specs/*` core spec family is authoritative
- the current repo implements one path-oriented client profile on top of that model
- `loond` also exposes a lower-level `/v0` upload / explicit commit / ordered change-feed surface
- `loon` is the current operator-facing CLI
- every CLI operation goes through `loond`, even in local mode
- profile `mode` is `local` or `remote`
- `local` means the CLI points at or manages a local `loond`
- `remote` means the CLI points at an already-running `loond`

Not implemented yet relative to the imported core spec:

 - long-running protocol state such as `ReadSession`, `CopyJob`, and `ImportJob`
- ACLs, shares, and mounts as public product behavior

## Spec Lock

The files under [`docs/specs/`](docs/specs/) are read-only on this branch.

- do not edit `docs/specs/*`
- record clarifications, divergences, or replacements under [`proposals/`](proposals/)

## Workspace

```text
crates/
  loon-api/          shared ids and HTTP DTOs
  loon-objectstore/  object-store trait, providers, key builders, conformance surface
  loon-core/         authoritative metadata, lease, WAL, path resolution, mutations
  loon-model/        pure metadata replay/model helpers for differential tests
  loon-server/       `loond` HTTP server over core + object storage
  loon-client/       thin HTTP transport client
  loon-cli/          profile-based `loon` CLI
  loon-testkit/      minimal shared test helpers
```

## Quickstart

This quickstart uses the current path-oriented CLI surface. The lower-level `/v0` upload,
explicit-commit, and ordered change-feed surface is available through `loond` and `loon-client`,
but the CLI does not expose first-class commands for it yet.

1. Create a local `loond` config from the example:

```bash
cp ./configs/loond.local-fs.example.toml ./configs/loond.local-fs.local.toml
```

2. Register a local profile and start the managed server:

```bash
cargo run -p loon-cli -- \
  profile add local local \
  --server-config ./configs/loond.local-fs.local.toml

cargo run -p loon-cli -- --profile local local up
```

3. Create a namespace and work with files:

```bash
cargo run -p loon-cli -- namespace create demo

cargo run -p loon-cli -- \
  filesystem put demo ./README.md /docs/README.md

cargo run -p loon-cli -- filesystem ls demo /docs
cargo run -p loon-cli -- filesystem stat demo /docs/README.md
cargo run -p loon-cli -- filesystem get demo /docs/README.md ./tmp-readme.md
```

4. Stop the managed local server when you are done:

```bash
cargo run -p loon-cli -- --profile local local down
```

## Commands

Current path-oriented CLI surface:

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

## Configs

Example CLI configs live in [`configs/`](configs/):

- [`configs/loon.local.example.toml`](configs/loon.local.example.toml)
- [`configs/loon.remote.example.toml`](configs/loon.remote.example.toml)
- [`configs/loond.local-fs.example.toml`](configs/loond.local-fs.example.toml)
- [`configs/loond.aws-s3.example.toml`](configs/loond.aws-s3.example.toml)
- [`configs/loond.cloudflare-r2.example.toml`](configs/loond.cloudflare-r2.example.toml)

Checked-in examples stay sanitized. For filled-in local credentials or machine-specific values,
copy an example to `configs/*.local.toml`. Those local config files are ignored by Git.

The current CLI schema is `config_version = 1`.

## Human And JSON Behavior

- stdout is command data only
- prompts and diagnostics go to stderr
- `--json` wraps command data in a versioned envelope with:
  - `kind`
  - `format_version`
  - `profile`
  - `mode`
  - `data` or `error`
- `filesystem cat` always streams raw bytes to stdout and rejects `--json`
- `filesystem get ... -` streams raw bytes to stdout and rejects `--json`

The JSON envelope and CLI commands above describe the current path-oriented client profile only.
They are not the full public surface described by the imported core specs.

Current transport layering:

- `/v1` path-oriented filesystem mutations are convenience operations that compile onto the
  explicit commit engine
- `/v0` exposes staged upload, explicit commit, and ordered change-feed APIs directly
- `loon-client` includes advanced path-oriented methods that accept caller-supplied `request_id`
  values for deterministic retries

## Verification

Current local baseline:

```bash
cargo fmt --all
cargo test --workspace
```

The canonical live-provider object-store conformance command remains:

```bash
cargo test -p loon-objectstore --test objectstore_conformance \
  cloudflare_r2_real_provider_conformance -- --ignored --exact
```

For single-host server acceptance with real object storage:

```bash
cargo run -p xtask -- smoke local \
  --server-config ./configs/loond.cloudflare-r2.local.toml \
  --namespace demo
```

For remote-server acceptance against an already-running `loond`:

```bash
cargo run -p xtask -- smoke remote \
  --server-url http://127.0.0.1:9400 \
  --auth-token dev-token \
  --namespace demo
```

## More Reading

- [`docs/specs/000-overview.md`](docs/specs/000-overview.md)
- [`docs/specs/040-filesystem-and-storage-model.md`](docs/specs/040-filesystem-and-storage-model.md)
- [`docs/specs/050-write-read-protocol.md`](docs/specs/050-write-read-protocol.md)
- [`docs/specs/060-interfaces-and-clients.md`](docs/specs/060-interfaces-and-clients.md)
- [`docs/adr/0019-cli-v0-server-backed-local-and-remote-profiles.md`](docs/adr/0019-cli-v0-server-backed-local-and-remote-profiles.md)
- [`docs/adr/0020-name-policy-follows-core-spec.md`](docs/adr/0020-name-policy-follows-core-spec.md)
- [`docs/runbooks/cli-v0.md`](docs/runbooks/cli-v0.md)
- [`docs/runbooks/two-machine-r2-demo.md`](docs/runbooks/two-machine-r2-demo.md)
- [`proposals/003-core-spec-family-alignment.md`](proposals/003-core-spec-family-alignment.md)
