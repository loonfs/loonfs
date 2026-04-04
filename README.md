# LoonDB

LoonDB is a spec-locked rewrite centered on a server-backed CLI.

Current product shape:

- `loon` is the first public UI
- every CLI operation goes through `loond`, even in local mode
- profile `mode` is `local` or `remote`
- `local` means the CLI points at or manages a local `loond`
- `remote` means the CLI points at an already-running `loond`

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

CLI v0 surface:

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
cargo run -p xtask -- smoke \
  --server-config ./configs/loond.cloudflare-r2.local.toml \
  --client-config ./configs/loon-client.r2.local.toml \
  --namespace demo
```

## More Reading

- [`docs/adr/0019-cli-v0-server-backed-local-and-remote-profiles.md`](docs/adr/0019-cli-v0-server-backed-local-and-remote-profiles.md)
- [`docs/runbooks/cli-v0.md`](docs/runbooks/cli-v0.md)
- [`docs/runbooks/two-machine-r2-demo.md`](docs/runbooks/two-machine-r2-demo.md)
- [`proposals/002-cli-v0-server-backed-correction.md`](proposals/002-cli-v0-server-backed-correction.md)
