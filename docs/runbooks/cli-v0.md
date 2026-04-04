# CLI v0

## Quickstart

Create a local profile that points at a `loond` config:

```bash
cargo run -p loon-cli -- \
  profile add local local \
  --server-config ./configs/loond.local-fs.example.toml
```

Start the managed local server and inspect the active profile:

```bash
cargo run -p loon-cli -- local up
cargo run -p loon-cli -- profile show
cargo run -p loon-cli -- local status
```

Create a namespace and upload a file:

```bash
cargo run -p loon-cli -- namespace create demo
cargo run -p loon-cli -- \
  filesystem put demo ./README.md /docs/README.md
```

Stop the managed local server when finished:

```bash
cargo run -p loon-cli -- local down
```

## Profile Setup

Profile `mode` is `local` or `remote`.

- `local` stores only a `loond` config path
- `remote` stores a `server_url` and optional bearer token
- both modes execute through `loond`

Examples:

- [`configs/loon.local.example.toml`](/Users/conormccarter/Code/loondb/configs/loon.local.example.toml)
- [`configs/loon.remote.example.toml`](/Users/conormccarter/Code/loondb/configs/loon.remote.example.toml)
- [`configs/loond.local-fs.example.toml`](/Users/conormccarter/Code/loondb/configs/loond.local-fs.example.toml)
- [`configs/loond.aws-s3.example.toml`](/Users/conormccarter/Code/loondb/configs/loond.aws-s3.example.toml)
- [`configs/loond.cloudflare-r2.example.toml`](/Users/conormccarter/Code/loondb/configs/loond.cloudflare-r2.example.toml)

v0 rules:

- local object-store credentials live only in `loond` config
- remote auth is just an optional bearer token in the CLI profile
- profiles do not carry a default namespace
- the current CLI schema uses `config_version = 1`

## Local Runtime

`loon local up|status|down` manages a local `loond` process for the selected local profile.

- `local up` starts `loond --config <server_config_path>` and waits for `/healthz`
- `local status` reports `running`, `stale`, or `stopped`
- `local down` stops the managed process and clears stale runtime state

## Human vs JSON

Behavior rules:

- stdout is command data only
- prompts and diagnostics go to stderr
- interactive prompting happens only when stdin/stderr are TTY and neither `--json` nor
  `--no-input` is set
- `--no-input` suppresses prompts but keeps normal human output

Streaming commands:

- `filesystem cat` writes raw bytes to stdout and rejects `--json`
- `filesystem get ... -` writes raw bytes to stdout and rejects `--json`

## JSON Contract

Every JSON response uses a versioned envelope:

```json
{
  "kind": "filesystem_stat",
  "format_version": 1,
  "profile": "local",
  "mode": "local",
  "data": {
    "type": "path_entry"
  }
}
```

Error responses use the same outer envelope and move the payload into `error`:

```json
{
  "kind": "local_up",
  "format_version": 1,
  "profile": "local",
  "mode": "local",
  "error": {
    "code": "local_server_already_running",
    "message": "managed local server for profile `local` is already running"
  }
}
```
