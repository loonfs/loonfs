# CLI

This runbook describes the current path-oriented `loon` / `loond` implementation layer.

It does not cover first-class CLI commands for the lower-level staged upload, explicit commit, or
ordered change-feed public surface described in the imported core specs. Those `/v0` APIs now
exist programmatically through `loond` and `loon-client`, but the current CLI only exposes direct
path-oriented operations. It also does not provide session/job control-plane commands.

## Quickstart

Install the current macOS arm64 binaries:

```bash
curl https://install.loonfs.com | sh
export PATH="$HOME/.loonfs/bin:$PATH"
```

Create a local profile that points at a user-authored `loond` config:

```bash
cp ~/.config/loonfs/loond/examples/loond.cloudflare-r2.example.toml \
  ~/.config/loonfs/loond/home.toml

$EDITOR ~/.config/loonfs/loond/home.toml

loon profile add local home \
  --server-config ~/.config/loonfs/loond/home.toml
```

Start the managed local server and inspect the active profile:

```bash
loon --profile home local up
loon --profile home profile show
loon --profile home local status
```

Create a namespace and upload a file:

```bash
printf 'hello from loonfs\n' > ./hello.txt

loon namespace create demo
loon filesystem put demo ./hello.txt /docs/hello.txt
```

Stop the managed local server when finished:

```bash
loon --profile home local down
```

## Profile Setup

Profile `mode` is `local` or `remote`.

- `local` stores only a `loond` config path
- `remote` stores a `server_url` and optional bearer token
- both modes execute through `loond`
- the current CLI is one path-oriented client profile over the broader LoonFS core model
- current `/v0` filesystem operations compile onto the explicit commit engine underneath

Examples:

- installed examples live under `~/.config/loonfs/loond/examples/`
- repository source examples remain in:
  - [`configs/loon.local.example.toml`](/Users/conormccarter/Code/loondb/configs/loon.local.example.toml)
  - [`configs/loon.remote.example.toml`](/Users/conormccarter/Code/loondb/configs/loon.remote.example.toml)
  - [`configs/loond.local-fs.example.toml`](/Users/conormccarter/Code/loondb/configs/loond.local-fs.example.toml)
  - [`configs/loond.aws-s3.example.toml`](/Users/conormccarter/Code/loondb/configs/loond.aws-s3.example.toml)
  - [`configs/loond.cloudflare-r2.example.toml`](/Users/conormccarter/Code/loondb/configs/loond.cloudflare-r2.example.toml)

Current rules:

- local object-store credentials live only in `loond` config
- remote auth is just an optional bearer token in the CLI profile
- profiles do not carry a default namespace
- the current CLI schema uses `config_version = 1`
- the default installed CLI config path on macOS is `~/.config/loonfs/loon/config.toml`
- staged upload / explicit commit / change-feed CLI commands are not implemented yet
- programmatic callers can use `loon-client` advanced path mutation methods with caller-supplied
  `request_id` values for deterministic retries

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

Current filesystem limitations in this client profile:

- `filesystem put` is file-only
- `filesystem cp` is file-only
- `filesystem get` rejects directories
- there is no first-class recursive/session/job surface in the current CLI

## Repository Development

If you are working from a checkout instead of installed binaries, keep using:

```bash
cargo run -p loon-cli -- ...
```

Repository source examples live under [`configs/`](/Users/conormccarter/Code/loondb/configs).

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
