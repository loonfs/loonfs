# Runbook: loon CLI

This runbook is the operator-facing manual for the current `loon-cli` surface.

## What `loon-cli` is today

`loon-cli` is an active thin frontend over the shared `loon-ops` contract.

Today that means:

- `loon ops ...` reuses the same command grammar, config shape, and stdout rendering as
  `xtask ops ...`
- `xtask ops ...` remains supported in parallel for repo/dev workflows
- `xtask rc-local` remains repo automation owned by `xtask`; it is not part of `loon-cli`
- the current config contract is still the existing ops TOML shape used by `loon-ops`

`loon-cli` is intentionally not the owner of:

- auth or token management
- profile storage
- HTTP endpoint abstraction
- auth or token management
- profile storage
- HTTP endpoint abstraction

## Start with built-in discovery

The first discovery surface is built into the CLI itself:

```bash
cargo run -p loon-cli -- --help
cargo run -p loon-cli -- help ops
cargo run -p loon-cli -- help ops bootstrap-namespace
```

Generated local manpages are also available:

```bash
cargo run -p loon-cli -- manpages ./target/man
```

Shell completions are available too:

```bash
cargo run -p loon-cli -- completion bash
cargo run -p loon-cli -- completion zsh
```

## Current command families

Current active families:

- `loon ops ...`
- `loon file ...`
- `loon config ...`
- `loon doctor`
- `loon completion ...`
- `loon manpages ...`
- `loon version`

## Namespace bootstrap path

The namespace-init path today is:

```bash
cargo run -p loon-cli -- ops bootstrap-namespace --config ./loondb-demo.local.toml --namespace demo
```

That is the supported way to create/bootstrap namespace control state in the current CLI.

Related inspection and sync commands:

```bash
cargo run -p loon-cli -- ops show-namespace-state --config ./loondb-demo.local.toml --namespace demo
cargo run -p loon-cli -- ops show-client-state --config ./loondb-demo.local.toml --namespace demo
cargo run -p loon-cli -- ops import-remote-observations --config ./loondb-demo.local.toml --namespace demo
cargo run -p loon-cli -- ops observe-local --config ./loondb-demo.local.toml --namespace demo --path ./mirror/hello.txt
cargo run -p loon-cli -- ops observe-delete --config ./loondb-demo.local.toml --namespace demo --path ./mirror/hello.txt
cargo run -p loon-cli -- ops observe-move --config ./loondb-demo.local.toml --namespace demo --from ./mirror/hello.txt --to ./mirror/archive/hello.txt
cargo run -p loon-cli -- ops observe-subtree --config ./loondb-demo.local.toml --namespace demo --path ./mirror/docs
cargo run -p loon-cli -- ops sync-once --config ./loondb-demo.local.toml --namespace demo
cargo run -p loon-cli -- ops sync-until-idle --config ./loondb-demo.local.toml --namespace demo --max-steps 50
cargo run -p loon-cli -- ops smoke --config ./loondb-demo.local.toml --namespace demo
```

## Product-facing authoritative file commands

`loon file ...` is the direct authoritative product shell. It does not route through the mirror,
client SQLite state, or sync execution.

Current commands:

```bash
cargo run -p loon-cli -- file ls demo:/
cargo run -p loon-cli -- file stat demo:/docs/report.txt
cargo run -p loon-cli -- file get demo:/hello.txt ./downloads
cargo run -p loon-cli -- file get --recursive demo:/docs ./docs-download
cargo run -p loon-cli -- file cat demo:/hello.txt
cargo run -p loon-cli -- file put ./hello.txt demo:/docs/hello.txt
cargo run -p loon-cli -- file put --replace ./hello-v2.txt demo:/docs/hello.txt
cargo run -p loon-cli -- file put --recursive ./docs demo:/uploaded-docs
cargo run -p loon-cli -- file cp demo:/docs/hello.txt demo:/docs/hello-copy.txt
cargo run -p loon-cli -- file cp --replace demo:/docs/hello.txt demo:/docs/existing.txt
cargo run -p loon-cli -- file cp --recursive demo:/docs demo:/docs-copy
cargo run -p loon-cli -- file mkdir demo:/docs/archive
cargo run -p loon-cli -- file rm demo:/docs/hello.txt
cargo run -p loon-cli -- file rm --recursive demo:/docs/archive
cargo run -p loon-cli -- file mv demo:/docs/hello.txt demo:/docs/hello-final.txt
```

Strict v1 rules:

- remote selectors are always `namespace:/absolute/path`
- `put` is local-file only
- plain `put` is create-only
- `put --replace` is update-only and requires an existing visible file destination
- `put --recursive` is local-directory only, create-only, and exact-root
- `put --recursive` rejects symlinks and other unsupported non-file/non-directory descendants
- plain `get` is file-only
- `get --recursive` is directory-only and downloads to one absent exact local root path
- plain `cp` is same-namespace, file-only, and create-only
- `cp --replace` is same-namespace, file-only, and update-only
- `cp --recursive` is same-namespace, directory-only, create-only, and exact-root
- `put` and `mv` treat the destination selector as the exact final path
- `cp` treats both selectors as exact final paths too
- destination overwrite is never implicit
- `rm` requires `--recursive` for directories
- root is rejected for all write commands

## Config files and templates

Tracked example configs live under:

```text
configs/
```

Current templates:

- `configs/loondb-demo.local-fs.example.toml`
- `configs/loondb-demo.aws-s3.example.toml`
- `configs/loondb-demo.cloudflare-r2.example.toml`

Copy a template to an untracked local file before editing values:

```bash
cp configs/loondb-demo.local-fs.example.toml loondb-demo.local.toml
```

`loon ops ...` still requires explicit `--config`.

The new CLI-only config helpers use this resolution order:

1. explicit `--config <path>`
2. `LOON_CONFIG`
3. `./loondb-demo.local.toml`
4. `./loondb-demo.toml`

Config inspection commands:

```bash
cargo run -p loon-cli -- config path
cargo run -p loon-cli -- config show
cargo run -p loon-cli -- config validate
```

## Doctor

Use `doctor` to confirm the local CLI surface is usable before running ops commands:

```bash
cargo run -p loon-cli -- doctor
cargo run -p loon-cli -- doctor --config ./loondb-demo.local.toml
```

`doctor` checks:

- config resolution
- config readability and parsing
- object-store constructor validity
- client DB path and parent-directory status
- mirror-root status
- presence of tracked example config templates

`doctor` does not bootstrap namespaces, mutate object-store state, or run `ops smoke`.

## Related runbooks

- RC path: [local-rc.md](/Users/conormccarter/Code/loondb/docs/runbooks/local-rc.md)
- Provider-backed conformance: [provider-conformance.md](/Users/conormccarter/Code/loondb/docs/runbooks/provider-conformance.md)
