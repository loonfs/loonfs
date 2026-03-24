# Runbook: local RC path

This runbook defines the canonical local release-candidate path for the current repo state.

## Why this exists

The repo now has a real local operability substrate. What it still needs is one repeatable answer to
"what does green look like before I demo this or hand it to another engineer?"

That answer is `xtask rc-local`.

## Config templates

Tracked example configs live under:

```text
configs/
```

Current templates:

- `configs/loondb-demo.local-fs.example.toml`
- `configs/loondb-demo.aws-s3.example.toml`
- `configs/loondb-demo.cloudflare-r2.example.toml`

Copy the template you need to an untracked local file before editing values.
The default local-fs template writes demo artifacts under `/.loondb-demo/`, and that directory is
ignored by Git so the canonical RC path does not dirty the worktree.

## Canonical local RC path

Use the local-fs template for the everyday path:

```bash
cp configs/loondb-demo.local-fs.example.toml loondb-demo.local.toml
cargo run -p xtask -- rc-local --config ./loondb-demo.local.toml --namespace demo
```

`xtask rc-local` runs, in order:

1. `cargo fmt --all --check`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo test --workspace`
4. `cargo test -p loon-objectstore --test conformance`
5. `xtask ops smoke` through the existing `loon-ops` smoke command path

## Provider-backed smoke

Real-provider smoke stays explicit and manual in this phase.

AWS S3:

```bash
cp configs/loondb-demo.aws-s3.example.toml loondb-demo.aws-s3.local.toml
# edit bucket, credentials, and any endpoint details

cargo run -p xtask -- ops smoke --config ./loondb-demo.aws-s3.local.toml --namespace demo
```

Cloudflare R2:

```bash
cp configs/loondb-demo.cloudflare-r2.example.toml loondb-demo.cloudflare-r2.local.toml
# edit account, endpoint, bucket, and credentials

cargo run -p xtask -- ops smoke --config ./loondb-demo.cloudflare-r2.local.toml --namespace demo
```

Provider-backed smoke does not replace provider conformance. Run the real-provider conformance
commands from `docs/runbooks/provider-conformance.md` as well.

## Boundary rules

- `xtask rc-local` is repo automation, not part of the future product CLI contract
- `ops smoke` remains bootstrap/inspection-only
- no provider env files are auto-loaded by `rc-local`
- real-provider credentials stay in local untracked files or external CI secrets, not tracked config
