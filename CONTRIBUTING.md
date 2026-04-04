# Contributing

## Rewrite rules

- `docs/specs/*` is sacred on this branch.
- proposed spec changes go in `proposals/*`, never as edits to `docs/specs/*`
- prefer deleting legacy surfaces over carrying placeholders
- keep `loon-client` transport-only for this phase
- keep `loon-server` stateless over object storage

## Expected change shape

Prefer small, reviewable batches:

- one object-store contract fix plus tests
- one core mutation/path-resolution change plus tests
- one CLI/client behavior change plus docs

## Baseline commands

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
```

## Local smoke

```bash
cargo run -p loon-server --bin loond -- --config ./configs/loond.local-fs.example.toml
cargo run -p loon-cli -- --config ./configs/loon-client.local.example.toml namespace create demo
cargo run -p xtask -- smoke --config ./configs/loon-client.local.example.toml --namespace demo
```
