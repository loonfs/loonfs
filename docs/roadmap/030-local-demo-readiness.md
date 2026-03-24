# Roadmap 030: local operability substrate and future CLI handoff

This roadmap keeps the existing path for link stability, but the goal is a **local operability substrate**:

- explicit config for provider, client, and server settings
- supported namespace bootstrap and authoritative-state inspection
- supported translation from authoritative namespace state into client-applicable
  `ObservedRemoteInode` values
- supported library-first full-namespace authoritative observation import through `loon-ops`
  composition, without adding a shell workflow yet
- supported client-local observation of one existing file through `loon-client` plus thin
  `loon-ops` composition
- supported explicit client-local delete and move observation through `loon-client` plus thin
  `loon-ops` composition
- supported recursive subtree observation through `loon-client` batch observation plus thin
  `loon-ops` composition
- supported single-step client execution through the real scheduler and real server mutation path
- supported thin repeat-until-idle execution through the same real scheduler step
- a thin internal shell in `xtask`
- one shared command/config/rendering layer that can later move into `loon-cli` unchanged

## Why this roadmap exists

The semantic center is now strong enough that the main risk is no longer "missing core protocol."
The main risk is that local operator flows still depend on test-only composition and crate-local
ad hoc wiring.

The repo needs one honest way to:

- point at a real provider
- bootstrap a namespace
- inspect authoritative state
- inspect client state
- smoke-test that configuration and storage access are valid

It does **not** need a half-product workflow shell yet.

## Milestone 15: shared ops core and thin frontend

Goal:

Add one shared shell-support layer and keep `xtask` as the only active frontend for now.

Primary crates:

- `loon-ops`
- `loon-objectstore`
- `loon-server`
- `loon-client`
- `xtask`

Deliverables:

- `crates/loon-ops` as the future-stable shell contract for:
  - config loading
  - object-store selection
  - stable command grammar
  - stable human-readable rendering
  - shell-neutral command execution
- public configured store construction in `loon-objectstore` for:
  - `local-fs`
  - `aws-s3`
  - `cloudflare-r2`
- supported authoritative ops in `loon_server::ops`:
  - `bootstrap_namespace`
  - `load_namespace_state_summary`
  - `translate_authoritative_state_to_remote_observations`
- supported library composition in `loon-ops`:
  - `import_authoritative_remote_observations`
  - full authoritative snapshot translation before any client DB mutation
  - atomic batch application through the existing client observation protocol
- thin `xtask ops ...` commands:
  - `bootstrap-namespace`
  - `show-namespace-state`
  - `show-client-state`
  - `import-remote-observations`
  - `observe-local`
  - `observe-delete`
  - `observe-move`
  - `observe-subtree`
  - `sync-once`
  - `sync-until-idle`
  - `smoke`

Required rules:

- `xtask` stays thin and must not become the owner of config parsing or business logic
- `loon-ops` is the shared shell core that future `loon-cli` work must reuse
- no `demo-*` command family
- no watcher or delete/move inference inside `observe-local`
- `observe-local` is still file-first and existing-file-only
- delete and move remain explicit shell commands, not inference inside `observe-local`
- `observe-subtree` is directory-only, recursive, and atomic
- `observe-subtree` may infer:
  - unique digest-equal file moves
  - unique exact-subtree directory moves
- directory inference remains strict:
  - rooted subtree fingerprints must match exactly
  - descendant add/delete/edit blocks pairing
  - ambiguity fails closed
- explicit `observe-move` remains the override for non-exact refactors
- `sync-once` is executor-only and progresses at most one real scheduler step
- `sync-until-idle` is only a loop over the real `sync-once` path
- `xtask ops import-remote-observations` is the first shell exposure of authoritative import and
  it must reuse the `loon-ops` import API unchanged
- `ops smoke` remains inspect/bootstrap only in this milestone
- no ambient provider env lookup in core crates
- bootstrap fails closed by default if the namespace already exists
- the shell output stays stable and human-readable

Command contract:

```bash
cargo run -p xtask -- ops bootstrap-namespace --config ./loondb-demo.toml --namespace demo
cargo run -p xtask -- ops show-namespace-state --config ./loondb-demo.toml --namespace demo
cargo run -p xtask -- ops show-client-state --config ./loondb-demo.toml --namespace demo
cargo run -p xtask -- ops import-remote-observations --config ./loondb-demo.toml --namespace demo
cargo run -p xtask -- ops observe-local --config ./loondb-demo.toml --namespace demo --path ./mirror/hello.txt
cargo run -p xtask -- ops observe-delete --config ./loondb-demo.toml --namespace demo --path ./mirror/hello.txt
cargo run -p xtask -- ops observe-move --config ./loondb-demo.toml --namespace demo --from ./mirror/hello.txt --to ./mirror/archive/hello.txt
cargo run -p xtask -- ops observe-subtree --config ./loondb-demo.toml --namespace demo --path ./mirror/docs
cargo run -p xtask -- ops sync-once --config ./loondb-demo.toml --namespace demo
cargo run -p xtask -- ops sync-until-idle --config ./loondb-demo.toml --namespace demo --max-steps 50
cargo run -p xtask -- ops smoke --config ./loondb-demo.toml --namespace demo
```

Exit criteria:

- one developer can point `xtask ops` at `local-fs`, AWS S3, or Cloudflare R2 using the same
  config shape
- namespace bootstrap and authoritative-state inspection no longer require test helper code
- client-state inspection is available through the same shell contract
- authoritative observation import is shell-exposed only by calling the existing `loon-ops` API
  unchanged
- one existing local file can be observed and planned through the supported client API rather than
  through shell-local SQLite mutation
- explicit local delete and move can be observed through the same supported client API surface,
  with bound rename/delete syncing through the real mutation path
- one recursive subtree scan can batch create/edit/delete observations and preserve unambiguous
  file and directory move identity without widening into heuristic move guesses
- one honest client scheduler step or idle loop can be exercised locally without inventing a
  second sync path
- future `loon-cli` work can reuse `loon-ops` instead of re-implementing config and renderers

## Milestone 16: provider-backed RC hardening

Goal:

Make the operability substrate honest enough to support a narrow provider-backed local RC path.

Primary crates:

- `loon-objectstore`
- `loon-ops`
- `xtask`

Deliverables:

- one canonical `xtask rc-local` path that runs:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - workspace tests
- local object-store conformance
- the existing `loon-ops` smoke path, called through `xtask rc-local`
- checked-in example configs for:
  - `local-fs`
  - `aws-s3`
  - `cloudflare-r2`
- documented real-provider smoke steps for AWS S3 first, then Cloudflare R2

Required rules:

- keep the object-store trait synchronous
- keep `xtask rc-local` in `xtask`, not `loon-ops`
- do not widen the shell into a workflow runner in this phase
- do not wake up `loon-cli`, `loond`, HTTP, or `loon-macos`
- `ops smoke` remains narrow and unchanged
- real-provider smoke stays documented/manual rather than auto-run by `rc-local`

Exit criteria:

- one canonical `xtask rc-local --config <path> --namespace <id>` path exists
- one canonical provider-backed smoke path exists
- local operability does not depend on test-only wiring

## What success looks like after this roadmap

At the end of this roadmap phase:

- the repo has a real local operability substrate
- `xtask ops ...` is the current frontend
- `xtask rc-local` is the canonical repo RC path
- `loon-ops` owns the shared shell contract
- `loon-cli` is still quarantined, but its eventual handoff path is explicit
- the project can return to client behavior work without losing a clear local operator path
