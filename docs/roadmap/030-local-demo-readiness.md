# Roadmap 030: local operability substrate and future CLI handoff

This roadmap keeps the existing path for link stability, but the goal is a **local operability substrate**:

- explicit config for provider, client, and server settings
- supported namespace bootstrap and authoritative-state inspection
- supported translation from authoritative namespace state into client-applicable
  `ObservedRemoteInode` values
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
- thin `xtask ops ...` commands:
  - `bootstrap-namespace`
  - `show-namespace-state`
  - `show-client-state`
  - `smoke`

Required rules:

- `xtask` stays thin and must not become the owner of config parsing or business logic
- `loon-ops` is the shared shell core that future `loon-cli` work must reuse
- no `demo-*` command family
- no observe/sync orchestration in this milestone
- no ambient provider env lookup in core crates
- bootstrap fails closed by default if the namespace already exists
- the shell output stays stable and human-readable

Command contract:

```bash
cargo run -p xtask -- ops bootstrap-namespace --config ./loondb-demo.toml --namespace demo
cargo run -p xtask -- ops show-namespace-state --config ./loondb-demo.toml --namespace demo
cargo run -p xtask -- ops show-client-state --config ./loondb-demo.toml --namespace demo
cargo run -p xtask -- ops smoke --config ./loondb-demo.toml --namespace demo
```

Exit criteria:

- one developer can point `xtask ops` at `local-fs`, AWS S3, or Cloudflare R2 using the same
  config shape
- namespace bootstrap and authoritative-state inspection no longer require test helper code
- client-state inspection is available through the same shell contract
- future `loon-cli` work can reuse `loon-ops` instead of re-implementing config and renderers

## Milestone 16: provider-backed RC hardening

Goal:

Make the operability substrate honest enough to support a narrow provider-backed local RC path.

Primary crates:

- `loon-objectstore`
- `loon-ops`
- `xtask`

Deliverables:

- reusable Tokio runtime ownership in the S3/R2 adapter
- one narrow RC path that runs:
  - format/checks
  - workspace tests
  - object-store conformance
  - `xtask ops smoke`
- documented real-provider smoke steps for AWS S3 first, then Cloudflare R2

Required rules:

- keep the object-store trait synchronous
- do not widen the shell into a workflow runner in this phase
- do not wake up `loon-cli`, `loond`, HTTP, or `loon-macos`

Exit criteria:

- repeated S3/R2 calls do not build a fresh Tokio runtime per call
- one canonical provider-backed smoke path exists
- local operability does not depend on test-only wiring

## What success looks like after this roadmap

At the end of this roadmap phase:

- the repo has a real local operability substrate
- `xtask ops ...` is the current frontend
- `loon-ops` owns the shared shell contract
- `loon-cli` is still quarantined, but its eventual handoff path is explicit
- the project can return to client behavior work without losing a clear local operator path
