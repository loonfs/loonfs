# Spec 080: repository and delivery plan

This repository is intentionally scaffolded around **large bodies of work** rather than around one giant application crate.

## Why the workspace is split

- `loon-types` isolates shared vocabulary
- `loon-objectstore` isolates provider assumptions
- `loon-core` owns canonical metadata rules
- `loon-model` owns pure semantics for tests
- `loon-queue` isolates rebuildable background coordination
- `loon-sim` owns determinism and failure injection

This split is not about micro-crates for their own sake. It is about review boundaries and test boundaries.

## Expected workflow

A team should be able to pick one workstream at a time:

- provider contract
- core metadata rules
- model/simulation
- server shell
- client shell

Current delivery order:

- implement the semantic core before widening shells and adapters
- treat review boundaries inside crates as equally important as crate boundaries
- prefer deleting or quarantining placeholder surfaces over expanding them
- use `docs/roadmap/020-semantic-core-reset.md` as the current execution-order document

Current real delivery surfaces:

- `loon-core`
- `loon-model`
- `loon-objectstore`
- `loon-ops`
- `loon-server::mutation`
- `loon-server::ops`
- `loon-client`
- `loon-testkit`
- `xtask`

Current operator-facing recovery shell:

- `xtask conflict-list <namespace_id> --db <path> --store-root <path>`
- `xtask conflict-list <namespace_id> --db <path> --store-root <path> --all`
- `xtask conflict-list <namespace_id> --db <path> --store-root <path> --archived`
- `xtask conflict-show <namespace_id> <conflict_id> --db <path> --store-root <path>`
- `xtask conflict-restore <namespace_id> <conflict_id> --db <path> --store-root <path> --to <path>`
- `xtask conflict-archive <namespace_id> <conflict_id> --db <path> --store-root <path>`
- `xtask conflict-unarchive <namespace_id> <conflict_id> --db <path> --store-root <path>`

Constraints for that shell:

- it requires an existing client SQLite DB path and an existing local-fs object-store root
- it may refresh only the local `conflict_artifacts` and `conflict_artifact_archives` caches
  during discovery
- restore is out-of-band and does not rebind recovered content into sync state
- archive state is canonical in object storage via per-artifact sidecars
- there is still no destructive delete/GC lifecycle

Current local operability shell:

- shared command/config/rendering layer in `loon-ops`
- thin active frontend in `xtask ops ...`
- current commands:
  - `xtask ops bootstrap-namespace --config <path> --namespace <id> [--allow-existing]`
  - `xtask ops show-namespace-state --config <path> --namespace <id>`
  - `xtask ops show-client-state --config <path> --namespace <id>`
  - `xtask ops import-remote-observations --config <path> --namespace <id>`
  - `xtask ops observe-local --config <path> --namespace <id> --path <path>`
  - `xtask ops sync-once --config <path> --namespace <id>`
  - `xtask ops smoke --config <path> --namespace <id>`

Constraints for that shell:

- `xtask` stays a thin wrapper; config loading and command execution belong in `loon-ops`
- namespace bootstrap and authoritative-state inspection belong in supported library code,
  primarily `loon-server::ops`
- full-namespace authoritative remote observation import remains a supported library path in
  `loon-ops`, and `xtask ops import-remote-observations` must call that API verbatim rather than
  re-implementing it
- local observation of one existing file remains a supported client/library path first, and
  `xtask ops observe-local` is only a thin adapter over that path
- `xtask ops sync-once` is intentionally single-step and executor-only
- the shell is intentionally narrow in the current phase; there is still no workflow runner,
  recursive scan, watcher, delete inference, rename inference, or `sync-until-idle`
- `ops smoke` remains bootstrap/inspection-only and does not compose the import path yet
- future `loon-cli` work must reuse the `loon-ops` command contract rather than fork it

Current local RC path:

- `xtask rc-local --config <path> --namespace <id>`

Constraints for that path:

- it is repo automation owned by `xtask`, not part of the `loon-ops` command contract
- it runs the strict baseline:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
  - `cargo test -p loon-objectstore --test conformance`
  - the existing `loon-ops` smoke command path
- real-provider validation remains documented/manual rather than auto-run by `rc-local`
- future `loon-cli` activation still reuses `loon-ops`; it does not inherit `rc-local`

Current delivery gates:

- object-store contract changes require the local FS conformance suite in-repo
- object-store contract changes also require the external AWS S3 and Cloudflare R2 conformance jobs
  documented in `docs/runbooks/provider-conformance.md`
- no provider CI workflow config lives in-repo; only the contract, path filters, commands, and env
  requirements are tracked here

Current quarantined delivery surfaces:

- `loon-cli`
- the `loond` binary shell
- `loon-server` HTTP/app placeholders
- `loon-macos`

These quarantined surfaces stay in the repository to preserve delivery intent and crate names, but
they should not advertise themselves as active product entrypoints until they wrap real behavior.

For `loon-cli`, the intended activation path is now explicit: it should become a frontend over
`loon-ops`, not a second owner of config parsing, command grammar, or rendering semantics.

## What should happen early

The repo should accumulate:

- more fixtures
- more invariants
- more model transitions
- better rendered traces

before it accumulates a large amount of production code.
