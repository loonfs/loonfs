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
- `loon-server::mutation`
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

Current quarantined delivery surfaces:

- `loon-cli`
- the `loond` binary shell
- `loon-server` HTTP/app placeholders
- `loon-macos`

These quarantined surfaces stay in the repository to preserve delivery intent and crate names, but
they should not advertise themselves as active product entrypoints until they wrap real behavior.

## What should happen early

The repo should accumulate:

- more fixtures
- more invariants
- more model transitions
- better rendered traces

before it accumulates a large amount of production code.
