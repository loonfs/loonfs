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

## What should happen early

The repo should accumulate:

- more fixtures
- more invariants
- more model transitions
- better rendered traces

before it accumulates a large amount of production code.
