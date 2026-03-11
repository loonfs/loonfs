# Roadmap 010: foundation workstreams after the major decisions are locked

This roadmap turns `docs/specs/090-major-implementation-decisions.md` into concrete work packages that different engineers or agents can pick up safely.

## Workstream A: object-store contract and control objects

Primary crates:
- `loon-objectstore`
- `loon-types`

Read first:
- Spec 020
- Spec 090 decisions 1, 3, and 5
- ADRs 0009, 0010, and 0011

Deliverables:
- object key builders for head, lease, WAL, checkpoints, derived progress, and queue shards
- JSON control-object types with explicit `format_version`
- CBOR WAL envelope type and checksum helpers
- local filesystem adapter
- conformance harness that S3 and R2 must pass

Exit criteria:
- control-object CAS helpers exist
- immutable object create-if-absent helpers exist
- at least the local provider passes the conformance suite

## Workstream B: namespace head, WAL, and checkpoint skeleton

Primary crates:
- `loon-core`
- `loon-types`

Read first:
- Spec 030
- Spec 040
- Spec 090 decisions 1, 2, 4, 5, 6, and 7

Deliverables:
- `HeadState` and `LeaseState` durable types
- commit validation skeleton with explicit preconditions
- WAL commit writer/reader
- checkpoint manifest and segment reader/writer skeletons
- invariants for replay, id allocation, and restore semantics

Exit criteria:
- one namespace can accept a metadata commit and replay it
- checkpoint read plus WAL tail replay reproduces current state

## Workstream C: pure model and differential tests

Primary crates:
- `loon-model`
- `loon-testkit`

Read first:
- Spec 060
- Spec 090 decisions 4, 6, 7, 8, and 9

Deliverables:
- pure namespace model that matches the accepted semantics
- YAML scenario loader that renders traces clearly
- at least one model-vs-core differential harness
- scenario coverage for rename, subtree delete, restore revision, and mount crossing

Exit criteria:
- failing randomized cases print seed, invariant, and rendered trace
- at least ten readable fixtures exist

## Workstream D: background work and progress objects

Primary crates:
- `loon-queue`
- `loon-core`

Read first:
- Spec 050
- Spec 090 decisions 1 and 8

Deliverables:
- queue shard state with broker lease embedded in the same durable object
- `BuildSnapshot` job class
- monotonic `progress.json` publication helpers
- repair logic that recreates lost enqueue from canonical head state

Exit criteria:
- duplicate or lost jobs do not affect correctness
- readers can prove whether a derived output is safe to use

## Workstream E: client local truth and transfer ledger

Primary crates:
- `loon-client`
- `loon-types`

Read first:
- Spec 070
- Spec 090 decisions 7, 9, and 10

Deliverables:
- SQLite schema migrations
- planner transaction boundaries
- block transfer ledger for large uploads/downloads
- durable conflict/error records

Exit criteria:
- client can restart without guessing its prior sync state
- one hot-file scenario is captured as a readable fixture

## Rule for all workstreams

Before a workstream adds production code, it should add or update:

1. a readable spec if behavior changed
2. a fixture if the behavior is user-visible or protocol-visible
3. a model or invariant if the behavior affects correctness

That keeps the repo aligned with the original intent: behavior first, code second.
