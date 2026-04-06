# AGENTS.md

This file is for both humans and automation. It describes how work should happen in this repository.

## 1. Mission

We are building a sync engine where **correctness, determinism, and testability are primary product features**.

The durable source of truth is object storage. Everything else is either compute, cache, or a rebuildable derivative.

## 2. Non-negotiable rules

1. Canonical metadata is inode-keyed. (not path-keyed)
2. Paths are derived views, never canonical identity.
3. Namespace commits are serialized within a namespace.
4. Background work is not authoritative.
5. Content must be durable before metadata can publish a revision.
6. Every new protocol must name its failure modes.
7. Deterministic tests are required for anything concurrency-adjacent.
8. Object storage provider behavior is assumed only after conformance tests pass.

## 2a. Required reading before changing core behavior

Before touching `loon-server`, `loon-core`, `loon-objectstore`, or `loon-client`, read:

- `README.md`
- `docs/specs/020-architecture-overview.md`
- `docs/specs/030-object-store-contract.md`
- `docs/specs/040-filesystem-and-storage-model.md`
- `docs/specs/050-write-read-protocol.md`
- `docs/specs/060-interfaces-and-clients.md`
- `docs/specs/080-background-jobs.md`
- `docs/specs/090-versioning-conformance-and-extensions.md`

These documents lock the high-leverage choices that are easiest to get wrong in a way that cascades through the codebase.

## 3. Development loop

For normal feature work, follow this order:

1. Confirm the relevant behavior in `docs/specs/` and `README.md`.
2. Update the reference model in `crates/loon-model/` when metadata semantics change.
3. Implement the production code to the current spec.
4. Add or update deterministic tests after the behavior is understood.
5. If the required behavior cannot fit the current spec, stop and escalate to the core team rather than inventing a local contract.

The point is simple: **the behavior should be explained before it is encoded**.

## 4. Testing policy

We follow a layered strategy:

- narrow, pure state-machine tests for semantic correctness
- broader deterministic simulation for crashes, reordering, and injected failures
- slower native/provider-conformance tests to keep mocks honest

Never hide flakes with retries. If a test is flaky, the bug is in the product or in the test harness.

Every randomized failure must print:

- seed
- scenario name
- commit hash if available
- minimized reproduction if available

## 5. What belongs where

- `README.md`: active product and operator guide
- `docs/specs/`: authoritative contracts and examples
- `docs/appendices/`: supporting matrices and reference material
- `crates/loon-model/`: pure reference model for metadata replay and semantic comparison
- `crates/loon-core/`: canonical implementation of metadata rules and replay
- `crates/loon-objectstore/`: provider contract, keys, and conformance behavior
- `crates/loon-testkit/`: shared test helpers

## 6. How to add a new namespace mutation

Suppose you add a new mutation, such as “restore revision.”

You must do all of the following:

1. Confirm the mutation in `docs/specs/050-write-read-protocol.md`, and update `docs/specs/040-filesystem-and-storage-model.md` if it changes visible resource semantics.
2. Document preconditions and failure modes.
3. Add or update the model behavior in `crates/loon-model/`.
4. Add or update invariants in `crates/loon-core/src/invariants.rs`.
5. Add deterministic tests for the new behavior.
6. Only then add the production implementation.

## 7. How to add a new object-store assumption

Example: “we need compare-and-swap update on small mutable control objects.”

Required steps:

1. Add the capability to `crates/loon-objectstore/`.
2. Add a conformance test case.
3. Mark the provider expectation for S3 and R2.
4. If the contract itself must change, stop and escalate to the core team so the spec can be reoriented before code diverges.

Do not smuggle provider-specific behavior into `loon-core`.

## 8. Commit and PR style

Always use [Conventional Commits](https://www.conventionalcommits.org/) for both commit messages and PR titles (e.g. `fix:`, `feat:`, `refactor:`, `test:`, `docs:`, `chore:`). Scope is optional but encouraged when the change is confined to a single crate (e.g. `fix(loon-client):`, `refactor(loon-server):`).

Prefer small commits. Good commit shapes:

- one spec + one fixture + one model change
- one provider capability + one conformance case
- one deterministic failure reproduction + one fix

Bad commit shapes:

- broad “sync engine refactor” commits
- commits that change semantics without updating docs
- commits that add state without naming invariants

## 9. Review checklist

Before merging, ask:

- Is the invariant easy to state?
- Is there a readable scenario for this behavior?
- Can a stale writer, worker, or client do something invalid now?
- Did we accidentally add ambient time, ambient randomness, or hidden concurrency?
- Does the code explain the same thing as the spec?

## 10. When in doubt

Make the simpler protocol stricter.

The project should prefer a smaller set of valid states over a wider, fuzzier protocol that is harder to reason about.
