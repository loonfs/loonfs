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

Before touching `loon-server` (including its `objectstore`, `core`, or `queue` modules) or `loon-client`, read:

- `docs/specs/020-objectstore-contract.md`
- `docs/specs/040-namespace-commit.md`
- `docs/specs/050-background-work.md`
- `docs/specs/090-major-implementation-decisions.md`
- ADRs 0009 through 0015

These documents lock the high-leverage choices that are easiest to get wrong in a way that cascades through the codebase.

## 3. Development loop

For normal feature work, follow this order:

1. Update or add the readable spec under `docs/specs/`.
2. Add or update an ADR if the decision is architectural or hard to reverse.
3. Add a scenario fixture under `tests/scenarios/`.
4. Update the reference model in `crates/loon-model/`.
5. Implement the production code.
6. Add unit tests only after the higher-level behavior is captured.

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

- `docs/specs/`: readable contracts and examples
- `docs/adr/`: decisions that are hard to reverse
- `docs/runbooks/`: “how to debug X” guides
- `crates/loon-testkit/src/model/`: pure reference model for state-machine testing
- `crates/loon-server/src/core/`: canonical implementation of metadata rules
- `crates/loon-sim/`: deterministic scheduler and failure injection
- `tests/scenarios/`: human-readable input cases

## 6. How to add a new namespace mutation

Suppose you add a new mutation, such as “restore revision.”

You must do all of the following:

1. Describe the mutation in `docs/specs/040-namespace-commit.md`.
2. Document preconditions and failure modes.
3. Add a model transition in `crates/loon-testkit/src/model/`.
4. Add at least one scenario fixture.
5. Add or update invariants in `crates/loon-server/src/core/invariants.rs`.
6. Only then add the production implementation.

## 7. How to add a new object-store assumption

Example: “we need compare-and-swap update on small mutable control objects.”

Required steps:

1. Add the capability to `loon-server/src/objectstore` provider profiles.
2. Add a conformance test case.
3. Mark the provider expectation for S3 and R2.
4. Update the object-store contract spec.

Do not smuggle provider-specific behavior into `loon-server/src/core`.

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
