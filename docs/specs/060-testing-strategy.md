# Spec 060: testing strategy

Testing is a product feature in this repository.

The goal is not only “high coverage.”
The goal is a test system that helps engineers and product reviewers understand behavior clearly.

## Design principle

Build the product so it is naturally testable.

Concretely, that means:

- strict state machines
- deterministic clocks and RNGs
- injectable filesystem, object-store, timer, and network boundaries
- explicit invariants
- narrow and broad test layers separated on purpose

## Test layers

### 1. Scenario fixtures

Human-readable YAML cases under `tests/scenarios/`.

Why they exist:
product and engineering should be able to talk about the same behavior using the same artifact.

Example:
`delete_then_stale_local_edit.yaml`.

### 2. Reference model

`crates/loon-model/` is a pure, side-effect-free state machine.

Why it exists:
it provides a small semantic truth we can compare the implementation against.

### 3. Differential tests

Drive the same operation stream through the reference model and `loon-core`.

Why it exists:
it catches logic drift.

### 4. Deterministic simulator

`crates/loon-sim/` will eventually run server and client logic under a single deterministic scheduler with injected faults.

Why it exists:
concurrency bugs are hard to find with normal tests and hard to debug without determinism.

### 5. Native and provider conformance

Slower tests against real providers and platform layers.

Why they exist:
mocks can drift from reality.

## Required output for failing randomized tests

- seed
- scenario name
- invariant name
- rendered trace
- minimized case when available

## Invariant expectation semantics

For fixtures that use `expect.invariants`:

- each listed invariant name is a stable ID, not free-form commentary
- the harness must evaluate that invariant explicitly for the scenario under test
- the fixture passes only when every listed invariant evaluates `passed = true`
- for differential harnesses, model and implementation must agree on the pass/fail outcome for the
  same invariant name, not only on the final state snapshot

The current executable-invariant rollout is:

- slice 1: namespace-core commit/apply, WAL replay, and checkpoint-plus-WAL replay invariants
- slice 2: background-work progress publication, queue shard mutation/repair, broker-worker lease
  flow, and checkpoint head-publish invariants
- slice 3: file content-object invariants across immutable upload and durable-content validation

Checkpoint immutable-object and client invariants are deferred to later slices.

## Rule for PM-friendly tests

If a scenario matters to product behavior, it should have a readable fixture and a rendered output, not only a Rust unit test.
