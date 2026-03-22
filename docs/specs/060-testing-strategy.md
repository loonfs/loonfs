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

`crates/loon-sim/` owns the shared deterministic scheduler runtime, delivery ordering, restart
events, and fault vocabulary.

The current shared simulator surface includes:

- `SimRuntime`
- `SimActorId`
- `SimDelivery`
- `SimTraceEvent`
- `FaultPlan`

Current once-only client fault kinds used by `loon-client` and `loon-testkit` are:

- `crash_after_step_once`
- `store_error_once`
- `dispatch_error_once`
- `local_apply_error_once`

Current scheduling rule:

- delivery order is sorted by `(deliver_at_ms, delivery_id)`
- actor steps are driven by the readable fixture action stream, not ambient timing
- traces are rendered from structured sim events, not ad hoc harness-local string logs

`tests/scenarios/sim/` is now the shared home for multi-actor interleaving fixtures.

`loon-testkit` owns the actor-family harnesses layered on top of the shared scheduler:

- queue broker/worker/repair interleavings
- client/server request-response-observation interleavings
- background writer/checkpoint/progress/repair interleavings

The hardening rule is that once-only injected faults must still produce a deterministic rendered
trace and a retryable postcondition.

Why it exists:
concurrency bugs are hard to find with normal tests and hard to debug without determinism.

### 5. Native and provider conformance

Slower tests against real providers and platform layers.

Why they exist:
mocks can drift from reality.

## Client crash/restart rule

Every multi-step client path must name its durable boundary checkpoints.

Retry is required to accept already-applied winner postconditions when the filesystem or object
store is already in the intended final state, even if SQLite did not advance before the crash.

Current required client crash families include:

- dispatch response received before SQLite apply
- local rename/delete/materialize/download finalize before SQLite apply
- conflict artifact and archive sidecar written before cache update
- subtree restore staged before final rename

Restore must satisfy an explicit absent-or-complete rule for the caller-provided destination.

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
- slice 4: checkpoint immutable-object invariants for checkpoint builder manifest/segment outputs
- slice 5a: client transfer invariants for file download/upload flows
- slice 5b: client reconciliation invariants for late authoritative observation, remote-only
  discovery, and remote-only directory materialization
- slice 6: scheduler-backed sim invariants for delayed response retry reuse, duplicate response
  idempotence, late observation ordering, and seed-stable traces
- slice 7: scheduler-backed background sim invariants for stale writer fencing after handover,
  checkpoint publish wait/monotonicity under interleaving, latest-visible-head snapshot repair,
  and seed-stable background traces

Provider conformance cases now live under `tests/conformance/objectstore/`, and the real-provider
AWS S3 plus Cloudflare R2 runs are required as an external CI gate documented in
`docs/runbooks/provider-conformance.md`.

## Rule for PM-friendly tests

If a scenario matters to product behavior, it should have a readable fixture and a rendered output, not only a Rust unit test.
