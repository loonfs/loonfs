# Roadmap 020: semantic-core reset and execution order

This roadmap is the current execution order for the repository.

It is a course correction after the initial foundation work. The repo now has enough object-store,
WAL, checkpoint, queue, and client scaffolding that the main risk is no longer "missing shape."
The main risk is "missing semantic center."

The current priority is therefore:

1. stop widening the surface area
2. implement the canonical metadata engine
3. prove it with model-vs-core tests
4. split the worst monoliths without changing behavior
5. resume feature expansion only after the semantic core is real

## Guardrails

Until Milestone 1 exits:

- do not add new top-level crates
- do not add new mutation kinds
- do not add more platform shell work in `loon-cli`, `loon-server`, or `loon-macos`
- do not treat string-named invariants as proof of correctness
- do not let `LocalFsStore` behavior define provider guarantees

This does not ban all work outside `loon-core`. It means all work should either:

- directly support the semantic core
- improve the test product surface around the semantic core
- reduce structural debt in modules that already exist

## Current status

The repo has strong partial foundations:

- Workstream A is materially in place: object-store contract, local FS, S3/R2 adapters, and
  conformance coverage exist
- Workstream B exists at the control-plane level: head, lease, WAL, and checkpoint machinery are
  present
- Workstream C has real fixtures and differential harnesses
- Workstream D has durable queue/progress primitives
- Workstream E has real client durable state and create/edit happy-path flows

The blocking gap is that metadata commits are still mostly validated and replayed at the envelope
level rather than applied against a canonical inode/direntry/revision/tombstone state machine.

## Milestone 1: canonical metadata engine

Goal:
make namespace commits meaningful by evaluating and applying metadata semantics, not just commit
frames.

Primary crates:

- `loon-model`
- `loon-core`
- `loon-server`

Read first:

- Spec 030
- Spec 035
- Spec 040
- Spec 090 decisions 1, 2, 4, 6, and 7
- ADRs 0002 through 0005

Deliverables:

- typed metadata state model for:
  - inode records
  - direntry bindings
  - revision heads/history
  - subtree tombstones
- executable evaluation of:
  - `HeadSeqIs`
  - `InodeRevisionIs`
  - `ChildNameAbsent`
  - `AncestorsNotSubtreeDeleted`
- canonical application logic for:
  - `create_dir`
  - `create_file`
  - `replace_file`
  - `rename`
  - `delete_subtree`
  - `restore_revision`
- WAL replay that reconstructs metadata state, not only head-summary progression
- differential tests that compare metadata results across `loon-model` and `loon-core`

Exit criteria:

- `build_commit_plan()` no longer treats metadata preconditions as opaque carried data
- one namespace can replay checkpoint + WAL tail into equivalent metadata state in both
  `loon-model` and `loon-core`
- create/replace/rename/delete/restore each have at least one readable scenario and one
  model-vs-core differential check

## Milestone 2: structural decomposition of current monoliths

Goal:
make the existing crate boundaries real review boundaries.

Primary crates:

- `loon-client`
- `loon-model`
- `loon-types`

Deliverables:

- split `crates/loon-client/src/state_db.rs` by responsibility
- split `crates/loon-client/src/executor.rs` by responsibility
- split `crates/loon-model/src/lib.rs` into semantic domains
- split `crates/loon-types/src/lib.rs` into shared vocabulary domains

Required rule:

- these are behavior-preserving refactors unless a semantic-core slice explicitly requires more
  than that

Exit criteria:

- no core file over roughly 700-900 lines without a clear reason
- planner logic, side-effect execution, and state-application logic are separately reviewable in
  `loon-client`
- `loon-model` is no longer a kitchen-sink single file

## Milestone 3: testing product surface

Goal:
make the testing strategy described in the specs real tooling instead of aspiration.

Primary crates:

- `loon-testkit`
- `xtask`

Deliverables:

- shared fixture loading, temp-dir helpers, and trace helpers in `loon-testkit`
- `xtask render-case` emits reviewer-friendly traces
- `xtask replay-seed` can rerun deterministic failures
- `xtask minimize-case` can shrink scenario reproductions
- reproducible snapshot generation for rendered scenario traces

Exit criteria:

- failing randomized or differential cases can report:
  - seed
  - scenario name
  - invariant
  - rendered trace
  - minimized reproduction when available
- test files stop duplicating local helper scaffolding across the repo

## Milestone 4: hardening and delivery hygiene

Goal:
remove misleading scaffolding and harden the first real sync paths.

Primary crates:

- `loon-client`
- `loon-objectstore`
- repo root delivery files

Deliverables:

- crash-safe local file apply path:
  - temp file in same directory
  - file sync
  - atomic rename
  - parent-directory sync where required
- explicit scheduler policy instead of only SQL ordering
- clearer object-store boundary for real providers
- quarantine or remove placeholder surfaces that are not yet supporting current work
- repo hygiene for CI/config/docs alignment

Exit criteria:

- remote apply does not write directly into the target file path without staging
- scheduler behavior is named and testable
- delivery docs match what actually exists in the repo

## Milestone 5: client remote discovery and reconciliation

Goal:
resume feature expansion narrowly by teaching the client to ingest authoritative remote state that
did not originate from its own immediate mutation-response loop.

Primary crates:

- `loon-client`
- `loon-model`

Deliverables:

- remote-only authoritative inode discovery persisted into SQLite instead of being ignored
- the first remote-only file materialization path from durable remote metadata into local mirror
  state
- later remote observation convergence that works even when the authoritative success response was
  lost
- durable conflict/error surfacing for discovery/reconciliation failures
- continued work toward the transfer-ledger pieces from Workstream E once remote discovery is real

Required rule:

- resume feature expansion only by extending already-real client truth and execution paths; do not
  widen placeholder delivery surfaces again

Exit criteria:

- an unmatched authoritative file observation can survive restart as durable client state
- the mixed client tick can materialize that discovered remote-only file into a converged bound
  inode
- client-side remote discovery failures are named and durable enough to debug

## Milestone 6: bounded transfer execution and file-focused reconciliation hardening

Goal:
finish the transfer-ledger half of Workstream E by turning the current ledgers into bounded,
restart-safe execution state instead of "resume metadata," and harden late authoritative file
observations while transfers or pending requests still exist.

Primary crates:

- `loon-client`
- `loon-model`

Deliverables:

- explicit one-block-per-tick execution for:
  - `download_remote_edit`
  - `upload_local_edit`
  - `upload_local_create`
- durable reset visibility for transfer restarts using existing issue tables
- file-focused late authoritative observation rules that do not silently clear active transfer
  ledgers or pending mutation rows
- restart-safe multi-tick readable fixtures that prove progress persists across process restarts

Required rules:

- keep the existing transfer table shape from schema v10; do not add new transfer tables unless a
  later slice proves they are required
- stay file-focused; remote rename/delete hierarchy reconciliation remains out of scope for this
  phase
- do not widen delivery surfaces while this transfer hardening work is underway

Exit criteria:

- each transfer-backed executor can return a durable "progressed" step without finishing the whole
  file in one tick
- stale transfer state is reset conservatively and leaves one durable named issue row
- a later authoritative file observation never silently discards an active transfer ledger or
  pending mutation row
- at least one readable multi-tick download case and one readable multi-tick upload case require
  restart to finish and still converge correctly

## Milestone 7: authority, ordered multi-op commits, and SQLite hardening

Goal:
pause further feature expansion and close the blocking correctness gaps that remain after the
semantic core and bounded transfer work.

Primary crates:

- `loon-server`
- `loon-core`
- `loon-client`
- `loon-testkit`

Deliverables:

- authoritative server-side mutation execution that reconstructs its metadata basis from verified
  checkpoint state plus contiguous WAL tail instead of accepting caller-supplied metadata
- explicit same-request operation ordering for multi-op commits, preserved through WAL, replay, and
  checkpoint materialization
- materially hardened SQLite schema constraints, indexes, and migration-ladder coverage
- more productized scenario fixtures with typed top-level metadata and batch validation tooling

Required rules:

- do not add new mutations, new delivery shells, or new provider assumptions while these blockers
  remain open
- keep invariant wording unchanged in this tranche; executable invariant evaluation is deferred
- active crates must not export public placeholder surfaces

Exit criteria:

- `execute_client_mutation()` can no longer be called with caller-supplied metadata basis
- replay and checkpoint restore preserve ordered same-seq multi-op semantics exactly
- the client DB rejects structurally invalid state earlier through schema constraints and tested
  migrations
- scenario fixtures have explicit top-level kind/version metadata and batch validation tooling

## Milestone 8: executable invariants

Goal:
turn the already-named invariants into executable checks in the harnesses before
resuming broader feature work.

Primary crates:

- `loon-testkit`
- `loon-core`
- `loon-model`

Deliverables:

- ordered Milestone 8 rollout:
  - slice 1: namespace-core commit/apply, WAL replay, and checkpoint-plus-WAL replay invariants
  - slice 2: background-work progress publication, queue shard mutation/repair, broker-worker lease flow, and checkpoint head-publish invariants
  - slice 3: file content-object invariants
  - slice 4: checkpoint immutable-object invariants
  - slice 5: client transfer and reconciliation invariants, starting with file transfers only
- structured pass/fail invariant reports in rendered traces and checked-in snapshots
- fixture `expect.invariants` semantics tightened so each listed name must evaluate `passed = true`
- model-vs-core differential checks that compare invariant outcomes as well as final state

Required rules:

- keep runtime `checked_invariants` strings unchanged for compatibility in this milestone
- keep fixture schema unchanged; add executable meaning to existing `expect.invariants` lists
- land invariant families in the rollout order above instead of widening all harnesses at once

Exit criteria:

- every native namespace-core, background-work, and file-content fixture that lists invariants is
  backed by executable evaluation in the harnesses
- traces and snapshots show invariant pass/fail details, not just string presence
- model/core differential harnesses fail when invariant outcomes diverge even if final state still
  matches

## Historical slice order

Milestones 1 through 7 were executed through the following ordered slices.

### Slice 1: metadata state contract

Update:

- `docs/specs/030-namespace-metadata.md`
- `docs/specs/040-namespace-commit.md`

Add:

- authoritative metadata tables and state views for inode, direntry, revision, and tombstone
- exact precondition lookup rules
- named failure modes for precondition violations

Why first:

- the repo currently has typed commit intents without the state model that makes them evaluable

### Slice 2: pure metadata model

Update:

- `crates/loon-model/`
- `tests/scenarios/`

Add:

- minimal pure metadata state
- evaluation of `ChildNameAbsent`, `InodeRevisionIs`, and `AncestorsNotSubtreeDeleted`
- first create/replace fixtures and model tests

Why second:

- this gives a small semantic truth before touching `loon-core`

### Slice 3: canonical core metadata evaluator

Update:

- `crates/loon-core/`
- `crates/loon-server/src/mutation.rs`

Add:

- metadata evaluator module in `loon-core`
- real precondition evaluation in commit planning/application
- create-dir and create-file state application

Why third:

- this is the smallest step that makes the existing commit path meaningful

### Slice 4: metadata WAL replay

Update:

- `crates/loon-core/src/wal.rs`
- `crates/loon-model/`

Add:

- replay that rebuilds metadata state from WAL entries
- differential tests comparing metadata replay, not just head-summary replay

Why fourth:

- without this, checkpoint + WAL correctness is still only partially proven

### Slice 5: replace/rename/delete/restore completion

Update:

- `docs/specs/040-namespace-commit.md`
- `crates/loon-model/`
- `crates/loon-core/`
- `tests/scenarios/`

Add:

- full op semantics for replace, rename, subtree delete, and restore
- differential tests for each mutation family

Why fifth:

- this closes the major semantic gap before broader client or platform work resumes

## What not to do during these slices

Do not spend the next cycle on:

- new HTTP routes
- new CLI features
- more macOS shell work
- additional queue work that is not needed by metadata correctness
- client feature breadth beyond what is needed to support semantic-core validation

## What success looks like

At the end of this roadmap phase:

- the repo has a real canonical metadata engine
- commits and WAL replay are evaluated against that engine
- tests prove metadata equivalence across model and core
- file and module boundaries become smaller and more reviewable
- future feature work can build on a semantic core instead of on envelope-level scaffolding
