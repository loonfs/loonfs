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
  - slice 5a: client file-transfer invariants
  - slice 5b: client reconciliation invariants for file observations and remote-only directory materialization
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

## Milestone 9: remote hierarchy reconciliation

Goal:
resume product-facing client work by teaching the client to reconcile authoritative remote path
changes for already-bound files without collapsing them into generic drift.

Primary crates:

- `loon-client`
- `loon-testkit`

Deliverables:

- the first bound-file remote rename observation path, including same-parent rename and moves
  between already-bound directories
- one executable `apply_remote_rename` planner/executor path that durably moves the local file
- durable failure surfacing for destination collisions and path-resolution failures
- reconciliation fixtures and executable invariant coverage for remote path-change handling

Required rules:

- stay file-only in this milestone's first slice; remote directory rename/delete reconciliation
  remains out of scope
- keep remote observations authoritative for durable metadata, but do not move files during
  `apply_remote_observation`
- fail closed when the destination slot is already occupied locally

Exit criteria:

- a bound-file authoritative path-only observation survives restart and plans `apply_remote_rename`
- the mixed client tick can apply that rename locally and return to `already_converged`
- destination collisions become durable named issue rows instead of generic executor failures

### Slice 2: bound-file remote delete observation

Goal:

- treat authoritative remote file deletion as real local unlink work instead of generic drift
- keep the remote tombstone durable after successful local delete
- make tombstoned remote-only state inert so it does not replan as a download

Update:

- `docs/specs/070-client-architecture.md`
- `crates/loon-client/`
- `tests/scenarios/client/`

Add:

- planner decision `apply_remote_delete`
- planner reasons for tombstoned authoritative observations
- durable local unlink executor and named failure issue rows
- reconciliation fixtures and invariants for successful delete, failure, and active-transfer
  preservation

Why next:

- remote rename and remote delete are the smallest file-only hierarchy reconciliation pair
- successful delete needs an explicit durable state shape before subtree delete work can be
  specified safely

Exit criteria:

- a bound-file tombstoned authoritative observation survives restart and plans `apply_remote_delete`
- the mixed client tick can unlink the local file, preserve the remote tombstone, and leave no
  local/anchor rows
- tombstoned remote rows without local/anchor become inert `no_op` planner state
- missing current-path failures become durable named issue rows instead of generic executor errors

### Slice 3: bound-directory remote subtree delete observation

Goal:

- treat authoritative remote directory deletion as one root-level local subtree remove instead of
  generic drift
- preserve only the observed root tombstone after successful local apply
- clear descendant durable client state instead of inventing synthetic descendant tombstones

Update:

- `docs/specs/070-client-architecture.md`
- `crates/loon-client/`
- `tests/scenarios/client/`

Add:

- planner decision `apply_remote_subtree_delete`
- planner reasons for tombstoned authoritative directory observations, including descendant-dirty
  and descendant-busy cases
- durable recursive local-remove executor and named failure issue rows
- reconciliation fixtures and invariants for successful subtree delete, failure, and deferred
  conflict handling

Why next:

- subtree delete is the next remote hierarchy gap after file rename and file delete
- it is still narrower than directory rename because it does not require descendant path rewrites
- it forces an explicit durable state shape for deleted bound directories before broader hierarchy
  reconciliation widens again

Exit criteria:

- a tombstoned bound directory with clean bound descendants survives restart and plans
  `apply_remote_subtree_delete`
- the mixed client tick can remove the local subtree, preserve only the root remote tombstone,
  clear descendant remote rows, and leave no local/anchor rows for the subtree
- tombstoned bound directories with dirty, placeholder, temp-identity, or busy descendants defer
  to conflict work instead of eagerly deleting
- missing current-path and recursive-remove failures become durable named issue rows instead of
  generic executor errors

### Slice 4: bound-directory remote rename/move observation

Goal:

- finish Milestone 9 for already-bound hierarchy reconciliation by treating authoritative
  bound-directory rename and move as explicit local work instead of generic drift
- keep the first slice strict by only handling clean, already-bound subtrees
- preserve descendant durable state instead of rewriting inode-keyed rows unnecessarily

Update:

- `docs/specs/070-client-architecture.md`
- `crates/loon-client/`
- `tests/scenarios/client/`

Add:

- planner decision `apply_remote_subtree_rename`
- planner reasons for root-local divergence, descendant divergence, descendant busy state, and
  unusable target parents
- durable root-level directory rename executor and named failure issue rows
- reconciliation fixtures and invariants for successful same-parent rename, successful move,
  occupied-destination failure, and deferred conflict handling

Why next:

- after file rename/delete and subtree delete, directory rename/move is the remaining already-bound
  hierarchy gap
- descendant rows are inode-keyed, so strict bound-only subtree rename can finish Milestone 9
  without inventing new descendant path-rewrite machinery

Exit criteria:

- a path-only authoritative observation for a clean bound directory subtree survives restart and
  plans `apply_remote_subtree_rename`
- the mixed client tick can durably rename or move the local subtree root and return to
  `already_converged`
- only the root path-view rows change on success; descendant durable rows remain unchanged
- occupied destinations and path-resolution failures become durable named issue rows, and unusable
  target parents or non-converged/busy descendants defer to conflict work instead of moving
  eagerly

## Milestone 10: mixed-state remote hierarchy convergence

Goal:
extend remote hierarchy reconciliation beyond the strict already-bound case by letting
authoritative rename/delete work converge through remote-only placeholders and materializable
target-parent chains.

Primary crates:

- `loon-client`
- `loon-testkit`

Deliverables:

- waiting-state planner reasons for remote-only placeholders and bound path changes whose parent
  chain is authoritative but not materialized yet
- one-directory-at-a-time remote-only directory materialization that no longer creates missing
  parents implicitly
- direct-child replanning after parent materialization or subtree rename so waiting child work
  becomes executable without a separate planner sweep
- mixed-state subtree rename/delete semantics that allow remote-only descendants while still
  deferring dirty, temp/local-only, or busy descendants
- readable fixtures and checked-in invariant artifacts for waiting parent materialization, file
  rename after parent materialization, subtree move after parent-chain materialization, and
  subtree delete clearing remote-only descendants

Required rules:

- target-parent creation remains a separate executable step; rename/delete executors never create
  missing parents inline
- `materialize_remote_dir` stays one-directory-at-a-time for these hierarchy flows
- remote-only descendants stop blocking subtree rename/delete, but dirty, temp/local-only, and
  busy descendants still block
- truly unusable target-parent chains still defer to `create_conflict_copy`

Exit criteria:

- remote-only file and directory placeholders wait with named `no_op` reasons until their parent
  directory is locally usable
- a bound-file authoritative move can wait for target-parent materialization, then replan to
  `apply_remote_rename` and converge without manual cleanup
- a bound-directory authoritative move can wait for a materializable remote-only parent chain, then
  replan to `apply_remote_subtree_rename` while preserving remote-only descendant placeholders
- subtree delete clears remote-only descendant placeholder rows instead of deferring on them
  automatically

## Milestone 11: conflict taxonomy and stable-path artifact resolution

Deliverables:

- explicit file conflict classes for same-inode stale-base edit, path binding collision,
  delete-vs-edit, and rename-vs-edit
- explicit subtree conflict classes for subtree delete-vs-local-changes and subtree
  rename-vs-local-changes
- canonical immutable conflict artifact objects under the namespace keyspace
- executable client decisions for file conflict resolution and direct remote
  rename-and-replace apply
- executable subtree conflict resolution decisions that preserve the loser subtree before applying
  the authoritative winner
- durable client cache/indexing for created conflict artifacts
- library-first discovery and explicit restore APIs for file and subtree conflict artifacts
- `xtask` operator commands for conflict artifact list/show/restore against explicit local client
  DB and local-fs object-store roots
- readable fixtures and executable invariants proving loser preservation and canonical-path
  stability

Required rules:

- `stable_paths` is the v1 default policy
- authoritative namespace commit remains strict CAS for stale-base writes
- file conflict resolution preserves the loser as an artifact instead of renaming the canonical
  winner
- subtree conflict resolution preserves the full loser subtree as a deterministic artifact instead
  of creating a visible sibling tree
- conflict artifact restore is out-of-band recovery into an explicit caller destination and does
  not mutate sync planner state
- the first operator-facing shell is `xtask`, using explicit `--db` and `--store-root` paths and
  binary `active`/`archived` lifecycle sidecars instead of envelope rewrites or destructive GC
- `create_conflict_copy` remains only for busy descendants, target-parent-unusable subtree rename,
  and still-unsupported future hierarchy classes after the stable-path taxonomy cleanup

## Milestone 12: client crash/restart hardening

Lock one client rule:

- every multi-step client path names durable boundary checkpoints
- retry accepts already-applied filesystem or object-store winner postconditions when they already
  match the intended final state

Current hardening coverage includes:

- dispatch response crash windows before SQLite apply
- authoritative rename retry accepting an already-moved local winner
- conflict artifact write crash windows before cache update
- archive sidecar create crash windows before cache update
- subtree restore staging crash windows with absent-or-complete retry behavior

## Milestone 13: provider conformance hardening

Expand the active provider contract and the conformance proof surface to cover:

- overwrite visibility and head freshness
- delete idempotence
- compare-and-swap rejection on missing objects
- sorted listing
- traversal rejection across all trait methods
- scoped key-prefix isolation

Project policy after this milestone:

- local FS conformance remains in-repo
- AWS S3 and Cloudflare R2 real-provider conformance is a required external CI gate
- no provider workflow YAML is committed in-repo; the contract lives in docs and tests

## Milestone 14: scheduler-backed deterministic multi-actor simulation

Deliverables:

- `loon-sim` owns a real deterministic scheduler runtime instead of only a fault vocabulary
- queue `sim` fixtures run through the shared scheduler shell with structured actor/time trace
  events
- first client/server `sim` fixtures cover delayed response retry reuse, duplicate response
  idempotence, and remote-observation-before-response ordering
- the existing once-only client fault plan is consumable from `tests/scenarios/sim/`
- seed-stable sim traces are checked in as readable artifacts

Required rules:

- multi-actor interleavings live under `tests/scenarios/sim/`
- scheduler order is explicit and deterministic
- `loon-sim` owns scheduling and trace primitives; actor-family harnesses live above it in
  `loon-testkit`
- no second bespoke scheduler appears inside `loon-client` or `loon-queue`

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
