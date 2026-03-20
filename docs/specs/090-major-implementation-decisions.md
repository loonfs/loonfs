# Spec 090: major implementation decisions to lock before broad development

## Purpose

This document exists to freeze the large technical choices that are easiest to get wrong in a way that cascades through the rest of the system.

Implementing engineers should treat the recommendations in this file as the project baseline. If one of these needs to change later, add or update an ADR first.

## Plain-language framing

A **checkpoint** is a complete snapshot of namespace metadata at one chosen `seq`.

Why it exists:
readers should not have to replay an unbounded amount of WAL history.

Example:
a namespace might have a checkpoint at `seq = 10_000`. A reader can start there and replay only later WAL entries.

Failure mode prevented:
slow restart and unsafe WAL retention.

A **run** is one immutable batch of sorted metadata files written together.

Why it exists:
object storage works best with immutable objects, not with in-place page updates.

Example:
a snapshot builder may write one inode run, one direntry run, one revision run, and one tombstone run for `seq = 10_000`.

Failure mode prevented:
partially rewritten metadata files.

---

## Decision 1: the durable replay model is `verified checkpoint + WAL tail`

### Problem

The repo already commits to a WAL plus snapshots. The large remaining choice is how those should interact.

If engineers treat “compaction” as “rewrite old WAL files into a new mutable WAL format,” the system becomes harder to reason about, harder to test, and much easier to corrupt.

### Recommendation

Treat the durable replay basis as:

- the latest **verified checkpoint** at or before the desired head
- plus the immutable WAL entries after that checkpoint

The authoritative mutation path must build its validation basis from that same durable replay
model. It must not accept caller-supplied metadata state as authority.

Do **not** make read correctness depend on a rewritten “compacted WAL.” In v1, compaction means “write a newer checkpoint and later advance retention,” not “mutate history.”

### Concrete storage shape

```text
/namespaces/{ns}/head.json
/namespaces/{ns}/lease.json

/namespaces/{ns}/wal/00000000000000000420-<commit_id>.cbor.zst

/namespaces/{ns}/snapshots/00000000000000000400/manifest.json
/namespaces/{ns}/snapshots/00000000000000000400/tables/inodes-00000.sst.zst
/namespaces/{ns}/snapshots/00000000000000000400/tables/direntries-00000.sst.zst
/namespaces/{ns}/snapshots/00000000000000000400/tables/revisions-00000.sst.zst
/namespaces/{ns}/snapshots/00000000000000000400/tables/tombstones-00000.sst.zst
```

### Publish rule

A checkpoint is usable only after all of the following are true:

1. every snapshot table object exists
2. the snapshot manifest exists
3. the builder verified row counts, key ranges, and checksums
4. `head.json` points at the checkpoint as the current `snapshot_hint`

### Retention rule

Only after a checkpoint is verified may background work propose a new `retention_floor_seq`.

The floor may advance only when:

- the checkpoint covers that seq or later
- the required derived progress objects cover that seq or later
- retention policy allows dropping older incremental replay

### Example

Suppose head is at `seq = 420` and the last checkpoint is `400`.

A reader does this:

1. read `head.json`
2. load checkpoint `400`
3. replay WAL `401..420`

A snapshot builder may later produce checkpoint `420`. Once that checkpoint is verified and published, the system can eventually stop promising incremental replay before `420`.

### Failure modes prevented

- readers depending on mutable compacted history files
- retention deleting WAL that is still needed
- partially published checkpoints becoming authoritative

### What is intentionally deferred

If object counts later become a problem, old raw WAL entries may be packed into **archive bundles** for cold history. That is not part of the normal read path in v1.

---

## Decision 2: checkpoints use immutable SSTable-like runs, not one giant snapshot object

### Problem

A checkpoint must support point lookups, directory-range scans, and revision-range scans. One giant object is simple to write once, but expensive to read and expensive to rebuild. Too many tiny objects are also a bad fit for object storage.

### Recommendation

Use immutable sorted runs per record family:

- inode run keyed by `inode_id`
- direntry run keyed by `(parent_inode_id, name_key)`
- revision run keyed by `(inode_id, revision_no)`
- tombstone run keyed by `inode_id`

Each run is split into segment objects. Each segment contains multiple pages plus a small page index.

### Initial physical defaults

These defaults are high level enough to be safe and concrete enough to start implementation:

- target segment size: about 64 MiB compressed
- target page size: about 1 MiB uncompressed
- compression: zstd
- manifest stores min/max key, row count, checksum, and page directory for each segment

### Why this is the right middle ground

- larger than one-row-per-object
- smaller than one-checkpoint-per-object
- easy to cache locally
- easy to range-read
- easy to rebuild deterministically

### Example

If a namespace has 8 million direntries, one checkpoint may emit many `direntries-xxxxx.sst.zst` objects, each with contiguous parent/name-key ranges. A listing read for one directory touches only the relevant segment and then only the relevant page range.

### Failure modes prevented

- pathological replay from one giant snapshot blob
- exploding object counts from tiny per-row objects
- future developers sneaking in mutable page updates to snapshot files

### Safe low-level decisions left to implementers

- exact page header encoding
- zstd compression level
- whether page indexes sit at the front or back of the segment

---

## Decision 3: use JSON for mutable control objects and manifests, CBOR+zstd for bulk immutable metadata

### Problem

Control objects need to be human-inspectable in production. Bulk metadata objects need to be compact, versioned, and stable across languages.

### Recommendation

Use:

- **JSON** for small mutable control objects and manifests
  - `head.json`
  - `lease.json`
  - queue shard state
  - snapshot manifest
  - derived progress objects
- **CBOR + zstd** for immutable WAL entries and checkpoint segments

Every stored object must have an explicit envelope with:

- `kind`
- `format_version`
- `created_by` or `writer_version`
- content checksum

### Example

A WAL object body should not be “whatever Rust `bincode` happened to serialize today.” It should be a versioned `WalCommitEnvelope` encoded into CBOR, then compressed.

### Failure modes prevented

- unreadable control-plane debugging during incidents
- unstable binary encodings leaking into durable storage
- future migrations that require reverse-engineering Rust-specific layouts

### Explicit anti-recommendation

Do not use `bincode` or any other Rust-implementation-defined encoding for durable objects. It is acceptable inside tests or in-memory paths. It is not acceptable for long-lived object-store data.

---

## Decision 4: in v1, one commit request maps to one namespace `seq`

### Problem

The spec allows batching in principle, but the broad question is whether the server should merge unrelated user requests into the same visible commit.

### Recommendation

Do not cross-batch authoritative namespace commits in v1.

The rule is:

- one user commit request may contain multiple operations
- that request publishes as one `seq` if it succeeds
- the vector position in `CommitRequest.ops` is the authoritative within-request order
- same-request order must stay durable through WAL `op_index`, metadata application, replay, and
  checkpoint materialization
- unrelated user requests do not share a `seq` in v1

### Why

This keeps:

- precondition failure obvious
- request idempotency simple
- audit trails readable
- seed repros understandable

### Example

If one client asks to rename inode 7 and another client asks to replace inode 42, those should become two visible commits, not one server-assembled super-commit.

### Failure modes prevented

- hard-to-explain request interaction
- confusing retries after partial batching logic
- developers accidentally turning the commit path into a queue system

### What is still allowed later

The server may eventually add **internal** group commit for lower-level I/O, but only if the published semantics remain identical to “one request, one visible commit.”

---

## Decision 5: the namespace head owns inode-id allocation

### Problem

New inodes need durable identifiers. Random ids are easy to generate, but they are noisy to debug and do not improve correctness once namespace writes are already serialized.

### Recommendation

Store `next_inode_id` in `head.json`. New inodes get monotonic `u64` ids within a namespace. The commit that creates the inode also consumes the ids it needs.

Other related rules:

- `revision_no` is a monotonic `u64` per inode
- namespace ids should be globally unique strings
- request ids and job ids should be time-sortable ids such as UUIDv7

### Example

If `head.json` says `next_inode_id = 501`, then a commit creating three new inodes allocates `501`, `502`, and `503` and publishes `next_inode_id = 504` in the new head.

### Failure modes prevented

- separate allocation side channels
- random id churn that makes traces unreadable
- hidden concurrency around id generation

---

## Decision 6: names use an explicit, versioned `NamePolicy`; do not rely on ambient filesystem rules

### Problem

The server and the macOS client must agree on when two sibling names collide. If one side uses a different normalization or case rule than the other, rename and conflict behavior become nondeterministic.

### Recommendation

Introduce a versioned `NamePolicy` now.

The first policy should be `macos_ci_v1` with these rules:

- preserve the original `display_name` exactly
- compute `name_key` by applying Unicode NFC normalization and full case folding
- pin the Unicode tables used by the implementation to an explicit version shipped in the repo
- reject sibling names whose computed `name_key` collides

### Why this is better than using host APIs directly

Host filesystem APIs are useful for validation, but they should not define canonical metadata semantics. Canonical semantics must be deterministic and shared between client and server.

### Example

If two proposed names differ only by case or normalization but map to the same `name_key`, the second create or rename fails with a deterministic conflict.

### Failure modes prevented

- client/server disagreement about valid directory contents
- path lookup bugs that appear only on one machine
- difficult-to-reproduce Unicode edge cases

### Safe low-level decisions left to implementers

- the exact Rust crates used for normalization and case folding
- how `NamePolicy` is represented in wire types

---

## Decision 7: file content uses fixed 16 MiB plaintext blocks, SHA-256, and per-namespace dedup in v1

### Problem

The repo already leans toward 16 MiB blocks, but the large remaining questions are: fixed vs variable chunking, digest choice, and dedup scope.

### Recommendation

For v1:

- block size is fixed at 16 MiB, except for the final partial block
- canonical block identity is SHA-256 of the plaintext block bytes
- content manifests list block digests in order plus the whole-file size and whole-file hash
- dedup scope is **per namespace**, not global and not per account

### Why fixed-size blocks first

Fixed-size chunking is easier to implement, easier to test, and good enough for the first correctness-focused milestone. Content-defined chunking is a later optimization, not a baseline requirement.

### Large-file capture rule

A file revision always represents one stable byte snapshot.

That means:

- capture from a local snapshot when possible
- otherwise stage a stable copy before publish
- if the source changes while upload is in flight, finish the captured revision and queue a later revision if needed

Do not repoint an in-flight manifest to a moving source file.

### Parallel transfer rule

Blocks may be uploaded and downloaded in parallel. Publish still happens only after every referenced block and the manifest are durable.

### Why dedup is namespace-scoped

Per-namespace dedup keeps GC, retention, privacy, and future cross-domain federation much simpler.

### Failure modes prevented

- dangling revisions pointing at missing content
- starvation on hot files without a stable capture policy
- global dedup tangling GC and access control too early

### What is intentionally deferred

- content-defined chunking
- cross-namespace dedup
- server-side content compression as part of canonical identity

---

## Decision 8: derived indices publish immutable runs and small monotonic progress objects

### Problem

It is easy for developers to accidentally make derived indices part of the write path or to mutate them incrementally in place. That would make correctness depend on background work.

### Recommendation

Every derived work class should publish:

1. immutable output objects keyed by `through_seq` or `plan_id`
2. one small `progress.json` object updated with CAS

Reads may use a derived index only when its progress object proves that it covers the requested boundary. Otherwise the read must fall back to checkpoint + WAL tail replay.

### Example

A `BuildListingIndex(ns, through_seq=420)` job writes new listing objects for `420`, then CAS-updates:

```json
{
  "work_class": "BuildListingIndex",
  "namespace_id": "ns",
  "built_through_seq": 420
}
```

If the update is missing or still at `400`, a reader asking for head `420` must not trust the new objects yet.

### Failure modes prevented

- stale index data being treated as canonical
- index rebuild bugs corrupting correctness
- hidden coupling between namespace commit and background work

---

## Decision 9: core logic stays in single-threaded state machines; I/O pools wrap around it

### Problem

The easiest way to destroy deterministic testing is to let core semantics depend on ambient async interleavings and hidden thread races.

### Recommendation

Keep the following logic serialized and explicit:

- namespace commit planning
- WAL replay
- background queue state transitions
- client sync planning

Use concurrency only around them for:

- block uploads/downloads
- object-store I/O
- filesystem scanning
- network request handling

### Concrete shape

- `loon-core` should expose pure or nearly pure transition planners
- `loon-queue` broker transitions should be serializable and replayable
- the server should treat each namespace as a logical actor for commit planning
- the client should keep one planner loop and separate worker pools for transfer work

### Example

The client may hash and upload multiple blocks in parallel, but the decision “publish remote revision 17 and mark local state as synced” should happen in one planner transaction, not in ad hoc callback code.

### Failure modes prevented

- heisenbugs that reproduce only under load
- tests that cannot replay from a seed
- subtle race bugs between planning and effect execution

---

## Decision 10: the client uses SQLite as its local durable truth and models three views explicitly

### Problem

A sync client has to recover from crashes, local filesystem changes, and remote changes without guessing what it previously believed.

### Recommendation

Use SQLite as the only durable local truth in the client. Model at least these durable tables or equivalent structures:

- `remote_state`: what remote metadata the client has observed
- `local_state`: what local filesystem state the client has observed
- `sync_anchor`: the last fully reconciled state
- `planned_actions`: uploads, downloads, applies, retries
- `transfer_ledger`: block-level progress for large transfers
- `conflicts_and_errors`: durable explanation for user-visible issues

### Why three views matter

Keeping remote observed state, local observed state, and last-synced state separate makes conflicts and convergence much easier to explain.

### Example

If a file was deleted remotely while the user edited it locally, the client can compare:

- remote observed: deleted at seq 420
- local observed: file content changed after the last sync
- sync anchor: last common revision was 17

That is enough to produce a deterministic conflict outcome.

### Failure modes prevented

- restart ambiguity after crashes
- planner logic depending on ephemeral in-memory observations
- future File Provider mode inventing a different local truth model

---

## Decision 11: executable invariants start in harnesses before they move into runtime APIs

### Problem

The repo already names many invariants, but string names alone are weak evidence once the semantic
core is real. At the same time, threading structured invariant reports through every production
surface immediately would create broad API churn.

### Recommendation

Start executable invariant checking in the harnesses first.

Milestone 8 rollout order:

- keep runtime `checked_invariants` strings unchanged
- add structured pass/fail invariant reports in `loon-testkit`
- treat fixture `expect.invariants` as “these names must evaluate true”
- slice 1 covers namespace-core commit/apply, WAL replay, and checkpoint-plus-WAL replay
- slice 2 covers background-work progress publication, queue shard mutation/repair, and verified
  checkpoint head publish
- slice 3 covers file content objects
- slice 4 covers checkpoint immutable objects
- slice 5a covers client file-transfer invariants
- slice 5b broadens to client reconciliation invariants for late authoritative observation and
  remote-only directory materialization

Only after the evaluator set is stable should structured invariant reports move into broader
runtime APIs.

### Why

This raises the proof bar now without widening production compatibility work before the project has
finished deciding which invariant families are worth keeping long term.

### Failure modes prevented

- tests passing because a string name was present even when the property was false
- large production API churn before invariant definitions are stable
- model/core differential runs hiding semantic disagreement behind matching final state

---

## Decisions that are now safe to leave to implementers

Once the decisions above are accepted, most remaining choices are comparatively low leverage. Examples:

- which HTTP framework to use in `loon-server`
- exact zstd levels
- exact metrics backend
- exact queue shard counts for staging vs production
- exact CLI command names
- page-index binary layout details inside one snapshot segment format

Those choices still matter, but they are much less likely to force a redesign of the correctness model.

## Recommended implementation order after this document

1. `loon-objectstore`: control-object helpers, local adapter, and conformance tests
2. `loon-core`: WAL envelope types, head/lease types, checkpoint reader/writer skeletons
3. `loon-model`: transitions that match the locked semantics above
4. `loon-queue`: immutable outputs + progress-object publication
5. `loon-client`: SQLite schema and planner transaction boundaries

That order follows the principle of making the durable contract concrete before building broad product surfaces.
