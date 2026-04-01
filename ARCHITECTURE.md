# LoonDB — System Overview & Orientation

## What is LoonDB?

LoonDB is a **Dropbox/Google-Drive-style file sync engine** whose only durable backend is **object storage** (S3, R2, local filesystem). There is no traditional database server — all authoritative state lives as immutable objects (blocks, WAL entries) and a small set of mutable control objects (head, lease, queue) in the object store.

---

## Core Design Principles

1. **Object storage is the only system of record** — no separate database. All durability comes from S3-compatible stores.
2. **Inode-keyed metadata** — files/dirs are identified by numeric inode IDs, never by path. Paths are derived views.
3. **Serialized namespace commits** — mutations within a namespace are strictly ordered via a monotonic sequence number and fencing tokens.
4. **Content-before-metadata** — file content must be durably stored before a metadata revision references it.
5. **Determinism-first testing** — concurrency is tested via deterministic simulation, not flaky timing-dependent tests.
6. **Spec → Model → Implementation** — design specs are written first, then a pure reference model, then production code.

---

## Repository Layout (13 Rust crates)

```
crates/
├── loon-types        # Shared IDs, wire types, envelopes (foundation — no internal deps)
├── loon-objectstore  # ObjectStore trait + providers (S3, R2, local FS)
├── loon-core         # Canonical metadata engine: commit planning, WAL, checkpoints
├── loon-model        # Pure reference model for state-machine testing
├── loon-queue        # Durable background work queue (snapshots, GC, indexing)
├── loon-client       # Client-side sync: SQLite state, planner, executor, uploads/downloads
├── loon-server       # Server-side mutation execution (lease → validate → commit → publish)
├── loon-ops          # Shared operability contract (import, observe, sync)
├── loon-cli          # Thin CLI frontend (`loon ops`, `loon doctor`, etc.)
├── loon-testkit      # Scenario fixtures, rendering, replay infrastructure
├── loon-sim          # Deterministic simulator (clock, scheduler, fault injection)
├── loon-macos        # macOS File Provider bridge (read-only spike)
└── xtask/            # Build automation (rc-local, render-case, replay-seed)
```

### Dependency Flow (bottom-up)

```
loon-types  ←  loon-objectstore  ←  loon-core  ←  loon-server
                                  ←  loon-model
                                  ←  loon-queue
                                  ←  loon-client  ←  loon-ops  ←  loon-cli
                                  ←  loon-testkit ←  loon-sim
                                  ←  loon-macos
```

---

## Data Model at a Glance

### Metadata (inode-keyed, 4 record types)
| Record | Key Fields | Purpose |
|---|---|---|
| `InodeRecord` | inode_id, kind, created_seq | Identity of a file/dir/symlink/mount |
| `DirentryRecord` | parent_inode_id, name_key, child_inode_id | Binds a name in a directory to a child inode |
| `RevisionRecord` | inode_id, revision_no, content_manifest_digest | Points a file revision to its content |
| `SubtreeTombstoneRecord` | root_inode_id, tombstone_seq | Marks a subtree as deleted |

### Control Objects (mutable, in object store)
- **HeadState** — current namespace seq, active fence token, next inode ID, retention floor
- **LeaseState** — write lease holder, fence token, expiry (prevents concurrent writers)
- **ProgressState** — background work high-water marks per work class

### Content Storage
- Files are split into **16 MB fixed blocks**, content-addressed by SHA-256
- A **ContentManifest** (JSON) lists block descriptors for a file
- Revisions point to manifests by digest

### Write-Ahead Log (WAL)
- 7 operation types: CreateDir, CreateFile, ReplaceFile, DeleteFile, Rename, DeleteSubtree, RestoreRevision
- Serialized as CBOR + Zstd, stored at `namespaces/{ns}/wal/{seq}-{id}.cbor.zst`
- Each commit carries preconditions that are checked atomically

### Checkpoints / Snapshots
- Periodic snapshots of all 4 metadata tables, compressed with Zstd
- Allow new readers to start from a recent snapshot instead of replaying the full WAL

---

## Mutation Flow (Server-Side)

```
Client Request
  → Load basis (head + metadata from checkpoint + WAL replay)
  → Acquire/renew write lease (fencing token)
  → Validate preconditions
  → Build commit plan (allocate inode IDs, compute next seq)
  → Write WAL entry to object store
  → Apply metadata changes
  → Publish new head (compare-and-swap on head object)
```

Key file: `crates/loon-server/src/mutation.rs` (~45KB)

---

## Client Architecture

The client (`loon-client`) maintains a **SQLite database** as local durable state and runs a planner/executor loop:

- **Observe** remote namespace changes and local filesystem events
- **Plan** what needs to upload, download, or reconcile
- **Execute** uploads (content blocks → manifest → server mutation) and downloads (fetch manifest → blocks → materialize)
- **Conflict resolution** — 6 conflict classes (stale edits, path collisions, delete-vs-edit, rename-vs-edit, subtree conflicts)

Key file: `crates/loon-client/src/planner.rs`, `executor.rs`, `state_db.rs`

---

## Testing Strategy (3 layers)

1. **Pure model tests** (`loon-model`) — state-machine semantics verified against a reference implementation
2. **Deterministic simulation** (`loon-sim`) — concurrency and failure scenarios with reproducible seeds (28+ scenarios)
3. **Native/conformance tests** — real provider tests, 200+ YAML scenario fixtures across `tests/scenarios/`

Scenarios are human-readable YAML files treated as product artifacts, not just test internals.

Run the full local check: `cargo run -p xtask -- rc-local`

---

## Documentation Map

| Path | Content |
|---|---|
| `docs/specs/000-overview.md` | System overview |
| `docs/specs/030-namespace-metadata.md` | Metadata model deep-dive |
| `docs/specs/040-namespace-commit.md` | Commit protocol (45KB, most detailed) |
| `docs/specs/070-client-architecture.md` | Client architecture (129KB, largest) |
| `docs/adr/` | 20 architectural decision records |
| `docs/roadmap/` | Implementation phases |
| `docs/runbooks/` | Operational guides |
| `AGENTS.md` | Non-negotiable rules and dev workflow |
| `README.md` | Quick start and command reference |

---

## Deep Dive: Object Store Layer

The object store is the **only durable backend** — there is no database server. Everything (metadata, content, control state) maps to keys in an S3-compatible store.

### The ObjectStore Trait

Defined in `crates/loon-objectstore/src/lib.rs`, five operations:

| Method | Semantics |
|---|---|
| `head(key)` | Returns `Option<ObjectMetadata>` (etag + size) |
| `get(key, range?)` | Returns `Option<Vec<u8>>`; supports byte-range reads |
| `put(key, bytes, mode)` | Three modes: Overwrite, CreateIfAbsent, CompareAndSwap |
| `delete(key)` | Idempotent — deleting a missing key succeeds |
| `list_prefix(prefix)` | Returns sorted keys under a prefix |

Key semantic: `etag` is an **opaque CAS token**, not content identity. It's only valid for compare-and-swap on the same key immediately after reading.

### Three Providers

| Provider | Module | Notes |
|---|---|---|
| **Local FS** | `fs.rs` | Atomic writes via temp+rename; etag = size+mtime; write-serialized via Mutex |
| **AWS S3** | `s3.rs` → `s3_compatible.rs` | AWS SDK; conditional headers for CAS; strong consistency |
| **Cloudflare R2** | `r2.rs` → `s3_compatible.rs` | S3-compatible; region="auto"; shares SDK impl with S3 |

S3 and R2 are thin wrappers around a shared `S3CompatibleStore` that handles conditional puts, pagination, and error mapping.

### Object Key Layout (`keys.rs`)

All keys are namespace-scoped. The full keyspace:

```
namespaces/{ns}/head.json                                          # HeadState (CAS)
namespaces/{ns}/lease.json                                         # LeaseState (CAS)
namespaces/{ns}/wal/{seq:020}-{commit_id}.cbor.zst                 # WAL commits (immutable)
namespaces/{ns}/blobs/sha256:{hex}                                 # Content blocks (immutable, 16MB)
namespaces/{ns}/manifests/{digest}.json                            # Content manifests (immutable)
namespaces/{ns}/snapshots/{seq:020}/manifest.json                  # Checkpoint manifest
namespaces/{ns}/snapshots/{seq:020}/tables/{family}-{idx:05}.sst.zst  # Checkpoint segments
namespaces/{ns}/derived/{work_class}/progress.json                 # Background work progress
namespaces/{ns}/conflicts/{conflict_id}.json                       # Conflict artifacts
namespaces/{ns}/conflict-archives/{conflict_id}.json               # Archived conflicts
queue/shards/{shard_index:05}.json                                 # Global work queue shards
```

**Two categories of objects:**
- **Immutable** (blobs, manifests, WAL entries, checkpoint segments): written via `CreateIfAbsent`, never updated
- **Mutable** (head, lease, progress, queue shards): updated via `CompareAndSwap` only

### Key Isolation & Safety (`keyspace.rs`)

All keys are validated to reject path traversal (`..`, `.`, empty segments). Providers can be scoped with a `key_prefix` — the scoping layer guarantees keys never leak outside the configured prefix.

### Serialization Formats

| Object Type | Format | Compression |
|---|---|---|
| WAL commits | CBOR | Zstd |
| Checkpoint segments | SST (custom) | Zstd |
| Content blocks | Raw bytes | None |
| Content manifests | JSON | None |
| Control objects (head, lease) | JSON | None |
| Checkpoint manifests | JSON | None |

All envelopes carry: `kind`, `format_version`, `writer_version`, `payload_checksum_sha256`.

### Conformance Test Suite

Located at `crates/loon-objectstore/tests/conformance.rs`, 10 test cases validate every provider against the contract:

1. `create_if_absent` — second write rejected, first bytes preserved
2. `compare_and_swap_stale_reject` — stale etag rejected after overwrite
3. `compare_and_swap_missing_object_reject` — CAS on missing key fails
4. `overwrite_visibility_and_head_freshness` — immediate read-after-write
5. `delete_idempotent` — deleting missing key succeeds
6. `list_visibility_after_write_and_delete` — list reflects mutations immediately
7. `sorted_list_prefix` — results sorted at trait boundary
8. `range_read_and_invalid_range` — byte range semantics
9. `invalid_key_and_traversal_rejection` — path traversal blocked
10. `scoped_key_prefixing` — keys isolated to prefix

LocalFS runs by default. S3/R2 require env vars and are `#[ignore]`'d.

### Provider Profiles (`provider.rs`)

Each provider declares its expected capabilities via a `ProviderProfile` struct. Capabilities can be `ExpectedYes`, `ExpectedNo`, or `VerifyByConformance`. This lets the system know what to test vs. assume for each backend.

### How It All Fits Together

```
Content upload:
  Split file → 16MB blocks → put_if_absent(blob key) → put_if_absent(manifest key)

Namespace mutation:
  Load head → acquire lease (CAS) → validate → write WAL (put_if_absent)
  → publish head (CAS with old etag)

Checkpoint:
  Build 4 table families → put_if_absent(segments) → put_if_absent(manifest)
  → update head.snapshot_hint_seq (CAS)

New reader bootstrap:
  get(head) → get(latest checkpoint manifest) → get(segments)
  → replay WAL entries from checkpoint seq to head seq
```

---

## Deep Dive: Commit Protocol

The commit protocol is how mutations become visible. It's a linear pipeline with a single atomicity point: the CAS-update of `head.json`.

### The 10-Step Mutation Pipeline

```
ClientMutationRequest
  │
  ├─ 1. Validate durable content    — manifest + blocks exist in object store
  ├─ 2. Acquire/renew lease         — CAS on lease.json; fence token rotation on takeover
  ├─ 3. Load basis                  — head.json → checkpoint → WAL replay → MetadataState
  ├─ 4. Translate request           — ClientMutationOp → CommitOp + explicit preconditions
  ├─ 5. Build commit plan           — validate preconditions, allocate inode IDs
  ├─ 6. Prepare WAL entry           — serialize ops + preconditions → CBOR + Zstd
  ├─ 7. Apply metadata (in-memory)  — transform MetadataState with ops
  ├─ 8. Prepare new head            — advance seq and next_inode_id
  ├─ 9. Write WAL                   — put_if_absent (immutable, durable)
  └─ 10. Publish head               — CAS head.json with old etag ← SINGLE VISIBILITY POINT
```

Entry point: `execute_client_mutation()` in `crates/loon-server/src/mutation.rs`

### Key Types

**`CommitRequest`** — the validated mutation request:
- `ops: Vec<CommitOp>` — 7 op types: CreateDir, CreateFile, ReplaceFile, DeleteFile, Rename, DeleteSubtree, RestoreRevision
- `preconditions: Vec<Precondition>` — explicit CAS conditions (HeadSeqIs, InodeRevisionIs, AncestorsNotSubtreeDeleted, ChildNameAbsent)
- `writer_fence_token` — must match head's active fence

**`CommitPlan`** — output of validation:
- `next_seq` — the seq this commit will occupy
- `allocated_inode_ids` — consumed from head's next_inode_id cursor
- `checked_invariants` — all validation rules verified

**`WalCommitPayload`** — what gets persisted:
- `namespace_id, seq, base_head_seq, commit_id, request_id, writer_id`
- `ops: Vec<WalOp>` — each op tagged with `op_index` for deterministic same-seq ordering
- `preconditions: Vec<WalPrecondition>`

### Basis Loading (Step 3)

The basis is always reconstructed fresh — never cached across requests:

1. Read `head.json` (get etag for later CAS)
2. Read `lease.json`, verify fence tokens match
3. If `head.snapshot_hint_seq` exists → load checkpoint at that seq
4. Replay WAL entries from checkpoint seq through `head.seq`
5. Result: fully reconstructed `MetadataState` at head seq

Checkpoint optimization: without checkpoints, every mutation replays the entire WAL from seq 0. Checkpoints let new readers start from a recent snapshot.

### Metadata State

`MetadataState` holds 4 append-only record vectors:

| Record | What it tracks |
|---|---|
| `InodeRecord` | Inode existence: id, kind (Dir/File), created_seq |
| `DirentryRecord` | Name bindings: parent → child, with bind_seq and op_index |
| `RevisionRecord` | File versions: inode → revision_no → content_manifest_digest |
| `SubtreeTombstoneRecord` | Deletions: root_inode, tombstone_seq |

Visibility is computed via functions like `visible_inode(id, seq)`, `visible_child(parent, name, seq)`, `current_revision_head(id, seq)`. These check that the record exists at or before `seq` and isn't covered by a tombstone.

### Fencing & Lease Mechanics

- **LeaseState** holds the write lock: `holder_id`, `fence_token`, `lease_expires_at_ms`
- **HeadState** carries `active_fence_token` — must match lease's fence token
- On lease takeover: fence token is rotated, preventing the old writer from publishing
- Every commit validates: `request.writer_fence_token == head.active_fence_token == lease.fence_token`

This is the system's concurrency control: fencing tokens make stale writers fail at the CAS step.

### Invariants Enforced

The protocol records named invariants at each step. Key ones:

- `stale_writer_cannot_publish` — fence token validation
- `create_mutation_consumes_next_inode_id` — allocation correctness
- `create_file_requires_durable_content` — content before metadata
- `subtree_tombstone_blocks_descendant_mutation` — no orphaned operations
- `head_publish_requires_durable_wal` — WAL before head
- `wal_payload_checksum_matches_payload` — integrity on read

### Failure & Retry

- If CAS on head.json fails → concurrent mutation detected → retry from step 2
- WAL write is idempotent (put_if_absent) — safe to retry
- Lease expiry → must re-acquire (new fence token → new basis load)
- Content not durable → rejected before any state mutation

### How Metadata Gets Applied (Step 7)

`apply_committed_wal_ops(committed_seq, ops)` transforms metadata:

| Op | Effect |
|---|---|
| CreateDir | Append InodeRecord + DirentryRecord |
| CreateFile | Append InodeRecord + DirentryRecord + RevisionRecord(rev=1) |
| ReplaceFile | Append RevisionRecord(rev=base+1) |
| DeleteFile | Append SubtreeTombstoneRecord |
| Rename | Append new DirentryRecord (old binding stays; latest wins by bind_seq) |
| DeleteSubtree | Append SubtreeTombstoneRecord |
| RestoreRevision | Look up historical revision, append new RevisionRecord with its digest |

All records are append-only. No rewrites, no deletions. Visibility is determined by seq ordering and tombstone coverage.

---

## Current Development Focus

Recent commits show active work on:
1. **macOS File Provider integration** — read-only bridge spike, sample app, C ABI boundary
2. **Client sync hardening** — same-path replacement ordering, local delete/move observation
3. **Server foundations** — lease acquisition/renewal, namespace bootstrapping
