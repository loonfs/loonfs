# LoonFS Format Specification

This document is the normative, mandatory specification of the LoonFS durable
format: the object-storage layout, the durable encodings, the commit protocol,
and the consistency and durability invariants. Any implementation that reads
and writes a store according to this document is format-conformant, whether or
not it exposes any API surface.

The companion document is `api.md` — the LoonFS API specification: API groups,
capability discovery, the standard error contract, and the HTTP binding;
normative where implemented.

Nothing in this document depends on how work is scheduled or which API surface
a deployment exposes.

Encoding conventions used by every durable and wire shape in this specification:
field names and enum values are `snake_case`; fields holding typed identifiers
are suffixed `_id`; every durable tagged union uses `kind` as its
discriminator. The HTTP binding uses `kind` too, and additionally uses `mode`,
`status`, `outcome`, and `inode_kind` as tag words where those read better at
the call site. Number suffixes have fixed meanings. `_seq` is a position in
the namespace commit history. `_no` is a monotonic counter scoped to a
resource, such as a file, inode, or namespace. `_index` is a 0-based position
inside a collection. `_number` is a 1-based position defined by a provider or
tool.

Unknown fields are tolerated where a reader must accept what a newer writer
added, and rejected where accepting one would lose information the sender
meant to send:

- **HTTP request bodies reject them.** Most request fields are optional, and
  many of those are preconditions, so a misspelled field would decode to its
  default and the server would carry out a different request than the caller
  asked for — an unguarded write answering 200. Rejection is over the whole
  request, at every level of nesting.
- **HTTP response bodies tolerate them**, so a client keeps working against a
  server newer than itself.
- **All authoritative durable envelopes and records reject them**, at every
  level of nesting. This includes immutable manifests, WAL records, metadata
  rows, and grep state: folding and compaction re-encode their contents into
  successor objects, so a reader must not accept fields it cannot preserve.
  New durable meaning requires a supported family format version; an older
  binary refuses newer data rather than silently dropping it.
- **`ContentRef`, `Checksum`, and `ActorRef` are closed shapes.** They reject
  unknown fields wherever they appear, in request bodies and in durable rows
  alike, because the same types decode request bodies. They evolve only by
  new `kind` and `algorithm` values, never by new fields.

Durable formats store inode IDs as integers. The public API uses strings such
as `ino_27`; this does not change stored data.

## 1. Object store contract

LoonFS relies on object storage as its only required durable dependency. The
object-store contract is therefore part of the format, not an implementation
detail.

### 1.1 Required guarantees

A conforming object-store layer must provide the following behavior.

| Guarantee | Rationale |
| --- | --- |
| **Create-if-absent** for immutable objects | File content objects, WAL segments, and manifests must never be silently overwritten. |
| **Compare-and-swap update** for small mutable objects | The namespace head and similar control objects must be advanced safely in the presence of concurrent writers. |
| **Full-object reads with identity metadata** | Mutable control-object readers must receive object bytes and the opaque compare token for those same bytes from one read operation, so one observation's payload cannot be paired with another observation's compare token. |
| **Strong consistency** | A successful put/delete operation must become authoritative immediately after it succeeds. |
| **Prefix enumeration** | Manifest discovery, WAL segment discovery for reclamation, and general namespace inspection need a reliable way to enumerate objects by prefix. Listings return keys in ascending lexicographic order; the conformance probes assert this. |
| **Deterministic key scoping** | Providers must not allow objects outside the configured namespace or tenant prefix to leak into operations. |
| **Consistent error signaling for failed preconditions** | Higher layers need one generic way to detect stale writes and retry or fail safely. |

The format deliberately avoids relying on multi-object transactions or
provider-specific behavior that is not exposed through this contract.

### 1.2 Durable object families

Namespace objects follow one global grammar — each subsystem owns its local
control file and its data files; there is no central control directory:

```text
{subsystem}/{role}.json                    small control/pointer object local to that subsystem
{subsystem}/{collection}/{id}.json         per-id JSON records
{subsystem}/{collection}/{id}.{kind}.zst   compressed immutable payloads
```

The required durable object families and standard key patterns are:

| Family | Mutability | Purpose | Standard object key pattern |
| --- | --- | --- | --- |
| **WAL head** | Mutable | Defines the namespace's durable identity and current state: content store, fork provenance, visible sequence, writer epoch, writer metadata, replay hints, and visible WAL tip. | `namespaces/{namespace_id}/wal/head.json` |
| **WAL segments** | Immutable | Record one or more logical commits with a contiguous sequence range. | `namespaces/{namespace_id}/wal/segments/wal_{start_seq:020}-{suffix}.wal.zst` |
| **Namespace manifests** | Immutable | Record one namespace file-set version: its metadata segment references and a head summary. Segment references carry their own owner, so a fork target's manifest names source-owned segments without recording anything about the fork. | `namespaces/{namespace_id}/metadata/manifests/{manifest_object_id}.manifest.json` |
| **Checkpoint records** | Mutable lifecycle | Durable stable-view pins to a metadata manifest, each carrying a required owner (user, fork target, or snapshot). The record's `status` is monotonic: a record is created `active` under a generated id, released once by compare-and-swap, and deleted a grace window after that release. | `namespaces/{namespace_id}/checkpoints/{checkpoint_id}.json` |
| **Metadata segments** | Immutable | Store metadata rows referenced by manifests. Segments may be owned by the namespace itself or by a fork source namespace. | `namespaces/{owner_namespace_id}/metadata/segments/{segment_id}.sst.zst` |
| **Compaction staging** | Immutable | Holds segments written by a streaming compaction before publication. The descriptor stores the job id used to derive this key. | `namespaces/{owner_namespace_id}/metadata/compactions/{job_id}/segments/{segment_id}.sst.zst` |
| **Compaction leases** | Mutable group slot | An `active` lease owns a running job's output. `completed` and `reaping` jobs release the group. Slots are replaced by CAS and never deleted. | `namespaces/{owner_namespace_id}/metadata/compaction_leases/{group}.json` |
| **Compaction output protection** | Mutable deadline | Before each publication attempt, a job records its lease deadline beside the sealed output. This record remains until the output prefix is empty. | `namespaces/{owner_namespace_id}/metadata/compactions/{job_id}/protection.json` |
| **Upload sessions** | Mutable lifecycle | Track one staged-content upload. The record's `status` is monotonic: a session is created `open` under a lease, and moves once to `completed` or `aborted`, both terminal. | `namespaces/{namespace_id}/uploads/{upload_id}.json` |
| **Metadata root** | Mutable | Cold pointer to the best known materialized metadata root; monotonic CAS. | `namespaces/{namespace_id}/metadata/root.json` |
| **GC run** | Mutable CAS | Coordinates marking and sweeping across calls and hosts; one active run per namespace. | `namespaces/{namespace_id}/gc/run.json` |
| **GC mark pages** | Immutable | Sorted, checksummed reference tables and intermediate merge output. | `namespaces/{namespace_id}/gc/runs/{gc_run_id}/tables/{table_id}/{page_no:020}.json` |
| **WAL floor** | Mutable | Cold lower bound of retained WAL/change history; monotonic CAS. | `namespaces/{namespace_id}/wal/floor.json` |
| **Content objects** | Immutable | Store one file revision's complete bytes. | `content-stores/{content_store_id}/objects/{content_id[4..6]}/{content_id[6..8]}/{content_id}` |

`.sst.zst` identifies the block encoding: sorted rows with each block compressed using zstd. Metadata and grep segments use this encoding. `.wal.zst` identifies the WAL encoding.

The WAL subtree has one mutable head, one optional mutable floor, and the
immutable segment collection:

```text
namespaces/{namespace_id}/wal/
├── head.json
├── floor.json
└── segments/{segment_id}.wal.zst
```

For example, segment `wal_00000000000000000002-fedcba9876543210` in namespace
`demo` is always stored at
`namespaces/demo/wal/segments/wal_00000000000000000002-fedcba9876543210.wal.zst`.
Pointers never store this key; every store boundary derives it from the
namespace and `segment_id`.

These key shapes are part of the interoperable storage contract.
Implementations may keep additional private control-plane objects — queues,
scheduler state, coordination records — outside the key families above;
private objects must not collide with the spec'd families and are not
interoperable state.

Namespace object keys are built through the central object layout API in
`loonfs-objectstore`. The namespace root remains `namespaces/{namespace_id}/`.
Forks are copy-on-write: the target's head names a source-owned manifest as
its starting basis, later target manifests may go on referencing source-owned
metadata segments, and the source holds a fork-owned checkpoint record
protecting that basis for the target's lifetime.

The content-store keyspace holds blobs and nothing else. A content store has
no descriptor object and no durable record of its own: its id is minted by
random generation when a namespace is created and recorded in that
namespace's head. Uniqueness rests on the generated id's randomness, so no
object has to be claimed, read, or verified before bytes may be written under
it, and a content store is shared exactly by the namespaces whose heads name
it.

WAL segment names sort by history position (section 1.3); recovery still
follows `head.visible_wal_tip` and the predecessor links inside verified WAL
envelopes. Listing order is an inspection and reclamation convenience, never
recovery authority. `wal/head.json` and `wal/floor.json` live outside the
`wal/segments/` listing prefix, so a reclamation listing of segments yields
only segment keys.

WAL and metadata-segment deletion is reachability-driven from the live
manifest, checkpoint records, and the retention floor. Extension-owned
objects below `namespaces/{namespace_id}/extensions/` are foreign to the core
key parser and are never core-GC candidates.

### 1.3 Durable naming conventions

The namespace tree's lifecycle can be read off its grammar:

- **`{subsystem}/{role}.json` objects are mutable singletons with one job**:
  compare-and-swap pointers and proofs (`wal/head.json`, `wal/floor.json`,
  and `metadata/root.json`) that are never swept. If a
  singleton cannot be explained in one sentence, it is too broad.
- **Collections are never authoritative via enumeration** (`wal/segments/`,
  `metadata/manifests/`, `metadata/segments/`, `uploads/`,
  `content-stores/.../objects/`). A record in a collection matters only when a
  pointer, chain link, or checkpoint reaches it — except to GC, which
  lists collections to find garbage and roots. WAL segment and namespace
  manifest ids are a family prefix (`wal_`, `man_`), then a 20-digit
  position, then a 16-character lowercase hex suffix, so listings stay
  ordered while concurrent writers avoid fighting for one immutable object
  name.
- **Paths express ownership, not authority.** Envelopes and payloads still
  validate namespace id, object id, family, checksum, and sequence fields;
  the same fact is never encoded twice (the row family lives in the manifest
  and the segment envelope, not the path).
  The compaction lease is the exception: its key embeds the family group so
  all seven lease keys can be computed and read without listing.
- **`wal/head.json` is the namespace.** The head exists, or the namespace does
  not. It is the existence marker, and after deletion it is kept forever as
  the tombstone that retires the namespace id.
- **Creation and forking are one conditional write.** Create and fork both
  build a complete active head in memory and install it with create-if-absent.
  Nothing under the new namespace's prefix is written before that write, so
  there is no partial namespace to classify, complete, or reap, and no
  ordering rule to get wrong. A create or fork that loses the conditional
  write answers `namespace_exists`, or `namespace_deleted` against a
  tombstone, unless the head it finds is its own earlier attempt, which
  succeeds idempotently (section 3.9.3).

Names are never authority anywhere — recovery follows the head and its
references.

The required invariants of this layout are:

> **Live visibility is defined only by `wal/head.json`.** Everything else is
> a read accelerator, retention boundary, reachability root, or workflow
> record.

> **Fencing authority is writer epoch plus CAS.** Wall-clock time never
> gates commit validity, and fenced sessions never reacquire on their own.

> **Nothing correct depends on listing.** GC and floor advancement alone
> list — under a safety window, with delete-time re-verification, and with
> retention winning every ambiguous race.

> **Throughput is group commit; deadlines are local monotonic budgets a
> writer applies to itself.** No validator ever compares clocks, and
> accelerators (`recent_segments`, WAL indexes) prefetch but never decide
> what the history is.

### 1.4 Head update authority

The namespace head is updated by different classes of work, and each class has
its own authority boundary.

Semantic namespace mutations — file writes, restores, renames, deletes,
alone or batched into one request — are fenced by `writer_epoch` and then
linearized by the head compare-and-swap that makes their WAL visible.

Checkpoint and retention maintenance updates are CAS-linearized metadata
updates. They preserve `writer_epoch` and the `writer` block, and must not
change WAL visibility at all. Maintenance may race with semantic writers; on head
CAS conflict it must reload the latest head and rebase or retry the metadata
update. It must not bump `writer_epoch` unless its purpose is to intentionally
fence writers.

Destructive namespace-admin updates that intentionally stop writers, such as
namespace deletion, must fence writers first or publish an equivalent terminal
state that prevents stale writers from succeeding.

### 1.5 WAL segment rules

The metadata log has six rules.

1. A logical commit is the semantic record of one accepted client commit
   request.
2. A WAL segment stores one or more logical commits with contiguous `seq`
   values.
3. Distinct client commit requests remain distinct logical commits even when
   they are stored in the same WAL segment.
4. The visible WAL chain is deterministically recoverable from the head's
   `visible_wal_tip` and the `prev_visible_segment` pointer inside each
   verified segment. Every pointer stores only `segment_id`, `start_seq`,
   `end_seq`, and `payload_checksum`; its object key is derived from the
   namespace and `segment_id`. The bounded `recent_segments` predecessor
   hints accelerate reads but never define chain history.
5. `segment_id` must be unique and never reused within a namespace
   incarnation. It is a stream-positioned id (section 1.3): the 20 digits
   after `wal_` are the segment's `start_seq` so listings and reclamation
   scans sort by history position, and the collision-resistant suffix keeps
   competing proposals for the same position distinct. A WAL pointer or
   segment payload is invalid when those digits differ from its `start_seq`.
   The order in a listing is never recovery authority — recovery follows the
   head and the chain (rule 4) exclusively.
6. Orphan WAL segments are permitted and harmless when a writer loses the head
   compare-and-swap.

### 1.6 Immutable content rules

The content model has six rules.

1. **Identity and integrity are separate.** A content object's identity is a
   random `content_id`. Its checksum verifies the bytes stored under that id.
2. A `content_ref` describes one complete file revision.
3. Immutable content objects are written with create-if-absent semantics.
   Random ids cannot collide, so a create that finds the key occupied is
   corruption and must fail rather than overwrite.
4. A metadata commit may reference a `content_ref` only after the referenced
   object is already durable.
5. **Every reference carries a mandatory full-object checksum.** Coverage
   comes from `ContentRef`, not from the checksum algorithm.
6. **Every read verifies the checksum.** The reader computes the algorithm in
   `content_ref.checksum` over the complete file. If it cannot compute that
   algorithm, the read fails. A HEAD request may check existence and size
   before downloading the object.

##### Checksum format

Every checksum has one canonical shape:

```json
{ "algorithm": "sha256", "value": "<64 lowercase hex>" }
```

The allowed algorithms are `sha256`, `crc64nvme`, and `crc32c`. Their values
contain exactly 64, 16, and 8 lowercase hexadecimal characters respectively.
Provider adapters convert other encodings, such as base64, before creating
this value. Unknown algorithms and invalid values fail to decode.

The surrounding field defines coverage. `ContentRef.checksum` and
`UploadContentClaim.checksum` cover complete content. A part checksum covers
one multipart upload part. A `checksum_algorithm` field selects an algorithm
but does not contain a checksum.

Service-proxied uploads produce SHA-256. Direct PUT uses the algorithm returned
when the session begins. Direct multipart uses the algorithm stored in the
session, currently CRC-64/NVME. For direct uploads, LoonFS accepts a client
checksum only after completion verifies the provider's stored object.

The metadata row families are canonical metadata families and validated
derived families. The canonical families are `inodes`,
`direntry_binds`, `direntry_unbinds`, `revisions`, `tombstones`,
`commit_receipts`, and `attributes`. The `direntry_child_binds` family is a
secondary index over the same direntry bind rows, keyed by child inode, and
must be present and verified before a namespace manifest is trusted. The
`active_deletions` family is derived from the tombstone rows and holds current
state rather than events (section 2.5).

The `attributes` family holds one row per attribute revision of one inode.
Each row carries `inode_id`, `attributes_revision_no`, `committed_seq`,
`delta_index`, and the inode's complete `attributes` map (section 8). Its row
key is

```text
attribute-{inode_id:020}-{u64::MAX - attributes_revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}
```

and its bloom-filter lookup prefix is `attribute-{inode_id:020}`. The
revision, the sequence, and the delta index are all stored inverted, so an
ascending scan of one inode's prefix reads its newest attribute state first
and a read at a sequence takes the first row at or below it. The family
stands alone: nothing reads attributes in any order but newest-first for one
inode, so it has no ascending twin and no cross-family index-parity rule
applies to it.

ETags remain opaque compare tokens. They may be used for object freshness or
compare-and-swap, but they are not content digests unless a provider-specific
behavior is separately exposed and verified through this contract.

A reader or writer resolves content through the namespace head:
`namespace_id -> head.content_store_id -> content-stores/{content_store_id}/...`.
File revisions and change-feed payloads store only `content_ref`; they do not
store content-store ids or object-store paths.

### 1.7 Mutable control-object rules

Small mutable objects such as the namespace head must use compare-and-swap
semantics. These objects must remain small enough that guarded rewrite is
practical.

Six control-object kinds are registered: `wal_head`, `wal_floor`,
`metadata_root`, `checkpoint_record`, `upload_session`, and
`compaction_lease`, `compaction_output_protection`, and `gc_run`. A control-object envelope carrying any other kind string
is rejected, not skipped.

The WAL floor and the metadata root are the only control objects that carry
`updated_at_ms`. That field records when the object's last rewrite succeeded,
and nothing reads it for ordering or validity.

A compaction lease carries `expires_at_ms`. The job writes its current time
plus `METADATA_COMPACTION_LEASE_EXPIRY_MS` when it creates or refreshes the
lease. Readers compare their current time directly with that stored instant;
they do not apply the writer's lifetime policy again.

Every durable decoder rejects unknown fields in its envelope and complete
nested payload. Mutable updates, WAL folding, and compaction all write
successor state; none may silently discard a field an older binary did not
understand. WAL pointers therefore use the same strict decoder in the head
and in immutable segments. New fields require a supported family version.

A durable object that references a namespace manifest stores this shape under `manifest`:

```json
{
  "owner_namespace_id": "demo",
  "manifest_no": 2,
  "manifest_object_id": "man_00000000000000000002-0123456789abcdef",
  "manifest_head_seq": 17,
  "manifest_payload_checksum": "sha256:<64 lowercase hex>"
}
```

`owner_namespace_id` identifies the namespace that stores the manifest and its segments. `manifest_no` is its logical position, `manifest_object_id` is its immutable object id, and `manifest_head_seq` is the greatest namespace sequence it contains. `manifest_payload_checksum` must match the referenced envelope. Metadata roots, checkpoint records, and fork bases all use this shape, which rejects unknown fields.

A metadata root or checkpoint record must reference a manifest owned by its own namespace. A fork basis must reference a different namespace. Violations return `namespace_corrupt`.

Grep manifests have no logical position or head sequence, so grep root pointers use the smaller shape defined in section 4.2.2.

Readers of small mutable control objects must use a full-object read that
returns bytes and the object identity metadata for those same bytes. This does
not by itself guarantee freshness; it guarantees self-consistency. A reader
must not separately load identity metadata and bytes, then use the identity
from one observation with the payload from another observation.

The namespace head is the only durable writer-fencing authority, and
`writer_epoch` plus the head compare-and-swap is the entire fencing story.
There is no lease and no expiry: a writer session acquires the epoch lazily on
its first semantic write (a guarded rewrite that advances the epoch and
records the non-authoritative `writer` observability block), caches it for the
session's lifetime, and publishes only for that epoch. Any other session that
acquires simply advances the epoch — deterministic last-writer-wins — and the
superseded session is fenced terminally: it must surface `writer_fenced` and
must never reacquire on its own. Nothing consults the `writer` block or any
clock for commit validity.

Every acquisition advances the epoch. There is no recognition step and no
session identity to recognize: the `writer` block carries the writer's
configured label and the acquisition stamp (`acquired_at_ms`) and nothing
else, so two runs of one writer are told apart by when they acquired. A
writer that acquires twice — which happens only when the first attempt's
outcome was unknown to it — fences an epoch of its own that published
nothing, and that is ordinary last-writer-wins.

Writers additionally apply a self-enforced publish budget: a local monotonic
elapsed-time bound between starting a WAL segment PUT and initiating the head
CAS (60 seconds). Overrunning it abandons the segment as an orphan and
rebuilds the commit as a fresh segment. The budget gates only the writer's own
next action; validators never consult time.

Large immutable file data may use multipart upload or another
provider-specific optimization. Small mutable control objects should not
depend on those mechanisms.

A payload whose length is not known before it starts arriving may be written
incrementally, cutting it into provider parts as it goes so the writer's
memory follows the part size rather than the object's size. Two rules apply
to such a write:

1. **A failed or abandoned incremental write leaves no provider state.** The
   multipart upload it opened is aborted — on failure, and on cancellation
   too, since a client that disconnects mid-upload is the ordinary case.
   Aborting is safe whatever the upload's real state, so this needs no proof
   of what it is cleaning up; an abort that itself fails leaves the bucket's
   incomplete-upload lifecycle rule to collect the parts.
2. **The payload is consumed before any precondition is evaluated.** A
   writer folding a digest over the same bytes as it forwards them therefore
   always ends up with a digest over the complete payload, even when the
   write is refused — which is what lets it tell "these are the same bytes
   again" from "these are different bytes". Beyond one part the precondition
   is not part of the write, because a provider assembles a multipart object
   unconditionally. The writer observes the key separately instead, after the
   payload is consumed and immediately before the assembly, and refuses a key
   that is already occupied. A writer that lands inside that window is not
   refused. Incremental writes are therefore for immutable, uniquely-named
   keys, where the condition is a corruption tripwire rather than a
   concurrency control; a caller that needs two writers kept off one key
   must exclude them itself, as staging does in §2.4.2.

### 1.8 Provider conformance

The format standardizes the required behaviors, not a brand name such as "S3
compatible." A provider is conforming only when those behaviors are verified
by conformance tests.

In practice:

- higher layers may depend on the LoonFS object-store contract;
- higher layers may not depend directly on provider headers, status codes, or
  SDK quirks.

## 2. Filesystem and storage model

### 2.1 Namespaces and identity

A namespace is the unit of visible metadata history.

The `namespace_id` is a durable storage identity, not a reusable display name.
It must not be reused after namespace destruction. Future aliases or
user-facing names may be reused only if they map to a new `namespace_id`.

Each namespace has:

- a head that records its namespace id, content-store id, fork provenance,
  and current visible boundary
- an ordered WAL of logical commits stored in immutable segments
- immutable namespace manifests that describe recoverable file-set versions
- zero or more checkpoints
- a retention policy

The head is the namespace's durable identity. No other object records its
content store or fork source. If the head is missing or unreadable, the
namespace does not exist or cannot be read.

The head also carries the next monotonic inode id for that namespace. New
inode ids are allocated from the head as part of commit publication.

The canonical identity of an item is `(namespace_id, inode_id)`.

Each namespace has exactly one immutable `content_store_id`, recorded in its
head. The content store is an immutable pool for file bytes and may be
referenced by many namespaces. A new root namespace mints a fresh content
store id by random generation; a forked namespace copies the source
namespace's id while starting an independent namespace metadata history,
which is what makes forks copy-on-write over the same bytes.

The head stores the namespace's creation time in `created_at_ms`. This value
never changes. A new namespace uses its bootstrap timestamp. A fork uses the
time the target namespace was created, not the source namespace's creation
time. Before the first manifest exists, readers also use this value as the
root inode's creation time.

Two consequences follow:

1. rename does not change identity;
2. path is a view, not the identity model.

If an item is deleted and a new item is later created at the same path, that
new item receives a new inode identity.

An inode is the durable namespace-local identity record for one filesystem
item.

An inode records:

- what item this is;
- what kind of item it is; and
- when it first entered namespace history.

An inode does not record:

- the item's current path;
- the parent directory that currently contains it; or
- the file bytes it currently references.

Those facts live in other metadata families:

- direntry bind records and direntry unbind records say where an inode is
  currently bound in the tree;
- revisions say which immutable file version is current for a file inode; and
- paths are derived views produced by walking visible directory bindings from
  the root.

#### 2.1.1 Example metadata shapes

The inode itself is only one part of the metadata model. A complete visible
file usually involves multiple logical records.

An illustrative inode row:

```json
{
  "kind": "inode",
  "inode_id": 42,
  "inode_kind": "file",
  "created_seq": 17,
  "commit_id": "c_1f0a3b5c7d9e11223344556677889900",
  "created_by": { "kind": "user", "id": "usr_8f3c" },
  "created_at_ms": 1752624000000
}
```

The bind row that places that inode in the tree:

```json
{
  "kind": "direntry_bind",
  "parent_inode_id": 9,
  "name_key": "report.txt",
  "display_name": "Report.txt",
  "child_inode_id": 42,
  "bind_seq": 17,
  "bind_delta_index": 1
}
```

The unbind row that removes one exact prior binding:

```json
{
  "kind": "direntry_unbind",
  "parent_inode_id": 9,
  "name_key": "report.txt",
  "display_name": "Report.txt",
  "child_inode_id": 42,
  "bind_seq": 17,
  "bind_delta_index": 1,
  "unbind_seq": 22,
  "unbind_delta_index": 0
}
```

The revision row for the current file contents:

```json
{
  "kind": "file_revision",
  "inode_id": 42,
  "revision_no": 7,
  "committed_seq": 91,
  "commit_id": "c_2b4d6f8a0c1e33445566778899aabbcc",
  "committed_at_ms": 1752625000000,
  "committed_by": { "kind": "service", "id": "render-worker" },
  "delta_index": 0,
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 19482,
    "checksum": { "algorithm": "sha256", "value": "42d..." }
  }
}
```

Together, the inode, bind, and revision rows mean:

- inode `42` is the durable identity of the file;
- the file is currently visible under parent directory inode `9` as
  `Report.txt`; and
- the current visible file bytes come from revision `7`.

If the file is renamed, the direntry changes but the inode stays `42`. If the
file contents are replaced, the revision row changes but the inode stays `42`.

In v0, every namespace root has `inode_id = 1` and `created_seq = 0`. Its
`created_by` value is `ActorRef::loonfs_system()`, and its `created_at_ms`
value is the namespace's bootstrap timestamp.

Actor references and timestamps are metadata. They are not part of row keys,
indexes, filters, or cache keys. The WAL commit envelope provides the actor and
timestamp when a commit is applied. Individual `WalDelta` values do not repeat
them.

Metadata rows use these attribution fields:

- Each new inode stores `created_by` and `created_at_ms`. This includes parent
  directories created automatically by a commit.
- Each file revision stores `committed_by` and `committed_at_ms`.
- Each commit receipt stores `committed_by` and `committed_at_ms`.
- Each tombstone event stores `deleted_by` and `deleted_at_ms`. The corresponding active-deletion row copies both values.
- Each persisted attribute revision stores `updated_by` and `updated_at_ms`. The initial empty state at revision 0 is not persisted and has neither value.
- Directory bind and unbind rows store neither an actor nor a timestamp.

Each timestamp comes from the commit that wrote the metadata row. Timestamps
are informational; sequence numbers determine ordering. Renaming or moving an
item does not change a timestamp, and directories do not have a modification
time.

### 2.2 Inode kinds

The core inode kinds are:

| Kind | Meaning |
| --- | --- |
| **dir** | A directory that can own child bindings. |
| **file** | A file whose history is an ordered set of revisions. |

The format does not require a larger type taxonomy in the core model. New
resource types should normally be represented through file content or resource
properties rather than by introducing new inode kinds.

### 2.3 Directories, names, and paths

Directories contain bindings from a name to a child inode. They do not contain
file bytes.

A path is produced by walking visible directory bindings from the root inode.
A path can change even when the underlying item has not.

Display names are stored as given and validated at admission. A display name
must be non-empty, must not contain `/` or any Unicode control character
(general category `Cc`, which covers NUL, C0, and C1), must not be `.` or
`..`, and must not exceed 255 UTF-8 bytes as stored. Names also satisfy a
portability floor — the set every target filesystem can hold: a name must
not contain any of the characters Windows reserves in a path component
(`:`, `?`, `*`, `|`, `"`, `<`, `>`, `\`), must not be entirely whitespace, must
not end with a space or a dot, and must not
be a Windows reserved device name (`CON`, `PRN`, `AUX`, `NUL`, `COM1`-`COM9`,
`LPT1`-`LPT9`, compared case-insensitively and ignoring any extension).
Name keys obey the same
character rules with a 768-byte cap: case folding expands at most threefold
in bytes, so every key derivable from a valid display name is admissible.
Requests carrying a name or key outside this grammar fail validation; nothing
is truncated or normalized on the caller's behalf.

An absolute path has one canonical spelling: exactly one leading `/`, no empty
components or repeated separators, and no trailing `/` except for the root
path `/`. Wire decoders reject every noncanonical spelling rather than
normalizing it. A canonical path is bounded at 4,096 UTF-8 bytes and 128
components, so any stored tree can materialize on a real filesystem, in an
archive, or through a sync client.

#### 2.3.1 Name-key folding

Sibling-name comparison is a fixed rule of the v0 format, not a per-namespace
choice. Every name key is derived from its display name by: normalize to NFC,
apply Unicode default case folding, then normalize to NFC again. Both the read
and the write path derive keys the same way, so a namespace cannot disagree
with itself about which two names collide.

Because the rule is fixed, nothing durable records it and there is nothing to
default. If a second supported folding rule ever ships, the head gains a field
that selects between them; until then there is nothing to select.

### 2.4 Files and revisions

A file is represented by one inode and a sequence of immutable revisions.

Each revision stores exactly one immutable `content_ref`. In v0, that
reference names one whole-file object containing the complete plaintext file
bytes. Revisions do not store object-store paths or `content_store_id`;
readers resolve those through the namespace head when bytes are needed.

Content objects belong to the namespace's content store. A file revision may
reference only content that is durable under the content store named by that
namespace's head.

LoonFS therefore uses a two-stage write model:

```text
make content durable  ->  then make metadata visible
```

This separation is part of the core model.

#### 2.4.1 Immutable content storage

The stable immutable content families are:

```text
content-stores/{content_store_id}/objects/{content_id[4..6]}/{content_id[6..8]}/{content_id}
```

The core rules are:

- `content_ref.kind` is `blob_v1` for the current content strategy;
- `content_id` is `con_` followed by 32 lowercase hex characters — 128 fully
  random bits, with no time component. Two shard directories use the first
  four characters of that body in two-character groups. This spreads ingest
  evenly across provider partitions and bounds directory fanout for
  filesystem-backed stores; a clock-derived prefix would put every upload in
  a window into one shard;
- because the id is random, the final object key is known before the first
  byte is read, and an object that was never published belongs to exactly one
  upload;
- `content_ref.size_bytes` records the complete byte length;
- `content_ref.checksum` is mandatory and covers the complete object;
- all content-object access resolves `namespace_id` through the namespace
  head to its `content_store_id`;
- future content strategies must use a new `content_ref.kind` and name their
  durability and validation rules before revisions may reference them.

`ContentRef` rejects unknown fields in every context, including immutable
durable records. Extend it with new `kind` values instead of adding fields:

```json
{
  "kind": "blob_v1",
  "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
  "size_bytes": 19482,
  "checksum": { "algorithm": "sha256", "value": "<64 lowercase hex>" }
}
```

Identical bytes uploaded twice produce two content objects. There is no
cross-upload deduplication: a shared key would also be an existence oracle,
letting anyone authorized to upload learn whether specific known bytes were
already stored. The duplicate case that actually matters — a client retrying —
is answered by resuming the upload session, not by colliding on a key.

#### 2.4.2 Upload-before-publish

Metadata may reference content only after that content is already durable.

This applies to:

- file create;
- file replace; and
- file restore, when the restore introduces a newly referenced content object.

### 2.5 Tombstones and deletion

Deletion is logical first. When an item is deleted, LoonFS records tombstone
metadata that hides the file or subtree from visible lookups. The delete
becomes visible as part of normal namespace history.

Physical reclamation is separate maintenance work (see section 6). It may
happen only when retention and reference-safety rules allow it.

Because deletion is logical, it is also revocable: the `undelete` operation
records a *revoke* event in the tombstone family for the deletion's root
inode and re-binds that inode under a visible parent. Every tombstone row
names its own `generation` — a `seq` and the `delta_index` that
disambiguates it within that commit — and carries a typed action: `set`
(the subtree is deleted) or `revoke` naming as its `target` the exact
generation it cancels. Rows for one root are ordered by generation with
the newest event winning: a `revoke` newest means no tombstone is active,
and a later delete of the same root supersedes the revoke with a newer
`set`. Newest-event-wins is the authoritative reduction; the revoke's
recorded target is guaranteed by commit validation to be the generation
that was active, so consumers that reduce target-aware reach the same
answer and may treat a target mismatch as corruption. Undelete is generation-scoped: the request names the deletion's
committed sequence, and validation refuses (`not_deleted`) unless that
exact generation is the active one, so a stale recovery request can never
cancel a later deletion. Only the root of a deletion can be undeleted —
descendants are covered by the root's tombstone, not their own.

A `set` also carries the binding the delete removed, as one
`deleted_direntry` value holding the `parent_inode_id`, `name_key`, and
`display_name` together. Tombstone rows are immortal, so this is where a
deleted name survives after unbind rows age out, and it is the binding
undelete restores in place. Every deletion records the binding it removed.
Only a `set` includes this field. A reader ignores it on a `revoke`, just like
any other unknown field (encoding conventions above). A partial binding is
not valid.

The `active_deletions` family tracks deletions that can still be restored. A
`set` tombstone creates a `listed` row keyed by `(deletion_seq,
root_inode_id)`. The API exposes `root_inode_id` as `inode_id`. The row also
copies `deleted_by`, `deleted_at_ms`, and `deleted_direntry` from the tombstone event.

A `revoke` tombstone creates a `removed` row with the same key and records the
revoke's sequence as `revocation_seq`. `removed` sorts before `listed`, which
lets reorganization discard both rows together.

The recoverable set is read as a range scan in deletion order. Tombstone rows
remain authoritative because this family is derived from them.

A namespace head records its `status`, and that status has exactly two
values: `active` and terminal `deleted`. Every head writes the field; there is
no default, and a head that omits it fails to decode. There is no
initialization status and no intermediate status of any kind, because there is
no initialization to observe: the head is published complete by one
conditional write, so a namespace either has a head or does not exist.
Deletion is the one transition the head must record, and it keeps that head
forever as the tombstone that retires its `namespace_id`. Readers MUST refuse
to serve a namespace whose head status they do not recognize; decoding is
fail-closed, never best-effort.

Deleting a namespace is a fenced control-plane transition, not a logical
commit: the deleting writer acquires the namespace writer epoch and
compare-and-swaps the head into `status: {"kind": "deleted"}`. The delete
linearizes at that swap. Every commit whose head advance serialized before it
remains committed and durable — deletion never retroactively falsifies an
acknowledgment; it ends the namespace's history at that `seq`. Every operation
that observes the deleted head afterward — reads, commits, forks from the
namespace, status, and re-creation of the same id — fails with
`namespace_deleted`.

Namespace deletion does not imply content-store deletion. In v0, deleting a
content store is unsupported operator-only work, and the only content garbage
collection is the narrow one described in section 6.4: an object a completed
upload session owns and no metadata references. Metadata is reclaimed by garbage collection: on a
terminally deleted namespace a GC pass reaps the WAL chain, metadata segments,
manifests, and non-protecting checkpoint records under the usual windows,
leaving the head as the id-retiring tombstone, together with the root and
floor objects if the namespace ever wrote them (section 6, rule 4). Objects
protected by fork-owned checkpoint records survive, so clones of a deleted
source stay readable. A deleted fork target that materialized its own metadata
root also keeps its source checkpoint: that root may be the basis of a nested
fork whose manifest still names source-owned segments and content.

### 2.6 Forks

Forking a namespace creates a new namespace with independent metadata history
and the same `content_store_id` as the source namespace. The fork point is the
source namespace's current head. Every attempt creates its own leased,
verified fork-owned source checkpoint at that head, then installs the complete
target head with one create-if-absent. The target writes no manifest, no root, and no floor: the
head's `fork_basis` names the source manifest the target starts from, and the
fork-owned checkpoint record is what keeps that manifest and its segments alive
for as long as the target or a nested descendant may still need them. A target
must materialize its own metadata root before it can be forked again; that
direct-read, non-swept control object conservatively keeps the source
checkpoint after target deletion.

Fork provenance lives in the target head and stays there for the namespace's
life. Reads and recovery use the target head, its own manifests once it has
written any, and its WAL; the one exception is the source manifest the head
itself authorizes, before the target's first flush (section 2.9.1). After
fork, the clone must remain readable even if the source namespace's later
metadata is deleted or corrupted, because the source checkpoint roots the
exact objects the clone depends on. Source writes after the fork do not
affect the clone, and clone writes do not affect the source.

### 2.7 Mounts

A mount may later present another namespace, or a subtree of another
namespace, inside the current tree; mount creation, mount metadata, mount
inode kinds, and mount traversal are reserved future work. The v0 model has no
mount inode kind and no standard mutation operation creates a mount.

A future mount would carry:

- a target namespace id
- a target root inode id within that namespace

This allows a composed visible tree without inventing one global namespace
history underneath.

When mounts are implemented, two rules will apply:

1. path resolution may cross a mount;
2. mount loops are invalid and must be rejected.

A share grants access to a subtree. A mount presents that accessible subtree
at a path. The two concepts are related, but they are not the same.

### 2.8 Cross-namespace moves

Identity is namespace-local. A true inode-preserving rename is therefore
namespace-local as well.

Across namespaces, a move is modeled as a copy plus a delete from the source
namespace. Sharing a content store does not by itself authorize reuse of a
`content_ref`: each namespace's collector sees only its own metadata roots and
upload sessions. A fork may keep refs already reachable through its pinned
basis. Other copies re-home the bytes under a fresh destination-owned content
identity unless a future protocol installs a durable source-side root first.
Cross-content-store copies likewise require import into the destination
content store. Inode identity does not cross the namespace boundary.

### 2.9 Recovery view

Readers reconstruct authoritative state from:

1. the current head and the metadata root, fetched concurrently;
2. the materialized metadata basis: the namespace manifest named by
   `metadata/root.json`, or, when that pointer is absent, the basis the head
   itself resolves (section 2.9.1); and
3. the visible WAL segment chain after that basis through `head.seq`,
   replayed as logical commits in ascending `seq` order.

The head records the namespace's immutable identity:

- `content_store_id`, required: where the namespace's file bytes live
- `created_at_ms`, required: when the namespace was created; readers also use
  it as the root inode's creation time
- `fork_basis`, optional: present in every head of a fork target, absent in
  every head of a created namespace

`fork_basis` contains the source `manifest` reference and `source_checkpoint_id`. The manifest's `manifest_head_seq` is the first sequence in the target's history, so no separate fork sequence is stored. Section 2.9.1 defines how readers use this reference.

Every successor head the publisher writes carries those fields forward
verbatim, along with `namespace_id`. They are the namespace's identity, not
its state, and a namespace cannot change which content store holds its bytes
or where it came from. A publisher that builds a successor differing in any of
them has a construction bug, and the difference is caught before the
compare-and-swap rather than persisted.

Head decoding is strict. Readers reject a head that is missing
`content_store_id` or `created_at_ms`. They do not supply defaults because
neither value is stored anywhere else. Readers also reject unknown fields as
required by the mutable control-object rules in section 1.7.

The head also summarizes the current visible boundary and replay hints,
including at minimum:

- `seq`
- `head_commit_id`
- `status` (`active`, or terminal `deleted`)
- `next_inode_id`
- `visible_wal_tip` and the bounded `recent_segments` accelerator

Every WAL pointer has this v1 shape, including `visible_wal_tip`, head hints,
and segment predecessor links:

```json
{
  "segment_id": "wal_00000000000000000002-fedcba9876543210",
  "start_seq": 2,
  "end_seq": 2,
  "payload_checksum": "sha256:<64 lowercase hex>"
}
```

The head stores the tip once. `recent_segments` is a bounded newest-first
list of pointers strictly below `visible_wal_tip`: publication replaces it
with the old tip followed by the old predecessor hints, truncated to the
limit. A head before its first commit omits the tip and lists no predecessor
hints. A head after its first commit carries a tip and may still list no
predecessor hints. For example:

```json
{
  "visible_wal_tip": {
    "segment_id": "wal_00000000000000000003-aaaaaaaaaaaaaaaa",
    "start_seq": 3,
    "end_seq": 3,
    "payload_checksum": "sha256:<64 lowercase hex>"
  },
  "recent_segments": [
    {
      "segment_id": "wal_00000000000000000002-fedcba9876543210",
      "start_seq": 2,
      "end_seq": 2,
      "payload_checksum": "sha256:<64 lowercase hex>"
    }
  ]
}
```

Chain links remain the history authority. The tip plus hints may prefetch or
count the bounded tail; hints never become GC roots by themselves.

`wal/floor.json` is the symmetrical pair to the head — the earliest retained
commit boundary next to the latest visible one. It records `floor_seq` and an
update stamp. It is updated only by monotonic compare-and-swap on its
own etag by floor advancement, which is a GC-family operation: it never
touches the WAL head, so the head changes only when commits land. A missing,
stale, or unverifiable floor means "retain more history", never less, and the
floor never affects live commit visibility.

Create and fork do not write a floor. Without one, a created namespace retains history from sequence 0. A fork retains history from `fork_basis.manifest.manifest_head_seq`, the first sequence in the target's history. The first retention-floor advance creates the floor object.

`metadata/root.json` is the read and recovery pointer. It stores `namespace_id`, one `manifest` reference (section 1.7), and `updated_at_ms`. Updates use compare-and-swap and cannot decrease `manifest.manifest_head_seq`. An update at the same sequence may select a different manifest after compaction. An update at a lower sequence has no effect. The root does not define live visibility; a stale root only requires more WAL replay. If a reader sees `root.manifest.manifest_head_seq > head.seq`, it reloads the head because the two reads may have occurred on opposite sides of a commit.

Create and fork write no root either. `metadata/root.json` is created by the
namespace's first flush or reorganization, which is the first moment there is
a materialized file set worth pointing at. Until then the head resolves the
basis on its own. Once the root exists it is the basis, and `fork_basis`
becomes provenance only.

A checkpoint pins one namespace manifest version in a record under `checkpoints/`. It does not affect current visibility. The record stores a `manifest` reference (section 1.7), the `head_commit_id` at that manifest, and tagged `owner` and `status` fields. A `user` owner has a name and optional `expires_at_ms`. A `fork` owner has the target namespace and a required `expires_at_ms`. A `snapshot` owner has a name and a required `expires_at_ms`. The record has no top-level expiry field.

A `snapshot` owner represents an application-created read view. Its name is a
label, not a key, and its expiry is required. A snapshot can be released
explicitly or by garbage collection after it expires. Its record is deleted
after the grace window.

Creation is write-then-verify: write the record active, then verify — under
the self-enforced verify budget — that the floor has not passed the basis and
the basis manifest still loads; on failure release the record and retry
against a newer basis. Combined with the GC grace window and delete-time
re-verification, this closes the create-vs-collect race: a record whose
`created_at_ms` is inside the grace window is still inside its own verify
budget, so nothing releases it for a basis it may yet prove.

The `status` is monotonic. It has two values and one transition:

```text
Missing
  -- create, under a freshly generated id -->
active
  -- one-way compare-and-swap: owner release, or GC observing a passed
     expiry; stamps released_at_ms -->
released { released_at_ms }
  -- GC delete, released_at_ms + grace window -->
Missing
```

`released` is terminal. Nothing returns a record to `active`: there is no
refresh, no renewal, and no revival, and a released record protects nothing
and serves no read. A new pin is a new record under a new id — ids are
generated, never derived and never supplied by a caller, so a pin can never
land on a released record's key and a released id is never reused. Distinct
pins over one basis are distinct records with independent lifecycles.

No checkpoint status transition consults a provider object timestamp. Every
instant the status depends on lives in the record: `created_at_ms` for the
create-vs-collect grace, `owner.expires_at_ms` for the release, and
`released_at_ms` for the deletion.

An owner's `expires_at_ms` means "GC may release this without asking anyone".
A user pin carries the caller's `ttl_ms`, or nothing at all, in which case it
is held until released. A fork owner structurally requires one: it is the
lease for a single fork attempt (section 3.9.2), and letting it pass is how an
abandoned attempt becomes collectable; a fork owner without one fails ordinary
strict deserialization as a missing field. A snapshot owner also requires an
expiry. An expired record remains a root until garbage collection releases it.

Explicit release is user-owned only, and it is idempotent: releasing an
already-released or already-deleted record leaves the same end state. Owner
release and expiry release converge for the same reason — both are the same
one-way compare-and-swap to the same state — so the loser of a race re-reads,
finds what it wanted, and writes nothing. A failed release CAS means the
inspected state changed, so the record is retained without retry. Its basis
becomes collectable only after the record itself is gone (records-last,
"Garbage collection").

A namespace manifest is the durable object for one namespace file-set version.
It may reference one or more immutable metadata runs; standalone checkpoint
records under `checkpoints/` pin manifest versions for retention, fork, or
stable read workflows. Each run is internally segmented without overlapping segment
key ranges; different runs may overlap and readers apply the normal metadata
visibility rules across all referenced runs. Within a segment, rows are stored
in ascending row-key order (adjacent equal keys permitted); readers reject a
segment whose rows are out of order as malformed. Readers load the referenced
runs, then replay only the visible WAL chain after the manifest's `head_seq`.

The WAL preserves commit order even when a segment contains several commits.
Each commit stores `commit_id`, `semantic_commit_fingerprint`, `committed_by`, `committed_at_ms`, an optional `message`, and its metadata changes. The actor reference contains a `kind` and an `id` and is stored once per commit. Validation inputs and operation results are not stored. Checkpoints keep replay work bounded as history grows.

#### 2.9.1 Resolving the metadata basis

The basis is the materialized starting point every read and every flush
builds on. A namespace publishes `metadata/root.json` at its first flush, not
at creation, so the basis is resolved from the head plus that root when the
root exists. There are exactly three cases, and none of them is a fallback
for another.

1. **`metadata/root.json` is present: the basis is the manifest it names**,
   under this namespace's own prefix. This is the steady state, and the two
   rules below no longer run.
2. **The root is absent and the head carries no `fork_basis`: the basis is
   the built-in genesis state.** It is exactly one root-inode row, at
   sequence zero. No manifest object is loaded, and none was ever written —
   create publishes a head and nothing else, so a created namespace with no
   manifest is the expected shape rather than a missing object.
3. **The root is absent and the head carries a `fork_basis`: the basis is the
   source namespace's manifest.** The head names it:
   `fork_basis.manifest.manifest_object_id`, read under
   `fork_basis.manifest.owner_namespace_id`'s prefix.

Case 3 is the only cross-namespace read in the format, and the head is the
only thing that may authorize one. Call this rule the **head-authorized
foreign basis**: no manifest, root, checkpoint, or segment may send a reader
into another namespace's prefix on its own say-so, because only the head is
carried forward verbatim by every publication and so only the head can be
trusted to still mean what it said when the fork happened.

Every load validates the source manifest. Its `namespace_id` must equal `fork_basis.manifest.owner_namespace_id`, and its `payload_checksum` must equal `fork_basis.manifest.manifest_payload_checksum`. Either mismatch returns `namespace_corrupt`; the reader does not try another manifest or fall back to the genesis basis.

`fork_basis.source_checkpoint_id` names the fork-owned checkpoint record on
the source that keeps the basis manifest and its segments alive. That record,
not the fork basis itself, is the reachability root; the fork basis only says
which objects to read.

Once the target's own first flush publishes a root, case 1 takes over and
`fork_basis` is provenance from then on.

The retention floor never advances past the materialized root (section 3.8),
so a namespace that got where it is by following this specification cannot
have an absent root and a floor advanced past its birth sequence. Basis
resolution reports that combination honestly as `namespace_corrupt` instead
of guessing which of the two readings is the true one.

## 3. Write and read protocol

### 3.1 Write protocol

A write has four phases: durably stage content (if a commit contains
content), reconstruct and validate, publish one or more logical commits into
the WAL, and advance the head. A commit request may be rejected immediately,
or tentatively accepted and written to a WAL segment, but it is committed and
successful only if the WAL segment is durably stored and the head advances to
reference it. A metadata change becomes visible only after the head advances.

#### 3.1.1 Content staging

Content must be durable before any metadata change can reference it.

1. Allocate a fresh `content_id`. This happens before any byte is read, so
   the final object key exists up front.
2. Read the namespace head for its `content_store_id`.
3. Upload the complete byte sequence to
   `content-stores/{content_store_id}/objects/{content_id[4..6]}/{content_id[6..8]}/{content_id}`
   with create-if-absent semantics.
4. Build a content reference:

   ```json
   {
     "kind": "blob_v1",
     "content_id": "con_<32hex>",
     "size_bytes": 123,
     "checksum": { "algorithm": "sha256", "value": "<64hex>" }
   }
   ```

Staging the same bytes twice under two different uploads writes two objects,
one per id. Retrying *within* one upload session reuses that session's id and
therefore its object, which is where staging idempotency now lives. An
orphaned content object — one whose upload never completed — is harmless
because nothing can reference an id that was never published.

Because every request against one session writes one object key, staging is
**exclusive**: a request takes a durable claim on the session record before it
writes, and gives it back in the same compare-and-swap that records what it
wrote. A second request that arrives while the claim is held is refused and
writes nothing. This is required rather than an optimization — step 3's
create-if-absent condition cannot be part of a multipart write (§1.7 rule 2),
so two requests that both found the key absent would both assemble over it,
and the one that lost the record swap would leave its bytes behind under the
winner's digest. The claim carries no expiry of its own: it is honoured only
while the session is open, so the session's lease bounds it and a request
cancelled while holding it costs that session the rest of its lease.

**Direct upload** hands the transfer to the client instead of proxying it. The
client declares the size and the checksum in the algorithm the begin response
named; the server mints the identity, signs both the digest and a create-only
precondition into a short-lived write capability, and returns the resulting
`content_ref`. A client can never name the object it writes to.

Completion **verifies rather than trusts**. The server issues one
`HeadObject` with checksum mode enabled and compares the provider's stored
checksum and size against the reference. `GetObjectAttributes` is never used:
Cloudflare R2 answers it with 501, so code that reaches for it passes its
tests against AWS S3 and fails in production. A mismatch fails the completion
and deletes the object — safe precisely because the id is random and
unpublished, so nothing references what is deleted.

#### 3.1.2 Metadata view loading

Before evaluating commit requests, the server loads the current metadata view
using the same procedure described in section 3.2.1: load the head, load the
current manifest, and project the visible WAL segment chain. The server never
trusts caller-supplied metadata.

#### 3.1.3 Validation and logical commits

The server validates each commit request against the reconstructed state:

1. Resolve any operation-local references needed to identify referenced
   content.
2. Verify that all referenced content objects are already durable in object
   storage, and that the reference's kind, size, and checksums match.
   Existence and size prevalidate from a HEAD; the checksum is verified by
   reading and hashing the object bytes (or skipped entirely under a valid
   content admission token, which proves this server already validated the
   staged bytes).
3. Evaluate preconditions in order (see section 3.6 for the precondition
   catalogue).
4. Resolve inode references and allocate new inode ids monotonically from the
   head's `next_inode_id`.

If a request contains multiple operations, they are evaluated sequentially
against ephemeral state advanced by earlier operations in the same request.

Passing validation does not by itself make the request committed or
successful. If a client commit request reaches the success boundary in
section 3.1.4, it becomes one logical commit.

Content reference validation fails before metadata preconditions are evaluated
when:

- `content_ref.kind` is unsupported;
- a checksum value is not the lowercase hex its algorithm's width requires;
- the referenced object is missing from the namespace's content store;
- the object size differs from `content_ref.size_bytes`; or
- the object bytes do not match the reference's checksum.

#### 3.1.4 WAL segment publication and head advance

This is the success boundary.

1. Collect one or more candidate commit requests.
2. Choose publication order and validate those requests against ephemeral
   state advanced by earlier tentatively accepted requests in the same batch.
3. Reject immediately any request whose preconditions fail or whose mutation
   is otherwise invalid.
4. Tentatively accept the remaining requests and assign contiguous `seq`
   values.
5. Write one immutable **WAL segment** containing logical commit records for
   the tentatively accepted requests and the segment metadata needed to
   identify the visible segment chain, recording the local monotonic time the
   PUT began.
6. If the publish budget has elapsed since step 5 began, abandon the segment
   as an orphan and rebuild the commit as a fresh segment; otherwise
   **CAS-update** the namespace head to advance `seq`, `next_inode_id`, the
   visible WAL tip, and `recent_segments` (the bounded newest-first
   predecessor accelerator below the tip; chain links remain the only
   history authority).
7. If step 5 or step 6 fails, the publication fails. A WAL segment written
   before a failed CAS — or abandoned over budget — is orphaned and harmless.

A tentatively accepted request is not yet committed or successful. A request
becomes committed, successful, and visible only if step 5 durably stores the
WAL segment and step 6 succeeds. A request rejected at step 3 receives no
`seq` and creates no durable WAL record.

If step 6's outcome cannot be observed — a transport failure after the update
was sent — the writer must report the commit outcome as unknown, never as
failure: the head may already reference the new segment. A retry must reuse
the same `commit_id` so it replays rather than double-commits (section
3.3.1).

#### 3.1.5 Failure semantics inside a publication batch

A publication batch is not an all-or-nothing multi-client transaction.

The server may:

- reject some candidate requests before publication; and
- tentatively accept other requests into the same batch and, if publication
  succeeds, publish them in the same WAL segment.

Each request still has its own success or failure outcome. Tentative
acceptance inside a batch is not success.

A rejection judged against ephemeral state advanced by a tentative
acceptance (step 2 in section 3.1.4) is contingent on that state publishing:
if publication fails, the writer must report the publication failure for it,
never the semantic rejection. Rejections judged against the durable metadata view
alone stand regardless of the publication outcome.

### 3.2 Read protocol

A read reconstructs the visible filesystem state from durable artifacts on
object storage. No server-side cache or local database is required for
correctness; everything needed is in the object store.

#### 3.2.1 Metadata view loading

The reader builds an in-memory metadata state from two kinds of durable
object:

1. Read the namespace **head** object (the namespace's identity, current
   `seq`, and visible WAL tip) and `metadata/root.json` (the manifest
   pointer), concurrently. The head also supplies the `content_store_id`
   every later step needs.
2. Load and verify the manifest the root references; its payload checksum
   must match the root's `manifest.manifest_payload_checksum`. The manifest
   references one or more materialized metadata runs through its `head_seq`.
   When the root is absent, resolve the basis from the head instead
   (section 2.9.1): the genesis state, or the head-authorized source
   manifest.
3. Use the visible WAL tip named by the head to identify the visible segment
   chain after the basis `head_seq`, then replay the logical commit records in ascending
   `seq` order through `head.seq`. Each logical commit appends normalized rows
   to the same row families.

The result is a metadata view pinned to one `seq`.

For latest path `stat` and directory `list`, an implementation may avoid
hydrating a complete metadata state. The reader may query verified metadata run segments and the visible WAL tail
overlay directly, provided it applies the same visibility rules and treats
missing or corrupt manifest/WAL objects as hard errors. An absent
`metadata/root.json` is not one of those errors: it resolves through the head
under section 2.9.1 like any other basis load.

#### 3.2.2 Visibility rules

Given a metadata state at seq N:

- An **inode** is visible if `created_seq <= N` and no active subtree
  tombstone covers the inode or any of its ancestors.
- A **directory binding** is active if it is the latest
  `(parent_inode_id, name_key)` pair with `bind_seq <= N`, is also the latest
  parent binding for the child inode, and has not been removed by a matching
  direntry unbind.
- A **file revision** is the latest revision for an inode with
  `committed_seq <= N`.

#### 3.2.3 Path resolution

To resolve an absolute path at seq N:

1. Start at the root inode (inode id 1).
2. For each path component, find the active directory binding whose
   normalized `name_key` matches the component under the v0 folding rule
   (section 2.3.1).
3. Follow the binding to its `child_inode_id`; v0 path resolution does not
   cross mounts, and mount traversal is reserved future work.
4. If any component has no matching visible binding, the path does not exist.

#### 3.2.4 File content retrieval

Given a visible file inode at seq N:

1. Look up the file's latest revision at N to obtain `content_ref`.
2. Read the namespace head for its `content_store_id`.
3. Verify that `content_ref.kind` is supported by the reader.
4. For `blob_v1`, fetch the object at
   `content-stores/{content_store_id}/objects/{content_id[4..6]}/{content_id[6..8]}/{content_id}`.
5. Verify `content_ref.size_bytes`, then compute the algorithm in
   `content_ref.checksum` over the complete file and compare the result. The
   read fails if the algorithm is unsupported or the values do not match.

A **file revision** is an immutable content state for one file inode,
identified by that inode's monotonic `revision_no`. A namespace commit `seq`
is the global visibility order for commits; it is not a file
revision number. Revision reads may target either the current path's current
inode or an inode id directly. Path-based revision reads first resolve the
path at the current head; inode-based revision reads use the retained revision
rows for that inode.

#### 3.2.5 Directory listing

Given a visible directory inode at seq N:

1. Collect all active directory bindings whose `parent_inode_id` matches the
   directory.
2. For each binding, resolve the child inode. If the child is a file, its
   latest revision provides size and content identity through `content_ref`.
3. Normal listing must not fetch or validate every referenced content object;
   committed metadata is authoritative for size and `content_ref` summaries.

### 3.3 Logical commits, sequence numbers, and visibility

A successful client commit request is one logical commit.

A request may contain more than one operation, but:

- the operations are evaluated in request order; and
- the request becomes one ordered logical commit in namespace history.

Each successful logical commit receives exactly one namespace `seq`. A request
that is rejected receives no `seq`. Tentative acceptance into a batch is not
success (section 3.1.4).

One head update may publish one or more contiguous logical commits.

A logical commit becomes visible only when the head advances to a value at or
beyond that commit's `seq` and the visible WAL chain includes that commit.

This gives each successful request one `seq` and one replay identity without
requiring one object write or one head update per request.

#### 3.3.1 Commit identity fingerprints

Retry idempotency needs a durable answer to "is this the same logical commit
already published under this `commit_id`?". That answer is the semantic commit
fingerprint stored as `semantic_commit_fingerprint` in every WAL commit record
and commit receipt.

A fingerprint value is `v2:sha256:<64 lowercase hex>`. The `v2` tag names the
canonicalization rules below and `sha256` the digest algorithm, so either can
change later without re-interpreting stored values. The `v2` tag and the `v2`
ending the preimage's `domain` string name the same version.

The `v2` preimage is the compact JSON encoding (no whitespace, object keys in
exactly the order shown) of:

```json
{
  "domain": "loonfs.commit.semantic.v2",
  "namespace_id": "...",
  "actor": { "kind": "user | service | system", "id": "..." },
  "operations": [...],
  "message": "... or null"
}
```

where `actor` contains `kind` followed by `id`, matching the request actor, and
`operations` appear in request order, each as its canonical form
(operation kind, canonical absolute paths, and the operation's semantic
parameters including its caller-supplied race guards), and `message` is
`null` when absent — so reusing a `commit_id` with a different message, a
different guard, or the same operations in a different order conflicts. The
preimage deliberately excludes `commit_id`, writer epoch, and
`committed_at_ms`: a retry of the same logical commit must fingerprint
identically no matter who retries it or when.

Every operation starts with `kind`, using the current API operation name.
The remaining fields appear in this order:

| Kind | Fields after `kind`, in order |
| --- | --- |
| `create_directory` | `path`, `parents` |
| `create_directory_by_inode` | `parent_inode_id`, `display_name` |
| `put_file` | `path`, `behavior`, `content_ref`, `expected_inode_id`, `expected_revision_no` |
| `put_file_by_inode` | `parent_inode_id`, `display_name`, `content_ref` |
| `put_file_revision_by_inode` | `inode_id`, `content_ref`, `expected_revision_no` |
| `move_by_inode` | `inode_id`, `expected_binding_generation`, `to_parent_inode_id`, `to_display_name`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `delete_by_inode` | `inode_id`, `expected_binding_generation`, `behavior` |
| `delete_path` | `path`, `behavior`, `expected_inode_id` |
| `move_path` | `from_path`, `to_path`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `copy_path` | `from_path`, `to_path`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `restore_revision` | `path`, `source_revision_no` |
| `undelete` | `inode_id`, `deletion_seq`, `path` |
| `update_attributes` | `path`, `set`, `remove`, `expected_inode_id`, `expected_attributes_revision_no` |

The encoding is UTF-8, with non-ASCII characters written directly. JSON
quotes, backslashes, and control characters are escaped; slashes are not.
Integers use decimal digits without leading zeroes, and sorted string keys
use lexicographic UTF-8 order.

Every listed field is present. Unset optional fields encode as `null`; default
booleans and behaviors are explicit. Paths use their validated absolute form.
Inode IDs use the numeric storage representation, not the public `ino_` string;
sequence and revision numbers are JSON integers. Binding generations retain
their opaque string representation. Attribute `set` keys are sorted, and
`remove` is sorted and deduplicated. Operation order remains significant.

Exact inputs, canonical JSON bytes, and expected digests for every operation
are shared in `crates/loonfs-api/tests/golden/commit_fingerprints_v2.json`.
These vectors include absent and present race guards and both undelete forms.
The API's request serializer is not the canonical encoder: omitted defaults,
public ID encoding, and transport evidence must not change stored identity.

A content reference enters the preimage as exactly:

```json
{ "kind": "blob_v1", "content_id": "con_<32hex>", "size_bytes": 123 }
```

The checksum is excluded because `content_id` identifies the object. The
checksum verifies that object but does not change its identity. Different
checksum evidence for the same object must not produce a different commit
fingerprint.

The visible consequence is a retry rule. Re-uploading bytes mints a new
content object, so a request that re-runs its upload is a genuinely different
mutation and a reused `commit_id` conflicts. Retrying a commit means sending
the same `ContentRef` again, which replays.

There is one preimage for every commit. A convenience call carrying one
operation and a request carrying a one-element operation list are the same
request, so they fingerprint identically by construction.

The idempotency horizon is the retention floor. Commit receipts below the
floor are dropped when metadata runs are rebuilt, and a dropped id is
indistinguishable from one never used: a commit retried from below the
floor is admitted as a new mutation and commits again. Replay is guaranteed
exactly while the receipt lives — the same window as retained history. This
is deliberate: rejecting late reuse loudly would require an unbounded index
of every id ever committed, and the durable format does not carry one.

A reused `commit_id` with an equal fingerprint replays the originally
committed response; an unequal fingerprint is rejected as
`commit_id_reuse_conflict`, which reports the stored fingerprint so a client
can prove its retry is the same request (API spec, section 5.2). Reference
values and canonical bytes are pinned by shared vectors and semantic tests in
`loonfs-api`; those values must never change within scheme `v2`.

### 3.4 Server authority

The server is authoritative for commit validation.

In particular, the server is responsible for:

- resolving any supplied paths against the current visible tree;
- allocating new inode ids;
- validating name collisions under the v0 folding rule (section 2.3.1);
- validating preconditions;
- verifying that referenced content is already durable; and
- publishing successful logical commits by durably writing a WAL segment and
  advancing the head.

Clients may assist with planning, hashing, upload, or retry, but they are not
the authority for visible state.

The server need not be centralized. The protocol is designed for multiple
writers.

### 3.5 Standard mutation operations

A commit request contains an ordered list of operations. Eight use paths:

- `create_directory(path, parents)`
- `put_file(path, content_ref, behavior, expected_inode_id?, expected_revision_no?)`
- `delete_path(path, behavior, expected_inode_id?)`
- `move_path(from_path, to_path, behavior, expected_destination_inode_id?, expected_destination_revision_no?)`
- `copy_path(from_path, to_path, behavior, expected_destination_inode_id?, expected_destination_revision_no?)`
- `undelete(inode_id, deletion_seq, path?)`
- `restore_revision(path, source_revision_no)`
- `update_attributes(path, set, remove, expected_inode_id?, expected_attributes_revision_no?)`

Five use inode IDs:

- `create_directory_by_inode(parent_inode_id, display_name)`
- `put_file_by_inode(parent_inode_id, display_name, content_ref)`
- `put_file_revision_by_inode(inode_id, content_ref, expected_revision_no)`
- `move_by_inode(inode_id, expected_binding_generation, to_parent_inode_id, to_display_name, behavior, expected_destination_inode_id?, expected_destination_revision_no?)`
- `delete_by_inode(inode_id, expected_binding_generation, behavior)`

Every `path`, `from_path`, and `to_path` is a canonical absolute path (section 2.3). Every `display_name` and `to_display_name` is one path component under the same grammar.

Parameters marked `?` are optional and have no default. The optional `expected_*` parameters prevent races; omitting one disables that check. A revision guard requires its matching inode guard. Inode revision writes require `expected_revision_no`, while inode moves and deletes require `expected_binding_generation`. `undelete.path` overrides the original parent and name.

`parents`, `behavior`, `set`, and `remove` have defaults. `parents` defaults to false. `behavior` defaults to `no_replace` for puts, moves, and copies, and to `non_recursive` for deletes. `set` and `remove` default to empty collections.

The operation kind and parameters are part of the durable commit fingerprint
(section 3.3.1), so this list is part of the format. The server converts each
operation into internal inode changes and then writes the WAL deltas below.
Those internal changes are not part of the wire format.

`move_path` is no-replace by default. Under `no_replace` a destination that
is already bound fails validation. Under `replace` the move replaces the
destination: the commit deletes the destination file and rebinds the source.
Only a file destination can be replaced, and a path never replaces itself.

`update_attributes` describes the requested changes, not the complete result.
`set` contains attributes to write, `remove` contains keys to delete, and all
other keys remain unchanged.
The published `attributes_revision_no` is exactly one past the inode's
current attribute revision, and validation derives it rather than taking it
from the request. An update whose resulting map equals the current one is
rejected: attributes are current state with no history, so a revision that
restates the same map has nothing behind it. Attributes are held against
inode identity, so an inode is the operation's target whether it is a file
or a directory, and every other operation leaves them alone.

These are semantic commit operations. Durable WAL payloads store normalized
metadata deltas derived from the semantic operations: `create_inode`,
`bind_direntry`, `unbind_direntry`, `append_file_revision`,
`tombstone_subtree`, `revoke_subtree_tombstone`, and
`append_attributes_revision`. Raw bind/unbind/create-inode deltas are not
standard client-facing commit operations.

The two tombstone deltas carry the same values their rows do (section 2.5):
`tombstone_subtree` states its `deleted_direntry` as a whole binding, and
`revoke_subtree_tombstone` names its `target` generation. The
delta's own generation is implied — its commit's sequence and its
`delta_index` — so it is not written a second time.

`append_attributes_revision` carries `inode_id`, `attributes_revision_no`,
and the inode's complete `attributes` map. Complete state rather than a
change set: replay never needs an earlier revision to answer what an inode
holds. An empty map is a real revision — the cleared state — and it hides
every earlier map for that inode. The delta's own position is implied by its
commit's sequence and its `delta_index`, like every other delta's.

### 3.6 Preconditions

The server derives each commit precondition from its operation. Callers state
additional guards through the `expected_*` parameters in section 3.5.

The core kinds of precondition are:

| Kind of check | Example use |
| --- | --- |
| **Name-slot based** | "Create this child only if that name slot is still empty." |
| **Name-binding based** | "Move or delete this item only if this name still points at the inode I saw." |
| **Revision-based** | "Replace this file only if it is still at the revision I saw." |
| **Attribute-revision based** | "Write these attributes only if the inode is still at the attribute revision I saw." |
| **Ancestor-visibility based** | "Apply this only if no ancestor was tombstoned." |
| **Directory-contents based** | "Delete this directory non-recursively only if it is still empty." |

The exact binding precondition is
`binding_is(parent_inode_id, name_key, child_inode_id, bind_seq, bind_delta_index)`.
It pins a source path to one specific prior binding, so a rename-away, delete,
or same-name rebind cannot accidentally satisfy a stale move or delete.

### 3.7 Change feed and replay

A namespace exposes an ordered change feed. The feed answers the question:

> What committed metadata changes happened after `seq = N`?

This feed is the basis for sync engines, replication, and other incremental
consumers.

The change feed is ordered by logical commit, not by physical WAL segment. A
segment containing N logical commits produces N ordered change events.

The feed exposes semantic filesystem events in request order; one request
operation can produce several events, and their kinds appear in the event
table in [API spec section 6.11](api.md#611-get-changes).

### 3.8 Retention floor

A namespace may advance a retention floor to say:

> Incremental replay older than this point is no longer promised.

Clients older than the retention floor must re-bootstrap from a fresh
checkpoint instead of replaying from an obsolete cursor.

#### 3.8.1 Attribution and retention

Durable attribution fields describe the recorded event. Inode rows use `created_by`; file revisions, commit receipts, and WAL commits use `committed_by`; tombstones and active deletions use `deleted_by`; and attribute revisions use `updated_by`.

A streaming compaction lease's `writer_id` is the writer id of the process running the job (`MutationContext::writer_id`): the server's in an embedded deployment, the maintenance node's in a standalone one. No other maintenance job stores this value.

The commit fingerprint preimage in section 3.3.1 describes the commit request rather than a durable record. It uses the same `{kind, id}` actor shape as `CommitRequest.actor`.

Retained commits keep their actor in the change feed and in metadata for inode
creation, file revisions, current attributes, and active deletions.

Moves and renames below the retention floor may no longer be available. A
consumer that needs permanent history must copy the change feed before the
floor advances. It keys changes by `(namespace_id, committed_seq)` and targets
by `inode_id`, not by path. It may store `commit_id` for correlation, but not
as a permanent key because the id may be reused after its receipt is removed.

The retention floor may advance only after the system has enough verified
material to keep replay safe at or after that point: advancement derives its
target from the manifest `metadata/root.json` references and verifies that
every metadata segment that basis references still exists before the floor
moves. The probe is advisory — the atomic guarantee is the garbage
collector's obligation to never remove reachable objects ("Garbage
collection") — but a segment that already disappeared must block the floor
while replay can still rebuild the lost state. Corruption discovered after
advancement is caught by read-path checksum validation.

A namespace with no `metadata/root.json` has nothing to derive a target from,
so its floor never advances; it retains from its birth sequence until a flush
publishes a root (section 2.9).

Advancement updates only `wal/floor.json` with compare-and-swap, creating the object on the first advance. The update stores `floor_seq` and never lowers the floor. `floor_seq <= metadata/root.manifest.manifest_head_seq` must hold. Being below the floor makes an object a deletion candidate, but deletion also requires verification at delete time. If the floor passes an active checkpoint's basis, retention wins ("Garbage collection").

A WAL flush materializes the current durable namespace
file-set version: if there is no manifest for the current head, the
implementation writes one absorbing the visible WAL tail and publishes it by
monotonic CAS on `metadata/root.json` — never by touching the WAL head, so
head watchers observe only commits. The flush is the latest-state
maintenance operation and creates no checkpoint record; a superseded manifest
becomes a garbage-collection candidate once nothing pins it.

Every metadata publication — WAL flush and reorganization alike —
self-enforces the metadata publication budget, measured from before its
first segment object is written until its root compare-and-swap is initiated.
A publication that exceeds the budget aborts without publishing: its
immutable outputs stay unreachable and are reclaimed by garbage collection
after the grace window. This bound (with the WAL publish budget for
commits) is what makes the GC grace window's floor derivable ("Garbage
collection", rule 1); maintenance therefore needs no durable build-intent
protocol.

Creating a checkpoint pins one such manifest version deliberately for one
owner. It first flushes the WAL tail as above, then writes
`checkpoints/{id}.json` under a freshly generated id and verifies the basis
after the write, releasing the record on failure and retrying under a new id.
A live manifest does not need to be checkpoint-pinned; checkpoint records
explain why a manifest version must be retained after the root moves on.

### 3.9 Namespace creation and forks

A namespace has one lifecycle, and both ways of starting one share it:

```text
Absent
  -- one create-if-absent of a complete active wal/head.json -->
Active
  -- guarded terminal compare-and-swap -->
Deleted
```

There is no state between absent and active. The head object is the
namespace, so publishing it complete in one conditional write is the entire
installation protocol: nothing under the new namespace's prefix is written
before it, and everything written after it is ordinary namespace history.

#### 3.9.1 Creating a namespace

The request supplies only the new namespace id; the server supplies the
mutation context. The protocol is one step: build the complete active genesis
head — sequence 0, the genesis commit id, writer epoch 0, the next inode id
after the root inode, a freshly minted content store id, and no fork basis —
and write it with create-if-absent.

That write is the whole protocol. No manifest, root, or floor is prepared
before it, and none is written after it: the genesis basis is built in
(section 2.9.1), and the root and floor objects appear when the namespace's
first flush and first retention advance need them.

#### 3.9.2 Forking a namespace

A fork creates a new namespace from the source namespace's current head. The
request supplies only the new namespace id; the server supplies the mutation
context. The protocol is:

1. Read and verify the source head and its WAL visibility chain.
2. Create a verified fork-owned source checkpoint at that head under a freshly
   generated id. Its owner carries the target namespace id and
   `expires_at_ms = now + FORK_CHECKPOINT_LEASE_MS`. This record is the
   reachability root that keeps the source's basis manifest and segments alive
   for as long as the target or a nested descendant may need them. Each attempt
   creates a new record.
3. Read the pinned manifest to get the target's next inode id. Build the active target head with the source's `content_store_id` and a `fork_basis` containing the record's `manifest` reference and checkpoint id. The reference's `manifest_head_seq` is the target's fork sequence.
4. Renew the source checkpoint with compare-and-swap. The record must be active, fork-owned, and assigned to this target. The new expiry must be later than the stored expiry and at least `now + FORK_CHECKPOINT_LEASE_MS`. The remaining lease must cover the target-head write. If either check fails, the fork stops before creating the target.
5. Write the target head with create-if-absent.

The target copies the source's `content_store_id` because a fork shares file
bytes copy-on-write. It inherits the source's materialized name keys
unchanged, which is sound because name-key folding is a fixed rule of the
format (section 2.3.1) rather than a per-namespace choice.

The renewal races with garbage collection on the same record. If GC releases the record first, the renewal fails. If the renewal succeeds first, its later expiry changes the record and causes a stale release to fail its compare-and-swap. Once the target exists, its `fork_basis` keeps the checkpoint reachable. An abandoned attempt leaves only a checkpoint that GC can release after its lease expires.

`FORK_CHECKPOINT_LEASE_MS` is two GC grace windows (section 6, rule 1): one for checkpoint creation and one for target installation.

The fork does not copy content blobs or source metadata segments, and it does not write a target manifest, root, or floor. The target starts its own WAL one sequence above `fork_basis.manifest.manifest_head_seq`. New WAL segments, checkpoints, and metadata segments are stored under the target namespace. Until the first target flush creates a root, readers resolve the basis from the head (section 2.9.1).

#### 3.9.3 Conflicting installs

Create and fork answer a lost create-if-absent the same way, because they
write the same object under the same conditions.

The loser reads the head back. An active head means the id is taken and the
answer is `namespace_exists`; a deleted head answers `namespace_deleted`; a
head that cannot be decoded is corruption and is reported as such — never
overwritten, and never taken as an empty slot. A confirmed precondition failure
remains a plain conflict.

The loser is not told whether the winner was its own earlier attempt.
Only an unacknowledged write compares the head's immutable `namespace_id`,
`content_store_id`, `created_at_ms`, and `fork_basis`; matching fields mean this
attempt's write landed. An embedded caller that wants a retry after a lost
acknowledgment to succeed asks for that explicitly, with its
create-if-not-exists option.

### 3.10 Long-running operations

Some operations are not well described by one request.

Examples include:

- recursive reads that need a pinned snapshot; and
- resumable uploads that need a stable destination binding.

v0 uses upload sessions for resumable uploads. It does not define read
sessions or put intents.

A durable upload session has three statuses:

- `open { expires_at_ms }`: accepts upload work until its lease expires.
- `completed { completed_at_ms, content_ref }`: contains the verified content
  reference and cannot change again.
- `aborted { aborted_at_ms }`: cannot be completed or reopened.

Completion and abort use compare-and-swap. Only one terminal transition can
succeed. Completion verifies the object before changing the status. Abort
changes the status before deleting the object. This prevents cleanup from
deleting an object for a session that is still open. Cleanup is safe to retry.

Each session has `namespace_id`, `upload_id`, `content_id`, `created_at_ms`, a tagged `mode`, and a tagged `status`. The content identity is assigned when the session begins. The durable record uses `mode`, matching the API.

The mode does not change:

- `service_proxied` stores a `staging` state: `idle`, `claimed`, or
  `staged { content_ref }`.
- `direct_put` stores the provider's whole-object `checksum_algorithm`. The
  client sends the final size and checksum at completion.
- `direct_multipart` stores `provider_upload_id`, `part_size_bytes`, and
  `checksum_algorithm`.

Multipart part progress remains on the client. The session stores the part
size and checksum algorithm so a resumed upload uses the original settings.

The following invariants are checked when a record is read:

- Every staged or completed content reference uses the session's
  `content_id`.
- The record carries a `mode` and a `status`. Neither has a default and
  neither may be omitted.
- A completed direct session's checksum uses the mode's stored
  `checksum_algorithm`.

A record that fails any invariant is rejected as corrupt. Upload sessions use
control-object format version 1.

Three rules apply:

1. these objects may be ephemeral when no durability guarantee is required; if
   an operation's correctness, restart safety, or promised resumability
   depends on them, they must be stored durably in object storage;
2. they do not advance namespace `seq`;
3. they do not appear in the namespace change feed.

## 4. Durable encodings and versioning

A stable format needs explicit versioning in three places.

| Layer | What is versioned |
| --- | --- |
| **Storage format** | Durable object envelopes and payload rules (this document). |
| **Protocol binding** | HTTP or other transport shapes (`api.md`). |

A new version should be introduced only when an old implementation could
misread or misapply a new feature.

For the protocol binding, the API spec's "Standard error contract" section is
the registry of stable error codes and HTTP statuses, and of the rule that
clients must ignore unknown JSON response fields and tolerate unknown error
codes.

The namespace head is a storage-format object, and it is authoritative for
the namespace-to-content-store relationship.

### 4.1 Durable envelope layout

Every durable LoonFS object except block segments (sections 4.2.1 and 4.2.2)
is an envelope document with the same leading fields, followed by the
payload as an opaque sub-document:

| Field | Meaning |
| --- | --- |
| `kind` | snake_case object kind string. |
| `format_version` | Per-family format version (see table below). |
| `payload_checksum` | `sha256:<64 lowercase hex>` digest of the exact payload bytes as stored. |
| `payload` | The payload: a raw JSON sub-document in JSON families, a CBOR byte string in CBOR families. |

`payload_checksum` covers the payload inside an envelope. `object_checksum`
covers a complete object that has no envelope.

Two rules make these envelopes evolvable:

1. **Checksums cover stored bytes, never a re-encoding.** Readers verify
   `payload_checksum` against the payload bytes exactly as stored, before
   decoding them. A checksum failure therefore always means corruption;
   version skew can never be misreported as corruption.
2. **Readers probe before they decode.** Readers first decode only `kind` and
   `format_version`, so an object written with an unknown kind or an
   unsupported format version fails with a precise, typed error rather than a
   generic decode error.

Every durable lifecycle field is named `status`, is always present, and uses a
`kind`-tagged object. HTTP responses flatten the same data beside their
`status` field.

One rule governs an absent value in every durable encoding.

**An optional field is omitted when it has no value, and absence never means a
default.** Every field that has a value is written, including a zero number and
an empty list. "Absent means the default" would be a third state beside present
and absent, and no schema language states it, so no durable encoding writes one.

### 4.2 Format families and versions

| Family | `kind` | Encoding | Current version |
| --- | --- | --- | --- |
| WAL segment | `namespace_wal_segment` | CBOR envelope, zstd-compressed; CBOR payload | 1 |
| Metadata segment | none (section 4.2.1) | block sections, per-block zstd + CRC32C | 3 (via namespace manifest) |
| Grep root pointer | `grep_root` | JSON, uncompressed | 1 |
| Grep manifest | `grep_manifest` | JSON, uncompressed | 1 |
| Grep segment | none (section 4.2.2) | block sections, per-block zstd + CRC32C | 1 (via the grep manifest) |
| Namespace manifest | `namespace_manifest` | JSON, uncompressed | 4 |
| Control objects (head, metadata root, WAL floor) | per-kind snake_case names | JSON, uncompressed | 1 (tracked per kind) |
| Checkpoint record | `checkpoint_record` | JSON, uncompressed | 1 |
| Upload session | `upload_session` | JSON, uncompressed | 1 |
| Compaction lease | `compaction_lease` | JSON, uncompressed | 3 |
| Compaction output protection | `compaction_output_protection` | JSON, uncompressed | 1 |
| GC run | `gc_run` | JSON, uncompressed | 1 |
| GC mark page | `gc_mark_page` | JSON, uncompressed | 1 |

JSON families keep their payload inline as raw JSON so manifests and control
objects stay directly readable with generic tooling; CBOR families carry the
payload as a byte string. Control-object versions are tracked per kind so one
kind's payload schema can change without invalidating the others.

#### 4.2.1 Metadata segments

A metadata segment object is not an envelope: it is a sequence of
independently readable sections — prefix-compressed data blocks holding rows
in ascending row-key order, one bloom filter block over per-family lookup
prefixes, then one index block naming each data block's last row key
(`last_row_key`) and byte range. There is no footer and no self-describing
header; the referencing manifest's segment descriptor carries the index and
filter block handles, and is the only entry point into the object. Each section's CRC32C is computed
over its stored (compressed) bytes and lives in the handle that names it —
index entries for data blocks, the manifest descriptor for the index and
filter — so a reader verifies every ranged read before decoding it, and the
manifest transitively binds the object's exact bytes. The descriptor also
stores `object_checksum`, the SHA-256 digest of the full segment, for
publication conflict checks and offline verification. Normal reads use the
per-block checksums instead. When the filter block is small (delta-run segments),
the descriptor additionally inlines the filter's stored bytes as lowercase hex
(`filter_inline`), so a point lookup can rule the segment out without any
object fetch. The inline copy is bound by the same filter handle — it must
decode against the handle's stored length and CRC32C exactly like a fetched
block, and a mismatch is corruption. When the field is absent (large filters
are not inlined), readers fetch the filter block by its handle. The filter
block sits directly before the index block at the end of the object; manifest
loading rejects a descriptor whose handles disagree with that layout, or whose
inline copy's length disagrees with its handle, so the read path assumes both.
Readers reject out-of-order rows, out-of-order index entries, and checksum
failures as malformed. The segment format is versioned by the manifest that
references it (`namespace_manifest` `format_version`), since a segment is
unreachable except through a manifest. Rows inside a segment use the attribution fields defined in section 3.8.1.

A descriptor does not store an object key. Readers derive the key from `owner_namespace_id`, `segment_id`, and optional `compaction_job_id`. Compaction output includes the job id and remains under that job's prefix (section 6.2). Other segments omit the field and use the owner's `metadata/segments/` prefix.

A **run** is the set of segments one producer wrote together, and `run_no` is
its identity. The manifest allocates run numbers from `next_run_no`: a
producer takes that value, stores it on the run, and publishes
a manifest whose `next_run_no` is one higher. A WAL flush takes one number for
the delta run it writes across every family. A rebuild takes one number for
the run it writes for one family group. So `run_no` and `family` together name
one family's segment list inside one run, and `segment_index` numbers that
list from zero, once each, in the order the segments were written.

Compaction planning derives run sizes from the referenced objects' block handles. The manifest stores the current run layout, without scheduling counters or merge history.

A run also carries `run_seq`, the namespace sequence it materialized through,
and `tier`, which is either `delta` or `base`. A WAL flush writes a delta run,
and so does a merge that starts above its family group's oldest run. A rebuild
that starts at its group's oldest run writes a base run, and it replaces the
base run it read, so a group holds at most one. Two runs never share a number.
Two runs may share a sequence and a tier, because a rebuild writes its output beside
the runs it did not read; those runs hold different families, so no read ever
compares them.

A producer writes a family's rows in ascending key order and writes no key
twice. So one family's segments inside one run have ascending key ranges that
never touch, stated by `min_row_key` and `max_row_key` and ordered by
`segment_index`.

Manifest loading rejects a manifest that breaks any of this: a `run_no` at or
above the manifest's `next_run_no`, a duplicate `run_no`, a family's
`segment_index` values inside one run that are not
zero-based and dense, and key ranges that descend or overlap.

Every metadata row key identifies exactly one row, so a read merges runs by key and never has to choose between two rows for one key.

##### Row-key grammar

A row key contains hyphen-separated components. The first component is the singular, kebab-case family name. Numeric components use fixed-width decimal encoding: 20 digits for `u64` and 10 for `u32`. This makes byte order match numeric order. String components such as `name_key` and `commit_id` use the lowercase hexadecimal encoding of their UTF-8 bytes.

The row's `kind` and its family serve different purposes. A row kind may appear in multiple families, so their names do not need to match. For example, a `direntry_bind` row appears in both `direntry_binds` and `direntry_child_binds`.

The ten families and their exact grammar:

| Family | Row key | Filter key |
| --- | --- | --- |
| `inodes` | `inode-{inode_id:020}` | the row key |
| `direntry_binds` | `direntry-bind-{parent_inode_id:020}-{name_key_hex}-{bind_seq:020}-{bind_delta_index:010}` | `direntry-bind-{parent_inode_id:020}-{name_key_hex}` |
| `direntry_child_binds` | `direntry-child-bind-{child_inode_id:020}-{bind_seq:020}-{bind_delta_index:010}-{parent_inode_id:020}-{name_key_hex}` | `direntry-child-bind-{child_inode_id:020}` |
| `direntry_unbinds` | `direntry-unbind-{parent_inode_id:020}-{name_key_hex}-{bind_seq:020}-{bind_delta_index:010}-{unbind_seq:020}-{unbind_delta_index:010}` | `direntry-unbind-{parent_inode_id:020}-{name_key_hex}` |
| `revisions` | `revision-{inode_id:020}-{u64::MAX - revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` | `revision-{inode_id:020}` |
| `tombstones` | `tombstone-{root_inode_id:020}-{generation.seq:020}-{generation.delta_index:010}` | `tombstone-{root_inode_id:020}` |
| `active_deletions` | `active-deletion-{deletion_seq:020}-{root_inode_id:020}-{sort_rank:010}` | the row key |
| `commit_receipts` | `commit-receipt-{commit_id_hex}-{committed_seq:020}` | `commit-receipt-{commit_id_hex}` |
| `attributes` | `attribute-{inode_id:020}-{u64::MAX - attributes_revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` | `attribute-{inode_id:020}` |

The family groups and their exact members:

| Group | Row families |
| --- | --- |
| `bindings` | `direntry_binds`, `direntry_child_binds`, `direntry_unbinds` |
| `revisions` | `revisions` |
| `inodes` | `inodes` |
| `tombstones` | `tombstones` |
| `active_deletions` | `active_deletions` |
| `commit_receipts` | `commit_receipts` |
| `attributes` | `attributes` |

`direntry_binds` and `direntry_child_binds` store the same `direntry_bind` rows under different keys. The two revision families do the same for `file_revision` rows. A row key therefore depends on both the row and its family.

The `inodes` and `active_deletions` families store the full row key in the filter. Inode lookups already know the full key, while active deletions are read only by range scans.

The active-deletion rank is `0000000000` for a removal marker and `0000000001` for a listed deletion. This order lets a scan process the removal first. Components written as `MAX - x` are inverted so ascending scans return the largest values first.

#### 4.2.2 Grep roots, manifests, and gram-index segments

`loonfs-grep` owns all grep durability under the namespace extension prefix:

```text
namespaces/{namespace_id}/extensions/grep/
├── root.json
├── manifests/{manifest_object_id}.manifest.json
└── segments/{segment_id}.sst.zst
```

`manifest_object_id` is `gmf_` followed by 32 lowercase hex characters, drawn
fresh for every candidate. It names *which object* holds the manifest and says
nothing about its contents: a content-derived id would make an identical
rebuild reuse the object an earlier publication left behind, and that reuse
is what would let collection race a publication for a manifest the winner is
about to point at. The bytes are bound to the pointer instead, through
`manifest_payload_checksum`. Namespace manifests carry no
grep pointer, watermark, status, or segment references. A fork therefore
starts without grep state until grep is enabled for the target.

`root.json` is a small mutable pointer envelope with these fields, in order:

- envelope: `kind = "grep_root"`, `format_version = 1`,
  `payload_checksum`, and raw JSON `payload`;
- payload: `namespace_id`, `manifest_object_id`, and
  `manifest_payload_checksum`, which must equal the named manifest envelope's
  own `payload_checksum`.

This is not the namespace manifest reference from section 1.7. A grep manifest has no logical position or head sequence, so the pointer names its manifest without them.

Each immutable manifest has the same envelope grammar with
`kind = "grep_manifest"` and `format_version = 1`. Its payload is the full
grep state: `namespace_id`, `status`, nested `index` bookkeeping, and the
`segments` descriptors. Both decoders verify the checksum over the exact
stored payload fragment before decoding, reject unknown versions and kind
mismatches without fallback, and validate namespace, status, fold,
run-allocation, and segment invariants at every boundary. A manifest
load additionally requires the loaded envelope's `payload_checksum` to equal
what the pointer promised, which is the same binding a metadata root holds
over its namespace manifest. Both root pointers and immutable manifests reject
unknown envelope and payload fields, including nested state and descriptors.

The nested `index` object holds what every phase has — the in-progress
`reorganize` state and the `next_run_no` allocator — while each phase's own
position lives in the `status` tag beside it:

- `backfilling`: `target_seq` (the namespace sequence the pinned checkpoint
  captured), optional `cursor_inode_id` (the inode the walk resumes strictly
  after), and `checkpoint_id`;
- `active`: `built_through_seq` and `next_event_index`, which is zero when
  the cursor sits at a commit boundary;
- `disabled`: no fields, no segments, and no reorganization.

A phase carrying another phase's sequence is not representable. The index is
derived state and can be rebuilt from a fresh checkpoint.

A gram-index segment uses the section 4.2.1 block grammar unchanged —
prefix-compressed data blocks, one bloom filter block, one index block,
handles and checksums in the grep-manifest descriptor — with a grep-owned row
payload instead of metadata rows. Its `object_checksum` is the SHA-256 digest
of the complete stored segment, with the same meaning as the metadata segment
field.

Its descriptor uses the section 4.2.1 run vocabulary unchanged too. `run_no`
is the run's identity and comes from the grep manifest's own `next_run_no`;
`run_seq` is the namespace sequence the run materialized through;
`segment_index` numbers one run's segments from zero; `row_count` records the
segment's rows; and `min_row_key` and `max_row_key` state the segment's key
range. Only `level` differs, because
grep reorganizes in three tiers rather than two: `0` is a delta run, `1` is a
mid run merged from delta runs, and `2` is the base run merged from everything
below it. A reorganize in progress records the level and the run number it
stamps on its outputs, so a step that resumes writes into the same run. Grep
loading rejects a `run_no` at or above `next_run_no`, in the manifest's
segments and in the reorganize state alike.

The tokenizer, row shapes, and posting encoding below are frozen by grep
manifest format version 1; their evolution follows the rules in section 4.3
and always permits rebuilding this derived work (section 6.6).

- The **tokenizer** is every overlapping three-byte window (gram) of an
  eligible revision's content, after folding ASCII letters to lower case.
  Grams are bytes, not characters.
- A **row** is a kind-tagged CBOR document, kind `gram_postings`: one gram
  (six lowercase hex characters), the batch's first inode id, and a packed
  posting batch. Its row key is `gram-{gram hex}-{first inode id:020}`;
  its filter key is the `gram-{gram hex}` prefix, so the segment's bloom
  filter answers gram-presence probes.
- A **posting batch** is a varint-packed run of `(inode_id, revision_no)`
  pairs sorted strictly ascending: the posting count, the first posting's
  inode id and revision number, then for each subsequent posting its inode
  delta and absolute revision number, all as LEB128 varints. Postings name
  durable inode identity, never paths. Readers reject empty, unordered, or
  trailing-byte batches as malformed.
- Several rows may carry the same gram (within a segment and across
  segments); readers union their batches.

Publication writes segments first, writes the manifest under a freshly minted
id with create-if-absent semantics, and finally installs `root.json` with one
etag compare-and-swap (or create-if-absent for the first pointer). A
pointer-CAS loser's manifest and segments remain unreachable derived garbage;
grep GC reclaims them after its grace window. Because every candidate is
written under an id no earlier publication used, an unreferenced manifest is
always the leftover of a publication that has already ended, and the grace
window covers the one that has not: it is at least the derived minimum grace
window ("Garbage collection", rule 1), and grep enforces the same publication
budget the runtime's own publications do. Query readers load the pointer
afresh, then load the immutable manifest it names and check its
`payload_checksum` against the pointer; decoded manifests may be cached by
that checksum.

The namespace-scoped layout is maintained only when that namespace is named
by an enable, publish, query, detached assignment, or explicit GC operation;
grep never enumerates namespaces. Every host schedules the index through the
runtime's maintenance runner, which is nudged by those events and otherwise
reconciles only the keys it has admitted. Grep GC is explicit and per
namespace: it retains the verified pointer, referenced manifest, and
referenced segments,
degrades to retention on corruption or ambiguity, and reaps the whole
`extensions/grep/` prefix when explicitly pointed at a tombstoned or absent
namespace. Core maintenance does not recognize or collect `extensions/` keys,
and grep maintenance does not collect core-owned objects.

### 4.3 Evolution rules

- **One version mechanism per object.** An object's version is the
  `format_version` field in its envelope. That field governs the whole payload,
  including nested objects, and no payload carries a `format_version` of its
  own. A kind name that ends in a version, such as the `blob_v1` content-ref
  kind, names one closed shape and is not a second version mechanism.
- **Additive within a released version.** Readers ignore unknown payload and
  envelope fields, at every level of nesting, except inside the closed shapes
  named in the encoding conventions above. After the first stable release,
  adding such fields is the only change permitted within an existing format
  version.
- **Other post-release changes require a new version.** After the first stable
  release, renaming, removing, retyping, or re-tagging any field — or changing
  the payload encoding — requires a new `format_version` for the owning family.
  Readers reject versions they do not support with a typed unsupported-version
  error; there is no silent fallback.
- **A durable digest names its algorithm, and where the algorithm is chosen
  decides the shape.** Three shapes cover every durable digest. An envelope,
  pointer, or whole-object digest is the string `sha256:<64 lowercase hex>`:
  `payload_checksum`, `manifest_payload_checksum`, and `object_checksum` are
  written this way, and the prefix lets a future algorithm be introduced
  without re-interpreting old values. A content or part checksum is an object
  with an `algorithm` field and a `value` field, because the algorithm is
  negotiated per transfer (section 1.6). The algorithm is its own field there,
  so the value carries no prefix. A block CRC is a bare integer whose field
  name is the algorithm, `crc32c` in a block handle (section 4.2.1), because a
  handle is fixed-size and the format fixes the algorithm. Commit fingerprints
  additionally carry their canonicalization scheme (`v2:sha256:<hex>`, section
  3.3.1) because their preimage rules can evolve independently of the
  algorithm.
- **Unknown content-ref kinds round-trip in immutable records.** A reader that
  does not understand a `content_ref.kind` must preserve the original string
  when it relays or rewrites an immutable record, so a later format version
  can add a kind. A reader of a mutable control object rejects an unknown kind
  instead, because a guarded rewrite must not carry a kind it cannot validate.
  No reader may create new references with kinds it does not understand
  (section 3.1.3).
- **Every encoding is pinned by golden-byte fixtures**
  (`crates/loonfs-api/tests/golden_formats.rs`). An encoder change that alters
  durable bytes fails those tests. The grep families are pinned under the same
  mechanism in `crates/loonfs-grep/tests/golden/`.

## 5. Extension-owned materialization

Derived subsystems own their durable state below
`namespaces/{namespace_id}/extensions/{name}/`. A namespace manifest contains
no extension registry or generic extension metadata. Each extension defines
its own key grammar, versioning, readiness marker, and collection rules; for
example, grep materialization is visible only through its verified
`extensions/grep/root.json` pointer.

Core readers and maintenance ignore extension-owned keys. An extension must
remain rebuildable from authoritative core state and must not require an
unknown extension to be understood before the namespace can be read.

Core defines no extension registry. In particular, grep state lives in the
section 4.2.2 keyspace. This separation lets derived indexes and similar
per-namespace capabilities arrive without changing the namespace-manifest
format.

## 6. Maintenance operations

Maintenance keeps read cost bounded, retention safe, and durable state clean.
Maintenance **effects** are normative format semantics; maintenance
**scheduling and triggering** are not. Two behaviors keep an un-administered
deployment's read costs bounded regardless of scheduling: the reference
implementation's writer folds the WAL tail into a manifest after a publish
observes the tail at or past the WAL-tail policy's checkpoint threshold
(32 segments at defaults), without delaying that publish, and every publish
surface rejects with `maintenance_required` once the tail reaches the same policy's
write-rejection threshold (128 at defaults). Reads never gate on tail
length. Bounded reads are the
automatic half only: the retention floor never advances on its own, so
history retention — and the row reclamation that follows it — remains an
explicit operator decision. An embedded engine where an operator
triggers maintenance manually and a server that runs the same work invisibly
are equally conformant (see `api.md` for the optional maintenance API group). The
invariants below bind every implementation, whoever runs the work: maintenance
never creates a second source of truth for the filesystem.

### 6.1 Manifest publication and checkpoint verification

A namespace manifest records one namespace file-set version (the section 1.2
table lists its contents).

A checkpoint is a durable record that pins one manifest version. A checkpoint
is useful only after both the checkpoint record and its referenced manifest
are verified. Readers must prefer the current verified manifest plus the
visible WAL segment chain over unverified or partial manifest artifacts.

The namespace manifest may reference one or more immutable metadata runs. Runs
are not a second source of truth; they are rebuildable metadata rows used to
keep normal metadata view loading from replaying an unbounded WAL tail.

File revisions are stored once in the `revisions` family, newest first within
an inode, using the same descending revision, sequence, and delta ordering as
attribute revisions. Exact revision reads and paginated history scans use this
family directly. The namespace manifest version governs these row-key meanings;
version 3 requires this ordering. The metadata block framing is unchanged.

Segment reads verify the per-block checksums in the block handles and enforce
key ranges. Directory bindings retain both parent-and-name and child lookup
families. Manifest loads enforce per-run row-count equality between these two
families; every reorganization rewrite checks their full row-level equality
over the complete input runs it selected.

### 6.2 Compaction

Compaction rewrites metadata runs (and, in the future, content layouts) into
more efficient physical shapes.

A rebuild merges an oldest-first run of runs for one family group. It may
skip the run at the oldest end when that run is too large to read inside one
step's budget, and then it merges the delta runs above it; it never steps
over a delta run.

**A rebuild's output is a base run if and only if its window starts at the
group's oldest run.** The tier a run carries and the rules that produced it
say the same thing: a base run is one some rebuild was allowed to drop rows
from, a delta run is one nothing has dropped from yet. So a family group
holds at most one base run — a bottom-anchored rebuild always contains the
group's existing base run and replaces it, and nothing else writes one — and
a manifest that carries two base runs for one group does not load.

The output stands where its window stood, so no row moves past any other. A
bottom-anchored rebuild's output is stamped at the manifest's `head_seq`;
base runs sort below every delta run whatever sequence they carry, so it
lands at the bottom of the group where its inputs were. A rebuild that
skipped the oldest run writes a delta run stamped at its newest input's
sequence, which is where that run stood: above every run the window left
below it, below every run it left above.

A rebuild that skipped the oldest run drops nothing, because the rules below
read across the merged rows and a skipped run may hold the other half of a
pair. Such a rebuild reduces the group's run count without touching its base.
It merges two or more runs into one — merging a single run would rewrite it
as itself, at its own identity — so a group whose delta runs are down to one
and whose base is over budget has no rebuild left to run.

A base rebuild that starts at the group's oldest run drops rows that no
retained sequence can observe: bindings superseded or unbound at or below the
retention floor, spent unbind markers, and commit receipts below the floor.
The floor governs replay state only.
Revision rows are never dropped: file revision history is durable data,
retained in full regardless of the floor, and a revisions listing is always
complete. Tombstone rows — set and revoke events alike — and inode rows are
always retained for now; reachability-based dropping for them is future
work.

The `active_deletions` family holds current state rather than history, so the
retention floor has no say over it at all: a `listed` row is never dropped
however far the floor advances, because a deletion stays recoverable
indefinitely and dropping the row would silently retire it. The only rows a
rebuild removes there are the cancelled pairs — a `removed` row and the
`listed` row whose key it repeats, dropped together, since a deletion that was
undeleted is not state any reader can still observe. A `removed` row can never
outlive the row it names: the deletion commits before the undelete, runs merge
oldest-first, and a rebuild only drops rows when its input starts at the
group's oldest run, so both rows are always in the same merge.

The `attributes` family is folded by the same rule the retention floor gives
every other superseded row, applied per inode: every revision above the floor
is kept, the newest revision at or below the floor is kept, and the rest are
dropped. The newest-at-floor row is kept even when its map is empty, because
an empty map is the cleared state — dropping it would let an older non-empty
map become the newest row and give a caller back attributes they cleared.
Attributes are never dropped for being unreachable: a deleted inode keeps its
rows, the same posture inode and tombstone rows take, and that is what makes
an undelete give back the map the inode had. A rewrite refuses to compact
when two rows for one inode share a revision number at or below the floor,
because that makes "the newest at the floor" arbitrary and the drop unsafe.

A rebuild that cannot fit within one bounded maintenance pass runs as a streaming compaction. The job merges every run in the group and writes output segments as they fill. These segments use `namespaces/{owner_namespace_id}/metadata/compactions/{job_id}/segments/{segment_id}.sst.zst` instead of `metadata/segments/`.

Each family group has one lease at `namespaces/{owner_namespace_id}/metadata/compaction_leases/{group}.json`. An unexpired `active` lease excludes another job for that group. An expired, `completed`, or `reaping` slot can be replaced with one compare-and-swap; the old job's refresh then fails. Before each publication attempt, the job confirms its current lease deadline at `metadata/compactions/{job_id}/protection.json`. No output is written after this record is created. After the final publication attempt, the group slot becomes `completed`. The protection record remains until the job's output prefix is empty. An older collector therefore retains its protection even after a newer job takes over the group. A published manifest references the segments in place. Each descriptor stores `compaction_job_id`, and readers use it to derive the key (section 4.2.1).

The rules a rebuild applies are the same however it runs them. A bounded
merge holds every row of its window and decides them together. A streaming
compaction cannot, because one inode's attribute history and one
parent-and-name slot's binding generations have no size limit, so it runs
each rule as a streaming operator holding a fixed number of fields and at
most one row. The row-key grammar is what makes the two agree: attribute rows
of one inode arrive newest first, a deletion's removal marker arrives before
the row it removes, and a bind arrives before the unbinds of its own binding
generation.

Invariants:

- Compaction MUST NOT change logical content: the visible metadata state at
  every retained `seq` is identical before and after.
- Compaction MUST publish its results through the normal manifest publication
  path; readers never observe a partially compacted state.
- Compacted inputs MUST remain available until no retained manifest version
  or checkpoint record references them.

Checkpoint records are standalone files under `checkpoints/`. Maintenance
never creates one: automatic root advancement leaves superseded manifests and
folded-away segments unpinned, and garbage collection reaps them under the
grace-window and delete-time re-verification rules ("Garbage collection").
A checkpoint record is a deliberate pin — fork sources and explicit maintenance
checkpoints — and roots its basis for as long as the record exists.

### 6.3 Retention management

Retention management decides how far back incremental replay is still
promised. It bounds only replay state — change-feed resumption, superseded
binding rows, and commit receipts — never file revision history, which is
retained in full.

A retention floor may advance only when the system has enough verified
material to support readers from the new floor forward, and it never
advances implicitly: the default posture retains everything, and the floor
moves only when an operator requests an explicit retention advance.

### 6.4 Garbage collection

Delete is tombstone-first. Garbage collection reclaims unreachable metadata
and content still owned by upload-session records under the rules below.
There is no general sweep or purge guarantee for previously published content;
content may remain indefinitely after file or namespace deletion. GC and floor advancement are the only
consumers of listing, and nothing sweeps by default: a pass runs only through
the maintenance endpoint or an explicit maintenance-step opt-in.

Before reserving a new collection, a collector verifies the namespace head.
An absent head means there is nothing to collect. WAL-head format version 2
also gates this GC protocol: a version-1 collector must refuse the head
before it can delete anything. The head payload is unchanged, but the
collection coordination required to interpret its references has changed.
There is no mixed-protocol collection or compatibility fallback.

GC uses resumable listing mark-and-sweep. Its inputs are `wal/head.json`,
`wal/floor.json`, `metadata/root.json`, the seven point-read compaction lease
keys, and the `metadata/manifests/`, `metadata/segments/`,
`metadata/compactions/`, `checkpoints/`, and `wal/segments/` collections. A live manifest roots every object key its
`runs` list names, wherever that key sits. The pass also
sweeps `uploads/`, and that sweep owns content reclamation as well; the two
halves are split at the completed line and described in rule 11.
Core GC never recognizes, lists specifically, or deletes any object below a
namespace's `extensions/` prefix; grep collection is owned by `loonfs-grep`.
Because floor, root, and checkpoint publication no longer serialize through
one head CAS, two cross-object races must be closed explicitly —
create-vs-collect (a record written while GC concludes its basis is
unreferenced) and publish-in-flight (an object written moments before its
publishing CAS) — under these rules:

1. **Grace window.** A configured window `T` with a derived floor, not a
   free tuning parameter:

   ```
   T >= max(WAL_PUBLISH_BUDGET, CHECKPOINT_VERIFY_BUDGET,
            METADATA_PUBLICATION_BUDGET)
        + PROVIDER_OP_DEADLINE + PROVIDER_ATTEMPT_TIMEOUT
        + GC_SAFETY_MARGIN
   ```

   The constants live in one place (`loonfs-core`'s `limits` module; the
   provider bounds in `loonfs-objectstore`), every publication self-enforces
   its budget by refusing to initiate its root compare-and-swap once the
   budget is spent, and provider operations consume one deadline across
   retries. Multipart transfers of large immutable payloads carry no
   whole-operation deadline; the floor's provider terms remain the
   small-object bounds because everything the inequality times — the budget
   self-checks, which are wall clock regardless of per-operation deadlines,
   and the final compare-and-swap — concerns small control objects. A
   window below the floor is rejected as `invalid_request` at every
   surface. Under the floor's inequality, any acknowledged root
   publication lands its compare-and-swap before an object it references
   could age past `T`, so GC never deletes any object younger than `T`,
   reachable or not. An object without a provider timestamp reads as
   young.

   Checkpoint records are the one family whose age is not a provider
   timestamp. The record carries every instant its lifecycle needs, and
   `T` is applied to those instead: a record is deleted `T` after its own
   `released_at_ms`, and a record whose basis is verifiably absent is
   released only `T` after its own `created_at_ms`, which is what keeps a
   create still inside its verify budget from being raced.

   An object's provider timestamp says when it appeared, not when it stopped
   being referenced, and those differ for every object a publication once
   named. `T` therefore also runs from the unreferencing, which the
   **reference manifest generation** dates. A lost root compare-and-swap can
   leave multiple immutable manifest candidates with one `manifest_no`, and
   an older root no longer records which candidate won. Call R the newest
   manifest generation under the namespace's own prefix whose surviving
   candidates all have provider timestamps at least `T` old. R roots every
   candidate key in that generation, the union of their `segments` and
   content references, and every WAL segment above the lowest `head_seq` any
   candidate carries. The published candidate is therefore included even
   when its same-generation siblings are abandoned. An unreferenced object
   is deleted only when the current root set, R, and the object's own age all
   agree. A
   namespace that has published no manifest needs no R: nothing has ever
   stopped referencing anything under it, and its floor cannot have advanced
   past its birth sequence without a root. A namespace whose manifests are
   all younger than `T` has no R available, and the pass then deletes no aged
   object at all rather than deleting against evidence it does not have. A
   terminally deleted namespace needs no R either: no read can reach a
   tombstone. R protects itself, so a pass never sweeps away the evidence the
   next pass needs.
2. **Floor is necessary, not sufficient.** Being below `wal/floor.json` only
   nominates an object for deletion.
3. **One reserved run, one fixed clock.** Collectors reserve `gc/run.json`
   by create-if-absent or CAS of a completed run **before** reading the root,
   head, and floor snapshot. Every collector joins this run. Its
   `started_at_ms` and grace policy remain fixed through all pauses and
   host changes. No candidate is deleted until its complete reference index
   has been sealed. Every readable checkpoint record in a live namespace
   protects its basis, including released records; only the later sweep can
   remove those records. Release is terminal and IDs are never reused.
   Consequently a new valid pin transfers references from the captured root
   or an already protected pin. A new immutable publication is protected by
   the fixed cutoff and publication budgets; streaming compaction uses the
   independent protection in rule 12. An arbitrarily paused older collector
   retains these same protections and cannot advance a newer run's CAS state.
   Mutable candidate lifecycles are still inspected when swept.
4. Roots: `metadata/root.json`; the reference manifest R (rule 1); active
   checkpoint records whose owner still
   stands — a user or snapshot pin until its expiry passes, a fork pin until
   nothing can read through it any more (rule 10); and the visible chain from
   `wal/head.json.visible_wal_tip` down to the floor. A namespace with no
   root of its own has no manifest or segment to protect under its own prefix;
   its basis, if it has a foreign one, is protected on the source side by
   the fork-owned checkpoint record. On a terminally
   deleted namespace the root set shrinks to fork-owned records protecting
   a live target (and their bases): reads are impossible and the tombstone
   is immutable, so user and snapshot pins, the final replay chain, and the
   last manifest protect nothing and age out. The head survives as the
   id-retiring tombstone, together with the root and floor objects if the
   namespace ever wrote them.
5. A root the pass cannot resolve causes retention, not deletion: a root
   manifest that is absent, or that the store will not hand over,
   suppresses manifest and segment deletion for the whole pass. A root that
   is corrupt is a different case, because retaining it would suppress
   deletion on every later pass as well and nothing would ever say why. An
   object under `checkpoints/` that does not decode as a checkpoint record,
   and a referenced manifest that reads but fails validation, both fail the
   pass and are reported with the object key.
6. Only validated manifests are trusted to protect data.
7. WAL needed to replay from the chosen metadata root to the head is never
   deleted.
8. **Retention wins residual races.** If the floor is ever observed ahead of
   an active checkpoint's basis, the checkpoint's objects remain protected;
   reconciling the floor is an explicit recovery action.
9. **Immutable sweep families need no two-step deletion.** WAL segments,
   metadata segments and manifests, grep segments and manifests, and content
   blobs have keys that can only contain identical bytes under their
   create-if-absent, content-derived, or write-verification protocols. Once
   one is unreferenced and grace-aged, unconditional deletion is safe: a
   zombie retry can at most recreate identical, still-unreferenced bytes for
   a later pass. Content objects are never enumerated by listing the content
   store, which is shared by every namespace whose head names it; they are
   reached only through the upload session that owns them (rule 11).
10. **Fork checkpoints require an exact reference.** A fork-owned record remains a root while its active target's `fork_basis` names the record's source namespace, checkpoint id, and manifest. An absent target keeps the record until its lease expires. A deleted target with no metadata root makes the record releasable immediately. A deleted target with a metadata root retains the record conservatively, because nested checkpoint creation must publish that root before installing a descendant and the descendant's manifest may still name source-owned objects. An active target without a fork basis, or one that names another source or checkpoint, makes the record releasable immediately. This also allows GC to release records created by failed fork attempts against an existing target.

    If the target head or metadata root cannot be read, GC retains the record or fails the pass without deletion. Checkpoint basis verification rejects a deleted source, so a checkpoint attempt that publishes a late root after the target tombstone cannot install a new descendant. If the source namespace and checkpoint id match but the manifest differs, the namespace is corrupt and the pass fails.
11. **Uploads and content, split at `completed`.** One sweep of `uploads/`
   owns both halves, because a session record is the only handle on the
   content object it created.

   *Before `completed`, the upload half owns everything, and its reasoning
   is session-local.* An `open` session whose `expires_at_ms` has passed by a
   grace window is compare-and-swapped to `aborted` under the etag loaded
   with it, and only then is the object at its content id deleted, together
   with any provider-side transfer the session had started. A lost
   compare-and-swap retains the session for a later pass. An `aborted`
   record repeats that cleanup — covering a crash between the swap and the
   delete — and is deleted a grace window after its own `aborted_at_ms`. No
   reachability question arises: a random content id that was never
   published belongs to exactly one session, and a session that never
   completed never had a receipt, so nothing anywhere can reference it.

   *At `completed`, ownership passes to the content half.* Completed content
   may or may not have been published, and consumption is inferred from
   metadata references rather than recorded on the session. A completed
   session's content object is reclaimed only when all of the following
   hold: `completed_at_ms` is older than the derived grace below; the pass is
   not degraded (rule 5); and no content reference reachable from this
   namespace's roots names it. The reachable set is the same root set the
   rest of the pass uses — every revision in every manifest the live set
   protects, which includes each fork basis a fork-owned record pins, plus
   every `AppendFileRevision` in the retained WAL. Either way the session
   record itself is then deleted; when the content is referenced, metadata
   protects it from that point on.

   *The grace is derived, not tuned.* A reference can enter metadata only
   through a receipt, a receipt is minted only from a durable `completed`
   session, and minting stops a fixed window after completion. So:

   ```
   CONTENT_RECLAMATION_GRACE
       >= COMPLETED_UPLOAD_RECEIPT_WINDOW   (the last receipt that can exist)
          + CONTENT_RECEIPT_TTL             (how long it admits a commit)
          + T                               (rule 1: that commit's publication)
   ```

   Past that sum, no receipt survives, so the set of references to this
   content can no longer grow and a reference set collected earlier in the
   pass is still sound at delete time — which is why the content family
   needs no delete-time re-verification. The constants live beside `T` in
   `loonfs-core`'s `limits` module and the inequality is a compile-time
   assertion. A publication in the same process that completed the session
   holds its admission directly instead of carrying a receipt, but that proof
   carries a deadline no later than the expiry of the last token the session
   could issue.
   Batch admission checks the deadline again, so the same inequality covers
   both local proofs and remote tokens. The reasoning above assumes a content
   object is referenced only by the namespace whose session created it and by
   fork descendants reading through a pinned basis. Prepared evidence is
   therefore namespace-bound even when catalogs share a content store, and an
   embedded raw-ref import (section 2.8) writes the verified bytes under a
   fresh destination-owned identity. A future identity-preserving copy would
   have to root the reference on the source side the way a fork does.
12. **A compaction job's objects are decided by its lease.** A streaming
   compaction ("Compaction") publishes nothing until it finishes and is paced
   by no budget, so its output is unreferenced for as long as the job runs.
   `T` only has to cover a publication in flight, so a sweep applying it to
   `metadata/compactions/` would delete the output of a job still writing it,
   and no fixed window can replace it: any such window is a guess at how long
   a job may run, which is exactly what the design refuses to bound.

   The lease says so instead. Every job owns the prefix
   `namespaces/{namespace_id}/metadata/compactions/{job_id}/` and writes its
   output under `segments/` inside it, while its family group has one lease at
   `namespaces/{namespace_id}/metadata/compaction_leases/{group}.json`. The
   lease carries ownership only — job, namespace, group, `writer_id`, the tagged
   `status`, `started_at_ms`, `expires_at_ms` — and never a cursor, an output
   descriptor, an offset, or resumable progress. The job creates a missing
   lease `active` with create-if-absent before its first output object, takes
   over an expired `active` lease with one compare-and-swap, and refreshes the
   etag it last observed every `METADATA_COMPACTION_LEASE_REFRESH_INTERVAL_MS`
   while it runs and at the top of every finalization attempt. Creation and
   every refresh store the job's current time plus
   `METADATA_COMPACTION_LEASE_EXPIRY_MS` in `expires_at_ms`. An `active`
   unexpired lease excludes every other job for the group.

   The lease is a fence, not a timestamp. An expired lease alone proves
   nothing — the job may be resuming from a long stall — so a pass claims one
   by compare-and-swapping its tagged `status` from `active` to `reaping`, and
   only the winner of that compare-and-swap may act:

   ```
   the job's refresh wins -> the pass retains the prefix
   the pass's claim wins  -> the job is fenced: its next refresh fails,
                             it publishes nothing, and the prefix is the
                             pass's to reclaim
   ```

   `reaping` fences that job permanently. The group slot can be replaced by
   a new job with a new identity; a replaced job never regains ownership.

   Before each root publication attempt, after refreshing the group lease,
   the job confirms an output-protection record at
   `metadata/compactions/{job_id}/protection.json`. It contains `namespace_id`,
   `job_id`, and `expires_at_ms`, and its deadline only advances by CAS. No
   output writes follow the first protection record. Confirming protection
   before the root CAS also covers a crash immediately after publication.

   After the final publication attempt, the job changes its group slot to
   `completed` by CAS, immediately admitting the next job. A failed completion
   write leaves the group held until expiry; a lost CAS cannot change a newer
   job's claim. Output protection is already independent of that slot.

   GC checks output protection before the group slot. A deadline at or after
   the pass's fixed clock retains the output. An expired deadline still
   requires the normal group-lease ownership check: a publisher may have
   refreshed its group lease and be about to extend output protection.
   GC removes protection only after its deadline and after the output prefix
   is empty. Thus a newer collector cannot erase the protection needed by
   one paused before publication. Removing the record may take one additional
   pass after the last output is removed.

   A pass reads the seven group lease keys once and decides one staged object
   as follows. An object the manifest references is live, like every other
   referenced object. Otherwise, if a lease naming that job is `active` and
   its `expires_at_ms` has not passed, the object is retained whatever its
   age; so is one whose expired lease the pass tried and failed to claim. A
   lease naming a different job protects nothing in this prefix. Otherwise
   the object ages as an ordinary unreferenced orphan under
   `METADATA_COMPACTION_STAGING_GRACE_MS`, which is derived rather than tuned:

   ```
   METADATA_COMPACTION_STAGING_GRACE_MS
       >= METADATA_COMPACTION_LEASE_EXPIRY_MS   (the lifetime written by a refresh)
          + T                                (rule 1: the publication that may still name it)
   ```

   `METADATA_COMPACTION_LEASE_EXPIRY_MS` is
   `METADATA_COMPACTION_LEASE_MISSED_REFRESHES` times the refresh interval,
   and it must itself be at least `T`: a job's last refresh before
   its output becomes referenced is at the top of a finalization attempt, and
   that attempt's compare-and-swap lands within one publication bound of it.
   That inequality is also what completes the fence — a job that refreshed
   at the top of an attempt cannot have its prefix claimed before that
   attempt's root compare-and-swap.

   Group slots are never deleted by GC. They stay available for replacement
   by CAS, which prevents a paused collector from deleting a newer job's
   lease. This retains at most one small slot per family group.

   The lease is a mutable control object and decodes strictly like every
   other. A lease that does not decode, or whose namespace, group, job identity, or terminal status disagrees
   with its location, fails the pass and is reported: nothing else reads the object,
   so believing a corrupt one would keep its named job's output alive forever
   and nothing would ever say why. Every other rule — the reference anchor,
   the complete run index, degraded roots — applies here exactly as it
   applies to `metadata/segments/`.

Deletion proceeds data first, records last, so a crash mid-sweep leaves
orphaned data for the next pass rather than a record whose data vanished.
To keep that true, every readable checkpoint record roots its basis for the
duration of a pass, whatever its lifecycle, expiry, or owner — no exceptions.
State, expiry, and owner fate gate only whether the record itself is a
candidate. A fork-owned record that no target uses (rule 10) is rechecked and
released with compare-and-swap on its current ETag. An active record is never
deleted directly, and a released fork record is retained if its target still
names it. A released record is deleted once its `released_at_ms` is a grace window old;
because release is terminal, no second state is needed between deciding to
delete and deleting, and a crash between the release CAS and the delete
leaves a record the next pass reaps unconditionally.
The collector persists the following state in the `gc_run` JSON control
family, version 1. The common fields are `namespace_id`, `gc_run_id`,
`step_no`, `started_at_ms`, `grace_window_ms`, and a tagged `phase`:

- `starting`: reservation exists; the control snapshot has not been captured.
- `marking`: the fixed root summary, source cursor, retained WAL pointer and
  floor, and a sorted mark index. Source scans visit the owned root,
  checkpoints, anchor-generation discovery, that generation's candidates,
  and retained WAL. A generation uses inclusive first/last keys rather than
  a growing list of candidates.
- `revisions`: the sealed object table, one entry/block position, and a
  separate content index. Each immutable revision segment is read once per
  run, one validated data block at a time. Shared descriptors use the
  tightest protected manifest sequence bound.
- `sealing`: merge the content index and object table into one complete table.
- `sweeping`: that table, the small retention summary, candidate family, and
  exclusive last-decided key. Data families precede checkpoint and upload
  records.
- `cleaning`: exclusive last-decided scratch key. Only recognized mark pages
  are removed, including abandoned pages from older runs.
- `complete`: the next caller without this run's token may replace the record
  by CAS and begin another run.

The `gc_mark_page` JSON family, version 1, stores immutable sorted pages at
`gc/runs/{gc_run_id}/tables/{table_id}/{page_no:020}.json`. Run IDs use `gcr_`
and table IDs use `gct_`, each followed by 32 lowercase hex digits. The payload
contains `namespace_id`, `gc_run_id`, `table_id`, `page_no`, and `entries`.
Pages contain 1–512 strictly increasing keys and at most 8 MiB of encoded
bytes. The shared envelope checksums the exact payload bytes. A table
reference contains its ID, page count, and entry count; every nonfinal page
is full. Readers validate envelope, identity, extent, page size/order, and
key/value correspondence. Missing or invalid pages fail collection, never
answer "not referenced."

Entry keys distinguish object references (`object/` followed by the full
object key), content IDs (`content/`), revision-segment tasks (`revision/`),
and missing manifest or checkpoint-basis observations (`missing-manifest/`
and `missing-basis/`). Tagged values preserve their meaning. Manifest marks
carry the complete verified manifest reference; revision tasks carry the
segment descriptor and sequence bound. Scan descriptors omit the optional
inline bloom filter because full revision scans do not consult it. Equal keys
must agree, except that
identical revision descriptors combine by taking the smaller sequence bound.

The index holds at most one table per binary merge level and one pending
two-table merge. Each merge saves two input positions and the output extent;
only a confirmed immutable page write followed by progress CAS advances those
positions. A lost or ambiguous write cannot turn partial marking into sweep
permission. Complete tables support binary-search lookup with a cache bounded
by 64 pages and 16 MiB of encoded page bytes. Memory also includes the source
manifest, WAL object, or metadata block being decoded; it does not grow with
the total number of namespace roots or content IDs.

This chooses extra temporary I/O for simple bounded merges: construction
writes O(N log N) entries and retains intermediate tables until cleanup.
An old worker can leave an unreferenced page after cleanup; a later run
reaps it. The singleton run record itself stays in place, preventing deletion
and recreation races. No marks, roots, or scan positions are trusted from
client continuation tokens.

### 6.5 Control-object cleanup

Upload sessions are moved to a terminal state and cleaned by the GC pass
("Garbage collection", rule 11). Implementations may additionally clean up
other expired control-plane objects. Mutable control objects MUST first enter
a terminal state by conditional write under the inspected etag, and any
provider-side state they own MUST be cleaned only after that write lands; a
failed conditional write retains them. This is control-plane maintenance, not
namespace history.

### 6.6 Derived work

Derived structures such as search indexes, caches, or materialized summaries
are optional. They may improve performance or higher-level features, but they
are not authoritative. They must be rebuildable from authoritative state, and
their presence and lifecycle are recorded in their extension-owned keyspace
(section 5).

## 7. Access-control boundaries

ACL and share design is reserved future work (`api.md` reserves the
authorization API group). Two boundaries are format rules today so that
authorization can arrive without a format break:

1. Authorization state is control-plane state. ACL or share changes never
   advance namespace `seq` and never appear in the change feed.
2. An access grant targets durable identity — a whole namespace or a subtree
   identified by `(namespace_id, inode_id)` — never path text. Paths are
   presentation; inode-rooted identity is durable.

## 8. Optional commit metadata, resource properties, and timestamps

A commit may carry optional human metadata such as:

- a commit message;

This metadata belongs to the logical commit, not to the resource itself.

A resource may carry optional structured properties such as display hints,
application tags, or a resource-type hint. These properties belong to the
resource, not to the commit. The core model spells them as **attributes**: a
map from an attribute key to an attribute value, held against inode identity.
Attributes move with the inode. A rename, a move, or a new file revision
leaves an inode's attributes unchanged.

An attribute key is 1 to 128 UTF-8 bytes and contains no Unicode control
character (general category `Cc`, which covers NUL). Keys are compared
exactly: nothing case-folds or normalizes them, so two spellings that differ
in any byte name two different attributes. Keys beginning with `loonfs.` are
reserved for system-owned attributes. The durable format carries a reserved
key like any other; a caller may not write one.

An attribute value is one UTF-8 string with no kind envelope. It is free text:
control characters and the empty string are legal. Empty is a stored value,
not a tombstone; only an explicit remove operation deletes an attribute. A
caller that needs a list chooses its own string encoding.

Four named format constants bound every map. Every size is counted in logical
UTF-8 bytes — the bytes of the text itself — so no encoder's framing changes
what a namespace may hold:

| Constant | Value | Bound |
| --- | --- | --- |
| `MAX_ATTRIBUTE_KEY_BYTES` | 128 | Longest attribute key. |
| `MAX_ATTRIBUTE_VALUE_BYTES` | 4,096 | Longest attribute value. |
| `MAX_ATTRIBUTE_ENTRIES` | 100 | Most entries in one map. |
| `MAX_ATTRIBUTES_TOTAL_BYTES` | 65,536 | Largest map, counting every key's bytes plus every value's bytes. |

Durable state that breaks any of these bounds fails to decode. Nothing is
truncated, dropped, or defaulted on a reader's behalf.

Every inode carries an attribute revision counter beside its map. An inode
begins at revision 0 with an empty map, and every effective update — one that
changes the map — advances the counter by one. The counter is the
optimistic-concurrency token for attribute writes: a writer states the
revision it expects, and the write fails when the inode has moved past it.
The counter is not an index into a history. A namespace keeps the current map
and this number, and offers no queryable record of earlier maps. An empty map
is a state and not an absence: clearing an inode's attributes advances the
counter to a revision whose map has no entries.

Every WAL commit record and commit receipt row stores a required
`committed_at_ms`: the request timestamp in Unix milliseconds. The metadata
rows described above copy this timestamp into fields such as `created_at_ms`,
`updated_at_ms`, and `deleted_at_ms`. This lets readers return the timestamp
without also loading the commit receipt.

These timestamps are informational. Sequence numbers determine ordering and
validity. Commit fingerprints do not include timestamps, and the format does
not require clocks to be synchronized.
