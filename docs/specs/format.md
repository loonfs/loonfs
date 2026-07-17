# LoonFS Format Specification

This document is the normative, mandatory specification of the LoonFS durable
format: the object-storage layout, the durable encodings, the commit protocol,
and the consistency and durability invariants. Any implementation that reads
and writes a store according to this document is format-conformant, whether or
not it exposes any API surface.

The companion document is `api.md` — the LoonFS API specification: profiles,
capability discovery, the standard error contract, and the HTTP binding;
normative where implemented.

Nothing in this document depends on how work is scheduled or which API surface
a deployment exposes.

Encoding conventions used by every durable and wire shape in this specification:
field names and enum values are `snake_case`; fields holding typed identifiers
are suffixed `_id`; tagged unions carry their discriminator in a `kind` field;
decoders tolerate unknown fields (readers ignore what they do not understand).

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
| **Prefix enumeration** | Manifest discovery, WAL segment discovery for repair and cleanup, and general namespace inspection need a reliable way to enumerate objects by prefix. Listings return keys in ascending lexicographic order; the conformance probes assert this. |
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
| **Namespace config** | Immutable | Stable namespace identity and immutable configuration, including the content-store binding; written last at creation as the completion marker. | `namespaces/{namespace_id}/namespace.json` |
| **WAL head** | Mutable | Hot head of the semantic commit stream: current visible boundary, writer epoch, writer liveness metadata, replay hints, and visible WAL tip. | `namespaces/{namespace_id}/wal/head.json` |
| **WAL segments** | Immutable | Record one or more logical commits with a contiguous sequence range. | `namespaces/{namespace_id}/wal/segments/{start_seq:020}-{suffix}.wal.zst` |
| **Namespace manifests** | Immutable | Record one namespace file-set version, including metadata table references, derived-index segment references, head summary, fork references, and the namespace features map. | `namespaces/{namespace_id}/metadata/manifests/{manifest_object_id}.manifest.json` |
| **Checkpoint records** | Mutable lifecycle | Durable stable-view pins to a metadata manifest, each carrying a required owner (user or fork target); written active, verified after the write, flipped released on verification failure or owner release. | `namespaces/{namespace_id}/checkpoints/{checkpoint_id}.json` |
| **Metadata tables** | Immutable | Store metadata rows referenced by manifests. Files may be owned by the namespace itself or by a fork source namespace. | `namespaces/{owner_namespace_id}/metadata/tables/{table_id}.sst.zst` |
| **Index segments** | Immutable | Store derived-index rows referenced by manifests ("Derived work"): the metadata-table block grammar with a feature-owned row payload. Files may be owned by the namespace itself or by a fork source namespace. | `namespaces/{owner_namespace_id}/metadata/indexes/{segment_id}.idx.zst` |
| **Upload sessions** | Mutable | Track one staged-content upload from begin to completion. | `namespaces/{namespace_id}/uploads/{upload_id}.json` |
| **Content-store descriptor** | Immutable | Record content-store identity. | `content-stores/{content_store_id}/descriptor.json` |
| **Metadata root** | Mutable | Cold pointer to the best known materialized metadata root; monotonic CAS. | `namespaces/{namespace_id}/metadata/root.json` |
| **WAL floor** | Mutable | Cold lower bound of retained WAL/change history; monotonic CAS. | `namespaces/{namespace_id}/wal/floor.json` |
| **Content objects** | Immutable | Store whole-file v0 bytes. | `content-stores/{content_store_id}/blobs/sha256/{hex[0..2]}/{hex[2..4]}/{hex}` |

The layout additionally reserves these paths for subsystems that land in
subsequent phases of the namespace layout redesign; the key parser recognizes
them, and no other family may claim them:

| Reserved path | Future role |
| --- | --- |
| `namespaces/{namespace_id}/wal/index.json` | Optional mutable pointer to the newest WAL index run (accelerator, never authority). |
| `namespaces/{namespace_id}/wal/indexes/{index_id}.json` | Optional immutable runs of visible-chain segment pointers. |

These key shapes are part of the interoperable storage contract.
Implementations may keep additional private control-plane objects — queues,
scheduler state, coordination records — outside the key families above;
private objects must not collide with the spec'd families and are not
interoperable state.

Namespace object keys are built through the central object layout API in
`loonfs-objectstore`. The namespace root remains `namespaces/{namespace_id}/`.
Forks are copy-on-write: the target manifest may reference source-owned
metadata tables through a source checkpoint-backed manifest, and the source
holds a fork-owned checkpoint record protecting that basis for the target's
lifetime.

WAL segment names sort by history position (section 1.3); recovery still
follows `head.visible_wal_tip` and the predecessor links inside verified WAL
envelopes. Listing order is an inspection and reclamation convenience, never
recovery authority. `wal/head.json` and `wal/floor.json` live outside the
`wal/segments/` listing prefix, so a reclamation listing of segments yields
only segment keys.

WAL, metadata table, and index segment deletion is reachability-driven from
the live manifest, checkpoint records, and the retention floor.

### 1.3 Durable naming conventions

The namespace tree's lifecycle can be read off its grammar:

- **`{subsystem}/{role}.json` objects are mutable singletons with one job**:
  compare-and-swap pointers and proofs (`wal/head.json`, `wal/floor.json`,
  and `metadata/root.json`) that are never swept. If a
  singleton cannot be explained in one sentence, it is too broad.
- **Collections are never authoritative via enumeration** (`wal/segments/`,
  `metadata/manifests/`, `metadata/tables/`, `uploads/`,
  `content-stores/.../blobs/`). A record in a collection matters only when a
  pointer, chain link, or checkpoint reaches it — except to GC, which
  lists collections to find garbage and roots. WAL segments and metadata
  manifests use ordered prefixes plus random suffixes so listings stay useful
  while concurrent writers avoid fighting for one immutable object name.
- **Paths express ownership, not authority.** Envelopes and payloads still
  validate namespace id, object id, family, checksum, and sequence fields;
  the same fact is never encoded twice (table family lives in the manifest
  and table envelope, not the path).
- **`namespace.json` is the existence marker**: written last at creation as
  the completion marker, kept forever after deletion as half of the
  tombstone pair that retires the namespace id.

Names are never authority anywhere — recovery follows the head and its
references.

The load-bearing invariants of this layout, in one place:

> **Live visibility is defined only by `wal/head.json`.** Everything else is
> a read accelerator, retention boundary, reachability root, or workflow
> record.

> **Fencing authority is writer epoch plus CAS.** Wall-clock time never
> gates commit validity, and fenced sessions never reacquire on their own.

> **Nothing correct depends on listing.** GC and floor advancement alone
> list — under a grace window, with delete-time re-verification, and with
> retention winning every ambiguous race.

> **Throughput is group commit; deadlines are local monotonic budgets a
> writer applies to itself.** No validator ever compares clocks, and
> accelerators (`recent_segments`, WAL indexes) prefetch but never decide.

### 1.4 Head update authority

The namespace head is updated by different classes of work, and each class has
its own authority boundary.

Semantic namespace mutations — file writes, restores, renames, deletes, and
explicit commits — are fenced by `writer_epoch` and then linearized by the head
compare-and-swap that makes their WAL visible.

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
4. The visible WAL chain must be deterministically recoverable from the head
   plus referenced segment metadata. A head field such as
   `wal_tip_segment_id`, together with segment metadata such as `segment_id`,
   `start_seq`, `end_seq`, `base_head_seq`, and `prev_visible_segment_id`, is
   one conforming shape. Equivalent semantics are acceptable.
5. `segment_id` must be unique and never reused within a namespace
   incarnation. It is a stream-positioned id (section 1.3): the ordered
   prefix is the segment's `start_seq` so listings and reclamation scans
   sort by history position, and the collision-resistant suffix keeps
   competing proposals for the same position distinct. The order in a
   listing is never recovery authority — recovery follows the head and the
   chain (rule 4) exclusively.
6. Orphan WAL segments are permitted and harmless when a writer loses the head
   compare-and-swap.

### 1.6 Immutable content rules

The content model has five rules.

1. Content digests are content-derived, not provider-derived.
2. A `content_ref` describes one complete file revision.
3. Immutable content objects are written with create-if-absent semantics.
4. A metadata commit may reference a `content_ref` only after the referenced
   object is already durable.
5. When provider-verified full-object SHA-256 metadata is available, the
   object-store layer may expose it as `sha256:<64hex>`. If SHA-256 metadata
   is absent, readers and writers must fall back to reading and hashing bytes
   before treating a `content_ref` as verified.

In v0, file content is stored as one whole-file object whose
`content_ref.kind` is `whole_file_v0`. The digest remains serialized as
`sha256:<64hex>`, while the object key partitions the hex as
`sha256/ab/cd/<hex>`.

Metadata materialization tables include canonical metadata families and
validated secondary indexes. The canonical families are `inodes`,
`direntry-binds`, `direntry-unbinds`, `revisions`, `tombstones`, and
`commit-receipts`. The `direntry-child-binds` family is a secondary index over
the same direntry bind rows, keyed by child inode, and must be present and
verified before a namespace manifest is trusted.

ETags remain opaque compare tokens. They may be used for object freshness or
compare-and-swap, but they are not content digests unless a provider-specific
behavior is separately exposed and verified through this contract.

A reader or writer resolves content through the namespace descriptor:
`namespace_id -> content_store_id -> content-stores/{content_store_id}/...`.
File revisions and change-feed payloads store only `content_ref`; they do not
store content-store ids or object-store paths.

### 1.7 Mutable control-object rules

Small mutable objects such as the namespace head must use compare-and-swap
semantics. These objects must remain small enough that guarded rewrite is
practical.

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

Writers additionally apply a self-enforced publish budget: a local monotonic
elapsed-time bound between starting a WAL segment PUT and initiating the head
CAS (60 seconds). Overrunning it abandons the segment as an orphan and
rebuilds the commit as a fresh segment. The budget gates only the writer's own
next action; validators never consult time.

Large immutable file data may use multipart upload or another
provider-specific optimization. Small mutable control objects should not
depend on those mechanisms.

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

- a descriptor that records its namespace id and content-store id
- a current head
- an ordered WAL of logical commits stored in immutable segments
- immutable namespace manifests that describe recoverable file-set versions
- zero or more checkpoints
- a retention policy

The head also carries the next monotonic inode id for that namespace. New
inode ids are allocated from the head as part of commit publication.

The canonical identity of an item is `(namespace_id, inode_id)`.

Each namespace has exactly one immutable `content_store_id`. The content store
is an immutable pool for file bytes and may be referenced by many namespaces.
A new root namespace receives a new content store; a forked namespace reuses
the source namespace's content store while starting an independent namespace
metadata history.

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
  "inode_id": 42,
  "inode_kind": "file",
  "created_seq": 17
}
```

The bind row that places that inode in the tree:

```json
{
  "parent_inode_id": 9,
  "name_key": "report.txt",
  "display_name": "Report.txt",
  "child_inode_id": 42,
  "bind_seq": 17
}
```

The unbind row that removes one exact prior binding:

```json
{
  "parent_inode_id": 9,
  "name_key": "report.txt",
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
  "inode_id": 42,
  "revision_no": 7,
  "committed_seq": 91,
  "content_ref": {
    "kind": "whole_file_v0",
    "digest": "sha256:42d...",
    "size_bytes": 19482
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

In v0, the root inode is created as `inode_id = 1` at `seq = 0`.

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
`..`, and must not exceed 255 UTF-8 bytes as stored. Name keys obey the same
character rules with a 768-byte cap: case folding expands at most threefold
in bytes, so every key derivable from a valid display name is admissible.
Requests carrying a name or key outside this grammar fail validation; nothing
is truncated or normalized on the caller's behalf.

#### 2.3.1 NamePolicy

Sibling-name comparison is governed by a versioned `NamePolicy`. A namespace
has exactly one active name policy.

The v0 policy is `nfc_casefold_v0`, which defines sibling-name comparison by
Unicode NFC normalization plus case folding. Future policies may exist, but
all writers for a namespace must agree on the namespace's active policy.

### 2.4 Files and revisions

A file is represented by one inode and a sequence of immutable revisions.

Each revision stores exactly one immutable `content_ref`. In v0, that
reference names one whole-file object containing the complete plaintext file
bytes. Revisions do not store object-store paths or `content_store_id`;
readers resolve those through the namespace descriptor when bytes are needed.

Content objects belong to the namespace's content store. A file revision may
reference only content that is durable under the content store named by that
namespace descriptor.

LoonFS therefore uses a two-stage write model:

```text
make content durable  ->  then make metadata visible
```

This separation is part of the core model.

#### 2.4.1 Immutable content storage

The stable immutable content families are:

```text
content-stores/{content_store_id}/blobs/sha256/{hex[0..2]}/{hex[2..4]}/{hex}
```

The core rules are:

- `content_ref.kind` is `whole_file_v0` for the v0 content strategy;
- `content_ref.digest` uses `sha256:<64 lowercase hex>` over the complete
  plaintext file bytes;
- `content_ref.size_bytes` records the complete byte length;
- the object key leaf is the raw 64-character hex digest, while JSON keeps the
  full `sha256:<hex>` digest string;
- all content-object access resolves `namespace_id` through the namespace
  descriptor to its `content_store_id`;
- future content strategies must use a new `content_ref.kind` and name their
  durability and validation rules before revisions may reference them.

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
inode and re-binds that inode under a visible parent. Tombstone rows carry
a typed action — `set` (the subtree is deleted) or `revoke` naming the
exact `(target_seq, target_delta_index)` deletion it cancels — and rows
for one root are ordered by `(tombstone_seq, tombstone_delta_index)` with
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

A namespace's lifecycle has two recorded states, carried in the head's
`state` field: `active` (the default; an absent field reads as active) and
the terminal `deleted`. Initialization progress is deliberately *not*
recorded here — a namespace is complete when its descriptor exists, partial
when only earlier objects exist — because object presence cannot go stale.
Deletion is the one transition presence cannot express: a deleted namespace
keeps its head and descriptor forever as the tombstone that retires its
`namespace_id`. Readers MUST refuse to serve a namespace whose head state
they do not recognize.

Deleting a namespace is a fenced control-plane transition, not a logical
commit: the deleting writer acquires the namespace writer epoch and
compare-and-swaps the head into `state: deleted`. The delete linearizes at that swap. Every
commit whose head advance serialized before it remains committed and
durable — deletion never retroactively falsifies an acknowledgment; it ends
the namespace's history at that `seq`. Every operation that observes the
deleted head afterward — reads, commits, forks from the namespace, status,
and re-creation of the same id — fails with `namespace_deleted`.

Namespace deletion does not imply content-store deletion. In v0, content-store
deletion and destructive content garbage collection are unsupported
operator-only work, and deletion does not physically reclaim metadata
objects; reclamation is future maintenance work bound by the invariants in
section 6 (notably: objects protected by fork-owned checkpoint records
survive, so clones of a deleted source stay readable).

### 2.6 Forks

Forking a namespace creates a new namespace with independent metadata history
and the same `content_store_id` as the source namespace. The fork point is the
source namespace's current head. The implementation creates or reuses a
verified fork-owned source checkpoint at that head, freshens that record by
compare-and-swap, then initializes the target namespace with a manifest that
references immutable metadata files owned by the source namespace. The target
descriptor is written last as the publish/list marker.

Fork provenance is stored in the target namespace manifest. Normal reads and
recovery use the target descriptor, head, manifest, and WAL only. After fork, the clone must remain readable even
if the source namespace metadata is deleted or corrupted. Source writes after
the fork do not affect the clone, and clone writes do not affect the source.

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
namespace. Same-content-store copies may reuse `content_ref`. Cross-content-
store copies are not supported in v0 unless the content is first imported into
the destination content store. Inode identity does not cross the namespace
boundary.

### 2.9 Recovery view

Readers reconstruct authoritative state from:

1. the current head and the metadata root, fetched concurrently;
2. the namespace manifest named by `metadata/root.json`; and
3. the visible WAL segment chain after that manifest through `head.seq`,
   replayed as logical commits in ascending `seq` order.

The head summarizes the current visible boundary and replay hints, including
at minimum:

- `seq`
- `head_commit_id`
- `state` (lifecycle: absent or `active`, or the terminal `deleted`)
- `next_inode_id`
- `visible_wal_tip` and the bounded `recent_segments` accelerator

`wal/floor.json` is the symmetrical pair to the head — the earliest retained
commit boundary next to the latest visible one. It records `floor_seq` and
verification and update stamps. It is updated only by monotonic compare-and-swap on its
own etag by floor advancement, which is a GC-family operation: it never
touches the WAL head, so the head changes only when commits land. A missing,
stale, or unverifiable floor means "retain more history", never less, and the
floor never affects live commit visibility.

`metadata/root.json` is the live read/recovery pointer. It is updated only by
monotonic compare-and-swap on its own etag: a replacement must not decrease
`manifest_head_seq`, a same-seq replacement may reference a different manifest
(that is how pure compaction publishes a better physical layout of the same
logical state), and a lower-seq attempt no-ops in favor of the newer root.
The root never defines live visibility, and a stale root only costs extra WAL
replay. A reader that observes `root.manifest_head_seq > head.seq` reloads
the head — the root can only reference published state, so a fresh head read
observes at least the root's seq; this race is not corruption.

A checkpoint is a durable pin to a namespace manifest version, stored as a
first-class record under `checkpoints/` — never inside a manifest, and never
an input to latest visibility. A record carries its basis facts (manifest id,
seq, payload checksum, head commit id), a required tagged `owner` — a `user`
owner with a name label, or a `fork` owner naming the target namespace the
pin protects — an optional expiry (user-owned records only; fork pins never
expire), and a lifecycle `state` of `active` or `released`. Only active,
non-expired records are long-term GC roots; released records are collectable
tombstones, whether verification failed or the owner let go. A user-owned
record persists until released or expired; a fork-owned record persists while
its fork target may still read the basis. Creation is write-then-verify:
write the record active, then verify — under the self-enforced verify
budget — that the floor has not passed the basis and the basis manifest still
loads; on failure flip the record to released and retry against a newer
basis. Combined with the GC grace window and delete-time re-verification,
this closes the create-vs-collect race: within the grace window any record is
protected unconditionally by age.

Record ids derive deterministically from the basis identity plus the owner
identity: repeating creation for the same pinned manifest and owner returns
the existing record (reviving a released one after re-verification) without
listing, while distinct owners of one basis hold distinct records with
independent lifecycles. Explicit release flips a user-owned record
`active -> released` by compare-and-swap and is idempotent; the record itself
is reaped by a later garbage-collection pass, and its basis becomes
collectable only on the pass after that (records-last, "Garbage
collection").

A namespace manifest is the durable object for one namespace file-set version.
It may reference one or more immutable metadata runs; standalone checkpoint
records under `checkpoints/` pin manifest versions for retention, fork, or
stable read workflows. Each run is internally segmented without overlapping segment
key ranges; different runs may overlap and readers apply the normal metadata
visibility rules across all referenced runs. Within a segment, rows are stored
in ascending row-key order (adjacent equal keys permitted); readers reject a
segment whose rows are out of order as malformed. Readers load the referenced
runs, then replay only the visible WAL chain after the manifest's `head_seq`.

The WAL preserves ordered history even when multiple logical commits are
stored in one segment. Each logical commit records the commit identity, optional
message, and normalized metadata deltas such as inode creation, direntry
bind/unbind, file revision append, and subtree tombstone rows. Validation
inputs and operation results are not persisted in the WAL. Checkpoints keep
replay bounded as history grows.

## 3. Write and read protocol

### 3.1 Write protocol

A write has four phases: durably stage content (if a mutation contains
content), reconstruct and validate, publish one or more logical commits into
the WAL, and advance the head. A commit request may be rejected immediately,
or tentatively accepted and written to a WAL segment, but it is committed and
successful only if the WAL segment is durably stored and the head advances to
reference it. A metadata change becomes visible only after the head advances.

#### 3.1.1 Content staging

Content must be durable before any metadata change can reference it.

1. Compute the `sha256` digest of the complete plaintext file bytes.
2. Resolve the namespace descriptor to its `content_store_id`.
3. Upload the complete byte sequence to
   `content-stores/{content_store_id}/blobs/sha256/{hex[0..2]}/{hex[2..4]}/{hex}`
   with create-if-absent semantics.
4. Build a content reference:

   ```json
   {
     "kind": "whole_file_v0",
     "digest": "sha256:<64hex>",
     "size_bytes": 123
   }
   ```

Content staging is idempotent and has no effect on the visible tree. If the
caller crashes mid-upload, orphaned content objects are harmless.

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
   storage, and that `content_ref.kind`, digest, and size match. When
   provider-verified SHA-256 metadata is present, this validation may use
   metadata instead of downloading the whole object; otherwise it must read
   and hash the object bytes.
3. Evaluate preconditions in order (see section 3.6 for the precondition
   catalogue).
4. Resolve inode references and allocate new inode ids monotonically from the
   head's `next_inode_id`.

If a request contains multiple operations, they are evaluated sequentially
against ephemeral state advanced by earlier operations in the same request.

Passing validation does not by itself make the request committed or
successful. If a client mutation request reaches the success boundary in
section 3.1.4, it becomes one logical commit.

Content reference validation fails before metadata preconditions are evaluated
when:

- `content_ref.kind` is unsupported;
- `content_ref.digest` is not a valid `sha256:<64 lowercase hex>` digest;
- the referenced object is missing from the namespace's content store;
- the object size differs from `content_ref.size_bytes`; or
- provider-verified checksum metadata, when present, differs from
  `content_ref.digest`; or
- when checksum metadata is absent, the object bytes hash to a different
  digest than `content_ref.digest`.

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
   accelerator over the visible chain, tip included; chain links remain the
   only history authority).
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

1. Read the namespace descriptor and content-store descriptor to learn the
   namespace's immutable content-store relationship.
2. Read the namespace **head** object (current `seq` and visible WAL tip)
   and `metadata/root.json` (the manifest pointer), concurrently.
3. Load and verify the manifest the root references; its payload checksum
   must match the root's `manifest_payload_checksum`. The manifest references
   one or more materialized metadata runs through its `head_seq`.
4. Use the visible WAL tip named by the head to identify the visible segment
   chain after the manifest `head_seq`, then replay the logical commit records in ascending
   `seq` order through `head.seq`. Each logical commit appends normalized rows
   to the same metadata tables.

The result is a metadata view pinned to one `seq`.

For latest path `stat` and directory `list`, an implementation may avoid
hydrating a complete metadata state. The reader may query verified metadata run tables and the visible WAL tail
overlay directly, provided it applies the same visibility rules and treats
missing or corrupt manifest/WAL objects as hard errors. If no current manifest
is published for a complete namespace, the namespace is corrupt.

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
   normalized `name_key` matches the component under the namespace's
   `NamePolicy`.
3. Follow the binding to its `child_inode_id`; v0 path resolution does not
   cross mounts, and mount traversal is reserved future work.
4. If any component has no matching visible binding, the path does not exist.

#### 3.2.4 File content retrieval

Given a visible file inode at seq N:

1. Look up the file's latest revision at N to obtain `content_ref`.
2. Resolve the namespace descriptor to its `content_store_id`.
3. Verify that `content_ref.kind` is supported by the reader.
4. For `whole_file_v0`, fetch the object at
   `content-stores/{content_store_id}/blobs/sha256/{hex[0..2]}/{hex[2..4]}/{hex}`,
   where `hex` is the digest suffix from `content_ref.digest`.
5. Verify that the fetched bytes match `content_ref.size_bytes` and
   `content_ref.digest`.

A **file revision** is an immutable content state for one file inode,
identified by that inode's monotonic `revision_no`. A namespace commit `seq`
is the global visibility order for committed mutations; it is not a file
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

A successful client mutation request is one logical commit.

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

A fingerprint value is `v0:sha256:<64 lowercase hex>`. The `v0` tag names the
canonicalization rules below and `sha256` the digest algorithm, so either can
change later without re-interpreting stored values.

The `v0` preimage is the compact JSON encoding (no whitespace, object keys in
exactly the order shown) of:

```json
{
  "domain": "loonfs.core.commit.semantic.v0",
  "namespace_id": "...",
  "preconditions": [...],
  "ops": [...],
  "message": "... or null"
}
```

where `ops` and `preconditions` appear in request order using their v0 wire
encoding, and `message` is `null` when absent. The preimage deliberately
excludes `commit_id`, writer identity, and writer epoch: a retry of the same
logical commit must fingerprint identically no matter who retries it or when.

Path-level mutations fingerprint the same way with domain
`loonfs.path.intent.semantic.v0` over the normalized path intent (intent kind,
normalized absolute paths, and the intent's semantic parameters).

The idempotency horizon is the retention floor. Commit receipts below the
floor are dropped when metadata runs are rebuilt, so a commit retried from
below the floor may be treated as new — the same re-bootstrap contract the change
feed gives sub-floor cursors.

A reused `commit_id` with an equal fingerprint replays the originally
committed response; an unequal fingerprint is rejected as
`commit_id_reuse_conflict`. Reference values are pinned by tests in
`loonfs-core` (`commit/identity.rs` and `path/write/planner.rs`); those literals
must never change within scheme `v0`.

### 3.4 Server authority

The server is authoritative for mutation validation.

In particular, the server is responsible for:

- resolving any supplied paths against the current visible tree;
- allocating new inode ids;
- validating name collisions according to the namespace's `NamePolicy`;
- validating preconditions;
- verifying that referenced content is already durable; and
- publishing successful logical commits by durably writing a WAL segment and
  advancing the head.

Clients may assist with planning, hashing, upload, or retry, but they are not
the authority for visible state.

The server need not be centralized. The protocol is designed for multiple
writers.

### 3.5 Standard mutation operations

The first standard lower-level mutation set includes:

- `create_directory(parent_inode_id, display_name)`
- `create_file(parent_inode_id, display_name, content_ref)`
- `replace_file(inode_id, base_revision_no, content_ref)`
- `rename(inode_id, new_parent_inode_id, new_display_name)`
- `delete_file(inode_id)`
- `delete_subtree(root_inode_id)`
- `restore_revision(inode_id, source_revision_no, base_revision_no)`
- `undelete(inode_id, deleted_at_seq, parent_inode_id, display_name)`

The path-oriented filesystem surface may compile higher-level operations into
these lower-level mutations.

`rename` is always no-replace: a destination binding that already exists
fails validation. Replacing moves exist on the path-oriented surface
(`move_path` with its behavior enum); if commit-level replace-rename is ever
needed it arrives as a new capability-gated field, not a silently tolerated
one.

These are semantic commit operations. Durable WAL payloads store normalized
metadata deltas derived from the semantic operations: `create_inode`,
`bind_direntry`, `unbind_direntry`, `append_file_revision`,
`tombstone_subtree`, and `revoke_subtree_tombstone`. Raw bind/unbind/
create-inode deltas are not standard client-facing commit operations.

### 3.6 Preconditions

A mutation may include explicit preconditions. Preconditions are how clients
say, "apply this only if the namespace still looks like the state I planned
against."

The core kinds of precondition are:

| Kind of check | Example use |
| --- | --- |
| **Name-slot based** | "Create this child only if that name slot is still empty." |
| **Name-binding based** | "Move or delete this item only if this name still points at the inode I saw." |
| **Revision-based** | "Replace this file only if it is still at the revision I saw." |
| **Ancestor-visibility based** | "Apply this only if no ancestor was tombstoned." |
| **Directory-contents based** | "Delete this directory non-recursively only if it is still empty." |

The exact wire shape of preconditions may vary by transport binding, but the
semantics must match these checks.

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

Each change event exposes the commit identity, optional message, and
materialized WAL deltas keyed by `semantic_op_index` and `delta_index`. These
deltas are the authoritative metadata facts that replay/projectors should
apply.

### 3.8 Retention floor

A namespace may advance a retention floor to say:

> Incremental replay older than this point is no longer promised.

Clients older than the retention floor must re-bootstrap from a fresh
checkpoint instead of replaying from an obsolete cursor.

The retention floor may advance only after the system has enough verified
material to keep replay safe at or after that point: advancement derives its
target from the manifest `metadata/root.json` references and verifies that
every metadata segment that basis references still exists before the floor
moves. The probe is advisory — the atomic guarantee is the garbage
collector's obligation to never remove reachable objects ("Garbage
collection") — but a segment that already disappeared must block the floor
while replay can still rebuild the lost state. Corruption discovered after
advancement is caught by read-path checksum validation.

Advancement then CASes only `wal/floor.json`, recording the new `floor_seq`
together with its verification stamp. Floor updates
are monotonic: a replacement never decreases `floor_seq`, and
`floor_seq <= metadata/root.manifest_head_seq` holds. The floor is necessary
but not sufficient for deletion — being below it makes an object a deletion
candidate; actual deletion additionally requires delete-time re-verification,
and if the floor ever observably passes an active checkpoint's basis,
retention wins ("Garbage collection").

A WAL flush materializes the current durable namespace
file-set version: if there is no manifest for the current head, the
implementation writes one absorbing the visible WAL tail and publishes it by
monotonic CAS on `metadata/root.json` — never by touching the WAL head, so
head watchers observe only commits. The flush is the latest-state
maintenance operation and creates no checkpoint record; a superseded manifest
becomes a garbage-collection candidate once nothing pins it.

Every metadata publication — WAL flush and reorganization alike —
self-enforces the metadata publication budget, measured from before its
first table object is written until its root compare-and-swap is initiated.
A publication that exceeds the budget aborts without publishing: its
immutable outputs stay unreachable and are reclaimed by garbage collection
after the grace window. This bound (with the WAL publish budget for
commits) is what makes the GC grace window's floor derivable ("Garbage
collection", rule 1); maintenance therefore needs no durable build-intent
protocol.

Creating a checkpoint pins one such manifest version deliberately for one
owner. It first flushes the WAL tail as above, then writes
`checkpoints/{id}.json` (id derived deterministically from the basis identity
plus the owner identity, so repeating creation for the same pinned manifest
and owner returns the existing record without listing) and verifies the basis
after the write, flipping the record to released on failure. A live manifest
does not need to be checkpoint-pinned; checkpoint records explain why a
manifest version must be retained after the root moves on.

### 3.9 Namespace forks

A fork creates a new namespace from the source namespace's current head. The
request supplies only the new namespace id; the server supplies the mutation
context.

The fork protocol is:

1. Check the target namespace initialization state. A complete target is
   rejected as existing, and a partial target is rejected as partially
   initialized.
2. Resolve and verify the source namespace descriptor, content-store
   descriptor, head, checkpoint, and WAL visibility chain.
3. Create or reuse a verified fork-owned source checkpoint at the current
   source head (owner: the target namespace id).
4. Build the target head, target manifest, and descriptor using the source
   namespace's `content_store_id`.
5. Freshen the fork-owned record by compare-and-swap before any target
   write: the rewrite re-stamps the record's provider timestamp so the
   abandoned-fork rule cannot fire under a live retry, and it serializes the
   fork against a concurrent GC release — whichever compare-and-swap lands
   second fails, so a released record is observed (and revived, re-verifying
   the basis) rather than raced.
6. Write a target namespace manifest that references the source-owned
   immutable metadata files for the source checkpoint.
7. Write the target `head.json` to reserve the namespace and point at the
   target manifest.
8. Write the target `namespace.json` last as the publish/list marker.
9. Start the new namespace WAL independently at `fork_seq + 1`.

The fork does not copy content-store blobs or source metadata SSTs. If
initialization fails after the target head exists but before the descriptor is
published, the target is partial. A successful fork has independent namespace
history from the fork point. Future target WAL, checkpoints, and metadata SSTs
are written under the target namespace root. The target manifest records where
the namespace came from.

### 3.10 Long-running operations

Some operations are not well described by one request.

Examples include:

- recursive reads that need a pinned snapshot; and
- resumable uploads that need a stable destination binding.

In those cases, the server may create control-plane objects such as read
sessions, upload sessions, or put intents.

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
| **Namespace naming rules** | `NamePolicy` and any future policy revisions. |

A new version should be introduced only when an old implementation could
misread or misapply a new feature.

For the protocol binding, the API spec's "Standard error contract" section is
the registry of stable error codes and HTTP statuses, and of the rule that
clients must ignore unknown JSON response fields and tolerate unknown error
codes.

The durable namespace descriptor and content-store descriptor are
storage-format objects. The namespace descriptor is authoritative for the
namespace-to-content-store relationship.

### 4.1 Durable envelope layout

Every durable LoonFS object except metadata segments (section 4.2.1) is an
envelope document with the same leading fields, followed by the payload as
an opaque sub-document:

| Field | Meaning |
| --- | --- |
| `kind` | snake_case object kind string. |
| `format_version` | Per-family format version (see table below). |
| `writer_version` | Informational `crate/<version>` of the writer. Never used for decode decisions. |
| `payload_checksum` | `sha256:<64 lowercase hex>` digest of the exact payload bytes as stored. |
| `payload` | The payload: a raw JSON sub-document in JSON families, a CBOR byte string in CBOR families. |

Two rules make these envelopes evolvable:

1. **Checksums cover stored bytes, never a re-encoding.** Readers verify
   `payload_checksum` against the payload bytes exactly as stored, before
   decoding them. A checksum failure therefore always means corruption;
   version skew can never be misreported as corruption.
2. **Readers probe before they decode.** Readers first decode only `kind` and
   `format_version`, so an object written with an unknown kind or an
   unsupported format version fails with a precise, typed error rather than a
   generic decode error.

### 4.2 Format families and versions

| Family | `kind` | Encoding | Current version |
| --- | --- | --- | --- |
| WAL segment | `namespace_wal_segment` | CBOR envelope, zstd-compressed; CBOR payload | 1 |
| Metadata segment | none (section 4.2.1) | block sections, per-block zstd + CRC32C | 1 (via manifest) |
| Gram index segment | none (section 4.2.2) | block sections, per-block zstd + CRC32C | 1 (via the `index.grams` feature value) |
| Namespace manifest | `namespace_manifest` | JSON, uncompressed | 1 |
| Control objects (head, descriptors, upload session) | per-kind snake_case names | JSON, uncompressed | 1 (tracked per kind) |

JSON families keep their payload inline as raw JSON so manifests and control
objects stay directly readable with generic tooling; CBOR families carry the
payload as a byte string. Control-object versions are tracked per kind so one
kind's payload schema can change without invalidating the others.

#### 4.2.1 Metadata segments

A metadata segment object is not an envelope: it is a sequence of
independently readable sections — prefix-compressed data blocks holding rows
in ascending row-key order, one bloom filter block over per-family lookup
prefixes, then one index block naming each data block's last key and byte
range. There is no footer and no self-describing header; the referencing
manifest's segment descriptor carries the index and
filter block handles, and is the only entry point into the object. Each
section's CRC32C is computed over its stored (compressed) bytes and lives in
the handle that names it — index entries for data blocks, the manifest
descriptor for the index and filter — so a reader verifies every ranged read
before decoding it, and the manifest transitively binds the object's exact
bytes. The descriptor also records a whole-object `sha256` digest for
publication conflict checks and offline verification; the read path never
consults it. When the filter block is small (delta-run segments), the
descriptor additionally inlines the filter's stored bytes as lowercase hex
(`filter_inline`), so a point lookup can rule the segment out without any
object fetch. The inline copy is bound by the same filter handle — it must
decode against the handle's stored length and CRC32C exactly like a fetched
block, and a mismatch is corruption. When the field is absent (large
filters are not inlined), readers fetch the filter block by its handle. The
filter block sits directly before the index block at the end of the object;
manifest loading rejects a descriptor whose handles disagree with that
layout, or whose inline copy's length disagrees with its handle, so the
read path assumes both. Readers reject out-of-order rows,
out-of-order index entries, and checksum failures as malformed. The segment
format is versioned by the manifest that references it (`namespace_manifest`
`format_version`), since a segment is unreachable except through a manifest.

#### 4.2.2 Gram index segments

A gram index segment stores the `index.grams` derived index (section 5). It
uses the section 4.2.1 block grammar unchanged — prefix-compressed data
blocks, one bloom filter block, one index block, handles and checksums in
the referencing descriptor — with a feature-owned row payload instead of
metadata rows. The tokenizer, the row shapes, and the posting encoding
below are frozen for feature version 1; changing any of them is a new
feature version and a rebuild, which is always legal for derived work
(section 6.6).

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

Manifests reference index segments through the `index_files` list, one
descriptor per segment mirroring the metadata descriptor fields plus an
open-vocabulary `family` string (`grams` for this section). The list is an
additive payload field with the section 5 contract: readers use the entries
whose family they understand, preserve the rest verbatim when rewriting a
manifest, and never let an unknown family affect how core state is read.
Garbage collection protects every listed object key regardless of family.
Maintenance that rewrites a manifest without folding the index — checkpoint
flushes, metadata reorganization, forks — must carry `index_files` and the
paired feature entry forward verbatim.

### 4.3 Evolution rules

- **Additive within a version.** A writer may add new payload fields without
  bumping `format_version`. Readers must ignore unknown payload and envelope
  fields. This is the only same-version change allowed.
- **Everything else bumps the version.** Renaming, removing, retyping, or
  re-tagging any field — or changing the payload encoding — requires bumping
  the owning family's `format_version`. Readers reject versions they do not
  support with a typed unsupported-version error; there is no silent fallback.
- **Digest strings are self-describing.** Durable digest values carry their
  algorithm as a prefix (`sha256:<hex>`) so a future algorithm can be
  introduced without re-interpreting old values. Commit fingerprints
  additionally carry their canonicalization scheme (`v0:sha256:<hex>`, section
  3.3.1) because their preimage rules can evolve independently of the
  algorithm.
- **Unknown content-ref kinds round-trip.** A reader that does not understand
  a `content_ref.kind` must preserve the original string when relaying or
  rewriting rows; it must not create new references with kinds it does not
  understand (section 3.1.3).
- **Every encoding is pinned by golden-byte fixtures**
  (`crates/loonfs-api/tests/golden_formats.rs`). An encoder change that alters
  durable bytes fails those tests; the failure message demands either
  reverting the change or bumping the format version and regenerating the
  fixtures.

## 5. Namespace features map

A namespace manifest may carry a `features` map recording per-namespace
capabilities that are materialized *on this data* — for example, which derived
indexes exist for the manifest's file-set version.

```json
{
  "features": {
    "index.grams": { "version": 1, "built_through_seq": 41290 }
  }
}
```

Rules:

- Feature keys describe data, not endpoints. They are **not** prefixed by API
  profile names (contrast the deployment capability document in `api.md`,
  whose keys are profile-prefixed).
- Each value is an open JSON object owned by the feature's own specification;
  `version` is the conventional first field.
- Readers must ignore feature keys they do not understand. The map is
  additive metadata: it never changes how the core filesystem model is read.
- An absent map and an empty map are equivalent.
- Successful use of a data-dependent capability requires both halves: the
  deployment must advertise the serving capability (`api.md` capability
  document) **and** the namespace's `features` map must show the capability
  materialized for the data being served.

One feature key is registered:

- **`index.grams`** — the gram index for content search (section 4.2.2).
  The value carries `version` (this spec defines version 1),
  `built_through_seq` (commits at or below this sequence are reflected in
  the manifest's `index_files` segments; later revisions are the query
  path's exhaustive-scan tail), and, while initial materialization is still
  walking existing revisions, a `backfill_cursor` resume key. While
  `backfill_cursor` is present the index is not yet materialized and
  data-dependent queries must be refused. Readers hard-reject an
  unsupported `version` with a typed error and tolerate unknown fields
  inside the value. While the key is present, the retention floor must not
  advance past `built_through_seq` — the WAL from the watermark forward is
  the index build's change feed.

The map exists so that derived indexes and similar per-namespace
capabilities can arrive without a format version bump.

## 6. Maintenance operations

Maintenance keeps read cost bounded, retention safe, and durable state clean.
Maintenance **effects** are normative format semantics; maintenance
**scheduling and triggering** are not. Two behaviors keep an un-administered
deployment's read costs bounded regardless of scheduling: the reference
implementation schedules a background maintenance tick after any runtime
publish that observes the WAL tail at or past the WAL-flush threshold
(32 segments at defaults), and every publish surface rejects with
`maintenance_required` once the tail exceeds four times that threshold
(128 at defaults). Reads never gate on tail length. Bounded reads are the
automatic half only: the retention floor never advances on its own, so
history retention — and the row reclamation that follows it — remains an
explicit operator decision. An embedded engine where an operator
triggers maintenance manually and a server that runs the same work invisibly
are equally conformant (see `api.md` for the optional maintenance plane). The
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

For file revisions, a valid manifest includes the canonical `revisions` table
and the `revisions_by_inode_desc` index table. The index table must contain
exactly the same revision rows as the canonical table, keyed for newest-first
inode revision scans. Readers treat a missing, extra, duplicate, or changed
revision index row as namespace corruption. Segment reads enforce per-segment
checksums and key ranges; manifest-table loads enforce per-run row-count
equality between canonical and index families; full row-level index equality
is enforced at every base rebuild, the production point that materializes all
rows.

### 6.2 Compaction

Compaction rewrites metadata runs (and, in the future, content layouts) into
more efficient physical shapes.

A base rebuild drops rows that no retained sequence can observe: revisions
superseded at or below the retention floor, bindings superseded or unbound at
or below the floor, spent unbind markers, and commit receipts below the
floor. The floor is the single retention policy: history below it — including
file revision history — is reclaimed, except where an active checkpoint record
still pins an older manifest that can serve it. Tombstone rows — set and
revoke events alike — and inode rows are always retained for now;
reachability-based dropping for them is future work.

Invariants:

- Compaction MUST NOT change logical content: the visible metadata state at
  every retained `seq` is identical before and after.
- Compaction MUST publish its results through the normal manifest publication
  path; readers never observe a partially compacted state.
- Compacted inputs MUST remain available until no retained manifest version
  or checkpoint record references them.

Checkpoint records are standalone files under `checkpoints/`. Maintenance
never creates one: automatic root advancement leaves superseded manifests and
folded-away tables unpinned, and garbage collection reaps them under the
grace-window and delete-time re-verification rules ("Garbage collection").
A checkpoint record is a deliberate pin — fork sources and explicit admin
checkpoints — and roots its basis for as long as the record exists.

### 6.3 Retention management

Retention management decides how far back incremental replay is still
promised.

A retention floor may advance only when the system has enough verified
material to support readers from the new floor forward.

### 6.4 Garbage collection

Delete is tombstone-first. Garbage collection is the separate process that
eventually reclaims content or metadata that is no longer reachable and no
longer protected by retention policy. GC and floor advancement are the only
consumers of listing, and nothing sweeps by default: a pass runs only through
the admin endpoint or an explicit maintenance-tick opt-in.

v1 GC is listing mark-and-sweep. Its inputs are `wal/head.json`,
`wal/floor.json`, `metadata/root.json`, and the `metadata/manifests/`,
`metadata/tables/`, `metadata/indexes/`, `checkpoints/`, and `wal/segments/`
collections. A live manifest roots every object key its `metadata_files`
and `index_files` lists name, whatever their family.
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
   could age past `T`, so GC never deletes or releases any object younger
   than `T`, reachable or not, and treats any checkpoint record younger
   than `T` as a root regardless of state. An object without a provider
   timestamp reads as young.
2. **Floor is necessary, not sufficient.** Being below `wal/floor.json` only
   nominates an object for deletion.
3. **Delete-time re-verification.** Immediately before deleting, GC re-lists
   `checkpoints/`, re-reads the root, head, and floor, and drops from the
   batch anything reachable from that fresh root set. Candidate selection
   may be arbitrarily stale; deletion may not. On large batches the
   re-verification repeats at least every bounded number of deletion
   decisions, so no deletion consults an arbitrarily stale root set.
4. Roots: `metadata/root.json`; active, non-expired checkpoint records whose
   owner still stands (a fork-owned record stops rooting once its target is
   provably gone); and the visible chain from
   `wal/head.json.visible_wal_tip` down to the floor.
5. Missing, corrupt, or ambiguous roots cause retention, not deletion — an
   unreadable checkpoint record suppresses manifest and table deletion for
   the whole pass.
6. Only validated manifests are trusted to protect data.
7. WAL needed to replay from the chosen metadata root to the head is never
   deleted.
8. **Retention wins residual races.** If the floor is ever observed ahead of
   an active checkpoint's basis, the checkpoint's objects remain protected;
   reconciling the floor is an explicit recovery action.
9. **Abandoned bootstraps.** A namespace tree with no `namespace.json` whose
   newest object is older than the reap window `R` (`R >= T`) may be reaped,
   re-checking the marker's absence immediately before deleting. A fork-owned
   checkpoint record whose target tree is completely gone is released under
   the same window: the record must be older than `R`, since a live fork
   retry freshens it before writing any target object.

Deletion proceeds data first, records last, so a crash mid-sweep leaves
orphaned data for the next pass rather than a record whose data vanished.
To keep that true, every readable checkpoint record roots its basis for the
duration of a pass, whatever its lifecycle, expiry, or owner; state, age,
and owner fate gate only the record object itself, and a freed basis becomes
collectable on the pass after its record is gone. A fork-owned record whose
target namespace is verifiably terminally deleted (target head
`state = deleted`, re-checked at delete time) or provably abandoned (rule 9)
is released by compare-and-swap on the record's freshly observed etag —
never deleted while active — so a racing fork freshen always wins or always
observes the release; the released record then reaps by age on a later pass.
The intended end-state remains tracked deletion derived from manifest
predecessor diffs, with the listing sweep demoted to a low-frequency
backstop.

### 6.5 Control-object cleanup

Implementations may clean up expired sessions, uploads, intents, or other
control-plane objects. This is control-plane maintenance, not namespace
history.

### 6.6 Derived work

Derived structures such as search indexes, caches, or materialized summaries
are optional. They may improve performance or higher-level features, but they
are not authoritative. They must be rebuildable from authoritative state, and
their presence on a namespace is recorded in the namespace features map
(section 5).

## 7. Access-control boundaries

ACL and share design is reserved future work (`api.md` reserves the
authorization plane). Two boundaries are format rules today so that
authorization can arrive without a format break:

1. Authorization state is control-plane state. ACL or share changes never
   advance namespace `seq` and never appear in the change feed.
2. An access grant targets durable identity — a whole namespace or a subtree
   identified by `(namespace_id, root_inode_id)` — never path text. Paths are
   presentation; inode-rooted identity is durable.

## 8. Optional commit metadata, resource properties, and timestamps

A commit may carry optional human metadata such as:

- a commit message;

This metadata belongs to the logical commit, not to the resource itself.

A resource may carry optional structured properties such as display hints,
application tags, or a resource-type hint. These properties belong to the
resource, not to the commit. They should move with the inode when the path
changes.

The semantic creation marker in the core model is the create commit in
namespace history, not a wall-clock field. An implementation may expose
wall-clock timestamps such as `committed_at` or `created_at`, but these are
optional and non-semantic.
