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
HTTP wire shapes and immutable, never-rewritten families tolerate unknown
fields, while mutable control-object envelopes and payloads reject them.

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
| **WAL head** | Mutable | The namespace itself. Carries its immutable identity — content store, name policy, and fork provenance — together with the hot head of the semantic commit stream: current visible boundary, writer epoch, writer liveness metadata, replay hints, and visible WAL tip. | `namespaces/{namespace_id}/wal/head.json` |
| **WAL segments** | Immutable | Record one or more logical commits with a contiguous sequence range. | `namespaces/{namespace_id}/wal/segments/{start_seq:020}-{suffix}.wal.zst` |
| **Namespace manifests** | Immutable | Record one namespace file-set version: its metadata table references and a head summary. Table references carry their own owner, so a fork target's manifest names source-owned tables without recording anything about the fork. | `namespaces/{namespace_id}/metadata/manifests/{manifest_object_id}.manifest.json` |
| **Checkpoint records** | Mutable lifecycle | Durable stable-view pins to a metadata manifest, each carrying a required owner (user or fork target); active records may release or revive, and GC conditionally condemns them before deletion. | `namespaces/{namespace_id}/checkpoints/{checkpoint_id}.json` |
| **Metadata tables** | Immutable | Store metadata rows referenced by manifests. Files may be owned by the namespace itself or by a fork source namespace. | `namespaces/{owner_namespace_id}/metadata/tables/{table_id}.sst.zst` |
| **Upload sessions** | Mutable lifecycle | Track one staged-content upload from begin to completion; GC conditionally condemns abandoned active sessions before deletion. | `namespaces/{namespace_id}/uploads/{upload_id}.json` |
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
Forks are copy-on-write: the target's head names a source-owned manifest as
its starting basis, later target manifests may go on referencing source-owned
metadata tables, and the source holds a fork-owned checkpoint record
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

WAL and metadata-table deletion is reachability-driven from the live
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

The load-bearing invariants of this layout, in one place:

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
5. Content verification is read-and-hash: a `content_ref` is verified only
   by reading the object bytes and computing the digest. Provider checksum
   metadata is not part of the read contract — checksum semantics diverge
   across providers, so it is used (where a provider supports it) purely as
   upload transport integrity, never consulted on reads. A HEAD may
   prevalidate existence and size so a wrong-sized object fails fast.

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

A reader or writer resolves content through the namespace head:
`namespace_id -> head.content_store_id -> content-stores/{content_store_id}/...`.
File revisions and change-feed payloads store only `content_ref`; they do not
store content-store ids or object-store paths.

### 1.7 Mutable control-object rules

Small mutable objects such as the namespace head must use compare-and-swap
semantics. These objects must remain small enough that guarded rewrite is
practical.

Five control-object kinds are registered: `wal_head`, `wal_floor`,
`metadata_root`, `checkpoint_record`, and `upload_session`. A control-object
envelope carrying any other kind string is rejected, not skipped.

Mutable control-object decoders reject unknown fields in both the envelope and
the complete nested payload. Otherwise, an older binary could tolerate a
newer field, erase it during read-modify-write, and still report a successful
guarded update. All five registered kinds use that strict decoder; immutable
WAL segments, metadata segments, namespace manifests, grep segments, and grep
manifests remain tolerant of additive fields.

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

- a head that records its namespace id, content-store id, name policy, and
  fork provenance, and that carries its current visible boundary
- an ordered WAL of logical commits stored in immutable segments
- immutable namespace manifests that describe recoverable file-set versions
- zero or more checkpoints
- a retention policy

The head is the whole of a namespace's durable identity. Nothing else records
which content store holds its bytes, which name policy compares its sibling
names, or which namespace it was forked from, so a head that is missing or
unreadable is not a namespace with a lost accessory: it is not a namespace.

The head also carries the next monotonic inode id for that namespace. New
inode ids are allocated from the head as part of commit publication.

The canonical identity of an item is `(namespace_id, inode_id)`.

Each namespace has exactly one immutable `content_store_id`, recorded in its
head. The content store is an immutable pool for file bytes and may be
referenced by many namespaces. A new root namespace mints a fresh content
store id by random generation; a forked namespace copies the source
namespace's id while starting an independent namespace metadata history,
which is what makes forks copy-on-write over the same bytes.

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
`..`, and must not exceed 255 UTF-8 bytes as stored. Names also satisfy a
portability floor — the set every target filesystem can hold: a name must
not be entirely whitespace, must not end with a space or a dot, and must not
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

#### 2.3.1 NamePolicy

Sibling-name comparison is governed by a versioned `NamePolicy`. A namespace
has exactly one active name policy, chosen at creation and recorded in the
head. The head is the single authority for name-key computation on both the
read and the write path: a stored name key means nothing except under the
policy that produced it, so there is nowhere else for the policy to live and
no default to fall back on.

The v0 policy is `nfc_casefold_v0`, which defines sibling-name comparison by
Unicode NFC normalization plus case folding. Future policies may exist, but
all writers for a namespace must agree on the namespace's active policy.

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
  head to its `content_store_id`;
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

A namespace head has exactly two recorded states: `active` (the default; an
absent field reads as active) and terminal `deleted`. There is no
initialization state and no intermediate state of any kind, because there is
no initialization to observe: the head is published complete by one
conditional write, so a namespace either has a head or does not exist.
Deletion is the one transition the head must record, and it keeps that head
forever as the tombstone that retires its `namespace_id`. Readers MUST refuse
to serve a namespace whose head state they do not recognize; decoding is
fail-closed, never best-effort.

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
operator-only work. Metadata is reclaimed by garbage collection: on a
terminally deleted namespace a GC pass reaps the WAL chain, metadata tables,
manifests, and non-protecting checkpoint records under the usual windows,
leaving the head as the id-retiring tombstone, together with the root and
floor objects if the namespace ever wrote them (section 6, rule 4). Objects
protected by fork-owned checkpoint records survive, so clones of a deleted
source stay readable.

### 2.6 Forks

Forking a namespace creates a new namespace with independent metadata history
and the same `content_store_id` as the source namespace. The fork point is the
source namespace's current head. The implementation creates or reuses a
verified fork-owned source checkpoint at that head and freshens that record by
compare-and-swap, then installs the complete target head with one
create-if-absent. The target writes no manifest, no root, and no floor: the
head's `fork_basis` names the source manifest the target starts from, and the
fork-owned checkpoint record is what keeps that manifest and its tables alive
for as long as the target may still need them.

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
namespace. Same-content-store copies may reuse `content_ref`. Cross-content-
store copies are not supported in v0 unless the content is first imported into
the destination content store. Inode identity does not cross the namespace
boundary.

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
- `name_policy`, required: the single authority for name-key computation
- `fork_basis`, optional: present in every head of a fork target, absent in
  every head of a created namespace

`fork_basis` records five facts: `source_namespace_id`,
`source_manifest_object_id`, `source_manifest_checksum`,
`source_checkpoint_id`, and `fork_seq` — the source sequence at which the
target's own history begins. Section 2.9.1 says what a reader may do with
them.

Every successor head the publisher writes carries those three fields forward
verbatim, along with `namespace_id`. They are the namespace's identity, not
its state, and a namespace cannot change which content store holds its bytes,
which policy compares its names, or where it came from. A publisher that
builds a successor differing in any of them has a construction bug, and the
difference is caught before the compare-and-swap rather than persisted.

Decoding is strict. A head whose `content_store_id` or `name_policy` is
missing is malformed and is hard-rejected, never defaulted: nothing else
records those facts, so a default would silently invent a namespace's content
store or rewrite how its names compare. A head carrying an unknown field is
rejected the same way, under the mutable control-object rules (section 1.7).

The head also summarizes the current visible boundary and replay hints,
including at minimum:

- `seq`
- `head_commit_id`
- `state` (absent or `active`, or terminal `deleted`)
- `next_inode_id`
- `visible_wal_tip` and the bounded `recent_segments` accelerator

`wal/floor.json` is the symmetrical pair to the head — the earliest retained
commit boundary next to the latest visible one. It records `floor_seq` and
verification and update stamps. It is updated only by monotonic compare-and-swap on its
own etag by floor advancement, which is a GC-family operation: it never
touches the WAL head, so the head changes only when commits land. A missing,
stale, or unverifiable floor means "retain more history", never less, and the
floor never affects live commit visibility.

Create and fork write no floor. An absent floor means "retain from the
namespace's birth sequence": 0 for a created namespace, and
`fork_basis.fork_seq` for a fork target, which is the sequence its own history
begins at and below which it never had WAL history to retain. The object is
created by the first retention-floor advance and not before.

`metadata/root.json` is the live read/recovery pointer. It is updated only by
monotonic compare-and-swap on its own etag: a replacement must not decrease
`manifest_head_seq`, a same-seq replacement may reference a different manifest
(that is how pure compaction publishes a better physical layout of the same
logical state), and a lower-seq attempt no-ops in favor of the newer root.
The root never defines live visibility, and a stale root only costs extra WAL
replay. A reader that observes `root.manifest_head_seq > head.seq` reloads
the head — the root can only reference published state, so a fresh head read
observes at least the root's seq; this race is not corruption.

Create and fork write no root either. `metadata/root.json` is created by the
namespace's first flush or reorganization, which is the first moment there is
a materialized file set worth pointing at. Until then the head resolves the
basis on its own. Once the root exists it is the basis, and `fork_basis`
becomes provenance only.

A checkpoint is a durable pin to a namespace manifest version, stored as a
first-class record under `checkpoints/` — never inside a manifest, and never
an input to latest visibility. A record carries its basis facts (manifest id,
seq, payload checksum, head commit id), a required tagged `owner` — a `user`
owner with a name label, or a `fork` owner naming the target namespace the
pin protects — an optional expiry (user-owned records only; fork pins never
expire), and a lifecycle `state` of `active`, `released`, or absorbing
`condemned`. Only active, non-expired records are long-term GC roots;
released records remain revivable collectable tombstones, while condemned
records refuse renewal, revival, and release. A user-owned
record persists until released or expired; a fork-owned record persists while
its fork target may still read the basis. Creation is write-then-verify:
write the record active, then verify — under the self-enforced verify
budget — that the floor has not passed the basis and the basis manifest still
loads; on failure flip the record to released and retry against a newer
basis. Combined with the GC grace window and delete-time re-verification,
this closes the create-vs-collect race: within the grace window any record is
protected unconditionally by age.

Record ids derive deterministically from the basis identity plus the owner
identity: repeating creation for the same pinned manifest and owner renews
the existing record (reviving a released one after re-verification) without
listing. Renewal is last-write-wins on the expiry: the record's
`expires_at_ms` becomes exactly what the latest create requested — extended,
shortened, or cleared — while `created_at_ms` keeps the original creation
instant, and the response always reports the durable state. Distinct owners
of one basis hold distinct records with
independent lifecycles. Explicit release flips a user-owned record
`active -> released` by compare-and-swap and is idempotent. GC loads a
collectable record and its etag together, changes `released` (or expired
`active`) to `condemned` with exactly that etag, and only then deletes the key
unconditionally. A failed condemn CAS means the inspected state changed, so
the record is retained without retry. A crash after condemnation blocks the
deterministic name benignly until the next GC pass deletes the condemned
record; a fresh create can then reuse the name. Its basis becomes collectable
only after condemnation (records-last, "Garbage collection").

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
   `fork_basis.source_manifest_object_id`, read under
   `fork_basis.source_namespace_id`'s prefix.

Case 3 is the only cross-namespace read in the format, and the head is the
only thing that may authorize one. Call this rule the **head-authorized
foreign basis**: no manifest, root, checkpoint, or table may send a reader
into another namespace's prefix on its own say-so, because only the head is
carried forward verbatim by every publication and so only the head can be
trusted to still mean what it said when the fork happened.

The foreign basis is hard-validated on every load. The manifest that comes
back must carry a `namespace_id` equal to `fork_basis.source_namespace_id`,
and a `payload_checksum` equal to `fork_basis.source_manifest_checksum`. Both
must hold. Either mismatch is `namespace_corrupt` and the load stops there.
The two checks are what make a cross-namespace read safe, so a failed check
can never be answered by reading something else instead: there is no fallback
path, no search for a nearby manifest, and no dropping back to the genesis
basis.

`fork_basis.source_checkpoint_id` names the fork-owned checkpoint record on
the source that keeps the basis manifest and its tables alive. That record,
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

A write has four phases: durably stage content (if a mutation contains
content), reconstruct and validate, publish one or more logical commits into
the WAL, and advance the head. A commit request may be rejected immediately,
or tentatively accepted and written to a WAL segment, but it is committed and
successful only if the WAL segment is durably stored and the head advances to
reference it. A metadata change becomes visible only after the head advances.

#### 3.1.1 Content staging

Content must be durable before any metadata change can reference it.

1. Compute the `sha256` digest of the complete plaintext file bytes.
2. Read the namespace head for its `content_store_id`.
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
   storage, and that `content_ref.kind`, digest, and size match. Existence
   and size prevalidate from a HEAD; the digest is verified by reading and
   hashing the object bytes (or skipped entirely under a valid content
   admission token, which proves this server already validated the staged
   bytes).
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

1. Read the namespace **head** object (the namespace's identity, current
   `seq`, and visible WAL tip) and `metadata/root.json` (the manifest
   pointer), concurrently. The head also supplies the `content_store_id` and
   `name_policy` every later step needs.
2. Load and verify the manifest the root references; its payload checksum
   must match the root's `manifest_payload_checksum`. The manifest references
   one or more materialized metadata runs through its `head_seq`. When the
   root is absent, resolve the basis from the head instead (section 2.9.1):
   the genesis state, or the head-authorized source manifest.
3. Use the visible WAL tip named by the head to identify the visible segment
   chain after the basis `head_seq`, then replay the logical commit records in ascending
   `seq` order through `head.seq`. Each logical commit appends normalized rows
   to the same metadata tables.

The result is a metadata view pinned to one `seq`.

For latest path `stat` and directory `list`, an implementation may avoid
hydrating a complete metadata state. The reader may query verified metadata run tables and the visible WAL tail
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
   normalized `name_key` matches the component under the namespace's
   `NamePolicy`.
3. Follow the binding to its `child_inode_id`; v0 path resolution does not
   cross mounts, and mount traversal is reserved future work.
4. If any component has no matching visible binding, the path does not exist.

#### 3.2.4 File content retrieval

Given a visible file inode at seq N:

1. Look up the file's latest revision at N to obtain `content_ref`.
2. Read the namespace head for its `content_store_id`.
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
excludes `commit_id`, writer identity, writer epoch, and `committed_at_ms`: a
retry of the same
logical commit must fingerprint identically no matter who retries it or when.

Path-level mutations fingerprint the same way with domain
`loonfs.path.intent.semantic.v0` over the canonical path intent (intent kind,
canonical absolute paths, the intent's semantic parameters, and the caller
message — `null` when absent, mirroring explicit commits, so reusing a
`commit_id` with a different message conflicts on either surface).

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

A namespace with no `metadata/root.json` has nothing to derive a target from,
so its floor never advances; it retains from its birth sequence until a flush
publishes a root (section 2.9).

Advancement then CASes only `wal/floor.json` — creating it on the first
advance — recording the new `floor_seq` together with its verification stamp.
Floor updates are monotonic: a replacement never decreases `floor_seq`, and
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
after the root inode, a freshly minted content store id, the namespace's name
policy, and no fork basis — and write it with create-if-absent.

That write is the whole protocol. No manifest, root, or floor is prepared
before it, and none is written after it: the genesis basis is built in
(section 2.9.1), and the root and floor objects appear when the namespace's
first flush and first retention advance need them.

#### 3.9.2 Forking a namespace

A fork creates a new namespace from the source namespace's current head. The
request supplies only the new namespace id; the server supplies the mutation
context. The protocol is:

1. Read and verify the source head and its WAL visibility chain.
2. Create or reuse a verified fork-owned source checkpoint at that head
   (owner: the target namespace id). This record is the reachability root
   that keeps the source's basis manifest and tables alive for as long as the
   target lives; nothing under the target's prefix protects them.
3. Freshen the fork-owned record by compare-and-swap before writing the
   target: the rewrite re-stamps the record's provider timestamp so the
   abandoned-fork rule cannot fire under a live retry, and it serializes the
   fork against a concurrent GC release — whichever compare-and-swap lands
   second fails, so a released record is observed (and revived, re-verifying
   the basis) rather than raced.
4. Read the manifest that record pins for the target's fork sequence and next
   inode id, then build the complete active target head: the source's
   `content_store_id` and `name_policy` copied verbatim from the source head,
   and a `fork_basis` naming the source namespace, that manifest's object id
   and payload checksum, the source checkpoint id, and the fork sequence.
   Write it with create-if-absent.
5. Re-read the source checkpoint record. If it is no longer active, delete
   the target through the ordinary namespace-delete path and return the
   checkpoint failure.

The target copies the source's `content_store_id` because a fork shares file
bytes copy-on-write, and its `name_policy` because it inherits the source's
materialized name keys, which mean nothing under a different policy.

Step 5 exists because steps 3 and 4 are two separate writes to two different
objects. A forker that stalls between them can have its freshened record
released underneath it, and without the re-read the target would survive with
an unprotected basis — a namespace that reads correctly today and reports
corruption after the next GC pass. Deleting the target through the ordinary
delete path, rather than erasing the head, keeps the failure inside the one
lifecycle every other operation already understands.

The fork does not copy content-store blobs or source metadata SSTs, and
writes no target manifest, root, or floor. A successful fork has independent
namespace history from the fork point, starting its own WAL at
`fork_seq + 1`. Future target WAL, checkpoints, and metadata SSTs are written
under the target namespace root. Until the target's first flush publishes a
root, its head resolves the basis (section 2.9.1).

#### 3.9.3 Conflicting installs

Create and fork answer a lost create-if-absent the same way, because they
write the same object under the same conditions.

The loser reads the head back. An active head means the id is taken and the
answer is `namespace_exists`; a deleted head answers `namespace_deleted`; a
head that cannot be decoded is corruption and is reported as such — never
overwritten, and never taken as an empty slot. There is no fourth answer,
because there is no state between absent and complete.

The loser is not told whether the winner was its own earlier attempt.
Nothing durable can say so: the head's writer block names one writer
session, and a server holds one session across every caller it serves, so
"written by my session" does not mean "written by this attempt" — two
callers of one server would otherwise both be told they created the same
namespace. An embedded caller that wants a retry after a lost
acknowledgment to succeed asks for that explicitly, with its
create-if-not-exists option.

This is a strictly better answer than the previous protocol gave. The
namespace named by `namespace_exists` is complete and usable; the multi-object
install could leave a namespace that existed, could not be used, and could
only be finished by an explicit repair.

### 3.10 Long-running operations

Some operations are not well described by one request.

Examples include:

- recursive reads that need a pinned snapshot; and
- resumable uploads that need a stable destination binding.

In those cases, the server may create control-plane objects such as read
sessions, upload sessions, or put intents. Durable upload sessions carry an
`active` or absorbing `condemned` lifecycle. GC condemns an aged abandoned
active session with the exact etag inspected with its provider age before
deleting it. Completion that observes `condemned` reports
`upload_not_found`: condemned is logically absent and that existing code is
also the result after physical deletion. A completion CAS that lands first
makes the GC CAS fail and the pass retains the session; a condemnation that
lands first makes completion lose and report `upload_not_found`.

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
| Grep root pointer | `grep_root` | JSON, uncompressed | `v1` |
| Grep manifest | `grep_manifest` | JSON, uncompressed | `v1` |
| Grep segment | none (section 4.2.2) | block sections, per-block zstd + CRC32C | `v1` (via the grep manifest) |
| Namespace manifest | `namespace_manifest` | JSON, uncompressed | 1 |
| Control objects (head, metadata root, WAL floor) | per-kind snake_case names | JSON, uncompressed | 1 (tracked per kind) |
| Checkpoint record | `checkpoint_record` | JSON, uncompressed | 1 |
| Upload session | `upload_session` | JSON, uncompressed | 1 |

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

#### 4.2.2 Grep roots, manifests, and gram-index segments

`loonfs-grep` owns all grep durability under the namespace extension prefix:

```text
namespaces/{namespace_id}/extensions/grep/
├── root.json
├── manifests/{manifest_id}.manifest.json
└── segments/{segment_id}.sst.zst
```

`manifest_id` is the 64-character lowercase hex portion of the SHA-256 digest
of the exact manifest payload fragment. The manifest envelope's
`payload_checksum` is `sha256:{manifest_id}`. Namespace manifests carry no
grep pointer, watermark, lifecycle, or segment references. A fork therefore
starts without grep state until grep is enabled for the target.

`root.json` is a small mutable pointer envelope with these fields, in order:

- envelope: `kind = "grep_root"`, `format_version = "v1"`, informational
  `writer_version`, `payload_checksum`, and raw JSON `payload`;
- payload: `namespace_id` and `manifest_id`.

Each immutable manifest has the same envelope grammar with
`kind = "grep_manifest"` and `format_version = "v1"`. Its payload is the full
grep state: `namespace_id`, `lifecycle`, nested `index` bookkeeping, and the
`segments` descriptors. Both decoders verify the checksum over the exact
stored payload fragment before decoding, reject unknown versions and kind
mismatches without fallback, and validate namespace, manifest-id, lifecycle,
fold, run-allocation, and segment invariants at every boundary. The mutable
root-pointer decoder rejects unknown envelope and payload fields; the
immutable manifest decoder tolerates additive fields.

A gram-index segment uses the section 4.2.1 block grammar unchanged —
prefix-compressed data blocks, one bloom filter block, one index block,
handles and checksums in the grep-manifest descriptor — with a grep-owned row
payload instead of metadata rows. The tokenizer, row shapes, and posting
encoding below are frozen by grep format `v1`; their evolution follows the
rules in section 4.3 and always permits rebuilding this derived work
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

Publication writes segments first, writes the content-derived manifest with
create-if-absent semantics, and finally installs `root.json` with one etag
compare-and-swap (or create-if-absent for the first pointer). A pointer-CAS
loser's manifest and segments remain unreachable derived garbage; grep GC
reclaims them after its grace window. Because an identical rebuild derives
the same manifest id, an AlreadyExists observation can race that collection:
after a successful pointer CAS, the publisher HEADs the installed manifest
and re-puts its still-buffered bytes with create-if-absent if GC removed it.
The pointer is returned only after that verification/heal completes. Query
readers load the pointer afresh, then load the immutable manifest it names;
decoded manifests may be cached by manifest id.

The namespace-scoped layout is maintained only when that namespace is named
by an enable, publish, query, detached assignment, or explicit GC operation;
grep never enumerates namespaces. Embedded drivers are event-driven and use
no recurring timer. A detached `loonfs-grep` deployment polls the head of each
explicitly assigned namespace at its configured `poll_interval_ms`. Grep GC
is explicit and per namespace: it retains the verified pointer, referenced
manifest, and referenced segments,
degrades to retention on corruption or ambiguity, and reaps the whole
`extensions/grep/` prefix when explicitly pointed at a tombstoned or absent
namespace. Core maintenance does not recognize or collect `extensions/` keys,
and grep maintenance does not collect core-owned objects.

### 4.3 Evolution rules

- **Additive within a released version.** Readers ignore unknown payload and
  envelope fields. After the first stable release, adding such fields is the
  only change permitted within an existing format version.
- **Other post-release changes require a new version.** After the first stable
  release, renaming, removing, retyping, or re-tagging any field — or changing
  the payload encoding — requires a new `format_version` for the owning family.
  Readers reject versions they do not support with a typed unsupported-version
  error; there is no silent fallback.
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
  durable bytes fails those tests. During pre-release development, an
  intentional change regenerates the version-1 fixture in place; after the
  first stable release, it follows the evolution rules above.

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
implementation schedules a background maintenance step after any runtime
publish that observes the WAL tail at or past the WAL-tail policy's
checkpoint threshold (32 segments at defaults), and every publish surface
rejects with `maintenance_required` once the tail exceeds the same policy's
write-rejection threshold (128 at defaults). Reads never gate on tail
length. Bounded reads are the
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
is enforced by every reorganization rewrite over the complete input runs that
the rewrite selected.

### 6.2 Compaction

Compaction rewrites metadata runs (and, in the future, content layouts) into
more efficient physical shapes.

A base rebuild drops rows that no retained sequence can observe: bindings
superseded or unbound at or below the retention floor, spent unbind markers,
and commit receipts below the floor. The floor governs replay state only.
Revision rows are never dropped: file revision history is durable data,
retained in full regardless of the floor, and a revisions listing is always
complete. Tombstone rows — set and revoke events alike — and inode rows are
always retained for now; reachability-based dropping for them is future
work.

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
promised. It bounds only replay state — change-feed resumption, superseded
binding rows, and commit receipts — never file revision history, which is
retained in full.

A retention floor may advance only when the system has enough verified
material to support readers from the new floor forward, and it never
advances implicitly: the default posture retains everything, and the floor
moves only when an operator opts in (an explicit retention advance, or a
maintenance step that requested it).

### 6.4 Garbage collection

Delete is tombstone-first. Garbage collection is the separate process that
eventually reclaims content or metadata that is no longer reachable and no
longer protected by retention policy. GC and floor advancement are the only
consumers of listing, and nothing sweeps by default: a pass runs only through
the admin endpoint or an explicit maintenance-step opt-in.

A pass reads the namespace head first. An absent head means the namespace
does not exist, so there is nothing to collect and nothing to ignore.

v1 GC is listing mark-and-sweep. Its inputs are `wal/head.json`,
`wal/floor.json`, `metadata/root.json`, and the `metadata/manifests/`,
`metadata/tables/`, `checkpoints/`, and `wal/segments/` collections. A live
manifest roots every object key its `metadata_files` list names. The pass also sweeps
`uploads/`: sessions root nothing and nothing durable references them, so a
session whose provider age exceeds the reap window is first compare-and-swapped
from `active` to absorbing `condemned` under the etag loaded with that age,
then deleted unconditionally. A lost CAS retains the session for a later
pass. Completed sessions are already absorbing and may be deleted directly;
a completion that loses to condemnation answers `upload_not_found`.
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
   `wal/head.json.visible_wal_tip` down to the floor. A namespace with no
   root of its own has no manifest or table to protect under its own prefix;
   its basis, if it has a foreign one, is protected on the source side by
   the fork-owned checkpoint record. On a terminally
   deleted namespace the root set shrinks to fork-owned records protecting
   a live target (and their bases): reads are impossible and the tombstone
   is immutable, so user pins, the final replay chain, and the last
   manifest protect nothing and age out. The head survives as the
   id-retiring tombstone, together with the root and floor objects if the
   namespace ever wrote them.
5. Missing, corrupt, or ambiguous roots cause retention, not deletion — an
   unreadable checkpoint record suppresses manifest and table deletion for
   the whole pass.
6. Only validated manifests are trusted to protect data.
7. WAL needed to replay from the chosen metadata root to the head is never
   deleted.
8. **Retention wins residual races.** If the floor is ever observed ahead of
   an active checkpoint's basis, the checkpoint's objects remain protected;
   reconciling the floor is an explicit recovery action.
9. **Immutable sweep families need no condemnation.** WAL segments, metadata
   tables and manifests, grep segments and manifests, and content blobs have
   keys that can only contain identical bytes under their create-if-absent,
   content-derived, or write-verification protocols. Once one is unreferenced
   and grace-aged, unconditional deletion is safe: a zombie retry can at most
   recreate identical, still-unreferenced bytes for a later pass. Content-blob
   GC remains unsupported in v0, but the same immutability argument governs a
   future sweep.
10. **Abandoned forks.** A fork that crashes after freshening its source
   checkpoint record but before installing its target head leaves that
   record with no target at all. Such a record remains GC-owned and is
   released under the reap window `R` (`R >= T`): the record must be older
   than `R`, since a live fork retry freshens it before writing the target
   head. This is the only debris an interrupted install can leave, and it
   lives on the source, never under the target's prefix — the target either
   has a complete head or has nothing.

Deletion proceeds data first, records last, so a crash mid-sweep leaves
orphaned data for the next pass rather than a record whose data vanished.
To keep that true, every readable non-condemned checkpoint record roots its
basis for the duration of a pass, whatever its lifecycle, expiry, or owner;
state, age, and owner fate gate only its condemnation. `condemned` is the sole
exception because it cannot legally revive; crash residue in that state is
deleted by the next pass and need not continue rooting. A fork-owned record whose
target namespace is verifiably terminally deleted (target head
`state = deleted`, re-checked at delete time) or provably abandoned (rule 10)
is released by compare-and-swap on the record's freshly observed etag —
never deleted while active — so a racing fork freshen always wins or always
observes the release. An aged collectable record is then condemned by one
exact-etag CAS and deleted only after that CAS succeeds; precondition failure
means retention, never fallback deletion. If physical deletion fails after
the CAS, the absorbing record makes the next pass self-healing.
The intended end-state remains tracked deletion derived from manifest
predecessor diffs, with the listing sweep demoted to a low-frequency
backstop.

### 6.5 Control-object cleanup

Upload sessions are condemned and cleaned by the GC pass after the reap
window ("Garbage collection"). Implementations may additionally clean up
other expired control-plane objects. Mutable control objects MUST first enter
an absorbing state by conditional write under the inspected etag; a failed
conditional write retains them. This is control-plane maintenance, not
namespace history.

### 6.6 Derived work

Derived structures such as search indexes, caches, or materialized summaries
are optional. They may improve performance or higher-level features, but they
are not authoritative. They must be rebuildable from authoritative state, and
their presence and lifecycle are recorded in their extension-owned keyspace
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
namespace history, not a wall-clock field. Every WAL commit record, commit
receipt row, and revision row carries a required `committed_at_ms`: the
wall-clock stamp of the publishing writer's request context, in Unix
milliseconds. The stamp is observational and non-semantic — sequences are
the only ordering and validity inputs, the fingerprint preimage excludes
it, and correctness never depends on clocks being aligned. Surfaces expose
it as modification metadata (a file's modified time is the stamp of the
commit that created its current revision).
