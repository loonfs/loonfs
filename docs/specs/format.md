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
are suffixed `_id`; tagged unions carry their discriminator in a `kind` field.

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
- **Immutable, never-rewritten durable families tolerate them**, for the same
  reason: nothing rewrites them, so nothing can erase a field it did not
  understand.
- **Mutable control-object envelopes and payloads reject them**, because a
  reader that tolerated an unknown field would erase it on the next
  read-modify-write and still report a successful guarded update
  (section 1.7).

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
| **Checkpoint records** | Mutable lifecycle | Durable stable-view pins to a metadata manifest, each carrying a required owner (user or fork target). The lifecycle is monotonic: a record is created active under a generated id, released once by compare-and-swap, and deleted a grace window after that release. | `namespaces/{namespace_id}/checkpoints/{checkpoint_id}.json` |
| **Metadata tables** | Immutable | Store metadata rows referenced by manifests. Files may be owned by the namespace itself or by a fork source namespace. | `namespaces/{owner_namespace_id}/metadata/tables/{table_id}.sst.zst` |
| **Upload sessions** | Mutable lifecycle | Track one staged-content upload. The lifecycle is monotonic: a session is created `open` under a lease, and moves once to `completed` or `aborted`, both terminal. | `namespaces/{namespace_id}/uploads/{upload_id}.json` |
| **Metadata root** | Mutable | Cold pointer to the best known materialized metadata root; monotonic CAS. | `namespaces/{namespace_id}/metadata/root.json` |
| **WAL floor** | Mutable | Cold lower bound of retained WAL/change history; monotonic CAS. | `namespaces/{namespace_id}/wal/floor.json` |
| **Content objects** | Immutable | Store one file revision's complete bytes. | `content-stores/{content_store_id}/objects/{content_id[4..6]}/{content_id[6..8]}/{content_id}` |

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

The content model has six rules.

1. **Identity and integrity are separate.** A content object's identity is a
   random `content_id`; its integrity evidence is the checksums carried
   beside that id. Nothing about a content object's name describes its bytes,
   so a reference's checksums are the only thing that ever does.
2. A `content_ref` describes one complete file revision.
3. Immutable content objects are written with create-if-absent semantics.
   Random ids cannot collide, so a create that finds the key occupied is
   corruption and must fail rather than overwrite.
4. A metadata commit may reference a `content_ref` only after the referenced
   object is already durable.
5. **Every reference carries a mandatory full-object checksum.** There is no
   checksum-*type* field: full-object coverage is an invariant of this
   format, established when the object is written and never read back from a
   provider. (Cloudflare R2 does not report a checksum type at all, so a type
   read back would be missing exactly where it would matter.)
6. **Read verification uses the reference's own evidence.** A read recomputes
   the reference's whole-file SHA-256 over the bytes it fetched. A reference
   whose evidence this implementation cannot recompute fails the read; no
   read is served unverified. A HEAD may prevalidate existence and size so a
   wrong-sized object fails fast.

##### Checksum provenance

`whole_file_sha256` present means **a trusted party computed it over the
complete stream**: either the LoonFS write path hashed the whole payload
itself, or a provider validated a signed whole-object SHA-256 on the write it
accepted. There are no client-claimed digests in this format. A client's
declared digest becomes trusted evidence only by being signed into a write the
provider refuses to accept unless the bytes match, and by being checked again
against the stored object at completion.

Absent therefore means *nobody trustworthy hashed these bytes* — never "the
client did not tell us". That single meaning is what lets a reader treat the
field as a decision rather than a hint.

`storage_checksum.algorithm` is one of `sha256`, `crc64nvme`, or `crc32c`, and
`storage_checksum.value` is the lowercase hex of the raw checksum bytes (the
algorithm is its own field, so the value carries no prefix; provider APIs that
report base64 are converted at the adapter).

Every algorithm in that list is producible, and the list is closed: an
algorithm spelling a reader does not know fails to decode rather than
decoding into a value nothing can recompute. Every path that moves bytes
through LoonFS hashes them and produces `sha256`. Direct multipart upload
produces `crc64nvme`: an S3-compatible provider assembles a multipart object
without any party hashing the whole stream, and the CRC-64/NVME it computes
over the assembly is the only full-object evidence that will ever exist for
those bytes. `crc32c` is the full-object checksum Google Cloud Storage
computes and reports, and is likewise the only evidence for an object
transferred straight to it. A reference produced either way carries no
`whole_file_sha256`, by the provenance rule above, and reads verify it by
recomputing that CRC. A reference carrying a checksum an implementation
cannot recompute must fail its reads rather than pass them unverified.

Metadata materialization tables include canonical metadata families and
validated derived families. The canonical families are `inodes`,
`direntry-binds`, `direntry-unbinds`, `revisions`, `tombstones`,
`commit-receipts`, and `attributes`. The `direntry-child-binds` family is a
secondary index over the same direntry bind rows, keyed by child inode, and
must be present and verified before a namespace manifest is trusted. The
`active-deletions` family is derived from the tombstone rows and holds current
state rather than events (section 2.5).

The `attributes` family holds one row per attribute revision of one inode.
Each row carries `inode_id`, `attributes_revision_no`, `committed_seq`,
`delta_index`, and the inode's complete `attributes` map (section 8). Its row
key is

```text
attributes-{inode_id:020}-{u64::MAX - attributes_revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}
```

and its bloom-filter lookup prefix is `attributes-{inode_id:020}`. The
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
   concurrency control, and a caller that must also catch the concurrent case
   observes the object again before it relies on it.

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
    "kind": "blob_v1",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 19482,
    "storage_checksum": { "algorithm": "sha256", "value": "42d..." },
    "whole_file_sha256": "42d..."
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
- `content_ref.storage_checksum` is mandatory and covers the complete object;
- `content_ref.whole_file_sha256` is present exactly when a trusted party
  hashed the whole stream (section 1.6);
- all content-object access resolves `namespace_id` through the namespace
  head to its `content_store_id`;
- future content strategies must use a new `content_ref.kind` and name their
  durability and validation rules before revisions may reference them.

The `ContentRef` document rejects unknown fields, so a field this format does
not define is corruption, not a future extension:

```json
{
  "kind": "blob_v1",
  "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
  "size_bytes": 19482,
  "storage_checksum": { "algorithm": "sha256", "value": "<64 lowercase hex>" },
  "whole_file_sha256": "<64 lowercase hex>"
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
undelete restores in place. A delete addressed by inode has no name to
record and writes `deleted_direntry` as `null`; the field is always
written, so an encoding that omits it is not a deletion without a name but
corruption. Only a `set` can carry one, and a `revoke` that does is
corruption too — a partial binding is not expressible at all. This schema
evolves in place at metadata-row version 1 before the first stable
release; the implementation carries no compatibility shim for intermediate
pre-release encodings, and in particular the earlier encoding that spelled
the generation as `tombstone_seq` and `tombstone_delta_index` beside
independent optional `parent_inode_id`, `name_key`, and `display_name`
fields does not decode.

Which deletions are recoverable *right now* is a separate question from the
event history that decides it, and it has its own family. Materialization
derives an `active-deletions` row from every tombstone event: a `set` adds a
`listed` row keyed by `(deleted_at_seq, root_inode_id)` — the exact handle
`undelete` takes — carrying the deletion's wall-clock stamp and a copy of
its `deleted_direntry`, and a `revoke` adds a `removed` row repeating its
target's key. The two rows for one deletion sort together with `removed` first, so an
ascending scan sees a removal before the row it removes and reorganization
drops the pair. Reading the recoverable set is therefore a range scan in
deletion order, not a walk over every deletion the namespace ever recorded.
Because the family is derived, the tombstone rows stay authoritative: a
disagreement between the two is corruption of the derived family.

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

Namespace deletion does not imply content-store deletion. In v0, deleting a
content store is unsupported operator-only work, and the only content garbage
collection is the narrow one described in section 6.4: an object a completed
upload session owns and no metadata references. Metadata is reclaimed by garbage collection: on a
terminally deleted namespace a GC pass reaps the WAL chain, metadata tables,
manifests, and non-protecting checkpoint records under the usual windows,
leaving the head as the id-retiring tombstone, together with the root and
floor objects if the namespace ever wrote them (section 6, rule 4). Objects
protected by fork-owned checkpoint records survive, so clones of a deleted
source stay readable.

### 2.6 Forks

Forking a namespace creates a new namespace with independent metadata history
and the same `content_store_id` as the source namespace. The fork point is the
source namespace's current head. Every attempt creates its own leased,
verified fork-owned source checkpoint at that head, then installs the complete
target head with one create-if-absent. The target writes no manifest, no root, and no floor: the
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
- `fork_basis`, optional: present in every head of a fork target, absent in
  every head of a created namespace

`fork_basis` records five facts: `source_namespace_id`,
`source_manifest_object_id`, `source_manifest_checksum`,
`source_checkpoint_id`, and `fork_seq` — the source sequence at which the
target's own history begins. Section 2.9.1 says what a reader may do with
them.

Every successor head the publisher writes carries those fields forward
verbatim, along with `namespace_id`. They are the namespace's identity, not
its state, and a namespace cannot change which content store holds its bytes
or where it came from. A publisher that builds a successor differing in any of
them has a construction bug, and the difference is caught before the
compare-and-swap rather than persisted.

Decoding is strict. A head whose `content_store_id` is missing is malformed
and is hard-rejected, never defaulted: nothing else records that fact, so a
default would silently invent a namespace's content store. A head carrying an
unknown field is rejected the same way, under the mutable control-object rules
(section 1.7).

The head also summarizes the current visible boundary and replay hints,
including at minimum:

- `seq`
- `head_commit_id`
- `state` (absent or `active`, or terminal `deleted`)
- `next_inode_id`
- `visible_wal_tip` and the bounded `recent_segments` accelerator

`recent_segments` always begins at `visible_wal_tip`. A head published before
the namespace's first commit carries neither, and every head published after
it carries the tip as the accelerator's first entry, because one
compare-and-swap writes both. A head that disagrees with itself is corrupt,
and decoding rejects it.

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
pin protects — an optional `expires_at_ms`, and a tagged lifecycle `state`.
Creation is write-then-verify: write the record active, then verify — under
the self-enforced verify budget — that the floor has not passed the basis and
the basis manifest still loads; on failure release the record and retry
against a newer basis. Combined with the GC grace window and delete-time
re-verification, this closes the create-vs-collect race: a record whose
`created_at_ms` is inside the grace window is still inside its own verify
budget, so nothing releases it for a basis it may yet prove.

The lifecycle is monotonic. It has two states and one transition:

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

No checkpoint state transition consults a provider object timestamp. Every
instant the lifecycle depends on lives in the record: `created_at_ms` for the
create-vs-collect grace, `expires_at_ms` for the release, and
`released_at_ms` for the deletion.

`expires_at_ms` means "GC may release this without asking anyone". A user pin
carries the caller's `ttl_ms`, or nothing at all, in which case it is held
until released. A fork-owned record always carries one: it is the lease for a
single fork attempt (section 3.9.2), and letting it pass is how an abandoned
attempt becomes collectable; a fork-owned record read without one is rejected
at decode, like any other corruption. An active record whose expiry has passed
still pins and still serves — until the pass that releases it, it is a root,
and answering from it is answering from state that is provably still there.

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
     "storage_checksum": { "algorithm": "sha256", "value": "<64hex>" },
     "whole_file_sha256": "<64hex>"
   }
   ```

Staging the same bytes twice under two different uploads writes two objects,
one per id. Retrying *within* one upload session reuses that session's id and
therefore its object, which is where staging idempotency now lives. An
orphaned content object — one whose upload never completed — is harmless
because nothing can reference an id that was never published.

**Direct upload** hands the transfer to the client instead of proxying it. The
client declares the size and SHA-256 of bytes it already holds; the server
mints the identity, signs both the digest and a create-only precondition into
a short-lived write capability, and returns the resulting `content_ref`. A
client can never name the object it writes to.

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
   pointer), concurrently. The head also supplies the `content_store_id`
   every later step needs.
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
5. Verify that the fetched bytes match `content_ref.size_bytes` and the
   reference's `whole_file_sha256`. A reference whose evidence the reader
   cannot recompute fails the read; nothing is served unverified.

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

A fingerprint value is `v0:sha256:<64 lowercase hex>`. The `v0` tag names the
canonicalization rules below and `sha256` the digest algorithm, so either can
change later without re-interpreting stored values.

The `v0` preimage is the compact JSON encoding (no whitespace, object keys in
exactly the order shown) of:

```json
{
  "domain": "loonfs.commit.semantic.v0",
  "namespace_id": "...",
  "operations": [...],
  "message": "... or null"
}
```

where `operations` appear in request order, each as its canonical form
(operation kind, canonical absolute paths, and the operation's semantic
parameters including its caller-supplied race guards), and `message` is
`null` when absent — so reusing a `commit_id` with a different message, a
different guard, or the same operations in a different order conflicts. The
preimage deliberately excludes `commit_id`, writer epoch, and
`committed_at_ms`: a retry of the same logical commit must fingerprint
identically no matter who retries it or when.

A content reference enters the preimage as exactly:

```json
{ "kind": "blob_v1", "content_id": "con_<32hex>", "size_bytes": 123 }
```

The checksums are excluded on purpose. They are evidence about the object,
pinned to its id by the verification every write and read performs — not part
of *which* object the request attaches. Including them would make a reference
that named the same object with a differently spelled checksum read as a
different mutation.

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
values are pinned by tests in `loonfs-api` (`commit_identity.rs`); those
literals must never change within scheme `v0`.

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

The first standard lower-level mutation set includes:

- `create_directory(parent_inode_id, display_name)`
- `create_file(parent_inode_id, display_name, content_ref)`
- `replace_file(inode_id, base_revision_no, content_ref)`
- `rename(inode_id, new_parent_inode_id, new_display_name)`
- `delete_file(inode_id)`
- `delete_subtree(root_inode_id)`
- `restore_revision(inode_id, source_revision_no, base_revision_no)`
- `undelete(inode_id, deleted_at_seq, parent_inode_id, display_name)`
- `update_attributes(inode_id, base_attributes_revision_no, attributes_revision_no, attributes)`

The path-oriented filesystem surface may compile higher-level operations into
these lower-level mutations.

`rename` is always no-replace: a destination binding that already exists
fails validation. Replacing moves exist on the path-oriented surface
(`move_path` with its behavior enum); if commit-level replace-rename is ever
needed it arrives as a new capability-gated field, not a silently tolerated
one.

`update_attributes` states the map the inode holds after the update, not the
writes and removals that produced it. Its `attributes_revision_no` is exactly
one past `base_attributes_revision_no`, and the operation applies only while
the inode is still at the base revision. An update whose resulting map equals
the current one is rejected: attributes are current state with no history, so
a revision that restates the same map has nothing behind it. Attributes are
held against inode identity, so an inode is the operation's target whether it
is a file or a directory, and every other operation leaves them alone.

These are semantic commit operations. Durable WAL payloads store normalized
metadata deltas derived from the semantic operations: `create_inode`,
`bind_direntry`, `unbind_direntry`, `append_file_revision`,
`tombstone_subtree`, `revoke_subtree_tombstone`, and
`append_attributes_revision`. Raw bind/unbind/create-inode deltas are not
standard client-facing commit operations.

The two tombstone deltas carry the same values their rows do (section 2.5):
`tombstone_subtree` states its `deleted_direntry`, as a whole binding or as
`null`, and `revoke_subtree_tombstone` names its `target` generation. The
delta's own generation is implied — its commit's sequence and its
`delta_index` — so it is not written a second time.

`append_attributes_revision` carries `inode_id`, `attributes_revision_no`,
and the inode's complete `attributes` map. Complete state rather than a
change set: replay never needs an earlier revision to answer what an inode
holds. An empty map is a real revision — the cleared state — and it hides
every earlier map for that inode. The delta's own position is implied by its
commit's sequence and its `delta_index`, like every other delta's.

### 3.6 Preconditions

A commit may include explicit preconditions. Preconditions are how clients
say, "apply this only if the namespace still looks like the state I planned
against."

The core kinds of precondition are:

| Kind of check | Example use |
| --- | --- |
| **Name-slot based** | "Create this child only if that name slot is still empty." |
| **Name-binding based** | "Move or delete this item only if this name still points at the inode I saw." |
| **Revision-based** | "Replace this file only if it is still at the revision I saw." |
| **Attribute-revision based** | "Write these attributes only if the inode is still at the attribute revision I saw." |
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
2. Create a verified fork-owned source checkpoint at that head (owner: the
   target namespace id) under a freshly generated id, with a lease:
   `expires_at_ms = now + FORK_CHECKPOINT_LEASE_MS`. This record is the
   reachability root that keeps the source's basis manifest and tables alive
   for as long as the target lives; nothing under the target's prefix
   protects them. Every attempt takes its own record; no attempt reuses,
   refreshes, or revives an earlier one's.
3. Read the manifest that record pins for the target's fork sequence and next
   inode id, then build the complete active target head: the source's
   `content_store_id` copied verbatim from the source head,
   and a `fork_basis` naming the source namespace, that manifest's object id
   and payload checksum, the source checkpoint id, and the fork sequence.
   Write it with create-if-absent.
4. Read the source checkpoint record once. The fork succeeds only if the
   record is active **and** `expires_at_ms > now + FORK_GUARD_MARGIN_MS`.
   Otherwise delete the target through the ordinary namespace-delete path and
   return the checkpoint failure.

The target copies the source's `content_store_id` because a fork shares file
bytes copy-on-write. It inherits the source's materialized name keys
unchanged, which is sound because name-key folding is a fixed rule of the
format (section 2.3.1) rather than a per-namespace choice.

Step 4 exists because steps 2 and 3 are two separate writes to two different
objects. A forker that stalls between them can have its record released
underneath it, and without the check the target would survive with an
unprotected basis — a namespace that reads correctly today and reports
corruption after the next GC pass. The margin is what makes the check sound
where a bare re-read raced: garbage collection releases a fork record only
once its lease has passed, so a lease with more than one provider operation
left cannot legally be released between the read and the caller acting on it.
Past that point the target head is the protection — a fork record whose
target namespace exists and is not deleted is retained by every pass,
whatever its lease says, so nothing has to clear the lease afterwards.
Deleting the target through the ordinary delete path, rather than erasing the
head, keeps the failure inside the one lifecycle every other operation
already understands.

`FORK_CHECKPOINT_LEASE_MS` is derived, not tuned: two GC grace floors
(section 6, rule 1), one for the create and one for everything after it, each
being one publication plus provider bounds plus clock skew.
`FORK_GUARD_MARGIN_MS` is one provider operation's total wall time — the
staleness bound on the guard's own read.

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
Nothing durable can say so: the head's writer block names a writer label,
not an attempt, and a server publishes every caller's work under one label,
so "written by my writer" does not mean "written by this attempt" — two
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
sessions, upload sessions, or put intents.

A durable upload session has three states and one transition:

- `open { expires_at_ms }` — the only live state. A session may stage bytes
  and complete only while open. The expiry is a lease carried in the record,
  so no session transition depends on an object's provider timestamp. Unlike
  a namespace, a session *is* a lease, so reclaiming an expired one on age
  alone is the correct reading rather than a guess about the client.
- `completed { completed_at_ms, content_ref }` — terminal. The reference is
  verified before the transition is written, and it is what every idempotent
  completion retry and every later read answers with.
- `aborted { aborted_at_ms }` — terminal. The session will never select
  content.

Two rules make this decidable under concurrency:

1. **The durable compare-and-swap is the serialization point.** Whichever of
   the two terminal transitions lands is what happened. The loser reports a
   terminal error rather than undoing anything: a completion that finds the
   session aborted reports `upload_not_found`, because an aborted session is
   logically absent and will never select content — the same code the
   subsequent physical deletion produces; an abort that finds the session
   completed reports `upload_already_completed`, because the content may
   already be published.
2. **Provider state follows the durable transition, never precedes it.**
   Completion verifies the object first and only then swaps. An abort swaps
   first and only then deletes the object the session owned. A crash in
   between therefore leaves an object the next garbage-collection pass
   reclaims from the terminal record — never an object deleted out from
   under a session that is still open. Cleanup is idempotent and may be
   repeated freely.

Nothing returns a session to `open`, and no state records consumption: a
client that wants another attempt begins another session, which mints its own
content identity, and publication never writes back to a session record.

A session record is an identity, a transport, and a state. It carries
`namespace_id`, `upload_id`, `content_id`, and `created_at_ms`, then a
`transport` object and a `state` object, each tagged by a `kind`. The content
identity is allocated when the session opens, before any byte is read, so the
object's final key is known from birth and belongs to exactly one session.

The `transport` is settled when the session opens and never changes. Each
`kind` carries exactly what its own path needs, and no other's:

- `service_proxied`: no fields. The service receives the bytes and writes the
  object, so it learns size and digest from the bytes as they pass and has
  nothing to record up front.
- `direct_put`: `promised_content`, the content reference the presigned write
  is signed against. Its `storage_checksum` is given to the provider, which
  refuses any body that does not match, so the reference has to exist before
  the session does; completion reads the stored object back against this same
  reference. The reference already names its own algorithm, so a provider
  that enforces something other than SHA-256 needs no new durable shape.
- `direct_multipart`: `provider_upload_id`, the handle the provider's
  multipart upload is addressed by, and `part_size_bytes`, the geometry the
  session was opened with, which is a non-zero integer.

A `direct_multipart` transport carries no content reference, and this is the
enforcement of section 6.9's rule that a multipart upload claims its payload
at completion: the session is opened for a payload whose length may not be
known yet, so there is nothing to promise, and the record has nowhere to put
a promise if there were. The provider upload id is likewise the *only*
provider handle a session keeps: parts are the uploader's bookkeeping,
exactly as they are in the provider's own API, so there is no durable record
per part and none is permitted. The geometry is recorded because it is
settled at begin and the client may not be told it twice — a session resumed
after a lost begin response reads it back rather than being handed a second,
possibly different, one. Cleanup reads the upload id to abandon what a
terminated session left open, under rule 2 above — after the durable
transition, never before it. Aborting an upload that already assembled its
object is safe on every supported provider: it succeeds and leaves the object
untouched, so cleanup never has to prove what state it is cleaning up first.

The `state` carries what its own phase of the lifecycle needs. `open` carries
`expires_at_ms` and, once bytes have passed validation, `staged_content`;
`completed` carries `completed_at_ms` and the verified `content_ref`;
`aborted` carries `aborted_at_ms`. A completed session's reference lives in
exactly one place — the `completed` state — so no reader has to decide which
of two copies is authoritative, and no writer can leave them disagreeing.

Four invariants are checked when the record is read, because the shape cannot
express them:

- Every content reference the record holds — the transport's promise, the
  staged reference, the completed reference — names the record's own
  `content_id`. A record that disagrees with itself describes two objects and
  could verify one while publishing the other.
- The record carries a `transport` and a `state`. Neither has a default and
  neither may be omitted.
- Only a `service_proxied` session holds `staged_content`: the other
  transports write past the service, so nothing here validated their bytes.
- A `direct_put` session's `completed` reference equals its
  `promised_content` in full, which is the reference the provider enforced
  and completion read back.

A record that fails any of them is rejected outright, like any other
corruption. This schema evolves in place at control-object version 1 before
the first stable release; the implementation carries no compatibility shim
for intermediate pre-release encodings, and in particular the earlier
encoding that spelled the transport as a bare `mode` beside independent
optional `claimed_checksum`, `direct_put_content_ref`,
`provider_multipart_upload_id`, `multipart_part_size_bytes`, and
`staged_content_ref` fields does not decode.

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
| Grep root pointer | `grep_root` | JSON, uncompressed | 1 |
| Grep manifest | `grep_manifest` | JSON, uncompressed | 1 |
| Grep segment | none (section 4.2.2) | block sections, per-block zstd + CRC32C | 1 (via the grep manifest) |
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

`manifest_id` is `gmf_` followed by 32 lowercase hex characters, drawn fresh
for every candidate. It names *which object* holds the manifest and says
nothing about its contents: a content-derived id would make an identical
rebuild reuse the object an earlier publication left behind, and that reuse
is what would let collection race a publication for a manifest the winner is
about to point at. The bytes are bound to the pointer instead, through
`manifest_payload_checksum`. Namespace manifests carry no
grep pointer, watermark, lifecycle, or segment references. A fork therefore
starts without grep state until grep is enabled for the target.

`root.json` is a small mutable pointer envelope with these fields, in order:

- envelope: `kind = "grep_root"`, `format_version = 1`,
  `payload_checksum`, and raw JSON `payload`;
- payload: `namespace_id`, `manifest_id`, and `manifest_payload_checksum`,
  which must equal the named manifest envelope's own `payload_checksum`.

Each immutable manifest has the same envelope grammar with
`kind = "grep_manifest"` and `format_version = 1`. Its payload is the full
grep state: `namespace_id`, `lifecycle`, nested `index` bookkeeping, and the
`segments` descriptors. Both decoders verify the checksum over the exact
stored payload fragment before decoding, reject unknown versions and kind
mismatches without fallback, and validate namespace, lifecycle,
fold, run-allocation, and segment invariants at every boundary. A manifest
load additionally requires the loaded envelope's `payload_checksum` to equal
what the pointer promised, which is the same binding a metadata root holds
over its namespace manifest. The mutable
root-pointer decoder rejects unknown envelope and payload fields; the
immutable manifest decoder tolerates additive fields.

The nested `index` object carries its own `format_version`, currently `1`.
It holds what every phase has — the in-progress `reorganize` state and the
`next_run_ordinal` allocator — while each phase's own position lives in the
`lifecycle` tag beside it:

- `backfilling`: `target_seq` (the namespace sequence the pinned checkpoint
  captured), optional `cursor` (the inode the walk resumes strictly after),
  and `checkpoint_id`;
- `steady`: `built_through_seq` and an optional `next_event_index`;
- `disabled`: no fields, no segments, and no reorganization.

A phase carrying another phase's sequence is not representable. Before the
first stable release, this nested schema evolves in place at version 1; the
implementation carries no compatibility shim for intermediate pre-release
encodings, and a namespace can rebuild its derived index from a fresh
checkpoint.

A gram-index segment uses the section 4.2.1 block grammar unchanged —
prefix-compressed data blocks, one bloom filter block, one index block,
handles and checksums in the grep-manifest descriptor — with a grep-owned row
payload instead of metadata rows. The tokenizer, row shapes, and posting
encoding below are frozen by grep manifest format version 1; their evolution
follows the rules in section 4.3 and always permits rebuilding this derived
work (section 6.6).

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
implementation nudges its maintenance runner after any runtime publish that
observes the WAL tail at or past the WAL-tail policy's checkpoint threshold
(32 segments at defaults), and every publish surface rejects with
`maintenance_required` once the tail reaches the same policy's
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

A rebuild merges an oldest-first run of runs for one family group. It may
skip runs at the oldest end that are too large to read inside one step's
budget, and then it merges the runs above them; it never steps over a delta
run, so its output is always older than every delta run it leaves behind. A
rebuild that skipped runs drops nothing, because the rules below read across
the merged rows and a skipped run may hold the other half of a pair.

A base rebuild that starts at the group's oldest run drops rows that no
retained sequence can observe: bindings superseded or unbound at or below the
retention floor, spent unbind markers, and commit receipts below the floor.
The floor governs replay state only.
Revision rows are never dropped: file revision history is durable data,
retained in full regardless of the floor, and a revisions listing is always
complete. Tombstone rows — set and revoke events alike — and inode rows are
always retained for now; reachability-based dropping for them is future
work.

The `active-deletions` family holds current state rather than history, so the
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
manifest roots every object key its `metadata_files` list names. The pass also
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
   **reference manifest** dates. Call R the newest surviving manifest under
   the namespace's own prefix whose provider timestamp is at least `T` old:
   it is a durable snapshot of what the namespace referenced when the window
   opened. R roots what it names exactly as `metadata/root.json` does — its
   own key, its `metadata_files`, its content references, and every WAL
   segment above its `head_seq` — so an unreferenced object is deleted only
   when the current root set, R, and the object's own age all agree. A
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
3. **Delete-time re-verification.** Immediately before deleting, GC re-lists
   `checkpoints/`, re-reads the root, head, and floor, and drops from the
   batch anything reachable from that fresh root set. Candidate selection
   may be arbitrarily stale; deletion may not. On large batches the
   re-verification repeats at least every bounded number of deletion
   decisions, so no deletion consults an arbitrarily stale root set.
4. Roots: `metadata/root.json`; the reference manifest R (rule 1); active
   checkpoint records whose owner still
   stands — a user pin until its expiry passes, a fork pin until its target
   is provably gone (rule 10), whatever its lease says; and the visible chain from
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
5. A root the pass cannot resolve causes retention, not deletion: a root
   manifest that is absent, or that the store will not hand over,
   suppresses manifest and table deletion for the whole pass. A root that
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
   metadata tables and manifests, grep segments and manifests, and content
   blobs have keys that can only contain identical bytes under their
   create-if-absent, content-derived, or write-verification protocols. Once
   one is unreferenced and grace-aged, unconditional deletion is safe: a
   zombie retry can at most recreate identical, still-unreferenced bytes for
   a later pass. Content objects are never enumerated by listing the content
   store, which is shared by every namespace whose head names it; they are
   reached only through the upload session that owns them (rule 11).
10. **Abandoned forks.** A fork that crashes after writing its leased source
   checkpoint record but before installing its target head leaves that
   record with no target at all. Such a record is released once its lease
   has passed and the target head is still absent: the lease covers a whole
   fork attempt with margin to spare (section 3.9.2), so only an attempt
   that is really gone can have let it pass. This is the only debris an
   interrupted install can leave, and it lives on the source, never under
   the target's prefix — the target either has a complete head or has
   nothing.
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
   holds its admission directly instead of carrying a receipt, so nothing
   expires it; what bounds it is the same grace, and a host that stages
   content and publishes it much later has to publish inside that grace for
   the same reason a remote client has to re-read its session for a fresh
   receipt. The reasoning above assumes a content object is referenced
   only by the namespace whose session created it and by fork descendants
   reading through a pinned basis; a same-content-store copy across
   namespaces (section 2.8) would have to root the reference on the source
   side the way a fork does.

Deletion proceeds data first, records last, so a crash mid-sweep leaves
orphaned data for the next pass rather than a record whose data vanished.
To keep that true, every readable checkpoint record roots its basis for the
duration of a pass, whatever its lifecycle, expiry, or owner — no exceptions.
State, expiry, and owner fate gate only whether the record itself is a
candidate. A fork-owned record whose target namespace is verifiably
terminally deleted (target head `state = deleted`, re-checked at delete time)
or provably abandoned (rule 10) is released by compare-and-swap on the
record's freshly observed etag — never deleted while active. A released
record is deleted outright once its `released_at_ms` is a grace window old;
because release is terminal, no second state is needed between deciding to
delete and deleting, and a crash between the release CAS and the delete
leaves a record the next pass reaps unconditionally.
The intended end-state remains tracked deletion derived from manifest
predecessor diffs, with the listing sweep demoted to a low-frequency
backstop.

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

An attribute value is one of two kinds:

| Kind | Meaning |
| --- | --- |
| **string** | One UTF-8 text value. |
| **string_list** | An ordered list of UTF-8 text values. The order is the order the writer supplied. |

Five named format constants bound every map. Every size is counted in logical
UTF-8 bytes — the bytes of the text itself — so no encoder's framing changes
what a namespace may hold:

| Constant | Value | Bound |
| --- | --- | --- |
| `MAX_ATTRIBUTE_KEY_BYTES` | 128 | Longest attribute key. |
| `MAX_ATTRIBUTE_VALUE_BYTES` | 4,096 | Longest string, and longest member of a string list. |
| `MAX_ATTRIBUTE_LIST_MEMBERS` | 256 | Most members in one string list. |
| `MAX_ATTRIBUTE_ENTRIES` | 100 | Most entries in one map. |
| `MAX_ATTRIBUTES_TOTAL_BYTES` | 65,536 | Largest map, counting every key's bytes plus every value's bytes, with each list member counted. |

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

The semantic creation marker in the core model is the create commit in
namespace history, not a wall-clock field. Every WAL commit record, commit
receipt row, and revision row carries a required `committed_at_ms`: the
wall-clock stamp of the publishing writer's request context, in Unix
milliseconds. The stamp is observational and non-semantic — sequences are
the only ordering and validity inputs, the fingerprint preimage excludes
it, and correctness never depends on clocks being aligned. Surfaces expose
it as modification metadata (a file's modified time is the stamp of the
commit that created its current revision).
