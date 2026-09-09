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
A named optional object member is omitted when absent.
`status` names a resource's lifecycle and `phase` names a computation's progress;
both use `kind` as the discriminator when tagged.

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
| **Compare-and-swap update** for small mutable objects | Checkpoint and upload records must be updated safely in the presence of concurrent writers. |
| **Full-object reads with identity metadata** | Mutable control-object readers must receive object bytes and the opaque compare token for those same bytes from one read operation, so one observation's payload cannot be paired with another observation's compare token. |
| **Strong consistency** | A successful put/delete operation must become authoritative immediately after it succeeds. |
| **Prefix enumeration** | WAL segment discovery for reclamation and general namespace inspection need a reliable way to enumerate objects by prefix. Listings return keys in ascending lexicographic order; the conformance probes assert this. |
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
| **WAL segments** | Immutable | Commit contiguous records by number, or fence a writer with zero records. Carry `wal_no`, `writer_epoch`, `base_head_seq`, `start_seq`, `end_seq`, `next_inode_id`, and `records`. | `namespaces/{namespace_id}/wal/{wal_no:020}.wal.zst` |
| **Namespace manifests** | Immutable | Record identity (`namespace_id`, `content_store_id`, `created_at_ms`, `fork_basis`), `status`, writer authority (`writer_epoch`, `writer`), `compactor_epoch`, `manifest_no`, `head_seq`, `head_commit_id`, `next_inode_id`, `base_seq`, `next_run_no`, `runs`, `last_folded_wal_no`, `retention_floor_seq`, and `retention_floor_wal_no`. Each segment reference keeps its owner. | `namespaces/{namespace_id}/manifests/{manifest_no:020}.json` |
| **Pin records** | Create and delete; snapshot expiry may extend | Pin one numbered manifest and its runs for a user, snapshot, or fork target. | `namespaces/{namespace_id}/pins/{pin_id}.json` |
| **Metadata segments** | Immutable | Store metadata rows referenced by manifests. Segments may be owned by the namespace itself or by a fork source namespace. | `namespaces/{owner_namespace_id}/segments/{segment_id}.sst.zst` |
| **Upload sessions** | Mutable lifecycle | Track one staged-content upload. The record's `status` is monotonic: a session is created `open` under a lease, and moves once to `completed` or `aborted`, both terminal. | `namespaces/{namespace_id}/uploads/{upload_id}.json` |
| **Hint** | Mutable | Starts forward discovery of numbered manifests and WAL objects; never authority. | `namespaces/{namespace_id}/hint.json` |
| **Content store descriptors** | Immutable | Identify the content domain held by a backend. | `content-stores/{content_store_id}/store.json` |
| **Content objects** | Immutable | Store one file revision's complete bytes. | `content-stores/{content_store_id}/objects/{owner_namespace_id}/{content_id[4..6]}/{content_id[6..8]}/{content_id}` |

`.sst.zst` identifies the block encoding: sorted rows with each block compressed using zstd. Metadata and grep segments use this encoding. `.wal.zst` identifies the WAL encoding.

WAL number 2 in namespace `demo` is stored at
`namespaces/demo/wal/00000000000000000002.wal.zst`.
The number identifies the object. There is no separate segment id or pointer.

These key shapes are the interoperable storage contract. Private objects
must not collide with these families. Core GC never recognizes objects under
`namespaces/{namespace_id}/extensions/`.

Forks copy run references into the target's manifest. Each reference keeps
its `owner_namespace_id`; its segment is read under that owner's prefix.
The source's fork-owned checkpoint protects the copied files.
Namespaces share a content store exactly when their manifests name the same
`content_store_id`.

### 1.3 Durable naming conventions

- `hint.json` is the namespace's only singleton. Installation uses
  put-if-absent; later updates are compare-and-swaps that only raise its
  numbers. GC never sweeps it.
- Manifest numbers start at 1. The highest contiguous number is current.
  Readers probe forward from the hinted number.
- WAL numbers start at 1 independently in every namespace. A segment's
  `wal_no` must equal its twenty-digit name. Readers probe consecutive
  numbers until the first missing object.
- Pin ids use `pin_{manifest_no:020}-{16 lowercase hex}`. The positive
  manifest number positions the id; the random suffix distinguishes pins
  over that manifest. The number is in the public ordinal range. Ids are
  never reused.
- GC lists `manifests/`, `wal/`, `segments/`, `pins/`, and `uploads/`.
  Metadata segment ids remain generated identities.
- Creation and forks write the content-store descriptor, hint naming
  manifest 1 and WAL 0, and manifest 1, in that order. Manifest 1's
  put-if-absent decides whether the namespace exists.
- Deletion and retirement publish successive manifests. The current
  manifest survives GC as the tombstone that permanently retires the id.

Envelopes verify namespace, number, family, checksum, and sequence fields.
Object listings do not determine history. A missing hint means the namespace
is not installed; readers report not found and nothing lists to find a basis.

Commit ordering and fencing depend on numbers and writer epochs.
Publication budgets use local monotonic elapsed time. Object reclamation
uses the bounded clock error and grace windows in section 6.4.

### 1.4 Manifest update authority

The current manifest is the authority for identity, status, and writer epoch.
Every successor preserves `namespace_id`, `content_store_id`, `created_at_ms`,
and `fork_basis` verbatim. Deleted status is terminal. An established
`reclaim_after_ms` is never cleared or moved. Publication checks these rules
before writing.

Writer acquisition publishes the next manifest with `writer_epoch + 1` and
its writer block, then puts a zero-record fence at the next WAL number.
Semantic mutations commit through the next WAL number under that epoch.

Flush, reorganization, compaction, and retention publish the next manifest
number. They preserve writer authority. A lost put loads the winning
manifest and either accepts its coverage or rebuilds against it. Coverage
includes the folded WAL number, even when a fence leaves the sequence unchanged.
Compaction also checks the manifest's compactor epoch.
Namespace deletion uses the acquired writer epoch and publishes terminal
status in the next manifest.

### 1.5 WAL segment rules

1. Each accepted client request has its own logical commit record and `seq`.
2. A data segment holds one or more records with contiguous sequences.
   It records the prior `base_head_seq`, its first and last sequence,
   writer epoch, WAL number, and allocation high-water mark `next_inode_id`.
3. Put-if-absent of the next WAL number is the commit and visibility point.
   A failed precondition writes nothing. The loser discovers the new tip,
   re-plans, and retries at the next number.
4. Numbers are contiguous from 1. Discovery stops at the first 404. Required
   objects between the manifest's folded number and the observed tip must
   exist and verify. There are no orphan proposals or predecessor pointers.
5. A zero-record segment is a fence. Its `start_seq`, `end_seq`, and
   `base_head_seq` are equal; `next_inode_id` is unchanged. Replay skips it.
   A stale writer collides with the new writer's numbered fence, discovers
   the higher epoch, and returns `writer_fenced` without another write.
6. Epochs never decrease along WAL numbers and never exceed the current
   manifest's writer epoch. A reader racing an epoch publication reloads
   the manifest before treating a higher WAL epoch as corruption.

### 1.6 Immutable content rules

The content model has seven rules.

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
7. A namespace owns the new content it creates. Forks preserve the ownership
   of inherited content. Sharing an ancestor does not make siblings retain one
   another's private content. A reference names the namespace that originally
   wrote the bytes in `owner_namespace_id`. Forking, restoring, replaying, and
   compacting never substitute the current namespace.

Recording the owner reclaims nothing by itself. Section 6.4 states what
garbage collection may delete.

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

A reader or writer resolves content through the namespace manifest:
`namespace_id -> manifest.content_store_id`, then
`content_ref -> owner_namespace_id + content_id -> one exact key`.
Reading inherited content does not load its owner's manifest or walk ancestry.
File revisions and change-feed payloads store only `content_ref`; they do not
store content-store ids or object-store paths.

### 1.7 Mutable control-object rules

Upload lifecycle changes and snapshot expiry extensions use compare-and-swap. The discovery hint is
raised by compare-and-swap; each of its numbers only increases. Registered kinds are `hint`,
`checkpoint_record`, `upload_session`, and `content_store`.

`hint.json` has kind `hint`, version 1, and strict payload
`{ namespace_id, manifest_no, wal_no }`. Installation names manifest 1 and
WAL 0. A missing hinted manifest 1 means the namespace does not exist yet.
A missing higher hinted manifest is corruption: a hint never runs ahead of
publication. Manifest number zero is invalid.

Readers load the hinted manifest and probe successive manifest numbers
until 404. They then load the WAL number in the hint when it is above the
manifest's folded number, and probe forward from the greater of those two
numbers until 404. A lagging hint is normal. The WAL write stop bounds the
unfolded tail. After probing WAL, readers check for a successor to the
selected manifest and reload if it appeared. A concurrent retention advance
cannot make a reclaimed WAL number look unused. Required missing or corrupt
objects fail closed.

A manifest publisher raises the hint within its publication budget. A
writer raises the hint's WAL number after each WAL put and before the
batch is acknowledged, so a reader polling the hint sees the commit at
once. A raise compare-and-swaps the greater of each number from the token
of the raiser's last write, reading the hint only when that token is stale,
so no actor lowers what another wrote and a writer's steady state is one
request. A raise never fails a commit. A writer whose raise finds a newer
manifest number has learned of a publication and reloads its view. A
freshness poll is HEAD on the hint.

Durable decoders reject unknown envelope and payload fields. A changed
shape requires its golden fixture to change. These format families are
version 1.

A pin has kind `checkpoint_record`, version 1, and these payload fields:
`namespace_id`, `pin_id`, `manifest_no`, `manifest_head_seq`,
`manifest_payload_checksum`, `head_commit_id`, `created_at_ms`, and `owner`.
The namespace and pin id must match the key. The manifest number must match
both the id and the referenced manifest. Creation is put-if-absent. Release
deletes the record. A missing pin cannot be released again.

The owner is `user` with `name` and optional `expires_at_ms`, `snapshot` with
`name` and required `expires_at_ms`, or `fork` with `target_namespace_id`.
User and snapshot expiry permits deletion after a grace window. A user pin
without expiry remains until explicit release, except on a deleted namespace.
Deleted namespaces collect user and snapshot pins after creation grace.
Fork collection follows section 2.6. A pin has no status, release timestamp,
or fork lease. Snapshot extension changes only its expiry by compare-and-swap.

A snapshot or checkpoint read derives the manifest number from the id and
loads that numbered manifest under the namespace. It reads the pin body to
confirm existence and owner validity, and verifies the manifest reference.

A fork basis stores this reference under `manifest`:

```json
{
  "owner_namespace_id": "demo",
  "manifest_no": 2,
  "manifest_head_seq": 17,
  "manifest_payload_checksum": "sha256:<64 lowercase hex>"
}
```

`owner_namespace_id` identifies the namespace that stores the manifest and its segments. `manifest_no` is its number and determines its object key, and `manifest_head_seq` is the greatest namespace sequence it contains. `manifest_payload_checksum` must match the referenced envelope. This shape rejects unknown fields. Pin records store the manifest fields directly.

A pin references a manifest under its own namespace. A fork basis references a different namespace. Violations return `namespace_corrupt`.

Grep manifests have no logical position or head sequence, so grep root pointers use the smaller shape defined in section 4.2.2.

Readers of small mutable control objects must use a full-object read that
returns bytes and the object identity metadata for those same bytes. This does
not by itself guarantee freshness; it guarantees self-consistency. A reader
must not separately load identity metadata and bytes, then use the identity
from one observation with the payload from another observation.

The manifest is the durable writer authority. A writer acquires lazily,
publishes its new epoch and writer block, then writes the fence before
accepting batches. Every acquisition advances the epoch. The block contains
`writer_id` and `acquired_at_ms`; neither its label nor a clock determines
commit validity. A fenced session never reacquires on its own.

The WAL publication budget is 60 seconds from observing the tip used to
plan a batch until initiating its numbered PUT. A cached tip has the same
bound. An expired attempt reloads and re-plans; it does not write a proposal.
Content proof checks run immediately before the put.

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

Each namespace has numbered manifests from birth, an ordered numbered WAL,
zero or more checkpoints, and a retention policy. The current manifest
records identity and writer authority. The highest WAL above its
`last_folded_wal_no` supplies the live sequence and allocation high-water
mark; otherwise the manifest supplies them.

The canonical identity of an item is `(namespace_id, inode_id)`.

Each namespace has exactly one immutable `content_store_id`, recorded in its
manifest. The content store is an immutable pool for file bytes and may be
referenced by many namespaces. A new root namespace mints a fresh content
store id by random generation; a forked namespace copies the source
namespace's id while starting an independent namespace metadata history,
which is what makes forks copy-on-write over the same bytes.

The manifest stores the namespace's creation time in `created_at_ms`. This value
never changes. A new namespace uses its bootstrap timestamp. A fork uses the
time the target namespace was created, not the source namespace's creation
time. An empty manifest represents the built-in root inode at sequence zero,
using this value as its creation time.

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
    "owner_namespace_id": "demo",
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
choice. Every name key is derived from its display name by normalizing to NFC,
applying full Unicode default (non-Turkic) case folding, then normalizing to NFC
again. Both steps use Unicode 17.0.0 data: `icu_normalizer` 2.1.1 and
`icu_casemap` 2.1.1 use compiled data from `icu_normalizer_data` 2.1.1 and
`icu_casemap_data` 2.1.1. Both data crates identify ICU `release-78.1rc` as
their source. ICU 78 uses Unicode 17.0.0.

Both admission and lookup use this rule. The display-name and name-key corpus
in `crates/loonfs-api/tests/golden/name_folding.v1.json` pins its mappings and
directory collisions. A dependency update that changes a mapping is a
format-semantic change under section 4.3, even if no field or encoding changes.
There is one rule, no per-namespace selector or manifest field, and no rewriting of
stored keys. A namespace written before a rule change keeps its stored keys;
this is acceptable before the format is released.

### 2.4 Files and revisions

A file is represented by one inode and a sequence of immutable revisions.

Each revision stores exactly one immutable `content_ref`. In v0, that
reference names one whole-file object containing the complete plaintext file
bytes. Revisions do not store object-store paths or `content_store_id`;
readers resolve those through the namespace manifest when bytes are needed.

Content objects belong to the namespace's content store. A file revision may
reference only content that is durable under the content store named by that
namespace's manifest.

LoonFS therefore uses a two-stage write model:

```text
make content durable  ->  then make metadata visible
```

This separation is part of the core model.

#### 2.4.1 Immutable content storage

Each content domain has a `content_store` version 1 control envelope at
`content-stores/{content_store_id}/store.json`. Its strict payload is
`ContentStoreState { content_store_id, created_at_ms }`, with `created_at_ms`
an unsigned 64-bit Unix-millisecond creation timestamp. Namespace creation
writes it with create-if-absent before installing the hint and manifest 1.
Fork installation attempts the same descriptor put for the shared domain.
An occupied descriptor key is allowed. The descriptor is immutable and
never collected. An abandoned attempt may leave an unused descriptor.
No reader consults it today. A deployment resolving a content domain to a
physical backend may read it to confirm that backend holds the domain.
The key is beside `objects/`, so content listings under `objects/` exclude it.

New uploads for a namespace are owned by that namespace inside the content
domain its manifest names.

The immutable content key is:

```text
content-stores/{content_store_id}/objects/{owner_namespace_id}/{content_id[4..6]}/{content_id[6..8]}/{content_id}
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
  manifest to its `content_store_id`, then uses the reference's owner and content id;
- future content strategies must use a new `content_ref.kind` and name their
  durability and validation rules before revisions may reference them.

`ContentRef` rejects unknown fields in every context, including immutable
durable records. Unknown `kind` values and references without
`owner_namespace_id` fail to decode. A new content kind requires a version
change on every durable family that carries references:

```json
{
  "kind": "blob_v1",
  "owner_namespace_id": "demo",
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

An upload session's staged content reference applies only while the session is
open. Completion clears service-proxied staging to `idle` in the same
conditional write that sets `status` to `completed`. The completed status is
the record's only content description. Staging carries no content information
after completion. Abort also clears service-proxied staging to `idle`.

A completed or aborted record that retains a staged content reference is
corrupt, even if that reference agrees with the completed reference. The mode
continues to identify how the content was produced. Direct modes retain their
provider state for cleanup and retries. Upload sessions remain at format
version 1.

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
Only the tagged `set` variant includes this field. The tagged `revoke`
variant has no binding field, and a reader rejects a `revoke` carrying
`deleted_direntry`. A partial binding is not valid.

The `active_deletions` family tracks deletions that can still be restored. A
`set` tombstone creates a `listed` row keyed by `(deletion_seq,
root_inode_id)`. The API exposes `root_inode_id` as `inode_id`. The row also
copies `deleted_by`, `deleted_at_ms`, and `deleted_direntry` from the tombstone event.

A `revoke` tombstone creates a `removed` row with the same key and records the
revoke's sequence as `revocation_seq`. `removed` sorts before `listed`, which
lets reorganization discard both rows together.

The recoverable set is read as a range scan in deletion order. Tombstone rows
remain authoritative because this family is derived from them.

Every namespace manifest records `status`: `active` or terminal `deleted`.
The field is required. Unknown status fails closed.

Deleting a namespace publishes the next manifest under the acquired writer
epoch with `status: {"kind":"deleted"}`. The manifest put is the deletion
point. It records the final sequence, commit id, and next inode id, so these
survive WAL reclamation. Its runs and folded WAL number remain the last
materialized file set. Operations observing deletion return
`namespace_deleted`. Previously acknowledged commits remain committed.

GC keeps the current manifest permanently to prevent reuse of the id.
A deleted namespace protects no WAL or current runs. Active checkpoint
records still protect their pinned manifests and segments, including files
used by forks. Namespace deletion does not delete its shared content store.

A deleted manifest with no `reclaim_after_ms` retains dependencies.
Retirement publishes a successor with a fixed deadline once no retained
view or dependent fork remains. Every later version preserves that deadline
verbatim and stays deleted. After the deadline, GC may sweep the retired
namespace's content owner prefix. Retirement itself deletes no content.

### 2.6 Forks

A fork starts independent metadata history in the source's content store.
It first creates a verified fork-owned source pin. It then
installs its own manifest 1 with the pinned source manifest's runs verbatim.
Every segment reference keeps its owner, including earlier ancestors.
New content and metadata segments use the target's own prefix.

`fork_basis` is immutable provenance in the target manifest. It holds the
source manifest reference and source checkpoint id. Readers never follow it.
The source collector uses the fixed call clock to apply these rules:

| Fork pin and target | Source pin action |
| --- | --- |
| Pin younger than `grace_window_ms` | Retain without reading the target. |
| Aged pin, absent target hint | Delete. |
| Aged pin, target manifest names the exact source, pin id, and manifest reference | Retain, regardless of target status. |
| Aged pin, target manifest has no fork basis or names another pin | Reclaimable: the pin belongs to an abandoned attempt that a later install superseded. |
| Aged pin, target manifest names this pin with another source or manifest reference | Corruption. |
| Aged pin, unreadable target | Retain. |
| Aged pin, invalid target | Corruption. |

The source reads the target through current-manifest discovery, which reads
its hint and numbered manifests. It never reads the target's WAL. The grace
is the installation margin. An absent target gets no tombstone.

A retired target deletes the source pin named by its current manifest's
`fork_basis` once its retirement deadline passes, in the same step that
sweeps its owned content. Every later pass repeats the deletion. The
permanent tombstone retains the pin id, so a failed delete is retried.
A deleted target without retirement leaves its source pin in place.

For `A -> B -> C`, C's pin prevents deleted B from retiring. A retains B's
pin until B's collector releases it. After C retires and its deadline
passes, C deletes its pin on B. B can then retire after a complete pin
sweep. Release proceeds from descendants to ancestors. Metadata compaction
preserves content references to their original owners, so rewriting metadata
runs alone does not release a source pin.

An unflushed fork can itself be forked. Its manifest already lists its
inherited files. Target reads never access source hints or source manifests
after installation. Source deletion leaves the pinned files readable, and
later source changes do not affect the target.

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

Readers load the hint, discover the current manifest, and probe the
numbered WAL tip. They read the manifest's runs and replay WAL numbers
`(last_folded_wal_no, tip]` in order. Empty manifests contribute exactly the
built-in root inode row at sequence zero.

The manifest carries immutable identity, terminal status, writer authority,
and the folded head summary. Data WAL segments supply `end_seq`, the last
record's `commit_id`, and `next_inode_id`. A fence changes only the WAL number
and epoch. When the tip consists of fences, readers find the preceding data
record or use the manifest's commit id.

Every successor preserves identity and never lowers its head sequence,
writer epoch, folded WAL number, or either retention floor. The retention
floor begins at zero for creation and at the pinned source sequence for a
fork. A retention advance records the current manifest's head sequence and
`last_folded_wal_no` as `retention_floor_seq` and `retention_floor_wal_no`.

A checkpoint pins one numbered namespace manifest under `pins/`. Its record
follows section 1.7 and does not affect current visibility. Each create uses
a fresh positioned id, even when the owner and manifest are unchanged.

Creation writes the pin and loads the current manifest within the verify
budget. If the namespace is deleted or its retention floor has passed the
pinned manifest's head sequence, the creator deletes the pin and returns
`checkpoint_unavailable`. A store error or an exceeded budget also deletes
the pin. Verification does not reload the pinned manifest. A concurrent
floor advance after verification leaves the pin protected under rule 8.

A snapshot requires an unexpired snapshot owner for reads and extension.
An expired snapshot stays a GC root until collection deletes it after expiry
plus grace. A user pin
remains readable while it exists, including after its expiry. Release
checks the owner and deletes the pin; a later release returns not-found.

A namespace manifest is the durable object for one namespace file-set version.
It may reference zero or more immutable metadata runs; standalone checkpoint
records under `pins/` pin manifest versions for retention, fork, or
stable read workflows. Each run is internally segmented without overlapping segment
key ranges; different runs may overlap and readers apply the normal metadata
visibility rules across all referenced runs. Within a segment, rows are stored
in ascending row-key order (adjacent equal keys permitted); readers reject a
segment whose rows are out of order as malformed. Readers load the referenced
runs, then replay WAL numbers above the manifest's `last_folded_wal_no`.

The WAL preserves commit order even when a segment contains several commits.
Each commit stores `commit_id`, `semantic_commit_fingerprint`, `committed_by`, `committed_at_ms`, an optional `message`, and its metadata changes. The actor reference contains a `kind` and an `id` and is stored once per commit. Validation inputs and operation results are not stored. Checkpoints keep replay work bounded as history grows.

#### 2.9.1 Resolving the metadata basis

The basis is always the current manifest under the namespace's own prefix.
It exists from namespace installation. A manifest with no runs represents
exactly the built-in root inode at sequence zero.

Run references name their own owners. A reader derives each segment key
from that owner and validates its envelope and checksums. `fork_basis` is
provenance and checkpoint identity only; it never selects a read basis.
There is no source-manifest read after fork installation.

## 3. Write and read protocol

### 3.1 Write protocol

A write stages content when needed, reconstructs and validates metadata,
and puts the next numbered WAL object with create-if-absent. That put
commits the batch. Tentative acceptance into a batch is not success.

#### 3.1.1 Content staging

Content must be durable before any metadata change can reference it.

1. Allocate a fresh `content_id`. This happens before any byte is read, so
   the final object key exists up front.
2. Read the namespace manifest for its `content_store_id`.
3. Upload the complete byte sequence to
   `content-stores/{content_store_id}/objects/{owner_namespace_id}/{content_id[4..6]}/{content_id[6..8]}/{content_id}`
   with create-if-absent semantics.
4. Build a content reference:

   ```json
   {
     "kind": "blob_v1",
     "owner_namespace_id": "demo",
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
using section 3.2.1: discover the current manifest and numbered WAL tip,
then project the required WAL records. The server never
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

#### 3.1.4 WAL segment publication

1. Collect requests and evaluate them in order against the current view plus
   earlier accepted requests in the batch. Resolve known commit ids first.
2. Reject new primaries with `maintenance_required` when
   `tip - last_folded_wal_no >= MAX_UNFLUSHED_WAL_SEGMENTS`. Fences count.
3. Assign contiguous sequences to accepted requests. Build one segment at
   `tip + 1` with their records and resulting `next_inode_id`.
4. Check the monotonic publication budget and content proofs immediately
   before putting that segment with put-if-absent.
5. A successful put commits the batch. Update the in-process tip and read
   state. Raise the hint's WAL number, then acknowledge.

A precondition failure writes nothing. Probe forward, detect fencing,
re-plan, and retry. Other definite failures create no WAL object. An
unobserved transport outcome is `commit_outcome_unknown`: the numbered put
may have landed. Retry with the same commit ids to resolve receipts.
Requests rejected before publication receive no sequence or WAL record.

#### 3.1.5 Failure semantics inside a publication batch

A publication batch is not an all-or-nothing multi-client transaction.

The server may:

- reject some candidate requests before publication; and
- tentatively accept other requests into the same batch and, if publication
  succeeds, publish them in the same WAL segment.

Each request still has its own success or failure outcome. Tentative
acceptance inside a batch is not success.

A rejection judged against ephemeral state advanced by a tentative
acceptance (step 1 in section 3.1.4) is contingent on that state publishing:
if publication fails, the writer must report the publication failure for it,
never the semantic rejection. Rejections judged against the durable metadata view
alone stand regardless of the publication outcome.

### 3.2 Read protocol

A read reconstructs the visible filesystem state from durable artifacts on
object storage. No server-side cache or local database is required for
correctness; everything needed is in the object store.

#### 3.2.1 Metadata view loading

The reader loads the hint and current manifest, then discovers the numbered
WAL tip. The manifest supplies identity and the file set. Its runs supply
materialized rows, or its empty run list supplies the root inode at zero.
Replay verifies all required WAL numbers after the folded number through
the discovered tip. Records advance the metadata rows; fences do not.

The result is pinned to one sequence. Point reads and directory listings
may query verified segments and the projected WAL tail directly. Missing or
corrupt required objects are errors. A shared writer/read runtime can seed
its read caches directly from an acknowledged batch without a store request.
Subsequent freshness polls HEAD the hint.

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
2. Read the namespace manifest for its `content_store_id`.
3. Verify that `content_ref.kind` is supported by the reader.
4. For `blob_v1`, fetch the object at
   `content-stores/{content_store_id}/objects/{owner_namespace_id}/{content_id[4..6]}/{content_id[6..8]}/{content_id}`.
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

One numbered WAL put may publish one or more contiguous logical commits.

A logical commit is visible after its numbered WAL put succeeds. The derived head is at or
beyond that commit's `seq`, and its WAL number is committed.

This gives each successful request one `seq` and one replay identity without
requiring a separate object write per request.

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

When non-empty, `assertions` follows `message` in the preimage, in request order.
Each assertion uses its request serde encoding, with `kind` followed by these fields in order:

| Kind | Fields after `kind`, in order |
| --- | --- |
| `namespace_head` | `expected_head_seq` |
| `file_revision` | `inode_id`, `expected_revision_no` |
| `binding` | `path`, `expected_inode_id`, `expected_binding_generation` |
| `attributes` | `inode_id`, `expected_attributes_revision_no` |

Assertion inode IDs use public `ino_` strings. Sequence and revision numbers
are JSON integers. Paths use their validated absolute form. Binding generations
are opaque strings. Absent optional assertion fields are omitted.
An empty assertion list is omitted, so
assertion-free requests retain their existing fingerprints. Assertions add no
WAL field or delta and are not evaluated during replay.

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

The owner is excluded because the random `content_id` is unique across owners.
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
- publishing successful logical commits by putting the next WAL number
  with put-if-absent.

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

Readers map a sequence to WAL numbers by reading segment headers forward
from `retention_floor_wal_no`, through the retained WAL. Fence segments
produce no change events.

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
target from the current manifest and verifies that
every metadata segment that basis references still exists before the floor
moves. The probe is advisory — the atomic guarantee is the garbage
collector's obligation to never remove reachable objects ("Garbage
collection") — but a segment that already disappeared must block the floor
while replay can still rebuild the lost state. Corruption discovered after
advancement is caught by read-path checksum validation.

Advancement publishes the next manifest number with the same runs, head
summary, and allocators. Its number, `retention_floor_seq`, and `retention_floor_wal_no` advance.
The new floors are the verified predecessor's `head_seq` and
`last_folded_wal_no`. Neither decreases.
Being below the floor makes an object a deletion candidate; deletion also
requires the GC checks. If the floor passes a pin's manifest head sequence,
retention wins (section 6.4).

A WAL flush materializes the current durable namespace
file-set version by folding numbers `(last_folded_wal_no, tip]`. It publishes
the next manifest with `last_folded_wal_no = tip`. A fence folds even when
the head sequence is unchanged. The flush is the latest-state
maintenance operation and creates no checkpoint record; a superseded manifest
becomes a garbage-collection candidate once nothing pins it.

Every metadata publication — WAL flush and reorganization alike —
self-enforces the metadata publication budget, measured from before its
first segment object is written until its manifest put-if-absent is initiated.
A publication that exceeds the budget aborts without publishing: its
immutable outputs stay unreachable and are reclaimed by garbage collection
after the grace window. This bound (with the WAL publish budget for
commits) is what makes the GC grace window's floor derivable ("Garbage
collection", rule 1); maintenance therefore needs no durable build-intent
protocol.

Creating a checkpoint pins one such manifest version deliberately for one
owner. It first flushes the WAL tail as above, then writes
`pins/{pin_id}.json` under a freshly generated id and verifies the basis
after the write, deleting the record on failure. A retry uses a new id.
A live manifest does not need to be checkpoint-pinned; checkpoint records
explain why a manifest version must be retained after a successor is published.

### 3.9 Namespace creation and forks

Manifest 1's put-if-absent installs the namespace. Deletion and retirement
publish successive manifest numbers. There is no intermediate namespace
status. An abandoned hint naming absent manifest 1 means no namespace exists.

#### 3.9.1 Creating a namespace

An existing active namespace returns `namespace_exists`, or its current
summary when `allow_existing` is set. Deleted status returns
`namespace_deleted`. Corruption and read failures are never absence.
These completed-namespace checks write nothing.

Build manifest 1 with the new namespace id, a generated content-store id,
creation time, no fork basis, and active status. Set `head_seq`, `base_seq`,
both retention floors, `last_folded_wal_no`, `next_run_no`, and both epochs
to zero. Use the genesis commit id, the next inode id after the root, no
writer block, and no runs.

Put the content-store descriptor, hint `{namespace_id, manifest_no:1,
wal_no:0}`, and manifest 1 with put-if-absent, in that order. Descriptor and
hint collisions are allowed. Only manifest 1 decides the namespace race.

#### 3.9.2 Forking a namespace

1. Create a verified fork-owned checkpoint at the source head, or pin a live
   snapshot's manifest number and commit id. Its owner names the target.
2. Read and verify the pinned manifest. After the fork checkpoint is
   durable, recheck a selected snapshot. A lost snapshot releases the fork
   pin and returns `snapshot_gone` before target installation.
3. Copy the source runs verbatim into target manifest 1. Copy its head
   sequence, head commit id, next inode id, next run number, and content-store
   id. Keep each segment owner. Set target identity, creation time,
   provenance, active status, no writer, and both epochs zero. Set
   `last_folded_wal_no` and `retention_floor_wal_no` zero and
   `retention_floor_seq` to the target's birth sequence.
4. Check the elapsed installation budget. The GC grace reserves time for
   the provider operation and clock allowance. Failure stops before installation.
5. Put the descriptor, target hint naming manifest 1 and WAL 0, and target
   manifest 1, in that order. Descriptor and hint collisions are allowed.

The target starts its WAL numbers at 1. Its first data commit is one sequence
above the fork point. The fork copies no content or metadata segments.
Its own manifest lists everything it reads, so it can be forked immediately.
Fork pins have no lease and are never renewed. The grace from `created_at_ms`
is the install margin. `FORK_INSTALL_BUDGET_MS` reserves the provider deadline,
attempt timeout, and clock allowance within the minimum grace window.

#### 3.9.3 Conflicting installs

A manifest 1 put that loses reads manifest 1 back and verifies its namespace
identity. The current manifest's active status returns `namespace_exists`;
deleted returns `namespace_deleted`. Invalid bytes or disagreement with the
key's namespace is corruption. No losing install overwrites the winner.

A confirmed precondition failure remains a conflict. An unacknowledged put
can confirm success only by reading back the exact proposed manifest 1.
An explicit `allow_existing` retry may return the existing active namespace.

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

Storage formats and protocol bindings are versioned separately.

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

The namespace manifest is authoritative for
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
| Metadata segment | none (section 4.2.1) | block sections, per-block zstd + CRC32C | 1 (via namespace manifest) |
| Grep root pointer | `grep_root` | JSON, uncompressed | 1 |
| Grep manifest | `grep_manifest` | JSON, uncompressed | 1 |
| Grep segment | none (section 4.2.2) | block sections, per-block zstd + CRC32C | 1 (via the grep manifest) |
| Namespace manifest | `namespace_manifest` | JSON, uncompressed | 1 |
| Hint | `hint` | JSON, uncompressed | 1 |
| Checkpoint record | `checkpoint_record` | JSON, uncompressed | 1 |
| Upload session | `upload_session` | JSON, uncompressed | 1 |
| Content store descriptor | `content_store` | JSON, uncompressed | 1 |

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

A descriptor does not store an object key. Readers derive its key from
`owner_namespace_id` and `segment_id`: `namespaces/{owner_namespace_id}/segments/{segment_id}.sst.zst`.

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
| `content_publications` | `content-publication-{content_id}-{committed_seq:020}` | `content-publication-{content_id}` |
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
| `content_publications` | `content_publications` |
| `attributes` | `attributes` |

`direntry_binds` and `direntry_child_binds` store the same `direntry_bind` rows under different keys. The single `revisions` family stores `file_revision` rows. A row key therefore depends on both the row and its family.

A `content_publication` row stores `content_id`, `committed_seq`, and
`delta_index`. Each WAL delta that publishes a file revision also emits its
publication row. Repeated references to the same content within one commit
share one publication row with the first publishing delta index. The content
id is stored directly in the key. The suffix uses the commit-receipt sequence
grammar. Publication rows survive every
rebuild at every floor, as file revisions do. A lookup checks in-memory rows
before probing this family in the manifest with its Bloom filter key.

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
what the pointer promised. Checkpoint and fork references bind namespace
manifests by the same checksum rule. Both root pointers and immutable manifests reject
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
  A version governs safe interpretation and operation, including collection
  protocols, not only field layout.
- **Every accepted field is understood.** A supported durable family version
  understands every authoritative field it accepts. Readers reject unknown
  envelope and payload fields at every level of nesting. New durable meaning
  requires a supported version change for the owning family.
- **Post-release changes require a new version.** After the first stable
  release, adding, renaming, removing, retyping, or re-tagging any field, changing
  the payload encoding, or changing an operation that the version governs
  requires a new `format_version` for the owning family.
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
- **New content kinds require family version changes.** A new `content_ref.kind`
  arrives with a version change on every durable family that carries references.
  Readers reject unknown kinds during decoding, as they reject unknown checksum
  algorithms. No reader preserves or creates a reference it cannot interpret.
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
surface refuses new commits with `maintenance_required` once the tail reaches
the same policy's write-rejection threshold (128 at defaults). A commit id the
namespace already knows is still answered from its receipt. Reads never gate on
tail length. Bounded reads are the
automatic half only: the retention floor never advances on its own, so
history retention — and the row reclamation that follows it — remains an
explicit operator decision. An embedded engine where an operator
triggers maintenance manually and a server that runs the same work invisibly
are equally conformant (see `api.md` for the optional maintenance API group). The
invariants below bind every implementation, whoever runs the work: maintenance
never creates a second source of truth for the filesystem.

### 6.1 Manifest publication and checkpoint verification

Publication writes `manifests/{predecessor_no + 1:020}.json` with
put-if-absent. Exactly one object exists per number. A legal successor has
the predecessor's number plus one, a head sequence at least as high, and a
retention floor at least as high. A lost put-if-absent loads the winner.
If it covers the candidate's head sequence and number, the attempt is
superseded. Otherwise the publisher rebuilds against the new predecessor
and retries at the next number. Flush, bounded reorganization, streaming
compaction, and retention use this path. A successful publication refreshes
the hint with a plain PUT within its publication budget. An expired attempt
leaves the hint unchanged. GC preserves the numbered manifest chain from
the hint through the current number so discovery can cross a lagging hint.
These intermediate manifests do not protect their runs. A later publication
that advances the hint makes the older, unpinned numbers eligible for deletion.

A namespace manifest records one namespace file-set version (the section 1.2
table lists its contents).

A checkpoint is a durable pin to one numbered manifest. Creation writes the
pin, then loads the current manifest and checks its retention floor within
`CHECKPOINT_VERIFY_BUDGET_MS`. A passed floor deletes the pin and fails with
`checkpoint_unavailable`. The verify step does not reload the pinned manifest. Readers must prefer the current verified manifest plus the
visible WAL segment chain over unverified or partial manifest artifacts.

The namespace manifest may reference zero or more immutable metadata runs.
Runs are produced from committed state. Once the WAL below the retention
floor is reclaimed, these runs are required recovery material, not a cache.
Recovery uses the verified materialized basis plus the required visible WAL,
bounded by the head's visibility boundary. Verification must precede floor
advancement (section 3.8). Missing or corrupt required recovery material is a
hard error; readers do not substitute another basis or replay reclaimed history.

File revisions are stored once in the `revisions` family, newest first within
an inode, using the same descending revision, sequence, and delta ordering as
attribute revisions. Exact revision reads and paginated history scans use this
family directly. The namespace manifest version governs these row-key meanings;
version 1 requires this ordering.

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

A rebuild that cannot fit within one bounded maintenance pass runs as a
streaming compaction. The job merges its selected runs and writes output
segments as they fill, with fresh generated ids, at
`namespaces/{owner_namespace_id}/segments/{segment_id}.sst.zst`. Publication
references these objects in place through the next manifest number.

Every manifest carries `compactor_epoch`, initially zero. A process claims
the namespace compactor role before its first compaction after open. It
publishes the next manifest number with `compactor_epoch + 1` and otherwise
identical content. The runtime remembers that claim in memory. Concurrent
family groups in one runtime share the epoch. Every other publication
preserves the current epoch.

Streaming and bounded compaction publications require the claimed epoch to
equal the current manifest's epoch. A stale compactor receives `fenced` and
writes no manifest. Its unreferenced output remains eligible for collection
once old enough. A streaming publication that loses the next-number put
reloads the manifest and retries only while its input runs still contain
the same segments. Changed inputs produce `abandoned`.

Before each publication attempt, a streaming job checks elapsed monotonic
time from before its first output. It reports `abandoned` without a manifest
write when elapsed time exceeds
`UNREFERENCED_SEGMENT_MIN_AGE_MS - GC_MIN_GRACE_WINDOW_MS`. The segment minimum
age is 86,400,000 ms. The remaining grace floor covers publication, provider
operations, clock error, and scheduling delay (section 6.4).

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

Checkpoint records are standalone files under `pins/`. Maintenance
never creates one: automatic manifest publication leaves superseded manifests and
folded-away segments unpinned, and garbage collection reaps them under the
grace-window rules ("Garbage collection").
A checkpoint record is a deliberate pin — fork sources and explicit maintenance
checkpoints — and roots its basis while the pin exists.

### 6.3 Retention management

Retention management decides how far back incremental replay is still
promised. It bounds only replay state — change-feed resumption, superseded
binding rows, and commit receipts — never file revision history, which is
retained in full.

A retention floor may advance only when the system has enough verified
material to support readers from the new floor forward, and it never
advances implicitly: the default posture retains everything, and the floor
moves only when an operator requests an explicit retention advance. The
current manifest stores the floor. Advancement verifies its segments and
publishes the next number with the same runs and head sequence.

### 6.4 Garbage collection

Delete is tombstone-first. Garbage collection reclaims unreachable metadata,
content still owned by upload-session records, and the owner prefix of a
retired namespace, under the rules below. Published content in a live
namespace is never swept, and a deleted ancestor's content stays while a
descendant depends on it, so content can remain long after file or namespace
deletion. Collection runs only through explicit maintenance.

An absent namespace has nothing to collect. A call discovers the current
manifest and reads one complete checkpoint listing before sweeping.
Invalid or unreadable roots fail before deletion. Deleted namespaces also
have a current manifest, which remains their permanent tombstone.

The collector writes no run object, reference table, phase, or cursor. It
builds its live set in memory from the current manifest and pin keys alone.
Each family has a fresh `max_steps` budget for candidates that need a store
request after listing: an age check, a record read, or a deletion. A
candidate the live set retains costs nothing. Exhausting one family's budget
stops that family and continues with the next. `budget_exhausted` is true
when any family stops with candidates remaining. Root reads and their
listings are not charged as sweep steps.

Manifests and WAL list from the start. Pins, metadata segments, and upload
sessions start after a key derived from `context.now_ms`. Its shape is the
lowest valid key in the family with the random part replaced by lowercase
hex from a hash of the clock. The pin's manifest number is
`1 + hash mod current_manifest_no`. Each sweep lists from that key to the
end, then from the beginning up to that key, excluding keys at or after it
on the second listing. Different clocks change the starting position, so
repeated bounded calls can reach every key without saved progress. The
complete pin listing used to identify roots always starts at the beginning;
only sweep order rotates. Retired content lists from the start.

Core GC never recognizes or deletes objects under `extensions/`. Grep owns
its own collector. Concurrent namespace collectors independently read roots
and delete only aged, unreferenced objects. Not-found during deletion is
harmless. Publication budgets, object age gates, and grace windows protect
concurrent publications under these rules:

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
   its budget by refusing to initiate its manifest put-if-absent once the
   budget is spent, and provider operations consume one deadline across
   retries. Multipart transfers of large immutable payloads carry no
   whole-operation deadline; the floor's provider terms remain the
   small-object bounds because everything the inequality times — the budget
   self-checks, which use local monotonic elapsed time,
   and the final conditional put — concerns small control objects. A
   window below the floor is rejected as `invalid_request` at every
   surface. Under the floor's inequality, any acknowledged root
   publication lands its conditional put before an object it references
   could age past `T`, so GC never deletes an object younger than its applicable minimum age,
   reachable or not. Metadata segments use the minimum age in rule 12; other
   families use `T`. An object without a provider timestamp reads as
   young.

   **Clock assumptions.** Object age compares the collector host's recorded
   `now_ms` with the provider's `last_modified_ms`:
   `now_ms.saturating_sub(last_modified_ms) >= T`. The minimum `T` is
   `GC_MIN_GRACE_WINDOW_MS = 1,230,000 ms`: 900,000 ms for the longest
   publication, 120,000 ms for the provider deadline, 30,000 ms for an attempt,
   and `GC_SAFETY_MARGIN_MS = 180,000 ms`. Thus the combined allowance for
   clock error that overstates age (collector ahead of provider), timestamp
   precision, and scheduling delay around the budget checks is at most
   180,000 ms. If scheduling and precision consume `S` ms, permissible
   relative clock error is at most `180,000 - S` ms. This is a relative
   host-to-provider bound, not an allowance of three minutes for each clock.
   A provider clock ahead of the collector delays collection. Missing age
   evidence retains the object; a timestamp in the future also retains it.

   Record ages compare clocks on different hosts. Checkpoint deletion uses
   expiry plus `T` for user and snapshot pins, and creation plus `T` for
   absent fork targets or user and snapshot pins on deleted namespaces.
   Upload cleanup uses stored expiry, completion, and abort instants (rule
   11). The grace part of each inequality covers the same combined 180,000 ms
   allowance, now between the collector and the host that stamped the record
   or admits the final publication. `CONTENT_RECLAMATION_GRACE_MS` adds the
   receipt admission window to `GC_MIN_GRACE_WINDOW_MS`; it assumes this
   relative error bound also holds between receipt issuers, admitting hosts,
   and collectors. Host clock drift between calls and any wall-clock steps must
   fit within these bounds.

   Direct record expiry is different: hosts compare their current instant
   with a stored `expires_at_ms` without adding `GC_SAFETY_MARGIN_MS`.
   `UPLOAD_SESSION_LEASE_MS`, caller-selected checkpoint lifetimes, and snapshot
   expiries specify lifetimes, not skew allowances. No constant guarantees
   that these remain usable until the creating host reaches the deadline:
   a host ahead by `E` ms can reject or release them up to `E` ms early by
   the creating host's clock. Even a positive error below 180,000 ms can do so.
   Grace-delayed reclamation protects publication; it does not promise
   simultaneous expiry decisions or the full requested lifetime on every host.

   Fork installation consumes at most `FORK_INSTALL_BUDGET_MS = 900,000 ms`
   before initiating the target manifest put. The creation grace reserves
   the 150,000 ms provider bound and the 180,000 ms clock allowance.
   Streaming compaction reserves the same grace floor within its segment
   minimum age (rule 12). These bounds exclude unbounded pauses between a
   budget check and its write.

   A manifest below the number the hint named at the start of the call is a
   deletion candidate when no pin names it and its provider
   timestamp is at least `T` old. Its immediate successor, if present, must
   also be at least `T` old, so a reader that loaded a lagging hint can still
   fetch the predecessor.
   The current manifest and every pinned manifest protect their runs
   through the in-memory live set.
2. **Floor is necessary, not sufficient.** Being below the current manifest's retention floor only
   nominates an object for deletion.
3. **One call, one clock.** `context.now_ms` is fixed for every age, lease,
   release, manifest retirement, and owner-sweep decision in the call. A later call
   has its own clock and reads its own roots. Clock error is covered by the
   grace and age bounds in rule 1.
4. Roots are the current manifest on a live namespace and every manifest
   number in the complete `pins/` key listing. No pin body is read to build
   this set. Each listed pin protects its manifest and every segment in its
   runs for the whole call, even if the call deletes that pin. Pin bodies
   are read only for deletion decisions, bounded by `max_steps`.
   A pin naming an absent manifest is corruption; the error names the pin
   key. An unreadable or invalid manifest fails before sweeping. There is
   no missing-basis sweep.
   The current manifest and hint are never swept. Manifest numbers at or
   above the observed hint remain for discovery; intermediate numbers
   protect no runs unless a pin names them.

7. An active namespace protects every WAL number above either
   `last_folded_wal_no` or `retention_floor_wal_no` of its current manifest.
   A number at or below both is reclaimable after its provider-age grace.
   A fence folds and reclaims by the same rule as a data segment.
   A deleted namespace protects no WAL.

8. **Retention wins residual races.** A floor may pass a pin's manifest head
   sequence. The pin still protects that manifest and its runs. Reads through
   the pin use that manifest.
9. **Immutable sweep families need no two-step deletion.** WAL segments,
   metadata segments and manifests, grep segments and manifests, and content
   blobs are published under conditional or immutable-object protocols.
   Once an object is unreferenced and grace-aged, unconditional deletion is
   safe. WAL publishers must refresh a cached tip within the publication
   budget, so they cannot reuse a number reclaimed after that budget. The only listed content prefix is the owner prefix of a retired namespace
   whose deadline has passed (rule 14). Every other owner prefix is reached
   only through upload sessions (rule 11).
10. **Fork checkpoints require an exact reference.** A pin younger than
    the grace window is retained without reading its target. After that
    window, the source reads the target's hint and current manifest through
    manifest discovery, never the target's WAL. An absent target hint makes
    the pin reclaimable. An existing target that names another pin in
    `fork_basis`, or no basis, was installed by a later attempt or a plain
    create, so the pin belongs to an abandoned attempt and is reclaimable.
    A target that names this pin with another source or manifest reference
    is corruption. An unreadable target retains the pin; an invalid target
    is corruption. A matching target retains the pin regardless of deletion
    or retirement status.

    Once the target's retirement deadline passes, its collector deletes the
    source pin named in its current manifest's `fork_basis` in the same step
    as the retired-owner content sweep. This deletion is idempotent and
    repeats every pass. A failed delete is retried using the permanent
    tombstone. A deleted target that has not retired leaves the source pin.

    Checkpoint creation writes its record and then verifies it against the
    manifest. `verify_checkpoint_basis` refuses a deleted namespace. A record that
    protects anything was therefore durable before deletion, and a complete
    post-deletion listing encounters it. A record written after the sweep
    passed its key cannot verify, so its creator releases it. If the creator
    crashes first, the pin has an absent target and blocks retirement until its creation
    grace passes. Neither case
    permits an early release of a verified dependency.

11. **Uploads and content, split at `completed`.** One sweep of `uploads/`
   handles both halves. Retired namespaces also sweep their owner prefix
   under rule 14.

   *Before `completed`, the upload half owns everything, and its reasoning
   is session-local.* An `open` session whose `expires_at_ms` has passed by a
   grace window is compare-and-swapped to `aborted` under the etag loaded
   with it, and only then is the object at its namespace owner and content id
   deleted, together with any provider-side transfer the session had started. A lost
   compare-and-swap retains the session for a later pass. An `aborted`
   record repeats that cleanup — covering a crash between the swap and the
   delete — and is deleted a grace window after its own `aborted_at_ms`. No
   reachability question arises: a random content id that was never
   published belongs to exactly one session, and a session that never
   completed never had a receipt, so nothing anywhere can reference it.

   A completed session in a namespace whose captured deleted manifest carries
   `reclaim_after_ms` at or before the fixed call clock deletes its content
   through the session cleanup helper, then deletes its record. It needs no
   publication lookup or additional completion grace. A duplicate delete
   by the owner sweep is harmless. A deleted but unretired namespace retains
   completed sessions because its content references are unknown. Open and
   aborted sessions keep their lease, abort, and provider cleanup paths even
   after retirement. Provider upload state can exist outside object listings.

   A completed session in an active namespace waits until
   `completed_at_ms + CONTENT_RECLAMATION_GRACE_MS`. The call builds one
   metadata read view the first time a completed session needs deciding.
   `find_content_publication(content_id)` checks the
   WAL rows and then the manifest's `content_publications` family with a
   Bloom probe. It never scans revision segments. A publication keeps the
   object and deletes the session record. An absent publication deletes the
   object first and then the record. A failed lookup deletes neither. A
   failed content cleanup retains the record for retry.

   *The grace is derived, not tuned.* A reference can enter metadata only
   through a receipt, a receipt is minted only from a durable `completed`
   session, and minting stops a fixed window after completion. The signer checks
   the completion time carried by the receipt on every mint. A retained receipt
   cannot extend the issuance window: minting refuses `now_ms` at or after
   `completed_at_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS`. A token expires at
   `now_ms + CONTENT_RECEIPT_TTL_MS`. So:

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
   Immediately before the numbered WAL put, publication checks every newly accepted
   primary's content references for a matching, unexpired proof. This check uses
   the request clock plus the attempt's elapsed monotonic time, including the
   writer check, view load, planning, and WAL preparation. Durable receipt replays
   need no fresh proof. The same inequality covers local proofs and remote
   tokens. The reasoning above assumes a content object is referenced only by
   the namespace whose session created it and by
   fork descendants reading through a pinned basis. Prepared evidence is
   therefore namespace-bound even when catalogs share a content store, and an
   embedded raw-ref import (section 2.8) writes the verified bytes under a
   fresh destination-owned identity. A future identity-preserving copy would
   have to root the reference on the source side the way a fork does.
12. **Unreferenced segments require a full day of age.** The in-memory live set
   determines whether any root manifest lists a segment. An unlisted segment
   under `segments/` is a deletion candidate only when its provider age
   exceeds `UNREFERENCED_SEGMENT_MIN_AGE_MS`, independently of the call's grace
   window. The clock-error allowance in rule 1 still applies.

   Streaming compaction checks its elapsed monotonic time before each
   publication attempt. The bound is derived by reserving the GC grace floor:

   ```text
   METADATA_COMPACTION_BUDGET_MS + GC_MIN_GRACE_WINDOW_MS
       <= UNREFERENCED_SEGMENT_MIN_AGE_MS = 24 * 60 * 60 * 1000
   ```

   A job past this bound abandons because its earliest output may have been
   collected. A crashed job leaves unreferenced segments that age out by the
   same rule. Every compaction publishes only while the current manifest's
   `compactor_epoch` equals its claim. A newer claim fences every older
   compactor. Numbered manifest puts serialize claims and compaction output.

13. **Namespace retirement requires a complete checkpoint listing.** The
    call must observe a deleted manifest without `reclaim_after_ms`. Retirement
    waits until no encountered pin remains. A retained pin, an unrecognized
    key, or an uncertain pin load prevents retirement.
    A pin family stopped before finishing cleanup prevents retirement.
    Exhausting another family's budget does not prevent retirement.

    At the end of the checkpoints family, GC re-reads the manifest and
    publishes the next manifest, preserving deleted status and adding this deadline:

    ```
    deadline = context.now_ms + max(grace_window_ms,
                                             NAMESPACE_RETIREMENT_GRACE_MS)
    NAMESPACE_RETIREMENT_GRACE_MS = max(GC_MIN_GRACE_WINDOW_MS,
        DIRECT_TRANSFER_URL_TTL_MS + PROVIDER_OPERATION_DEADLINE_MS
        + PROVIDER_ATTEMPT_TIMEOUT_MS + GC_SAFETY_MARGIN_MS)
    DIRECT_TRANSFER_URL_TTL_MS = 15 * 60 * 1000
    ```

    The call's fixed clock stamps the deadline. This grace covers a download
    capability issued just before deletion, an in-flight read within the
    publication budgets, one provider operation, and the clock allowance.
    Core asserts the inequality at compile time.

    Every other manifest field is preserved verbatim, and successor identity is
    checked. A lost put-if-absent reloads the manifest. Another collector's
    deadline wins and remains unchanged. An active manifest is corruption because
    deletion is terminal. An uncertain transport outcome requires readback
    before deciding success or returning an error. An error writes no further
    state. The manifest's status is checked at the
    transition. Retirement releases fork records from descendants to
    ancestors. It deletes no content by itself.

14. **Retired namespaces sweep their own content.** After upload sessions,
    GC lists exactly `content-stores/{content_store_id}/objects/{namespace_id}/`
    if the call observed a deleted manifest whose `reclaim_after_ms` is at or
    before `context.now_ms`. A call before the deadline reports it in
    `next_reclamation_at_ms` and skips this prefix. Active namespaces never
    list their published content.

    Before listing, GC re-reads the manifest once. It must still be deleted,
    name the captured content store, and carry a deadline at or before the
    fixed call clock. A mismatch fails with `namespace_corrupt`; a failed read
    fails with the store error. Neither permits deletion.
    Retirement cannot regress, so this check covers the whole family,
    within this call.

    Each candidate must parse as a content blob, name this exact owner, and
    start with the exact owner prefix. Anything else is retained as
    `unrecognized_key`. Recognized objects are deleted unconditionally under
    rule 9. Not-found is harmless. A delete error ends the call. The next call starts listing from the
    beginning and can retry the failed key. Each deletion uses one step of the budget.

    Each call lists again from the start. An empty completed
    pass is never authority to stop listing. This removes late writes,
    including objects inserted before the last key visited by an earlier call. Current manifests, content-store
    descriptors, and other owners' objects are never candidates for this
    family. The deleted manifest rejects new upload capabilities and commits;
    already-issued capabilities may still write until they expire.

Pins are deleted directly. User and snapshot pins become collectable at
expiry plus grace, or creation plus grace on a deleted namespace. Permanent
user pins on live namespaces require explicit release. Fork pins follow
rule 10. Pin ids are never reused. Upload cleanup deletes content before
its record, so interrupted cleanup retains the evidence needed to retry.

### 6.5 Control-object cleanup

Upload sessions are moved to a terminal state and cleaned by the GC pass
("Garbage collection", rule 11). Implementations may additionally clean up
other expired control-plane objects. Upload control objects MUST first enter
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
validity. Commit fingerprints do not include timestamps. Commit ordering and
writer fencing use sequences, epochs, and compare-and-swap and do not depend
on clock agreement. Time-based reclamation and expiry require bounded clock
error. Section 6.4 states what the maintenance safety margins cover and where
direct expiry comparisons have no added margin.
