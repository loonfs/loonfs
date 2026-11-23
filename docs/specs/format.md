# LoonFS storage format

This specification defines how LoonFS stores a filesystem in object storage: its object layout, wire encodings, and the protocols for reading, writing, and maintaining namespaces. Any implementation following these rules can read, write, and garbage-collect a LoonFS store without exposing the HTTP API ([API specification][api-spec]).

Because a crash can interrupt any multi-step sequence (such as an upload succeeding before the commit that references it), recovery relies on a single durable source of truth: an immutable, numbered write-ahead log (WAL). Writers append batches to the next WAL object (`1, 2, 3, ...`), and creating that object commits every record inside it. Periodically, LoonFS folds committed metadata into sorted segments and publishes a numbered manifest listing them. A small manifest boundary object records a manifest number at or below which no manifest can be current, so cold readers list only manifests above that number.

File bytes are stored separately in immutable content objects and become durable no later than the commit referencing them ([File contents and ownership](#15-file-contents-and-ownership)). Everything required for recovery (control records, metadata segments, and content for every retained view) is stored in object storage. Local caches and databases can always be rebuilt.

The key words MUST and MUST NOT in this document are to be interpreted as described in BCP 14 ([RFC 2119](https://www.rfc-editor.org/rfc/rfc2119), [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)) when, and only when, they appear in capitals. Conventions used throughout:

- Examples clarify rules without adding fields or behavior.
- Implementation defaults are labeled as defaults and are not format requirements.
- Absolute timestamps are Unix milliseconds. Budgets, leases, and grace windows are durations in milliseconds.

## Contents

| Section | Subject |
| --- | --- |
| [1. The storage model](#1-the-storage-model) | Namespaces, inodes, names, revisions, deletion, attributes, access, and row families |
| [2. Objects and references](#2-objects-and-references) | Object keys, numbered manifests and WAL objects, and the manifest boundary |
| [3. Object-storage requirements](#3-object-storage-requirements) | Conditional writes, reads, listings, and provider assumptions |
| [4. Reading a namespace](#4-reading-a-namespace) | Recovery, visibility, path lookup, and content verification |
| [5. Uploading content](#5-uploading-content) | Upload sessions, direct transfers, and admission proofs |
| [6. Publishing a commit](#6-publishing-a-commit) | Writer fencing, validation, group commit, and retries |
| [7. Folding the WAL](#7-folding-the-wal) | Manifests, runs, segments, and publication |
| [8. Snapshots](#8-snapshots) | Stable views, verification, deletion, and expiry |
| [9. Namespace lifecycle and forks](#9-namespace-lifecycle-and-forks) | Creation, copy-on-write forks, deletion, and retirement |
| [10. Retention and compaction](#10-retention-and-compaction) | History boundaries and metadata rewrites |
| [11. Garbage collection](#11-garbage-collection) | Reference roots, collection safety, and complete collections |
| [12. Encodings, versions, and extensions](#12-encodings-versions-and-extensions) | Compatibility rules and extension boundaries |
| [13. Alternatives considered](#13-alternatives-considered) | Designs that were considered and not used |
| [Appendix A](#appendix-a-durable-records-and-byte-encodings) | Record fields, row keys, and block encoding |
| [Appendix B](#appendix-b-semantic-commit-fingerprints) | Exact fingerprint canonicalization |
| [Appendix C](#appendix-c-timing-and-size-reference) | Timing assumptions, constants, and implementation defaults |
| [Appendix D](#appendix-d-grep-extension-format) | Grep's separately owned durable state |

## 1. The storage model

### 1.1 Namespaces and identity

A "namespace" is a directory tree with its own ordered change history and an immutable, non-reusable `namespace_id`.

Lifecycle states recorded in the current manifest:

- **`initializing`.** Every namespace starts by writing manifest 1 with put-if-absent and status `initializing` while required external objects (pool member entry, fork snapshots) are installed. LoonFS treats initializing namespaces as missing (`namespace_not_found`) for all operations except create, fork, delete, or collection. An abandoned ID cannot be reused ([Creating a namespace](#91-creating-a-namespace), [Forking a namespace](#92-forking-a-namespace), [Deleting a namespace](#94-deleting-a-namespace)).
- **`active`.** Published once initialization completes (manifest 2, or manifest 3 when a fork folds a source WAL tail).
- **`deleted`.** Deleting a namespace publishes a final "namespace tombstone" manifest, which stays current until [retirement](#95-retirement). Retirement raises the [manifest boundary](glossary.md#manifest-boundary) to `retired`, after which garbage collection deletes every other object under the namespace prefix except `boundary.json`, fences, floor objects, and unrecognized keys. Any request to a deleted or retired namespace (including creating or forking into its ID) returns `namespace_deleted`.

A "fork" is a new namespace starting from a retained view of a source namespace, sharing existing content and metadata objects while isolating later commits ([Namespace lifecycle and forks](#9-namespace-lifecycle-and-forks)). Every namespace belongs to one immutable "pool" (`pool_id`, e.g. `pool_3s0drmj8em84yb4mnetqkz2j0x`), whose prefix (`pools/{pool_id}/`) stores content objects, upload records, and assembly temporary objects for all namespaces in the fork tree. Plain creates generate a new pool ID and forks copy their source's, and each namespace maintains a [member entry](glossary.md#member-entry) in the pool until retirement ([Creating a namespace](#91-creating-a-namespace), [Content roots](#119-content-roots)).

Across namespaces, items are identified by `(namespace_id, inode_id)` and commit positions by `(namespace_id, committed_seq)`. Every new namespace starts at sequence 0 with root inode `ino_00000000000000000000000000` (all zero bits).

Every non-root inode ID is a 128-bit value formatted as `ino_` plus 26 lowercase Crockford base 32 characters (e.g. `ino_000000r0000013kje0d47gc8kw`, see [Numbered objects and generated IDs](#23-numbered-objects-and-generated-ids) and [Field conventions](#121-field-conventions)):

- **High 64 bits:** Low 32 bits of the creating `committed_seq`, followed by a 1-based per-commit create number (counting files, directories, and auto-created parents, so generated IDs are never zero). Grouping new inodes by commit keeps each fold run's inode key range narrow so lookups skip unrelated runs. (Sequence wraparound after 2^32 commits only shifts sort order, and no rule relies on inode ID ordering.)
- **Low 64 bits:** Random, preventing collisions (1 in 2^64) when a fork and its source both create inodes at the same sequence and create number and later [merge](glossary.md#merge) (merges preserve the fork's inode IDs).

Format version 1 defines two inode kinds, `directory` and `file` (`link` is reserved in [Reserved functionality](#125-reserved-functionality)). Inode rows store only kind and creation attribution, while paths are stored in directory bindings and file bytes are stored in revisions. Renaming keeps the same inode ID, while deleting and recreating at the same path allocates a new inode ID.

### 1.2 Commits and revisions

A "commit" is one published mutation request. A single commit can bundle multiple operations that apply atomically at one namespace sequence (`seq`).

A "file revision" is one version of a file's contents, assigned a per-file monotonic `revision_no` independent of namespace sequence numbers:

| Namespace sequence | Operation | Result |
| --- | --- | --- |
| 17 | Create `/reports/Report.txt` | Creates a new inode, binds the name, and publishes revision `1`. |
| 18 | Rename to `/reports/Final.txt` | Keeps inode ID and revision `1`, replaces the directory binding. |
| 19 | Replace contents | Keeps inode ID, publishes revision `2`. |
| 20 | Delete `/reports/Final.txt` | Removes the binding and records a deletion tombstone. |
| 21 | Restore it | Rebinds the same inode with its revision history intact. |

### 1.3 Directory bindings

A "directory binding" maps a "directory slot" (`parent_inode_id`, `name_key`) to one child inode. Together, `(parent_inode_id, name_key, child_inode_id)` form an "edge". Each bound slot stores:

- `display_name` (exact caller spelling),
- the child's immutable kind and creation attribution, and
- for files, a "revision summary" (`revision`) copying the file's current `revision_no`, [content reference](#a3-content-references-and-checksums) (including size), and committing actor and timestamp. Copying the summary into the binding avoids per-file revision lookups during directory listings (roughly 136 vs. 101 compressed bytes per binding row in a 1,000-file directory, or 78 vs. 45 bytes per row for a file rewritten 1,000 times).

Each binding row (`direntry_binding`) records its own position (`committed_seq`, `delta_index`), commit attribution (`commit_id`, `committed_by`, `committed_at_ms`), and `bind_position` (the position of the bind establishing the edge: equal to its own position on a bind row, and to the removed bind's position on an unbind row). The API's `binding_version` encodes `bind_position`, so content updates leave `binding_version` unchanged while moves change it.

Binding rows are indexed from both sides:

- `direntry_binds` (the "slot index"): ordered by `(parent, name_key, child)` for path lookups and directory listings. Its segment filter also adds a per-parent probe `direntry-bind-{parent_inode_id_body}` ([Metadata rows and row keys](#a6-metadata-rows-and-row-keys)) so directory listings skip runs that contain no rows for that parent.
- `direntry_child_binds` (the "child index"): ordered by `(child, parent, name_key)` for parent lookups.

Both indexes sort an edge's rows newest first. At sequence `N`, an edge's state is its first row with `committed_seq <= N` (if one commit changes an edge multiple times, the last change wins). A slot's state is the newest across its edges. If unbound or absent, the slot is free.

Two deltas write binding rows:

1. **Binding events (`bind_direntry`, `unbind_direntry`).** Write bound or unbound rows to both indexes. A file bind includes the file's current revision summary. An unbind references the exact bind position being removed (verified by validation) and acts as a tombstone for older rows of that edge ([Row retention during a base compaction](#103-row-retention-during-a-base-compaction)).
2. **File revisions (`append_file_revision`).** Write a revision row to `revisions` and a "summary row" (a bound row with the revision delta's position and the existing edge's `bind_position`, display name, creation attribution, and new revision summary) to `direntry_binds` only. Revisions on unbound files are rejected. When one commit both binds a file and appends a revision, both write the same summary and the later delta wins.

Validation enforces at most one child per slot and at most one parent per child (a bind MUST target an unbound child unless the same operation unbinds it first), keeping both indexes consistent at every sequence.

Directory listings scan the parent prefix in `direntry_binds` once in `name_key` order, taking each edge's first row at or below `N`, emitting bound entries and skipping unbound ones without reading inode or revision rows (a bound file row missing a revision summary is corrupt). For example, if `/docs/a.txt` is bound at seq 10 and rewritten at seq 12 and 15, and `/docs/b.txt` is bound at seq 11 and unbound at seq 14, a listing at seq 13 returns `a.txt` (seq 12 summary) and `b.txt` (seq 11 bind), while a head listing returns only `a.txt` (seq 15 summary).

### 1.4 Names and paths

Display names are preserved as supplied and MUST satisfy:

- 1 to 255 UTF-8 bytes.
- No `/`, no Unicode `Cc` control characters, and none of `: ? * | " < > \`.
- Not `.` or `..`, not all whitespace, and not ending in a space or dot.
- Not a Windows reserved device name (`CON`, `PRN`, `AUX`, `NUL`, `COM1`-`COM9`, `LPT1`-`LPT9`, matched case-insensitively ignoring any extension, so `con.txt` is rejected).

Sibling comparisons and lookups in every namespace derive a canonical "name key" from the display name: NFC normalization, full Unicode 17.0.0 default case folding (non-Turkic), then NFC again (max 768 bytes, matching the 3x worst-case expansion). Comparing without case everywhere avoids case-collision incompatibilities when syncing trees to case-insensitive filesystems without forcing a permanent per-namespace setting at creation ([Alternatives considered](#13-alternatives-considered)).

Under this rule, `Report.txt` and `report.txt` share one name key (renaming between them updates `display_name` in place), and precomposed `é` matches `e` + combining acute accent (see the [name-folding fixtures][name-vectors]). Because stored keys depend on this mapping, implementations MUST preserve the exact Unicode 17.0.0 folding (simple lowercasing is insufficient).

Canonical request paths start with one `/`, have no empty components, repeated slashes, or trailing `/` (except `/`), and are at most 4,096 UTF-8 bytes and 128 components. Noncanonical paths are rejected rather than rewritten. Derived paths built from stored names can exceed these limits and are still returned by inode-addressed reads.

### 1.5 File contents and ownership

Every file revision stores one `ContentRef` (only `blob_v1` in version 1): a random `content_id`, a byte length `size_bytes`, and a checksum of the first `size_bytes` bytes of that chain (raw file bytes, no framing). References contain no bucket, key path, or namespace. Combining `content_id` with the reading manifest's `pool_id` locates the object identically across every member of the [pool](glossary.md#pool), so forks and merges share content without copying bytes or needing the source's manifest.

A "chain" is the byte sequence under one `content_id`. Appends extend a chain, and each `ContentRef` addresses a prefix of it (e.g., revisions 1, 2, and 3 addressing the first 100, 200, and 210 bytes of one chain). Because content objects are immutable, a chain's bytes are listed in chain order by its "layout" as "extents". Each extent records the object's content ID, object key kind (whole vs. span), object offset, length, and the provider's stored whole-object checksum ("object checksum"). Every extent referencing the same object records the same object checksum ([Content verification](#45-content-verification), [WAL records](#a5-wal-records)).

Object keys under the pool prefix:

```text
pools/{pool_id}/content/{content_id}
pools/{pool_id}/content/{content_id}-{span_object_id}
```

The first key is the chain's "whole object", and every other key is a "span object" identified by a fresh random `span_object_id` per write. Using random span IDs rather than offset-based keys gives each span object a single writer even if two pool members concurrently materialize the same pieces (leaving any unreferenced duplicate for GC), while sorting every span right after its whole object. Storing content under a separate `pools/{pool_id}/` prefix rather than under any namespace prefix means no namespace's retirement deletes shared content and one pool collection sees every root ([Content roots](#119-content-roots)).

File bytes become durable no later than the commit referencing them through one of two paths:

| Path | Durability mechanism |
| --- | --- |
| Uploaded | An upload session writes and verifies the content object before the commit ([Uploading content](#5-uploading-content)). |
| Inline | The committing WAL object stores raw bytes as "pieces" ([WAL records](#a5-wal-records)), either a full small file or appended bytes. Before a fold publishes a manifest covering that WAL object, it writes all pieces to content objects as extents ([Publishing a manifest](#72-publishing-a-manifest)). |

Because GC deletes a commit WAL object only after a manifest covers it, bytes never disappear while only in the WAL. Only the chain's "creator" (the namespace that created it, stored in its layout) extends a chain in place. An append by another pool member starts a new chain sharing the original extents ([Operations and WAL deltas](#66-operations-and-wal-deltas)). LoonFS does not deduplicate across separate uploads (retries within one session reuse that session's content ID).

### 1.6 File and subtree deletion

Deleting an item removes its directory binding and writes a "tombstone" for the deleted root inode, leaving its revisions and descendants in place while hiding the entire subtree.

Tombstones are sequences of position-ordered events (`committed_seq`, `delta_index`):

- **`set`.** Starts a deletion, storing `root_kind` and `deleted_binding` (`parent_inode_id`, `name_key`, `display_name`) so restoration works even after old bindings compact away.
- **`revoke`.** Committed by a restore. Cancels one `set` event identified by `DeltaPosition` (`{seq, delta_index}`) and MUST NOT include `deleted_binding`. A restore commits `revoke` alongside a new binding for the item. A later `set` on the same inode begins a new deletion.

A deletion's `deletion_seq` is the sequence of its `set` commit. Relative to a view's effective retention floor (manifest floor raised by any [floor objects](glossary.md#floor-object) in the WAL tail, [Advancing the retention floor](#101-advancing-the-retention-floor)):

- **In the trash** (`latest event == set` and `deletion_seq > floor`): listed in trash, restorable, and all rows kept.
- **Expired** (`latest event == set` and `deletion_seq <= floor`): omitted from trash, cannot be restored (see [Expiring deletions and creating floor objects](#expiring-deletions-and-creating-floor-objects) for what a restore returns), still hidden by its tombstone, and its hidden rows are removed by later base compactions ([Row retention during a base compaction](#103-row-retention-during-a-base-compaction)). Expiry commits nothing. Tying expiry to the single retention floor lets callers check one number for both history and restore reachability.

Restore requests specify `(inode_id, deletion_seq)`. Validation MUST verify the deletion is in the trash at the writer's [position](glossary.md#writer-position) (returning `not_deleted` otherwise and never revoking a different deletion, and a commit cannot restore a deletion from earlier in the same commit). A rights check runs before validation, so a subject can get a different error for an expired deletion (see [Expiring deletions and creating floor objects](#expiring-deletions-and-creating-floor-objects)). Only deletion roots can be restored. Because an item cannot be deleted while hidden by an ancestor deletion, nested deletions are always older than enclosing ones, so no trash entry ever sits inside an expired deletion.

Because `tombstones` is keyed by root inode, LoonFS also maintains the `trash` [query index](#19-row-families) keyed by `(deletion_seq, root_inode_id)`. A `set` writes `listed`, and a `revoke` writes `removed` (sorting before `listed` so scans skip restored items). Trash listings start scanning strictly above the view's retention floor so expired deletions never appear. `tombstones` remains the source of truth.

Deleting a file does not immediately delete its content ([Retention and compaction](#10-retention-and-compaction), [Namespace lifecycle and forks](#9-namespace-lifecycle-and-forks)).

### 1.7 Attributes and attribution

Attributes are a per-inode string-to-string map unaffected by renames, moves, or file content revisions:

| Part | Rule |
| --- | --- |
| Key | 1 to 128 UTF-8 bytes, no Unicode `Cc` control characters, exact byte comparison (no case folding/normalization). Keys starting with `loonfs.` are reserved and forbidden to callers. |
| Value | UTF-8 string up to 4,096 bytes (empty strings and control characters allowed). |
| Map | At most 100 entries and at most 65,536 combined UTF-8 bytes of keys and values (excluding framing). |

Setting a key to `""` stores an empty string rather than deleting it. Keys are deleted only via `remove`. Durable maps violating these limits are rejected rather than truncated.

Every inode starts at `attributes_revision_no = 0` with an empty map and no stored row. Each accepted update increments `attributes_revision_no` by 1 and stores the full resulting map even if unchanged, so clearing a map MUST remain distinct from revision 0. Storage retains historical attribute rows as needed for sequence-based views.

Every commit records a timestamp, optional message, and required opaque application `actor` string (stored without authentication and unused for authorization). Event rows copy their commit's `committed_seq`, `commit_id`, `committed_by`, and `committed_at_ms`:

| Metadata | Actor and timestamp fields |
| --- | --- |
| Inode, file revision, tombstone event, attribute revision, access revision, commit | `committed_by`, `committed_at_ms` |
| Trash entry (`listed` row) | `deleted_by`, `deleted_at_ms` |
| Directory binding | `child_created_by`, `child_created_at_ms` (copied from child inode) |

A new namespace's root inode uses actor `loonfs`. Forks inherit existing inodes and their original timestamps while recording the fork's own creation time in the manifest. Event timestamps are informational only (sequence numbers define order), and directories have no modification timestamp ([Timing assumptions](#33-timing-assumptions) covers deadlines).

### 1.8 Access rows

An inode can store an "access row" with a `boundary` flag and a map from principal IDs (1 to 256 visible ASCII bytes excluding `,`, never resolved by LoonFS) to sets of rights:

| Right | Allows |
| --- | --- |
| `read` | Reading item metadata and current content. |
| `history` | Reading older revisions and snapshots (together with `read`). |
| `write` | Modifying file contents or item attributes. |
| `create` | Adding entries to a directory. |
| `remove` | Removing entries from a directory. |
| `share` | Modifying grants for rights the caller already has. |
| `manage` | Any modification to the item's access row. |
| `admin` | Every right on every item (valid only on the root inode). |

Rights encode as distinct names in table order (decoders accept any order but reject duplicate rights or principals). Maps allow at most 1,000 principals and 65,536 bytes of principal IDs, with at least one right per principal. Invalid durable maps are rejected, not truncated.

Every inode (including the root of a new namespace) starts at `access_revision_no = 0` with no stored row (`boundary: false`, empty grants) until an `update_access` writes one. Each accepted update increments `access_revision_no` by 1 and stores the complete row, and clearing all grants and `boundary` is a real revision distinct from revision 0. Forks inherit their source's access rows. Requests with principals are authorized against these rows, while requests in the [service context](glossary.md#service-context) skip rights checks ([API specification][api-spec]).

A principal's effective rights on an inode are the union of its grants on the inode and each ancestor walking upward along current parent bindings, stopping after the first row with `boundary: true` or at the root (revisiting an inode indicates corruption, and `admin` on `/` grants all rights everywhere and is not inherited). The walk continues past an inode with no current parent binding only when that inode is the root of a deletion in the trash, through the tombstone's saved parent. The root of an expired deletion has no current parent binding and isn't in the trash, so the walk ends there, and an expired subtree inherits nothing from above. See [Paths, listings, and revision reads](#44-paths-listings-and-revision-reads) for inode-read rejection of expired subtrees and [API specification][api-spec] for `update_access` and snapshot admin rules.

### 1.9 Row families

Metadata is stored across twelve "row families" (one family per [metadata segment](glossary.md#metadata-segment), exact keys in [Metadata rows and row keys](#a6-metadata-rows-and-row-keys)). Every row is derived deterministically from a single WAL delta or commit record during folding or replay without reading other rows (so folds, replays, and readers always produce identical rows), with two exceptions: the root inode row (synthesized from manifest 1 at seq 0) and fold-written `content_layouts` rows ([Publishing a manifest](#72-publishing-a-manifest)).

Every family falls into one of three categories:

- **Entity header.** One immutable row per entity (`inodes`, `commits`), keyed by entity ID.
- **History family.** Mutable per-entity versions sorted newest first so a read at sequence `N` stops at the first row at or below `N` (especially important in `direntry_binds` when a file has thousands of rewrites). Keyed by entity ID, optional revision number, and `(committed_seq, delta_index)` (binding families key by edge). Every history row records `committed_seq`, `commit_id`, `committed_by`, and `committed_at_ms`.
- **Query index.** Alternate lookup key over facts stored elsewhere (`commit_receipts`, `trash`, `content_layouts`, `merges`), recording the target commit sequence (and copying actor/timestamp only where needed).

Denormalized copies (child kind/attribution in bindings, file revision summary in `direntry_binds`, deletion metadata in `trash`) are permitted only for immutable fields or fields written in the same delta that mutates them, and are never repaired on read. Families sharing the same deltas and retention rules compact together in one [family group](glossary.md#family-group) (the two binding families together, and `commits`, `commit_receipts`, and `merges` together) so one index can never drop rows another still exposes.

| Family | Kind | Key starts with | Retention ([10.3](#103-row-retention-during-a-base-compaction)) |
| --- | --- | --- | --- |
| `inodes` | Entity header | Inode ID | Inode lifetime |
| `commits` | Entity header | Commit sequence | Floor commit and later |
| `revisions` | History family | Inode ID | Floor state and later |
| `attributes` | History family | Inode ID | Floor state and later |
| `access` | History family | Inode ID | Floor state and later |
| `direntry_binds` | History family | Parent inode ID | Floor state and later |
| `direntry_child_binds` | History family | Child inode ID | Floor state and later |
| `tombstones` | History family | Root inode ID | Every event |
| `commit_receipts` | Query index | Commit ID | Floor commit and later |
| `trash` | Query index | Deletion sequence | Until restored or expired |
| `content_layouts` | Query index | Content ID | Longest referenced layout |
| `merges` | Query index | Fork's namespace ID | Floor commit and later |

## 2. Objects and references

### 2.1 Storage layout

Namespace-scoped objects go under `namespaces/{namespace_id}/`, while shared content, uploads, pool membership, and assembly temporary objects go under `pools/{pool_id}/` ([Object keys](#a8-object-keys)):

```text
namespaces/{namespace_id}/
|-- boundary.json
|-- manifests/{manifest_no:020}.json.zst
|-- wal/{wal_no:020}.wal.zst
|-- segments/{segment_id}.sst
|-- snapshots/{snapshot_id}.json
\-- extensions/{extension_name}/segments/{segment_id}.sst

pools/{pool_id}/
|-- members/{namespace_id}.json
|-- content/
|-- uploads/{upload_id}.json
|-- uploads/{upload_id}.outcome.json
\-- temporary/{temporary_id}
```

Newly written metadata and extension segments (including compaction output) go under the writing namespace's prefix, while inherited segments remain under their `owner_namespace_id` (readers always use the descriptor's `owner_namespace_id`, never substituting the reading namespace). Composing providers (such as GCS) write assembly intermediates under `temporary/` and delete them on completion ([Immutable writes and retries](#32-immutable-writes-and-retries)). Other objects MUST NOT use keys in these families. Core readers ignore extension segments, and GC leaves non-segment keys under extension prefixes untouched.

### 2.2 Object roles and mutability

| Object | What it stores | Mutability |
| --- | --- | --- |
| Manifest | Namespace identity, status, writer/compactor epochs, runs, retention floor, extension entries | Immutable. New manifests publish at `manifest_no + 1`. |
| WAL object | Commits, a writer fence, or a [floor object](glossary.md#floor-object) | Immutable. New WAL objects publish at `wal_no + 1`. |
| Manifest boundary | Manifest number at or below which manifests are dead, or `retired` | Put-if-absent, then monotonic CAS by GC/retirement, never deleted. |
| Metadata segment | Sorted rows of one metadata row family | Immutable. |
| Extension segment | Sorted rows of an extension's state | Immutable. |
| Snapshot | Pin keeping one manifest for a user or fork | Created/deleted. User snapshot expiry and deletion updated via CAS. |
| Upload session | One transfer and its verified content | Two write-once put-if-absent records (session + outcome). |
| Member entry | Membership of one namespace in one pool | Write-once put-if-absent at create/fork, deleted at retirement. |
| Content object (whole or span) | Chain bytes plus provider stored whole-object checksum | Immutable. |
| Temporary object | Assembly intermediate bytes | Created, then deleted on completion (or by GC). |

Manifest publications can update layout or control state without creating a commit, and WAL publications can commit mutations without publishing a manifest.

### 2.3 Numbered objects and generated IDs

Manifest numbers (`manifest_no`) and WAL numbers (`wal_no`) are separate 1-based per-namespace counters formatted as 20 zero-padded decimal digits so lexicographic order matches numeric order (e.g. `wal/00000000000000000002.wal.zst`). Every numbered object is created with put-if-absent, and losers reload the winner before retrying. Payload namespace ID and object number MUST match the key.

Generated IDs use a prefix plus 26 lowercase Crockford base 32 characters (`0123456789abcdefghjkmnpqrstvwxyz`, encoding 2 leading zero bits + 128 bits, so the first character is `0`-`7`):

- **Random UUIDv4 (128 bits):** content (`con_`), metadata segment (`seg_`), extension segment (`idx_`), pool (`pool_`), upload (`upl_`), span object (`span_`), manifest publication (`pub_`), and temporary object (`tmp_`).
- **Inode (`ino_`):** High 64 bits = low 32 bits of creating `committed_seq` + 32-bit create number, low 64 bits = random (root is all zeros, `ino_00000000000000000000000000`).
- **Snapshot (`snp_`):** High 64 bits = pinned `manifest_no`, low 64 bits = random (e.g. `snp_000000000000nb4mnetqkz2j0x` pins manifest 42).

A collision at a newly generated immutable key MUST NOT overwrite existing bytes.

### 2.4 The current manifest and WAL tip

The current manifest is authoritative for namespace identity, lifecycle status, writer/compactor epochs, and folded runs through `head_seq` and `folded_wal_no`. WAL objects after `folded_wal_no` up to the "WAL tip" contain uncommitted-to-segments records (commits, fences, or [floor objects](glossary.md#floor-object), where fences and floor objects keep the prior `head_seq`). Cold readers find the newest manifest via the [manifest boundary](glossary.md#manifest-boundary) ([Resolving the metadata basis](#41-resolving-the-metadata-basis)).

| Counter | Advances on |
| --- | --- |
| `manifest_no` | Any manifest publication (fold, compaction, epoch claim, retention window, lifecycle, extension). |
| `wal_no` | Any WAL object creation (commits, fence, or floor object). |
| `seq` | Each committed mutation request (one WAL object can batch multiple sequences). |
| `revision_no` | Each new content revision of one file. |

For example, manifest 5 may fold WAL objects through 8 (`head_seq = 40`), WAL 9 may be a fence at seq 40, and WAL 10 may batch commits 41-43 (producing read view seq 43). Folding WAL 10 publishes manifest 6 without allocating seq 44.

### 2.5 Manifest references

Durable manifest references contain:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | Namespace storing the manifest. |
| `manifest_no` | Manifest number (determines the key). |
| `head_seq` | Manifest's folded head sequence. |
| `payload_checksum` | Digest that MUST match the manifest payload. |

Snapshots reference manifests in their own namespace (`namespace_id` as owner), and a fork basis references a source manifest plus both fork snapshot IDs. Segment descriptors store `owner_namespace_id` and the provider's stored whole-object checksum, and layout extents store the content object's stored checksum, so each reference verifies independently without extra lookups. Any missing or mismatched required object is an error, never a fallback.

### 2.6 A summary of the log protocol

```text
manifests:  [4] | [5]  [6]   <- current manifest, folded_wal_no = 8
                ^
                manifest boundary = 4 (readers list manifests above it)

WAL:        [7]  [8]  [9]  [10]  [11]   <- WAL tip
                      |_____________|
                      visible WAL, replayed on top of manifest 6
```

How the pieces fit together (full rules in Sections 4, 6, 7, and 11):

- **Commits.** A writer creates WAL `tip + 1` with put-if-absent stamped with its writer epoch, re-planning on collision ([Publication and group commit](#63-publication-and-group-commit)). Any process can create a commitless [floor object](glossary.md#floor-object) without claiming the writer epoch ([Advancing the retention floor](#101-advancing-the-retention-floor)).
- **Takeover.** A new writer publishes manifest `n + 1` with `writer_epoch + 1` and writes a fence at the next WAL number, blocking older writers ([Writer ownership](#61-writer-ownership)).
- **Folds.** Any process writes WAL objects past `folded_wal_no` into metadata segments and pending pieces into content objects, then publishes the next manifest ([Publishing a manifest](#72-publishing-a-manifest)).
- **Discovery.** Cold readers read `boundary.json`, list manifests above it, load the highest manifest, replay WAL objects past `folded_wal_no`, then run a successor probe and re-read `boundary.json` ([Resolving the metadata basis](#41-resolving-the-metadata-basis), [Replaying the visible WAL](#42-replaying-the-visible-wal)).
- **Garbage collection.** Deletes folded commit WAL objects (never fences or floor objects), unpinned replaced manifests (raising `boundary.json` first, so manifest puts at or below the boundary count as lost), and unreferenced segments past grace. Reads needing to outlive the current manifest take a snapshot ([Snapshots](#8-snapshots)). Because fences and floor objects can race into emptied numbers, those puts verify their position after writing.

Protocol guarantees:

1. Creating WAL `n + 1` commits its records, and the visible log is an unbroken sequence above `folded_wal_no`.
2. Every acknowledged commit appears in the visible log or folded into the current manifest (because fences and floor objects are never deleted).
3. Writer epochs never decrease along the WAL or exceed the current manifest's epoch. Once a fence is written, older epochs cannot commit.
4. Fresh discovery observes every commit acknowledged before it started.
5. Cached views and writer positions never move backward, and a read served from either one reflects a successor check made within `READ_REVALIDATION_BOUND_MS`. (The bound applies to reads, not to publications. For example, an acquired writer puts WAL object `tip + 1` however long ago it observed `tip`, as [Publication and group commit](#63-publication-and-group-commit) explains.)
6. Inline/appended pieces remain readable in the WAL until folded into content objects before the covering manifest publishes.
7. Identical commit retries do not double-commit while their commit receipt is retained.
8. Current and snapshot-pinned manifests keep all of their listed segments. Reads or writes on a replaced manifest whose segments were collected retry against the current manifest.

## 3. Object-storage requirements

### 3.1 Required operations

An object store MUST provide (within its configured storage scope):

- Create-if-absent (put-if-absent) for immutable objects at every size, including multipart completion.
- Compare-and-swap (CAS) for mutable control objects, reporting precondition failures separately from transport errors and never auto-retrying a conditional write (which would turn a lost success into a misleading precondition failure).
- Full and ranged reads, with mutable-object reads returning bytes and opaque compare token together atomically.
- Version-conditional reads returning no body when unchanged (or full GET with local comparison where the provider lacks conditional reads, see [Provider matrix](object-storage-providers.md#2-provider-matrix)).
- Deletion.
- Strongly consistent prefix listings (from start or after a key) in ascending lexicographic order returning key, size, and modification time (GC identifies fences and floor objects by listed size alone, [Candidate and age rules](#113-candidate-and-age-rules)), plus paginated child-prefix listings (one page per request) for discovering namespaces without listing their contents.

Successful writes and deletes MUST be strongly consistent. Providers MUST also apply any request that takes effect within `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` of operation start ([Clock and operation assumptions](#114-clock-and-operation-assumptions), [Publication and collection timing](#c1-publication-and-collection-timing)). The format assumes no multi-object transactions or atomic reads across control objects.

### 3.2 Immutable writes and retries

An immutable object MUST NOT be overwritten with different bytes. Collisions on newly generated content/segment keys are errors, while collisions on numbered manifests or WAL objects follow publication retry rules. Ambiguous writes resolve only via stored evidence:

- **Generated-ID records (snapshots, upload sessions).** Identified by ID alone. After an unknown-outcome put, the record at that key is the caller's own even if another caller later modified it.
- **Member entries.** Payload is determined solely by key, so read-back confirms installation regardless of which caller won.
- **Immutable data objects (content, segments).** Verified by the provider's stored whole-object checksum (single algorithm per store: CRC-64/NVME on S3, R2, S3-compatible, Azure, local, and in-memory, CRC-32C on GCS, and local store optionally CRC-32C for conformance tests).

Writers compute the store's checksum and require the provider to verify and store it (a write accepted without returning a stored checksum fails, and SHA-256 metadata is not used). Segment descriptors and layout extents record the stored checksum returned by the object's write (or assembly, or upload reference).

Handling occupied keys and streams:

- **Buffered / large writes.** If an immutable key is already occupied, a single HEAD compares stored checksums (succeeding if equal, failing if missing or different, with zero byte read-back). Among content objects, only whole-object keys are intentionally written concurrently (by retries on one service-proxied upload session, [Service-proxied staging](#53-service-proxied-staging)). Span keys are fresh per write, so a differing object there indicates corruption.
- **Streamed writes.** Hash bytes on the fly. Streams shorter than one part use a single create request, while longer streams upload parts and complete with the full-object checksum. Stream errors prevent completion, and transport failures aren't auto-retried. The incremental adapter consumes the full stream before evaluating existence preconditions so the computed digest covers all bytes even on collision.
- **Mandatory conditional completion.** Every large create MUST complete conditionally (`If-None-Match: *` on S3/R2/Azure, `ifGenerationMatch=0` on GCS) so concurrent uploads to one service-proxied session cannot overwrite already-reported bytes, since a prior absence check is insufficient. Failed or cancelled multipart transfers MUST attempt provider-side abort (deployments still need provider incomplete-multipart cleanup policies, and aborting never deletes an already-completed object).

#### Assemblies

An "assembly" builds one immutable object from ordered source byte ranges (each with its checksum, plus length and version from a prior HEAD to avoid extra HEADs) followed by optional new tail bytes, checking the full expected checksum in the store's algorithm (missing or truncated sources fail with a precondition error). If the destination already exists, LoonFS checks its stored checksum via HEAD (after a conditional create below the single-PUT threshold, or before assembling at or above it so retries skip re-copying).

Provider-specific assembly implementations are detailed in the [Provider matrix](object-storage-providers.md#2-provider-matrix) and [Local filesystem provider](object-storage-providers.md#5-local-filesystem-provider): S3 pads short non-final parts to 5 MiB from the next source and completes with `If-None-Match: *`. GCS checks per-source generation preconditions and deletes temporary compose objects on finish or failure (crash leftovers go to GC). R2 uses 64 MiB chunks and HEAD checksum verification. Azure uses `Put Block From URL` and combines verified block CRCs. Small control objects always use single-object conditional writes rather than multipart.

### 3.3 Timing assumptions

Writer fencing and commit order rely strictly on epochs, sequence numbers, and conditional writes, never clocks. Deadlines govern upload admission, snapshot expiry, and garbage collection, assuming bounded clock skew and bounded publication/provider durations ([Appendix C](#appendix-c-timing-and-size-reference)). Because client timeouts never prove a remote write didn't happen, ambiguous outcomes MUST preserve unknown-outcome handling.

## 4. Reading a namespace

### 4.1 Resolving the metadata basis

A cold read (or collection, create, or fork with no in-memory state) finds the newest manifest using the [manifest boundary](glossary.md#manifest-boundary) (`namespaces/{namespace_id}/boundary.json`). The boundary records either a manifest number `B` (missing object = 0) at or below which every manifest is dead (never current and never published to, though a snapshot or pending GC may still keep the object), or `retired` (greater than every manifest number). Only garbage collection and retirement write `boundary.json`.

Cold discovery steps:

1. Read the boundary `b`. If `retired`, stop (the namespace is retired).
2. List the manifest prefix starting strictly after manifest `b`.
3. If the listing is empty, re-read the boundary (conditionally using step 1's version when step 1 found an object, [Required operations](#31-required-operations)):
   - If `retired`, stop.
   - If changed, restart at step 1.
   - If unchanged and no boundary object exists, the namespace does not exist. If a boundary object exists, the store is corrupt (GC never leaves a non-retired namespace with no manifest above its boundary).
4. Load the highest-numbered manifest `n` from the listing (if missing, restart at step 1).
5. Re-read the boundary (conditionally if present in step 1). If `retired`, stop. If `n <= boundary`, restart at step 1 (handling pauses during which newer manifests were published and collected). Otherwise `n` is the newest manifest (if its status is `deleted`, the namespace is deleted).

A find failing to complete after `MAX_CONTENTION_ATTEMPTS` (8) attempts returns `store_contention`. After loading `n`, a cold read reads the WAL tail above `n.folded_wal_no`, sends a HEAD for manifest `n + 1`, and sends a conditional boundary read. If a successor exists or `n <= boundary`, discovery restarts.

| Discovery outcome | Interpretation |
| --- | --- |
| Empty listing, unchanged boundary, no boundary object | Namespace does not exist. |
| Empty listing, unchanged boundary, boundary object exists | Store is corrupt. |
| Boundary changed, highest manifest vanished, or `n <= boundary` | Restart discovery. |
| Required object unreadable or invalid | Error (never treated as absence). |
| Newest manifest has status `initializing` | `namespace_not_found` for all operations except create, fork, delete, or collection. |
| Newest manifest has status `deleted` | `namespace_deleted` for ordinary operations. |
| Boundary is `retired` | `namespace_deleted` for all operations except collection. |

Every namespace has its own manifest from creation (a new namespace's empty manifest represents `/` at seq 0, see [Publishing a manifest](#72-publishing-a-manifest)). Readers never chase `fork_basis` to change basis. A process that loads a manifest doesn't check it against a manifest that it loaded earlier, because every publisher checks each manifest against its basis before the put (see [Basis, collision, and boundary checks for every manifest publication](#basis-collision-and-boundary-checks-for-every-manifest-publication)).

```text
boundary 4 --list--> 5, 6 --load 6, boundary 4--> manifest 6 is newest
                                                              |
                                            +-----------------+-----------------+
                                            |                                   |
                                       listed runs                      folded WAL = 10
                                            |                                   |
                                      metadata rows                    replay WAL 11, 12
                                            |                                   |
                                            +-----------------+-----------------+
                                                              |
                                                          read view
```

### 4.2 Replaying the visible WAL

After selecting a manifest, a reader reads WAL objects from `folded_wal_no + 1` upward until the first missing number (when reading in concurrent windows, re-read any missing slot before treating it as absent, and a confirmed gap below a present object is a missing object).

Every replayed WAL object MUST satisfy:

- Data WAL objects contain contiguous commits starting from the preceding head, while fences and [floor objects](glossary.md#floor-object) contain no commits.
- `prior_head_seq` and floor-object `retention_floor_seq` obey [WAL records](#a5-wal-records) (first commit seq cannot be 0, and fences and floor objects set `prior_head_seq == head_seq`).
- Valid namespace ID, WAL number, sequence range, and writer epoch.
- Epochs are non-decreasing along the log and never exceed the manifest's `writer_epoch`.

If discovery encounters a missing or invalid WAL object (including one with a higher epoch than the loaded manifest, which can occur after a concurrent writer claim), it reloads the current manifest and restarts if the manifest advanced (reporting corruption if unchanged). A deleted manifest ends discovery immediately without probing WAL or successors.

If an ordinary read on a replaced manifest hits a GC-deleted segment, it retries from the current manifest (returning `store_contention` after `MAX_CONTENTION_ATTEMPTS` = 8 retries in the reference runtime). A pinned `ReadView` returns `stale_head` ([Reference roots](#112-reference-roots)).

#### Checking an in-memory manifest

Every process with a manifest in memory (cold reads after WAL discovery, cached views, [writer positions](glossary.md#writer-position), and post-put checks for fences and floor objects) validates it with the same two-step check (never trusting a missing successor based on recency alone, because GC could have deleted a newer manifest after raising `boundary.json`):

1. Probe for a successor manifest (`n + 1`), then advance or reload per role rules if found.
2. Read `boundary.json` conditionally using the cached boundary version (or a plain read while no boundary exists, skipping if cached boundary is already `>= n`). If `n <= boundary`, drop the manifest and run cold discovery.

(Manifest publications check `boundary.json` directly after their put, [Publishing a manifest](#72-publishing-a-manifest). Namespace collections skip this check after discovery because their roots form a safe lower bound and newer segments are within the grace window.)

Processes keeping only a manifest reference (like a writer position) run step 2 before reloading an evicted manifest from storage. If the manifest is missing on load, they re-read `boundary.json` and run cold discovery if `n <= boundary` (reporting corruption otherwise).

Replaying the WAL produces the "projected WAL tail" (metadata deltas plus inline pieces) and computes the view's effective [retention floor](glossary.md#retention-floor) as `max(manifest.retention_floor_seq, floor objects in tail)`.

#### Warm reads from a cached view

A warm read refreshes a cached view in strict order (serialized one refresh per namespace, with concurrent arrivals sharing the next refresh):

1. **WAL probe first (`strong` reads).** Read WAL numbers after the cached tip in concurrent windows (costing 1 GET when unchanged). Running the WAL probe before the successor probe ensures that a concurrent fold + GC cannot hide commits between a negative successor check and the WAL read. If a gap appears below a present object, restart full discovery.
2. **Successor probe (when due).** If `manifest_revalidation_interval_ms` (default 1 s, monotonic clock) has elapsed since the last manifest check, HEAD manifest `n + 1`.
3. **Boundary read.** If the successor probe finds nothing, conditionally read `boundary.json` (propagating read errors). If a successor exists or `n <= boundary`, restart full discovery.

An `eventual` warm read ([Read consistency](api.md#read-consistency)) skips probes while the tip confirmation (timestamped before the confirming request was sent) is younger than `max_staleness_ms` and the view's last manifest check is younger than `READ_REVALIDATION_BOUND_MS` (the last manifest check is the start of the last successor probe in the same runtime that found no manifest after the view's manifest or after an earlier manifest, or of the last publication in that runtime that published the view's manifest or found it current with a request) (ensuring referenced content survives until any issued download URL expires, [Content roots](#119-content-roots)). Cached views only advance monotonically. On namespace deletion, unknown mutation outcome, or fencing, the runtime drops the cached view and ignores in-flight loads started before the drop.

A create installs manifest 2 as its initial view, and a fork installs its active manifest (and only updates the source's view if it had to fold an aged source tail).

#### Warm reads from a local writer position

In a runtime with an active writer session, reads start from the session's [writer position](glossary.md#writer-position) (updated before acknowledging each commit, on local floor objects, and on local manifest publications). No WAL probe is needed because another writer must publish a claim manifest before writing commits (an external floor object changes only the floor, so reading the earlier floor until the next collision or fold is safe since compaction drops nothing until a manifest records it).

- `eventual` reads use the position without probing while the position's last manifest check is younger than `max_staleness_ms`. Past that (and always for `strong` reads), reads wait for a serialized successor probe started after the read began.
- If a successor exists, the reader walks newer active manifests with the same `writer_epoch` (up to 8 manifests ahead) as long as none folds past the position's WAL tip (`folded_wal_no <= position.tip`), advancing the position onto the newest one and keeping `max(position.floor, manifest.floor)`.
- If the walk hits a higher `writer_epoch`, a tombstone, more than 8 manifests, or a manifest folding past the position's tip (one that folded a WAL object the position hasn't read, such as another process's floor object), the reader drops the position and runs cold discovery. A writer MUST NOT keep a position whose manifest folds past the position's tip. The position's projected WAL tail and its count of unfolded WAL objects assume that the tip is at or above the manifest's `folded_wal_no`.
- After the probe/walk, the session conditionally reads `boundary.json` (regardless of position age) and drops the position for cold discovery if `n <= boundary`.
- When a healthy session closes, its position seeds the cached view so later reads stay monotonic (unless dropped due to successor, boundary, or fencing, in which case the cached view is also dropped).

### 4.3 Visible metadata

At sequence `N`, readers ignore events after `N`. An inode is visible at `N` only if it was created at or before `N` and a walk up its current bound parent bindings at `N` reaches the root inode without passing a tombstone, on the inode or on any ancestor, whose latest event at or below `N` is `set` ([File and subtree deletion](#16-file-and-subtree-deletion)). If the walk reaches an inode that has no bound parent binding before it reaches the root or a tombstone (a "disconnected" ancestry), the inode isn't visible. An inode that isn't deleted has bound parent bindings up to the root, and a deleted inode has them up to the inode that its deletion's tombstone is on, so a disconnected ancestry occurs only below an expired deletion, after a base compaction of the bindings group has removed the subtree's edges while other family groups still store its rows ([Row retention during a base compaction](#103-row-retention-during-a-base-compaction)). Reads that reach an inode without resolving a path apply this rule too, including a listing of the visible files at a sequence or in a snapshot, which scans the `inodes` family. For example, if `/old/a.txt` is below an expired deletion of `/old` and the bindings group no longer stores the edge from `/old` to `a.txt`, a file listing leaves out `a.txt` even though its inode row and revisions are still stored. A file listing that includes deleted files doesn't check visibility, so it returns `a.txt` with its newest stored revision until a base compaction removes the file's rows.

Edge, slot, and parent lookups take the first row at or below `N` (newest first, [Directory bindings](#13-directory-bindings)). Path resolution does one slot-index lookup per component, and directory listings scan the parent prefix once without reading child inode or file revision rows. Current content is the newest file revision `<= N`, and attributes are the newest attribute row `<= N` (or `{}` if none). Trash listings read `trash`, while historical tombstone evaluation reads `tombstones`.

### 4.4 Paths, listings, and revision reads

Paths resolve from the root inode by mapping each component to its name key ([Names and paths](#14-names-and-paths)) and following active bindings, so any invisible component means the path does not exist. Directory listings report file revision summaries from binding rows and MUST NOT fetch or verify file content objects.

Path-based revision reads resolve the current inode at that path first, while inode-based revision reads read the inode's history directly (including files inside trash deletions).

Before evaluating any access grant, an inode-addressed read (entry, revisions, attributes, or content) walks bound parent bindings upward to the first active tombstone:

- **Tombstone with `deletion_seq <= view.retention_floor_seq` (expired):** MUST return `inode_not_found`. Base compactions remove an expired subtree's rows one family group at a time, so without this refusal a read by inode ID under the deletion would return different results as each group compacts. The refusal also keeps rights from widening, because a base compaction can remove a hidden inode's access boundary while a grant above it is still stored, for example when a false positive of the kept inode filter keeps the grant (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)).
- **Tombstone with `deletion_seq > view.retention_floor_seq` (in trash):** Allowed, with rights evaluated through the tombstone's saved parent ([Access rows](#18-access-rows)).
- **Reaches root with no tombstone:** Allowed.
- **Reaches an unbound inode with no tombstone (disconnected ancestry):** MUST return `inode_not_found`, because the inode isn't visible ([Visible metadata](#43-visible-metadata)). This occurs after base compaction removes bindings under an expired deletion before removing its revisions.

Revoked tombstones are skipped during the walk. For example, if `/old/draft.txt` was deleted at seq 30 and `/old` at seq 40, with floor 35 an inode read of `/old/notes.txt` succeeds while `/old/draft.txt` returns `inode_not_found`. At floor 50 both return `inode_not_found`, even before or partway through base compaction.

### 4.5 Content verification

To read a `ContentRef`, a reader finds the longest visible layout for its chain (newest on tie, checking both tail rows and manifest segments) plus any visible WAL pieces for that chain:

| Condition | Byte source |
| --- | --- |
| Tail has pieces at or below `size_bytes` | Chain layout up to the lowest tail offset, then pieces up to `size_bytes`. |
| No covering pieces in tail | Chain layout up to `size_bytes`. |
| `size_bytes == 0` and no pieces | None, verify empty bytes with zero store requests. |
| Nonzero folded prefix with no layout | Corruption error identifying the content ID. |

When `size_bytes` is shorter than the layout, the final extent is truncated. Each extent is fetched with one ranged GET (no HEAD requests). Missing extents report the extent key, short ranges report expected vs. actual length, and full-file length or checksum mismatches report the reference key.

- **Whole-span verification.** Whenever a ranged GET covers an entire span object (`offset == 0` and full `end - start`), the reader verifies the bytes against the extent's object checksum (reporting corruption with the span key on mismatch). Partial slices of an object and whole-object extents (whose total object length isn't recorded in the extent) aren't checked against the object checksum, because object checksums cover whole objects, and whole-file reads always verify against the reference checksum.
- **Forks and WAL pieces.** Forks derive identical pool keys for inherited layouts. In-memory WAL pieces need no content GETs (envelope checks run at replay, not per-file checksums). If a view's WAL projection must be rebuilt and required WAL objects are missing, the read MUST fail even if content objects exist.
- **Streamed and direct reads.** Streamed reads MUST report full-file verification failures even after delivering bytes. Direct downloads issue one presigned range per extent, and clients verify length and checksum end-to-end ([API specification][api-spec]).

## 5. Uploading content

### 5.1 Upload sessions

Every uploaded content object belongs to an upload session whose `content_id` is allocated when the session begins. Both the content object and session records are stored under the namespace's [pool](glossary.md#pool) prefix (`pools/{pool_id}/`) and record `namespace_id`.

A session uses two write-once put-if-absent records rather than one mutated record (avoiding rapid same-key overwrites that hit 1 write/s provider caps, [Alternatives considered](#13-alternatives-considered)):

- **Session record (`uploads/{upload_id}.json`).** Written at creation with `namespace_id`, `upload_id`, `content_id`, `actor_id` (only that actor may use the session), tagged `mode`, and `expires_at_ms`.
- **Outcome record (`uploads/{upload_id}.outcome.json`).** Written once on completion or abort.

| Status | Records present | Meaning |
| --- | --- | --- |
| `open` | Session record only | Accepts uploads strictly before `expires_at_ms`. |
| `completed` | Outcome record with `completed_at_ms` and `content_ref` | Object verified, commits may admit `content_ref`. |
| `aborted` | Outcome record with `aborted_at_ms` | Terminal, cannot complete or reopen. |

```text
+------+   complete (content verified)   +-----------+
| open | ------------------------------> | completed |
+------+                                 +-----------+
   |
   | abort, or cleanup after expiry
   v
+---------+
| aborted |
+---------+
```

Completion verifies bytes before creating the outcome record, while abort creates the outcome record before deleting the object or provider upload. Put-if-absent ensures whichever finishes first wins. After an unknown-outcome put, the caller reads the outcome record back and won if it matches what it sent.

Because GC deletes the session record before the outcome record ([Upload-session cleanup](#116-upload-session-cleanup)), a request MUST check that the session record still exists after writing an outcome record, and MUST NOT return admission evidence or delete content if the session record is gone (preventing a paused request from reviving a cleaned-up session).

At or after `expires_at_ms`, staging, part signing, and completion reject the session as aborted (status reads and aborts ignore expiry). Upload records never advance namespace sequence or emit change-feed events, and completing an upload does not change any file until a commit publishes it.

### 5.2 Transfer modes

Each session selects one immutable transfer mode at creation:

| Mode | Transfer path | Stored mode metadata |
| --- | --- | --- |
| `service_proxied` | Client sends bytes to server, server writes content object | None |
| `direct_put` | Client writes whole object via presigned PUT | `checksum_algorithm` |
| `direct_multipart` | Client uploads parallel parts via presigned URLs | `provider_upload_id`, `part_size_bytes`, `checksum_algorithm` |

Completed outcome records MUST match the session's `content_id` and (for direct modes) its `checksum_algorithm`. Missing required fields or contradictions indicate corruption.

### 5.3 Service-proxied staging

In `service_proxied` mode, staging writes bytes straight to the session's content object without writing a separate staging record (the stored object itself provides verified size and checksum via HEAD). Concurrent requests to the same session write the same key: [Immutable writes and retries](#32-immutable-writes-and-retries) accepts matching size and checksum as an idempotent retry and rejects different bytes.

While streaming, the server computes the store's checksum. The server checks that the session is open before the write, and checks again after the write before returning `content_ref` (since a concurrent abort deletes the object). Completion verifies the object with one HEAD and writes `content_ref` into the outcome record (failing and leaving the session open if nothing was staged).

### 5.4 Direct uploads

Direct transfers use short-lived presigned URLs scoped by the server to the allocated content key (clients never choose keys). Providers MUST enforce signed create-only constraints and expose the stored whole-object checksum (`direct_put` uses the session's chosen algorithm, and `direct_multipart` uses CRC-64/NVME).

At completion, the server verifies the client's claimed size and checksum against the stored object via provider metadata (the stored object is the sole proof of success, regardless of the assembly response). On mismatch, the server rejects the claim leaving the session open and object untouched. Uncompleted sessions end via abort or lease expiry ([Upload-session cleanup](#116-upload-session-cleanup)). Provider adapters normalize all checksums into LoonFS's canonical representation.

### 5.5 Admission proofs

Commits referencing uploaded content require "admission evidence" bound to the namespace and full `ContentRef` (proving the bytes were verified in a completed upload). Inline content needs no evidence ([Operations and WAL deltas](#66-operations-and-wal-deltas)).

Evidence expires so GC can eventually reclaim completed but unpublished uploads ([Upload-session cleanup](#116-upload-session-cleanup), [Upload admission](#c2-upload-admission)):

- A completed session issues new evidence only during `COMPLETED_UPLOAD_RECEIPT_WINDOW_MS` after `completed_at_ms` (status reads never extend this window).
- HTTP signed content tokens last `CONTENT_RECEIPT_TTL_MS` from issuance (never past the admission window). Embedded runtimes need not sign tokens.
- In-process evidence expires `COMPLETED_UPLOAD_ADMISSION_WINDOW_MS` after `completed_at_ms`.

Immediately before publishing, the writer verifies matching, unexpired evidence for every uploaded `ContentRef` using request clock plus monotonic elapsed time since batch start (so retries never extend expiry, [Publication and group commit](#63-publication-and-group-commit)). Retained commit-receipt retries replay without new evidence, internal copies/restores reuse existing namespace references, and merges reuse fork references.

## 6. Publishing a commit

A metadata commit becomes visible when put-if-absent creates its numbered WAL object. Upload completion and validation run beforehand and commit nothing on their own.

### 6.1 Writer ownership

A writer session claims authority lazily before its first metadata-changing publication, moving through three states (using monotonic epochs rather than leases so a new writer can take over in ~200 ms without waiting for a lease timeout):

```text
                                 +--- unknown put outcome ---+
                                 v                           |
+-------+  claim published   +-------+  fence written   +----------+
| Claim | -----------------> | Fence | ---------------> | Acquired |
+-------+                    +-------+                  +----------+
                                 |                           |
                                 +-------------+-------------+
                                               | a higher epoch is found
                                               v
                                    returns writer_fenced
```

1. **Claim.** Starting from a cached manifest (if no writer has claimed yet, such as a freshly created or forked namespace) or loading the current manifest, the session publishes manifest `n + 1` with `writer_epoch + 1` and a diagnostic writer block (`writer_id`, `acquired_at_ms`, since only the epoch determines authority), followed by the mandatory boundary read ([Publishing a manifest](#72-publishing-a-manifest)).
   - Runs under `METADATA_PUBLICATION_BUDGET_MS` from acquisition start, checked right before the claim's put (failing without a put if the budget has expired).
   - On collision, reloads the current manifest and retries with the next epoch.
   - On unknown outcome, reads back manifest `n + 1`. If that manifest is the session's own claim, the claim succeeded. If it's another manifest, the session claims again, and if it's missing, acquisition fails.
2. **Fence.** Discovers the WAL tip (without pre-checking the manifest, since the post-put check validates it). If the current manifest now has a higher epoch than the session's, returns `writer_fenced`. Otherwise it writes an empty fence WAL object (`prior_head_seq == head_seq`, no commits) with its epoch at `tip + 1`.
   - After the fence put succeeds, runs the manifest successor probe + boundary read ([Replaying the visible WAL](#42-replaying-the-visible-wal)). If the newest manifest has a higher epoch, returns `writer_fenced`. Otherwise the fence is live if its WAL number is above the `folded_wal_no` of the newest manifest after the put, and dead if not. The same rule applies to a floor object that a process without the writer epoch creates (see [Advancing the retention floor](#101-advancing-the-retention-floor)). When the loaded manifest is still the newest, the fence is live without loading another manifest, because discovery found the tip above that manifest's `folded_wal_no`. A fence is dead when the previous writer committed between claim and fence and a fold + GC emptied that WAL number, or when a concurrent fold covered the live fence, and the session then writes another fence at the new tip.
   - Collisions and dead-fence retries continue until `METADATA_PUBLICATION_BUDGET_MS` expires. Transport failures abort acquisition. The next attempt of a first acquisition claims a new epoch, and the next attempt of a session that owes a fence (step 3) writes the fence again with the epoch it holds.
3. **Acquired.** Uses its epoch for all later commits, causing older writers to collide. Any discovery encountering a higher epoch returns `writer_fenced` terminally without auto-reacquiring (checked against the manifest before reading any WAL tail). From the moment the session sends a WAL put until it learns the put's outcome, the session "owes a fence", which means that it has to write a fence with its own epoch before it plans another commit. If it doesn't learn the outcome, it returns to step 2 with the epoch it holds, so its next publication writes that fence before it plans a commit (see [Failed and unknown outcomes](#64-failed-and-unknown-outcomes)). The fence's discovery returns `writer_fenced` if another writer has claimed a higher epoch. The session doesn't claim a new epoch for this fence, so it can't be fenced by a claim of its own.

### 6.2 Validation

Before planning mutations, the writer reconstructs state from a verified basis and visible WAL, evaluates operations in order within each request (and across requests in a group-commit batch), validates content references and admission proofs before metadata preconditions, checks preconditions, and generates IDs for newly created inodes ([Namespaces and identity](#11-namespaces-and-identity), while [merges](glossary.md#merge) keep the fork's inode IDs).

Metadata preconditions can assert namespace head sequence, name availability, exact binding version (`binding_is(parent_inode_id, name_key, child_inode_id, committed_seq, delta_index)`, because checking inode ID alone would miss a move away and back), file/attribute/access revisions, ancestor visibility, and directory emptiness. Revision preconditions paired with optional inode preconditions require the inode precondition, inode-addressed revision writes require `expected_revision_no`, and inode-addressed moves/deletes require `expected_binding_version`. A rejected request gets no sequence number and creates no WAL record (validation can still materialize unfolded base pieces into a span object before a later operation fails, leaving any unreferenced span for pool garbage collection). Restore and merge-floor validation use the writer position's effective [retention floor](glossary.md#retention-floor), including any [floor objects](glossary.md#floor-object) in its WAL tail.

#### Merges

A [merge](glossary.md#merge) applies a contiguous range of a fork's commits `(source_after_seq, source_through_seq]` once and in order:

- **Range continuity.** Checking commit receipts first (so retries replay cleanly), the writer checks each range that the merge record covers: the merge's own range, and each range in `covered_ranges`. A range's `source_after_seq` MUST equal the `source_through_seq` of the newest row in `merges` for the range's fork, returning `merge_range_conflict` otherwise. If no row exists for the merge's own source, its range MUST start at the fork point. If no row exists for the fork of a covered range, the writer doesn't check where that range starts, because the namespace would have to read that fork to learn its fork point. No merge in the namespace's merge index covers that fork, so no row can contradict the range. The merge that commits first writes the row that every later range of that fork is checked against. Unlike other commits, a merge with an empty net change MAY have zero operations so `merges` still advances.
- **Covered ranges.** A merge of a [rebase](glossary.md#rebase)'s new fork also covers the commits of the fork that the new fork was rebased from, because the new fork's replay commits applied them. A replay commit is a merge into the new fork whose source is the new fork's `fork_basis.rebased_from`. For each fork whose commits the replay commits in the merge's range cover, the merge record's `covered_ranges` has one range, from the smallest `source_after_seq` to the largest `source_through_seq` of that fork's ranges in those replay commits. A replay commit's own record covers ranges in the same way, so when a new fork is itself rebased, the next new fork's merge covers both earlier forks. A range counts as covered even when its replay commit skipped conflicting changes, because the rebase reported those changes to the resolver. For example, if the new fork N replayed fork F's commits above 4 through 17, a merge of N whose range includes that replay commit records `covered_ranges: [{"source_namespace_id": "F", "source_after_seq": 4, "source_through_seq": 17}]`, and F's next merge starts after 17.
- **Retention floor.** Because base compaction removes `merges` rows strictly below `retention_floor_seq`, the fork point MUST be `>=` the writer position's retention floor (returning `below_retention_floor` otherwise) so prior merge rows cannot vanish and allow duplicate merges. Similarly, if the merge's range starts below the fork's own retention floor, the planner returns `below_retention_floor`.
- **Rebase replay commits.** When merging into a [rebased](glossary.md#rebase) fork whose `fork_basis.rebased_from` matches the source fork, the new fork inherits parent `merges` rows at or below its own fork point. Those rows record merges of the source fork into the parent, not replay commits into the new fork, and `resume_after_seq` already records where they end. Therefore: (a) range continuity considers only the new fork's own `merges` rows above its fork point, starting from `resume_after_seq` if none exist, and (b) the floor rule compares the new fork's own `fork_point_seq` against its retention floor.

A merge MUST NOT overwrite rows the fork didn't see. The planner computes the fork's "net change" across `(source_after_seq, source_through_seq]` by comparing each touched inode's binding row (parent + name, not visibility), content reference, attributes, and access row at both endpoints, omitting any row whose value already equals the target's. (If a deletion hides a touched inode at `source_through_seq`, the merge deletes it in the target unless already hidden by an unrevoked deletion or an ancestor directory deleted by the same merge.)

A target row is "the fork's own" if its `committed_seq <= fork_point_seq` or its commit record is an earlier merge from the same fork (a merge whose own source is the fork, not one that covers the fork's range in `covered_ranges`). All other rows are "foreign". For a rebase replay commit, `fork_point_seq` is still the source fork's fork point on the parent, not the new fork's. The planner reads the parent's history up to the new fork's fork point (the commit records of rows committed at or below that point, and check 3's diff up to that point) through the new fork's [fork point snapshot](#fork-snapshots-and-fork-point-snapshots), and reads the history above the new fork's fork point from the new fork. (The new fork's base compaction can remove the commit records that the new fork inherited from the parent, because the new fork's floor starts at its fork point. Judging ownership by the new fork's fork point would count the parent's changes after the source fork's fork point as the fork's own, and the replay commit would overwrite them with no conflict reported.) Conflict checks:

1. **Existing target inode.** If hidden by a tombstone on itself at the target head, that tombstone MUST be the fork's own and still in the target's trash (planned as a restore). For moves, restores, and inodes hidden by an ancestor deletion, walking upward from the relevant parent in the target MUST reach a visible directory (or one created/restored by this merge) before hitting an active tombstone (`visibility` conflict otherwise, except deletes of already-hidden inodes, which are omitted). Every replaced row (revision, binding, attributes, access, or tombstone) MUST be the fork's own. Destination names MUST be free (unbound, or vacated by a delete/move in the same merge, noting that items inside a restored directory remain bound unless moved/deleted).
2. **Fork-created inode.** Planned with the fork's inode ID. The parent must pass the ancestor check and destination name must be free (creates/restores parents before children). If an earlier merge already created the inode, case 1 applies.
3. **Deleted or moved directory.** The planner diffs the target from the fork point to head over that directory, and every changed row in that diff MUST be the fork's own (`subtree` conflict otherwise).

Any failing inode causes the planner to reject the merge before submitting. For conflict-free merges, the planner submits current target values as preconditions (plus `namespace_head` at the planned head whenever deleting or moving a directory so concurrent child additions fail the commit). The writer enforces the range/floor rules. A merge creates an inode under the ID that the fork's create gave it. Among the namespaces that can merge into the target, only the fork and the new forks that rebases replayed the fork's commits into can have that ID while the target doesn't, and every merge of one of them covers a range of the fork that starts where the target's newest row for the fork ends. The range rule admits one merge for each such start, so a merge that creates an already-existing inode ID is a bug in the planner, which the writer MUST reject.

### 6.3 Publication and group commit

```text
upload and verify content (uploaded path only)
            |
            v
validate requests A, B, C against current view
            |
            v
put-if-absent WAL 10: [seq 41, seq 42, seq 43]   <- commit boundary
            |
            v
acknowledge
```

To publish a batch ("group commit" bundles multiple requests at contiguous sequences inside one WAL object while keeping each request a separate commit):

1. Resolve retained `commit_id` receipts, then validate remaining requests sequentially against the current view and earlier accepted requests in the batch.
2. If the unfolded WAL is at a write-stop threshold (bytes or objects, counting fences and floor objects), pause new commits in the queue until a fold frees room. Requests waiting past the [write-stop wait](#73-recovery-material-and-maintenance-policy) fail with `maintenance_required`.
3. Assign contiguous sequences and build WAL object `tip + 1`.
4. Re-verify content-admission evidence immediately before put-if-absent. If any request's evidence expired, return that request's error, return `stale_head` to the other requests for re-planning, and write nothing.
5. On put-if-absent success, advance the [writer position](glossary.md#writer-position) and acknowledge.

On a confirmed precondition failure, the writer writes nothing, discovers the winning object, checks for fencing, and re-plans. If the collision was a [floor object](glossary.md#floor-object) (which adds no epoch increase), the writer isn't fenced and re-plans cleanly against the new floor.

Because GC never deletes fences or floor objects ([Candidate and age rules](#113-candidate-and-age-rules)), an acquired writer can put at `tip + 1` regardless of how long ago it observed `tip`. When the session observed `tip`, nothing was at `tip + 1`, and GC deletes only folded commit objects, so `tip + 1` could be an emptied commit number only if a commit was written there after that observation. That commit would have to be written by one of the following, and none of them can write it there:

- **An older writer.** It can't get past this session's fence, because it would have to write at the fence's number, which is taken, or run discovery, which finds the fence or the newer epoch and fences it.
- **A newer writer.** It commits only above its own live fence, and that fence is at or above `tip + 1`, because this session would otherwise have collided with it or found it by discovery.
- **This session's put whose outcome was known.** The session publishes one WAL object at a time. A put that succeeded moved its tip, and a put that collided wrote nothing.
- **This session's put whose outcome is unknown.** That put may still be in flight at the provider when the session plans its next number, so the session [owes a fence](glossary.md#owes-a-fence), and it writes that fence with the epoch it holds before it plans a commit ([Failed and unknown outcomes](#64-failed-and-unknown-outcomes)). Say the put in flight was sent to WAL number `N`. The fence's discovery ran after that, so once the check after the fence's put finds the fence live, the fence is at `N` or above. If the fence is at `N`, the put in flight collides with it, because GC never deletes a fence. If the fence is above `N`, the discovery found an object at `N`. Either that object is the put in flight, which the session's view replays, or the put in flight collides with that object, or a collection later empties `N` and the put in flight is written there, below the fence, where no reader replays it. If the put was written and collected before the fence's put, the check after the fence's put finds the fence dead, and the session writes another fence at the new tip ([Writer ownership](#61-writer-ownership)).

Thus `tip + 1` can only be occupied by a fence or floor object, never an emptied commit number. (Fences and non-writer floor objects don't enjoy this invariant and therefore verify their position after putting.)

Why does the session write a fence rather than wait, or claim the next epoch? The only limit on when the put in flight can still be written is `PROVIDER_PUBLICATION_REQUEST_BOUND_MS`, so waiting would add that bound to every timeout and make WAL puts depend on it for ordering safety. A claim of the next epoch would also block the put, but a session can't tell its own claim from another writer's, because the host sets the writer ID, not the session. So if the fence put after such a claim also had an unknown outcome, the session's next attempt would find its own claim and fence itself. A fence with the session's own epoch needs no claim. It costs one small WAL object that GC never deletes and that counts toward the unfolded WAL thresholds until a fold covers it.

### 6.4 Failed and unknown outcomes

A transport error after sending a WAL put MUST be reported as unknown (`outcome_unknown`), never as a definite failure.

From the moment a writer session sends a WAL put until it learns the put's outcome (the put succeeded, collided, or failed definitely), the session [owes a fence](glossary.md#owes-a-fence), and it MUST write that fence with the writer epoch it holds before it plans another commit (see [Writer ownership](#61-writer-ownership), and [Publication and group commit](#63-publication-and-group-commit) for why). The session records that it owes a fence before it sends the put, so it still owes the fence when a caller abandons the publication while the put is in flight, or when the task that sent the put fails. The same holds for a floor object that the session puts from its publication queue. The reference implementation keeps one such record for each session, which relies on the rule that a session publishes one WAL object at a time ([Publication and group commit](#63-publication-and-group-commit)), and its runtime publishes through one engine per session. A host that sends puts from two engines of one session at the same time isn't protected, because a known outcome of one put clears the record that the other put set. The fence goes through step 2 of [Writer ownership](#61-writer-ownership). Its discovery returns `writer_fenced` if another writer has claimed a higher epoch, so fencing stays terminal for the session, and a fence put whose own outcome is unknown leaves the session still owing a fence.

An implementation may auto-retry with the exact same `commit_id` and request (checking the commit receipt after the fence's discovery, where only success or receipt replay resolves the outcome, and any other error still reports `outcome_unknown`). If the earlier put was written before that discovery, the receipt check replays it. Otherwise the earlier put either collides with the fence or is written below it, where no reader replays it, so the retry can't commit the request twice.

Warning: when retrying a commit, never re-upload the file. A fresh upload allocates a new content ID, changing the commit fingerprint and causing a retry of an already-committed write to fail with `commit_id_reuse_conflict`.

Within a group-commit batch, a validation failure caused by an earlier tentative request in the same batch can only be reported as a validation error if that earlier request actually publishes. If the WAL put fails, dependent rejections MUST receive the publication error instead. Rejections based solely on already-durable state stand on their own.

### 6.5 Commit identity and retries

Every WAL commit and `commits` row stores a `semantic_commit_fingerprint` hashing `(namespace_id, actor_id, ordered operations with inline preconditions, request-level preconditions, optional message)` while excluding publication details like writer epoch and timestamp ([Appendix B](#appendix-b-semantic-commit-fingerprints)). For [merges](glossary.md#merge), the fingerprint hashes the merge record (`source_namespace_id`, `source_after_seq`, `source_through_seq`) instead of planned operations so planner changes never alter retry identity ([Merge commits](#b5-merge-commits)).

Each `commit_receipts` row maps `commit_id` to `committed_seq`, pointing into `commits` (compacted together with `merges` in one family group). While a receipt is retained:

- Same `commit_id` + equal fingerprint replays the original `Commit` from the `commits` row without re-running mutations, re-evaluating preconditions, or re-uploading inline content (even across restarts, different servers, or at the WAL write-stop threshold).
- Same `commit_id` + different fingerprint returns `commit_id_reuse_conflict`.
- Inline and appended content are fingerprinted by raw bytes (retrying identical inline bytes replays even if a new content ID is assigned).

Receipt lookups use the publisher's current projection (refreshed on collision or external manifest successor, while local folds advance the projection without a refresh). Once compaction drops a receipt below `retention_floor_seq`, reusing that `commit_id` executes as a new commit. Callers MUST NOT rely on uncompacted receipts below the floor. Keeping old file revisions does not require keeping commit receipts.

### 6.6 Operations and WAL deltas

Standard operations act on paths or inode IDs ([Appendix B](#appendix-b-semantic-commit-fingerprints)):

| Setting | Default |
| --- | --- |
| Destination behavior (puts, moves, copies) | `no_replace` |
| Deletes | `non_recursive` |
| Directory creation | `parents: false` |
| Attribute `set` and `remove` collections | Empty |

Optional preconditions have no default. Replacing moves delete the destination file and rebind the source atomically (directories can't be replaced, and a path never replaces itself). Restores bind under the saved original parent/name, a new path, or a `(parent_inode_id, display_name)` pair.

#### Appends and chain ownership

An append writes appended bytes as WAL pieces and combines base + appended CRCs algebraically in the store's algorithm without reading prior bytes (`namespace_corrupt` if the base uses a different algorithm):

- **Same chain.** Keeps the same `content_id` (no layout in delta, no content write) only when this namespace is the chain's [creator](#15-file-contents-and-ownership), base size `> 0`, and base equals the chain's full current length. Limiting in-place extension to the creator ensures every layout of a chain is a prefix of its longest layout even after cross-member merges.
- **New chain.** Otherwise, allocates a new `content_id`. For an empty base file (base size `0`), the commit writes a whole-value piece at offset `0` and omits `layout` (the WAL decoder rejects a `layout` when a piece starts at offset `0`). For a non-empty base (branched revision or another pool member's chain), the delta embeds the base layout truncated to the append offset with this namespace as creator. If that base has unfolded pieces, validation materializes them into a span object first (missing base bytes report corruption).

Every append's `append_file_revision` delta records `append_offset` (equal to the base revision's `size_bytes`) so the change feed reports the exact offset both before and after folding (when pieces are stripped from `commits` rows, and even when `append_offset == 0`). Non-append revisions omit `append_offset`.

#### WAL delta kinds

The WAL stores eight resulting delta types (each with `delta_index` and `semantic_operation_index`, with actor and timestamp stored once per commit and copied on fold, full schemas in [Appendix A](#appendix-a-durable-records-and-byte-encodings)):

| Delta | Records |
| --- | --- |
| `create_inode` | New inode. |
| `bind_direntry` | New directory binding (including file revision summary for files). |
| `unbind_direntry` | Removed directory binding. |
| `append_file_revision` | New file revision plus current binding (for writing the slot-index summary row). |
| `tombstone_subtree` | Deletion (`set` event) plus `root_kind` for `trash`. |
| `revoke_subtree_tombstone` | Restore (`revoke` event). |
| `append_attributes_revision` | New attribute revision. |
| `append_access_revision` | New access revision. |

### 6.7 Change feed

The change feed orders events by `committed_seq` (not WAL object) and operation order within each commit, reading folded commits from `commits` followed by the unfolded WAL tail. Fences and [floor objects](glossary.md#floor-object) emit no events. Merges include their merge record (on [rebases](glossary.md#rebase), the merge record covers the examined range while the conflict report lists skipped changes). Cursors below the view's [retention floor](glossary.md#retention-floor) MUST re-bootstrap from a fresh listing or snapshot ([API specification][api-spec]).

Consumers needing permanent event history MUST persist it before the floor advances, keying commits by `(namespace_id, committed_seq)` and items by `(namespace_id, inode_id)` (not `commit_id`, which can be reused after receipt compaction).

## 7. Folding the WAL

A "fold" writes WAL tail records into immutable sorted segments and publishes the next manifest so later reads replay fewer WAL objects. Folding changes physical storage only (allocating no sequence number and creating no snapshot).

### 7.1 Manifests, runs, and segments

A manifest represents complete metadata through its "folded head" (`head_seq`) and `folded_wal_no`, listing all required metadata "runs". Each run gets a unique `run_no` from `next_run_no` (1 per fold across all written families, 1 per compaction for its family group) and records `run_seq`, `tier`, and segment descriptors. `base_seq` is the minimum `run_seq` among non-empty runs (or `head_seq` if empty).

Invariants:

- A non-empty manifest MUST have a non-empty run at `head_seq`, and no run may exceed `head_seq`. Every segment contains at least one row.
- No duplicate `run_no` and no `run_no >= next_run_no` (extension runs have independent numbering, [Extensions](#124-extensions)).
- Within a run, each family's segments are in ascending non-overlapping key order with no duplicate logical keys.
- In every run, `direntry_binds` row count `>=` `direntry_child_binds` row count (binding events agree row-for-row during compaction, while the slot index adds summary rows), and `commits` row count `==` `commit_receipts` row count.

### 7.2 Publishing a manifest

Manifest 1 values at namespace creation:

| Field | Plain create | Fork |
| --- | --- | --- |
| `manifest_no` | 1 | 1 |
| `namespace_id`, `created_at_ms`, `created_by` | From request | From request |
| `retention_window_ms` | Request value, else host default (if set) | Copied from source manifest |
| `status` | `initializing` (manifest 2 sets `active`) | `initializing` (manifest 2 folds source tail if non-empty, next manifest sets `active`) |
| Writer block | None | None |
| `writer_epoch`, `compactor_epoch`, `folded_wal_no`, `activity` | 0 | 0 |
| `fork_basis` | None | Source manifest ref, fork point, source fork snapshot ID, source user snapshot ID, fork point snapshot ID |
| `pool_id` | Newly generated | Copied from source manifest |
| `publication_id` | Newly generated by the put | Newly generated by the put |
| `runs`, `next_run_no`, `extensions` | Empty / 0 / None | Copied from source manifest |
| `head_seq`, `retention_floor_seq`, `base_seq` | 0 | Source manifest's `head_seq`, `retention_floor_seq`, and derived `base_seq` |

#### Fold protocol

Any process can fold as the namespace's [maintainer](glossary.md#maintainer) under the current `compactor_epoch` without claiming either epoch (covered by one `METADATA_PUBLICATION_BUDGET_MS` deadline checked before the manifest put so output segments cannot age past grace before publication):

1. Start from a verified manifest and discovered WAL tip (or the writer's retained view).
2. Validate every chain's ordered tail pieces up to its newest reference (missing pieces are corruption, and verify offset-0 whole values before writing).
3. Build and write each chain's new extent under `pools/{pool_id}/content/` (rules below).
4. Write metadata segments (including new `content_layouts` rows and each segment's stored checksum). A tail containing only fences and floor objects folds into a manifest with no new run.
5. Publish manifest `basis + 1` with put-if-absent, setting `folded_wal_no = tip` and `retention_floor_seq = max(predecessor.floor, covered floor objects)`. (Apart from a fork's active manifest, only folds raise a manifest's floor.)

#### Basis, collision, and boundary checks for every manifest publication

Every manifest publication starts from a basis manifest in memory (checking `boundary.json` first if reloaded from storage, or running cold discovery if no basis exists) and puts `basis + 1` directly:

- **Lost put (collision):** Probe `basis + 1` with HEAD, then probe an initial window of `MANIFEST_PROBE_WINDOW` (8) manifests and, if all exist, gallop exponentially and narrow the bracket in parallel batches until finding the highest existing manifest, then read `boundary.json` (falling back to cold discovery if `<= boundary`).
- **Succeeded put:** Read `boundary.json` conditionally (plain read for manifest 1). This final check catches any put that raced into a manifest number GC had already emptied:
  1. If `manifest_no > boundary` (or no boundary exists), publication succeeded.
  2. If `manifest_no <= boundary`, treat the put as lost and run cold discovery (a `retired` boundary returns `namespace_deleted`).
  3. If the boundary read fails, return `outcome_unknown`.
- **Fold retry after lost put:** Complete immediately if the winning manifest already covers `tip` (`folded_wal_no >= tip`, `head_seq >= fold.head_seq`, `manifest_no >= fold.manifest_no`). Otherwise rebuild on the winning predecessor, reusing the already-read WAL tail (and already-written layouts if at the same head) when the predecessor changed only in number, compactor epoch, floor, retention window, runs, next run number, or extensions, reloading the tail after any other manifest. If another writer claimed the namespace, the fold still folds the new writer's log, while the old session's writer position refuses to advance onto it.

A publisher MUST publish only a manifest that keeps these rules against its basis:

- Its manifest number is the basis's number plus one.
- Its `namespace_id`, `created_at_ms`, `created_by`, `fork_basis`, and `pool_id` are the basis's. (`retention_window_ms` can be set, changed, or cleared.)
- Its `folded_wal_no`, `writer_epoch`, `compactor_epoch`, `head_seq`, `retention_floor_seq`, `next_run_no`, and each activity counter are at least the basis's values, and if its `head_seq` equals the basis's, its activity is the same.
- Its basis isn't a namespace tombstone.
- If the basis isn't `initializing`, the manifest isn't `initializing` either.
- If the basis is `initializing`, the manifest is one of three: a fork's initializing manifest 2 that folds the source's WAL tail ([Forking a namespace](#92-forking-a-namespace), step 4), the basis's activation, or the basis's namespace tombstone. The activation is the basis with the status set to `active`, and for a fork, with the retention floor raised to the fork point ([Forking a namespace](#92-forking-a-namespace), step 7). The tombstone is the basis with the status set to `deleted` with its `deleted_at_ms`, and with the runs of each extension entry emptied ([Extensions](#124-extensions)). Neither changes any other field except the manifest number and `publication_id`.

#### Extent assembly and the doubling rule

For each chain with tail pieces, the fold constructs one new "candidate" extent from the prior layout plus ordered tail pieces:

1. Start the candidate with the ordered tail pieces (not joined in memory), skipping any pieces already covered by validation-written own extents (if those cover all pieces, start with the last own extent).
2. If folding in a namespace other than the chain's creator, write only the tail pieces as a single extent (skipping steps 3-4 so the creator's bytes aren't duplicated in objects listed only by another namespace).
3. **Doubling rule:** While the chain's last combinable extent has length `<= 2 * candidate.length`, merge that extent into the candidate (bounding extent count logarithmically while copying each byte roughly once per size doubling). A combinable extent is an own extent, or, when the fold runs in the chain's creator, a shared base extent whose object is a span and that takes the whole of that span. A shared base extent that takes part of its object, or whose object is a whole object (the layout records a span's bounds but not a whole object's length), is never merged, and it stops the rule.
4. **32-extent cap:** Count the combinable extents after the last extent that the fold can't combine. If more than `MAX_LAYOUT_EXTENTS` (32) of them would remain, keep the longest prefix of them, of at most 31 extents, in which each extent is at least as long as every extent after it and the candidate together, and merge every extent after that prefix into the candidate. For example, with extents of 200, 100, and 31 of 3 bytes and a 1-byte candidate, the fold keeps 200 and 100 and writes one 94-byte extent.

Each candidate is written via one put-if-absent assembly to a fresh random [span object](#15-file-contents-and-ownership) ID (except 0-byte chains, which write an empty whole object, and HEAD checks each copied source's stored checksum against the extent's object checksum, reporting `namespace_corrupt` on mismatch). Random span IDs let two pool members materialize the same pieces concurrently without conflict (at the cost of storing at most one tail's pieces twice while both layouts exist). A creator's fold may also copy shared base span extents that take their whole objects into a span object that the chain's layout lists, and those bytes are then stored twice in the pool while both layouts are listed. New layouts copy the chain's `creator` (existing layout creator, or the WAL object's `namespace_id` for new chains) and replace tail layouts in the fold's delta run.

Content writes from folds, validation, merges, direct downloads, and proxied staging share `max_concurrent_content_writes` (default 32), and `STORE_WRITES_IN_FLIGHT` caps concurrency per fold or merge.

### 7.3 Recovery material and maintenance policy

Once folded WAL objects are deleted, metadata segments are required recovery material, so missing or corrupt segments fail reads rather than silently falling back.

Reference implementation folding defaults (not format requirements):

| Trigger | Reference default |
| --- | --- |
| Request a fold | 8 MiB unfolded WAL bytes, or 256 unfolded WAL objects |
| Stall new commits waiting for a fold (write-stop) | 64 MiB unfolded WAL bytes, or 1,024 unfolded WAL objects |
| Reject stalled commit with `maintenance_required` (write-stop wait) | 30 s from queue entry |
| Idle tail fold | Tail has a commit and newest `committed_at_ms` is >= 15 min old on maintenance clock |

Fences and [floor objects](glossary.md#floor-object) count toward WAL object counts (not byte counts or idleness). The commit crossing a threshold may finish first, and reads plus retained-receipt replays continue during write-stop. Folding never advances retention on its own beyond floor objects already in the tail. See [Appendix C](#appendix-c-timing-and-size-reference) for sizing defaults.

### 7.4 Statistics

Each manifest records three cumulative counters in `activity`:

| Counter | Counts |
| --- | --- |
| `content_bytes` | Full length of every committed file revision (for inline/appended revisions, counts only the piece bytes added once). |
| `file_revisions` | Every committed file revision (including empty ones). |
| `mutations` | Each `semantic_operation_index` group in committed WAL records. |

Folds add only activity after the predecessor's position (re-counting on collision). Uploads, fences, floor objects, snapshots, content materialization, compaction, retention, deletion, and recognized retries add nothing, and all publishers copy counters forward. Forks start with zero activity counters (folding a source WAL tail during fork adds no activity). A counter above the API integer bound is an error.

Footprint metrics derived from the manifest alone (zero extra reads, extension segments excluded):

- `inode_record_count`: sum of `row_count` across `inodes` segments (includes retained deleted rows, implicit root counts as 0).
- `metadata_stored_bytes`: sum of `index_block.offset + index_block.stored_bytes` across all listed metadata segments (shared segments count in every referencing manifest).
- `manifest_bytes`: stored compressed size of the manifest object.
- `segments_by_family`: descriptor count per metadata family (0 when absent).

Observations also include namespace identity, status, `manifest_no`, `head_seq`, and `folded_wal_no`. Snapshot statistics read the snapshot's manifest.

## 8. Snapshots

A "snapshot" pins one manifest and all of its listed segments so GC keeps that view readable after newer manifests publish. Ordinary folds create no snapshots.

### 8.1 Records and owners

Stored at `snapshots/{snapshot_id}.json` (high 64 bits of ID = `manifest_no`, low 64 bits = random, so repeated snapshots on one manifest get unique IDs). Each record stores `namespace_id`, `head_seq`, `payload_checksum`, `created_at_ms`, and owner fields (no lifecycle status field, since creating the record creates the snapshot and deleting it removes it):

| Owner | Owner fields | Readability | Removal |
| --- | --- | --- | --- |
| `user` | Optional `name`, optional `expires_at_ms`, optional `deleted_at_ms` | Readable while undeleted and before `expires_at_ms` (indefinitely if no expiry). | Deleted by GC after `expires_at_ms + grace` or `deleted_at_ms + grace`. Explicit delete stamps `deleted_at_ms` via CAS. A failed creation also stamps it, via CAS or, when the key is empty, put-if-absent ([Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)). Live-head forks, merges, and rebases also create temporary unnamed user snapshots expiring after `OPERATION_SNAPSHOT_TTL_MS` (5 min). Live-head forks let theirs expire for GC. |
| `fork` | `target_namespace_id` | Readable while record exists (source fork snapshot pins inherited runs, and fork point snapshot pins the fork's manifest at the fork point). | Deleted during the target's [retirement](#118-retiring-an-eligible-tombstone) (no expiry or renewal). |

Expired or deleted user snapshots are unreadable via normal APIs but remain in the maintenance snapshot inventory and continue protecting their manifests until GC removes them ([Snapshot cleanup](#117-snapshot-cleanup)).

### 8.2 Creating and verifying a snapshot

Creating a user snapshot from the live head:

1. Fold the observed WAL tail as [maintainer](glossary.md#maintainer) to obtain a verified current manifest (or skip folding when taking a no-fold snapshot from a discovered manifest + boundary, as live-head forks do for their temporary snapshot).
2. Write the snapshot record with put-if-absent, checking `METADATA_PUBLICATION_BUDGET_MS` before every put, including the put of a later attempt after a put whose outcome was unknown. A read-back of a put isn't checked, so a put that was applied is confirmed even after the budget has passed. A creation whose budget has passed before its first put writes nothing and fails with the metadata publication budget error. A creation whose budget passes between attempts stops and fails with the error of its last put, and then deletes its candidate snapshot as described below, because that put can still be applied.
3. Verify the namespace is active, then run the successor probe + boundary read ([Replaying the visible WAL](#42-replaying-the-visible-wal)). Writing before verifying ensures any concurrent GC either saw this manifest as current or saw a successor after the snapshot existed in its complete listing. If any successor exists (even at the same `head_seq`) or `manifest_no <= boundary`, delete the candidate snapshot as described below and retry from a fresh basis.

Acknowledged snapshots remain readable even after `retention_floor_seq` advances past them.

A creation that fails after step 2 sent a put (for example, because the verification refused the manifest, or because every put of the record had an unknown outcome) deletes its candidate snapshot by setting `deleted_at_ms`, as an explicit deletion does ([Reads, deletion, and expiry](#83-reads-deletion-and-expiry)). The creator first reads the record's key. If the key has the record, the creator sets `deleted_at_ms` with a CAS against the version it loaded. If the key is empty, the creator writes the record with `deleted_at_ms` already set, with put-if-absent. The creator writes the deleted record even after its publication budget has passed, because a put of the creation that started within the budget can still be applied, and no host can read or fork a deleted snapshot. Once that delete succeeds, the key stays occupied, so a put of the creation that the provider applies after the creation returned fails, because put-if-absent never replaces an object and every put that one creation sends is to the same key. (If the creator removed the record instead, such a put could create an active snapshot whose ID no caller received, and if that snapshot had no expiry, it would keep its manifest until someone deleted it.) GC deletes the record by the ordinary rules for a deleted user snapshot, so the record can be deleted before `deleted_at_ms + T` ([Snapshot cleanup](#117-snapshot-cleanup)). By `deleted_at_ms + T`, the provider has applied every put of the creation that it will apply, because each of those puts started before `deleted_at_ms`, a provider applies a request within `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` of its start, and `T` is at least the publication completion bound, which includes that request bound and the safety margin ([Clock and operation assumptions](#114-clock-and-operation-assumptions)). GC deletes the record earlier only if the snapshot expired at least `T` before that collection, or if the namespace is deleted and the snapshot was created at least `T` before it. A put that the provider applies after such an early deletion creates a snapshot that no host can read or fork. Every host's clock is within the safety margin of the collector's clock, so every host finds that snapshot expired, and a deleted namespace refuses reads and forks. Until GC deletes the creation's record, the record keeps its manifest and every segment that manifest lists, as every deleted user snapshot does. So if a successor replaced the manifest, the manifest and the segments that only it lists can stay for up to about `T` after the failure, even if they're already older than `T`.

#### Fork snapshots and fork point snapshots

A source fork snapshot is protected by the source user snapshot (caller-provided or temporary) rather than requiring that manifest to remain current:

1. Check `METADATA_PUBLICATION_BUDGET_MS` (ensuring the write occurs well before the user snapshot's grace-protected manifest could be collected), then put-if-absent the fork snapshot on the source manifest.
2. Verify the source is active and the user snapshot still exists, is undeleted, and is unexpired (using request clock + elapsed monotonic time, and for tail-folding forks, expired once `OPERATION_SNAPSHOT_TTL_MS` has elapsed since `created_at_ms`). Because snapshots are never recreated, a user snapshot present after the write existed throughout.

A fork point snapshot pins the fork's newest initializing manifest (checked against `METADATA_PUBLICATION_BUDGET_MS` before writing. If an abandoned initializing manifest was already deleted and collected before a finishing call wrote the snapshot, GC keeps it for retirement without reporting corruption, [Snapshot cleanup](#117-snapshot-cleanup)).

### 8.3 Reads, deletion, and expiry

Reads accept both user snapshots and fork snapshots (allowing rebases, conflict resolvers, and grep to read a fork's starting state via its fork point snapshot even after the source's retention floor advances, or read the source manifest via the source fork snapshot). Forking and extending accept user snapshots only (`snapshot_not_found` for fork snapshots or missing/deleted/expired user snapshots).

Reading through a snapshot extracts `manifest_no` from the ID, verifies the record exists (and is undeleted and unexpired on the reader's clock for user snapshots), verifies its manifest reference, and reads that manifest without replaying later WAL objects.

Explicit deletion of a user snapshot sets `deleted_at_ms` via CAS (reusing the creator's write version if known, else loading first, leaving `expires_at_ms` unchanged), immediately blocking reads/forks/extensions regardless of clock skew while GC deletes the record after the grace window ([Snapshot cleanup](#117-snapshot-cleanup)). Deleting a missing or already-deleted user snapshot succeeds as a no-op, while deleting a fork snapshot returns `snapshot_not_found`.

### 8.4 Extending a snapshot

Extending a user snapshot moves `expires_at_ms` forward via CAS without altering identity, owner, or manifest reference (never moving expiry earlier, a no-op when no expiry is set, and never reviving deleted or expired snapshots). Expiry is checked on each attempt using request clock + total elapsed monotonic time, and a CAS already in flight that succeeds stands. Each CAS first checks `METADATA_PUBLICATION_BUDGET_MS`, as a snapshot record's creation does, so an extension whose budget has passed writes nothing and fails with the metadata publication budget error. (Without that check, a stalled CAS could be applied after a collection decided to delete the record, and the extension would report success for a snapshot that collection then deletes.)

## 9. Namespace lifecycle and forks

Creating, forking, deleting, and retiring namespaces all coordinate through numbered manifests, pool member entries, and the manifest boundary.

### 9.1 Creating a namespace

Because a standalone namespace creates a new [pool](glossary.md#pool) and needs a [member entry](glossary.md#member-entry) under `pools/{pool_id}/` before it becomes active (without risking orphaned entries from lost races), creation writes manifest 1 (`initializing`), then the member entry, then manifest 2 (`active`):

1. Find the newest manifest for the target ID ([Resolving the metadata basis](#41-resolving-the-metadata-basis)) before writing anything: active returns `namespace_exists` (or current summary with `allow_existing`), deleted/retired returns `namespace_deleted`, `initializing` continues via [Finishing an initializing namespace](#finishing-an-initializing-namespace), and read errors/corruption fail immediately.
2. Generate `pool_id` and put-if-absent manifest 1 (`status: initializing`), then read `boundary.json`. If refused or `1 <= boundary`, restart at step 1 (catching late puts against already-collected IDs).
3. Check `METADATA_PUBLICATION_BUDGET_MS`, then put-if-absent `pools/{pool_id}/members/{namespace_id}.json` ([Control and manifest payloads](#a4-control-and-manifest-payloads)).
4. Put-if-absent manifest 2 (`status: active`). If lost, re-discover per [Finishing an initializing namespace](#finishing-an-initializing-namespace).

If two callers race on the same ID, the one whose manifest 1 succeeds above the boundary chooses `pool_id`, and the other restarts at step 1 (finishing the namespace if its request matches, or returning `namespace_exists`).

### 9.2 Forking a namespace

A fork starts from a source namespace's retained state without copying file bytes or existing metadata segments. Its "fork point" (`fork_point_seq`) is the head of the source view (live-head fork) or the snapshot's folded head (snapshot fork). Rather than publishing a manifest on the source, a live-head fork folds any source WAL tail into its own initializing manifest 2 (folding the source first only if a tail commit is old, so a pool collection can't lose tail-referenced content before the fork's member entry exists).

Fork creation steps:

1. Check the target ID ([Resolving the metadata basis](#41-resolving-the-metadata-basis)): active returns `namespace_exists`, deleted/retired returns `namespace_deleted` (writing nothing), and `initializing` continues via [Finishing an initializing namespace](#finishing-an-initializing-namespace).
2. Select the source user snapshot:
   - If the request names a user snapshot, load it (`snapshot_not_found` if absent, deleted, or expired).
   - Otherwise, discover the source's current manifest and WAL tail. If any commit in the tail to fold has `committed_at_ms` older than `MAX_TAIL_COMMIT_AGE_MS` (11 h 45 m) before fork start, fold the source first and re-discover. Then take a no-fold unnamed "temporary snapshot" on that source manifest expiring `OPERATION_SNAPSHOT_TTL_MS` (5 min) after fork start (retrying discovery if a successor appeared). That temporary snapshot pins the source manifest until the fork snapshot is verified, and is left to expire for GC.
   - For a [rebase](glossary.md#rebase) fork, also compute `rebased_from` (`source_namespace_id`, `resume_after_seq`) from this view under [Merges](#merges), returning `below_retention_floor` before publishing manifest 1 if the replayed fork's fork point is below the view's floor.
3. Generate IDs for the source fork snapshot (`source_snapshot_id`) and the fork's own "fork point snapshot" (`fork_point_snapshot_id`, pinning manifest 2 if folding a tail else manifest 1), and put-if-absent initializing manifest 1 copying the source manifest's runs, `pool_id`, `retention_window_ms`, and [extensions](#124-extensions) (plus `fork_basis` recording the step-2 snapshot ID so finishing calls can locate it). Putting manifest 1 before writing fork snapshots ensures the target's tombstone always lists every fork snapshot written for it. On lost put, restart at step 1.
4. If the source view has commits above the source manifest's head (a tail of only fences/floor objects counts as empty), fold those commits into initializing manifest 2: write pending pieces as extents under `pools/{pool_id}/content/` with the source as chain creator, write new segments under the fork's prefix, set `head_seq = fork_point_seq`, keep manifest 1's `retention_floor_seq` (ignoring tail floor objects so finishing calls always produce an identical floor and head), and keep `folded_wal_no = 0` and `activity = 0`.
5. Check `METADATA_PUBLICATION_BUDGET_MS` and put-if-absent the fork point snapshot on the newest initializing manifest (manifest 2 or 1), keeping the source's state at the fork point readable for the fork's lifetime.
6. Check `METADATA_PUBLICATION_BUDGET_MS`, put-if-absent the source fork snapshot on the step-2 source manifest, then verify the source is still active and the step-2 snapshot still exists, is undeleted, and is unexpired. Independently, check `METADATA_PUBLICATION_BUDGET_MS` and put-if-absent the pool member entry `pools/{pool_id}/members/{namespace_id}.json`.
7. Publish the active manifest (`status: active`, `retention_floor_seq = fork_point_seq` with no floor object needed since the fork WAL is empty, manifest 3 if tail was folded else manifest 2).

Tail-folding forks enforce a hard deadline of `OPERATION_SNAPSHOT_TTL_MS` from the temporary snapshot's immutable `created_at_ms` (not its extendable `expires_at_ms`, matching [Content roots](#119-content-roots)) before every write after step 2 and before step 6's verification, returning `snapshot_not_found` if exceeded. Snapshot forks have no such deadline.

A fork's first data commit is `fork_point_seq + 1`, and an active fork can be forked immediately.

#### Finishing an initializing namespace

Because every object written after manifest 1 has a deterministic key recorded in manifest 1, any caller with a matching request can finish an interrupted create or fork (making retries idempotent while still initializing):

- **Matching rule:** A create matches manifest 1 if it has no `fork_basis` and the same `retention_window_ms`. A fork matches if `fork_basis` has the same source namespace, matching requested snapshot (an untargeted fork matches whichever snapshot manifest 1 chose), and matching `rebased_from` (`created_by` and `created_at_ms` are ignored). Mismatches return `namespace_exists` even with `allow_existing`.
- **Resuming a fork:** First verify the source is active and load the step-2 snapshot recorded in `fork_basis` (returning `namespace_deleted` or `snapshot_not_found` without writing if invalid, since loading before writing establishes a fresh liveness timestamp for the budget check so a stale manifest 1 cannot create a fork snapshot pointing to a deleted source manifest). Apply the same `OPERATION_SNAPSHOT_TTL_MS` deadline from `created_at_ms` if tail-folding. Start the call's [publication budget](glossary.md#publication-budget) before reading the target's newest manifest. If the caller found the target initializing before the call started, read the target again after the budget starts, and return `namespace_exists` if the target became active in between, or `namespace_deleted` if it was deleted or retired.
- **Re-folding a tail:** If newest manifest is manifest 1 and `fork_point_seq > manifest1.head_seq`, read source WAL objects from `source_manifest.folded_wal_no + 1` through `fork_point_seq` (returning `snapshot_not_found` if any were already collected) and publish manifest 2. Concurrent finishers race cleanly on put-if-absent (losing fold segments/extents age out via GC).
- **Failure in step 6:** Never delete either fork snapshot. Re-discover and return success if active, or `snapshot_not_found` / `namespace_deleted` otherwise (leaving the initializing namespace unusable until deleted or collected).

### 9.3 Conflicting and unknown installations

A lost manifest put (or one written at or below `boundary.json`) never overwrites the winner. After a transport-level unknown outcome, the publisher reads back the manifest at that number (returning `outcome_unknown` if still absent), then reads `boundary.json` (if `<= boundary`, treats the put as lost and re-discovers):

- A manifest with the candidate's [manifest reference](#25-manifest-references) (the same number, head, and payload checksum): the put succeeded.
- Any other manifest: the put was lost, and the publisher treats that manifest as the winner of a collision. (A create or fork whose manifest 1 put was lost continues as described in [Finishing an initializing namespace](#finishing-an-initializing-namespace).)

A publisher MUST generate a new `publication_id` for each manifest put, including a put of content that an earlier put already tried. Another publisher that builds the same manifest content then stores different bytes, so only this put can store the candidate's manifest reference. For example, if two deletions of one namespace build the same tombstone at manifest 7, and the first deletion's put loses its response, the first deletion reads back manifest 7. If its own put was written, the manifest there has its own `publication_id`, and the deletion succeeded. If the second deletion's put was written first, the manifest there has the second deletion's `publication_id`, and the first deletion returns `namespace_deleted`.

### 9.4 Deleting a namespace

Deleting an active namespace acquires the writer epoch, drains admitted commits, folds the remaining WAL as [maintainer](glossary.md#maintainer) (staying active if the fold fails), and publishes the next manifest as a namespace tombstone (`status: deleted`, `deleted_at_ms` from the call's clock, started within `METADATA_PUBLICATION_BUDGET_MS` of reading that clock) with `folded_wal_no` covering the final head.

Deleting an `initializing` namespace needs no writer session, fence, or fold: it publishes the tombstone directly at the next manifest number, racing cleanly against activation so the namespace becomes either active or deleted, never both.

Abandoned `initializing` namespaces (such as an interrupted create with a generated ID) are automatically tombstoned by namespace collection once `manifest1.created_at_ms` is older than `GC_GRACE_WINDOW_MS` (24 hours, and until then collection reports the deadline in `next_reclamation_at_ms`).

Once observed, operations return `namespace_deleted` (cached readers may serve until their next revalidation). Deletion does not immediately remove content: the tombstone protects content until its retirement deadline, and snapshots protect their manifests until removed.

### 9.5 Retirement

"Retirement" removes a deleted namespace from its [pool](glossary.md#pool) by raising `boundary.json` to `retired`. No deadline is stored. For tombstone `T`, the retirement deadline is:

```text
deadline(T) = T.deleted_at_ms + GC_GRACE_WINDOW_MS
```

At `deadline(T)`, the tombstone's own content references stop protecting pool content (even if a fork snapshot still delays retirement, [Content roots](#119-content-roots)). The tombstone becomes eligible for retirement once the collection clock reaches `deadline(T)` and the complete snapshot listing contains no snapshots except its own fork point snapshot (and no unrecognized keys under `snapshots/`).

Namespace collection retires an eligible tombstone in strict order ([Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone)):

1. Delete both fork snapshots recorded in `fork_basis` (if a fork): the source fork snapshot and its own fork point snapshot (must precede step 3 so a crash never loses the tombstone that names them).
2. Delete `pools/{pool_id}/members/{namespace_id}.json`.
3. Raise `boundary.json` to `retired` (distinct from a numeric boundary so readers never mistake an empty manifest listing for corruption, and kept permanently so the ID is never reused and late manifest puts count as lost).

Later collections delete every object under the retired namespace prefix (including the tombstone) past the age rules while keeping `boundary.json`, fences, floor objects, and unrecognized keys. Because each fork keeps a fork snapshot on its source, descendants always retire before ancestors. Once every pool member retires, normal pool collection reclaims all content and upload records under standard age rules.

### 9.6 Fork dependencies after deletion

Fork snapshots persist until their target retires (descendants to ancestors: if C is forked from B and B from A, retiring C deletes C's fork snapshot on B, allowing B to retire and delete its fork snapshot on A). Because content is stored under the shared pool prefix, rewriting a fork's metadata never copies or moves inherited file bytes.

### 9.7 Cross-namespace copies and moves

Inode-preserving renames stay within one namespace, while cross-namespace moves are non-atomic copy-then-delete operations. Within one [pool](glossary.md#pool), forks and merges share content references without copying bytes (cross-pool transfers require uploading bytes because pools collect content independently).

When a [merge](glossary.md#merge) plans from a fork's [read view](glossary.md#read-view), any referenced bytes still sitting as WAL [pieces](glossary.md#piece) in the fork's tail are written by the merge as one new span extent under `pools/{pool_id}/content/` without combining existing extents (avoiding publishing a manifest on the fork, while the fork's own later fold writes those pieces separately). The merge's layout rows record the chain's existing creator (or the fork for tail-created chains), so every reference in the merge commit has a layout and no inline bytes.

While a merge or rebase plans, LoonFS takes a no-fold unnamed user snapshot on the fork's current manifest expiring `OPERATION_SNAPSHOT_TTL_MS` (5 min) after plan start (deleted after commit) so the fork's content stays protected until the merge commits into the target's WAL tail. Because that snapshot does not cover the fork's WAL tail, if any commit in the fork's tail has `committed_at_ms` older than `MAX_TAIL_COMMIT_AGE_MS` (11 h 45 m) before plan start, LoonFS folds the fork first before taking the snapshot (preventing a pool collection from reading the target before the merge and the fork after a retention advance + compaction):

```text
MAX_TAIL_COMMIT_AGE_MS = GC_GRACE_WINDOW_MS - (POOL_ROOTS_READ_BUDGET_MS
    + GC_SAFETY_MARGIN_MS + OPERATION_SNAPSHOT_TTL_MS
    + PROVIDER_PUBLICATION_REQUEST_BOUND_MS)
```

## 10. Retention and compaction

Every commit adds metadata rows, so without a limit, old revisions, bindings, and receipts would accumulate indefinitely. The retention floor sets how far back history is guaranteed. Compaction rewrites a window of metadata runs into one run and drops rows that the floor no longer requires. Neither operation publishes new filesystem changes, and compaction preserves every retained view.

### 10.1 Advancing the retention floor

The retention floor limits how far back LoonFS keeps incremental replay, superseded bindings, file revisions, old attribute states, and commit receipts. Above the floor, every file's history is complete. At or below it, each file that's visible or under a deletion in the trash keeps its newest revision, and a file hidden by an expired deletion keeps none (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)). The floor doesn't expire layout rows or keep WAL objects (garbage collection removes folded WAL objects on its own schedule).

The floor moves only when an advance creates a [floor object](glossary.md#floor-object) or a fork publishes its active manifest, and it never decreases. A new namespace starts at floor 0. A fork's initializing manifests keep its source's floor, and its active manifest sets the floor to the fork point (see [Forking a namespace](#92-forking-a-namespace)). Because the change feed returns only commits above the floor, starting the fork's floor at its fork point keeps the source's commit at that sequence out of the fork's feed.

#### Targets and the grace-window bound

A floor advance takes one of three targets:

- the current manifest's `head_seq`,
- a sequence number (capped at `head_seq`), or
- a cutoff time.

For a cutoff time, the advance scans the `commits` family upward from the current floor and stops at the first commit whose `committed_at_ms` is later than the cutoff. The target is the last commit the walk passed, or the current floor if it passed none. Commit timestamps don't have to increase with sequence. Stopping at the first commit after the cutoff guarantees the floor never passes a later-dated commit because a writer's clock ran behind. For example, suppose the floor is 10, commits 11, 12, and 13 have timestamps 9:00, 9:05, and 9:02, and the cutoff is 9:03. The walk passes 11 and stops at 12, so the target is 11 even though 13 is earlier than 9:03. The tradeoff is that a writer whose clock runs a day ahead keeps the floor below its commits for about two days. Such a clock is outside the [clock assumption](#114-clock-and-operation-assumptions), but the floor still never passes a commit early, because a clock that runs ahead only makes commits look newer.

Whatever the target, an advance MUST NOT move the floor past a commit whose `committed_at_ms` is later than the advancing host's clock minus the [grace window](glossary.md#grace-window) (`GC_GRACE_WINDOW_MS`, 24 hours, see [Publication and collection timing](#c1-publication-and-collection-timing)). Keeping replaced revisions for at least 24 hours protects active readers, presigned URLs, and pool collections (see [Content roots](#119-content-roots)). The advancing host reads its own clock, just as garbage collection does (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).

The grace-window cutoff uses the same walk as a cutoff time, so the advance reads the commits between the floor and the cutoff only once. For a head or sequence target, the new floor is the lower of the target and the last commit the walk passes. For a cutoff time or retention window, the advance walks once to the earlier of the two cutoffs. If the first commit above the current floor is already later than the cutoff, the floor stays where it is. Using the same commits 11 (9:00), 12 (9:05), and 13 (9:02) with head 13, an advance targeting head at 9:01 the next day stops at commit 12, moving the floor to 11. At 9:04 the next day, commit 12 is still only 23 hours and 59 minutes old, so the floor stays at 11. At 9:05, the floor moves to 13.

The advance plans from the floor and head of a writer session's [writer position](glossary.md#writer-position), or from the current manifest if there's no position, walking that manifest's `commits` family. Planning from the manifest's floor rather than the full WAL tail avoids extra requests on an idle namespace whose floor already reached the cutoff, because a floor object only raises the floor. The advance returns the floor of the view its discovery found, which is below the target whenever the grace window stopped the walk first.

A [read view's](glossary.md#read-view) or writer position's retention floor is the highest of the manifest's `retention_floor_seq` and the `retention_floor_seq` of every floor object in the unfolded WAL tail (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). Every rule that checks a view's floor reads this value: restore validation (deleted items and file revisions), the trash listing's scan start, the change feed's lower bound, the merge floor rule, and reads that return `below_retention_floor`. Using one definition ensures that restore validation refuses every deletion the trash listing hides. Base compaction is the one exception because it reads its basis manifest's floor directly (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)).

#### Retention windows

A namespace's retention window (`retention_window_ms`) is an optional manifest setting in milliseconds, at least as long as the grace window. Storing it in the manifest lets namespaces in the same store keep different amounts of history while keeping every maintainer consistent at no extra request cost. A scheduled maintainer (such as the reference server's sweep) reads the window from the current manifest and advances the floor using `now - retention_window_ms` as the cutoff. A namespace with a 7-day window keeps 7 days of history. A namespace with no window isn't advanced on a schedule and keeps its whole history.

At creation, the window comes from the create request or falls back to the host's default (`retention_window_ms`). A fork copies its source's window (see [Publishing a manifest](#72-publishing-a-manifest)). A later publication can set, change, or clear the window while copying every other manifest field. If the current manifest already has the requested window, LoonFS publishes nothing. A host MUST refuse any window shorter than the grace window, whether from a create request, a host default, or a later update, so the configured window always matches the history the namespace actually keeps.

A snapshot below the new floor still protects its own manifest and runs.

#### Expiring deletions and creating floor objects

A deletion whose `deletion_seq` is at or below a view's floor has expired in that view. The trash listing omits it, no read can reach the inodes it hides, and a read of a hidden inode by inode ID returns `inode_not_found` for every caller (see [File and subtree deletion](#16-file-and-subtree-deletion) and [Paths, listings, and revision reads](#44-paths-listings-and-revision-reads)). The advance commits nothing for the deletions it passes. Every deletion a fork inherits is already expired from creation because the fork's floor starts at the fork point.

A restore of an expired deletion fails, and the error depends on the caller, because LoonFS checks the caller's rights before it checks the deletion. For a deletion in the trash, LoonFS checks `remove` on the parent directory that the tombstone saved. An expired deletion isn't in the trash, and deleting the item removed its directory binding, so LoonFS checks `remove` on the item itself. An expired subtree inherits no rights from above it ([Access rows](#18-access-rows)), so only a grant in the item's own access row counts, and a base compaction removes that row with the subtree's other rows. A caller that fails the check gets the error that any rights check returns: `inode_not_found` if it has no right on the item, or `forbidden` if it has some other right.

| Caller | Restore of an expired deletion | Restore of an inode ID that never existed |
| --- | --- | --- |
| The [service context](glossary.md#service-context), or an administrator | `not_deleted` | `not_deleted` |
| A subject with `remove` in the item's own access row | `not_deleted`, or `inode_not_found` once a base compaction removes the row | `inode_not_found` |
| A subject with some other right in the item's own access row | `forbidden`, or `inode_not_found` once a base compaction removes the row | `inode_not_found` |
| Any other subject, including one with `remove` on a directory above the item | `inode_not_found` | `inode_not_found` |

A restore of an inode that isn't a deletion root checks `remove` on the inode's current parent instead. So a restore of an inode inside an expired subtree depends on the subject's rights on that parent until a base compaction removes the inode's directory binding.

A floor object is a WAL object with no commit records whose payload sets `retention_floor_seq` to the new floor (see [WAL records](#a5-wal-records)). Like a fence, it takes the next WAL number without advancing `head_seq`, so it adds no commit, sequence number, or change feed entry. For example, if the current manifest has writer epoch 3 and the [WAL tip](glossary.md#wal-tip) is WAL object 30 at sequence 140, an advance to floor 20 creates WAL object 31 with writer epoch 3, `head_seq` 140, no records, and `retention_floor_seq` 20.

Putting the floor advance in the WAL rather than in a standalone manifest orders it cleanly against concurrent restores: a writer can validate a restore against an older manifest, but it can't write a WAL object past a floor object's number without first reading that floor object or a manifest that folded it.

Any process MAY create a floor object without claiming the writer epoch or the compactor epoch, because a floor object contains no commits and put-if-absent at the next WAL number prevents branching. The floor object's writer epoch MUST be the writer epoch of the manifest that the creating attempt's discovery loaded right before the put. Copying the epoch from discovery prevents a floor object from being written above a higher-epoch fence, keeping writer epochs non-decreasing as WAL numbers rise.

To advance the floor to a new floor `T`, a process follows these steps:

1. Plan `T` under the rules above. If `T` isn't above the planned starting floor, write nothing and return that floor.
2. Check that every referenced segment of the planning manifest exists. Ending the replay guarantee for older history requires first checking that the remaining history is readable, though this check isn't a transaction and reads still verify checksums.
3. Run discovery to find the WAL tip. If the discovered view's floor is already at or above `T`, write nothing. Otherwise, put the floor object at `tip + 1` with put-if-absent, using the writer epoch of the manifest this discovery loaded.
4. After the put succeeds, check that manifest with the successor probe and boundary read (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), and decide whether the floor object is live by the rule for a fence in [Writer ownership](#61-writer-ownership). A dead floor object stays in storage, and no reader replays it. This check handles the race where commits at `tip + 1` were folded and deleted before the put arrived, leaving an emptied slot at or below `folded_wal_no`. (If a fold covered a live floor object before the check, the next attempt's discovery sees the floor already recorded and writes nothing.)
5. If the put collides or the floor object is dead, repeat from step 3 at the next WAL number, up to a bounded number of attempts (8 in the reference implementation).

If the process can't tell whether the put succeeded, it reports the outcome as unknown. A retry that writes a second floor object with the same floor is harmless.

A writer session that already owns the writer epoch puts the floor object from its publication queue at `tip + 1`, just like a commit. That avoids collisions with its own commits and skips the post-put check (the argument in [Publication and group commit](#63-publication-and-group-commit) applies here too). For that argument to hold, the session [owes a fence](glossary.md#owes-a-fence) for this put until it learns the put's outcome, as it does for a commit's put (see [Failed and unknown outcomes](#64-failed-and-unknown-outcomes)). Its position replays the floor object immediately, updating its tip and floor to `T`. Commits queued ahead of the advance validate against the old floor, and commits queued behind it validate against `T`. A session without the writer epoch, or one that discovery finds fenced, uses the general steps above.

A fold that covers a floor object records the new floor in the manifest it publishes (see [Publishing a manifest](#72-publishing-a-manifest)). Every publication other than a fold or a fork's active manifest MUST copy its predecessor's `retention_floor_seq`, so LoonFS never publishes a floor in a manifest before every WAL object below the floor object has been folded. The reference implementation folds the namespace after creating a floor object whenever no manifest yet records the view's floor.

A [writer session](glossary.md#writer-session) in another process doesn't get fenced by a floor object, because the floor object doesn't raise the writer epoch. Its next put at `tip + 1` simply collides, triggers one discovery that replays the floor object, and replans its batch against the new floor (see [Publication and group commit](#63-publication-and-group-commit)). A floor object is written without a claim manifest, so this flow relies on the rule that no process deletes a floor object (see [Candidate and age rules](#113-candidate-and-age-rules)). The session's put at `tip + 1` collides with the floor object even after a fold covers the floor object, so the session can't commit at a WAL number that a manifest has already folded. If the session's successor probe finds that fold's manifest first, the session drops its position and runs discovery instead (see [Replaying the visible WAL](#42-replaying-the-visible-wal)).

#### The expired-deletion invariant

These rules maintain one key invariant: for every manifest `M` and every deletion `D` whose deletion sequence is at or below `M`'s retention floor and whose latest event in `M` is `set`, no WAL object above `M`'s `folded_wal_no` contains an accepted restore of `D`, and none written later ever will.

Here's why. `M`'s floor comes from a floor object at some WAL number `k` at or below `M`'s `folded_wal_no` (or from an earlier manifest by the same argument). Consider any WAL object `W` containing an accepted restore `R` of `D`:

- **`W` is below `k`.** The fold that published `M` (or an earlier fold) already folded `W`, so `D`'s latest event in `M` is `revoke`, not `set`.
- **`W` is above `k`.** The writer wrote `W` at `tip + 1`, so its tip was at least `k`. A writer's tip crosses `k` only by writing `k` itself or running discovery (a manifest from another process never jumps a position forward). Since the writer didn't write `k`, its discovery either replayed the floor object at `k` or started from a manifest that already folded `k`. Either way, it validated `R` against a floor at or above `D`'s deletion sequence and refused `R`.

This applies across all writer epochs and every later manifest, so a base compaction can safely delete every row hidden by an expired deletion (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)). The same argument protects file-revision restores by reference: any restore accepted before the advance is below `k` and already present as a new revision row in `M`. A writer MUST still refuse to restore a revision at or below its position's retention floor unless that revision is the file's newest at or below the floor.

For example, suppose the floor is 5, and a writer session at tip 30 queues a restore of deletion 10, an advance to 20, and a second restore of deletion 10. The first restore commits at 31, the floor object at 32, and the second restore returns `not_deleted`. If another process writes the floor object at 31 first, the writer's restore collides at 31, and the writer replays the floor object and keeps its epoch. The restore then fails because the deletion has expired (see [Expiring deletions and creating floor objects](#expiring-deletions-and-creating-floor-objects) for the error). If the restore wins 31 instead, the advance writes the floor object at 32, so any fold covering 32 also folds the restore's `revoke` event at 31.

### 10.2 Compaction windows

Metadata is compacted by family group. `direntry_binds` and `direntry_child_binds` form one group because they MUST stay consistent. `commits`, `commit_receipts`, and `merges` form a second group, and the other seven families each form a group of one (see [Metadata rows and row keys](#a6-metadata-rows-and-row-keys)).

A compaction rewrites a contiguous window of a group's runs into one run without skipping any run in the middle.

```text
oldest                                       newest
[ base run ] [ delta 1 ] [ delta 2 ] [ delta 3 ]
|<-------- bottom-anchored window --------->|   output: a new base run, can drop rows
             |<----- window above base ---->|   output: a new delta run, drops nothing
```

| Window | Output run | Can it drop rows? | Sequence stamp |
| --- | --- | --- | --- |
| Includes the group's oldest run (bottom-anchored) | `base`, replacing the existing base run | Yes, under the retention rules | The manifest's `head_seq` |
| Skips the oldest run | `delta` | No | The newest input's sequence |

A compaction above the oldest run can't drop rows because an excluded older run might store rows that a row in the window supersedes (such as an older binding version beneath an unbound marker, or a listed deletion beneath its removal marker). Dropping the newer marker would resurrect the older row.

A group has at most one base run. Base runs always sort before delta runs regardless of sequence stamp, and a delta-only compaction stays in the same position in the group. A delta-only compaction MUST rewrite at least two runs, so once a group has a single delta run above a much larger base, the next compaction includes the base.

LoonFS selects windows by size tier to avoid rewriting a large base every time a small fold arrives. A size tier is a contiguous set of runs whose stored segment sizes are within 4x of each other. In a tier, each byte is rewritten roughly once per 4x size growth, and every compaction of four or more runs removes at least three runs.

To pick a window, LoonFS:

1. Lists the group's runs from newest to oldest, with the base last.
2. Starts a tier at the newest run and adds each next older run while its size is at most 4x the smallest run already in the tier.
3. Compacts the tier if it has at least 4 runs. If the tier has more than 8 runs, it compacts the oldest 8 and leaves the newer ones for later.
4. Otherwise starts a new tier at the next older run and repeats step 2 until no older run remains.
5. If no tier reaches 4 runs and the group has at least 8 delta runs, compacts a window starting with the 2 newest runs and adding each next older run while that run is at most 4x the combined size of the runs already in the window, up to 8 runs.

Step 5 caps the number of delta runs a lookup must probe when run sizes grow steeply (for instance, each run more than 4x the next newer one). Following the 4x ratio rather than a set count keeps write amplification proportional to group size instead of the arrival rate of small runs, while always taking at least 2 runs so the run count drops.

For example, suppose a group's runs from oldest to newest are a 300 MiB base and delta runs of 90, 100, 110, and 2 MiB. The tier starting at 2 MiB stops before 110 MiB (more than 4x 2). The tier starting at 110 MiB adds 100, 90, and the 300 MiB base (300 <= 4 * 90), giving 4 runs that compact into a new base while the 2 MiB run waits.

For step 5, suppose a group with 8 delta runs and no 4-run tier has newest runs of 400, 40, 2, 1, and 1 MiB (oldest to newest). The window takes 1, 1, and 2 MiB and stops before 40 MiB (more than 4x 4 MiB). With 400, 40, 30, and 1 MiB instead, it takes 1, 30, and 40 MiB (40 <= 4 * 31).

The tier size of 4 runs, 4x ratio, 8-delta-run cap, and 8-run compaction limit are implementation defaults, not format requirements. Any compaction window following the structural rules above is valid.

### 10.3 Row retention during a base compaction

These retention rules apply only when the compaction window includes the group's oldest run (a bottom-anchored compaction). A delta-only compaction keeps every row, including unbound bindings at or below the floor, except that `content_layouts` keeps only the first layout row per content ID (since the longest layout covers every shorter prefix).

| Family | Rows kept or removed |
| --- | --- |
| `inodes` | Keep every inode row except inodes hidden by an expired deletion. |
| `direntry_binds`, `direntry_child_binds` | For each edge (parent, name key, child) and row kind (binding events and slot-index summary rows), keep every row above the floor and the newest row at or below it. If the edge's newest binding event at or below the floor is unbound, drop all rows of both kinds for that edge at or below the floor. Remove every row whose parent is hidden by an expired deletion. |
| `revisions` | For each file, keep every revision above the floor and the newest revision at or below it, and drop earlier ones. Remove every revision of a file hidden by an expired deletion. |
| `tombstones` | Keep every `set` and `revoke` event. |
| `trash` | Drop a cancelled `listed` and `removed` pair together, and drop the `listed` row of any deletion at or below the floor. Keep the `listed` row of any deletion above the floor. |
| `commits`, `commit_receipts`, `merges` | Drop rows strictly below the floor. Keep the row at the floor (the change feed never replays it, and in a fork it's the source's commit at the fork point). |
| `content_layouts` | Keep the first row for each content ID (longest layout, then newest commit). Drop chains absent from the basis's revision filter. |
| `attributes` | For each inode, keep every revision above the floor and the newest revision at or below it, and drop earlier ones. Remove every revision of an inode hidden by an expired deletion. |
| `access` | For each inode, keep every revision above the floor and the newest revision at or below it, and drop earlier ones. Remove every revision of an inode hidden by an expired deletion. |

#### Pruning layouts and expired subtrees

A bottom-anchored compaction of `content_layouts` drops chains referenced by no `revisions` row in its basis. Before rewriting layouts, it scans the basis's revisions once and builds a Bloom filter sized per [Filter sizes](#filter-sizes) up to `MAX_BLOOM_FILTER_BYTES` (64 MiB), using two xxh64 hashes seeded by the output run number and an expected count equal to the sum of the revision segments' `row_count`. At the 64 MiB cap, the filter uses fewer bits per ID and fewer hash probes, erring only toward keeping extra rows for a later compaction to drop.

A deletion is expired in a compaction's [metadata basis](#41-resolving-the-metadata-basis) when its deletion sequence is at or below the basis manifest's `retention_floor_seq`. By the invariant in [Advancing the retention floor](#101-advancing-the-retention-floor), no folded or unfolded commit can restore it. If its latest tombstone event in the basis is `set`, a bottom-anchored compaction of `inodes`, `revisions`, `attributes`, `access`, or `bindings` drops every row of the deleted root and all its descendants.

Because LoonFS never commits changes to a hidden inode, all of a hidden inode's rows are at or below the floor and invisible at every retained sequence. Tombstone events stay permanently because family groups compact independently, keeping inodes hidden while other groups still contain their rows. Binding rows are tested by the edge's parent, dropping every version of edges under hidden parents, while the root's own edge has a visible parent and is dropped by the normal unbound-binding rule. `trash` removes the root's `listed` row, and the revision filter later drops the orphaned layout chains.

To find hidden inodes without writing a per-descendant flag on delete or loading the whole tree into memory, the compaction builds a **kept inode filter** (a Bloom filter of every inode in the basis that's visible or inside a trash deletion):

1. Walk the basis's directory tree depth first from the root inode, keeping one cursor for the active directory and one for each ancestor with unread entries. Read children from `direntry_binds`, inserting and descending into each edge whose newest row is bound.
2. Walk the subtree of every active deletion in the trash the same way, starting from `listed` rows in `trash` that have no `removed` row and whose `deletion_seq` is above the floor.

Deleting an item unbinds it, and an expired deletion starts no walk, so hidden subtrees are never reached. Because a nested deletion is always older than an enclosing one (see [File and subtree deletion](#16-file-and-subtree-deletion)), no active trash deletion sits inside an expired deletion, and the walks also cover every ancestor needed to evaluate rights inside the trash.

The compaction drops any row whose inode isn't in the filter and applies the table's normal rules to rows that are. Both binding indexes compact together against the same filter so they stay identical. The kept inode filter uses the same design as the layout filter: two xxh64 hashes seeded by the output run number, 10 bits per expected inode (sum of `inodes` segments' `row_count`), and the same min/max sizes. False positives simply survive until the next base compaction runs with a different output run number.

The walk stops if it exceeds `MAX_KEPT_INODE_WALK_DEPTH` (4,096) concurrent cursors. (A cursor is released before descending into a directory's last child once the directory's last row is read, so deep narrow chains fitting in a single 64-row scan use only one cursor.) If the walk hits that limit, the compaction skips expired-row filtering, logs the decision, and reports `expired_row_removal_walk_limit` in the `metadata` job result (see [Jobs and outcomes](api.md#jobs-and-outcomes)). Delta-only compactions, and base compactions at `retention_floor_seq == 0`, skip the filter entirely because no deletion has expired.

For example, suppose `/old` contains `a.txt`, `/old` is deleted at sequence 40, and the basis manifest's floor is 50. Neither walk reaches `/old` because it's unbound and expired. Base compaction of `revisions` drops every revision of `a.txt`, base compaction of `bindings` drops the `/old -> a.txt` edge and the root's unbound edge to `old`, and `tombstones` keeps the `set` event.

#### Preservation and consistency checks

Several details protect correctness across compactions:

- **Restores by reference.** A restore copies a file revision without duplicating its layout, which would break if compaction dropped the old revision while the restore was still in flight. Because a manifest only records a new floor after folding every restore validated against the old floor (see [Advancing the retention floor](#101-advancing-the-retention-floor)), the restore's own revision row is already in the compaction's basis. `attributes` and `access` follow the same rule.
- **Binding parity.** Summary rows count as versions of their edge, and an unbound binding acts as a tombstone that MUST stay until a bottom-anchored compaction removes it together with all older versions it hides. Compaction digests verify that binding events match across both binding indexes, and manifest row counts verify that each run's slot index has at least as many rows as its child index.
- **Cleared states.** A binding row's position comes from the delta that published it, so later binds don't depend on earlier unbound rows. An empty attribute map or an empty access row (no boundary, no grants) is kept when it's the state at the floor, because dropping it would expose an older row and break revision validation on the next update.
- **Trash ordering.** A trash `removed` marker sorts before its matching `listed` entry, letting bottom-anchored compaction drop the cancelled pair cleanly without exposing an older entry.
- **Ambiguous history.** A rewrite MUST refuse any file revision, attribute, or access history where two rows for one inode share the same revision number at or below the retention floor.

Compaction MUST preserve visible metadata at every retained sequence. It publishes a complete replacement through the numbered manifest protocol, and its input segments stay intact until no protected manifest or snapshot references them.

### 10.4 Streaming compaction

Compaction rewrites its selected runs as a stream without keeping all rows in memory. It writes new segments with fresh IDs under `segments/`, then publishes them in the next manifest. Readers keep using the previous manifest until that publication succeeds.

A runtime claims the namespace's compactor epoch before its first compaction after opening the namespace by publishing a manifest with `compactor_epoch + 1` and no other changes. Concurrent family groups in the same runtime share that claim. Every compaction publication MUST match the current epoch, and each compaction checks the epoch before reading segments. Because a large compaction may run for hours before publishing once at the end, the epoch lets a newer runtime fence a stale compactor promptly. (Grep publishes every few seconds, so a lost race costs at most one interval and grep doesn't need an epoch.)

Before each publication attempt, a compaction checks its elapsed monotonic time and finds the current manifest (see [Publishing a manifest](#72-publishing-a-manifest)). Its selected input segments MUST still be present and unchanged. While both checks pass, a lost numbered put retries against the newer manifest. A changed epoch returns `fenced`, and changed inputs return `abandoned`.

Compaction enforces two deadlines:

| Deadline | Starts | Limit | Past the limit |
| --- | --- | --- | --- |
| Compaction deadline | Before reading input | Last publication attempt starts within `METADATA_COMPACTION_BUDGET_MS`. | Returns `abandoned`. |
| Publication deadline | When finalization starts | Every finalization attempt runs within `METADATA_PUBLICATION_BUDGET_MS`. | Fails with the metadata publication budget error. |

Unreferenced segments are protected until their provider age exceeds the grace window (24 hours). The compaction deadline plus the publication completion bound (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)) is strictly shorter than that window:

```text
compaction deadline + publication completion bound < grace window
```

With the current constants, the last attempt starts within 12 hours and finishes within 12 hours and 25 minutes, well before any unreferenced output becomes eligible for deletion. Cancelled or crashed compactions leave orphaned outputs for garbage collection to remove after the grace window. A restart simply plans fresh from the current manifest with no durable compaction cursor or protection record.

## 11. Garbage collection

Garbage collection deletes objects that no retained view needs after checking references, age, and publication bounds. A logical deletion never allows file bytes to be removed immediately: a [pool's](glossary.md#pool) content stays while any member's root refers to it, and a deleted member's tombstone stays a root until its retirement deadline (see [Content roots](#119-content-roots)). Retirement itself deletes no content (see [Retirement](#95-retirement)).

Unreferenced objects aren't deleted immediately because an in-flight publisher might still be about to reference them. Garbage collection waits until each unreferenced object is older than the grace window, guaranteeing that any publication planned against an older state has already finished or failed (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).

### 11.1 One complete collection

LoonFS splits garbage collection into two separate jobs:

- **Namespace collection** deletes a namespace's unreferenced manifests, data WAL objects, metadata and extension segments, and snapshots, while keeping fences and floor objects. It retires the namespace once its tombstone is eligible (see [Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone)) and writes a tombstone for an initializing namespace whose manifest 1 is older than the grace window (see [Deleting a namespace](#94-deleting-a-namespace)). Its only request under `pools/` is deleting the namespace's [member entry](glossary.md#member-entry) during retirement.
- **Pool collection** deletes a [pool's](glossary.md#pool) unreferenced content objects, upload records, and temporary objects using roots gathered across every member (see [Content roots](#119-content-roots)).

A namespace collection begins by establishing roots:

1. Find the namespace's newest manifest (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)).
2. List all snapshot keys, read each snapshot record, and load the manifests they protect.
3. Record the protected manifests, protected segments, and the newest manifest's `folded_wal_no`.

If the current manifest is invalid or unreadable, the collection stops without deleting anything. An absent namespace has nothing for core garbage collection to delete.

The collection reads no WAL objects and doesn't probe for a successor to step 1's manifest, because it protects every WAL number above `folded_wal_no` directly (see [Reference roots](#112-reference-roots) and [Replaying the visible WAL](#42-replaying-the-visible-wal)).

If step 1 finds a [retired](#95-retirement) namespace, there are no roots and the collection reads no manifest, WAL object, or snapshot record. It lists `namespaces/{namespace_id}/` once, deletes every manifest, data WAL object, metadata segment, extension segment, and snapshot record that meets its family's age rule in [Candidate and age rules](#113-candidate-and-age-rules), and leaves the boundary object, fences, floor objects, and unrecognized keys alone. It raises no boundary, deletes no fork snapshot, and returns a normal summary rather than `namespace_deleted`.

Each collection lists its candidate families completely from the start and keeps no durable progress, cursor, or reference table. A pool collection keeps exact root sets only for members' WAL tails and upload sessions, sharing its memory budget with extent filters (see [Content roots](#119-content-roots)). Snapshot cleanup reuses the root-gathering snapshot listing, and upload-session cleanup reuses the pool's session listing.

To delete old manifests, a namespace collection lists the manifests, picks the deletable ones under [Candidate and age rules](#113-candidate-and-age-rules), raises the [manifest boundary](glossary.md#manifest-boundary) to the highest chosen number, and then deletes those exact manifests. If the chosen list exceeds the memory budget, it processes them in ascending number order, raising the boundary to each batch's highest number before deleting that batch. Raising the boundary loads the boundary object, skips writing if it's already high enough, and otherwise creates it (put-if-absent) or updates it (compare-and-swap), retrying on conflict. If raising fails, the collection stops before deleting any more manifests. If no manifests are chosen, no boundary request is sent. The boundary isn't a resume cursor: every collection picks candidates from its own fresh listing. For example, if the boundary is 30, the newest manifest is 45, snapshots keep 7 and 38, and manifests 31 to 44 are older than the grace window, the collection picks 31 to 37 and 39 to 44, raises the boundary to 44, and deletes those 13 manifests while keeping 7 and 38 (see [The manifest boundary](../design/manifest-boundary.md#cleaning-up-manifests)).

Every age check in a collection uses one `now_ms` captured at the start. Later collections capture fresh roots and a new clock. Concurrent collectors can delete eligible objects independently, missing objects are harmless, and a failed collection can simply be run again from scratch.

### 11.2 Reference roots

| Evidence captured for the collection | Objects protected |
| --- | --- |
| The current active manifest | The manifest and every segment it lists. |
| The current deleted manifest (tombstone), until retirement | The tombstone and every segment it lists. |
| Every recognized snapshot key in the complete listing | The manifest numbered in its ID and every segment in that manifest. |
| The newest manifest number the collection found | Every manifest numbered above it, because other processes may publish newer manifests afterward. (This doesn't protect their runs.) |
| The current active manifest's folded boundary | Every WAL number above `folded_wal_no`. |

The segments a manifest lists include both its metadata run segments and the extension segments in its `extensions` entries (see [Extensions](#124-extensions)). A tombstone's extension entries list no segments.

Because the manifest number is embedded in the snapshot key, root gathering doesn't need snapshot bodies (bodies are read later for owner and expiry checks). If a snapshot's manifest is missing, the collector reads the snapshot body. The snapshot doesn't require the missing manifest if the body is gone (for example, another collection deleted the snapshot), if the snapshot is a user snapshot, or if the namespace is deleted and the snapshot is the namespace's own fork point snapshot. A user snapshot whose manifest is missing is a candidate that was never verified (a verified snapshot protects its manifest from the moment it's acknowledged), and garbage collection deletes it under the snapshot cleanup rules, or once the record's provider age is at least `T` (see [Snapshot cleanup](#117-snapshot-cleanup)). Any other snapshot pointing to a missing manifest is corruption. Invalid or unreadable manifests always fail the collection. Any listed snapshot whose manifest exists protects its segments for the entire collection, even if that collection later deletes the snapshot.

A manifest below the newest manifest isn't a root unless a snapshot keeps it, no matter how recently it was replaced. Protecting recently replaced manifests would force every collection to list and load them. Instead, reads and writes that hit a missing segment on a replaced manifest retry on the current manifest (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), and operations that need a stable older manifest take a snapshot (see [Snapshots](#8-snapshots)).

Advancing the retention floor past a snapshot's folded head doesn't remove the snapshot's protection, and reads through the snapshot read that manifest's runs directly (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot) and [Reads, deletion, and expiry](#83-reads-deletion-and-expiry)).

A tombstone keeps its metadata runs until retirement, which doesn't read them (see [Retirement](#95-retirement)). A retired namespace has no roots: its boundary marks every manifest dead, and its snapshots become ordinary candidates.

### 11.3 Candidate and age rules

Being unreferenced makes an object a candidate, not immediately deletable. Provider age is calculated from the listing's modification time, and an object listed without a timestamp is kept. Let `T` be the grace window (`GC_GRACE_WINDOW_MS`, 24 hours), shared across every collection and always at least `PUBLICATION_COMPLETION_BOUND_MS`.

| Family | Conditions for deletion |
| --- | --- |
| Namespace manifest | Below the newest manifest found, not a root (see [Reference roots](#112-reference-roots)), and provider age at least `T`. |
| Namespace manifest in a retired namespace | Provider age at least `T` (tombstone included). |
| Data WAL object in an active namespace | At or below `folded_wal_no` and provider age at least `T`. |
| Data WAL object in a deleted or retired namespace | Provider age at least `T` (no current WAL object is protected). |
| Fence or floor object | Never deleted, in any namespace. |
| Metadata or extension segment | Listed by no root and provider age strictly greater than `T`. |
| Snapshot record | Owner-specific rules in [Snapshot cleanup](#117-snapshot-cleanup), or provider age at least `T` for a user snapshot whose manifest is missing. |
| Snapshot record in a retired namespace | Provider age at least `T`, regardless of owner. |
| Upload session | Status-specific rules in [Upload-session cleanup](#116-upload-session-cleanup). |
| Content object under a pool's prefix | Matched by no extent filter in any member's layout view, referenced by no tail or session root (see [Content roots](#119-content-roots)), and provider age at least `T`. |
| Temporary object | Provider age at least `T`. |
| Member entry | The namespace in the key is retired, has no manifest, or records another pool (see [Content roots](#119-content-roots)). |

The manifest boundary and the current manifest of a non-retired namespace are never deleted. A retired namespace has no current manifest, so its tombstone is deleted by age like any other manifest. Core garbage collection leaves unrecognized keys untouched. A missing or future timestamp never satisfies an age check.

For example, if a collector finds manifest 10 as newest and no snapshot keeps 8 or 9, it keeps manifest 10, all higher numbers, and all segments listed by manifest 10 or any snapshot. Manifests 8 and 9 are deleted once their provider age reaches `T`. A segment listed only by 8 or 9 becomes a candidate immediately, and is deleted once its provider age exceeds `T`.

Collections distinguish fences and floor objects from data WAL objects using object size from the listing alone, avoiding a GET per small WAL object on every pass. Any WAL object of at most `MAX_FENCE_OR_FLOOR_OBJECT_BYTES` (572 bytes) is a fence or floor object and is always kept. Larger ones are data WAL objects (writers pad small data WAL objects above 572 bytes, see [WAL records](#a5-wal-records)). For example, if folded WAL objects 1 through 30 include floor object 12 and fence 25 (300 bytes or less) alongside 28 data objects (over 572 bytes), the collector keeps 12 and 25 and deletes the other 28 without reading any of them.

### 11.4 Clock and operation assumptions

Because garbage collection measures age as `now_ms.saturating_sub(last_modified_at_ms)`, safety depends on bounding how much an object's age can be overstated due to clock skew, timestamp granularity, or process pauses.

`GC_SAFETY_MARGIN_MS` (300,000 ms, or 5 minutes) is one combined allowance covering relative clock skew, timestamp precision, and scheduling delays together, not 5 minutes per participant. A provider clock ahead of the collector merely delays cleanup rather than risking premature deletion. The same allowance bounds the difference between the clocks of any two hosts, because LoonFS compares one host's clock with a time that another host recorded (a creation, expiry, completion, deletion, or commit time). For example, once the collector's clock passes a user snapshot's expiry plus `T`, every host finds the snapshot expired, and a retention advance compares the advancing host's clock with the `committed_at_ms` that a writer recorded.

Publication budgets track monotonic elapsed time on a clock that advances while the host sleeps. A forward wall-clock jump of 1 second or more also counts as elapsed time, so a frozen process (such as a restored VM snapshot) detects an expired budget once its clock syncs. The **publication completion bound** (`PUBLICATION_COMPLETION_BOUND_MS`) is the metadata publication budget plus the provider publication request bound plus the safety margin, and the grace window `T` is at least that long. Compaction reserves this bound inside `T` (see [Publication and collection timing](#c1-publication-and-collection-timing)).

A client timeout never proves a remote write failed (see [Timing assumptions](#33-timing-assumptions)). A WAL put with an unknown outcome returns `outcome_unknown`, and the caller reloads and replans from the current state.

Neither WAL puts nor manifest puts depend on how late the provider applies them for ordering safety: fences and floor objects are never deleted, a fence put or non-writer floor put checks its number against the newest manifest's `folded_wal_no` after succeeding (see [A summary of the log protocol](#26-a-summary-of-the-log-protocol), [Publication and group commit](#63-publication-and-group-commit), and [Writer ownership](#61-writer-ownership)), a writer session writes a fence with its own epoch after a WAL put whose outcome it doesn't know, and a manifest publisher checks the [manifest boundary](glossary.md#manifest-boundary) after its put so any write into a recycled number counts as lost (see [Publishing a manifest](#72-publishing-a-manifest)). That fence is what keeps this true for a writer session's own put. A put whose outcome is unknown may be written long after the session moved on, even at a number that GC emptied, but only at or below the fence's number. So it either collides or is written where no reader replays it, and the session never acknowledged it.

Manifest publications (including epoch claims) check `METADATA_PUBLICATION_BUDGET_MS` right before their put, not after. That check ensures new segments written by a fold or compaction are referenced before their age exceeds `T`. Every put that creates a user snapshot, fork snapshot, or fork point snapshot record, a pool member entry, or an upload session record also checks the budget, including each retry after a put whose outcome was unknown, and so does every CAS that extends a user snapshot. So every put that makes one of those records live, or keeps a snapshot live longer, starts within the budget (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot) and [Extending a snapshot](#84-extending-a-snapshot)). A put that marks a snapshot deleted doesn't check it, because a deleted record keeps no snapshot usable. Both checks rely on one operational requirement that LoonFS can't enforce itself: if a provider applies a request at all, it MUST do so within `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` of when the client started that operation.

Direct expiry checks don't add `T` to the requested lifetime. A host whose clock is `E` milliseconds ahead may reject an expired upload or user snapshot up to `E` milliseconds early, and `E` is within the clock allowance. The grace window doesn't make hosts agree on the moment of expiry. It keeps the record until every host finds it expired.

### 11.5 Publication during collection

LoonFS acknowledges a new snapshot from the current head only after re-checking its manifest after the snapshot write, closing the race between garbage collection reading the current manifest and listing snapshots. A fork is protected either by the user snapshot it's built from or by its fork snapshot written before that user snapshot is re-checked (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)).

After probing for a manifest's successor, a process reads the manifest boundary and discards any manifest at or below it, so a deleted successor can't masquerade as the end of the chain (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). Metadata segments have no equivalent check (see [Reference roots](#112-reference-roots)).

New metadata and extension segments are protected by `T` while being written and verified. Streaming compaction MUST start publication before its budget expires and verify its epoch and inputs before publishing.

Any failed read of a required root aborts garbage collection, except in pool collection, where a member whose roots can't be read, or an extent filter that can't be loaded, makes the collection skip content deletion (see [Content roots](#119-content-roots)). Every cleanup step MUST keep the durable evidence it needs to retry safely after a crash.

### 11.6 Upload-session cleanup

Upload session records are stored under the pool's prefix, so pool collection cleans up sessions for all members and namespace collection ignores them. Each session records its `namespace_id`. Namespace retirement, or reaching a deleted namespace's retirement deadline (see [Retirement](#95-retirement)), overrides the ordinary session status rules.

Pool collection compares a deleted namespace's retirement deadline against its own clock and applies retired-namespace rules from that deadline onward, without waiting for actual retirement. That prevents a fork snapshot on a deleted namespace from keeping unpublished uploads alive for the fork's entire lifetime. It's safe because upload admission evidence works only in the session's own namespace (see [Admission proofs](#55-admission-proofs)), every commit in a deleted namespace finishes before its tombstone (within 25 minutes of `deleted_at_ms`), and forks, merges, and rebases only copy already-committed references (see [Cross-namespace copies and moves](#97-cross-namespace-copies-and-moves)). A namespace whose boundary is retired or that a full walk doesn't find is treated as retired. If its state can't be determined, the session is kept (see [Content roots](#119-content-roots)). A session of an initializing namespace follows the rules of an active namespace, because a retried create or fork can finish the namespace, and a writer can then commit the session's content.

| Session and namespace | Action |
| --- | --- |
| Open session | Keep until expiry plus `T`, then write an aborted outcome record and clean up its content and provider transfer state. If an outcome record already exists, keep the session. |
| Aborted session | Keep until abort time plus `T`, then run content and provider cleanup again and delete its records. |
| Completed session in an active or initializing namespace | Keep until the completed-content grace passes, then delete its records. Content follows [Content roots](#119-content-roots). |
| Session in a deleted namespace, before retirement deadline | Keep it and report the deadline. |
| Session in a deleted namespace, at or after retirement deadline | Apply retired-namespace rules, even if a snapshot delays retirement. |
| Completed session in a retired namespace | Delete its records immediately (no extra completion grace). Content follows [Content roots](#119-content-roots). |
| Orphan outcome record (no matching session record) | Delete once its provider age is at least `T`. |

Garbage collection always deletes the session record before the outcome record. Deleting the outcome first would make a crash leave an apparently open session that a later pass would abort, potentially deleting committed content. An orphan outcome record never issues admission evidence and protects no content. It appears only if a collector crashed between the two deletes or if a late completion or abort wrote it after the session was cleaned up (see [Upload sessions](#51-upload-sessions)). Waiting until an orphan outcome's provider age reaches `T` protects against non-atomic listings that miss a newly created session record while catching its outcome record.

Because an outcome key sorts right after its session key (see [Object keys](#a8-object-keys)), a single listing of the uploads prefix shows which sessions have ended without reading each record.

Before completion, a session exclusively owns its random content ID and can't issue admission evidence. Cleanup writes the aborted outcome record first, then deletes the content object and aborts any provider multipart transfer. LoonFS keeps an aborted session's records until abort time plus `T` for three reasons. During that window a repeated abort returns the original `aborted_at_ms` and a status read returns `aborted`, as the [API specification][api-spec] describes. The cleanup at the end of the window sends the provider's multipart abort and the content object's delete a second time, so it removes an object written after the first cleanup (for example by a direct PUT through a URL issued before the abort). Pool collection runs that second cleanup even in a pass that skips content deletion (see [Content roots](#119-content-roots)), so an object written after the first cleanup doesn't have to wait for a pass that deletes content. The pass runs it if it read the namespace that created the session, or if it read every listed member and found no member entry for that namespace (so the namespace is treated as retired). A pass that stopped before it read every listed member keeps the records of a session whose namespace isn't one of the members it read, for a later collection. A session of a deleted namespace waits for the retirement deadline first, as the table above shows. Failed cleanups leave the records for retry. Open and aborted sessions still require provider cleanup even after namespace retirement because provider multipart state isn't visible in object listings. Pool collection derives every content key and provider target from the pool ID and the session's `content_id`.

A completed session record remains a content root while it exists, including during the collection that deletes it, so unreferenced content is deleted on a later pass once no other root points to it (see [Content roots](#119-content-roots)).

The completed-content grace covers every possible piece of admission evidence:

```text
completion
|<-- receipt issuance window -->|
                                |<-- final token lifetime -->|
                                                             |<-- publication completion bound -->|
|<--------------------------------- completed-content grace (eight days) ------------------------------------>|
                                                                                               earliest cleanup
```

In milliseconds:

```text
CONTENT_RECLAMATION_GRACE_WINDOW_MS
    >= COMPLETED_UPLOAD_RECEIPT_WINDOW_MS
       + CONTENT_RECEIPT_TTL_MS
       + PUBLICATION_COMPLETION_BOUND_MS
```

With the current constants, that's 8 days against 7 days plus 1 hour plus 25 minutes. Every token issue checks the original completion time so a receipt can't extend its window. In-process proofs expire at the end of the admission window, and publication checks expiry immediately before the WAL put. Once the 8-day grace passes, an unpublished upload can never be committed for the first time, while published content stays as long as any content root refers to it.

### 11.7 Snapshot cleanup

User snapshots become collectable after the earliest of: their expiry plus `T`, their deletion plus `T`, or (in a deleted namespace) their creation plus `T`. A user snapshot with no expiry stays in an active namespace until explicitly deleted. Retained user snapshots report the earliest of those times in `next_reclamation_at_ms`. Snapshots are deleted directly, and snapshot IDs are never reused.

A source namespace's collection never deletes fork snapshots and reads nothing under any fork's prefix. Instead, the fork's retirement is the sole path that deletes its fork snapshot. That's safe because a fork call checks `METADATA_PUBLICATION_BUDGET_MS` before putting the fork snapshot and only starts after writing or finding the fork's manifest 1, while the fork's retirement can't begin until its tombstone is older than `T` (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)). The call's last read of the target comes after the budget starts, because a call that found the target initializing before the budget started reads the target again (see [Finishing an initializing namespace](#finishing-an-initializing-namespace)).

The fork's own collection keeps its fork point snapshot too. After the fork is deleted, its collection keeps that snapshot even if the snapshot's manifest is missing, without reporting corruption, because a finishing fork call can write the snapshot against a manifest that a concurrent delete and collection removed (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)). Nothing reads a deleted namespace's fork point snapshot, and the tombstone lists the same runs. Both snapshots are deleted during retirement right before raising the boundary to retired (see [Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone)).

If the creation of a user snapshot fails, its creator deletes the candidate snapshot by setting `deleted_at_ms`, and GC deletes the record by the rules at the start of this section, like any other deleted user snapshot (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)). If the creator's delete fails, or the creator stops before the delete runs, the key is left with the undeleted candidate or empty, and a put of the creation that the provider applies later creates an undeleted candidate. If the candidate's manifest is still stored, garbage collection applies the ordinary user snapshot rules to the record, so the candidate is listed and usable like any other user snapshot until it expires, and a candidate with no expiry stays until someone deletes it. A successor may have replaced the candidate's manifest, and a collection may already have deleted that manifest. Garbage collection deletes a user snapshot whose manifest is missing once the record's provider age is at least `T`, even if the snapshot has no expiry, and logs a warning. (Waiting for that age keeps garbage collection from deleting a candidate that its creator is still verifying or deleting. Until then, the record is listed, and a read through it returns `snapshot_not_found`.) A verified snapshot protects its manifest from the moment it's acknowledged, so a user snapshot loses its manifest only as an unverified candidate. A fork snapshot's creator never deletes it on either the source or the fork (see [Forking a namespace](#92-forking-a-namespace)).

### 11.8 Retiring an eligible tombstone

After sweeping its candidate families, a namespace collection retires the namespace if its tombstone is eligible (see [Retirement](#95-retirement)):

1. If the tombstone has a fork basis, delete the fork snapshot on the source and the namespace's own fork point snapshot, adding each to `deleted_snapshots_by_owner.fork` only if it was present.
2. Delete the namespace's [member entry](glossary.md#member-entry) at `pools/{pool_id}/members/{namespace_id}.json`.
3. Raise the namespace's [manifest boundary](glossary.md#manifest-boundary) to retired, creating it with put-if-absent if missing or updating it with compare-and-swap (retrying on conflict). If it's already retired, do nothing.

If any step fails, the collection fails and the tombstone stays the newest manifest so the next collection retries from step 1. Because deleting a missing object succeeds and absent snapshots aren't counted, every step is safely idempotent without any progress journal. Later collections see the retired boundary and sweep the namespace prefix as [One complete collection](#111-one-complete-collection) describes.

For a deleted namespace, LoonFS refuses every request that starts an upload session, returns a presigned upload URL, or completes an upload. Preparing content above the inline threshold starts an upload session, so LoonFS refuses that too. A runtime checks its cached head for a successor at least every `READ_REVALIDATION_BOUND_MS` (20 minutes), so it can still stage content or grant one of those requests for up to 20 minutes after another process deletes the namespace. The 24-hour grace window before the retirement deadline covers the 20 minutes in which such a request can start. LoonFS refuses to complete an open session when the request starts more than 20 minutes after the namespace was deleted, or at or after the session's expiry, and it refuses every completion of an aborted session, whenever the request started. So when a PUT through a presigned URL arrives after one of those points, only a completion request that was already in progress can admit the object, and only if the session isn't aborted and the object is written before the request verifies the object (for a multipart upload, before the request asks the provider to assemble the parts). Pool collection deletes an object that no completion admits, whatever the URL's lifetime. A completion request has no time limit, so one that started in those 20 minutes can record its outcome after the deadline. The session's content still can't be published, because a deleted namespace refuses commits, and from the deadline on, pool collection applies the rules of a retired namespace to the session, so it deletes the records of a completed session without the completed-content grace ([Upload-session cleanup](#116-upload-session-cleanup)).

### 11.9 Content roots

Because any member of a [pool](glossary.md#pool) can reference any content object under `pools/{pool_id}/content/`, pool collection gathers roots from every member rather than letting members collect content independently. A content object is kept while any member's extent filter matches its name, or any member's WAL tail or pool upload session refers to its chain:

| Root | Content it protects |
| --- | --- |
| A member's layout view: its current manifest (while active, or deleted before its retirement deadline) and every manifest kept by its snapshots (see [Reference roots](#112-reference-roots)) | Objects whose names appear in the [extent filter](#extent-filter) of any `content_layouts` segment in those manifests (covering every extent of every layout row). |
| A member's unfolded WAL tail (tombstones have none, since deletion folds the full WAL) | Every chain referenced by an `append_file_revision` delta and every extent in the tail's layouts. |
| Any upload session record under the pool's prefix, regardless of status | The session's `content_id`. |

Every other valid content object key under `pools/{pool_id}/content/` is deletable once its provider age is at least `T`. Keys that don't parse as content object keys are left alone. Namespace collections never list content. Pool collection collects content across active and deleted members alike (see [Retirement](#95-retirement)).

#### Discovering pool members

Each pool member has a [member entry](glossary.md#member-entry) at `pools/{pool_id}/members/{namespace_id}.json`, written after manifest 1 (see [Creating a namespace](#91-creating-a-namespace)) and deleted at retirement (see [Retirement](#95-retirement)). Pool collection lists `pools/{pool_id}/members/` once without reading the entry payloads. For each listed namespace, it finds the newest manifest (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)) and classifies it:

- **Active or deleted, recording this pool:** It's a member. The collection reads its roots, then lists its snapshots and extracts each snapshot's manifest number from the key without reading snapshot bodies. If a snapshot's manifest is missing, the collection reads that snapshot's body. If the body is gone, the snapshot is a user snapshot, or the member is deleted and the snapshot is the member's own fork point snapshot, the snapshot protects nothing. (A user snapshot whose manifest is missing is a candidate that was never verified. Nothing reads a deleted namespace's fork point snapshot, and its tombstone lists the same runs.) Pool collection doesn't delete either snapshot. The member's own collection deletes the user snapshot under the snapshot cleanup rules, or once the record's provider age is at least `T`, and retirement deletes the fork point snapshot (see [Snapshot cleanup](#117-snapshot-cleanup)). Any other snapshot whose manifest is missing makes the member unreadable.
- **Initializing:** It has no roots. Initializing namespaces reject commits (no WAL tail), a fork's manifest 1 lists only segments covered by the source's snapshot, and any object a fork writes for its manifest 2 is younger than `T`. A tail-folding fork's manifest 2 can also refer to older content that only the source's WAL tail referred to. That content is protected by the deadline for the fork's active manifest, as "A fork created during a collection" below explains. The namespace's upload sessions follow the rules of an active namespace (see [Upload-session cleanup](#116-upload-session-cleanup)).
- **Retired, missing manifest, or recording another pool:** It isn't a member and has no roots. The collection deletes its member entry, cleaning up any stale entry written by a slow create or fork call after retirement.
- **Unreadable:** The collection skips content deletion for this pass, as described below.

A deleted member's tombstone stays a layout view only until its retirement deadline (`deleted_at_ms + GC_GRACE_WINDOW_MS`, compared against the collection's `now_ms`). Nothing reads a tombstone's own view after deletion (forks read through their fork snapshots, which stay roots), so dropping the tombstone at the deadline prevents a deleted namespace with a fork from pinning post-fork content forever. Pool collection doesn't retire the namespace itself, and a collector with a lagging clock simply keeps the tombstone's content a bit longer.

Pool collection reads each member once and MUST finish reading all members' roots within 12 hours (`POOL_ROOTS_READ_BUDGET_MS`), measured monotonically from when it captured `now_ms`. If it exceeds that budget, it skips content deletion and reports `content_deletion_pool_budget`. If any listed member is unreadable, it skips content deletion and reports `content_deletion_unreadable_member`. A pass that stops reading members for either reason can't tell which namespaces are retired, so it keeps the upload sessions of every namespace that isn't one of the members it read (see [Upload-session cleanup](#116-upload-session-cleanup)). If the collection reads every listed member but can't load the extent filter of a segment that a member's layout view lists, it also skips content deletion and reports `content_deletion_unreadable_member`, and it cleans up upload sessions from the statuses of the members it read, as a pass that deletes content does. (A member can publish a compaction after the collection reads its roots, and the member's own collection can then delete the replaced segment before the pool's collection loads that segment's extent filter.) In each case it still lists the content prefix once to measure `listed_bytes`, finishes the rest of the collection, and leaves other pools unaffected.

A pool with no member entries has no roots, so collection deletes its content and temporary objects by age and cleans up its upload records under [Upload-session cleanup](#116-upload-session-cleanup). If no objects exist under the pool's prefix at all, collection returns `pool_not_found`.

#### Collection steps

Pool collection runs in this exact order:

1. List the pool's upload sessions (before reading any member's WAL tail).
2. List the pool's member entries.
3. For each listed namespace, find its newest manifest and either gather its roots or delete its stale member entry.
4. Deduplicate the `content_layouts` segments across all members' layout views. Because forks inherit their source's segments, each distinct segment is processed once.
5. Read each distinct segment's extent filter and charge it to the collection's memory budget.
6. List `pools/{pool_id}/content/` once. Protect any object whose name (key minus prefix) matches an extent filter or whose chain is referenced by a tail or session root, and delete any other valid content object with provider age at least `T`.
7. Delete temporary objects older than `T`, and run [Upload-session cleanup](#116-upload-session-cleanup) on the session records.

Pool collection writes one kind of object, the aborted outcome record of an expired upload session (see [Upload-session cleanup](#116-upload-session-cleanup)), and deletes only content objects, temporary objects, upload records, and stale member entries.

Storing an extent filter inside each layout segment avoids scanning every layout row across every rooted view and keeps collector memory small (around 2.5 MB for one million files at 20 bits per name), while letting the segment cache reuse filters across passes. The collector checks each segment's filter independently rather than merging views, so an object referenced only by a superseded layout row or a Bloom false positive stays until compaction drops the row and no view lists the segment.

Tail and session IDs protect all objects in their chains. A presigned download URL for a revision that's current when issued stays valid for its full `DIRECT_TRANSFER_URL_TTL_MS` (15 minutes), because the current manifest keeps replaced revisions for at least `T` (24 hours), far longer than the revalidation bound, URL lifetime, and clock allowance combined (see [Advancing the retention floor](#101-advancing-the-retention-floor)). A URL for an already-replaced revision can stop working before expiry once base compaction drops the revision and collection removes its content.

A tail revision delta omits its layout only when the commit includes an inline piece at offset 0 or continues a chain with an existing revision (see [WAL records](#a5-wal-records)), in which case retention keeps the base revision and layout compaction keeps its row until the append folds. Fork snapshots keep the source manifest's layouts protected, and fork point snapshots keep the fork's manifest (including any source tail history folded into it) protected for the fork's lifetime.

#### Why the ordering and 12-hour read budget keep references safe

Listing upload sessions before reading any WAL tail ensures no newly committed content slips through: any session absent from the listing was either created later (so its content is younger than `T`) or removed earlier (meaning every commit that could admit it already succeeded before any tail was read, so a tail or protected manifest now references it).

The 12-hour `POOL_ROOTS_READ_BUDGET_MS` cap handles references moving between pool members while a collection runs. Consider a merge copying a reference from a fork to its target when the collector reads the target before the merge commits and the fork afterward. While planning, the merge takes a snapshot of the fork (see [Cross-namespace copies and moves](#97-cross-namespace-copies-and-moves)) that expires after `OPERATION_SNAPSHOT_TTL_MS`, and that snapshot's record stays for `T` after the earliest of its expiry, deletion, or (in a deleted fork) creation (see [Snapshot cleanup](#117-snapshot-cleanup)). Because the plan's content proofs expire with the snapshot and are checked right before the WAL put, the merge commits within `OPERATION_SNAPSHOT_TTL_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS` of being prepared. The collector therefore reaches the fork while the snapshot still protects the content, because:

```text
POOL_ROOTS_READ_BUDGET_MS + GC_SAFETY_MARGIN_MS
    + OPERATION_SNAPSHOT_TTL_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
    < GC_GRACE_WINDOW_MS
```

The same inequality covers three other concurrent races:

- **A fork deleted during a merge.** The fork can't retire while the merge's snapshot record exists (see [Retirement](#95-retirement)), and that record stays until at least its creation plus `T`, so the collector still finds the fork's member entry, tombstone, and snapshots.
- **A revision replaced during a merge.** Any revision replaced after the merge was prepared stays in the fork for at least `T` after the replacing commit (see [Advancing the retention floor](#101-advancing-the-retention-floor)). Any revision replaced earlier is either in the snapshot's manifest or in the fork's WAL tail, where `MAX_TAIL_COMMIT_AGE_MS` (equal to `T` minus the other terms above) ensures retention can't pass it before the collector reads the fork (see [Cross-namespace copies and moves](#97-cross-namespace-copies-and-moves)).
- **A fork created during a collection.** A fork whose member entry is written after step 2 isn't visited by this collection. References it receives later from merges or replays are protected by the source snapshot as above. References it starts with come either from the source manifest kept by its user snapshot or from folding the source's WAL tail. Any new span objects written by the fold are younger than `T`, while existing objects referenced from the source's tail are protected because the fork only folds a tail whose commits are at most `MAX_TAIL_COMMIT_AGE_MS` old and becomes active within `OPERATION_SNAPSHOT_TTL_MS` (see [Forking a namespace](#92-forking-a-namespace)). Because step 3 reads a member's manifest and tail before listing its snapshots, either the source's snapshot listing includes the fork's snapshot (protecting its manifest even after expiry) or the snapshot was created on the manifest the collector just read (or a newer one). In every case, every reference the new fork starts with was already rooted in the source when the collector read it, or arrived afterward.

## 12. Encodings, versions, and extensions

Storage versions describe how durable objects are encoded and operated on. API versions describe request and response contracts. Changing one doesn't automatically change the other.

### 12.1 Field conventions

Durable field names and enum values use `snake_case`, and tagged unions use `kind`. `status` describes a resource's lifecycle, and `phase` describes a computation's progress (tagged objects in either use `kind`).

Field name suffixes follow these conventions:

| Suffix | Meaning |
| --- | --- |
| `_id` | An identifier. |
| `_seq` | A position in the namespace's commit history. |
| `_no` | A counter scoped to its resource. |
| `_index` | A zero-based position in a collection. |
| `_number` | A one-based position defined by a provider or tool. |

Absent optional fields are omitted when written. Required fields are always written, even when zero, false, or empty. Fingerprint preimages aren't ordinary durable records and follow their own rules (see [Appendix B](#appendix-b-semantic-commit-fingerprints)).

CBOR records store an inode ID as a 16-byte big-endian byte string, while JSON documents (and commit fingerprint preimages) store its 26-character Crockford base 32 spelling (see [Numbered objects and generated IDs](#23-numbered-objects-and-generated-ids)). Neither durable form includes the public API's `ino_` prefix.

### 12.2 Envelopes and checksums

Structured control records, manifests, and WAL objects use an envelope with `kind`, `format_version`, `payload_checksum`, and `payload`. Content objects store raw file bytes, and block segments use the sectioned layout in [Block-segment encoding](#a7-block-segment-encoding) instead of an envelope.

[Envelope layout](#a2-envelope-layout) defines how each envelope wraps its payload, which families use zstd compression, and which bytes `payload_checksum` hashes (the exact payload bytes in the document, never a re-serialized copy).

Decoders check `kind` and `format_version` first, then verify `payload_checksum` before decoding payload fields. Unknown kinds, unsupported versions, malformed payloads, and checksum mismatches are always hard errors, never reasons to fall back to another object.

Checksums use three encodings depending on context:

- **Envelope and manifest-reference digests:** `sha256:<64 lowercase hex>`.
- **Content, object, and part checksums:** `{ "algorithm": ..., "value": ... }`, because the store selects the algorithm.
- **Block handles:** a numeric `crc32c` field, because block segments always use CRC32C.

Commit fingerprints also prefix their canonicalization scheme. Per-block CRC32C values verify ranged block reads against their handles, segment descriptor checksums verify whole-segment reads, and envelope digests verify full payloads. Neither an unauthenticated checksum nor an object key grants authorization.

### 12.3 Strict decoding and evolution

Authoritative durable envelopes and their nested payloads reject unknown fields. That applies to immutable objects too, because folds and compaction re-encode their data and silently dropping an unknown field would lose durable state.

`ContentRef` and `Checksum` are closed shapes wherever they appear. Each content kind and checksum algorithm is its own closed shape. Unknown content kinds and algorithms are rejected, and adding a new content kind requires bumping the version of every durable family that embeds it.

API request bodies also reject unknown fields (including nested ones) so a misspelled precondition can't silently disappear. API response bodies generally tolerate additive fields except inside shared closed shapes, as defined in the API specification.

Versioning rules work by layer:

- The owning envelope's `format_version` governs its entire payload, including nested objects and collection semantics. Payloads don't add a separate version field.
- A metadata segment is its own object, versioned by its descriptor's `encoding` field, which covers both block layout ([Block-segment encoding](#a7-block-segment-encoding)) and row schemas ([Metadata rows and row keys](#a6-metadata-rows-and-row-keys)). A manifest can mix segments of different encodings, and compaction migrates rewritten rows to the current output encoding. (The manifest's `format_version` governs the descriptor itself.)
- Because a `commit` row embeds its WAL commit deltas, the `commits` family's encoding bumps whenever the WAL delta vocabulary changes.
- Extension segments are interpreted under their extension entry's `version` (see [Extensions](#124-extensions)).
- Strategy tags like `blob_v1` identify a closed content strategy and don't override the owning family's version.

Once the stable format ships, any change to a field's name, presence, type, tag, encoding, or meaning (including adding a field) requires a new family version. Older binaries already reject unknown fields, and bumping the version lets them fail cleanly at the version check while telling newer binaries which shape to decode. Before the stable format ships, changes keep version 1 so newer binaries reject objects written by older pre-release builds (for example, manifests missing segment `checksum` fields).

A change to the garbage collection protocol can also require a version bump even when most fields stay unchanged. An implementation MUST NOT operate on a newer protocol just because it can deserialize some fields. Every role that mutates durable state (manifest/WAL publication, compaction, snapshots, retention advances, garbage collection, and extensions) loads the current manifest before acting, or gets fenced by a newer binary's epoch claim, and refuses unsupported versions on load.

A reader supports a set of versions per family and a set of metadata segment encodings. It rejects any unsupported envelope version before verifying checksums or decoding payloads (see [Envelopes and checksums](#122-envelopes-and-checksums)), and rejects any unsupported segment encoding in a manifest before opening segments, reporting the encountered version in the error. After the stable format ships, readers will support every version from the first stable release through the newest version they write, since snapshots and forks pin old manifests by checksum. Writers emit the lowest version that can represent an object so older binaries remain compatible until a newer feature is used. Until the stable format ships, all families are version 1 and metadata segments are encoding 1.

Golden fixtures in `crates/loonfs-types/tests/golden_formats.rs`, grep fixtures, and fingerprint vectors pin exact reference bytes. The [compatibility table](../../RELEASING.md#format-compatibility) in `RELEASING.md` lists what each release reads and writes. [Family versions](#a1-family-versions) summarizes current versions. Rolling back a binary is safe only if the older binary supports every stored version and protocol (before the stable format, that means only the release that wrote the store), and downgrading never converts stored data.

### 12.4 Extensions

An extension is an optional subsystem that maintains derived state from a namespace's committed metadata and content (such as the grep index). Rather than managing separate manifests, extensions store their state inside the core manifest so they automatically get atomic publication, snapshot and fork protection, and segment garbage collection.

The manifest's `extensions` map keys each extension by name (1 to 64 lowercase ASCII letters, digits, or underscores, starting with a letter, such as `grep`). Each entry is a JSON object with two core-managed fields:

- `version`: the extension format version governing the entry's remaining fields and segment rows.
- `runs`: the extension's runs, each with `run_no`, `level`, and `segments` (see [Control and manifest payloads](#a4-control-and-manifest-payloads)). Core doesn't interpret `run_no` or `level`.

Core copies all other members of an entry verbatim without interpreting them. Extension segments are stored under the writing namespace's prefix:

```text
namespaces/{owner_namespace_id}/extensions/{extension_name}/segments/{segment_id}.sst
```

Core handles extension entries in three ways:

- **Publication.** Every publication copies `extensions` forward unchanged, except a namespace tombstone, which empties each entry's `runs` so the deleted namespace's extension segments become eligible for collection after 24 hours while leaving the rest of the entry as a diagnostic record. (Snapshots on earlier manifests still protect their own segments.) An extension updates its entry by publishing a manifest that changes nothing else, as a maintainer publication under the current compactor epoch without claiming a new epoch (see [Publishing a manifest](#72-publishing-a-manifest)). On a lost put, if the winning manifest kept the extension's entry unchanged (including both lacking an entry), the extension rebuilds its change over the winner because folds, compactions, and claims copy the entry verbatim. If the entry changed, the extension drops the attempt and replans from the current manifest.
- **Validation.** Core checks that segment IDs within an entry are unique and that each descriptor has positive row counts, valid key ranges, and valid block handles. Core never opens extension segments.
- **Collection.** Garbage collection deletes unreferenced extension segments under the segment rule in [Candidate and age rules](#113-candidate-and-age-rules) and leaves unrecognized keys under an extension's prefix alone.

A fork's first manifest copies the fork snapshot's `extensions` map without interpreting entries. Inherited segments keep their original `owner_namespace_id` and stay protected by the fork snapshot until the fork retires (see [Forking a namespace](#92-forking-a-namespace)). Each extension defines how a fork interprets an inherited entry.

Every extension MUST remain rebuildable from authoritative core state, and a missing extension can never make core metadata unreadable. Core readers don't read extension segments and don't need to understand an extension's version. [Appendix D](#appendix-d-grep-extension-format) defines the grep extension.

### 12.5 Reserved functionality

Version 1 defines only `file` and `directory` inodes. Several concepts are explicitly reserved for future versions:

- **Links (`link`).** Reserved as an inode with one parent binding and an immutable target inode ID substituted during path resolution. Version 1 defines no link rows, deltas, or operations.
- **Multiple parents.** Version 1 restricts every inode to at most one parent binding. No version will allow directories to have more than one parent.
- **Mounts.** Version 1 defines no mount creation or traversal.
- **Label-based namespaces.** Namespaces whose names are non-unique labels rather than keys (where siblings can share a name and path routes return `not_supported`) are reserved, with no selector field defined in version 1.

## 13. Alternatives considered

Here are the major format-level designs LoonFS considered and rejected. Design notes cover narrower choices in detail: [commit history](../design/commit-history.md#alternatives-considered), [inline small content](../design/inline-small-content.md#alternatives-considered), [directory bindings](../design/directory-bindings.md#costs-and-alternatives), [family backfill](../design/family-backfill.md#alternatives-considered), [pools](../design/pools.md#alternatives-considered), [merging a fork](../design/merge.md#alternatives-considered), [grep across a pool](../design/grep-across-a-pool.md#alternatives-considered), and [object checksums in references](../design/object-checksums.md#alternatives-considered).

- **Timed writer leases.** Expiring leases would force a new writer to wait out the old lease before taking over. Epochs and fences let a new writer take over immediately (see [Writer ownership](#61-writer-ownership)).
- **Mutable single-object upload sessions.** Updating one session object from open to completed or aborted via compare-and-swap would write the same key twice within a second, hitting per-key rate limits on some providers (see [Upload sessions](#51-upload-sessions)). Splitting into two write-once records keeps completion and abort racing on one put-if-absent, at the small cost of an extra key per finished session.
- **Per-namespace case sensitivity.** Letting each namespace pick case-sensitive or case-insensitive names forces a permanent choice at creation that forks inherit, and allows sibling collisions when trees are later synced to case-insensitive filesystems. One universal rule avoids the mismatch (see [Names and paths](#14-names-and-paths)).
- **Per-deletion expire commits in the WAL.** Committing an `expire` event for every deletion would cost one WAL event per deletion, require a writer scan loop, and stall reclamation on idle namespaces. One [floor object](glossary.md#floor-object) in the WAL orders the advance against all restores and expires every passed deletion at once (see [Advancing the retention floor](#101-advancing-the-retention-floor) and the [retention window](../design/retention-window.md#alternatives-considered) note).
- **Per-namespace content prefixes.** Storing uploads under each writing namespace's prefix would force cross-namespace copies or permanent cross-namespace snapshots just to share bytes across forks and merges (see [pools](../design/pools.md#alternatives-considered)). A shared pool prefix makes every content reference valid across all pool members and lets one collection gather all roots (see [File contents and ownership](#15-file-contents-and-ownership)).
- **Owner namespace on every content reference.** Repeating the storage prefix inside every reference and extent would duplicate the manifest's `pool_id` and make unsafe cross-pool references representable (see [pools](../design/pools.md#alternatives-considered)).
- **In-place appends by any pool member.** If two pool members appended to the same chain from the same length, one content ID would map to two conflicting byte sequences after a merge. Recording each chain's creator and branching a new chain for other members' appends prevents this at the cost of one content ID and one layout row (see [Operations and WAL deltas](#66-operations-and-wal-deltas)).
- **A separate durable WAL boundary object.** Tracking used WAL numbers in a boundary object (like manifests do) would add a read to every commit acknowledgement for no gain, because retained fences and floor objects already block writers from reusing numbers that another process took.

## Appendix A. Durable records and byte encodings

This appendix specifies the fields and byte encodings of all durable records. Field names are literal. A `?` suffix in a table marks an optional field omitted when absent (not part of the stored key name).

### A.1 Family versions

The five control object kinds are `snapshot`, `upload_session`, `upload_outcome`, `pool_member`, and `boundary`.

| Object | Envelope kind | Encoding | Version |
| --- | --- | --- | --- |
| WAL object | `wal_object` | zstd-compressed CBOR envelope with CBOR payload bytes | 1 |
| Namespace manifest | `manifest` | zstd-compressed JSON envelope with an inline JSON payload | 1 |
| Metadata segment | No envelope | Block sections (see [A.7](#a7-block-segment-encoding)), containing the rows in [A.6](#a6-metadata-rows-and-row-keys) | 1, given by the segment descriptor's `encoding` |
| Snapshot record | `snapshot` | Uncompressed JSON | 1 |
| Upload session record | `upload_session` | Uncompressed JSON | 1 |
| Upload outcome record | `upload_outcome` | Uncompressed JSON | 1 |
| Member entry | `pool_member` | Uncompressed JSON | 1 |
| Manifest boundary | `boundary` | Uncompressed JSON | 1 |
| Content object | No envelope | Immutable extent bytes | Not versioned. A layout lists the whole and span objects for a `blob_v1` chain prefix. |
| Grep segment | No envelope | Block sections with grep rows | 1, given by the grep entry's `version` |

### A.2 Envelope layout

Every enveloped object contains these top-level fields:

| Field | Representation |
| --- | --- |
| `kind` | The family's discriminator string. |
| `format_version` | The family's version as an unsigned integer. |
| `payload_checksum` | `sha256:` followed by 64 lowercase hexadecimal characters. |
| `payload` | A raw JSON sub-document for JSON families, or a CBOR byte string of encoded payload bytes for WAL objects. |
| `padding` | WAL only, optional: a CBOR byte string keeping data WAL objects larger than fences and floor objects (see [WAL records](#a5-wal-records)). |

To encode an enveloped object, a writer:

1. Serializes the payload (JSON for JSON families, CBOR for WAL objects).
2. Computes `payload_checksum` as the SHA-256 of those exact payload bytes.
3. Wraps those payload bytes in the envelope (as an inline JSON fragment or a CBOR byte string).
4. Compresses the entire document with zstd if it's a manifest or WAL object. Control objects are under 1 KiB and stay uncompressed, whereas manifests and WAL objects grow with the namespace (for example, 193 KiB of manifest JSON compresses to 36 KiB).

`payload_checksum` hashes the exact payload bytes in the document (including JSON whitespace and key spelling), not the outer envelope or compressed bytes, and neither writer nor reader re-serializes to check it.

Readers unconditionally zstd-decompress every manifest and WAL object (accepting any zstd level, with level 3 used by the reference writer) and reject any decompressed document exceeding `MAX_NAMESPACE_MANIFEST_BYTES` (see [Control and manifest payloads](#a4-control-and-manifest-payloads)) or `MAX_WAL_OBJECT_BYTES` (see [WAL records](#a5-wal-records)). After verifying `kind`, `format_version`, and `payload_checksum`, readers decode and validate all field and protocol invariants.

### A.3 Content references and checksums

A `blob_v1` reference contains all four fields:

| Field | Meaning |
| --- | --- |
| `kind` | `blob_v1`. |
| `content_id` | `con_` followed by 26 lowercase Crockford base 32 characters encoding a random UUIDv4. |
| `size_bytes` | File length in bytes, identifying the prefix of the chain. |
| `checksum` | Algorithm and digest of those bytes. |

For example, here is a reference to the 15 UTF-8 bytes `Hello, LoonFS!\n` (illustrative ID, real size and CRC-64/NVME digest):

```json
{
  "kind": "blob_v1",
  "content_id": "con_014d2pf2dbsqqg28t5cy4tqkff",
  "size_bytes": 15,
  "checksum": {
    "algorithm": "crc64nvme",
    "value": "c569275606d1d1a0"
  }
}
```

[File contents and ownership](#15-file-contents-and-ownership) defines content identity and key derivation.

A checksum object has the shape `{ "algorithm": <name>, "value": <lowercase hex> }`:

| Algorithm | Value width |
| --- | --- |
| `crc64nvme` | 16 hexadecimal characters |
| `crc32c` | 8 hexadecimal characters |

The hex value encodes the CRC integer in big-endian byte order. Provider-specific encodings are converted to this format before storage, and an algorithm without a value isn't a valid checksum.

A content reference's checksum covers the prefix it identifies, and a multipart part checksum covers one part. Upload-content claims, extent object checksums, and segment descriptor checksums each cover the complete object.

### A.4 Control and manifest payloads

| Payload | Fields |
| --- | --- |
| Writer block | `writer_id`, `acquired_at_ms` |
| Fork basis | `manifest`, `fork_point_seq`, `source_snapshot_id`, `forked_from_snapshot_id`, `fork_point_snapshot_id`, optional `rebased_from` |
| Rebased from | `source_namespace_id`, `resume_after_seq` |
| Manifest reference | `owner_namespace_id`, `manifest_no`, `head_seq`, `payload_checksum` |
| Snapshot record | `namespace_id`, `snapshot_id`, `head_seq`, `payload_checksum`, `created_at_ms`, `owner` |
| Upload session record | `namespace_id`, `upload_id`, `content_id`, `actor_id`, `mode`, `expires_at_ms` |
| Upload outcome record | `namespace_id`, `upload_id`, `outcome` |
| Member entry | `pool_id`, `namespace_id` |
| Manifest boundary | `namespace_id`, `boundary` |

Namespace `status` is required and takes one of three forms: `{"kind":"initializing"}`, `{"kind":"active"}`, or `{"kind":"deleted","deleted_at_ms":...}`. Every manifest 1 is `initializing` (see [Creating a namespace](#91-creating-a-namespace) and [Forking a namespace](#92-forking-a-namespace)). The genesis commit ID is `cmt_00000000000000000000000000`.

Snapshot `owner` variants match [Records and owners](#81-records-and-owners) (snapshots have no `status` field). Upload records use tagged variants:

- **`mode` (session record):** `service_proxied` (no extra fields), `direct_put` (`checksum_algorithm`), or `direct_multipart` (`provider_upload_id`, `part_size_bytes`, `checksum_algorithm`).
- **`outcome` (outcome record):** `completed` (`completed_at_ms`, `content_ref`) or `aborted` (`aborted_at_ms`).

Both upload records are stored under the creating namespace's pool prefix (see [Object keys](#a8-object-keys)), recording that namespace in `namespace_id`.

A manifest boundary is stored under its namespace prefix, and its `namespace_id` MUST match the key. Its `boundary` is either:

- `{"kind":"manifest","manifest_no":N}` (`N >= 1`): every manifest numbered `N` or lower is dead.
- `{"kind":"retired"}`: the namespace is [retired](#95-retirement), every manifest is dead, and the boundary never changes again.

An absent boundary object means no manifest is at or below the boundary.

A member entry is stored at `pools/{pool_id}/members/{namespace_id}.json`, and both `pool_id` and `namespace_id` MUST match the key (see [Creating a namespace](#91-creating-a-namespace) and [Content roots](#119-content-roots)).

A namespace manifest contains these fields:

| Field | Meaning |
| --- | --- |
| `namespace_id` | The namespace described by this manifest. |
| `created_at_ms` | Creation timestamp. |
| `created_by` | Application-supplied actor that created or forked the namespace. |
| `retention_window_ms?` | Optional retention window in milliseconds, at least `GC_GRACE_WINDOW_MS` (see [Advancing the retention floor](#101-advancing-the-retention-floor)). Can be changed by a successor manifest. |
| `fork_basis?` | Immutable fork provenance: source manifest reference, `fork_point_seq`, `source_snapshot_id`, `forked_from_snapshot_id`, `fork_point_snapshot_id`, and (for rebased forks) `rebased_from` (`source_namespace_id`, `resume_after_seq`, see [Merges](#merges)). |
| `pool_id` | Immutable [pool](glossary.md#pool) ID whose prefix stores the namespace's content objects and upload records (see [File contents and ownership](#15-file-contents-and-ownership)). Generated on create, copied on fork. |
| `status` | `initializing`, `active`, or `deleted`. |
| `writer?` | Diagnostic writer block. |
| `folded_wal_no` | Highest local WAL number folded into this manifest's runs. |
| `manifest_no` | Positive manifest number matching the object key. |
| `publication_id` | A random [publication ID](glossary.md#publication-id) (`pub_` prefix) that each manifest put generates, even a put of content that an earlier put already tried. LoonFS never interprets it, and the rules that a manifest keeps against its basis don't read it. Because each put has its own ID, no two puts store the same payload checksum, so a publisher can tell its own put from another publication's (see [Conflicting and unknown installations](#93-conflicting-and-unknown-installations)). |
| `compactor_epoch` | Current compaction authority. |
| `head_seq` | Folded head sequence (final sequence on a deleted manifest). |
| `activity` | Required cumulative activity counters (see [Statistics](#74-statistics)). |
| `writer_epoch` | Current writer authority. |
| `next_run_no` | Next run number to allocate. |
| `retention_floor_seq` | Earliest sequence covered by incremental replay and row retention. Folds set this to the max of the predecessor's floor and any folded floor objects (see [Publishing a manifest](#72-publishing-a-manifest)). |
| `runs` | Complete list of metadata runs. |
| `extensions?` | Map of [extension](#124-extensions) entries by name, omitted when empty. |

Each metadata run has `run_no`, `run_seq`, `tier` (`delta` or `base`), and `segments`. Each metadata segment descriptor contains:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | Namespace whose prefix stores the segment. |
| `segment_id` | Immutable generated segment ID. |
| `checksum` | Full-object checksum in the store's algorithm (see [Content references and checksums](#a3-content-references-and-checksums)). |
| `family` | Metadata row family. |
| `encoding` | Required unsigned integer segment version covering block layout and row schemas (currently `1`). |
| `row_count` | Positive number of stored rows. |
| `min_row_key`, `max_row_key` | Inclusive key range. |
| `index_block`, `filter_block` | Index and Bloom filter block handles. |
| `filter_inline?` | Exact stored filter bytes encoded as lowercase hex. |
| `extent_filter_block?` | Extent filter handle, present if and only if `family` is `content_layouts` (see [Block-segment encoding](#a7-block-segment-encoding)). |

Each [extension entry](#124-extensions) lists its `runs` in order (`run_no`, `level`, and `segments`). Each extension segment descriptor contains:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | Namespace whose prefix stores the segment. |
| `segment_id` | Immutable generated segment ID (`idx_` prefix). |
| `checksum` | Full-object checksum in the store's algorithm (see [Content references and checksums](#a3-content-references-and-checksums)). |
| `encoding` | Required unsigned integer segment version covering block layout and row schemas (currently `1`). |
| `row_count` | Positive number of stored rows. |
| `min_row_key`, `max_row_key` | Inclusive key range. |
| `index_block`, `filter_block` | Index and Bloom filter block handles. |
| `filter_inline?` | Exact stored filter bytes encoded as lowercase hex. |

`base_seq` isn't stored: it's the oldest non-empty core run's `run_seq`, or `head_seq` if none exist (extension runs don't count). Segments appear in list order within each family, and their object keys are derived directly from `owner_namespace_id` and `segment_id`.

`MAX_NAMESPACE_MANIFEST_BYTES` is 512 MiB (536,870,912 bytes) for the complete decompressed manifest document including its envelope. Readers reject larger documents, and publications fail before putting a manifest that exceeds it, keeping decompression bounded. (A namespace of 1 million files written in large commits has roughly 700 segments and a 440 KiB manifest, so the 512 MiB limit corresponds to roughly 1.2 billion files.)

### A.5 WAL records

`MAX_WAL_OBJECT_BYTES` is 512 MiB (536,870,912 bytes) for the complete decompressed WAL document including its envelope. Readers reject larger documents, and writers enforce this limit during request and batch admission so every published WAL object stays readable with bounded decompression.

A WAL object's payload contains `namespace_id`, `wal_no`, `writer_epoch`, `head_seq`, `records`, and optional `retention_floor_seq`. `wal_no` MUST match the object key.

There are three kinds of WAL object:

- **Data WAL object:** `records` is non-empty, `prior_head_seq` is derived via checked subtraction from the first record's sequence (sequence 0 is invalid), and `records` spans `(prior_head_seq, head_seq]` with no gaps. `retention_floor_seq` is omitted.
- **Fence:** `records` is empty, `retention_floor_seq` is omitted, and `prior_head_seq` equals `head_seq`. Fences participate in WAL numbering and epoch checks without adding logical state. A fence's `writer_epoch` may equal the epoch of the WAL object before it, because a writer session writes a fence with the epoch it holds after a WAL put whose outcome it doesn't know (see [Failed and unknown outcomes](#64-failed-and-unknown-outcomes)).
- **Floor object:** `records` is empty, `retention_floor_seq` is present (see [Advancing the retention floor](#101-advancing-the-retention-floor)), and `head_seq` is unchanged. Encoding and decoding reject any WAL object where `retention_floor_seq` appears with non-empty `records` or exceeds `head_seq` (`InvalidWalFloorObject`). For example, WAL 31 with `head_seq: 140`, `records: []`, and `retention_floor_seq: 20` is valid, while `retention_floor_seq: 150` is rejected.

`MAX_FENCE_OR_FLOOR_OBJECT_BYTES` is 572 bytes, which is zstd's compression bound for the longest possible empty-record WAL document (508 bytes uncompressed: envelope fields, a 128-byte `namespace_id`, four 9-byte integers, an empty record list, and `retention_floor_seq`). Every fence and floor object therefore stores in at most 572 bytes, while every data WAL object stores in strictly more than 572 bytes. If a compressed data WAL object would be 572 bytes or smaller, the writer adds a deterministic `padding` byte string to the envelope (generated from successive SHA-256 digests of `payload_checksum` plus a 4-byte big-endian counter starting at 0, truncated to the needed length) and re-compresses until the result exceeds 572 bytes. Fences and floor objects never include `padding`, `payload_checksum` doesn't cover `padding`, and readers ignore `padding`. Garbage collection uses this 572-byte threshold to distinguish data WAL objects from fences and floor objects without reading them (see [Candidate and age rules](#113-candidate-and-age-rules)).

#### Commit records and deltas

Each commit record in `records` contains `committed_seq`, `commit_id`, `committed_by`, `semantic_commit_fingerprint`, `committed_at_ms`, optional `message`, optional `merge`, `deltas`, and optional `inline_content`. Each element of `deltas` wraps `semantic_operation_index` and a kind-tagged `delta`:

| Delta kind | Fields after `kind` |
| --- | --- |
| `create_inode` | `delta_index`, `inode_id`, `inode_kind` |
| `bind_direntry` | `delta_index`, `parent_inode_id`, `name_key`, `display_name`, `child_inode_id`, `child_kind`, `child_created_by`, `child_created_at_ms`, `revision?` |
| `unbind_direntry` | `delta_index`, `parent_inode_id`, `name_key`, `display_name`, `child_inode_id`, `child_kind`, `child_created_by`, `child_created_at_ms`, `target` |
| `append_file_revision` | `delta_index`, `inode_id`, `parent_inode_id`, `name_key`, `display_name`, `bind_position`, `child_created_by`, `child_created_at_ms`, `revision_no`, `content_ref`, `layout?`, `append_offset?` |
| `tombstone_subtree` | `delta_index`, `root_inode_id`, `root_kind`, `deleted_binding` |
| `revoke_subtree_tombstone` | `delta_index`, `root_inode_id`, `target` |
| `append_attributes_revision` | `delta_index`, `inode_id`, `attributes_revision_no`, `attributes` |
| `append_access_revision` | `delta_index`, `inode_id`, `access_revision_no`, `boundary`, `grants` |

Delta field details:

- Every delta inherits `committed_seq` from its enclosing commit.
- `target` in unbind and revoke deltas is `{seq, delta_index}`, pointing to the exact event being undone.
- `bind_direntry.revision` is `{revision_no, content_ref, revision_committed_by, revision_committed_at_ms}`. It MUST be present when `child_kind` is `file` and absent when `child_kind` is `directory` (violations fail with `InvalidWalBind`).
- `append_file_revision` includes the file's current binding fields (`parent_inode_id`, `name_key`, `display_name`, `bind_position`, `child_created_by`, `child_created_at_ms`), which replay copies into the slot index's summary row (see [Directory bindings](#13-directory-bindings)).
- `tombstone_subtree` includes `root_kind` and `deleted_binding` (`{parent_inode_id, name_key, display_name}`).
- Attribute and access deltas store the complete new state, including empty maps after clears.
- `append_offset` is present only for append revisions and equals the base revision's `size_bytes`, which is strictly less than `content_ref.size_bytes` (see [Operations and WAL deltas](#66-operations-and-wal-deltas)).

#### Revision delta layouts

A revision delta's `layout` records the chain's [creator](#15-file-contents-and-ownership) (the committing namespace for uploads or new chains, or another pool member for copies and restores) and replay copies it verbatim into the `content_layouts` row (see [Metadata rows and row keys](#a6-metadata-rows-and-row-keys)). Its presence follows four rules:

- `layout` can be omitted only when the commit includes an inline piece at offset 0 for that content ID, or an inline piece appending to a chain that already has a revision in the planning view (which retention protects until the append folds).
- A commit with an inline piece at offset 0 MUST NOT include `layout`.
- When present, `layout` MUST validate against its own `size_bytes()`, and its size MUST equal the lowest inline piece offset for that content ID in the commit (or `content_ref.size_bytes` if there are no inline pieces for it). Violations fail with `InvalidWalRevisionLayout`.
- Upload evidence provides one whole extent. Copies, restores, and new chains include the required prefix from the planning view, flushing any needed pending inline pieces to content objects first so the layout survives even if compaction later drops the source chain (see [Operations and WAL deltas](#66-operations-and-wal-deltas)).

#### Merge records

A [merge](glossary.md#merge) commit includes `merge`, and non-merge commits omit it:

| Field | Meaning |
| --- | --- |
| `source_namespace_id` | Fork whose commits were applied. |
| `source_after_seq` | Exclusive lower bound of applied fork commits (the fork point on the first merge). |
| `source_through_seq` | Inclusive upper bound of applied fork commits. |
| `covered_ranges` | Ranges of other forks' commits that the merge also covers (see [Merges](#merges)), each with `source_namespace_id`, `source_after_seq`, and `source_through_seq`, in the order in which the commits in the merge's range first refer to each fork. Omitted when empty, defaults to empty when absent. |
| `conflicts` | Conflict report (`MergeConflict` shape) recording the conflicting changes a [rebase](glossary.md#rebase) replay commit skipped. Omitted when empty, defaults to empty when absent. |

A replay commit stores `conflicts` so retries with the same commit ID return the identical report (ordinary merges never commit on conflict). The conflict list counts toward `MAX_WAL_OBJECT_BYTES`.

The merge range matches change-feed cursor semantics: reading the fork's feed after `source_after_seq` through `source_through_seq` yields the exact applied commits (for example, `100` and `120` for a fork created at 100 merging commits 101 through 120). Encoding and decoding reject any merge where, for its own range or a covered range, `source_after_seq >= source_through_seq` or `source_namespace_id` equals the WAL's own `namespace_id`, or where two of its ranges are of the same fork (`InvalidWalMerge`).

LoonFS always publishes a merge alone in its own WAL object so `MAX_WAL_OBJECT_BYTES` applies to the merge alone without starving other commits. Merge `create_inode` deltas preserve the fork's inode IDs (see [Merges](#merges)), and merges include no inline content or appends because they reference bytes already written by the fork.

#### Inline content

`inline_content` is a list of `{content_id, offset, bytes}` pieces, where `bytes` is a CBOR byte string starting at `offset` in the chain. It's omitted when empty and defaults to empty when absent. A piece pairs with an ordinary `blob_v1` reference, and a whole value is a piece at offset 0. Because appending to a zero-byte file starts a new content ID with a whole value (see [Operations and WAL deltas](#66-operations-and-wal-deltas)), zero-byte chains are never appended to and downloads never sign zero-length ranges (see the [API specification][api-spec]).

Encoding and decoding enforce five local rules on `inline_content`:

1. Every piece's `content_id` is referenced by an `append_file_revision` delta in the same commit, and every such delta that includes a layout records the WAL object's `namespace_id` as the layout's creator.
2. Pieces for the same `content_id` in one commit, sorted by `offset`, are contiguous and end at the largest `size_bytes` among those references. Zero-length pieces are valid.
3. A delta with no pieces for its `content_id` requires a layout. A first piece at `offset > 0` allows omitting `layout` (or requires a present layout covering exactly `[0, offset)`). A first piece at `offset == 0` forbids `layout`. (The planner checks that brand-new chains include a layout since the decoder can't inspect prior revisions.)
4. At most one piece per `(content_id, offset)` per commit.
5. Each piece is at most `MAX_WAL_INLINE_CONTENT_BYTES` (256 KiB, 262,144 bytes), and all pieces in one WAL object total at most `MAX_WAL_OBJECT_INLINE_CONTENT_BYTES` (4 MiB, 4,194,304 bytes).

Writer thresholds may sit below these hard reader limits, and `MAX_WAL_OBJECT_BYTES` still caps the full decompressed WAL document. Replay trusts the envelope's `payload_checksum` rather than re-hashing inline pieces against `content_ref.checksum`, and any read or fold that finds a referenced layout object missing reports corruption. Replay applies normalized WAL records strictly in sequence and delta order without re-running request preconditions or re-planning under newer rules.

### A.6 Metadata rows and row keys

Metadata rows are kind-tagged CBOR objects stored in data blocks. Row schemas are independent of row-family sort order, so the same `direntry_binding` row appears in both binding families.

| Row kind | Fields after `kind` |
| --- | --- |
| `inode` | `inode_id`, `inode_kind`, `committed_seq`, `commit_id`, `committed_by`, `committed_at_ms` |
| `direntry_binding` | `parent_inode_id`, `name_key`, `child_inode_id`, `child_kind`, `child_created_by`, `child_created_at_ms`, `committed_seq`, `delta_index`, `commit_id`, `committed_by`, `committed_at_ms`, `bind_position`, `state` |
| `file_revision` | `inode_id`, `revision_no`, `committed_seq`, `commit_id`, `committed_by`, `committed_at_ms`, `delta_index`, `content_ref` |
| `tombstone` | `root_inode_id`, `committed_seq`, `delta_index`, `commit_id`, `action`, `committed_by`, `committed_at_ms` |
| `trash` | `root_inode_id`, `deletion_seq`, `action` |
| `commit_receipt` | `commit_id`, `committed_seq` |
| `commit` | `committed_seq`, `commit_id`, `committed_by`, `semantic_commit_fingerprint`, `committed_at_ms`, `message?`, `merge?`, `deltas` |
| `merge` | `source_namespace_id`, `committed_seq`, `source_after_seq`, `source_through_seq` |
| `content_layout` | `content_id`, `committed_seq`, `size_bytes`, `layout` |
| `attributes_revision` | `inode_id`, `attributes_revision_no`, `committed_seq`, `commit_id`, `delta_index`, `committed_by`, `committed_at_ms`, `attributes` |
| `access_revision` | `inode_id`, `access_revision_no`, `committed_seq`, `commit_id`, `delta_index`, `committed_by`, `committed_at_ms`, `boundary`, `grants` |

Row field notes:

- A `commit` row matches the WAL commit record in [WAL records](#a5-wal-records) minus `inline_content` (including `inline_content` is invalid).
- A `merge` row indexes one range of a fork's commits that a merge commit covers, by the fork's `source_namespace_id`. A merge commit has one row for its own range and one for each of its `covered_ranges`, so scanning a fork's prefix returns the ranges of that fork that merges covered, newest first.
- In `direntry_binding`, `state` is `{"kind":"bound","display_name":...,"revision"?:...}` (with `revision` present for files and absent for directories) or `{"kind":"unbound"}`, and `bind_position` is `{seq, delta_index}`. Both binding families store full rows. Parity checks compare binding events only, skipping slot-index summary rows (see [Directory bindings](#13-directory-bindings)). Unbound rows are always binding events.
- In `tombstone`, `committed_seq` and `delta_index` are the row's own position, `action` is `{"kind":"set","root_kind":...,"deleted_binding":...}` or `{"kind":"revoke","target":{seq, delta_index}}`.
- In `trash`, `action` is nested (not top-level): `listed` contains required `inode_kind` (the `root_kind` of the `set` event), `deleted_at_ms`, `deleted_by`, and `deleted_binding`, while `removed` contains `removed_seq`.

#### Row key grammar

Every row key starts with its family prefix followed by hyphen-separated components (see [Row families](#19-row-families)):

- `*_body`: a generated ID's 26-character Crockford base 32 body without its type prefix (no `ino_` or `con_`, see [Numbered objects and generated IDs](#23-numbered-objects-and-generated-ids)). Constant width and a byte-sorted alphabet keep keys in numeric ID order.
- `*_hex`: lowercase hex encoding of a client-chosen UTF-8 string (name keys, commit IDs, namespace IDs). Because hex contains no hyphens and `-` sorts before `0`-`9` and `a`-`f`, variable-width strings never collide with separators and shorter prefixes sort first.
- `:020` / `:010`: zero-padded decimal widths for `u64` (20 digits) and `u32` (10 digits).
- `u64::MAX - x` / `u32::MAX - x`: inverted before zero-padding so larger values sort first.

| Family | Row key |
| --- | --- |
| `inodes` | `inode-{inode_id_body}` |
| `direntry_binds` | `direntry-bind-{parent_inode_id_body}-{name_key_hex}-{child_inode_id_body}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |
| `direntry_child_binds` | `direntry-child-bind-{child_inode_id_body}-{parent_inode_id_body}-{name_key_hex}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |
| `revisions` | `revision-{inode_id_body}-{u64::MAX - revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |
| `tombstones` | `tombstone-{root_inode_id_body}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |
| `trash` | `trash-{deletion_seq:020}-{root_inode_id_body}-{sort_rank:010}` |
| `commit_receipts` | `commit-receipt-{commit_id_hex}-{committed_seq:020}` |
| `commits` | `commit-{committed_seq:020}` |
| `merges` | `merge-{source_namespace_id_hex}-{u64::MAX - committed_seq:020}` |
| `content_layouts` | `content-layout-{content_id_body}-{u64::MAX - size_bytes:020}-{u64::MAX - committed_seq:020}` |
| `attributes` | `attribute-{inode_id_body}-{u64::MAX - attributes_revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |
| `access` | `access-{inode_id_body}-{u64::MAX - access_revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |

In `trash`, `sort_rank` is `0` for `removed` and `1` for `listed` (still padded to 10 digits).

Because binding keys group a slot's edges by child (and a child's edges by parent and name key) rather than by global position, a slot's or child's value is the newest value across its edges (see [Directory bindings](#13-directory-bindings)), and each edge's value is its first row in key order at or below the read sequence. During compaction, streaming both binding families by edge lets the compactor decide each row on the fly while checking that no slot binds two children and no child binds two parents at the retention floor (any violation is corruption).

#### Filter keys

Segment Bloom filters hash these keys (often prefixes rather than full row keys):

| Family | Filter key |
| --- | --- |
| `inodes` | The complete row key |
| `direntry_binds` | `direntry-bind-{parent_inode_id_body}-{name_key_hex}` for each row (for slot lookups), plus `direntry-bind-{parent_inode_id_body}` once per consecutive run of rows sharing a parent (for directory listings) |
| `direntry_child_binds` | `direntry-child-bind-{child_inode_id_body}` |
| `revisions` | `revision-{inode_id_body}` |
| `tombstones` | `tombstone-{root_inode_id_body}` |
| `trash` | `trash-{deletion_seq:020}-{root_inode_id_body}` |
| `commit_receipts` | `commit-receipt-{commit_id_hex}` |
| `commits` | The complete row key |
| `merges` | `merge-{source_namespace_id_hex}` |
| `content_layouts` | `content-layout-{content_id_body}` |
| `attributes` | `attribute-{inode_id_body}` |
| `access` | `access-{inode_id_body}` |

#### Content layouts

A `content_layout` row records a chain's physical objects as of the commit or fold that wrote them. Sorting by inverted `size_bytes` then inverted `committed_seq` puts the longest (and newest, on ties) layout first under a content ID's prefix, superseding shorter layouts. Compaction keeps only the first row per content ID, subject to base-compaction pruning (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)). A layout row's `size_bytes` can differ from an individual revision reference's `size_bytes`. Rows written from a revision delta copy the delta's layout, while rows written by a fold record the folding namespace as creator (see [Publishing a manifest](#72-publishing-a-manifest)).

A `layout` object bundles `creator_namespace_id` (a property of the chain as a whole, just like its extents) and an ordered list of `extents`. Each extent contains `content_id`, `object` (`{"kind":"whole"}` or `{"kind":"span","object_id":...,"start":...,"end":...}`), `object_checksum` (the store's full-object checksum regardless of slice range, see [Content references and checksums](#a3-content-references-and-checksums)), `offset`, and `length`. A span's `object_id` (`span_` plus 26 characters) identifies its span object (see [Object keys](#a8-object-keys)), and `start`/`end` are chain byte offsets covered by that span object.

Every layout MUST satisfy five rules:

- Extent lengths are positive and sum to `size_bytes`.
- Zero bytes require exactly one `whole` extent of length 0.
- `creator_namespace_id`, every `content_id`, and every span `object_id` are valid IDs.
- Every extent has a valid `object_checksum` whose width matches its algorithm.
- Every span satisfies `start < end`, and the extent's `[offset, offset + length)` fits inside `end - start`.

Shared prefixes can slice a span extent without rewriting the span object, and readers honor nonzero object offsets.

#### Compaction family groups

| Group | Members |
| --- | --- |
| `bindings` | `direntry_binds`, `direntry_child_binds` |
| `revisions` | `revisions` |
| `inodes` | `inodes` |
| `tombstones` | `tombstones` |
| `trash` | `trash` |
| `commits` | `commits`, `commit_receipts`, `merges` |
| `content_layouts` | `content_layouts` |
| `attributes` | `attributes` |
| `access` | `access` |

### A.7 Block-segment encoding

A segment is a sequence of independently readable sections with no outer header or footer:

```text
[zstd data block 0]
[zstd data block 1]
...
[uncompressed extent filter]  (content_layouts segments only)
[uncompressed bloom filter]
[zstd index block]
```

Because there's no header or footer, a segment can only be opened through its manifest descriptor, which supplies the filter, extent-filter, and index block handles. The index block in turn supplies the data-block handles.

Each handle has four fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `offset` | `u64` | Zero-based byte offset in the segment object. |
| `stored_bytes` | `u32` | Stored length of the section in bytes. |
| `decoded_bytes` | `u32` | Expected length after decompression (for uncompressed filters, equal to `stored_bytes`). |
| `crc32c` | `u32` | CRC32C of the exact stored section bytes. |

Verified puts check the whole object checksum on write, and any reader fetching an entire segment checks it against the descriptor's `checksum`. Any mismatch is corruption.

Every section MUST pass three checks: the returned byte count matches `stored_bytes`, the CRC32C of the stored bytes matches `crc32c`, and the decoded length matches `decoded_bytes`. Readers cap decompression at `decoded_bytes + 1` bytes so an oversized section fails immediately without expanding arbitrarily. (This bounds decompressed output, not vector capacity or decoded row allocations. See [Reference-implementation sizing and admission defaults](#c4-reference-implementation-sizing-and-admission-defaults) for initial allocation sizing.)

#### Data blocks

Before zstd compression, a data block consists of prefix-compressed entries, a restart-offset array, and a 4-byte restart count. Each entry encodes:

```text
shared_prefix_len     unsigned LEB128
key_suffix_len        unsigned LEB128
key_suffix            key_suffix_len UTF-8 bytes
row_len               unsigned LEB128
row                    row_len CBOR bytes
```

A row's key is reconstructed from the first `shared_prefix_len` bytes of the previous key plus `key_suffix`. The shared prefix MUST end on a valid UTF-8 boundary, and keys are strictly non-decreasing. (The block format allows duplicate adjacent keys, though metadata producers enforce uniqueness per run.)

The first entry and every 16th entry afterward starts a restart point with `shared_prefix_len == 0` (encoding the full key). The restart array lists each restart point's byte offset from the start of the entry region as a little-endian `u32`, followed by the restart count as a little-endian `u32`. Readers verify every restart offset and reject missing or unused entries. The entire block (entries, restart offsets, and count) is compressed as one zstd section. Metadata and grep segments share this exact block layout.

Unsigned LEB128 integers MUST use their shortest form (7 payload bits per byte, high bit set if more bytes follow). All values fit in `u64` (at most 10 bytes, with 1 payload bit in the final byte).

#### Filter block

The Bloom filter is uncompressed:

```text
n_hashes              u32, little-endian
bit_len               u64, little-endian
bits                  ceil(bit_len / 8) bytes
```

Version 1 requires `n_hashes == 7` and `bit_len >= 64`. Readers reject other header values before probing, and `bits` MUST be exactly `ceil(bit_len / 8)` bytes. The reference producer sizes filters per [Filter sizes](#filter-sizes), counting repeated filter keys as separate insertions.

Filter keys (UTF-8 bytes) are hashed with XXH64 using two constant seeds:

```text
h1 = XXH64(key, seed = 0)
h2 = XXH64(key, seed = 0x9e3779b97f4a7c15)
```

For each probe `i` in `0..n_hashes`, compute `x = h1 + i * h2`, mix `x` with the splitmix64 finalizer (using wrapping `u64` arithmetic), and reduce modulo `bit_len`:

```text
x = h1 + i * h2
x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
x = (x ^ (x >> 27)) * 0x94d049bb133111eb
b = (x ^ (x >> 31)) % bit_len
```

Bit `b` sits in byte `b / 8` at mask `1 << (b % 8)`. Mixing ensures every probe depends on all 64 bits of both hashes, preventing short filters or unlucky `h2` multiples of `bit_len` from repeatedly testing the same bit.

A negative probe safely skips the segment. A positive probe doesn't bypass row-level visibility rules.

If `filter_inline` is present in the manifest descriptor, it encodes the exact stored filter bytes in lowercase hex and MUST match the filter handle's `stored_bytes` and `crc32c`. The segment still stores the full filter block for readers without an inline copy.

#### Filter sizes

The reference producer sizes each Bloom filter based on what a false positive costs (these are producer defaults, not format requirements, and readers accept any `bit_len >= 64`):

| Filter | A false match | Bits per key | Fewest bits |
| --- | --- | --- | --- |
| A segment's filter ([Filter block](#filter-block)) | Costs a lookup one more read | 10 | 64 |
| A segment's extent filter ([Extent filter](#extent-filter)) | Keeps a content object until no root lists the segment | 20 | 4,096 |
| A compaction's layout filter and kept inode filter ([Row retention during a base compaction](#103-row-retention-during-a-base-compaction)) | Keeps a row until a compaction with another seed | 10 | 4,096 |
| A reorganization's value and revision filters ([Pruning the index](#d5-pruning-the-index)) | Keeps an index row until a later reorganization | 10 | 4,096 |

Filters whose false matches retain data use a 4,096-bit floor because 10 bits per key has roughly a 1-in-110 false-positive rate at small sizes. At 4,096 bits, 30 keys match roughly once in 1.2 billion, 100 keys once in 426,000, and 200 keys once in 5,800. Segment lookup filters use a 64-bit floor since a false match only costs one extra read (and 3 keys in 64 bits match roughly once in 6,300).

#### Extent filter

Every `content_layouts` segment includes an extent filter immediately before its Bloom filter, and no other family may have one. Missing `extent_filter_block` on a `content_layouts` descriptor, or including one on another family, is corruption. Extent filters use the exact same binary format, header rules, and hashing as the filter block, with `decoded_bytes == stored_bytes` and no inline copy in the manifest.

Extent filters insert the pool-relative names of every content object referenced by the segment's layout rows (including shared base extents): `{content_id}` for whole objects and `{content_id}-{span_object_id}` for span objects (see [Object keys](#a8-object-keys)). Omitting `pools/{pool_id}/content/` makes names pool-independent so segments written by any pool member match the pool's content listing directly. The reference producer sizes extent filters per [Filter sizes](#filter-sizes), counting repeated names as separate insertions.

Only garbage collection reads extent filters (see [Content roots](#119-content-roots)). Normal metadata lookups fetch only the Bloom filter and index block.

#### Index block

The index block is a zstd-compressed CBOR list of `{last_row_key, block}` entries ordered by `last_row_key`, where `block` is the data-block handle.

Data blocks MUST occupy contiguous, non-overflowing byte ranges starting at offset 0. Immediately after the data region come the optional extent filter (for `content_layouts`), the Bloom filter, and finally the index block at the end of the segment object. The manifest descriptor's handles MUST match this exact layout.

Range lookups use `last_row_key` entries to locate candidate data blocks, verify each section before decoding rows, and enforce the descriptor's family and `[min_row_key, max_row_key]` bounds.

### A.8 Object keys

These patterns define all core object families. [Storage layout](#21-storage-layout) describes prefix ownership and where inherited segments are stored.

| Family | Object key pattern |
| --- | --- |
| WAL objects | `namespaces/{namespace_id}/wal/{wal_no:020}.wal.zst` |
| Namespace manifests | `namespaces/{namespace_id}/manifests/{manifest_no:020}.json.zst` |
| Snapshot records | `namespaces/{namespace_id}/snapshots/{snapshot_id}.json` |
| Metadata segments | `namespaces/{owner_namespace_id}/segments/{segment_id}.sst` |
| Extension segments | `namespaces/{owner_namespace_id}/extensions/{extension_name}/segments/{segment_id}.sst` |
| Upload sessions | `pools/{pool_id}/uploads/{upload_id}.json` |
| Upload outcomes | `pools/{pool_id}/uploads/{upload_id}.outcome.json` |
| Manifest boundary | `namespaces/{namespace_id}/boundary.json` |
| Member entries | `pools/{pool_id}/members/{namespace_id}.json` |
| Content objects | `pools/{pool_id}/content/{content_id}` |
| Content span objects | `pools/{pool_id}/content/{content_id}-{span_object_id}` |
| Temporary objects | `pools/{pool_id}/temporary/{temporary_id}` |

## Appendix B. Semantic commit fingerprints

A commit fingerprint has the format `v1:sha256:<64 lowercase hex>`, where `v1` identifies the canonicalization rules below (not the general API JSON format). It hashes compact UTF-8 JSON (no formatting whitespace) with these top-level fields in exact order:

```text
{
  "domain": "loonfs.commit.semantic.v1",
  "namespace_id": <namespace string>,
  "actor_id": <actor ID string>,
  "operations": <ordered canonical operations>,
  "message": <string or null>,
  "preconditions": <ordered canonical preconditions>
}
```

Every field is always present (`preconditions` is `[]` when empty, `message` is `null` when absent). Changing `actor_id` produces a different fingerprint even if the same application is retrying. Commit ID, writer epoch, and commit timestamp are excluded. Merge commits use a separate preimage (see [Merge commits](#b5-merge-commits)).

### B.1 Operation fields

Each canonical operation starts with `kind` followed by every field below in exact order, always writing optional fields as `null` and explicit default booleans/behaviors:

| Kind | Fields after `kind`, in order |
| --- | --- |
| `create_directory` | `path`, `parents` |
| `create_directory_by_inode` | `parent_inode_id`, `display_name` |
| `put_file` | `path`, `behavior`, `content_ref`, `expected_inode_id`, `expected_revision_no` |
| `create_file_by_inode` | `parent_inode_id`, `display_name`, `content_ref` |
| `put_file_revision_by_inode` | `inode_id`, `content_ref`, `expected_revision_no` |
| `append_file` | `path`, `inline_content`, `expected_inode_id`, `expected_revision_no` |
| `append_file_by_inode` | `inode_id`, `inline_content`, `expected_revision_no` |
| `move_by_inode` | `inode_id`, `expected_binding_version`, `destination_parent_inode_id`, `destination_display_name`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `delete_by_inode` | `inode_id`, `expected_binding_version`, `behavior` |
| `delete_path` | `path`, `behavior`, `expected_inode_id` |
| `move_path` | `source_path`, `destination_path`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `copy_path` | `source_path`, `destination_path`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `copy_by_inode` | `inode_id`, `destination_parent_inode_id`, `destination_display_name`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `restore_revision` | `path`, `source_revision_no` |
| `restore_revision_by_inode` | `inode_id`, `source_revision_no` |
| `restore_deleted` | `inode_id`, `deletion_seq`, `destination_path`, `destination_parent_inode_id`, `destination_display_name` |
| `update_attributes` | `path`, `set`, `remove`, `expected_inode_id`, `expected_attributes_revision_no` |
| `update_attributes_by_inode` | `inode_id`, `set`, `remove`, `expected_attributes_revision_no` |
| `update_access` | `path`, `boundary`, `grants`, `expected_inode_id`, `expected_access_revision_no` |
| `update_access_by_inode` | `inode_id`, `boundary`, `grants`, `expected_access_revision_no` |

Paths use canonical absolute form, and display names are single validated components. Inode IDs use their 26-character body without `ino_`. Sequences and revisions are JSON integers, and binding versions remain opaque strings. Attribute `set` keys are sorted in UTF-8 byte order, `remove` is sorted and deduplicated, and operations keep request order.

### B.2 Content in a fingerprint

A blob content reference canonicalizes to three fields in order (omitting `checksum` and `layout` since they describe verification and physical placement):

```json
{"kind":"blob_v1","content_id":"con_014d2pf2dbsqqg28t5cy4tqkff","size_bytes":15}
```

Uploading identical bytes twice produces different content IDs and fingerprints, so retries MUST reuse the original reference.

Inline content has no pre-assigned object, so `content_ref` hashes the complete bytes (lowercase SHA-256) instead of a content ID:

```json
{"kind":"inline_v1","sha256":"<64 lowercase hex>","size_bytes":15}
```

The form reflects how the request sent the content, not where the writer stored it: inline content uses `inline_v1` even if staged to an object, and uploaded content uses `blob_v1`.

Appended bytes in `append_file` and `append_file_by_inode` use a third form hashing only the appended bytes and their length:

```json
{"kind":"append_v1","sha256":"<64 lowercase hex>","size_bytes":6}
```

### B.3 Preconditions

Preconditions follow `message` in exact caller order (neither sorted nor deduplicated). Each starts with `kind`, followed by every field in order (with `null` for absent optional fields and values formatted per [Operation fields](#b1-operation-fields)):

| Kind | Fields after `kind`, in order |
| --- | --- |
| `namespace_head` | `expected_head_seq` |
| `file_revision` | `inode_id`, `expected_revision_no` |
| `attributes_revision` | `inode_id`, `expected_attributes_revision_no` |
| `access_revision` | `inode_id`, `expected_access_revision_no` |
| `path_binding` | `path`, `expected_inode_id`, `expected_binding_version` |
| `inode_binding` | `inode_id`, `expected_binding_version` |
| `path_absence` | `path` |
| `name_absence` | `parent_inode_id`, `display_name` |

Preconditions affect request identity and validation but produce no WAL fields or replay deltas.

### B.4 Strings, integers, and example bytes

Non-ASCII characters are emitted as raw UTF-8. Quotes, backslashes, and control characters are JSON-escaped, while `/` is unescaped. Integers use decimal digits with no leading zeros. Field ordering follows the tables above rather than alphabetical sorting.

Example preimage for creating `/reports` (no trailing newline):

```json
{"domain":"loonfs.commit.semantic.v1","namespace_id":"demo","actor_id":"usr_8f3c","operations":[{"kind":"create_directory","path":"/reports","parents":false}],"message":null,"preconditions":[]}
```

Fingerprint:

```text
v1:sha256:a64ec097a98f4c881c3770868ad31e77efd8ec9fedb6101672de2091311666ba
```

Single-operation convenience endpoints and single-element commits produce identical preimages (raw request JSON isn't hashed because omitted defaults must be materialized). Shared test vectors in [commit_fingerprints_v1.json][fingerprint-vectors] cover every operation and precondition variant, and encoders MUST match those exact bytes and digests.

### B.5 Merge commits

A [merge](glossary.md#merge) fingerprint hashes its merge metadata instead of operations and preconditions (see [Commit identity and retries](#65-commit-identity-and-retries)), using compact UTF-8 JSON with these fields in exact order:

```text
{
  "domain": "loonfs.commit.merge.v1",
  "namespace_id": <namespace string>,
  "actor_id": <actor ID string>,
  "merge": {
    "source_namespace_id": <namespace string>,
    "source_after_seq": <integer>,
    "source_through_seq": <integer>
  },
  "message": <string or null>
}
```

Every field is always present (`null` for absent `message`), formatted per [Strings, integers, and example bytes](#b4-strings-integers-and-example-bytes).

The preimage includes the merge's own range only. A merge's `covered_ranges` and a replay commit's `conflicts` are left out, because both follow from the fork's commits in that range, which never change, so a retry with the same range has the same ones.

Example preimage for merging `demo-fork` commits `(418, 425]` into `demo` (no trailing newline):

```json
{"domain":"loonfs.commit.merge.v1","namespace_id":"demo","actor_id":"usr_8f3c","merge":{"source_namespace_id":"demo-fork","source_after_seq":418,"source_through_seq":425},"message":null}
```

Fingerprint:

```text
v1:sha256:004b16fd5b647a8003f24e3b558b5f93ff5e6bc836ca42322f93ddff1ffaa365
```

## Appendix C. Timing and size reference

Publication and garbage collection rely on the timing relationships below. [C.4](#c4-reference-implementation-sizing-and-admission-defaults) lists configurable defaults, and the [limits module][limits-source] defines the reference constants.

### C.1 Publication and collection timing

| Constant | Milliseconds | Meaning |
| --- | ---: | --- |
| `METADATA_PUBLICATION_BUDGET_MS` | 900,000 | Max time from the start of a bounded manifest publication (including an epoch claim) to starting its put. |
| `PROVIDER_OPERATION_DEADLINE_MS` | 120,000 | Shared retry budget for a client operation. |
| `PROVIDER_ATTEMPT_TIMEOUT_MS` | 30,000 | Timeout for one control-operation attempt. |
| `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` | 300,000 | Retry budget (120 s) plus max final backoff (15 s) plus payload-sized final request (120 s), rounded up to 5 minutes. |
| `GC_SAFETY_MARGIN_MS` | 300,000 | Combined allowance for relative clock error, timestamp precision, and scheduling delays. |
| `PUBLICATION_COMPLETION_BOUND_MS` | 1,500,000 | Publication completion bound: `METADATA_PUBLICATION_BUDGET_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS + GC_SAFETY_MARGIN_MS`. |
| `READ_REVALIDATION_BOUND_MS` | 1,200,000 | Max staleness a read can request, max manifest check interval an operator can configure, and max cached-view age before an `eventual` read rechecks. Doesn't bound publication bases or writer positions. |
| `GC_GRACE_WINDOW_MS` | 86,400,000 | Shared grace window (`T`, 24 hours). Unreferenced segments MUST exceed `T` before deletion, tombstones retire after `T`, and retention advances never pass commits younger than `T`. |
| `METADATA_COMPACTION_BUDGET_MS` | 43,200,000 | Max time before a compaction starts its final publication attempt. |
| `DIRECT_TRANSFER_URL_TTL_MS` | 900,000 | Lifetime of presigned download URLs and whole-object upload URLs (multipart part URLs last 1 hour). |
| `OPERATION_SNAPSHOT_TTL_MS` | 300,000 | Lifetime of internal operation snapshots (merge/rebase fork snapshots and temporary fork source snapshots) and their plan content proofs. |
| `POOL_ROOTS_READ_BUDGET_MS` | 43,200,000 | Monotonic deadline for a pool collection to finish reading all members' roots before skipping the content sweep. |
| `MAX_TAIL_COMMIT_AGE_MS` | 42,300,000 | Max age of a WAL tail commit for merges, rebases, or head forks to plan or fold directly from the tail instead of folding first. Equals `T` minus the other terms of the pool-collection inequality below (see [Cross-namespace copies and moves](#97-cross-namespace-copies-and-moves) and [Forking a namespace](#92-forking-a-namespace)). |

`PROVIDER_PUBLICATION_REQUEST_BOUND_MS` budgets 120 seconds for a payload-sized final request (rather than the 30-second control timeout) because conditional WAL and manifest writes are single requests (see [Required operations](#31-required-operations)) that can exceed the payload threshold. This bound covers the request phase (not response streaming) and doesn't prove a timed-out write had no effect (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).

Protocol safety depends on three relationships:

```text
PUBLICATION_COMPLETION_BOUND_MS
    = METADATA_PUBLICATION_BUDGET_MS
      + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
      + GC_SAFETY_MARGIN_MS
```

Compaction safety (12 hours 25 minutes < 24 hours, see [Streaming compaction](#104-streaming-compaction)):

```text
METADATA_COMPACTION_BUDGET_MS + PUBLICATION_COMPLETION_BOUND_MS
    < GC_GRACE_WINDOW_MS
```

Pool collection safety (12 hours 15 minutes < 24 hours, leaving 11 hours 45 minutes for `MAX_TAIL_COMMIT_AGE_MS`, see [Content roots](#119-content-roots)):

```text
POOL_ROOTS_READ_BUDGET_MS + GC_SAFETY_MARGIN_MS
    + OPERATION_SNAPSHOT_TTL_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
    < GC_GRACE_WINDOW_MS
```

In addition, `GC_GRACE_WINDOW_MS` MUST be at least `PUBLICATION_COMPLETION_BOUND_MS`. A fork folding its source's WAL tail must start the put of its active manifest within `OPERATION_SNAPSHOT_TTL_MS` (5 minutes) of taking its temporary snapshot, or it writes nothing more and returns `snapshot_not_found` (see [Forking a namespace](#92-forking-a-namespace)). The fork checks this deadline before each put, so the provider can apply the active manifest's put up to `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` later, and the pool collection inequality above includes that term. A fork that misses the deadline stays initializing, and namespace collection tombstones it once manifest 1 is older than `T` (see [Deleting a namespace](#94-deleting-a-namespace)).

Completed-content grace uses its own constant below, not `T`. Changing any publication bound or inequality alters the protocol, not just tuning.

### C.2 Upload admission

| Constant | Milliseconds | Meaning |
| --- | ---: | --- |
| `UPLOAD_SESSION_LEASE_MS` | 86,400,000 | Open session lifetime on the creating host. |
| `COMPLETED_UPLOAD_RECEIPT_WINDOW_MS` | 604,800,000 | 7-day window during which completed uploads can issue new receipts. |
| `CONTENT_RECEIPT_TTL_MS` | 3,600,000 | 1-hour receipt token lifetime. |
| `COMPLETED_UPLOAD_ADMISSION_WINDOW_MS` | 608,400,000 | Receipt issuance window plus final token lifetime. |
| `CONTENT_RECLAMATION_GRACE_WINDOW_MS` | 691,200,000 | 8-day completed-content grace, at least the admission window plus `PUBLICATION_COMPLETION_BOUND_MS`. |

[Upload-session cleanup](#116-upload-session-cleanup) details how the 8-day completed-content grace covers every receipt and in-flight publication. Reaching that age never deletes referenced content.

Direct multipart uploads also follow provider limits (in the reference implementation: 10,000 parts max, configured part size between 5 MiB and 5 GiB, and up to 1,000 presigned part URLs per signing request), plus any stricter per-provider limits.

### C.3 Fixed logical bounds

| Value | Bound |
| --- | ---: |
| Display-name length | 255 UTF-8 bytes |
| Name-key length | 768 UTF-8 bytes |
| Canonical path length | 4,096 UTF-8 bytes |
| Canonical path components | 128 |
| `MAX_ATTRIBUTE_KEY_BYTES` | 128 UTF-8 bytes |
| `MAX_ATTRIBUTE_VALUE_BYTES` | 4,096 UTF-8 bytes |
| `MAX_ATTRIBUTE_ENTRIES` | 100 |
| `MAX_ATTRIBUTES_TOTAL_BYTES` | 65,536 logical UTF-8 bytes |
| A merge's planned WAL record | 536,870,912 bytes (`MAX_WAL_OBJECT_BYTES`) |

Attribute limits count raw UTF-8 bytes before JSON escaping or CBOR encoding. Path limits apply to canonical request paths.

Because LoonFS plans [merges](glossary.md#merge) internally rather than parsing them from a request body, merges aren't subject to API request limits on operations, preconditions, or content references. Instead, merges are bounded by `MAX_WAL_OBJECT_BYTES` (failing with `merge_too_large`) and by the reference server's `max_merge_bytes` memory allowance (see `merge_too_large` in the API's [Standard error contract](api.md#3-standard-error-contract)).

### C.4 Reference-implementation sizing and admission defaults

These defaults configure the reference producer and runtime without changing the wire format. Rows are never split to hit a target block or segment size, so a single large row can exceed a target.

| Setting | Value |
| --- | ---: |
| Target decoded data-block size | 64 KiB |
| Inline filter threshold | 1,024 stored bytes |
| Target segment rows | 65,536 |
| Target decoded segment size | 8 MiB |
| Maximum decoded input one compaction keeps in memory | 64 MiB |
| Automatic WAL-fold threshold | 8 MiB of WAL bytes, or 256 WAL objects |
| Idle WAL-fold period | 15 minutes |
| Unfolded-tail write stop (new commits wait for a fold) | 64 MiB of WAL bytes, or 1,024 WAL objects |
| `PublicationPolicy::write_stop_wait_budget_ms` | 30,000 ms |
| `RuntimeConfig::manifest_revalidation_interval_ms` | 1,000 ms |
| `RuntimeConfig::max_staleness_ms` | 1,000 ms |
| Maximum commit-message size | 4,096 bytes |
| `MANIFEST_PROBE_WINDOW` (manifest numbers probed at once above a manifest whose successor exists) | 8 |

`max_staleness_ms` sets the default staleness bound for `eventual` reads (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). Neither `max_staleness_ms` nor `manifest_revalidation_interval_ms` may exceed `READ_REVALIDATION_BOUND_MS`.

Decoders MUST NOT treat target block or segment sizes as hard limits. The reference block reader initially reserves `min(decoded_bytes, 64 KiB)` and grows on demand while capping output at `decoded_bytes + 1` bytes (see [Block-segment encoding](#a7-block-segment-encoding)). Request admission limits are defined in the [API specification][api-spec], and hard manifest/WAL document caps are defined in [Control and manifest payloads](#a4-control-and-manifest-payloads) and [WAL records](#a5-wal-records).

## Appendix D. Grep extension format

Grep is an [extension](#124-extensions) that indexes a namespace's file content. Its metadata sits in the `grep` entry of the namespace manifest, and its segments are stored under `namespaces/{owner_namespace_id}/extensions/grep/segments/`. Because the index is derived from authoritative file content and history, it can always be rebuilt from a fresh backfill.

### D.1 The entry and its publication

The manifest's `grep` entry has four members:

| Member | Meaning |
| --- | --- |
| `version` | Grep format version (`1`). |
| `status` | Lifecycle state and position fields (see [Index lifecycle](#d2-index-lifecycle)). |
| `reorganize?` | In-progress reorganization state, if any. |
| `runs` | Index runs in list order, each with `run_no`, `level`, and `segments` (see [Control and manifest payloads](#a4-control-and-manifest-payloads)). |

Grep validates `status` and `reorganize` strictly and rejects unknown versions, unknown fields, or invalid nested state. Disabling grep overwrites even a corrupt version 1 entry with a disabled entry so the index can be re-enabled, but refuses to overwrite an unknown version so an older binary never discards a newer index format. A namespace without a `grep` entry has no index until grep is enabled.

Indexing progresses in bounded units of work called **steps** (backfill, build, or reorganization steps), executed sequentially in a run of steps for one namespace at a time. Each step starts from the pending entry left by earlier steps in the same run (not the last published manifest, so steps don't re-index unpublished `texts` rows), writes new segments, and updates the pending entry. New runs are numbered one above the highest `run_no` in the pending entry (including any in-progress reorganization), starting at 0 when empty. Run numbers only need to be unique within the current entry and can be reused once old runs disappear.

Grep batches publications instead of writing a manifest per step: it publishes once the pending changes reach one maintenance interval in age (5 seconds by default in the reference server and CLI) and when the run of steps finishes. Batching keeps grep to roughly one manifest every 5 seconds so writers and cached readers don't churn through more than 8 manifests and trigger full tail rediscoveries (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). A crash loses at most one interval's work, whose orphaned segments garbage collection cleans up.

To publish pending changes, grep:

1. Loads the current manifest before the first step writes a segment. `METADATA_PUBLICATION_BUDGET_MS` runs from this load (see [Publishing a manifest](#72-publishing-a-manifest)).
2. Executes steps against that manifest's entry.
3. Puts the next manifest with the pending entry at `loaded_manifest_no + 1` via put-if-absent.
4. On conflict, if the winning manifest kept the same grep entry (or neither had one), rebases onto the winner and retries.
5. If the grep entry changed, abandons the publication, leaves the new segments for garbage collection, and replans from the current manifest.

Unknown put outcomes resolve per [Conflicting and unknown installations](#93-conflicting-and-unknown-installations).

Every step MUST either preserve `captured_seq` without moving the indexing position backward, or start a newer build at a strictly higher `captured_seq` (the only time a backfill may reset its inode cursor).

Queries read the `grep` entry from their read view's manifest, so the view's head is always at or above every sequence in the entry (`captured_seq`, `pruning`, etc.). A paginated search can still read below the index watermark on later pages (see [Querying the index](#d6-querying-the-index)).

### D.2 Index lifecycle

The entry's `status` tracks lifecycle state and progress:

| Kind | Status fields |
| --- | --- |
| `backfilling` | `captured_seq`, optional `cursor_inode_id` |
| `active` | `captured_seq`, `built_through_seq`, `next_event_index` |
| `disabled` | No other status fields |

A backfill scans files in inode order as of `captured_seq` (the head when backfill began), resuming strictly after `cursor_inode_id` until the retention floor passes `captured_seq`. Once complete, the index becomes `active`, keeping `captured_seq` as its build identifier (`next_event_index` is 0 at commit boundaries). A `disabled` index lists no segments and no reorganization.

If the retention floor passes an index's position (`built_through_seq` when active, or `captured_seq` when backfilling), grep **rebootstraps**: it starts a fresh backfill at the current head from the first inode while keeping all existing runs and any reorganization. Because postings index content values (which remain valid across commits), kept `texts` rows let the backfill skip re-reading and re-tokenizing unchanged values. Only disabling grep clears the runs.

A fork's manifest 1 copies its source's `grep` entry verbatim (see [Forking a namespace](#92-forking-a-namespace)). The fork's first step adapts it by state:

- **`active`:** Immediately usable in the fork, with the fork's commits as the unindexed tail. If the source's watermark lagged behind the fork point, the step reads the gap `(watermark, fork_point_seq]` through the fork's fork point snapshot without touching the source (see [Reads, deletion, and expiry](#83-reads-deletion-and-expiry)), then continues into the fork's feed. If the source's retention floor was already above the watermark when forked, the step rebootstraps.
- **`backfilling`:** Continues only if `captured_seq == fork_point_seq`. Otherwise `captured_seq` is below the fork's retention floor, so the step rebootstraps at the fork's head while keeping inherited runs.
- **Inherited `reorganize`:** Dropped on the fork's first change (identified by its outputs' `owner_namespace_id`) while keeping all listed segments. Because reorganization unions its inputs, reorganizing inherited inputs alongside inherited outputs is harmless.
- **`disabled`:** Stays disabled.

An optional `reorganize` object contains `input_segment_ids`, `output_segment_ids`, inclusive `row_key_cursor`, `output_level`, and `run_no` (plus `pruning` for base-level reorganizations, see [Pruning the index](#d5-pruning-the-index)). Input and output segment IDs MUST be unique, disjoint, and present in `runs`. Every output MUST belong to the run with `run_no` and `output_level`, and all run numbers in `runs` MUST be distinct.

Grep organizes runs into three levels instead of core's base/delta tiers:

- **Level 0:** delta output from backfill and build steps.
- **Level 1:** intermediate reorganization output.
- **Level 2:** base run.

Assigning `run_no` and `output_level` when a reorganization starts lets resumed steps keep appending to the same run while ensuring concurrent runs get higher numbers. A reorganization triggers whenever level 0 reaches 8 runs (rewritten into level 1) or level 1 reaches 8 runs (rewritten with level 2 into a new base). (8 is a worker default, not a format constraint.) Within a run of steps, reorganization always takes priority over backfill/build whenever a reorganization is active or a level hits its limit. Otherwise, a continuously busy namespace could accumulate unbounded level-0 runs, driving up `texts` lookups on every build step.

### D.3 Tokenization and postings

A file revision is eligible for version 1 trigram indexing if its size is at most 8 MiB and its first 8 KiB contain no NUL byte and are valid UTF-8 (allowing one incomplete trailing multibyte character after a non-empty valid prefix). Bytes past the first 8 KiB aren't checked for UTF-8.

For eligible content, the tokenizer lowercases ASCII letters only (distinct from filename Unicode folding) and emits every overlapping 3-byte window as 6 lowercase hex characters. For example, `Abcd` emits `abc` (`616263`) and `bcd` (`626364`).

Grep indexes **content values** (`(content_id, size_bytes)` pairs), not individual file revisions, because the same content value always maps to identical bytes across a [pool](glossary.md#pool) (see [File contents and ownership](#15-file-contents-and-ownership)). Copies and restores therefore reuse existing index entries without re-reading bytes.

Grep segments use the exact data, filter, and index block framing from Appendix A with five kind-tagged CBOR row families (storing content IDs with their `con_` prefix): postings, value tables, `texts`, `content_revisions`, and base-reorganization revision filters (see [Pruning the index](#d5-pruning-the-index)).

All segments in one run share a **value table** mapping `(content_id, size_bytes)` values (every value with a `texts` row in that run) to dense zero-based **ordinals** sorted by `content_id`, then `size_bytes`. For example, `(con_1..., 12)`, `(con_1..., 40)`, and `(con_7..., 5)` get ordinals 0, 1, and 2. Indexing postings by ordinal instead of 16-byte random content IDs lets each gap compress into about 1 byte while storing each content ID only once per run.

A `value` row has fields `ordinal`, `content_id`, and `size_bytes`, row key `value-{ordinal:020}`, and filter key `value`. Value rows sort after all other families, so readers locate them by segment key range without probing the Bloom filter. A run's value rows MUST cover `0..N` contiguously in content order across the run's segments.

A `gram_postings` row lists value ordinals containing a trigram. Its fields are `gram` (6-char hex), `first_ordinal`, and `postings` (a CBOR byte string of delta-encoded LEB128 ordinals), with row key `gram-{gram_hex}-{first_ordinal:020}` and filter key `gram-{gram_hex}`. Multiple rows can store batches for the same gram across a run, and readers merge them. Each batch stores strictly ascending ordinals:

```text
posting_count   (unsigned LEB128)
first_ordinal   (unsigned LEB128)
gap             (unsigned LEB128, at least 1)
... one gap for each remaining posting
```

For example, ordinals 7, 8, and 300 encode as `03 07 01 a4 02`. `first_ordinal` MUST match the batch's first ordinal. Empty batches, zero gaps, ordinals exceeding `u64::MAX`, trailing bytes, and out-of-range ordinals are invalid.

Reorganization builds a new run whose value table is the union of its inputs' tables (minus pruned values on a base reorganization, see [Pruning the index](#d5-pruning-the-index)). Its first step writes the complete output value table so every output segment is immediately readable, and each step translates input ordinals to output ordinals via the two tables.

Version 1 pins the tokenizer, posting layout, and row keys, so none may change under version 1.

### D.4 The `texts` and `content_revisions` families

Two helper families let build steps skip already-examined content and let queries map matched content values back to files:

- **`texts`** stores one `text` row (`content_id`, `size_bytes`, `eligible`) per examined content value, with row key `text-{content_id_body}-{size_bytes:020}` and filter key `text-{content_id_body}`. Before reading bytes, a grep step checks the pending entry's segments for a `texts` row and skips the value if found. Otherwise it records eligibility (marking values over 8 MiB ineligible without reading them) and writes the `texts` row, value-table entry, and any postings in the same run. Eligibility is a pure function of bytes, so duplicate rows MUST agree and merge into one during reorganization.
- **`content_revisions`** stores one `content_revision` row (`content_id`, `size_bytes`, 16-byte `inode_id`, `revision_no`, `committed_seq`) per indexed file revision, with row key `content-revision-{content_id_body}-{size_bytes:020}-{inode_id_body}-{u64::MAX - revision_no:020}` and filter key `content-revision-{content_id_body}-{size_bytes:020}`. Because no core family indexes by content value, grep maintains this reverse index directly from the listings and change events it already scans. Scanning a value's prefix returns matching revisions ordered by inode, newest revision first. Backfill writes rows for every file's current revision (including deleted files), and build steps write rows for every `file_created` and `content_changed` event even when the content value already has a `texts` row. Duplicate rows merge into one during reorganization.

### D.5 Pruning the index

A reorganization into the base level (level 2) drops postings, `texts` rows, and `content_revisions` rows for content values and revisions that the namespace no longer retains. Its first step records `pruning` with two sequences: `filter_seq` (the current head) and `retention_floor_seq` (the namespace's retention floor at that time, at or below `filter_seq`).

The reorganization keeps every file revision at or below `filter_seq` that's above `retention_floor_seq`, plus each file's newest revision at or below `retention_floor_seq` (including deleted files). Applying the floor rule excludes revisions already replaced at or below the floor even if core `revisions` compaction hasn't dropped them yet.

Pruning runs across steps without rescanning the namespace:

1. Step 1 scans the namespace's revisions at `filter_seq` (including the WAL tail) once and builds two Bloom filters sized per [Filter sizes](#filter-sizes) (up to 64 MiB each): a **value filter** of kept content values, and a **revision filter** of kept `(inode_id, revision_no)` pairs. It writes the revision filter and the pruned output value table (streaming input tables in content order and keeping values present in the value filter) into output segments of the new run.
2. Later steps read the revision filter and output value table directly from those output segments, keeping only 4-byte-per-value ordinal translation lists and the filters in memory.
3. Each step translates posting ordinals and drops postings whose value isn't in the output table. Because a run's `n`th `texts` row corresponds to its `n`th table entry, `texts` rows drop in lockstep with postings. (Dropping postings without dropping `texts` would cause future build steps to skip the value permanently.)
4. Each step drops any `content_revisions` row whose `(inode_id, revision_no)` misses the revision filter.

The revision filter is stored in `filter` rows (`chunk`, `payload` up to 32 KiB each, row key `filter-{chunk:020}`, filter key `filter`) whose concatenated payloads form a standard filter block over keys `{inode_id_body}-{revision_no:020}` (see [Block-segment encoding](#a7-block-segment-encoding)). Reorganizations never copy `filter` rows from input runs. Bloom false positives simply survive until a later reorganization. If the retention floor passes `filter_seq` before completion, the reorganization is abandoned.

Right before finishing, the final step runs a **re-reference check**: it scans the change feed from `filter_seq` to head for any `file_created` or `content_changed` event referencing a content value that an input table contained but pruning dropped. If one appears, a concurrent build step may have skipped indexing that value because the old `texts` row was still visible, so publishing the pruned run would permanently hide the new file. The final step therefore abandons the reorganization, removes its outputs from the pending entry, keeps the inputs, and lets garbage collection reclaim the abandoned outputs.

### D.6 Querying the index

A search reads the namespace at a **read sequence**: the head for page 1, and page 1's head (stored in the cursor) for later pages, ensuring every page sees a consistent file set and stable ordering. If the retention floor passes the read sequence, the page returns `below_retention_floor`.

The query extracts trigrams, intersects their posting lists per run, and merges surviving ordinals across runs in content order, reading value-table batches incrementally proportional to examined candidates. Because ordinal order equals content order, later pages binary-search directly to the cursor's content value in each run. For each candidate content value:

1. Read its `content_revisions` rows and keep each inode's newest revision with `committed_seq <= cut`.
2. Verify the file belongs in the page's cut: at the read sequence, the file is visible, its current revision matches the row, and its path lies inside the scope resolved at the read sequence. Later pages compute cut membership as the service so permission changes don't shift pagination. Files unchanged at head are filtered with the subject's current rights, while historical files require the `history` right.
3. Read and verify the value's bytes once and emit matches for every passing file.

The **cut** is normally the read sequence, ignoring revisions committed above it. For `allow_stale` queries whose unindexed tail exceeds the single-query tail scan limit, the cut drops to the last commit fully covered by the index (`built_through_seq`, or `built_through_seq - 1` when `next_event_index > 0`), while visibility is still checked at the read sequence (excluding newer revisions and deleted files). Later pages use `min(cursor.cut, current_index_full_commit)` and skip tail scanning (`tail_scanned: false`) whenever the cut is below the read sequence, keeping stale searches consistent even as indexing catches up.

The **unindexed tail** covers file revisions committed in `(watermark, read_sequence]`. In a fork whose watermark sits below the fork point, the tail starts with the gap read through the fork point snapshot without a subject (just like a build step, see [Index lifecycle](#d2-index-lifecycle)), then enforces subject rights on matches in the fork. For each tail file's content value, the query checks `texts`: values with no row are read and verified regardless of grams, ineligible rows are skipped, and eligible rows are scanned only if the posting intersection produced them. Tail files merge in content order with indexed files.

Matches are ordered by `(content_id, size_bytes)`, then `inode_id`, then byte offset. Pages bound both the number of content values read and files checked, so a page can stop mid-value. The cursor records `(content_value, inode_id, file_offset)`, `read_sequence`, `cut`, the active index's `captured_seq`, and the request fingerprint. Checking `captured_seq` prevents silent omissions if grep was disabled and rebuilt after page 1 (a rebuilt index lacks `content_revisions` for files replaced before the new backfill started). A mismatched `captured_seq` returns `invalid_request` so the client restarts from page 1.

### D.7 Grep collection

Core garbage collection reclaims unreferenced grep segments under the normal segment rule (see [Extensions](#124-extensions) and [Candidate and age rules](#113-candidate-and-age-rules)), treating both inputs and outputs of an in-progress reorganization as listed. Segments from lost or crashed updates are unlisted immediately, and disabling grep (or tombstoning the namespace) empties `runs` so all segments become eligible for deletion. Inherited segments stay under the source namespace's prefix until neither the fork's manifests nor the source manifest kept by the fork snapshot list them.

[api-spec]: api.md
[provider-spec]: object-storage-providers.md
[limits-source]: ../../crates/loonfs-core/src/limits.rs
[fingerprint-vectors]: ../../crates/loonfs-types/tests/golden/commit_fingerprints_v1.json
[name-vectors]: ../../crates/loonfs-types/tests/golden/name_folding.v1.json
