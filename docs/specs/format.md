# LoonFS storage format

This document describes how LoonFS stores a filesystem in object storage: the objects it writes, how they're encoded, and the rules for reading, writing, and maintaining them. An implementation that follows these rules can read and write any LoonFS namespace, and it doesn't have to offer the LoonFS HTTP API (the [API specification][api-spec] defines that interface separately).

A filesystem in object storage has to recover from a crash at any point. For example, an upload might finish, and then the metadata commit that references it might fail. The uploaded object exists, but it isn't referenced by any undeleted file. To recover after a restart, a reader needs a durable record of which changes were committed.

LoonFS keeps that record in an immutable write-ahead log (WAL). WAL objects are numbered in order (1, 2, 3, ...). To commit a batch of changes, a writer writes them into the next WAL object, and once that object is durably written to object storage, every change in it is committed. Periodically, LoonFS writes the committed metadata into sorted segments and publishes a numbered manifest that lists the segments a reader needs. A small hint records a recent manifest number, so readers know where to start looking for the current manifest.

File contents are stored separately from metadata, in content objects. File content is durable no later than the commit that references it ([File contents and ownership](#15-file-contents-and-ownership) describes the two ways content becomes durable).

Everything needed to recover a namespace is stored in object storage. That includes the control records, and the metadata and content of every retained view (keeping only the latest version of each file isn't enough). Local databases and caches can be rebuilt.

The key words MUST and MUST NOT in this document are to be interpreted as described in BCP 14 ([RFC 2119](https://www.rfc-editor.org/rfc/rfc2119), [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)) when, and only when, they appear in capitals. This document also follows these conventions:

- Examples explain requirements, but they don't add fields or behavior.
- Implementation defaults are labeled as defaults, and they aren't format requirements.
- Absolute timestamps are Unix milliseconds. Budgets, lease lifetimes, and grace periods are durations in milliseconds.

## Contents

| Section | Subject |
| --- | --- |
| [1. The storage model](#1-the-storage-model) | Namespaces, inodes, names, revisions, deletion, attributes, and access |
| [2. Objects and references](#2-objects-and-references) | Object keys, numbered manifests and WAL objects, and the hint |
| [3. Object-storage requirements](#3-object-storage-requirements) | Conditional writes, reads, listings, and provider assumptions |
| [4. Reading a namespace](#4-reading-a-namespace) | Recovery, visibility, path lookup, and content verification |
| [5. Uploading content](#5-uploading-content) | Upload sessions, direct transfers, and admission proofs |
| [6. Publishing a commit](#6-publishing-a-commit) | Writer fencing, validation, group commit, and retries |
| [7. Materializing metadata](#7-materializing-metadata) | Manifests, runs, segments, and publication |
| [8. Pins](#8-pins) | Stable views, verification, deletion, and expiry |
| [9. Namespace lifecycle and forks](#9-namespace-lifecycle-and-forks) | Creation, copy-on-write forks, deletion, and retirement |
| [10. Retention and compaction](#10-retention-and-compaction) | History boundaries and metadata rewrites |
| [11. Garbage collection](#11-garbage-collection) | Reference roots, collection safety, and complete passes |
| [12. Encodings, versions, and extensions](#12-encodings-versions-and-extensions) | Compatibility rules and extension boundaries |
| [13. Alternatives considered](#13-alternatives-considered) | Designs that were considered and not used |
| [Appendix A](#appendix-a-durable-records-and-byte-encodings) | Record fields, row keys, and block encoding |
| [Appendix B](#appendix-b-semantic-commit-fingerprints) | Exact fingerprint canonicalization |
| [Appendix C](#appendix-c-timing-and-size-reference) | Timing assumptions, constants, and implementation defaults |
| [Appendix D](#appendix-d-grep-extension-format) | Grep's separately owned durable state |

## 1. The storage model

This section describes the logical model that the stored objects represent: namespaces, inodes, commits, directory bindings, names, file contents, deletions, attributes, and access rows.

### 1.1 Namespaces and identity

A "namespace" is a directory tree with its own ordered history of changes. A namespace ID identifies one namespace from its creation to its deletion, and it can't be reused. Deleting a namespace publishes a final manifest, the "namespace tombstone", which remains the namespace's current manifest. A create or fork that uses a deleted namespace's ID returns `namespace_deleted`.

A namespace is created when its manifest 1 is written with put-if-absent. The current manifest records the namespace's identity, creation time, lifecycle status, and writer authority. Together, the current manifest and the WAL objects after it describe the namespace's current metadata.

Across namespaces, two pairs of values identify items and commit positions:

- An item is identified by `(namespace_id, inode_id)`.
- A commit position is identified by `(namespace_id, committed_seq)`.

Inode IDs are integers in storage. The public API represents the same IDs as strings, such as `ino_42`.

A "fork" is a new namespace that starts from a retained view of another namespace. It starts with the same files and shares their stored objects, and later commits only change the fork. [Namespace lifecycle and forks](#9-namespace-lifecycle-and-forks) describes how the shared objects are kept.

The root directory has inode ID 1. A new namespace starts at sequence 0, with inode ID 2 as the next ID to allocate. New inode IDs are allocated in increasing order as part of metadata publication. Renaming a file doesn't change its inode ID, but deleting a file and creating another one at the same path allocates a new ID.

Version 1 of the format defines two inode kinds, `dir` and `file`. ([Reserved functionality](#125-reserved-functionality) reserves a third kind, `link`.) An inode row records only the item's kind and creation metadata. The item's current parent and name are stored as directory bindings, and its contents are stored as file revisions, so neither the path nor the current content reference is stored on the inode row.

### 1.2 Commits and revisions

A "commit" is one successfully published mutation request. A request can contain several operations, and they're all published together at one namespace sequence number (`seq`).

A "file revision" is one version of a file's contents. Each file revision is assigned a revision number (`revision_no`) that's scoped to its file. Sequence numbers and revision numbers are separate counters, and a namespace can commit many renames, directory changes, and writes to other files between two revisions of one file.

For example, a file might go through the following history (the sequence values are illustrative):

| Namespace sequence | Operation | Result |
| --- | --- | --- |
| 17 | Create `/reports/Report.txt` | Allocate inode `42`, bind the name, and publish file revision `1`. |
| 18 | Rename it to `/reports/Final.txt` | Keep inode `42` and revision `1`, and replace the directory binding. |
| 19 | Replace its contents | Keep inode `42`, and publish revision `2`. |
| 20 | Delete `/reports/Final.txt` | Remove the binding, and record a deletion for inode `42`. |
| 21 | Undelete it | Bind inode `42` again, and keep its revision history. |

The directory state at sequence 18 and the file's contents at sequence 19 are read from different metadata rows, and the inode row doesn't change in either case.

### 1.3 Directory bindings

A "directory binding" places an item in the tree. It links a "directory slot", which is a parent inode and a name key (`parent_inode_id`, `name_key`), to one child inode. A slot is either bound to a child or unbound. A bound slot stores:

- the display name (`display_name`), exactly as the caller provided it, and
- the child's kind and creation attribution, which never change for the life of the inode.

LoonFS compares sibling names by their name key, which is derived from the display name (see [Names and paths](#14-names-and-paths)).

Each binding change writes a directory binding row (`direntry_binding`). The row records its own position in the namespace history: the sequence number of its commit (`committed_seq`) and its position within that commit (`delta_index`). To find a slot's value at sequence `N`, a reader takes the slot's row with the greatest position whose sequence is at or below `N`. If that row is unbound, or the slot has no such row, the name is available. When one commit changes a slot more than once, the last change wins.

LoonFS stores the same binding rows in two indexes, so it can look them up from either side:

- `direntry_binds` orders the rows by slot, to find the child at a name.
- `direntry_child_binds` orders the rows by child, to find a child's current parent (using the same rule).

Commit validation allows at most one child per slot and at most one parent per child. A bind MUST refer to a child that has no current binding, unless the same operation removes that binding first. A move removes the child's old binding before it binds the new one. Because of these rules, the two indexes agree at every read sequence.

A bind writes a bound value to both indexes. An unbind writes an unbound value to both, with the parent, name key, and child from its WAL delta. The WAL delta also identifies the bind it removes, by sequence and delta index, and validation checks that this bind is the current binding. (The materialized row stores only the unbind's own position.) An unbound value acts as a tombstone for older values of its slot or child, and [Row retention during a base compaction](#103-row-retention-during-a-base-compaction) defines when it can be removed.

### 1.4 Names and paths

LoonFS stores each display name exactly as the caller provided it. A display name MUST follow these rules:

- It's between 1 and 255 UTF-8 bytes long.
- It doesn't contain `/`, any Unicode control character (general category `Cc`), or any of `: ? * | " < > \`.
- It isn't `.` or `..`, it isn't made up entirely of whitespace, and it doesn't end in a space or a dot.
- It isn't a reserved device name: `CON`, `PRN`, `AUX`, `NUL`, `COM1` through `COM9`, or `LPT1` through `LPT9`. These are compared case-insensitively and without the extension, so `con.txt` is also reserved.

These rules avoid common cross-platform naming problems. They're LoonFS's own name grammar, not a guarantee that every external filesystem, archive tool, or sync client accepts every tree LoonFS can store.

Every namespace has a "naming mode", which controls how names are compared. The mode is stored in the manifest and set when the namespace is created, and it can't be changed (a fork uses its source's mode). LoonFS derives a name key from each display name using the namespace's mode, and it compares and looks up sibling names by their keys:

- `case_insensitive` (the default): NFC normalization, then full Unicode default case folding (not the Turkic variant), then NFC normalization again.
- `case_sensitive`: NFC normalization only.

Both modes use Unicode 17.0.0 data. Validating new names and looking up existing names both use the namespace's mode. Name keys follow the same character rules as display names, with a 768-byte limit. Every key derived from a valid display name fits within that limit, because normalization and case folding expand a name by at most three times in bytes.

For example, `Report.txt` and `report.txt` have the same name key in a `case_insensitive` namespace, so they can't be siblings there. In a `case_sensitive` namespace, their keys are different, and they can be siblings. In both modes, a precomposed `é` and an `e` followed by a combining acute accent have the same key. The display name always keeps the caller's spelling. The [name-folding fixtures][name-vectors] list each mode's key for normalization and case-folding cases beyond ASCII.

The display-name rules above don't depend on the mode, so a `case_sensitive` namespace still compares reserved device names case-insensitively.

A mode's mapping can't change once a namespace has rows, because the namespace's stored slots contain keys derived with that mapping. A change to either mapping would change the format's meaning, even if the serialized fields stayed the same. Implementations MUST preserve the Unicode behavior specified here (converting to lowercase isn't enough).

An absolute path starts with exactly one `/`. It has no empty components, repeated separators, or trailing `/`, except for the root path `/`. A canonical path is at most 4,096 UTF-8 bytes and 128 components. LoonFS rejects noncanonical input instead of rewriting it.

These limits apply to request paths, and they don't promise compatibility with other filesystems' path limits. A path built from stored names can be longer than the limits, and reads by inode still return it.

### 1.5 File contents and ownership

Each file revision has one content reference (`ContentRef`). The only kind of content reference is `blob_v1`, which identifies the first `size_bytes` bytes of a chain and includes a checksum of those bytes. Those bytes are the file's bytes (LoonFS doesn't add an envelope around them). Content derived from other content, such as an append, is still stored behind an ordinary `blob_v1` reference, never a second kind of reference.

A content reference contains:

- the original owner namespace,
- a random content ID,
- the size of the prefix it refers to, and
- a checksum of that prefix.

A reference contains no bucket address or object-store path. The owner namespace and content ID determine where the bytes are stored.

A "chain" is the bytes stored under one content ID. A chain grows when a file is appended to, and each reference refers to a prefix of the chain, so two references with the same content ID can refer to different lengths of one chain. For example, a log file's first revision might refer to the first 100 bytes of a chain, and after two appends, revisions 2 and 3 might refer to the first 200 and the first 210 bytes of the same chain.

Content objects are immutable, so the bytes appended to a chain are written to new content objects, and a chain's bytes can be spread across several of them. A "layout" lists those objects, in chain order, as "extents". Each extent records the object's owner namespace, its content ID, the object, the offset in that object, and the length. A revision's WAL delta includes the extents its bytes need, except for bytes that are still stored as WAL pieces and bytes that are already covered by the chain's earlier revisions. Copies and restores include layouts that cover their references, and validation writes any pending pieces those layouts need to content objects.

A chain's "whole object" is stored at the chain's own key. Every other object of the chain is a "span object", and its key adds the start and end chain offsets of the range it stores:

```text
namespaces/{owner_namespace_id}/content/{content_id}
namespaces/{owner_namespace_id}/content/{content_id}-{start:020}-{end:020}
```

The whole object sorts before its spans, so all the objects of one chain are next to each other in a listing.

File content is durable no later than the commit that references it. File content can be published via one of two paths:

| Path | How the bytes become durable |
| --- | --- |
| Uploaded | An upload session writes and verifies the content object before the commit (see [Uploading content](#5-uploading-content)). |
| Inline | The WAL object that commits the revision includes the bytes as "pieces" ([WAL records](#a5-wal-records)), so the bytes become durable in the same write that makes the revision visible. A piece contains a whole value, or the bytes an append adds after bytes that are already durable. Before a fold publishes a manifest whose `folded_wal_no` covers that WAL object, it writes the pieces to content objects as extents (see [Publishing a materialized file set](#72-publishing-a-materialized-file-set)). |

Garbage collection deletes a WAL object only after a manifest covers it, and the fold that published that manifest has already written the WAL object's pieces as extents. So a WAL object is never deleted while some of its pieces exist only in the WAL. This is also why a reference identifies content but doesn't prove that a content object contains its bytes yet. [Content verification](#45-content-verification) defines where a reader finds the bytes.

The owner of a new upload is the uploading namespace, and inherited references keep their original owners. Forking, restoring a revision, replaying the WAL, and compacting metadata MUST NOT replace a reference's owner with the current namespace.

Reading inherited content doesn't require the original owner's manifest. This matters for forks, because a fork can keep reading the exact objects in its pinned basis after its source namespace is deleted.

Two separate uploads of identical bytes create two objects, because LoonFS doesn't deduplicate content across uploads. A retry within one upload session reuses that session's content ID, so retries don't depend on finding another upload with identical bytes.

### 1.6 File and subtree deletion

Deleting an item removes its current binding and records a "tombstone" for it. The item's revisions, and everything below it, stay in place, and the tombstone hides them. A tombstone on a directory hides the directory and all of its descendants, so a deletion doesn't need a separate tombstone for every descendant.

A tombstone is a series of events, stored as rows. Each row records its own position in the namespace history (`committed_seq` and `delta_index`), and events are ordered by position. There are three kinds of event:

- A `set` event starts a deletion. It stores the removed binding (`deleted_binding`, with `parent_inode_id`, `name_key`, and `display_name`), so the name can be restored even after old binding versions are compacted.
- A `revoke` event, which an undelete commits, cancels one `set` event. It identifies that event by position (a `DeltaPosition` value, `{seq, delta_index}`). A `revoke` MUST NOT contain a deleted binding.
- An `expire` event ends the recoverability of one `set` event, so the deletion can no longer be undeleted. It identifies that event by position, like a `revoke`. Only the namespace's writer commits an `expire`, and it's attributed to the actor `loonfs`. An `expire` MUST NOT contain a deleted binding.

At a given sequence, the latest event for the deletion's root inode determines the state of the root and everything below it:

- After a `set`, the tombstone hides the subtree, and the deletion is recoverable.
- After an `expire`, the tombstone still hides the subtree, but the deletion isn't recoverable.
- After a `revoke`, the tombstone hides nothing.

An undelete commits its `revoke` together with a new binding for the item, so the subtree reappears. An `expire` restores nothing, so the subtree stays hidden. A `set` after a revoked deletion of the same inode is a new deletion.

For example, suppose the directory `/old` (inode 7) is deleted at sequence 40, and the namespace's writer commits an `expire` for that deletion at sequence 61. At sequence 50, an undelete can restore `/old`. From sequence 61 on, `/old` and everything below it stay hidden, the trash listing leaves the deletion out, and an undelete of the deletion returns `not_deleted`.

An undelete request identifies a deletion by its inode and the sequence it was committed at. Validation MUST confirm that this deletion is recoverable, which means that its `set` is the root's latest event. A request for a deletion that's no longer recoverable returns `not_deleted`, and it MUST NOT cancel a later deletion. (A request can't undelete a deletion made earlier in the same request, because that deletion doesn't have a committed sequence yet.) An undelete restores the saved parent and name, unless the request provides a different destination. Only the root of a deletion can be undeleted, because the descendants it hides don't have deletions of their own. The writer validates each `expire` the same way, so it can't expire a deletion that an undelete has already revoked.

The `active_deletions` family is derived from tombstone events, and it's used to list recoverable deletions in deletion-sequence order. A `set` produces a `listed` row, and a `revoke` or an `expire` produces a `removed` row for the same `(deletion_seq, root_inode_id)`. Removed rows sort before listed rows, so a scan can skip deletions that were undeleted or expired. The tombstone events remain the source of truth.

Deleting a file doesn't delete its content right away. In a live namespace, file revisions and recoverable deletions are kept under the rules in [Retention and compaction](#10-retention-and-compaction). Deleting a namespace is a separate operation, described in [Namespace lifecycle and forks](#9-namespace-lifecycle-and-forks).

Why can a deletion stop being recoverable? LoonFS can't reclaim the rows and content that a deletion hides while an undelete could still restore them, so a deletion needs a point in the namespace's history after which it can't be undone.

Why is expiry an event in the writer's log? An undelete is a writer commit, so recording the end of a deletion's recoverability as another writer commit lets the WAL order the two. An undelete that the writer validated before an `expire` comes before it in the WAL and has already revoked the deletion, so that `expire` fails validation. An undelete that the writer validates after the `expire` returns `not_deleted`. Either way, no request can restore a deletion after its `expire` is committed.

### 1.7 Attributes and attribution

"Attributes" are a map from strings to strings, attached to an inode. Applications use them for tags, display hints, and similar metadata. Renaming or moving an item, or writing a new file revision, doesn't change its attributes.

An attribute map follows these limits:

| Part | Rule |
| --- | --- |
| Key | 1 to 128 UTF-8 bytes, with no Unicode `Cc` control characters. Keys are compared exactly, without normalization or case folding. Keys that start with `loonfs.` are reserved for LoonFS, and callers can't write them. |
| Value | A UTF-8 string of at most 4,096 bytes. Empty strings and control characters are valid. |
| Map | At most 100 entries, and at most 65,536 bytes of keys and values combined (counted as UTF-8, without serialization overhead). |

Removing a key is an explicit operation, and setting a key to an empty value doesn't remove it. A durable map that breaks these limits is rejected, not truncated.

An inode starts with an empty map at attribute revision `0`, and that initial state has no stored row. Each accepted update increases the attribute revision number (`attributes_revision_no`) by one and stores the complete resulting map, even when the map is unchanged. Clearing the map is a real update, and it MUST remain distinguishable from an inode whose attributes were never changed.

Clients use attribute revision numbers for optimistic concurrency. The API doesn't offer a way to list old attribute maps, but the storage layer keeps the rows needed for supported sequence-based views, as the compaction rules specify.

Every commit also records a timestamp, an optional message, and an "actor". The actor is a required, opaque identifier that the application supplies. LoonFS stores it without authenticating it or looking up any profile information, so the application chooses a stable identifier and its identity scope. The actor records who made a change, and it plays no part in authorization.

Every row that records an event copies its commit's actor and timestamp (as `committed_by` and `committed_at_ms`), along with the commit's `committed_seq` and `commit_id`. The API exposes these fields under the names the API specification defines, such as `created_by` for the commit that created an inode and `attributes_updated_by` for its newest attribute revision.

| Metadata | Actor and timestamp fields |
| --- | --- |
| Inode, file revision, tombstone event, attribute revision, access revision, commit | `committed_by`, `committed_at_ms` |
| Listed active deletion | `deleted_by`, `deleted_at_ms` |
| Directory binding | `child_created_by`, `child_created_at_ms`, copied from the child inode |

The root inode of a new namespace is attributed to the actor ID `loonfs`. A fork inherits its root inode from its source. The fork's manifest records the fork's own creation time, and the inherited inodes keep their original timestamps.

Event timestamps are informational, and sequence numbers determine order. Renaming an inode doesn't change its creation or content timestamps, and directories don't have a general modification timestamp. (Lease and reclamation deadlines are a different kind of time, covered in [Timing assumptions](#33-timing-assumptions).)

### 1.8 Access rows

An inode can have an "access row", which controls what principals can do to the inode and, through inheritance, to the items below it. An access row has a boundary flag and a map from principals to rights. A "principal" is an identity that the application assigns, such as a user, a group, or a public audience. It's identified by an opaque ID of 1 to 256 visible ASCII characters, excluding the comma. LoonFS stores principal IDs, but it never resolves them.

There are eight rights. The [API specification][api-spec] defines exactly which rights each operation requires.

| Right | Allows |
| --- | --- |
| `read` | Reading the item's metadata and current content. |
| `history` | Reading older revisions and snapshots of the item (together with `read`). |
| `write` | Changing a file's contents or an item's attributes. |
| `create` | Adding entries to a directory. |
| `remove` | Removing entries from a directory. |
| `share` | Changing the item's grants, for rights the subject has itself. |
| `manage` | Making any change to the item's access row. |
| `admin` | Every right on every item, when it's granted on the root inode's row. |

A set of rights is stored as a list of distinct names, in the order of the table above. A decoder accepts the names in any order, but it rejects repeated names. An access map has at most 1,000 principals and at most 65,536 bytes of principal IDs, and every entry has at least one right. A decoder rejects a repeated principal. A durable map that breaks these limits is rejected, not truncated.

An inode starts at access revision `0`, with no boundary and no grants, and that initial state has no stored row. The exception is the root inode of an ACL namespace, which starts with a row at revision `0` that contains the manifest's `root_grants` (written when the namespace is created, like the root inode row). Each accepted update increases the access revision number (`access_revision_no`) by one and stores the complete resulting state. A row with no boundary and no grants is a real revision, distinguishable from an inode whose access was never changed.

Every namespace has an "access mode", which is stored in its manifest and set when the namespace is created (a fork uses its source's access mode and scope):

- `unrestricted`: requests aren't checked against any rights, and the namespace has no access rows.
- `acl`: requests are authorized against access rows. The mode includes a `principal_scope`, which identifies the identity domain of the namespace's principal IDs, and the `root_grants` that the root inode starts with (normally `admin` for each initial administrator). Before authorization reads a grant, the request's subject scope MUST match the namespace's `principal_scope`.

A principal's effective rights on an inode are the union of its grants on the inode's own row and on each ancestor's row. LoonFS walks up the current parent bindings, and it stops after a row whose boundary flag is set, or at the root. For example, suppose a principal has `read` on `/projects`, and `/projects/secret` has its boundary flag set. That `read` doesn't apply inside `/projects/secret`, unless the row for `/projects/secret` (or a row below it) grants it.

A deletion root has no current binding, so while its deletion is recoverable, its tombstone's saved parent is used for that step of the walk. The walk stops at the root of an expired deletion, so the inodes that an expired deletion hides inherit no grants from above its root. Visiting the same inode twice during the walk means the namespace is corrupt. `admin` on the root row grants every right on every item, and it's never inherited. A read by inode ID of an inode that an expired deletion hides, or whose ancestry is disconnected, returns `inode_not_found` before LoonFS evaluates any grant, so no principal can read the inode, an administrator included (see [Paths, listings, and revision reads](#44-paths-listings-and-revision-reads)). A snapshot from before the expiry still shows the inode, and in an ACL namespace, the [API specification][api-spec] allows only an administrator to read it through the snapshot.

The `update_access` operation replaces an inode's access row, and it appears in the change feed as an `access_changed` event. The [API specification][api-spec] defines that operation, and how reads, commits, and upload sessions are authorized against access rows.

## 2. Objects and references

This section describes the objects LoonFS stores for a namespace, how their keys are formed, and how one object refers to another.

### 2.1 Storage layout

Each namespace's objects are stored under its own prefix ([Object keys](#a8-object-keys) lists the complete key patterns):

```text
namespaces/{namespace_id}/
├── hint.json
├── manifests/{manifest_no:020}.json.zst
├── wal/{wal_no:020}.wal.zst
├── segments/{segment_id}.sst.zst
├── pins/{pin_id}.json
├── uploads/{upload_id}.json
├── uploads/{upload_id}.outcome.json
├── content/
├── temporary/{temporary_id}
└── extensions/{extension_name}/...
```

Content objects are stored under their owner's prefix (see [File contents and ownership](#15-file-contents-and-ownership)). Metadata segments work the same way. A segment that a fork inherited stays under its owner's prefix, and its descriptor records the owner (`owner_namespace_id`), so readers never substitute the reading namespace into the key. Every newly written segment, including compaction output, is stored under the writing namespace's `segments/` prefix.

On providers that build a new object by composing whole objects (such as GCS), an assembly writes temporary objects for the parts it can't compose directly: a range of a source object, new bytes, or an intermediate result. The assembly deletes them when it finishes (see [Immutable writes and retries](#32-immutable-writes-and-retries)).

The key layout is part of the format. Other objects MUST NOT use keys in these families. Core readers and garbage collection don't interpret keys under an extension's prefix.

### 2.2 Object roles and mutability

| Object | What it stores | How it changes |
| --- | --- | --- |
| Manifest | The namespace's identity, lifecycle status, writer authority, materialized file set, and retention floor | A new manifest is published at the next number. |
| WAL object | Commits, or a writer fence, after the materialized boundary | A new WAL object is created at the next number. |
| Hint | A recent manifest number, where discovery starts | Compare-and-swap. The number never decreases. |
| Metadata segment | Sorted metadata rows that a manifest references | A new immutable object is written. |
| Pin | A reference that keeps one manifest for a user or a fork | Created and deleted. A user pin's expiry time can be extended by compare-and-swap. |
| Upload session | One transfer and its completed content, until a commit uses it or cleanup removes it | A session record and an outcome record, each created once with put-if-absent. |
| Content object (whole or span) | A range of chain bytes, along with the provider's stored checksum of the whole object | Immutable. |
| Temporary object | Bytes that an assembly needs as a separate object | Created, then deleted. |

Manifests and WAL objects are immutable once they're written. A manifest publication can change the physical layout or the namespace's control state without creating a commit, and a WAL publication can add commits without creating a manifest. The hint doesn't determine history or visibility. It's a best-effort pointer, so its number can lag behind successful manifest publications.

### 2.3 Numbered objects and generated IDs

Manifest numbers and WAL numbers are separate counters for each namespace, and both start at 1. Object keys use twenty decimal digits, so lexicographic order matches numeric order. For example, WAL object 2 is stored at `wal/00000000000000000002.wal.zst`.

Each number refers to exactly one immutable object. A publisher creates the next number with put-if-absent, so competing publishers can't write different objects at the same number. A publisher that loses loads the winning object before it plans another attempt. The namespace and number in an object's payload MUST match its key.

Other objects use generated IDs:

- A pin ID has the form `pin_{manifest_no:020}-{16 lowercase hex}`. The number identifies the pinned manifest, and the random suffix distinguishes separate user and fork pins on the same manifest. The durable record calls it `pin_id`, the API calls it a `checkpoint_id`, and a user pin's ID is also its `snapshot_id`.
- A content ID is `con_` followed by 32 random lowercase hexadecimal characters.
- A metadata segment ID is also generated, instead of being derived from the segment's position in publication history.

A collision at a newly generated immutable key MUST NOT overwrite the existing bytes.

### 2.4 The current manifest and WAL tip

The current manifest is the source of truth for the namespace's identity, status, writer epoch, and compactor epoch. Its runs describe the materialized metadata through the head sequence (`head_seq`), and `folded_wal_no` is the last WAL number that those runs include. The WAL objects after `folded_wal_no` contain the later commits, and the highest-numbered one is the "WAL tip".

Every WAL object records the head sequence and the inode allocator's high-water mark after its commits. A fence moves the WAL number and the writer epoch forward without adding a commit, and it keeps the allocator value it started from.

Creating or forking a namespace writes the hint before manifest 1, and nothing deletes the hint of a live namespace. So a reader that doesn't find a hint can report that the namespace doesn't exist, without listing any objects.

The namespace's counters advance at different times:

| Counter | Advances when |
| --- | --- |
| `manifest_no` | A manifest is published, including for a layout, authority, retention, or lifecycle change. |
| `wal_no` | A WAL object is created, including an empty writer fence. |
| `seq` | A mutation request commits. One WAL object can contain several sequences. |
| `revision_no` | A new content revision is committed for one file. |

For example, manifest 5 might cover WAL objects through 8 and sequences through 40. WAL object 9 can be a fence at sequence 40, and WAL object 10 can contain commits 41 to 43. The read view is then at sequence 43. A fold can publish manifest 6, covering WAL object 10, without allocating sequence 44.

Why does the hint exist, and why can it lag? A reader needs a place to start that doesn't require listing the namespace's manifests, because object-storage listings are slow, and LoonFS keeps them off every hot path. LoonFS updates the hint after it publishes a manifest, as a separate compare-and-swap. Keeping the hint and the manifest in step would need a transaction across two objects, which object storage doesn't support. So readers treat the hint as a starting point and check for newer manifests (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)).

### 2.5 Manifest references

A durable reference to a manifest contains:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | The namespace that stores the manifest. |
| `manifest_no` | The manifest's number, which determines its key. |
| `head_seq` | The head sequence that the manifest records. |
| `payload_checksum` | A digest that MUST match the manifest's stored payload. |

A pin refers to a manifest in its own namespace, and it stores these fields directly, with its `namespace_id` as the owner. A fork's basis includes a reference to a manifest in the source namespace, along with the source pin's ID. The segment references inside a manifest keep their own owners, so a manifest can list segments that belong to other namespaces.

Readers validate the referenced identity, sequence, and checksum. A missing or corrupt required object is an error, and never a reason to use another manifest instead.

### 2.6 A summary of the log protocol

This section summarizes how commits, writer takeover, folds, discovery, and garbage collection fit together. Sections 4, 6, 7, and 11 define the complete rules.

Each namespace has two sequences of numbered objects, manifests and WAL objects. Each sequence starts at 1, and a publisher creates each object with put-if-absent at the previous number plus one.

```text
                 hint (best effort)
                   |
                   v
manifests:  [4]  [5]  [6]   <- current manifest, folded_wal_no = 8

WAL:        [7]  [8]  [9]  [10]  [11]   <- WAL tip
                      |_____________|
                      visible WAL, replayed on top of manifest 6
```

- A manifest records the namespace's lifecycle status, its writer and compactor epochs, `head_seq`, `folded_wal_no`, and the metadata segments that contain its rows through `head_seq`.
- A WAL object contains the commits that follow the previous WAL object, stamped with the writer epoch of the session that wrote it. A fence contains no commits.
- The hint is mutable. It records a manifest number, manifest publications raise it by compare-and-swap, and it never decreases.
- Content objects store immutable ranges of file bytes, and a layout lists those ranges in chain order.
- Pins are retained manifests (see [Pins](#8-pins)).

A commit creates WAL object `tip + 1` with put-if-absent. An acquired writer session plans its batch against the tip it discovered, and the put is the commit boundary. A put that collides creates nothing, so the writer discovers the new tip and plans again (see [Publication and group commit](#63-publication-and-group-commit)).

A new writer session takes over by publishing the next manifest with `writer_epoch + 1`, and then creating a fence with that epoch at the next WAL number. Between the manifest and the fence, the previous writer can still commit at `tip + 1`. Those commits are valid, because their epoch is below the manifest's and the fence is written above them. Once the fence is written, the previous writer session can't commit. Its next put collides, the discovery that follows finds the higher epoch, and the session is fenced (it stops committing, and it doesn't reacquire authority on its own). Any earlier discovery that finds the higher epoch fences the session the same way (see [Writer ownership](#61-writer-ownership)).

A fold doesn't need a writer epoch, and any process can run one. A fold moves the WAL objects after the current manifest's `folded_wal_no` into new metadata segments, and it publishes the next manifest with `folded_wal_no` set to the tip it read. That manifest keeps the previous manifest's writer and compactor epochs. Before it publishes, the fold writes the pieces it covers to content objects as immutable extents (see [Publishing a materialized file set](#72-publishing-a-materialized-file-set)).

To discover a namespace's current state, a reader:

1. Loads the hint and the manifest it points to.
2. Probes the following manifest numbers until one is missing, and rereads the hint in case it was raised in the meantime.
3. Reads WAL objects from `folded_wal_no + 1` upward, in concurrent windows, until the first missing number.
4. Checks that the manifest still has no successor.

If a successor appeared, or the check in step 4 happened `READ_REVALIDATION_BOUND_MS` or more after the manifest probe, the reader reloads the manifest and starts again. A missing or invalid WAL object also makes the reader reload the manifest, and if the manifest number hasn't changed, the store is corrupt (see [Resolving the metadata basis](#41-resolving-the-metadata-basis) and [Replaying the visible WAL](#42-replaying-the-visible-wal)).

Garbage collection deletes WAL objects at or below the current manifest's `folded_wal_no`, and unpinned manifests below the hint value it observed. It deletes each one only after the object's provider age reaches the reclamation grace (see [Reference roots](#112-reference-roots) and [Candidate and age rules](#113-candidate-and-age-rules)). It keeps a superseded manifest, and every segment in its runs, until the manifest's successor is one grace old. It deletes a metadata segment once no current, pinned, or kept superseded manifest lists it, and the segment is more than 24 hours old.

Every numbered put has a time budget. A publisher starts each numbered put within a budget of the observation it planned against, and it treats a put that returns after the budget as having an unknown outcome. The reclamation grace is at least the longest budget, plus the provider request bound, plus the clock allowance. Under the provider requirement in [Required operations](#31-required-operations), an object can therefore be deleted only after the provider has applied or rejected every put planned against an observation older than the object (see [Clock and operation assumptions](#114-clock-and-operation-assumptions) and [Publication and collection timing](#c1-publication-and-collection-timing)). Readers have a matching bound. A reader trusts that a manifest has no successor only within `READ_REVALIDATION_BOUND_MS` of the probe that confirmed it (see [Replaying the visible WAL](#42-replaying-the-visible-wal)).

The protocol guarantees the following:

1. Successfully creating WAL object `n + 1` commits its records, and the visible log is the unbroken sequence of WAL numbers above the current manifest's `folded_wal_no` (because of put-if-absent numbering, see [Publication and group commit](#63-publication-and-group-commit)).
2. Every acknowledged commit is in that sequence or folded into the current manifest (because of the publication budget and the grace, see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).
3. Writer epochs never decrease along the sequence, and they never exceed the current manifest's epoch. Once a newer writer's fence is written, no older writer commits (because of the claim and the fence, see [Writer ownership](#61-writer-ownership) and [Replaying the visible WAL](#42-replaying-the-visible-wal)).
4. A fresh discovery sees every commit that was acknowledged before it began (because of the successor check within the revalidation bound, see [Replaying the visible WAL](#42-replaying-the-visible-wal)).
5. A cached view or a writer position never moves backward, and a read served from either one reflects a successor check no older than the revalidation bound (because of the successor probe and the revalidation bound, see [Replaying the visible WAL](#42-replaying-the-visible-wal)).
6. Pieces stay readable, in the WAL until a fold has written them as extents, and through the layout after that (because content is written before the manifest, see [Publishing a materialized file set](#72-publishing-a-materialized-file-set) and [File contents and ownership](#15-file-contents-and-ownership)).
7. A retry with the same commit ID and request doesn't commit twice while its receipt is kept (because of the commit receipt, see [Commit identity and retries](#65-commit-identity-and-retries)).
8. A view that revalidates within its bound never lists a deleted segment. Its manifest was current at its last successor check, so the manifest and its segments are kept for at least one grace after that check (because of the superseded-manifest root and the revalidation bound, see [Reference roots](#112-reference-roots) and [Replaying the visible WAL](#42-replaying-the-visible-wal)).

## 3. Object-storage requirements

This section describes what LoonFS requires from an object store, and how it writes immutable objects safely on each provider.

### 3.1 Required operations

The object-store layer MUST provide:

- create-if-absent (put-if-absent), for publishing immutable objects, at every size (so a large create sent in parts completes conditionally),
- compare-and-swap, for mutable control objects,
- full and ranged reads,
- deletion, and
- reliable prefix listing.

Every operation MUST stay within the configured storage scope.

A compare-and-swap (CAS) replaces an object only if the object's compare token still matches the version the caller read. If another writer has changed the object since then, the update fails its precondition. A conditional update MUST report this result separately from a transport error, because a confirmed precondition failure means the object wasn't replaced. The object-store layer sends a conditional write as one request and never resends it. (If it resent a write whose first attempt had succeeded, the second attempt would fail its precondition, and that failure would be misleading.) A transport failure after the request was sent can leave the outcome unknown.

A read of a mutable object MUST return the bytes and the compare token for those same bytes in one response. Reading them separately isn't enough, because a concurrent update could pair one version's payload with another version's token. Compare tokens (including ETags) are opaque, and LoonFS doesn't assume they're content digests.

Successful writes and deletes MUST be strongly consistent. Prefix listings return keys in ascending lexicographic order, and the garbage collection protocols depend on this behavior (being "S3 compatible" doesn't by itself mean a provider conforms). The object-store layer MUST also list the child prefixes directly under a prefix, in the same order and one page per provider request, starting after a given key. A host uses this listing to find namespaces without listing their objects.

A provider MUST also apply each request, if it applies it at all, within `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` of when the client started the operation that sent it. The reclamation grace reserves this much provider time. An implementation can't check this assumption. [Clock and operation assumptions](#114-clock-and-operation-assumptions) explains what depends on it, and [Publication and collection timing](#c1-publication-and-collection-timing) lists the numbers.

Readers determine committed state from verified manifests and numbered WAL objects. Garbage collection also uses listings, to find candidates and dependencies (for example, retiring a namespace requires a complete listing of its pins after deletion). The format doesn't assume transactions across objects, or a consistent snapshot across control objects that are read separately.

### 3.2 Immutable writes and retries

An immutable object MUST NOT be replaced with different bytes. A collision at a newly generated content or segment key is an error, while contention for the next manifest or WAL number follows that object's publication protocol. When an operation retries the same object, it can resolve an ambiguous earlier write only with evidence that the stored object has the expected immutable contents. A record created under a generated ID (a pin or an upload session record) is identified by its ID alone. After a put with an unknown outcome, a record under that ID is the creator's own, whatever its bytes are now. (Another caller can change a pin as soon as it's written, so a pin's bytes can't show whose put created it.)

An immutable object is identified by the full-object checksum that its provider stores, including the checksum algorithm. Each store uses one algorithm:

| Store | Algorithm |
| --- | --- |
| S3, R2, S3-compatible endpoints, local storage, and the in-memory reference store | CRC-64/NVME |
| GCS | CRC-32C |

(The local store can be set to use CRC-32C when it's constructed, for conformance tests.)

A writer computes the checksum over its bytes and sends it for the provider to verify, and an assembly combines the verified checksums of its sources. A provider can store a checksum it computes itself, or a value the adapter combines from requests the provider verified. A provider that can do neither isn't supported. Writers and readers don't use SHA-256 metadata from the writer. Every content reference uses the store's algorithm, including references created by inline writes, uploads, copies, restores, and appends.

An immutable write creates its key with put-if-absent, at every size. A large write sends parts and completes conditionally, so it can take longer than one publication request (see [Publication and collection timing](#c1-publication-and-collection-timing)). If the key is already occupied, the writer checks it with one HEAD request. If the stored checksum is equal, the write succeeds, and if it's different or missing, the write fails. No bytes are read back, so a retry whose earlier attempt succeeded also succeeds, by the same check.

A streamed immutable write computes the store's checksum as the bytes pass through. A stream shorter than one part is buffered and written with a single create request. A longer stream sends parts as they arrive and completes with the full-object checksum. An occupied key is checked the same way as for a buffered write. An error in the stream prevents completion, and because a stream can't be sent twice, a transport failure isn't retried.

An "assembly" creates one immutable object from source byte ranges followed by new bytes, all in the order supplied by the caller. Each source identifies an object and includes an optional half-open byte range and the checksum of the selected bytes. If no range is specified, LoonFS uses the whole source object. The expected checksum covers the complete result, and every checksum in an assembly uses the store's algorithm. If a source is missing or shorter than the requested range, LoonFS returns a precondition error.

The caller includes each source object's length and version from a HEAD response. The adapter uses this metadata to read or copy the requested version without sending another HEAD request for the source.

If the destination object already exists, LoonFS compares its stored checksum with the expected checksum. Below the provider's single-request PUT threshold, the adapter attempts a conditional create and sends a HEAD request if the destination already exists. At or above the threshold, the adapter sends a HEAD request before assembling the object. If a previous attempt succeeded, the adapter can verify the existing object without copying the sources again.

Each provider adapter builds an assembly differently, depending on the size of the result:

- **S3.** Below the single-request put threshold (8 MiB), the adapter reads the sources, in reads of at most 8 MiB that are each checked against their checksums, and sends one create-only PUT. At or above the threshold, S3 copies sources of at least 5 MiB inside the provider, in parts of at most 5 GiB. Smaller sources and tail bytes pass through the server, in reads of at most 8 MiB. When an uploaded part needs more bytes to reach 5 MiB, it takes them from the next copied source (except for the final part). S3 combines the parts' CRC-64/NVME checksums before completing, and a mismatch with the expected CRC aborts before completion.
- **R2.** Below 8 MiB, the adapter works the same way as on S3. At or above it, the adapter cuts the result into 64 MiB parts (except the last), and it copies a part inside the provider only when the part lies within one source. Other parts pass through the server. R2 doesn't receive a copy-source `If-Match` header, and it reports no checksum for copied parts, so the adapter checks the stored full-object CRC with a HEAD request after completion. A missing or different checksum fails the assembly and leaves the object for garbage collection. (A writer never deletes an immutable key, because another writer might already have accepted it.)
- **GCS.** Below 8 MiB, the adapter reads the result, verifies each source, and sends one create-only media request. At or above it, GCS composes whole sources. Ranged sources and tail bytes first become temporary objects, and each compose takes at most 32 sources, with intermediate temporary objects for longer lists. Each source has a generation precondition, and the destination uses `ifGenerationMatch=0`. The adapter combines the sources' CRC-32C values before composing, and it checks the returned CRC against a CRC-32C expectation. Every temporary object is deleted afterwards. Failed or cancelled attempts also try to clean up, but a crash can leave temporary objects for garbage collection.
- **Local store.** The adapter streams source ranges and tail pieces into a staging file, buffering at most one 8 MiB read chunk. It checks the expected checksum before publishing with a create-only link, and it computes the store's selected checksum over the bytes.
- **In-memory test store.** The store can buffer the result before its verified put.

On S3 and R2, the adapter completes with `If-None-Match: *` and the expected full-object CRC.

Small control objects use conditional single-object writes. Multipart upload is an optimization for larger payloads, and it doesn't replace compare-and-swap for control objects.

A large create MUST complete conditionally, so that the provider refuses the completion if the key is already occupied. An absence check before completion isn't enough, because another request could write the key between the check and the completion. A provider that can't complete a create conditionally isn't supported. The [provider documentation][provider-spec] describes each provider's path.

The incremental-write adapter reads the whole payload before it evaluates its existence precondition. So a digest computed while forwarding the stream covers the complete payload, even when the write is refused.

A failed or cancelled multipart transfer MUST attempt a provider-side abort. An abort can fail and leave incomplete parts behind, so deployments also need the provider cleanup policy that their adapter assumes. An abort doesn't delete a content object that has already completed. Cleanup of that object follows the upload's durable lifecycle.

Why must a large create complete conditionally? Two requests that upload to one service-proxied session write the same content key (see [Service-proxied staging](#53-service-proxied-staging)). The conditional completion stops the second request from replacing the bytes that the first request wrote, after the session has already reported them.

### 3.3 Timing assumptions

Writer fencing and commit order don't depend on clocks. They use epochs, sequence numbers, and conditional writes. Upload admission, pin expiry, and reclamation do use deadlines.

Garbage collection assumes that clock error is bounded, and that publications and provider operations finish within bounded times. [Appendix C](#appendix-c-timing-and-size-reference) states these bounds. A client-side timeout isn't proof that a remote change had no effect, so an operation with an ambiguous outcome MUST keep its documented unknown-outcome behavior.

## 4. Reading a namespace

This section describes how a reader finds a namespace's current state, determines what's visible at a sequence, and verifies file contents.

### 4.1 Resolving the metadata basis

A cold read (a read with no cached view) starts by finding the current manifest:

1. Load `hint.json`, and load the manifest at its `manifest_no`.
2. Probe the following manifest numbers until one returns not found. The last manifest found is the current one.
3. If the probe ended after an active manifest, read the hint again. If the hint now points to a higher number than the last manifest found, start again from that number. (Garbage collection might have deleted manifests below the new hint while the probe ran.)

The hint is expected to lag. It's a starting point, not a statement that later objects don't exist.

Each probed manifest MUST be a valid successor of the one before it:

- Its identity fields are equal.
- Its `folded_wal_no`, both epochs, `head_seq`, retention floor, allocators, and activity counters are the same or higher.
- It doesn't follow a namespace tombstone (a tombstone ends the probe).

A sequence of manifests that fails this check is corrupt.

| Discovery result | Interpretation |
| --- | --- |
| The hint is missing | The namespace isn't installed. |
| The hint points to manifest 1, which is missing | Installation hasn't finished. |
| A higher hinted manifest is missing | Read the hint again. If it now points to a higher number, start again from it. If it's unchanged, the store is corrupt. (Garbage collection deletes manifests below the hint value it observed, so a manifest that an older hint value pointed to can be gone after the hint is raised.) |
| A required object is unreadable or invalid | An error, not absence. |
| The current manifest is deleted | Ordinary namespace operations return `namespace_deleted`. |

Every namespace has its own manifest from the moment it's installed. A new namespace's empty manifest describes root inode 1 at sequence 0. A fork's first manifest lists the source runs it inherits. Its `fork_basis` records where the fork came from and which pin keeps its basis, and readers don't follow that field to choose a different basis.

The current manifest and the unfolded WAL together make up one read view:

```text
hint --(start)--> manifest 5 --probe--> manifest 6 --probe--> 7 is missing
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

In this example, WAL object 12 is the discovered tip. Discovery MUST also account for a manifest that's published while it runs, as [Replaying the visible WAL](#42-replaying-the-visible-wal) describes.

### 4.2 Replaying the visible WAL

After it selects a manifest, a reader reads the WAL from `folded_wal_no + 1` upward until the first missing number. Discovery and replay are one pass, because the WAL objects that discovery reads are the ones replay needs, so the reader keeps them. Numbers can be read in concurrent windows. Those reads don't all happen at the same moment, so a number that was missing in one read might have been published before a later number was read. The reader reads a missing number again before it treats it as missing. The first number that's still missing ends the tail, and a number that's still missing below a present one is a missing object. Replay requires every WAL number between the folded boundary and the discovered tip.

Each WAL object MUST pass these checks:

- A WAL object other than a fence contains contiguous commits that follow the preceding head. Its `prior_head_seq` is its first record's sequence minus one, so a first sequence of 0 is invalid. A fence's preceding head is its own `head_seq`.
- Its namespace identity, WAL number, sequence range, allocation state, and writer epoch are valid.
- A fence contains no metadata changes.
- Epochs don't decrease along the log, and they don't exceed the current manifest's epoch.

When discovery finds a missing or invalid WAL object, the reader reloads the current manifest. If the manifest number changed, discovery starts again from it, and if it didn't, the store is corrupt. A WAL object whose epoch is higher than the loaded manifest's is one case of this rule, because another writer might have claimed that epoch after the manifest was loaded.

A deleted manifest ends discovery, without a WAL probe or a manifest successor probe. For an active manifest, the reader checks for a successor after WAL discovery, and it reloads if one appeared. This check stops a concurrent fold or retention advance from making a WAL number look unused when garbage collection has actually deleted its object. A required object that's missing or malformed fails the read. The successor check is trusted only while the manifest probe it confirms is younger than `READ_REVALIDATION_BOUND_MS`. A reader that has taken longer reloads the manifest instead, because a successor published after its probe might already have been deleted.

A read is evaluated at one sequence. Replaying the WAL produces a projection of its metadata changes and any inline file bytes, called the "projected WAL tail". An implementation can query verified metadata segments and this projection directly, without loading every metadata row into memory, as long as it applies the same visibility rules.

A warm read is a read from a cached view. The reference runtime checks a cached view with two probes:

- The WAL probe runs for every `strong` warm read. It reads the WAL numbers after the cached tip in the same concurrent windows as discovery, starting with a GET of the next number, so a probe at the tip costs one GET. A missing next number confirms the cached tip. Each present object advances the reader's state, and the first number that's still missing ends the probe. If a number below a present one is still missing, the reader runs discovery again, because a fold and garbage collection might have deleted the WAL objects after the cached tip.
- The successor probe, a HEAD of the next manifest number, runs on a monotonic revalidation interval (one second by default). If it finds a successor, the reader runs discovery again.

The runtime sends a namespace's probes together, as one refresh, and runs one refresh at a time. A read that finds a refresh running waits for it, because the refresh might have sent its probes before the read started, and then shares the next refresh, which every read that arrives during the first refresh shares.

The reader also runs discovery again if an answer to either probe arrives `READ_REVALIDATION_BOUND_MS` or more after the previous successor probe was sent, whatever the answer is. The time is measured when the answer arrives, and against the previous successor probe, not the one being answered. A successor probe that finds nothing becomes the previous successor probe for later reads only after both answers have arrived in time. This is how cached readers notice deletions and retention changes. The hint isn't used to validate a cached view.

An `eventual` warm read (see [Read consistency](api.md#read-consistency)) reads the cached view without a WAL probe or a successor probe while the cached tip was confirmed less than the read's maximum staleness ago. The tip's confirmation time is taken before the WAL probe or the load that confirmed the tip is sent, so the read sees every commit that was acknowledged before then. The read also requires the previous successor probe to be less than `READ_REVALIDATION_BOUND_MS` old. After that, an `eventual` read runs the probes the way a `strong` read does. The reference runtime replaces a cached view only with a newer one, so an `eventual` read never reads an older head than a read that completed before it. When the runtime makes or learns of a change whose effect on the head it can't identify (it creates, forks, or deletes the namespace, a mutation's outcome is unknown, or it finds its writer session fenced), it drops the cached view, and the next read loads the namespace by discovery. A load that started before the drop doesn't install its result, because it might have read the namespace before the change.

In a runtime with an open writer session for the namespace, reads start from the session's [writer position](glossary.md#writer-position) instead of a cached view. The runtime moves the position to each commit before it acknowledges the commit, and to each manifest that it publishes itself (a fold, a compaction, a pin, or a retention advance). A read from the position sends no WAL probe, because another writer has to publish a claim manifest before it can write a WAL object. Instead, the session checks the position with the successor probe. An `eventual` read starts from the position without a probe while the previous successor probe is younger than the read's maximum staleness, and after that it waits for the session's probe. A `strong` read always waits for the session's probe. A probe counts for a read only if it was sent after the read started, and the session runs one probe at a time, so a read that finds a probe running waits for it and then for the next one, which every read that arrives during the first probe shares. The previous successor probe of a new position is the writer's last confirmation of its manifest (see [Publication and group commit](#63-publication-and-group-commit)), or a later probe of the session. If the probe finds a successor, the reader loads the manifests above the position's manifest. While the newest is an active manifest with the position's writer epoch (a fold, a compaction, a pin, or a retention advance, whether this runtime or another process published it), each describes the head the session wrote, so the position moves onto them the way it moves onto the manifests this runtime publishes, and the read starts from the position. The walk that ends on the newest manifest is that manifest's successor probe, so a position that moved onto it records the probe. A manifest with a later writer epoch is another writer's claim, and a tombstone ends the namespace, so the walk stops at either one. The reader then drops the position and runs discovery, and the session's next publish runs discovery too. The reader does the same when the walk finds more than eight manifests above the position's. A read doesn't start from a position whose previous successor probe is `READ_REVALIDATION_BOUND_MS` old or older. The reader drops the position and runs discovery instead, because a probe's answer couldn't confirm the position then. A runtime without the namespace's writer session, or a session that's fenced, closed, or hasn't published yet, reads from a cached view. When a session's position is dropped or the session ends, the position becomes the runtime's cached view of the namespace, so the next read starts from a head at least as new as the reads from the position returned. If the position was dropped because its probe found a successor, or because the session was fenced, the runtime drops that cached view too, and the next read runs discovery.

For example, after another writer takes over the namespace, an `eventual` read in the old writer's runtime that starts within its maximum staleness of the position's last successor probe returns the old writer's head. The first `eventual` read after that waits for the session's probe, finds the new writer's claim manifest, and returns the new writer's commits, including any access changes. A `strong` read waits for the probe too, so it finds the claim and returns the new writer's commits on that read.

### 4.3 Visible metadata

At sequence `N`, a reader ignores events after `N`. A visible inode MUST have been created at or before `N`, and it MUST NOT be hidden by a tombstone on itself or on an ancestor. A tombstone hides its root and the root's descendants at `N` when its latest event at or below `N` is a `set` or an `expire` (see [File and subtree deletion](#16-file-and-subtree-deletion)). For example, a file in a directory whose deletion has expired stays hidden, even though nobody can undelete the directory anymore.

A directory slot's value is its newest version at or below `N`, ordered by `(committed_seq, delta_index)`. A bound version identifies the child, and an unbound version, or no version, means the slot is empty. Parent lookup applies the same rule to the child index.

Resolving a path takes one slot-index lookup per component. A listing scans one parent's prefix in name order and takes the newest visible version of each name. It includes a bound entry only if the child inode was created at or before `N` and no tombstone hides it. Each binding row includes a copy of the child inode's kind and creation attribution. Those never change for the life of an inode, so listings and path resolution read them from the binding instead of from the child's inode row. User pins apply these rules through their pinned manifests, at their captured sequences.

A file's current content is its latest revision committed at or before `N`. Its attributes are the latest applicable complete attribute revision, or the initial empty map when there's no applicable attribute row. Listing recoverable deletions uses the derived active-deletion state, while evaluating a tombstone at a past sequence uses the tombstone events.

### 4.4 Paths, listings, and revision reads

A reader resolves a path from root inode `1`. It maps each component to its name key, using the namespace's naming mode, and follows the active binding. If a component isn't visible, the path doesn't exist. (The current format has no mount points to traverse.)

A directory listing resolves the visible child bindings, and it uses committed revision metadata for file sizes and content-reference summaries. It MUST NOT fetch and verify every file's content object just to list a directory.

A path-based revision read first resolves the current inode at that path, and then it looks up the requested revision of that inode. It isn't a request for every file that has ever been at the path. An inode-based revision read reads the retained history of that inode directly, so it can read a file that has no current path, such as a file under a recoverable deletion.

A read by inode ID MUST return `inode_not_found`, the error for an unknown inode, when the inode's ancestry refuses the read, whether it reads the inode's entry, its revisions, its attributes, or its content. To check the ancestry, the reader starts at the inode, follows the current bound bindings upward, and stops at the first tombstone that hides the inode or one of those ancestors (see [File and subtree deletion](#16-file-and-subtree-deletion)):

- If that tombstone's latest event is `expire`, the read returns `inode_not_found`.
- If its latest event is `set`, the read is allowed, and the subject's rights are evaluated through the deletion's saved parent, as for any recoverable deletion (see [Access rows](#18-access-rows)).
- If the walk reaches the root inode without meeting a tombstone, the read is allowed.
- If the walk reaches an inode with no bound binding before it meets a tombstone, the ancestry is disconnected, and the read returns `inode_not_found`.

The reader applies this rule before it evaluates any access grant, so every principal gets the same result, and it applies the rule from the `expire` event on, even while the inode's rows are still stored.

For example, suppose `/old/draft.txt` is deleted at sequence 30, `/old` is deleted at sequence 40, and the namespace's writer expires the deletion of `/old` at sequence 61. At sequence 50, a read by inode of the revisions of `/old/notes.txt` returns them, and from sequence 61 on, it returns `inode_not_found`. The revisions of `/old/draft.txt` stay readable by inode, because the first tombstone above that file is its own, and that deletion is still recoverable. If a base compaction of the bindings group then removes the bindings under `/old` while the revisions group still stores the revisions of `/old/notes.txt`, the walk from `/old/notes.txt` finds no binding and no tombstone, so the read still returns `inode_not_found`.

Why does a disconnected ancestry refuse the read? Every live inode has bound bindings up to the root inode, and every deleted inode has them up to its deletion's root, which has a tombstone, so a walk that finds neither the root inode nor a tombstone has found an inode that belongs to nothing the namespace can show. Base compactions remove an expired subtree's rows a family group at a time, so the bindings that lead to the expired root can be gone while the revisions below it are still stored, and the walk can then no longer reach the `expire` event.

Why does a read by inode refuse an expired subtree whose rows are still stored? An expired deletion can't be undeleted, so no request needs those rows anymore. Refusing every read from the `expire` event on means that removing the rows, in whatever order the family groups are compacted, can't change what a read returns or who can read it. For example, if a read by inode still evaluated the grants inside the subtree, removing a directory's access row before the revisions below it would remove that directory's boundary, and principals that the boundary excluded could then read those revisions.

### 4.5 Content verification

A content reference identifies a prefix of a chain and its checksum. To read it, a reader uses the view's layout for that chain, and then the chain's visible WAL pieces. The projected tail indexes pieces by content ID and offset. The reader uses the longest visible layout for the reference's own chain (the newest one, if several have the same length), and it looks in both the tail rows and the manifest's segments.

| Condition | Where the reader reads the bytes |
| --- | --- |
| The tail has pieces at or below the reference's size | The reference's own chain layout, up to the lowest tail offset, and then the pieces, up to the reference's size. |
| No pieces cover the reference | The view's layout, up to the reference's size. |
| Zero bytes, and no pieces | Nowhere. The reader verifies the empty value without a content request. |
| A nonzero folded prefix with no layout | Nowhere. The reader reports corruption, identifying the content ID. |

When the reference is shorter than the layout, the read shortens the last extent it uses. Each extent is read with one ranged read at its object offset, and no read makes a HEAD request. If an object is missing, the error identifies the missing extent's key, and a short range reports its expected and actual lengths. The reader checks the assembled length and checksum against the reference, and those errors identify the reference's own key.

Inherited layouts keep their object owners. WAL replay loads pieces along with the metadata, so pieces that are already in memory need no content request. Replay checks each WAL object's envelope, but it doesn't recompute file checksums.

Deleting a WAL object doesn't remove bytes that are already in a read view's projection. But if that projection has to be rebuilt and the WAL objects it needs are gone, the read MUST fail. Finding a content object isn't enough, because the missing WAL objects are also needed to rebuild the view's metadata.

For a streamed read, the full-file checksum can only be verified after the complete stream has been processed. The transport MUST report a verification failure, even if some bytes have already reached the client. A direct download grants one range per extent, and the client receives the bytes directly from object storage. The [API specification][api-spec] defines the client's verification responsibilities for direct downloads.

## 5. Uploading content

This section describes upload sessions, the ways bytes can be transferred, and the proof a commit needs before it can reference uploaded content.

### 5.1 Upload sessions

Every uploaded content object is tied to an upload session before a commit can reference it. The content ID is allocated when the session is created, before any file bytes are read, and new content belongs to the session's namespace (see [File contents and ownership](#15-file-contents-and-ownership)).

An upload session is stored as two records. LoonFS creates each record once, with put-if-absent, and never changes it:

- The "session record" is written when the session starts. It contains `namespace_id`, `upload_id`, `content_id`, an optional `subject_id`, a tagged `mode`, and `expires_at_ms`.
- The "outcome record" is written when the session ends. It records whether the session completed or was aborted, and when.

A session's status follows from which records exist:

| Status | Records | Meaning |
| --- | --- | --- |
| `open` | The session record, without an outcome record | Upload work is allowed while the clock reads before `expires_at_ms`. |
| `completed` | An outcome record with `completed_at_ms` and `content_ref` | The object was verified, and a commit can admit its reference. |
| `aborted` | An outcome record with `aborted_at_ms` | The upload can't complete or reopen. |

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

A session starts open and ends at most once. Completion and abort both create the outcome record with put-if-absent, so the first create to succeed ends the session, and the other request finds that outcome. Completion verifies the content before it creates the outcome record, and abort creates the outcome record before it cleans up the object or the provider transfer. If a request's put has an unknown result, the request reads the outcome record back, and if the stored record is equal to the record it sent, the request treats the create as its own.

An outcome record counts only while its session record exists. After a request creates an outcome record, it MUST check that the session record still exists, and it MUST NOT return admission evidence or delete content if the session record is gone. Garbage collection deletes the session record before the outcome record (see [Upload-session cleanup](#116-upload-session-cleanup)).

At or after `expires_at_ms`, staging, part signing, and completion refuse an open session, the same way they refuse an aborted one. An expired session can never provide content for a commit, and garbage collection aborts it (see [Upload-session cleanup](#116-upload-session-cleanup)). Status reads and aborts don't check the expiry.

Upload records don't advance the namespace's sequence, and they aren't change-feed events. Completing an upload doesn't automatically update a file in a LoonFS namespace, and a completed upload might never be referenced by any file.

Why does a session have two records instead of one record whose status changes? Some providers allow about one write per second to one object (see the [provider documentation][provider-spec]). With one record, an upload would write it when the session starts and again when the session ends, often less than a second later. With two records, no upload writes the same object twice.

Why does a request check the session record after it creates an outcome record? After garbage collection deletes an ended session's records, the outcome key is free again. A completion or abort could read the session as open, pause while the session ends and garbage collection deletes its records, and then create a new outcome record for a session that no longer exists. If the session had been aborted, the completion would then issue evidence for content that the abort deleted. If the session had completed, the abort would delete content that a commit might reference. Because garbage collection deletes the session record first, the check finds the session record missing in exactly those cases.

### 5.2 Transfer modes

Each session uses one transfer mode, which is set when the session is created and can't be changed:

| Mode | How the bytes are transferred | Stored mode data |
| --- | --- | --- |
| `service_proxied` | The client sends the bytes to the LoonFS server, which writes the content object. | Nothing |
| `direct_put` | The client writes the whole object directly to the provider, with one presigned PUT. | The selected whole-object `checksum_algorithm` |
| `direct_multipart` | The client uploads parts directly to the provider. | `provider_upload_id`, `part_size_bytes`, and `checksum_algorithm` |

For a direct multipart upload, the client tracks part progress. The session stores the part size and checksum algorithm so that resumed work uses the same settings, and it stores provider upload identifiers where they're needed to recover completion or to clean up.

The content reference in a completed outcome record MUST match the session's content ID and owning namespace, and for a direct upload it MUST use the algorithm recorded in the mode. A record with missing required fields is corrupt, and so is an outcome record that contradicts its session record.

### 5.3 Service-proxied staging

With a service-proxied upload, the client sends the bytes to the LoonFS server, which writes the session's content object. This step is called "staging", and it doesn't write a record. The content object itself shows what was uploaded, because no other session uses its content ID, and the provider stores the object's size and full-object checksum.

The server writes the content object with put-if-absent, and a large write completes conditionally (see [Immutable writes and retries](#32-immutable-writes-and-retries)). So if two requests upload to one session at the same time, only one of them creates the object. A request that finds the object already written compares the stored size and checksum with its own bytes. The same bytes are accepted as a retry, and different bytes conflict.

Service-proxied staging computes the store's checksum while it streams the bytes. When the body ends, the request reads the session's records again, and it lets the object store finish the write only if the session is still open. After the write, the request reads the records once more, and it reports the uploaded reference only if the session is still open, because an abort during the write deletes the object.

Completion reads the content object's stored size and checksum with one HEAD request, and it records them in the outcome record's content reference. If the object doesn't exist yet, completion fails, and the session stays open.

Why doesn't staging record the content reference? Recording it would add a write to the session's records during the upload, and the stored object already has the size and checksum that the record would contain. The provider verified that checksum when the object was written, and a direct PUT completion relies on the same stored values.

### 5.4 Direct uploads

A direct upload sends bytes from the client to the provider, using a short-lived capability (a presigned request). The server allocates the content ID and constrains the target key, so clients can't choose an arbitrary object key.

Direct PUT uses the checksum algorithm selected when the session starts. Direct multipart uses the algorithm stored in the session (currently CRC-64/NVME). The provider MUST enforce the signed transfer constraints, and it MUST expose the stored whole-object checksum in the supported algorithm.

At completion, the server compares the provider's stored size and checksum with the client's completion claim (a digest supplied by the client isn't enough on its own). After the completion call, the object at the key is the only evidence, whatever the provider's response said. If the stored object doesn't match the claim, the server rejects the claim and changes nothing. The session stays open, and nothing is deleted. A session that can't complete ends with an explicit abort, or when its lease ends (see [Upload-session cleanup](#116-upload-session-cleanup)). The same rules apply to direct PUT and direct multipart.

Provider-specific checksum headers, completion APIs, and response encodings belong in the provider adapter. A LoonFS checksum has the same canonical representation whether the provider returned it as hexadecimal or base64.

### 5.5 Admission proofs

A commit can reference uploaded content only with "admission evidence", which is proof that the referenced bytes were verified in a completed upload. The evidence is tied to the namespace and the complete content reference, so a match on the content ID alone isn't enough. An inline reference MUST be owned by the committing namespace, and admission evidence MUST match the complete reference.

Admission evidence expires. It's valid while the clock reads before its expiry, and it's expired at the expiry instant. Three windows apply ([Upload admission](#c2-upload-admission) lists the values):

- A completed session can produce evidence only during `COMPLETED_UPLOAD_RECEIPT_WINDOW_MS` after its original completion time. A status read or a cached response doesn't restart that window, and nothing is issued at or after its end.
- A signed token lasts `CONTENT_RECEIPT_TTL_MS` from when it's issued.
- Evidence prepared in process, without a token, expires at the end of the admission window, `COMPLETED_UPLOAD_ADMISSION_WINDOW_MS` after the completion. No token lasts past that window.

The reference HTTP server represents admission evidence as signed content tokens. An embedded implementation doesn't need to issue tokens. The [API specification][api-spec] defines token encoding, signing, and HTTP responses.

Immediately before it publishes newly accepted requests, the writer checks that every externally supplied content reference has matching, unexpired admission evidence. This check includes the time spent acquiring or checking the writer, loading the view, planning, and preparing the WAL object. It uses the request clock plus the monotonic time elapsed since the batch call began, including earlier attempts, so retrying doesn't restart the evidence's validity.

A retained matching commit receipt replays without new admission evidence. Internal copy and restore operations reuse references that the validated namespace state already established, and they don't authorize arbitrary imports from other namespaces. An import from outside the pinned fork relationship writes verified bytes under a new content ID owned by the destination.

Why does admission evidence expire? Garbage collection has to know when it can delete a completed upload that nothing references. If evidence never expired, a commit could reference the upload at any time, and its content could never be deleted safely. The receipt window, the token lifetime, and the publication budget determine the earliest safe deletion time, and [Upload-session cleanup](#116-upload-session-cleanup) and [Appendix C](#appendix-c-timing-and-size-reference) show the exact calculation.

## 6. Publishing a commit

This section describes how a writer session becomes the namespace's writer, validates requests, and publishes them as commits.

A metadata commit becomes visible when put-if-absent creates its numbered WAL object. Upload completion and validation happen before that point, and neither one alone commits a file change.

### 6.1 Writer ownership

A writer session acquires authority lazily, before its first publication that changes metadata. It moves through these states in order:

```text
+-------+  claim published   +-------+  fence written   +----------+
| Claim | -----------------> | Fence | ---------------> | Acquired |
+-------+                    +-------+                  +----------+
                                 |                           |
                                 +-------------+-------------+
                                               | a higher epoch is found
                                               v
                                    returns writer_fenced
```

1. **Claim.** The session loads the current manifest and publishes its successor with `writer_epoch + 1` and a diagnostic writer block.
   - The claim runs under `METADATA_PUBLICATION_BUDGET_MS`, measured from the start of the acquisition (before the session loads the manifest it copies). If the budget has expired before the put starts, or by the time the put is confirmed, the acquisition fails.
   - If another publisher took the claim's number, the session reloads the current manifest and claims again with the next epoch.
   - If the put's outcome is unknown, the session reads back the manifest at its number. If it finds its own bytes, that manifest covers the claim, and the session claims again (the epoch it leaves behind never gets a fence). If it finds another manifest, the claim lost, and the session claims again. If it finds no manifest, the acquisition fails.
2. **Fence.** The session discovers the tip. If the current manifest now has a higher epoch than the claim, another session has claimed since, and the acquisition returns `writer_fenced`. Otherwise, the session creates a fence (a WAL object with no records) with its epoch at the next WAL number.
   - The fence put is planned against a discovered tip, under `WAL_PUBLISH_BUDGET_MS`, like any numbered put.
   - After a collision, or a budget that expired before the put, the session discovers the tip again and retries at the new number. These retries stop when the acquisition's `METADATA_PUBLICATION_BUDGET_MS` expires, and the next retry fails the acquisition instead.
   - A transport failure, or a put that returns after its budget, fails the acquisition. The next attempt claims a new epoch.
   - Between the claim and the fence, the previous writer can still commit at `tip + 1`. Those commits are valid, because their epoch is below the manifest's, and the new writer's discovery includes them (a fence planned before such a commit collides with it and retries above it).
3. **Acquired.** After the fence is written, the session is acquired, and it uses that epoch for every later batch. If a later discovery finds a higher epoch (after a collision or after a refresh), the session is fenced. A fenced session returns `writer_fenced`, and it doesn't reacquire authority on its own.

When another session acquires a higher epoch, its fence prevents an older writer from extending the log from a tip it saw earlier. The stale writer's put collides, discovery finds the higher epoch, and the session returns `writer_fenced`.

A fence keeps `head_seq` and `next_inode_id` unchanged and contains no commit records, so its derived `prior_head_seq` equals its `head_seq`. It moves the WAL position forward without moving the logical history forward. Concurrent attempts are ordered by the conditional creation of the next number.

There's no writer lease or writer-expiry timestamp. The `writer_id` and `acquired_at_ms` fields describe the acquisition, and the epoch alone determines authority. An acquisition retried after an uncertain outcome can raise the epoch again. Commit retries are identified separately, with durable receipts (see [Commit identity and retries](#65-commit-identity-and-retries)).

Why use epochs instead of a lease? With a lease, a new writer would have to wait for the old lease to expire before it could take over. With epochs, a new writer can take over at any time, and correctness depends only on conditional writes. Taking over costs a manifest publication and a fence write (about 200 ms), so most deployments keep one long-lived writer session in a server that many clients share.

### 6.2 Validation

Before it plans new mutations, the writer rebuilds the metadata state from a verified basis and the visible WAL. It resolves paths and inode references, checks content admission, evaluates preconditions, and allocates new inode IDs from `next_inode_id`.

The operations in one request are evaluated in order, and later operations see the tentative effects of earlier ones. When several requests share a publication batch, the writer chooses their order, and it evaluates each one against the requests accepted before it.

The writer MUST check that a content reference has a supported kind, a correctly encoded checksum, and applicable evidence that the object is durable with the stated size and checksum. Content validation happens before metadata preconditions. Content that was established earlier can be reused only through the namespace's validated state or the admission paths in [Admission proofs](#55-admission-proofs).

Metadata preconditions can check:

- the namespace's head sequence,
- whether a name slot is available,
- exact binding versions,
- file and attribute revisions,
- ancestor visibility, and
- whether a directory is empty.

The internal exact-binding check is:

```text
binding_is(parent_inode_id, name_key, child_inode_id, committed_seq, delta_index)
```

Checking the inode ID alone isn't equivalent, because the item might have been moved away and bound under the same name again since the caller looked at it.

Callers add specific checks with `expected_*` preconditions. Leaving out an optional precondition turns off that check, but it doesn't turn off the operation's normal structural validation. When a revision precondition comes with an optional inode precondition, the revision precondition requires the matching inode precondition. Inode-addressed revision writes require `expected_revision_no`, and inode-addressed moves and deletes require `expected_binding_version`.

A rejected request isn't assigned a sequence number and creates no WAL record. Passing validation is only tentative acceptance, not success.

### 6.3 Publication and group commit

To publish a batch, a writer:

1. Resolves retained commit IDs, and then validates new requests against the discovered view and the requests accepted earlier in the batch.
2. Keeps new commits in the writer's queue while the unfolded WAL is at its write-stop threshold (fence objects count toward that threshold), until a fold frees room. A commit that's still waiting when the [write-stop wait](#73-recovery-material-and-maintenance-policy) passes (measured from when it entered the queue) returns `maintenance_required` and isn't committed.
3. Assigns contiguous sequences to the accepted requests, and builds one WAL object at `tip + 1`, including the resulting `next_inode_id`.
4. Checks the publication budget and the content-admission evidence immediately before the put-if-absent. If a request's evidence has expired, the writer returns that request's own error, and `stale_head` for the other accepted requests so they can be planned again. It writes no WAL object for that batch.
5. On success, moves its [writer position](glossary.md#writer-position) to the new head and acknowledges the requests.

Writing several requests in one WAL object is called "group commit". For example, three requests accepted after sequence 40 can be written together as sequences 41, 42, and 43 in WAL object 10. Successfully writing that object commits all three. They're still three separate commits, and a request with several operations is still one commit.

```text
upload and verify content (uploaded path only)
            |
            v
validate requests A, B, C against the current view
            |
            v
put-if-absent WAL 10: [seq 41, seq 42, seq 43]   <- commit boundary
            |
            v
acknowledge
```

A confirmed precondition failure creates no object. The writer discovers the winning publication, checks whether it has been fenced, and plans again before it tries another number. It never creates a parallel branch of history.

Commits don't raise the hint. Manifest publications raise it (see [Publishing a materialized file set](#72-publishing-a-materialized-file-set)), and readers stay correct while it lags, because discovery probes forward.

Every put is planned against an observed state, and that observation has to be recent. A writer tracks two kinds of observation:

| Observation | What it confirms | Recorded by |
| --- | --- | --- |
| Tip observation | The WAL tip that the batch was planned against | Discovery, a successful put (as of the start of the batch that made it), or a confirmation attempt |
| Basis confirmation | That the manifest the writer planned against still has no successor | Discovery, a fold that the writer published and moved its view onto (as of the start of that fold), or a confirmation attempt |

The publication budget (`WAL_PUBLISH_BUDGET_MS`) runs from the tip observation used to plan a batch until the batch's numbered put starts. A publication is within its budget while the elapsed time is at most the budget, and a cached tip has the same time limit. A successful put confirms nothing about the manifest the batch was planned against, because a fold or compaction might have replaced it since.

A writer that keeps publishing checks the next manifest number when its last basis confirmation is more than `WAL_PUBLISH_BUDGET_MS` old. It trusts a "not found" answer only if the answer arrives less than `READ_REVALIDATION_BOUND_MS` after that previous confirmation. Otherwise, or if a successor exists, it reloads. Twice `WAL_PUBLISH_BUDGET_MS` is at most `READ_REVALIDATION_BOUND_MS`, so a writer whose commits are less than a budget apart sends this check while its basis is at most two budgets old, and the check can confirm the basis instead of making the writer reload.

A writer whose cached tip is more than `WAL_PUBLISH_BUDGET_MS` old confirms the tip, or reloads, before it plans. A confirmation attempt sends one HEAD request, for the next manifest number. If the request finds nothing, and the answer arrives less than `READ_REVALIDATION_BOUND_MS` after the previous basis confirmation, the writer keeps its position and the position's tail. The attempt's start (before the HEAD request) becomes the new tip observation and basis confirmation, even if the batch ends up writing nothing, and the put's budget runs from that start. If the request finds a manifest or fails, or the answer arrives late, the writer runs discovery instead. A basis that's already at the revalidation bound when the attempt starts is dropped without a request. [Publication and collection timing](#c1-publication-and-collection-timing) records the bounds.

After a WAL put whose outcome is unknown, or a publication that's cancelled after its put might have been sent, the writer drops its position. The put might have succeeded, so the position might be missing the writer's own commit. The writer's next publication runs discovery, which finds that commit, and a retry with the same commit ID returns the original result from the commit's receipt (see [Commit identity and retries](#65-commit-identity-and-retries)).

Why check for a manifest successor at all? The check stops a successor that garbage collection already deleted from looking absent. Any successor created after the previous confirmation is kept for at least the revalidation bound, under the grace and clock allowance in [Clock and operation assumptions](#114-clock-and-operation-assumptions). Another writer has to publish a claim manifest before it can write the next WAL number, and a compaction by another process also publishes a successor, so the manifest probe finds both, and the writer runs discovery. A manifest this runtime publishes itself (a fold, a compaction, a pin, or a retention advance) moves the session to it, so the probe doesn't find it as a successor.

Why doesn't the writer also check the next WAL number? A WAL put is create-only, so the put itself checks that number. If another writer has already created the next WAL object, the put collides and creates nothing. The writer then drops its position, and the discovery that follows finds the other writer's higher epoch, so the session is fenced (see [Writer ownership](#61-writer-ownership)). Another writer creates a WAL object only after its claim manifest, so if the manifest probe found no claim, the other writer's WAL object was created after the probe. Garbage collection can't delete an object that young before the put runs (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)), so the put collides with it. A HEAD request for the next WAL number would save one failed put in this case, but it would add a round trip before every commit that follows a quiet period longer than the publication budget. The writer's own puts don't need the check either, because the writer drops its position after a put whose outcome is unknown.

### 6.4 Failed and unknown outcomes

A transport error after the WAL object's put was sent isn't necessarily a failed commit, because the put might have succeeded and only its response was lost. An implementation that can't establish the outcome MUST report it as unknown, not as a definite failure.

An implementation can resolve an unknown outcome itself, by retrying with the same commit ID and request. The retry discovers the tip again and looks up the commit receipt, so it returns the original result if the earlier put succeeded. Only a success or a receipt replay establishes the outcome. A retry that ends in any other error doesn't show that the earlier put failed, so the implementation still reports `commit_outcome_unknown`. The reference publisher retries a bounded number of times before it reports.

A caller that receives an unknown outcome retries with the same commit ID and the same logical request.

Warning: a caller that retries a commit shouldn't retry the upload too. If the upload is retried, it's assigned a different content ID, so the retried commit is no longer the same request. If the original commit succeeded, the retry would fail with `commit_id_reuse_conflict` instead of returning the original result.

A publication batch isn't an all-or-nothing transaction across every request in it. Some requests can fail validation while others are accepted. A validation error that depends on another tentative request can be reported that way only if that other request is published.

For example, suppose request A creates `/reports`, and request B also tries to create `/reports`. B might fail because of A's tentative creation. If A's publication then fails, the writer MUST report the publication failure for B too, and not claim that B conflicted with a committed directory. A rejection based entirely on the view that was already durable doesn't have this dependency, and it can stand on its own.

### 6.5 Commit identity and retries

Every WAL commit and every `commit` row stores a semantic fingerprint (`semantic_commit_fingerprint`) of the logical request: the `namespace_id`, the `actor_id`, the optional `subject_id`, the ordered operations with their inline preconditions, the request-level preconditions, and the optional message. It doesn't include publication details, such as the writer epoch and the timestamp. [Semantic commit fingerprints](#appendix-b-semantic-commit-fingerprints) specifies the exact canonical bytes.

The materialized file set also stores each retained commit's record in the `commits` family, keyed by sequence. A commit receipt stores only `commit_id` and `committed_seq`. When a request reuses a commit ID, the writer reads the commit row at the receipt's sequence and compares that row's fingerprint with the request's fingerprint. A replay rebuilds its response from that row. The `commits` and `commit_receipts` families are one family group and follow the same retention rule, so a commit row is kept for as long as its receipt.

While the receipt is kept:

- An equal fingerprint under the same `commit_id` is a replay of the original commit. A replay doesn't run the mutation again, and it doesn't evaluate the original preconditions again against the current state.
- A different fingerprint returns `commit_id_reuse_conflict`.

Inline content is identified by its bytes. While the receipt is kept, retrying the same request with the same inline bytes returns the original commit, even if a new content ID was assigned. Changed bytes, or a different subject, return `commit_id_reuse_conflict`. The bytes an append adds are identified the same way.

Receipt lookup uses the publisher's current projection. The publisher refreshes that projection when a WAL publication budget passes without a successful put and the tip can't be confirmed, or when its basis manifest has a successor (see [Publication and group commit](#63-publication-and-group-commit)). A fold that the publisher published itself moves the projection's basis to the new manifest and keeps the commits published since, without a refresh.

The writer checks for the commit receipt before it uploads inline bytes as content objects. If the receipt is still available, it returns the original result for an identical request, or a reuse conflict for a changed one. Neither case requires another upload, even after a restart or on another server.

The guarantee lasts as long as the receipt. Compaction can remove receipts below the retention floor, and once a receipt is gone, the old ID can't be distinguished from an unused one, so a later request can run as a new mutation. A receipt that hasn't been compacted yet might still be available, but callers MUST NOT depend on that extra lifetime.

Receipt lookup still works at the WAL write-stop threshold. A retained matching receipt returns the original result, and a retained conflicting receipt returns `commit_id_reuse_conflict`. Only new commits wait for a fold, and they return `maintenance_required` if the write-stop wait passes first. The writer-session, availability, and corruption checks still apply.

File revision history and commit idempotency have different retention rules. Keeping an old file revision doesn't require keeping its commit receipt.

### 6.6 Operations and WAL deltas

Standard requests operate on paths or inode IDs. They create directories, write files, append to files, move or copy items, delete and undelete items, restore file revisions, and update attributes. [Appendix B](#appendix-b-semantic-commit-fingerprints) lists their exact parameters, because those parameters also determine retry identity.

| Setting | Default |
| --- | --- |
| Destination behavior for puts, moves, and copies | `no_replace` |
| Deletes | `non_recursive` |
| Directory creation | `parents: false` |
| Attribute `set` and `remove` collections | Empty |

Optional preconditions have no implied value.

The namespace's writer also commits an operation of its own, an expiry, which ends the recoverability of deletions (see [File and subtree deletion](#16-file-and-subtree-deletion)). `POST /commits` doesn't accept an expiry, and no other client route does either. An expiry commit is attributed to the actor `loonfs`, it isn't checked against a subject, and it has no message or preconditions. It records one `expire_subtree_tombstone` delta for each deletion it expires, and each deletion is its own operation, so the change feed reports one event for each.

A replacing move deletes the destination file and binds the source in its place, within the same commit. Only a file destination can be replaced, and moving a path onto itself isn't a replacement. An undelete can use the deleted binding's original parent and name, the caller's replacement path, or the caller's parent inode and name, subject to normal validation.

An append adds bytes as the file's next revision, and its commit includes the new bytes as pieces. The writer computes the new checksum by combining the base reference's checksum with the appended bytes' checksum, in the store's algorithm, without reading the earlier bytes. A base in another algorithm is `namespace_corrupt`, before anything is written.

An append either extends the same chain or starts a new one:

- **Same chain.** The appended piece keeps the same content ID only when the namespace owns the chain, the base size is positive, and the base is the chain's head (its full current length). The visible tail and the longest layout determine that length, so a copy or restore of an older prefix can't shorten the chain's head. A same-chain append includes no layout and writes no content object.
- **New chain.** Otherwise, the piece uses a new content ID. Its delta includes the base chain's layout, cut at the first piece's offset in the commit (the cut shortens the last extent and drops later ones). Before it admits a new chain from bytes that haven't been folded yet, validation writes the base chain's pending pieces as its next extent, the same way a fold does. The new chain shares those extents and includes only the appended bytes as pieces. Missing base bytes are corruption, and the error identifies the base. An append to an empty file starts a new chain with a piece at offset 0 and no layout.

For example, a fork inherits `/log.txt`, whose chain is owned by the source namespace. The fork doesn't own that chain, so appending 10 bytes starts a new chain, whose layout lists the source's objects followed by a piece with the 10 new bytes. Earlier operations in the same commit count when later appends are planned.

The WAL stores the resulting metadata changes ("deltas"), not the original request bodies or validation inputs. There are nine kinds of delta:

| Delta | Records |
| --- | --- |
| `create_inode` | A new inode. |
| `bind_direntry` | A new directory binding. |
| `unbind_direntry` | A removed directory binding. |
| `append_file_revision` | A new file revision. |
| `tombstone_subtree` | A deletion (a tombstone `set` event). |
| `revoke_subtree_tombstone` | An undelete (a tombstone `revoke` event). |
| `expire_subtree_tombstone` | An expiry (a tombstone `expire` event). |
| `append_attributes_revision` | A new attribute revision. |
| `append_access_revision` | A new access revision. |

Each delta has a `delta_index`, and its wrapper has a `semantic_operation_index` that identifies the internal operation that produced it. A convenience request can expand into several internal operations (for example, creating missing parent directories), and each operation's deltas are contiguous within its commit. The actor and timestamp are recorded once per commit and copied into the rows during materialization. [Appendix A](#appendix-a-durable-records-and-byte-encodings) lists the complete stored fields.

### 6.7 Change feed

The change feed is ordered by commit, not by WAL object. A WAL object that contains three commits contributes three commit boundaries to the feed. Within a commit, filesystem events follow the order of the request's operations, and one operation can produce several events.

A consumer resuming after sequence `N` reads the `commits` family of the current file set, and then the commits in the unfolded WAL after it. Fence objects produce no change events. If the consumer's cursor is older than the retention floor, it MUST start again from a fresh pin. The [API specification][api-spec] specifies the event shapes and the cursor contract.

A consumer that needs permanent event history MUST keep its own copy before the floor advances. The stable keys are `(namespace_id, committed_seq)` for a commit's position and `(namespace_id, inode_id)` for item identity. A commit ID is useful for correlating events, but it isn't a permanent unique event key, because it can be reused after its receipt is removed.

## 7. Materializing metadata

This section describes how committed metadata moves from the WAL into sorted segments, and how a new manifest is published.

Replaying a longer WAL takes more object reads and more work. To keep reads fast, a "fold" materializes committed metadata into immutable sorted segments and publishes a manifest that lists them. Later reads start from that manifest and replay only the WAL objects written after it.

A fold changes the physical representation, not the namespace's visible history. It doesn't allocate a sequence number, and it doesn't create a pin.

### 7.1 Manifests, runs, and segments

A namespace manifest describes one complete set of metadata files through `head_seq`. It includes the inode allocator, the folded WAL number, the retention floor, and every metadata run needed to rebuild that state. Its `manifest_no` determines its immutable key, so only one publication can succeed at each number.

A "run" is the set of segments produced together. Its `run_no` is allocated from the manifest's `next_run_no`, which advances when the run is published. A WAL fold allocates one run number across all the families it writes, and a compaction allocates one run number for its selected family group.

Each run records `run_seq`, `tier`, and its segment descriptors. The manifest's `base_seq` is derived as the smallest `run_seq` among runs with segments, or `head_seq` when no run has segments. A non-empty manifest MUST have a non-empty run at `head_seq`, and no run's sequence can be higher than `head_seq`. An empty manifest describes a new, empty namespace. Within one run, each family's segments follow list order and have strictly separated, ascending key ranges, and every segment contains at least one row. Different runs can overlap, because a later run can contain more rows for the same inode, name slot, or revision history.

A manifest MUST NOT contain duplicate run numbers, or a run number at or above `next_run_no`. A family's segment ranges MUST NOT overlap or descend, and a metadata producer MUST NOT write the same logical row key twice within one run.

Metadata rows describe immutable facts at specific positions. Reads merge those facts and apply the visibility rules, instead of choosing between conflicting values for one row key. The two directory binding indexes (by parent and name, and by child) contain the same binding versions in different orders. Manifest validation checks their per-run row counts, and compaction checks full row-by-row equality across its selected input runs. The `commits` and `commit_receipts` families contain one row each per retained commit, and manifest validation checks their per-run row counts the same way.

### 7.2 Publishing a materialized file set

The first manifest of a namespace has these values:

| Field | Plain create | Fork |
| --- | --- | --- |
| Manifest number | 1 | 1 |
| Namespace ID, creation time, `created_by` | From the request | From the request |
| Access mode and naming mode | From the request | From the source (see [Forking a namespace](#92-forking-a-namespace)) |
| Status | Active | Active |
| Writer block | None | None |
| Writer epoch, compactor epoch, folded WAL number, activity counters | 0 | 0 |
| Fork basis | None | The pinned source manifest and the source pin's ID |
| Runs | None | The source's runs |
| Next inode ID | 2 | The source's |
| Head sequence and retention floor | 0 | The pinned source sequence |
| Base sequence | 0 | Derived from the inherited runs |
| Next run number | 0 | The source's |

A fold works in these steps:

1. Start from a verified manifest and a discovered WAL tip. (A fold call discovers both, and a writer's own fold takes them from its retained view.)
2. Check every chain's pieces in the tail (see below).
3. Write each chain's new extent to a content object (see below).
4. Write new metadata segments for the WAL objects after `folded_wal_no`, including the new layout rows. A fence is folded even when the sequence doesn't change, and an ordinary fold writes a run at the head when it materializes new state.
5. Publish the next manifest, with `folded_wal_no` set to the tip it captured, using put-if-absent at the predecessor's `manifest_no + 1`.
6. Raise the hint.

A fold is a publication by the namespace's "maintainer", the role that folds, compacts, advances the retention floor, and collects garbage, while the writer session commits and deletes the namespace. A fold publishes under the current compactor epoch without claiming it, and it copies the predecessor's writer and compactor epochs forward, so it doesn't need a writer epoch, and any process can run one. One `METADATA_PUBLICATION_BUDGET_MS` deadline covers the whole call, including rebuilds.

A fold loads the current manifest before its put, and again if its put loses, and then it checks whether its work is already covered. The fold is covered if the current manifest's `folded_wal_no` is at least the fold's captured tip, and the current manifest's head sequence and manifest number are at least the fold's own. A covered fold is finished (coverage includes the WAL position, not just the sequence). Otherwise, the fold rebuilds against the new predecessor, and it uses that predecessor's epochs. The rebuild can keep the WAL tail the fold already read when the new predecessor is different from the fold's own only in its number, compactor epoch, retention floor, runs, and next run number, and it has runs exactly when the fold's own predecessor has runs. A compactor claim, a compaction, and a retention advance publish that kind of manifest. After any other manifest, the rebuild reads the tail again, above the new `folded_wal_no`. (A compaction that loses its put also requires its selected inputs to still be valid.)

A fold that rebuilds after another writer's claim folds the new writer's log. The old writer session's [writer position](glossary.md#writer-position) doesn't move onto that fold's manifest, as described in [Replaying the visible WAL](#42-replaying-the-visible-wal), and the old session finds that it's fenced at its next commit.

Before it writes segments or publishes the manifest, a fold checks each chain's ordered tail pieces, up to the chain's newest reference. Missing pieces are corruption. The fold checks every whole value that starts at offset 0 before it writes any object.

For each chain, the fold builds a "candidate" (the new extent it will write) and resolves the layout before the pieces:

1. The candidate starts as the ordered pieces, without joining their bytes. Its chain offset is the total length of the extents before it.
2. Validation might already have written some bytes to content objects while they're still in the WAL tail. The fold keeps such an own extent whole and leaves out the pieces it covers. If it covers all the remaining pieces, the last own extent starts the candidate.
3. While the chain's last own extent is at most twice the candidate's length, the fold merges that extent into the candidate (the "doubling rule"). Shared base extents are never merged.
4. If the result would leave more than `MAX_LAYOUT_EXTENTS` (32) own extents, the fold merges every own extent after the chain's largest own extent into the candidate as well. Shared base extents stay separate, and they don't count toward this limit.

For example, suppose a chain's layout has one own extent of 100 bytes, and the tail adds 30 bytes. The last extent (100 bytes) is more than twice the candidate (30 bytes), so the fold writes the 30 bytes as a new extent, and the layout has two extents. If the next fold has 70 new bytes, the last extent (30 bytes) is at most twice 70, so it's merged, and the candidate grows to 100 bytes. The next extent (100 bytes) is now at most twice the candidate, so it's merged too, and the fold writes one 200-byte extent.

The fold writes each chain's candidate with one assembly call. Each source of the assembly is a whole own extent (a chain's own extents are never cut), and one HEAD request per source supplies its stored checksum. A missing stored checksum, or one in another algorithm, is `namespace_corrupt`. The fold combines the source checksums and the tail checksum in the store's algorithm, and the tail bytes stay in ordered pieces. A candidate with no merged extents has no sources, at every size. A new chain shares the extents its delta included, and it writes only its own pieces.

A chain whose pieces start at offset 0 writes the whole object at its own key, when the tail has no shorter reference to it. Every other write is a span object, whose key uses the candidate's start and the newest reference's size (including a span that starts at 0). A failed fold can leave the first whole object behind, so later bytes MUST use a different key. Every chain extent is written with an assembly. If the key is already occupied with the same stored checksum, the write succeeds, and if the checksum is different or missing, the fold reports corruption, identifying the key. A fold never changes an existing object's bytes.

The resulting layout is the remaining extents, followed by the new extent. Its `committed_seq` is the newest commit whose bytes it covers. When segments are built, the layout's row replaces any tail layout row for the same content ID, and it's included in the fold's delta run. Revision digests are copied from the deltas, and a fold never recomputes them. A manifest at `folded_wal_no` `n` includes layouts for all pieces through WAL object `n`, and garbage collection relies on that before it deletes a WAL object.

Folds, validation (when it writes pending pieces to content objects), direct-download writes, and proxied staging all share the execution budget's content-write permits. `max_concurrent_content_writes` defaults to 32 and is configurable. Each write uses one permit until it finishes, and runtimes that share a budget share the limit. `STORE_WRITES_IN_FLIGHT` also limits how many writes one fold starts at a time. The adapter chooses how to write by the result's size (see [Immutable writes and retries](#32-immutable-writes-and-retries)), and an assembly buffers at most one part window (or one read chunk on the local store).

Every successor manifest MUST:

- keep the namespace's identity, including its access and naming modes,
- not lower the head sequence, the writer or compactor epoch, the folded WAL number, the retention floor, the allocators, or the cumulative activity counters, and
- keep the activity counters exactly the same, if it's at the same head.

A namespace tombstone has no successor.

The bounded metadata publication budget runs from the start of the publication, before any output segment is written, until the manifest put starts. An attempt whose budget expired publishes nothing more, and its unreferenced output is deleted under the segment-age rules in [Candidate and age rules](#113-candidate-and-age-rules). Compaction has a longer bound (see [Streaming compaction](#104-streaming-compaction)).

After the publication, the fold raises the hint within the publication budget. If raising the hint fails, the manifest is still published. Garbage collection keeps every manifest number at or above the hint value it observed for its pass, so discovery can probe forward from a hint that lags. The intermediate manifests kept for discovery don't keep their own runs. A manifest whose successor is younger than the grace does keep its runs, as a root (see [Reference roots](#112-reference-roots)).

Forks follow the same path from manifest 1. A manifest owned by the fork doesn't mean the inherited segments were copied into the fork's prefix, and each segment keeps its owner.

Why does the doubling rule exist? Without it, every fold of a growing chain would either add one more extent (so a log file with many appends would end up with thousands of objects to read) or copy the whole chain again (so each append would cost a copy of everything before it). The doubling rule keeps each extent at least twice as large as the one after it, so a chain has a small number of extents, and each byte is copied only about once for each doubling of the chain's size. The 32-extent limit covers the case where many small appends would otherwise build up more extents than that.

### 7.3 Recovery material and maintenance policy

Once the WAL objects they came from are deleted, metadata segments are required recovery material, not caches. A missing or corrupt required manifest or segment is an error. Readers don't choose a different file set and return another state without saying so.

Implementations can fold automatically as the WAL grows. The reference implementation uses these defaults (these are implementation defaults, not format requirements):

| Trigger | Reference default |
| --- | --- |
| Request a fold | 32 unfolded WAL objects |
| Make new commits wait for a fold | 128 unfolded WAL objects |
| Refuse a commit still waiting for a fold with `maintenance_required` (the write-stop wait) | 30 s after the commit entered the queue |
| Fold an idle tail | The tail contains a commit, and the newest commit's `committed_at_ms` is at least 15 minutes older than the maintenance clock |

The commit that triggers a fold can finish before the fold does. Reads and lookups of retained receipts still work at the write-stop threshold. Idleness is measured from the newest commit's timestamp, so a tail that contains only fences is never idle. The timestamp determines when to fold, but a fold is valid at any time.

Folding doesn't advance retention. An operator decides separately when older replay history can be discarded. [Appendix C](#appendix-c-timing-and-size-reference) lists the reference implementation's sizing defaults.

### 7.4 Statistics

Each manifest stores three cumulative activity counters in a required `activity` object:

| Counter | What counts |
| --- | --- |
| `content_bytes` | The full content length of every committed file revision, including overwrites and revisions that reuse stored content. A revision whose new bytes are pieces in its commit counts only those bytes, once, so an append counts the bytes it appends. |
| `file_revisions` | Every committed file revision, including an empty one. |
| `mutations` | Each semantic operation group in a committed WAL record, identified by `semantic_operation_index`. |

For example, writing a 10-byte file and then replacing it with a 6-byte revision adds 16 bytes, two revisions, and two mutations. Creating a directory adds one mutation. Recursively deleting that directory adds one mutation, however many descendants it hides. Convenience requests can contain several internal operations, so the mutation total can be higher than the number of requests.

A fold adds only the activity after its predecessor's covered position, and it publishes the counters, runs, head sequence, and folded WAL number together. If another publication wins, the fold reloads the predecessor and counts only the remaining tail. A recognized commit retry adds nothing. If an expired receipt allows a new durable commit, that commit counts as new activity.

Uploads on their own, fences, pins, writing inline content to content objects, and compaction add no activity. Retention and deletion don't subtract past activity. Every publisher copies the counters forward from the predecessor it actually updates, including when a compaction races a newer fold.

Two footprint values are calculated from the manifest's segment descriptors:

| Value | Calculation |
| --- | --- |
| `inode_record_count` | The sum of `row_count` for `inodes` segments. |
| `metadata_stored_bytes` | The sum of `index_block.offset + index_block.stored_bytes` for every referenced segment. |

The inode count includes retained deleted records, and an implicit root counts as zero until it's stored as an inode record. Metadata bytes don't include content, WAL objects, manifests, pins, unreferenced outputs, or segments that only other manifests reference. A shared segment counts in every manifest that references it, so these totals don't measure unique physical storage.

A statistics read uses one validated manifest, without reading segments or replaying newer WAL objects. An observation includes the namespace's identity, lifecycle status, manifest number, head sequence, and folded WAL number. Pin statistics read the pinned manifest. Compaction can change the footprint without changing the head, which is why the manifest number is included too.

An observation also includes `manifest_bytes`, the stored length of the manifest object as read (the length of the compressed document, so it counts stored bytes like `metadata_stored_bytes` does), and `segments_by_family`, which counts each family's segment descriptors across every run (every family is listed, with zero for a family that has none). Both values come from the same validated manifest, so they don't add any requests.

A fork inherits the source manifest's segment descriptors, so its footprint values start out the same as the source's, but its counters start at zero. Activity committed in the source stays in the source's manifests.

To meter usage, an application compares two observations of the same namespace, and it keeps its own billing cursors and policy. The counters are public integers, within the bound in the API specification. Exceeding that bound is an error, and a counter that decreases is corruption.

Grep reports its own referenced segment bytes (see [Appendix D](#appendix-d-grep-extension-format)), and its manifest and indexing position are separate from the core observation. A new fork starts without an index, and a failed index read MUST NOT be reported as zero bytes.

## 8. Pins

A "pin" is a durable record that keeps one manifest, and the segments it lists, after newer manifests are published. Without a pin, garbage collection deletes a superseded manifest once its successor is old enough. Ordinary folds don't create pins. Callers create pins when they need a stable view of the namespace (the API calls these pins snapshots), and forks create pins on their source.

### 8.1 Records and owners

Each pin is stored at `pins/{pin_id}.json`. Its ID includes the pinned manifest's number and a new random suffix, so creating a pin twice on the same manifest, or with the same label, creates two separate records.

| Owner | Stored owner fields | Reads | Removal |
| --- | --- | --- | --- |
| `user` | `name`, optional `expires_at_ms` | Readable before its expiry time. A pin without an expiry stays readable while the record exists. | Explicit deletion, or garbage collection after expiry plus the grace. Without an expiry, only explicit deletion removes it from an active namespace. |
| `fork` | `target_namespace_id` | Not read through the pin. It keeps the runs that the target's manifest lists. | Garbage collection, after the target stops depending on it (see [Pin cleanup](#117-pin-cleanup)). |

The record also stores the namespace, head sequence, payload checksum, and creation time. The manifest number comes from the pin ID. A pin has no lifecycle status. Creating the record creates the pin, and deleting the record ends it. Fork pins have no expiry and no renewal protocol.

Expiry, read eligibility, eligibility for garbage collection, and removal are separate. An expired user pin can't be read, but the record still exists. It appears in the complete pin listing, and it keeps its manifest and segments until garbage collection deletes it. [Pin cleanup](#117-pin-cleanup) defines when each kind of pin can be collected.

### 8.2 Creating and verifying a pin

To create a pin from the current head, LoonFS:

1. Folds the observed WAL tail as the namespace's [maintainer](glossary.md#maintainer), under the current compactor epoch, and selects a verified current manifest.
2. Writes a new pin with put-if-absent.
3. Loads the current manifest again.
4. Checks that the manifest number and payload checksum are the same, that the namespace is active, and that its retention floor is no later than the pinned head sequence.

If another manifest became current in the meantime, LoonFS deletes the candidate pin and retries from a fresh basis. A deletion, a store failure, or running out of contention retries prevents acknowledgement. The creator deletes a pin whose write or verification fails. That deletion is best effort. If it fails, the creator still returns the error that failed the creation, and garbage collection applies the rules in [Pin cleanup](#117-pin-cleanup) to the pin. Verification has no time limit of its own. The number and checksum checks apply even when the new manifest has the same sequence.

A put whose outcome is unknown can succeed after its creator stopped waiting (see [Required operations](#31-required-operations)), even after the creator's delete. A pin created that way was never returned to a caller. It appears in the pin listing and is collected like any other pin, so a user pin without an expiry stays until someone deletes it.

Once a pin is acknowledged, it keeps its manifest's files, even if the retention floor later moves past its sequence.

Why write the pin before checking the manifest again? A garbage collection pass that runs at the same time either captured the pinned manifest as current, or captured a successor after the pin was written, and in that case the pass's complete pin listing includes the pin. Either way, the pinned manifest is protected.

A fork of a user pin is protected by the user pin instead. It doesn't require the user pin's historical manifest to still be current. The fork:

1. Checks that the source namespace is active, and that the user pin exists and hasn't expired.
2. Writes a fork pin on the user pin's manifest.
3. Checks the same two conditions again.

Both expiry checks use the request clock plus the monotonic time elapsed since the fork call began. A user pin that no longer exists or has expired returns `snapshot_not_found`, and a deleted source returns `namespace_deleted`. If the fork pin write or the second check fails, the fork deletes the new fork pin, under the same best-effort rule. Verification has no time limit of its own. Pins are never recreated, so if the user pin still exists after the fork pin is written, it existed the whole time. So every garbage collection pass that listed pins before that write also listed the user pin, and it protected the same manifest.

### 8.3 Reads, deletion, and expiry

An operation that reads, forks, or extends through a pin ID serves user pins only. An ID with no pin, an ID of a fork pin, and an ID of an expired user pin all return `snapshot_not_found`.

A read through a user pin takes the manifest number from the ID, confirms that the pin exists, is a user pin, and hasn't expired, and verifies its manifest reference. The read uses that manifest directly, without replaying later namespace history. The expiry check uses the reader's own clock.

Explicit deletion looks up the pin and deletes a user pin. Deleting an ID with no pin succeeds without a write, so a retried deletion returns the same result as the first one. Fork pins can't be deleted explicitly, and their IDs return `snapshot_not_found`. [Reference roots](#112-reference-roots) defines which pins protect manifests from garbage collection, and [Retirement](#95-retirement) defines when a deleted namespace can be retired.

### 8.4 Extending a pin

An unexpired user pin's expiry time can be moved later by compare-and-swap. The manifest reference, the identity, and the owner don't change. An extension never moves the expiry earlier, so extending a user pin without an expiry leaves it unchanged. An extension can't recreate a deleted pin, or make an expired pin readable again.

Each extension attempt checks the expiry after it loads the pin, using the request clock plus the monotonic time elapsed across the whole extension call. If the pin expires while a compare-and-swap that already started is in progress, a successful extension still stands.

## 9. Namespace lifecycle and forks

This section describes how namespaces are created, forked, deleted, and retired. Creating and deleting a namespace both publish numbered manifests.

### 9.1 Creating a namespace

To create a namespace, LoonFS:

1. Reads the existing namespace state before it writes any objects. An existing active namespace returns `namespace_exists` (or its current summary, with `allow_existing`). Corruption and read errors aren't absence. These checks write nothing.
2. Builds the first manifest for an absent namespace (see [Publishing a materialized file set](#72-publishing-a-materialized-file-set)).
3. Writes the hint, pointing to manifest 1, and then manifest 1, both with put-if-absent. A hint collision is allowed.

If two processes create the same namespace at the same time, the one whose manifest 1 put succeeds creates the namespace. A hint written before that put doesn't mean the namespace exists, because its writer might have failed before it wrote manifest 1.

### 9.2 Forking a namespace

A fork starts its own history from the source's retained metadata. To create one, LoonFS:

1. Loads the target's current manifest before it writes a source pin, and continues only if the target doesn't exist. It then creates a verified source pin whose owner is the target namespace, either from the source's head or from an unexpired user pin (see [Creating and verifying a pin](#82-creating-and-verifying-a-pin)).
2. Loads and verifies the pinned manifest.
3. Copies the pinned manifest's run references, head sequence, inode allocator, next run number, access configuration, and naming mode. The inherited runs determine the same base sequence. Every segment keeps its owner. The fork basis is set to the pinned manifest reference and the source pin's ID.
4. Builds the rest of the first manifest's fields (see [Publishing a materialized file set](#72-publishing-a-materialized-file-set)) and installs it (see [Creating a namespace](#91-creating-a-namespace)).

The fork copies no file bytes or metadata segments. Its first commit is one sequence above its initial head. The fork can itself be forked right away, because its manifest already lists its inherited runs.

The source pin's creation grace protects the installation. Garbage collection keeps a fork pin for at least the ordinary grace `T` of the pass (see [Candidate and age rules](#113-candidate-and-age-rules)) after its `created_at_ms`, which the fork call takes from its request clock (see [Pin cleanup](#117-pin-cleanup)). The metadata publication budget (see [Publishing a materialized file set](#72-publishing-a-materialized-file-set)) starts at the beginning of the fork call, before target discovery and source pin creation, and the rest of the grace covers provider operations and the clock allowance.

### 9.3 Conflicting and unknown installations

A losing manifest 1 put goes back to the existing-state checks in [Creating a namespace](#91-creating-a-namespace). Deleted status follows the lifetime rule in [Namespaces and identity](#11-namespaces-and-identity). Invalid bytes, or a key that doesn't match its payload, are corruption. A loser never overwrites the winner.

A confirmed precondition failure is a conflict. After a put with an unknown transport outcome, the publisher reads the manifest at that number:

- Another manifest there is a conflict.
- Identical bytes confirm the attempt's own success only when the manifest refers to an object that's unique to that attempt. A fork's first manifest refers to its source pin, and a manifest that lists a segment its predecessor doesn't list refers to a segment that this attempt wrote.
- Every other manifest (a plain create, a compactor claim, a writer acquisition, or a namespace tombstone) can be rebuilt byte for byte by another publisher. Identical bytes count as the current manifest, and the attempt retries from it.
- Identical bytes count only if the read-back returns within the publication budget (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)). Identical bytes read back after the budget, or a number that's still missing, end the call with an unknown outcome, and a later call starts from the current state.

Why don't identical bytes always prove success? Two publishers can build exactly the same manifest from the same predecessor (for example, two plain creates with the same request), so finding identical bytes doesn't show which one wrote them. Only a manifest that refers to something this attempt created, such as a new pin or a new segment, proves that the attempt succeeded.

When a fork installer gets `namespace_exists` or `namespace_deleted`, it reloads the target and deletes its source pin, unless the target's fork basis refers to that pin. It returns the original error in every case. If the reload or the deletion fails, the pin stays, and garbage collection applies the rules in [Pin cleanup](#117-pin-cleanup) to it. A matching pin ID with a different manifest reference is corruption. An unused fork pin is collected after its creation grace.

### 9.4 Deleting a namespace

Deletion uses the acquired writer epoch. After the admitted commits finish, LoonFS folds the remaining WAL as the namespace's [maintainer](glossary.md#maintainer), under the current compactor epoch, and then it publishes the next manifest with a deleted status under the writer epoch. That manifest records `deleted_at_ms` from the deletion call's clock, and deletion starts the tombstone publication within `METADATA_PUBLICATION_BUDGET_MS` of capturing that clock. The tombstone's runs, counters, and folded WAL boundary cover the final head, so every WAL object of the deleted namespace is at or below `folded_wal_no`. If the fold fails, the namespace stays active, and the deletion can be retried. Data that was already committed stays committed.

An ordinary operation that finds the namespace deleted returns `namespace_deleted`. A cached reader can keep using its active view until its next manifest revalidation is due. Deleting a namespace doesn't immediately remove its content. [Reference roots](#112-reference-roots) defines what garbage collection keeps, and a separate purge operation is outside this specification.

### 9.5 Retirement

"Retirement" is the point at which a deleted namespace's own content can be deleted. No retirement deadline is stored, and garbage collection publishes no manifest. For a namespace tombstone `T`, the deadline is derived as:

```text
deadline(T) = T.deleted_at_ms
              + max(configured_grace, NAMESPACE_RETIREMENT_GRACE_WINDOW_MS)
```

A tombstone is eligible for retirement when the pass clock reaches its deadline and the complete pin listing contains no pin on the namespace. Pins deleted later in the same pass still count, and any unrecognized key under the pin prefix also blocks reclamation.

An eligible tombstone's own content and source pins are released (see [Sweeping a retired owner's content](#118-sweeping-a-retired-owners-content)). Content owned by other namespaces isn't part of that sweep.

### 9.6 Fork dependencies after deletion

A source pin is required until its target is retired. While the target's current manifest (active or deleted) refers to the pin, garbage collection in the source keeps it (see [Pin cleanup](#117-pin-cleanup)). Retiring the target deletes the pin (see [Sweeping a retired owner's content](#118-sweeping-a-retired-owners-content)), even though the target's tombstone is still its current manifest and still refers to the pin. Rewriting a target's metadata doesn't transfer ownership of inherited file bytes.

For example, suppose B is a fork of A, and C is a fork of B. C's fork pin on B prevents B from being retired, and B's pin on A stays until B is retired. Retiring C deletes C's pin on B. B can then be retired once its deadline and pin checks pass, which releases its pin on A.

Pins are deleted from descendants to ancestors. If deleting a source pin fails, the target's tombstone is still available for another pass.

### 9.7 Cross-namespace copies and moves

A rename that keeps the inode stays within one namespace. Across namespaces, a move is a copy to the destination followed by a deletion in the source, without an atomic transaction across both histories.

A fork can keep using references through its source pin. Every other import writes verified bytes under a new content ID owned by the destination. (Reusing another owner's content ID would require an extra durable retention protocol on the source side.)

An import checks the owner's current view for authorization, including an import within the same namespace. The import reads the requested reference where the owner's current view finds its bytes (see [Content verification](#45-content-verification)). Pieces in that view's projected tail are used only when the reference's owner is the namespace the view reads, and every other byte comes from a content object. Reclaimed content returns the same error as any missing content object. The import verifies the bytes and stages them under a new content ID owned by the destination. Forks pin manifests, so inherited content has always been written to content objects. Inherited references keep their owner.

A subject that imports a bare reference MUST be an administrator of the reference's owner namespace. An unrestricted owner, and a request with no subject, don't need an administrator grant. Authorization always uses the owner's current head, and a deleted owner uses the access state in its final manifest.

## 10. Retention and compaction

This section describes how much history a namespace keeps, and how compaction rewrites metadata without changing what's visible.

Every commit adds metadata rows, and without a limit, old revisions, bindings, and receipts would build up indefinitely. The "retention floor" sets how far back history is guaranteed, and "compaction" merges metadata runs and removes the rows that the floor no longer requires. Neither operation publishes new filesystem changes, and compaction preserves every retained view.

### 10.1 Advancing the retention floor

The retention floor limits how far back these kinds of history are kept: incremental replay, superseded bindings, file revisions, old attribute states, and commit receipts. Above the floor, every file's history is complete. At or below it, each file that's visible or under a recoverable deletion keeps its newest revision, and a file that an expired deletion hides keeps none (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)). The floor doesn't expire layout rows, and it doesn't keep WAL objects (garbage collection removes them once they're folded).

The floor only moves when it's explicitly advanced, and automatic folds don't advance it. Its initial value is 0 for a new namespace, and the fork point for a fork. The change feed returns only commits above the floor, so the commit row at the floor is kept, but it's never replayed. That's also why a fork's floor is its fork point, so that the source's commit at that sequence stays out of the fork's change feed.

A floor advance has one of three targets:

- the current manifest's `head_seq`,
- a sequence (a sequence above `head_seq` targets `head_seq`), or
- a cutoff time.

For a cutoff time, the advance reads the `commits` family upward from the current floor, and it stops at the first commit whose `committed_at_ms` is later than the cutoff. The target is the last commit the scan passed, or the current floor if it passed none. Commit timestamps are informational and don't have to increase with sequence, so the scan stops at the first later commit instead of skipping it. For example, suppose the floor is 10, commits 11, 12, and 13 were committed at 9:00, 9:05, and 9:02, and the cutoff is 9:03. The scan passes 11, stops at 12, and the target is 11, even though 13 is earlier than the cutoff.

Whatever the target, an advance MUST NOT move the floor past the newest commit at or below the target that's at least as old as the default ordinary grace (`GC_DEFAULT_GRACE_WINDOW_MS`), which is one hour in the reference implementation (see [Publication and collection timing](#c1-publication-and-collection-timing)). If no commit above the current floor is that old, the floor stays where it is. A commit's age is the advancing host's clock minus the commit's `committed_at_ms`, measured the same way as an object's age in [Clock and operation assumptions](#114-clock-and-operation-assumptions), so a commit dated later than that clock has an age of zero.

To find the new floor, the advance reads commits downward from the target, one sequence at a time, and it stops at the first commit that's at least as old as the default grace, or at the current floor. The floor passes every commit below that commit, whatever their timestamps. For example, with the same commits and the head at 13, an advance at 10:01 that targets the head reads commit 13 (59 minutes old), commit 12 (56 minutes old) and commit 11 (61 minutes old), so the new floor is 11. At 10:04, commit 13 is 62 minutes old, so the new floor is 13, and the floor passes commit 12 even though it's only 59 minutes old.

The advance loads the current manifest. If the new floor is at or below the manifest's floor, the advance publishes nothing. Otherwise, the advance checks that the manifest's referenced segments exist, and it publishes a successor with the same runs, head summary, allocators, and authority, with `retention_floor_seq` set to the new floor. In both cases the advance returns the resulting floor, which is below the target when the commit at the target is younger than the default grace. The floor never decreases.

A pin below the new floor still protects its own manifest and runs.

The floor also bounds how long a deletion stays recoverable. Once the floor is at or above a deletion's sequence, the namespace's writer expires the deletion by committing an `expire` event for it (see [File and subtree deletion](#16-file-and-subtree-deletion)). The advance doesn't change any deletion itself, so a deletion at or below the floor stays recoverable until its `expire` is committed. A [writer session](glossary.md#writer-session) keeps the last floor whose deletions it has finished expiring, which is 0 in a new session. Whenever the session's [writer position](glossary.md#writer-position) is on a manifest with a higher floor than the last one it finished, the session repeats these steps until no `listed` row at or below the new floor remains:

1. Scans its view of `active_deletions` for the next `listed` rows at or below the new floor, at most 1,000 of them. The first scan starts above the last finished floor, and each later scan starts after the last deletion that the step before it expired.
2. Commits an `expire` event for each deletion that the scan found, in one commit that's attributed to the actor `loonfs`. The session MUST NOT commit anything else between a scan and this commit.

Between one `expire` commit and the next scan, the session can commit other requests. For example, if 2,500 deletions are at or below a new floor, the writer expires them in three commits of 1,000, 1,000, and 500 deletions, and it can commit requests in between. (The 1,000 is an implementation limit, `MAX_EXPIRED_DELETIONS_PER_COMMIT`, not a format requirement.) In a fork, a new writer session expires every deletion at or below the fork point before it commits its first request, so an undelete of an inherited deletion returns `not_deleted` even as the fork's first request.

Validation MUST reject an `expire` whose deletion is above the floor of the manifest that the commit is validated against. The rejected commit fails with a server error (`server_error`), and none of its events are committed. A fork's inherited deletions pass this check, because a fork's floor starts at its fork point and never decreases.

Why does the floor stop at a commit that's at least as old as the default grace? A reader that opened a file revision can keep reading that revision after a newer revision of the file is committed. Garbage collection keeps the replaced revision's content for at least the ordinary grace `T` after a compaction publishes a manifest without the revision (see [Content roots](#119-content-roots)). But if the floor could reach the replacing commit right away, a base compaction could remove the replaced revision's row within minutes of the replacement, and a read of the revision, or a download grant for it, would return `revision_not_found` while its content still exists. With this rule, the floor reaches the replacing commit only once that commit, or a later one, is at least as old as the default grace, so the reader has at least that long to finish. The advance uses the default grace because each garbage collection pass can set its own grace, so the grace of a later pass isn't known when the floor advances.

Why can the floor pass a commit that's younger than the default grace? Commits are assigned sequences in commit order, so if commit 12 is at least an hour old, every commit below 12 was committed at least an hour ago, whatever its timestamp records. A writer whose clock runs far ahead therefore can't stop every later advance. A future-dated commit at the head isn't passed until a commit above it is at least as old as the default grace, or until its own timestamp is at least the default grace in the past.

Why does the advance read downward from the target? It reads only the commits at the top of the history that are younger than the default grace, which is about an hour of commits, instead of every commit above the floor.

Why check that the segments exist first? Advancing the floor ends the guarantee that older history can be replayed, so LoonFS first makes sure the history it's keeping is readable. The check isn't a transaction across objects, and it doesn't replace garbage collection's reference rules. Reads still verify checksums.

Why does validation check the floor when the scan already ends at it? A base compaction removes every row of an inode that an expired deletion hides, which is safe only because the floor is at or above every expired deletion. The check keeps that rule true even if a scan were wrong.

Why doesn't the advance expire the deletions itself? An advance is a manifest publication and an undelete is a writer commit, so LoonFS can't order an advance against an undelete. An `expire` is a writer commit, so the WAL orders it against every undelete. The session also commits nothing between a scan and its `expire` commit, so every deletion that the scan found is still recoverable when LoonFS validates that commit.

### 10.2 Compaction windows

Metadata is compacted by family group. The `direntry_binds` and `direntry_child_binds` families form one group, because they MUST stay consistent with each other. The `commits` and `commit_receipts` families form another group, and each of the other seven groups contains one family. [Metadata rows and row keys](#a6-metadata-rows-and-row-keys) lists the groups.

A compaction merges a contiguous window of at most eight of a group's runs. The window either includes the group's oldest run or starts above it, and it can't skip a run in the middle.

```text
oldest                                       newest
[ base run ] [ delta 1 ] [ delta 2 ] [ delta 3 ]
|<-------- bottom-anchored window --------->|   output: a new base run, can drop rows
             |<----- window above base ---->|   output: a new delta run, drops nothing
```

| Window | Output run | Can it drop rows? | Sequence stamp |
| --- | --- | --- | --- |
| Includes the group's oldest run ("bottom-anchored") | `base`, which replaces the existing base run | Yes, under the retention rules | The manifest's `head_seq` |
| Skips the oldest run | `delta` | No | The newest input's sequence |

A group has at most one base run. Base runs are ordered before delta runs, whatever their sequence stamps, and a compaction that skips the oldest run stays at its position in the group.

A delta-only compaction MUST merge at least two runs. Once a group has only one delta run above a much larger base, a delta-only compaction can't reduce the run count any further, so the next compaction of the group includes the base.

LoonFS chooses a group's window by size tier. A "size tier" is a contiguous set of a group's runs whose sizes are close to each other, measured as the stored bytes of the group's segments in each run. To choose a window, LoonFS:

1. Lists the group's runs from newest to oldest, with the base last.
2. Starts a tier at the newest run, and adds each next older run while that run is at most four times the size of the smallest run already in the tier.
3. Merges the tier if it has at least four runs. A tier of more than eight runs merges its oldest eight, and the newer runs wait for a later compaction.
4. Otherwise starts a new tier at the next older run, and repeats step 2, until no older run is left.
5. If no tier has four runs, and the group has at least eight delta runs, merges the group's four newest runs, whatever their sizes.

For example, suppose a group's runs, from oldest to newest, are a 300 MiB base and delta runs of 90, 100, 110, and 2 MiB. The tier that starts at the 2 MiB run stops before the 110 MiB run, because 110 is more than four times 2. The tier that starts at the 110 MiB run adds the 100 and 90 MiB runs, and then the base, because 300 is at most four times 90. That tier has four runs, so the compaction merges them into a new base, and the 2 MiB run waits until later runs of a similar size make a tier with it.

An explicit compaction request merges a group's runs whatever their sizes. If the group has more than eight runs, the request merges its oldest eight, unless the oldest run is more than four times the size of the smallest of the next seven. In that case, it merges the eight runs above the oldest one first, so the oldest run is rewritten once, by the compaction that finishes the group.

(The tier size of four runs, the size ratio of four, and the limit of eight delta runs are implementation defaults, not format requirements. A compaction can merge any window that follows the earlier rules in this section.)

Why can't a compaction above the oldest run drop rows? An excluded older run might contain rows that a row in the window supersedes, such as an older binding version below an unbound one, or a listed deletion whose removal row is in the window. Dropping rows in the window could make those older rows visible again.

Why merge by size tier? A merge rewrites every byte of its input, so merging a small new run into a large one rewrites the large run for little new data. A tier merges runs of similar size, so a byte is rewritten about once each time the size of the run that contains it grows four times, and each merge of at least four runs removes at least three runs from the group. A new run that doesn't fit a tier waits. For example, if a fold publishes while a compaction of the group runs, the compaction publishes its new base over the fold's manifest, and the group is left with that base and the fold's run. Two runs aren't a tier, so the fold's run waits for later folds instead of making the group rewrite its base again.

Why is there a limit of eight delta runs? A read of a key checks every run of the key's group that might contain it. If a group's runs never make a tier of four (for example, if each run is more than four times the size of the next newer one), the limit keeps the number of runs a read checks bounded.

### 10.3 Row retention during a base compaction

These rules apply only when the selected inputs include the group's oldest run. A delta-only compaction keeps every history row and the first layout row for each content ID.

| Family | Rows kept or removed |
| --- | --- |
| `inodes` | Keep every inode row, except the row of an inode that an expired deletion hides (see below). |
| `direntry_binds`, `direntry_child_binds` | For each edge (a parent, a name key, and a child), keep every version above the floor and the newest version at or below it. Drop that floor version too if it's unbound. Remove every version of an edge whose parent an expired deletion hides. |
| `revisions` | For each file, keep every revision above the floor and the newest revision at or below it, and remove earlier revisions. Remove every revision of a file that an expired deletion hides. |
| `tombstones` | Keep every `set`, `revoke`, and `expire` event. |
| `active_deletions` | Keep listed deletions until they're revoked or expired, and remove a cancelled `listed` and `removed` pair together. A deletion at or below the floor keeps its `listed` row until the namespace's writer commits the deletion's `expire` event. |
| `commits`, `commit_receipts` | Remove rows strictly below the floor. The row at the floor is kept, and the change feed never replays it (in a fork, it's the source's commit at the fork point). |
| `content_layouts` | Keep the first row for each content ID, ordered by longest layout and then newest commit. Drop chains that aren't in the basis's revision filter (see below). |
| `attributes` | For each inode, keep every revision above the floor and the newest revision at or below it, and remove earlier revisions. Remove every revision of an inode that an expired deletion hides. |
| `access` | For each inode, keep every revision above the floor and the newest revision at or below it, and remove earlier revisions. Remove every revision of an inode that an expired deletion hides. |

An unbound binding version acts as a tombstone, and it MUST stay while it hides older versions of its edge. Only a bottom-anchored compaction can drop it, and that compaction drops it together with every older version it hides. A compaction that excludes the oldest run keeps every history row, including unbound versions at or below the floor, but it can drop superseded layout rows, because the longest layout covers every shorter prefix. Runs cover separate sequence ranges, so a bottom-anchored compaction includes every older version that can affect its floor state.

A bottom-anchored compaction of `content_layouts` also drops the rows of chains that no `revisions` row in its basis refers to. It checks each chain against a Bloom filter, which can err only toward keeping a row, and a later compaction drops what the filter kept. Every compaction scans the basis's revisions once before it merges layouts. The filter uses two xxh64 hashes, a seed from the output run number, and 10 bits per expected ID, and its expected count is the sum of the revision segments' `row_count`. If the filter would be larger than `MAX_BLOOM_FILTER_BYTES` (64 MiB), the merge keeps every layout and logs that choice.

A deletion has "expired" in a compaction's [metadata basis](#41-resolving-the-metadata-basis) when its latest tombstone event in that basis is `expire` (see [File and subtree deletion](#16-file-and-subtree-deletion)). An expired deletion hides its root and every descendant of the root, and a bottom-anchored compaction of the `inodes`, `revisions`, `attributes`, or `access` group, or of the bindings group, removes every row of those inodes, except the ancestors of a recoverable deletion below them (see step 3 below). It tests a binding row by the edge's parent, so it removes every version of an edge whose parent is hidden. The root's own edge has a visible parent, and the binding rule above drops that edge, because its newest version at or below the floor is unbound. The other families don't need a rule of their own: `tombstones` keeps the deletion's events, `active_deletions` removes the root's `listed` row together with the `removed` row of its `expire` event, and the revision filter drops the chains of the removed revisions.

Before such a compaction merges, it builds a "kept inode filter", which is a Bloom filter of every inode that's visible or under a recoverable deletion in the basis, and of each recoverable deletion's ancestors. To build the filter, the compaction:

1. Walks the basis's directory tree from the root inode, depth first, with one cursor for each directory between the root and the directory that it's reading. It reads each directory's children from `direntry_binds`, and it inserts and follows the child of each edge whose newest version is bound.
2. Walks the subtree of every recoverable deletion the same way, starting at the deletion's root. It reads those roots from the `listed` rows in `active_deletions` that have no `removed` row.
3. Inserts the ancestors of every recoverable deletion: the parent saved in the deletion's tombstone, then that inode's current bound parent or, at a deletion root, its saved parent, and so on up to the root inode.

Deleting an item unbinds it, so the first walk doesn't reach a deleted subtree. An expired deletion isn't a starting point for the second walk, and its root is unbound, so neither walk reaches the inodes that the deletion hides, and step 3 inserts only the ones that are ancestors of a recoverable deletion. The merge removes a row whose inode isn't in the filter, and it applies the rules in the table to a row whose inode is in the filter. The two binding indexes are merged in one compaction against the same filter, so the compaction keeps or removes each binding event the same way in both indexes. The walk reads the slot-index rows of every directory it reaches, so each of these compactions reads the bindings of the visible and recoverable tree once more.

The kept inode filter follows the layout filter's rules: two xxh64 hashes, a seed from the output run number, and 10 bits per expected inode, and its expected count is the sum of the basis's `inodes` segments' `row_count`. A false positive keeps a hidden inode's rows for one compaction, and the next compaction of the group has a different output run number, so it removes them. If the filter would be larger than `MAX_BLOOM_FILTER_BYTES`, or the walk would need more than `MAX_KEPT_INODE_WALK_DEPTH` (4,096) cursors at once, or would follow more than that many ancestors up from one deletion, the compaction keeps every row that it would have tested, removes no hidden rows, and logs that choice. A delta-only compaction doesn't build the filter, and it removes none of these rows.

For example, suppose the directory `/old` (inode 7) contains `a.txt` (inode 8). The deletion of `/old` at sequence 40 expires at sequence 61, and the floor is at 50. The walk from the root doesn't reach inode 7, because the deletion unbound `/old`, and no walk starts at inode 7, because the deletion isn't recoverable. So a base compaction of `revisions` removes every revision of `a.txt`, and a base compaction of the bindings group removes every version of the edge from inode 7 to `a.txt` and, under the binding rule, the root's unbound edge to `old`. The tombstone keeps its `set` and `expire` events.

Why can a base compaction remove every row of a hidden inode, and not just the older rows that the usual rules drop? The writer expires a deletion only once the floor reaches the deletion, and LoonFS commits no change to a hidden inode, so every row of a hidden inode is at or below the floor, and the tombstone hides the inode at every retained sequence. The tombstone events stay, because family groups are compacted at different times, and the tombstone keeps the inodes hidden while some groups have removed their rows and others haven't.

Why does the filter keep a recoverable deletion's ancestors? The authorization of a recoverable deletion is judged on its saved ancestry (see [Access rows](#18-access-rows)), so those ancestors' access rows have to outlive the expired subtree around them. For example, suppose `/team` grants a team `read`, `/team/hr` sets its boundary flag and grants one person `read`, and `/team/hr/salaries.csv` is deleted on its own before `/team` is deleted and expires. A read of the deleted file is still authorized through `/team/hr`, so if a compaction removed that row, the walk could find the team's grant on `/team` in a run that a compaction hasn't merged yet, and a team member could read the file. The ancestors keep their rows in every family while the inner deletion is recoverable.

Why does the compaction find hidden inodes by walking the tree, and not by a flag on each row? A tombstone on a directory is one row that hides the whole subtree, so no row below the directory records that it's hidden, and a flag on every row would turn one deletion into a write for every descendant. The visible tree and a hidden subtree can each have more inodes than fit in memory, so the compaction uses a filter whose size is set before the walk starts, and which can err only toward keeping a row.

Each binding event appears in both indexes, and both indexes group their rows by edge, so compaction keeps or removes each event in both indexes the same way. Row counts and row digests verify that the two indexes agree, and every event above the floor stays.

A binding row's position comes from the delta that published it, so a later bind doesn't depend on an earlier unbound value. Attribute and access revisions keep a cleared floor state, because the next update is validated against its revision number.

An empty attribute map, or an access row with no boundary and no grants, is kept when it's the state at the floor. Removing it could expose an older row and bring back state that had been cleared. A base compaction doesn't remove the rows that a deletion hides until the deletion expires, so an undelete restores the same state.

A rewrite MUST refuse an ambiguous file revision, attribute, or access history, in which two rows for one inode have the same revision number at or below the floor. It can't choose one row and discard the other.

The active-deletion family is an index of current state, not a separate historical trash log. Its removal marker sorts before the matching listed entry, so a bottom-anchored compaction can remove the pair without leaving an older entry that would reappear in a later read.

Compaction MUST preserve the visible metadata at every retained sequence. It publishes a complete replacement through the numbered manifest protocol. Its input objects stay available until no protected manifest or pin references them (a successful publication isn't permission to delete them right away).

### 10.4 Streaming compaction

A compaction streams: it merges the selected runs without keeping every row in memory. It writes completed segments under the namespace's normal `segments/` prefix, with new IDs, and then it publishes references to that output in the next manifest. Readers keep using the previous manifest until the publication succeeds.

A runtime claims the namespace's compactor epoch before its first compaction after it opens the namespace. Claiming publishes a manifest with `compactor_epoch + 1` and no other changes, and concurrent family groups in that runtime share the claim. Compaction publications MUST match the current epoch, so a newer claim fences older compactors. A compaction also checks the epoch of the manifest it loads before it reads a segment.

Before each publication attempt, a compaction checks its elapsed monotonic time and reloads the current manifest. Its selected input segments MUST still be present and unchanged. While both conditions are true, a lost numbered put can be retried against a new manifest. A changed epoch produces `fenced`, and changed inputs produce `abandoned`.

A compaction runs under two deadlines:

| Deadline | Starts | Limit | Past the limit |
| --- | --- | --- | --- |
| Compaction deadline | Before the compaction reads its input | The compaction starts its last publication attempt within `METADATA_COMPACTION_BUDGET_MS`. | The compaction returns `abandoned`. |
| Publication deadline | When finalization starts | Every finalization attempt runs under one `METADATA_PUBLICATION_BUDGET_MS`. | The attempt fails with the metadata publication budget error. |

Segments that no root references are protected until their provider age is at least 24 hours. The compaction deadline reserves the minimum garbage collection grace inside that interval:

```text
compaction deadline + minimum GC grace <= 24 hours
```

With the current constants, a compaction can start its publication for at most 23 hours, 18 minutes, and 30 seconds after its timer starts (before it writes any output). After that, it abandons the publication, because its earliest unreferenced output might already be eligible for deletion. A cancelled or crashed compaction leaves output that's subject to the same age rule.

A compaction applies the row-retention rules for its selected window, and it MUST preserve every retained view. A restart begins a new plan from the current manifest, and there's no durable compaction cursor or output-protection record.

Why does core keep a compactor epoch? A compaction of a large window publishes once at the end, which can be hours later. The epoch lets a newer runtime stop a stale compactor at its next check. (Grep publishes each bounded step separately, so a lost race costs one step, and grep doesn't need an epoch.)

## 11. Garbage collection

This section describes how garbage collection determines which objects are still referenced, and when an unreferenced object is old enough to delete safely.

Garbage collection deletes objects that no retained view needs, after the required age and publication checks. A logical deletion on its own doesn't allow file bytes to be removed. A live namespace's content stays while a retained view refers to it (see [Content roots](#119-content-roots)), and a deleted ancestor's content stays while fork descendants depend on it.

An object can't be deleted as soon as nothing references it, because a publisher might be about to reference it. Garbage collection waits until an unreferenced object is older than the reclamation grace, which is long enough for every publication planned against an older state to have finished or failed (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).

### 11.1 One complete pass

A garbage collection pass first establishes the roots. It:

1. Discovers the namespace's current manifest.
2. Lists the namespace's upload sessions.
3. Reads the WAL tail.
4. Lists all pin keys and manifest numbers, and takes each manifest's successor age from that listing.
5. Records the protected manifests and segments.

An invalid or unreadable root manifest stops the pass before anything is deleted. An absent namespace has nothing for core garbage collection to delete.

The pass then lists each candidate family completely, from the beginning. Unless its shared base filter would be larger than its limit (see [Content roots](#119-content-roots)), it sweeps content one ID shard at a time, where a shard is the content IDs with a common hex prefix. The exact sets of content roots use read working memory for one shard, plus the tail and session IDs. Shared base keys use a Bloom filter, with its own 64 MiB limit. The pass stores no durable progress, run, phase, reference table, or cursor. The complete pin and session listings that establish the roots are separate from the later sweeps that determine which records can be deleted.

Every age decision in a pass uses the same `now_ms`, captured when the pass starts. A later pass reads fresh roots and uses its own clock. Concurrent collectors can delete eligible objects independently, and an object that's already gone needs no more cleanup. A failed pass might have deleted some candidates already, but the next pass can safely start over.

### 11.2 Reference roots

| Evidence captured for the pass | Objects protected |
| --- | --- |
| The current active manifest | The manifest, and every segment in its runs. |
| The current deleted manifest (the namespace tombstone) | The tombstone, and every segment in its runs. |
| Every recognized pin key in the complete listing | The manifest numbered in its ID, and every segment in that manifest. |
| Every manifest below the current one whose immediate successor is younger than the ordinary grace `T` | The manifest, and every segment in its runs. |
| The hint's observed manifest number | Every manifest number at or above it, so discovery can probe forward. (This root doesn't protect their runs.) |
| The current active manifest's folded boundary | Every WAL number above `folded_wal_no`. |

Pin bodies aren't needed to identify these roots, because the manifest number is part of the pin key. Bodies are normally read later, for owner and expiry decisions. If a pin's manifest is missing, the collector reads the pin and applies the same owner and grace rules as pin cleanup. An absent or collectable pin doesn't require that missing basis, which allows recovery from a failed installation cleanup or a concurrent pin removal. A retained pin that refers to a missing manifest is corruption, and invalid or unreadable manifests still fail the pass. Each listed pin whose manifest is present protects its files for the whole pass, even if that pass deletes the pin.

A manifest below the current one stays a root while its immediate successor is younger than `T`, whether it's above or below the observed hint. The successor's age comes from its modification time in the manifest listing. A successor listed without a time counts as young, and a successor missing from the listing doesn't make the older manifest a root.

The tombstone keeps its runs, so an import from a deleted owner can still be authorized against its final access state. Retirement doesn't read those segments (see [Retirement](#95-retirement)). Content objects have their own roots (see [Content roots](#119-content-roots)).

A retention floor can move past a pinned manifest's head sequence, and that doesn't remove the pin's protection. Reads through the pin use the pinned file set directly.

Why keep a superseded manifest for one grace? A reader or writer confirms its view with a successor check that finds the next manifest number missing, so that successor is created no earlier than the check. The view's manifest, and every segment in its runs, therefore stay for at least one grace after the check. A reader revalidates within `READ_REVALIDATION_BOUND_MS` of its previous check, and the writer confirms its basis at least once per `WAL_PUBLISH_BUDGET_MS` (see [Replaying the visible WAL](#42-replaying-the-visible-wal) and [Publication and group commit](#63-publication-and-group-commit)). Both bounds are below the grace, and the clock allowance in [Clock and operation assumptions](#114-clock-and-operation-assumptions) is the margin. The writer can use its basis for up to twice `WAL_PUBLISH_BUDGET_MS` after confirming it, because a batch planned when the basis is a budget old can still start its put up to a budget later, and twice the budget plus the provider request bound and the clock allowance is also below the grace.

### 11.3 Candidate and age rules

Being unreferenced makes an object a candidate for deletion, but it doesn't make it deletable right away. Provider age uses the modification time that the listing reports, and a candidate listed without a time is kept. Let `T` be the configured ordinary grace, which MUST be at least `GC_MIN_GRACE_WINDOW_MS`.

| Family | Conditions for deletion |
| --- | --- |
| Namespace manifest | Below the observed hint, not a root (see [Reference roots](#112-reference-roots)), and a provider age of at least `T`. |
| WAL object in an active namespace | At or below `folded_wal_no`, and a provider age of at least `T`. |
| WAL object in a deleted namespace | A provider age of at least `T` (no current WAL object is protected). |
| Metadata segment | No root lists it, and its provider age is strictly greater than 24 hours. |
| Pin record | The owner-specific rules in [Pin cleanup](#117-pin-cleanup). |
| Upload session | The status-specific rules in [Upload-session cleanup](#116-upload-session-cleanup). |
| Content object in an active namespace | No content root refers to it (see [Content roots](#119-content-roots)), and a provider age of at least `T`. |
| Temporary object | A provider age of at least `T`. |
| An eligible tombstone's own content | See [Retirement](#95-retirement) and [Sweeping a retired owner's content](#118-sweeping-a-retired-owners-content). |

The hint and the current manifest are never deleted. Core garbage collection keeps unrecognized keys outside an eligible tombstone's content prefix. For a candidate that's deleted by age, a missing provider timestamp, or one in the future, can't show that it's old enough.

For example, suppose a collector observes hint 8 and current manifest 10. It keeps manifests 8, 9, and 10 for discovery. It keeps the segments in manifest 10, in any pinned manifest, and in any manifest whose successor is younger than `T` (such as manifest 9, if manifest 10 is recent). If manifest 9 is older than `T`, a segment that only manifest 8 lists is a candidate, but it still has to be older than the segment minimum age before it's deleted.

### 11.4 Clock and operation assumptions

Garbage collection deletes an unreferenced object based on its age, which is calculated as `now_ms.saturating_sub(last_modified_at_ms)`. So safety depends on a bound on how much an age can be overstated. The collector's clock might be ahead of the provider's, timestamps might have limited precision, and a process might pause around a publication check.

The combined allowance (`GC_SAFETY_MARGIN_MS`) is 180,000 ms. It's one allowance for relative clock error, timestamp precision, and scheduling together, not three minutes for each participant. A provider clock that's ahead of the collector's delays garbage collection, but doesn't make it unsafe. Ages based on recorded times (creation, expiry, or completion) use the corresponding bound between the collector and the host that recorded the time.

Publication budgets use monotonic elapsed time, on a clock that keeps running while the host sleeps. The minimum ordinary grace covers the longer of two times: the longest bounded publication, or the revalidation bound plus the lifetime of a direct transfer capability (a view up to that bound old can still issue a capability). It then adds the provider publication request bound and the combined allowance. Fork installation and compaction reserve that grace within their own lifetime bounds. [Publication and collection timing](#c1-publication-and-collection-timing) records the exact calculations.

If a publication's budget has expired when its put returns, the publisher treats the outcome as unknown, even if the put succeeded. A client timeout doesn't show that a remote write had no effect, so unknown outcomes still have to be reconciled. The same rule applies when a manifest put has an unknown outcome and a read-back finds identical bytes. The read-back confirms the publication only while the budget hasn't expired. After that, the put might have been applied outside the provider bound, so the outcome stays unknown. The operation returns `outcome_unknown`, and the caller reloads and plans from the current state.

The publication protocol depends on one assumption that the implementation can't check. A provider applies a request, if it applies it at all, within `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` of when the client started the operation that sent it. This is the provider time that the reclamation grace reserves, and it's an operational requirement on the provider. The protocol is safe under that assumption, for these reasons:

1. A WAL object or manifest at number `n` is created no earlier than the observation that `n - 1` was the tip, because that observation found `n` missing.
2. The object can be deleted only one grace after it's created, and the grace is at least the longest budget, plus the provider request bound, plus the clock allowance.
3. A put planned against that observation starts within its budget, and (by the assumption) the provider applies or rejects it within the request bound after that.
4. So before `n` can be deleted, the provider has applied or rejected every such put, and each one has succeeded, failed, or been reported as unknown. No put recreates a deleted number.

Two budget checks make this true. The check before a put stops a stale observation from being used, and the check after a put turns a late success into an unknown outcome, so every acknowledged put was applied within its budget. The same two checks cover the epoch claim and other manifest puts (under `METADATA_PUBLICATION_BUDGET_MS`), and the fence and data puts (under `WAL_PUBLISH_BUDGET_MS`). ([Alternatives considered](#13-alternatives-considered) explains why the format doesn't keep a record of used numbers instead.)

Direct expiry checks don't add the grace to the requested lifetime. A host whose clock is `E` milliseconds ahead can reject an expired upload or user pin up to `E` milliseconds earlier than the creating host would. The reclamation grace protects concurrent publication, and it doesn't synchronize expiry decisions across hosts.

### 11.5 Publication during collection

A new pin from the current head is acknowledged only after LoonFS checks its manifest's identity again, after the pin write. This closes the race between garbage collection's read of the current manifest and its complete pin listing (see [Creating and verifying a pin](#82-creating-and-verifying-a-pin)). A fork of a user pin is protected by the user pin, or by the new fork pin written before the user pin is checked again.

A collector protects every WAL number above its captured folded boundary. Writers MUST observe a cached tip within the publication budget before they try to write its successor, by discovery or by the confirmation in [Publication and group commit](#63-publication-and-group-commit). So a writer can't treat a much later WAL number, whose object was deleted, as a free slot.

Readers have the matching obligation. A cold discovery trusts a missing successor only if the answer arrives within `READ_REVALIDATION_BOUND_MS` of its manifest probe, and a cached view trusts its probe answers only if they arrive within that bound of its previous successor probe (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). Metadata segments have the matching guarantee. A view that revalidates within its bound never lists a deleted segment, because its manifest was current at its last successor check, and it stays a root for at least one grace after its successor appeared (see [Reference roots](#112-reference-roots)).

New metadata segments are protected by their minimum age while a publisher writes and verifies them. Streaming compaction MUST start its publication before its budget expires, and every compaction checks its epoch and its selected inputs. These rules apply to output that isn't yet listed by a root the pass captured earlier.

A failed read of a required root stops garbage collection, and an uncertain read of a fork's target keeps that pin. Each cleanup operation MUST keep the durable evidence it needs to retry after a failure.

### 11.6 Upload-session cleanup

Uploads are collected through their session records. Retirement (see [Retirement](#95-retirement)) takes precedence over the session status rules. [Content roots](#119-content-roots) defines when an active namespace's content is deleted, and [Sweeping a retired owner's content](#118-sweeping-a-retired-owners-content) defines the content sweep of a retired one.

| Session and namespace | Action |
| --- | --- |
| Open session | Keep it until its expiry plus `T`, then create its aborted outcome record, and clean up its content and provider transfer state. If the session already has an outcome record, keep the session. |
| Aborted session | Retry content and provider cleanup, and delete its records after its abort time plus `T`. |
| Completed session in an active namespace | Keep it until the content grace passes, then delete its records. Its content follows [Content roots](#119-content-roots). |
| Session in a deleted namespace that isn't eligible for retirement | Keep it, and report a future retirement deadline, if there is one. |
| Completed session in an eligible deleted namespace | Delete its records (no extra completion grace is required). The retirement sweep removes its content. |
| An outcome record without its session record | Delete it once its provider age is at least `T`. |

Garbage collection deletes a session's records in order: the session record first, and then the outcome record. If a pass stops between the two deletes, the outcome record is left without its session record, and a later pass deletes it without acting on the session's content. A completion or abort that creates an outcome record after the session's records were deleted also leaves an outcome record without its session record (see [Upload sessions](#51-upload-sessions)).

An outcome record without its session record is deleted only once its provider age is at least `T`, the age rule of the other unreferenced objects, because a listing isn't a snapshot. A listing can miss the session record of a session that was created and ended while the listing ran, and then show that session's new outcome record on its own. Deleting that outcome record would make the session read as open, and a later pass would abort it and delete content that a commit might reference. The outcome record was created after the pass started, so its age is below `T`.

An outcome key sorts directly after its session key (see [Object keys](#a8-object-keys)), so one listing of the uploads prefix shows which sessions have ended, without a read for each session.

Before completion, a session exclusively owns its random content ID, and it can't issue admission evidence. In active namespaces and eligible tombstones, cleanup first creates the aborted outcome record, and then it removes the content and any provider transfer. Cleanup keeps an aborted session's records until its abort time plus `T`. So when a streamed write that passed its final check before the abort finishes, the session's records still exist, and a later cleanup attempt can remove the object that the write created. A failed cleanup leaves the records for another attempt. Open and aborted sessions still need provider cleanup after the namespace is retired, because provider upload state can exist outside object listings.

Cleanup derives every content key and provider cleanup target from the namespace and content ID recorded when the session opened.

A completed session is a content root while its session record exists (see [Content roots](#119-content-roots)). Once the session's records are deleted, its content stays only while another root refers to it.

The completed-content grace covers every possible piece of admission evidence:

```text
completion
|<-- receipt issuance window -->|
                                |<-- final token lifetime -->|
                                                             |<-- minimum GC grace -->|
                                                                       earliest cleanup
```

In milliseconds:

```text
CONTENT_RECLAMATION_GRACE_WINDOW_MS
    = COMPLETED_UPLOAD_RECEIPT_WINDOW_MS
      + CONTENT_RECEIPT_TTL_MS
      + GC_MIN_GRACE_WINDOW_MS
```

Every token issue checks the original completion time, so a retained receipt can't extend its issuance window. In-process proofs expire at the end of the admission window, and publication checks proof expiry immediately before the numbered WAL put. After the full grace, an unpublished upload can't be committed for the first time. Published content stays while a content root refers to it.

Why is the session record deleted first? A session record left without its outcome record reads as an open session. Once the session's lease had passed, garbage collection would abort the session and delete its content, which a commit might reference. No request issues evidence from an outcome record left without its session record, and that outcome record doesn't protect any content, so a later pass can delete it safely.

### 11.7 Pin cleanup

User pins become collectable after their expiry plus `T`, or after their creation plus `T` in a deleted namespace. A user pin with no expiry stays in an active namespace until it's explicitly deleted. A retained user pin reports the earlier of those times in `next_reclamation_at_ms`. Pins are deleted directly, and pin IDs are never reused.

A fork pin is cleaned up based on its target namespace:

| Condition | Decision |
| --- | --- |
| The pin is inside its creation grace (less than `T` after its `created_at_ms`), the target's current fork basis refers to this pin with the same manifest reference, or a read of the target's store fails | Keep the pin, including when the target is deleted. |
| The target is absent, or its fork basis doesn't refer to this pin | Delete the pin of the abandoned installation. |
| The target's current fork basis refers to this pin with a different manifest reference | Fail with `namespace_corrupt`. |

The source finds the target's current manifest through the target's hint. It reads no target WAL objects and makes no listings. [Sweeping a retired owner's content](#118-sweeping-a-retired-owners-content) defines when a source pin is released.

A candidate pin that fails verification is deleted by its creator. If that deletion fails, or the creator crashes first, garbage collection applies the pin owner's rules, including a fork pin's creation grace.

### 11.8 Sweeping a retired owner's content

After upload-session cleanup, garbage collection reclaims a tombstone that's eligible for retirement (see [Retirement](#95-retirement)) in two steps:

1. List the namespace's content prefix (see [Object keys](#a8-object-keys)), and delete every listed key, with bounded concurrency. Count the listed keys whose deletion succeeds in `deleted.retired_content_objects`. No additional age check applies. A listing or deletion failure ends the call before the source pin is released.
2. Delete the source pin that the tombstone's fork basis refers to, if there is one, and count it only if it was present.

Content ownership follows [File contents and ownership](#15-file-contents-and-ownership). A deleted namespace refuses upload capabilities and completion, and the retirement grace is longer than the lifetime of any issued presigned URL.

Content and source-pin cleanup are idempotent. With no new objects, a later content sweep makes one empty LIST request and no DELETE requests, and it reports zero reclaimed objects. Retained roots follow [Reference roots](#112-reference-roots). Deleting listed content counts as maintenance progress, and no progress record or journal is stored.

### 11.9 Content roots

In an active namespace `N`, a content object that `N` owns is kept while a layout extent refers to that object, or a tail or session root refers to its chain:

| Root | Content it protects |
| --- | --- |
| A layout view: the current manifest, every pinned manifest, and every manifest whose immediate successor is younger than `T` (see [Reference roots](#112-reference-roots)) | The objects of every extent, owned by `N`, in every `content_layouts` row. |
| `N`'s unfolded WAL tail | Every chain that an `append_file_revision` delta refers to, and every extent owned by `N` in the layouts of the tail's row state. |
| An upload session record in `N`, whatever its status | The session's `content_id`. |

Garbage collection considers every other content object under `namespaces/N/content/` for deletion once its provider age is at least `T`, subject to the shared base filter described below. It keeps an object younger than `T`, and a key that doesn't parse as a content object key.

To find the protected objects, the pass:

1. Lists each layout segment of the layout views once, however many views list it.
2. Scans the rows of each listed segment for extents whose content ID is different from their row's (shared base extents, which can point into any shard), and inserts their object keys into a Bloom filter.
3. Scans the rows of the listed segments in disjoint content ID ranges, one shard at a time, and protects the exact extent object keys in that shard.

The pass reads each segment's own rows, and doesn't merge the segments of a view. A view's layout rows are the rows of its segments, so the listed segments include every row of every view. That includes a superseded layout row (a shorter layout of a chain that a longer row replaces), so an object that only a superseded row refers to stays until a compaction drops that row and no layout view lists the segment that contains it.

Tail and session IDs protect every object of their chains. A direct download capability issued from a view of a superseded manifest expires before that manifest stops being a root, because `T` covers the revalidation bound plus the capability lifetime (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).

The shared base filter follows these rules:

- It has no false negatives. A false positive keeps an unreferenced object for this pass.
- The pass's `now_ms` seeds the filter, so a pass with a different clock uses a different seed.
- It uses xxh64 double hashing and seven probes.
- Its initial expected entry count is the current manifest's layout row count, with a minimum of one. Each insertion counts toward the current level's expectation, including repeated keys. When an insertion would exceed that expectation, the filter adds a level with twice the expectation and a seed derived from the pass seed and the level index. A key matches the filter when any level matches.
- Each level is sized at ten bits per expected entry, so a filter with `n` levels has roughly `n` times the false-positive rate of one level.
- The levels' total size is limited to 64 MiB, separate from read working memory. If the initial level or a new level would exceed the limit, garbage collection logs it and skips the active namespace's content sweep. (An initial allocation over the limit skips the shared base scan, and growth over the limit stops it.) Neither case lists content, and other families are still swept.

Why does the pass scan twice? The exact keys for one shard fit in bounded memory, but a shared base extent refers to an object of another chain, which can be in any shard. The first scan collects those keys into the filter, so the second scan can protect them whichever shard they fall in.

A tail revision delta can leave out its layout only when its commit includes a piece at offset 0, or a piece that continues a chain with an existing revision. In the second case, the base stays the inode's head until the append is folded, so retention keeps that revision, and layout compaction keeps the chain's row.

A fork's source pin protects the inherited revisions and layouts in the owner's namespace.

The pass lists upload sessions before it reads the WAL tail. A session record is removed only after any admission evidence it could issue has expired (see [Upload-session cleanup](#116-upload-session-cleanup)). A session that the pass doesn't list was either created after the listing or removed before it. The content of a session created later is younger than `T`, and every commit that refers to the content of a removed session succeeded before the tail was read, so the tail or a protected manifest refers to that content.

A deleted namespace keeps its content until it's retired. [Sweeping a retired owner's content](#118-sweeping-a-retired-owners-content) then removes its whole content prefix.

## 12. Encodings, versions, and extensions

This section describes how durable objects are encoded and versioned, and how extensions store their own state.

Storage versions describe how durable objects are interpreted and operated on, and API versions describe request and response contracts. They're related, but a change to one isn't automatically a change to the other.

### 12.1 Field conventions

Durable field names and enum values use `snake_case`, and tagged unions use `kind`. A `status` field describes a resource's lifecycle, and a `phase` field describes the progress of a computation. When the values of either are tagged objects, they use `kind`.

Field name suffixes have these meanings:

| Suffix | Meaning |
| --- | --- |
| `_id` | An identifier. |
| `_seq` | A position in the namespace's commit history. |
| `_no` | A counter scoped to its resource. |
| `_index` | A zero-based position in a collection. |
| `_number` | A one-based position defined by a provider or tool. |

An absent optional object field is left out when it's written. A required value is written even when it's zero, false, or an empty collection. Fingerprint preimages have their own explicit absence rules (see [Appendix B](#appendix-b-semantic-commit-fingerprints)), because they aren't ordinary durable records.

Durable inode IDs are integers. Public API inode strings, such as `ino_42`, don't change the stored representation. The canonical fingerprint rules identify the few places where public strings are part of the preimage.

### 12.2 Envelopes and checksums

Structured control records, manifests, and WAL objects use an envelope with `kind`, `format_version`, `payload_checksum`, and `payload`. Content objects are raw file bytes. Block segments use the sectioned encoding in [Block-segment encoding](#a7-block-segment-encoding) instead of an envelope.

A JSON envelope keeps its payload as an inline raw JSON fragment. A WAL envelope contains its encoded CBOR payload as a CBOR byte string. A manifest or a WAL object stores its whole document compressed with zstd, and a control record stores its document uncompressed. The checksum covers the payload bytes exactly as they appear in the document, not a decoded object that the reader serialized again.

Decoders identify the kind and the supported version before they interpret the payload, and they verify the exact payload checksum before they decode its fields. Unknown kinds, unsupported versions, malformed payloads, and checksum failures are errors, and none of them is a reason to use another object instead.

Envelope and manifest-reference digests are encoded as `sha256:<64 lowercase hex>`. A content, object, or part checksum uses `{ "algorithm": ..., "value": ... }`, because the store selects its algorithm. A block handle has a numeric `crc32c` field, because the block format always uses CRC32C. Commit fingerprints also identify their canonicalization scheme.

Per-block CRC32C checksums verify ranged block reads against their handles, and envelope digests verify complete payloads. Neither an unauthenticated checksum nor an object name is an authorization mechanism.

### 12.3 Strict decoding and evolution

Authoritative durable envelopes, and their nested payloads, reject unknown fields. That includes immutable objects, because folds and compaction encode their contents again, so accepting an unknown field and dropping it from a successor would lose durable meaning.

`ContentRef` and `Checksum` are closed shapes wherever they appear. Each content kind and each checksum algorithm is its own closed shape, and a new kind or algorithm is a new shape. Unknown content kinds and checksum algorithms are rejected, and a new content kind requires a supported version change for every durable family that contains the reference.

API request bodies also reject unknown fields, including nested fields, so a misspelled precondition can't quietly become a request without that precondition. Response bodies generally tolerate additions, except for shared closed shapes. The API specification defines those transport rules.

Versions work like this:

- The owning envelope's `format_version` governs its whole payload, including nested objects and collection semantics. A payload doesn't add its own format-version field.
- A metadata segment is a separate object. Its descriptor's `encoding` is the segment's own version, and it covers both the block layout ([Block-segment encoding](#a7-block-segment-encoding)) and the row-kind schema ([Metadata rows and row keys](#a6-metadata-rows-and-row-keys)). Each segment has its own encoding, so one manifest can list segments of different encodings, and compaction migrates the rows it rewrites to the encoding of the segments it writes. The encoding isn't a version of the manifest payload (the manifest's version governs the descriptor itself).
- A `commit` row contains the deltas of its WAL commit record, so the `commits` family's encoding changes whenever the WAL delta vocabulary changes.
- A reader rejects a manifest that refers to an encoding it doesn't support, before it opens any segment.
- Grep segments are interpreted under the version of the grep manifest that references them.
- A name such as `blob_v1` identifies a closed content strategy, and it isn't permission to ignore the owning family's version.

After the stable format is released, a change to a field's name, presence, type, tag, encoding, or governed meaning requires a new version of the owning family. A change to the garbage collection protocol can require a version gate, even when most stored fields stay the same. An implementation MUST NOT operate on a newer protocol just because it can deserialize some of its fields. This rule applies to every role that changes durable state: publishing a successor manifest or WAL object, compacting, creating or changing pins, advancing retention, garbage collection, and maintaining an extension. Each of those roles loads the current manifest before it publishes, or is fenced by the epoch that a newer binary claims when it first publishes, and an unsupported version is rejected when it's loaded. So a process that was running when a newer version was published stops changing durable state at its next publication attempt, and a garbage collection pass stops at its next start.

A reader accepts every released version of a family up to its own, and it rejects a newer one. Segment encodings follow the same rule. Pins and fork bases capture manifests by checksum, and they keep their segments for as long as any pin or fork exists, so objects written by any earlier release can stay in use. A writer writes the lowest version that can express the object it writes, so an upgraded binary writes objects that an older binary can read until an object needs a newer version.

Golden fixtures pin the reference encodings in `crates/loonfs-types/tests/golden_formats.rs` and the grep fixtures, and fingerprint vectors also pin canonical JSON bytes and digests. Validating a new release means checking that it preserves the meaning of retained data.

[Family versions](#a1-family-versions) lists the family versions. A binary rollback is valid only if the older binary supports every stored family version and its protocol. Downgrading the binary doesn't convert stored data.

### 12.4 Extensions

An optional derived subsystem, such as grep, stores its objects under:

```text
namespaces/{namespace_id}/extensions/{name}/
```

An extension defines its own object grammar, versions, discovery and publication rules, and garbage collection rules. Core manifests contain no generic extension registry and no extension-state fields. Core reads don't require support for an extension's encoding, and core garbage collection doesn't delete extension objects.

An extension MUST stay rebuildable from authoritative core state, and its absence can't make the core namespace unreadable. [Appendix D](#appendix-d-grep-extension-format) specifies the grep extension, including its manifest, tokenizer, postings, and garbage collection.

### 12.5 Reserved functionality

Version 1 defines the inode kinds `file` and `dir`. The kind `link` is reserved, not defined. A link would be an inode with exactly one parent binding and an immutable target inode ID, and path resolution would use the target in place of the link. Version 1 has no row, delta, or operation for links.

In version 1, an inode never has more than one parent binding. A directory can never have more than one parent, in any version of the format.

This version doesn't define mount creation or traversal, and no standard operation creates a mount.

The naming mode `labels` is reserved. In a namespace with that mode, names would be labels, so siblings could share a name, and path routes would return `not_supported`. This version doesn't define it, and a manifest with that mode is invalid.

## 13. Alternatives considered

This section describes designs that were considered for the format and the reasons they weren't used. The design notes cover narrower choices: [commit history](../design/commit-history.md#alternatives-considered), [inline small content](../design/inline-small-content.md#alternatives-considered), and [directory bindings](../design/directory-bindings.md#costs-and-alternatives).

- **A writer lease.** A writer could take a lease with an expiry time instead of an epoch. A new writer would then have to wait for the old lease to expire before it could take over. With epochs and fences, a new writer can take over at any time, and correctness depends only on conditional writes (see [Writer ownership](#61-writer-ownership)).
- **One upload session record whose status changes.** A single record could move from open to completed or aborted by compare-and-swap. But an upload would then write that record when the session starts and again when it ends, often less than a second later, and some providers allow about one write per second to one object. With two records that are each created once, completion and abort still race on one put-if-absent, and the cost is a second key for each ended session in the uploads listing (see [Upload sessions](#51-upload-sessions)).
- **A durable record of used WAL and manifest numbers.** Recording every number that was used would remove the provider timing assumption in [Clock and operation assumptions](#114-clock-and-operation-assumptions), because a deleted number could never be mistaken for a free one. But checking the record would add a read to every commit acknowledgement and every read of the current state, and it would remove only that one assumption.

## Appendix A. Durable records and byte encodings

This appendix is the field and encoding reference for the protocols above. Field names are literal. A `?` after a field in a table means the object member is optional and is left out when absent (the question mark isn't part of its stored name).

### A.1 Family versions

There are four kinds of control object: `hint`, `pin`, `upload_session`, and `upload_outcome`.

| Object | Envelope kind | Encoding | Version |
| --- | --- | --- | --- |
| WAL object | `wal_object` | zstd-compressed CBOR envelope with CBOR payload bytes | 1 |
| Namespace manifest | `manifest` | zstd-compressed JSON envelope with an inline JSON payload | 1 |
| Namespace hint | `hint` | Uncompressed JSON | 1 |
| Metadata segment | No envelope | Block sections (see [A.7](#a7-block-segment-encoding)), containing the rows in [A.6](#a6-metadata-rows-and-row-keys) | 1, given by the segment descriptor's `encoding` |
| Pin record | `pin` | Uncompressed JSON | 1 |
| Upload session record | `upload_session` | Uncompressed JSON | 1 |
| Upload outcome record | `upload_outcome` | Uncompressed JSON | 1 |
| Content object | No envelope | Immutable extent bytes | Not versioned. A layout lists the whole and span objects for a `blob_v1` chain prefix. |
| Grep hint | `grep_hint` | Uncompressed JSON | 1 |
| Grep manifest | `grep_manifest` | Uncompressed JSON | 1 |
| Grep segment | No envelope | Block sections with grep rows | Governed by grep manifest version 1 |

### A.2 Envelope layout

An envelope contains these fields:

| Field | Representation |
| --- | --- |
| `kind` | The family's discriminator string. |
| `format_version` | The family's version, as an unsigned integer. |
| `payload_checksum` | `sha256:` followed by 64 lowercase hexadecimal characters. |
| `payload` | A raw JSON sub-document for JSON families, or a CBOR byte string containing the encoded payload for the WAL. |

To encode an object, a writer:

1. Serializes the payload, to JSON for a JSON family or to CBOR for the WAL.
2. Calculates `payload_checksum`, the SHA-256 of those payload bytes.
3. Builds the envelope document around the payload bytes. A JSON document includes them as an inline fragment, and a CBOR document includes them as a byte string.
4. Compresses the whole document with zstd, if the object is a manifest or a WAL object. A [control object](glossary.md#control-object) is stored as the document itself.

The checksum covers the payload bytes exactly as they appear in the document, including a JSON fragment's internal whitespace and spelling. It doesn't cover the rest of the envelope (so a JSON envelope's own whitespace isn't part of it), the compressed representation, or a second encoding of the decoded payload. Neither a writer nor a reader parses and serializes the payload again to calculate its checksum.

Every manifest and every WAL object is compressed, so a reader doesn't check whether an object of those families is compressed. (The reference writer uses zstd level 3, and a reader accepts any level.) A reader decompresses the object first, and it refuses a document longer than the family's limit: `MAX_NAMESPACE_MANIFEST_BYTES` for a manifest (see [Control and manifest payloads](#a4-control-and-manifest-payloads)) or `MAX_WAL_OBJECT_BYTES` for a WAL object (see [WAL records](#a5-wal-records)).

A reader then identifies the declared kind and version, validates the envelope and checksum, and interprets the payload under the supported schema. Field and protocol validation still apply after the checksum matches, because a matching checksum doesn't make an inconsistent record valid.

Why are manifests and WAL objects compressed, but control objects aren't? A manifest grows with the namespace and a WAL object grows with its commits, and a reader with an empty cache loads both. For example, a manifest of 193 KiB of JSON is 36 KiB compressed. A control object is under 1 KiB, so compressing it would save almost nothing.

### A.3 Content references and checksums

A `blob_v1` reference contains all of these fields:

| Field | Meaning |
| --- | --- |
| `kind` | `blob_v1`. |
| `owner_namespace_id` | The namespace that originally wrote the content. |
| `content_id` | `con_` followed by 32 random lowercase hexadecimal characters. |
| `size_bytes` | The length of the file, which is the prefix of the chain that the reference identifies. |
| `checksum` | The algorithm and digest of those bytes. |

For example, the 15 UTF-8 bytes `Hello, LoonFS!\n` (ending in a single LF) have this reference. The ID is illustrative, while the size and CRC-64/NVME checksum are calculated from those bytes:

```json
{
  "kind": "blob_v1",
  "owner_namespace_id": "demo",
  "content_id": "con_0123456789abcdef0123456789abcdef",
  "size_bytes": 15,
  "checksum": {
    "algorithm": "crc64nvme",
    "value": "c569275606d1d1a0"
  }
}
```

[File contents and ownership](#15-file-contents-and-ownership) defines content identity and key derivation.

A checksum is `{ "algorithm": <name>, "value": <lowercase hex> }`:

| Algorithm | Value width |
| --- | --- |
| `crc64nvme` | 16 hexadecimal characters |
| `crc32c` | 8 hexadecimal characters |

The value is the CRC result written as a big-endian integer, so the hex is the checksum bytes in wire order.

What a checksum covers depends on the field that contains it. A content reference's checksum covers the prefix it identifies, an upload-content claim covers the complete object, and a multipart part checksum covers one part. An algorithm selector without a value isn't a checksum. Provider encodings are converted to this representation before the stored value is built.

### A.4 Control and manifest payloads

These tables list the durable payload fields. The protocol sections define how they change.

| Payload | Fields |
| --- | --- |
| Namespace hint | `namespace_id`, `manifest_no` |
| Writer block | `writer_id`, `acquired_at_ms` |
| Fork basis | `manifest`, `source_pin_id` |
| Manifest reference | `owner_namespace_id`, `manifest_no`, `head_seq`, `payload_checksum` |
| Pin record | `namespace_id`, `pin_id`, `head_seq`, `payload_checksum`, `created_at_ms`, `owner` |
| Upload session record | `namespace_id`, `upload_id`, `content_id`, optional `subject_id`, `mode`, `expires_at_ms` |
| Upload outcome record | `namespace_id`, `upload_id`, `outcome` |

Namespace status is `{"kind":"active"}`, or `{"kind":"deleted"}` with a required `deleted_at_ms` (only on the deleted variant). A missing status is invalid. The genesis commit ID is `c_00000000000000000000000000000000`.

Pin owners have the fields in [Records and owners](#81-records-and-owners), and a pin has no status field. Upload records use these tagged variants, and both use `kind` tags:

- Mode, in the session record: `service_proxied` has no other fields, `direct_put` contains `checksum_algorithm`, and `direct_multipart` contains `provider_upload_id`, `part_size_bytes`, and `checksum_algorithm`.
- Outcome, in the outcome record: `completed` with `completed_at_ms` and `content_ref`, or `aborted` with `aborted_at_ms`.

A namespace manifest contains:

| Field | Meaning |
| --- | --- |
| `namespace_id` | The namespace that the manifest describes. |
| `created_at_ms` | The namespace's creation time. |
| `created_by` | The application-supplied actor that created or forked the namespace. |
| `access` | The access mode, which can't change after creation: `{"kind":"unrestricted"}`, or `{"kind":"acl"}` with `principal_scope` and `root_grants`. |
| `naming` | The naming mode, which can't change after creation: `"case_insensitive"` or `"case_sensitive"` (see [Names and paths](#14-names-and-paths)). |
| `fork_basis?` | The source reference and pin identity, which can't change after creation. |
| `status` | Whether the namespace is active or deleted. |
| `writer?` | The diagnostic writer block. |
| `folded_wal_no` | The highest local WAL number included in the file set. |
| `manifest_no` | A positive number that matches the object key. |
| `compactor_epoch` | The current compaction authority. |
| `head_seq` | The materialized head sequence (on deletion, the final namespace sequence). |
| `activity` | The required cumulative activity counters (see [Statistics](#74-statistics)). |
| `writer_epoch` | The current writer authority. |
| `next_inode_id` | The first inode ID available at the recorded boundary. |
| `next_run_no` | The next run number to allocate. |
| `retention_floor_seq` | The earliest sequence covered by incremental replay and row retention. |
| `runs` | The complete list of materialized metadata runs. |

A run contains `run_no`, `run_seq`, `tier`, and `segments`, and its tier is `delta` or `base`. A segment descriptor contains:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | The namespace that stores the segment. |
| `segment_id` | The segment's immutable generated identity. |
| `family` | The metadata row family. |
| `encoding` | A required unsigned integer that is the segment's version, which covers its block layout and its rows. The value is 1. |
| `row_count` | A positive number of stored rows. |
| `min_row_key`, `max_row_key` | The inclusive key range. |
| `index_block`, `filter_block` | The index and filter handles. |
| `filter_inline?` | The exact stored filter bytes, as lowercase hexadecimal. |

`base_seq` is the oldest non-empty run's sequence, or `head_seq` when there's no such run, and it isn't stored. Segments follow their list order within each family. The owner and segment ID determine the object key, and the descriptor stores no separate path or compaction-job identity.

`MAX_NAMESPACE_MANIFEST_BYTES` is 512 MiB (536,870,912 bytes) for the complete decompressed manifest document, including its envelope. Readers refuse larger documents, and a publication whose manifest document would be larger fails before its put. A manifest lists every segment of its namespace, so this limit also caps how much metadata a namespace can have. (A namespace of one million files has a manifest document of about 193 KiB, so the limit is reached at about 2.7 billion files.) This is a format constraint, because every published manifest MUST stay readable with bounded decompression.

### A.5 WAL records

`MAX_WAL_OBJECT_BYTES` is 512 MiB (536,870,912 bytes) for the complete decompressed WAL document, including its envelope. Writers keep every WAL object within this limit through request and batch admission, and readers refuse larger documents. A writer builds each batch so that the sum of its requests' bounds, plus the document overhead, stays within the limit. This is a format constraint, because every successful publication MUST stay readable with bounded decompression.

A WAL object's payload contains `namespace_id`, `wal_no`, `writer_epoch`, `head_seq`, `next_inode_id`, and `records`.

For a data WAL object, `prior_head_seq` is derived (with checked subtraction) from the first record's sequence, and `records` covers the sequences after it through `head_seq`, without gaps. A first sequence of 0 is invalid. The WAL number MUST match the key, and the allocation high-water mark MUST agree with replay. A fence has an empty record list and an unchanged head and allocator, so its derived `prior_head_seq` equals `head_seq`. Fences take part in WAL numbering and epoch validation, but they produce no logical changes.

Each commit contains `committed_seq`, `commit_id`, `committed_by`, `semantic_commit_fingerprint`, `committed_at_ms`, an optional `message`, `deltas`, and an optional `inline_content`. A delta wrapper contains `semantic_operation_index` and `delta`, which is a kind-tagged object with these fields:

| Delta kind | Fields after `kind` |
| --- | --- |
| `create_inode` | `delta_index`, `inode_id`, `inode_kind` |
| `bind_direntry` | `delta_index`, `parent_inode_id`, `name_key`, `display_name`, `child_inode_id`, `child_kind`, `child_created_by`, `child_created_at_ms` |
| `unbind_direntry` | `delta_index`, `parent_inode_id`, `name_key`, `display_name`, `child_inode_id`, `child_kind`, `child_created_by`, `child_created_at_ms`, `target` |
| `append_file_revision` | `delta_index`, `inode_id`, `revision_no`, `content_ref`, `layout?` |
| `tombstone_subtree` | `delta_index`, `root_inode_id`, `deleted_binding` |
| `revoke_subtree_tombstone` | `delta_index`, `root_inode_id`, `target` |
| `expire_subtree_tombstone` | `delta_index`, `root_inode_id`, `target` |
| `append_attributes_revision` | `delta_index`, `inode_id`, `attributes_revision_no`, `attributes` |
| `append_access_revision` | `delta_index`, `inode_id`, `access_revision_no`, `boundary`, `grants` |

A delta's own commit sequence is implied by its commit. An unbind target and a tombstone target are `{seq, delta_index}`, the position of the exact event that the delta removes, revokes, or expires. A deleted binding is `{parent_inode_id, name_key, display_name}`. Attribute and access deltas contain the complete resulting state, including an empty map after a clear.

A revision delta's `layout` follows these rules:

- It can be left out only when the commit includes a piece at offset 0 for its content ID, or a piece for a chain that already has a revision in the planning view. The same-chain case is safe, because the base stays the inode's head until the append is folded, so retention keeps a revision that refers to the chain, and layout compaction can't prune its row.
- A commit with a piece at offset 0 can't include a layout.
- A present layout validates against its own `size_bytes()`, and its extents can refer to other owners and content IDs. Its size equals the lowest offset of the commit's pieces for that content ID, or the reference's `size_bytes` if the commit includes no piece for it. A failed check is `InvalidWalRevisionLayout`.
- Upload evidence provides one whole extent. Copies, restores, and new chains include the required prefix from the planning view. Before it admits the reference, validation writes any pending pieces that prefix needs to content objects, the same way a fold does. The resulting layout refers to those objects even if compaction later removes the source chain's row.

`inline_content` is a list of pieces, `{content_id, offset, bytes}`, where `bytes` is a CBOR byte string that the chain contains starting at `offset`. The field is left out when it's empty, and it defaults to an empty list when absent. The reference that a piece goes with is an ordinary `blob_v1` reference. A whole value is a piece at offset 0. A piece past offset 0 continues an existing chain, or starts a new chain whose delta includes the layout of the first `offset` bytes. An append to zero bytes starts a new content ID with a whole value. A chain of zero bytes is never appended to, so its download grant can sign no range (see the [API specification][api-spec]).

Encoding and decoding enforce these rules, using only the WAL object:

1. Every entry's `content_id` is referenced by an `append_file_revision` delta in the same commit, whose `content_ref.owner_namespace_id` equals the WAL object's `namespace_id`.
2. The entries for one content ID in one commit, ordered by offset, are contiguous, and the last one ends at the largest `size_bytes` of those references. A zero-length entry is valid.
3. A delta without pieces for its content ID requires a layout. A first piece past offset 0 allows an absent layout, and a present layout covers exactly `[0, offset)`. A first piece at offset 0 can't have a layout. The planner checks that a new chain includes a layout, because the decoder can't see earlier revisions.
4. A commit contains at most one entry per content ID and offset.
5. Each entry contains at most `MAX_WAL_INLINE_CONTENT_BYTES` (256 KiB, or 262,144 bytes), and the sum of all entry lengths in one WAL object is at most `MAX_WAL_OBJECT_INLINE_CONTENT_BYTES` (4 MiB, or 4,194,304 bytes).

These are reader limits, and writer thresholds are policy at or below them. `MAX_WAL_OBJECT_BYTES` still limits the complete decompressed document. A delta can refer to content that has no inline entry. Replay doesn't hash pieces against the reference's checksum, because the envelope's `payload_checksum` covers the stored payload bytes. The decoder can't check whether the objects a layout refers to exist, so a read or fold that finds their bytes missing reports corruption.

WAL replay applies these normalized records in sequence and delta order. It doesn't run the original request's preconditions again, or interpret the request again under a newer planner.

### A.6 Metadata rows and row keys

Rows are kind-tagged CBOR objects in the data blocks. The row-kind schema and the row-family ordering are separate, so the same `direntry_binding` row appears in both binding families.

| Row kind | Fields after `kind` |
| --- | --- |
| `inode` | `inode_id`, `inode_kind`, `committed_seq`, `commit_id`, `committed_by`, `committed_at_ms` |
| `direntry_binding` | `parent_inode_id`, `name_key`, `child_inode_id`, `child_kind`, `child_created_by`, `child_created_at_ms`, `committed_seq`, `delta_index`, `state` |
| `file_revision` | `inode_id`, `revision_no`, `committed_seq`, `commit_id`, `committed_by`, `committed_at_ms`, `delta_index`, `content_ref` |
| `tombstone` | `root_inode_id`, `committed_seq`, `delta_index`, `commit_id`, `action`, `committed_by`, `committed_at_ms` |
| `active_deletion` | `root_inode_id`, `deletion_seq`, `action` |
| `commit_receipt` | `commit_id`, `committed_seq` |
| `commit` | `committed_seq`, `commit_id`, `committed_by`, `semantic_commit_fingerprint`, `committed_at_ms`, `message?`, `deltas` |
| `content_layout` | `owner_namespace_id`, `content_id`, `committed_seq`, `size_bytes`, `layout` |
| `attributes_revision` | `inode_id`, `attributes_revision_no`, `committed_seq`, `commit_id`, `delta_index`, `committed_by`, `committed_at_ms`, `attributes` |
| `access_revision` | `inode_id`, `access_revision_no`, `committed_seq`, `commit_id`, `delta_index`, `committed_by`, `committed_at_ms`, `boundary`, `grants` |

A `commit` row is the WAL commit record from [WAL records](#a5-wal-records), without its inline content. The `inline_content` field is left out, and a row that includes one is invalid.

For a directory binding, `state` is `{"kind":"bound","display_name":...}` or `{"kind":"unbound"}`. Both binding families store the complete row.

For a tombstone, the top-level `committed_seq` and `delta_index` fields store the row's own position. A revoke or expire target is a `DeltaPosition` value, `{seq, delta_index}`. A set action is `{"kind":"set","deleted_binding":...}`, a revoke action is `{"kind":"revoke","target":...}`, and an expire action is `{"kind":"expire","target":...}`.

An active-deletion `listed` action contains `inode_kind`, `deleted_at_ms`, `deleted_by`, and `deleted_binding`, and a `removed` action contains `removed_seq`, the sequence of the undelete or expiry that removed the deletion from the listing. These are nested action fields, not extra top-level fields on every active-deletion row. The `inode_kind` is copied from the deleted root inode, and a `listed` action without it fails decoding.

Each row key starts with its family's prefix, followed by hyphen-separated components. Unsigned 64-bit components use 20 decimal digits, and unsigned 32-bit components use 10, with leading zeros. Variable names and commit IDs are the lowercase hexadecimal encoding of their UTF-8 bytes, so a name's own punctuation is never read as a component separator. Content-layout keys use the content ID directly.

In the following grammar, `u64::MAX - x` and `u32::MAX - x` mean subtraction before the fixed-width decimal encoding. They aren't literal text in a key.

| Family | Row key |
| --- | --- |
| `inodes` | `inode-{inode_id:020}` |
| `direntry_binds` | `direntry-bind-{parent_inode_id:020}-{name_key_hex}-{child_inode_id:020}-{committed_seq:020}-{delta_index:010}` |
| `direntry_child_binds` | `direntry-child-bind-{child_inode_id:020}-{parent_inode_id:020}-{name_key_hex}-{committed_seq:020}-{delta_index:010}` |
| `revisions` | `revision-{inode_id:020}-{u64::MAX - revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |
| `tombstones` | `tombstone-{root_inode_id:020}-{committed_seq:020}-{delta_index:010}` |
| `active_deletions` | `active-deletion-{deletion_seq:020}-{root_inode_id:020}-{sort_rank:010}` |
| `commit_receipts` | `commit-receipt-{commit_id_hex}-{committed_seq:020}` |
| `commits` | `commit-{committed_seq:020}` |
| `content_layouts` | `content-layout-{content_id}-{u64::MAX - size_bytes:020}-{u64::MAX - committed_seq:020}` |
| `attributes` | `attribute-{inode_id:020}-{u64::MAX - attributes_revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |
| `access` | `access-{inode_id:020}-{u64::MAX - access_revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |

The two binding families contain the same edge events in two orders (an edge is a parent, a name key, and a child):

- The slot index sorts a slot's rows by child, and then by position, oldest first.
- The child index sorts a child's rows by parent, then by name key, and then by position, oldest first.

So key order isn't position order within a slot or a child, and a read selects the greatest visible position among a slot's rows or a child's rows.

Compaction groups both families by edge. While it scans one edge, it keeps at most one candidate floor row. It also tracks the last edge bound at the floor, across the edges of one slot in the slot index, and across the edges of one child in the child index. Commit validation enforces one child per slot and one parent per child (see [Directory bindings](#13-directory-bindings)), so a second child bound in one slot at the floor, or a second parent bound for one child at the floor, is corruption, and compaction refuses it.

Ascending byte order scans an inode's file revisions, attributes, and access rows newest first, because their keys subtract the revision number from the maximum value. The active-deletion `sort_rank` is 0 for a removed entry and 1 for a listed entry, and it's still stored as ten digits.

Bloom filters use these keys, which aren't always full row keys:

| Family | Filter key |
| --- | --- |
| `inodes` | The complete row key |
| `direntry_binds` | `direntry-bind-{parent_inode_id:020}-{name_key_hex}` |
| `direntry_child_binds` | `direntry-child-bind-{child_inode_id:020}` |
| `revisions` | `revision-{inode_id:020}` |
| `tombstones` | `tombstone-{root_inode_id:020}` |
| `active_deletions` | The complete row key |
| `commit_receipts` | `commit-receipt-{commit_id_hex}` |
| `commits` | The complete row key |
| `content_layouts` | `content-layout-{content_id}` |
| `attributes` | `attribute-{inode_id:020}` |
| `access` | `access-{inode_id:020}` |

A layout row records a chain's objects as of the commit or fold that wrote its bytes. The first row under a content ID's prefix is the longest layout (the newest, among layouts of equal length), and shorter rows are superseded. Compaction keeps the first row for each ID in its inputs, subject to the pruning rule in [Row retention during a base compaction](#103-row-retention-during-a-base-compaction). A layout row's `size_bytes` is the layout's size, which can be different from the reference's size.

A `layout` contains `extents`, in chain order. Each extent contains `owner_namespace_id`, `content_id`, `object`, `offset`, and `length`, and its object is `{"kind":"whole"}` or `{"kind":"span","start":...,"end":...}`. A layout MUST follow these rules:

- The lengths add up to `size_bytes`, and each length is positive.
- Zero bytes require exactly one whole extent of length zero.
- Each owner namespace and content ID is valid.
- Span bounds satisfy `start < end`, and the extent's object range fits within `end - start`.

A shared prefix can shorten an extent without changing its span key, and readers honor nonzero object offsets.

The family groups are:

| Group | Members |
| --- | --- |
| `bindings` | `direntry_binds`, `direntry_child_binds` |
| `revisions` | `revisions` |
| `inodes` | `inodes` |
| `tombstones` | `tombstones` |
| `active_deletions` | `active_deletions` |
| `commits` | `commits`, `commit_receipts` |
| `content_layouts` | `content_layouts` |
| `attributes` | `attributes` |
| `access` | `access` |

### A.7 Block-segment encoding

A segment is a concatenation of independently readable sections:

```text
[zstd data block 0]
[zstd data block 1]
...
[uncompressed bloom filter]
[zstd index block]
```

There's no segment header or footer. The manifest descriptor provides the filter and index handles, and the index provides the data-block handles, so a segment can't be opened from its own bytes without the descriptor.

A metadata segment descriptor's `encoding` is the version of this layout and of the rows its data blocks contain. Grep segments take their version from the grep manifest that references them.

Each handle is an encoded object with these fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `offset` | `u64` | The zero-based byte offset in the complete segment. |
| `stored_bytes` | `u32` | The number of stored bytes in the section. |
| `decoded_bytes` | `u32` | The expected length after decompression (for a filter, the uncompressed length). |
| `crc32c` | `u32` | The CRC32C of the exact stored section bytes. |

The verified put checks the whole object when it's written.

Each section MUST pass three checks: the returned byte count matches `stored_bytes`, the CRC of the stored bytes matches `crc32c`, and the decoded length matches `decoded_bytes`. Both lengths fit in `u32`. Readers stop decompressing after at most `decoded_bytes + 1` bytes, so the extra byte reports a decoded-length mismatch without expanding the rest of an invalid section. This limits the decompressed output, but not vector capacity or the total memory of decoded rows and parser state. [Reference-implementation sizing and admission defaults](#c4-reference-implementation-sizing-and-admission-defaults) describes initial allocation sizing.

#### Data blocks

Before compression, a data block is made up of entries, a restart-offset array, and a four-byte restart count. An entry contains:

```text
shared_prefix_len     unsigned LEB128
key_suffix_len        unsigned LEB128
key_suffix            key_suffix_len UTF-8 bytes
row_len               unsigned LEB128
row                    row_len CBOR bytes
```

The key is the first `shared_prefix_len` bytes of the previous key, followed by `key_suffix`. The prefix MUST end on a valid UTF-8 boundary, and keys are in ascending order. The block grammar can represent adjacent equal keys, but the uniqueness rule for metadata runs still applies to metadata producers.

The first entry, and every sixteenth entry after it, starts a restart and encodes the full key with a shared-prefix length of zero. The restart array contains their offsets relative to the start of the entry region, each as a little-endian `u32`. Readers verify each offset and reject missing or unused offsets. Restarts limit shared-prefix reconstruction to groups of sixteen rows. The array is followed by its count, also a little-endian `u32`, and the entries and restart data are compressed together as one zstd section.

Metadata and grep data blocks use the same entry and restart layout.

Writers encode unsigned LEB128 values with seven payload bits per byte, and with the high bit set when another byte follows. The last byte has no continuation bit. Encoded values fit in `u64`, so in a ten-byte encoding, only one payload bit is available in the final byte. The writer uses the shortest representation of each value.

#### Filter block

The filter is uncompressed, and it has this byte layout:

```text
n_hashes              u32, little-endian
bit_len               u64, little-endian
bits                  ceil(bit_len / 8) bytes
```

Version 1 requires `n_hashes` to be seven and `bit_len` to be at least 64, and readers reject other header values before probing. The reference producer reserves ten bits per inserted filter key, with a minimum of 64 bits, and repeated filter keys count as separate insertions for sizing. The number of stored bit-array bytes MUST equal `ceil(bit_len / 8)`.

Hashing is XXH64 over the filter key's UTF-8 bytes, with two constant seeds:

```text
h1 = XXH64(key, seed = 0)
h2 = XXH64(key, seed = 0x9e3779b97f4a7c15)
```

For each probe `i` from zero through `n_hashes - 1`, calculate `(h1 + i * h2)` with unsigned 64-bit wrapping arithmetic, and then take the remainder modulo `bit_len`. Bit `b` is stored in byte `b / 8`, at the mask `1 << (b % 8)`.

A valid negative filter result means the lookup can skip the segment. A positive result doesn't replace the row-level visibility rules.

When `filter_inline` is present in the descriptor, it's the lowercase hexadecimal encoding of these exact stored filter bytes, and it MUST have the filter handle's stored length and checksum. Its presence doesn't remove the filter block from the segment, and a reader without an inline copy fetches the section by its handle.

#### Index block

The decoded index is a CBOR list. Each entry contains `last_row_key` and `block`, where `block` is the corresponding data-block handle. The list is zstd-compressed as one section.

Index entries are ordered by their last keys. Data-block byte ranges are contiguous and MUST NOT overflow. The filter comes right after the data region, and the index comes right after the filter, at the end of the object. The descriptor's handles MUST agree with that layout.

A range lookup uses the last keys to find candidate blocks. Readers verify each fetched section before they decode its rows, and they apply the descriptor's family and key-range constraints.

### A.8 Object keys

These patterns define the core object families. A segment's owner can be different from the namespace that reads it.

| Family | Object key pattern |
| --- | --- |
| WAL objects | `namespaces/{namespace_id}/wal/{wal_no:020}.wal.zst` |
| Namespace manifests | `namespaces/{namespace_id}/manifests/{manifest_no:020}.json.zst` |
| Pin records | `namespaces/{namespace_id}/pins/{pin_id}.json` |
| Metadata segments | `namespaces/{owner_namespace_id}/segments/{segment_id}.sst.zst` |
| Upload sessions | `namespaces/{namespace_id}/uploads/{upload_id}.json` |
| Upload outcomes | `namespaces/{namespace_id}/uploads/{upload_id}.outcome.json` |
| Hint | `namespaces/{namespace_id}/hint.json` |
| Content objects | `namespaces/{owner_namespace_id}/content/{content_id}` |
| Content span objects | `namespaces/{owner_namespace_id}/content/{content_id}-{start:020}-{end:020}` |
| Temporary objects | `namespaces/{namespace_id}/temporary/{temporary_id}` |

## Appendix B. Semantic commit fingerprints

A commit fingerprint is stored as `v1:sha256:<64 lowercase hex>`. The scheme identifies the canonicalization rules below, not the API's general-purpose JSON serialization.

The fingerprint is the SHA-256 of compact UTF-8 JSON with these top-level fields, in this exact order:

```text
{
  "domain": "loonfs.commit.semantic.v1",
  "namespace_id": <namespace string>,
  "actor_id": <actor ID string>,
  "subject_id": <subject ID string or null>,
  "operations": <ordered canonical operations>,
  "message": <string or null>,
  "preconditions": <ordered canonical preconditions>
}
```

The layout above illustrates the schema, and the actual preimage bytes contain no formatting whitespace. The `preconditions` member is always present (and `[]` for an empty list), and `message` and `subject_id` are always present (and `null` when absent).

The `namespace_id`, `actor_id`, `subject_id`, message, operation order, and caller preconditions are all significant. A changed `actor_id` is a changed logical request, even if a different process is retrying on behalf of the same application. The principal scope isn't included, because a namespace has one scope and refuses every other. The commit ID itself, the writer epoch, and the commit timestamp aren't included either.

### B.1 Operation fields

Every operation starts with `kind`, followed by the fields in the order below. Every listed field is written, including unset optional fields (as `null`) and default booleans or behavior values.

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
| `undelete` | `inode_id`, `deletion_seq`, `destination_path`, `destination_parent_inode_id`, `destination_display_name` |
| `expire_deletion` | `inode_id`, `deletion_seq` |
| `update_attributes` | `path`, `set`, `remove`, `expected_inode_id`, `expected_attributes_revision_no` |
| `update_attributes_by_inode` | `inode_id`, `set`, `remove`, `expected_attributes_revision_no` |
| `update_access` | `path`, `boundary`, `grants`, `expected_inode_id`, `expected_access_revision_no` |
| `update_access_by_inode` | `inode_id`, `boundary`, `grants`, `expected_access_revision_no` |

`expire_deletion` is the writer's expiry of one deletion, which no request can contain (see [Operations and WAL deltas](#66-operations-and-wal-deltas)). Like `undelete`, it identifies the deletion by its root's `inode_id` and its `deletion_seq`. An expiry commit's preimage has the `actor_id` `loonfs`, a `null` `subject_id` and `message`, and an empty precondition list, so the deletions it expires, in order, are what distinguish one expiry from another.

Paths use their validated canonical absolute form, and display-name fields contain one validated component. Inode IDs in these operations use their numeric storage representation, not public `ino_` strings. Sequence and revision numbers are JSON integers, and binding versions keep their opaque string representation.

Attribute `set` keys are sorted in lexicographic UTF-8 byte order, and the `remove` list is sorted and deduplicated. The order of operations isn't sorted or otherwise changed.

### B.2 Content in a fingerprint

A content reference is represented by exactly these fields, in this order:

```json
{"kind":"blob_v1","content_id":"con_0123456789abcdef0123456789abcdef","size_bytes":15}
```

The owner namespace and the checksum aren't included in the preimage, and neither is the delta's `layout`, which records physical placement, not request identity. Every reference a commit can admit is owned by the committing namespace, so the owner would repeat the `namespace_id` that the preimage already includes. The checksum is verification evidence, not a second identity. Both fields are still present and validated on the actual reference (leaving them out of the fingerprint doesn't make them optional on a commit).

Two uploads of identical bytes have different IDs, so they have different fingerprints. A retry reuses the original reference, instead of repeating the upload and using a new one.

Content that a commit includes inline has no object to refer to, so it's identified by its bytes. Each operation whose reference is for inline content uses this form in its `content_ref` member instead:

```json
{"kind":"inline_v1","sha256":"<64 lowercase hex>","size_bytes":15}
```

`sha256` is the lowercase hexadecimal SHA-256 of the complete content, and the fields appear in the order shown. The fingerprint hashes the request bytes separately from the content reference's checksum, and the content ID and owner aren't included. The form follows how the request provided the content, not where the bytes are stored. Content provided inline keeps this form even if the writer stages it, and staged or uploaded content always keeps the reference form. So the same bytes, sent once inline and once as an uploaded object, produce different fingerprints.

The bytes an append adds don't have a reference until the writer plans the commit, so the `inline_content` member of `append_file` and `append_file_by_inode` uses a third form:

```json
{"kind":"append_v1","sha256":"<64 lowercase hex>","size_bytes":6}
```

`sha256` is the lowercase hexadecimal SHA-256 of the appended bytes alone, and `size_bytes` is their length.

### B.3 Preconditions

The precondition list comes after `message`, and it keeps the caller's order, without sorting or deduplication. Each precondition starts with `kind`, followed by:

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

Precondition inode IDs use their numeric storage representation, not public `ino_` strings. Every listed field is written, and optional fields are `null` when absent.

Precondition sequence and revision values are JSON integers. Paths use their validated absolute spelling, display names use one validated component, and binding versions stay opaque strings. Preconditions affect request identity and validation, but they add no separate WAL field or replay delta.

### B.4 Strings, integers, and example bytes

Non-ASCII characters are encoded directly as UTF-8. JSON quotes, backslashes, and control characters are escaped, and `/` isn't escaped. Integers use decimal digits without leading zeros. The field order above is kept as specified (it isn't a general instruction to sort every JSON object's fields alphabetically).

For example, this is the complete canonical preimage for one directory-creation request. The hashed bytes have no trailing newline:

```json
{"domain":"loonfs.commit.semantic.v1","namespace_id":"demo","actor_id":"usr_8f3c","subject_id":null,"operations":[{"kind":"create_directory","path":"/reports","parents":false}],"message":null,"preconditions":[]}
```

Its fingerprint is:

```text
v1:sha256:2d44c045a1d4eaaf3e808abbe1088b581fa51292cd410bc34dfd8e63f67140d3
```

A one-operation convenience call and a one-element commit request use the same canonical input. The wire request can leave out defaults that the canonical operation writes explicitly, so its raw request JSON isn't the fingerprint preimage.

The complete shared vectors are in [commit_fingerprints_v1.json][fingerprint-vectors]. They cover every operation, every precondition variant, path bindings with and without binding versions, path absence, an empty precondition list, and operations with and without inline preconditions. Encoders MUST preserve those exact bytes and digests.

## Appendix C. Timing and size reference

Publication and garbage collection use the timing relationships below, and [C.4](#c4-reference-implementation-sizing-and-admission-defaults) lists the configurable sizing targets separately. The [limits module][limits-source] defines the reference values.

### C.1 Publication and collection timing

| Constant | Milliseconds | Meaning |
| --- | ---: | --- |
| `WAL_PUBLISH_BUDGET_MS` | 300,000 | From observing the planning tip to starting the next numbered put. |
| `METADATA_PUBLICATION_BUDGET_MS` | 900,000 | From the start of a bounded manifest publication (including an epoch claim) to starting its put. |
| `PROVIDER_OPERATION_DEADLINE_MS` | 120,000 | The shared retry budget for a client operation. |
| `PROVIDER_ATTEMPT_TIMEOUT_MS` | 30,000 | One attempt of a control operation. |
| `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` | 255,000 | The retry budget, the final backoff, and a payload-sized final request. |
| `GC_SAFETY_MARGIN_MS` | 180,000 | The combined allowance for relative clock error, timestamp precision, and scheduling. |
| `GC_MIN_GRACE_WINDOW_MS` | 2,490,000 | The derived minimum ordinary garbage collection grace. |
| `READ_REVALIDATION_BOUND_MS` | 1,155,000 | The longest gap between a previous basis confirmation and the answer to the next successor check, for a cached view, a writer, or a writer position. |
| `GC_DEFAULT_GRACE_WINDOW_MS` | 3,600,000 | The default configured ordinary grace. |
| `UNREFERENCED_SEGMENT_GRACE_WINDOW_MS` | 86,400,000 | An unreferenced segment MUST be strictly older than this before it's deleted. |
| `METADATA_COMPACTION_BUDGET_MS` | 83,910,000 | The longest time before a compaction starts its publication. |
| `DIRECT_TRANSFER_URL_TTL_MS` | 900,000 | The lifetime of a direct transfer capability. |
| `NAMESPACE_RETIREMENT_GRACE_WINDOW_MS` | 3,645,000 | The minimum grace after the deletion call's clock. |

A provider retry can be admitted before its operation deadline, wait up to 15 seconds of backoff, and then take up to 120 seconds for a payload-sized request. The object-store layer never resends a conditional write (see [Required operations](#31-required-operations)), but a conditional WAL or manifest write can still be larger than the payload threshold in its one request. So the publication allowance reserves 120 + 15 + 120 seconds, not the 30-second timeout for small requests. The allowance covers the request phase, not reading the response body. It keeps the relative-clock and scheduling assumptions, and it doesn't prove that a timed-out remote change had no effect.

```text
GC_MIN_GRACE_WINDOW_MS
    = max(WAL_PUBLISH_BUDGET_MS, METADATA_PUBLICATION_BUDGET_MS,
          READ_REVALIDATION_BOUND_MS + DIRECT_TRANSFER_URL_TTL_MS)
      + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
      + GC_SAFETY_MARGIN_MS

READ_REVALIDATION_BOUND_MS
    = max(WAL_PUBLISH_BUDGET_MS, METADATA_PUBLICATION_BUDGET_MS)
      + PROVIDER_PUBLICATION_REQUEST_BOUND_MS

METADATA_COMPACTION_BUDGET_MS
    = UNREFERENCED_SEGMENT_GRACE_WINDOW_MS - GC_MIN_GRACE_WINDOW_MS

NAMESPACE_RETIREMENT_GRACE_WINDOW_MS
    = METADATA_PUBLICATION_BUDGET_MS
      + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
      + GC_MIN_GRACE_WINDOW_MS
```

The configured grace `T` can't be lower than the minimum, and retirement uses the greater of `T` and the retirement minimum. Segment age and the completed-content grace use their own constants. Changing a publication bound, or its safety relationship, changes the protocol, not just a scheduling preference.

### C.2 Upload admission

| Constant | Milliseconds | Meaning |
| --- | ---: | --- |
| `UPLOAD_SESSION_LEASE_MS` | 86,400,000 | An open session's lifetime on the creating host. |
| `COMPLETED_UPLOAD_RECEIPT_WINDOW_MS` | 604,800,000 | The seven-day window in which completed content can issue new receipts. |
| `CONTENT_RECEIPT_TTL_MS` | 3,600,000 | The one-hour token lifetime. |
| `COMPLETED_UPLOAD_ADMISSION_WINDOW_MS` | 608,400,000 | The receipt issuance window plus the final token's lifetime. |
| `CONTENT_RECLAMATION_GRACE_WINDOW_MS` | 610,890,000 | The admission window plus the minimum garbage collection grace. |

The completed-content interval is seven days, one hour, forty-one minutes, and thirty seconds. Deletion still depends on the namespace's state and on reference evidence, and reaching that age doesn't delete referenced content.

A direct multipart upload is also subject to the provider's transfer limits. The current reference limits are 10,000 parts, a 5 MiB minimum configured part size, a 5 GiB maximum configured part size, and 1,000 part capabilities per signing request. These limits don't override a provider's lower maximum object size, or other constraints of supported providers.

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

Attribute limits count the original UTF-8 strings, before JSON escaping or CBOR encoding. Path limits apply to canonical request paths.

### C.4 Reference-implementation sizing and admission defaults

These are defaults for the reference producer and runtime. A target size can be exceeded by one large row, because rows are never split to meet a size target, and the stored encoding stays the same when a producer changes these values.

| Setting | Value |
| --- | ---: |
| Target decoded data-block size | 64 KiB |
| Inline filter threshold | 1,024 stored bytes |
| Target segment rows | 65,536 |
| Target decoded segment size | 8 MiB |
| Maximum compaction input runs | 8 |
| Maximum compaction input rows in one metadata maintenance pass | 131,072 |
| Maximum decoded input one merge keeps in memory | 64 MiB |
| Automatic WAL-fold threshold | 32 WAL objects |
| Idle WAL-fold period | 15 minutes |
| Unfolded-tail write stop (new commits wait for a fold) | 128 WAL objects |
| `PublicationPolicy::write_stop_wait_budget_ms` | 30,000 ms |
| `LoonFsBuilder::manifest_revalidation_interval_ms` | 1,000 ms |
| `LoonFsBuilder::max_staleness_ms` | 1,000 ms |
| Maximum commit-message size | 4,096 bytes |

`manifest_revalidation_interval_ms` is the minimum monotonic interval between checks for a successor to the manifest of a cached view. `max_staleness_ms` is the default maximum staleness of an `eventual` read: how old the confirmation of a cached view's tip, or the last successor check of a writer position, can be for the read to use it without a probe. Neither setting can exceed `READ_REVALIDATION_BOUND_MS`, and a runtime build with a longer one fails.

A decoder can't use the target block or segment sizes as hard allocation limits. The reference block reader initially reserves at most the smaller of `decoded_bytes` and 64 KiB, and further allocation follows the bytes actually decompressed. Output stops at the declared length plus one byte (see [Block-segment encoding](#a7-block-segment-encoding)), and vector capacity can be larger than that output length. This doesn't impose a smaller maximum block size. The [API specification][api-spec] specifies request admission limits.

[Control and manifest payloads](#a4-control-and-manifest-payloads) and [WAL records](#a5-wal-records) specify the hard limits on the sizes of a manifest document and a WAL document.

## Appendix D. Grep extension format

Grep is derived from authoritative file content and namespace history. It's optional, and it's stored separately. Core manifests contain no grep pointer, index watermark, or segment references, and a new fork has no grep index until the extension builds one for it.

### D.1 Objects and publication

```text
namespaces/{namespace_id}/extensions/grep/
├── hint.json
├── manifests/{manifest_no:020}.json
└── segments/{segment_id}.sst.zst
```

The `grep_hint` version 1 JSON payload contains `namespace_id` and `manifest_no`. Enabling grep writes a hint that points to manifest 1, and then creates manifest 1 with put-if-absent. A hint collision is allowed, and the manifest put determines which installation succeeds. Later publications use the next number.

A `grep_manifest` version 1 payload contains `namespace_id`, `manifest_no`, `status`, `index`, and `segments`, and its namespace and number MUST match the key. Both envelopes verify their stored payload checksum, and they reject unknown kinds, versions, fields, and invalid nested state. The hint contains no separate manifest checksum.

`index_stored_bytes` is the sum of `index_block.offset + index_block.stored_bytes` for the manifest's referenced segments, computed with checked arithmetic, so it doesn't need any segment reads. It's reported with the grep manifest number and the full indexing status, including any partial-commit position. Confirmed absence means zero referenced index bytes, and a read failure stays an error.

Grep discovery works like core discovery:

1. Load the hinted manifest, and probe the following numbers until one returns not found. A missing hint, or a missing manifest 1, means grep isn't enabled. A missing higher hinted manifest is corruption.
2. After any not-found result, read the hint again, because garbage collection might have deleted manifests below a newer hint. Start again from that hint when it points to a later manifest, or when its answer arrives `READ_REVALIDATION_BOUND_MS` or more after a time read before discovery loaded the hint it started from.

A successor manifest MUST have the same `namespace_id` and the next manifest number, and it MUST NOT lower `next_run_no` or the indexing position in `status`. A backfill can reset its inode cursor only when it restarts from a new user pin. Disabling grep keeps `next_run_no` and drops the rest of the index state, and enabling it starts a new backfill.

A query validates a cached manifest with a HEAD request for its successor:

- It runs discovery again when the successor is present, or when the answer arrives `READ_REVALIDATION_BOUND_MS` or more after the cached manifest's previous check.
- A query whose previous check is already that old runs discovery without the HEAD.
- The query reads the time before it sends the HEAD, and that time becomes the previous check only after a not-found answer arrives within the bound.

A manifest loaded by discovery is dated by a time read before discovery began, and decoded manifests can be cached by namespace and number.

If an index watermark is ahead of the head of a query's read view, the index is still used. The query takes a new read view once before it uses that index.

A step writes segments first, and then it publishes the next manifest with put-if-absent. A publisher that loses reloads the durable state and plans again against the current inputs. If a manifest put ends with an unknown outcome, the publisher reads the manifest at the number it tried:

- If that manifest has an identical payload and the publication budget hasn't expired, the publication succeeded.
- If it has another payload, the publisher lost, and it reloads and plans its next step from the current state.
- If the number is missing, the outcome stays unknown. The publisher returns `outcome_unknown`, and it keeps the backfill's user pin that the step created, because the put might still succeed and refer to it.

A put that succeeds after the budget has expired also has an unknown outcome (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)). A successful publisher raises the hint with CAS, taking the greater number. A hint read or write that returns no compare token is a store error, and a failed hint raise doesn't undo the publication.

Every step that publishes a manifest MUST start that publication within `METADATA_PUBLICATION_BUDGET_MS`, measured from before it loads the grep manifest it builds on, and it writes no manifest after that bound. Grep uses bounded steps instead of core compaction's epoch claim. Conditional numbered publication and input revalidation control competing steps.

### D.2 Index lifecycle

The manifest's `status` contains the position fields for its lifecycle state:

| Kind | Status fields |
| --- | --- |
| `backfilling` | `captured_seq`, `pin_id` (the ID of the backfill's user pin), optional `cursor_inode_id` |
| `active` | `built_through_seq`, `next_event_index` |
| `disabled` | No other status fields |

Backfill's `captured_seq` is the user pin's captured sequence, and its cursor resumes strictly after the last inode ID. An active index's `next_event_index` is zero at a commit boundary. A disabled index contains no segments and no reorganization work.

The nested `index` contains `next_run_no` and an optional `reorganize`. Reorganization contains `input_segment_ids`, `output_segment_ids`, `row_key_cursor`, `output_level`, and `run_no`, and its cursor is inclusive. Input and output segment IDs MUST be unique, disjoint, and present in the manifest's segment list, and each output MUST have the recorded level and run number. The extension can rebuild its state from a fresh user pin.

Grep segments follow list order, and each descriptor has a positive row count. The complete descriptor fields are `segment_id`, `run_no`, `level`, `row_count`, `min_row_key`, `max_row_key`, `index_block`, `filter_block`, and an optional `filter_inline`. The block handles and checksums use the same representation as metadata segment descriptors.

Grep uses a numeric `level` instead of the core's base and delta tiers:

- Level 0 is delta output.
- Level 1 is an intermediate merge.
- Level 2 is the base.

In-progress reorganization records the output level and run number, so resumed steps continue the same run. Every descriptor and in-progress output run number MUST be below `next_run_no`.

### D.3 Tokenization and postings

A revision is eligible for the version 1 gram index when its content is at most 8 MiB, and its first 8 KiB contain no NUL byte and pass the text check. The sample MUST be valid UTF-8, except that an incomplete final character is accepted when it follows a nonempty valid prefix. This only checks the sample, so the bytes after it don't have to be UTF-8.

For each eligible revision, the tokenizer folds ASCII letters to lowercase and takes every overlapping three-byte window. Grams are bytes, not Unicode characters, and this folding is different from the Unicode rule that's used to compare file names.

For example, `Abcd` produces the byte grams `abc` and `bcd`, represented as `616263` and `626364`. A gram is encoded as six lowercase hexadecimal characters.

A row is a kind-tagged CBOR object with the kind `gram_postings`. Its fields after `kind` are `gram`, `first_inode_id`, and `postings`. The gram is a hexadecimal string, the inode ID is an integer, and `postings` is a CBOR byte string that contains the packed batch. `first_inode_id` MUST equal the first posting's inode ID. The row key is:

```text
gram-{gram_hex}-{first_inode_id:020}
```

Its filter key is `gram-{gram_hex}`. Several rows can contain batches for the same gram, within a segment or across segments, and a reader combines those batches.

A posting identifies `(inode_id, revision_no)`, not a path. Batches are strictly ordered by that pair, and they're encoded as unsigned LEB128 values:

```text
posting_count
first_inode_id
first_revision_no
next_inode_id - previous_inode_id
next_revision_no
... repeated for the remaining postings
```

Revision numbers are unsigned 32-bit absolute values, and inode IDs are unsigned 64-bit values, delta-encoded after the first. Empty batches, unordered postings, and trailing bytes after the declared batch are invalid.

The segment framing is exactly the data, filter, and index format in Appendix A, with grep CBOR rows instead of metadata rows, and per-block CRCs verify ranged reads.

Grep manifest version 1 governs the tokenizer, the posting representation, and the meaning of row keys. They aren't implementation choices that can change while an existing index is read under the same version.

### D.4 Grep collection

Each explicit call discovers the current grep manifest, builds its live segment set, and lists `manifests/` and `segments/` completely, using one supplied `now_ms`. The collector stores no durable progress cursor.

The current manifest protects all the segments it lists, including pending reorganization inputs and outputs. Manifest numbers at or above the observed hint stay available for discovery. A manifest below the current one, and every segment it lists, stays while its immediate successor is younger than the ordinary grep grace, whether it's above or below the observed hint. A successor without a provider timestamp counts as young. The collector applies this rule from the current manifest downward, and it stops at the first successor that's missing or whose provider age meets the grace. A query confirms its cached manifest within `READ_REVALIDATION_BOUND_MS` of its previous check (see [Objects and publication](#d1-objects-and-publication)), and that bound is below the grace, so a query never lists a deleted segment. An invalid or unreadable current or kept manifest stops the call before anything is deleted.

| Candidate in a live namespace | Rule |
| --- | --- |
| A manifest below the observed hint | Keep it while its immediate successor is younger than the ordinary grep grace. Otherwise, its own provider age MUST meet the ordinary grep grace. |
| A segment outside the live set | Its provider age MUST be strictly greater than 24 hours. |
| Unknown age, or an unrecognized key | Keep it. |

The ordinary grep grace is one hour in the reference implementation. The output publication budget plus the minimum garbage collection grace fits inside the segment minimum age, using the same provider and clock assumptions as core garbage collection.

For an absent or deleted core namespace, an explicit grep garbage collection call can remove the entire extension prefix after the ordinary grace, including its hint. Core garbage collection never deletes extension objects. Grep never lists namespaces, so callers and hosts choose which namespace to maintain.

[api-spec]: api.md
[provider-spec]: object-storage-providers.md
[limits-source]: ../../crates/loonfs-core/src/limits.rs
[fingerprint-vectors]: ../../crates/loonfs-types/tests/golden/commit_fingerprints_v1.json
[name-vectors]: ../../crates/loonfs-types/tests/golden/name_folding.v1.json
