# LoonFS storage format

This document describes how LoonFS stores a filesystem in object storage: the objects it writes, how they're encoded, and the rules for reading, writing, and maintaining them. An implementation that follows these rules can read and write any LoonFS namespace, and it doesn't have to offer the LoonFS HTTP API (the [API specification][api-spec] defines that interface separately).

A filesystem in object storage has to recover from a crash at any point. For example, an upload might finish, and then the metadata commit that references it might fail. The uploaded object exists, but it isn't referenced by any visible file. To recover after a restart, a reader needs a durable record of which changes were committed.

LoonFS keeps that record in an immutable write-ahead log (WAL). WAL objects are numbered in order (1, 2, 3, ...). To commit a batch of changes, a writer writes them into the next WAL object, and once that object is durably written to object storage, every change in it is committed. Periodically, LoonFS writes the committed metadata into sorted segments and publishes a numbered manifest that lists the segments a reader needs. A small object, the manifest boundary, records a manifest number at or below which no manifest can be the current one, so a reader finds the current manifest by listing the manifests above that number.

File contents are stored separately from metadata, in content objects. File content is durable no later than the commit that references it ([File contents and ownership](#15-file-contents-and-ownership) describes the two ways content becomes durable).

Everything needed to recover a namespace is stored in object storage. That includes the control records, and the metadata and content of every retained view (keeping only the latest version of each file isn't enough). Local databases and caches can be rebuilt.

The key words MUST and MUST NOT in this document are to be interpreted as described in BCP 14 ([RFC 2119](https://www.rfc-editor.org/rfc/rfc2119), [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)) when, and only when, they appear in capitals. This document also follows these conventions:

- Examples explain requirements, but they don't add fields or behavior.
- Implementation defaults are labeled as defaults, and they aren't format requirements.
- Absolute timestamps are Unix milliseconds. Budgets, lease lifetimes, and grace periods are durations in milliseconds.

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

This section describes the logical model that the stored objects represent: namespaces, inodes, commits, directory bindings, names, file contents, deletions, attributes, access rows, and the row families that store them.

### 1.1 Namespaces and identity

A "namespace" is a directory tree with its own ordered history of changes. A namespace ID identifies one namespace from creation to deletion and can't be reused. Deleting a namespace publishes a final manifest, the "namespace tombstone", which stays current until the namespace is [retired](#95-retirement). Retirement raises the namespace's [manifest boundary](glossary.md#manifest-boundary) to a retired state, and garbage collection then deletes every other object under the namespace's prefix. The boundary object stays and records that the ID was used. A request to a deleted or retired namespace, including a create or fork with its ID, returns `namespace_deleted`.

A namespace is created when its manifest 1 is written with put-if-absent. The current manifest records the namespace's identity, creation time, writer authority, and lifecycle status (`initializing`, `active`, or `deleted`). A namespace is "initializing" while its newest manifest has the status `initializing`, which means no call has finished writing the objects the namespace needs outside its prefix. Every manifest 1 is initializing, and the create or fork call makes the namespace active by publishing manifest 2 (see [Creating a namespace](#91-creating-a-namespace) and [Forking a namespace](#92-forking-a-namespace)). LoonFS treats an initializing namespace as missing, so every request with its ID except a create, a fork, or a delete returns `namespace_not_found`. Like a deleted namespace's ID, an ID that an abandoned attempt used can't be used again (see [Deleting a namespace](#94-deleting-a-namespace)). The current manifest and the WAL objects after it describe the namespace's current metadata.

Across namespaces, an item is identified by `(namespace_id, inode_id)`, and a commit position by `(namespace_id, committed_seq)`.

An inode ID is a 128-bit number, which the API spells like `ino_000000r0000013kje0d47gc8kw` (see [Numbered objects and generated IDs](#23-numbered-objects-and-generated-ids) and [Field conventions](#121-field-conventions)).

A "fork" is a new namespace that starts from a retained view of another namespace, with the same files and shared stored objects, and later commits only change the fork ([Namespace lifecycle and forks](#9-namespace-lifecycle-and-forks) describes how the shared objects are kept).

A "pool" is a set of namespaces that share one set of content objects, together with the key prefix (`pools/{pool_id}/`) that stores those objects and the upload records of every namespace in the pool (see [File contents and ownership](#15-file-contents-and-ownership)). Every manifest records the namespace's pool (`pool_id`), which can't change after creation. A plain create generates a new pool ID, such as `pool_3s0drmj8em84yb4mnetqkz2j0x`, and a fork copies its source's. A "member" of a pool is a namespace that has a [member entry](glossary.md#member-entry) in the pool and whose newest manifest records the pool, until the namespace is [retired](#95-retirement). So the members are the namespace created with the pool and every namespace forked, directly or not, from a member. [Creating a namespace](#91-creating-a-namespace) and [Content roots](#119-content-roots) describe how member entries are written and used.

Every namespace's root directory has the inode ID whose 128 bits are all zero (`ino_00000000000000000000000000`), and a new namespace starts at sequence 0. Every other inode ID has two 64-bit words. The high word is the low 32 bits of the creating commit's sequence (`committed_seq`), followed by the inode's "create number" in that commit. The create number is 1 for the first inode the commit creates, 2 for the second, and so on, counting files and directories alike, including the parent directories a commit creates for a path. The low word is random. For example, `ino_000000r0000013kje0d47gc8kw` is the second inode that commit 12 creates, and its high word is `0x0000000c00000002`. The high word orders IDs, and the low word identifies them. Create numbers start at 1, so no generated ID is all zero bits. The sequence field wraps after 2^32 commits, which changes the order of later IDs without making any invalid, and no LoonFS rule depends on the order of two IDs. A [merge](glossary.md#merge) creates each inode under its ID in the fork, so in the receiving namespace that inode's high word comes from the fork's commit. Renaming a file keeps its inode ID, but deleting a file and creating another at the same path creates a new inode with a new ID.

Version 1 of the format defines two inode kinds, `directory` and `file` ([Reserved functionality](#125-reserved-functionality) reserves a third, `link`). An inode row records only the item's kind and creation metadata. Its current parent and name are stored as directory bindings, and its contents as file revisions.

Why does an inode ID begin with its commit's sequence and create number? Most metadata rows are keyed by inode ID (see [Metadata rows and row keys](#a6-metadata-rows-and-row-keys)), and a [run](glossary.md#run) contains the rows of a span of commits, so a run of mostly new inodes has a narrow inode key range that lookups for other keys skip. Commits in one batch share a commit time but not a sequence, so their inodes still sort by commit and create number.

Why are the low 64 bits random? A fork keeps its inherited inode IDs and continues from its source's sequence, so the fork and its source can each create an inode at the same sequence and create number, and a merge can bring both IDs into one namespace. The random word keeps them apart, with a 1 in 2^64 chance of a collision, so LoonFS doesn't check a new inode ID against the existing inodes.

### 1.2 Commits and revisions

A "commit" is one successfully published mutation request. A request can contain several operations, and they're all published together at one namespace sequence number (`seq`).

A "file revision" is one version of a file's contents. Each file revision is assigned a revision number (`revision_no`) that's scoped to its file. Sequence numbers and revision numbers are separate counters, and a namespace can commit many renames, directory changes, and writes to other files between two revisions of one file.

For example, a file might go through the following history (the sequence values are illustrative):

| Namespace sequence | Operation | Result |
| --- | --- | --- |
| 17 | Create `/reports/Report.txt` | Create an inode with a new ID, bind the name, and publish file revision `1`. |
| 18 | Rename it to `/reports/Final.txt` | Keep the inode ID and revision `1`, and replace the directory binding. |
| 19 | Replace its contents | Keep the inode ID, and publish revision `2`. |
| 20 | Delete `/reports/Final.txt` | Remove the binding, and record a deletion for the inode. |
| 21 | Restore it | Bind the same inode again, and keep its revision history. |

The directory state at sequence 18 and the contents at sequence 19 are read from different rows, and the inode row changes in neither case.

### 1.3 Directory bindings

A "directory binding" places an item in the tree. It links a "directory slot", which is a parent inode and a [name key](#14-names-and-paths) (`parent_inode_id`, `name_key`), to one child inode. A parent, a name key, and a child together are an "edge". A slot is either bound to a child or unbound. A bound slot stores:

- the display name (`display_name`), exactly as the caller provided it,
- the child's kind and creation attribution, which never change for the life of the inode, and
- for a file, a "revision summary" (`revision`) of the file's current revision: its revision number, its size, and the actor and time of the commit that published it.

Each binding change writes a directory binding row (`direntry_binding`). The row records its commit's sequence (`committed_seq`) and its index within that commit (`delta_index`), which together are its position, and, like every [history row](#19-row-families), its commit's ID, actor, and time. It also stores the position of the bind that established the edge's current binding (`bind_position`). A bind row's `bind_position` is its own position, and an unbind row's is the position of the bind it removes.

LoonFS stores binding rows in two indexes, so it can look them up from either side:

- `direntry_binds`, the "slot index", orders the rows by parent, name key, and child, to find the child at a name and to list a directory.
- `direntry_child_binds`, the "child index", orders the rows by child, parent, and name key, to find a child's current parent.

Both indexes sort one edge's rows newest first, so an edge's value at sequence `N` is its first row, in key order, whose `committed_seq` is at or below `N`, and at the head it's the edge's first row. A slot's value at `N` is the newest of its edges' values, and a child's binding at `N` is the newest of its edges' values in the child index. If a slot's value is unbound, or the slot has no row at or below `N`, the name is available. When one commit changes an edge more than once, the last change wins.

Two kinds of delta write rows to the slot index:

1. A bind (`bind_direntry`) writes a bound row to both indexes, and an unbind (`unbind_direntry`) writes an unbound row to both. These rows are "binding events". A bind of a file includes the file's current revision summary, which the bound row stores.
2. A file revision (`append_file_revision`) writes the revision's row to `revisions`, and a bound row under the file's current edge to the slot index alone. This "summary row" copies the edge's display name and the file's creation attribution, keeps the edge's `bind_position`, and stores the new revision's summary. Its position is the revision delta's, so a summary row is a bound row whose position isn't its `bind_position`.

LoonFS refuses a revision of a file that has no current binding (a deleted or disconnected file), so every revision has a current edge to write under. When one commit binds a file and writes its revision, both deltas write a slot-index row with the same summary, and the later delta wins.

Commit validation allows at most one child per slot and at most one parent per child. A bind MUST refer to a child that has no current binding, unless the same operation removes that binding first. A move removes the child's old binding before it binds the new one. Because of these rules, the two indexes agree at every read sequence.

An unbind's WAL delta identifies the bind it removes by sequence and delta index, and validation checks that this bind is current. An unbound value acts as a tombstone for older values of its edge, and [Row retention during a base compaction](#103-row-retention-during-a-base-compaction) defines when it can be removed. The API's binding version encodes a binding's `bind_position`, so writing a file's content doesn't change its binding version, and moving the file does.

A directory listing scans the parent's prefix of the slot index once, in name order, taking each edge's first row at or below the read's sequence and skipping the edge's older rows. A bound value is an entry, and an unbound value produces no entry. A file's entry takes its revision number, size, and revision attribution from the row's revision summary, and the child's kind and creation attribution from the row, so the listing reads no revision or inode row. A bound file row without a revision summary is corrupt.

For example, suppose `/docs/a.txt` is bound at sequence 10 and written at sequences 12 and 15, and `/docs/b.txt` is bound at sequence 11 and unbound at sequence 14. In key order, the slot index stores `a.txt`'s summary rows at 15 and 12 and its bind at 10, and then `b.txt`'s unbind at 14 and its bind at 11. A listing at sequence 13 reports `a.txt` with the revision written at 12 and `b.txt` from its bind at 11. A listing at the head reports `a.txt` with the revision written at 15, and nothing for `b.txt`.

Why does the binding row copy the revision summary? Without the copy, a listing would make one lookup for each file (see [Row families](#19-row-families)). The copy leaves out the content reference, which a stat reads from `revisions`.

Why does a `direntry_binds` filter contain a parent probe? A fold's run covers every directory its commits changed, so a run's `direntry_binds` segment can span a directory's key range without containing any of its rows. The filter contains the probe `direntry-bind-{parent_inode_id_body}` of each parent with rows in the segment (see [Metadata rows and row keys](#a6-metadata-rows-and-row-keys)), so a listing skips such a segment after reading only its filter.

### 1.4 Names and paths

LoonFS stores each display name exactly as the caller provided it. A display name MUST follow these rules:

- It's between 1 and 255 UTF-8 bytes long.
- It doesn't contain `/`, any Unicode control character (general category `Cc`), or any of `: ? * | " < > \`.
- It isn't `.` or `..`, it isn't made up entirely of whitespace, and it doesn't end in a space or a dot.
- It isn't a reserved device name: `CON`, `PRN`, `AUX`, `NUL`, `COM1` through `COM9`, or `LPT1` through `LPT9`. These are compared case-insensitively and without the extension, so `con.txt` is also reserved.

These rules avoid common cross-platform naming problems, but they don't guarantee that every other filesystem or tool accepts every tree LoonFS can store.

LoonFS compares and looks up sibling names by a "name key" that it derives from each display name, the same way in every namespace: NFC normalization, then full Unicode default case folding (not the Turkic variant), then NFC normalization again, with Unicode 17.0.0 data. Validating new names and looking up existing names both use the key. Name keys follow the same character rules as display names, with a 768-byte limit. Normalization and case folding expand a name by at most three times in bytes, so every key derived from a valid display name fits.

So names that differ only in letter case or Unicode normalization are the same name, and the display name keeps the caller's spelling. For example, `Report.txt` and `report.txt` have the same name key, so they can't be siblings, and a rename from one to the other changes the display name of the same entry. A precomposed `é` and an `e` followed by a combining acute accent also have the same key. The [name-folding fixtures][name-vectors] list the key for normalization and case-folding cases beyond ASCII.

The key mapping can't change once any namespace has rows, because stored slots contain keys derived with it, so changing it would change the format's meaning even with the same serialized fields. Implementations MUST preserve the Unicode behavior specified here (converting to lowercase isn't enough).

Why does every namespace compare names without case? The filesystems and sync tools that trees are copied to compare names that way, so a tree with two siblings that differ only in case can't be stored on them, and a name typed with the wrong case still finds the entry. A per-namespace setting would force the choice at creation, before anyone knows where the tree will go, and a fork would inherit it (see [Alternatives considered](#13-alternatives-considered)).

An absolute path starts with exactly one `/`. It has no empty components, repeated separators, or trailing `/`, except for the root path `/`. A canonical path is at most 4,096 UTF-8 bytes and 128 components. LoonFS rejects noncanonical input instead of rewriting it.

These limits apply to request paths and don't promise compatibility with other filesystems. A path built from stored names can exceed them, and reads by inode still return it.

### 1.5 File contents and ownership

Each file revision has one content reference (`ContentRef`), and the only kind of content reference is `blob_v1`. A `blob_v1` reference identifies the first `size_bytes` bytes of a chain, which are the file's bytes with no envelope, and it contains:

- a random content ID,
- the size of the prefix it refers to, and
- a checksum of that prefix.

Content derived from other content, such as an append, also uses an ordinary `blob_v1` reference. A reference contains no bucket address, object-store path, or namespace. The content ID and the reading namespace's [pool](glossary.md#pool) determine where the bytes are stored, so one reference identifies the same object in every [member](glossary.md#pool) of a pool.

A "chain" is the bytes stored under one content ID. A chain grows when a file is appended to, and each reference refers to a prefix of it, so two references with one content ID can refer to different lengths. For example, a log file's first revision might refer to the first 100 bytes of a chain, and after two appends, revisions 2 and 3 might refer to the first 200 and the first 210 bytes of the same chain.

Content objects are immutable, so appended bytes go to new content objects, and a chain's bytes can be spread across several. A "layout" lists those objects, in chain order, as "extents". Each extent records the object's content ID, the object, the offset in that object, the length, and the checksum that the store keeps for the whole object (its "object checksum"). Every extent that lists one object records the same object checksum, so a reader can check the object without looking at the content references that use the extent (see [Content verification](#45-content-verification)). [WAL records](#a5-wal-records) defines which layout a revision's WAL delta includes, and when validation writes pending pieces to content objects.

A chain's "whole object" is stored at the chain's own key, under the reading namespace's pool prefix. Every other object of the chain is a "span object", which stores one byte range of the chain. Its key is the chain's key followed by a random span object ID that the writer generates for each write, and the extent that lists the object records the range of chain bytes it stores:

```text
pools/{pool_id}/content/{content_id}
pools/{pool_id}/content/{content_id}-{span_object_id}
```

The whole object sorts before its spans, so all the objects of one chain are next to each other in a listing. For example, a fold that writes bytes 100 to 130 of the chain `con_4z59p0wjvx9a8b2frdhhf6mat1` writes them to `pools/pool_3s0drmj8em84yb4mnetqkz2j0x/content/con_4z59p0wjvx9a8b2frdhhf6mat1-span_1q8yd3a2gxmz0w4pke6tcv5rhb`, and the extent in the chain's new layout records the span object ID with the start 100 and the end 130.

File content is durable no later than the commit that references it, and it becomes durable by one of two paths:

| Path | How the bytes become durable |
| --- | --- |
| Uploaded | An upload session writes and verifies the content object before the commit (see [Uploading content](#5-uploading-content)). |
| Inline | The WAL object that commits the revision includes the bytes as "pieces" ([WAL records](#a5-wal-records)), so the bytes become durable in the same write that makes the revision visible. A piece contains a whole value, or the bytes an append adds after bytes that are already durable. Before a fold publishes a manifest whose `folded_wal_no` covers that WAL object, it writes the pieces to content objects as extents (see [Publishing a manifest](#72-publishing-a-manifest)). |

Garbage collection deletes a WAL object only after a manifest covers it, and the fold that published that manifest has already written the WAL object's pieces as extents, so a WAL object is never deleted while some of its pieces exist only in the WAL. A reference identifies content but doesn't prove that a content object contains its bytes yet, and [Content verification](#45-content-verification) defines where a reader finds the bytes.

Every member of a pool writes its new content under the pool's prefix, including the object of each upload, the objects a fold writes for inline pieces, and the extents of new chains. So a fork reads inherited content at its source's keys, and content is never copied between members.

A chain's "creator" is the namespace whose commit started the chain, and only the creator extends the chain in place. The chain's layout records the creator next to its extents (see [Metadata rows and row keys](#a6-metadata-rows-and-row-keys)). An append by any other member of the pool starts a new chain that shares the first chain's extents (see [Operations and WAL deltas](#66-operations-and-wal-deltas)).

Reading inherited content doesn't require the source's manifest. The fork's manifest records the pool, so the fork derives every content key itself, and after its source is deleted it keeps reading the objects in the basis that its fork snapshot keeps.

LoonFS doesn't deduplicate content across uploads, so two uploads of identical bytes create two objects. A retry within one upload session reuses that session's content ID, so retries don't depend on finding another upload with identical bytes.

Why does every member of a pool write content under one prefix? Only a pool's members can share a content object (a fork inherits its source's references, a merge adds a fork's references to its source, and nothing adds a reference to another pool's content). With one prefix, a reference identifies the same object in every member, and one collection sees every root of every object it can delete (see [Content roots](#119-content-roots)). With a prefix for each namespace, a reference into another member's prefix would need a snapshot in that direction for as long as the reference exists.

Why is a pool's prefix separate from every namespace's prefix? Otherwise the member whose prefix stored the pool's content would have to be retired last, and its collections would have to keep running after its deletion. With a pool prefix, no namespace's collection or retirement deletes content.

Why does a span object have a random ID instead of a key built from its range? Two members can write the same pieces of a chain, such as a fork's fold of its source's WAL tail and the source's own later fold. With range-based keys, both would put the same key, and every writer would depend on an occupied key containing the same bytes. With a random ID, each span object has one writer, as a metadata segment does, and garbage collection deletes an object that no layout row lists once it's old enough. The cost is that two writes of the same pieces store the bytes twice (see [Publishing a manifest](#72-publishing-a-manifest)).

### 1.6 File and subtree deletion

Deleting an item removes its current binding and records a "tombstone" for it. The item's revisions, and everything below it, stay in place, and the tombstone hides them. A tombstone on a directory hides all of its descendants too, so a deletion doesn't need a tombstone for every descendant.

A tombstone is a series of events, stored as rows. Each row records its own position (`committed_seq` and `delta_index`), and events are ordered by position. There are two kinds of event:

- A `set` event starts a deletion. It stores the deleted root's kind (`root_kind`), which the trash listing reports, and the removed binding (`deleted_binding`, with `parent_inode_id`, `name_key`, and `display_name`), so the name can be restored even after old binding versions are compacted.
- A `revoke` event, which a restore commits, cancels one `set` event, which it identifies by position (a `DeltaPosition` value, `{seq, delta_index}`). A `revoke` MUST NOT contain a deleted binding.

At a given sequence, the root inode's latest event determines the state of the root and everything below it. After a `set`, the tombstone hides the subtree, and after a `revoke`, it hides nothing. A restore commits its `revoke` together with a new binding for the item, so the subtree reappears. A `set` after a revoked deletion of the same inode is a new deletion.

A deletion's "deletion sequence" (`deletion_seq`) is the sequence of the commit that committed its `set`. A deletion has "expired" in a view when its deletion sequence is at or below the view's retention floor, which is the manifest's floor raised by any [floor objects](glossary.md#floor-object) in the view's WAL tail (see [Advancing the retention floor](#101-advancing-the-retention-floor)). A deletion is "in the trash" when its `set` is the root's latest event and its deletion sequence is above that floor. Each deletion in the trash is a "trash entry", and together they are the namespace's [trash](glossary.md#trash). An expired deletion can't be restored, its tombstone keeps hiding the subtree, and the trash listing leaves it out. Expiry commits nothing. When a retention advance moves the floor, every view whose floor has reached the new floor treats the deletions the floor passed as expired, and a base compaction over such a manifest removes the rows an expired deletion hides (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)).

For example, suppose the directory `/old` is deleted at sequence 40, and a retention advance later moves the floor from 35 to 50. While the floor is 35, a restore can bring back `/old`. In every view whose floor is 50 or higher, `/old` and everything below it stay hidden, the trash listing leaves the deletion out, and a restore of the deletion returns `not_deleted`.

A restore request identifies a deletion by its inode and the sequence it was committed at. Validation MUST confirm that this deletion is in the trash at the writer's [position](glossary.md#writer-position), measured against the position's retention floor. A request for a deletion that isn't in the trash returns `not_deleted`, and it MUST NOT cancel a later deletion. (A request can't restore a deletion made earlier in the same request, which has no committed sequence yet.) A restore binds the item under the saved parent and name, unless the request provides a different destination. Only the root of a deletion can be restored, because the descendants it hides don't have deletions of their own. An item can't be deleted while a deletion above it hides it, so a deletion inside another deletion is always the older of the two, and no deletion in the trash is inside an expired one.

The `trash` family is a [query index](#19-row-families) over the tombstone events, which the trash listing reads in deletion-sequence order. Its actions are listing states, not events. A `set` event lists the deletion with a `listed` row, and a restore's `revoke` event removes it from the listing with a `removed` row for the same `(deletion_seq, root_inode_id)`. Removed rows sort before listed rows, so a scan can skip restored deletions. The family's keys begin with the deletion sequence, and a listing starts its scan above the view's retention floor, so an expired deletion never appears in it. The tombstone events remain the source of truth.

Deleting a file doesn't delete its content right away. In a live namespace, file revisions and the trash are kept under the rules in [Retention and compaction](#10-retention-and-compaction). [Namespace lifecycle and forks](#9-namespace-lifecycle-and-forks) describes deleting a namespace.

Why does a deletion leave the trash? LoonFS can't reclaim the rows and content that a deletion hides while a restore could bring them back, so a deletion needs a point after which it can't be undone.

Why does a deletion expire at the retention floor, and not at a point of its own? The floor is the namespace's one bound on how far back history is kept, and a deletion in the trash is history, so a client reads one number to know how far back it can restore anything.

Why does the trash listing have its own index, with its own states? The `tombstones` family is keyed by root inode, so it can't serve deletion-sequence order, and the listing needs each deletion's current state rather than the root's whole event history.

### 1.7 Attributes and attribution

"Attributes" are a map from strings to strings, attached to an inode, which applications use for tags, display hints, and similar metadata. Renaming or moving an item, or writing a new file revision, doesn't change its attributes.

An attribute map follows these limits:

| Part | Rule |
| --- | --- |
| Key | 1 to 128 UTF-8 bytes, with no Unicode `Cc` control characters. Keys are compared exactly, without normalization or case folding. Keys that start with `loonfs.` are reserved for LoonFS, and callers can't write them. |
| Value | A UTF-8 string of at most 4,096 bytes. Empty strings and control characters are valid. |
| Map | At most 100 entries, and at most 65,536 bytes of keys and values combined (counted as UTF-8, without serialization overhead). |

Removing a key is an explicit operation, and setting a key to an empty value doesn't remove it. A durable map that breaks these limits is rejected, not truncated.

An inode starts with an empty map at attribute revision `0`, and that initial state has no stored row. Each accepted update increases the attribute revision number (`attributes_revision_no`) by one and stores the complete resulting map, even when the map is unchanged. Clearing the map is a real update, and it MUST remain distinguishable from an inode whose attributes were never changed.

Clients use attribute revision numbers for optimistic concurrency. The API doesn't list old attribute maps, but the storage layer keeps the rows that supported sequence-based views need, as the compaction rules specify.

Every commit also records a timestamp, an optional message, and an "actor", a required, opaque identifier that the application supplies to record who made the change. LoonFS stores the actor without authenticating it or looking up any profile, so the application chooses a stable identifier and its identity scope. The actor plays no part in authorization.

Every row that records an event copies its commit's actor and timestamp (`committed_by` and `committed_at_ms`), `committed_seq`, and `commit_id`. The API exposes these fields under the names the API specification defines, such as `created_by` for the commit that created an inode and `attributes_updated_by` for its newest attribute revision.

| Metadata | Actor and timestamp fields |
| --- | --- |
| Inode, file revision, tombstone event, attribute revision, access revision, commit | `committed_by`, `committed_at_ms` |
| Trash entry (`listed` row) | `deleted_by`, `deleted_at_ms` |
| Directory binding | `child_created_by`, `child_created_at_ms`, copied from the child inode |

The root inode of a new namespace is attributed to the actor ID `loonfs`. A fork inherits its root inode from its source, and its inherited inodes keep their original timestamps, while the fork's manifest records the fork's own creation time.

Event timestamps are informational, and sequence numbers determine order. Renaming an inode doesn't change its creation or content timestamps, and directories don't have a general modification timestamp. (Lease and reclamation deadlines are a different kind of time, covered in [Timing assumptions](#33-timing-assumptions).)

### 1.8 Access rows

An inode can have an "access row", which controls what principals can do to the inode and, through inheritance, to the items below it. An access row has a boundary flag and a map from principals to rights. A "principal" is an identity that the application assigns, such as a user, a group, or a public audience, identified by an opaque ID of 1 to 256 visible ASCII characters, excluding the comma. LoonFS stores principal IDs, but it never resolves them.

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

A set of rights is stored as a list of distinct names, in the order of the table above. A decoder accepts the names in any order, but it rejects a repeated name or a repeated principal. An access map has at most 1,000 principals and at most 65,536 bytes of principal IDs, and every entry has at least one right. A durable map that breaks these limits is rejected, not truncated.

An inode starts at access revision `0`, with no boundary and no grants, and that initial state has no stored row. The root inode starts the same way, so a new namespace has no access rows and no administrators until an `update_access` on the root sets them. Each accepted update increases the access revision number (`access_revision_no`) by one and stores the complete resulting state. A row with no boundary and no grants is a real revision, distinguishable from an inode whose access was never changed.

LoonFS authorizes a request with principals against the access rows, and checks no rights for a request in the [service context](glossary.md#service-context) (see the [API specification][api-spec]). A fork starts with its source's access rows, as it starts with every other row of its source.

A principal's effective rights on an inode are the union of its grants on the inode's own row and on each ancestor's row. LoonFS walks up the current parent bindings, and it stops after a row whose boundary flag is set, or at the root. For example, suppose a principal has `read` on `/projects`, and `/projects/secret` has its boundary flag set. That `read` doesn't apply inside `/projects/secret`, unless the row for `/projects/secret` (or a row below it) grants it.

A deletion root has no current binding, so while its deletion is in the trash, the walk uses its tombstone's saved parent for that step. The walk stops at the root of an expired deletion, so the inodes an expired deletion hides inherit no grants from above its root. Visiting the same inode twice during the walk means the namespace is corrupt. `admin` on the root row grants every right on every item, and it's never inherited. [Paths, listings, and revision reads](#44-paths-listings-and-revision-reads) defines when a read by inode ID returns `inode_not_found` before any grant is evaluated, such as for an inode that an expired deletion hides. A snapshot from before the expiry still shows the inode, and for a request with principals, the [API specification][api-spec] allows only an administrator to read it through the snapshot.

The `update_access` operation replaces an inode's access row and appears in the change feed as an `access_changed` event. The [API specification][api-spec] defines that operation, and how reads, commits, and upload sessions are authorized against access rows.

### 1.9 Row families

LoonFS stores metadata rows in twelve "row families", and every family that the sections above describe follows the rules in this section. A row family is a set of rows with one key grammar, such as `revisions`, and each [metadata segment](glossary.md#metadata-segment) stores the rows of one family ([Metadata rows and row keys](#a6-metadata-rows-and-row-keys) lists every family and its keys). Each family serves one kind of read, such as a directory listing or a lookup by commit ID. The rules define how an entity's versions sort, what a version is, who committed a row, and what a row can copy from another entity, where an entity is an inode, a commit, a binding edge, a deletion, or a chain.

A [fold](glossary.md#fold) and a replay of the WAL tail both compute each durable row from exactly one WAL delta or one commit record, and its commit, without reading another row. When a row needs a field of another entity, the delta includes that field, because the planner read it when it planned the commit. For example, a bind delta includes the child's kind and creation attribution, and the binding row copies them from the delta.

These rows are computed another way:

- The root inode row is written from the namespace's first manifest, at sequence 0.
- A fold writes a layout row for each chain whose pieces it writes to a content object, from the chain's earlier layout and the new extent (see [Publishing a manifest](#72-publishing-a-manifest)).

Each family is one of three kinds:

- An "entity header" is a family with one immutable row for each entity, written when the entity is created.
- A "history family" stores the versions of one mutable part of an entity, keyed by the entity and then by version, newest first. A row of a history family is a "history row". The entity of a binding family is an edge, and each of its rows is one version of the edge.
- A "query index" is a family that stores a second key over facts that another family stores, for a read that the other family's key can't serve.

A row can also copy a field of another entity, so that a read doesn't look up that entity. Binding rows copy the child's kind and creation attribution, a bound file's row in the [slot index](#13-directory-bindings) copies the file's [revision summary](glossary.md#revision-summary), and a `listed` row in the `trash` family copies the deletion root's kind and the deletion's binding, actor, and time.

Every family follows these rules:

1. Every row is computed from one delta or one commit record, in a fold or a replay, without reading another row (except the rows listed above).
2. An entity header is keyed by its entity's stable ID (an inode ID, or a commit's sequence). A history row is keyed by its entity's stable ID, then by its revision number if the family has one, and then by its position (`committed_seq` and `delta_index`). The binding families key by the edge instead, because the name key is part of the edge's identity, and a query index starts with the key of the read it serves.
3. Every history family sorts each entity's rows newest first, so the current value is the first row, and the value at sequence `N` is the first row at or below `N`. The other families' orders follow from their keys in [Metadata rows and row keys](#a6-metadata-rows-and-row-keys).
4. Every history row records its commit's sequence, ID, actor, and time (`committed_seq`, `commit_id`, `committed_by`, and `committed_at_ms`). A query index row records the sequence of the commit it points to, and it records an actor or a time only as a copy.
5. A row copies a field of another entity only if the field can't change, or if the delta that changes the field also writes the copy. LoonFS never changes a copy after writing it, and it never repairs a copy when it reads one.
6. A family keeps rows under its retention rule in the table below (for the history families and `commits`, `commit_receipts`, and `merges`, a rule relative to the [retention floor](glossary.md#retention-floor)), and [Row retention during a base compaction](#103-row-retention-during-a-base-compaction) defines each rule.
7. Every family's keys encode generated IDs, client-chosen strings (such as a name key or a commit ID), and counters the same way as every other family's keys, and a family's filter key is its prefix followed by the components that identify one entity, or one slot in `direntry_binds` (which also adds a parent probe) and one child in `direntry_child_binds`. [Metadata rows and row keys](#a6-metadata-rows-and-row-keys) defines each encoding.
8. Two families that store rows for the same deltas or commits, under the same retention rule, are compacted in one [family group](glossary.md#family-group). The two binding families form one group, and `commits`, `commit_receipts`, and `merges` form another.

This table lists each family's kind, the first component of its key, and its retention rule. [Row retention during a base compaction](#103-row-retention-during-a-base-compaction) defines each retention rule exactly, and [Metadata rows and row keys](#a6-metadata-rows-and-row-keys) defines each key.

| Family | Kind | Key starts with | Retention |
| --- | --- | --- | --- |
| `inodes` | Entity header | Inode ID | The inode's lifetime |
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

Why is every row computed from one delta, without reading other rows? Such a row depends only on its commit, so a fold, a replay, and every reader of the same WAL tail compute the same rows, and LoonFS can rebuild a row from its commit.

Why do history families sort newest first? A read at sequence `N` stops at the first row at or below `N` instead of reading older rows. This matters most in the slot index, where a file written ten thousand times has ten thousand summary rows under its edge until retention removes the old ones.

Why can a row copy a field of another entity? Without the copy, a directory listing or any other read of one parent's rows would make one lookup for each child, whose rows are stored far from the parent's. The delta that changes the field writes the copy in the same commit, so a read at any sequence finds the copy as of that sequence.

Why are two families with rows for the same deltas or commits compacted together? Compacted separately, one family could keep a row that the other had removed, and a lookup by one key would find an event that a lookup by the other key misses. (A fold writes every family into one run, so only compaction could separate them.)

## 2. Objects and references

This section describes the objects LoonFS stores for a namespace, how their keys are formed, and how one object refers to another.

### 2.1 Storage layout

Each namespace's objects are stored under its own prefix, except content objects and upload records, which every member of a [pool](glossary.md#pool) stores under the pool's prefix (see [File contents and ownership](#15-file-contents-and-ownership)). The pool's prefix also stores a member entry for each namespace in the pool, which the namespace's create or fork call writes (see [Creating a namespace](#91-creating-a-namespace)), and the temporary objects of assemblies, because every assembly writes a content object. [Object keys](#a8-object-keys) lists the complete key patterns:

```text
namespaces/{namespace_id}/
├── boundary.json
├── manifests/{manifest_no:020}.json.zst
├── wal/{wal_no:020}.wal.zst
├── segments/{segment_id}.sst
├── snapshots/{snapshot_id}.json
└── extensions/{extension_name}/segments/{segment_id}.sst

pools/{pool_id}/
├── members/{namespace_id}.json
├── content/
├── uploads/{upload_id}.json
├── uploads/{upload_id}.outcome.json
└── temporary/{temporary_id}
```

Every newly written metadata segment, including compaction output, is stored under the writing namespace's `segments/` prefix, and an [extension's](#124-extensions) segments are stored the same way under `extensions/{extension_name}/segments/`. A segment that a fork inherited stays under its owner's prefix, and its descriptor records the owner (`owner_namespace_id`), so readers never substitute the reading namespace into the key.

On providers that build a new object by composing whole objects (such as GCS), an assembly writes temporary objects for the parts it can't compose directly (a range of a source object, new bytes, or an intermediate result), and it deletes them when it finishes (see [Immutable writes and retries](#32-immutable-writes-and-retries)).

The key layout is part of the format, and other objects MUST NOT use keys in these families. Core readers don't read an extension's segments, and garbage collection keeps every key under an extension's prefix other than a segment key.

### 2.2 Object roles and mutability

| Object | What it stores | How it changes |
| --- | --- | --- |
| Manifest | The namespace's identity, lifecycle status, writer authority, metadata runs, and retention floor | A new manifest is published at the next number. |
| WAL object | Commits, a writer fence, or a [floor object](glossary.md#floor-object), after the folded boundary | A new WAL object is created at the next number. |
| Manifest boundary | A manifest number at or below which every manifest of the namespace has been replaced by a newer manifest, or the retired state, which is above every manifest number. A reader lists the manifests above the number, and a publisher reads the boundary after its manifest put. | Created with put-if-absent, then changed by compare-and-swap. The boundary never decreases, a retired boundary never changes, and nothing deletes the object. Garbage collection writes it. |
| Metadata segment | Sorted metadata rows that a manifest references | A new immutable object is written. |
| Extension segment | Sorted rows of an extension's state, which an extension entry in a manifest lists | A new immutable object is written. |
| Snapshot | A reference that keeps one manifest for a user or a fork | Created and deleted. A user snapshot's expiry time can be extended, and its deletion recorded, by compare-and-swap. |
| Upload session | One transfer and its completed content, until a commit uses it or cleanup removes it | A session record and an outcome record, each created once with put-if-absent. |
| Member entry | The membership of one namespace in one pool | Created once with put-if-absent, by the namespace's create or fork call, and deleted by the namespace's retirement. |
| Content object (whole or span) | A range of chain bytes, along with the provider's stored checksum of the whole object | Immutable. |
| Temporary object | Bytes that an assembly needs as a separate object | Created, then deleted. |

Manifests and WAL objects are immutable once they're written. A manifest publication can change the physical layout or the namespace's control state without creating a commit, and a WAL publication can add commits without creating a manifest.

### 2.3 Numbered objects and generated IDs

Manifest numbers and WAL numbers are separate counters for each namespace, and both start at 1. Object keys spell them with twenty decimal digits, so lexicographic order matches numeric order. For example, WAL object 2 is stored at `wal/00000000000000000002.wal.zst`.

Each number refers to exactly one immutable object. A publisher creates the next number with put-if-absent, so competing publishers can't write different objects at the same number, and a publisher that loses loads the winning object before it plans another attempt. The namespace and number in an object's payload MUST match its key.

Other objects use generated IDs, which all use one 26-character lowercase Crockford base 32 spelling of 128 bits. The spelling adds two zero bits in front of the 128 bits and writes each group of 5 bits as one character of `0123456789abcdefghjkmnpqrstvwxyz`, so the first character is `0` through `7`:

- A content ID (`con_`), a metadata segment ID (`seg_`), an extension segment ID (`idx_`), a pool ID (`pool_`), an upload ID (`upl_`), a span object ID (`span_`), and a temporary object's ID (`tmp_`) are each the prefix followed by that spelling of a random version 4 UUID. A metadata segment ID is generated, instead of being derived from the segment's position in publication history. A span object's key spells its full ID, as every object key does (see [File contents and ownership](#15-file-contents-and-ownership)).
- An inode ID is `ino_` followed by the same spelling, but its 128 bits aren't a UUID. Its high 64 bits are the creating commit's sequence and the inode's create number, and its low 64 bits are random. The root directory's ID is all zero bits (`ino_00000000000000000000000000`). See [Namespaces and identity](#11-namespaces-and-identity).
- A snapshot ID has the form `snp_{manifest_no:020}-{26 lowercase Crockford base 32 characters}`. The number identifies the snapshot's manifest, and the random part, in the same 26-character spelling, distinguishes separate user and fork snapshots on the same manifest.

A collision at a newly generated immutable key MUST NOT overwrite the existing bytes.

### 2.4 The current manifest and WAL tip

The current manifest is the source of truth for the namespace's identity, status, writer epoch, and compactor epoch. Its runs describe the folded metadata through the folded head sequence (`head_seq`), and `folded_wal_no` is the last WAL number that those runs include. The WAL objects after `folded_wal_no` contain the later commits, and the highest-numbered one is the "WAL tip".

Every WAL object records the head sequence after its commits. A fence moves the WAL number and the writer epoch forward without adding a commit. A [floor object](glossary.md#floor-object) is a WAL object with no commits that records a new [retention floor](glossary.md#retention-floor) (see [Advancing the retention floor](#101-advancing-the-retention-floor)). A fence and a floor object both keep the head sequence they started from.

A process that has no manifest of the namespace in memory finds the newest manifest by reading the [manifest boundary](glossary.md#manifest-boundary) and listing the manifests numbered above it (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)).

The namespace's counters advance at different times:

| Counter | Advances when |
| --- | --- |
| `manifest_no` | A manifest is published, including for a layout, authority, retention, or lifecycle change. |
| `wal_no` | A WAL object is created, including an empty writer fence or a floor object. |
| `seq` | A mutation request commits. One WAL object can contain several sequences. |
| `revision_no` | A new content revision is committed for one file. |

For example, manifest 5 might cover WAL objects through 8 and sequences through 40. WAL object 9 can be a fence at sequence 40, and WAL object 10 can contain commits 41 to 43. The read view is then at sequence 43. A fold can publish manifest 6, covering WAL object 10, without allocating sequence 44.

### 2.5 Manifest references

A durable reference to a manifest contains:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | The namespace that stores the manifest. |
| `manifest_no` | The manifest's number, which determines its key. |
| `head_seq` | The head sequence that the manifest records. |
| `payload_checksum` | A digest that MUST match the manifest's stored payload. |

A snapshot refers to a manifest in its own namespace and stores these fields directly, with its `namespace_id` as the owner. A fork's basis includes a reference to a manifest in the source namespace, along with the IDs of the fork's two fork snapshots. The segment references inside a manifest keep their own owners, so a manifest can list segments that belong to other namespaces.

A segment descriptor records the checksum that the store keeps for the segment object (see [Control and manifest payloads](#a4-control-and-manifest-payloads)), and a layout's extent records the store's checksum of the content object it lists (see [Metadata rows and row keys](#a6-metadata-rows-and-row-keys)). So a manifest reference, a segment descriptor, and an extent can each be checked against its object without reading another object.

Readers validate the referenced identity, sequence, and checksum. A missing or corrupt required object is an error, and never a reason to use another manifest instead.

### 2.6 A summary of the log protocol

This section summarizes how commits, writer takeover, folds, discovery, and garbage collection fit together. Sections 4, 6, 7, and 11 define the complete rules.

Each namespace has two sequences of numbered objects, manifests and WAL objects (see [Numbered objects and generated IDs](#23-numbered-objects-and-generated-ids)).

```text
manifests:  [4] | [5]  [6]   <- current manifest, folded_wal_no = 8
                ^
                manifest boundary = 4 (a reader lists the manifests above it)

WAL:        [7]  [8]  [9]  [10]  [11]   <- WAL tip
                      |_____________|
                      visible WAL, replayed on top of manifest 6
```

- A WAL object is a data object, a fence, or a [floor object](glossary.md#floor-object). A data object contains commits stamped with the writer epoch of the session that wrote them, and any process can create a floor object without the writer epoch (see [Advancing the retention floor](#101-advancing-the-retention-floor)).
- Retirement and garbage collection raise the [manifest boundary](glossary.md#manifest-boundary), and a manifest put at or below the boundary counts as lost (see [Retirement](#95-retirement), [One complete collection](#111-one-complete-collection), and [Publishing a manifest](#72-publishing-a-manifest)).

A writer session commits a batch by creating WAL object `tip + 1` with put-if-absent, and it plans the batch again after a collision (see [Publication and group commit](#63-publication-and-group-commit)).

A new writer session takes over by publishing the next manifest with `writer_epoch + 1` and then a fence at the next WAL number, after which the previous session can't commit (see [Writer ownership](#61-writer-ownership)).

Any process can run a fold, which writes the WAL objects after `folded_wal_no` to new metadata segments and the pieces it covers to content objects, and then publishes the next manifest (see [Publishing a manifest](#72-publishing-a-manifest)).

A reader discovers a namespace's current state by finding the newest manifest through the boundary, reading the WAL above its `folded_wal_no` until the first missing number, and then checking the manifest for a successor and reading the boundary again (see [Resolving the metadata basis](#41-resolving-the-metadata-basis) and [Replaying the visible WAL](#42-replaying-the-visible-wal)).

Garbage collection deletes the data WAL objects at or below the current manifest's `folded_wal_no`, the manifests that a newer manifest replaced and that no snapshot keeps, and the segments that aren't listed by the current manifest or by any manifest that a snapshot keeps, once each one is old enough under the age rules. It keeps every fence and floor object. A read that must outlive the current manifest takes a snapshot (see [Reference roots](#112-reference-roots), [Candidate and age rules](#113-candidate-and-age-rules), and [Snapshots](#8-snapshots)).

A WAL put succeeds only at a number where no object was ever created, but a fence put, or a floor object put without the writer epoch, can succeed at a number that a collection emptied, so the process checks that number after the put (see [Publication and group commit](#63-publication-and-group-commit), [Writer ownership](#61-writer-ownership), and [Advancing the retention floor](#101-advancing-the-retention-floor)).

The protocol guarantees the following:

1. Successfully creating WAL object `n + 1` commits its records, and the visible log is the unbroken sequence of WAL numbers above the current manifest's `folded_wal_no` (because of put-if-absent numbering, see [Publication and group commit](#63-publication-and-group-commit)).
2. Every acknowledged commit is in that sequence or folded into the current manifest (because no process deletes a fence or a floor object, see [Publication and group commit](#63-publication-and-group-commit)).
3. Writer epochs never decrease along the sequence, and they never exceed the current manifest's epoch. Once a newer writer's fence is written, no older writer commits (because of the claim and the fence, see [Writer ownership](#61-writer-ownership) and [Replaying the visible WAL](#42-replaying-the-visible-wal)).
4. A fresh discovery sees every commit that was acknowledged before it began (because of the successor check and the boundary read after it, see [Replaying the visible WAL](#42-replaying-the-visible-wal)).
5. A cached view or a writer position never moves backward, and a read served from either one reflects a successor check no older than the revalidation bound (because before a read uses a view or a position whose last successor check is that old, the read checks it again, see [Replaying the visible WAL](#42-replaying-the-visible-wal)).
6. Pieces stay readable, in the WAL until a fold has written them as extents, and through the layout after that (because content is written before the manifest, see [Publishing a manifest](#72-publishing-a-manifest) and [File contents and ownership](#15-file-contents-and-ownership)).
7. A retry with the same commit ID and request doesn't commit twice while its receipt is kept (because of the commit receipt, see [Commit identity and retries](#65-commit-identity-and-retries)).
8. The current manifest keeps all of its segments, and so does every manifest that a snapshot keeps. A read or a write on a manifest that a newer manifest replaced can find a segment deleted, and LoonFS then reads again, or plans the write again, on the current manifest (because of the current-manifest and snapshot roots, see [Reference roots](#112-reference-roots) and [Replaying the visible WAL](#42-replaying-the-visible-wal)).

## 3. Object-storage requirements

This section describes what LoonFS requires from an object store, and how it writes immutable objects safely on each provider.

### 3.1 Required operations

The object-store layer MUST provide:

- create-if-absent (put-if-absent) for immutable objects, at every size, so that a large create sent in parts completes conditionally,
- compare-and-swap, for mutable control objects,
- full and ranged reads,
- a read that's conditional on the object's version, which returns no body when the stored version matches the caller's (a store that can't make the read conditional returns the object, and LoonFS compares the versions itself, see the [provider reference](object-storage-providers.md#2-provider-matrix)),
- deletion, and
- reliable prefix listing, from the start of a prefix or after a given key, that returns each object's key, size, and modification time (garbage collection tells a fence or a floor object from a data WAL object by the listed size alone, see [Candidate and age rules](#113-candidate-and-age-rules)).

Every operation MUST stay within the configured storage scope.

A compare-and-swap (CAS) replaces an object only if its compare token still matches the version the caller read, and otherwise fails its precondition. A conditional update MUST report a precondition failure separately from a transport error, because a confirmed precondition failure means the object wasn't replaced. The object-store layer sends a conditional write as one request and never resends it. If it resent a write whose first attempt had succeeded, the second attempt would fail its precondition, and that failure would be misleading. A transport failure after the request was sent can leave the outcome unknown.

A read of a mutable object MUST return the bytes and their compare token in one response, so that a concurrent update can't pair one version's payload with another version's token. Compare tokens (including ETags) are opaque, and LoonFS doesn't assume they're content digests.

Successful writes and deletes MUST be strongly consistent. Prefix listings return keys in ascending lexicographic order, which the garbage collection protocols depend on. (Being "S3 compatible" doesn't by itself mean a provider conforms.) The object-store layer MUST also list the child prefixes directly under a prefix, in the same order and one page per provider request, starting after a given key, so that a host can find namespaces without listing their objects.

A provider MUST also apply each request, if it applies it at all, within `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` of when the client started the operation that sent it. The grace window reserves this provider time, and an implementation can't check the assumption (see [Clock and operation assumptions](#114-clock-and-operation-assumptions) and [Publication and collection timing](#c1-publication-and-collection-timing)).

Readers determine committed state from verified manifests and numbered WAL objects, and garbage collection also uses listings to find candidates and dependencies (for example, retiring a namespace requires a complete listing of its snapshots after deletion). The format doesn't assume transactions across objects, or a consistent snapshot across control objects that are read separately.

### 3.2 Immutable writes and retries

An immutable object MUST NOT be replaced with different bytes. A collision at a newly generated content or segment key is an error, while contention for the next manifest or WAL number follows that object's publication protocol. A retry of the same object can resolve an ambiguous earlier write only with evidence that the stored object has the expected contents.

A record created under a generated ID (a snapshot or an upload session record) is identified by its ID alone, so after a put with an unknown outcome, the record under that ID is the creator's own, whatever its bytes are now. (Another caller can change a snapshot as soon as it's written, so its bytes can't show whose put created it.) A [member entry](glossary.md#member-entry)'s payload is set by its key, so after a put with an unknown outcome, the call reads the key back, and an entry there is the one the call meant to write, whichever call wrote it.

An immutable object is identified by the full-object checksum that its provider stores, including the checksum algorithm. Each store uses one algorithm:

| Store | Algorithm |
| --- | --- |
| S3, R2, S3-compatible endpoints, local storage, and the in-memory reference store | CRC-64/NVME |
| GCS | CRC-32C |

(The local store can be set to use CRC-32C when it's constructed, for conformance tests.)

A writer computes the checksum over its bytes and sends it for the provider to verify, and an assembly combines its sources' verified checksums. The provider stores either a checksum it computes itself or a value the adapter combines from requests the provider verified, and a provider that can do neither isn't supported. Writers and readers don't use SHA-256 metadata from the writer. Every content reference uses the store's algorithm, including references created by inline writes, uploads, copies, restores, and appends.

The stored checksum that a segment descriptor or a layout extent records comes from the object's own write:

- A segment's writer computes the checksum over the segment's bytes, and the segment's put checks that the store keeps the same value.
- A fold, or another write of a chain's pieces, records the checksum that the assembly returns for the new object.
- An upload's extent records the upload's reference checksum, which covers the whole uploaded object.

A write that the provider accepts without returning a stored checksum fails.

A large immutable write sends parts, so it can take longer than one publication request (see [Publication and collection timing](#c1-publication-and-collection-timing)). If the key is already occupied, the writer sends one HEAD request, and the write succeeds if the stored checksum is equal and fails if it's different or missing. No bytes are read back, so a retry whose earlier attempt succeeded also succeeds, by the same check. Among content objects, only a whole object's key is written more than once by design, by the requests to one service-proxied upload session (see [Service-proxied staging](#53-service-proxied-staging)). A span object's key is new for each write, so only an earlier attempt of the same write can occupy it with an equal checksum, and any other object at that key is corruption.

A streamed immutable write computes the store's checksum as the bytes pass through. A stream shorter than one part is buffered and written with a single create request, and a longer stream sends parts as they arrive and completes with the full-object checksum. An occupied key is checked as for a buffered write. An error in the stream prevents completion, and a transport failure isn't retried, because a stream can't be sent twice.

An "assembly" creates one immutable object from source byte ranges followed by new bytes, in the order the caller supplies. Each source identifies an object, an optional half-open byte range (the whole object when there's none), and the checksum of the selected bytes. The expected checksum covers the complete result, and every checksum in an assembly uses the store's algorithm. An assembly fails with a precondition error when a source is missing or shorter than its range. The caller includes each source's length and version from a HEAD response, so the adapter reads or copies that version without another HEAD request.

If the destination already exists, LoonFS compares its stored checksum with the expected checksum. Below the provider's single-request PUT threshold, the adapter attempts a conditional create, and sends a HEAD request if the destination exists. At or above the threshold, it sends a HEAD request before assembling, and if a previous attempt succeeded, it verifies the existing object without copying the sources again.

Each provider adapter builds an assembly differently, depending on the size of the result. The Assembly row of the [Provider matrix](object-storage-providers.md#2-provider-matrix) describes the S3, R2, and GCS paths, and [Local filesystem provider](object-storage-providers.md#5-local-filesystem-provider) describes the local store's. On S3, when an uploaded part needs more bytes to reach 5 MiB, it takes them from the next copied source, except for the final part. On GCS, each compose source also has a generation precondition. A failed or cancelled GCS attempt tries to delete its temporary objects, and a crash can leave them for garbage collection. The in-memory test store can buffer the result before its verified put.

On S3 and R2, the adapter completes with `If-None-Match: *` and the expected full-object CRC.

Small control objects use conditional single-object writes. Multipart upload is an optimization for larger payloads, and it doesn't replace compare-and-swap for control objects.

A large create MUST complete conditionally, so that the provider refuses the completion if the key is already occupied. An absence check before completion isn't enough, because another request could write the key in between. A provider that can't complete a create conditionally isn't supported. The [provider documentation][provider-spec] describes each provider's path.

The incremental-write adapter reads the whole payload before it evaluates its existence precondition, so a digest computed while forwarding the stream covers the complete payload, even when the write is refused.

A failed or cancelled multipart transfer MUST attempt a provider-side abort. An abort can fail and leave incomplete parts behind, so deployments also need the provider cleanup policy that their adapter assumes. An abort doesn't delete a content object that has already completed, and that object's cleanup follows the upload's durable lifecycle.

Why must a large create complete conditionally? Two requests to one service-proxied upload session write the same content key (see [Service-proxied staging](#53-service-proxied-staging)), and the conditional completion stops the second request from replacing bytes that the session has already reported.

### 3.3 Timing assumptions

Writer fencing and commit order use epochs, sequence numbers, and conditional writes, not clocks. Upload admission, snapshot expiry, and garbage collection use deadlines.

Garbage collection assumes that clock error is bounded, and that publications and provider operations finish within the bounds that [Appendix C](#appendix-c-timing-and-size-reference) states. A client-side timeout doesn't prove that a remote change had no effect, so an operation with an ambiguous outcome MUST keep its documented unknown-outcome behavior.

## 4. Reading a namespace

This section describes how a reader finds a namespace's current state, determines what's visible at a sequence, and verifies file contents.

### 4.1 Resolving the metadata basis

A cold read (a read with no cached view) starts by finding the newest manifest. The [manifest boundary](glossary.md#manifest-boundary) (`namespaces/{namespace_id}/boundary.json`) records a manifest number `B`, and every manifest numbered `B` or lower is dead, which means that no process publishes a manifest at such a number or treats one as the namespace's newest. A dead manifest can still exist, because a snapshot keeps it or garbage collection hasn't deleted it yet. A missing boundary object means that `B` is 0. A [retired](#95-retirement) namespace's boundary is in a second state, "retired", which is above every manifest number, so every manifest of a retired namespace is dead. Garbage collection raises the boundary before it deletes manifests (see [One complete collection](#111-one-complete-collection)), and no publisher writes it.

A process that has nothing in memory for the namespace (a cold read, a collection, or a create or fork) finds the newest manifest in these steps:

1. Read the boundary, `b`. If `b` is retired, the namespace is retired, and the find stops.
2. List the manifests numbered above `b`, with a listing of the manifest prefix that starts after the key of manifest `b`.
3. If the listing is empty, read the boundary again. If it's retired, the namespace is retired, and the find stops. If it changed, start again. If it's unchanged, no namespace has the ID when no boundary object exists, and otherwise the store is corrupt, because garbage collection never leaves a namespace that isn't retired without a manifest above its boundary.
4. Load the highest-numbered manifest in the listing, `n`. If that manifest is gone, start again.
5. Read the boundary again. If it's retired, the namespace is retired, and the find stops. If `n` is at or below the boundary, start again. Otherwise `n` is the newest manifest, and a tombstone at `n` means that the namespace is deleted.

When step 1 found the boundary object, the read in step 3 or step 5 is a conditional read with the version that step 1 read, so the store returns no body when the boundary is unchanged (see [Required operations](#31-required-operations)). A find that hasn't returned after `MAX_CONTENTION_ATTEMPTS` (8) attempts fails with `store_contention`.

A cold read then reads the WAL tail above the newest manifest (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), sends a HEAD request for the next manifest number, and sends a conditional boundary read. If the HEAD request finds a successor, or the manifest is at or below the boundary, the read finds the newest manifest again.

For example, a cold read of `main` reads the boundary 44, lists manifests 45 and 46, loads manifest 46, and reads the boundary again. The boundary is still 44, so manifest 46 is the newest, after four requests. If its `folded_wal_no` is 80, the read then sends a GET for each of WAL numbers 81, 82, and so on until one is missing, a HEAD request for manifest 47, and a conditional boundary read. For an ID that no namespace has, the find sends a boundary read, a listing that returns nothing, and a second boundary read.

Each manifest that a successor probe loads above a manifest that a process already has MUST be a valid later manifest of that manifest:

- Its identity fields are equal.
- Its `folded_wal_no`, both epochs, `head_seq`, retention floor, next run number, and activity counters are the same or higher.
- It doesn't follow a namespace tombstone (a tombstone ends the probe).
- It isn't initializing. An initializing manifest 1 is followed only by the same manifest with the status `active`, or by the namespace's tombstone.

A publication's successor check applies the same rules to the next number (see [Publishing a manifest](#72-publishing-a-manifest)). A sequence of manifests that fails this check is corrupt.

| Result of a find | Interpretation |
| --- | --- |
| The listing is empty, the boundary is unchanged, and no boundary object exists | No namespace has the ID. |
| The listing is empty, the boundary is unchanged, and a boundary object exists | The store is corrupt. |
| The boundary changed after an empty listing, the highest listed manifest is gone, or the loaded manifest is at or below the boundary | Start again. |
| A required object is unreadable or invalid | An error, not absence. |
| The newest manifest is initializing | Every request except a create, fork, or delete with the namespace's ID, and a collection, returns `namespace_not_found`. |
| The newest manifest is deleted | Ordinary namespace operations return `namespace_deleted`. |
| A boundary read returns retired | The namespace is retired. Every request except a collection returns `namespace_deleted`. |

Every namespace has its own manifest from the moment it's installed. A new namespace's empty manifest describes the root directory at sequence 0, and [Publishing a manifest](#72-publishing-a-manifest) lists what a first manifest contains for a create and for a fork. Readers don't follow a fork's `fork_basis` to choose a different basis.

The newest manifest and the unfolded WAL together make up one read view:

```text
boundary 4 --list--> 5, 6 --load 6, boundary 4--> manifest 6 is the newest
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

Why is the boundary read again after the highest manifest is loaded? A manifest at or below the boundary is dead, whatever the listing showed. For example, a process loads manifest 46 above the boundary 44 and pauses. Meanwhile manifests 47 to 50 are published, and a collection raises the boundary to 49 and deletes manifests 45 to 49. The second read finds the boundary 49, so the process starts again and finds manifest 50, however long it paused.

### 4.2 Replaying the visible WAL

After it selects a manifest, a reader reads the WAL from `folded_wal_no + 1` upward until the first missing number, and keeps those objects for replay. It can read numbers in concurrent windows, so it reads a missing number again before treating it as missing, because the number might have been published before a later number was read. The first number still missing ends the tail, and one still missing below a present number is a missing object. Replay requires every WAL number between the folded boundary and the discovered tip.

Each WAL object MUST pass these checks:

- A WAL object other than a fence or a [floor object](glossary.md#floor-object) contains contiguous commits that follow the preceding head.
- A fence or a floor object contains no metadata changes.
- It meets the rules in [WAL records](#a5-wal-records) for how `prior_head_seq` is derived (so a first sequence of 0 is invalid, and a fence's or floor object's preceding head is its own `head_seq`) and for a floor object's `retention_floor_seq`.
- Its namespace identity, WAL number, sequence range, and writer epoch are valid.
- Epochs don't decrease along the log, and they don't exceed the current manifest's epoch.

When discovery finds a missing or invalid WAL object, the reader reloads the current manifest. If the manifest number changed, discovery starts again from it, and if it didn't, the store is corrupt. This includes a WAL object whose epoch is higher than the loaded manifest's, because another writer might have claimed that epoch after the manifest was loaded.

A deleted manifest ends discovery, without a WAL probe or a successor probe. For an active manifest, the check after WAL discovery (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)) stops a concurrent fold from making a WAL number look unused when garbage collection has deleted its object. A required object that's missing or malformed fails the read. An ordinary read on a manifest that a newer manifest replaced, which finds a segment missing, runs again from the current manifest, and returns `store_contention` after `MAX_CONTENTION_ATTEMPTS` (8) reads in the reference implementation. A read view on such a manifest returns `stale_head`, and the caller reads again from current state (see [Reference roots](#112-reference-roots)).

Every process that already has a manifest in memory checks it the same way: a cold read after WAL discovery, a cached view, a [writer position](glossary.md#writer-position), and a process after a fence put or a floor object put (see [Writer ownership](#61-writer-ownership) and [Advancing the retention floor](#101-advancing-the-retention-floor)). A manifest publication instead sends its put and then reads the boundary (see [Publishing a manifest](#72-publishing-a-manifest)), and a namespace's collection doesn't check the manifest that its find returns (see [One complete collection](#111-one-complete-collection)). The process:

1. Probes for a successor of its manifest. If newer manifests exist, the process moves onto the highest one or reloads, as the rules for each kind of check describe.
2. Reads the [manifest boundary](glossary.md#manifest-boundary). If the manifest that the process now has is at or below the boundary, the process drops it and finds the newest manifest from the start (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)). Otherwise that manifest is the newest.

No process trusts a missing successor because its last check was recent.

A process keeps the boundary as it last read it (the value and the version) beside its manifest, and a check's boundary read is a conditional read with that version (see [Required operations](#31-required-operations)). Until a collection first raises the boundary, there's no boundary object and no version, so each check sends a plain read that finds no object. A process whose kept boundary is at or above its manifest's number sends no request, because the boundary never decreases, but it never trusts a kept boundary below that number without the read.

A process can keep a reference to its manifest instead of the manifest itself, as a writer position does, and read the manifest from the store again when it isn't in the segment cache. Before that read, the process runs step 2 with its kept boundary, and it doesn't read a manifest at or below the boundary. If the read finds the manifest missing, the process reads the boundary again. If the manifest is at or below the boundary, the process finds the newest manifest from the start, and otherwise the store is corrupt. For example, if other processes publish manifests 41 to 45 and a collection raises the boundary to 44 and deletes manifests 40 to 44, a session whose position is on manifest 40 reads the boundary before its next fold reads manifest 40, finds 44, and folds from manifest 45.

A read is evaluated at one sequence. Replaying the WAL produces a projection of its metadata changes and any inline file bytes, called the "projected WAL tail". An implementation can query verified metadata segments and this projection directly, without loading every metadata row into memory, as long as it applies the same visibility rules.

Replay also determines the view's [retention floor](glossary.md#retention-floor), the highest of the manifest's `retention_floor_seq` and that of each replayed floor object, which [Advancing the retention floor](#101-advancing-the-retention-floor) defines along with the rules that read it. For example, if the manifest's floor is 5 and the tail contains a floor object at WAL number 31 with floor 20, the view's floor is 20.

A warm read is a read from a cached view. The reference runtime checks a cached view with a "refresh", which sends its requests in this order:

1. For every `strong` warm read, the WAL probe reads the WAL numbers after the cached tip in discovery's concurrent windows, starting with a GET of the next number (so a probe at the tip costs one GET, and a missing next number confirms the tip). Each present object advances the reader's state, and the first number still missing ends the probe. If a number below a present one is still missing, the reader runs discovery again, because a fold and garbage collection might have deleted the WAL objects after the cached tip.
2. If the view's manifest check is due, the successor probe (a HEAD of the next manifest number) follows. The check is due when the view's last manifest check is older than the manifest check interval (`manifest_revalidation_interval_ms`, one second by default, on a monotonic clock).
3. If the successor probe finds nothing, the reader reads the manifest boundary, and if the read fails, the refresh returns the error to the read.

If the successor probe finds a successor, or the view's manifest is at or below the boundary, the reader runs discovery again. A refresh whose manifest check isn't due sends the WAL probe alone. The runtime runs one refresh of a namespace at a time. A read that finds a refresh running waits for it, because the refresh might have sent its probes before the read started, and then shares the next refresh with every read that arrived during the first.

Why does the WAL probe come before the successor probe? A fold can move a WAL object above the view's tip into a newer manifest, and garbage collection can then delete the WAL object. If the successor probe ran first and found nothing, the fold and the deletion could happen before the WAL probe, and the view would miss those commits. With the WAL probe first, the successor probe finds the fold's manifest, or the boundary read finds that garbage collection deleted it. A cold read uses the same order.

An `eventual` warm read (see [Read consistency](api.md#read-consistency)) uses the cached view without probes while the cached tip was confirmed less than the read's maximum staleness ago and the previous successor probe is less than `READ_REVALIDATION_BOUND_MS` old. The confirmation time is taken before the request that confirmed the tip (a WAL probe or a load) is sent, so the read sees every commit acknowledged before then. The bound keeps the content that the view refers to until a download URL that the read issues expires (see [Content roots](#119-content-roots)). After that, an `eventual` read runs the probes as a `strong` read does.

The reference runtime replaces a cached view only with a newer one, so an `eventual` read never reads an older head than a read that completed before it. When the runtime makes or learns of a change whose effect on the head it can't identify (it deletes the namespace, a mutation's outcome is unknown, or it finds its writer session fenced), it drops the cached view, and the next read runs discovery. A load that started before the drop doesn't install its result, because it might have read the namespace before the change.

A fork publishes a manifest of its source only when it has to fold the source first (when the tail is older than `MAX_TAIL_COMMIT_AGE_MS`, see [Forking a namespace](#92-forking-a-namespace)), and it folds through the runtime's fold path, which moves the view as any fold does. The snapshot records written by the fork on the source aren't part of the view. The runtime's view of the fork's new namespace is the active manifest that the fork published, and a create's is the new namespace's manifest 2. Both checks of that view are at the start of the create or the fork, when the namespace had no commits and no newer manifest, and the view's WAL tail is empty.

In a runtime with an open writer session for the namespace, reads start from the session's [writer position](glossary.md#writer-position) instead of a cached view. The runtime moves the position to each commit before acknowledging it, to each floor object that the session creates, and to each manifest that the runtime publishes (a fold, a compaction, a snapshot, or an [extension's](#124-extensions) publication). A read from the position sends no WAL probe, because another writer has to publish a claim manifest before it writes a WAL object with commits, and the session's successor probe finds that manifest.

A [floor object](glossary.md#floor-object) that another process creates changes only the floor, and it reaches the position at the session's next discovery (after the session's next put collides with it) or when the successor probe finds the manifest of the fold that covers it. Reading under the earlier floor is safe, because compaction removes nothing until a manifest records the new floor, and the session's next put collides with the floor object, so no restore is committed under the earlier floor.

An `eventual` read starts from the position without a probe while the previous successor probe is younger than the read's maximum staleness, and after that it waits for the session's probe, as a `strong` read always does. A probe counts for a read only if it was sent after the read started. The session runs one probe at a time, so a read that finds a probe running waits for it and then for the next one, which it shares with every read that arrived during the first. A new position's previous successor probe is the last check of its manifest: the check of the discovery that loaded the position, the check after the session's fence put (see [Writer ownership](#61-writer-ownership)), or a later probe of the session.

If the probe finds a successor, the reader loads the manifests above the position's manifest. While the newest is an active manifest with the position's writer epoch (a fold, a compaction, a snapshot, or an extension's publication, by any process) that doesn't fold past the position's tip, each describes the head the session wrote, so the position moves onto them as onto its own runtime's manifests, and the read starts from the position. A moved position keeps the higher of its own floor and the manifest's floor, and records the walk that ends on the newest manifest as that manifest's successor probe.

The walk stops at another writer's claim (a manifest with a later writer epoch) and at a tombstone. It also stops at a manifest whose `folded_wal_no` is above the position's tip, because that manifest folded a WAL object that the session hasn't read, such as another process's floor object. It stops, too, when it finds more than eight manifests above the position's. The reader then drops the position and runs discovery, and so does the session's next publish. A writer MUST NOT keep a position whose manifest folds past the position's tip, whichever process published that manifest. For example, a session has tip 30, and another process creates a floor object at 31 and publishes manifest 7, whose `folded_wal_no` is 31. The session's probe finds manifest 7, so the session drops its position, and its discovery starts from manifest 7 with tip 31.

After the successor probe and the walk, the session reads the manifest boundary. If the position's manifest is at or below the boundary, the reader drops the position and runs discovery, and if the boundary read fails, the check returns the error to the read. The session checks a position this way whatever its age, and doesn't drop a position because of its age.

A runtime without the namespace's writer session, or whose session is fenced, closed, or hasn't published yet, reads from a cached view. When a session's position is dropped or the session ends, the position becomes the runtime's cached view, so the next read starts from a head at least as new as the position's reads returned. If the position was dropped because its probe found a successor, because its manifest is at or below the boundary, or because the session was fenced, the runtime drops that cached view too, and the next read runs discovery.

Why does a process read the boundary after its successor probe? A missing successor means either that no newer manifest was published, or that one was published and garbage collection deleted it. Garbage collection raises the boundary first, so the boundary read tells the two apart.

Why doesn't a namespace's collection check the manifest that its find returns? The collection's roots are a lower bound (see [Reference roots](#112-reference-roots)), and a later manifest's new segments are younger than the grace window, so a collection that found an older manifest deletes fewer objects, not more. (The find's last boundary read already makes sure that the manifest it returns is above the boundary.)

Why does a session drop its position instead of moving it onto a manifest that folds past the position's tip? If the folded tail contained another process's floor object, moving would leave the position's tip below the manifest's `folded_wal_no`, and in the floor object example above, the position would plan its next commit at 31. That put would collide with the floor object, which no process deletes, but the position's head would be one that no discovery from that manifest returns, and the argument in [Advancing the retention floor](#101-advancing-the-retention-floor) relies on a manifest from another process never raising a position's tip.

### 4.3 Visible metadata

At sequence `N`, a reader ignores events after `N`. A visible inode MUST have been created at or before `N`, and it MUST NOT be hidden by a tombstone on itself or on an ancestor. A tombstone hides its root and the root's descendants at `N` when its latest event at or below `N` is a `set` (see [File and subtree deletion](#16-file-and-subtree-deletion)). For example, a file in a directory whose deletion has expired stays hidden, even though nobody can restore the directory anymore.

As in [Directory bindings](#13-directory-bindings), an edge's value at `N` is its first row in key order at or below `N` (its newest, because each edge's rows sort newest first), a slot's value is the newest of its edges' values, and parent lookup applies the same rule to the child index. Resolving a path takes one slot-index lookup per component, which reads each edge of the slot only as far as the edge's first row at or below `N`. A listing reads only the parent's binding rows, in one scan of the parent's prefix in name order, and includes a bound entry if no tombstone hides the child. Binding rows copy the child's kind and creation attribution, and a bound file row copies the file's revision summary, so listings and path resolution don't read the child's inode row, and a listing doesn't read the file's revision rows. User snapshots apply these rules through their own manifests, at their captured sequences.

A file's current content is its latest revision committed at or before `N`. Its attributes are the latest applicable complete attribute revision, or the initial empty map when there's no applicable attribute row. Listing the trash uses the derived trash rows, while evaluating a tombstone at a past sequence uses the tombstone events.

### 4.4 Paths, listings, and revision reads

A reader resolves a path from the root directory's inode. It maps each component to its name key (see [Names and paths](#14-names-and-paths)) and follows the active binding. If a component isn't visible, the path doesn't exist. (The current format has no mount points to traverse.)

A directory listing reports each file's revision number, size, and revision attribution from the revision summary in its binding row (see [Directory bindings](#13-directory-bindings)), and not the file's content reference, which a read of the file's entry by path or by inode returns. A listing MUST NOT fetch and verify every file's content object just to list a directory.

A path-based revision read resolves the current inode at that path and then looks up the requested revision of that inode, not of every file that has ever been at the path. An inode-based revision read reads the inode's retained history directly, so it can read a file that has no current path, such as a file under a deletion in the trash.

A read by inode ID MUST return `inode_not_found`, the error for an unknown inode, when the inode's ancestry refuses the read, whether it reads the inode's entry, revisions, attributes, or content. To check the ancestry, the reader starts at the inode, follows the current bound bindings upward, and stops at the first tombstone that hides the inode or one of those ancestors (see [File and subtree deletion](#16-file-and-subtree-deletion)):

- If that tombstone's deletion is at or below the view's [retention floor](glossary.md#retention-floor) (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), the deletion has expired, and the read returns `inode_not_found`.
- If its deletion is above that floor, the read is allowed, and the subject's rights are evaluated through the deletion's saved parent, as for any deletion in the trash (see [Access rows](#18-access-rows)).
- If the walk reaches the root inode without meeting a tombstone, the read is allowed.
- If the walk reaches an inode with no bound binding before it meets a tombstone, the ancestry is disconnected, and the read returns `inode_not_found`.

A tombstone whose latest event is `revoke` hides nothing, and the walk passes it. The reader applies this rule before it evaluates any access grant, so every principal gets the same result, and the reader applies the rule from the moment the view's floor reaches the deletion, even while the inode's rows are still stored.

For example, suppose `/old/draft.txt` is deleted at sequence 30, `/old` is deleted at sequence 40, and a retention advance moves the floor from 35 to 50. With the floor at 35, a read by inode of the revisions of `/old/notes.txt` returns them, and the same read for `/old/draft.txt` returns `inode_not_found`, because that file's own deletion is the first tombstone and it's at or below the floor. With the floor at 50, both reads return `inode_not_found`. If a base compaction of the bindings group then removes the bindings under `/old` while the revisions group still stores the revisions of `/old/notes.txt`, the walk finds no binding and no tombstone, so the read still returns `inode_not_found`.

Why does a disconnected ancestry refuse the read? Every live inode has bound bindings up to the root inode, and every deleted inode has them up to its deletion's root, which has a tombstone, so a walk that finds neither has found an inode that belongs to nothing the namespace can show. Base compactions remove an expired subtree's rows one family group at a time, so the bindings that lead to the tombstone can be gone while the revisions below it are still stored.

Why does a read by inode refuse an expired subtree whose rows are still stored? An expired deletion can't be restored, so no request needs those rows, and refusing every read from the floor advance on means that removing the rows, in any order of family groups, can't change what a read returns or who can read it. If a read by inode still evaluated the grants inside the subtree, removing a directory's access row before the revisions below it would remove that directory's boundary, and principals that the boundary excluded could read those revisions. A compaction removes those rows only in a manifest whose retention floor is at or above the deletion (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)), so every view that can be missing them already hides the subtree.

### 4.5 Content verification

A content reference identifies a prefix of a chain and its checksum. To read it, a reader uses the view's layout for that chain, and then the chain's visible WAL pieces, which the projected tail indexes by content ID and offset. The reader uses the longest visible layout for the reference's own chain (the newest one, if several have the same length), and it looks in both the tail rows and the manifest's segments.

| Condition | Where the reader reads the bytes |
| --- | --- |
| The tail has pieces at or below the reference's size | The reference's own chain layout, up to the lowest tail offset, and then the pieces, up to the reference's size. |
| No pieces cover the reference | The view's layout, up to the reference's size. |
| Zero bytes, and no pieces | Nowhere. The reader verifies the empty value without a content request. |
| A nonzero folded prefix with no layout | Nowhere. The reader reports corruption, identifying the content ID. |

When the reference is shorter than the layout, the read shortens the last extent it uses. Each extent is read with one ranged GET at its object offset, and no read makes a HEAD request. If an object is missing, the error identifies the missing extent's key, and a short range reports its expected and actual lengths. The reader checks the assembled length and checksum against the reference, and those errors identify the reference's own key. A [ranged read](glossary.md#ranged-read) returns part of a file, so the reader checks the length of each ranged GET but can't check the file's checksum.

When a ranged GET returns a whole span object (from offset 0 to the span's `end - start`), the reader also checks the bytes against the extent's object checksum, which needs no extra request. A mismatch is corruption, and the error identifies the span object's key. For example, if a chain's layout lists a span object from 100 to 130, a ranged read of bytes 100 to 130 reads the whole object and is checked, and a read of bytes 110 to 120 isn't. An extent that lists a chain's whole object records the range it takes from the object but not the object's size, so the reader can't tell whether a ranged GET returned the complete object, and doesn't check it against the object checksum. (A read of the whole file is still checked against the reference's checksum.)

A fork reads an inherited layout's objects with the same keys as its source. WAL replay loads pieces along with the metadata, so pieces already in memory need no content request. Replay checks each WAL object's envelope, but it doesn't recompute file checksums.

Deleting a WAL object doesn't remove bytes that are already in a read view's projection. But if that projection has to be rebuilt and the WAL objects it needs are gone, the read MUST fail, even if the content object exists, because those WAL objects are also needed to rebuild the view's metadata.

For a streamed read, the full-file checksum can only be verified after the complete stream has been processed, and the transport MUST report a verification failure even if some bytes have already reached the client. A direct download returns one presigned range per extent, and the client receives the bytes directly from object storage. The [API specification][api-spec] defines the client's verification responsibilities for direct downloads.

Why isn't a ranged GET of part of an object checked? The object checksum is a CRC of the whole object, so it can't check a slice. That would need a checksum for each part of the object, such as one per block.

## 5. Uploading content

This section describes upload sessions, the ways bytes can be transferred, and the proof a commit needs before it can reference uploaded content.

### 5.1 Upload sessions

Every uploaded content object is tied to an upload session before a commit can reference it, and the session's content ID is allocated when the session is created, before any file bytes are read. The object and the session's records are stored under the prefix of the namespace's [pool](glossary.md#pool) (see [File contents and ownership](#15-file-contents-and-ownership)), and each one stores the namespace that created the session.

A session is stored as two records, each created once with put-if-absent and never changed:

- The "session record" is written when the session starts. It contains `namespace_id`, `upload_id`, `content_id`, the `actor_id` of the actor that opened the session, a tagged `mode`, and `expires_at_ms`. Only that actor can use the session.
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

A session starts open and ends at most once. Completion and abort both create the outcome record with put-if-absent, so the first create to succeed ends the session, and the other request finds that outcome. Completion verifies the content before it creates the outcome record, and abort creates the outcome record before it cleans up the object or the provider transfer. After a put with an unknown result, a request reads the outcome record back, and the request created that record if the stored record equals the one it sent.

An outcome record counts only while its session record exists. After creating an outcome record, a request MUST check that the session record still exists, and if it's gone, MUST NOT return admission evidence or delete content. Garbage collection deletes the session record first (see [Upload-session cleanup](#116-upload-session-cleanup)).

At or after `expires_at_ms`, staging, part signing, and completion refuse an open session as they refuse an aborted one, so an expired session never provides content for a commit, and garbage collection aborts it (see [Upload-session cleanup](#116-upload-session-cleanup)). Status reads and aborts don't check the expiry.

Upload records don't advance the namespace's sequence and aren't change-feed events. Completing an upload doesn't automatically update a file in a LoonFS namespace, and a completed upload might never be referenced by any file.

Why does a session have two records instead of one whose status changes? With one record, an upload would write it at the start and again at the end, often less than a second later, and some providers allow about one write per second to one object (see the [provider documentation][provider-spec]). [Alternatives considered](#13-alternatives-considered) gives the full argument.

Why does a request check the session record after it creates an outcome record? Once garbage collection deletes an ended session's records, the outcome key is free again, so a completion or abort that read the session as open and then paused could create an outcome record for a deleted session. A completion could then issue evidence for content that an abort deleted, or an abort could delete content that a commit might reference. Garbage collection deletes the session record first, so the check catches both cases.

### 5.2 Transfer modes

Each session uses one transfer mode, which is set when the session is created and can't be changed:

| Mode | How the bytes are transferred | Stored mode data |
| --- | --- | --- |
| `service_proxied` | The client sends the bytes to the LoonFS server, which writes the content object. | Nothing |
| `direct_put` | The client writes the whole object directly to the provider, with one presigned PUT. | The selected whole-object `checksum_algorithm` |
| `direct_multipart` | The client uploads parts directly to the provider. | `provider_upload_id`, `part_size_bytes`, and `checksum_algorithm` |

The client tracks multipart progress, and resumed work uses the stored part size and algorithm. Completion and cleanup use the stored provider upload ID.

A completed outcome record's content reference MUST match the session's content ID, and for a direct upload it MUST use the algorithm recorded in the mode. A record with missing required fields is corrupt, and so is an outcome record that contradicts its session record.

### 5.3 Service-proxied staging

In a service-proxied upload, the server writes the bytes it receives to the session's content object, a step called "staging" that writes no record. The object itself shows what was uploaded, because no other session uses its content ID and the provider stores its size and full-object checksum.

Every request to one session writes the same key, and [Immutable writes and retries](#32-immutable-writes-and-retries) lets only one of them create it. A request that finds the object written accepts it as a retry if its stored size and checksum match the request's bytes, and conflicts otherwise.

Staging computes the store's checksum while it streams the bytes. When the body ends, the request rereads the session's records and lets the object store finish the write only if the session is still open. After the write, it rereads them and reports the uploaded reference only if the session is still open, because an abort during the write deletes the object.

Completion reads the object's stored size and checksum with one HEAD request and records them in the outcome record's content reference. If the object doesn't exist yet, completion fails, and the session stays open.

Why doesn't staging record the content reference? The stored object already has the size and checksum, which the provider verified on write, so a record would only add a write during the upload. A direct PUT completion relies on the same stored values.

### 5.4 Direct uploads

A direct upload sends bytes from the client to the provider through a short-lived presigned URL. The server allocates the content ID and constrains the target key, so a client can't choose an arbitrary object key.

Direct PUT uses the checksum algorithm selected when the session starts, and direct multipart uses the one stored in the session (currently CRC-64/NVME). The provider MUST enforce the signed transfer constraints and MUST expose the stored whole-object checksum in the supported algorithm.

At completion of either kind, the server compares the provider's stored size and checksum with the client's claim (a client digest alone isn't enough). After the completion call, only the object at the key counts as evidence, whatever the provider responded. On a mismatch, the server rejects the claim and changes nothing, so the session stays open and nothing is deleted. A session that can't complete ends with an explicit abort, or when its lease ends (see [Upload-session cleanup](#116-upload-session-cleanup)).

Provider-specific checksum headers, completion APIs, and response encodings belong in the provider adapter. A LoonFS checksum has one canonical representation, whether the provider returned it as hexadecimal or base64.

### 5.5 Admission proofs

A commit can reference uploaded content only with "admission evidence", which is proof that the referenced bytes were verified in a completed upload. The evidence is tied to the namespace and MUST match the complete content reference, not only the content ID. Inline content needs no evidence, because it starts a new chain or extends one that the committing namespace created (see [Operations and WAL deltas](#66-operations-and-wal-deltas)).

Admission evidence is valid while the clock reads before its expiry, and expired at the expiry instant. Three windows apply ([Upload admission](#c2-upload-admission) lists the values):

- A completed session can produce evidence only during `COMPLETED_UPLOAD_RECEIPT_WINDOW_MS` after its original completion time. A status read or a cached response doesn't restart that window, and nothing is issued at or after its end.
- A signed token lasts `CONTENT_RECEIPT_TTL_MS` from when it's issued.
- Evidence prepared in process, without a token, expires at the end of the admission window, `COMPLETED_UPLOAD_ADMISSION_WINDOW_MS` after the completion. No token lasts past that window.

The reference HTTP server represents admission evidence as signed content tokens, which an embedded implementation doesn't need to issue. The [API specification][api-spec] defines token encoding, signing, and HTTP responses.

Immediately before publishing, the writer checks that every externally supplied content reference has matching, unexpired evidence (see [Publication and group commit](#63-publication-and-group-commit)). The check includes the time spent acquiring or checking the writer, loading the view, planning, and preparing the WAL object. It uses the request clock plus the monotonic time elapsed since the batch call began, including earlier attempts, so a retry doesn't restart the evidence's validity.

A retry that matches a retained commit receipt replays without new evidence. Internal copy and restore operations reuse references that the validated namespace state already established, and a merge reuses the fork's. A commit has no other way to reference content that another namespace wrote.

Why does admission evidence expire? Without an expiry, a commit could reference a completed upload at any time, so garbage collection could never safely delete one that nothing references. The receipt window, token lifetime, and publication budget set the earliest safe deletion time (see [Upload-session cleanup](#116-upload-session-cleanup) and [Appendix C](#appendix-c-timing-and-size-reference)).

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

1. **Claim.** The session builds on a manifest it has, or loads the current one, and publishes its successor with `writer_epoch + 1` and a diagnostic writer block. When no writer has claimed the namespace at its cached view (such as the active manifest that a create or a fork in the runtime published), the reference runtime builds on that view's manifest, so the claim's put is its first request. The boundary read after the put checks the manifest, however old the manifest is (see [Publishing a manifest](#72-publishing-a-manifest)).
   - The claim runs under `METADATA_PUBLICATION_BUDGET_MS`, measured from the start of the acquisition (before the session loads the manifest it copies, if it loads one). If the budget expires before the put starts or before the put is confirmed, the acquisition fails.
   - If another publisher took the claim's number, the session reloads the current manifest and claims again with the next epoch.
   - If the put's outcome is unknown, the session reads back the manifest at its number. If it finds its own bytes, that manifest covers the claim, and the session claims again (the epoch it leaves behind never gets a fence). If it finds another manifest, the claim lost, and the session claims again. If it finds no manifest, the acquisition fails.
2. **Fence.** The session discovers the tip. If the current manifest now has a higher epoch than the claim, another session has claimed since, and the acquisition returns `writer_fenced`. Otherwise, the session creates a fence (a WAL object with no records) with its epoch at the next WAL number. This discovery reads the WAL tail without checking its manifest, because the check after the put checks that manifest.
   - After the fence put succeeds, the session checks the manifest that its discovery loaded, with the successor probe and the boundary read (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). If that manifest is still the newest, the fence is live, because the discovery read the tail above its `folded_wal_no`. Otherwise, the session loads the newest manifest. If that manifest has a higher epoch than the claim, the acquisition returns `writer_fenced`. If the fence's number is above that manifest's `folded_wal_no`, the fence is live. Otherwise the fence is dead. It stays, no reader replays it, and the session puts a fence again at the tip that it finds.
   - After a collision, the session discovers the tip again and retries at the new number. These retries, and the retries after a dead fence, stop when the acquisition's `METADATA_PUBLICATION_BUDGET_MS` expires, and the next retry fails the acquisition instead.
   - A transport failure fails the acquisition, and the next attempt claims a new epoch.
   - Between the claim and the fence, the previous writer can still commit at `tip + 1` (see [A summary of the log protocol](#26-a-summary-of-the-log-protocol)), and a fence planned before such a commit collides with it and retries above it. Once a fold covers those commits and they're older than the grace window, a collection can delete them, so a fence put that waited that long can succeed at an emptied number, and the check after the put finds the fence at or below the newest manifest's `folded_wal_no`. For example, a new session for `main` claims epoch 6, discovers tip 30, and pauses before its fence put. The previous writer (epoch 5) commits 31 to 40, a fold covers them, and two days later a collection deletes them. The fence put at 31 succeeds, but the check finds `folded_wal_no` 40, so the session puts its fence again at 41.
   - A fold that covers a live fence between the put and the check also makes the check treat the fence as dead, which costs one more fence and changes nothing else.
3. **Acquired.** After the fence is written, the session uses its epoch for every later batch, and an older writer's next put collides. If a later discovery (after a collision or a refresh) finds a higher epoch, the session is fenced, so it returns `writer_fenced` and doesn't reacquire authority on its own. Replay never changes the epoch, so the discovery before a commit or a fence compares each manifest's epoch with the session's before it reads the WAL tail above that manifest, and a fenced session's discovery reads no WAL object.

A fence contains no commit records and keeps `head_seq` unchanged, so its derived `prior_head_seq` equals its `head_seq`. Concurrent attempts are ordered by the conditional creation of the next number.

There's no writer lease or writer-expiry timestamp. The `writer_id` and `acquired_at_ms` fields describe the acquisition, and the epoch alone determines authority. An acquisition retried after an uncertain outcome can raise the epoch again. Commit retries are identified separately, by durable receipts (see [Commit identity and retries](#65-commit-identity-and-retries)).

Why use epochs instead of a lease? With a lease, a new writer would have to wait for the old lease to expire. With epochs, a new writer can take over at any time, and correctness depends only on conditional writes. Taking over costs a manifest publication and a fence write (about 200 ms), so most deployments keep one long-lived writer session in a server that many clients share.

### 6.2 Validation

Before it plans new mutations, the writer rebuilds the metadata state from a verified basis and the visible WAL. It resolves paths and inode references, checks content admission, evaluates preconditions, and generates an ID for each inode that a request creates, from the commit's sequence and the inode's create number (see [Namespaces and identity](#11-namespaces-and-identity)). A [merge](glossary.md#merge) doesn't generate IDs, and each inode that it creates keeps its ID from the fork.

The operations in one request are evaluated in order, and later operations see the tentative effects of earlier ones. When several requests share a publication batch, the writer chooses their order and evaluates each one against the requests accepted before it.

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

Callers add specific checks with `expected_*` preconditions. Leaving out an optional precondition turns off that check, but not the operation's normal structural validation. When a revision precondition comes with an optional inode precondition, the revision precondition requires the matching inode precondition. Inode-addressed revision writes require `expected_revision_no`, and inode-addressed moves and deletes require `expected_binding_version`.

A rejected request isn't assigned a sequence number and creates no WAL record. Passing validation is only tentative acceptance, not success.

Restore validation (of a deleted item or a file revision) and the merge floor rule (see [Merges](#merges)) read the [retention floor](glossary.md#retention-floor) of the writer's position, which accounts for the [floor objects](glossary.md#floor-object) in the position's WAL tail (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). So with a manifest floor of 5 and a floor object with floor 20 in the tail, a restore of a deletion at sequence 10 returns `not_deleted`.

#### Merges

A [merge](glossary.md#merge) MUST continue the previous merge from its fork, so that each of the fork's commits is applied once and in order. The writer reads the fork's newest row in the [merge index](glossary.md#merge-index) (the `merges` family). The merge's `source_after_seq` MUST equal that row's `source_through_seq`, or, if the fork has no row, the fork point (the sequence of this namespace that the fork was made at). Otherwise the merge fails with `merge_range_conflict`. For example, if a fork made at sequence 100 has had its commits 101 through 120 merged, its next merge has to apply the commits after 120, and a merge that starts after 110 (an overlap) or after 125 (a gap) is refused. The writer checks the commit receipt first, as for every commit, so a retry of a merge with its original commit ID returns the original commit instead of a conflict. Unlike every other commit, a merge MAY have no operations. When the fork's net change over the range is empty, the merge's commit includes only its merge record, so the merge index still records that the range was examined.

A base compaction removes the `merges` rows strictly below the [retention floor](glossary.md#retention-floor) (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)), so a merge's fork point MUST be at or above the retention floor of the writer's position, and the writer refuses a merge whose fork point is below that floor with `below_retention_floor`. Every merge from a fork is above the fork point, so while the floor is at or below the fork point, compaction keeps all of the fork's `merges` rows. Without this rule, a compaction could remove the fork's newest merge row, and the writer would accept a next merge that started at the fork point and applied merged commits again. For example, a fork made at sequence 100 had its commits 101 through 120 merged as this namespace's commit 300. Once the floor is above 100, the writer refuses the fork's next merge, whether the floor is at 150 (the row for commit 300 is still kept) or at 310 (a compaction can remove it).

A [rebase](glossary.md#rebase) replays a fork's unmerged commits into a new fork of the namespace that the fork was forked from, and each "replay commit" is a merge into the new fork. The new fork's fork basis records `rebased_from`, which identifies the replayed fork (see [Forking a namespace](#92-forking-a-namespace)). For a merge into a namespace whose `fork_basis.rebased_from` identifies the merge's source, the two rules above use the namespace's own fork point (`fork_basis.fork_point_seq`) in place of the source's:

- The range rule considers only the namespace's own `merges` rows, the ones above its fork point. If the namespace has such a row, the merge's `source_after_seq` MUST equal the newest row's `source_through_seq`, and otherwise `source_after_seq` MUST equal `resume_after_seq`.
- The floor rule compares the namespace's own fork point with its retention floor, so the namespace accepts replay commits only while its floor is at its fork point.

For example, a fork made at `main`'s sequence 100 had its commits 101 through 120 merged into `main`, and a rebase then made a new fork of `main` at sequence 400, whose `resume_after_seq` is 120. The new fork's first replay commit starts after 120, the next one starts where the first stopped, and once the new fork's floor is above 400, the writer refuses its replay commits with `below_retention_floor`.

Why does a replay commit need the substitutions? The new fork inherits the `merges` rows that `main` committed at or below the new fork's fork point, and a base compaction of the new fork removes them, because the new fork's floor starts at the new fork's fork point. Without the first substitution, the writer would then expect a replay commit to start after the replayed fork's fork point, and would accept a replay of commits that `main` already merged. Without the second, the floor rule would refuse every replay commit, because the new fork's floor starts above the replayed fork's fork point.

A merge MUST NOT replace a row that the fork didn't see, so that a change committed in this namespace after the fork point isn't lost. LoonFS plans a merge's operations itself, from the fork's "net change" over the merge's range (the difference between the fork's state at `source_through_seq` and at `source_after_seq`, with one entry for each inode). For each inode, the net change compares the inode's binding row, which records its parent and name, and not whether the inode is visible. The binding row changes only when the fork creates, moves, deletes, or restores that inode. For example, if the fork deletes `/docs/a.txt` and then `/docs`, the net change includes both deletions. If the fork deletes only `/docs`, the net change has no entry for `/docs/a.txt`. If a deletion in the fork hides an inode at `source_through_seq`, and the inode is in the net change, the merge deletes the inode wherever this namespace has it bound, unless a deletion that the merge doesn't undo already hides the inode there (see the first case of the conflict rule below). The commit also leaves out that delete when the inode's ancestors in this namespace reach a directory that the merge deletes before they reach an inode that the merge moves, because that directory's recursive delete already removes the inode. The "planner" (the part of LoonFS that turns the net change into operations) applies the "conflict rule" below.

If the merge's range starts below the fork's own retention floor, the fork no longer keeps the history that the net change needs, so the planner MUST refuse the merge with `below_retention_floor`. The conflict rule works per row. For each inode in the net change, the planner MUST leave out each row (content reference, binding, attributes, and access row) whose value in the fork equals its current value in this namespace, so the merge replaces only the rows that differ. A row of this namespace that the merge would replace is "the fork's own" if its `committed_seq` is at or below the fork point, or if the commit row at its `committed_seq` includes a merge record whose `source_namespace_id` is the fork. Any other row is "foreign".

The rule has three cases:

1. **An inode that exists in this namespace.** If a tombstone on the inode itself hides the inode at this namespace's head, that tombstone MUST be the fork's own, and its deletion MUST still be in this namespace's trash. The planner then plans a restore, which replaces the tombstone row. The planner checks the ancestors of the destination parent of each move and restore, and, for an inode that an ancestor's deletion hides at this namespace's head, the ancestors of the inode's current parent, whatever the change. From that parent, the walk up through this namespace's current bindings MUST reach a visible directory, or a directory that the same merge creates or restores, before it reaches a directory that a tombstone on that directory itself hides. Otherwise the change is a `visibility` conflict, except a delete, which the planner leaves out, because the inode is already hidden in this namespace and the merge doesn't undo the deletion that hides it (so if the fork deletes `/docs/a.txt` and this namespace deletes `/docs`, the merge has nothing to do for `a.txt`). For example, if an earlier merge of the fork deleted `/docs`, and the fork then restores `/docs` and edits `/docs/sub/a.txt`, the edit is planned, because the walk from `/docs/sub` reaches `/docs`, which the merge restores. The rows that the merge replaces MUST be the fork's own: the current revision for a content change, the current binding for a move, both for a delete, the current attribute or access row for an attribute or access change, and the tombstone row for a restore. For a move or a restore, the name MUST be free in the destination directory. A name is free when no inode is bound to it there, or when the merge deletes or moves away the inode that's bound to it. An item below a deleted directory is still bound to its name, so in a directory that the merge restores, the item's name is taken unless the merge deletes or moves that item.
2. **An inode that the fork created.** The planner plans a create under the fork's inode ID. The new inode's parent MUST pass the ancestor check of the first case, and the name MUST be free in the parent, as the first case defines a free name. The merge's commit creates or restores a parent before the items below it. If an earlier merge of the fork already created the inode here, the first case applies instead.
3. **A directory that the fork deleted or moved.** This namespace may have changed something beneath the directory since the fork point, so the planner reads this namespace's diff from the fork point to the head over the directory, and each row in that diff MUST be the fork's own.

An inode that fails a check conflicts, and the planner refuses a merge with any conflicting inode, without submitting anything. For example, a fork made at sequence 100 changes `/a.txt`. If this namespace's current revision of `/a.txt` was committed at sequence 150 by an ordinary commit, that revision is foreign, and the planner refuses the merge. If the commit at 150 was an earlier merge of the same fork, the revision is the fork's own, and the merge replaces it.

To apply the rule, the planner reads:

- the fork's manifest, for the fork point and the namespace that the fork was made from,
- the fork's rows in the merge's range and its state at `source_after_seq`, for the net change,
- this namespace's current rows for the inodes in the net change and their ancestors,
- this namespace's commit rows at the sequences that those rows record, and
- this namespace's diff from the fork point to the head over each directory that the fork deleted or moved.

The writer doesn't apply the conflict rule again. For a merge with no conflict, the planner submits this namespace's current values as preconditions, which the writer evaluates at the head as for every commit, so a change committed between planning and publication fails the merge. When the net change deletes or moves a directory, the planner MUST also submit a `namespace_head` precondition whose `expected_head_seq` is the head it applied the rule at, so that an item created beneath the directory after planning (which changes no row that the other preconditions cover) also fails the merge. The writer also checks the range and floor rules, and a writer MUST refuse a merge that creates an inode whose ID this namespace already has.

### 6.3 Publication and group commit

To publish a batch, a writer:

1. Resolves retained commit IDs, and then validates new requests against the discovered view and the requests accepted earlier in the batch.
2. Keeps new commits in the writer's queue while the unfolded WAL is at a write-stop threshold, in bytes or in objects (fences and floor objects count toward the object threshold), until a fold frees room. A commit that's still waiting when the [write-stop wait](#73-recovery-material-and-maintenance-policy) passes (measured from when it entered the queue) returns `maintenance_required` and isn't committed.
3. Assigns contiguous sequences to the accepted requests, and builds one WAL object at `tip + 1`.
4. Checks the content-admission evidence immediately before the put-if-absent. If a request's evidence has expired, the writer returns that request's own error, and `stale_head` for the other accepted requests so they can be planned again. It writes no WAL object for that batch.
5. On success, moves its [writer position](glossary.md#writer-position) to the new head and acknowledges the requests.

Writing several requests in one WAL object is called "group commit". For example, three requests accepted after sequence 40 can be written together as sequences 41, 42, and 43 in WAL object 10, and successfully writing that object commits all three. They're still three separate commits, and a request with several operations is still one commit.

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

A confirmed precondition failure creates no object. The writer discovers the winning publication, checks whether it has been fenced, and plans again before it tries another number, so it never creates a parallel branch of history. The object at the next number can be another writer's commit or fence, or a [floor object](glossary.md#floor-object), which any process can create (see [Advancing the retention floor](#101-advancing-the-retention-floor)). After a floor object, discovery finds no higher writer epoch, so the writer isn't fenced. It plans the batch again against the new floor, and its callers see no error.

The writer puts the WAL object however long ago it observed the tip. No process deletes a fence or a floor object (see [Candidate and age rules](#113-candidate-and-age-rules)), so a put at `tip + 1` succeeds only if no object was ever created at that number, and a successful put commits its batch without a check of the elapsed time. For example, a session with tip 30 that has been idle for a day puts its next batch at 31. If another process created a floor object at 31 that day, the put collides, and the session runs discovery and commits at 32. The session's successor probe still checks the position between commits (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), but the put doesn't wait for it.

After a WAL put whose outcome is unknown, or a publication cancelled after its put might have been sent, the writer drops its position, which might be missing the writer's own commit. The next publication's discovery finds that commit, and a retry with the same commit ID returns the original result from the commit's receipt (see [Commit identity and retries](#65-commit-identity-and-retries)).

Why does the session check its position for a manifest successor? Another writer's claim, and a fold or a compaction by another process, each publish a successor, and the probe finds each of those manifests. The boundary read after the probe finds a successor that garbage collection already deleted (see [Replaying the visible WAL](#42-replaying-the-visible-wal)).

A manifest publication that builds on a manifest it already has sends no check before its put (see [Publishing a manifest](#72-publishing-a-manifest)). If garbage collection had deleted that manifest's successor, the put would succeed at the emptied number, but the boundary read after the put would find the boundary at or above that number, so the put would count as lost.

Why can't a put at `tip + 1` reuse a number that a collection emptied? When an acquired session observed tip `t` (by discovery, or by creating WAL object `t`), no object existed at `t + 1`. A collection keeps fences and floor objects, so an empty `t + 1` means that no object was ever created there, or that a data object was created there after the observation and later deleted. A writer learns that the tip passed a number in one of two ways: it created the object at that number, or it ran discovery, which replays every WAL object above the manifest's `folded_wal_no`. None of these writers could have created that data object:

- **The session itself.** It publishes one WAL object at a time, and after a put whose outcome is unknown, it drops its position and runs discovery.
- **A writer with an older epoch.** It would have to pass the session's fence, which is at or below `t`. It can't create the fence's number itself, so it would have run a discovery that replays the fence or loads a manifest with the newer epoch, and it would be fenced.
- **A writer with a newer epoch.** It commits only above its own live fence (see [Writer ownership](#61-writer-ownership)). A fence at `t` or below would have fenced the session at discovery or collided with its put, so the writer's first object at `t + 1` or above is its fence, not a commit.

So only a fence or a floor object can take `t + 1` after the observation, and the put collides with either one, however long it takes. A fold covers only WAL objects that exist, so a number where no object was ever created is above every manifest's `folded_wal_no`, and every reader replays a batch committed there. The argument doesn't cover a fence put, or a floor object put by a process without the writer epoch, because the previous writer can commit at those numbers, so the process checks the put's number afterward (see [Writer ownership](#61-writer-ownership) and [Advancing the retention floor](#101-advancing-the-retention-floor)). It does cover a floor object that a writer session puts from its own publication queue, at its position's tip plus one, like a commit.

### 6.4 Failed and unknown outcomes

A transport error after the WAL object's put was sent isn't necessarily a failed commit, because the put might have succeeded and only its response was lost. An implementation that can't establish the outcome MUST report it as unknown, not as a definite failure.

An implementation can resolve an unknown outcome itself by retrying with the same commit ID and request. The retry discovers the tip again and looks up the commit receipt, so it returns the original result if the earlier put succeeded. Only a success or a receipt replay establishes the outcome, so a retry that ends in any other error still reports `outcome_unknown`. The reference publisher retries a bounded number of times before it reports.

A caller that receives an unknown outcome retries with the same commit ID and the same logical request.

Warning: a caller that retries a commit shouldn't retry the upload too. If the upload is retried, it's assigned a different content ID, so the retried commit is no longer the same request. If the original commit succeeded, the retry would fail with `commit_id_reuse_conflict` instead of returning the original result.

A publication batch isn't an all-or-nothing transaction, so some requests can fail validation while others are accepted. A validation error that depends on another tentative request can be reported that way only if that other request is published. For example, suppose request A creates `/reports`, and request B, which also tries to create `/reports`, fails because of A's tentative creation. If A's publication then fails, the writer MUST report the publication failure for B too, and not claim that B conflicted with a committed directory. A rejection based entirely on the view that was already durable doesn't have this dependency, and it can stand on its own.

### 6.5 Commit identity and retries

Every WAL commit and every `commit` row stores a semantic fingerprint (`semantic_commit_fingerprint`) of the logical request: the `namespace_id`, the `actor_id`, the ordered operations with their inline preconditions, the request-level preconditions, and the optional message. It doesn't include publication details, such as the writer epoch and the timestamp. [Semantic commit fingerprints](#appendix-b-semantic-commit-fingerprints) specifies the exact canonical bytes.

A [merge](glossary.md#merge) into another namespace of the fork's [pool](glossary.md#pool) (the namespace that the fork was forked from, or a new fork of that namespace that a [rebase](glossary.md#rebase) created) includes a merge record that identifies the fork and the range of the fork's commits that it applied (see [WAL records](#a5-wal-records)). LoonFS plans a merge's operations itself, so a merge's fingerprint covers its merge record in place of its operations and preconditions, and a change in how LoonFS plans merges doesn't change a merge's identity. A retry with the same commit ID and merge record (and the same actor and message) is a replay, even if LoonFS planned different operations for it, and a request that reuses the commit ID for a different range returns `commit_id_reuse_conflict`. [Merge commits](#b5-merge-commits) specifies the canonical bytes.

The manifest's runs also store each retained commit's record in the `commits` family, keyed by sequence. A commit receipt stores only `commit_id` and `committed_seq`. When a request reuses a commit ID, the writer reads the commit row at the receipt's sequence and compares that row's fingerprint with the request's. A replay rebuilds its response from that row. The `commits`, `commit_receipts`, and `merges` families are one family group with one retention rule, so a commit row is kept for as long as its receipt, and a merge's index row for as long as its commit row.

While the receipt is kept:

- An equal fingerprint under the same `commit_id` is a replay of the original commit. A replay doesn't run the mutation again, and it doesn't evaluate the original preconditions again against the current state.
- A different fingerprint returns `commit_id_reuse_conflict`.

Inline content is identified by its bytes. While the receipt is kept, retrying the same request with the same inline bytes returns the original commit, even if a new content ID was assigned. Changed bytes, or a different actor, return `commit_id_reuse_conflict`. The bytes an append adds are identified the same way.

Receipt lookup uses the publisher's current projection. The publisher refreshes that projection after its put collides, or when its basis manifest has a successor (see [Publication and group commit](#63-publication-and-group-commit)). A fold that the publisher published itself moves the projection's basis to the new manifest and keeps the commits published since, without a refresh.

The writer checks for the commit receipt before it uploads inline bytes as content objects. If the receipt is still available, the writer returns the original result for an identical request, or a reuse conflict for a changed one, without another upload, even after a restart or on another server.

The guarantee lasts as long as the receipt. Compaction can remove receipts below the retention floor, and once a receipt is gone, the old ID can't be distinguished from an unused one, so a later request can run as a new mutation. A receipt that hasn't been compacted yet might still be available, but callers MUST NOT depend on that extra lifetime.

Receipt lookup, with both of the results above, still works at the WAL write-stop threshold while new commits wait for a fold (see [Publication and group commit](#63-publication-and-group-commit)). The writer-session, availability, and corruption checks still apply.

File revision history and commit idempotency have different retention rules. Keeping an old file revision doesn't require keeping its commit receipt.

### 6.6 Operations and WAL deltas

Standard requests operate on paths or inode IDs. They create directories, write files, append to files, move or copy items, delete items, restore deleted items and file revisions, and update attributes. [Appendix B](#appendix-b-semantic-commit-fingerprints) lists their exact parameters, because those parameters also determine retry identity.

| Setting | Default |
| --- | --- |
| Destination behavior for puts, moves, and copies | `no_replace` |
| Deletes | `non_recursive` |
| Directory creation | `parents: false` |
| Attribute `set` and `remove` collections | Empty |

Optional preconditions have no implied value.

A replacing move deletes the destination file and binds the source in its place, within the same commit. Only a file destination can be replaced, and moving a path onto itself isn't a replacement. A restore can use the deleted binding's original parent and name, the caller's replacement path, or the caller's parent inode and name, subject to normal validation.

An append adds bytes as the file's next revision, and its commit includes the new bytes as pieces. The writer computes the new checksum by combining the base reference's checksum with the appended bytes' checksum, in the store's algorithm, without reading the earlier bytes. A base in another algorithm is `namespace_corrupt`, before anything is written.

An append either extends the same chain or starts a new one:

- **Same chain.** The appended piece keeps the same content ID only when the namespace is the chain's creator (see [File contents and ownership](#15-file-contents-and-ownership)), the base size is positive, and the base is the chain's head (its full current length). The visible tail and the longest layout determine that length, so a copy or restore of an older prefix can't shorten the chain's head. The namespace is the creator of a chain that started in its own unfolded tail, and otherwise the chain's layout in its view records the creator. A same-chain append includes no layout and writes no content object.
- **New chain.** Otherwise, the piece uses a new content ID. Its delta includes the base chain's layout, cut at the first piece's offset in the commit (the cut shortens the last extent and drops later ones), with the committing namespace as the layout's creator. Before it admits a new chain from unfolded bytes, validation writes the base chain's pending pieces to a new span object as its next extent, as a fold does, and the base chain's later fold writes them again (see [Publishing a manifest](#72-publishing-a-manifest)). The new chain shares those extents and includes only the appended bytes as pieces. Missing base bytes are corruption, and the error identifies the base. An append to an empty file starts a new chain with a piece at offset 0 and no layout.

For example, a fork inherits `/log.txt`, whose chain the source namespace created. The fork isn't the chain's creator, so appending 10 bytes starts a new chain, whose layout lists the first chain's objects followed by a piece with the 10 new bytes. Later appends in the same commit are planned against the effects of earlier operations.

Whether an append extends its chain or starts a new one, its `append_file_revision` delta records the offset where the appended bytes begin (`append_offset`), which is the base revision's `size_bytes`, and the change feed reports that offset in the append's event. The delta of a put, a copy, a restore, or a file's first revision doesn't include the field.

The WAL stores the resulting metadata changes ("deltas"), not the original request bodies or validation inputs. There are eight kinds of delta:

| Delta | Records |
| --- | --- |
| `create_inode` | A new inode. |
| `bind_direntry` | A new directory binding, with the file's current revision summary when the child is a file. |
| `unbind_direntry` | A removed directory binding. |
| `append_file_revision` | A new file revision, with the file's current binding, under which replay writes the revision's summary row. |
| `tombstone_subtree` | A deletion (a tombstone `set` event), with the deleted root's kind, which the deletion's `listed` row in the `trash` family stores. |
| `revoke_subtree_tombstone` | A restore (a tombstone `revoke` event). |
| `append_attributes_revision` | A new attribute revision. |
| `append_access_revision` | A new access revision. |

Each delta has a `delta_index`, and its wrapper has a `semantic_operation_index` that identifies the internal operation that produced it. A convenience request can expand into several internal operations (for example, creating missing parent directories), and each operation's deltas are contiguous within its commit. The actor and timestamp are recorded once per commit and copied into the rows when a fold writes them. [Appendix A](#appendix-a-durable-records-and-byte-encodings) lists the complete stored fields.

Why does an append's delta record its offset, when the append's piece already starts at that offset? A fold writes each commit to a `commit` row without its pieces, and the change feed reads folded commits from those rows. A piece at offset 0 also doesn't tell an append to an empty file from a put of inline content. With the offset in the delta, the feed reports the same event before and after a fold.

Why does only a chain's creator extend the chain in place? A reader takes the longest visible layout of a chain (see [Content verification](#45-content-verification)), and the doubling rule combines a creator's own extents, so a content ID has to identify one sequence of bytes, and every layout of a chain has to be a prefix of its longest layout. If two members appended to one chain from the same length, a merge could bring one member's layout row into the other member, and a reader there could read the other member's bytes. A new chain costs a content ID and a layout row, but no copy of bytes, because it shares the first chain's extents.

### 6.7 Change feed

The change feed is ordered by commit, not by WAL object, so a WAL object that contains three commits contributes three commit boundaries to the feed. Within a commit, filesystem events follow the order of the request's operations, and one operation can produce several events.

A consumer resuming after sequence `N` reads the `commits` family of the current manifest's runs, and then the commits in the unfolded WAL after it. Fences and [floor objects](glossary.md#floor-object) produce no change events. The feed reports a [merge's](glossary.md#merge) merge record with its commit, from the WAL tail and the `commits` row alike, so a consumer can tell which fork, and which of its commits, the merge applied. A [rebase's](glossary.md#rebase) replay commit leaves out the changes that conflict, so its merge record identifies the range of the fork's commits that it examined, and the rebase's conflict report identifies the changes that it left out. The feed's lower bound is the [retention floor](glossary.md#retention-floor) of the view that the request reads (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). If the consumer's cursor is older than that floor, it MUST start again from a fresh snapshot. The [API specification][api-spec] specifies the event shapes and the cursor contract.

A consumer that needs permanent event history MUST keep its own copy before the floor advances. The stable keys are `(namespace_id, committed_seq)` for a commit's position and `(namespace_id, inode_id)` for item identity. A commit ID is useful for correlating events, but it isn't a permanent unique event key, because it can be reused after its receipt is removed.

## 7. Folding the WAL

This section describes how a fold moves committed metadata from the WAL into sorted segments and publishes a new manifest.

A longer WAL takes more reads to replay. A "fold" writes committed metadata into immutable sorted segments and publishes a manifest that lists them, so later reads replay only the WAL objects written after that manifest. A fold changes how metadata is stored, not the namespace's visible history. It allocates no sequence number and creates no snapshot.

### 7.1 Manifests, runs, and segments

A namespace manifest describes the namespace's complete metadata through its "folded head" sequence (`head_seq`), the newest commit in its runs. It includes the folded WAL number, the next run number, the retention floor, and every metadata run needed to rebuild that state. Its `manifest_no` determines its immutable key, so only one publication can succeed at each number.

A "run" is the set of segments produced together. Its `run_no` is allocated from the manifest's `next_run_no`, which advances when the run is published. A fold allocates one run number for all the families it writes, and a compaction one for its selected family group.

Each run records `run_seq`, `tier`, and its segment descriptors. The manifest's `base_seq` is derived as the smallest `run_seq` among runs with segments, or `head_seq` when none has segments. A non-empty manifest MUST have a non-empty run at `head_seq`, and no run's sequence can be higher than `head_seq`. An empty manifest describes a new, empty namespace. Every segment contains at least one row. Runs can overlap, because a later run can add rows for the same inode, name slot, or revision history.

A manifest MUST NOT contain two metadata runs with the same number, or a metadata run number at or above `next_run_no`. This rule doesn't cover an [extension's](#124-extensions) runs, which have numbers of their own. Within one run, each family's segments follow list order, and their key ranges MUST NOT overlap or descend. A metadata producer MUST NOT write the same logical row key twice within one run.

Metadata rows record immutable facts at specific positions, and reads combine them under the visibility rules instead of choosing between conflicting values for one row key. The two directory binding indexes contain the same binding events, and the slot index also contains summary rows (see [Directory bindings](#13-directory-bindings)). Manifest validation checks that each run's slot index has at least as many rows as its child index, and compaction checks that the binding events of its selected input runs agree row by row. The `commits` and `commit_receipts` families each contain one row per retained commit, and manifest validation checks that each run has as many rows in one as in the other.

### 7.2 Publishing a manifest

The first manifest of a namespace has these values:

| Field | Plain create | Fork |
| --- | --- | --- |
| Manifest number | 1 | 1 |
| Namespace ID, creation time, `created_by` | From the request | From the request |
| Retention window | From the request if the request includes `retention_window_ms`, and otherwise the host's default window (if the host has a default window) | From the source manifest that the fork is built from, if that manifest sets `retention_window_ms` |
| Status | Initializing (manifest 2 is the same manifest with the status `active`) | Initializing (the fork's manifest 2 folds the source's WAL tail when the tail has commits, and the active manifest follows the newest initializing manifest, as described below the table) |
| Writer block | None | None |
| Writer epoch, compactor epoch, folded WAL number, activity counters | 0 | 0 |
| Fork basis | None | The source manifest that the fork is built from, the fork point, the ID of the fork snapshot on the source, the ID of the snapshot that the fork is built from, and the fork point snapshot's ID |
| Pool | A pool ID that the create call generates | From the source manifest that the fork is built from |
| Runs | None | The source's runs |
| Folded head sequence and retention floor | 0 | The folded head sequence and the retention floor of the source manifest that the fork is built from |
| Base sequence | 0 | Derived from the inherited runs |
| Next run number | 0 | The source's |
| Extensions | None | The entries of the source manifest that the fork is built from |

[Forking a namespace](#92-forking-a-namespace) describes a fork's manifest 2, which folds the source's WAL tail, and its active manifest.

A fold works in these steps:

1. Start from a verified manifest and a discovered WAL tip. (A fold call discovers both, and a writer's own fold takes them from its retained view.)
2. Check every chain's pieces in the tail (see below).
3. Write each chain's new extent to a content object under the pool's prefix (see below).
4. Write new metadata segments for the WAL objects after `folded_wal_no`, including the new layout rows. Each segment's descriptor records the segment's stored checksum. Fences and [floor objects](glossary.md#floor-object) are folded even when the sequence doesn't change. An ordinary fold writes a run at the folded head when it folds new state, and a tail of fences and floor objects with no commits is folded into a manifest with no new run.
5. Publish the next manifest with put-if-absent at the predecessor's `manifest_no + 1`, with `folded_wal_no` set to the captured tip and `retention_floor_seq` set to the higher of the predecessor's floor and the floors of the floor objects that the fold covers.

For example, if the predecessor's floor is 5 and the fold covers a floor object with floor 20, the fold's manifest has floor 20. Apart from a fork's active manifest, a fold is the only publication that raises a manifest's floor (see [Advancing the retention floor](#101-advancing-the-retention-floor)).

A fold is a publication by the namespace's [maintainer](glossary.md#maintainer), not its writer. It publishes under the current compactor epoch without claiming it and copies the predecessor's writer and compactor epochs forward, so it needs no writer epoch, and any process can run one. One `METADATA_PUBLICATION_BUDGET_MS` deadline covers the whole call, including rebuilds.

Every manifest publication builds on a manifest it has, its "basis": a writer session's own fold, compaction, or deletion on the manifest of the session's [writer position](glossary.md#writer-position), a fold call on the manifest that its discovery found, a compaction on the manifest it planned from, and a claim after a create or a fork in the same runtime on the active manifest that the create or fork published. The publication puts its manifest at the basis's number plus one with put-if-absent, however old the basis is, and sends no request before the put when it has the basis's manifest in memory. A basis manifest read from the store is first checked against the manifest boundary (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). A publication without a basis first finds the newest manifest (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)).

If the put loses because the number is taken, the publication probes above its basis. It sends a HEAD request for the next number, and if that manifest exists, it probes the `MANIFEST_PROBE_WINDOW` (8) numbers after it at once, takes the highest that exists, and repeats from it while the window's last number exists. It then reads the [manifest boundary](glossary.md#manifest-boundary). If the manifest that the probe found (or the basis, when the probe found nothing) is at or below the boundary, the publication finds the newest manifest from the start (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)). Otherwise its next attempt builds on the manifest that the probe found. For example, a session's fold on manifest 40, last checked two hours ago, puts manifest 41 with no request first. Other processes have published 41 and 42, so the put loses, and the fold finds 42 above the boundary and builds on it.

After a put that succeeds, the publisher reads the manifest boundary. When the basis keeps a boundary, this is a conditional read with the kept version, so an unchanged boundary returns no body (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), and a publication of manifest 1, which has no basis, sends a plain read. This read follows every put that the publisher counts as its own, including a put with an unknown outcome that a read-back confirmed (see [Conflicting and unknown installations](#93-conflicting-and-unknown-installations)):

1. If the manifest's number is above the boundary, or no boundary object exists, the publication succeeded.
2. If the number is at or below the boundary, the put counts as lost, like a put that found the number taken. The basis is at or below the boundary too, so the publisher doesn't probe above it. It finds the newest manifest (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)) and continues as after any lost put. A retired boundary is above every manifest number, so a put that a provider applies after the namespace's [retirement](#95-retirement) counts as lost, the find returns a retired namespace, and the publication returns `namespace_deleted`.
3. If the boundary read fails, the publication returns `outcome_unknown`, and the caller reloads as after a put whose response was lost.

The boundary read is the publication's last request. For example, if others have published manifests 41 to 45, and a collection has deleted 41 to 44 and raised the boundary to 44, a publication on manifest 40 puts 41 successfully at the emptied number. The boundary read returns 44, so the put counts as lost, and the next attempt builds on 45.

After a lost put, a fold finds the current manifest again and is finished if that manifest covers its work, which means that its `folded_wal_no` is at least the fold's captured tip, and its folded head sequence and manifest number are at least the fold's own. Otherwise, the fold rebuilds against the new predecessor, with that predecessor's epochs. The rebuild can keep the WAL tail it already read when the new predecessor differs from the fold's own only in its number, compactor epoch, retention floor, retention window, runs, next run number, and extension entries, and has runs exactly when the fold's own predecessor does. A compactor claim, a compaction, a retention window change, and an [extension's](#124-extensions) publication publish that kind of manifest. After any other manifest, the rebuild reads the tail again above the new `folded_wal_no`. (A compaction that loses its put also requires its selected inputs to still be valid.)

A fold that rebuilds after another writer's claim folds the new writer's log. The old session's [writer position](glossary.md#writer-position) doesn't move onto that fold's manifest (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), so the old session finds that it's fenced at its next commit.

Before it writes segments or publishes the manifest, a fold checks each chain's ordered tail pieces up to the chain's newest reference, and missing pieces are corruption. It checks every whole value that starts at offset 0 before it writes any object.

For each chain, the fold resolves the layout before the pieces, and builds a "candidate", the new extent it will write:

1. The candidate starts as the ordered pieces, without joining their bytes. Its chain offset is the total length of the extents before it.
2. If the fold runs in a namespace that isn't the chain's creator, the candidate is the pieces alone, and steps 3 to 5 don't apply. The fold writes the pieces as one new extent and never combines them with the chain's existing extents, just as it never combines a shared base extent.
3. Validation might already have written some tail bytes to content objects. The fold keeps such an own extent whole and leaves out the pieces it covers. If it covers all the remaining pieces, the last own extent starts the candidate.
4. While the chain's last own extent is at most twice the candidate's length, the fold combines that extent into the candidate (the "doubling rule"). Shared base extents are never combined.
5. If the result would leave more than `MAX_LAYOUT_EXTENTS` (32) own extents, the fold also combines every own extent after the chain's largest own extent into the candidate. Shared base extents stay separate and don't count toward this limit.

For example, a chain has one own extent of 100 bytes, and the tail adds 30 bytes. 100 bytes is more than twice 30, so the fold writes the 30 bytes as a new extent, and the layout has two extents. If the next fold has 70 new bytes, the 30-byte extent is at most twice 70, so it's combined, and the candidate grows to 100 bytes. The 100-byte extent is now at most twice the candidate, so it's combined too, and the fold writes one 200-byte extent.

The fold writes each candidate with one assembly call. Each source is a whole own extent (own extents are never cut), and one HEAD request per source supplies its stored checksum. A stored checksum that's missing, in another algorithm, or different from the extent's object checksum is `namespace_corrupt`. The fold combines the source checksums and the tail checksum in the store's algorithm, and the tail bytes stay in ordered pieces. A candidate with no combined extents has no sources, at every size. A new chain shares the extents its delta included and writes only its own pieces.

Every write of a chain's pieces, including a span that starts at 0, stores a new [span object](#15-file-contents-and-ownership) under an ID that the write generates. The new extent records that ID, the candidate's start, the newest reference's size, and the stored checksum that the assembly returns, which combines the checksums of the sources and the tail. Only an upload writes a chain's bytes to the whole object at the chain's own key, except for a chain of zero bytes, whose layout is one whole extent of length zero, so the fold writes an empty whole object. Every chain extent is written with an assembly, with put-if-absent, and an occupied key with a different or missing stored checksum is corruption, which the fold reports with the key (see [Immutable writes and retries](#32-immutable-writes-and-retries)).

Two writes of the same pieces, such as a fork's fold of its source's tail and the source's own fold, store two span objects with the same bytes, and each layout lists its own write's object. A fold that fails before its manifest is published, or that loses its put and rebuilds over a manifest at a different head, leaves span objects that no layout row lists, and garbage collection deletes them under the age rules, as it deletes a lost fold's segments (see [Candidate and age rules](#113-candidate-and-age-rules)). A rebuild over a manifest at the same head keeps its first attempt's layouts and writes no content object again.

The resulting layout is the remaining extents followed by the new extent, and its `committed_seq` is the newest commit whose bytes it covers. The layout records the chain's [creator](#15-file-contents-and-ownership): the creator that the chain's existing layout records, or, for a chain that started in the WAL tail, the namespace recorded in the WAL object that started the chain (`namespace_id`). A namespace's WAL includes pieces only for chains that it created (see [WAL records](#a5-wal-records)), so a layout records another creator only when a namespace writes pieces that are still in another member's WAL, so that the member doesn't have to fold first. The fold's layout row replaces any tail layout row for the same content ID and goes into the fold's delta run. A fold copies revision digests from the deltas and never recomputes them. A manifest at `folded_wal_no` `n` includes layouts for all pieces through WAL object `n`, and garbage collection relies on that before it deletes a WAL object.

Folds, validation that writes pending pieces to content objects, a merge's writes of a fork's pending pieces, direct-download writes, and proxied staging share the execution budget's content-write permits, `max_concurrent_content_writes`, which defaults to 32 and is configurable. Each write uses one permit until it finishes, and runtimes that share a budget share the limit. `STORE_WRITES_IN_FLIGHT` also limits how many writes one fold or merge starts at a time. The adapter chooses how to write by the result's size (see [Immutable writes and retries](#32-immutable-writes-and-retries)), and an assembly buffers at most one part window (or one read chunk on the local store).

Every successor manifest MUST:

- keep the namespace's identity, including its pool,
- not lower the folded head sequence, the writer or compactor epoch, the folded WAL number, the retention floor, the next run number, or the cumulative activity counters, and
- keep the activity counters exactly the same, if it's at the same folded head.

A successor can set, change, or clear the [retention window](glossary.md#retention-window), because the window is a maintenance setting, not part of the namespace's identity. A namespace tombstone has no successor.

The bounded metadata publication budget runs from the start of the publication, before any output segment is written, until the manifest put starts. An attempt whose budget expired publishes nothing more, and its unreferenced output is deleted under the segment-age rules in [Candidate and age rules](#113-candidate-and-age-rules). Compaction has a longer bound (see [Streaming compaction](#104-streaming-compaction)). The budget is checked before the put and not after it, because the boundary read after the put shows whether garbage collection emptied the number.

Forks follow the same path from manifest 1. Inherited segments aren't copied into the fork's prefix, and each segment keeps its owner.

Why read the boundary after a put that succeeded? A put-if-absent succeeds at a number that garbage collection emptied. Garbage collection raises the boundary before it deletes a manifest, so the read finds every such put, however long the publisher waited before or after it.

Why check the budget before the put? A fold or a compaction writes segments before it publishes, and garbage collection deletes a segment that no root lists once it's older than the grace window. The budget makes sure that the manifest that lists the segments is put before they're old enough to delete.

Why does the doubling rule exist? Without it, each fold of a growing chain would either add an extent, leaving a much-appended log file with thousands of objects to read, or copy the whole chain on every append. With the rule, each extent is at least twice as large as the next, so a chain has few extents, and each byte is copied about once per doubling of the chain's size. The 32-extent limit covers many small appends that would build up more extents than that.

Why doesn't a fold combine the extents of a chain that another member of its pool created? The pool would then store the creator's earlier bytes twice, the second time in an object that only the folding namespace's layouts list.

Why can two members write pieces of the same chain without a conflict? Each write puts its own span object key, and only the creator extends a chain, so every object that stores a range of the chain stores the same bytes. The cost is a second copy of at most one WAL tail's pieces for each fork, merge, validation, or direct download that writes them, and each copy stays for as long as a layout lists it.

### 7.3 Recovery material and maintenance policy

Once the WAL objects they came from are deleted, metadata segments are required recovery material, not caches. A missing or corrupt required manifest or segment is an error, and readers don't silently choose other runs and return another state.

Implementations can fold automatically as the WAL grows. The reference implementation uses these defaults, which aren't format requirements:

| Trigger | Reference default |
| --- | --- |
| Request a fold | 8 MiB of unfolded WAL bytes, or 256 unfolded WAL objects |
| Make new commits wait for a fold | 64 MiB of unfolded WAL bytes, or 1,024 unfolded WAL objects |
| Refuse a commit still waiting for a fold with `maintenance_required` (the write-stop wait) | 30 s after the commit entered the queue |
| Fold an idle tail | The tail contains a commit, and the newest commit's `committed_at_ms` is at least 15 minutes older than the maintenance clock |

Unfolded WAL objects include fences and [floor objects](glossary.md#floor-object), so both count toward the object thresholds. Unfolded WAL bytes are the stored sizes, inline pieces included, of the WAL objects with commits above the folded boundary, which is what a reader downloads to replay the tail. The byte thresholds bound what a replay downloads and what a writer keeps in memory, and the object thresholds bound the GETs that a replay sends. One commit can pass the byte threshold, because the write stop applies only to commits after the tail reaches it.

The commit that triggers a fold can finish before the fold does. Reads and lookups of retained receipts still work at the write-stop threshold. Idleness is measured from the newest commit's timestamp, so a tail of only fences and floor objects is never idle. The timestamp decides when to fold, but a fold is valid at any time.

Folding doesn't advance retention on its own. A fold publishes only the floors that its tail's floor objects record, and an operator or a scheduled advance decides separately when older replay history can be discarded (see [Advancing the retention floor](#101-advancing-the-retention-floor)). [Appendix C](#appendix-c-timing-and-size-reference) lists the reference implementation's sizing defaults.

### 7.4 Statistics

Each manifest stores three cumulative activity counters in a required `activity` object:

| Counter | What counts |
| --- | --- |
| `content_bytes` | The full content length of every committed file revision, including overwrites and revisions that reuse stored content. A revision whose new bytes are pieces in its commit counts only those bytes, once, so an append counts the bytes it appends. |
| `file_revisions` | Every committed file revision, including an empty one. |
| `mutations` | Each semantic operation group in a committed WAL record, identified by `semantic_operation_index`. |

For example, writing a 10-byte file and then replacing it with a 6-byte revision adds 16 bytes, two revisions, and two mutations. Creating a directory adds one mutation, and recursively deleting it adds one, however many descendants it hides. A convenience request can contain several internal operations, so the mutation total can exceed the number of requests.

A fold adds only the activity after its predecessor's covered position, and publishes the counters together with its runs, head sequence, and folded WAL number. If another publication wins, the fold reloads the predecessor and counts only the remaining tail. A recognized commit retry adds nothing, but a new durable commit that an expired receipt allows counts as new activity.

Uploads on their own, fences, floor objects, snapshots, writing inline content to content objects, and compaction add no activity, and retention and deletion don't subtract any. Every publisher copies the counters forward from the predecessor it actually updates, including a compaction that races a newer fold.

Two footprint values are calculated from the manifest's segment descriptors:

| Value | Calculation |
| --- | --- |
| `inode_record_count` | The sum of `row_count` for `inodes` segments. |
| `metadata_stored_bytes` | The sum of `index_block.offset + index_block.stored_bytes` for every referenced segment. |

The inode count includes retained deleted records, and an implicit root counts as zero until it's stored as an inode record. Metadata bytes exclude content, WAL objects, manifests, snapshots, unreferenced outputs, and segments that only other manifests reference. A shared segment counts in every manifest that references it, so these totals don't measure unique physical storage.

A statistics read uses one validated manifest, without reading segments or replaying newer WAL objects. An observation includes the namespace's identity, lifecycle status, manifest number, head sequence, and folded WAL number. The manifest number is there because compaction can change the footprint without changing the head. Snapshot statistics read the snapshot's manifest.

An observation also includes `manifest_bytes`, the stored (compressed) length of the manifest object as read, which counts stored bytes as `metadata_stored_bytes` does, and `segments_by_family`, the number of each family's segment descriptors across every run, listing every family and zero for a family that has none. Both come from the same validated manifest, so they add no requests.

A fork inherits the source manifest's segment descriptors, so its footprint values start out equal to the source's, but its counters start at zero. Activity committed in the source stays in the source's manifests. A fork's fold of the source's WAL tail adds no activity, because those commits are the source's, so the fork's counters are still zero when it becomes active.

To meter usage, an application compares two observations of the same namespace and keeps its own billing cursors and policy. The counters are public integers within the bound in the API specification. Exceeding that bound is an error, and a counter that decreases is corruption.

Extension segments aren't metadata segments, so neither footprint value counts them, and `segments_by_family` doesn't list them. An extension records its position, such as grep's indexing position, in its entry (see [Extensions](#124-extensions)).

## 8. Snapshots

A "snapshot" is a durable record that keeps one manifest, and the segments it lists, after newer manifests are published. Otherwise garbage collection can delete a replaced manifest and the segments that only that manifest lists (see [Candidate and age rules](#113-candidate-and-age-rules)). Ordinary folds don't create snapshots. A caller creates a "user snapshot" (owner `user`) when it needs a stable view of the namespace. A caller can delete a user snapshot, and a user snapshot may expire. A fork creates two "fork snapshots" (owner `fork`): one on its source, which keeps the source manifest that the fork reads through, and a "fork point snapshot" on the fork itself, which keeps the fork's manifest at the fork point.

### 8.1 Records and owners

Each snapshot is stored at `snapshots/{snapshot_id}.json`. Its ID includes its manifest's number and a new random suffix, so creating a snapshot twice on the same manifest, or with the same label, creates two records.

| Owner | Stored owner fields | Reads | Removal |
| --- | --- | --- | --- |
| `user` | optional `name`, optional `expires_at_ms`, optional `deleted_at_ms` | Readable before its expiry time if it isn't deleted. A snapshot without an expiry stays readable until it's deleted. | Garbage collection after expiry plus the grace window, or after deletion plus the grace window. Explicit deletion records the time of the deletion in `deleted_at_ms`. Without an expiry, the snapshot stays in an active namespace until it's explicitly deleted. A fork of a source's head takes a user snapshot of the source's current manifest, without a fold and with no name, which expires `OPERATION_SNAPSHOT_TTL_MS` (5 minutes) after the fork call started, as a merge or a rebase takes one of its fork. The fork doesn't delete that snapshot, so the snapshot expires and garbage collection removes it (see [Forking a namespace](#92-forking-a-namespace)). |
| `fork` | `target_namespace_id` | Readable while the record exists. A fork snapshot on the source keeps the runs that the target's manifest lists. A fork point snapshot, whose target is its own namespace, keeps the fork's manifest at the fork point. | The target's retirement (see [Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone)). |

The record also stores the namespace, head sequence, payload checksum, and creation time. The manifest number comes from the snapshot ID. A snapshot has no lifecycle status. Writing the record creates the snapshot, and deleting the record removes it. Fork snapshots have no expiry and no renewal protocol.

Expiry, readability, collectability, and removal are separate. An expired or deleted user snapshot can't be read, but its record appears in the complete snapshot listing and keeps its manifest and segments until garbage collection deletes it. [Snapshot cleanup](#117-snapshot-cleanup) defines when each kind of snapshot can be collected.

### 8.2 Creating and verifying a snapshot

To create a snapshot from the current head, LoonFS:

1. Folds the observed WAL tail as the namespace's [maintainer](glossary.md#maintainer), under the current compactor epoch, and selects a verified current manifest.
2. Writes a new snapshot with put-if-absent.
3. Checks that the namespace is active in the selected manifest. It then checks that manifest for a successor and reads the manifest boundary, as [Replaying the visible WAL](#42-replaying-the-visible-wal) describes for a process with a manifest in memory. If a successor exists, even one with the same sequence, or the manifest is at or below the boundary, another manifest became current, and the snapshot is invalid.

If another manifest became current, LoonFS deletes the candidate snapshot and retries from a fresh basis. A deletion, a store failure, or running out of contention retries prevents acknowledgement. The creator deletes a snapshot whose write or verification fails, as best effort, and if that deletion fails, it still returns the original error and leaves the snapshot to [Snapshot cleanup](#117-snapshot-cleanup). Verification has no time limit of its own.

A put with an unknown outcome can succeed after its creator stopped waiting, even after the creator's delete (see [Required operations](#31-required-operations)). That snapshot was never returned to a caller, but it's listed and collected like any other.

An acknowledged snapshot keeps its manifest's files even if the retention floor later moves past its sequence.

LoonFS can also take a snapshot without a fold, so that an operation can keep the manifest that its discovery found without writing segments or publishing a manifest. It starts from a manifest and a manifest boundary that it already read (such as a discovery of the namespace's head, see [Resolving the metadata basis](#41-resolving-the-metadata-basis)) and performs steps 2 and 3. If step 3 fails, LoonFS deletes the candidate snapshot (best effort), and the caller starts again from a fresh basis. The snapshot keeps the manifest and not the WAL tail, so its head is the manifest's folded head, which can be below the namespace's head. For example, if a discovery finds manifest 46 with folded head 400 and a WAL tail with commits 401 to 418, the snapshot keeps manifest 46, and its head is 400. A fork of the namespace's head takes its temporary snapshot this way (see [Forking a namespace](#92-forking-a-namespace)).

Why write the snapshot before checking the manifest again? A concurrent collection either captured the snapshot's manifest as current, or captured a successor after the snapshot was written, and then its complete snapshot listing includes the snapshot. This is true whether a fold selected the manifest or a discovery found it.

A fork snapshot on the source is protected instead by the user snapshot that its fork is built from: the one the fork request identifies, or the fork's temporary snapshot of the source's head (see [Forking a namespace](#92-forking-a-namespace)). The fork snapshot doesn't require that user snapshot's manifest to still be current. After the fork publishes its manifest 1, and its manifest 2 when it folds the source's WAL tail, the fork:

1. Checks `METADATA_PUBLICATION_BUDGET_MS` (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)), and writes the fork snapshot on the source with put-if-absent, on that user snapshot's manifest.
2. Checks that the source is active, and that the user snapshot exists, isn't deleted, and hasn't expired.

The expiry check uses the request clock plus the monotonic time elapsed since the fork call began. For a fork that folds the source's WAL tail, the temporary snapshot has expired once `OPERATION_SNAPSHOT_TTL_MS` has passed since its `created_at_ms`, whatever expiry its record has. [Forking a namespace](#92-forking-a-namespace) and [Finishing an initializing namespace](#finishing-an-initializing-namespace) define the errors, and why a failed write or check deletes neither fork snapshot. Verification has no time limit of its own. Snapshots are never recreated, so a user snapshot that still exists after the write existed throughout, and every collection that listed snapshots before the write saw it protecting the same manifest.

A fork point snapshot keeps the fork's newest initializing manifest, whose other segments are the source's, which the user snapshot keeps. Its write checks `METADATA_PUBLICATION_BUDGET_MS` first too, and a call past its budget writes nothing. Within the budget, a manifest that the call published can't have been deleted, because a collection deletes an initializing namespace's manifest only once a tombstone replaced it and it's older than the grace window, which is longer than the budget. A call that finishes a fork can find a manifest already older than the grace window, which a delete and a collection can remove before the write. The fork's collection keeps such a snapshot for retirement without reporting corruption (see [Snapshot cleanup](#117-snapshot-cleanup)).

Why doesn't the fork check these conditions before the write? The check after the write decides the outcome, so an earlier check would only spare writing a fork snapshot whose check then fails, which the target's retirement deletes anyway (see [Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone)). What keeps the manifest in place is the budget check. The call found the user snapshot live after it started (in step 2 of [Forking a namespace](#92-forking-a-namespace), or in the read before the write when it finishes another call's fork, see [Finishing an initializing namespace](#finishing-an-initializing-namespace)), and the record is deleted no earlier than the grace window after the snapshot ends, which is at least the publication completion bound, so a write that starts within the budget is written while the record still keeps the manifest. A check of the store can't promise that, because a call can stall before the write.

### 8.3 Reads, deletion, and expiry

A read through a snapshot ID serves a user snapshot or a fork snapshot of the namespace, and a fork or an extension serves user snapshots only. An ID with no snapshot, an ID of a deleted or expired user snapshot, and a fork snapshot's ID in a fork or an extension all return `snapshot_not_found`.

A read through a snapshot takes the manifest number from the ID, confirms that the snapshot exists and, for a user snapshot, that it isn't deleted and hasn't expired by the reader's own clock, verifies its manifest reference, and uses that manifest without replaying later history.

Explicit deletion looks up the snapshot and, for a user snapshot, records the deletion time in `deleted_at_ms` by compare-and-swap, reading the clock after the load as an extension does. The creating process can instead swap against the record version that its write stored, and falls back to the lookup if that swap fails or the write returned no version. The deletion leaves the expiry unchanged. From then on, a read, a fork, or an extension through the snapshot returns `snapshot_not_found` whatever the checking host's clock reads, and garbage collection deletes the record after the grace window (see [Snapshot cleanup](#117-snapshot-cleanup)). Deleting an ID with no snapshot, or an already deleted user snapshot, succeeds without a write, so a retried deletion returns the same result. Fork snapshots can't be deleted explicitly, and their IDs return `snapshot_not_found`. [Reference roots](#112-reference-roots) defines which snapshots protect manifests from garbage collection, and [Retirement](#95-retirement) defines when a deleted namespace can be retired.

Why can a read use a fork snapshot? The fork point snapshot keeps, for as long as the fork exists, a manifest that lists the source's runs and the fork's fold of the source's WAL tail, with the source's retention floor, so a read through it returns the source's state at the fork point even after the source's floor passes it. A [rebase](glossary.md#rebase) reads the source at its new fork's fork point this way, as does the program that resolves its conflicts (for the version each conflicting file started from) and grep (for source commits that a fork's inherited index doesn't cover, see [Index lifecycle](#d2-index-lifecycle)). A read through the fork snapshot on the source returns the source manifest that the fork was built from. A fork snapshot can't be forked or extended, because only a user snapshot has a lifetime that a caller controls.

### 8.4 Extending a snapshot

An extension moves the expiry time of an unexpired, undeleted user snapshot later by compare-and-swap. The manifest reference, the identity, and the owner don't change. An extension never moves the expiry earlier, so extending a user snapshot without an expiry leaves it unchanged, and it can't make a deleted or expired snapshot readable again.

Each extension attempt checks the expiry after it loads the snapshot, using the request clock plus the monotonic time elapsed across the whole extension call. If the snapshot expires while a compare-and-swap that already started is in progress, a successful extension still stands.

## 9. Namespace lifecycle and forks

This section describes how namespaces are created, forked, deleted, and retired. Creating and deleting a namespace both publish numbered manifests.

### 9.1 Creating a namespace

A namespace created without a source starts a new pool, and the namespace needs a [member entry](glossary.md#member-entry) under that pool's prefix before it can be used. The entry is an object outside the namespace's prefix, so the create call publishes manifest 1 with the status `initializing`, writes the member entry, and makes the namespace active with manifest 2. To create a namespace without a source, LoonFS:

1. Finds the newest manifest for the new ID (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)). An active namespace returns `namespace_exists` (or its current summary, with `allow_existing`), a tombstone or a retired namespace returns `namespace_deleted`, and an initializing namespace is finished as described in [Finishing an initializing namespace](#finishing-an-initializing-namespace). Corruption and read errors aren't absence. If no namespace has the ID, the call continues.
2. Generates a pool ID, and publishes manifest 1 with put-if-absent, with the status `initializing` and that pool ID (see [Publishing a manifest](#72-publishing-a-manifest)). As after every successful manifest put, LoonFS then reads the [manifest boundary](glossary.md#manifest-boundary). If the put is refused, or succeeds at or below the boundary, the put is lost, and the call starts again at step 1.
3. Writes the member entry (`pools/{pool_id}/members/{namespace_id}.json`) with put-if-absent (see [Control and manifest payloads](#a4-control-and-manifest-payloads)).
4. Publishes manifest 2, the same manifest with the status `active`. A call that loses this put finds the newest manifest again, as [Finishing an initializing namespace](#finishing-an-initializing-namespace) describes.

For example, a create of `main` publishes manifest 1 with the status `initializing` and `pool_3s0drmj8em84yb4mnetqkz2j0x`, writes `pools/pool_3s0drmj8em84yb4mnetqkz2j0x/members/main.json`, and publishes manifest 2 with the status `active`. The create writes three objects.

Manifest 1's put and the boundary read after it decide whether the call created the namespace, because a put-if-absent at a number that garbage collection emptied succeeds (see [Publishing a manifest](#72-publishing-a-manifest)). If two processes create the same ID at once, both can find no namespace in step 1, and the one whose manifest 1 put succeeds above the boundary creates the namespace. The other starts again at step 1, finishes the namespace if it's initializing and its manifest 1 matches the request, and otherwise returns `namespace_exists`.

For example, `main` has manifests 45 and 46 and a boundary of 44, and a create of `main` that started before `main` existed puts manifest 1 late. The put succeeds, because manifest 1 was deleted long ago, but the boundary read returns 44, so the put counts as lost, and the create finds manifest 46 and returns `namespace_exists`.

Why does the call look for the namespace before its first write? The lookup keeps a call for a taken ID from writing anything, such as a fork's temporary snapshot. A create that fails before manifest 1 leaves no manifest, so a later find finds no namespace.

Why does a namespace without a source need two manifests? The pool's collection finds members only through their member entries (see [Content roots](#119-content-roots)), so the entry has to exist before the namespace is active. If the call wrote the entry before manifest 1, a call that lost the manifest 1 put would leave an entry in a pool that no manifest records, and two calls with the same request would write entries in two pools. With manifest 1 first, the winning call chooses the pool ID, and every later write uses a key that manifest 1 sets.

### 9.2 Forking a namespace

A fork starts its own history from the source's retained metadata. The "fork point" is the source's sequence that the fork starts at: the head of the source's view for a fork of the head, or the snapshot's head for a fork of a user snapshot. The fork reads the source's segments through a fork snapshot on the source, an object outside the fork's prefix, so the fork call publishes the fork's manifest 1 with the status `initializing` before it writes the fork snapshot and the fork's [member entry](glossary.md#member-entry). It folds the source's [WAL tail](glossary.md#wal-tail) into a manifest of its own, and the source publishes nothing for the fork. To create a fork, LoonFS:

1. Finds the newest manifest for the target's ID (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)). A tombstone or a retired namespace returns `namespace_deleted`, and an active namespace returns `namespace_exists`, as for a create, and in both cases the fork writes nothing. An initializing namespace is finished as described in [Finishing an initializing namespace](#finishing-an-initializing-namespace). If no namespace has the ID, the fork continues.
2. Chooses the user snapshot of the source that the fork is built from. If the request identifies a user snapshot, LoonFS loads it, and returns `snapshot_not_found` if it doesn't exist, is deleted, or has expired. Otherwise LoonFS reads the source's current manifest and WAL tail with the checks of a discovery (see [Resolving the metadata basis](#41-resolving-the-metadata-basis) and [Replaying the visible WAL](#42-replaying-the-visible-wal)), and the fork point is the head of that view. If a commit in that tail (the tail that the fork folds) has a `committed_at_ms` more than `MAX_TAIL_COMMIT_AGE_MS` (11 hours and 45 minutes) before the time that the fork started, the fork writes nothing, LoonFS folds the source, and the fork reads and checks the source's view again. LoonFS then takes a "temporary snapshot" of that manifest, which is a user snapshot with no name that expires `OPERATION_SNAPSHOT_TTL_MS` (5 minutes) after the fork call started, written and verified without a fold (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)). If a newer manifest became current, LoonFS reads the source's view again and checks its age again. A fork that a [rebase](glossary.md#rebase) creates also checks its fork point here, as described below.
3. Generates the IDs of the fork snapshot and the "fork point snapshot" (a fork snapshot on the fork itself), and publishes manifest 1 with the status `initializing`, built from the source manifest that the snapshot of step 2 keeps. A lost put, judged as in step 2 of [Creating a namespace](#91-creating-a-namespace), starts the call again at step 1.
4. If the source's view has commits above the source manifest's head, folds the WAL tail into the fork and publishes manifest 2, still `initializing` (see below).
5. Checks `METADATA_PUBLICATION_BUDGET_MS`, and writes the fork point snapshot with put-if-absent, with the ID that manifest 1 records and no expiry, on the fork's newest manifest (manifest 2 when the fork folded a tail, and manifest 1 otherwise).
6. Checks `METADATA_PUBLICATION_BUDGET_MS`, writes the fork snapshot on the source with put-if-absent, with the ID that manifest 1 records, no expiry, and the source manifest that the snapshot of step 2 keeps. Then it checks that the source is active and that the snapshot of step 2 still exists, isn't deleted, and hasn't expired (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)). Independently of that write, the call writes the member entry with put-if-absent, under the pool that manifest 1 records.
7. Publishes the active manifest, which is the fork's newest manifest with the status `active`, its retention floor raised to the fork point, and the next number.

A fork that folds the source's WAL tail (a fork of the head whose view has commits above the source manifest's head) has a deadline. Before each write after step 2, and before the check after the fork snapshot's write in step 6, it checks that `OPERATION_SNAPSHOT_TTL_MS` hasn't passed since the temporary snapshot's `created_at_ms`. A call past the deadline writes nothing more and returns `snapshot_not_found`. A fork that folds no tail has no deadline, because every reference that it starts with is in the source manifest that the snapshot of step 2 keeps.

Manifest 1 copies the fields that the table in [Publishing a manifest](#72-publishing-a-manifest) lists, including the runs, pool, and [extension](#124-extensions) entries, from the source manifest that the snapshot of step 2 keeps, and every inherited segment keeps its owner. The fork basis records the reference to that manifest (`manifest`), the fork point (`fork_point_seq`), the ID of the fork snapshot on the source (`source_snapshot_id`), the ID of the snapshot of step 2 (`forked_from_snapshot_id`), and the fork point snapshot's ID (`fork_point_snapshot_id`), whose manifest number is 2 when the fork point is above the source manifest's head and 1 otherwise.

The fork folds the source's WAL tail as a fold of its own tail would (see [Publishing a manifest](#72-publishing-a-manifest)). It writes each chain's pending pieces as one extent under the pool's prefix, with the source as the chain's creator, and a segment under its own prefix for each family that the tail's commits touched. Manifest 2 lists manifest 1's runs followed by the new run. Its head is the fork point, its retention floor is manifest 1's (the source manifest's floor), and the floor objects in the tail aren't applied. Its folded WAL number and activity counters stay 0 (see [Statistics](#74-statistics)). A tail of fences and floor objects with no commits counts as no tail. In that case the fork publishes no second initializing manifest, its fork point is the source manifest's head, its fork point snapshot is on manifest 1, and its active manifest is manifest 2.

The fork copies no file bytes, and writes metadata segments only for the source's WAL tail. Its first commit is one sequence above the fork point. An active fork can be forked right away, because its manifest already lists its inherited runs. The fork doesn't delete its temporary snapshot, which expires and is collected like any expired user snapshot (see [Snapshot cleanup](#117-snapshot-cleanup)).

For example, `main`'s current manifest 46 has folded head 400, and its WAL objects 81 to 83 contain commits 401 to 418. A fork of `main` into `sandbox` reads manifest 46 and WAL objects 81 to 83, takes a temporary snapshot of manifest 46, and publishes `sandbox`'s manifest 1, initializing and built from manifest 46, at head 400. It writes span objects for the tail's pieces and its own segments for commits 401 to 418, and publishes its manifest 2 at head 418, still initializing. It then writes its fork point snapshot on its manifest 2, its fork snapshot on `main`'s manifest 46, and `pools/pool_3s0drmj8em84yb4mnetqkz2j0x/members/sandbox.json`, and publishes its manifest 3 with the status `active` and floor 418. `main` publishes nothing, and `sandbox`'s first commit is 419. Apart from the fold's own content and segment writes, that's seven object writes. A fork of a user snapshot of `main` takes no temporary snapshot and folds no tail, so it makes five, and its manifest 2 is active.

A rebase forks the head of the namespace that the replayed fork was forked from, so that namespace is the new fork's source namespace. The new fork's fork basis records `rebased_from`, which can't change after creation: the rebase's source, which is the fork whose commits the rebase replays into the new fork (`source_namespace_id`), and the fork sequence that the replayed fork's next merge into the new fork's source namespace would start after (`resume_after_seq`). LoonFS reads `resume_after_seq` from the source's view of step 2 (the manifest that the temporary snapshot keeps and the WAL tail above it), under the range rule in [Merges](#merges), so it describes exactly the history that the new fork inherits. If the replayed fork's fork point is below that view's retention floor, LoonFS returns `below_retention_floor` in step 2, before it publishes any manifest, and the temporary snapshot expires.

Why does the fork fold the source's tail, and not the source? If the fold's writes happen in the fork, no operation publishes another namespace's manifest, and a fork competes with the source's own folds only when the source's tail is old. The cost is that one tail's metadata segments, and possibly its extents, are stored twice, once by the fork and once by the source's own fold.

Why does a fork keep a snapshot on itself? When the source's WAL tail has commits, the fork's manifest 2 is the only manifest at the fork point, and reads of that state need it (see [Reads, deletion, and expiry](#83-reads-deletion-and-expiry)). The fork point snapshot keeps that manifest whatever the fork's own retention removes. A fork of a user snapshot has one too, so a client reads every fork's starting state the same way.

Why are there two initializing manifests, and why is the floor raised last? Manifest 1 is the claim that decides which call creates the fork, and it records the fork basis that a finishing call needs to fold the tail again, so the fold's result comes after it, in a manifest of its own. The fork point snapshot has to exist before the fork is active, or a call that stopped right after activation would leave an active fork without one, and nothing would finish it. So the snapshot is written on an initializing manifest, which keeps the source manifest's floor, and a read through the snapshot sees the source's history at the fork point (its change feed, for example) from that floor up. (If the tail has a floor object, that read can return history that a read of the source's head no longer returns.) The active manifest then raises the floor to the fork point (see [Advancing the retention floor](#101-advancing-the-retention-floor)), which needs no floor object, because the fork's WAL has no commits yet.

Why doesn't manifest 2 apply the floor objects in the tail? A finishing call reads the source's WAL objects only through the fork point, so it can't see a floor object written after the tail's last commit. If manifest 2 applied floor objects, a finishing call's manifest 2 could record a lower floor than the first call's. With the source manifest's floor, every call's manifest 2 records the same folded head and floor, and two calls' manifests differ only in the IDs of the segments and span objects that each wrote. The active manifest raises the floor to the fork point in any case, and a read through the fork point snapshot still finds every row between the source manifest's floor and the floor that the tail's floor objects set, because the fork's manifests list the source's segments and the fork point snapshot keeps them.

Why does an old tail make the fork fold the source first? A pool collection that listed the members before the fork's member entry existed protects the references that the fork takes from the tail only through the source's roots. With an older tail, the source's fold, retention advance, and base compaction could remove a revision that a later tail commit replaced before that collection reads the source, and the collection could delete content that the fork point snapshot keeps (see the fork case in [Content roots](#119-content-roots)). The check runs on the tail that the fork folds, and not on an earlier view of the source that a process keeps, because that view can be older than the tail. The reference implementation folds a tail that has been idle for 15 minutes (see [Recovery material and maintenance policy](#73-recovery-material-and-maintenance-policy)), so this fold rarely runs.

Why can the source fold and collect its tail while a fork folds it? The checks in step 2 make sure that no fold replaced the source manifest during the read, and the fork folds the records from memory, so a later fold or collection of the source doesn't change what the fork writes.

Why is manifest 1 published before the fork snapshots are written? Manifest 1 records the IDs of both fork snapshots before they exist, so the target's tombstone refers to every fork snapshot that a call writes for the target, and the target's retirement deletes both through that reference (see [Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone) and [Snapshot cleanup](#117-snapshot-cleanup)).

Why does a fork of the source's head take a temporary snapshot? The fork snapshot that keeps the source manifest is written after manifest 1, which lists that manifest's segments. Until the fork snapshot is verified, the temporary snapshot keeps them from a collection of the source, as a user snapshot that the request identifies does.

Why does a fork that folds the source's WAL tail measure its 5 minutes from the temporary snapshot's creation and not from its expiry? [Content roots](#119-content-roots) reserves `OPERATION_SNAPSHOT_TTL_MS` from the fork's read of the tail to its active manifest, and a caller can extend the temporary snapshot's expiry (see [Extending a snapshot](#84-extending-a-snapshot)). The snapshot's `created_at_ms` is stored, no extension changes it, and it's the first call's start, which is no later than its read of the tail.

Why does manifest 1 record the snapshot of step 2? A finishing call reads that snapshot before and after it writes the fork snapshot, and manifest 1 is the only place where it can find the snapshot's ID.

#### Finishing an initializing namespace

A create or fork call that finds the target's ID initializing compares its request with the fork basis of the namespace's newest manifest, or with its manifest 1 for a create:

- A create request matches when manifest 1 has no fork basis and records the retention window that the request would record.
- A fork request matches when the fork basis records the same source namespace, the same user snapshot if the request identifies one, and the same rebase source if the request is a rebase (or no rebase source in both). A request that identifies no snapshot matches whichever snapshot the fork basis records, so the call finishes the fork from the snapshot that the first call chose.

A create never matches a fork's manifest 1, and a fork never matches a create's. `created_by` and `created_at_ms` aren't compared. A request that doesn't match returns `namespace_exists`, with or without `allow_existing`. A matching call continues from the step after the newest manifest that it found (step 3 of a create, or step 4 or 5 of a fork), with the values that manifest 1 records: the pool ID, and for a fork, the fork basis. A fork call first checks that the source is active and loads the snapshot that the fork basis records (the snapshot of step 2), and returns `namespace_deleted` or `snapshot_not_found` without writing anything if the source was deleted or that snapshot doesn't exist, is deleted, or has expired. A fork that folds the source's WAL tail (its fork point is above the head of the source manifest that its fork basis refers to) also has the first call's deadline, measured from that snapshot's `created_at_ms`.

A fork call whose newest manifest is manifest 1, and whose fork point is above manifest 1's head, folds the source's WAL tail again into a manifest 2 with the same folded head, floor, and rows as the first call's, listing this call's segments. It reads the source's WAL objects from the one after the `folded_wal_no` of the source manifest that the fork basis refers to, through the first WAL object whose head is the fork point, whether or not the source has folded them since. If one of those WAL objects is gone, because a fold covered it and a collection deleted it, the call returns `snapshot_not_found`, with the same outcome for the namespace as after the snapshot of step 2 ended.

Each write after manifest 1 is a put-if-absent of a key that manifest 1 sets (the member entry, the active manifest, and for a fork, its manifest 2 and both fork snapshots), so two calls that finish the same namespace at once write the same objects, and both return success. If two calls fold a fork's tail at once, the call whose manifest 2 put loses continues from the other call's manifest 2, and garbage collection deletes the losing fold's segments and extents under the age rules. A call whose put of the active manifest is lost finds the newest manifest again, and returns success if the namespace is active, or `namespace_deleted` if it finds a tombstone or a retired ID.

When a fork's write or its check in step 6 fails (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)), the call doesn't delete either fork snapshot, and finds the newest manifest again. It returns success if the namespace is active, and otherwise `snapshot_not_found` for a snapshot that ended (it expired, or a caller deleted it), or `namespace_deleted` for a deleted source. The namespace then can't become active. It stays initializing until a delete call or the namespace's collection publishes its tombstone (see [Deleting a namespace](#94-deleting-a-namespace)), and its ID is used up.

Why does a finishing fork load the snapshot before its write, when the first call relies on the check after the write? The budget argument in [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot) needs a time at which the call found the snapshot live. Manifest 1 can be as old as the grace window, so without the load, a finishing call could put a fork snapshot on a manifest that the source's collection has deleted. That fork snapshot would keep a missing manifest, which the source's collection reports as corruption until the target is retired.

For example, a create of `main` stops after `main`'s manifest 1. If the client sends the same create request again, the second call writes the member entry, publishes manifest 2, and returns success. A create of `main` with a different retention window returns `namespace_exists`.

For example, a fork of `main` into `sandbox` stops after `sandbox`'s manifest 1. If the client sends the same fork request again within 5 minutes of the first call's temporary snapshot, the second call folds `main`'s WAL objects 81 to 83 again into `sandbox`'s manifest 2, writes the fork point snapshot, the fork snapshot, and the member entry, publishes the active manifest, and returns success. If the client sends it 10 minutes later, the 5 minutes have passed, whether or not the snapshot's record was extended, so the call writes nothing and returns `snapshot_not_found`, and `sandbox` stays initializing until it's deleted.

Why can any call finish the namespace? Every write after manifest 1 uses a key that manifest 1 sets, so no call needs to know which call wrote manifest 1. A call whose manifest 1 put had an unknown outcome finishes the namespace like any other call. A client that retries a create or fork after a lost response gets success if the first call stopped before the active manifest, and `namespace_exists` if the namespace is already active.

### 9.3 Conflicting and unknown installations

A manifest 1 put that loses, or succeeds at or below the [manifest boundary](glossary.md#manifest-boundary), finds the newest manifest and reads the existing namespace state, as [Creating a namespace](#91-creating-a-namespace) describes. Deleted status follows the lifetime rule in [Namespaces and identity](#11-namespaces-and-identity). Invalid bytes, or a key that doesn't match its payload, are corruption. A loser never overwrites the winner.

A confirmed precondition failure is a conflict. After a put with an unknown transport outcome, the publisher reads the manifest at that number. If the number is still missing, the call ends with an unknown outcome, and a later call starts from the current state. If a manifest is there, the publisher reads the manifest boundary next. A number at or below the boundary counts as lost, whatever the read-back found, and the publisher finds the newest manifest and continues from it as after any lost put (see [Resolving the metadata basis](#41-resolving-the-metadata-basis) and [Publishing a manifest](#72-publishing-a-manifest)). Otherwise, the publisher classifies the manifest that it read back:

- Another manifest there is a conflict.
- A first manifest is resolved by finishing: the call finishes the namespace when manifest 1 matches its request, whichever call wrote manifest 1 (see [Finishing an initializing namespace](#finishing-an-initializing-namespace)).
- Identical bytes confirm the attempt's own success when the manifest lists a metadata or extension segment that its predecessor doesn't list, because that segment is one that this attempt wrote. (An [extension's](#124-extensions) publication usually lists a segment it wrote.)
- Every other manifest (the active manifest of a create or a fork, a compactor claim, a writer acquisition, a namespace tombstone, or an extension's publication that lists no new segment) can be rebuilt byte for byte by another publisher. Identical bytes count as the current manifest, and the attempt retries from it, but an extension's publication publishes nothing more, because that manifest's entry is no longer the one that the change built on (see [Extensions](#124-extensions)).

For example, a fold's put of manifest 41 returns a transport error. The read-back finds manifest 41 with the fold's bytes, but the boundary read returns 44, so the put counts as lost. The fold finds manifest 45 as the newest and checks whether manifest 45 already covers its work.

Why don't identical bytes always prove success? Two publishers can build exactly the same manifest from the same predecessor (for example, two compactors that claim the same manifest). Only a manifest that lists a segment that this attempt wrote proves that the attempt succeeded.

### 9.4 Deleting a namespace

Deletion uses the acquired writer epoch. After the admitted commits finish, LoonFS folds the remaining WAL as the namespace's [maintainer](glossary.md#maintainer), under the current compactor epoch, and then publishes the next manifest with a deleted status under the writer epoch. That manifest records `deleted_at_ms` from the deletion call's clock, and deletion starts the tombstone publication within `METADATA_PUBLICATION_BUDGET_MS` of capturing that clock. The tombstone's runs, counters, and folded WAL boundary cover the final head, so every WAL object of the deleted namespace is at or below `folded_wal_no`. If the fold fails, the namespace stays active, and the deletion can be retried. Data that was already committed stays committed.

An initializing namespace has no writer and no WAL objects, so its deletion needs no writer session, writer claim, fence, or fold. A delete call publishes the tombstone as the next manifest after the namespace's newest manifest, with `deleted_at_ms` from the deletion call's clock. The tombstone and the active manifest compete for the same number, so the namespace becomes deleted or becomes active, and never both.

A create or fork call can also be abandoned before its active manifest, by a process that stops or by a fork whose snapshot ended. LoonFS generates a namespace ID when the request doesn't include one, and a caller that never learned that ID can't finish or delete the namespace, so a namespace's collection publishes the same tombstone when the namespace's newest manifest is initializing and that manifest's `created_at_ms` is more than the grace window `T` (`GC_GRACE_WINDOW_MS`, 24 hours) before the collection's clock. Until then, the collection reports the end of that window in `next_reclamation_at_ms`. After that, the namespace is retired like any deleted namespace (see [Retirement](#95-retirement)).

For example, a create with no namespace ID generates `ns_5gk2a8m3qy9b7d0f4h6j8n2p1r`, publishes that namespace's manifest 1, and stops. The reference server's [sweep](glossary.md#sweep) runs the collection of every namespace that it lists, so the first collection of `ns_5gk2a8m3qy9b7d0f4h6j8n2p1r` that runs more than 24 hours later finds manifest 1 initializing and older than the grace window, and publishes a tombstone as manifest 2. A later collection retires the namespace.

Why does the collection wait for the grace window? The wait keeps the collection from deleting the namespace of a call that's still running. Safety doesn't need the wait, because the tombstone and manifest 2 compete for one number.

An ordinary operation that finds the namespace deleted returns `namespace_deleted`, but a cached reader can keep using its active view until its next manifest revalidation is due. Deleting a namespace doesn't immediately remove its content. The tombstone protects it until the retirement deadline (see [Retirement](#95-retirement)), and the manifests that the namespace's snapshots keep protect it until those snapshots are deleted (see [Reference roots](#112-reference-roots)). A separate purge operation is outside this specification.

### 9.5 Retirement

"Retirement" is the point at which a deleted namespace stops being a member of its [pool](glossary.md#pool). Garbage collection retires a namespace by raising its [manifest boundary](glossary.md#manifest-boundary) to the retired state. At the retirement deadline (defined below), the tombstone's own content references stop protecting the pool's content, even while a fork's snapshot keeps the namespace from retirement (see [Content roots](#119-content-roots)). At retirement, the namespace has no roots and no content references left.

No retirement deadline is stored. A deleted namespace can be retired the grace window (24 hours) after its deletion, so for a namespace tombstone `T`, the deadline is derived as:

```text
deadline(T) = T.deleted_at_ms + GC_GRACE_WINDOW_MS
```

A tombstone is eligible for retirement when the collection's clock reaches its retirement deadline and the complete snapshot listing contains no snapshot on the namespace other than the namespace's own fork point snapshot. Snapshots deleted later in the same collection still count, and any unrecognized key under the snapshot prefix also blocks retirement.

To retire a namespace whose tombstone is eligible, the namespace's collection:

1. If the namespace is a fork, deletes the two fork snapshots that the tombstone's fork basis refers to: the fork snapshot on its source and its own fork point snapshot.
2. Deletes the namespace's [member entry](glossary.md#member-entry) under the pool that the tombstone records.
3. Raises the boundary to retired.

The pool ID and fork basis can't change after manifest 1, so the collection finds both keys in the tombstone. [Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone) describes how a collection that stops partway starts again.

After the boundary is retired, later collections delete the namespace's manifests (the tombstone included), data WAL objects, segments, and snapshot records under the age rules (see [Candidate and age rules](#113-candidate-and-age-rules)), and keep the boundary object (`boundary.json`), the fences, the floor objects, and every unrecognized key (see [One complete collection](#111-one-complete-collection)). The ID is never reused, so a stale writer's put in a retired namespace collides with the fence or floor object at its number. Every request to a retired namespace except a collection returns `namespace_deleted`. A create or fork that uses the retired ID writes nothing, and a manifest put that a provider applies after the retirement counts as lost (see [Publishing a manifest](#72-publishing-a-manifest)).

For example, `sandbox`, a fork of `main`, writes its last segments at 09:50 on Monday and is deleted at 10:00. With the 24-hour grace window, its tombstone is eligible at 10:00 on Tuesday. A collection at 03:00 on Wednesday deletes `sandbox`'s fork snapshot on `main`, `sandbox`'s fork point snapshot, and `sandbox`'s member entry, and raises `sandbox`'s boundary to retired. The next collection of `sandbox` deletes the tombstone, the other manifests, the data WAL objects, and the segments, which are all more than 24 hours old, and keeps its fences and floor objects.

Each fork keeps a fork snapshot on its source, so a namespace is retired after every fork of it. Once every member of a pool is retired, no root protects the pool's content, and the pool's collection deletes its content under the age rule and its upload records under the rules in [Upload-session cleanup](#116-upload-session-cleanup).

Why are the fork snapshots deleted before the boundary is raised? The tombstone's fork basis is the record of which snapshots to delete, and the tombstone stays until the boundary is retired. If the boundary were raised first and the collection stopped, a later collection could delete the tombstone, and nothing would delete the fork snapshot on the source (see [Snapshot cleanup](#117-snapshot-cleanup)).

Why doesn't a namespace's own fork point snapshot keep it from retirement? No other namespace reads through that snapshot, so the namespace's retirement deletes it with the fork snapshot on the source.

Why can the member entry be deleted before the boundary is raised? The entry is deleted only once the tombstone is eligible, so the namespace leaves the member list no earlier than its retirement deadline, the bound that the pool's time limit for reading every member depends on (see [Content roots](#119-content-roots)).

Why is the boundary raised to retired, and not to the tombstone's number? Garbage collection never leaves a namespace that isn't retired without a manifest above its boundary, so a reader that found no manifest above a boundary at the tombstone's number would report a corrupt store.

Why is the boundary object kept? It records that the ID was used, so a create or fork with the ID, and a publisher whose manifest put is applied late, find the retired boundary.

Why doesn't the last member's retirement delete the pool's content in one step? The ordinary age rule already deletes the content, upload records, and temporary objects of a pool with no members, so a separate rule would be a second way to delete content.

### 9.6 Fork dependencies after deletion

A fork snapshot is required until its target is retired, and the target's retirement deletes it before raising the target's boundary, so a failed deletion leaves the tombstone for another collection (see [Snapshot cleanup](#117-snapshot-cleanup) and [Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone)). Rewriting a target's metadata doesn't copy or move inherited file bytes, because every member of a pool finds them under the pool's prefix.

Snapshots are deleted from descendants to ancestors. For example, suppose B is a fork of A, and C is a fork of B. C's fork snapshot on B prevents B from being retired, and B's fork snapshot on A stays until B is retired. Retiring C deletes C's fork snapshot on B. B can then be retired once its deadline and snapshot checks pass, which deletes its fork snapshot on A.

### 9.7 Cross-namespace copies and moves

A rename that keeps the inode stays within one namespace. Across namespaces, a move is a copy to the destination followed by a deletion in the source, without an atomic transaction across both histories.

Within a [pool](glossary.md#pool), a reference identifies the same object in every member, so a fork keeps using inherited references through its fork snapshot, and a merge adds the fork's references to its source. No operation adds a reference to another pool's content, because that pool's collection doesn't read the destination's roots, so the destination would need a snapshot on the source for as long as the reference exists. Content moves between pools by an upload.

A [merge](glossary.md#merge) admits its content references from the fork's own rows, and it plans from the fork's [read view](glossary.md#read-view) (its current manifest and WAL tail). A reference whose bytes are partly or wholly [pieces](glossary.md#piece) in the fork's tail has no layout yet, so before LoonFS admits it, the merge writes those pieces under the pool's prefix as the chain's next extent, as validation does before it admits a copy (see [Operations and WAL deltas](#66-operations-and-wal-deltas)), and never combines that extent with the chain's existing extents. The layout records the creator that the fork's layout row for the chain records, or the fork for a chain that started in the fork's tail (see [Publishing a manifest](#72-publishing-a-manifest)). So the merge includes each reference with the layout that the fork's layout rows record, followed by the merge's extent when it wrote one. LoonFS admits the reference without reading its bytes, because the fork wrote those rows and pieces itself and no caller supplied the references. Every reference in the commit has a layout, so the commit includes no inline content.

For example, a fork creates `/notes.txt` with 10 KiB, appends 2 KiB, and merges before it folds. The merge writes the 12 KiB as one span object of the fork's chain, and the commit's layout lists that extent, with the fork as the chain's creator. The fork's own later fold writes the same pieces again, to a span object of its own (a longer one when the fold combines them with the chain's earlier extents), so the pool stores the 12 KiB twice, once in the object that each layout lists.

Until the merge is in the namespace's WAL tail, where the namespace's own content roots keep the content (see [Content roots](#119-content-roots)), a snapshot protects the fork's content. While a merge or a rebase plans, LoonFS takes an unnamed user snapshot of the fork's current manifest without a fold (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)), which expires `OPERATION_SNAPSHOT_TTL_MS` (5 minutes) after the merge was prepared (a time that LoonFS takes before it reads the fork's head), and LoonFS deletes the snapshot after the commit. The snapshot keeps the fork's manifest but not its WAL tail. So if the fork's tail has a commit whose `committed_at_ms` is more than `MAX_TAIL_COMMIT_AGE_MS` (11 hours and 45 minutes) before the time that the merge was prepared, LoonFS folds the fork first, takes the snapshot on the manifest that the fold published, and plans from that manifest. `MAX_TAIL_COMMIT_AGE_MS` is the grace window less the terms on the left of the pool collection's inequality (see [Content roots](#119-content-roots) and [Publication and collection timing](#c1-publication-and-collection-timing)):

```text
MAX_TAIL_COMMIT_AGE_MS = GC_GRACE_WINDOW_MS - (POOL_ROOTS_READ_BUDGET_MS
    + GC_SAFETY_MARGIN_MS + OPERATION_SNAPSHOT_TTL_MS
    + PROVIDER_PUBLICATION_REQUEST_BOUND_MS)
```

Why does a merge write the fork's pieces instead of folding the fork? A fold of the fork would publish a manifest of the fork from another namespace's operation, and it would take the fork's fold path, so it would wait for a fold that the fork's writer session is running, and the session's next fold would wait for it. The merge needs only the part of a fold that writes a layout for each reference. The cost is that each chain's pending pieces are written twice, once by the merge and once by the fork's own later fold.

Why does an old tail make the merge fold the fork first? Suppose a revision in the merge's range is in the fork's tail, and a later tail commit replaced it before the plan, so the snapshot doesn't keep it. Once a retention advance passes the replacing commit (at least the grace window after it), a base compaction can remove the revision's layout row. While no tail commit is older than `MAX_TAIL_COMMIT_AGE_MS`, any pool collection that read the receiving namespace (the one that the merge commits into) before the merge's commit has also read the fork by the time that happens, so the collection still finds the layout (see [Content roots](#119-content-roots)). With an older tail, a collection could read that namespace before the commit and the fork after the compaction, and delete objects that the merge's commit refers to. As with a fork, the reference implementation's fold of idle tails keeps tails young, so this fold rarely runs.

## 10. Retention and compaction

This section describes how much history a namespace keeps, and how compaction rewrites metadata without changing what's visible.

Every commit adds metadata rows, so without a limit, old revisions, bindings, and receipts would build up indefinitely. The "retention floor" sets how far back history is guaranteed, and "compaction" rewrites a window of metadata runs into one run and removes the rows that the floor no longer requires. Neither operation publishes new filesystem changes, and compaction preserves every retained view.

### 10.1 Advancing the retention floor

The retention floor limits how far back LoonFS keeps incremental replay, superseded bindings, file revisions, old attribute states, and commit receipts. Above the floor, every file's history is complete. At or below it, each file that's visible or under a deletion in the trash keeps its newest revision, and a file that an expired deletion hides keeps none (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)). The floor doesn't expire layout rows, and it doesn't keep WAL objects (garbage collection removes them once they're folded).

The floor moves only when an advance creates a [floor object](glossary.md#floor-object) (described below). A new namespace's floor is 0. A fork's initializing manifests keep its source's floor, and its active manifest sets the floor to the fork point (see [Forking a namespace](#92-forking-a-namespace)). The change feed returns only commits above the floor, so a fork's floor at its fork point keeps the source's commit at that sequence out of the fork's feed.

A floor advance has one of three targets:

- the current manifest's `head_seq`,
- a sequence (a sequence above `head_seq` targets `head_seq`), or
- a cutoff time.

For a cutoff time, the advance reads the `commits` family upward from the current floor and stops at the first commit whose `committed_at_ms` is later than the cutoff. The target is the last commit the walk passed, or the current floor if it passed none. Commit timestamps don't have to increase with sequence. For example, suppose the floor is 10, commits 11, 12, and 13 were committed at 9:00, 9:05, and 9:02, and the cutoff is 9:03. The walk passes 11 and stops at 12, so the target is 11, even though 13 is earlier than the cutoff.

Whatever the target, an advance MUST NOT move the floor past a commit whose `committed_at_ms` is later than the advancing host's clock minus the [grace window](glossary.md#grace-window) (`GC_GRACE_WINDOW_MS`, 24 hours, see [Publication and collection timing](#c1-publication-and-collection-timing)). That time is a cutoff for the same walk. For the head or a sequence, the new floor is the lower of the target and the last commit the walk passes. For a cutoff time, or a retention window (described below), the advance walks once, to the earlier of the two cutoffs. If the first commit above the current floor is later than the cutoff, the floor stays where it is. The advancing host reads its own clock, as garbage collection does (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).

For example, with the same commits and the head at 13, an advance that targets the head at 9:01 the next day stops at commit 12, so the floor becomes 11. At 9:04 the next day, the walk still stops at commit 12 (23 hours and 59 minutes old), although commit 13 is more than 24 hours old. At 9:05 the next day, the floor becomes 13.

The advance plans the new floor from the floor and head of a writer session's [writer position](glossary.md#writer-position), or, without a position, of the current manifest, and the walk reads that manifest's `commits` family. The advance returns the floor of the view that its discovery found (see the steps below), which is below the target when the grace window's cutoff stops the walk first. The floor never decreases.

The retention floor of a [read view](glossary.md#read-view), or of a writer position, is the highest of the manifest's `retention_floor_seq` and the `retention_floor_seq` of every floor object in the view's unfolded WAL tail (see [Replaying the visible WAL](#42-replaying-the-visible-wal) for an example). Every rule that reads the floor of a view reads this value: restore validation (of a deleted item and of a file revision), the trash listing's scan start, the change feed's lower bound, the merge floor rule, and the reads that return `below_retention_floor`. A base compaction is the exception, because it reads the floor of its basis manifest (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)).

A namespace's "retention window" (`retention_window_ms`) is an optional manifest setting, a number of milliseconds that's at least the grace window. A maintainer that advances the floor on a schedule (like the reference server's sweep) reads the window from the current manifest at each advance and uses the current time minus the window as the cutoff time. If the current manifest has no window, the maintainer doesn't advance that namespace's floor on a schedule. For example, the sweep keeps 7 days of history for a namespace with a 7-day window, and a namespace without a window keeps its whole history.

The window comes from the create request, or else from the host's default window (the reference server's `retention_window_ms` setting), and a fork copies it from its source (see [Publishing a manifest](#72-publishing-a-manifest)). A later publication can set, change, or clear the window, and it copies every other field of the current manifest. If the current manifest already has the requested window, LoonFS publishes nothing. A host MUST refuse a window shorter than the grace window, whether it comes from a create request, the host's default window, or a later publication.

A snapshot below the new floor still protects its own manifest and runs.

A deletion whose `deletion_seq` is at or below a view's floor has expired in that view. A restore of it then returns `not_deleted`, the trash listing leaves it out, and no read reaches the inodes it hides (see [File and subtree deletion](#16-file-and-subtree-deletion) and [Paths, listings, and revision reads](#44-paths-listings-and-revision-reads)). The advance commits nothing for the deletions it passes. Every deletion that a fork inherits has expired in the fork from its creation, because the fork's floor starts at its fork point.

A floor object is a WAL object with no commit records whose payload sets `retention_floor_seq` to the new floor (see [WAL records](#a5-wal-records)). Like a fence, it's created at the next WAL number and doesn't advance `head_seq`, so it adds no commit, sequence number, or change feed entry. For example, if the current manifest has writer epoch 3 and the [WAL tip](glossary.md#wal-tip) is WAL object 30 at sequence 140, an advance to floor 20 creates WAL object 31 with writer epoch 3, `head_seq` 140, no records, and `retention_floor_seq` 20.

Any process MAY create a floor object, without claiming the writer epoch or the compactor epoch. A floor object's writer epoch MUST be the writer epoch of the manifest that the creating attempt's discovery loaded before the put. Because each attempt copies the epoch from its own discovery, a floor object is never written above a fence with a higher epoch, and writer epochs never decrease as the WAL number increases.

To advance the floor to a new floor `T`, a process:

1. Plans `T` with the rules above. If `T` isn't above the floor that it planned from, the process writes nothing and returns that floor.
2. Checks that the referenced segments of the manifest that it planned from exist.
3. Runs discovery to find the WAL tip. If the discovered view's floor is already at or above `T`, the process writes nothing. Otherwise, it creates the floor object at `tip + 1` with put-if-absent, with the writer epoch of the manifest that this discovery loaded.
4. After the put succeeds, checks that manifest with the successor probe and the boundary read (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). The floor object is live if that manifest is still the newest, or else if its number is above the newest manifest's `folded_wal_no`. A floor object that isn't live is dead. It stays, but no reader replays it.
5. If the put collides, or the floor object is dead, runs step 3 again at the next WAL number that the new discovery finds, up to a bounded number of attempts (eight in the reference implementation).

If the process can't tell whether the put succeeded, it reports the outcome as unknown, and a retry that creates a second floor object with the same floor does no harm.

Why does the process check the floor object's number after the put? Before the put, a writer session can commit at the discovered `tip + 1`, and a collection can delete those commits once they're folded and older than the grace window. The put then succeeds at an emptied number at or below the newest manifest's `folded_wal_no`, where no reader replays it, and the check sends the next attempt above that manifest. If a fold covers a live floor object before the check, the check treats it as dead too, and the next attempt's discovery finds the floor already in the view and writes nothing.

A writer session that has its writer epoch instead puts the floor object from its publication queue at its position's tip plus one, like a commit, so it never collides with the session's own commits and needs no check after the put (the argument in [Publication and group commit](#63-publication-and-group-commit) covers such a put). The position replays the floor object, so its tip is the floor object's number and its floor is `T`. Commits admitted before the advance are below the floor object, and commits admitted after it are validated against the new floor, because a writer validates a batch at publication, against its position. For example, a restore queued behind an advance returns `not_deleted` for a deletion that the advance expires, and one queued before it brings the item back. A session that doesn't have its writer epoch, or that discovery finds fenced, follows the steps above.

A fold that covers a floor object records the floor in the manifest it publishes (see [Publishing a manifest](#72-publishing-a-manifest)). Every publication other than a fold MUST copy its predecessor's `retention_floor_seq`, so LoonFS publishes no separate manifest for the floor, and a base compaction removes rows only at or below a floor that a fold has published. The reference implementation's advance folds the namespace after it creates the floor object whenever no manifest records the view's floor.

A [writer session](glossary.md#writer-session) in another process doesn't learn of a floor object when it's created. The session's next put at `tip + 1` collides with the floor object, and the session then plans its batch again against the new floor without being fenced (see [Publication and group commit](#63-publication-and-group-commit)). A floor object is created without a claim manifest, so two more rules apply: a writer never keeps a position whose manifest folds past the position's tip (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), and no process deletes a floor object (see [Candidate and age rules](#113-candidate-and-age-rules)). Without them, a writer could commit at a WAL number that a manifest has already folded, after a floor object that the writer never read was folded and collected.

These rules keep an invariant. For every manifest `M` and every deletion `D` whose deletion sequence is at or below `M`'s retention floor and whose latest event in `M`'s rows is `set`, no WAL object above `M`'s `folded_wal_no` contains an accepted restore of `D`, and no WAL object written later will.

`M`'s floor comes from a floor object at some WAL number `k` at or below `M`'s `folded_wal_no`, or from an earlier manifest, for which the same argument applies. Suppose that a WAL object `W` contains an accepted restore `R` of `D`:

- **`W` is below `k`.** The fold that published `M`, or an earlier fold, folded `W`, so `D`'s latest event in `M` is the restore's `revoke`, not `set`.
- **`W` is above `k`.** The writer created `W` at its tip plus one, so its tip was at or above `k`. A writer's tip passes a WAL number only when the writer creates the object at that number or runs discovery, which replays every WAL object above the manifest's `folded_wal_no` (a manifest from another process never raises the tip, because a position doesn't move onto a manifest that folds past it). The writer didn't create the object at `k`, so it ran a discovery that replayed the floor object at `k` or started from a manifest that had folded `k` and records a floor at least as high. Either way, it validated `R` against a floor at or above `D`'s deletion sequence and refused `R`.

The second case applies to a writer of any epoch. Every later manifest copies or raises `M`'s floor and folds at least as much of the WAL, so the invariant is true for it too. So a base compaction can remove the rows that an expired deletion hides (see [Row retention during a base compaction](#103-row-retention-during-a-base-compaction)). The same argument covers a restore of a file revision by reference. A restore validated against the floor before the advance is in a WAL object below `k`, so its new revision row is in `M`'s rows, and a compaction can drop old revisions at `M`'s floor. A writer MUST still refuse to restore a revision at or below its position's retention floor, unless the revision is the file's newest at or below that floor.

For example, suppose the floor is 5, and a writer session with tip 30 has queued a restore of a deletion at sequence 10, an advance to 20, and a second restore of that deletion. The session commits the first restore at 31 and the floor object at 32, and the second restore, validated against floor 20, returns `not_deleted`. The next fold publishes floor 20 with the deletion's latest event `revoke`, so a base compaction keeps the restored item's rows.

If instead another process creates the floor object at 31, a writer session's restore collides at 31, the session's discovery replays the floor object, the restore returns `not_deleted`, and the session keeps its writer epoch. If the writer's restore is written at 31 first, the advancing process's put collides, and it creates the floor object at 32. Any fold that covers 32 also covers 31, so the manifest that records floor 20 also contains the restore's `revoke` event.

Why does the floor stop at a commit that's at least the grace window old? A reader that opened a file revision can keep reading it after a newer revision is committed. If the floor could reach the replacing commit right away, a base compaction could remove the replaced revision's row within minutes, so a read of it, or a request for a presigned URL for it, would return `revision_not_found`, and garbage collection could delete its content while a presigned URL is still valid (see [Content roots](#119-content-roots)). With the rule, the revision and its content stay for at least the grace window after the replacing commit, which views, their presigned URLs, and pool collections rely on.

Why does the walk stop at a commit dated after the cutoff, even when a later commit is older? The floor then passes a commit only when that commit's own timestamp is at or before the cutoff, so a commit from a writer whose clock runs behind can't move the floor past later-dated commits below it. The cost is that a writer whose clock runs a day ahead keeps the floor below its commits for about two days.

Why does the advance apply the grace window's cutoff with the same walk as a cutoff time? With one walk, the advance reads the commits between the floor and the cutoff once: one commit if every commit is younger than the grace window, the commits since the previous advance with a scheduled window, and the whole history on the first advance.

Why is the window stored in the manifest, and not in each maintainer's configuration? Namespaces in one store can need different amounts of history, and every maintainer of a namespace reads the same current manifest, so all of them use the same window, at no extra request. If two servers applied their own settings of 30 days and 7 days instead, the namespace would keep 7 days, because the floor never decreases.

Why can't a window be shorter than the grace window? A 6-hour window would still keep 24 hours of history, because of the grace window rule, so refusing it means that the window a caller sets is the history that the namespace keeps.

Why does an advance without a writer position plan from the current manifest's floor, and not from the view's floor after the WAL tail? A floor object only raises the floor, so a plan that doesn't move the floor above the manifest's floor wouldn't move it above the view's either. On an idle namespace whose floor already reached its cutoff, the advance then sends only the requests that find the current manifest, and a plan that moves the floor runs discovery before the put anyway.

Why check that the segments exist first? Advancing the floor ends the guarantee that older history can be replayed, so LoonFS first makes sure the history it keeps is readable. The check isn't a transaction and doesn't replace garbage collection's reference rules, and reads still verify checksums.

Why does a WAL object order the advance when a manifest can't? A writer can validate a restore against an older manifest after another process publishes a manifest that moves the floor, but it can't create a WAL object above a number without reading the WAL object at that number or a manifest that folded it, so it can't skip a floor object.

Why may a process without the writer epoch create a WAL object? The writer epoch fences commits from an older writer, and a floor object contains no commits and is ordered by put-if-absent at the next WAL number like any other WAL object, so it can't branch the history.

Why doesn't a floor object fence the writer? A writer is fenced when its discovery finds a higher writer epoch, and a floor object doesn't change the epoch. A writer session in another process pays one refused put and one discovery per floor object, and several processes can advance a namespace's floor, with the higher floor applying.

Why does the floor take effect in a view before a manifest records it? Restore validation has to use the new floor from the floor object's WAL number onward, and with one definition of a view's floor, a restore refuses every deletion that the trash listing leaves out.

Why does a fold publish the floor, instead of a separate manifest from the advancing process? The invariant requires a manifest to record a floor only after every WAL object below the floor object is folded, which a fold that covers the floor object has done anyway.

### 10.2 Compaction windows

Metadata is compacted by family group. The `direntry_binds` and `direntry_child_binds` families form one group, because they MUST stay consistent with each other. The `commits`, `commit_receipts`, and `merges` families form another, and each of the other seven groups contains one family. [Metadata rows and row keys](#a6-metadata-rows-and-row-keys) lists the groups.

A compaction rewrites a contiguous window of a group's runs into one run. The window either includes the group's oldest run or starts above it, and it can't skip a run in the middle.

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

A delta-only compaction MUST rewrite at least two runs, so once a group has one delta run above a much larger base, the next compaction of the group includes the base.

LoonFS chooses a group's window by size tier. A "size tier" is a contiguous set of a group's runs whose sizes are close to each other, where a run's size is the stored bytes of the group's segments in that run. To choose a window, LoonFS:

1. Lists the group's runs from newest to oldest, with the base last.
2. Starts a tier at the newest run, and adds each next older run while that run is at most four times the size of the smallest run already in the tier.
3. Compacts the tier if it has at least four runs. If the tier has more than eight runs, LoonFS compacts its oldest eight, and the newer runs wait for a later compaction.
4. Otherwise starts a new tier at the next older run, and repeats step 2, until no older run is left.
5. If no tier has four runs, and the group has at least eight delta runs, compacts a window of the group's newest runs. The window starts with the group's two newest runs, and adds each next older run while that run is at most four times the total size of the runs already in the window, up to eight runs.

For example, suppose a group's runs, from oldest to newest, are a 300 MiB base and delta runs of 90, 100, 110, and 2 MiB. The tier that starts at the 2 MiB run stops before the 110 MiB run, because 110 is more than four times 2. The tier that starts at the 110 MiB run adds the 100 and 90 MiB runs, and then the base, because 300 is at most four times 90. That tier has four runs, so they become a new base, and the 2 MiB run waits.

In step 5, if a group with eight delta runs and no tier of four has newest runs of 400, 40, 2, 1, and 1 MiB (oldest to newest), the window takes the two 1 MiB runs and the 2 MiB run, and stops before the 40 MiB run, which is more than four times 4 MiB. With newest runs of 400, 40, 30, and 1 MiB, it would take the 1, 30, and 40 MiB runs, because 40 is at most four times 31.

(The tier size of four runs, the size ratio of four, the limit of eight delta runs, and the limit of eight input runs to one compaction are implementation defaults, not format requirements. A compaction can rewrite any window that follows the earlier rules in this section.)

Why can't a compaction above the oldest run drop rows? An excluded older run might contain rows that a row in the window supersedes, such as an older binding version below an unbound one, or a listed deletion whose removal row is in the window. Dropping rows in the window could make them visible again.

Why compact by size tier? A compaction rewrites every byte of its input, so compacting a small new run with a large one rewrites the large run for little new data. In a tier, a byte is rewritten about once each time its run grows four times, and each compaction of at least four runs removes at least three runs. A new run that doesn't fit a tier waits, so a fold that publishes during a compaction leaves a run that waits for later folds instead of making the group rewrite its new base.

Why is there a limit of eight delta runs? A read of a key checks every run of its group that might contain it, and if the runs never make a tier of four (for example, if each run is more than four times the size of the next newer one), the limit bounds that number.

Why does the window at the delta run limit follow the size ratio instead of taking a set number of runs? With a set number, a group that keeps receiving small runs would rewrite its large run at every compaction. With the ratio, the number of times a byte is rewritten grows with the group's size, not with the number of small runs that arrive. The window always includes the two newest runs, because compacting one run wouldn't reduce the run count.

### 10.3 Row retention during a base compaction

These rules apply only when the selected inputs include the group's oldest run. A delta-only compaction keeps every row, including unbound versions at or below the floor, except that it keeps only the first layout row for each content ID, because the longest layout covers every shorter prefix.

| Family | Rows kept or removed |
| --- | --- |
| `inodes` | Keep every inode row, except the row of an inode that an expired deletion hides (see below). |
| `direntry_binds`, `direntry_child_binds` | For each edge (a parent, a name key, and a child) and each kind of row (binding events, and the summary rows that the slot index alone stores), keep every row above the floor and the newest row at or below it. If the edge's newest binding event at or below the floor is unbound, drop the edge's rows of both kinds at or below the floor. Remove every row of an edge whose parent an expired deletion hides. |
| `revisions` | For each file, keep every revision above the floor and the newest revision at or below it, and remove earlier revisions. Remove every revision of a file that an expired deletion hides. |
| `tombstones` | Keep every `set` and `revoke` event. |
| `trash` | Remove a cancelled `listed` and `removed` pair together, and remove the `listed` row of a deletion at or below the floor. A deletion above the floor keeps its `listed` row. |
| `commits`, `commit_receipts`, `merges` | Remove rows strictly below the floor. The row at the floor is kept, and the change feed never replays it (in a fork, it's the source's commit at the fork point). |
| `content_layouts` | Keep the first row for each content ID, ordered by longest layout and then newest commit. Drop chains that aren't in the basis's revision filter (see below). |
| `attributes` | For each inode, keep every revision above the floor and the newest revision at or below it, and remove earlier revisions. Remove every revision of an inode that an expired deletion hides. |
| `access` | For each inode, keep every revision above the floor and the newest revision at or below it, and remove earlier revisions. Remove every revision of an inode that an expired deletion hides. |

Summary rows are versions of their edge for retention, and the child index keeps the same binding event as the slot index. An unbound binding version acts as a tombstone, and it MUST stay while it hides older versions of its edge, so only a bottom-anchored compaction drops it, with every version it hides. Runs cover separate sequence ranges, so a bottom-anchored compaction includes every older version that can affect its floor state.

A bottom-anchored compaction of `content_layouts` also drops the rows of chains that no `revisions` row in its basis refers to. It scans the basis's revisions once before it rewrites layouts and checks each chain against a Bloom filter, which can err only toward keeping a row. The filter uses two xxh64 hashes and a seed from the output run number, it's sized as [Filter sizes](#filter-sizes) describes up to `MAX_BLOOM_FILTER_BYTES` (64 MiB), and its expected count is the sum of the revision segments' `row_count`. A filter at the maximum size has fewer bits per ID and uses fewer hash positions, so it keeps more rows for a later compaction to drop.

A deletion has expired in a compaction's [metadata basis](#41-resolving-the-metadata-basis) when its deletion sequence is at or below the basis manifest's `retention_floor_seq`, and no commit can restore it, folded or not (see [Advancing the retention floor](#101-advancing-the-retention-floor)). If its latest tombstone event in the basis is `set`, a bottom-anchored compaction of the `inodes`, `revisions`, `attributes`, or `access` group, or of the bindings group, removes every row of the root and its descendants. It tests a binding row by the edge's parent, so it removes every version of an edge whose parent is hidden. The root's own edge has a visible parent, and the binding rule drops it, because its newest version at or below the floor is unbound. The other families need no rule of their own: `tombstones` keeps the deletion's events, `trash` removes the root's `listed` row, and the revision filter drops the chains of the removed revisions.

Before such a compaction rewrites its window, it builds a "kept inode filter", a Bloom filter of every inode in the basis that's visible or under a deletion in the trash. To build the filter, the compaction:

1. Walks the basis's directory tree from the root inode, depth first, with one cursor for the directory that it's reading and one for each ancestor directory that still has unread entries. It reads each directory's children from `direntry_binds`, and it inserts and follows the child of each edge whose first row, which is its newest version, is bound.
2. Walks the subtree of every deletion in the trash the same way, starting at the deletion's root. It reads those roots from the `listed` rows in the `trash` family that have no `removed` row and whose deletion sequence is above the floor.

Deleting an item unbinds it, and an expired deletion starts no walk, so neither walk reaches the inodes that the deletion hides. A deletion inside another deletion is the older of the two (see [File and subtree deletion](#16-file-and-subtree-deletion)), so no deletion in the trash is inside an expired one, and the two walks reach every ancestor through which the rights on an item under a deletion in the trash are evaluated. The compaction removes a row whose inode isn't in the filter, and it applies the rules in the table to a row whose inode is. Both binding indexes are rewritten in one compaction against the same filter, so each binding event is kept or removed the same way in both. Each of these compactions reads the bindings of the visible tree and the trash once more.

The kept inode filter follows the layout filter's rules: two xxh64 hashes, a seed from the output run number, 10 bits per expected inode with the same minimum and maximum sizes, and an expected count that is the sum of the basis's `inodes` segments' `row_count`. A false positive keeps a hidden inode's rows for one compaction, and the next compaction of the group has a different output run number, so it removes them. The walk stops at `MAX_KEPT_INODE_WALK_DEPTH` (4,096) cursors. (Once the walk has read a directory's last row, it drops the directory's cursor before it descends into the directory's last child, so a chain of directories that each fit in one 64-row scan uses one cursor at any depth.) A compaction whose walk stops at the limit keeps every row that it would have tested against the filter, logs that choice, and reports `expired_row_removal_walk_limit` in the `metadata` job's result (see [Jobs and outcomes](api.md#jobs-and-outcomes)). A delta-only compaction doesn't build the filter, and neither does a base compaction over a manifest whose `retention_floor_seq` is 0, which keeps every row, because no deletion in its basis has expired.

For example, suppose the directory `/old` contains `a.txt`, `/old` is deleted at sequence 40, and the basis manifest's floor is 50. Neither walk reaches the inode of `/old`, because the deletion unbound `/old` and has expired. So a base compaction of `revisions` removes every revision of `a.txt`, and a base compaction of the bindings group removes every version of the edge from `/old` to `a.txt` and, under the binding rule, the root's unbound edge to `old`. The tombstone keeps its `set` event.

Why can a base compaction remove every row of a hidden inode, and not just the older rows that the usual rules drop? LoonFS commits no change to a hidden inode, so all its rows are at or below the floor, and the tombstone hides it at every retained sequence. The tombstone events stay because family groups are compacted at different times, and they keep the inodes hidden while some groups still store their rows.

Why does the compaction find hidden inodes by walking the tree, and not by a flag on each row? A tombstone on a directory is one row that hides the whole subtree, and a flag on every row would turn one deletion into a write for every descendant. The visible tree and a hidden subtree can each have more inodes than fit in memory, so the compaction uses a filter sized before the walk, which can err only toward keeping a row.

Why can a compaction drop a revision that a restore validated against an older floor might copy? A restore copies a revision by reference, without its layout, so the restored revision could refer to a chain that garbage collection reclaims. But every manifest that records a new floor has folded the restores validated before the advance (see [Advancing the retention floor](#101-advancing-the-retention-floor)), so the restore's own revision row, which refers to the chain, is in the compaction's basis. The `attributes` and `access` families follow the same rule as `revisions`.

The compaction's row digests cover the binding events of both binding indexes and verify that they agree, a manifest's row counts verify that each run's slot index has at least as many rows as its child index, and every event above the floor stays.

A binding row's position comes from the delta that published it, so a later bind doesn't depend on an earlier unbound value. Attribute and access revisions keep a cleared floor state, because the next update is validated against its revision number.

An empty attribute map, or an access row with no boundary and no grants, is kept when it's the state at the floor, because removing it could expose an older row and bring back cleared state. A base compaction doesn't remove the rows that a deletion hides until the deletion has expired, so a restore brings back the same state.

A rewrite MUST refuse an ambiguous file revision, attribute, or access history, in which two rows for one inode have the same revision number at or below the retention floor. It can't choose one row and discard the other.

A trash removal marker sorts before the matching listed entry, so a bottom-anchored compaction can remove the pair without leaving an older entry that would reappear in a later read.

Compaction MUST preserve the visible metadata at every retained sequence. It publishes a complete replacement through the numbered manifest protocol. Its input objects stay available until no protected manifest or snapshot references them, even after a successful publication.

### 10.4 Streaming compaction

A compaction rewrites the selected runs as a stream, without keeping every row in memory. It writes completed segments under the namespace's normal `segments/` prefix, with new IDs, and then publishes references to that output in the next manifest. Readers keep using the previous manifest until the publication succeeds.

A runtime claims the namespace's compactor epoch before its first compaction after it opens the namespace. Claiming publishes a manifest with `compactor_epoch + 1` and no other changes, and concurrent family groups in that runtime share the claim. Compaction publications MUST match the current epoch, so a newer claim fences older compactors. A compaction also checks the epoch of the manifest it plans from before it reads a segment.

Before each publication attempt, a compaction checks its elapsed monotonic time and finds the current manifest (see [Publishing a manifest](#72-publishing-a-manifest)). Its selected input segments MUST still be present and unchanged. While both conditions are true, a lost numbered put can be retried against a new manifest. A changed epoch produces `fenced`, and changed inputs produce `abandoned`.

A compaction runs under two deadlines:

| Deadline | Starts | Limit | Past the limit |
| --- | --- | --- | --- |
| Compaction deadline | Before the compaction reads its input | The compaction starts its last publication attempt within `METADATA_COMPACTION_BUDGET_MS`. | The compaction returns `abandoned`. |
| Publication deadline | When finalization starts | Every finalization attempt runs under one `METADATA_PUBLICATION_BUDGET_MS`. | The attempt fails with the metadata publication budget error. |

Segments that no root references are protected until their provider age is more than the grace window (24 hours). The compaction deadline plus the publication completion bound (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)) is shorter than that interval:

```text
compaction deadline + publication completion bound < grace window
```

With the current constants, the last attempt starts within 12 hours of the timer's start (before any output is written) and finishes within 12 hours and 25 minutes, before the earliest unreferenced output is old enough to delete. A cancelled or crashed compaction leaves output that's subject to the same age rule.

A restart begins a new plan from the current manifest, and there's no durable compaction cursor or output-protection record.

Why does core keep a compactor epoch? A compaction of a large window publishes once at the end, which can be hours later, and the epoch lets a newer runtime stop a stale compactor at its next check. (Grep publishes the steps of one maintenance interval together, so a lost race costs at most that interval's steps, and grep doesn't need an epoch.)

## 11. Garbage collection

This section describes how garbage collection determines which objects are still referenced, and when an unreferenced object is old enough to delete safely.

Garbage collection deletes objects that no retained view needs, after the required age and publication checks. A logical deletion on its own doesn't allow file bytes to be removed, because a [pool's](glossary.md#pool) content stays while a content root of any member refers to it, and a deleted member's tombstone is a content root until its retirement deadline (see [Content roots](#119-content-roots)). No retirement deletes content (see [Retirement](#95-retirement)).

An object can't be deleted as soon as nothing references it, because a publisher might be about to reference it. So garbage collection waits until an unreferenced object is older than the grace window, which is long enough for every publication planned against an older state to have finished or failed (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).

### 11.1 One complete collection

LoonFS runs two kinds of collection, and each one is a job of its own:

- A namespace's collection deletes the namespace's unreferenced manifests, data WAL objects, metadata and extension segments, and snapshots, and keeps its fences and floor objects. It retires the namespace when its tombstone is eligible (see [Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone)), and it publishes the tombstone of an initializing namespace whose manifest 1 is older than the grace window (see [Deleting a namespace](#94-deleting-a-namespace)). Its one request under `pools/` is the deletion of the namespace's [member entry](glossary.md#member-entry) when it retires the namespace.
- A [pool's](glossary.md#pool) collection deletes the pool's unreferenced content objects, upload records, and temporary objects, with roots gathered from every member (see [Content roots](#119-content-roots)).

A namespace's collection first establishes the roots. It:

1. Finds the namespace's newest manifest (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)).
2. Lists all snapshot keys, reads each snapshot record, and loads the manifests that the snapshots keep.
3. Records the protected manifests and segments, and the newest manifest's `folded_wal_no`.

If the namespace's current manifest is invalid or unreadable, the collection stops before it deletes anything. An absent namespace has nothing for core garbage collection to delete.

The collection reads no WAL object, and it doesn't check the manifest that step 1 found for a successor. It keeps every WAL object numbered above the newest manifest's `folded_wal_no` (see [Reference roots](#112-reference-roots)), so it needs that number and not the WAL objects. ([Replaying the visible WAL](#42-replaying-the-visible-wal) explains why the collection doesn't check for a successor.)

If step 1 finds that the namespace is [retired](#95-retirement), the namespace has no roots, and the collection reads no manifest, WAL object, or snapshot record. It makes one listing, of the namespace's prefix (`namespaces/{namespace_id}/`), deletes each manifest, data WAL object, metadata segment, extension segment, and snapshot record whose listed provider age meets the rule for its family in [Candidate and age rules](#113-candidate-and-age-rules), and keeps the boundary object, the fences, the floor objects, and every unrecognized key. It raises no boundary, deletes no fork snapshot, and returns an ordinary response that reports what it deleted, not `namespace_deleted`.

Each collection then lists each of its candidate families completely, from the beginning, and stores no durable progress, run, phase, reference table, or cursor. A pool's collection keeps exact sets of content roots only for the chains of the members' WAL tails and the content IDs of the upload sessions, and they share the collection's memory budget with the extent filters (see [Content roots](#119-content-roots)). Snapshot cleanup uses the complete snapshot listing that establishes a namespace's roots, and session cleanup uses the complete session listing that establishes the pool's roots.

A namespace's collection lists the namespace's manifests, chooses the manifests to delete under the rules in [Candidate and age rules](#113-candidate-and-age-rules), raises the [manifest boundary](glossary.md#manifest-boundary) to the highest chosen number, and then deletes exactly the chosen manifests. If the chosen manifests don't fit the memory budget, the collection works through them in number order, a batch at a time, and raises the boundary to a batch's highest number before it deletes that batch. To raise the boundary, the collection loads the boundary object, and writes nothing if the stored number is already that high. Otherwise it creates a missing object with put-if-absent, or writes the higher number with a compare-and-swap, and after a lost write it loads the boundary again and tries again. If a raise fails, the collection fails before it deletes another manifest. A collection that chooses no manifest sends no request for the boundary. The boundary isn't progress that a later collection continues from, because each collection chooses its manifests from its own listing. For example, suppose the boundary is 30, the newest manifest is 45, and snapshots keep manifests 7 and 38. If manifests 31 to 44 are older than the grace window, the collection chooses 31 to 37 and 39 to 44, raises the boundary to 44, and deletes those 13 manifests, so manifests 7 and 38 stay. [The manifest boundary](../design/manifest-boundary.md#cleaning-up-manifests) explains the reasons for this order.

Every age decision in a collection uses the same `now_ms`, captured when the collection starts. A later collection reads fresh roots and uses its own clock. Concurrent collectors can delete eligible objects independently, and an object that's already gone needs no more cleanup. A failed collection might have deleted some candidates already, but the next collection can safely start over.

### 11.2 Reference roots

| Evidence captured for the collection | Objects protected |
| --- | --- |
| The current active manifest | The manifest, and every segment it lists. |
| The current deleted manifest (the namespace tombstone), until the namespace is retired | The tombstone, and every segment it lists. |
| Every recognized snapshot key in the complete listing | The manifest numbered in its ID, and every segment in that manifest. |
| The number of the newest manifest that the collection found | Every manifest numbered above it, because other processes can publish manifests after the collection's find. (This root doesn't protect their runs.) |
| The current active manifest's folded boundary | Every WAL number above `folded_wal_no`. |

The segments that a manifest lists are the segments of its runs and the extension segments of its `extensions` entries (see [Extensions](#124-extensions)). A tombstone's entries list no segments.

The manifest number is part of the snapshot key, so the roots need no snapshot bodies. Bodies are normally read later, for owner and expiry decisions. If a snapshot's manifest is missing, the collector reads the snapshot and applies the owner and grace rules of snapshot cleanup, and an absent or collectable snapshot doesn't require the missing basis. This allows recovery from a creator's failed deletion of a snapshot or a concurrent snapshot removal. A retained snapshot that refers to a missing manifest is corruption, except a deleted namespace's own fork point snapshot (see [Snapshot cleanup](#117-snapshot-cleanup)), and invalid or unreadable manifests still fail the collection. Each listed snapshot whose manifest is present protects its files for the whole collection, even if that collection deletes the snapshot.

A manifest below the newest manifest that the collection found isn't a root unless a snapshot keeps it, however recently the newer manifest was published.

A retention floor that moves past the folded head of a manifest that a snapshot keeps doesn't remove the snapshot's protection, and reads through the snapshot use the runs of that manifest directly (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot) and [Reads, deletion, and expiry](#83-reads-deletion-and-expiry)).

The tombstone keeps its runs, and its segments stay until retirement, which doesn't read them (see [Retirement](#95-retirement)). A retired namespace has no roots. Its boundary is above every manifest number, so none of its manifests is current, and its snapshot records are candidates like its other objects.

Why isn't a replaced manifest a root? A reader or writer can still be using a manifest after a newer one replaces it, but protecting recently replaced manifests would make every collection list the manifests and load the recent ones. Instead, a read on a replaced manifest that finds a segment missing runs again from the current manifest (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), and a write planned on a replaced manifest that finds a segment missing is planned again on the current manifest, without an error to the caller. A read that must outlive the current manifest, such as a merge's read of its fork, takes a snapshot (see [Snapshots](#8-snapshots)).

### 11.3 Candidate and age rules

Being unreferenced makes an object a candidate for deletion, not deletable right away. Provider age uses the modification time that the listing reports, and a candidate listed without a time is kept. Let `T` be the grace window (`GC_GRACE_WINDOW_MS`, 24 hours), which is the same for every collection and MUST be at least `PUBLICATION_COMPLETION_BOUND_MS`.

| Family | Conditions for deletion |
| --- | --- |
| Namespace manifest | Below the newest manifest that the collection found, not a root (see [Reference roots](#112-reference-roots)), and a provider age of at least `T`. |
| Namespace manifest in a retired namespace | A provider age of at least `T` (the tombstone included). |
| Data WAL object in an active namespace | At or below `folded_wal_no`, and a provider age of at least `T`. |
| Data WAL object in a deleted or retired namespace | A provider age of at least `T` (no current WAL object is protected). |
| Fence or floor object | Never deleted, in any namespace. |
| Metadata or extension segment | No root lists it, and its provider age is strictly greater than `T`. |
| Snapshot record | The owner-specific rules in [Snapshot cleanup](#117-snapshot-cleanup). |
| Snapshot record in a retired namespace | A provider age of at least `T`, whatever its owner. |
| Upload session | The status-specific rules in [Upload-session cleanup](#116-upload-session-cleanup). |
| Content object under a pool's prefix | No extent filter of a layout segment in the members' layout views may contain its name, no tail or session root of the pool's members refers to its chain (see [Content roots](#119-content-roots)), and a provider age of at least `T`. |
| Temporary object | A provider age of at least `T`. |
| Member entry | The namespace that the entry's key identifies is retired, has no manifest, or records another pool (see [Content roots](#119-content-roots)). |

The manifest boundary is never deleted, and neither is the current manifest of a namespace that isn't retired. A retired namespace has no current manifest, so its tombstone is deleted under the age rule like its other manifests. Core garbage collection keeps unrecognized keys. For a candidate that's deleted by age, a missing provider timestamp, or one in the future, can't show that it's old enough.

For example, suppose a collector finds manifest 10 as the newest, and no snapshot keeps manifest 8 or 9. It keeps manifest 10 and every higher number, and the segments that manifest 10 or a snapshot's manifest lists. Manifests 8 and 9 are deleted once their provider age is at least `T`. A segment that only manifest 8 or 9 lists is a candidate even if manifest 10 was published a moment ago, but it's deleted only once it's strictly older than `T`.

A collection tells the kinds of WAL object apart from the listing alone, with no read. A listed WAL object whose size is at most `MAX_FENCE_OR_FLOOR_OBJECT_BYTES` is a fence or a floor object, and the collection keeps it. A larger one is a data WAL object. The writer pads a data WAL object that would otherwise be stored at or below that size (see [WAL records](#a5-wal-records)). For example, the listing of WAL objects 1 to 30 of `main`, all folded and old enough, shows floor object 12 and fence 25 at 300 bytes or less, and 28 objects of more than 572 bytes. The collection keeps 12 and 25 and deletes the other 28.

Why pad data WAL objects instead of reading the small candidates? A read would cost one GET for each small data WAL object that a collection deletes, and every collection would read every fence and floor object again, a cost that grows as long as the store exists.

### 11.4 Clock and operation assumptions

Garbage collection deletes an unreferenced object based on its age, `now_ms.saturating_sub(last_modified_at_ms)`, so safety depends on a bound on how much an age can be overstated. The collector's clock might be ahead of the provider's, timestamps might have limited precision, and a process might pause around a publication check.

The combined allowance (`GC_SAFETY_MARGIN_MS`) is 300,000 ms. It's one allowance for relative clock error, timestamp precision, and scheduling together, not five minutes for each participant. A provider clock that's ahead of the collector's delays garbage collection but doesn't make it unsafe. Ages based on recorded times (creation, expiry, or completion) use the corresponding bound between the collector and the host that recorded the time.

Publication budgets use monotonic elapsed time, on a clock that keeps running while the host sleeps. A forward step of the wall clock of one second or more also counts as elapsed time, so a process that was frozen in a way its own clock can't measure (such as a virtual machine restored from a snapshot) finds its budget passed once its wall clock is corrected. The "publication completion bound" (`PUBLICATION_COMPLETION_BOUND_MS`) is the metadata publication budget, plus the provider publication request bound, plus the combined allowance, and the grace window is at least that long. Compaction reserves the publication completion bound within the grace window. [Publication and collection timing](#c1-publication-and-collection-timing) records the exact calculations.

A client timeout doesn't show that a remote write had no effect (see [Timing assumptions](#33-timing-assumptions)). A WAL put whose outcome the provider didn't report returns `outcome_unknown`, and the caller reloads and plans from the current state.

Neither a WAL put nor a manifest put depends on how late the provider applies it. No process deletes a fence or a floor object, and a fence put, or a floor object put by a process without the writer epoch, checks its number against the newest manifest's `folded_wal_no` after the put (see [A summary of the log protocol](#26-a-summary-of-the-log-protocol), [Publication and group commit](#63-publication-and-group-commit), and [Writer ownership](#61-writer-ownership)). A manifest publisher reads the [manifest boundary](glossary.md#manifest-boundary) after its put, so a put at a number that garbage collection emptied counts as lost (see [Publishing a manifest](#72-publishing-a-manifest)).

A manifest publication, including an epoch claim, checks `METADATA_PUBLICATION_BUDGET_MS` before its put, and not after the put. A fork snapshot's put checks the same budget before the put (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)). That budget protects the segments that a fold or a compaction writes, because the manifest that lists them has to be put before they're older than the grace window, when garbage collection can delete them. That protection depends on one assumption that the implementation can't check: a provider applies a request, if it applies it at all, within `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` of when the client started the operation that sent it. This is the provider time that the grace window reserves, and it's an operational requirement on the provider.

Direct expiry checks don't add the grace window to the requested lifetime. A host whose clock is `E` milliseconds ahead can reject an expired upload or user snapshot up to `E` milliseconds earlier than the creating host would. The grace window protects concurrent publication, and it doesn't synchronize expiry decisions across hosts.

### 11.5 Publication during collection

LoonFS acknowledges a new snapshot from the current head only after it checks the snapshot's manifest again, after the snapshot write, which closes the race between garbage collection's read of the current manifest and its complete snapshot listing. A fork is protected by the user snapshot that it's built from, or by the fork snapshot written before that user snapshot is checked again (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)).

A process that has a manifest reads the manifest boundary after each probe for a successor and drops a manifest that's at or below the boundary, so a successor that garbage collection deleted doesn't look like the end of the sequence (see [Replaying the visible WAL](#42-replaying-the-visible-wal)). Metadata segments have no matching check (see [Reference roots](#112-reference-roots)).

New metadata and extension segments are protected by the grace window while a publisher writes and verifies them. Streaming compaction MUST start its publication before its budget expires, and every compaction checks its epoch and its selected inputs. These rules apply to output that isn't yet listed by a root that the collection captured earlier.

A failed read of a required root stops garbage collection. Each cleanup operation MUST keep the durable evidence it needs to retry after a failure.

### 11.6 Upload-session cleanup

Uploads are collected through their session records, which are stored under the pool's prefix, so the pool's collection covers the sessions of every member, and a namespace's collection covers none. A session's namespace is the one recorded in its session record. Retirement of that namespace, or its retirement deadline (see [Retirement](#95-retirement)), takes precedence over the session status rules. The pool's collection compares a deleted namespace's retirement deadline with its own clock, as it does for the tombstone's content root, and from the deadline on it applies the rules of a retired namespace to the namespace's sessions. A namespace whose boundary is retired, or that a complete walk didn't find, is retired, and a session whose namespace the collection can't decide is kept (see [Content roots](#119-content-roots)). [Content roots](#119-content-roots) defines when a pool's content is deleted.

| Session and namespace | Action |
| --- | --- |
| Open session | Keep it until its expiry plus `T`, then create its aborted outcome record, and clean up its content and provider transfer state. If the session already has an outcome record, keep the session. |
| Aborted session | Retry content and provider cleanup, and delete its records after its abort time plus `T`. |
| Completed session in an active namespace | Keep it until the completed-content grace passes, then delete its records. Its content follows [Content roots](#119-content-roots). |
| Session in a deleted namespace, before the namespace's retirement deadline | Keep it, and report the deadline. |
| Session in a deleted namespace, from the namespace's retirement deadline on | Follow the rules of a retired namespace, even while a snapshot keeps the namespace from retirement. |
| Completed session in a retired namespace | Delete its records (no extra completion grace is required). Its content follows [Content roots](#119-content-roots). |
| An outcome record without its session record | Delete it once its provider age is at least `T`. |

Garbage collection deletes a session's records in order: the session record first, and then the outcome record. If the outcome record were deleted first and the collection stopped, the session record would be left on its own and would read as an open session, so once the session's lease had passed, garbage collection would abort the session and delete content that a commit might reference. No request issues evidence from an outcome record without its session record, and that outcome record protects no content. It's left when a collection stops between the two deletes, or when a completion or abort creates it after the session's records were deleted (see [Upload sessions](#51-upload-sessions)), and a later collection deletes it without acting on the session's content.

Such an outcome record is deleted only once its provider age is at least `T`, because a listing isn't a snapshot. A listing can miss the session record of a session that was created and ended while the listing ran, and then show that session's new outcome record on its own, which is younger than `T`. Deleting that outcome record would make the session read as open, and a later collection would abort the session and delete content that a commit might reference.

An outcome key sorts directly after its session key (see [Object keys](#a8-object-keys)), so one listing of the uploads prefix shows which sessions have ended, without a read for each session.

Before completion, a session exclusively owns its random content ID, and it can't issue admission evidence. In active and retired namespaces, and in a deleted namespace from its retirement deadline on, cleanup first creates the aborted outcome record, and then removes the content and any provider transfer. It keeps an aborted session's records until its abort time plus `T`, so if a streamed write that passed its final check before the abort finishes, a later cleanup attempt can still remove the object that the write created. A failed cleanup leaves the records for another attempt. Open and aborted sessions still need provider cleanup after the namespace is retired, because provider upload state can exist outside object listings.

Cleanup runs in the pool's collection, so it derives every content key and provider cleanup target from the pool's ID and the content ID that the session record stores.

A completed session's record is a content root while it exists, including in the collection that deletes it, so a later collection deletes the content, once no other root refers to it (see [Content roots](#119-content-roots)).

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

With the current constants, that's eight days, against seven days, one hour, and twenty-five minutes.

Every token issue checks the original completion time, so a retained receipt can't extend its issuance window. In-process proofs expire at the end of the admission window, and publication checks proof expiry immediately before the numbered WAL put. After the full completed-content grace, an unpublished upload can't be committed for the first time. Published content stays while a content root refers to it.

Why do a deleted namespace's sessions follow the rules of a retired namespace from the retirement deadline, and not from retirement? A snapshot on a deleted namespace, such as a fork's fork snapshot, keeps the namespace from retirement while the snapshot exists, and waiting for it would keep an unpublished completed upload for the fork's lifetime. No commit can reference that upload, because admission evidence admits an upload only in a commit of the session's own namespace (see [Admission proofs](#55-admission-proofs)), and the deleted namespace's admitted commits are all published before its tombstone, which is published within `PUBLICATION_COMPLETION_BOUND_MS` (25 minutes) of `deleted_at_ms`, far inside the grace window (see [Deleting a namespace](#94-deleting-a-namespace)). A fork copies its source's manifest references, and a merge or a rebase admits references from the fork's read view (its current manifest and WAL tail), so none of them refers to an upload that no commit published (see [Cross-namespace copies and moves](#97-cross-namespace-copies-and-moves)).

### 11.7 Snapshot cleanup

User snapshots become collectable after their expiry plus `T`, after their deletion plus `T`, or after their creation plus `T` in a deleted namespace, whichever comes first. A user snapshot with no expiry stays in an active namespace until it's explicitly deleted. A retained user snapshot reports the earliest of those times in `next_reclamation_at_ms`. Snapshots are deleted directly, and snapshot IDs are never reused.

The source's collection keeps every fork snapshot, and it reads nothing under the target's prefix. The target's own collection keeps the target's fork point snapshot. After the target is deleted, it keeps that snapshot even when the snapshot's manifest is missing, without reporting corruption, because a call that finishes the fork can write the snapshot on a manifest that a delete and a collection removed during the call (see [Creating and verifying a snapshot](#82-creating-and-verifying-a-snapshot)). Nothing reads through a deleted namespace's fork point snapshot, and the tombstone lists the same runs. The target's retirement deletes both snapshots before it raises the target's boundary to retired (see [Retiring an eligible tombstone](#118-retiring-an-eligible-tombstone)).

A candidate user snapshot that fails verification is deleted by its creator. If that deletion fails, or the creator crashes first, garbage collection applies the user snapshot rules to it. A fork snapshot's creator never deletes the fork snapshot, on the source or on the fork (see [Forking a namespace](#92-forking-a-namespace)).

Why does the source's collection keep every fork snapshot? Every fork snapshot is written before its target's retirement starts, so that retirement is the one path that deletes it. The fork call checks `METADATA_PUBLICATION_BUDGET_MS` before it puts the fork snapshot, and after the call started it either found the target initializing or wrote the target's manifest 1. The target's tombstone is published after that, and the target is retired only once its tombstone is older than the grace window, which is at least the publication completion bound (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).

### 11.8 Retiring an eligible tombstone

After it sweeps its candidate families, a namespace's collection retires the namespace if its tombstone is eligible for retirement (see [Retirement](#95-retirement)). The collection:

1. If the tombstone has a fork basis, deletes the fork snapshot on the source and the namespace's own fork point snapshot that the fork basis refers to, and adds each one to `deleted_snapshots_by_owner.fork` only if the snapshot was present.
2. Deletes the namespace's [member entry](glossary.md#member-entry) (`pools/{pool_id}/members/{namespace_id}.json`, under the pool that the tombstone records).
3. Raises the namespace's [manifest boundary](glossary.md#manifest-boundary) to retired. If the boundary object is missing, the collection creates it with put-if-absent, and otherwise it writes the retired state with a compare-and-swap. After a lost write, it loads the boundary again and tries again. A boundary that's already retired needs no write.

If any step fails, the collection fails, and the tombstone stays the newest manifest. A later collection finds the same eligible tombstone and starts again at step 1. Deleting a missing object succeeds, and a fork snapshot that's already gone adds nothing to that count, so every step can run any number of times. Retained roots follow [Reference roots](#112-reference-roots), and no progress record or journal is stored.

Later collections of the namespace find the retired boundary and clean up its prefix as [One complete collection](#111-one-complete-collection) describes.

[File contents and ownership](#15-file-contents-and-ownership) defines where content is stored. A deleted namespace refuses requests for presigned upload URLs and upload completion. A runtime handles these requests with the namespace head that it has (its [writer position](glossary.md#writer-position) or its cached view), which it checks for a successor at least once every `READ_REVALIDATION_BOUND_MS` (20 minutes), so it can still grant one of these requests up to 20 minutes after another process deletes the namespace. The grace window before the retirement deadline (24 hours) is far longer than that bound and the lifetime of any issued presigned URL (one hour for a part URL) together.

### 11.9 Content roots

A [pool's](glossary.md#pool) content objects are stored under the pool's prefix, and any member can reference any of them, so the pool's collection protects them with roots gathered from every member. A content object under the pool's prefix is kept while the extent filter of a layout segment in a member's layout view may contain the object's name, or a member's tail or session root refers to its chain:

| Root | Content it protects |
| --- | --- |
| A member's layout view: its current manifest while the member is active, or deleted and before its retirement deadline, and every manifest that its snapshots keep while those snapshots exist (see [Reference roots](#112-reference-roots)) | The objects whose names the [extent filter](#extent-filter) of any of its `content_layouts` segments may contain, which include the objects of every extent in every layout row. |
| A member's unfolded WAL tail (a tombstone has none, because deleting a namespace folds its whole WAL) | Every chain that an `append_file_revision` delta refers to, and every extent in the layouts of the tail's row state. |
| An upload session record under the pool's prefix, whatever its status | The session's `content_id`. |

Garbage collection considers every other content object under the pool's content prefix (`pools/{pool_id}/content/`) for deletion once its provider age is at least `T`. It keeps a key that doesn't parse as a content object key. A namespace's collection lists no content, and the pool's collection collects the pool's content whether its members are active or deleted (see [Retirement](#95-retirement)).

Each namespace in the pool has a [member entry](glossary.md#member-entry) under the pool's prefix (`pools/{pool_id}/members/{namespace_id}.json`), which the namespace's create or fork call writes after manifest 1 (see [Creating a namespace](#91-creating-a-namespace)) and the namespace's retirement deletes (see [Retirement](#95-retirement)). The collection lists `pools/{pool_id}/members/` once, uses each entry's key, and doesn't read the entry's payload. For each listed namespace, it finds the newest manifest (see [Resolving the metadata basis](#41-resolving-the-metadata-basis)), and then:

- **Active or deleted, and recording this pool.** The namespace is a member, and the collection reads its roots. It lists the member's snapshots after it finds the member's newest manifest, and that listing also finds the manifests that the member's snapshots keep. It takes each snapshot's manifest number from the snapshot key and doesn't read the snapshot records.
- **Initializing.** The namespace has no roots.
- **Retired, no manifest, or a manifest that records another pool.** The namespace isn't a member, so it has no roots, and the collection deletes its member entry.
- **Unreadable.** The collection deletes no content objects in this pass, as described below.

A deleted member's tombstone is one of the member's layout views until the tombstone's retirement deadline (`deleted_at_ms + GC_GRACE_WINDOW_MS`), compared with the collection's `now_ms`, and from the deadline on its layout segments protect no content. The collection compares the deadline with its clock for the content roots, and leaves retirement to the member's own collection. A collection whose clock is behind keeps the tombstone's view as a root for longer, which keeps more content.

The collection reads each member once, and it MUST read the roots of every member within 12 hours (`POOL_ROOTS_READ_BUDGET_MS`), measured as monotonic time elapsed since the collection captured its `now_ms`. A collection that hasn't read every member's roots by then deletes no content objects and reports `content_deletion_pool_budget` as skipped work. If one listed namespace can't be read, the collection deletes no content objects and reports `content_deletion_unreadable_member` as skipped work. In both cases, the collection still lists the content prefix once to measure `listed_bytes`, the rest of the collection continues, and other pools are unaffected.

Why does a tombstone's own view stop being a root at the retirement deadline, and not at retirement? Nothing reads a tombstone's own view after the deletion, and a fork reads through its fork snapshot, whose manifest stays a root. If the tombstone's view stayed a root until retirement, a deleted namespace with a fork would keep the content that it wrote after the fork point, which no reader can reach, for the fork's lifetime.

Why does an initializing namespace have no roots? LoonFS refuses commits to it, so it has no WAL tail. A fork's manifest 1 lists only the source's segments, and its manifest 2 adds only the segments of its fold of the source's WAL tail. The snapshot that the fork is built from, or its fork snapshot, keeps the source manifest that the fork reads through as one of the source's roots, and an object that the namespace writes after the collection starts is younger than `T` for the collection.

A pool with no member entries has no roots, so the collection deletes the pool's content objects and temporary objects under the age rule, and deletes its upload records under the rules in [Upload-session cleanup](#116-upload-session-cleanup). A collection whose listings find no object under the pool's prefix returns `pool_not_found`, because a pool with no object under its prefix doesn't exist.

A pool's collection works in these steps:

1. Lists the pool's upload sessions, before it reads any member's WAL tail.
2. Lists the pool's member entries.
3. For each listed namespace, finds the newest manifest, and reads the member's roots or deletes the member entry, as described above.
4. Lists each layout segment of the members' layout views once, however many views list it. A fork's inherited segments are its source's, so the distinct segments are the ones that the members wrote.
5. Reads the extent filter of each listed segment, and charges the filters to the collection's memory budget.
6. Lists the pool's content prefix once. It protects an object when any extent filter may contain the object's name (its key with the content prefix removed), or when a tail or session root refers to its chain.
7. Deletes the pool's temporary objects that are older than `T`, and applies the rules in [Upload-session cleanup](#116-upload-session-cleanup) to the pool's session records.

The collection writes no object. Besides the pool's content objects, temporary objects, and upload records, it deletes only member entries.

The collection reads each segment's extent filter, and doesn't combine the segments of a view. A view's layout rows are the rows of its segments, so the listed segments' filters cover every extent of every view, including a superseded layout row (a shorter layout of a chain that a longer row replaces). An object that only a superseded row refers to stays until a compaction drops that row and no layout view lists the segment that contains it. A filter can also match the name of an object that no row of its segment refers to, and the collection keeps that object until no layout view lists the segment.

Tail and session IDs protect every object of their chains. A presigned download URL lasts `DIRECT_TRANSFER_URL_TTL_MS` (15 minutes), and the URL lifetime isn't an input to the grace window. A URL for a revision that's current in the view that issued it works for the URL's whole lifetime, because the current manifest refers to the revision for at least the grace window after a commit replaces it, which is longer than the revalidation bound, the URL lifetime, and the clock allowance together (see [Advancing the retention floor](#101-advancing-the-retention-floor)). A URL for a revision that a later commit replaced can stop working before it expires, once a base compaction removes the revision and a collection deletes its content.

Why does each layout segment store a filter of its extents instead of the collection reading the rows? A segment's extents are set when the segment is written, so the writer records them once, and a collection reads one small section for each distinct layout segment (which the segment cache keeps for the next collection) and one listing of the content prefix. A scan of the rows would read every layout row of every rooted view, and exact sets of extent keys would grow with the namespace. At one million files, the extent filters take about 2.5 MB (one million names at 20 bits each).

A tail revision delta can leave out its layout only when its commit includes a piece at offset 0, or a piece that continues a chain with an existing revision (see [WAL records](#a5-wal-records)). In the second case, retention keeps the base revision until the append is folded, and layout compaction keeps the chain's row.

A fork snapshot keeps the source manifest that its fork reads through as one of the source's layout views, which protects the objects of the inherited layouts. A fork's fork point snapshot keeps the fork's manifest at the fork point as one of the fork's layout views, so the content at the fork point stays for the fork's life. That includes the content of a revision that a later commit in the source's WAL tail replaced, because the fork's fold of the tail lists the tail's whole history above the source's floor.

The collection lists upload sessions before it reads any member's WAL tail. A session record is removed only once no commit can admit its content (see [Upload-session cleanup](#116-upload-session-cleanup)). A session that the collection doesn't list was either created after the listing, and its content is younger than `T`, or removed before it. Every commit that refers to the content of a removed session succeeded before the tail was read, so the tail or a protected manifest refers to that content.

Why does the pool's collection gather every member's roots, instead of each member collecting its own content? Every member can reference every object under the pool's prefix, so one member's roots can't show that an object it doesn't reference is garbage.

Why does the pool's collection delete a member entry? Retirement deletes a namespace's entry, but a slow create or fork call can write the entry after the namespace is retired. The namespace isn't a member, and without this rule the entry would stay under the pool's prefix after every member was retired.

Why does the collection have to read every member within 12 hours? A reference can move from one member to another while a collection runs. For example, a merge adds references from a fork to its target, and the collection might read the target before the merge's commit is applied and the fork after it. While a merge or a rebase plans, LoonFS takes a snapshot of the fork (see [Cross-namespace copies and moves](#97-cross-namespace-copies-and-moves)), which keeps the fork's manifest and its content until garbage collection deletes the snapshot's record. The snapshot expires `OPERATION_SNAPSHOT_TTL_MS` after the merge was prepared, and its record stays for the grace window after the earliest of its expiry, its deletion, and, in a deleted fork, its creation (see [Snapshot cleanup](#117-snapshot-cleanup)), all of which come after the merge was prepared. The plan's content proofs expire with the snapshot, and publication checks them immediately before the WAL put, so the merge's commit is applied within the snapshot's lifetime plus the provider's request bound after the merge was prepared. So a collection that read the target before the commit reads the fork while the snapshot's record still keeps the content, because the read cap, the clock allowance, the snapshot's lifetime, and the provider's request bound together are less than the grace window:

```text
POOL_ROOTS_READ_BUDGET_MS + GC_SAFETY_MARGIN_MS
    + OPERATION_SNAPSHOT_TTL_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
    < GC_GRACE_WINDOW_MS
```

The same inequality covers three more cases:

- **A fork deleted during a merge.** A fork that's deleted while a merge from it is in flight is retired no earlier than the grace window after its deletion, so the collection still finds the fork through its member entry, and the fork's tombstone and snapshots still refer to the content.
- **A replaced revision.** A revision that the fork replaces after the merge was prepared stays in the fork's history for at least the grace window after the replacing commit (see [Advancing the retention floor](#101-advancing-the-retention-floor)), so the collection reads the fork's current manifest with the revision still in it. A revision that the merge includes and that the fork replaced before the merge was prepared is in the manifest that the plan read and its snapshot keeps, when that manifest includes the revision's commit. When the revision's commit is in the fork's WAL tail instead, the limit on the tail's age (`MAX_TAIL_COMMIT_AGE_MS`, the grace window less the other terms of the inequality above) protects it, so a retention advance passes the replacing commit only after the collection has read the fork (see [Cross-namespace copies and moves](#97-cross-namespace-copies-and-moves)).
- **A fork created during a collection.** A fork whose member entry is written after the collection lists the member entries isn't read by that collection. A reference that the new fork receives from a merge or a replay is kept by the plan's snapshot of the fork that the reference came from, as above. A reference that the new fork starts with is in the source manifest that the fork's user snapshot keeps, or in the source's WAL tail that the fork folds. Each span object that the fork's fold writes is as young as the write, so the age rule protects it for the grace window. But a reference that the fork takes from the tail also refers to the source's existing objects, which can be older than the grace window, so the fork relies on the source for every reference that it takes from the tail. The fork folds the source's tail only while no commit in it is older than `MAX_TAIL_COMMIT_AGE_MS`, and becomes active within `OPERATION_SNAPSHOT_TTL_MS` of its read of the tail, the term that the inequality reserves for it, or never (see [Forking a namespace](#92-forking-a-namespace)). A retention advance of the source passes a commit only once it's at least the grace window old, so by the inequality above, each of those references is still in a root of the source when the collection reads the source.

  The collection loads a member's current manifest and WAL tail before it lists the member's snapshots. If the source's snapshot listing includes the fork's user snapshot, that snapshot's manifest is one of the source's roots, even if the snapshot has expired, because a snapshot's record stays for the grace window after the snapshot ends (see [Snapshot cleanup](#117-snapshot-cleanup)). If the listing doesn't include it, the snapshot was created after the listing, on the manifest that the collection read or on a newer one. Either way, the reference was in a root of the source that the collection read, or the source gained the reference after that read. A member gains a reference to content that it didn't already refer to in three ways: a commit of newly written or uploaded content, which the age rule and the upload session records protect, a merge or a replay, which the paragraph above covers, and its own creation as a fork, which this case covers.

## 12. Encodings, versions, and extensions

This section describes how durable objects are encoded and versioned, and how extensions store their own state.

Storage versions describe how durable objects are interpreted and operated on, and API versions describe request and response contracts. A change to one isn't automatically a change to the other.

### 12.1 Field conventions

Durable field names and enum values use `snake_case`, and tagged unions use `kind`. A `status` field describes a resource's lifecycle and a `phase` field describes the progress of a computation, and when the values of either are tagged objects, they use `kind`.

Field name suffixes have these meanings:

| Suffix | Meaning |
| --- | --- |
| `_id` | An identifier. |
| `_seq` | A position in the namespace's commit history. |
| `_no` | A counter scoped to its resource. |
| `_index` | A zero-based position in a collection. |
| `_number` | A one-based position defined by a provider or tool. |

An absent optional object field is left out when it's written, and a required value is written even when it's zero, false, or an empty collection. Fingerprint preimages aren't ordinary durable records, so they have their own absence rules (see [Appendix B](#appendix-b-semantic-commit-fingerprints)).

A CBOR record stores an inode ID's 16 bytes, most significant byte first, as a 16-byte byte string, and a JSON document stores the ID's 26-character spelling (see [Numbered objects and generated IDs](#23-numbered-objects-and-generated-ids)). Neither form includes the `ino_` prefix of the public API string. The preimage of a commit fingerprint uses the JSON form (see [Appendix B](#appendix-b-semantic-commit-fingerprints)).

### 12.2 Envelopes and checksums

Structured control records, manifests, and WAL objects use an envelope with `kind`, `format_version`, `payload_checksum`, and `payload`. Content objects are raw file bytes, and block segments use the sectioned encoding in [Block-segment encoding](#a7-block-segment-encoding) instead of an envelope.

[Envelope layout](#a2-envelope-layout) defines how an envelope contains its payload, which families are compressed with zstd, and which bytes the checksum covers (the payload bytes exactly as they appear in the document, not a decoded object serialized again).

Decoders identify the kind and the supported version before they interpret the payload, and they verify the exact payload checksum before they decode its fields. Unknown kinds, unsupported versions, malformed payloads, and checksum failures are errors, and none of them is a reason to use another object instead.

Envelope and manifest-reference digests are encoded as `sha256:<64 lowercase hex>`. A content, object, or part checksum uses `{ "algorithm": ..., "value": ... }`, because the store selects its algorithm, and a block handle has a numeric `crc32c` field, because the block format always uses CRC32C. Commit fingerprints also identify their canonicalization scheme.

Per-block CRC32C checksums verify ranged block reads against their handles, a segment descriptor's checksum verifies a segment that a reader fetches whole, and envelope digests verify complete payloads. Neither an unauthenticated checksum nor an object name is an authorization mechanism.

### 12.3 Strict decoding and evolution

Authoritative durable envelopes and their nested payloads reject unknown fields. That includes immutable objects, because folds and compaction encode their contents again, and a successor that dropped an accepted unknown field would lose durable meaning.

`ContentRef` and `Checksum` are closed shapes wherever they appear. Each content kind and each checksum algorithm is its own closed shape, so a new kind or algorithm is a new shape. Unknown content kinds and checksum algorithms are rejected, and a new content kind requires a supported version change for every durable family that contains the reference.

API request bodies also reject unknown fields, including nested ones, so a misspelled precondition can't quietly become a request without that precondition. Response bodies generally tolerate additions, except for shared closed shapes. The API specification defines those transport rules.

Versions work like this:

- The owning envelope's `format_version` governs its whole payload, including nested objects and collection semantics, and a payload doesn't add a format-version field of its own.
- A metadata segment is a separate object. Its descriptor's `encoding` is the segment's own version, covering both the block layout ([Block-segment encoding](#a7-block-segment-encoding)) and the row-kind schema ([Metadata rows and row keys](#a6-metadata-rows-and-row-keys)). One manifest can list segments of different encodings, and compaction migrates the rows it rewrites to the encoding of the segments it writes. The encoding isn't a version of the manifest payload (the manifest's version governs the descriptor itself).
- A `commit` row contains the deltas of its WAL commit record, so the `commits` family's encoding changes whenever the WAL delta vocabulary changes.
- An extension's segments are interpreted under the `version` of the extension entry that lists them (see [Extensions](#124-extensions)).
- A name such as `blob_v1` identifies a closed content strategy, and it isn't permission to ignore the owning family's version.

After the stable format is released, a change to a field's name, presence, type, tag, encoding, or governed meaning, including adding a field, requires a new version of the owning family. Durable envelopes reject unknown fields, so an older binary can't read the new object anyway, but the new version lets it refuse the object at its version check and tells a newer binary which shape to decode. Until the stable format is released, a change keeps the family's version, so a newer binary can reject objects that an older binary wrote. (For example, a manifest without the segment descriptors' `checksum` field doesn't decode.) A change to the garbage collection protocol can require a version gate even when most stored fields stay the same. An implementation MUST NOT operate on a newer protocol just because it can deserialize some of its fields. This rule applies to every role that changes durable state: publishing a successor manifest or WAL object, compacting, creating or changing snapshots, advancing retention, garbage collection, and maintaining an extension. Each of those roles loads the current manifest before it publishes, or is fenced by the epoch that a newer binary claims on its first publication, and it rejects an unsupported version on load. So a process that was running when a newer version was published stops changing durable state at its next publication attempt, and a collection stops at its next start.

A reader supports a set of versions of each family and a set of metadata segment encodings. It rejects an envelope whose version isn't in its set before it verifies the checksum or decodes the payload (see [Envelopes and checksums](#122-envelopes-and-checksums)), and a manifest that lists a segment encoding outside its set before it opens any segment. Either error reports the version that the reader found. After the stable format is released, a reader's set contains every version from the first stable format release up to the newest version that the reader's release writes. Snapshots and fork bases capture manifests by checksum and keep their segments for as long as any snapshot or fork exists, so objects written by any earlier release can stay in use. A writer writes the lowest version that can express the object, so an upgraded binary writes objects that an older binary can read until an object needs a newer version. Until the stable format is released, every family is at version 1, metadata segments use encoding 1, and a reader supports exactly those versions.

Golden fixtures pin the reference encodings in `crates/loonfs-types/tests/golden_formats.rs` and the grep fixtures, and fingerprint vectors also pin canonical JSON bytes and digests. Validating a new release means checking that it preserves the meaning of retained data. The [compatibility table](../../RELEASING.md#format-compatibility) in RELEASING.md lists the versions that each release writes and reads.

[Family versions](#a1-family-versions) lists the family versions. A binary rollback is valid only if the older binary supports every stored family version and its protocol, which until the stable format is released means only a binary of the release that wrote the stored objects, because a change keeps the version. Downgrading the binary doesn't convert stored data.

### 12.4 Extensions

An "extension" is an optional subsystem that keeps state derived from a namespace's committed metadata and content, such as the grep index. Its state needs what the core manifest already provides for metadata runs (publishing a new version, keeping the version that a snapshot or a fork refers to, and collecting unlisted objects), so an extension lists its state in the core manifest instead of keeping manifests of its own.

The manifest's `extensions` member maps each extension's name to the extension's "entry". A name is 1 to 64 lowercase ASCII letters, digits, or underscores, and it starts with a letter (for example, `grep`). An entry is a JSON object, and core reads two of its members:

- `version` is the version of the extension's format, which governs the entry's other members and the rows of the extension's segments.
- `runs` lists the extension's runs, each with its run number, its level, and its segment descriptors (see [Control and manifest payloads](#a4-control-and-manifest-payloads)). The extension assigns each run's number and level, and core doesn't interpret either one.

Core copies the entry's other members unchanged without interpreting them, and the extension's specification defines them. An extension's segments are stored under the prefix of the namespace that wrote them:

```text
namespaces/{owner_namespace_id}/extensions/{extension_name}/segments/{segment_id}.sst
```

Core handles an entry like this:

- **Publication.** Every publication copies the `extensions` map forward unchanged, except a namespace tombstone, which copies each entry with its `runs` emptied. An extension publishes a change to its entry in a manifest that changes nothing else, as a maintainer publication under the current compactor epoch, without a claim (see [Publishing a manifest](#72-publishing-a-manifest)). If another publication puts a manifest at the next number first, and that manifest's entry for the extension is the one that the change built on, the change is rebuilt over that manifest. Two manifests without an entry for the extension count as having the same entry. If the entry changed, the change publishes nothing, and the extension plans its next change from the current manifest.
- **Validation.** Core checks that an entry's segment IDs are unique, and that each segment descriptor has rows, a key range, and the block layout of a metadata segment descriptor. Core never reads an extension's segments.
- **Collection.** Garbage collection deletes extension segments under the segment rule in [Candidate and age rules](#113-candidate-and-age-rules), and it keeps every other key under an extension's prefix.

A fork's first manifest copies the `extensions` map of the fork snapshot's manifest without reading an entry. Each inherited segment keeps its owner, and the fork snapshot protects it until the fork is retired, as the fork snapshot protects an inherited metadata segment (see [Forking a namespace](#92-forking-a-namespace)). The extension's specification defines what an inherited entry means in the fork.

An extension MUST stay rebuildable from authoritative core state, and its absence can't make the core namespace unreadable. A core reader doesn't read an extension's segments, so it doesn't need to support the extension's version. [Appendix D](#appendix-d-grep-extension-format) specifies the grep extension, including its entry, tokenizer, and postings.

Why is a change that lost its put rebuilt over the winning manifest only when the entry is the one it built on? A fold, a compaction, or a claim copies the entry unchanged, so the change is still valid over the winner. Core's own publishers rebuild over a winner in the same way when their work is still valid. If the entry changed, another process planned from a different state, and rebuilding over it would publish a state that the extension didn't plan.

Why does a tombstone empty an entry's runs? A deleted namespace has no reads that need the extension's state, so the extension's segments become unlisted and later collections delete them once they're more than 24 hours old. The other members stay as a record of the extension's last state, and since the extension doesn't read a tombstone's entry, they needn't stay valid together without the runs. A snapshot on an earlier manifest keeps the segments that its manifest lists.

### 12.5 Reserved functionality

Version 1 defines the inode kinds `file` and `directory`. The kind `link` is reserved, not defined. A link would be an inode with exactly one parent binding and an immutable target inode ID, and path resolution would use the target in place of the link. Version 1 has no row, delta, or operation for links.

In version 1, an inode never has more than one parent binding. In any version of the format, a directory never has more than one parent.

This version doesn't define mount creation or traversal, and no standard operation creates a mount.

A namespace whose names are labels rather than keys is reserved. Siblings in it could share a name, and path routes would return `not_supported`. This version defines neither that namespace nor any field that would select it.

## 13. Alternatives considered

This section describes designs that were considered for the format and why they weren't used. The design notes cover narrower choices: [commit history](../design/commit-history.md#alternatives-considered), [inline small content](../design/inline-small-content.md#alternatives-considered), [directory bindings](../design/directory-bindings.md#costs-and-alternatives), [family backfill](../design/family-backfill.md#alternatives-considered), [pools](../design/pools.md#alternatives-considered), [merging a fork](../design/merge.md#alternatives-considered), [grep across a pool](../design/grep-across-a-pool.md#alternatives-considered), and [object checksums in references](../design/object-checksums.md#alternatives-considered).

- **A writer lease.** A lease with an expiry time instead of an epoch would make a new writer wait for the old lease to expire, while epochs and fences let a new writer take over at any time (see [Writer ownership](#61-writer-ownership)).
- **One upload session record whose status changes.** A single record that moves from open to completed or aborted by compare-and-swap would be written twice per upload, often less than a second apart, and some providers allow about one write per second to one object (see [Upload sessions](#51-upload-sessions)). With two records that are each created once, completion and abort still race on one put-if-absent, at the cost of a second key for each ended session in the uploads listing.
- **A per-namespace naming mode.** A namespace could choose at creation whether names that differ only in case are the same name. But the choice comes before anyone knows which filesystems the tree will be copied to, it can't change later, a fork inherits it, and siblings that differ only in case can't be stored on the case-insensitive filesystems most trees end up on. One rule for every namespace avoids the choice (see [Names and paths](#14-names-and-paths)).
- **An expire event in the writer's log.** The writer could commit an `expire` event for each deletion once the floor reaches it, but that costs one event per deletion and a scan loop in the writer, and a namespace that nobody writes to reclaims nothing. A [floor object](glossary.md#floor-object) in the WAL orders the advance against every restore, so one object per advance expires every deletion that the floor passed (see [Advancing the retention floor](#101-advancing-the-retention-floor) and the [retention window](../design/retention-window.md#alternatives-considered) note).
- **Content stored under the namespace that writes it.** If each namespace stored its uploads and folded pieces under its own prefix, with a fork reading its source's objects through the fork snapshot, another member of the pool could use the content only by copying the bytes under a new content ID, or through a snapshot from the reader onto the writer for as long as the reference exists (see the [pools](../design/pools.md#alternatives-considered) note). With one prefix for each pool, a reference identifies the same object in every member, and one collection sees every root (see [File contents and ownership](#15-file-contents-and-ownership)).
- **An owner namespace on every content reference.** A field that recorded the prefix storing a reference's bytes would repeat, in every reference and extent, the pool that the reader's manifest already records. It would also make a reference into another pool's prefix representable, and such a reference isn't safe, because that pool's collection doesn't read the reader's roots (see the [pools](../design/pools.md#alternatives-considered) note).
- **Any member extends a chain in place.** Two members that appended to one chain from the same length would describe one content ID as two byte sequences, so after a merge a reader could read the other member's bytes. Recording each chain's creator, and starting a new chain for every other member's append, avoids that at the cost of a content ID and a layout row (see [Operations and WAL deltas](#66-operations-and-wal-deltas)).
- **A durable record of used WAL numbers.** Such a record would keep a deleted number from being mistaken for a free one. Kept fences and floor objects already do that, because a put collides with the object at any number that another process could have taken since the writer saw the tip, so a separate record would add a read to every commit acknowledgement for nothing. For manifests, the [manifest boundary](glossary.md#manifest-boundary) is such a record.

## Appendix A. Durable records and byte encodings

This appendix lists the fields and byte encodings of durable records. Field names are literal, and a `?` after a field in a table marks an optional member that's left out when absent (the `?` isn't part of the stored name).

### A.1 Family versions

There are five kinds of control object: `snapshot`, `upload_session`, `upload_outcome`, `pool_member`, and `boundary`.

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

An envelope contains these fields:

| Field | Representation |
| --- | --- |
| `kind` | The family's discriminator string. |
| `format_version` | The family's version, as an unsigned integer. |
| `payload_checksum` | `sha256:` followed by 64 lowercase hexadecimal characters. |
| `payload` | A raw JSON sub-document for JSON families, or a CBOR byte string containing the encoded payload for the WAL. |
| `padding` | WAL only, and optional: a CBOR byte string that keeps a data WAL object above the size of a fence or a floor object (see [WAL records](#a5-wal-records)). |

To encode an object, a writer:

1. Serializes the payload, to JSON for a JSON family or to CBOR for the WAL.
2. Calculates `payload_checksum`, the SHA-256 of those payload bytes.
3. Builds the envelope document around the payload bytes, as an inline fragment in a JSON document or a byte string in a CBOR document.
4. Compresses the whole document with zstd, if the object is a manifest or a WAL object. A [control object](glossary.md#control-object) is stored as the document itself.

The checksum covers the payload bytes exactly as they appear in the document (see [Envelopes and checksums](#122-envelopes-and-checksums)), including a JSON fragment's internal whitespace and spelling, and not the rest of the envelope or the compressed bytes. Neither a writer nor a reader parses and serializes the payload again to calculate the checksum.

A reader decompresses every manifest and WAL object without checking whether it's compressed, and it refuses a document longer than the family's limit: `MAX_NAMESPACE_MANIFEST_BYTES` for a manifest (see [Control and manifest payloads](#a4-control-and-manifest-payloads)) or `MAX_WAL_OBJECT_BYTES` for a WAL object (see [WAL records](#a5-wal-records)). (The reference writer uses zstd level 3, and a reader accepts any level.) The reader then identifies the declared kind and version, validates the envelope and checksum, and interprets the payload under the supported schema. A matching checksum doesn't make an inconsistent record valid, so field and protocol validation still apply.

Why are manifests and WAL objects compressed, but control objects aren't? A manifest grows with the namespace and a WAL object with its commits, and a reader with an empty cache loads both (a manifest of 193 KiB of JSON is 36 KiB compressed). A control object is under 1 KiB, so compressing it would save almost nothing.

### A.3 Content references and checksums

A `blob_v1` reference contains all of these fields:

| Field | Meaning |
| --- | --- |
| `kind` | `blob_v1`. |
| `content_id` | `con_` followed by 26 lowercase Crockford base 32 characters that spell a random version 4 UUID. |
| `size_bytes` | The length of the file, which is the prefix of the chain that the reference identifies. |
| `checksum` | The algorithm and digest of those bytes. |

For example, this is a reference to the 15 UTF-8 bytes `Hello, LoonFS!\n` (ending in a single LF). The ID is illustrative, and the size and CRC-64/NVME checksum are calculated from those bytes:

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

A checksum is `{ "algorithm": <name>, "value": <lowercase hex> }`:

| Algorithm | Value width |
| --- | --- |
| `crc64nvme` | 16 hexadecimal characters |
| `crc32c` | 8 hexadecimal characters |

The value is the CRC result written as a big-endian integer, so the hex is the checksum bytes in wire order. Provider encodings are converted to this representation before the stored value is built, and an algorithm selector without a value isn't a checksum.

A content reference's checksum covers the prefix it identifies, and a multipart part checksum covers one part. An upload-content claim, an extent's object checksum, and a segment descriptor's checksum each cover the complete object.

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

Namespace status is `{"kind":"initializing"}`, `{"kind":"active"}`, or `{"kind":"deleted"}` with a required `deleted_at_ms`. A missing status is invalid. Every manifest 1 is initializing (see [Creating a namespace](#91-creating-a-namespace) and [Forking a namespace](#92-forking-a-namespace)). The genesis commit ID is `cmt_00000000000000000000000000`.

Snapshot owners have the fields in [Records and owners](#81-records-and-owners), and a snapshot has no status field. The upload records use these variants, each tagged with `kind`:

- Mode, in the session record: `service_proxied` has no other fields, `direct_put` contains `checksum_algorithm`, and `direct_multipart` contains `provider_upload_id`, `part_size_bytes`, and `checksum_algorithm`.
- Outcome, in the outcome record: `completed` with `completed_at_ms` and `content_ref`, or `aborted` with `aborted_at_ms`.

Both upload records are stored under the prefix of the pool of the namespace that created the session (see [Object keys](#a8-object-keys)), and their `namespace_id` is that namespace.

A manifest boundary is stored under the prefix of its namespace, and its `namespace_id` MUST match the key. Its `boundary` is one of two tagged variants:

- `manifest`, with a required `manifest_no` of at least 1, as in `{"kind":"manifest","manifest_no":44}`. Every manifest numbered `manifest_no` or lower is dead.
- `retired`, with no other fields, as in `{"kind":"retired"}`. The namespace is [retired](#95-retirement), so every manifest of it is dead, and the boundary never changes again.

A missing boundary object means that no manifest of the namespace is at or below the boundary.

A member entry records that one namespace is in one pool. It's stored under its pool's prefix at `members/{namespace_id}.json`, and both its `pool_id` and its `namespace_id` MUST match the key. [Creating a namespace](#91-creating-a-namespace) and [Content roots](#119-content-roots) describe when it's written and how the pool's collection uses it.

A namespace manifest contains:

| Field | Meaning |
| --- | --- |
| `namespace_id` | The namespace that the manifest describes. |
| `created_at_ms` | The namespace's creation time. |
| `created_by` | The application-supplied actor that created or forked the namespace. |
| `retention_window_ms?` | The retention window, a number of milliseconds that's at least the grace window (see [Advancing the retention floor](#101-advancing-the-retention-floor)). Without a window, a maintainer doesn't advance the namespace's floor on a schedule. A successor can change the window. |
| `fork_basis?` | The source reference, the fork point (`fork_point_seq`), the ID of the fork snapshot on the source, the ID of the user snapshot that the fork was built from (`forked_from_snapshot_id`), and the fork point snapshot's ID (`fork_point_snapshot_id`), which can't change after creation. A fork that a rebase created also records `rebased_from` there: the fork whose commits the rebase replays into this namespace, and the fork sequence that the first replay commit starts after (see [Merges](#merges)). |
| `pool_id` | The [pool](glossary.md#pool) whose prefix stores the namespace's content objects and upload records (see [File contents and ownership](#15-file-contents-and-ownership)). A plain create records a pool ID that the create call generates, and a fork copies its source's value. The value can't change after creation. |
| `status` | Whether the namespace is initializing, active, or deleted. |
| `writer?` | The diagnostic writer block. |
| `folded_wal_no` | The highest local WAL number included in the manifest's runs. |
| `manifest_no` | A positive number that matches the object key. |
| `compactor_epoch` | The current compaction authority. |
| `head_seq` | The folded head sequence (on deletion, the final namespace sequence). |
| `activity` | The required cumulative activity counters (see [Statistics](#74-statistics)). |
| `writer_epoch` | The current writer authority. |
| `next_run_no` | The next run number to allocate. |
| `retention_floor_seq` | The earliest sequence covered by incremental replay and row retention. A fold sets the field to the higher of the predecessor's floor and the floors of the floor objects that the fold covers (see [Publishing a manifest](#72-publishing-a-manifest)). |
| `runs` | The complete list of the manifest's metadata runs. |
| `extensions?` | Each [extension's](#124-extensions) entry, under the extension's name, left out when the manifest has none. |

A run contains `run_no`, `run_seq`, `tier`, and `segments`, and its tier is `delta` or `base`. A segment descriptor contains:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | The namespace that stores the segment. |
| `segment_id` | The segment's immutable generated identity. |
| `checksum` | The full-object checksum that the store keeps for the segment object, in the store's algorithm (see [Content references and checksums](#a3-content-references-and-checksums)). |
| `family` | The metadata row family. |
| `encoding` | A required unsigned integer that is the segment's version, which covers its block layout and its rows. The value is 1. |
| `row_count` | A positive number of stored rows. |
| `min_row_key`, `max_row_key` | The inclusive key range. |
| `index_block`, `filter_block` | The index and filter handles. |
| `filter_inline?` | The exact stored filter bytes, as lowercase hexadecimal. |
| `extent_filter_block?` | The extent filter handle, present exactly when `family` is `content_layouts` (see [Block-segment encoding](#a7-block-segment-encoding)). |

An [extension's entry](#124-extensions) lists its runs in order in `runs`. Each run contains `run_no` and `level`, which only the extension interprets, and `segments`, its segment descriptors in list order. An extension segment descriptor contains:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | The namespace that stores the segment. |
| `segment_id` | The segment's immutable generated identity, with the `idx_` prefix. |
| `row_count` | A positive number of stored rows. |
| `min_row_key`, `max_row_key` | The inclusive key range. |
| `index_block`, `filter_block` | The index and filter handles. |
| `filter_inline?` | The exact stored filter bytes, as lowercase hexadecimal. |

`base_seq` is the oldest non-empty run's sequence, or `head_seq` when there's no such run, and it isn't stored. Extension runs don't count toward it. Segments follow their list order within each family. The owner and segment ID determine the object key, and the descriptor stores no separate path or compaction-job identity.

`MAX_NAMESPACE_MANIFEST_BYTES` is 512 MiB (536,870,912 bytes) for the complete decompressed manifest document, including its envelope. Readers refuse larger documents, and a publication whose manifest document would be larger fails before its put. This is a format constraint, because every published manifest MUST stay readable with bounded decompression. Because a manifest lists every segment, the limit also caps a namespace's metadata. (A namespace of one million files written in large commits has about 700 segments and a manifest document of about 440 KiB, so the limit is reached at about 1.2 billion files.)

### A.5 WAL records

`MAX_WAL_OBJECT_BYTES` is 512 MiB (536,870,912 bytes) for the complete decompressed WAL document, including its envelope. Readers refuse larger documents, and writers keep every WAL object within the limit through request and batch admission. A writer builds each batch so that the sum of its requests' bounds, plus the document overhead, stays within it. This is a format constraint, because every successful publication MUST stay readable with bounded decompression.

A WAL object's payload contains `namespace_id`, `wal_no`, `writer_epoch`, `head_seq`, `records`, and an optional `retention_floor_seq`.

For a data WAL object, `prior_head_seq` is derived (with checked subtraction) from the first record's sequence, and `records` covers the sequences after it through `head_seq`, without gaps. A first sequence of 0 is invalid. The WAL number MUST match the key. A fence has an empty record list and an unchanged head, so its derived `prior_head_seq` equals `head_seq`. Fences take part in WAL numbering and epoch validation, but they produce no logical changes.

A [floor object](glossary.md#floor-object) is a WAL object whose payload includes `retention_floor_seq`, the retention floor that it records (see [Advancing the retention floor](#101-advancing-the-retention-floor)), and every other WAL object leaves the field out. Like a fence, a floor object has an unchanged head and takes part in WAL numbering and epoch validation. Encoding and decoding refuse a WAL object whose `retention_floor_seq` is present with a non-empty record list, or is above the object's `head_seq` (`InvalidWalFloorObject`). For example, WAL object 31 with `head_seq` 140, no records, and `retention_floor_seq` 20 is a valid floor object, and the same object with `retention_floor_seq` 150 is refused.

`MAX_FENCE_OR_FLOOR_OBJECT_BYTES` is 572 bytes, which is zstd's compression bound for the longest document of a WAL object with no records (508 bytes: the envelope fields and a payload with a `namespace_id` of 128 bytes, four integers of 9 bytes, an empty record list, and a `retention_floor_seq`). So every fence and floor object is stored in at most 572 bytes, and a writer stores every data WAL object in more than 572 bytes. When a data WAL object's stored size would be at most 572 bytes, the writer adds a `padding` field to the envelope document and compresses again until it's larger. The padding is successive SHA-256 digests of the `payload_checksum` string followed by a 4-byte big-endian counter from 0, cut to the length that's needed, so it doesn't compress and the same payload encodes to the same bytes. The field is left out when the object is larger without it, and never added to a fence or a floor object. `payload_checksum` doesn't cover the padding, and a reader ignores its content. Garbage collection tells a fence or a floor object from a data WAL object by this bound alone (see [Candidate and age rules](#113-candidate-and-age-rules)).

Each commit contains `committed_seq`, `commit_id`, `committed_by`, `semantic_commit_fingerprint`, `committed_at_ms`, an optional `message`, an optional `merge`, `deltas`, and an optional `inline_content`. A delta wrapper contains `semantic_operation_index` and `delta`, which is a kind-tagged object with these fields:

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

A delta's own commit sequence is implied by its commit. An unbind target and a tombstone target are `{seq, delta_index}`, the position of the exact event that the delta removes or revokes. A bind's `revision` is the child's revision summary, `{revision_no, size_bytes, revision_committed_by, revision_committed_at_ms}`. It's present exactly when `child_kind` is `file`, and encoding and decoding refuse a file bind without it and a directory bind with it (`InvalidWalBind`). A revision delta's `parent_inode_id`, `name_key`, `display_name`, `child_created_by`, `child_created_at_ms`, and `bind_position` describe the file's current binding, which replay copies into the revision's summary row (see [Directory bindings](#13-directory-bindings)). A tombstone delta's `root_kind` is the kind of the deleted root inode, and a deleted binding is `{parent_inode_id, name_key, display_name}`. Attribute and access deltas contain the complete resulting state, including an empty map after a clear. A revision delta's `append_offset` is present only when an append produced the revision, and it's the base revision's `size_bytes` (see [Operations and WAL deltas](#66-operations-and-wal-deltas)), so it's less than the delta's own `content_ref.size_bytes`.

A revision delta's layout records the [creator](#15-file-contents-and-ownership) of its chain. That's the committing namespace for an upload or a new chain. A copy or a restore includes the chain's layout from the planning view, so its creator can be another member of the namespace's [pool](glossary.md#pool). Replay copies the delta's layout, creator included, into the layout row that it writes (see [Metadata rows and row keys](#a6-metadata-rows-and-row-keys)).

A revision delta's `layout` follows these rules:

- It can be left out only when the commit includes a piece at offset 0 for its content ID, or a piece for a chain that already has a revision in the planning view. The second case is safe, because the base stays the file's newest revision until the append is folded, so retention keeps a revision that refers to the chain and compaction can't prune its layout row.
- A commit with a piece at offset 0 can't include a layout.
- A present layout validates against its own `size_bytes()`, and its extents can refer to other content IDs. Its size equals the lowest offset of the commit's pieces for that content ID, or the reference's `size_bytes` if the commit includes no piece for it. A failed check is `InvalidWalRevisionLayout`.
- Upload evidence provides one whole extent. Copies, restores, and new chains include the required prefix from the planning view, and validation first writes any pending pieces that the prefix needs to content objects (see [Operations and WAL deltas](#66-operations-and-wal-deltas)), so the layout still refers to those objects if compaction later removes the source chain's row.

A [merge's](glossary.md#merge) commit includes a merge record (`merge`), and every other commit leaves the field out. The merge record contains these fields:

| Field | Meaning |
| --- | --- |
| `source_namespace_id` | The fork whose commits the merge applied. |
| `source_after_seq` | The fork's commits above this sequence were applied. For a fork's first merge, this is the fork point. |
| `source_through_seq` | The fork's commits at or below this sequence were applied. |
| `conflicts` | The changes that a [rebase's](glossary.md#rebase) replay commit left out, as its conflict report, in the API's `MergeConflict` shape. It's left out when it's empty, and it defaults to an empty list when absent. |

A replay commit stores its conflict report in `conflicts`, so a retry with the replay commit's commit ID returns the same report. A merge stores no report, because a merge that conflicts commits nothing. The report counts toward the commit's size like the rest of the record.

The range follows the change feed's cursor convention, so a consumer that reads the fork's change feed after `source_after_seq`, through `source_through_seq`, reads the commits that the merge applied. For example, a first merge of a fork made at sequence 100, which applies the fork's commits 101 through 120, records 100 and 120. Encoding and decoding refuse a merge record whose `source_after_seq` isn't below its `source_through_seq`, or whose `source_namespace_id` is the WAL object's own `namespace_id` (`InvalidWalMerge`).

LoonFS publishes a merge as the only commit in its WAL object, so `MAX_WAL_OBJECT_BYTES` bounds the merge alone, and the merge's size can't cause another commit to be refused. A merge's `create_inode` deltas record the inodes' IDs in the fork (see [Merges](#merges)). A merge includes no inline content and appends no bytes, because it refers only to content that the fork already wrote.

`inline_content` is a list of pieces, `{content_id, offset, bytes}`, where `bytes` is a CBOR byte string of the chain's bytes starting at `offset`. The field is left out when it's empty, and it defaults to an empty list when absent. A piece goes with an ordinary `blob_v1` reference, and a whole value is a piece at offset 0. An append to zero bytes starts a new content ID with a whole value (see [Operations and WAL deltas](#66-operations-and-wal-deltas)), so a chain of zero bytes is never appended to, and a download of the chain signs no range (see the [API specification][api-spec]).

Encoding and decoding enforce these rules, using only the WAL object:

1. Every entry's `content_id` is referenced by an `append_file_revision` delta in the same commit, and every such delta that includes a layout records the WAL object's `namespace_id` as the layout's creator.
2. The entries for one content ID in one commit, ordered by offset, are contiguous, and the last one ends at the largest `size_bytes` of those references. A zero-length entry is valid.
3. A delta without pieces for its content ID requires a layout. A first piece past offset 0 allows an absent layout, and a present layout covers exactly `[0, offset)`. A first piece at offset 0 can't have a layout. The planner checks that a new chain includes a layout, because the decoder can't see earlier revisions.
4. A commit contains at most one entry per content ID and offset.
5. Each entry contains at most `MAX_WAL_INLINE_CONTENT_BYTES` (256 KiB, or 262,144 bytes), and the sum of all entry lengths in one WAL object is at most `MAX_WAL_OBJECT_INLINE_CONTENT_BYTES` (4 MiB, or 4,194,304 bytes).

These are reader limits, and a writer's thresholds are policy at or below them. `MAX_WAL_OBJECT_BYTES` still limits the complete decompressed document. A delta can refer to content that has no inline entry. Replay doesn't hash pieces against the reference's checksum, because the envelope's `payload_checksum` covers the stored payload bytes. The decoder can't check whether the objects that a layout refers to exist, so a read or fold that finds their bytes missing reports corruption.

WAL replay applies these normalized records in sequence and delta order. It doesn't run the original request's preconditions again, or interpret the request again under a newer planner.

### A.6 Metadata rows and row keys

Rows are kind-tagged CBOR objects in the data blocks. The row-kind schema and the row-family ordering are separate, so the same `direntry_binding` row of a binding event appears in both binding families.

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

A `commit` row is the WAL commit record from [WAL records](#a5-wal-records) without its inline content, and a row that includes an `inline_content` field is invalid.

A `merge` row indexes one merge by the fork that it applied. A fold, and a replay of the WAL tail, write one `merge` row for each commit record that includes a merge record, with the merge record's fork and range and the commit's own sequence, and none for any other commit. A scan of one fork's prefix in `merges` returns that fork's merges newest first.

For a directory binding, `state` is `{"kind":"bound","display_name":...,"revision"?:...}` or `{"kind":"unbound"}`. A bound file row's `revision` is the file's revision summary, in the shape of a bind delta's `revision` (see [WAL records](#a5-wal-records)), and a directory row has none. `bind_position` is a `DeltaPosition` value, `{seq, delta_index}`. Both binding families store the complete row.

Index parity checks compare binding events alone, and leave out the summary rows that the slot index also stores (see [Directory bindings](#13-directory-bindings)). An unbound row is always a binding event.

For a tombstone, the top-level `committed_seq` and `delta_index` fields store the row's own position. A revoke target is a `DeltaPosition` value, `{seq, delta_index}`. A set action is `{"kind":"set","root_kind":...,"deleted_binding":...}`, and a revoke action is `{"kind":"revoke","target":...}`.

A trash `listed` action contains `inode_kind`, `deleted_at_ms`, `deleted_by`, and `deleted_binding`, and a `removed` action contains `removed_seq`, the sequence of the restore that removed the deletion from the listing. These fields are nested in the action, not top-level fields of the row. The `inode_kind` is the `root_kind` of the deletion's `set` event, and a `listed` action without it fails decoding.

Each row key starts with its family's prefix, followed by hyphen-separated components, under the key rules in [Row families](#19-row-families). In the grammar below, these placeholders and expressions aren't literal text in a key:

- A placeholder that ends in `_body` is a generated ID's 26-character body, without its prefix, so an inode ID leaves out `ino_` and a content ID leaves out `con_` (see [Numbered objects and generated IDs](#23-numbered-objects-and-generated-ids)).
- A placeholder that ends in `_hex` is the lowercase hexadecimal encoding of a client-chosen string's UTF-8 bytes. A name key is one, and so is a commit ID, because a client can supply any commit ID, such as `retry-1`.
- A counter is zero-padded decimal, 20 digits for an unsigned 64-bit value (`:020`) and 10 for an unsigned 32-bit value (`:010`).
- `u64::MAX - x` and `u32::MAX - x` mean subtraction before the zero-padded encoding, so that larger values sort first.

Every generated ID body has the same width, and its alphabet is in byte order, so the byte order of two keys follows the numeric order of their inode IDs. A `_hex` component is the only component whose width varies. It contains no hyphen, and a hyphen sorts before every hexadecimal digit, so a name's punctuation is never read as a separator, and a shorter name key sorts before every longer one with the same leading bytes.

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

The trash `sort_rank` is 0 for a removed entry and 1 for a listed entry, and it's still stored as ten digits.

Key order sorts a slot's edges by child, and a child's edges by parent and name key, not by position. So a slot's or a child's value is the newest of its edges' values (see [Directory bindings](#13-directory-bindings)), and an edge's value is the first of its rows in key order at or below the read's sequence.

Compaction groups both binding families by edge, and decides each row as it arrives, without keeping a row back. It also tracks the last edge bound at the floor, across the edges of one slot in the slot index and of one child in the child index. Commit validation allows one child per slot and one parent per child, so compaction refuses as corruption a second child bound in one slot at the floor, or a second parent bound for one child at the floor.

Bloom filters use these keys, which aren't always full row keys:

| Family | Filter key |
| --- | --- |
| `inodes` | The complete row key |
| `direntry_binds` | `direntry-bind-{parent_inode_id_body}-{name_key_hex}` for each row, which a slot lookup probes, and `direntry-bind-{parent_inode_id_body}` once for each run of consecutive rows with the same parent in the segment, which a directory listing probes |
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

A layout row records a chain's objects as of the commit or fold that wrote its bytes. The first row under a content ID's prefix is the longest layout (the newest, among layouts of equal length), and shorter rows are superseded. Compaction keeps the first row for each ID in its inputs, subject to the pruning rule in [Row retention during a base compaction](#103-row-retention-during-a-base-compaction). A layout row's `size_bytes` is the layout's size, which can differ from the reference's size. A layout row written from a revision delta copies the delta's layout (see [WAL records](#a5-wal-records)), and a row that a fold writes records the folding namespace as the layout's creator (see [Publishing a manifest](#72-publishing-a-manifest)).

A `layout` contains `creator_namespace_id` and `extents`. Every layout of one chain records the same [creator](#15-file-contents-and-ownership), and the extents are in chain order. Each extent contains `content_id`, `object`, `object_checksum`, `offset`, and `length`, and its object is `{"kind":"whole"}` or `{"kind":"span","object_id":...,"start":...,"end":...}`. A span's `object_id` is the span object's ID (`span_` and a 26-character body), which ends the object's key (see [Object keys](#a8-object-keys)), and `start` and `end` are the chain offsets of the bytes that the object stores. The `object_checksum` is the full-object checksum that the store keeps for the object, in the store's algorithm (see [Content references and checksums](#a3-content-references-and-checksums)), whatever part of the object the extent takes. A layout MUST follow these rules:

- The lengths add up to `size_bytes`, and each length is positive.
- Zero bytes require exactly one whole extent of length zero.
- The creator, each content ID, and each span object ID are valid.
- Each extent has an object checksum, with an algorithm and a value of that algorithm's width.
- Span bounds satisfy `start < end`, and the extent's object range fits within `end - start`.

A shared prefix can shorten an extent without changing its span object, and readers honor nonzero object offsets.

Why does a layout record the chain's creator, instead of the delta and the row each having a field? The creator is a fact about the chain, like its extents, so the layout describes the whole chain, a revision delta and a layout row each include one record, and no rule has to tie one field's presence to another's.

The family groups are:

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

A segment is a concatenation of independently readable sections:

```text
[zstd data block 0]
[zstd data block 1]
...
[uncompressed extent filter]  (content_layouts segments only)
[uncompressed bloom filter]
[zstd index block]
```

There's no segment header or footer. The manifest descriptor provides the filter, extent filter, and index handles, and the index provides the data-block handles, so a segment can't be opened without its descriptor.

Each handle is an encoded object with these fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `offset` | `u64` | The zero-based byte offset in the complete segment. |
| `stored_bytes` | `u32` | The number of stored bytes in the section. |
| `decoded_bytes` | `u32` | The expected length after decompression (for a filter, the uncompressed length). |
| `crc32c` | `u32` | The CRC32C of the exact stored section bytes. |

The verified put checks the whole object when it's written, and a reader that fetches the whole segment checks it against the descriptor's `checksum`. A mismatch is corruption.

Each section MUST pass three checks: the returned byte count matches `stored_bytes`, the CRC of the stored bytes matches `crc32c`, and the decoded length matches `decoded_bytes`. Both lengths fit in `u32`. Readers stop decompressing after at most `decoded_bytes + 1` bytes, so the extra byte reports a mismatch without expanding the rest of an invalid section. This limits the decompressed output, but not vector capacity or the memory of decoded rows and parser state. [Reference-implementation sizing and admission defaults](#c4-reference-implementation-sizing-and-admission-defaults) describes initial allocation sizing.

#### Data blocks

Before compression, a data block is made up of entries, a restart-offset array, and a four-byte restart count. An entry contains:

```text
shared_prefix_len     unsigned LEB128
key_suffix_len        unsigned LEB128
key_suffix            key_suffix_len UTF-8 bytes
row_len               unsigned LEB128
row                    row_len CBOR bytes
```

The key is the first `shared_prefix_len` bytes of the previous key, followed by `key_suffix`. The prefix MUST end on a valid UTF-8 boundary, and keys are in ascending order. The block grammar can represent adjacent equal keys, but metadata producers still follow the uniqueness rule for metadata runs.

The first entry, and every sixteenth entry after it, starts a restart and encodes the full key with a shared-prefix length of zero. The restart array contains their offsets relative to the start of the entry region, each as a little-endian `u32`. Readers verify each offset and reject missing or unused offsets. The array is followed by its count, also a little-endian `u32`, and the entries and restart data are compressed together as one zstd section. Metadata and grep data blocks use the same entry and restart layout.

Writers encode unsigned LEB128 values in their shortest form, with seven payload bits per byte and the high bit set when another byte follows. Values fit in `u64`, so a ten-byte encoding has one payload bit in its final byte.

#### Filter block

The filter is uncompressed, and it has this byte layout:

```text
n_hashes              u32, little-endian
bit_len               u64, little-endian
bits                  ceil(bit_len / 8) bytes
```

Version 1 requires `n_hashes` to be seven and `bit_len` to be at least 64, and readers reject other header values before probing. The number of stored bit-array bytes MUST equal `ceil(bit_len / 8)`. The reference producer sizes the filter as [Filter sizes](#filter-sizes) describes, and repeated filter keys count as separate insertions for sizing.

Hashing is XXH64 over the filter key's UTF-8 bytes, with two constant seeds:

```text
h1 = XXH64(key, seed = 0)
h2 = XXH64(key, seed = 0x9e3779b97f4a7c15)
```

For each probe `i` from zero through `n_hashes - 1`, calculate `x = h1 + i * h2`, mix `x` with the splitmix64 finalizer, and take the remainder modulo `bit_len`, with unsigned 64-bit wrapping arithmetic throughout:

```text
x = h1 + i * h2
x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
x = (x ^ (x >> 27)) * 0x94d049bb133111eb
b = (x ^ (x >> 31)) % bit_len
```

Bit `b` is stored in byte `b / 8`, at the mask `1 << (b % 8)`. The mix makes each probe depend on all 64 bits of both hashes. Without it, a small filter would have few distinct probe sequences, and a key whose `h2` is a multiple of `bit_len` would test one bit seven times.

A valid negative filter result means the lookup can skip the segment. A positive result doesn't replace the row-level visibility rules.

When `filter_inline` is present in the descriptor, it's the lowercase hexadecimal encoding of the exact stored filter bytes, and it MUST have the filter handle's stored length and checksum. The segment still contains the filter block, and a reader without an inline copy fetches the block by its handle.

#### Filter sizes

The reference producer sizes each Bloom filter by what a false match costs, with a number of bits for each key and a minimum number of bits. (These sizes aren't format requirements, and a reader accepts any `bit_len` of at least 64.)

| Filter | A false match | Bits per key | Fewest bits |
| --- | --- | --- | --- |
| A segment's filter ([Filter block](#filter-block)) | Costs a lookup one more read | 10 | 64 |
| A segment's extent filter ([Extent filter](#extent-filter)) | Keeps a content object until no root lists the segment | 20 | 4,096 |
| A compaction's layout filter and kept inode filter ([Row retention during a base compaction](#103-row-retention-during-a-base-compaction)) | Keeps a row until a compaction with another seed | 10 | 4,096 |
| A reorganization's value and revision filters ([Pruning the index](#d5-pruning-the-index)) | Keeps an index row until a later reorganization | 10 | 4,096 |

Why do filters whose false match keeps data have a 4,096-bit minimum? A filter with 10 bits for each key matches another key about once in 100 to 120 at any size, which is too often when a false match keeps data. With 4,096 bits, a filter of 30 keys matches about once in 1.2 billion, of 100 keys about once in 426,000, and of 200 keys about once in 5,800. A segment's filter has the smaller minimum because its false match costs one read, and a 64-bit filter of three keys matches about once in 6,300.

#### Extent filter

A `content_layouts` segment has an extent filter right before its filter, and a segment of another family has none. A descriptor of a `content_layouts` segment without `extent_filter_block`, or of another family with one, is corruption. The extent filter has the filter block's byte layout, header rules, and hashing, and its `decoded_bytes` equals its `stored_bytes`. The manifest has no inline copy.

The extent filter's keys are the names of the content objects that the extents of the segment's rows reference, including shared base extents. A name is the object key without the pool's content prefix (`pools/{pool_id}/content/`), so it's `{content_id}` for a whole object and `{content_id}-{span_object_id}` for a span object (see [Object keys](#a8-object-keys)). A name doesn't depend on the pool, so a segment that any member wrote and the listing of the pool's content prefix agree on the names. The reference producer sizes the extent filter as [Filter sizes](#filter-sizes) describes, and repeated names count as separate insertions.

Garbage collection reads the extent filter (see [Content roots](#119-content-roots)), and a lookup's fetch of the filter and the index leaves it out.

#### Index block

The decoded index is a CBOR list. Each entry contains `last_row_key` and `block`, where `block` is the corresponding data-block handle. The list is zstd-compressed as one section.

Index entries are ordered by their last keys. Data-block byte ranges are contiguous and MUST NOT overflow. A `content_layouts` segment's extent filter comes right after the data region, the filter comes right after the data region or the extent filter, and the index comes right after the filter, at the end of the object. The descriptor's handles MUST agree with that layout.

A range lookup uses the last keys to find candidate blocks. Readers verify each fetched section before they decode its rows, and they apply the descriptor's family and key-range constraints.

### A.8 Object keys

These patterns define the core object families. [Storage layout](#21-storage-layout) describes which prefix stores each one, and where an inherited segment is stored.

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

A commit fingerprint is stored as `v1:sha256:<64 lowercase hex>`, where `v1` identifies the canonicalization rules below, not the API's general JSON serialization. It's the SHA-256 of compact UTF-8 JSON with these top-level fields, in this exact order:

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

The hashed bytes have no formatting whitespace. Every member is always present: `preconditions` is `[]` when empty, and `message` is `null` when absent. A merge uses a different preimage (see [Merge commits](#b5-merge-commits)).

A changed `actor_id` is a different request, even from a process retrying for the same application. The preimage leaves out the commit ID, writer epoch, and commit timestamp.

### B.1 Operation fields

Every operation starts with `kind`, then the fields below in order. Every field is written, including unset optional fields (as `null`) and default booleans and behaviors.

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

Paths use their validated canonical absolute form, and display names are one validated component. Inode IDs use their 26-character spelling without the public `ino_` prefix. Sequence and revision numbers are JSON integers, and binding versions stay opaque strings.

Attribute `set` keys are sorted in lexicographic UTF-8 byte order, and the `remove` list is sorted and deduplicated. Operations keep the request's order.

### B.2 Content in a fingerprint

A content reference has exactly these fields, in this order:

```json
{"kind":"blob_v1","content_id":"con_014d2pf2dbsqqg28t5cy4tqkff","size_bytes":15}
```

The preimage leaves out the checksum (verification evidence, still required and validated on the reference) and the delta's `layout` (physical placement). Identical bytes uploaded twice have different IDs and fingerprints, so a retry reuses the original reference.

Inline content has no object to refer to, so its `content_ref` identifies it by its bytes:

```json
{"kind":"inline_v1","sha256":"<64 lowercase hex>","size_bytes":15}
```

`sha256` is the lowercase hexadecimal SHA-256 of the complete content, separate from the reference's checksum, and the fields appear in the order shown. The content ID isn't included. The form follows how the request sent the content, not where it's stored. Inline content keeps this form even if the writer stages it, and staged or uploaded content keeps the reference form.

Appended bytes have no reference until the writer plans the commit, so the `inline_content` member of `append_file` and `append_file_by_inode` uses a third form:

```json
{"kind":"append_v1","sha256":"<64 lowercase hex>","size_bytes":6}
```

`sha256` is the lowercase hexadecimal SHA-256 of the appended bytes alone, and `size_bytes` is their length.

### B.3 Preconditions

The precondition list comes after `message` and keeps the caller's order, unsorted and not deduplicated. Each precondition starts with `kind`, followed by:

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

Every field is written, with `null` for an absent optional field, and values follow [Operation fields](#b1-operation-fields). Preconditions affect request identity and validation but add no WAL field or replay delta.

### B.4 Strings, integers, and example bytes

Non-ASCII characters are written directly as UTF-8. JSON quotes, backslashes, and control characters are escaped, and `/` isn't. Integers use decimal digits without leading zeros. The field orders above are kept as specified, but they aren't a general instruction to sort every object's fields alphabetically.

For example, the complete preimage of one directory-creation request, with no trailing newline, is:

```json
{"domain":"loonfs.commit.semantic.v1","namespace_id":"demo","actor_id":"usr_8f3c","operations":[{"kind":"create_directory","path":"/reports","parents":false}],"message":null,"preconditions":[]}
```

Its fingerprint is:

```text
v1:sha256:a64ec097a98f4c881c3770868ad31e77efd8ec9fedb6101672de2091311666ba
```

A one-operation convenience call and a one-element commit request use the same canonical input. The raw request JSON isn't the preimage, because it can leave out defaults that the canonical operation writes.

The shared vectors in [commit_fingerprints_v1.json][fingerprint-vectors] cover every operation and precondition variant, path bindings with and without binding versions, an empty precondition list, and operations with and without inline preconditions. Encoders MUST preserve those exact bytes and digests.

### B.5 Merge commits

A [merge's](glossary.md#merge) fingerprint covers its merge record in place of its operations and preconditions (see [Commit identity and retries](#65-commit-identity-and-retries)). It's the SHA-256 of compact UTF-8 JSON with these top-level fields, in this exact order:

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

Every member is always present, with `null` for an absent `message`. Strings and integers follow [Strings, integers, and example bytes](#b4-strings-integers-and-example-bytes), and the stored form is an ordinary commit's.

For example, the complete preimage of a merge into `demo` of the fork `demo-fork`'s commits above sequence 418, through sequence 425, with no trailing newline, is:

```json
{"domain":"loonfs.commit.merge.v1","namespace_id":"demo","actor_id":"usr_8f3c","merge":{"source_namespace_id":"demo-fork","source_after_seq":418,"source_through_seq":425},"message":null}
```

Its fingerprint is:

```text
v1:sha256:004b16fd5b647a8003f24e3b558b5f93ff5e6bc836ca42322f93ddff1ffaa365
```

## Appendix C. Timing and size reference

Publication and garbage collection use the timing relationships below. [C.4](#c4-reference-implementation-sizing-and-admission-defaults) lists the configurable sizing targets, and the [limits module][limits-source] defines the reference values.

### C.1 Publication and collection timing

| Constant | Milliseconds | Meaning |
| --- | ---: | --- |
| `METADATA_PUBLICATION_BUDGET_MS` | 900,000 | From the start of a bounded manifest publication (including an epoch claim) to starting its put. |
| `PROVIDER_OPERATION_DEADLINE_MS` | 120,000 | The shared retry budget for a client operation. |
| `PROVIDER_ATTEMPT_TIMEOUT_MS` | 30,000 | One attempt of a control operation. |
| `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` | 300,000 | The retry budget, the final backoff, and a payload-sized final request, rounded up to 5 minutes. |
| `GC_SAFETY_MARGIN_MS` | 300,000 | The combined allowance for relative clock error, timestamp precision, and scheduling. |
| `PUBLICATION_COMPLETION_BOUND_MS` | 1,500,000 | The publication completion bound: the metadata publication budget, plus the provider's request bound, plus the combined allowance. |
| `READ_REVALIDATION_BOUND_MS` | 1,200,000 | The largest maximum staleness that a read can ask for, the largest manifest check interval that an operator can set, and the age of a cached view's last manifest check at which an `eventual` read stops using the view without a request. It doesn't limit the age of a publication's basis or of a writer position. |
| `GC_GRACE_WINDOW_MS` | 86,400,000 | The grace window that every collection uses. An unreferenced segment MUST be strictly older than this before it's deleted, a deleted namespace's tombstone is eligible for retirement this long after its deletion, and a retention advance doesn't move the floor past a younger commit. |
| `METADATA_COMPACTION_BUDGET_MS` | 43,200,000 | The longest time before a compaction starts its publication. |
| `DIRECT_TRANSFER_URL_TTL_MS` | 900,000 | The lifetime of a presigned download URL and of a presigned URL for a whole-object upload. A presigned URL for one part of a multipart upload lasts one hour. |
| `OPERATION_SNAPSHOT_TTL_MS` | 300,000 | The lifetime of a snapshot that LoonFS takes for one of its own operations (the snapshot of the fork that a merge or a rebase plans from, and the temporary snapshot of the source's head that a fork takes), and of the content proofs of the operation's plan. |
| `POOL_ROOTS_READ_BUDGET_MS` | 43,200,000 | The monotonic time, from the start of a pool's collection, within which the collection reads the roots of every member of the pool, or skips the pool's content sweep. |
| `MAX_TAIL_COMMIT_AGE_MS` | 42,300,000 | The greatest age of a commit in a namespace's WAL tail at which a merge, or a rebase's replay commit, still plans from the fork's tail, and a fork of the head still folds the source's tail into itself. When a commit is older, the merge folds the fork before planning, and the fork folds the source and reads the source's head again. It's the grace window less the other terms of the pool collection's inequality below (see [Cross-namespace copies and moves](#97-cross-namespace-copies-and-moves) and [Forking a namespace](#92-forking-a-namespace)). |

The provider's request bound is the 120-second operation deadline, plus up to 15 seconds of backoff for a retry admitted just before that deadline, plus 120 seconds for a payload-sized request, which is 120 + 15 + 120 seconds, rounded up to 300 seconds. It reserves a payload-sized request instead of the 30-second attempt timeout because the object-store layer sends a conditional WAL or manifest write as one request (see [Required operations](#31-required-operations)), and that one request can be larger than the payload threshold. The bound covers the request phase, not reading the response body, and it doesn't show that a timed-out remote change had no effect (see [Clock and operation assumptions](#114-clock-and-operation-assumptions)).

```text
PUBLICATION_COMPLETION_BOUND_MS
    = METADATA_PUBLICATION_BUDGET_MS
      + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
      + GC_SAFETY_MARGIN_MS
```

The grace window `T` can't be lower than the publication completion bound. A compaction's last publication attempt finishes before its earliest unreferenced output is old enough to delete when this inequality is true (see [Streaming compaction](#104-streaming-compaction)):

```text
METADATA_COMPACTION_BUDGET_MS + PUBLICATION_COMPLETION_BOUND_MS
    < GC_GRACE_WINDOW_MS
```

With the current constants, that's 12 hours and 25 minutes, against 24 hours. A pool's collection keeps content safe when this inequality is true (see [Content roots](#119-content-roots)):

```text
POOL_ROOTS_READ_BUDGET_MS + GC_SAFETY_MARGIN_MS
    + OPERATION_SNAPSHOT_TTL_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
    < GC_GRACE_WINDOW_MS
```

With the current constants, that's 12 hours plus 15 minutes, against 24 hours, so `MAX_TAIL_COMMIT_AGE_MS` is the remaining 11 hours and 45 minutes.

A fork that folds the source's WAL tail publishes its active manifest within `OPERATION_SNAPSHOT_TTL_MS` (5 minutes) of its temporary snapshot's creation, which is shorter than the 15 minutes (`METADATA_PUBLICATION_BUDGET_MS`) that a manifest publication can take. A fork call that's past those 5 minutes writes nothing and returns `snapshot_not_found` (see [Forking a namespace](#92-forking-a-namespace)), and a namespace's collection publishes the initializing fork's tombstone once manifest 1 is more than `T` old (see [Deleting a namespace](#94-deleting-a-namespace)).

The completed-content grace has its own constant, not `T`. Changing a publication bound, or one of these inequalities, changes the protocol, not just a scheduling preference.

### C.2 Upload admission

| Constant | Milliseconds | Meaning |
| --- | ---: | --- |
| `UPLOAD_SESSION_LEASE_MS` | 86,400,000 | An open session's lifetime on the creating host. |
| `COMPLETED_UPLOAD_RECEIPT_WINDOW_MS` | 604,800,000 | The seven-day window in which completed content can issue new receipts. |
| `CONTENT_RECEIPT_TTL_MS` | 3,600,000 | The one-hour token lifetime. |
| `COMPLETED_UPLOAD_ADMISSION_WINDOW_MS` | 608,400,000 | The receipt issuance window plus the final token's lifetime. |
| `CONTENT_RECLAMATION_GRACE_WINDOW_MS` | 691,200,000 | The completed-content grace, at least the admission window plus the publication completion bound. |

[Upload-session cleanup](#116-upload-session-cleanup) compares the eight-day completed-content grace with the receipt issuance window, the final token's lifetime, and the publication completion bound. Reaching that age doesn't delete referenced content.

A direct multipart upload is also subject to the provider's transfer limits, which in the reference are 10,000 parts, a configured part size from 5 MiB to 5 GiB, and 1,000 presigned part URLs per signing request. A supported provider's lower maximum object size, and its other constraints, still apply.

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

Attribute limits count the original UTF-8 strings, before JSON escaping or CBOR encoding. Path limits apply to canonical request paths.

A [merge](glossary.md#merge) isn't held to the API's request limits on operations, preconditions, content tokens, and content references, because LoonFS plans a merge instead of reading it from a request body. LoonFS checks the merge's planned WAL record against `MAX_WAL_OBJECT_BYTES`, as for every commit, and a merge that exceeds it fails with `merge_too_large`. The reference server's merge allowance (`max_merge_bytes`) also bounds a merge (see the `merge_too_large` row in the API's [Standard error contract](api.md#3-standard-error-contract), which describes the allowance and how the server estimates a merge's memory).

### C.4 Reference-implementation sizing and admission defaults

These are defaults for the reference producer and runtime, and changing them doesn't change the stored encoding. One large row can exceed a target size, because rows are never split to meet a size target.

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

`max_staleness_ms` is the default maximum staleness of an `eventual` read, and [Replaying the visible WAL](#42-replaying-the-visible-wal) describes how reads use `max_staleness_ms` and `manifest_revalidation_interval_ms`. Neither setting can exceed `READ_REVALIDATION_BOUND_MS`, and a runtime build with a longer one fails.

A decoder can't use the target block or segment sizes as hard allocation limits. The reference block reader first reserves at most the smaller of `decoded_bytes` and 64 KiB, and allocates more only as it decompresses more bytes, so it doesn't impose a smaller maximum block size. Output stops at the declared length plus one byte (see [Block-segment encoding](#a7-block-segment-encoding)), and vector capacity can exceed that length. The [API specification][api-spec] specifies request admission limits.

[Control and manifest payloads](#a4-control-and-manifest-payloads) and [WAL records](#a5-wal-records) specify the hard limits on the sizes of a manifest document and a WAL document.

## Appendix D. Grep extension format

Grep is an [extension](#124-extensions) that indexes a namespace's file content. Its state is the `grep` entry in the namespace manifest, and its segments are stored under `namespaces/{owner_namespace_id}/extensions/grep/segments/`. The index is derived from authoritative file content and namespace history, so it can be rebuilt from a new backfill.

### D.1 The entry and its publication

This section describes grep's entry in a namespace manifest and how grep publishes a change to it.

Grep's entry has these members:

| Member | Meaning |
| --- | --- |
| `version` | The grep format's version, which is 1. |
| `status` | The index's lifecycle state and its position fields (see [Index lifecycle](#d2-index-lifecycle)). |
| `reorganize?` | The in-progress reorganization, if there is one. |
| `runs` | The index's runs in list order, each with its run number, its level, and its segment descriptors (see [Control and manifest payloads](#a4-control-and-manifest-payloads)). |

Grep decodes `status` and `reorganize`, and it rejects an entry with another version, a member that grep doesn't define, or invalid nested state. Disabling grep replaces a rejected entry of version 1 with a disabled entry, so the index can be enabled again, but fails for an entry of another version and leaves it in place, so a build never discards an index in a format version that the build doesn't read. A namespace whose manifest has no `grep` entry has no index, and enabling grep builds one.

A grep "step" is one bounded unit of work: a backfill, build, or reorganization step. A host runs steps in a "run of steps", such as the index steps that the reference server's scheduler runs for one namespace at a time. Each step starts from the entry that its run of steps' earlier steps left pending (the "pending entry"), writes its segments, and records its change in the pending entry. A run that a step writes is numbered one above the largest run number in the pending entry, counting an in-progress reorganization's. The first run of an entry without runs is numbered 0. Run numbers identify runs only within one entry, so a number can be reused after its run is gone.

Grep publishes the pending entry in a manifest of its own (see [Extensions](#124-extensions)) once the change is one maintenance interval old, and when the run of steps ends. The reference server's interval is five seconds by default, and a host without a scheduler, such as the CLI, uses five seconds. A crash loses at most one interval's steps, whose segments are left to the collector.

To publish a change, grep:

1. Loads the current manifest before the change's first step writes a segment. The publication budget (`METADATA_PUBLICATION_BUDGET_MS`) runs from this load (see [Publishing a manifest](#72-publishing-a-manifest)).
2. Runs the change's steps from that manifest's entry.
3. Puts the next manifest, which lists the pending entry, at the loaded manifest's number plus one with put-if-absent.
4. If another manifest has that number and its grep entry is the same as the loaded manifest's (or neither manifest has one), puts the change over that manifest, and repeats this step if that put loses too.
5. If the entries differ, publishes nothing and leaves the change's new segments to the collector. The next step plans from the current manifest's entry.

A put with an unknown outcome is resolved as [Conflicting and unknown installations](#93-conflicting-and-unknown-installations) describes, where an identical manifest confirms success when it lists a segment that the change wrote.

Each step's change to the entry MUST keep the `captured_seq` of the build it follows or start a later build at a later sequence, and within a build it MUST NOT move the indexing position in `status` back. So a manifest's entry can describe a later build than the one before it, when a backfill restarted within the publication, and a backfill can reset its inode cursor only when it restarts at a later sequence.

A query reads grep's entry from its read view's manifest, so the index is as current as that manifest. That manifest was published after every commit that its entry records, so the view's head is at or above every sequence in the entry (such as a backfill's captured sequence and the sequences in `pruning`). A page can still read below the index watermark, for example on a later page of a search (see [Querying the index](#d6-querying-the-index)).

Why does a run of steps publish once for each maintenance interval? Sixteen steps that each published would make sixteen manifests. A writer whose position walk finds more than eight new manifests drops its position and rediscovers it with a tail replay (see [Replaying the visible WAL](#42-replaying-the-visible-wal)), and every cached reader's successor probe would find a successor and run discovery. One publication for each interval keeps grep to about one manifest every five seconds.

Why does a step start from the pending entry instead of the manifest's? The earlier steps of its run of steps wrote `texts` rows that aren't published yet, so a step that started from the manifest's entry would index their range again.

### D.2 Index lifecycle

The entry's `status` contains the position fields for its lifecycle state:

| Kind | Status fields |
| --- | --- |
| `backfilling` | `captured_seq`, optional `cursor_inode_id` |
| `active` | `captured_seq`, `built_through_seq`, `next_event_index` |
| `disabled` | No other status fields |

A backfill reads the files in inode order as they were at its `captured_seq` (the namespace's head when the backfill starts), and its cursor resumes strictly after the last inode ID. Its steps read the live namespace at that sequence, so the backfill can continue until the retention floor passes the sequence. An active index keeps its backfill's `captured_seq`, which identifies that build, and its `next_event_index` is zero at a commit boundary. A disabled index lists no segments and no reorganization.

A rebootstrap happens when the retention floor passes the index's position (an active index's watermark, or a backfill's `captured_seq`). Like disabling and enabling grep, a rebootstrap starts a new backfill at the namespace's head with its cursor at the beginning, but it keeps the entry's runs and reorganization, because postings refer to content values, which stay valid. Each value that a kept `texts` row records then costs the backfill a lookup instead of a read. Only disabling grep empties the runs.

A fork's first manifest copies its source's grep entry (see [Forking a namespace](#92-forking-a-namespace)), and the fork's first step handles the entry by its state:

- An `active` entry is the source's index as of the snapshot's manifest, so a query in the fork uses it immediately, with the fork's own commits as the unindexed tail. The source's index can lag its head, so its watermark can be below the fork point, where the fork's change feed starts. A step reads the source's commits above the watermark and at or below the fork point (the "gap") from the fork's change feed through its fork point snapshot, without reading the source (see [Reads, deletion, and expiry](#83-reads-deletion-and-expiry)), and then continues in the fork's own feed. If the snapshot can't serve the gap, because the source's retention floor was above the watermark when the fork was made, the step rebootstraps the fork's index.
- A `backfilling` entry continues in the fork when its `captured_seq` is the fork point, where the fork's state is the source's. A lower `captured_seq` is below the fork's retention floor, so the fork's first step rebootstraps the index at the fork's head and keeps the inherited runs.
- An inherited `reorganize` is the source's, and the source owns its outputs, so the fork's first change drops it and keeps every segment that the entry lists. Grep recognizes an inherited reorganization by its outputs' owner. The fork's own reorganizations start from the inherited runs, and reorganizing inputs whose outputs the fork also inherited is harmless, because a reorganization takes the union of its inputs.
- A `disabled` entry stays disabled.

The entry's optional `reorganize` contains `input_segment_ids`, `output_segment_ids`, `row_key_cursor` (which is inclusive), `output_level`, and `run_no`. A reorganization into the base level also contains `pruning`, which its first step records to identify the revisions whose content values it keeps (see [Pruning the index](#d5-pruning-the-index)). Input and output segment IDs MUST be unique, disjoint, and present in the entry's runs. Each output MUST be a segment of the run with the recorded run number, and that run MUST have the recorded level. Two runs of the entry MUST NOT have the same number.

Grep's runs, and each run's segments, follow list order. Grep uses a run's `level` instead of the core's base and delta tiers:

- Level 0 is delta output.
- Level 1 is intermediate reorganization output.
- Level 2 is the base.

An in-progress reorganization records its output level and its run number, which is assigned when the reorganization starts (see [The entry and its publication](#d1-the-entry-and-its-publication)). So resumed steps continue the same run, and a run that a later step writes has a higher number even before the reorganization writes its first output.

A reorganization starts when a level reaches its run limit: eight delta runs are rewritten into a new run at level 1, and eight runs at level 1 are rewritten with the base into a new base. (Eight is the reference worker's default for both limits, not a format requirement.) Within a run of steps, the next step reorganizes whenever the pending entry records a reorganization or a level has reached its limit, and backfills or builds otherwise. So grep reorganizes even while the index is behind the namespace's head, and the entry lists no more delta runs than the limit, however many commits are left to index.

Why doesn't a reorganization wait until a build step finds the index up to date? While a writer keeps committing, that might never happen. Each step that indexes something adds a delta run, and each step looks up its values' `texts` rows in every listed segment (see [The `texts` and `content_revisions` families](#d4-the-texts-and-content_revisions-families)), so the delta runs and the cost of each build step would grow without a limit.

### D.3 Tokenization and postings

Content is eligible for the version 1 gram index when it's at most 8 MiB, and its first 8 KiB contain no NUL byte and pass the text check. The sample MUST be valid UTF-8, except that an incomplete final character is accepted when it follows a nonempty valid prefix. The check reads only the sample, so the bytes after it don't have to be UTF-8.

For eligible content, the tokenizer folds ASCII letters to lowercase and takes every overlapping three-byte window. Grams are bytes, not Unicode characters, and this folding is different from the Unicode rule that's used to compare file names. A gram is encoded as six lowercase hexadecimal characters. For example, `Abcd` produces the byte grams `abc` and `bcd`, represented as `616263` and `626364`.

A "content value" is the bytes that a content reference refers to, identified by the reference's content ID and size (see [File contents and ownership](#15-file-contents-and-ownership)). The same content ID and size refer to the same bytes in every member of a [pool](glossary.md#pool). So the index records content values, not file revisions, and it reads and tokenizes a value once, however many revisions refer to the value (for example, a file copied by reference, or restored to an earlier revision).

The index's segments store five row families: postings, value tables, `texts`, `content_revisions`, and the revision filters of reorganizations into the base level (see [Pruning the index](#d5-pruning-the-index)). A row is a kind-tagged CBOR object, and it stores a content ID in its string form (`con_` followed by its 26-character body). One run's segments (the output of one step or one reorganization) share a value table, and the run's postings refer to content values by their positions in that table.

A run's "value table" lists each content value that the run has a `texts` row for (see [The `texts` and `content_revisions` families](#d4-the-texts-and-content_revisions-families)), ordered by content ID and then by size. A value's "ordinal" is its position in that order, from zero, so ordinal order is content order. For example, a run's values `(con_1..., 12)`, `(con_1..., 40)`, and `(con_7..., 5)` have ordinals 0, 1, and 2. A `value` row's fields after `kind` are `ordinal`, `content_id`, and `size_bytes`, and its row key is:

```text
value-{ordinal:020}
```

Every `value` row has the filter key `value`. Value rows sort after every other family's rows, so a segment has value rows exactly when its key range reaches `value-`, and a reader finds them by key range without probing the segment's filter. A run's value rows MUST list its ordinals from zero without a gap, in content order, and a reader decodes the run's table from the value rows of all of the run's segments.

A posting identifies a content value whose bytes contain a gram, by the value's ordinal in its run's table. A `gram_postings` row's fields after `kind` are `gram` (a hexadecimal string), `first_ordinal`, and `postings` (a CBOR byte string that contains the packed batch). `first_ordinal` MUST equal the batch's first ordinal. The row key is:

```text
gram-{gram_hex}-{first_ordinal:020}
```

Its filter key is `gram-{gram_hex}`. Several rows, within a segment or across a run's segments, can contain batches for the same gram, and a reader combines them. A batch's ordinals are strictly ascending. The batch stores the number of postings, the first ordinal, and then the gap from each ordinal to the next (at least one), each as an unsigned LEB128 value:

```text
posting_count   (unsigned LEB128)
first_ordinal   (unsigned LEB128)
gap             (unsigned LEB128, at least 1)
... one gap for each remaining posting
```

For example, the batch of ordinals 7, 8, and 300 is the bytes `03 07 01 a4 02`. Empty batches, a zero gap, an ordinal above 2^64 - 1, trailing bytes after the declared batch, and an ordinal that the run's table doesn't list are invalid.

A reorganization writes a new run, whose table lists the union of its input runs' values, less the values that a reorganization into the base level drops (see [Pruning the index](#d5-pruning-the-index)). Each step translates the inputs' ordinals to the new table's ordinals through the two tables. The first step writes the whole table, so every output that an entry lists can be read.

Why do postings refer to ordinals instead of content IDs? A content ID is 16 random bytes, so postings of content IDs wouldn't compress and would be most of the index. The gap between two ordinals in one gram's batch usually fits in one byte, and the run's table stores each content ID once.

The segment framing is exactly the data, filter, and index format in Appendix A, with grep CBOR rows instead of metadata rows, and per-block CRCs verify ranged reads.

Grep's version 1 governs the tokenizer, the posting representation, and the meaning of row keys, so none of them can change while an existing index is read under the same version.

### D.4 The `texts` and `content_revisions` families

This section describes the row families that let a grep step skip an examined value and let a query find the files that refer to a value.

The `texts` family has one row for each content value that a grep step examined. A `text` row's fields after `kind` are `content_id`, `size_bytes`, and `eligible`, which records whether the value passed the eligibility rule, and so whether the index has postings for it. Its row key and filter key are:

```text
text-{content_id_body}-{size_bytes:020}
text-{content_id_body}
```

Before a grep step reads a value's bytes, it looks up the value's `texts` row in the segments that its pending entry lists, and it skips a value that has a row. A step that examines a value, eligible or not, writes the value's `texts` row in the same run as the value's postings, and lists the value in that run's value table. A value above the size cap is recorded as ineligible without being read. Eligibility depends only on the bytes, so two rows for one value MUST agree, and a reorganization combines them into one row.

The `content_revisions` family has one row for each file revision that a grep step indexed. A `content_revision` row's fields after `kind` are `content_id`, `size_bytes`, `inode_id` (a 16-byte byte string, see [Field conventions](#121-field-conventions)), `revision_no`, and `committed_seq` (the sequence of the commit that published the revision). Its row key and filter key are:

```text
content-revision-{content_id_body}-{size_bytes:020}-{inode_id_body}-{u64::MAX - revision_no:020}
content-revision-{content_id_body}-{size_bytes:020}
```

A scan of one value's prefix returns the revisions that refer to the value, ordered by inode, and newest first within each inode. A backfill step writes a row for the current revision of each file that it lists, including a deleted file. A build step writes a row for each `file_created` or `content_changed` event that it indexes, even when it skips the event's value because the value has a `texts` row. So the family covers every revision committed since the index was enabled, and each file's revision that was current at that time. Two rows with the same key describe the same revision, and a reorganization combines them into one row.

Why does the index store `content_revisions` itself? A query that finds a content value needs the revisions that refer to the value, and no core row family is keyed by content value. The steps write these rows from the listings and change events that they already read.

### D.5 Pruning the index

This section describes how a reorganization into the base level drops the rows of content values and revisions that the namespace no longer retains, so the index doesn't keep a posting for every value it ever examined.

A reorganization into the base level keeps the content values and `content_revisions` rows of a set of revisions, which its first step records in `pruning`:

- `filter_seq` is the namespace head when the first step starts.
- `retention_floor_seq` is the namespace's retention floor at that time, at or below `filter_seq`.

The reorganization keeps every file revision at or below `filter_seq` that's newer than `retention_floor_seq`, and each file's newest revision at or below `retention_floor_seq`, deleted files included. The first step scans the namespace's revisions at `filter_seq`, including the WAL tail's commits, and builds two Bloom filters from the kept revisions: one of their content values, and one of their `(inode_id, revision_no)` pairs. Then:

1. The first step writes the revision filter, then decides the output run's value table in one read of the input tables in content order, listing each input value that the value filter contains. It writes each table row as it decides it, in segments of the output run that contain the filter and the table.
2. Every later step reads the revision filter back from the outputs, so the namespace is scanned once, and it reads the output's table beside the input tables in the same kind of read.
3. Every step translates each input posting's ordinal to the output ordinal of the same value, through a list that it builds for each input run (both tables are in content order), and drops a posting whose value the output's table doesn't list. A run's `n`th `texts` row is its table's `n`th value, so `texts` rows translate the same way, and the step keeps those whose value the output's table lists. So a value's postings and its `texts` row are dropped together.
4. Every step drops each `content_revisions` row whose `(inode_id, revision_no)` pair isn't in the revision filter.

The revision filter is stored as `filter` rows, whose fields after `kind` are `chunk` and `payload`, with the row key `filter-{chunk:020}` and the filter key `filter`. In chunk order, the payloads (at most 32 KiB each) form the payload of a filter block (see [Block-segment encoding](#a7-block-segment-encoding)) over one key for each kept revision, `{inode_id_body}-{revision_no:020}`. A reader finds the rows by key range, and a reorganization doesn't copy an input's `filter` rows to its output. Each filter is sized as [Filter sizes](#filter-sizes) describes for the inputs' rows, up to 64 MiB, so a larger namespace fills it more densely and keeps more rows.

Because a step reads the tables a block at a time, it keeps the translation lists (four bytes for each input value) and the filters, whatever the tables' size. A Bloom filter can err only toward keeping a row, which a later reorganization drops. If the retention floor passes `filter_seq` before the reorganization finishes, the reorganization is abandoned.

Before recording its change, the reorganization's last step runs a "re-reference check". If a `file_created` or `content_changed` event in the change feed from `filter_seq` to the head refers to a value that an input run's table lists and the run's translation drops, the reorganization is abandoned. The step then removes the reorganization and its outputs from the pending entry, keeps its inputs, and leaves the outputs to the collector.

Why are a value's postings and its `texts` row dropped together? A build step skips a value that has a `texts` row, so if only the postings were dropped, a file that later refers to the value would never be indexed or found.

Why does the check abandon the reorganization? Suppose that the value filter doesn't contain a value, and then a merge or a restore commits a file with that value. A build step finds the value's `texts` row in the inputs, skips the value, and advances the watermark past the commit. Publishing the reorganization would then drop the value's rows, and grep would never find the file, with no indexing work left. Abandoning keeps the inputs listed, and the next reorganization's filters keep the value.

Why does the first step write the table and the revision filter? A scan reads every retained revision, so scanning in every step would multiply that cost. With the filter in the output run, later steps prune without a scan, and with one table for all steps, no step keeps a value's postings while another drops its `texts` row. Both are in a run that the entry lists, so they're kept as long as the outputs are.

Why does the scan apply the rule at the floor instead of keeping every revision that it finds? A revision replaced at or below the floor can't be read at any retained sequence, though the metadata lists it until a base compaction of `revisions` drops it. The rule leaves it out as that compaction does, and `pruning` records the floor that the filters describe.

### D.6 Querying the index

This section describes how a query uses the index's postings, value tables, `texts`, and `content_revisions`, and the order and cursor of its pages.

A query reads the namespace at its "read sequence", which is the head for a first page, and for later pages the first page's head, which the cursor records. So every page sees the same files, and a file whose content changed between pages keeps its place in the order. A page whose read sequence is below the retention floor returns `below_retention_floor`.

The query plans its grams and, in each run, intersects the ordinals of the grams' batches, and the candidates are the content values of any run's surviving ordinals. A page reads the walked ordinals' values from each run's table, a batch at a time, and merges the runs in content order, so it reads table rows in proportion to the values it examines. Because ordinal order is content order, a later page finds its cursor's value in each run's surviving ordinals by binary search. For each candidate value, in content order, the query:

1. Reads the value's `content_revisions` rows, and keeps each inode's newest revision whose `committed_seq` is at or below the query's "cut".
2. Checks that the file is in the page's cut: at the read sequence, the file is visible, its current revision is the row's revision, and its path (which a match reports) is within the scope resolved at that sequence. A later page decides this as the service, so the subject's rights don't change the cut. A file whose revision and path are the same at the namespace's head is then served with the subject's rights at the head, as on a first page, and any other file only to a subject with the `history` right, so a page never serves a file outside its cut.
3. Reads and verifies the value's bytes once, and reports the matches for each file that passed.

The cut is usually the read sequence, so a row of a commit above the read sequence (from an index built past that sequence) is left out. A query with `allow_stale` whose unindexed tail is larger than one query can scan uses a lower cut, which is the last commit that the index covers in full (the watermark, or the commit before it when `next_event_index` is above zero). Visibility and current revisions are still read at the read sequence, so a file whose current revision is newer than the cut is left out, and a file deleted after the cut isn't reported.

A later page uses the lower of its cursor's cut and the last commit that its own index covers in full, and when that cut is below the read sequence, it doesn't scan the tail and reports `tail_scanned: false`. So a stale search keeps one cut across its pages, even after the index moves past the read sequence.

The unindexed tail is the files with a revision committed after the watermark and at or below the read sequence. In a fork whose watermark is below the fork point, the tail begins with the gap, which the query reads from the source through the fork snapshot as a step does (see [Index lifecycle](#d2-index-lifecycle)), with no subject, as it reads the fork's own change feed. It checks the subject's rights on each matching file in the fork, as for any tail file. For a tail file's value, the query looks up the `texts` row, reads and verifies a value without a row whatever its grams are, and skips a value with an ineligible row. A value with an eligible row is a candidate only if the posting intersection produced it, which the walk checks on reaching the value. A tail file is ordered with the other files of its value.

Matches are ordered by content value (content ID, then size), then by inode, and then by byte offset. A page reads the bytes of a bounded number of values and checks the state of a bounded number of files, so it can stop inside one value's files. The cursor records where it stopped (the content value, the inode, and the position in the file), with the read sequence, the page's cut, the active index's `captured_seq`, and the request's fingerprint, and the next page reads the value's rows after that inode. A page served by an index with a different `captured_seq` returns `invalid_request`, and the client issues the search again from its first page.

Why does the cursor record the index's `captured_seq`? An index rebuilt after the first page's read sequence (for example, after grep is disabled and enabled again) can have `content_revisions` rows only for the files that were current when its backfill started, and for later commits. A file whose revision at the read sequence was replaced before that backfill started has no row, so a resumed page would leave the file out, and the client would have no way to tell.

### D.7 Grep collection

Core garbage collection collects grep segments under the segment rule (see [Extensions](#124-extensions) and [Candidate and age rules](#113-candidate-and-age-rules)), and an in-progress reorganization's inputs and outputs count as listed. A lost or crashed change's segments are unlisted from the start, and disabling grep, like a namespace tombstone, empties the runs, so their segments become unlisted. A segment that a fork inherited stays in the source's prefix until neither the fork's manifests nor the source's manifest that the fork snapshot keeps lists it.

[api-spec]: api.md
[provider-spec]: object-storage-providers.md
[limits-source]: ../../crates/loonfs-core/src/limits.rs
[fingerprint-vectors]: ../../crates/loonfs-types/tests/golden/commit_fingerprints_v1.json
[name-vectors]: ../../crates/loonfs-types/tests/golden/name_folding.v1.json
