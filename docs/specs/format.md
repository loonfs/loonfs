# LoonFS storage format

LoonFS stores a directory tree, file revision history, and the metadata needed to read and update both in object storage. File contents are stored separately from metadata. Before publishing metadata that references a file's contents, LoonFS writes the complete bytes durably.

For example, an upload may finish even though the subsequent metadata commit fails. The uploaded object exists, but no file references it. To recover the filesystem after a restart, a reader needs a durable record of which changes were committed.

LoonFS records committed changes in an immutable write-ahead log, or WAL. Each WAL object has the next number in the namespace’s log; creating that object commits its records. Periodically, the committed metadata is written into sorted segments, with a numbered manifest listing the segments required to read the directory tree and retained history. A small hint records where discovery can begin. Readers verify the numbered objects and probe forward to find later publications.

Everything required for recovery is stored in object storage. This includes control records and the metadata and content referenced by retained views; preserving only the latest file bytes is insufficient. Local databases and caches can be rebuilt.

This specification defines the storage layout, encodings, read and write protocols, and maintenance rules required for format conformance. An implementation can conform without exposing the LoonFS HTTP API. The [API specification][api-spec] defines that interface separately.

“Must” and “must not” describe format requirements. Examples explain those requirements but do not introduce additional fields or behavior. Implementation defaults are labeled separately from format requirements. Absolute timestamps are Unix milliseconds. Budgets, lease lifetimes, and grace windows are durations in milliseconds.

## Contents

| Section | Subject |
| --- | --- |
| [1. The storage model](#1-the-storage-model) | Namespaces, inodes, names, revisions, deletion, and attributes |
| [2. Objects and references](#2-objects-and-references) | Object keys, numbered manifests and WAL, and discovery hints |
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
| [Appendix A](#appendix-a-durable-records-and-byte-encodings) | Record fields, row keys, and block encoding |
| [Appendix B](#appendix-b-semantic-commit-fingerprints) | Exact fingerprint canonicalization |
| [Appendix C](#appendix-c-timing-and-size-reference) | Timing assumptions, constants, and implementation defaults |
| [Appendix D](#appendix-d-grep-extension-format) | Grep's separately owned durable state |

## 1. The storage model

### 1.1 Namespaces and identity

A namespace is a directory tree with its own ordered metadata history. Its `namespace_id` names a sequence of generations. Generation 1 begins when the id is first created. Creating or forking into an id whose current manifest is deleted begins the next generation. A plain create starts with an empty tree; a fork starts with the source tree. Both continue the id's counters.

Each namespace id starts with manifest number 1. Its current manifest records the generation, identity, creation time, lifecycle, and writer authority. The manifest and later WAL objects together describe the current metadata state. Creating manifest 1 with put-if-absent installs the first generation. Manifest and WAL numbers continue across generations. A plain recreation starts at sequence zero with inode ID 2 available; a fork into a deleted id starts at the captured source sequence, as every fork does. Sequences and inode IDs identify state within one generation.

An item reference retained across generations uses `(namespace_id, generation, inode_id)`. A commit position retained across generations uses `(namespace_id, generation, committed_seq)`. Inode IDs are integers in storage. The public API represents the same IDs as strings such as `ino_42`.

A fork is a new namespace initialized from a retained view of another namespace. It starts with the same files while sharing their stored objects; subsequent commits belong to the target's independent history. Section 9 describes how the shared objects remain retained.

The root directory has inode ID `1`. A newly created namespace starts at sequence `0`, with inode ID `2` available for allocation. New inode IDs are allocated monotonically as part of metadata publication. Renaming a file does not change its inode ID. Deleting a file and creating another at the same path allocates a different ID.

There are two inode kinds: `dir` and `file`. An inode records the item's kind and creation metadata. Its current parent and name are represented by directory bindings, and its file contents are represented by revisions. Neither the path nor the current content reference is stored on the inode row itself.

### 1.2 Commits and revisions

A logical commit is one successfully published mutation request. A request can contain several operations, but those operations are published together at one namespace sequence, `seq`.

A file revision is the content state of one file inode. Its `revision_no` is scoped to that file. Namespace sequences and file revision numbers are different counters: a namespace may process many renames, directory operations, and writes to other files between two revisions of a particular file.

For example, a file might pass through the following history. The sequence values are illustrative.

| Namespace sequence | Operation | Result |
| --- | --- | --- |
| 17 | Create `/reports/Report.txt` | Allocate inode `42`, bind the name, and publish file revision `1`. |
| 18 | Rename it to `/reports/Final.txt` | Keep inode `42` and revision `1`; replace the directory binding. |
| 19 | Replace its contents | Keep inode `42` and publish revision `2`. |
| 20 | Delete `/reports/Final.txt` | Remove that binding and record a deletion for inode `42`. |
| 21 | Undelete that deletion | Rebind inode `42`; retain its revision history. |

The directory state at sequence 18 and the file content state at sequence 19 are reconstructed from different metadata rows. The inode row remains unchanged in both cases.

### 1.3 Directory bindings

A directory binding associates a parent inode and a name with a child inode. The stored `display_name` preserves the caller's spelling. The derived `name_key` is used for sibling-name comparison.

Each bind has a generation consisting of `(bind_seq, bind_delta_index)`. An unbind identifies the exact bind it removes, including the parent, name key, child inode, and bind generation. It does not mean “remove whatever currently has this name.”

For example, suppose a file is moved away from `/reports/Final.txt` and a different file is created there. An unbind of the original generation must not remove the replacement. The generation also distinguishes multiple bindings created within one commit.

The format maintains bindings in two orders: by parent and name, and by child inode. These are the same bind rows, not independent sources of directory state. The child ordering supports parent lookup and the rule that a child has only one current parent binding.

### 1.4 Names and paths

Display names are stored without rewriting their spelling. A display name must be non-empty and at most 255 UTF-8 bytes. It must not contain `/`, any Unicode control character in general category `Cc`, or any of `: ? * | " < > \`. The names `.` and `..`, entirely whitespace names, and names ending in a space or dot are invalid.

The following device names are also reserved, compared case-insensitively and without an extension: `CON`, `PRN`, `AUX`, `NUL`, `COM1` through `COM9`, and `LPT1` through `LPT9`.

These restrictions are intended to reduce common cross-platform naming problems. They are LoonFS's name grammar, not a guarantee that every external filesystem, archive tool, or sync client accepts every possible tree.

A name key is computed in this order:

```text
NFC normalization
    -> full Unicode default, non-Turkic case folding
    -> NFC normalization
```

Both normalization and folding use Unicode 17.0.0 data. There is no per-namespace case-sensitivity setting. Admission and lookup use the same rule. Name keys follow the same character restrictions as display names, with a 768-byte limit.

For example, `Report.txt` and `report.txt` have the same name key, so they cannot be separate siblings. The chosen display spelling is still preserved. The [name-folding fixtures][name-vectors] cover normalization and case-folding cases beyond ASCII.

An absolute path starts with exactly one `/`. It has no empty components, repeated separators, or trailing `/`, except for the root path `/`. A canonical path is limited to 4,096 UTF-8 bytes and 128 components. Noncanonical input is rejected rather than rewritten. These are limits on canonical paths; they do not establish universal compatibility with external filesystem path limits.

A change to any name-key mapping changes the format semantics, even if the serialized fields stay the same. Implementations must preserve the Unicode behavior specified here; lowercase conversion alone is insufficient.

### 1.5 File contents and ownership

Each file revision contains one `ContentRef`. The current kind, `blob_v1`, represents a complete file stored as one immutable object. Its bytes are the file bytes; LoonFS does not add an envelope around the content object.

A reference contains the original owner namespace, that owner's generation, a random content ID, the complete size, and a full-object checksum. It does not contain a bucket address or object-store path.

The owner namespace and content ID in the reference determine the key:

```text
namespaces/{owner_namespace_id}/content/{content_id}
```

The original owner's manifest is not required to read inherited content. This matters for forks: a descendant can continue reading the exact objects in its pinned basis after the source namespace is deleted.

New uploads are owned by the namespace generation that creates them. Inherited references retain their original owners and owner generations. Forking, restoring a revision, replaying the WAL, or compacting metadata must not replace either value with the current namespace or generation.

Two separate uploads of identical bytes create two objects. There is no cross-upload content deduplication. A retry within the same upload session reuses that session's identity; retry behavior does not depend on discovering another upload with identical contents.

### 1.6 File and subtree deletion

Deleting an item removes its current binding and records a subtree tombstone. A tombstone at a directory hides that directory and its descendants without requiring a separate tombstone for every descendant.

Tombstone events are ordered by `(seq, delta_index)`. A `set` event starts a deletion. A `revoke` event identifies the exact `set` generation it cancels. At a given sequence, the latest event for the deletion root determines whether the tombstone is active. A later `set` is a new deletion, even if an earlier deletion of the same inode was revoked.

An undelete request identifies the deletion by inode and committed deletion sequence. Validation must confirm the currently active deletion. A request for a deletion that is no longer active returns `not_deleted`; it must not cancel a later deletion. A deletion created earlier in the same uncommitted request cannot yet be addressed by its committed deletion sequence. Only the root of a deletion can be undeleted independently; descendants hidden by that root do not each have a separate deletion to revoke.

A `set` stores the removed binding in `deleted_binding`, with `parent_inode_id`, `name_key`, and `display_name`. This information remains available after old bind and unbind rows are compacted. An undelete restores that parent and name unless the request supplies a different destination. A `revoke` stores its target generation and must not contain a deleted binding.

The derived `active_deletions` family supports listing recoverable deletions in deletion-sequence order. A `set` produces a `listed` row. A revoke produces a `removed` row for the same `(deletion_seq, root_inode_id)`. Removal rows sort before listed rows, so a scan can suppress cancelled entries. Tombstone events remain authoritative.

File deletion does not immediately delete content. Within a live namespace, file revisions and recoverable deletions are retained under the rules in [section 10](#10-retention-and-compaction). Namespace deletion is a separate lifecycle transition described in [section 9](#9-namespace-lifecycle-and-forks).

### 1.7 Attributes and attribution

Attributes are a string-to-string map associated with an inode. They can contain application tags, display hints, or similar metadata. A rename, move, or new file revision does not modify the existing inode's attributes.

Attribute keys are compared exactly, without normalization or case folding. A key is 1–128 UTF-8 bytes and cannot contain a Unicode `Cc` control character. Keys beginning with `loonfs.` are reserved for system use and cannot be written by callers.

Values are UTF-8 strings of at most 4,096 bytes. Empty strings and control characters are valid values. Removing a key is an explicit operation; an empty value does not remove it. A map contains at most 100 entries and at most 65,536 logical UTF-8 bytes, counting all keys and values without serialization overhead. Invalid durable maps are rejected, not truncated.

An inode begins with an empty map at attribute revision `0`; that initial state has no persisted attribute row. Each accepted update increments `attributes_revision_no` by one and stores the complete resulting map. An accepted update advances the revision even when the resulting map equals the previous one. Clearing the map is a real update and must remain distinguishable from an inode whose attributes were never changed.

Attribute revision numbers support optimistic concurrency. The API does not expose a separate history-listing interface for old attribute maps. The storage layer nevertheless retains the rows needed to reconstruct supported sequence-based views, as specified in the compaction rules.

Commits also record a timestamp and optional message. An actor is a required, application-supplied opaque identifier recorded with a logical commit. LoonFS preserves it without authenticating it or resolving profile information. The application supplies a stable identifier with the identity scope it needs. An actor is attribution, not an authorization decision.

Every row that records an event copies the commit's actor and timestamp as `committed_by` and `committed_at_ms`, beside the `committed_seq` and `commit_id` of that commit. The API projects them under the names the API specification defines, such as `created_by` for an inode's creating commit and `attributes_updated_by` for its newest attribute revision.

| Metadata | Actor and timestamp fields |
| --- | --- |
| Inode, file revision, tombstone event, attribute revision, access revision, commit | `committed_by`, `committed_at_ms` |
| Listed active deletion | `deleted_by`, `deleted_at_ms` |
| Bind and unbind | Neither actor nor timestamp |

The root inode in a newly created namespace is attributed to the actor id `loonfs`. A fork inherits the root inode from its source basis; the target manifest's creation time is the creation time of the namespace, not a rewrite of inherited inode timestamps.

These event timestamps are informational. Sequences determine order. Renaming an inode does not change its creation or content timestamps, and directories have no general modification timestamp. Lease and reclamation deadlines have a different role and are covered separately.

### 1.8 Access rows

An inode can carry an access row: a boundary flag and a map from principal to rights. A principal is an application-assigned identity such as a user, a group, or a public audience, named by an opaque id of 1 to 256 visible ASCII characters other than the comma. LoonFS stores principal ids and never resolves them.

The rights are `read`, `history`, `write`, `create`, `remove`, `share`, `manage`, and `admin`. A set of rights is stored as a list of distinct names in that order. A decoder accepts the names in any order and rejects repeated names. A map holds at most 1,000 principals and at most 65,536 bytes of principal ids, and no entry has an empty set of rights. A decoder rejects a repeated principal. Invalid durable maps are rejected, not truncated.

An inode begins at access revision `0` with no boundary and no grants; that initial state has no persisted access row. The root inode of an ACL namespace is the exception: it begins with a row at revision `0` holding the manifest's `root_grants`, materialized at genesis like the root inode row itself. Each accepted update increments `access_revision_no` by one and stores the complete resulting state. A row with no boundary and no grants is a real revision, distinguishable from an inode whose access was never changed.

Each namespace records an access mode in its manifest, fixed at creation: `unrestricted`, or `acl` with a `principal_scope` naming the identity domain of its principal ids and the `root_grants` the root inode holds at genesis, normally `admin` for each initial administrator. Before authorization reads a grant, the request's subject scope must match this value. A fork copies the source access mode and scope. An inode's effective rights for a set of principals are the union of those principals' grants on the inode's own row and on each ancestor's row, walking current parent bindings and stopping after a row whose boundary is set or at the root. A deletion root has no current binding; its tombstone's saved parent supplies that edge. Revisiting an inode during the walk is namespace corruption. `admin` on the root row confers every right everywhere and is never inherited. The `update_access` operation replaces an inode's row; the API specification defines it.

## 2. Objects and references

### 2.1 Storage layout

Content objects live beside their owner's metadata. A fork reads inherited content at the source owner's key. Retirement deletes the exact objects that the tombstone's publication rows and upload sessions name.

```text
namespaces/{namespace_id}/
├── hint.json
├── manifests/{manifest_no:020}.json
├── wal/{wal_no:020}.wal.zst
├── segments/{segment_id}.sst.zst
├── pins/{pin_id}.json
├── uploads/{upload_id}.json
├── content/{content_id}
└── extensions/{extension_name}/...
```

A segment inherited by a fork can remain under an ancestor's namespace. Its descriptor records `owner_namespace_id`; the reading namespace is not substituted into the key. All newly written metadata segments, including compaction output, use the producing namespace's `segments/` prefix.

The key layout is part of the format. Other objects must not collide with these families. Core readers and collectors do not interpret extension-owned keys. Appendix A.8 lists the complete key patterns.

### 2.2 Object roles and mutability

| Object | Role | How it changes |
| --- | --- | --- |
| Namespace manifest | Namespace identity, lifecycle, writer authority, materialized file set, and retention floor | Publish the next immutable number. |
| WAL segment | Ordered commits or a writer fence after the materialized boundary | Create the next immutable number. |
| Hint | Starting point for manifest and WAL discovery | Compare-and-swap; neither number decreases. |
| Metadata segment | Sorted metadata rows referenced by a manifest | Write a new immutable object. |
| Pin record | Retain one manifest for a user, snapshot, or fork | Create and delete; snapshot expiry can be extended by CAS. |
| Upload session | Own a transfer and its completed content until publication or cleanup | Conditional lifecycle transitions. |
| Content object | Complete bytes of one file revision | Write once. |

A manifest publication can change physical layout or control state without creating a logical commit. A WAL publication can advance logical history without creating a manifest. The hint selects neither history nor visibility: its numbers may lag successful publications.

### 2.3 Numbered objects and generated IDs

Manifest and WAL numbers are independent, positive counters scoped to a namespace. Each starts at 1. Object names use twenty decimal digits, so lexicographic order matches numeric order. WAL number 2 is `wal/00000000000000000002.wal.zst`.

Each number names one immutable object. A publisher creates the next number with put-if-absent. Competing publishers cannot install different objects at the same number; a loser loads the winner before planning another attempt. The payload's namespace and number must agree with its key.

A pin ID has the form `pin_{manifest_no:020}-{16 lowercase hex}`. The number identifies its manifest; a random suffix distinguishes separate user, snapshot, and fork pins over that manifest. A retired pin uses the first sixteen lowercase hex characters of the SHA-256 of the UTF-8 bytes of `retired\n{namespace_id}\n{manifest_no:020}`. Its id is `pin_{manifest_no:020}-{derived suffix}`, so repeated recreation attempts over one tombstone address the same record. The API calls a pin id a `checkpoint_id`; the durable record calls it `pin_id`.

Content IDs are `con_` followed by 32 random lowercase hexadecimal characters. Metadata segment IDs are also generated identities, rather than positions in publication history. A collision at a newly generated immutable key must not overwrite existing bytes.

### 2.4 The current manifest and WAL tip

Creation and fork write the hint before manifest 1, nothing deletes a live namespace's hint, and a reader that finds no hint reports the namespace as absent without listing.

The current manifest is authoritative for the namespace's identity, status, writer epoch, and compactor epoch. Its runs describe materialized metadata through `head_seq`, and `folded_wal_no` identifies the WAL boundary already included in those runs.

Every WAL object records the head sequence, head commit ID, and inode allocation high-water mark after it. A fence advances the WAL number and writer epoch without adding a logical commit, and repeats the head commit ID and allocator it received.

The counters have different meanings:

| Counter | Advances when |
| --- | --- |
| `manifest_no` | A manifest is published, including a layout, authority, retention, or lifecycle change. |
| `wal_no` | A WAL object is created, including an empty writer fence. |
| `seq` | A logical mutation request commits; one WAL object may contain several sequences. |
| `revision_no` | A new content revision is committed for one file inode. |

For example, manifest 5 may cover WAL 8 through sequence 40. WAL 9 can be a fence at sequence 40, and WAL 10 can contain commits 41–43. The read view is then at sequence 43. A flush can publish manifest 6 covering WAL 10 without allocating sequence 44.

### 2.5 Manifest references

A durable reference to a manifest contains:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | Namespace storing the manifest. |
| `manifest_no` | Number determining its object key. |
| `head_seq` | Head sequence recorded by that manifest. |
| `payload_checksum` | Digest that must match its stored payload. |

A pin refers to a manifest under its own namespace and stores these manifest fields directly, using its `namespace_id` as owner. A fork basis embeds a reference to a source namespace and records the source pin ID. Segment references inside that manifest retain their own owners; not every segment must belong to the manifest's owner.

Readers validate the referenced identity, sequence, and checksum. A missing or corrupt required object is an error, never permission to substitute another manifest.

## 3. Object-storage requirements

### 3.1 Required operations

The object-store layer must provide create-if-absent for immutable publication, compare-and-swap for mutable control objects, full and ranged reads, deletion, and reliable prefix enumeration. Operations must remain within the configured storage scope.

A compare-and-swap (CAS) replaces an object only if its compare token still matches the version read by the caller. If another writer has changed that version, the update fails its precondition. Conditional updates must distinguish this result from a transport error. A confirmed precondition failure means the inspected state was not replaced. A transport failure after sending a request may leave the outcome unknown.

A mutable-object read must return the bytes and the compare token for those same bytes in one observation. Reading metadata and content separately is insufficient: a concurrent update could otherwise pair one version's payload with another version's token. Compare tokens, including ETags, are opaque. They are not assumed to be content digests.

Successful writes and deletes require strong consistency. Prefix enumeration returns keys in ascending lexicographic order. The collection protocols depend on this behavior; “S3 compatible” is not, by itself, evidence of conformance.

Readers determine committed state from verified manifests and numbered WAL objects. Collectors also use listings to find candidates and dependencies. In particular, namespace retirement requires a complete pin scan after deletion. The format does not assume a multi-object transaction or a snapshot across separately read control objects.

### 3.2 Immutable writes and retries

An immutable object must not be replaced with different bytes. A collision at a newly generated content or segment key is an error; contention at the next manifest or WAL number follows its publication protocol. Where an operation retries the same object identity, it may reconcile an ambiguous write only using evidence that establishes the expected immutable contents.

Small control objects use conditional single-object writes. Multipart upload is an optimization for larger immutable payloads, not a substitute for atomic control-object CAS.

Multipart completion must preserve immutable content identity. When completion cannot atomically require an absent key, the adapter must prevent concurrent writes through exclusive upload-session ownership and check for an existing object before completion. An absence check alone is insufficient because another request could write the key before completion.

An adapter with conditional multipart completion can use that capability. In either case, the session protocol in section 5 applies. See the [provider documentation][provider-spec] for supported transfer constraints.

The incremental-write adapter consumes the payload before evaluating its existence precondition. A digest calculated while forwarding the stream therefore covers the complete payload even when the write is refused. When multipart completion cannot enforce create-if-absent atomically, its final absence check is not sufficient concurrency control; exclusive session ownership remains required.

A failed or cancelled multipart transfer must attempt provider-side abort. Abort failures can leave incomplete parts, so deployments also need the provider cleanup policy assumed by their adapter. An abort does not imply deletion of an already completed content object; object cleanup follows the upload's durable lifecycle.

### 3.3 Timing assumptions

Writer fencing and commit order are based on epochs, sequences, and conditional writes. Upload admission, snapshot expiry, and reclamation also use deadlines.

The collection protocol assumes bounded clock error and bounded publication and provider-operation times. These assumptions are stated in [Appendix C](#appendix-c-timing-and-size-reference). A client-side timeout is not proof that a remote mutation had no effect; ambiguous operations must retain their documented unknown-outcome behavior.

Section 11.4 explains the clock assumptions and their limits.

## 4. Reading a namespace

### 4.1 Resolving the metadata basis

A cold read loads `hint.json`, loads the manifest at its `manifest_no`, and probes successive manifest numbers until the first not-found response. That selects the current manifest. A lagging hint is expected; it is a starting point, not a statement that later objects do not exist.

| Discovery result | Interpretation |
| --- | --- |
| Missing hint | Namespace not installed. |
| Hint names manifest 1, which is absent | Installation has not completed. |
| A higher hinted manifest is absent | Corruption; hints cannot run ahead of publication. |
| A required object is unreadable or invalid | Error; do not treat it as absence. |
| Current manifest is deleted | `namespace_deleted` for ordinary namespace operations. A create or a fork into the id recreates it as its next generation. |

Every namespace has its own manifest from installation. A newly created namespace's empty manifest represents root inode 1 at sequence zero. A fork's initial manifest lists the source runs it inherits. Its `fork_basis` records provenance and the retaining pin; readers do not follow that field to select a different basis.

The current manifest plus the unfolded WAL forms one read view:

```text
hint ── starting number ──> manifest 5 ── probe ──> manifest 6 ──> absent 7
                                                  │
                                   ┌──────────────┴──────────────┐
                                   │                             │
                              listed runs                folded WAL = 10
                                   │                             │
                             metadata rows              replay WAL 11, 12
                                   └──────────────┬──────────────┘
                                            read view
```

This example assumes 12 is the discovered WAL tip. Manifest and WAL discovery must also account for a concurrent manifest publication, as described below.

### 4.2 Replaying the visible WAL

After selecting a manifest, discover the WAL tip by probing consecutive numbers. If the hint names a WAL number above `folded_wal_no`, load that object and probe forward from it; otherwise probe from the folded boundary. The first absent successor ends discovery. Replay still requires every WAL number between the folded boundary and the discovered tip, including numbers below the hint.

Each data segment must contain contiguous commits following its `prior_head_seq`. Namespace identity, WAL number, sequence range, head commit ID, allocation state, and writer epoch must validate. Empty fence segments contain no metadata changes. Epochs cannot decrease along the log or exceed the current manifest's epoch. If a WAL object exposes a newer epoch, reload the manifest before deciding that the object is invalid.

After WAL discovery, check for a successor to the selected manifest and reload if one appeared. This prevents a concurrent fold or retention advance from making a reclaimed WAL number look unused. Required missing or malformed objects fail the read.

A read is evaluated at one sequence. Replaying the WAL produces a projection of its metadata changes and any file bytes stored inline. This is the *projected WAL tail*. An implementation may query verified metadata segments and this projection directly, without loading every metadata row into memory, provided it applies the same visibility rules.

For a warm read, the reference runtime probes the next WAL number with GET. An absent object confirms the cached tip; a present object requires advancing the state and probing onward. On a monotonic revalidation interval, defaulting to one second, it also probes the next manifest number with HEAD. A successor triggers discovery again. This is how cached readers observe deletion and retention changes. The hint is not used to validate a cached view. The acknowledging runtime seeds its read caches from the publication. Its next read probes the next WAL number like any warm read. If the manifest and WAL tip are unchanged and the seeded projection is retained, it replays nothing and does not reload the manifest.

### 4.3 Visible metadata

At sequence `N`, ignore events after `N`. An inode must have been created by `N` and must not be covered by an active tombstone on itself or an ancestor.

A directory binding must be the current binding for its parent-and-name slot and the current parent binding for its child. A matching unbind removes only the targeted generation. Sequence and delta position determine the order when several events affect the same item.

A file's current content is its latest revision committed by `N`. Attributes are the latest applicable complete attribute revision, or the initial empty map when no applicable attribute row exists. Recoverable-deletion listing uses the derived active-deletion state; historical tombstone evaluation remains based on tombstone events.

### 4.4 Paths, listings, and revision reads

Resolve a path from root inode `1`, folding each component into its name key and following the active binding. If a component is not visible, the path does not exist. The current format has no mount traversal.

A directory listing resolves visible child bindings and uses committed revision metadata for file size and content-reference summaries. It must not fetch and verify every file's content object simply to list a directory.

A path-based revision read first resolves the current inode at that path, then looks up the requested revision of that inode. It is not a request for every file that ever occupied the path. An inode-based revision read addresses the retained history of that inode directly.

### 4.5 Content verification

File bytes can be stored in a content object or included directly in a WAL commit. In either case, the file revision contains a content reference with the owner namespace, content ID, size, and checksum. Use the read view to determine where to read the bytes:

| Condition | Where to read |
| --- | --- |
| The reference belongs to the namespace being read, and its content ID is present in the projected WAL tail | Use the inline bytes already in that projection. |
| Otherwise | Read the content object using the reference's owner namespace, owner generation, and content ID. |

Content inherited through a fork is always read from a content object. WAL replay loads inline bytes along with the metadata, so reading them from the resulting projection needs no separate content request.

Every content read must validate the reference's kind and checksum algorithm, then verify the complete byte length and checksum. These checks apply to both sources. WAL replay verifies the record envelope but does not recompute each file's checksum. A HEAD request can check an object's existence and size, but the content read must still verify the bytes. A missing required object or a failed validation must fail the read.

Before materializing inline bytes for a direct download, verify that the current manifest has the view's generation. An earlier generation returns missing content. This check also applies to retained in-memory views. Reclaiming a WAL object does not remove bytes already held in a read view's projection. If that projection must be rebuilt and the required WAL objects are gone, the read must fail. Finding a content object is insufficient because the missing WAL is also required to reconstruct the view's metadata.

For a streamed read, the full-file checksum is verified only after the complete stream has been processed. The transport must report a verification failure even if some bytes have already reached the client. For provider-direct downloads, the client receives bytes directly from object storage; the [API specification][api-spec] defines its verification responsibilities.

### 4.6 Speculative reads

The embedded runtime can reduce latency by reading a small file while checking whether its cached view is still current. This optimization applies to nonempty files of at most 64 KiB and must respect any smaller configured limit on buffered reads.

First, resolve the path in the cached view. Start the normal freshness check described in section 4.2 and the content read together. If the namespace read state and manifest are unchanged, the cached resolution is still valid. Otherwise, resolve the path again in the current view.

Use the speculative bytes only if the current path has the same complete content reference. Return the entry from the current view. If the reference changed, discard the earlier result, including any content error, before reading the current content once. An error resolving the current path takes precedence over the speculative result.

For example, if `/report.txt` now refers to different content, discard the old file bytes and read the replacement. If the path was deleted, return the current metadata error even if the old bytes were read successfully.

For content objects, request a byte range covering the declared size plus one extra byte. The extra byte detects an object larger than its reference claims; the read must still verify the exact length and checksum. A size error reports the length observed in that range, which may be less than the full object size. Inline content uses the bytes in the projected tail and requires no content request.

Reading speculative bytes does not by itself establish freshness or change retention. The ordinary metadata checks remain required.

## 5. Uploading content

### 5.1 Upload sessions

Every new content object is associated with an upload session before it becomes eligible for metadata publication. The content ID is allocated when the session is created, before the file bytes are read. New content belongs to the session's namespace generation and is stored at `namespaces/{owner_namespace_id}/content/{content_id}`. A session belongs to the generation in which it opened.

An upload session contains `namespace_id`, `owner_generation`, `upload_id`, `content_id`, `created_at_ms`, optional `subject_id`, a tagged `mode`, and a tagged `status`.

| Status | Stored fields | Meaning |
| --- | --- | --- |
| `open` | `expires_at_ms` | Upload work is still permitted under the session lease. |
| `completed` | `completed_at_ms`, `content_ref` | The object was verified and its reference is available for admission. |
| `aborted` | `aborted_at_ms` | The upload cannot complete or reopen. |

A session starts open and makes at most one terminal transition. Completion and abort race on the same CAS-protected record. Completion verifies content before recording `completed`; abort records `aborted` before cleaning up the object or provider transfer.

Upload records do not advance namespace sequence and are not filesystem change-feed events. Completion is not a file commit. A completed upload may never be referenced by a file.

### 5.2 Transfer modes

The mode is fixed for the session's lifetime.

| Mode | Durable mode data |
| --- | --- |
| `service_proxied` | Tagged staging state: `idle`, `claimed`, or `staged` with a `content_ref` |
| `direct_put` | The selected whole-object `checksum_algorithm` |
| `direct_multipart` | `provider_upload_id`, `part_size_bytes`, and `checksum_algorithm` |

Multipart part progress remains client-side. The part geometry and algorithm remain on the session so resumed work uses the same settings. Provider upload identifiers are retained where required for completion recovery and cleanup.

Every staged or completed reference must match the session's content identity, namespace ownership, and owner generation. Completed direct-upload references must use the algorithm recorded in the mode. Missing required fields, inconsistent references, or invalid mode/status combinations are corrupt.

### 5.3 Service-proxied staging

A staging request conditionally changes the session's staging state from `idle` to `claimed` before writing content. A concurrent staging request that finds the claim cannot write the same object. When staging succeeds, the request stores the verified reference and releases its claim in the same record update.

A retry against already staged content compares the incoming content with the staged reference rather than overwriting the object. The same bytes can be accepted as a retry; different bytes conflict. The claim has no independent expiry. After a request is cancelled, the claim can remain active for the remainder of the session lease.

Completion clears service-proxied staging to `idle` in the same CAS that records the terminal completed reference. Abort also clears staging to `idle`. A terminal record must not retain a second staged content description, even if the two references would agree.

Service-proxied staging calculates SHA-256 while streaming the bytes. The exclusive staging claim prevents a second request from writing the same content object, including during multipart completion.

### 5.4 Direct uploads

A direct upload transfers bytes from the client to the provider under a short-lived capability. The server allocates the content identity and constrains the target key. Clients cannot choose an arbitrary object key through this interface.

Direct PUT uses the algorithm selected when the session begins. Direct multipart uses the algorithm retained in the session, currently CRC-64/NVME. The provider must enforce the signed transfer constraints and expose the stored whole-object checksum in the supported algorithm.

At completion, the server compares the provider's stored size and checksum with the completion claim. A client-supplied digest alone is not sufficient. A rejected claim must not trigger deletion of content that a concurrent completion has already made eligible for publication. Cleanup of unusable content follows the session's conditional terminal transition.

Provider-specific checksum headers, completion APIs, and response encodings belong in the provider adapter. A LoonFS checksum has the same canonical representation regardless of whether the provider returned hexadecimal or base64 data.

### 5.5 Admission proofs

Content is admitted only with evidence that the named bytes were verified in a completed upload. The evidence is bound to the namespace and the complete content reference. A match on the content ID alone is not enough.

An inline reference owned by the committing namespace must name its current generation. Otherwise the commit is invalid because the inline content is not owned by that namespace generation. A staged reference owned by the committing namespace must also name its current generation; otherwise it is `content_not_prepared`. A reference owned by another namespace is not checked against the committing namespace's generation. These checks still apply when a token or receipt otherwise admits the reference.

The evidence expires. A completed session can produce evidence only during `COMPLETED_UPLOAD_RECEIPT_WINDOW_MS` after its original completion time. A status read or cached response does not restart that window, and nothing is issued at or after its end. A signed token lasts `CONTENT_RECEIPT_TTL_MS` from issuance. Evidence prepared in process without a token expires no later than the last token its session could have issued.

The reference HTTP server mints signed content tokens as its representation of this evidence. An embedded implementation need not mint tokens. Token encoding, signing, and HTTP responses are defined in the [API specification][api-spec].

Immediately before publishing newly accepted requests, the writer checks that externally supplied content references have matching, unexpired admission evidence. The check includes time spent acquiring or checking the writer, loading the view, planning, and preparing the WAL. It uses the request clock plus the attempt's elapsed monotonic time, rather than the original request timestamp alone.

A retained matching commit receipt replays without new content-admission evidence. Internal copy and restore operations retain references already established by the validated namespace state; they do not authorize arbitrary cross-namespace imports. An import outside the pinned fork relationship writes verified bytes under a fresh destination-owned identity.

The receipt window, token lifetime, and publication bound determine the earliest safe collection time for a completed but unreferenced upload. The exact calculation appears in section 11.6 and Appendix C.

## 6. Publishing a commit

A metadata commit becomes visible when put-if-absent creates its numbered WAL object. Upload completion and validation precede this boundary; neither alone commits a file change.

### 6.1 Writer ownership

A writer session acquires authority lazily, before its first semantic publication. It publishes the next manifest with `writer_epoch + 1` and a diagnostic writer block, then creates a zero-record fence at the next WAL number. The session uses that epoch for later batches.

Another session can acquire a higher epoch. Its numbered fence prevents an older writer from extending the log using a previously observed tip: the stale writer's put collides, discovery observes the higher epoch, and the session returns `writer_fenced`. A fenced session does not automatically reacquire authority.

A fence has `head_seq` equal to `prior_head_seq`, preserves `next_inode_id` and `head_commit_id`, and contains no commit records. It advances WAL position without advancing logical history. Concurrent attempts are serialized by conditional creation of the next number.

There is no writer lease or writer-expiry timestamp. The `writer_id` and `acquired_at_ms` fields describe the acquisition; the epoch determines authority. An acquisition retried after an uncertain outcome can advance the epoch again. Commit retry identity is separate and uses durable receipts.

### 6.2 Validation

Before planning new mutations, the writer reconstructs the metadata state from a verified basis and the visible WAL. It resolves paths and inode references, checks content admission, evaluates preconditions, and allocates new inode IDs from `next_inode_id`.

Operations in one request are evaluated in order. Later operations can observe the tentative effects of earlier operations in that request. When several requests share a publication batch, the writer chooses their order and evaluates them against the preceding accepted requests as well.

The writer must check that a content reference has a supported kind, a correctly encoded checksum, and applicable evidence that the object is durable with the stated size and checksum. Content validation precedes metadata preconditions. Previously established content can be reused only through the namespace's validated state or the admission paths described in section 5.5.

Metadata preconditions include namespace generation and head sequence, name-slot availability, exact binding generations, file and attribute revisions, ancestor visibility, and directory emptiness. The internal exact-binding check is:

```text
binding_is(parent_inode_id, name_key, child_inode_id, bind_seq, bind_delta_index)
```

Checking the inode ID alone is not equivalent. An item may have been moved away and rebound under the same name since the caller observed it.

Caller-supplied `expected_*` preconditions add specific checks. Omitting an optional precondition disables that check; it does not disable the operation's normal structural validation. Where a revision precondition accompanies an optional inode precondition, the revision precondition requires the matching inode precondition. Inode-addressed revision writes require `expected_revision_no`, and inode-addressed moves and deletes require `expected_binding_generation`.

A rejected request receives no sequence number and creates no WAL record. Passing validation is tentative acceptance, not success.

### 6.3 Publication and group commit

The publication procedure is:

1. Resolve retained commit IDs, then validate new requests against the discovered view and earlier accepted requests in the batch.
2. Refuse new commits with `maintenance_required` when the unfolded WAL reaches its write-stop threshold. Fence objects count toward that threshold.
3. Assign contiguous sequences to accepted requests and construct one object at `tip + 1`, including the resulting `next_inode_id`.
4. Check the publication budget and content-admission evidence immediately before the put-if-absent.
5. On success, update the local read state and acknowledge the requests. Raise the hint first if a raise is due; a failed hint update does not fail the commits.

The publication budget is measured from observing the tip used to plan the batch until initiating its numbered put. A cached tip has the same time limit. An expired attempt reloads and re-plans before writing. Appendix C records the bound.

For example, three requests accepted after sequence 40 can be written together as sequences 41, 42, and 43 in WAL object 10. Creating that object commits all three. They remain separate logical commits, while a request containing several operations remains one commit.

```text
upload and verify content
           │
           v
validate requests A, B, C against the current view
           │
           v
put-if-absent WAL 10: [seq 41, seq 42, seq 43]  ← commit boundary
           │
           v
raise hint if due, then acknowledge
```

A confirmed precondition failure creates no object. The writer discovers the winning publication, checks for fencing, and re-plans before trying another number. It does not create a parallel branch of history.

The reference writer raises the hint when at least eight WAL objects have accumulated since its last raise or the revalidation interval has elapsed, whichever occurs first. Each CAS takes the greater of the old and proposed numbers. A stale compare token requires rereading the hint. Failed raises are retried at a later trigger. Readers remain correct while the hint lags because discovery probes forward.

### 6.4 Failed and unknown outcomes

A transport error after the numbered WAL put was sent is not necessarily a failed commit. The put may have succeeded even though its response was lost. An implementation that cannot establish the outcome must report it as unknown, not as a definite failure.

The caller should retry with the same commit ID and the same logical request. Re-uploading the bytes first creates a new content identity and is not the same request.

A publication batch is not an all-or-nothing transaction across every candidate request. Some requests can fail validation while other requests are accepted. A validation error that depends on another tentative request can be reported as such only if that request is published.

Suppose request A creates `/reports` and request B also tries to create `/reports`. B may fail because of A's tentative creation. If the publication of A then fails, the writer must report the publication failure for B as well, not claim that B conflicted with a committed directory. A rejection based entirely on the previously durable view does not have that dependency and can stand independently.

### 6.5 Commit identity and retries

Every WAL commit and commit receipt stores a `semantic_commit_fingerprint`. It represents the logical request: `namespace_id`, `actor_id`, ordered operations with their inline preconditions, request-level preconditions, and optional message. It excludes publication details such as the writer epoch and timestamp. Appendix B specifies the exact canonical bytes.

The materialized file set also stores each retained commit's record in the `commits` family, keyed by sequence. A replay rebuilds its response from that row.

While the receipt is retained, an equal fingerprint under the same `commit_id` identifies a replay of the original commit. A different fingerprint returns `commit_id_reuse_conflict`. A replay does not execute the mutation again or reevaluate its original preconditions against current state.

Inline content is identified by its bytes. While the commit receipt is retained, retrying the same request with the same inline bytes returns the original commit, even if a new content ID was assigned. Changed bytes or a different subject return `commit_id_reuse_conflict`.

Before returning a retained publication receipt, check for a successor to its manifest with HEAD when manifest revalidation is due. A present successor invalidates the projection and requires ordinary publication discovery and writer fencing. Repeated replays share that check for the namespace within the revalidation interval. Check for the commit receipt before uploading inline bytes as content objects. If the receipt is still available, return the original result for an identical request or a reuse conflict for a changed request. Neither requires another upload, even after a restart or on another server.

The guarantee is bounded by retention. Receipts below the retention floor can be removed during compaction. Once a receipt is gone, the old ID cannot be distinguished from an unused ID and a later request can execute as a new mutation. A receipt that has not yet been compacted may still be available, but callers must not depend on that extra lifetime.

Receipt lookup remains available at the WAL write-stop threshold. A retained matching receipt returns the original result, and a retained conflicting receipt returns `commit_id_reuse_conflict`. Only new commits are rejected with `maintenance_required`. Writer-session, availability, and corruption checks still apply.

File revision history and commit idempotency have different retention rules. Keeping an old file revision does not require retaining its commit receipt forever.

### 6.6 Operations and WAL deltas

Standard requests operate on paths or inode IDs. They create directories, write files, move or copy items, delete and undelete items, restore file revisions, and update attributes. Their exact parameters are listed in Appendix B because those parameters also determine retry identity.

The default destination behavior for puts, moves, and copies is `no_replace`. Deletes default to `non_recursive`, directory creation defaults to `parents: false`, and attribute `set` and `remove` collections default to empty. Optional preconditions have no implied value.

A replacing move deletes the destination file and rebinds the source within the same logical commit. Only a file destination can be replaced; moving a path onto itself is not a replacement. An undelete can use the deleted binding's original parent and name or the caller's replacement path, subject to normal validation.

The WAL stores the resulting metadata changes, not the original request bodies or validation inputs. The delta kinds are `create_inode`, `bind_direntry`, `unbind_direntry`, `append_file_revision`, `tombstone_subtree`, `revoke_subtree_tombstone`, `append_attributes_revision`, and `append_access_revision`.

Each delta has a `delta_index`, and its wrapper has a `semantic_operation_index` identifying the internal operation that produced it. A convenience request can expand into several internal operations, such as creating missing parent directories. Each operation's deltas are contiguous within its commit. Actor and timestamp are recorded once per commit and copied into the appropriate rows during materialization. The complete stored fields appear in Appendix A.

### 6.7 Change feed

The change feed is ordered by logical commit, not by physical WAL object. A segment containing three commits contains three commit boundaries in the feed. Within a commit, semantic filesystem events follow request-operation order; one operation can produce several events.

A consumer resuming after sequence N reads the `commits` family of the current file set and then the commits in the unfolded WAL after it. Fence objects produce no change events. If its cursor is older than the retention floor, it must bootstrap from a fresh checkpoint instead. Sequences restart when a deleted namespace ID is recreated, so a cursor from an earlier generation is refused when it is below the retention floor or above the head. A cursor within that range is accepted. A consumer that can span a recreation compares the namespace's generation. The API's event shapes and cursor contract are specified in [the API specification][api-spec].

A consumer that requires permanent event history must retain its own copy before the floor advances. Use `(namespace_id, generation, committed_seq)` for a commit's position and `(namespace_id, generation, inode_id)` for item identity. A commit ID is useful for correlation, but is not a permanent unique event key because it can be reused after receipt reclamation.

## 7. Materializing metadata

Replaying a longer WAL requires more object reads and more work. A flush materializes committed metadata into immutable sorted segments and publishes a manifest describing those segments. Subsequent reads start from that manifest and replay only the later visible WAL.

This changes the physical representation, not the namespace's visible history. A flush does not allocate a logical commit sequence or create a pin record.

### 7.1 Manifests, runs, and segments

A namespace manifest describes one complete metadata file set through `head_seq`. It includes the head commit ID, inode allocator, folded WAL number, retention floor, and all metadata runs required to reconstruct that state. Its `manifest_no` determines its immutable key; only one publication can succeed at that number.

A run is the collection of segments produced together. `run_no` is allocated from the manifest's `next_run_no`, which advances when that run is published. A WAL flush allocates one run number across the families it writes. A compaction allocates a run number for its selected family group.

Each run records `run_seq`, `tier`, and its segment descriptors. A non-empty manifest must have a run at `base_seq` and a run at `head_seq`, and all run sequences lie between them. An empty active manifest has equal head, base, and floor sequences and the genesis commit id. Within one run, each family's segments have dense, zero-based `segment_index` values and strictly separated ascending key ranges. Different runs can overlap because a later run can contain additional rows for the same inode, name slot, or revision history.

The manifest must not contain duplicate run numbers or a run number at or above `next_run_no`. A family's segment ranges must not overlap or descend. Metadata producers must not write the same logical row key twice within one run.

Metadata rows describe immutable facts at specific positions. Reads merge those facts and apply the visibility rules, rather than choosing arbitrary values for a conflicting row key. The parent-and-name binding family and child-binding index contain the same bind records in different orders. Manifest validation checks their per-run row counts; reorganization checks full row-level equality across its selected complete input runs.

The `commits` and `commit_receipts` families hold one row each per retained commit; manifest validation checks their per-run row counts the same way.

### 7.2 Publishing a materialized file set

A flush starts from the verified manifest and discovered WAL tip. It materializes the required numbers after `folded_wal_no`, writes new segments, and publishes the next manifest with `folded_wal_no` set to the captured tip. A fence is folded even when the logical sequence does not change. Ordinary flushes write a run at the head when materializing new state.

Before writing segments or publishing the manifest, a flush writes every inline value it covers as a content object, verified against its reference. A manifest whose `folded_wal_no` is `n` implies a content object exists for every inline value in WAL segments up to `n`. WAL collection's rule is unchanged because it already requires each segment to be at or below `folded_wal_no`.

Publication uses put-if-absent at `predecessor.manifest_no + 1`. A lost put loads the winning manifest. A flush already covered by the winner needs no further publication; coverage includes WAL position as well as sequence. Otherwise it rebuilds against the new predecessor. Reorganization and compaction additionally require their selected inputs to remain valid.

Within one generation, successors preserve namespace identity and cannot lower head sequence, writer or compactor epoch, folded WAL number, the retention floor, allocators, or cumulative activity counters. A successor at the same head within that generation must preserve activity counters exactly. Deletion is terminal within a generation: a tombstone's only successor is the first manifest of the next generation. That manifest copies the tombstone's folded WAL number, increments both epochs, and otherwise starts exactly as manifest 1 of a create or fork does (section 9.1): active, no writer, zero activity, and either empty with zero sequences, the genesis commit ID, and fresh allocators, or with a fork basis whose captured sequence is its head and retention floor. Manifest numbers, WAL numbers, and epochs continue across the boundary.

The bounded metadata publication budget runs from before the first output segment write until initiation of the manifest put. An expired attempt publishes nothing further. Its unreferenced output remains subject to segment-age collection rules. Streaming compaction has the longer bound in section 10.4.

After publication, raise the hint within the publication budget. Failure to raise it does not undo the manifest. GC preserves manifest numbers at or above the hint observed for its pass so discovery can cross a lagging hint. Intermediate manifests retained for discovery do not independently retain their runs.

Forks follow the same path from their generation's first manifest. Publishing a target-owned manifest does not imply copying inherited segments into the target's prefix; each segment retains its owner.

### 7.3 Recovery material and maintenance policy

After the corresponding WAL is reclaimed, metadata segments are required recovery material. They are not disposable caches. A missing or corrupt required manifest or segment is an error; readers do not select a different file set and silently return another state.

Implementations can flush automatically as the WAL grows. The reference defaults request a flush at 32 unflushed segments and reject new commits with `maintenance_required` at 128. The commit that triggers a flush can finish before the flush completes. Reads and retained-receipt lookup remain available at the threshold.

Flushing does not advance retention. An operator separately decides when older replay history may be discarded. Appendix C lists the reference implementation's sizing defaults.

### 7.4 Statistics

Each manifest stores three cumulative activity counters in a required `activity` object:

| Counter | What counts |
| --- | --- |
| `content_bytes` | Full content length of every committed file revision, including overwrites and revisions that reuse stored content. |
| `file_revisions` | Every committed file-revision append, including an empty revision. |
| `mutations` | Each semantic operation group in a committed WAL record, identified by `semantic_operation_index`. |

For example, writing a 10-byte file and then replacing it with a 6-byte revision adds 16 bytes, two revisions, and two mutations. Creating a directory adds one mutation. Recursively deleting that directory adds one mutation, regardless of how many descendants it hides. Convenience requests can contain several internal operations, so mutation totals can exceed request counts.

A fold adds only the activity after its predecessor's covered position. It publishes the counters, runs, head sequence, and folded WAL number together. A competing publication requires the fold to reload the predecessor and count only the remaining tail. A recognized commit retry adds nothing again. If an expired receipt allows a new durable commit, that commit counts as new activity.

Uploads alone, fence records, pins, inline-content extraction, and compaction add no activity. Retention and deletion do not subtract past activity. Within one generation, every publisher carries forward the counters from the predecessor it actually updates, including when compaction races a newer fold.

Two footprint values are calculated from the manifest's segment descriptors:

| Value | Calculation |
| --- | --- |
| `inode_record_count` | Sum of `row_count` for `inodes` segments. |
| `metadata_stored_bytes` | Sum of `index_block.offset + index_block.stored_bytes` for all referenced segments. |

The inode count includes retained deleted records. An implicit root counts as zero until stored as an inode record. Metadata bytes exclude content, WAL, manifest and pin JSON, unreferenced outputs, and segments referenced only by other manifests. Shared segments count in each referencing manifest; these totals do not measure unique physical storage.

Statistics reads use one validated manifest without reading segments or replaying newer WAL. Observations include namespace identity, generation, lifecycle status, manifest number, head sequence, and folded WAL number. Checkpoint statistics read the current manifest for generation visibility and the pinned manifest for the numbers. They do not discover the WAL tip. Compaction can change footprint without changing the logical head, so the manifest number matters too.

A fork inherits the source manifest's segment descriptors, so its footprint values begin as the source's. Its counters begin at zero. Activity committed in the source stays in the source's manifests.

Counters start at zero in each generation. Meters compare observations for the same namespace and generation. Billing cursors and policy belong to the application.

The counters are public integers within the bound in the API specification; exceeding it is an error, and a counter that regresses within one generation is corruption.

Grep reports its own referenced segment bytes under Appendix D. Its manifest and indexing position are independent of the core observation. A new fork starts without an index; a failed index read must not be reported as zero bytes.

## 8. Pins

A pin is a durable record that holds one manifest. It preserves that manifest and its segments after newer manifests are published. Ordinary flushing creates no pin; applications, operators, and forks create pins when they need a stable retained basis.

### 8.1 Records and owners

Each pin is stored under `pins/{pin_id}.json`. Its positioned ID identifies the manifest number and includes a fresh random suffix. Repeated creates over the same manifest or label create distinct records.

| Owner | Stored owner fields | Lifetime |
| --- | --- | --- |
| `user` | `name`, optional `expires_at_ms` | Explicit deletion, or GC after expiry and grace. |
| `snapshot` | `name`, required `expires_at_ms` | Reads require an unexpired snapshot; GC adds grace before deletion. |
| `fork` | `target_namespace_id` | Retained while the target depends on the source. |
| `retired` | None | Until its generation is reclaimed. |

The record also stores namespace, head sequence, payload checksum, and creation time. The manifest number comes from the pin ID. It has no lifecycle status. Creating the record establishes the candidate pin; deleting it ends the pin. Fork pins have no expiry or renewal protocol.

### 8.2 Creating and verifying a pin

For a pin created from the current head:

1. Flush the observed WAL tail and select a verified current manifest.
2. Write a fresh pin with put-if-absent.
3. Load the current manifest again within `PIN_VERIFY_BUDGET_MS`.
4. Require the same manifest number and payload checksum, an active namespace, and a retention floor no later than the pinned head sequence.

If another manifest became current, delete the candidate pin and retry from a fresh basis. A deletion, an exceeded verification budget, or exhausted contention retries prevents acknowledgement. Store and cleanup failures also prevent acknowledgement. The number and checksum checks apply even when the new manifest has the same logical sequence.

This order matters when collection races with pin creation. A collector either captured the pinned manifest as current, or captured a successor after the pin was durable and includes the pin in its complete listing. Once acknowledged, the pin retains its files even if the retention floor later passes its sequence.

A fork of a snapshot uses a different protecting root. It first writes a fork pin for the snapshot's historical manifest, then rereads the snapshot pin and requires it still to exist and be unexpired. It does not require that historical manifest to remain current. Failure deletes the new fork pin and returns `snapshot_gone` when the snapshot was lost; verification remains time-bounded.

### 8.3 Reads, deletion, and expiry

A read through a pin derives the manifest number from the ID, confirms the pin's existence and owner, and verifies its manifest reference. Reads use that manifest directly, without replaying later namespace history.

User pins remain readable while their records exist, even after an optional expiry. Snapshot reads and extensions require an unexpired snapshot owner. Expiry and physical deletion are therefore different events: an expired snapshot remains a collection root until its record is deleted after grace.

Explicit deletion checks the owner and deletes the pin. Deleting it again returns not-found. Callers cannot delete fork-owned pins through the user checkpoint API. Every pin encountered in a collector's initial listing protects its files for that whole pass, including a pin that the same pass subsequently deletes.

### 8.4 Extending a snapshot

An unexpired snapshot's expiry can be extended by compare-and-swap. The manifest reference, identity, and owner do not change. An extension cannot recreate a deleted pin or make an expired snapshot usable again. See the API specification for duration and request constraints.

## 9. Namespace lifecycle and forks

Namespace installation, deletion, and recreation publish numbered manifests. Retirement is derived from a generation's tombstone and the complete pin listing. It publishes no manifest.

```text
absent → create generation 1 → active → publish deletion → deleted
                                ↑                           │
                                └── recreate generation ────┤
                                                            │
                                      deadline and pin checks pass
                                                            ↓
                                      reclaim the deleted generation
```

### 9.1 Creating a namespace

Read existing namespace state before writing new objects. An existing active namespace returns `namespace_exists`, or its current summary with `allow_existing`. Corruption and read errors are not absence. These completed-namespace checks write nothing.

For an absent namespace, build manifest 1 with the namespace's creation time and application-supplied `created_by`, no fork basis, active status, the genesis commit ID, next inode ID 2, and no runs or writer block. Head sequence, base sequence, the retention floor, folded WAL number, next run number, both epochs, and all three activity counters start at zero.

Write the hint naming manifest 1 and WAL 0, then manifest 1, both with put-if-absent. A hint collision is permitted. The manifest put decides which installation wins. A hint left before that put does not establish namespace existence.

After an unknown transport outcome, identical read-back bytes do not prove who published a generation's first manifest without a fork basis, so a plain create or recreate answers `namespace_exists`, or the existing summary with `allow_existing`.

A plain create or a fork into an id whose current manifest is a tombstone recreates it. A fork first creates and verifies its source pin under section 9.2:

1. Load the current manifest. If it is active, return `namespace_exists`, or its current summary with `allow_existing`. Otherwise it is the tombstone, and its folded WAL number is the WAL tip of the deleted generation.
2. Check the metadata publication budget, measured from the start of recreation, before writing a retired pin over the tombstone with put-if-absent. An existing record at its derived id is success.
3. Build the next manifest number with the next generation and its own number as `generation_first_manifest_no`. Copy the tombstone's folded WAL number and increment both epochs. For a plain create, head, base, and retention floor start at zero, the inode and run allocators start over, and the manifest is active, has no fork basis or writer, has no runs, carries the genesis commit id, and starts every activity counter at zero. For a fork, copy the pinned source manifest's runs, head, base, head commit ID, and allocators as section 9.2 describes, with the retention floor at the head and activity at zero.
4. Publish the manifest after the tombstone within the metadata publication budget, measured from the start of the create or, for a fork, from before its source pin write. Reload when another manifest wins. An active winner answers `namespace_exists`, or its current summary with `allow_existing`.
5. Publication raises the hint to the new manifest and its folded WAL number. A failed hint raise does not fail creation.

Recreation reads no WAL object. The deleted generation's WAL objects are unprotected and may already be collected.

The retired pin records the tombstone's manifest reference. Its creation time comes from the recreation call.

### 9.2 Forking a namespace

A fork starts independent history from the source's retained metadata:

1. Create a verified source pin whose owner names the target namespace, either from the source head or a live snapshot under section 8.2.
2. Load and verify the pinned manifest.
3. Copy its run references, base sequence, head commit ID, inode allocator, and next run number into the target manifest. The initial head is the captured source sequence, for a fresh id and for a deleted id alike. Preserve every segment's owner.
4. Set target identity, creation time, and `created_by` from the fork request, immutable `fork_basis`, active status, and no writer block. Activity counters start at zero and the retention floor equals the head. For a fresh id, both epochs and the local folded WAL number start at zero. For a deleted id, apply the generation, manifest, WAL, and epoch rules from section 9.1; copy the source's allocators.
5. Install the target exactly as section 9.1 installs a create: for a fresh id, the target hint naming manifest 1 and WAL 0, and target manifest 1, in that order; for a deleted id, the retired pin and the successor to the tombstone.

The target copies no file bytes or metadata segments. Its head is at least the captured source sequence and every copied run sequence. Its WAL starts at number 1 for a fresh id, or after the tombstone's folded WAL number for a recreated id. Its first data commit is one sequence above its initial head. It can itself be forked immediately because its manifest already lists its inherited runs.

The fixed creation grace on the source pin protects installation. Before initiating the target manifest put, the installer checks the time elapsed since before the source pin write against `METADATA_PUBLICATION_BUDGET_MS`. The remaining grace covers provider operations and the clock allowance.

### 9.3 Conflicting and unknown installations

A losing manifest-1 put reads the winner and verifies its namespace identity. Current active status means `namespace_exists`; current deleted status enters the recreation procedure for either a create or a fork. A recreation manifest put that loses reloads the current manifest. An active winner means another recreation succeeded. A newer tombstone means another generation was created and deleted, so recreation retries over that tombstone. Invalid bytes or key/payload disagreement are corruption. No loser overwrites the winner.

A confirmed precondition failure is a conflict. A put with an unknown transport outcome can confirm its own success only by reading back the exact proposed manifest, except for a plain create or recreate under section 9.1. A fork's unique source pin makes its exact read-back proof of publication. An explicit `allow_existing` retry can instead return an existing active namespace.

Abandoned attempts can leave a hint or fork pin. A leftover hint does not install a namespace. An unused fork pin is collected after its installation grace under section 11.7.

### 9.4 Deleting a namespace

Deletion uses the acquired writer epoch. After admitted commits finish, it folds the remaining WAL, then publishes the next manifest with deleted status. The manifest records `deleted_at_ms` from the deletion call's clock. Deletion initiates tombstone publication within `METADATA_PUBLICATION_BUDGET_MS` of capturing that clock. Its runs, counters, and folded WAL boundary cover the final head, so every WAL object of the deleted generation is at or below `folded_wal_no`. A failed fold leaves the namespace active; deletion can be retried. Previously committed data remains committed.

An ordinary operation that observes deletion returns `namespace_deleted`. A create or a fork into the id recreates it. A cached reader can still use its active view until the next manifest revalidation is due. Deletion does not immediately remove content.

The current deleted manifest is the generation's tombstone. It protects its runs until recreation supersedes it; the retired pin then keeps those runs protected until reclamation finishes. It protects no current WAL.

### 9.5 Retirement

Retirement is derived, not stored. For a tombstone `T`, the deadline is:

```text
deadline(T) = T.deleted_at_ms
              + max(configured_grace, NAMESPACE_RETIREMENT_GRACE_MS)
```

A generation is eligible when the pass clock reaches its deadline and the complete pin listing contains no pin in `[T.generation_first_manifest_no, T.manifest_no]` except its own retired pin. A pin's manifest number comes from its key. Pins deleted later in the same pass still count. An unrecognized key under the pin prefix makes every generation ineligible.

The current deleted manifest and each retired pin's tombstone provide independent reclamation evidence. No manifest is published to retire a generation. Eligible generations release their own content and source pins under section 11.8. Other generations and other owners remain outside that sweep.

### 9.6 Fork dependencies after deletion

A source pin remains required while a target's current manifest or a retired pin's tombstone refers to it. Rewriting a target's metadata does not transfer ownership of inherited file bytes.

For `A → B → C`, C's fork pin on B prevents B's generation from being reclaimed. B's pin on A remains until B's generation is reclaimed. Reclaiming C deletes C's pin on B. B can then be reclaimed once its deadline and pin checks pass, releasing its pin on A.

Pin deletion proceeds from descendants to ancestors. A failed source-pin deletion leaves the target tombstone available for another pass, through the current manifest or its retired pin.

### 9.7 Cross-namespace copies and moves

An inode-preserving rename is namespace-local. Across namespaces, a move is a destination copy followed by source deletion, without an atomic transaction across both histories.

A fork can retain references through its source pin; other imports write verified bytes under a fresh destination-owned identity. Reusing another owner's identity would require an additional durable source-side retention protocol.

An import checks the owner's current view for authorization, including imports within that namespace. Resident inline bytes are used only when the reference's owner and generation match the reading namespace and generation. Otherwise the import reads the object key derived from the reference, including references from earlier generations. A reclaimed generation's object is missing and returns the same error as any missing content object. No retired-pin or tombstone lookup is needed. The import verifies the bytes and stages them under a fresh identity owned by the destination's current generation. Forks pin manifests, so inherited content is always materialized. Inherited references retain their owner and owner generation.

A subject importing a bare reference must be an administrator of its owner namespace. An unrestricted owner and a request with no subject need no administrator grant. Authorization always uses the owner's current head, including after recreation. A deleted owner uses the access state in its surviving head.

## 10. Retention and compaction

Retention determines which historical views remain available under the format guarantee. Compaction rewrites the physical representation while preserving those views. Neither operation publishes new filesystem changes.

### 10.1 Advancing the retention floor

The floor bounds incremental replay, superseded binding history, old attribute states, and commit receipts. It does not expire file revisions or content-publication evidence. WAL objects are not retained by the floor; collection removes them once folded.

Floor advancement is explicit. The initial sequence floor is 0 for a new root namespace and the fork point for a fork. Automatic flushes do not advance it.

A floor advance loads the current manifest and verifies that its referenced segments exist. It publishes a successor with the same runs, head summary, allocators, and authority, setting `retention_floor_seq` to the predecessor's `head_seq`. The floor cannot decrease.

The existence check detects missing recovery material before abandoning the corresponding replay guarantee. It is not a multi-object transaction or a substitute for collection's reference rules. Read paths still verify checksums. A pin below the new sequence floor continues to protect its own manifest and runs.

### 10.2 Compaction windows

Metadata is compacted by family group. Directory binds, the child-binding index, and unbinds form one group because they must remain consistent. Each of the other seven groups contains one family.

A bounded rebuild merges an oldest-first contiguous window. It can skip the group's oldest run when that run is too large for one bounded step and merge the delta runs above it instead. It cannot skip an intervening delta run.

An output run is `base` if and only if the window includes the group's oldest run. Only that kind of rebuild can drop rows under the retention rules. A rebuild above the oldest run produces a `delta` run and drops nothing, because an omitted older run may contain the other half of a binding or removal pair.

A group has at most one base run. A bottom-anchored rebuild replaces the existing base when one exists and is stamped with the manifest's `head_seq`. Base runs are ordered before delta runs regardless of their sequence stamp. A rebuild that skips the oldest run is stamped with its newest input's sequence and remains at that position in the group.

A delta-only rebuild must merge at least two runs. Once a group has only one delta run above an oversized base, another bounded delta-only rebuild cannot reduce the run count. A larger streaming compaction can handle the complete group.

### 10.3 Row retention during a base rebuild

The following rules apply only when the selected inputs include the group's oldest run. A delta-only rebuild retains all input rows.

| Family | Rows retained or removed |
| --- | --- |
| `inodes` | Retain all inode rows. |
| `direntry_binds`, `direntry_child_binds`, `direntry_unbinds` | Remove bindings superseded or unbound at or below the floor and spent unbind markers, while preserving state at every retained sequence and parity between both bind indexes. |
| `revisions` | Retain every file revision, including revisions of deleted files. |
| `tombstones` | Retain all set and revoke events. |
| `active_deletions` | Retain listed deletions until revoked. Remove a cancelled `listed`/`removed` pair together. The floor does not expire a recoverable deletion. |
| `commits`, `commit_receipts` | Remove rows strictly below the floor. |
| `content_publications` | Retain all publication evidence, regardless of floor. |
| `attributes` | For each inode, retain all revisions above the floor and the newest revision at or below it; remove earlier revisions. |
| `access` | For each inode, retain all revisions above the floor and the newest revision at or below it; remove earlier revisions. |

An empty attribute map, or an access row with no boundary and no grants, is retained when it is the state at the floor. Removing it could expose an older row and restore state that had been cleared. Attribute and access rows are not removed merely because the inode is deleted, so undelete can restore the same state.

A rewrite must refuse an ambiguous attribute or access history in which two rows for one inode have the same revision number at or below the floor. It cannot choose an arbitrary row and discard the other.

The active-deletion family is a current-state index, not an independent historical trash log. Its removal marker sorts before the corresponding listed entry. Bottom-anchored compaction can remove the pair without leaving an older entry that would reappear in a subsequent read.

Compaction must preserve visible metadata at every retained sequence. It publishes a complete replacement through the numbered manifest protocol. Input objects remain available until no protected manifest or pin references them; successful publication is not permission to delete them immediately.

### 10.4 Streaming compaction

A streaming compaction processes selected runs without holding every row in memory. It writes completed segments under the namespace's normal `segments/` prefix using fresh IDs, then publishes references to that output in the next manifest. Readers continue using the preceding manifest until publication succeeds.

A runtime claims the namespace's compactor epoch before its first compaction after open. Claiming publishes a manifest with `compactor_epoch + 1` and otherwise unchanged state. Concurrent family groups in that runtime share the claim. Bounded and streaming compaction publications must match the current epoch; a newer claim fences older compactors. Core keeps this epoch because streaming compaction rebuilds a whole family group and publishes once at the end, so a newer runtime can stop a stale compactor at its next check; grep publishes each bounded step, so a lost race costs one step.

Before each publication, a job checks its elapsed monotonic time and reloads the current manifest. Its selected input segments must still be present and unchanged. A lost numbered put can be retried against a new manifest while those conditions hold. A changed epoch produces `fenced`; changed inputs or an exceeded time bound produce `abandoned`.

Segments not referenced by a collection root are protected for a minimum provider age of 24 hours. The streaming publication budget reserves the minimum GC grace inside that interval:

```text
streaming publication budget + minimum GC grace <= 24 hours
```

With the current constants, the job can initiate publication for at most 23 hours, 39 minutes, and 30 seconds after its timer begins before output. Beyond that point it abandons publication because its earliest unreferenced output may become collectable. A cancelled or crashed job leaves output subject to the same age rule.

Streaming compaction applies the row-retention rules for its selected window, just as bounded compaction does. It must preserve every retained view. Restart begins a new plan from the current manifest; there is no durable compaction cursor or output-protection record.

## 11. Garbage collection

Collection removes objects that no retained view needs, after the applicable age and publication checks. A logical delete alone is not permission to remove file bytes. File revision history remains retained in a live namespace, and a deleted ancestor's content remains while fork descendants depend on it.

### 11.1 One complete pass

A call discovers the namespace's current manifest, lists all pin keys, and builds an in-memory set of protected manifests and segments. Invalid or unreadable root manifests stop the call before sweeping. An absent namespace has nothing for core GC to collect.

The call then lists each candidate family from the beginning to completion. It stores no durable run, phase, reference table, or cursor. The complete pin listing used to establish roots is separate from the later pin sweep that decides which records can be deleted.

Every age decision uses the call's fixed `now_ms`. A later call reads fresh roots and uses its own clock. Concurrent collectors can independently delete eligible objects; an already absent object needs no further cleanup. A failed pass can have deleted earlier candidates, but the next pass safely starts again.

### 11.2 Reference roots

| Evidence captured for the pass | Objects protected |
| --- | --- |
| Current active namespace manifest | The manifest and every segment in its runs. |
| Current deleted manifest or a retired pin's tombstone | The tombstone and every segment in its runs. A retired pin protects them for the whole pass that deletes it. |
| Every recognized pin key in the complete listing | The numbered manifest in its ID and every segment in that manifest. |
| Hint's observed manifest number | All manifest numbers at or above it, so discovery can probe forward. Intermediate numbers do not protect additional runs. |
| Current active manifest's folded boundary | Every WAL number above `folded_wal_no`. |

Pin bodies are not needed to identify these roots: the manifest number is part of the pin key. Bodies are read later for owner and expiry decisions. A pin naming a missing manifest is corruption, except for a retired pin. A retired pin whose manifest is absent protects nothing and is deleted in the pin sweep with the retired count. Each listed pin protects its files for the whole pass, even if that pass deletes the pin.

Tombstone segments remain roots because retirement reads their publication rows to find exact content keys; after the retired pin is deleted and the tombstone is no longer current, the ordinary segment-age sweep can remove them.

A retention floor may pass a pinned manifest's head sequence. That does not remove its protection. Reads through the pin use the pinned file set directly.

### 11.3 Candidate and age rules

Being unreferenced makes an object a candidate; it does not make it immediately deletable. Let `T` be the configured ordinary grace, which must be at least `GC_MIN_GRACE_WINDOW_MS`.

| Family | Conditions for deletion |
| --- | --- |
| Namespace manifest | Below the observed hint and unpinned; its provider age is at least `T`, and its immediate successor, if present, is also at least `T` old. |
| WAL object in an active namespace | At or below `folded_wal_no`, with provider age at least `T`. |
| WAL object in a deleted namespace | Provider age at least `T`; no current WAL is protected. |
| Metadata segment | No root lists it, and its provider age is strictly greater than 24 hours. |
| Pin record | Owner-specific rules in section 11.7. |
| Upload session and its content | Status-specific rules in section 11.6. |
| Any eligible generation's owned content | Deadline and pin checks in section 9.5 pass, followed by the evidence and owner checks in section 11.8. |

The hint and current manifest are never swept. Unrecognized keys are retained by core GC. On an age-gated candidate, a missing provider timestamp or one in the future cannot establish sufficient age. If a manifest's successor is absent, that absence does not itself prevent deleting the predecessor.

For example, a collector observing hint 8 and current manifest 10 keeps manifests 8–10 for discovery. It keeps the segments in manifest 10 and any pinned manifests. It does not keep every segment mentioned only by 8 or 9. Such a segment still needs to exceed the segment minimum age before deletion.

### 11.4 Clock and operation assumptions

Ordinary age is calculated as `now_ms.saturating_sub(last_modified_ms)`. Safety therefore depends on a bound on age overstatement: the collector clock may be ahead of the provider, timestamps may have limited precision, and a process may pause around a publication check.

The combined allowance is 180,000 milliseconds. This is one relative-clock, precision, and scheduling allowance, not three minutes for each participant. A provider ahead of the collector delays collection. Record-based ages use the corresponding bound between the collector and the host that recorded creation, expiry, or completion.

Publication budgets use monotonic elapsed time. The minimum ordinary grace includes the longest bounded publication, one provider-operation deadline, one attempt timeout, and the combined allowance. Fork installation and streaming compaction reserve that grace within their respective lifetime bounds. Appendix C records the exact calculations.

These assumptions exclude an unbounded pause between a budget check and the write it permits. A client timeout does not establish that a remote write had no effect; unknown outcomes still require reconciliation.

Direct expiry checks do not add GC grace to the requested lifetime. A host ahead by `E` milliseconds can reject an expired upload or snapshot up to `E` milliseconds earlier than the creating host would. Reclamation grace protects concurrent publication; it does not synchronize expiry decisions across hosts.

### 11.5 Publication during collection

A new pin from the current head is acknowledged only after its manifest identity is checked again following the pin write. This closes the race between collection's current-manifest read and its complete pin listing. A snapshot fork is protected by the snapshot pin or by the new fork pin written before the snapshot recheck.

A collector protects every WAL number above its captured folded boundary. Writers must refresh a cached tip within the publication budget before attempting its successor. They cannot treat a much later reclaimed WAL number as a free publication slot.

New metadata segments remain protected by their minimum age while a publisher writes and verifies them. Streaming compaction must initiate publication before its budget expires, and every compaction checks its epoch and selected inputs. These rules apply to output that is not yet listed by a root captured earlier in the pass.

A failed required-root read stops collection. An uncertain fork-target read retains that pin. A failed content-publication lookup deletes neither the completed upload's bytes nor its session. Each cleanup operation must retain the durable evidence needed to retry after a failure.

### 11.6 Upload-session cleanup

Uploads are collected through their session records. Content prefixes are never enumerated. Waiting, held, and reclaimed generation rules take precedence over the session status rules.

| Session and namespace | Action |
| --- | --- |
| Open session, before expiry plus `T` | Retain. |
| Open session, after expiry plus `T` | CAS to `aborted`, then clean content and provider transfer state. A lost CAS retains it. |
| Aborted session | Retry content and provider cleanup; remove the record after abort time plus `T`. |
| Completed session in an active namespace, before content grace | Retain. |
| Completed session in an active namespace, after content grace | Check publication evidence. Keep published content; delete unreferenced content. Remove the session after successful cleanup or a confirmed publication. |
| Session in a deleted generation inside its retirement grace | Retain; report the generation's derived deadline. |
| Session in a deleted generation that pins hold after its grace | Retain; report no time of its own. |
| Completed session in an eligible current deleted generation | Delete the session's exact content key, then the record; no publication lookup or additional completion grace is required. |
| Prior-generation session, generation eligible | Use the retired namespace rules. The session's namespace and content ID select its exact key. Open and aborted sessions keep their expiry, abort grace, and provider cleanup rules. Completed sessions delete that key, then the record. |
| Prior-generation session, generation reclaimed | For open and aborted sessions, run provider cleanup. For every status, delete the exact key named by the session's namespace and content ID before deleting the record. A failed cleanup retains the record. |

Before completion, a session owns its random content identity exclusively and cannot issue admission evidence. In current or eligible generations, cleanup first wins the terminal transition, then removes content and any provider transfer. A failed cleanup leaves the record for another attempt. Open and aborted sessions still require provider cleanup after namespace retirement because provider upload state can exist outside object listings.

Cleanup derives every content key and provider cleanup target from the namespace and content ID recorded when the session opened. A pass records each generation with a retained session and keeps that generation's retired pin until a pass leaves no session behind. A generation below the head with no retired pin is reclaimed: recreation writes the pin before publishing the new head.

For eligible completed uploads on an active namespace, the collector loads a metadata view lazily and looks up `content_id` in the WAL projection and `content_publications` family. It does not scan every revision. These publication rows are retained permanently, independently of commit receipts and the retention floor. If publication is found, only the session record is removed. If no publication exists, content is deleted before the session. An error permits neither a speculative content deletion nor removal of retry evidence.

The completed-content grace covers all possible admission evidence:

```text
completion
    ├── receipt issuance window ──┤
                                  ├── final token lifetime ──┤
                                                             ├── minimum GC grace ──┤
                                                                                   earliest cleanup
```

In milliseconds:

```text
CONTENT_RECLAMATION_GRACE_MS
    = COMPLETED_UPLOAD_RECEIPT_WINDOW_MS
      + CONTENT_RECEIPT_TTL_MS
      + GC_MIN_GRACE_WINDOW_MS
```

Every token mint checks the original completion time. A retained receipt cannot extend its issuance window. In-process proofs expire no later than the final token could, and publication checks proof expiry immediately before the numbered WAL put. After the full grace, an unpublished upload cannot acquire a new valid first publication. Previously published content remains protected by its durable publication row.

### 11.7 Pin cleanup

User and snapshot pins become collectable after expiry plus `T`, or creation plus `T` on a deleted namespace. A pin whose manifest number is below the head's `generation_first_manifest_no` also uses creation plus `T`. A user pin with no expiry remains until explicit deletion only within the active current generation. A retained user or snapshot pin reports the earlier of those times in `next_reclamation_at_ms`, so a generation it holds is collected once it goes. Pin deletion is direct; IDs are never reused.

A retired pin with a present manifest is retained before any age check. Reclaiming its generation removes it. A retired pin whose manifest is absent is deleted in the pin sweep.

Fork pins use this decision table:

| Source pin and target state | Action |
| --- | --- |
| Pin younger than `T` | Retain without reading the target. |
| Aged pin, target absent | Delete the abandoned installation's pin. |
| Target names the exact source, pin ID, and manifest reference | Retain, including if the target is deleted. |
| Target does not name this pin, generation above 1, and a retired pin's tombstone names it | Retain as `referenced_by_prior_generation`. |
| Target does not name this pin and no retired tombstone names it | Delete the pin from the abandoned attempt. |
| A retired tombstone cannot be loaded | Retain the source pin. |
| Target names this pin but disagrees on source or manifest | Report corruption. |
| Target cannot be read | Retain the pin. |
| Target data is invalid | Report corruption. |

The source discovers the target's current manifest through its hint; it does not read the target WAL. An absent target does not need a tombstone. When the current target does not name the pin and its generation is above 1, list its pin prefix. Select keys whose ID equals `PinId::retired(target, manifest_no)` for their own manifest number and load those tombstones without reading pin bodies. Generation 1 needs no extra reads. A matching target's collector deletes the source pin when it reclaims the generation.

An unrecognized key prevents reclamation of every generation. Any other listed pin prevents reclamation of the generation whose manifest range contains it, even if the pass later deletes that pin. A candidate pin written too late to verify must be deleted by its creator; if that creator crashes first, its installation grace and owner rules still apply.

### 11.8 Sweeping a retired owner's content

After session cleanup, reclaim each eligible generation: the current tombstone first, then retired pins in key order. Use the deadline and complete pin listing captured for the pass, as specified in section 9.5.

For each eligible tombstone `T`:

1. Confirm the evidence. For a retired pin, `head` its key and require it to be present. For the current tombstone, reload the current manifest and require the same manifest number. A mismatch skips this generation without error. A failed read stops the call.
2. Open only segments in `T`'s runs whose `owner_namespace_id` equals `T.namespace_id`; inherited segments cannot hold its publication rows because folds and compaction write owned segments and cross-namespace imports mint destination-owned identities. Page through the whole `content_publications` family in those segments. For each row whose `owner_namespace_id` equals `T.namespace_id` and `owner_generation` equals `T.generation`, delete `namespaces/{owner_namespace_id}/content/{content_id}`. Skip every other row. Its object belongs to another owner, whose lifecycle and the fork pin protect it. Count successful deletions, including `NotFound`, in `deleted.retired_content_objects`. Do not list content or apply an additional age check. A segment read failure or deletion failure ends the call before either pin is released.
3. If `T.fork_basis` is present, delete its source pin. Count the deletion when the pin was present.
4. For a retired pin, delete it only if this pass left no upload session of its generation behind. Count it under `deleted_checkpoints_by_owner.retired`. A retained session leaves the pin for the next pass.

Content and source-pin cleanup are idempotent. Deleting the retired pin last preserves the tombstone until cleanup finishes. The current tombstone remains when the namespace is never recreated. Later passes read the same publication rows and repeat the same exact deletes. No progress record or journal is stored.

## 12. Encodings, versions, and extensions

Storage versions describe how durable objects are interpreted and operated on. API versions describe request and response contracts. They are related, but a change to one is not automatically a change to the other.

### 12.1 Field conventions

Durable field names and enum values use `snake_case`. Tagged unions use `kind`. A `status` field describes a resource lifecycle, while `phase` describes computation progress; the values of either use `kind` when they are tagged objects.

Identifier fields use `_id`. A `_seq` is a position in namespace commit history, a `_no` is a counter scoped to its resource, an `_index` is a zero-based collection position, and a `_number` is a one-based position defined by a provider or tool.

An absent optional object field is omitted when writing. A required value is written even when it is zero, false, or an empty collection. Fingerprint preimages have their own explicit absence rules in Appendix B; they are not ordinary durable records.

Durable inode IDs are integers. Public API inode strings such as `ino_42` do not change the stored ID representation. The canonical fingerprint rules explicitly identify the few contexts where public strings are part of the preimage.

### 12.2 Envelopes and checksums

Structured control records, manifests, and WAL segments use an envelope with `kind`, `format_version`, `payload_checksum`, and `payload`. Content objects are raw file bytes. Block segments use the sectioned encoding in Appendix A instead of an envelope.

JSON envelopes retain the payload as an inline raw JSON fragment. A WAL envelope contains the encoded CBOR payload as a CBOR byte string. The enclosing document is compressed with zstd. The checksum covers the payload bytes that were stored, not a decoded object serialized again by the reader.

Decoders identify the kind and supported version before interpreting the payload. They verify the exact payload checksum before decoding its fields. Unknown kinds, unsupported versions, malformed payloads, and checksum failures are errors; none is a reason to substitute another object.

Envelope, manifest-reference, and whole-object digests are encoded as `sha256:<64 lowercase hex>`. A content or part checksum uses `{ "algorithm": ..., "value": ... }` because its algorithm is selected for the transfer. A block handle has a numeric `crc32c` field because that algorithm is fixed by its block format. Commit fingerprints additionally name their canonicalization scheme.

Per-block CRC32C verifies ranged block reads against their handles. It is not the same operation as verifying a full-object SHA-256 digest, and neither an unauthenticated checksum nor an object name is an authorization mechanism.

### 12.3 Strict decoding and evolution

Authoritative durable envelopes and their nested payloads reject unknown fields. This includes immutable objects: WAL folding and compaction re-encode their contents, so accepting an unknown field and dropping it in a successor would lose durable meaning.

`ContentRef` and `Checksum` are closed shapes wherever they appear. They evolve through supported `kind` or `algorithm` values, not additional fields on an existing closed shape. Unknown content kinds and checksum algorithms are rejected. A new content kind requires a supported version change for every durable family containing that reference.

API request bodies also reject unknown fields, including nested fields, so a misspelled precondition cannot silently become a request without that precondition. Response bodies generally tolerate additions, except for shared closed shapes. The companion API specification defines those transport rules.

The owning envelope's `format_version` governs its entire payload, including nested objects and collection semantics. A payload does not add an independent format-version field. Block segments are interpreted under the version of the manifest that references them. A name such as `blob_v1` identifies a closed content strategy; it is not permission to ignore the owning family's version.

After the stable format is released, a change to a field's name, presence, type, tag, encoding, or governed semantics requires a new owning-family version. Collection-protocol changes can require a version gate even when most stored fields remain unchanged. An implementation must not operate on a newer protocol merely because it can deserialize a subset of its fields.

Golden fixtures pin the reference encodings in `crates/loonfs-api/tests/golden_formats.rs` and the grep fixtures. Fingerprint vectors additionally pin canonical JSON bytes and digests. Validating a new release requires preserving the meaning of retained data, not just recompiling its type definitions.

Appendix A lists the family versions. A binary rollback is valid only if the older binary supports every stored family version and its associated protocol. Downgrading the binary does not convert stored data.

### 12.4 Extensions

An optional derived subsystem stores its objects below:

```text
namespaces/{namespace_id}/extensions/{name}/
```

It defines its own object grammar, versions, discovery and publication rules, and collection rules. Core manifests contain no generic extension registry or extension-state fields. Core reads do not require support for the extension's encoding. Core collectors do not delete extension objects.

An extension must remain rebuildable from authoritative core state. Its absence cannot make the core namespace unreadable. The grep extension is specified separately in Appendix D, including its manifest, tokenizer, postings, and GC behavior.

### 12.5 Reserved functionality

The current inode kinds are `file` and `dir`. Mount creation and traversal are not defined by this version; no standard operation creates a mount.

Access rows and the namespace access mode are stored as section 1.8 defines. Commits and upload sessions are authorized against access rows as the API specification defines. Reads evaluate access rows as the API specification defines. Access changes publish in commit order and appear in the change feed as `access_changed` events.

## Appendix A. Durable records and byte encodings

This appendix is the field and encoding reference for the protocols above. Field names are literal. A `?` after a field in a table means the object member is optional and omitted when absent; the question mark is not part of its stored name.

### A.1 Family versions

The three control-object kinds are `hint`, `pin`, and `upload_session`.

| Object | Envelope kind | Encoding | Version |
| --- | --- | --- | --- |
| WAL segment | `wal_segment` | zstd-compressed CBOR envelope with CBOR payload bytes | 1 |
| Namespace manifest | `manifest` | Uncompressed JSON | 1 |
| Namespace hint | `hint` | Uncompressed JSON | 1 |
| Metadata segment | No envelope | Block sections described in A.7 | Governed by namespace manifest version 1 |
| Pin record | `pin` | Uncompressed JSON | 1 |
| Upload session | `upload_session` | Uncompressed JSON | 1 |
| Content object | No envelope | Complete file bytes | Referenced as `blob_v1` |
| Grep hint | `grep_hint` | Uncompressed JSON | 1 |
| Grep manifest | `grep_manifest` | Uncompressed JSON | 1 |
| Grep segment | No envelope | Block sections with grep rows | Governed by grep manifest version 1 |

### A.2 Envelope layout

An envelope contains these fields:

| Field | Representation |
| --- | --- |
| `kind` | Family discriminator string. |
| `format_version` | Unsigned family-version integer. |
| `payload_checksum` | `sha256:` followed by 64 lowercase hexadecimal characters. |
| `payload` | Raw JSON sub-document for JSON families; CBOR byte string containing the encoded payload for WAL. |

For JSON, hash the UTF-8 bytes of the stored payload fragment, including its internal whitespace and spelling. Do not parse and re-serialize the payload to calculate its stored checksum. The enclosing envelope's whitespace is not part of that payload checksum.

For WAL, serialize the payload to CBOR, hash those bytes, store them as the envelope's CBOR byte string, and zstd-compress the envelope. The digest does not cover the compressed representation or a second encoding of the decoded payload.

A reader first identifies the declared kind and version, then validates the envelope and checksum, then interprets the payload under the supported schema. Field and protocol validation still applies after a checksum succeeds. A matching checksum does not make an inconsistent record valid.

### A.3 Content references and checksums

A `blob_v1` reference contains all of the following fields:

| Field | Meaning |
| --- | --- |
| `kind` | `blob_v1`. |
| `owner_namespace_id` | Namespace that originally wrote the object. |
| `owner_generation` | Generation of the owner namespace that wrote the object. |
| `content_id` | `con_` followed by 32 random lowercase hexadecimal characters. |
| `size_bytes` | Length of the complete file. |
| `checksum` | Algorithm and digest of the complete file. |

For example, the 15 UTF-8 bytes represented by `Hello, LoonFS!\n`, with a single LF at the end, have this reference shape. The ID is illustrative, while the size and SHA-256 are calculated from those bytes:

```json
{
  "kind": "blob_v1",
  "owner_namespace_id": "demo",
  "owner_generation": 1,
  "content_id": "con_0123456789abcdef0123456789abcdef",
  "size_bytes": 15,
  "checksum": {
    "algorithm": "sha256",
    "value": "15ac23a641835390d4e417dbc382692c81fa08c237ffb61ca8ba24c042522a13"
  }
}
```

The key is `namespaces/{owner_namespace_id}/content/{content_id}`. Content IDs are random and never reused.

A checksum is `{ "algorithm": <name>, "value": <lowercase hex> }`:

| Algorithm | Value width |
| --- | --- |
| `sha256` | 64 hexadecimal characters |
| `crc64nvme` | 16 hexadecimal characters |
| `crc32c` | 8 hexadecimal characters |

Coverage is defined by the containing field. A content reference or upload-content claim covers the complete object. A multipart part checksum covers one part. An algorithm selector without a value is not itself a checksum. Provider encodings are converted to this representation before constructing the stored value.

### A.4 Control and manifest payloads

The following tables list the durable payload fields. Their transition rules are in the protocol chapters.

| Payload | Fields |
| --- | --- |
| Namespace hint | `namespace_id`, `manifest_no`, `wal_no` |
| Writer block | `writer_id`, `acquired_at_ms` |
| Fork basis | `manifest`, `source_pin_id` |
| Manifest reference | `owner_namespace_id`, `manifest_no`, `head_seq`, `payload_checksum` |
| Pin record | `namespace_id`, `pin_id`, `head_seq`, `payload_checksum`, `created_at_ms`, `owner` |
| Upload session | `namespace_id`, `owner_generation`, `upload_id`, `content_id`, `created_at_ms`, optional `subject_id`, `mode`, `status` |

Namespace status is `{"kind":"active"}` or `{"kind":"deleted"}` with required `deleted_at_ms` only on the deleted variant. Missing status is invalid. The genesis commit ID is `c_00000000000000000000000000000000`.

Pin owners have the fields in section 8.1. The `retired` owner has no fields. There is no status field on a pin. Upload status is `open` with `expires_at_ms`, `completed` with `completed_at_ms` and `content_ref`, or `aborted` with `aborted_at_ms`. The mode remains present in every status. A service-proxied mode contains `staging`; direct PUT contains `checksum_algorithm`; direct multipart contains `provider_upload_id`, `part_size_bytes`, and `checksum_algorithm`. Staging is `idle`, `claimed`, or `staged` with `content_ref`. All these variants use `kind` tags.

A namespace manifest contains:

| Field | Meaning |
| --- | --- |
| `namespace_id` | Namespace described by the manifest. |
| `created_at_ms` | Generation creation time, immutable within that generation. |
| `created_by` | Application-supplied actor that created or forked the generation, immutable within that generation. |
| `access` | Access mode, immutable within a generation: `{"kind":"unrestricted"}`, or `{"kind":"acl"}` with `principal_scope` and `root_grants`. |
| `fork_basis?` | Source reference and pin identity, immutable within a generation. |
| `status` | Active or deleted state for this generation. |
| `writer?` | Diagnostic writer block. |
| `folded_wal_no` | Highest local WAL number incorporated into the file set. |
| `manifest_no` | Positive number matching the object key. |
| `generation` | Positive namespace generation. Generation 1 is the first creation; recreation increments it. |
| `generation_first_manifest_no` | Positive manifest number at which this generation began, no later than `manifest_no`. |
| `compactor_epoch` | Current compaction authority. |
| `head_seq` | Materialized head sequence; on deletion, the final namespace sequence. |
| `head_commit_id` | Commit ID at the recorded head. |
| `activity` | Required cumulative activity counters defined in section 7.4. |
| `base_seq` | Oldest run sequence represented by the file set. |
| `writer_epoch` | Current writer authority. |
| `next_inode_id` | First inode ID available at the recorded boundary. |
| `next_run_no` | Next run number to allocate. |
| `retention_floor_seq` | Earliest sequence covered by incremental replay and row retention. |
| `runs` | Complete list of materialized metadata runs. |

A run contains `run_no`, `run_seq`, `tier`, and `segments`. Tier is `delta` or `base`. A segment descriptor contains:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | Namespace storing the segment. |
| `segment_id` | Immutable generated segment identity. |
| `family` | Metadata row family. |
| `segment_index` | Zero-based position within this run's family segment list. |
| `row_count` | Number of stored rows. |
| `min_row_key`, `max_row_key` | Inclusive key range. |
| `index_block`, `filter_block` | Index and filter handles. |
| `filter_inline?` | Exact stored filter bytes as lowercase hexadecimal. |
| `object_checksum` | SHA-256 of the complete stored segment. |

The owner and segment ID determine the object key. The descriptor stores no separate path or compaction-job identity.

### A.5 WAL records

`MAX_WAL_SEGMENT_BYTES` is 512 MiB (536,870,912 bytes) for the complete decompressed WAL document, including its envelope. Writers keep every segment within this limit through request and batch admission; readers refuse larger documents. A writer composes each batch so the sum of its requests' bounds plus the document overhead stays within the limit. This is a format constraint because every successful publication must remain readable with bounded decompression.

A WAL segment's payload contains `namespace_id`, `wal_no`, `writer_epoch`, `prior_head_seq`, `head_seq`, `head_commit_id`, `next_inode_id`, and `records`.

For a data segment, `records` covers the sequences after `prior_head_seq` through `head_seq` contiguously. The WAL number must match the key, and the allocation high-water mark and head commit ID must agree with replay. A fence has an empty record list, `head_seq` equal to `prior_head_seq`, and an unchanged allocator and head commit ID. Fences participate in WAL numbering and epoch validation but produce no logical changes.

Each commit contains `seq`, `commit_id`, `committed_by`, `semantic_commit_fingerprint`, `committed_at_ms`, optional `message`, `deltas`, and optional `inline_content`. A delta wrapper contains `semantic_operation_index` and `delta`. The latter is a kind-tagged object with these fields:

| Delta kind | Fields after `kind` |
| --- | --- |
| `create_inode` | `delta_index`, `inode_id`, `inode_kind` |
| `bind_direntry` | `delta_index`, `parent_inode_id`, `name_key`, `display_name`, `child_inode_id` |
| `unbind_direntry` | `delta_index`, `parent_inode_id`, `name_key`, `display_name`, `child_inode_id`, `bind_seq`, `bind_delta_index` |
| `append_file_revision` | `delta_index`, `inode_id`, `revision_no`, `content_ref` |
| `tombstone_subtree` | `delta_index`, `root_inode_id`, `deleted_binding` |
| `revoke_subtree_tombstone` | `delta_index`, `root_inode_id`, `target` |
| `append_attributes_revision` | `delta_index`, `inode_id`, `attributes_revision_no`, `attributes` |
| `append_access_revision` | `delta_index`, `inode_id`, `access_revision_no`, `boundary`, `grants` |

A delta's own commit sequence is implicit in its containing commit. A tombstone target is `{seq, delta_index}`. A deleted binding is `{parent_inode_id, name_key, display_name}`. Attribute and access deltas contain the complete resulting state, including an empty map after a clear.

`inline_content` is a list of `{content_id, bytes}`, where `bytes` is a CBOR byte string; the field is omitted when empty and defaults to an empty list when absent. This field is part of the version 1 format, and the reference it accompanies is an ordinary `blob_v1` reference.

Encoding and decoding enforce these rules using only the WAL segment:

1. Every entry's `content_id` is named by at least one `append_file_revision` delta in the same commit whose `content_ref.owner_namespace_id` equals the segment's `namespace_id`.
2. The entry's length equals the `size_bytes` of every such reference. A zero-length entry is valid.
3. A content ID appears at most once in a commit's `inline_content`.
4. Each entry contains at most `MAX_WAL_INLINE_CONTENT_BYTES`: 256 KiB (262,144 bytes).
5. The sum of all entry lengths in one WAL segment is at most `MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES`: 4 MiB (4,194,304 bytes).

These are reader limits; writer thresholds are policy at or below them. `MAX_WAL_SEGMENT_BYTES` still limits the complete decompressed document. A delta may name content with no inline entry. Replay does not hash inline bytes against the reference's checksum; the envelope's `payload_checksum` covers the stored payload bytes.

WAL replay applies these normalized records in sequence and delta order. It does not re-run the original request's preconditions or reinterpret the request under a newer planner.

### A.6 Metadata rows and row keys

Rows are kind-tagged CBOR objects in the data blocks. The row-kind schema and the row-family ordering are separate: the same `direntry_bind` row appears in both bind families.

| Row kind | Fields after `kind` |
| --- | --- |
| `inode` | `inode_id`, `inode_kind`, `committed_seq`, `commit_id`, `committed_by`, `committed_at_ms` |
| `direntry_bind` | `parent_inode_id`, `name_key`, `display_name`, `child_inode_id`, `bind_seq`, `bind_delta_index` |
| `direntry_unbind` | The bind fields, followed by `unbind_seq`, `unbind_delta_index` |
| `file_revision` | `inode_id`, `revision_no`, `committed_seq`, `commit_id`, `committed_by`, `committed_at_ms`, `delta_index`, `content_ref` |
| `tombstone` | `root_inode_id`, `generation`, `commit_id`, `action`, `committed_by`, `committed_at_ms` |
| `active_deletion` | `root_inode_id`, `deletion_seq`, `action` |
| `commit_receipt` | `commit_id`, `committed_seq`, `semantic_commit_fingerprint` |
| `commit` | `seq`, `commit_id`, `committed_by`, `semantic_commit_fingerprint`, `committed_at_ms`, `message?`, `deltas` |
| `content_publication` | `owner_namespace_id`, `owner_generation`, `content_id`, `committed_seq`, `delta_index` |
| `attributes_revision` | `inode_id`, `attributes_revision_no`, `committed_seq`, `commit_id`, `delta_index`, `committed_by`, `committed_at_ms`, `attributes` |
| `access_revision` | `inode_id`, `access_revision_no`, `committed_seq`, `commit_id`, `delta_index`, `committed_by`, `committed_at_ms`, `boundary`, `grants` |

A `commit` row is the WAL commit record of A.5 without its inline content. The `inline_content` field is omitted, and a row that carries one is invalid.

For a tombstone, `generation` is `{seq, delta_index}`. A set action is `{"kind":"set","deleted_binding":...}`. A revoke action is `{"kind":"revoke","target":...}`.

An active-deletion `listed` action contains `inode_kind`, `deleted_at_ms`, `deleted_by`, and `deleted_binding`. A `removed` action contains `revocation_seq`. These are nested action fields, not additional top-level fields on every active-deletion row. The `inode_kind` is copied from the deleted root inode. A `listed` action without it fails decoding.

The fixed row-key prefixes are followed by hyphen-separated components. Unsigned 64-bit components use 20 decimal digits and unsigned 32-bit components use 10, with leading zeroes. Variable names and commit IDs are the lowercase hexadecimal encoding of their UTF-8 bytes. Content-publication keys use the content ID directly. This avoids interpreting a name's own punctuation as a component delimiter.

In the following grammar, `u64::MAX - x` and `u32::MAX - x` mean subtraction before fixed-width decimal encoding. They are not literal text in a key.

| Family | Row key |
| --- | --- |
| `inodes` | `inode-{inode_id:020}` |
| `direntry_binds` | `direntry-bind-{parent_inode_id:020}-{name_key_hex}-{bind_seq:020}-{bind_delta_index:010}` |
| `direntry_child_binds` | `direntry-child-bind-{child_inode_id:020}-{bind_seq:020}-{bind_delta_index:010}-{parent_inode_id:020}-{name_key_hex}` |
| `direntry_unbinds` | `direntry-unbind-{parent_inode_id:020}-{name_key_hex}-{bind_seq:020}-{bind_delta_index:010}-{unbind_seq:020}-{unbind_delta_index:010}` |
| `revisions` | `revision-{inode_id:020}-{u64::MAX - revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |
| `tombstones` | `tombstone-{root_inode_id:020}-{generation.seq:020}-{generation.delta_index:010}` |
| `active_deletions` | `active-deletion-{deletion_seq:020}-{root_inode_id:020}-{sort_rank:010}` |
| `commit_receipts` | `commit-receipt-{commit_id_hex}-{committed_seq:020}` |
| `commits` | `commit-{seq:020}` |
| `content_publications` | `content-publication-{content_id}-{committed_seq:020}` |
| `attributes` | `attribute-{inode_id:020}-{u64::MAX - attributes_revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |
| `access` | `access-{inode_id:020}-{u64::MAX - access_revision_no:020}-{u64::MAX - committed_seq:020}-{u32::MAX - delta_index:010}` |

Ascending byte order therefore scans an inode's file revisions, attributes, and access rows newest-first. The active-deletion `sort_rank` is 0 for a removed entry and 1 for a listed entry. The stored widths are still ten digits.

Bloom filters use the following keys, which are not always full row keys:

| Family | Filter key |
| --- | --- |
| `inodes` | Complete row key |
| `direntry_binds` | `direntry-bind-{parent_inode_id:020}-{name_key_hex}` |
| `direntry_child_binds` | `direntry-child-bind-{child_inode_id:020}` |
| `direntry_unbinds` | `direntry-unbind-{parent_inode_id:020}-{name_key_hex}` |
| `revisions` | `revision-{inode_id:020}` |
| `tombstones` | `tombstone-{root_inode_id:020}` |
| `active_deletions` | Complete row key |
| `commit_receipts` | `commit-receipt-{commit_id_hex}` |
| `commits` | Complete row key |
| `content_publications` | `content-publication-{content_id}` |
| `attributes` | `attribute-{inode_id:020}` |
| `access` | `access-{inode_id:020}` |

Every delta that appends a file revision also produces a content-publication row with the reference's original owner namespace and generation. Repeated references to the same content within one commit share one row with the first publishing delta index. These rows survive every base rebuild, regardless of retention floor.

The family groups are fixed:

| Group | Members |
| --- | --- |
| `bindings` | `direntry_binds`, `direntry_child_binds`, `direntry_unbinds` |
| `revisions` | `revisions` |
| `inodes` | `inodes` |
| `tombstones` | `tombstones` |
| `active_deletions` | `active_deletions` |
| `commits` | `commits`, `commit_receipts` |
| `content_publications` | `content_publications` |
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

There is no segment header or footer. The manifest descriptor supplies the filter and index handles; the index supplies the data-block handles. A segment cannot be opened from its own bytes without the necessary descriptor information.

Each handle is an encoded object with the following fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `offset` | `u64` | Zero-based byte offset in the complete segment. |
| `stored_bytes` | `u32` | Number of stored bytes in the section. |
| `decoded_bytes` | `u32` | Expected length after decompression, or the uncompressed length for a filter. |
| `crc32c` | `u32` | CRC32C of the exact stored section bytes. |

For each section, the returned byte count must match `stored_bytes`, the stored-byte CRC must match `crc32c`, and the decoded length must match `decoded_bytes`. Both lengths fit in `u32`. Readers stop decompression after at most `decoded_bytes + 1` bytes. The extra byte reports a decoded-length mismatch without expanding the rest of an invalid section. This caps decompressed output, not vector capacity or the total memory of decoded rows and parser state. Appendix C.4 describes initial allocation sizing.

#### Data blocks

Before compression, a data block consists of entries, a restart-offset array, and a four-byte restart count. An entry contains:

```text
shared_prefix_len     unsigned LEB128
key_suffix_len        unsigned LEB128
key_suffix            key_suffix_len UTF-8 bytes
row_len               unsigned LEB128
row                    row_len CBOR bytes
```

The key is the first `shared_prefix_len` bytes of the preceding key followed by `key_suffix`. The prefix must end on a valid UTF-8 boundary. Keys are in ascending order. The generic block grammar can represent adjacent equal keys; the metadata-run uniqueness rule still applies to metadata producers.

The first entry and every sixteenth entry thereafter begin a restart and encode the full key with a zero shared-prefix length. The restart array contains their offsets relative to the start of the entry region, each as a little-endian `u32`. Readers verify each offset and reject missing or unused offsets. These restarts limit shared-prefix reconstruction to groups of sixteen rows. The array is followed by its count, also a little-endian `u32`. Entries and restart data are compressed together as one zstd section.

The same entry and restart layout applies to metadata and grep data blocks.

Writers encode unsigned LEB128 values with seven payload bits per byte and the high bit indicating continuation. The last byte has no continuation bit. Encoded values fit in `u64`; in a ten-byte encoding only one payload bit is available in the final byte. The writer uses the shortest representation of each value.

#### Filter block

The filter is uncompressed and has this byte layout:

```text
n_hashes              u32, little-endian
bit_len               u64, little-endian
bits                  ceil(bit_len / 8) bytes
```

Version 1 requires `n_hashes` to be seven and `bit_len` to be at least 64. Readers reject other header values before probing. The reference producer reserves ten bits per inserted filter key, with a minimum of 64 bits. Repeated filter keys count as separate insertions for sizing. The number of stored bit-array bytes must equal `ceil(bit_len / 8)`.

Hashing is XXH64 over the filter key's UTF-8 bytes, with two fixed seeds:

```text
h1 = XXH64(key, seed = 0)
h2 = XXH64(key, seed = 0x9e3779b97f4a7c15)
```

For each probe `i` from zero through `n_hashes - 1`, calculate `(h1 + i * h2)` with unsigned 64-bit wrapping arithmetic, then take the remainder modulo `bit_len`. Bit `b` is stored in byte `b / 8` at the mask `1 << (b % 8)`.

A valid negative filter result can skip the segment for that lookup. It does not replace row-level visibility rules for a positive result.

When `filter_inline` is present in the descriptor, it is the lowercase hexadecimal encoding of these exact stored filter bytes. The inline value must have the filter handle's stored length and checksum. Its presence does not remove the filter block from the segment. A reader without an inline copy fetches the section by its handle.

#### Index block

The decoded index is a CBOR list. Each entry contains `last_row_key` and `block`, where `block` is the corresponding data-block handle. The list is zstd-compressed as one section.

Index entries are ordered by their last keys. Data-block byte ranges are contiguous and must not overflow. The filter immediately follows the data region, and the index immediately follows the filter at the end of the object. The descriptor's handles must agree with that layout.

A range lookup uses the last keys to identify candidate blocks. Readers verify each fetched section before decoding its rows and apply the descriptor's family and key-range constraints.

The complete segment's `object_checksum` is SHA-256 over all stored sections. Normal ranged reads use their per-section CRCs instead of downloading the entire segment to recompute that digest. Full-object verification and publication conflict checks can use the complete digest.

### A.8 Object keys

These patterns define the core object families. Segment owners can differ from the namespace reading them.

| Family | Standard object key pattern |
| --- | --- |
| **WAL segments** | `namespaces/{namespace_id}/wal/{wal_no:020}.wal.zst` |
| **Namespace manifests** | `namespaces/{namespace_id}/manifests/{manifest_no:020}.json` |
| **Pin records** | `namespaces/{namespace_id}/pins/{pin_id}.json` |
| **Metadata segments** | `namespaces/{owner_namespace_id}/segments/{segment_id}.sst.zst` |
| **Upload sessions** | `namespaces/{namespace_id}/uploads/{upload_id}.json` |
| **Hint** | `namespaces/{namespace_id}/hint.json` |
| **Content objects** | `namespaces/{owner_namespace_id}/content/{content_id}` |

## Appendix B. Semantic commit fingerprints

A commit fingerprint is stored as `v1:sha256:<64 lowercase hex>`. The scheme identifies the canonicalization rules below, not the API's general-purpose JSON serialization.

The fingerprint is the SHA-256 of compact UTF-8 JSON with the following top-level fields in this exact order:

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

The layout above is a schema illustration. Actual preimage bytes contain no formatting whitespace. The `preconditions` member is always present and is `[]` for an empty list; `message` and `subject_id` are always present and are `null` when absent.

The `namespace_id`, `actor_id`, `subject_id`, message, operation order, and caller preconditions are significant. A changed `actor_id` is a changed logical request even if a different process is otherwise retrying on behalf of the same application. The principal scope is excluded because the namespace has one scope and refuses every other. The commit ID itself, writer epoch, and committed timestamp are excluded.

### B.1 Operation fields

Every operation begins with `kind`, followed by the fields in the order below. Every listed field is written, including unset optional fields as `null` and default booleans or behavior values explicitly.

| Kind | Fields after `kind`, in order |
| --- | --- |
| `create_directory` | `path`, `parents` |
| `create_directory_by_inode` | `parent_inode_id`, `display_name` |
| `put_file` | `path`, `behavior`, `content_ref`, `expected_inode_id`, `expected_revision_no` |
| `create_file_by_inode` | `parent_inode_id`, `display_name`, `content_ref` |
| `put_file_revision_by_inode` | `inode_id`, `content_ref`, `expected_revision_no` |
| `move_by_inode` | `inode_id`, `expected_binding_generation`, `destination_parent_inode_id`, `destination_display_name`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `delete_by_inode` | `inode_id`, `expected_binding_generation`, `behavior` |
| `delete_path` | `path`, `behavior`, `expected_inode_id` |
| `move_path` | `source_path`, `destination_path`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `copy_path` | `source_path`, `destination_path`, `behavior`, `expected_destination_inode_id`, `expected_destination_revision_no` |
| `restore_revision` | `path`, `source_revision_no` |
| `undelete` | `inode_id`, `deletion_seq`, `destination_path` |
| `update_attributes` | `path`, `set`, `remove`, `expected_inode_id`, `expected_attributes_revision_no` |
| `update_access` | `path`, `boundary`, `grants`, `expected_inode_id`, `expected_access_revision_no` |

Paths use their validated canonical absolute form. Display-name fields contain one validated component. Inode IDs in these operation shapes use their numeric storage representation, not public `ino_` strings. Sequence and revision numbers are JSON integers. Binding generations retain their opaque string representation.

Attribute `set` keys are sorted by lexicographic UTF-8 byte order. The `remove` list is sorted and deduplicated. Operation order is not sorted or otherwise changed.

### B.2 Content in a fingerprint

A content reference is represented by exactly these fields, in this order:

```json
{"kind":"blob_v1","content_id":"con_0123456789abcdef0123456789abcdef","size_bytes":15}
```

The owner namespace, owner generation, and checksum are excluded from the preimage. Every reference a commit can admit is owned by the committing namespace and carries its current generation. The owner repeats the `namespace_id` the preimage already names, and the generation is fixed by that namespace's current head. The checksum is verification evidence rather than a second identity. All three fields are still present and validated on the actual reference; their exclusion from the fingerprint does not make them optional on a commit.

Two uploads of identical bytes have different IDs and different fingerprints. A retry reuses the original reference rather than repeating the upload and substituting a new one.

Content that a commit carries inline has no object to name, so it is identified by its bytes. Each operation whose reference names inline content uses this form in its `content_ref` member instead:

```json
{"kind":"inline_v1","sha256":"<64 lowercase hex>","size_bytes":15}
```

`sha256` is the lowercase hexadecimal SHA-256 of the complete content, and the fields appear in the order shown. A reference that accompanies inline content carries a SHA-256 checksum, which the writer computes from the bytes. The content ID and owner are excluded. The form follows how the request supplied the content, not where the bytes are stored: content supplied inline keeps this form if the writer stages it instead, and staged or uploaded content always keeps the reference form. The same bytes sent once inline and once as an uploaded object produce different fingerprints.

### B.3 Preconditions

The precondition list appears after `message` and retains caller order without sorting or deduplication. Each precondition starts with `kind`, followed by:

| Kind | Fields after `kind`, in order |
| --- | --- |
| `namespace_head` | `expected_head_seq` |
| `file_revision` | `inode_id`, `expected_revision_no` |
| `attributes_revision` | `inode_id`, `expected_attributes_revision_no` |
| `access_revision` | `inode_id`, `expected_access_revision_no` |
| `path_binding` | `path`, `expected_inode_id`, `expected_binding_generation` |
| `path_absence` | `path` |

Precondition inode IDs use their numeric storage representation, not public `ino_` strings. Every listed field is written. Optional fields are `null` when absent.

Precondition namespace generation, sequence, and revision values are JSON integers. Paths use validated absolute spelling, and binding generations remain opaque strings. Preconditions affect request identity and validation; they add no separate WAL field or replay delta.

### B.4 Strings, integers, and example bytes

Non-ASCII characters are encoded directly as UTF-8. JSON quotes, backslashes, and control characters are escaped; `/` is not escaped. Integers use decimal digits without leading zeroes. The fixed field order above is retained; it is not a general instruction to sort every JSON object's fields alphabetically.

For example, the following is the complete canonical preimage for one directory-creation request. There is no trailing newline in the bytes being hashed:

```json
{"domain":"loonfs.commit.semantic.v1","namespace_id":"demo","actor_id":"usr_8f3c","operations":[{"kind":"create_directory","path":"/reports","parents":false}],"message":null,"preconditions":[]}
```

Its fingerprint is:

```text
v1:sha256:a64ec097a98f4c881c3770868ad31e77efd8ec9fedb6101672de2091311666ba
```

A one-operation convenience call and a one-element commit request use the same canonical input. The wire request can omit defaults that the canonical operation writes explicitly; its raw request JSON is not the fingerprint preimage.

The complete shared vectors are in [commit_fingerprints_v1.json][fingerprint-vectors]. They cover every operation, every precondition variant, path bindings with and without generations, path absence, an empty precondition list, and operations with and without inline preconditions. Encoders must preserve those exact bytes and digests.

## Appendix C. Timing and size reference

Publication and collection use the timing relationships below. Configurable sizing targets are listed separately in C.4. The reference values are defined in the [limits module][limits-source].

### C.1 Publication and collection timing

| Constant | Milliseconds | Interpretation |
| --- | ---: | --- |
| `WAL_PUBLISH_BUDGET_MS` | 60,000 | Observing the planning tip through initiation of its next numbered put. |
| `PIN_VERIFY_BUDGET_MS` | 60,000 | Pin write through completion of post-write verification. |
| `METADATA_PUBLICATION_BUDGET_MS` | 900,000 | First output through initiation of a bounded manifest publication. |
| `PROVIDER_OPERATION_DEADLINE_MS` | 120,000 | Shared client-operation retry budget. |
| `PROVIDER_ATTEMPT_TIMEOUT_MS` | 30,000 | One control-operation attempt. |
| `GC_SAFETY_MARGIN_MS` | 180,000 | Combined relative-clock, timestamp-precision, and scheduling allowance. |
| `GC_MIN_GRACE_WINDOW_MS` | 1,230,000 | Derived minimum ordinary collection grace. |
| `GC_DEFAULT_GRACE_WINDOW_MS` | 3,600,000 | Default configured ordinary grace. |
| `UNREFERENCED_SEGMENT_MIN_AGE_MS` | 86,400,000 | Segments must be strictly older than this before unreferenced collection. |
| `METADATA_COMPACTION_BUDGET_MS` | 85,170,000 | Maximum elapsed time before initiating streaming publication. |
| `DIRECT_TRANSFER_URL_TTL_MS` | 900,000 | Lifetime of a direct transfer capability. |
| `NAMESPACE_RETIREMENT_GRACE_MS` | 2,130,000 | Minimum grace from the deletion call clock. |

A provider attempt can begin before its operation deadline and finish within its separate timeout. The grace therefore includes both terms. This client-side calculation does not prove that a timed-out remote mutation had no effect.

```text
GC_MIN_GRACE_WINDOW_MS
    = max(WAL_PUBLISH_BUDGET_MS, PIN_VERIFY_BUDGET_MS,
          METADATA_PUBLICATION_BUDGET_MS)
      + PROVIDER_OPERATION_DEADLINE_MS
      + PROVIDER_ATTEMPT_TIMEOUT_MS
      + GC_SAFETY_MARGIN_MS

METADATA_COMPACTION_BUDGET_MS
    = UNREFERENCED_SEGMENT_MIN_AGE_MS - GC_MIN_GRACE_WINDOW_MS

NAMESPACE_RETIREMENT_GRACE_MS
    = METADATA_PUBLICATION_BUDGET_MS
      + max(GC_MIN_GRACE_WINDOW_MS,
            DIRECT_TRANSFER_URL_TTL_MS + PROVIDER_OPERATION_DEADLINE_MS
            + PROVIDER_ATTEMPT_TIMEOUT_MS + GC_SAFETY_MARGIN_MS)
```

Configured grace `T` cannot be below the minimum. Retirement uses the greater of `T` and the retirement minimum. Segment age and completed-content grace use their fixed constants. Changing a publication bound or its safety relationship changes the protocol, not just a scheduling preference.

### C.2 Upload admission

| Constant | Milliseconds | Interpretation |
| --- | ---: | --- |
| `UPLOAD_SESSION_LEASE_MS` | 86,400,000 | Open-session lifetime on the creating host. |
| `COMPLETED_UPLOAD_RECEIPT_WINDOW_MS` | 604,800,000 | Seven-day window in which completed content can issue new receipts. |
| `CONTENT_RECEIPT_TTL_MS` | 3,600,000 | One-hour token lifetime. |
| `COMPLETED_UPLOAD_ADMISSION_WINDOW_MS` | 608,400,000 | Receipt issuance window plus the final token's lifetime. |
| `CONTENT_RECLAMATION_GRACE_MS` | 609,630,000 | Admission window plus minimum GC grace. |

The completed-content interval is seven days, one hour, and twenty minutes thirty seconds. Eligibility is still conditional on the namespace state and reference evidence; reaching that age does not delete referenced content.

A direct multipart upload is also subject to provider transfer geometry. The current reference limits are 10,000 parts, 5 MiB minimum configured part size, 5 GiB maximum configured part size, and 1,000 part capabilities per signing request. These limits do not override a provider's lower maximum object size or other supported-provider constraints.

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

These are reference producer and runtime defaults. A target size can be exceeded by one large row; rows are never split to meet a size target. The stored encoding remains the same when a producer changes these tuning values.

| Setting | Value |
| --- | ---: |
| Target decoded data-block size | 64 KiB |
| Inline filter threshold | 1,024 stored bytes |
| Target segment rows | 65,536 |
| Target decoded segment size | 8 MiB |
| Maximum bounded reorganization input runs | 8 |
| Maximum bounded reorganization input rows | 131,072 |
| Maximum bounded reorganization decoded input | 64 MiB |
| Automatic WAL-flush threshold | 32 segments |
| Unflushed-tail write rejection threshold | 128 segments |
| Hint-raise threshold | 8 WAL objects |
| `RuntimeCacheConfig::manifest_revalidation_interval_ms` | 1,000 ms |
| Maximum commit-message size | 4,096 bytes |

`manifest_revalidation_interval_ms` is the minimum monotonic interval between checks for a successor to the cached manifest. It also paces the writer's hint raise.

A decoder cannot use target block or segment sizes as hard allocation bounds. The reference block reader initially reserves at most the smaller of `decoded_bytes` and 64 KiB. Further allocation follows bytes actually decompressed. Output stops at the declared length plus one byte as specified in Appendix A.7; vector capacity can exceed that output length. This does not impose a smaller maximum block size. Request admission limits are specified in the [API specification][api-spec].

The hard WAL document limit is specified in [Appendix A.5](#a5-wal-records).

## Appendix D. Grep extension format

Grep is derived from authoritative file content and namespace history. It is optional and separately stored. Core manifests contain no grep pointer, index watermark, or segment references. A new fork has no grep index until the extension builds one for that target.

### D.1 Objects and publication

```text
namespaces/{namespace_id}/extensions/grep/
├── hint.json
├── manifests/{manifest_no:020}.json
└── segments/{segment_id}.sst.zst
```

The `grep_hint` version-1 JSON payload contains `namespace_id` and `manifest_no`. Enabling writes a hint naming manifest 1, then creates manifest 1 with put-if-absent. A hint collision is permitted; the manifest put decides installation. Later publications use the next contiguous number.

A `grep_manifest` version-1 payload contains `namespace_id`, `generation`, `manifest_no`, `status`, `index`, and `segments`. The generation is the namespace generation the index was built for; an index from another generation is rebuilt from a fresh checkpoint. Its namespace and number must agree with the key. Both envelopes verify their stored payload checksum and reject unknown kinds, versions, fields, and invalid nested state. The hint contains no separate manifest checksum.

`index_stored_bytes` is the sum of `index_block.offset + index_block.stored_bytes` for its referenced segments, using checked arithmetic. This calculation needs no segment reads. Report it with the grep manifest number and full indexing status, including any partial-commit position. Confirmed absence means zero referenced index bytes; a read failure remains an error.

Discovery loads the hinted manifest and probes successive numbers until not-found. A missing hint or missing manifest 1 means grep is not enabled. A missing higher hinted manifest is corruption. Queries validate a cached manifest with a HEAD of its successor on every query; a present successor reloads discovery. Decoded manifests can be cached by namespace and number.

A step writes segments first, then publishes the next manifest with put-if-absent. A losing publisher reloads durable state and re-plans against current inputs. A successful publisher raises the hint with CAS, taking the greater number. A failed hint raise does not undo publication.

Every output-producing step must initiate publication within `METADATA_PUBLICATION_BUDGET_MS`, measured from before its first output. It writes no manifest after that bound. Grep uses bounded steps rather than core streaming compaction's epoch claim; conditional numbered publication and input revalidation control competing steps.

### D.2 Index lifecycle

The manifest's `status` contains the position fields defined for that lifecycle state:

| Kind | Status fields |
| --- | --- |
| `backfilling` | `target_seq`, `checkpoint_id`, optional `cursor_inode_id` |
| `active` | `built_through_seq`, `next_event_index` |
| `disabled` | No additional status fields |

Backfill's `target_seq` is the pinned checkpoint's sequence. Its cursor resumes strictly after the last inode ID. An active index's `next_event_index` is zero at a commit boundary. A disabled index contains no segments and no reorganization work.

The nested `index` contains `next_run_no` and optional `reorganize`. Reorganization contains `snapshot_segment_ids`, `output_segment_ids`, `row_key_cursor`, `output_level`, and `run_no`. Its cursor is inclusive. Input and output segment IDs must be unique, disjoint, and present in the manifest's segment list. Each output must have the recorded level and run number. The extension can rebuild its state from a fresh core checkpoint.

Grep segment descriptors use the shared run vocabulary: `run_no`, `run_seq`, `segment_index`, `row_count`, and inclusive minimum and maximum keys. The complete descriptor fields are `segment_id`, `run_no`, `run_seq`, `level`, `segment_index`, `row_count`, `min_row_key`, `max_row_key`, `index_block`, `filter_block`, optional `filter_inline`, and `object_checksum`. The block handles and checksums have the same representation as metadata segment descriptors.

Grep uses a numeric `level` rather than the core's base/delta tier. Level 0 is delta output, level 1 is an intermediate merge, and level 2 is the base. In-progress reorganization records the output level and run number so resumed steps continue the same run. Every descriptor and in-progress output run number must be below `next_run_no`.

### D.3 Tokenization and postings

A revision is eligible for the version-1 gram index when its content is at most 8 MiB and the first 8 KiB contains no NUL byte and passes the text check. The sample must be valid UTF-8, except that an incomplete final character is accepted when it follows a nonempty valid prefix. This is a sample check; bytes after it are not required to be UTF-8.

For each eligible revision, the tokenizer folds ASCII letters to lowercase and takes every overlapping three-byte window. Grams are bytes, not Unicode characters. This folding is distinct from the Unicode rule used for filename comparison.

For example, `Abcd` produces the byte grams `abc` and `bcd`, represented as `616263` and `626364`. A gram is encoded as six lowercase hexadecimal characters.

A row is a kind-tagged CBOR object with kind `gram_postings`. Its fields after `kind` are `gram`, `first_inode_id`, and `postings`. The gram is a hexadecimal string, the inode ID is an integer, and `postings` is a CBOR byte string containing the packed batch. `first_inode_id` must equal the first posting's inode ID. The row key is:

```text
gram-{gram_hex}-{first_inode_id:020}
```

Its filter key is `gram-{gram_hex}`. Several rows can contain batches for the same gram, within a segment or across segments. A reader unions those batches.

A posting identifies `(inode_id, revision_no)`, not a path. Batches are strictly ordered by that pair and encoded as unsigned LEB128 values:

```text
posting_count
first_inode_id
first_revision_no
next_inode_id - previous_inode_id
next_revision_no
... repeated for the remaining postings
```

Revision numbers are unsigned 32-bit absolute values; inode IDs are unsigned 64-bit values, delta-encoded after the first. Empty batches, unordered postings, and trailing bytes after the declared batch are invalid.

The segment framing is exactly the data/filter/index format in Appendix A, with grep CBOR rows instead of metadata rows. Per-block CRCs verify ranged reads. `object_checksum` is the SHA-256 of the complete stored object.

The tokenizer, posting representation, and row-key meaning are governed by grep manifest version 1. They are not implementation-only choices that can change while an existing index is interpreted under the same version.

### D.4 Grep collection

Each explicit call discovers the current grep manifest, builds its live segment set, and lists `manifests/` and `segments/` from beginning to end using one supplied `now_ms`. The collector stores no durable progress cursor.

The current manifest protects all listed segments, including pending reorganization inputs and outputs. Manifest numbers at or above the observed hint remain available for discovery, but intermediate manifests do not protect additional segments. An invalid or unreadable current manifest stops the call before deletion.

| Candidate on a live namespace | Collection rule |
| --- | --- |
| Manifest below the observed hint | Its own provider age and its immediate successor's age must meet ordinary grep grace. An absent successor does not prevent deletion. |
| Segment outside the current live set | Provider age must be strictly greater than 24 hours. |
| Unknown age or unrecognized key | Retain. |

Ordinary grep grace is one hour in the reference implementation. The output publication budget plus the minimum GC grace fits inside the segment minimum age, using the same provider and clock assumptions as core collection.

For an absent or deleted core namespace, an explicit grep collection call can reap the entire extension prefix after ordinary grace, including its hint. Core GC never collects extension objects. Grep never enumerates namespaces; callers and hosts select which namespace to maintain.

[api-spec]: api.md
[provider-spec]: object-storage-providers.md
[limits-source]: ../../crates/loonfs-core/src/limits.rs
[fingerprint-vectors]: ../../crates/loonfs-api/tests/golden/commit_fingerprints_v1.json
[name-vectors]: ../../crates/loonfs-api/tests/golden/name_folding.v1.json
