# LoonFS glossary

Terms used across the LoonFS specs, grouped by topic. See [Storage format](format.md) for durable layouts and [API](api.md) for wire operations.

## Namespaces and history

### Namespace

One directory tree with its own history under `namespaces/{namespace_id}/`, including its own manifests, WAL, and retention floor. Every namespace belongs to one [pool](#pool), which stores its content objects and upload records. A namespace whose newest manifest has status `initializing` is still being created or forked and is treated as absent except by create, fork, or delete calls for that ID. See [Namespaces and identity](format.md#11-namespaces-and-identity).

### Manifest

A numbered, immutable object describing a namespace: its identity and status (`initializing`, `active`, or `deleted`), current writer epoch, metadata segment runs, and [retention floor](#retention-floor). The highest-numbered manifest above the [manifest boundary](#manifest-boundary) is current, and commits published after it sit in later WAL objects. See [Manifests, runs, and segments](format.md#71-manifests-runs-and-segments).

### Manifest boundary

A small control object (`namespaces/{namespace_id}/boundary.json`) storing either a manifest number or the terminal state `retired` (`{"kind":"retired"}`). Every manifest numbered at or below the boundary is dead (no process publishes there or treats it as newest, even if a snapshot or pending GC still keeps the object on disk).

Only a namespace [collection](#collection) raises the boundary, right before deleting old manifests, and [namespace retirement](#namespace-retirement) raises it to `retired` (which sorts above every manifest number and is never deleted, preserving the tombstone of the namespace ID). Cold readers list manifests strictly above the boundary and re-read it after loading, while publishers and cached readers re-check it to discard obsolete state. See [Resolving the metadata basis](format.md#41-resolving-the-metadata-basis) and [One complete collection](format.md#111-one-complete-collection).

### Publication ID

A random ID (`publication_id`, such as `pub_5b7tx2k9wq0mhd4yzc8vn3jr6e`) that each put of a [manifest](#manifest) generates and stores in the manifest, new for every put, even a put of content that an earlier put already tried. LoonFS never interprets it. It lets a publisher whose put lost its acknowledgment recognize its own manifest when it reads back the manifest at that number. See [Conflicting and unknown installations](format.md#93-conflicting-and-unknown-installations).

### WAL

The write-ahead log: a namespace's sequence of numbered, immutable WAL objects. Each WAL object contains one or more commits, one [fence](#fence), or one [floor object](#floor-object). Writing the next WAL object commits every record inside it. See [Publishing a commit](format.md#6-publishing-a-commit).

### WAL tip

The highest-numbered WAL object in a namespace. Readers and writers find it by reading WAL numbers upward until one is missing, and writers commit at `tip + 1`.

### WAL tail

All WAL objects numbered after the current manifest's `folded_wal_no`. Readers replay the tail on top of the manifest until a [fold](#fold) moves its records into metadata segments.

### Commit

One published mutation request. A commit can bundle multiple operations that apply atomically at a single sequence number.

### Sequence (`seq`)

A monotonic integer ordering commits within a namespace. Because one WAL object can batch multiple commits, sequence numbers and WAL numbers are separate counters (for example, WAL 10 can contain commits 41, 42, and 43).

### Number (`_no`)

An object's 1-based position in a numbered series (`manifest_no`, `wal_no`, or per-file `revision_no`). Each series increments independently.

### Garbage collection (`gc`)

The stateless process that deletes unreferenced stored objects once they pass the [grace window](#grace-window), including obsolete manifests, folded commit WAL objects, unused segments, expired snapshots, and unreferenced content. It never deletes [fences](#fence) or [floor objects](#floor-object). Each invocation runs one [collection](#collection).

### Head

A namespace's latest committed state, identified by its newest commit sequence (`head_seq`). The head isn't stored as a single object: readers compute it by replaying the WAL tail over the current manifest (for example, manifest folded head 40 plus tail commits 41 through 43 gives head 43).

### Folded head

The newest commit sequence included in a manifest's metadata runs (recorded as the manifest's `head_seq`). The namespace's live [head](#head) may be ahead of the folded head if commits sit in the WAL tail. See [Manifests, runs, and segments](format.md#71-manifests-runs-and-segments).

### Commit ID

A client-supplied (or client-generated) idempotency key for a commit. Retrying the exact same request with the same commit ID replays the original result while its [commit receipt](#commit-receipt) is retained. See [Commit identity and retries](format.md#65-commit-identity-and-retries).

### Commit receipt

A stored row mapping a commit ID to the sequence it committed at so LoonFS can detect and replay retries. Compaction can drop receipts below the retention floor, after which reusing that commit ID commits anew.

### Semantic fingerprint

A digest of a commit's logical request (namespace, actor, operations, preconditions, and message, excluding writer epoch and timestamp). On commit ID reuse, LoonFS compares fingerprints to distinguish an idempotent retry from a conflicting reuse (`commit_id_reuse_conflict`). A [merge's](#merge) fingerprint hashes its merge record instead of its planned operations.

### Change feed

A namespace's committed filesystem events in sequence order (and operation order within each commit), available for commits above the retention floor.

### Precondition

An assertion about current namespace state that must be true for a commit to be accepted (for example, that a file is still at revision 7).

## Files and directories

### Inode

The durable identity of a file or directory, along with its kind and creation metadata. An inode ID is a 128-bit identifier (such as `ino_000000r0000013kje0d47gc8kw`) whose high 64 bits pack the low 32 bits of the creating commit sequence and the per-commit create index, and whose low 64 bits are random. The root directory is all zero bits. Moving or renaming preserves the inode ID, while deleting and recreating creates a new inode. See [Namespaces and identity](format.md#11-namespaces-and-identity).

### Directory binding (direntry)

The link placing a child inode under a parent directory and name. Renaming or moving removes the old binding and writes a new one. See [Directory bindings](format.md#13-directory-bindings).

### Binding version

An opaque token identifying an item's current directory binding (encoding the bind's `bind_position`). Moving, renaming, or restoring produces a new binding version, while content and attribute updates leave it unchanged. Callers pass it as a precondition on moves and deletes.

### Path

An absolute slash-separated name (such as `/reports/final.txt`) resolved by walking directory bindings from the root.

### Display name

An entry's filename preserved exactly as supplied by the caller. Sibling comparisons use the [name key](#name-key) instead, while listings return the display name.

### Name key

The canonical comparison key derived from a display name via Unicode normalization and case folding, used identically in every namespace (so `Report.txt` and `report.txt` share one name key). See [Names and paths](format.md#14-names-and-paths).

### Revision

One committed version of a file's contents, assigned a per-file monotonically increasing revision number (`revision_no`) independent of namespace sequence numbers.

### Revision summary

The current-revision metadata copied into a file's directory binding rows: revision number, content reference (including size), and committing actor and timestamp. Writing a new revision writes a summary row under the file's binding so directory listings don't have to read file revision rows. See [Directory bindings](format.md#13-directory-bindings).

### Tombstone

A committed deletion record. Deleting an item removes its binding from its parent directory and writes a tombstone, leaving the item's revisions and descendants in place while hiding them like a filter. A [restore](#restore) revokes the tombstone and rebinds the root so the subtree reappears intact. Once the [retention floor](#retention-floor) reaches the deletion sequence, the deletion expires: it can no longer be restored, and base compactions remove the subtree's rows, the tombstone included. See [File and subtree deletion](format.md#16-file-and-subtree-deletion). (A deleted namespace's final manifest is a separate record, the [namespace tombstone](#namespace-tombstone).)

### Restore

An operation recovering prior file or directory state. LoonFS has two forms:

- **Deleted-item restore (`restore_deleted`).** Revokes an active [tombstone](#tombstone) in the [trash](#trash) and rebinds the deleted root (in place or at a new destination), bringing back the subtree and its revision history and emitting a `restored` event.
- **Revision restore (`restore_revision`).** Publishes an earlier file revision's content as a new current revision, emitting a `content_changed` event.

See [File and subtree deletion](format.md#16-file-and-subtree-deletion).

### Trash

All deletions in a namespace that can still be undone: tombstones whose latest event is `set` and whose `deletion_seq` is strictly above the [retention floor](#retention-floor). Stored as the `trash` [query index](#query-index) and listed oldest first via `list_trash`. A deletion leaves the trash when restored or when the retention floor reaches its `deletion_seq`. See [File and subtree deletion](format.md#16-file-and-subtree-deletion).

### Namespace tombstone

A deleted namespace's final manifest recording that the namespace was deleted and when. It stays the newest manifest (protecting its listed segments) until [namespace retirement](#namespace-retirement), after which collection deletes it once it passes the [grace window](#grace-window). See [Deleting a namespace](format.md#94-deleting-a-namespace).

## Content

### Content object

An immutable object storing file bytes under the namespace's [pool](#pool) prefix (`pools/{pool_id}/content/`). A file's bytes may be stored in one object or span several. See [File contents and ownership](format.md#15-file-contents-and-ownership).

### Content reference

A revision's pointer to its bytes: `kind`, `content_id`, `size_bytes`, and `checksum`. Combined with the reading manifest's `pool_id`, it resolves identically across every member of the [pool](#pool). It does not guarantee a standalone content object exists yet, since unfolded inline or appended bytes may still sit in the WAL.

### Chain

The byte sequence addressed by one content ID. Appending to a file extends its chain, and each revision references a prefix of that chain (for example, revisions 1, 2, and 3 referencing the first 100, 200, and 210 bytes of chain A).

### Chain creator

The namespace whose commit created a chain, recorded in its [layout](#layout). Only the creator extends the chain in place. When another member of the [pool](#pool) appends to it, LoonFS starts a new chain that references the original chain's extents as shared base extents. See [Operations and WAL deltas](format.md#66-operations-and-wal-deltas).

### Extent

A byte range inside one content object representing part of a chain, storing the object reference, offset, length, and the provider's stored whole-object checksum ("object checksum").

Extents are either "own extents" (written for the chain itself) or "shared base extents" (borrowed from an earlier chain when a fork or branched revision appends). A fold in the chain's creator may merge a shared base extent that takes the whole of its object (a span object or an upload's whole object) into a new own extent, which copies those bytes. A shared base extent that takes part of its object is never merged, and no other write of a chain's pieces merges shared base extents.

### Layout

The ordered list of extents making up a chain's bytes plus its [chain creator](#chain-creator), stored as a metadata row and used by readers to map byte offsets to content objects.

### Span object

Any content object of a chain other than the chain's "whole object" (`content/{content_id}`). Its key appends a random span ID (`{content_id}-{span_object_id}`, such as `con_4z59p0wjvx9a8b2frdhhf6mat1-span_1q8yd3a2gxmz0w4pke6tcv5rhb`), and its layout entry records the chain byte range it covers. Two independent writes of the same bytes produce distinct span objects.

### Piece

A range of file bytes stored inside a WAL object (either a complete inline file or appended bytes). Folds write pieces to content objects as extents before publishing a manifest covering that WAL object. Copy validation, merge planning, and forking a source with a WAL tail can also materialize another member's pieces into extents so they don't wait for that member's fold.

### Inline content

Small file bytes committed directly inside a WAL object as pieces rather than uploaded first. A fold writes them to a content object before the WAL object becomes eligible for garbage collection. See [File contents and ownership](format.md#15-file-contents-and-ownership).

### Assembly

An object-store operation building one new immutable content object from existing object ranges plus optional tail bytes. Folds use assemblies to write chain extents, using server-side part copies (S3, R2, Azure) or composes (GCS) where supported, or client-side read-and-write fallback otherwise. A GCS assembly writes its intermediate objects (for example, when it composes more than 32 sources) as span objects of the destination's chain, and deletes them when it finishes. See [Immutable writes and retries](format.md#32-immutable-writes-and-retries).

### Upload session

The stored state of one upload under the namespace's [pool](#pool) prefix, consisting of two write-once records that each store the owning namespace ID: the session record (written at start with content ID, mode, and expiry) and the outcome record (written on completion or abort). Status (`open`, `completed`, or `aborted`) is derived from which records exist. Completing an upload does not change any namespace file until a later commit publishes it. See [Upload sessions](format.md#51-upload-sessions).

### Presigned URL

A short-lived URL (15 minutes in the reference implementation) signed by the server for direct client-to-provider transfer, returned in an `access` object of kind `presigned_url` with required signed headers. Anyone possessing the URL can use it until it expires. See [Download transport](api.md#610-download-transport).

### Admission evidence

Short-lived proof tied to a namespace and exact content reference showing that an upload's bytes were verified, or that the runtime committing them wrote them itself. Commits require it before referencing uploaded content, and the HTTP server represents it as a signed content token. See [Admission proofs](format.md#55-admission-proofs).

### Content root

Any reference protecting a [pool's](#pool) content objects from deletion: layouts in a kept manifest, unfolded WAL tails, or upload session records across any member of the pool. See [Content roots](format.md#119-content-roots).

## Metadata storage

### Metadata segment

An immutable object storing sorted rows from one [row family](#row-family) in independently readable blocks. Manifests list each segment with a descriptor containing block locations and the provider's stored whole-object checksum.

### Run

The set of metadata segments produced together by one fold or compaction, identified by a run number and listed in the manifest.

### Row family

A set of metadata rows sharing one key grammar (such as `revisions` or `direntry_binds`). Every family is an [entity header](#entity-header), a [history family](#history-family), or a [query index](#query-index). See [Row families](format.md#19-row-families).

### Entity header

A row family storing one immutable row per entity at creation: `inodes` (kind and creation attribution) and `commits` (commit record).

### History family

A row family storing versions of a mutable entity attribute (such as file revisions or inode attributes), keyed by entity and version newest first. Each row records the sequence, ID, actor, and timestamp of its commit.

### Query index

A derived row family indexing facts from another family under an alternate key (for example, `commit_receipts` indexed by commit ID so retries avoid scanning `commits` by sequence).

### Family group

A set of related row families compacted together to keep them consistent (for example, both directory binding indexes form one group, and `commits`, `commit_receipts`, and `merges` form another).

### Fold

Writing committed records from the WAL tail into new metadata segments and publishing the next manifest so readers replay fewer WAL objects. Folding does not change visible history, and the namespace's writer session runs it. See [Folding the WAL](format.md#7-folding-the-wal).

### Fold age

A setting, in milliseconds, of how old the oldest unfolded commit of a [WAL tail](#wal-tail) can be before a [fold](#fold) of the tail is due, whatever the tail's size. It's measured from the commit's `committed_at_ms`, so a tail below the fold thresholds is folded even while its writer keeps committing. For example, with the default of 15 minutes, a writer that commits every 10 minutes has its commits at 0 and 10 minutes folded at 15 minutes. The fold age always applies, and it has to be below `MAX_TAIL_COMMIT_AGE_MS` (11 hours and 45 minutes), the age at which a fork or a merge folds a tail first. A tail without a commit (a tail of fences alone) never comes due by age. See [Recovery material and maintenance policy](format.md#73-recovery-material-and-maintenance-policy).

### Compaction

Rewriting a window of runs within one family group (up to 8 runs by default) into fewer runs while dropping obsolete rows, without altering any retained view. Publishes once at the end. See [Retention and compaction](format.md#10-retention-and-compaction).

### Retention floor

The sequence boundary below which full history is no longer guaranteed. Above the floor, file history and trash are complete. At or below the floor:

- Visible files and recoverable deleted files keep at least their newest revision, while compaction can drop older revisions, superseded bindings, old attribute/access rows, and commit receipts.
- Deletions with `deletion_seq <= retention_floor_seq` expire (cannot be restored, leave trash, and have their hidden rows removed by base compaction).

The floor only moves forward, in a manifest that the namespace's writer publishes for a retention advance while it pauses its commit queue for the fold of what arrived (ordering advances against restores), or in a fork's active manifest. A [read view's](#read-view) effective floor is the maximum of its manifest's floor and any floor objects in its WAL tail. Snapshots below the floor still protect their manifests. See [Advancing the retention floor](format.md#101-advancing-the-retention-floor).

### Retention window

An optional per-namespace duration in milliseconds (at least the 24-hour [grace window](#grace-window)) controlling how much history the reference server's [sweep](#sweep) retains before advancing the [retention floor](#retention-floor). Stored as `retention_window_ms` in the manifest (defaulting to the server's `namespace.default_retention_window_ms` when omitted on create). Namespaces without a window aren't advanced automatically. See [Advancing the retention floor](format.md#101-advancing-the-retention-floor).

### Control object

A small object coordinating discovery, cleanup, retained views, uploads, or pool membership: the [manifest boundary](#manifest-boundary), snapshots, upload session and outcome records, and [member entries](#member-entry).

## Reads and retained views

### Read view

The consistent in-memory namespace state observed by one read (or a grouped request), with its effective [retention floor](#retention-floor) raised by any [floor objects](#floor-object) in its WAL tail. A read view is ephemeral and does not protect objects from garbage collection.

### Metadata basis

The verified set of metadata runs from the current manifest that a read starts from. See [Resolving the metadata basis](format.md#41-resolving-the-metadata-basis).

### Snapshot

A stored record pinning one manifest and all of its listed segments so garbage collection keeps that historical state readable. Every snapshot has one of two owners:

- **User snapshot (`owner: user`).** Created and deleted by callers via the snapshot API, with an optional expiry time. Accepted as `snapshot_id` by reads and forks while unexpired (expired user snapshots are hidden from normal snapshot calls but remain in the maintenance inventory until GC deletes them). Merges, rebases, and live-head forks also create temporary unnamed user snapshots while running.
- **Fork snapshot (`owner: fork`).** Each active fork keeps two fork snapshots until retirement: one on its source protecting the inherited source manifest, and a "fork point snapshot" on itself protecting its state at the fork point (accepted as `snapshot_id` on reads of the fork via `fork_basis.fork_point_snapshot_id`).

The maintenance snapshot inventory lists both kinds. See [Snapshots](format.md#8-snapshots).

### Fork

A new namespace created as a zero-copy clone of another namespace at a point in history. A fork joins its source's [pool](#pool), writes an `initializing` manifest 1, installs its fork snapshots and pool [member entry](#member-entry), folds any source WAL tail into an initializing manifest of its own (folding the source first only if a tail commit exceeds `MAX_TAIL_COMMIT_AGE_MS`), and publishes its `active` manifest. Later commits stay separate unless applied via [merge](#merge). See [Forking a namespace](format.md#92-forking-a-namespace).

### Source

The namespace an operation reads from: for a fork, its parent namespace, and for a [merge](#merge) or [rebase](#rebase), the fork whose commits are being applied.

### Fork point

The source sequence at which a fork was created (`fork_basis.fork_point_seq`), including the source's WAL tail for live-head forks or the snapshot's folded head for snapshot forks. The fork's initial retention floor, fork point snapshot, and first merge or rebase all start at this sequence.

### Pool

A group of namespaces sharing one content prefix (`pools/{pool_id}/`), identified by a generated immutable `pool_id` (such as `pool_3s0drmj8em84yb4mnetqkz2j0x`) recorded in every member's manifest. A freshly created namespace generates a new pool, and every fork inherits its source's pool.

All content objects and upload records for every pool member are stored under the pool prefix, so forks and merges share content without copying bytes. Pool [collection](#collection) finds members via [member entries](#member-entry), gathers roots from every member, and deletes unreferenced pool objects past grace. Namespace collections never delete content. See [Pools](../design/pools.md).

### Member entry

A write-once control object (`pools/{pool_id}/members/{namespace_id}.json`) recording that a namespace belongs to a [pool](#pool). Written during creation or forking (after manifest 1 and before the active manifest) and deleted during [namespace retirement](#namespace-retirement). Pool collections list these keys to discover members without reading their payloads. See [Creating a namespace](format.md#91-creating-a-namespace).

### Merge

A single commit applying a contiguous range of a fork's commits (`(source_after_seq, source_through_seq]`) into another namespace in the same [pool](#pool) (normally the fork's parent, or a new fork during a [rebase](#rebase)). Its commit record includes a "merge record" reported on the change feed and covered by its [semantic fingerprint](#semantic-fingerprint). Compaction run rewriting is never called a merge. See [Merges](format.md#merges).

### Merge index

The derived `merges` row family storing one row for each range of a fork's commits that a committed merge covers, keyed by that fork and merge sequence newest first. A merge covers its own source's range, and a merge of a rebase's new fork also covers the range of the fork that the new fork's replay commits replayed. Each row records the source of the merge that covered the range. Before committing a merge, LoonFS checks the latest row of each fork that the merge covers, to ensure the fork's commits apply once and contiguously. See [Merges](format.md#merges).

### Rebase

Replaying a fork's unmerged commits onto a fresh fork of its parent namespace when a direct merge conflicts. Non-conflicting changes apply as "replay commits" (merges into the new fork), while conflicts are returned so a person or tool can resolve them before merging the new fork back into the parent. The new fork's `fork_basis.rebased_from` identifies the original fork and starting sequence. Once a merge of the new fork has written a row for the original fork in the parent's merge index, which happens when that merge's range includes a replay commit, the original fork can't merge or rebase into the parent again. See [Merges](format.md#merges).

### Writer position

The in-memory head and manifest that an active writer session plans commits against. Reads in the writer's process start from the writer position instead of probing the WAL. The position advances on every local commit (before acknowledgement) and onto every manifest that the session's [publication lane](#publication-lane) publishes, each of which builds on the session's newest published manifest. Only the session's queues (the commit queue and the publication lane) change the position, so a read never changes it itself.

A manifest check of the position (a HEAD of the next manifest number and a conditional read of `boundary.json`) finds a newer manifest when the session's own publication is between its put and its record, or after another writer's claim. The read then asks the session to settle its position: the session waits for its publication lane, and when the position doesn't follow a newer manifest of the session's own, it loads the namespace again from the store under its writer epoch, which fences the session after a claim. `strong` reads wait for a fresh manifest check, while `eventual` reads wait only past `max_staleness_ms`. See [Replaying the visible WAL](format.md#42-replaying-the-visible-wal).

### Consistency

How recently a live read confirms the namespace [head](#head): `eventual` (the default) skips head confirmation while the last confirmation is younger than the runtime's `max_staleness_ms` setting, while `strong` confirms the head before every read. An HTTP request sets it with the `consistency` parameter, and a Rust caller sets it on the handle that the read goes through. Pinned reads (such as snapshot reads) have no consistency. See [Read consistency](api.md#read-consistency).

### Eventual read

A live read using `eventual` [consistency](#consistency). While the cached head (or local [writer position](#writer-position)) was confirmed less than `max_staleness_ms` ago (1 second by default), the read sends no head-confirmation request and serves monotonically non-decreasing state within that process. See [Read consistency](api.md#read-consistency).

### Revalidation bound

The 20-minute ceiling (`READ_REVALIDATION_BOUND_MS`) on `max_staleness_ms` and manifest check intervals. Past this bound, cached views always re-confirm before serving reads. Because the 24-hour [grace window](#grace-window) exceeds this bound plus presigned URL lifetime and clock skew, content referenced by a cached view stays valid until download URLs issued from it expire. See [Read consistency](api.md#read-consistency) and [Replaying the visible WAL](format.md#42-replaying-the-visible-wal).

### Cursor

An opaque resume token returned by paginated reads or bounded index builds.

### Ranged read

A service-proxied content read fetching a single byte range via an HTTP `Range` header (for reading part of a file or chunking a file larger than the proxied limit). Whole-file checksum verification does not apply to partial reads, though LoonFS verifies any content object, whether an upload's whole object or a [span object](#span-object), that the range covers completely against its extent object checksum. See [Ranged reads](api.md#ranged-reads).

## Writers

### Writer session

A process's active right to write a namespace's WAL and publish its manifests, identified by a monotonic writer epoch. Every commit, fold, compaction result, extension entry, retention window change, and retention floor advance of an active namespace is published by the namespace's writer session, and a process that needs to publish one claims the namespace first. Because epoch takeover through object storage takes around 200 ms, most deployments share one long-lived writer session per namespace inside a server. See [Writer ownership](format.md#61-writer-ownership).

### Publication lane

The second serial queue of a [writer session](#writer-session), beside the queue that sends its WAL puts, through which every manifest publication of the session other than a claim goes: a fold, a compaction result, an extension entry, a retention window change, a deletion's manifests, and a snapshot's record put with its verification. The lane sends one manifest put at a time, and each publication builds on the manifest that the one before it published, so two publications of one session never put at the same manifest number, and a commit's WAL put doesn't wait for a manifest put. A claim keeps the lane and the queue of WAL puts while it runs, so it waits for the publication in flight. See [Publishing a manifest](format.md#72-publishing-a-manifest).

### Writer epoch

A monotonic counter in the manifest incremented each time a new writer session claims a namespace. Once the new session writes its fence, older epochs cannot commit.

### Epoch claim

A manifest publication incrementing the writer epoch by 1. The claim must be followed by a fence before the previous writer is blocked.

### Fence

A commitless WAL object written by a new writer session at the next WAL number to block commits from older writer epochs. Fences are never deleted (even after retirement), ensuring stale WAL writes always collide.

### Floor object

A commitless WAL object recording a `retention_floor_seq`. A view that replays one raises its floor to it, and a fold that covers one copies the floor into its manifest. No writer creates one, because the writer raises the floor in a manifest. Floor objects are never deleted. See [Advancing the retention floor](format.md#101-advancing-the-retention-floor).

### Job

One maintenance API request (`POST .../jobs`) and the work it executes, selected by `kind` (`metadata`, `retention`, `retention_window`, or `gc` on namespaces, or `gc` on pools). Jobs run to completion, return a report, and never create snapshots. See [Maintenance jobs](api.md#maintenance-jobs).

### Step

One bounded unit of background maintenance work on a namespace (a metadata fold/compaction step, a collection step, or a grep index step). The reference server runs due steps for open sessions every maintenance interval (5 seconds by default) and for all namespaces during a [sweep](#sweep).

### Sweep

The reference server's full store maintenance pass (at startup and every 24 hours by default): lists every namespace (including deleted and retired ones) to run due steps, collections, and retention-window advances, then lists every [pool](#pool) to run pool collections. Idle namespaces and pool content are maintained only during sweeps. See [Deleting, retaining, and reclaiming](api.md#deleting-retaining-and-reclaiming).

### Compare-and-swap (CAS)

A conditional object update that succeeds only if the object's version token (such as its ETag or generation) still matches an earlier read. Used for raising the [manifest boundary](#manifest-boundary), extending user snapshot expiry, and recording user snapshot deletion.

### Publication budget

The maximum elapsed time allowed between starting a manifest publication and issuing its manifest put (`METADATA_PUBLICATION_BUDGET_MS`). Exceeding it aborts before putting and returns `store_contention` (`cause: "publication_budget"`). WAL puts have no publication budget because fences and floor objects are never deleted. See [Publication and collection timing](format.md#c1-publication-and-collection-timing).

## Deletion and garbage collection

### Collection

One complete, stateless garbage collection pass over a namespace or a [pool](#pool). It reads live roots, lists every object family in scope, and deletes unreferenced objects older than the [grace window](#grace-window) without saving progress between runs:

- **Namespace collection** covers manifests, WAL commit objects (distinguishing fences and floor objects by listed size and keeping them), metadata segments, extension segments, and snapshots.
- **Pool collection** gathers roots from every pool member and covers content objects and upload records.

See [One complete collection](format.md#111-one-complete-collection).

### Grace window

The universal 24-hour safety wait (`GC_GRACE_WINDOW_MS`) applied to unreferenced objects before deletion, exceeding `PUBLICATION_COMPLETION_BOUND_MS` so in-flight writes finish or fail before anything they could reference is removed. It also sets the wait before retiring a deleted namespace and the minimum age of a commit before a retention advance can pass it (keeping replaced revisions readable for at least 24 hours). Completed upload records use a separate window, the "completed-content grace". See [Candidate and age rules](format.md#113-candidate-and-age-rules).

### Namespace retirement

Removing a deleted namespace from its [pool](#pool) once the [grace window](#grace-window) has elapsed after deletion and no snapshots remain on it besides its own fork point snapshot. The namespace's collection deletes its fork snapshots (if any), deletes its pool [member entry](#member-entry), and raises its [manifest boundary](#manifest-boundary) to `retired`. (The tombstone's own content references stop protecting pool content at the retirement deadline.)

After retirement, later namespace collections delete everything under its prefix except `boundary.json`, fences, and floor objects, while requests to the ID continue returning `namespace_deleted`. Retirement itself deletes no content. Pool collection reclaims unreferenced content under its normal age rules. See [Retirement](format.md#95-retirement) and [Retiring an eligible tombstone](format.md#118-retiring-an-eligible-tombstone).

## API

### API group

A bundle of operations advertised together in the capability document only when all of its required operations are implemented (such as `filesystem/v0`).

### Feature

An optional capability within an API group (such as `filesystem.uploads.direct_put`).

### Capability document

The JSON document returned by `GET /v0/capabilities` listing the deployment's protocol version, API groups, enabled features, and advisory limits.

### Extension

An optional subsystem maintaining derived state (such as the grep index) stored as an entry in the namespace manifest pointing to extension segments under `namespaces/{owner_namespace_id}/extensions/{extension_name}/segments/`. Manifest publications copy extension entries forward (tombstones empty their runs), forks inherit them, snapshots pin their segments, and garbage collection deletes unreferenced extension segments under the same age rule as metadata segments. See [Extensions](format.md#124-extensions).

### Service context

An authority mode where an application acts on its own behalf using the full authority of its deployment token (`Loonfs-Context: service`), bypassing per-inode rights checks. Every namespace content request sends either `Loonfs-Context: service` or `Loonfs-Principals` (the user's principals). See [Subject and principals](api.md#subject-and-principals).
