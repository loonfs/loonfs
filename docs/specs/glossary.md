# LoonFS glossary

This glossary defines the terms used in the LoonFS specs, grouped by topic. The [format specification](format.md) defines how each kind of durable data is stored, and the [API specification](api.md) defines requests, responses, and cursors.

## Namespaces and history

### Namespace

A directory tree with its own history, stored under its own prefix in object storage (`namespaces/{namespace_id}/`). Each namespace has its own manifests, WAL, and retention floor, and a fork can share stored objects with the namespace it was forked from. See [Namespaces and identity](format.md#11-namespaces-and-identity).

### Manifest

A numbered, immutable object that describes a namespace: its identity and status (active or deleted), which writer can commit, the metadata segments that make up its files and directories, and how much history it keeps (its [retention floor](#retention-floor)). The highest-numbered manifest is the current one. Commits made after it was published are in the WAL objects that follow it. See [Manifests, runs, and segments](format.md#71-manifests-runs-and-segments).

### Hint

A small object that records the number of a recent manifest, so readers know where to start looking for the current one. The hint is best effort. LoonFS updates it after it publishes a manifest, but the update can lag, so the hint is a safe starting point but not an authoritative pointer to the current manifest. Readers load the hinted manifest and then check for newer ones. See [The current manifest and WAL tip](format.md#24-the-current-manifest-and-wal-tip).

### WAL

The write-ahead log: a namespace's sequence of numbered, immutable WAL objects. Each WAL object contains one or more commits, or a single [fence](#fence). Successfully writing the next WAL object to object storage commits every record contained in that WAL object. See [Publishing a commit](format.md#6-publishing-a-commit).

### WAL tip

The highest-numbered WAL object. A reader or writer finds it by reading WAL numbers upward until one is missing. A writer commits by creating the WAL object one number above the tip.

### WAL tail

The WAL objects after the current manifest's folded boundary (`folded_wal_no`). Their commits aren't in any metadata segment yet, so readers replay them on top of the manifest. A [fold](#fold) moves them into segments.

### Commit

One successfully published mutation request. A request can contain several operations, and they all commit together at one sequence number.

### Sequence (`seq`)

A number that orders the commits in one namespace. Each commit is assigned the next sequence number. One WAL object can contain several commits, so sequence numbers and WAL numbers are separate counters. For example, WAL object 10 might contain three commits at sequences 41, 42, and 43.

### Number (`_no`)

The position of one object in a numbered series, such as a manifest number (`manifest_no`), a WAL number (`wal_no`), or a revision number (`revision_no`). In code, `_no` is the short form of "number" for these positions. Each series is counted separately, so manifest 41 and WAL object 41 are unrelated, and revision numbers are counted per file (see [Revision](#revision)).

### Garbage collection (`gc`)

The process that deletes stored objects that nothing references anymore, such as superseded manifests, folded WAL objects, and unused content objects. Code uses the short form `gc` in names, as in `gc_namespace`. Each call runs one [garbage collection pass](#garbage-collection-pass).

### Head

The namespace's latest committed state, identified by the sequence number of its newest commit (`head_seq`). The head isn't a stored object. A reader finds it by reading the current manifest and replaying the WAL tail. For example, if the current manifest includes commits through sequence 40 and the WAL tail contains commits 41 to 43, the head is at sequence 43.

### Commit ID

An identifier for a commit, chosen by the client, that makes retries safe. It's usually a random ID (the Rust client generates one when the caller doesn't provide it). When a retry uses the same commit ID and the same request, LoonFS returns the original commit's result instead of committing again, for as long as the commit receipt is kept. See [Commit identity and retries](format.md#65-commit-identity-and-retries).

### Commit receipt

A stored row that maps a commit ID to the sequence it committed at. LoonFS uses receipts to recognize retries. Compaction can remove receipts below the retention floor, and after that, a reused commit ID is treated as a new commit.

### Semantic fingerprint

A digest of a commit's logical request: the namespace, the actor, the subject, the operations, the preconditions, and the message, but not details like the writer epoch or the timestamp. When a request reuses a commit ID, LoonFS compares fingerprints to tell whether it's a retry of the original request or a different request (which fails with `commit_id_reuse_conflict`).

### Change feed

The namespace's committed filesystem events, in sequence order (and in operation order within a commit). Clients read the change feed to follow changes to a namespace. It includes commits above the retention floor.

### Precondition

A condition on the namespace's current state that has to be true for a mutation to be accepted (for example, that a file's current revision is the one the client last read).

## Files and directories

### Inode

The identity of a file or directory, along with its kind and creation metadata. An inode's ID doesn't change when the item is renamed or moved within its namespace. Deleting a file and creating a new one at the same path creates a new inode. An inode ID is a 128-bit version 7 UUID whose top 48 bits are the inode's creation time in milliseconds and whose other bits are random, such as `ino_01k087hae0ep1tzg4zw1ghyvqb`, except the root directory's ID, which is all zero bits (see [Namespaces and identity](format.md#11-namespaces-and-identity)).

### Directory binding (direntry)

The link between a parent directory, a name, and a child inode, which places an item in the tree. Renaming or moving an item removes one binding and adds another. See [Directory bindings](format.md#13-directory-bindings).

### Binding version

An opaque token that identifies an item's current directory binding. Moving, renaming, or undeleting an item produces a new binding version. Clients pass it as a precondition on moves and deletes, so the change fails if the item was moved or renamed after the client read it.

### Path

An absolute name, like `/reports/final.txt`, that LoonFS resolves by following directory bindings from the root.

### Display name

A directory entry's name, stored exactly as the caller provided it. LoonFS compares names by their name key instead, so Report.txt and report.txt count as the same name, but the display name keeps the capitalization the caller used.

### Name key

The value LoonFS uses to compare and look up sibling names, the same way in every namespace. It's derived from the display name by Unicode normalization and case folding, so Report.txt and report.txt have the same key. See [Names and paths](format.md#14-names-and-paths).

### Revision

One committed version of a file's contents. Each file revision is assigned a monotonically increasing number that's scoped to its file, so revision numbers are unrelated to the namespace's sequence numbers.

### Tombstone

A committed deletion record. Deleting an item removes its name from its parent directory and records a tombstone. The item's revisions, and everything below it, stay in place, and the tombstone hides them like a filter applied on top of the metadata. An undelete revokes the tombstone and restores the item's name, so the whole subtree reappears unchanged. Once the [retention floor](#retention-floor) reaches the deletion, the deletion has expired. An expired deletion can't be undeleted, its tombstone keeps hiding the subtree, and a base compaction removes the subtree's rows once the writer's log has acknowledged the floor (see [Acknowledged floor](#acknowledged-floor)). See [File and subtree deletion](format.md#16-file-and-subtree-deletion). (A deleted namespace's final manifest is a different record, the namespace tombstone.)

### Namespace tombstone

A deleted namespace's final manifest, which records that the namespace was deleted and when. See [Deleting a namespace](format.md#94-deleting-a-namespace).

## Content

### Content object

An immutable object that stores file bytes, under the owner namespace's prefix (`namespaces/{owner_namespace_id}/content/`). A file's bytes can be stored in one content object or spread across several. See [File contents and ownership](format.md#15-file-contents-and-ownership).

### Content reference

The record in a file revision that identifies its bytes: the owner namespace, a content ID, a size, and a checksum. A content reference identifies content, but it doesn't prove that a content object contains those bytes yet (they may still be stored in the WAL).

### Chain

The bytes stored under one content ID. A chain grows when a file is appended to, and each content reference identifies a prefix of the chain. For example, a log file's first revision might refer to the first 100 bytes of chain A. After two appends, revisions 2 and 3 might refer to the first 200 and the first 210 bytes of the same chain.

### Extent

A byte range of one content object that stores part of a chain. Content objects are immutable, so the bytes appended to a chain are written to new content objects, and a chain's bytes usually end up spread across several objects. Each extent records which content object it's in, the offset in that object, and the length.

Most extents are "own extents", which store bytes written for the chain itself. A "shared base extent" stores bytes of an earlier chain that this chain was started from. For example, a fork inherits `/log.txt`, whose bytes are in a content object owned by the source namespace. The fork doesn't own that chain, so when it appends 10 bytes to the file, it starts a new chain. The new chain's layout lists the source's object as a shared base extent, followed by an own extent that stores the 10 new bytes. A fold never merges a shared base extent with other extents, so the shared bytes are never copied.

### Layout

The ordered list of extents that together make up a chain's bytes. LoonFS stores each chain's layout as a metadata row, and a reader uses it to find which content objects to read for a range of bytes.

### Span object

A content object that stores one byte range of a chain. The content object at a chain's own key (`content/{content_id}`) is called the "whole object". Every other content object of the chain is a span object, and its key is the chain's key followed by the start and end offsets of its range (`{content_id}-{start:020}-{end:020}`).

### Piece

A range of file bytes stored inside a WAL object. A piece contains a whole small value, or the bytes an append adds. Before a fold publishes a manifest that covers that WAL object, it writes the pieces to content objects as extents.

### Inline content

File bytes that are committed inside a WAL object, as pieces, instead of being uploaded first. A fold writes them to a content object before the WAL object can be deleted. See [File contents and ownership](format.md#15-file-contents-and-ownership).

### Assembly

An object-store operation that creates one new immutable object from ranges of existing objects followed by new bytes. A fold uses an assembly to write each chain's new extent. Where the provider supports it, an assembly copies parts inside the provider (S3 and R2) or composes objects (GCS), so the bytes don't pass through the LoonFS server. Otherwise, LoonFS reads the sources and writes the result itself. See [Immutable writes and retries](format.md#32-immutable-writes-and-retries).

### Temporary object

An object that an assembly writes for intermediate results (for example, when GCS composes more than 32 sources) and deletes when it finishes. Temporary objects are stored under `namespaces/{namespace_id}/temporary/`. Nothing references them, so garbage collection deletes any that are older than the reclamation grace (for example, after a crash).

### Upload session

The stored state of one upload, kept as two records that are each written once. The session record is written when the upload starts, and it includes the content ID that's assigned to the upload, the transfer mode, and when the session expires. The outcome record is written when the upload completes or is aborted. The session's status (`open`, `completed`, or `aborted`) follows from which records exist. For example, a session record without an outcome record is an open session. Completing an upload doesn't automatically update a file in a LoonFS namespace. A file changes only when a later commit references the uploaded content. See [Upload sessions](format.md#51-upload-sessions).

### Admission evidence

Proof that a content reference's bytes were verified in a completed upload. A commit needs it before it can reference uploaded content, so a commit can't reference bytes that were never verified. The proof is tied to the namespace and the complete content reference, and it expires. The HTTP server represents it as a signed content token. See [Admission proofs](format.md#55-admission-proofs).

### Content root

A reference that keeps a namespace's content objects from being deleted. There are three kinds: the layouts in a manifest that garbage collection protects, the WAL tail, and upload session records. See [Content roots](format.md#119-content-roots).

## Metadata storage

### Metadata segment

An immutable object that stores sorted metadata rows from one row family (for example, directory bindings), in blocks that can be read independently.

### Run

The set of metadata segments written together by one fold or compaction, identified by a run number. A manifest lists the runs that make up the namespace.

### Family group

A set of related metadata row families that compaction always processes together, so that they stay consistent. For example, the two directory binding indexes form one group, and commits and commit receipts form another.

### Fold

The process that moves committed records from the WAL tail into new metadata segments and publishes a manifest that lists them, so later readers replay less of the WAL. A fold doesn't change the namespace's visible history, and any process can run one. See [Materializing metadata](format.md#7-materializing-metadata).

### Compaction

The process that merges a namespace's metadata runs into fewer runs, and drops rows that are no longer needed, without changing any retained view. Each compaction merges one family group's window of at most eight runs without keeping every row in memory, and publishes once at the end. See [Retention and compaction](format.md#10-retention-and-compaction).

### Retention floor

The sequence number below which a namespace's history is no longer guaranteed. Above the floor, every file's history is complete. At or below it, each visible file keeps at least its newest revision, and so does a deleted file while its deletion is recoverable. Compaction can remove the older rows at or below the floor (superseded bindings, old revisions, and commit receipts). A deletion at or below the floor has expired, so it can't be undeleted, and a base compaction removes the rows that it hides once the writer's log has acknowledged the floor (see [Acknowledged floor](#acknowledged-floor)). A retention advance moves the floor forward, and the floor never moves back. A pin below the floor still protects its manifest. See [Advancing the retention floor](format.md#101-advancing-the-retention-floor).

### Retention window

A namespace setting, in milliseconds, for how much recent history a scheduled retention advance keeps. The reference server's maintenance sweep advances each namespace's [retention floor](#retention-floor) to the last commit made at or before the current time minus the window. It uses the namespace's own window if the namespace's manifest sets `retention_window_ms`, and the server's `retention_window_ms` setting otherwise. For example, a namespace with a 7-day window keeps at least 7 days of revisions and recoverable deletions. See [Advancing the retention floor](format.md#101-advancing-the-retention-floor).

### Acknowledged floor

A manifest field (`acknowledged_floor_seq`) that records the highest [retention floor](#retention-floor) that any WAL object folded into the manifest was validated against. Each WAL object records the floor of the manifest that its writer validated it against (`basis_floor_seq`), and a fold copies the highest one into the manifest. A base compaction removes the rows that a deletion hides only when the deletion is at or below the acknowledged floor, because every undelete validated against a lower floor is folded by then. For example, if the floor is 20 and the acknowledged floor is 5, a deletion at sequence 10 has expired, but its rows stay until a WAL object validated against the floor of 20 is folded. See [Advancing the retention floor](format.md#101-advancing-the-retention-floor).

### Control object

A small object used for discovery, retained views, or upload state: the hint, pins, and the session and outcome records of upload sessions. Each kind has its own update rules (for example, the hint only changes by compare-and-swap, and an upload record is never changed after it's written).

## Reads and retained views

### Read view

The namespace state that one read sees (or a group of related reads in one request). A read view exists only in memory, and it doesn't protect anything from garbage collection.

### Metadata basis

The verified set of metadata runs, from the current manifest, that a read starts from. A fork's manifest lists the runs it inherited from its source. See [Resolving the metadata basis](format.md#41-resolving-the-metadata-basis).

### Pin

A stored record that keeps one manifest, and every segment it lists, from being deleted after newer manifests are published, so the namespace stays readable as it was at that point in its history. A pin has one of two owners:

- A "user pin" is a pin that a caller creates and deletes. The API calls it a "snapshot", and its ID is the `snapshot_id` that the read routes and forks accept. A snapshot can have an expiry time, or none. An expired snapshot can't be read or forked, and it stays listed until garbage collection deletes it. For example, a sandbox can create a snapshot before an agent edits it, and then read or fork the snapshot after the edits.
- A "fork pin" keeps the source manifest that a fork reads through, until the fork is retired.

The maintenance API's "checkpoint" inventory lists every pin of a namespace, so an operator can see what keeps old manifests, and the API calls any pin's ID a `checkpoint_id`. See [Pins](format.md#8-pins).

### Fork

A new namespace that starts as a copy of another namespace at one point in its history. The fork shares the source's stored objects, and the source keeps a pin for the fork until the fork is retired. After the fork, commits to one namespace never appear in the other. See [Forking a namespace](format.md#92-forking-a-namespace).

### Writer position

The head that a writer session plans its next commit against, together with the manifest that head was read against. The session's process keeps the writer position in memory, and reads in that process start from it instead of from a cached view, so they send no WAL probe. The process moves the position to each commit before it acknowledges the commit, and to each manifest it publishes itself. For example, after the session commits sequence 43 above manifest 5, an `eventual` `stat` in the same process reads at sequence 43 against manifest 5 without sending a request. The session's manifest check finds a manifest published above the position's. The position moves onto one with the session's writer epoch, and a new writer's claim drops it. A `strong` read waits for the check, and an `eventual` read waits for it once the read's maximum staleness has passed since the position's last check. See [Replaying the visible WAL](format.md#42-replaying-the-visible-wal).

### Consistency

A setting on a live read that controls how recently LoonFS confirms the namespace [head](#head) before the read. `eventual` (the default, unless the host sets `strong`) confirms the head only when the last confirmation is older than the read's maximum staleness, so a read before then reads at the head that was confirmed last, and it may miss commits acknowledged since. `strong` confirms the head before every read, so the read sees every commit that was acknowledged before it started. A pinned read, such as a snapshot read, has no consistency setting. See [Read consistency](api.md#read-consistency).

### Eventual read

A live read with the `eventual` [consistency](#consistency), which is the default unless the host sets `strong`. An eventual read sends no request to confirm the head while the head was confirmed less than the read's maximum staleness ago (one second by default). For example, a dashboard that lists a directory every 100 milliseconds with a maximum staleness of five seconds confirms the head once every five seconds instead of on every listing. An eventual read sees every commit that was acknowledged before the last confirmation, and it never reads an older head than a read that completed earlier in the same process. In the writer's process, an eventual read starts from the [writer position](#writer-position), and the head's confirmation is the session's last check of the position. See [Read consistency](api.md#read-consistency).

### Revalidation bound

The longest time a reader can trust that no newer manifest exists after it last checked (`READ_REVALIDATION_BOUND_MS`). The bound applies to a cached view, to a [writer position](#writer-position), and to the manifest that a manifest publication builds on. After that, the reader loads the current manifest again before it serves a read, and the publication loads the current manifest through the hint before its put. Garbage collection's minimum grace includes this bound, so it doesn't delete a superseded manifest that a reader might still be using. See [Replaying the visible WAL](format.md#42-replaying-the-visible-wal).

### Cursor

A position that a paginated read, or a bounded index build, resumes from.

### Ranged read

A service-proxied content read that returns one byte range of a file instead of the whole file. A client uses it to read part of a large file, such as the newest records of a log, or to read a file over the proxied read limit in pieces. A client asks for the range with an HTTP `Range` header, such as `bytes=0-99` for the first 100 bytes. The server doesn't verify a ranged read against the file's checksum, because the checksum covers the whole file. See [Ranged reads](api.md#ranged-reads).

## Writers

### Writer session

A process's claim on the right to commit to a namespace, identified by its writer epoch. A writer session starts with a new writer epoch before its first commit, and it keeps using that epoch until a newer writer session takes over. LoonFS coordinates writers through object-storage writes (an epoch claim followed by a fence), so a change of writer takes about 200 ms. Because of that, most applications use one long-lived writer session in a server that many clients share. See [Writer ownership](format.md#61-writer-ownership).

### Writer epoch

A counter in the manifest that increases each time a new writer session takes over. A writer session with an older epoch can't commit once the newer session's fence is written.

### Epoch claim

A manifest publication that increases the writer epoch or the compactor epoch by one. A writer's claim is followed by a fence, and until the fence is written, the previous writer can still commit.

### Fence

A WAL object with no commits. A new writer session publishes a fence as part of starting up, so that it can begin committing with its writer epoch. Once the fence is written, a writer session with an older epoch can't commit. A fence also records the retention floor of the manifest it was planned against, so a maintainer can write one to acknowledge a floor for a namespace that nobody writes to (see [Acknowledged floor](#acknowledged-floor)).

### Compactor epoch

A counter in the manifest, like the writer epoch, that stops a compaction from publishing its result if a newer compactor has claimed the namespace since the compaction started.

### Maintainer

The role that folds a namespace's WAL, compacts its metadata, advances its retention floor, and collects its garbage. The namespace's writer session commits and deletes the namespace, and any process can be its maintainer, with or without the writer session. A compaction claims the compactor epoch first, and a fold or a retention advance publishes under the current compactor epoch without a claim. A maintainer that acknowledges the retention floor of a namespace that nobody writes to claims the writer epoch and writes a fence (see [Acknowledged floor](#acknowledged-floor)). See [Publishing a materialized file set](format.md#72-publishing-a-materialized-file-set).

### Compare-and-swap (CAS)

A conditional update that replaces an object only if it hasn't changed since the caller read it (that is, its compare token, such as an ETag, still matches). LoonFS uses compare-and-swap to raise the hint and to extend a user pin's expiry.

### Publication budget

The longest time allowed between the state a publisher planned against and the start of its numbered put. A put that returns after its budget has an unknown outcome. Garbage collection relies on these budgets to choose a safe reclamation grace. See [Publication and collection timing](format.md#c1-publication-and-collection-timing).

## Deletion and garbage collection

### Garbage collection pass

One complete garbage collection call. A pass loads fresh roots (everything that's still referenced), builds the set of live objects in memory, and deletes the candidates that are old enough. It stores no progress, so after a failure the next pass starts over.

### Reclamation grace

How old an unreferenced manifest, WAL object, content object, or temporary object has to be before garbage collection deletes it. The grace is configurable (one hour by default), but it can't be lower than `GC_MIN_GRACE_WINDOW_MS`. That minimum makes sure every write planned against an older state has finished or failed before garbage collection deletes an object the write might reference. See [Candidate and age rules](format.md#113-candidate-and-age-rules).

### Namespace retirement

The point at which a deleted namespace's own content, and its pin on its source if it's a fork, can be deleted. A deleted namespace is retired once its retirement grace has passed and no pins on it remain. See [Retirement](format.md#95-retirement).

## API

### API group

A set of API operations that a deployment advertises only when it implements all of them, such as `filesystem/v0`.

### Feature

An optional capability within an API group, such as `filesystem.uploads.direct_put`.

### Capability document

The document a deployment returns from `GET /v0/capabilities`. It lists the deployment's protocol version, API groups, features, and advisory limits. Clients typically use it to find out which features are enabled on a deployment before they use them.

### Extension

A subsystem built on top of core LoonFS, such as grep, with its own objects and garbage collection rules under `namespaces/{namespace_id}/extensions/{name}/`. Core garbage collection never deletes extension objects.
