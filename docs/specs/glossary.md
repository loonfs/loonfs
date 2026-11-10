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

The process that deletes stored objects that nothing references anymore, such as replaced manifests, folded WAL objects, and unused content objects. Code uses the short form `gc` in names, as in `gc_namespace`. Each call runs one [collection](#collection).

### Head

The namespace's latest committed state, identified by the sequence number of its newest commit (`head_seq`). The head isn't a stored object. A reader finds it by reading the current manifest and replaying the WAL tail. For example, if the current manifest includes commits through sequence 40 and the WAL tail contains commits 41 to 43, the head is at sequence 43. A manifest's own `head_seq` is its [folded head](#folded-head), which can be earlier than the namespace's head.

### Folded head

The sequence of the newest commit that a manifest's runs include, recorded as the manifest's `head_seq`. A snapshot's folded head is the folded head of its manifest. The namespace's [head](#head) can be later, because the commits after the folded head are in the WAL tail. For example, if a manifest's runs include commits through sequence 40 and the WAL tail contains commits 41 to 43, the manifest's folded head is 40 and the namespace's head is 43. See [Manifests, runs, and segments](format.md#71-manifests-runs-and-segments).

### Commit ID

An identifier for a commit, chosen by the client, that makes retries safe. It's usually a random ID (the Rust client generates one when the caller doesn't provide it). When a retry uses the same commit ID and the same request, LoonFS returns the original commit's result instead of committing again, for as long as the commit receipt is kept. See [Commit identity and retries](format.md#65-commit-identity-and-retries).

### Commit receipt

A stored row that maps a commit ID to the sequence it committed at. LoonFS uses receipts to recognize retries. Compaction can remove receipts below the retention floor, and after that, a reused commit ID is treated as a new commit.

### Semantic fingerprint

A digest of a commit's logical request: the namespace, the actor, the subject, the operations, the preconditions, and the message, but not details like the writer epoch or the timestamp. When a request reuses a commit ID, LoonFS compares fingerprints to tell whether it's a retry of the original request or a different request (which fails with `commit_id_reuse_conflict`). A [merge's](#merge) fingerprint covers its merge record in place of its operations and preconditions.

### Change feed

The namespace's committed filesystem events, in sequence order (and in operation order within a commit). Clients read the change feed to follow changes to a namespace. It includes commits above the retention floor.

### Precondition

A condition on the namespace's current state that has to be true for a mutation to be accepted (for example, that a file's current revision is the one the client last read).

## Files and directories

### Inode

The identity of a file or directory, along with its kind and creation metadata. An inode's ID doesn't change when the item is renamed or moved within its namespace. Deleting a file and creating a new one at the same path creates a new inode. An inode ID is a 128-bit number, such as `ino_000000r0000013kje0d47gc8kw`. Its high 64 bits are the low 32 bits of the creating commit's sequence and the inode's create number, which is 1 for the first inode that the commit creates, and its low 64 bits are random. The root directory's ID is all zero bits (see [Namespaces and identity](format.md#11-namespaces-and-identity)).

### Directory binding (direntry)

The link between a parent directory, a name, and a child inode, which places an item in the tree. Renaming or moving an item removes one binding and adds another. See [Directory bindings](format.md#13-directory-bindings).

### Binding version

An opaque token that identifies an item's current directory binding. It encodes the position of the bind that established the binding (the binding rows' `bind_position`), so moving, renaming, or restoring an item produces a new binding version, and writing the item's content or attributes doesn't. Clients pass it as a precondition on moves and deletes, so the change fails if the item was moved or renamed after the client read it.

### Path

An absolute name, like `/reports/final.txt`, that LoonFS resolves by following directory bindings from the root.

### Display name

A directory entry's name, stored exactly as the caller provided it. LoonFS compares names by their name key instead, so Report.txt and report.txt count as the same name, but the display name keeps the capitalization the caller used.

### Name key

The value LoonFS uses to compare and look up sibling names, the same way in every namespace. It's derived from the display name by Unicode normalization and case folding, so Report.txt and report.txt have the same key. See [Names and paths](format.md#14-names-and-paths).

### Revision

One committed version of a file's contents. Each file revision is assigned a monotonically increasing number that's scoped to its file, so revision numbers are unrelated to the namespace's sequence numbers.

### Revision summary

The fields of a file's current revision that a directory listing reports: the revision number, the size, and the actor and time of the commit that published the revision. LoonFS copies the summary into the bound rows of the file's directory binding. A bind of a file includes it, and each new revision writes a "summary row" under the file's binding with the new summary, so a listing reads the summary from the binding rows instead of from the file's revisions. The summary doesn't include the content reference. See [Directory bindings](format.md#13-directory-bindings).

### Tombstone

A committed deletion record. Deleting an item removes its name from its parent directory and records a tombstone. The item's revisions, and everything below it, stay in place, and the tombstone hides them like a filter applied on top of the metadata. A [restore](#restore) revokes the tombstone and binds the item's name again, so the whole subtree reappears unchanged. Once the [retention floor](#retention-floor) reaches the deletion, the deletion has expired. An expired deletion can't be restored, its tombstone keeps hiding the subtree, and a base compaction removes the subtree's rows. See [File and subtree deletion](format.md#16-file-and-subtree-deletion). (A deleted namespace's final manifest is a different record, the namespace tombstone.)

### Restore

Bringing a deleted file or directory back, or making a prior revision's content current again. Restoring a deletion revokes its [tombstone](#tombstone) and binds the deleted root again, under its saved parent and name or at a new destination, so the whole subtree reappears with its revision history. Only a recoverable deletion can be restored. The API spells this operation `undelete`, and the change feed reports it as an `undeleted` event. Restoring a revision (`restore_revision`) makes that revision's content the file's current content. See [File and subtree deletion](format.md#16-file-and-subtree-deletion).

### Namespace tombstone

A deleted namespace's final manifest, which records that the namespace was deleted and when. See [Deleting a namespace](format.md#94-deleting-a-namespace).

## Content

### Content object

An immutable object that stores file bytes, under the family root's prefix (`namespaces/{family_root_id}/content/`). A file's bytes can be stored in one content object or spread across several. See [File contents and ownership](format.md#15-file-contents-and-ownership).

### Content reference

The record in a file revision that identifies its bytes: a content ID, a size, and a checksum. The reader's manifest supplies the [family root](#family-root), so a reference identifies the same object in every member of a [fork family](#fork-family). A content reference identifies content, but it doesn't prove that a content object contains those bytes yet (they may still be stored in the WAL).

### Chain

The bytes stored under one content ID. A chain grows when a file is appended to, and each content reference identifies a prefix of the chain. For example, a log file's first revision might refer to the first 100 bytes of chain A. After two appends, revisions 2 and 3 might refer to the first 200 and the first 210 bytes of the same chain.

### Chain creator

The namespace whose commit started a chain, recorded in the chain's [layout](#layout). Only the creator's commits extend the chain in place. When another member of the [fork family](#fork-family) appends to the chain, LoonFS starts a new chain that lists the first chain's extents as [shared base extents](#extent). See [Operations and WAL deltas](format.md#66-operations-and-wal-deltas).

### Extent

A byte range of one content object that stores part of a chain. Content objects are immutable, so the bytes appended to a chain are written to new content objects, and a chain's bytes usually end up spread across several objects. Each extent records which content object it's in, the offset in that object, and the length.

Most extents are "own extents", which store bytes written for the chain itself. A "shared base extent" stores bytes of an earlier chain that this chain was started from. For example, a fork inherits `/log.txt`, whose chain the source namespace created. The fork isn't the chain's creator, so when it appends 10 bytes to the file, it starts a new chain. The new chain's layout lists the first chain's object as a shared base extent, followed by an own extent that stores the 10 new bytes. A fold never combines a shared base extent with other extents, so the shared bytes are never copied.

### Layout

The ordered list of extents that together make up a chain's bytes, along with the chain's [creator](#chain-creator). LoonFS stores each chain's layout as a metadata row, and a reader uses it to find which content objects to read for a range of bytes.

### Span object

A content object that stores one byte range of a chain. The content object at a chain's own key (`content/{content_id}`) is called the "whole object". Every other content object of the chain is a span object, and its key is the chain's key followed by the start and end offsets of its range (`{content_id}-{start:020}-{end:020}`).

### Piece

A range of file bytes stored inside a WAL object. A piece contains a whole small value, or the bytes an append adds. Before a fold publishes a manifest that covers that WAL object, it writes the pieces to content objects as extents.

### Inline content

File bytes that are committed inside a WAL object, as pieces, instead of being uploaded first. A fold writes them to a content object before the WAL object can be deleted. See [File contents and ownership](format.md#15-file-contents-and-ownership).

### Assembly

An object-store operation that creates one new immutable object from ranges of existing objects followed by new bytes. A fold uses an assembly to write each chain's new extent. Where the provider supports it, an assembly copies parts inside the provider (S3 and R2) or composes objects (GCS), so the bytes don't pass through the LoonFS server. Otherwise, LoonFS reads the sources and writes the result itself. See [Immutable writes and retries](format.md#32-immutable-writes-and-retries).

### Temporary object

An object that an assembly writes for intermediate results (for example, when GCS composes more than 32 sources) and deletes when it finishes. Temporary objects are stored under `namespaces/{namespace_id}/temporary/`. Nothing references them, so garbage collection deletes any that are older than the [grace window](#grace-window) (for example, after a crash).

### Upload session

The stored state of one upload, kept as two records that are each written once. The session record is written when the upload starts, and it includes the content ID that's assigned to the upload, the transfer mode, and when the session expires. The outcome record is written when the upload completes or is aborted. Both records are stored under the family root's prefix, and each one records the namespace that created the session. The session's status (`open`, `completed`, or `aborted`) follows from which records exist. For example, a session record without an outcome record is an open session. Completing an upload doesn't automatically update a file in a LoonFS namespace. A file changes only when a later commit references the uploaded content. See [Upload sessions](format.md#51-upload-sessions).

### Presigned URL

A short-lived URL, signed by the deployment, that lets a client upload or download one object directly with object storage, without sending the bytes through the LoonFS server. The API returns it in an `access` object of kind `presigned_url`, with the headers that the client has to send unchanged. A direct upload returns one presigned URL (or one for each part of a multipart upload), and a download request returns one for each range of the file revision. A presigned URL lasts long enough for one transfer (15 minutes in the reference implementation), and anyone who has it can use it. See [Download transport](api.md#610-download-transport).

### Admission evidence

Proof that a content reference's bytes were verified in a completed upload. A commit needs it before it can reference uploaded content, so a commit can't reference bytes that were never verified. The proof is tied to the namespace and the complete content reference, and it expires. The HTTP server represents it as a signed content token. See [Admission proofs](format.md#55-admission-proofs).

### Content root

A reference that keeps a [fork family's](#fork-family) content objects from being deleted. A root in any member of the fork family counts. There are three kinds: the layouts in a manifest that garbage collection protects, the WAL tail, and upload session records. See [Content roots](format.md#119-content-roots).

## Metadata storage

### Metadata segment

An immutable object that stores sorted metadata rows from one [row family](#row-family) (for example, directory bindings), in blocks that can be read independently.

### Run

The set of metadata segments written together by one fold or compaction, identified by a run number. A manifest lists the runs that make up the namespace.

### Row family

A set of metadata rows with one key grammar, such as `revisions` (a file's revisions, keyed by inode ID) or `direntry_binds` (directory bindings, keyed by parent). Each family is an [entity header](#entity-header), a [history family](#history-family), or a [query index](#query-index), and every family follows the rules in [Row families](format.md#19-row-families).

### Entity header

A row family with one immutable row for each inode or commit, written when the inode or commit is created. The `inodes` family stores each inode's kind and creation attribution, and the `commits` family stores each commit record.

### History family

A row family that stores the versions of one mutable part of an entity, such as a file's revisions or an inode's attributes, keyed by the entity and then by version, newest first. A row of a history family is a "history row", and it records the sequence, ID, actor, and time of its commit.

### Query index

A row family that stores a second key over facts that another family stores, for a read that the other family's key can't serve. For example, `commit_receipts` is keyed by commit ID, so a retry finds its original commit's sequence without scanning `commits`, which is keyed by sequence.

### Family group

A set of related metadata row families that compaction always processes together, so that they stay consistent. For example, the two directory binding indexes form one group, and commits, commit receipts, and the [merge index](#merge-index) form another.

### Fold

The process that moves committed records from the WAL tail into new metadata segments and publishes a manifest that lists them, so later readers replay less of the WAL. A fold doesn't change the namespace's visible history, and any process can run one. See [Folding the WAL](format.md#7-folding-the-wal).

### Compaction

The process that rewrites a namespace's metadata runs into fewer runs, and drops rows that are no longer needed, without changing any retained view. Each compaction rewrites one family group's window of at most eight runs without keeping every row in memory, and publishes once at the end. See [Retention and compaction](format.md#10-retention-and-compaction).

### Retention floor

The sequence number below which a namespace's history is no longer guaranteed. Above the floor, every file's history is complete. At or below it, each visible file keeps at least its newest revision, and so does a deleted file while its deletion is recoverable. Compaction can remove the older rows at or below the floor (superseded bindings, old revisions, old attribute and access states, and commit receipts). A deletion at or below the floor has expired, so it can't be restored, and if its latest event is still `set`, a base compaction removes the rows that it hides. For example, if the floor is 20, a deletion at sequence 10 that nobody restored has expired, and the next base compaction of each family group removes the rows of the inodes that the deletion hides. A retention advance moves the floor forward, and the floor never moves back. Only the holder of the namespace's [writer epoch](#writer-epoch) advances the floor, after it has folded every commit validated against the old floor, so no restore of an expired deletion can be committed after the advance. A pin below the floor still protects its manifest. See [Advancing the retention floor](format.md#101-advancing-the-retention-floor).

### Retention window

A namespace setting, in milliseconds, for how much recent history a scheduled retention advance keeps. The reference server's [sweep](#sweep) advances each namespace's [retention floor](#retention-floor) to the last commit made at or before the current time minus the window. It reads the window from the namespace's manifest (`retention_window_ms`), and it doesn't advance the floor of a namespace without a window. The server records its own `retention_window_ms` setting as the window of each namespace that it creates without a window in the request. For example, a namespace with a 7-day window keeps at least 7 days of revisions and recoverable deletions. See [Advancing the retention floor](format.md#101-advancing-the-retention-floor).

### Control object

A small object used for discovery, retained views, or upload state: the hint, pins, and the session and outcome records of upload sessions. Each kind has its own update rules (for example, the hint only changes by compare-and-swap, and an upload record is never changed after it's written).

## Reads and retained views

### Read view

The namespace state that one read sees (or a group of related reads in one request). A read view exists only in memory, and it doesn't protect anything from garbage collection.

### Metadata basis

The verified set of metadata runs, from the current manifest, that a read starts from. A fork's manifest lists the runs it inherited from its source. See [Resolving the metadata basis](format.md#41-resolving-the-metadata-basis).

### Snapshot

A stored record that keeps one manifest, and every segment it lists, from being deleted after newer manifests are published, so the namespace stays readable as it was at that point in its history. The format calls the record a "pin". Every pin is a snapshot with one of two owners:

- A "user pin" is a snapshot whose owner is `user`. A caller creates it and can delete it, and it may expire. Its ID is the `snapshot_id` that the read routes and forks accept, and the snapshot routes manage only user pins. A snapshot can have an expiry time, or none. An expired snapshot can't be read or forked, and it stays listed until garbage collection deletes it. For example, a sandbox can create a snapshot before an agent edits it, and then read or fork the snapshot after the edits. A merge or a rebase also creates a user pin on the fork while it plans, and deletes the pin after its commit.
- A "fork pin" is a snapshot whose owner is `fork`. It keeps the source manifest that a fork reads through, until the fork is retired. A read of the source accepts a fork pin's ID as `snapshot_id`, so a client can read the source as it was at the fork point.

The maintenance API's "checkpoint" inventory lists every pin of a namespace, so an operator can see what keeps old manifests, and the API calls any pin's ID a `checkpoint_id`. See [Pins](format.md#8-pins).

### Fork

A new namespace that starts as a copy of another namespace at one point in its history. The fork shares the source's stored objects, and the source keeps a pin for the fork until the fork is retired. The fork is a member of its source's [fork family](#fork-family), so it reads inherited content and writes new content under the family root's prefix. After the fork, commits to one namespace appear in the other only when a [merge](#merge) applies them. See [Forking a namespace](format.md#92-forking-a-namespace).

### Source

The namespace that an operation reads from. For a fork, the source is the namespace that the fork was taken from (its parent), and the fork basis records it. For a [merge](#merge) or a [rebase](#rebase), the source is the fork whose commits are applied. For example, if `sandbox` is a fork of `main`, `main` is the source namespace in `sandbox`'s fork basis, and `sandbox` is the source of a merge of `sandbox` into `main`.

### Fork point

The source namespace's sequence at which a fork was taken. The fork basis records it (`fork_basis.source_head_seq`), the fork's retention floor starts at it, and the fork's first merge or rebase starts from it. For example, if `sandbox` was forked from `main` at sequence 100, the fork point is 100, and the first merge of `sandbox` into `main` applies `sandbox`'s commits after 100.

### Fork family

A set of namespaces: one namespace that was created without a source (the [family root](#family-root)), and every namespace forked from it, directly or through other forks. Each namespace in the set is a "member" of the family, and a member leaves the fork family when it's retired. The members store their content objects and upload records under the family root's prefix, so a content reference identifies the same object in every member. The family root's [collection](#collection) deletes the family's unreferenced content, with roots from every member. The collection finds the members through fork pins, because each fork pins the namespace it was forked from. For example, if `sandbox` is a fork of `main`, and `trial` is a fork of `sandbox`, all three are members of `main`'s family, and their content objects are stored under `namespaces/main/content/`. See [Namespaces and identity](format.md#11-namespaces-and-identity).

### Family root

The member of a [fork family](#fork-family) that was created without a source. Every member's manifest records the family root (`family_root_id`), and the family root's prefix stores the family's content objects and upload records. A family root is the last member of its family to be retired, because each fork pins its source. Its collections keep deleting the family's unreferenced content after the family root is deleted, until the family root itself is retired. See [File contents and ownership](format.md#15-file-contents-and-ownership) and [Retirement](format.md#95-retirement).

### Merge

A commit that applies a fork's commits to another namespace in its [fork family](#fork-family), so that the changes made in the fork appear there as one commit. The other namespace is usually the one that the fork was forked from, and for a [rebase](#rebase), it's a new fork of that namespace. The commit's record includes a "merge record", which identifies the fork and the range of the fork's commits that the merge applied: the commits above `source_after_seq`, through `source_through_seq`. For example, if `sandbox` was forked from `main` at sequence 100 and then committed sequences 101 through 120, a merge of those commits is one commit on `main` whose merge record is `sandbox`, 100, and 120. The change feed reports the merge record with the commit, and a merge's [semantic fingerprint](#semantic-fingerprint) covers its merge record in place of its operations. Rewriting metadata runs during [compaction](#compaction) isn't called a merge. See [Merges](format.md#merges), [Commit identity and retries](format.md#65-commit-identity-and-retries), and [WAL records](format.md#a5-wal-records).

### Merge index

A derived row family (`merges`) with one row for each merge that a namespace committed, keyed by the fork that the merge applied and then by the merge's sequence, newest first. Each row records the merge's sequence and its merge record's range. Before LoonFS commits a merge, it reads the fork's newest row and checks that the new merge starts where that merge stopped (or at the fork point, for the fork's first merge), so a fork's commits are applied once and in order. For example, if `main` committed a merge of `sandbox`'s commits 101 through 120 at sequence 300, and a merge of commits 121 through 140 at sequence 350, the first row under `sandbox` is the merge at 350, and the next merge from `sandbox` has to start after 140. See [Merges](format.md#merges) and [Metadata rows and row keys](format.md#a6-metadata-rows-and-row-keys).

### Rebase

A replay of a fork's unmerged commits onto a new fork of the namespace that the fork was forked from. A caller rebases a fork when a merge of the fork conflicts. The rebase creates the new fork at that namespace's head and applies the changes that don't conflict as "replay commits", which are [merges](#merge) into the new fork. It also reports the changes that do conflict, so that a program or a person can resolve them in the new fork before the new fork merges. The new fork's fork basis records `rebased_from`, which identifies the fork and the fork sequence that the first replay commit starts after. For example, if `sandbox` was forked from `main` at sequence 100 and its merge into `main` conflicts, a rebase forks `main`'s head into `sandbox-2` and replays `sandbox`'s unmerged commits there, and `sandbox-2` merges into `main` once its conflicts are resolved. See [Merges](format.md#merges).

### Writer position

The head that a writer session plans its next commit against, together with the manifest that head was read against. The session's process keeps the writer position in memory, and reads in that process start from it instead of from a cached view, so they send no WAL probe. The process moves the position to each commit before it acknowledges the commit, and to each manifest it publishes itself. For example, after the session commits sequence 43 above manifest 5, an `eventual` `stat` in the same process reads at sequence 43 against manifest 5 without sending a request. The session's manifest check finds a manifest published above the position's. The position moves onto one with the session's writer epoch, and a new writer's claim drops it. A `strong` read waits for the check, and an `eventual` read waits for it once the read's maximum staleness has passed since the position's last check. See [Replaying the visible WAL](format.md#42-replaying-the-visible-wal).

### Consistency

A setting on a live read that controls how recently LoonFS confirms the namespace [head](#head) before the read. `eventual` (the default, unless the host sets `strong`) confirms the head only when the last confirmation is older than the read's maximum staleness, so a read before then reads at the head that was confirmed last, and it may miss commits acknowledged since. `strong` confirms the head before every read, so the read sees every commit that was acknowledged before it started. A pinned read, such as a snapshot read, has no consistency setting. See [Read consistency](api.md#read-consistency).

### Eventual read

A live read with the `eventual` [consistency](#consistency), which is the default unless the host sets `strong`. An eventual read sends no request to confirm the head while the head was confirmed less than the read's maximum staleness ago (one second by default). For example, a dashboard that lists a directory every 100 milliseconds with a maximum staleness of five seconds confirms the head once every five seconds instead of on every listing. An eventual read sees every commit that was acknowledged before the last confirmation, and it never reads an older head than a read that completed earlier in the same process. In the writer's process, an eventual read starts from the [writer position](#writer-position), and the head's confirmation is the session's last check of the position. See [Read consistency](api.md#read-consistency).

### Revalidation bound

The longest time a reader can trust that no newer manifest exists after it last checked (`READ_REVALIDATION_BOUND_MS`). The bound applies to a cached view, to a [writer position](#writer-position), and to the manifest that a manifest publication builds on. After that, the reader loads the current manifest again before it serves a read, and the publication loads the current manifest through the hint before its put. The minimum [grace window](#grace-window) is at least this bound plus the clock allowance, so an object published after a reader's last check isn't deleted while the reader trusts that check. See [Replaying the visible WAL](format.md#42-replaying-the-visible-wal).

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

A WAL object with no commits. A new writer session publishes a fence as part of starting up, so that it can begin committing with its writer epoch. Once the fence is written, a writer session with an older epoch can't commit.

### Compactor epoch

A counter in the manifest, like the writer epoch, that stops a compaction from publishing its result if a newer compactor has claimed the namespace since the compaction started.

### Maintainer

The role that folds a namespace's WAL, compacts its metadata, publishes an extension's entry, and collects its garbage. The other role is the writer, which commits, deletes the namespace, and advances its [retention floor](#retention-floor), under the [writer epoch](#writer-epoch). Any process can be a namespace's maintainer, with or without the writer session. A compaction claims the compactor epoch first, and a fold or an extension's publication publishes under the current compactor epoch without a claim. A process without a writer session that advances the floor acquires the writer epoch first, with a claim and a fence (see [Advancing the retention floor](format.md#101-advancing-the-retention-floor)). See [Maintenance roles](../design/maintenance-roles.md) for the two roles and [Publishing a manifest](format.md#72-publishing-a-manifest).

### Job

One maintenance API request (`POST .../runs`) and the work it does. The request body's `kind` selects the job: a `metadata` job folds the WAL tail and compacts metadata, the retention jobs (`retention` and `retention_window`) advance the retention floor and set the namespace's retention window, a `gc` job runs one [collection](#collection), and a `recover_administrator` job grants a principal `admin` on the root again. A job runs to completion and returns its report in the response. A job never creates a snapshot. See [Maintenance jobs](api.md#maintenance-jobs).

### Step

One bounded unit of maintenance work on one namespace. A metadata step folds the WAL tail or compacts metadata within a budget, a collection step runs one [collection](#collection), and an index step builds or reorganizes part of a grep index. Every maintenance interval (five seconds by default), the reference server's scheduler runs the due steps of each namespace session that the server keeps open, and a [sweep](#sweep) runs the due steps of every namespace in the store.

### Sweep

The reference server's scheduler listing every namespace in the store, including deleted ones, and running each listed namespace's due steps, including a collection and a retention advance under the namespace's [retention window](#retention-window). A sweep runs when the server starts, and then on the sweep interval (24 hours by default). A namespace that nobody writes to through the server is maintained only by sweeps. See [Deleting, retaining, and reclaiming](api.md#deleting-retaining-and-reclaiming).

### Compare-and-swap (CAS)

A conditional update that replaces an object only if it hasn't changed since the caller read it (that is, its compare token, such as an ETag, still matches). LoonFS uses compare-and-swap to raise the hint and to extend a user pin's expiry.

### Publication budget

The longest time allowed between the state a publisher planned against and the start of its numbered put. A put that returns after its budget has an unknown outcome. Garbage collection relies on these budgets to choose a safe [grace window](#grace-window). See [Publication and collection timing](format.md#c1-publication-and-collection-timing).

## Deletion and garbage collection

### Collection

One garbage collection call over a namespace. A collection reads the current manifest and the other roots (everything that's still referenced), lists each object family, and deletes the unreferenced objects that are older than the [grace window](#grace-window). It's complete and stateless: it stores no progress, so after a failure the next collection starts over. On a [family root](#family-root), a collection also reads the roots of every member of the fork family and deletes the family's unreferenced content. See [One complete collection](format.md#111-one-complete-collection).

### Grace window

How old an unreferenced manifest, WAL object, content object, or temporary object has to be before garbage collection deletes it (`grace_window_ms`). The grace window is configurable (one hour by default), but it can't be lower than `GC_MIN_GRACE_WINDOW_MS`. That minimum makes sure every write planned against an older state has finished or failed before garbage collection deletes an object the write might reference. Garbage collection doesn't wait for reads, so a read that must outlive the current manifest takes a [snapshot](#snapshot). Two other windows have names of their own: the "retirement grace", which a deleted namespace waits before it can be retired, and the "completed-content grace", which keeps a completed upload's records. See [Candidate and age rules](format.md#113-candidate-and-age-rules).

### Namespace retirement

The point at which garbage collection deletes a deleted fork's pin on its source, or deletes the objects under a deleted family root's content prefix. A deleted namespace is retired once its retirement grace has passed and no pins on it remain. See [Retirement](format.md#95-retirement).

## API

### API group

A set of API operations that a deployment advertises only when it implements all of them, such as `filesystem/v0`.

### Feature

An optional capability within an API group, such as `filesystem.uploads.direct_put`.

### Capability document

The document a deployment returns from `GET /v0/capabilities`. It lists the deployment's protocol version, API groups, features, and advisory limits. Clients typically use it to find out which features are enabled on a deployment before they use them.

### Extension

An optional subsystem that keeps state derived from a namespace's committed metadata and content, such as the grep index. An extension's state is an entry in the namespace's manifest, under the extension's name, and the entry lists the extension's segments, which are stored under `namespaces/{owner_namespace_id}/extensions/{extension_name}/segments/`. Every manifest publication copies the entry forward (a tombstone copies it with its runs emptied), a fork's first manifest copies its source's entry, a pin keeps the segments that its manifest lists, and garbage collection collects the segments that no kept manifest lists. See [Extensions](format.md#124-extensions).
