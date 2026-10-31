# Inline small content

A commit can include the bytes of a small file in its WAL object. The bytes become durable and visible in one conditional write, and the next fold writes them to an ordinary content object. Inline writes are enabled by default for files up to 64 KiB.

The storage format defines the durable rules:

- [File contents and ownership](../specs/format.md#15-file-contents-and-ownership) covers durability.
- [Content verification](../specs/format.md#45-content-verification) covers reads.
- [Publishing a materialized file set](../specs/format.md#72-publishing-a-materialized-file-set) covers the fold.
- [WAL records](../specs/format.md#a5-wal-records) defines the record and its limits.
- [Content in a fingerprint](../specs/format.md#b2-content-in-a-fingerprint) defines the fingerprint form.

In the API specification, [Commit responses and safe retry](../specs/api.md#52-commit-responses-and-safe-retry) covers inline commits and retries. [Resource sizing](../../crates/loonfs-server/docs/self-hosting.md#resource-sizing) in the self-hosting guide lists the writer settings. This note explains why the design takes this shape.

## Why commits carry small files

The uploaded path writes a file's bytes to a content object before the commit that references them. That order suits a large file, because the transfer can be direct, resumable, and independent of the commit. For a small file, though, those writes before the commit are most of the cost. A 1 KiB write through the uploaded path makes three object-store writes to make the bytes durable and owned, and then a fourth to commit.

These measurements replay each request sequence for a 1 KiB write on S3 `us-east-2`, with 24 interleaved rounds per sequence:

| Request sequence for a 1 KiB write | PUTs | p50 ms | p90 ms |
| --- | ---: | ---: | ---: |
| Uploaded path: session, content, completion, WAL | 4 | 289.1 | 428.8 |
| Inline path: WAL only | 1 | 58.8 | 69.9 |

The inline sequence was faster in all 24 rounds. These timings include store requests only, not writer authority, freshness checks, WAL encoding, or folding. The run is `20260917T025704Z-bench-s3-small-content-sequences` in the lab repository.

## A reference names content, not a location

An inline commit records the same `append_file_revision` delta as an uploaded commit, with an ordinary `blob_v1` reference. The reference identifies a prefix of a [chain](../specs/glossary.md#chain). A fresh chain's revision delta includes the layout of its base bytes. A same-chain append uses the chain's earlier revisions and includes no layout.

The WAL entry is a [piece](../specs/glossary.md#piece), which contains either the whole value at offset 0 or, for an append, the bytes after a prefix that's already durable. The WAL stores those bytes until a fold covers the record. A copy, a restore, a fresh chain, or a direct download can materialize them before the fold, but a same-chain append leaves them in the WAL.

Using the same reference keeps one identity for the same content over its whole life. Revision rows, the change feed, and retained receipts never see two different references for the same bytes. Readers resolve a reference through the read view and never build a content key directly, so no reader depends on the content object existing as soon as the commit is visible.

The writer draws the content ID at random before publication, as it does for staged content. Every fold reads the ID from the log, so racing folds, a restarted fold, and a batch that's planned again at a later WAL number all write the same key.

Two rules follow from this:

- **One content ID has one lifecycle.** A content ID belongs to exactly one staged upload or one committed inline value, and a later attempt never reuses the ID of an earlier one, even for the same bytes. The store's verified immutable write is safe to retry only because every writer that can write a given key supplies the same bytes for the same prefix. Cleaning up an expired open upload session also deletes its content without checking whether it was published, so if a staged attempt and an inline attempt shared an ID, the content could be deleted after it was committed.
- **An inline value's content ID isn't its retry identity.** The server draws the ID for a hosted write, and a write that falls back to staging draws another one. So the fingerprint identifies inline content by its bytes instead.

## Retry identity

This section describes how LoonFS recognizes a retried write that includes inline content.

An uploaded put is identified by the content object that it references. A caller retries it by preparing the content once, and publishing the same prepared value on every attempt.

Inline content has no object to refer to. For a hosted write, the server draws the content ID, so a client that loses the response has nothing stable to resend except the bytes. For an embedded write, a fallback to staging draws a second ID, so a fingerprint built on the ID would turn a valid retry into a conflict. For both reasons, the fingerprint hashes the inline request bytes with SHA-256 and records their size, separately from the content checksum.

The identity is set when the content is prepared. Preparing content at or under the writer's threshold creates an inline prepared value, and that value keeps the inline form even if it falls back to staging. If the same bytes are sent once inline and once as an uploaded object under one commit ID, they're different requests, and they conflict. A writer whose threshold changed between two attempts can run into this case.

`put_file` prepares the content and then publishes it. So if `put_file` runs again under the same commit ID, it replays the original result for content that's small enough to be inline, and it conflicts for larger content. The failure is safe in both directions, because it's a conflict, never a replay of the wrong bytes. At every size, a caller should retry with the prepared content.

## Where the writer chooses the path

Every inline limit is a preference. When a limit is reached, the writer stages that content through an upload session under its own content ID, so no inline limit causes a write error. Uploads are never staged inside the publication loop, but validation can materialize pending pieces for a copy, a restore, or a fresh chain. The writer chooses the path at three points:

- **Building the commit candidate.** A commit must fit in one WAL object. If a commit's inline total would exceed the per-object budget, the writer keeps values inline in operation order until the budget is reached, and stages the rest. The commit stays atomic.
- **Admission.** The writer chooses based on the unfolded inline bytes that the publisher knows about. [Resource sizing](../../crates/loonfs-server/docs/self-hosting.md#resource-sizing) describes how that count is kept.
- **Publishing.** When a WAL object's inline budget is full, the publisher closes that object, and the next commit starts the next object. One commit is never split.

## Reading

This section describes where readers find inline bytes before a fold.

A view replays the unfolded WAL tail into a projection of its rows, and that projection also includes the tail's inline content. So the bytes are in memory wherever the projected tail is (in the reader's tail cache, in the writer's own projection, and in the input that a fold reads). A reader that has a projected tail already downloaded those bytes while it replayed the tail, so keeping them costs memory but no requests. The existing projection budgets count them. There's no second cache, and no on-demand read of a WAL object.

A long-lived view can outlast its tail. A later fold can publish, and garbage collection can then delete the WAL objects. Rebuilding that view then fails, as it does for any stale view. There's no fallback to the content object, and none is needed, because a view that still has its projection still has the bytes.

## Direct downloads

A direct download returns a presigned URL for the content object. Inline content is small, so the proxied read serves it in one request instead of two. A client can still ask for a direct download of a small file that hasn't been folded.

In that case, a handle with write authority writes the content ID's pieces into the object at the key that the reference points to, through the fold's own materialization, before it signs the URL. This is one step of the fold done early. A later fold finds the object already at its target, and a concurrent fold either writes the same bytes or finds them already written. An object that already contains the reference's bytes is signed as it is. A deployment that can't write content objects returns `content_not_materialized` until the next fold.

## Folding

This section describes how a fold writes inline bytes to content objects.

A fold writes every piece in its range into immutable content objects before it writes segments or publishes the manifest. The chain's own layout lists the objects that store its earlier bytes. A fresh chain's revision delta includes the base's extents, cut to the base size. Validation first writes any pending base pieces through the fold's materialization. The fresh chain shares the returned extents, and includes only its appended bytes as pieces. Copies and restores also include complete layouts, so they can still be read after the source chain's layout row is removed.

The fold writes a whole object or a span object according to the rules in [Publishing a materialized file set](../specs/format.md#72-publishing-a-materialized-file-set). It merges the chain's own last extents by the doubling rule, and it writes every extent with one assembly call. If the result would exceed 32 own extents, it assembles the extents after the largest own extent. Shared base extents stay separate. For an append, the fold combines the base reference's checksum with the checksum of the appended bytes. Every reference uses the store's checksum algorithm (see [Required operations](../specs/format.md#31-required-operations)).

The storage adapter chooses how to produce the object. Below its single-request threshold, it reads the sources and the tail in bounded chunks, and creates the object with one request. Above that threshold, the provider copies what it can, and the adapter reads the rest. An adapter buffers at most one part window or one read chunk.

Validation, folds, direct downloads, uploads, and imports each take one content-write permit from `ExecutionBudget` (32 permits by default), so the memory used by content writes is bounded by the number of permits times one part window. Content writes run with bounded concurrency after the fold's publication budget starts, and the fold checks the budget when they return.

A fold's objects stay [rooted](../specs/glossary.md#content-root) while a fold can still need them. The unfolded tail refers to its pieces, and to the extents in its layout rows. After folding, a rooted manifest's revisions refer to the layouts whose extents keep those objects (see [Content roots](../specs/format.md#119-content-roots)). A fold that crashes, exceeds its budget, or loses the manifest race leaves objects that the next fold can reuse. An immutable write to a key that's already occupied succeeds when the existing object has the same stored checksum. If the bytes are different, the fold stops without publishing.

A fold can pause between reading a tail and writing its content, and the namespace can be deleted and swept in the meantime. A fold call reads a fresh manifest, but a writer's own fold starts from its retained view, and neither one checks its publication budget until its content writes return. So a content object can be written after a sweep. It's found the same way as a late upload, because the family root's pass lists the content prefix again on every later pass, and so does the root's retirement sweep (see [Sweeping a retired owner's content](../specs/format.md#118-sweeping-a-retired-owners-content)).

A fold becomes due when the unfolded tail reaches 32 WAL objects, or when the bytes of its pieces reach the fold threshold.

Why is there a byte trigger? It limits how much a cold reader has to download to replay a tail.

## Collection

Garbage collection has no separate object family for inline content. It deletes WAL objects at or below `folded_wal_no`, and the fold writes inline bytes to content objects before it publishes that boundary.

A content object that a fold or an early direct download wrote is collected like any other content object. The unfolded WAL tail refers to it until a manifest covers it, and a rooted manifest's revisions refer to it after that. Upload sessions aren't involved in an inline write. A rejected or interrupted publication can leave a materialized extent behind, and garbage collection removes it after the grace once no root refers to it.

## Costs

Inline content has these costs:

- A cold reader replays a tail that can contain MiB of data instead of tens of KiB. Metadata-only operations pay this cost too, because a cold stat or listing replays the same tail. The byte trigger limits it.
- Commits share WAL objects. Inline bytes make an object larger and its PUT slower, and every commit in the batch waits, including commits with no content. This is why the per-object budget is small.
- A fold does more work, up to thousands of small writes. A copy, restore, or fresh chain that needs pending bytes moves their content write into validation.
- A WAL object stores a second copy of small content until it's folded and collected. Inline bytes also make change-feed reads larger.
- A direct download, copy, restore, or fresh chain can write pending bytes before the fold. Same-chain appends write only the WAL.
- The fingerprint contract has a second content form, with its own pinned test vectors.
- Inlining is a writer policy, so a deployment that must keep file bytes out of its metadata store turns inline writes off.

## Threshold measurements

The defaults come from a lab sweep that compared thresholds of 1, 4, 16, and 64 KiB with the uploaded path. At p50, every size was at least three times faster than the uploaded path. The sweep stops at 64 KiB because small-object PUT latency is flat up to about that size. At 64 KiB, warm writes were three times faster at p50 and twice as fast at p90, and the read after a write made no content request.

Cold-stat time and fold time grew with the tail. The tails in this table were made of 4 KiB files, and the cold-stat times are compared with a folded namespace:

| Unfolded tail | Added to a cold stat | Time to fold |
| --- | ---: | ---: |
| 2 MiB | about 0.45 s | about 3 s |
| 8 MiB | about 1.0 s | about 7 s |
| 32 MiB | about 2.6 s | about 19 s |

Based on that result, the fold threshold is 2 MiB. The sweep is `analysis/inline-content-sweep-20260918.md` in the lab repository.

## Alternatives considered

- **Keep values in segments.** Small reads wouldn't need a content object at all. But every compaction of the revisions family rewrites the revisions it keeps, so it would rewrite file bytes too. Metadata blocks would fill with payloads, which would slow down path resolution and listing, and direct downloads would still need an object. Writing the bytes out at the fold keeps file bytes out of the metadata tree.
- **A new content reference kind.** One revision would have two references over time, an inline kind in the log and `blob_v1` in segments. Fingerprints, retries, the change feed, and content equality checks would all need to treat the two as equal. A reference that identifies logical content, with its location resolved separately, avoids this.
- **Deriving the content ID from the commit ID and operation index.** A retry would then build the same reference, but the scheme is unsound. The fingerprint leaves out the checksum because a fresh ID already ties the reference to one set of bytes. With a derived ID, a second request under the same commit ID, with different bytes of the same length, would have an equal fingerprint and replay the first receipt. A commit ID can also be reused after its receipt is reclaimed while the revision it wrote is kept, so one immutable key could be asked to store two different contents.

  Adding the payload digest to the derivation solves both problems, but it still lets a staged attempt and an inline attempt share one object, and that object would then have two cleanup lifecycles.
- **Payload identity for every put that supplies bytes.** A rerun of `put_file` would then replay at any size. But this reverses the rule that a put is identified by its content object, and the benefit is small. A rerun of a large write would upload everything again before it found its receipt, and prepared content already avoids that. It would also split the two Rust interfaces, because the server never sees the bytes of a direct upload, so it couldn't fingerprint a hosted large write the same way.
- **Content IDs drawn by the client for hosted inline writes.** A resent request would include the same ID, so the reference form could stay the only form. But the server couldn't check that no open upload session uses the ID, so the one-lifecycle rule would depend on every client being correct. A wrong ID could stop a fold, or let an expired session's cleanup delete committed content.
- **A separate payload section in the WAL object.** Metadata readers could skip inline bytes by reading a range, which would remove the cold-replay and change-feed costs. But it needs a second framing layer, with its own integrity check.
- **Packing a fold's values into one object.** A fold would make one write instead of one per value. But a base compaction can remove revisions at or below the retention floor, so a pack could become partly dead, and reclaiming its space would need a rewrite. Packing would also change reference resolution and direct downloads.
- **Removing upload-session writes from the staged path.** Content would stay outside the log, but something else would have to replace the upload session's role as the garbage collector's index of candidates. It would also help only embedded writers.
