# Inline small content

A commit can carry the bytes of a small file inside its WAL object. The bytes become durable and visible in one conditional write, and the next flush writes them to an ordinary content object. Inline writes are enabled by default for files up to 64 KiB.

The storage format defines the durable rules: [section 1.5](../specs/format.md#15-file-contents-and-ownership) for durability, [section 4.5](../specs/format.md#45-content-verification) for reads, [section 7.2](../specs/format.md#72-publishing-a-materialized-file-set) for the flush, [Appendix A.5](../specs/format.md#a5-wal-records) for the record and its limits, and [Appendix B.2](../specs/format.md#b2-content-in-a-fingerprint) for the fingerprint form. The [API specification](../specs/api.md#52-commit-responses-and-safe-retry) covers inline commits and retries, and [self-hosting](../../crates/loonfs-server/docs/self-hosting.md#resource-sizing) lists the writer settings. This note explains why the design takes this shape.

## Why commits carry small files

The uploaded path writes a file's bytes to a content object before the commit that names them. That order suits a large file: the transfer can be direct, resumable, and independent of the commit. For a small file it is most of the cost. A 1 KiB write through the uploaded path makes three object-store writes to make the bytes durable and owned, then a fourth to commit.

The following measurements replay each request sequence for a 1 KiB write on S3 `us-east-2`, 24 interleaved rounds per sequence:

| Request sequence for a 1 KiB write | PUTs | p50 ms | p90 ms |
| --- | ---: | ---: | ---: |
| Uploaded path: session, content, completion, WAL | 4 | 289.1 | 428.8 |
| Inline path: WAL only | 1 | 58.8 | 69.9 |

The inline sequence was faster in all 24 rounds. These timings cover store requests only, not writer authority, freshness checks, WAL encoding, or flushing. The run is `20260917T025704Z-bench-s3-small-content-sequences` in the lab repository.

## A reference names content, not a location

An inline commit records the same `append_file_revision` delta as an uploaded commit, with an ordinary `blob_v1` reference. The reference names the content object that the flush will write. Until then the WAL record holds the only copy.

This keeps one identity for one piece of content over its whole life. Revision rows, the change feed, retained receipts, and equality checks such as the speculative read's same-content test never see two different references for the same bytes. Readers resolve a reference through the read view and never build a content key directly, so no reader depends on the object existing as soon as the commit is visible.

The writer draws the content ID at random before publication, as it does for staged content. Every flush reads the ID from the log, so racing flushes, a restarted flush, and a batch planned again at a later WAL number all write the same key.

Two rules follow from this:

- One content ID has one lifecycle. A content ID belongs to exactly one staged upload or one committed inline value. A later attempt never reuses the ID of an earlier one, even for the same bytes. The store's verified immutable write is safe to retry only because every writer that can name a key supplies identical bytes. An expired open upload session also deletes its content without checking for publication, so an ID shared between a staged attempt and an inline attempt could be deleted after it was committed.
- An inline value's content ID is not its retry identity. The server draws the ID for a hosted write, and a write that falls back to staging draws another. The fingerprint identifies inline content by its bytes instead.

## Retry identity

An uploaded put is identified by the content object it names. A caller retries it by preparing content once and publishing the same prepared value on every attempt.

Inline content has no object to name. For a hosted write the server draws the content ID, so a client that loses the response has nothing stable to resend except the bytes. For an embedded write, a fallback to staging draws a second ID, so a fingerprint built on the ID would turn a valid retry into a conflict. The fingerprint therefore uses the SHA-256 digest and size of inline content.

Identity is fixed when content is prepared. Preparing content at or under the writer's threshold makes an inline prepared value, and that value keeps the inline form even if it falls back to staging. The same bytes sent once inline and once as an uploaded object under one commit ID are different requests and conflict. A writer whose threshold changed between two attempts can meet this case.

`put_file_bytes` prepares and then publishes, so a rerun under the same commit ID replays for content small enough to be inline and conflicts for larger content. The failure is safe in both directions: a conflict, never a replay of the wrong bytes. The advice is the same at every size: retry with the prepared content.

## Where the writer chooses the path

Every inline limit is a preference. When a limit is reached, the writer stages that content through an upload session under its own content ID, and no inline limit produces a write error. Content is never staged inside the publication loop. The writer decides at three points:

- When it builds the commit candidate. A commit must fit in one WAL object, so a commit whose inline total would pass the per-object budget keeps values inline in operation order until the budget is reached and stages the rest. The commit stays atomic.
- At admission, against the unfolded inline bytes that the publisher knows about. Self-hosting describes how that count is kept.
- In the publisher. A WAL object whose inline budget is full closes, and the next commit starts the next object. One commit is never split.

## Reading

A view replays the unfolded WAL tail into a projection of its rows, and that projection also carries the tail's inline content. The bytes are therefore resident wherever the projected tail is: in the reader's tail cache, in the writer's own projection, and in the input a flush consumes. A reader that holds a projected tail already downloaded those bytes while replaying it, so keeping them costs memory and no request. The existing projection budgets count them. There is no second cache and no read of a WAL object on demand.

A long-lived view can outlast its tail: a later flush publishes, and collection deletes the WAL objects. Rebuilding that view fails as any stale view does. There is no fallback to the content object, and none is needed, because a view that still holds its projection still holds the bytes.

## Direct downloads

A direct download returns a presigned URL for the content object. Inline content is small, so the proxied read serves it in one request instead of two. A client can still ask for a direct download of a small file that has not been flushed. A handle with write authority then writes the content object at the key the reference names, with a verified immutable write, before it signs the URL. This is one step of the flush done early: a later flush finds the object present, and a concurrent flush writes identical bytes. A deployment that cannot write content objects answers `content_not_materialized` until the next flush.

## Flushing

A flush writes every inline value in its range as a content object before it writes segments or publishes the manifest. The writes run with bounded concurrency inside the flush's publication budget.

In a live namespace, materialized content is never garbage: each object belongs to a committed revision, and revisions are retained. A flush that crashes, exceeds its budget, or loses the manifest race leaves objects that the next flush finds already present. There is no orphan to discover and no cleanup state to persist. An object already at the key must hold the same bytes, and the verified write checks this. A mismatch stops the flush without publishing.

A flush can pause between reading a tail and writing its content while the namespace is deleted and swept. Every flush attempt starts from a fresh manifest observation, and materialization runs inside the flush's publication budget, which the retirement grace exceeds. A write that still lands late is found the way a late upload is: the retired-owner sweep lists the content prefix again on every later pass ([format section 11.8](../specs/format.md#118-sweeping-a-retired-owners-content)).

A flush becomes due when the unfolded tail reaches 32 WAL objects, or when its inline bytes reach the fold threshold. The second trigger bounds what a cold reader downloads to replay a tail.

## Collection

Collection has no family for inline content. WAL objects are collected at or below `folded_wal_no`, and the flush writes inline bytes to content objects before it publishes that boundary. Content objects written by a flush are published content with permanent `content_publications` rows. Upload sessions are not involved in an inline write. Unpublished inline content cannot exist, so the ownership question that sessions answer does not arise.

## Costs

- A cold reader replays a tail that can hold MiB rather than tens of KiB. Metadata-only operations pay this too, because a cold stat or list replays the same tail. The byte trigger bounds it.
- Commits share WAL objects. Inline bytes make an object larger and its PUT slower, and every commit in the batch waits, including commits with no content. The per-object budget is small for this reason.
- A flush does more work: up to thousands of small writes, off the commit path. Request cost still falls, from four writes per small file to two.
- A WAL object holds a second copy of small content until it is folded and collected. Inline bytes also make change-feed reads larger.
- The first direct download of a small file written since the last flush costs one extra content write.
- The fingerprint contract has a second content form, with its own pinned test vectors.
- A deployment that must keep file bytes out of its metadata store turns inline writes off. Inlining is writer policy.

## Threshold measurements

A lab sweep of 1, 4, 16, and 64 KiB thresholds against the uploaded path set the defaults. Every size beat the uploaded path by three times or more at p50. 64 KiB is the top of the sweep because small-object PUT latency is flat to about that size, and it matches the speculative read limit. At 64 KiB, warm writes were three times faster at p50 and twice as fast at p90, and the read after a write made no content request.

Cold-stat time and flush time grew with the tail. Tails of 2, 8, and 32 MiB of 4 KiB files added about 0.45, 1.0, and 2.6 seconds to a cold stat compared with a flushed namespace, and flushed in about 3, 7, and 19 seconds. That result set the fold threshold at 2 MiB. The sweep is `analysis/inline-content-sweep-20260918.md` in the lab repository.

## Alternatives considered

**Keep values in segments.** Small reads would need no content object at all. But revisions are retained permanently, so every compaction of the revisions family would rewrite file bytes. Metadata blocks would fill with payloads and slow path resolution and listing, and direct downloads still need an object. Writing the bytes out at the flush keeps the metadata tree free of file bytes.

**A new content reference kind.** One revision would have two references over time: an inline kind in the log and `blob_v1` in segments. Fingerprints, retries, the change feed, and content equality would all need to treat them as equal. Naming logical content and resolving its location avoids this.

**Deriving the content ID from the commit ID and operation index.** A retry would then build the same reference, but the scheme is unsound. The fingerprint leaves out the checksum because a fresh ID pins the bytes. With a derived ID, a second request under the same commit ID, with different bytes of the same length, has an equal fingerprint and replays the first receipt. A commit ID can also be reused after its receipt is reclaimed, while the revision it wrote is kept, so one immutable key could be asked to hold two different contents. Adding the payload digest to the derivation repairs both cases, but it still lets a staged attempt and an inline attempt share one object, and that object then has two cleanup lifecycles.

**Payload identity for every put that supplies bytes.** A rerun of `put_file_bytes` would then replay at any size. This reverses the rule that a put is identified by its content object, for a small benefit: a rerun of a large write uploads everything again before it finds its receipt, and prepared content already avoids that. It would also split the two Rust interfaces, because the server never sees the bytes of a direct upload and could not fingerprint a hosted large write the same way.

**Content IDs drawn by the client for hosted inline writes.** A resent request would carry the same ID, so the reference form could stay the only form. But the server could not check that the ID is unused by an open upload session, so the one-lifecycle rule would depend on every client being correct. A wrong ID can stop a flush or let an expired session delete committed content.

**A separate payload section in the WAL object.** Metadata readers could skip inline bytes by reading a range, which removes the cold-replay and change-feed costs. It needs a second framing layer and its own integrity check.

**Packing a flush's values into one object.** A flush would make one write instead of one per value. Revisions are never dropped, so a pack in a live namespace never becomes partly dead. It changes reference resolution and direct downloads.

**Removing upload-session writes from the staged path.** Content stays outside the log, but something must replace the session's role as the collector's candidate index. It helps embedded writers only.
