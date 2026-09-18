# Inline small content

**Status: implemented.** Inline writes are enabled by default for files up to 64 KiB. The default fold trigger is 2 MiB.

LoonFS writes file bytes to a content object before it commits the metadata that names them. For a large file that is the right order: the transfer can be direct, resumable, and independent of the commit. For a small file it is most of the cost. A 1 KiB write spends three object-store writes making the bytes durable and owned, then a fourth to commit.

This proposal lets a commit carry small file bytes inside its WAL object. The bytes become durable and visible in one conditional write. The next fold copies them to ordinary content objects, so everything that reads a manifest sees the storage model it sees today.

Measured on real S3 by replaying each protocol's request sequence, 24 interleaved rounds per arm ([lab evidence](#evidence)):

| Sequence for a 1 KiB write | PUTs | p50 ms | p90 ms |
| --- | ---: | ---: | ---: |
| Current: session, content, completion, WAL | 4 | 289.1 | 428.8 |
| Inline: WAL only | 1 | 58.8 | 69.9 |

Inline was faster in 24 of 24 rounds. These timings cover store requests only, not writer authority, freshness, WAL encoding, or folding. They show what the request sequence allows, not what the product will deliver.

## What changes and what does not

| | Today | Proposed for content at or under the inline threshold |
| --- | --- | --- |
| Embedded small write | Upload session, content, completion, WAL | WAL |
| Hosted small write | Three or more HTTP requests; four to six store writes | One HTTP request; one store write |
| Read of a recently written small file | Freshness probe, then content GET | Freshness probe; bytes come from the replayed tail |
| Read after the next fold | Freshness probe, then content GET | Unchanged |
| Content references, revision rows, change feed | A visible reference names an existing object | Fields unchanged. A reference in the unfolded tail may name an object the next fold has yet to write |
| Retry identity of a put | Which content object it names | Unchanged for uploaded objects. Content a commit carries inline is identified by its bytes |
| Checkpoints, snapshots, forks | | Unchanged |
| Garbage collection | | No new candidate family. Existing rules hold under the fold invariant |
| Large files, direct transfers, and the download contract | | Unchanged |

Three other systems built on object storage put payload bytes in their log and reorganize them later: SlateDB keeps values in WAL and SST objects, turbopuffer commits documents to its WAL and indexes them asynchronously, and Cursor's Git storage writes each push's packfile as a WAL entry and compacts in the background. LoonFS is the exception because it needs an independently addressable object for large and direct transfers. This proposal keeps that object and removes the wait for it.

## A reference names content, not a location

The content reference keeps its fields. A commit that carries inline bytes records the same `append_file_revision` delta it records today, with an ordinary `blob_v1` reference: owner, content ID, size, and checksum. The reference names the object the fold will write. Until then the WAL record holds the authoritative copy.

This keeps one identity for one piece of content over its whole life. Revision rows, the change feed, retained receipts, and equality checks such as the speculative read's same-content test never see two different references for the same bytes.

The meaning of a reference changes in one way. Today a visible reference implies that its object exists. With inline content, a reference in the unfolded tail may name an object that the next fold has yet to write. Every reader resolves a reference through the rule in [Reading](#reading), and none builds an object key directly. The rule that content is durable before publication becomes: content is durable no later than publication.

The content ID is drawn at random by the writer before publication, as it is for staged content today. It is recorded in the reference and in the inline entry. Every fold reads the ID from the log. Racing folds, a restarted fold, and a batch re-planned at a later WAL number all write the same key, and nothing has to derive it.

Two rules follow.

**One content ID, one lifecycle.** A content ID belongs to exactly one staged upload or one committed inline value. A later attempt never reuses the ID of an earlier one, even for the same bytes. Two facts make this necessary. The store's verified immutable write is safe to retry only because every writer that can name a key supplies identical bytes. And an expired open upload session deletes its content without checking for publication, so an ID shared between a staged attempt and an inline attempt could be deleted after it was committed.

**An inline value's content ID is not its retry identity.** The server draws the ID for a hosted write, and a write that falls back to staging draws another. Inline content is recognized by its bytes instead.

## Retry identity

A put is identified by which content object it names, never by what the bytes are. The fingerprint represents the content as the reference's kind, content ID, and length (format specification, Appendix B.2). A fresh upload under a used commit ID is a different request and conflicts. A retry resends the same reference, which a caller does by preparing content once and publishing the prepared value on every attempt. That rule was chosen deliberately, it is one rule, and it stays as it is for every uploaded object.

Inline content has no object to name. For a hosted write the server draws the content ID, so a client that loses the response has nothing stable to resend except the bytes. For an embedded write a fallback to staging draws a second ID, so a fingerprint built on the ID would turn a valid retry into a conflict. Content that a commit carries inline is therefore identified by its bytes. This form takes the place of the reference form in the operation's preimage:

```json
{"kind":"inline_v1","sha256":"<64 lowercase hex>","size_bytes":15}
```

Everything else in the fingerprint is unchanged. The rule is the same one stated at a higher level: a put is identified by what its request names, an uploaded object or the bytes themselves.

Identity is fixed when content is prepared. Preparing content at or under the writer's threshold makes an inline prepared value: the bytes and their digest, with no store request. Preparing larger content stages an object as today. The rules:

- An inline prepared value uses the inline form whether it is published inline or falls back to staging. The form never depends on where the bytes were stored.
- A staged prepared value and a hosted content reference keep the reference form. Nothing about uploaded objects changes.
- While the commit receipt is retained, retrying the same request returns the original commit and content reference without uploading the bytes again. This applies after a restart or on another server.
- A retry with a different payload returns `commit_id_reuse_conflict`, including when the length is equal.
- After the receipt is reclaimed, the same request executes as a new commit with a new content ID. It cannot collide with the content of the earlier commit, whose revision is kept.
- The same bytes sent once inline and once as an uploaded object, under one commit ID, conflict. They are different requests. A writer whose threshold changed between two attempts can meet this case.

One consequence is visible to callers. `put_file_bytes` prepares and then publishes, so its rerun under the same commit ID replays for content small enough to be inline and conflicts for larger content. The failure direction is safe: a conflict, never a replay of the wrong bytes. The advice is the same at every size and on both Rust interfaces: retry with the prepared content.

The digest is SHA-256. The fingerprint scheme fixes it, independent of the reference's checksum algorithm. Embedded byte and stream writes already compute it for the reference. A fingerprint is computed once, when the commit is planned, and stored in the WAL record and the receipt. Replay never recomputes it, so replay does not change. The form has no caller until inline writes exist, so it lands with the writer.

## The WAL record

A commit record gains one optional field:

| Field | Meaning |
| --- | --- |
| `inline_content` | A list of `{content_id, bytes}`. `bytes` is a CBOR byte string. |

Replay validation adds these rules:

- Every entry's `content_id` is named by an `append_file_revision` delta in the same commit, and that reference's owner is the namespace whose log this is.
- An entry's length equals that reference's `size_bytes`. A zero-length entry is valid.
- A content ID appears at most once per commit.
- Each entry and the object's inline total are within the format limits in [Resource bounds](#resource-bounds).

Those limits are part of the format. A writer's thresholds are policy and may be lower. Lowering a threshold never makes an existing record invalid.

Inline bytes sit inside the WAL payload, so the envelope's existing validation covers them. Each value is also checked against its reference's size and checksum when it is read and when it is materialized, as content objects are today. Replay does not hash payloads.

## Writing

**Embedded.** Preparation decides. `prepare_file_bytes` and `prepare_file_stream` make an inline prepared value when the content is at or under the writer's threshold. Empty files qualify. A stream is buffered up to the threshold to decide. Larger content stages as today. `put_file_prepared` publishes either kind, and `put_file_bytes` and `put_file_stream` remain preparation followed by publication. An inline publication draws a content ID, builds the reference, attaches the bytes to the commit candidate, and skips staging. No upload session is written and no admission proof is needed, because no content exists outside the commit. A prepared value has never outlived the process that made it, because the proof it carries is held in memory. An inline prepared value keeps that contract.

**Hosted.** A `put_file` operation in a commit request names exactly one content source: a content reference with its token, as today, or `inline_content`. For inline content the server checks the size, computes the checksum and digest, draws the content ID, and publishes. A small hosted write becomes one request and one store write. It shares batching, preconditions, and the commit response with every other operation. The server advertises its inline limit in the capability document, and the Rust HTTP client prepares content at or under it as an inline value, as the embedded runtime does. The 2 MiB JSON request limit already bounds a commit's inline bytes, to about 1.5 MiB of content after base64 encoding.

**Falling back is always allowed.** Every inline limit is a preference. When a limit is reached the writer uses the staged path for that content, and no inline limit produces a write error. A write that falls back stages through an upload session exactly as today, under a content ID of its own, with the completion and abort rules unchanged. Its fingerprint keeps the inline form, so a retry can take a different path from the attempt before it.

**Where the choice is made.** Content is never staged inside the publication loop.

- *When the candidate is built.* The value threshold and the commit's own inline total are known here. A commit must fit in one WAL object. A commit whose inline total would pass the per-object budget keeps values inline in operation order until the budget is reached and stages the rest. The commit stays atomic.
- *At admission.* The tail ceiling is checked against the loaded projection, or the last tail size this publisher observed, plus every admitted commit not yet published and this commit. Projection invalidation does not discard the remembered size. Only a publisher that has never observed the namespace counts the tail as empty, so its first inline commit can pass the ceiling by at most its own inline bytes, once during that publisher's lifetime. Another writer can make the remembered size stale until this publisher's next publish. The overshoot is at most one segment budget. `MAX_UNFLUSHED_WAL_SEGMENTS` stops new commits regardless of the inline tail limit.
- *In the publisher.* A WAL object whose inline budget is full closes, and the next commit starts the next object. Independent commits are split between objects. One commit is never split.

The model in the API specification has three stages: make content durable, make metadata visible, observe changes. Inline content merges the first two. Durability and visibility arrive in the same conditional write.

## Reading

A view already replays the unfolded WAL tail into a projection of its rows. That projection now also carries the tail's inline content, as a map from content ID to bytes. Only a reference owned by this namespace can resolve to the tail, because the format lets a segment carry inline content for its own namespace only. Content inherited through a fork is always in an object. Resolving a reference then has two answers:

| Location | Condition | Source of bytes |
| --- | --- | --- |
| Tail | The view's projected tail carries this content ID | The bytes the projection holds |
| Object | Otherwise | The content object, as today |

The bytes are resident wherever the projected tail is: in the reader's tail cache, in the writer's own projection, and in the input a fold consumes. A reader that holds a projected tail already downloaded those bytes while replaying it, so keeping them costs memory and no request. The existing projection budgets count them and evict whole projections as they do today. A reader that lost its projection replays the tail again, as it does today for rows. There is no second cache and no read of a WAL object on demand.

A long-lived view can outlast its tail: a later fold publishes, retention advances, and collection deletes the WAL objects. Rebuilding that view fails as a stale view fails today. There is no fallback to the content object, and none is needed, because a view that still holds its projection still holds the bytes.

About a dozen call sites turn a reference into a key or bytes today, in `engine.rs`, `path/read/materialized_view.rs`, and `storage/content.rs`. They go through one resolver on the view. Reading by reference, which the search indexer and bulk reads use with references taken from the change feed, consults the projected tail of its read context.

A small file read shortly after it was written needs no content request. With the speculative read path (#966), the candidate's bytes are already available, so the read finishes when validation does.

## Direct downloads

The download contract does not change. `POST …/filesystem/downloads` exists so a deployment can serve back content larger than its proxied read limit. It returns a presigned URL for the content object. Inline content is small, so the ordinary proxied read always serves it, in one request instead of two.

A client may still ask for a direct download of a small file that has not been folded. It cannot know whether a fold has happened, and it does not need to. The service materializes the object on demand and then signs the URL:

1. Resolve the reference. If its location is the tail, take the verified bytes from the tail.
2. Write the content object at the key the reference names, with a verified immutable write.
3. Issue the presigned URL as today.

This is one step of the fold done early. The fold reads the same ID from the same log, so a later fold finds the object present, and a concurrent fold writes identical bytes. The object is published content, so collection never removes it from a live namespace. A crash after the write leaves nothing to clean up. The write follows the fold's rules for verification and for a namespace that is being deleted. The cost is one content write on the first direct download of a small file written since the last fold. The response shape, the capability, and client code are unchanged.

A deployment that cannot write content objects, such as a read-only replica, serves tail content through proxied reads. Its direct endpoint answers that the content is not yet materialized, which clears at the next fold.

The embedded `DirectDownloadTarget` follows the same rule: a handle with write authority materializes first; a read-only handle reports that no object exists yet.

## Folding

A fold turns the WAL after `last_folded_wal_no` into segments and publishes the next manifest. That manifest is what allows garbage collection to delete the folded WAL objects, so it is where inline bytes must leave the log:

1. For each inline value in the range being folded, write the content object at the key its reference names, with a verified immutable write. Run these with bounded concurrency.
2. Only after every write succeeds, build segments and publish the manifest as today.

The invariant is: **a manifest whose `last_folded_wal_no` is `n` implies a content object exists for every inline value in WAL objects up to `n`.** WAL collection already requires a WAL number to be at or below `last_folded_wal_no`, so that rule stays safe without modification.

An object already present at the key must hold the same bytes, and the verified write checks this. A mismatch, or a value that fails its own checksum, stops the fold without publishing. The bytes are inside a validated WAL payload, so this indicates the same class of fault as a corrupt WAL record, which already stops replay.

Step 1 runs inside the flush's publication budget, before any segment is written. That budget exists because unpublished segments are garbage that must not outlive its grace. In a live namespace, materialized content is never garbage: each object corresponds to a committed revision, and revisions are retained, so it is referenced whether or not this fold publishes. A fold that crashes, exceeds its budget, or loses the manifest race leaves objects the next fold finds already present. There is no orphan to discover and no cleanup state to persist.

**Deletion during a fold.** A flush can pause between reading a tail and writing its content, and the namespace can be deleted and swept in between. Every flush attempt begins with a fresh manifest observation, and materialization runs inside the flush's publication budget, which the namespace retirement grace exceeds by construction. A write that still lands late is caught the way a late upload is: the retired-owner sweep keeps listing on later passes (format specification, "Sweeping a retired owner's content"). This needs no lease, journal, or new collection family.

A fold becomes due when the unfolded tail reaches 32 segments, as today, or when its inline bytes reach the tail threshold. The second trigger bounds what any reader must download to replay a tail.

## Collection

No candidate family is added. The existing deletion rules remain, provided the invariants in this document hold: publication records the content ID, the fold materializes before it publishes, one content ID has one lifecycle, and materialization stops at deletion.

- **WAL objects** keep their rule: at or below both `last_folded_wal_no` and the WAL retention floor, and old enough. The fold invariant makes the first condition sufficient for inline bytes.
- **Content objects** written by a fold are published content. They produce the same permanent `content_publications` rows. A live namespace's content prefix is still never enumerated.
- **Upload sessions** are not involved in an inline write. Unpublished inline content cannot exist, so the ownership question that sessions answer does not arise. A write that falls back uses a session as today.
- **Deleted namespaces** sweep WAL objects and the owner's content prefix as today. Inline bytes that were never folded are removed with their WAL object. No object was written for them, and nothing needs one.

The WAL is retained until the retention floor advances, and advancing it is opt-in. By default, then, every inline value is stored twice for as long as the namespace lives: once in its WAL object and once in its content object. The per-object budget bounds one WAL object, not how many are retained. Ten million 4 KiB files duplicate about 38 GiB. Inline bytes also make change-feed reads larger.

## Pins, forks, copies, and imports

Checkpoints, snapshots, and forks pin a manifest and never replay a later tail. Every reference reachable from a manifest is materialized by the invariant, so none of them can observe tail content.

A copy or restore within the unfolded tail records the same content reference in a new revision. It adds no bytes, and the fold writes the object once.

An import reads a reference owned by another namespace and writes the bytes under a target-owned identity, as it does today. It reads the source by object key. If the source content is still in its owner's unfolded tail, no object exists yet, so the import resolves the reference through the owner namespace's view.

## Resource bounds

**Format limits.** Every reader accepts a record within these limits. They are part of the durable format and are set well above the writer's defaults, so that raising a default is never a format change.

| Limit | Proposed |
| --- | ---: |
| Largest inline value | 256 KiB |
| Largest inline total in one WAL object | 4 MiB |

**Writer policy.** These are runtime settings at or below the format limits.

| Setting | Default | Purpose |
| --- | ---: | --- |
| Inline threshold per value | 64 KiB | The lab swept 1, 4, 16, and 64 KiB against a staged control; every size beat the control by three times or more at p50 |
| Inline budget per WAL object | 1 MiB | Every commit in a batch waits for the object's PUT, including commits with no content |
| Tail inline bytes that make a fold due | 2 MiB | Bounds what a cold reader downloads: a 2 MiB tail adds about 0.45 s to a cold stat and folds in about 3 s |
| Tail inline bytes beyond which writes use the staged path | 32 MiB | Checked at admission against the loaded or last observed tail, admitted bytes, and this commit. Projection invalidation keeps the observed size. A publisher that has never observed the namespace can pass the limit once by at most one segment budget. The WAL segment-count write stop still applies. |
| Inline bytes in flight per runtime | Byte budget | Charges queued payloads, encoding copies, and materialization buffers. A write that cannot reserve uses the staged path |
| Resident tail bytes | The existing projection budgets | The reader's tail cache and the writer's projection count inline bytes |
| Materialization concurrency | 32 | Matches WAL prefetch concurrency |

64 KiB is the top of the sweep because small-object PUT latency is flat to about that size and it is the speculative read cap (#966). The sweep found no size under it where the inline path lost to the staged control. At 64 KiB, warm writes were three times faster at p50 and twice as fast at p90, and the read after a write cost no content request. The fold trigger dropped from the proposed 8 MiB because cold-stat time and fold time grew with the tail: 2, 8, and 32 MiB tails of 4 KiB files cost about 0.45, 1.0, and 2.6 s more than a folded namespace to stat cold, and folded in about 3, 7, and 19 s.

`MAX_WAL_SEGMENT_BYTES` remains a document-size limit. It is not a working-memory budget and is not the inline bound.

## Costs

- A cold reader replays a tail that can now hold MiB rather than tens of KiB. Metadata-only operations pay this too: a cold stat or list replays the same tail. The byte trigger bounds it; a separately ranged payload section would remove it.
- A reader or writer that holds a projected tail holds its inline bytes too, within the existing projection budgets.
- Commits share WAL objects. Inline bytes make an object larger and its PUT slower, and every commit in the batch waits, including commits with no content. The per-object budget is small for this reason.
- A fold does more work: up to thousands of small writes, off the commit path. Sustained fold throughput decides how fast small writes can arrive before they fall back to staging. Request cost falls overall, from four writes per small file to two.
- Inline bytes pass through WAL compression and CBOR encoding on the commit path.
- Retained WAL objects hold a second copy of small content, by default for the life of the namespace.
- The first direct download of a small file written since the last fold costs one extra content write.
- The fingerprint contract gains a second content form, with its own pinned test vectors. A rerun of `put_file_bytes` replays for inline content and conflicts for uploaded content.
- Every reader, writer, and folder of a namespace must understand the record field before any writer uses it.
- A deployment that must keep file bytes out of its metadata store could not use inline content. None exists today. Inlining is writer policy, so such a deployment would turn it off.

## Alternatives considered

**Keep values in segments, as SlateDB does.** Small reads would need no content object at all. But revisions are retained permanently, so every compaction of the revisions family would rewrite file bytes; metadata blocks would fill with payloads and slow path resolution and listing; and direct downloads need an object. Copying out at the fold keeps the metadata tree free of file bytes.

**A new content reference kind.** One revision would have two references over time: an inline kind in the log and `blob_v1` in segments. Fingerprints, retries, the change feed, and content equality would all need to treat them as equal. Naming logical content and resolving its location avoids this.

**Deriving the content ID from the commit ID and operation index.** An earlier draft did this so that a retry would build the same reference. It is unsound. The fingerprint leaves out the checksum because a fresh ID pins the bytes. With a derived ID, a second request under the same commit ID, with different bytes of the same length, has an equal fingerprint and replays the first receipt. A commit ID can also be reused after its receipt is reclaimed, while the revision it wrote is kept forever, so one immutable key could be asked to hold two different contents. Adding the payload digest to the derivation repairs both cases. It still lets a staged attempt and an inline attempt share one object, and that object then has two cleanup lifecycles.

**Payload identity for every put that supplies bytes.** This would make a rerun of `put_file_bytes` replay at any size. It reverses the rule that a put is identified by its content object, for a benefit that is small: a rerun of a large write uploads everything again before it finds its receipt, and prepared content already avoids that. It would also split the two Rust interfaces, because the server never sees the bytes of a direct upload and could not fingerprint a hosted large write the same way.

**Content IDs drawn by the client for hosted inline writes.** A resent request would carry the same ID, so the reference form could stay the only form. But the server could not check that the ID is unused by an open upload session, so one content ID having one lifecycle would depend on every client being correct. A wrong ID can stop a fold or let an expired session delete committed content.

**A separate payload section in the WAL object,** readable by range so that metadata readers skip it. This removes the cold-replay and change-feed costs. It needs a second framing layer and its own integrity check. It is a compatible later step.

**Packing a fold's values into one object.** One write per fold instead of one per value. Because revisions are never dropped, a pack in a live namespace never becomes partly dead. It changes reference resolution and direct downloads. Deferred.

**Removing upload-session writes from the staged path.** This keeps content outside the log and must replace the session's role as the collector's candidate index. It helps embedded writers only. It is the smaller step if inline content is judged too large a change. See the lab's discussion of that option.

## Rollout

Durable formats are at version 1 and carry no compatibility paths before the stable release. If this lands before that release, the record field is added to the WAL family, the golden fixtures regenerate, and writers emit inline content only when the runtime enables it. After the release, the same change needs a new WAL family version and a manifest capability so that older binaries refuse the namespace rather than report missing content. An unchanged reference shape does not remove the need for every reader and folder to understand the field. Adding the field and the read side before the release, even with writers disabled, keeps the later step small.

Order, as landed in #968 (record field), #969 (core publish), #970 (tail reads), #971 (fold materialization), #972 (downloads and imports), #973 (embedded writer), #974 (hosted), and #975 (enabled by default):

1. The read side, with writers disabled, in slices: the record field with its format limits and validation; the projected tail carrying its inline content, with one content location resolver and reads from the tail; fold materialization with its deletion rule; on-demand materialization for direct downloads and the byte-based fold trigger.
2. The embedded writer: inline prepared content with its inline fingerprint form, admission accounting, and the inline policy with its fallback. The lab sweep runs here.
3. `inline_content` on hosted commit operations, with the limit in the capability document.

The resolver is not a step of its own. With one location it would be an abstraction without a second case. Writers are enabled only when the identity, fallback, materialization, deletion, and resource rules are all in place. Download behavior, the fold trigger, and admission accounting are part of the first usable version, not later tuning.

## Verification

Tests pin contracts a reviewer would otherwise have to trust, using the request-counting and fault-injecting stores:

- A small embedded write issues one store write, and a read before the fold issues no content request.
- A fold interrupted after materialization and before publication repeats cleanly: no missing content, no unreferenced objects.
- Two folds racing over the same tail write identical keys and one manifest.
- In a live namespace, collection never deletes a WAL object whose inline value lacks a content object, across interleavings of inline commits, folds, retention advances, and collection passes. This is a simulator property. A deleted namespace may drop never-folded values with their WAL objects.
- Copy and restore of tail content, reads across a reader restart, and reads after the fold return the same verified bytes.
- A full budget sends the write down the staged path without an error.
- A corrupted inline value fails the read as content corruption and stops the fold before publication.

Adversarial cases:

| Case | Required result |
| --- | --- |
| Same commit ID, different payload of the same length | Conflict while the receipt is retained. Never a replay of the wrong bytes |
| Commit ID reused after its receipt is reclaimed | A new content ID. No collision with the earlier content |
| An inline prepared value retried after it fell back to staging, and the reverse | The same fingerprint. No content ID is reused |
| `put_file_bytes` rerun under the same commit ID | Inline content replays the original commit. Uploaded content conflicts, as today |
| The same bytes sent inline and then as an uploaded object under one commit ID | Conflict |
| An earlier attempt's session expires after a later attempt commits | Cleanup removes only the earlier attempt's content |
| Deletion or retirement during materialization | Materialization stops within one step. Late writes are covered by the repeated owner sweep |
| A pinned view rebuilt after its WAL objects were reclaimed | Fails as a stale view does today. No content fallback |
| Concurrent writes near the tail ceiling | The count includes each proposed commit. Every commit stays atomic |
| Writer threshold lowered after larger records exist | The old records still read and fold |

In the lab, the sweep measures steady small writes, cold reads, replay after a restart, and sustained fold throughput at each threshold, on the product path with the publisher and folding included. A steady small write should approach the measured one-write sequence plus the freshness probe. The hosted path should show one request per small write.

## Evidence

Lab repository, `analysis/steady-state-floors-experiments-20260916.md`, section "H3"; emulation run `20260917T025704Z-bench-s3-small-content-sequences`, release, S3 `us-east-2`, 1 KiB content, 24 rounds per arm. The inline arm submitted one 1,564-byte WAL object per write. These are request-sequence timings, not a product speedup claim. The history of the staged path's session writes is in `analysis/h2-small-write-session-cost-discussion-20260917.md`. The threshold sweep that set the defaults is `analysis/inline-content-sweep-20260918.md`.

## Open questions

1. Answered by the sweep: 64 KiB, the top of the range, and no size under it lost to the staged control. Cold replay and fold time set the fold trigger instead. Two observations stay open in the lab: a cold reader downloaded about 1.3 times the tail's inline bytes, and the sweep harness's memory grew about 14 MiB per MiB of tail.
2. Should the direct-download response later gain an inline access kind for small content, to save the second request? `access` is already kind-tagged. It would need a capability so older clients are not surprised.
3. Should hosted inline writes have a limit per namespace as well as the runtime budget, to protect the shared publisher?
4. Should the payload section be separately ranged? It would spare cold readers and change-feed readers the inline bytes they do not need. The plan is to build the simple layout, measure both costs in the sweep, and decide before the format is frozen.

Settled: nothing outside the repository reads content objects by key, and inside it one module builds content keys, so no consumer depends on an object existing as soon as a commit is visible.
