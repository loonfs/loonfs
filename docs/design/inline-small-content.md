# Inline small content

**Status: proposal.** Nothing here is implemented. Constants are starting points to tune by measurement.

LoonFS writes file bytes to a content object before it commits the metadata that names them. For a large file that is the right order: the transfer can be direct, resumable, and independent of the commit. For a small file it is most of the cost. A 1 KiB write spends three object-store writes making the bytes durable and owned, then a fourth to commit.

This proposal lets a commit carry small file bytes inside its WAL object. The bytes become durable and visible in one conditional write. The next fold copies them to ordinary content objects, so everything that reads a manifest sees the storage model it sees today.

Measured on real S3 by replaying each protocol's request sequence, 24 interleaved rounds per arm ([lab evidence](#evidence)):

| Sequence for a 1 KiB write | PUTs | p50 ms | p90 ms |
| --- | ---: | ---: | ---: |
| Current: session, content, completion, WAL | 4 | 289.1 | 428.8 |
| Inline: WAL only | 1 | 58.8 | 69.9 |

Inline was faster in 24 of 24 rounds. These timings cover store requests only, not writer authority, freshness, or WAL encoding.

## What changes and what does not

| | Today | Proposed for content at or under the inline limit |
| --- | --- | --- |
| Embedded small write | Upload session, content, completion, WAL | WAL |
| Hosted small write | Three or more HTTP requests; four to six store writes | One HTTP request; one store write |
| Read of a recently written small file | Freshness probe, then content GET | Freshness probe; bytes come from the replayed tail |
| Read after the next fold | Freshness probe, then content GET | Unchanged |
| Content references, revision rows, change feed | | Unchanged |
| Checkpoints, snapshots, forks | | Unchanged |
| Garbage collection rules | | Unchanged |
| Large files, direct transfers, and the download contract | | Unchanged |

Three other systems built on object storage put payload bytes in their log and reorganize them later: SlateDB keeps values in WAL and SST objects, turbopuffer commits documents to its WAL and indexes them asynchronously, and Cursor's Git storage writes each push's packfile as a WAL entry and compacts in the background. LoonFS is the exception because it needs an independently addressable object for large and direct transfers. This proposal keeps that object and removes the wait for it.

## A reference names content, not a location

The content reference does not change. A commit that carries inline bytes records the same `append_file_revision` delta it records today, with an ordinary `blob_v1` reference: content ID, size, and checksum. The reference names the object the fold will write. Until then the WAL record holds the authoritative copy.

This keeps one identity for one piece of content over its whole life. Revision rows, commit fingerprints, the change feed, retained receipts, and equality checks such as the speculative read's same-content test never see two different references for the same bytes.

The content ID must be the same on every attempt of the same commit, because the commit fingerprint includes it and because folds must agree on where to write. It is derived rather than drawn:

```text
content_id = "con_" + hex(first 128 bits of SHA-256(commit_id, semantic_op_index))
```

The ID keeps its current shape and still shards uniformly. It still says which object, never what the object contains, and it has no clock component. A retried request, a batch re-planned at a later WAL number, and two folds racing over the same log all arrive at the same key.

## The WAL record

A commit record gains one optional field:

| Field | Meaning |
| --- | --- |
| `inline_content` | A list of `{content_id, bytes}`. `bytes` is a CBOR byte string. |

Replay validation adds these rules:

- Every entry's `content_id` is named by an `append_file_revision` delta in the same commit.
- An entry's length equals that reference's `size_bytes` and is at most the inline limit.
- A content ID appears at most once per commit.
- The segment's inline total is within the per-segment budget.

Inline bytes sit inside the WAL payload, so the envelope's existing validation covers them. Each value is also checked against its reference's size and checksum when it is read and when it is materialized, as content objects are today. Replay does not hash payloads.

## Writing

**Embedded.** `put_file_bytes` and `put_file_stream` choose the inline path when the content is nonempty, at or under the inline limit, and the budgets below have room. They build the reference, attach the bytes to the commit candidate, and skip staging. No upload session is written and no admission proof is needed, because no content exists outside the commit. `prepare_file_bytes` keeps its meaning: it makes content durable before publication and returns evidence, so it keeps the staged path.

**Hosted.** A `put_file` operation in a commit request may carry `inline_content` instead of a content reference and token. The server checks the size, computes the checksum, derives the content ID, and publishes. A small hosted write becomes one request and one store write. The 2 MiB JSON request limit already bounds a commit's inline bytes, to about 1.5 MiB of content after base64 encoding.

**Falling back is always allowed.** Every inline limit is a preference. When a value is too large, a segment's inline budget is full, or the unfolded tail already holds too many inline bytes, the writer uses the staged path for that content. No inline limit produces a write error.

The model in the API specification has three stages: make content durable, make metadata visible, observe changes. Inline content merges the first two. Durability and visibility arrive in the same conditional write, which is strictly stronger than today's ordering.

## Reading

A view already replays the unfolded WAL tail. While it does, it records which content IDs the tail carries inline. Resolving a reference then has two answers:

| Location | Condition | Source of bytes |
| --- | --- | --- |
| Tail | The view's tail carries this content ID | The replayed bytes, or a GET of the WAL object that holds them |
| Object | Otherwise | The content object, as today |

Replayed bytes are kept in a byte-budgeted cache next to the tail projection. A value that is not resident costs one GET of its WAL object, the same as a content object read. About a dozen call sites turn a reference into a key or bytes today, in `engine.rs`, `path/read/materialized_view.rs`, and `storage/content.rs`. The first implementation step routes them through one resolver without changing behavior.

A small file read shortly after it was written needs no content request. With the speculative read path proposed in #966, the candidate's bytes are already available, so the read finishes when validation does.

## Direct downloads

The download contract does not change. `POST …/filesystem/downloads` exists so a deployment can serve back content larger than its proxied read limit. It returns a presigned URL for the content object. Inline content is at most the inline limit, so the ordinary proxied read always serves it, in one request instead of two.

A client may still ask for a direct download of a small file that has not been folded. It cannot know whether a fold has happened, and it does not need to. The service materializes the object on demand and then signs the URL:

1. Resolve the reference. If its location is the tail, take the verified bytes from the tail.
2. Write the content object at its derived key with a create-only verified write.
3. Issue the presigned URL as today.

This is one step of the fold done early. The key is the one the fold would use, so a later fold finds the object present, and a concurrent fold writes identical bytes. The object is published content, so collection never removes it from a live namespace. A crash after the write leaves nothing to clean up. The cost is one content write on the first direct download of a small file written since the last fold. The response shape, the capability, and client code are unchanged.

A deployment that cannot write content objects, such as a read-only replica, serves tail content through proxied reads. Its direct endpoint answers that the content is not yet materialized, which clears at the next fold.

The embedded `DirectDownloadTarget` follows the same rule: a handle with write authority materializes first; a read-only handle reports that no object exists yet.

## Folding

A fold turns the WAL after `last_folded_wal_no` into segments and publishes the next manifest. That manifest is what allows garbage collection to delete the folded WAL objects, so it is where inline bytes must leave the log:

1. For each inline value in the range being folded, write the content object at its derived key with a create-only verified write. Run these with bounded concurrency.
2. Only after every write succeeds, build segments and publish the manifest as today.

The invariant is: **a manifest whose `last_folded_wal_no` is `n` implies a content object exists for every inline value in WAL objects up to `n`.** WAL collection already requires a WAL number to be at or below `last_folded_wal_no`, so that rule stays safe without modification.

Step 1 runs before the metadata publication budget starts. That budget exists because unpublished segments are garbage that must not outlive its grace. Materialized content is never garbage: each object corresponds to a committed revision, so it is referenced whether or not this fold publishes. A fold that crashes, exceeds its budget, or loses the manifest race leaves objects the next fold finds already present. There is no orphan to discover and no cleanup state to persist.

A fold becomes due when the unfolded tail reaches 32 segments, as today, or when its inline bytes reach the tail threshold. The second trigger bounds what any reader must download to replay a tail.

If a value fails verification during materialization, the fold stops without publishing. The bytes are inside a validated WAL payload, so this indicates the same class of fault as a corrupt WAL record, which already stops replay.

## Collection

No family, rule, or clock assumption is added.

- **WAL objects** keep their rule: at or below both `last_folded_wal_no` and the WAL retention floor, and old enough. The fold invariant makes the first condition sufficient for inline bytes.
- **Content objects** written by a fold are published content. They produce the same permanent `content_publications` rows. A live namespace's content prefix is still never enumerated.
- **Upload sessions** are not involved. Unpublished inline content cannot exist, so the ownership question that sessions answer does not arise on this path.
- **Deleted namespaces** sweep WAL objects and the owner's content prefix as today. Inline bytes that were never folded are removed with their WAL object.

Because the WAL is retained until the retention floor advances, inline bytes remain in folded WAL objects beside their content objects. This costs storage, bounded by the per-segment budget, and makes change-feed reads larger.

## Pins, forks, copies, and imports

Checkpoints, snapshots, and forks pin a manifest and never replay a later tail. Every reference reachable from a manifest is materialized by the invariant, so none of them can observe tail content.

A copy or restore within the unfolded tail records the same content reference in a new revision. It adds no bytes, and the fold writes the object once. An import into another namespace reads the source bytes through the resolver and writes them under a target-owned identity, as it does today.

## Resource bounds

| Bound | Proposed | Purpose |
| --- | ---: | --- |
| Inline limit per value | 64 KiB | Small-object PUT latency is flat to this size; matches the speculative read cap proposed in #966 |
| Inline bytes per WAL object | 4 MiB | Bounds publisher memory, WAL object size, and one replay step |
| Tail inline bytes that make a fold due | 32 MiB | Bounds what a cold reader downloads |
| Tail inline bytes beyond which writes use the staged path | 64 MiB | Hard ceiling when folding falls behind |
| Tail content cache | Byte budget shared by the runtime | Bounds resident inline bytes |
| Materialization concurrency | 32 | Matches WAL prefetch concurrency |

`MAX_WAL_SEGMENT_BYTES` remains a document-size limit. It is not a working-memory budget and is not the inline bound.

## Costs

- A cold reader replays a tail that can now hold tens of MiB rather than tens of KiB. The byte trigger bounds it; a separately ranged payload section would remove it.
- A fold does more work: up to thousands of small writes, off the commit path. Request cost falls overall, from four writes per small file to two.
- Inline bytes pass through WAL compression and CBOR encoding on the commit path.
- Retained WAL objects hold a second copy of small content until retention advances.
- The first direct download of a small file written since the last fold costs one extra content write.
- Every reader, writer, and folder of a namespace must understand the record field before any writer uses it.

## Alternatives considered

**Keep values in segments, as SlateDB does.** Small reads would need no content object at all. But revisions are retained permanently, so every compaction of the revisions family would rewrite file bytes; metadata blocks would fill with payloads and slow path resolution and listing; and direct downloads need an object. Copying out at the fold keeps the metadata tree free of file bytes.

**A new content reference kind.** One revision would have two references over time: an inline kind in the log and `blob_v1` in segments. Fingerprints, retries, the change feed, and content equality would all need to treat them as equal. Naming logical content and resolving its location avoids this.

**Random content IDs assigned at the fold.** A fold that loses the manifest race would leave unreferenced objects that need discovery after a restart. Derived IDs make materialization idempotent.

**A separate payload section in the WAL object,** readable by range so that metadata readers skip it. This removes the cold-replay and change-feed cost. It needs a second framing layer and its own integrity check. It is a compatible later step.

**Packing a fold's values into one object.** One write per fold instead of one per value. Because revisions are never dropped, a pack in a live namespace never becomes partly dead. It changes reference resolution and direct downloads. Deferred.

**Removing upload-session writes from the staged path.** This keeps content outside the log and must replace the session's role as the collector's candidate index. It helps embedded writers only. See the lab's discussion of that option.

## Rollout

Durable formats are at version 1 and carry no compatibility paths before the stable release. If this lands before that release, the record field is added to the WAL family, the golden fixtures regenerate, and writers emit inline content only when the runtime enables it. After the release, the same change needs a new WAL family version and a manifest capability so that older binaries refuse the namespace rather than report missing content. Adding the field and the read side before the release, even with writers disabled, keeps the later step small.

Suggested order:

1. One resolver for content location. No behavior change.
2. The record field, replay validation, tail content cache, reads from the tail, and fold materialization. Writers disabled.
3. The embedded inline policy with its budgets and fallback; lab measurement.
4. `inline_content` on hosted commit operations.
5. On-demand materialization for direct downloads.
6. The byte-based fold trigger and tuned constants.

## Verification

Tests pin contracts a reviewer would otherwise have to trust, using the request-counting and fault-injecting stores:

- A small embedded write issues one store write, and a read before the fold issues no content request.
- A fold interrupted after materialization and before publication repeats cleanly: no missing content, no unreferenced objects.
- Two folds racing over the same tail write identical keys and one manifest.
- Collection never deletes a WAL object whose inline value lacks a content object, across interleavings of inline commits, folds, retention advances, and collection passes. This is a simulator property.
- Copy and restore of tail content, reads across a reader restart, and reads after the fold return the same verified bytes.
- A full budget sends the write down the staged path without an error.
- A retried commit derives the same content ID and replays its receipt.
- A corrupted inline value fails the read as content corruption and stops the fold before publication.

In the lab, a product scenario for steady small writes should approach the measured one-write sequence plus the freshness probe, and the hosted path should show one request per small write.

## Evidence

Lab repository, `analysis/steady-state-floors-experiments-20260916.md`, section "H3"; emulation run `20260917T025704Z-bench-s3-small-content-sequences`, release, S3 `us-east-2`, 1 KiB content, 24 rounds per arm. The inline arm submitted one 1,564-byte WAL object per write. The history of the staged path's session writes is in `analysis/h2-small-write-session-cost-discussion-20260917.md`.

## Open questions

1. Should the inline limit start below 64 KiB while the cold-replay cost is unmeasured?
2. Should the direct-download response later gain an inline access kind for small content, to save the second request? `access` is already kind-tagged. It would need a capability so older clients are not surprised.
3. Does any consumer depend on a content object existing as soon as a commit is visible?
4. Should hosted inline writes be limited per request or per namespace to protect the shared publisher?
5. Is the byte-based fold trigger enough, or should the payload section be separately ranged from the start?
