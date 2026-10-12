# Content search index

LoonFS supports grep-style regular-expression queries through a derived content index. The index narrows down the files that a query needs to read, and then the query checks those files against the original pattern. Index maintenance runs separately from metadata maintenance, so a file's metadata commit doesn't wait until its content has been indexed.

The index is stored under the namespace's grep extension prefix. It can be rebuilt from filesystem state, and core readers don't need to decode it to read the namespace. This note explains what the index contains, how it's built, how queries use it, and how its objects are collected. [Grep extension format](../specs/format.md#appendix-d-grep-extension-format) defines the durable encodings.

## The approach

This section describes how a trigram index narrows a regular-expression search.

Many regular expressions require particular literal bytes in every match. A "trigram index" records every three-byte substring (a "gram") of eligible content. To run a query, LoonFS extracts the grams that the pattern requires, uses posting lists to find candidate revisions, and runs the original regular expression over their contents.

```text
pattern -> required grams -> candidate revisions
        -> visibility and scope checks -> content verification -> matches
```

For example, the literal `invoice` requires the grams `inv`, `nvo`, `voi`, `oic`, and `ice`. A file that's missing any of those grams can't contain the literal. A file that contains all five might still not match, because the grams could occur in separate places. The final content check removes those false positives.

The index has two separate correctness requirements:

- Every reported match must pass the actual pattern and the visibility checks.
- Every eligible revision covered by the index's completed watermark must have the required postings.

Verification can remove an extra candidate, but it can't recover a matching file that never became a candidate. So LoonFS scans newer, unindexed revisions exhaustively, within the query's tail budget. It returns an index-only result only when the caller explicitly allows stale results.

## What gets indexed

This section describes which revisions the index includes.

LoonFS checks whether content is eligible when it indexes the content, not when the content is uploaded. Content is eligible when it meets both of these conditions:

| Condition | Rule |
| --- | --- |
| Content size | At most 8 MiB. |
| Text sample | The first 8 KiB contains no NUL byte and passes the UTF-8 sample check. |

The sample check accepts an incomplete final UTF-8 character when it follows a nonempty valid prefix. The sample is a heuristic for detecting text, not a guarantee about every byte in the file. Files that fail the rule are left out of both indexed search and the scan of the unindexed tail. LoonFS doesn't use a list of filename extensions, so files without an extension, such as `README` and `Makefile`, are checked the same way as files in formats with a known text extension.

Index builders and queries must apply the same eligibility rule. So the size cap and the sampling rule are part of the versioned search semantics, not deployment settings that can be tuned on their own. Attributes don't override eligibility.

Why must builders and queries use the same rule? Otherwise, a builder could advance its watermark past a file that a query considers searchable, even though the file has no postings.

Why can't an attribute mark a file as text or binary? An override like that would need the builder, queries, and rebuilds to change together.

## Postings

This section describes the posting rows that the index stores.

The tokenizer folds ASCII letters to lowercase, and then extracts every overlapping three-byte window. Grams are byte sequences, not Unicode characters. This folding supports common case-insensitive ASCII searches without storing separate postings for each case, but it doesn't implement general Unicode case folding.

Each posting identifies `(inode_id, revision_no)`, and it doesn't contain a path. Paths are resolved from metadata when a query runs, so a rename can change the paths in results without rewriting any postings.

A posting row has this structure:

```text
key:      gram-{six lowercase hex characters}-{first inode id:020}
payload:  a packed batch of (inode_id, revision_no) pairs
```

The writer's target is about 256 postings per row. The batch stores its count, then the first inode ID and revision number, and then the later inode deltas and absolute revision numbers, all as unsigned varints. The pairs are ordered, and the rows for a gram are read together.

Batching reduces the overhead of each row, and it keeps using the same segment and iteration code as other sorted rows. Readers take the union of posting batches from different runs, so reorganization doesn't need to combine every posting for a gram into one large payload to stay correct.

Sharing inode IDs doesn't make an index reusable across namespaces. A fork starts with no grep state and no copied posting segments, and it's indexed on its own, even though its inherited metadata contains the same inode IDs.

## Segments, manifests, and discovery

This section describes how the index's objects are stored, and how a query finds the current set of segments.

Grep segments use the layout described in [Metadata block storage](metadata-block-storage.md), which has prefix-compressed row keys, independently compressed data blocks, a Bloom filter, an index, and per-section checksums. Only the row payload is specific to grep. The filter key is the `gram-{hex}` part of the row key, so a negative filter result rules out a segment for that gram.

```text
namespaces/{namespace_id}/extensions/grep/
├── hint.json
├── manifests/{manifest_no:020}.json
└── segments/{segment_id}.sst.zst
```

The grep hint records the manifest number where discovery starts. A reader loads that manifest, and then probes the following numbers until one isn't found. The current grep manifest contains the lifecycle state, the visible segments, the run allocator, and any pending reorganization. Core manifests contain none of this state.

Enabling the index writes a hint that points to manifest 1, and then creates manifest 1. Each later publication works in these steps:

1. The publisher writes the completed segments.
2. It creates the next manifest with put-if-absent. A publisher that loses the race stops, and its next step loads the winning manifest and plans again.
3. After a successful publication, it raises the hint with compare-and-swap. If raising the hint fails, the publication still stands.

On every request, a query checks whether its cached manifest has a successor. The query runs discovery again if a successor exists, or if the response arrives `READ_REVALIDATION_BOUND_MS` or more after the manifest's previous check. The hint can lag, so it doesn't replace this freshness check. [Grep extension format](../specs/format.md#appendix-d-grep-extension-format) defines how identity and checksums are validated.

## Index lifecycle

An index is in one of three lifecycle states, and the current grep manifest records the state along with its position:

| State | Position and behavior |
| --- | --- |
| `disabled` | No segment set that queries can see, and no pending reorganization. |
| `backfilling` | A checkpoint ID, `captured_seq`, and optionally the last inode consumed (`cursor_inode_id`). Queries are unavailable. |
| `active` | An incremental cursor made of `built_through_seq` and `next_event_index`. Queries can use the completed index plus the unindexed tail. |

```text
                             +-- rebuild from a new checkpoint --+
                             v                                   |
+----------+  enable  +-------------+  final backfill step  +--------+
| disabled | -------> | backfilling | --------------------> | active |
+----------+          +-------------+                       +--------+
     ^                       |                                   |
     |                       |              disable              |
     +-----------------------+-----------------------------------+
```

A `next_event_index` of zero means the cursor is at a completed commit boundary. A nonzero value identifies the next event to process within `built_through_seq`, so that commit is only partly indexed. A consumer must not treat the sequence alone as complete coverage of that commit. The HTTP status response can leave out an event index of zero, but the durable encoding has its own rules for which fields are present.

One request operation can produce several change events, so the event index is an offset within the commit's ordered events, not a count of request operations.

Disabling the index publishes a `disabled` manifest with no segment references. The operation doesn't delete the previous objects synchronously. A worker that's in progress when it finds the disabled manifest stops, instead of publishing its earlier state again.

## Building the index

A worker builds the index in two phases, first with a backfill from a checkpoint, and then incrementally from the change feed.

### Initial backfill

Enabling the index creates an expiring user checkpoint at the namespace head, and then publishes a `backfilling` manifest with that checkpoint ID, the target sequence, and no inode cursor.

Each bounded build step:

1. Lists files from the checkpoint in ascending inode order.
2. Reads one current revision of each retained file, including deleted files.
3. Applies the eligibility rule and extracts grams.
4. Writes new delta segments.
5. Publishes the segment set and the last inode it consumed together, in the next numbered grep manifest.

The final backfill step changes the lifecycle state to `active` at the captured sequence, and releases the checkpoint. Writes committed after that sequence are then processed through the change feed. They don't change the target of the backfill that's already in progress, which was set when the backfill started.

If the checkpoint, or change history that the worker needs, becomes unavailable, the worker abandons the incomplete build and starts another backfill from a new checkpoint. It doesn't report the partial index as complete.

### Incremental builds

An active worker resumes the change feed from its cursor (a sequence and an event index). It indexes the eligible revisions that those events publish, and it advances the cursor in the same numbered manifest publication that publishes their segment references.

Moves, deletes, and undeletes don't publish new content revisions in those events, so they don't need new postings. Queries check their effect on visibility and paths against metadata. Operations that append a new revision are processed as revision events, even when the underlying bytes were already stored.

By default, the build budget is 256 files or 64 MiB per step. Each batch is bounded, but the total cost of backfilling a namespace still depends on how much eligible content it has. Failed attempts, or a restarted backfill, can read content again.

The index doesn't stop the namespace's retention floor from advancing. If the change history that the worker needs has been removed, the worker rebuilds the index from a new checkpoint.

## Maintenance scheduling

This section describes when the reference server and the CLI run index work.

The reference server runs `GrepWorker` from its maintenance sweep, alongside core metadata maintenance. Each visit runs build steps for as long as each step publishes, up to 16 steps, and then runs one reorganization step once the index is up to date. A collection pass also runs grep garbage collection. A failure for one namespace is logged and retried on the next pass, and it doesn't delay unrelated namespaces.

Every five seconds, a second pass checks each writer session that the server has open. If the session's last published sequence has moved since that pass last indexed it, the pass builds that namespace's index, so a commit through that server is indexed within seconds. The sweep and that pass never build the same namespace's index at the same time. Enabling the index publishes the backfill, and the next sweep pass starts it. Queries don't schedule any work.

A query-only server doesn't build indexes, and it rejects index mutations. No grep operation lists namespaces. The sweep lists them from the store.

Embedded CLI profiles don't run any index work after a write. `loonfs maintenance index enable` captures a target sequence and runs bounded build steps until the index reaches it. Later writes to the namespace don't extend that target. The command has these options:

- `--no-wait` returns after the index is enabled.
- `--max-steps` and `--deadline-ms` limit the wait. When either one runs out, the command prints its progress and exits with a nonzero status.

Running the command again brings an index that has fallen behind up to the namespace head.

Building and reorganizing the index don't collect garbage. Grep garbage collection is a separate call (see [Grep garbage collection](#grep-garbage-collection)).

## Reorganization

Reorganization merges the index's runs across three tiers: delta, mid, and base. Delta runs are merged into mid runs, and accumulated mid runs are merged with the base into a new base.

The thresholds count logical runs, not physical segments. Every build publication that writes rows allocates one `run_no` for its batch from `next_run_no`. A run can contain several segments, and it still counts as one run toward reorganization. Separate backfill batches stay separate runs, even though they share the same captured target sequence.

With eight delta runs per mid run, and eight mid runs per base rewrite, a simplified sequence of full batches rewrites the base about once every 64 build runs. A two-tier scheme that rewrote the base after every eight delta runs would rewrite it eight times as often in that example. This is a constant-factor reduction. Because the structure always has three tiers, it doesn't provide logarithmic cumulative write amplification as the corpus grows.

### Processing a reorganization in steps

A reorganization first records the exact set of input segments that it will merge. It then scans the gram key space in steps that each process a bounded number of rows. Each step writes output segments at the selected tier and publishes its progress.

Until the scan completes, both the inputs and the completed outputs stay referenced and visible to queries. Segments published after the reorganization selected its inputs aren't part of that reorganization, and they stay referenced when it completes. The final step removes the selected inputs, and keeps the completed outputs and the unrelated segments.

The row budget can be exceeded for rows with equal keys. All the rows with the same key are processed together before the cursor moves past that key.

The manifest records each segment's `level` (`0`, `1`, or `2`) and `run_no`, and the index's `next_run_no`. An in-progress reorganization records its `output_level`, its output `run_no` (which doesn't change during the reorganization), its selected inputs, its outputs, and its cursor. A replacement worker resumes from the last manifest that was published successfully.

Why can queries see both the inputs and the outputs? Postings are add-only, and readers take the union of the batches, so a posting that appears twice doesn't cause a missed candidate. This isn't a general rule for replacing metadata rows that have different retention semantics.

Why aren't rows with equal keys split across steps? If a step split that group and recorded an exclusive last-key cursor, a resumed reorganization could skip the group's remaining rows.

### Old postings

The index doesn't remove a posting just because its revision can no longer be observed. Queries filter those candidates against current metadata before they report results. Removing those postings would need a separate, correct liveness policy. Routine segment merging doesn't remove them.

So the index can grow with historical revisions, not just with the files that are currently visible. How much it grows depends on the workload. It should be measured, instead of being hidden by an index-size estimate that's based only on the current files.

## Queries

This section describes how a grep query runs.

To run a query, the serving deployment must advertise the query capability, and the namespace must have a usable grep index. Advertising the capability doesn't show that backfill is complete for every namespace.

The endpoint is `GET /v0/namespaces/{ns}/grep`. A request includes the pattern, case sensitivity, an optional path-prefix scope, and an optional cursor. It can also set two flags:

- `allow_scan` allows a bounded scan for a pattern that grams can't narrow.
- `allow_stale` allows indexed-only results when the unindexed tail can't be scanned within the applicable budget.

Patterns use the Rust `regex` dialect, without backreferences or lookaround. The planner must interpret the pattern syntax the same way as the final verifier.

### Planning

The planner derives constraints that every match must satisfy. It represents the plan as an AND of sets of alternative grams, so a candidate must contain at least one gram from every set.

For concatenated literal text, the planner can intersect the required trigrams directly. For alternatives, the constraints must keep every branch that could match. When the planner can't establish a constraint safely, it leaves the constraint out. Fewer constraints produce more candidates, but they don't cause valid matches to be missed.

Grams folded to ASCII lowercase work well for common case-insensitive patterns. A non-ASCII case alternative might not map to the same indexed bytes, so the planner must weaken that constraint instead of assuming that the index implements Unicode case folding.

By default, LoonFS rejects a valid pattern that has no useful required grams (such as `.*` or a single character) as unindexable. With `allow_scan`, it runs an explicitly bounded scan instead. A syntax error and a valid pattern that can't be indexed are different outcomes.

### Candidate selection and verification

Each page of results uses one read view, and the query works through it in these steps:

1. For each required gram, the query skips segments whose key ranges don't include the gram, checks Bloom filters, and reads posting batches. Sorted intersections and unions of those batches produce candidate (inode, revision) pairs.
2. It resolves the candidates in batches against that read view, to find each one's visibility, current revision, and current path.
3. For each remaining candidate, it reads the referenced content, verifies it, and runs the original pattern. Content reads use limited concurrency.

A posting for an older revision isn't a match against a file's current revision. Path-prefix filtering applies to the resolved path, not to a path cached when the posting was created. The response contains line-oriented matches, not a streaming file response.

Each match identifies the inode, the revision, the derived absolute path, the line number (starting at 1), the byte offset, and the matching line. A line longer than 512 bytes is truncated, and `line_truncated` shows that it was. Matches are ordered by inode ID and byte offset.

### Pagination

A page is limited by both its match limit and its budget of verified candidates.

The continuation cursor records the scan's progress, including candidates that produced no matches, not just the last match that was returned. It's bound to the request fields that select results (the pattern, the case flag, the path scope, `allow_scan`, and `allow_stale`), and a request that reuses it with different values for those fields is rejected.

Each page reports the namespace `head_seq` that it used. Every metadata phase within the page uses that view, but later pages can see a newer head. A grep request has no snapshot selector, so a search across several pages isn't kept at one durable snapshot.

Why does a page need a candidate budget as well as a match limit? A pattern with many false-positive candidates produces few matches, and it must not trigger an unlimited content scan just because of that.

## Freshness and the unindexed tail

This section describes how a query covers revisions that the index doesn't include yet.

At a completed watermark, the index contains the postings needed for eligible revisions through that boundary. The query lists the revisions after the watermark, up to the head of its read view, from the change feed, and checks them exhaustively with the same eligibility rule and verifier. A partly indexed commit must be handled according to its event cursor, not assumed to be complete because of its sequence.

For example, an index completed through sequence 100 can still return a current result at head 103, because the query also checks the new revisions from commits 101 to 103. A metadata-only rename in that range changes a result's path through the read view, without needing a new content posting.

If the tail is larger than the query's budget:

- By default, the query fails with an `index_lagging` error.
- With `allow_stale`, the server can return indexed-only results and report `tail_scanned: false`, along with the index and head positions. Stale results still go through visibility checks and content verification, but they might leave out eligible revisions that aren't indexed yet.

If the tail's change history is below the retention floor, the query returns `rebootstrap_required`, with or without `allow_stale`. The worker's next step rebuilds the index from a new checkpoint.

Index maintenance reduces this gap when it runs. Core backpressure on the WAL tail doesn't limit grep lag by itself, because metadata can be folded while grep maintenance is still behind. When the gap is too large, the query's tail budget and the explicit stale-result option determine what happens.

## Grep garbage collection

Grep garbage collection deletes grep manifests and segments that are no longer needed. Each call covers one namespace, and it runs through any of these:

- `loonfs maintenance grep-gc`
- `POST /v0/maintenance/namespaces/{ns}/runs` with `kind: "grep_gc"`
- the reference server's sweep, on each collection pass

Building and reorganizing the index don't run it.

Each call loads the current manifest, builds its set of live segments, and scans the manifest and segment collections from beginning to end. It uses one clock value for the whole call, and it stores no progress cursor. If a root is invalid or unreadable, the call fails before it deletes anything.

The collector keeps objects under these rules:

- Manifest numbers at or above the observed hint stay, so discovery can use them.
- A manifest below the current one, and every segment it lists, stays while its immediate successor is younger than the ordinary grace. A successor without a provider timestamp counts as young.
- Once that rule no longer keeps a manifest below the hint, the manifest's own provider age must meet the ordinary grace before it's deleted.
- The current manifest keeps every segment it lists, including the inputs and outputs of a pending reorganization.
- An unreferenced segment must be strictly older than 24 hours before it's deleted.

Every step that produces output checks the bounded metadata publication budget before it creates its next manifest. That budget and the collection age rules protect concurrent builds. A failed or interrupted step can leave segments behind for a later collection.

If the core namespace is missing or deleted, an explicit call can delete the entire grep prefix after the ordinary grace. The core and grep collectors work on separate families of objects, and neither one resumes a durable collection run from a previous call.

## Costs and defaults

The index has three main costs:

- reading content to build the index,
- reading and rewriting posting segments during reorganization, and
- reading candidate content during queries.

A pattern's selectivity determines how much of the last cost can be avoided.

Each eligible revision is read and tokenized during its build attempt, and retries and restarted backfills can repeat that work. The build budgets limit one invocation, not the total work needed to index an arbitrarily large namespace.

These distinctions matter for compatibility and tuning:

| Category | Rules or settings |
| --- | --- |
| Versioned search semantics | ASCII-folded byte trigrams, the eligibility cap and sample rule, posting keys, and the packed posting encoding. |
| Build defaults | 256 files or 64 MiB per step. |
| Reorganization defaults | Eight delta runs and eight mid runs per trigger, with a bounded row target for each step. |
| Posting writer target | About 256 postings per row. |
| Serving limits | Match count, candidate verification, matching-line length, content-read concurrency, and unindexed-tail budgets. |

Writer targets and serving budgets don't change how stored postings are interpreted. A change to tokenization or eligibility needs a new version and a rebuild strategy. Because the index can be rebuilt, its derived state can be replaced, but that doesn't make a large rebuild inexpensive.
