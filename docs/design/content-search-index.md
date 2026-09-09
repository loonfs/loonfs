# Content search index

LoonFS supports grep-style regular-expression queries through a derived content index. The index narrows the files that need to be read, then the query implementation checks those files against the original pattern. Index maintenance runs separately from metadata maintenance and does not delay a file's metadata commit until its content has been indexed.

The index is stored under the namespace's grep extension prefix. It can be rebuilt from filesystem state, and core readers do not need to decode it to read the namespace. This document explains its contents, build process, query behavior, and collection rules. The durable encodings are defined in the [storage format](../specs/format.md).

## The approach

Many regular expressions require particular literal bytes in every match. A trigram index records every three-byte substring of eligible content. Query planning extracts gram requirements from the pattern, uses posting lists to find candidate revisions, and runs the original regular expression over their contents.

```text
pattern -> required grams -> candidate revisions
        -> visibility and scope checks -> content verification -> matches
```

For example, the literal `invoice` requires the grams `inv`, `nvo`, `voi`, `oic`, and `ice`. A file missing any of those grams cannot contain the literal. A file containing all five may still fail to match: the grams could occur in separate locations. The final content check removes those false positives.

There are two distinct correctness requirements. First, every reported match must pass the actual pattern and visibility checks. Second, every eligible revision covered by the index's completed watermark must have the required postings. Verification can remove an extra candidate, but it cannot recover a matching file that never became a candidate.

Newer, unindexed revisions are therefore scanned exhaustively within the query's tail budget. An index-only result is returned only when the caller explicitly permits stale results.

## What gets indexed

Eligibility is evaluated during indexing rather than upload. The initial rule has two conditions:

| Condition | Version-1 rule |
| --- | --- |
| Content size | At most 8 MiB. |
| Text sample | The first 8 KiB contains no NUL byte and passes the UTF-8 sample check. |

An incomplete final UTF-8 character is accepted when it follows a nonempty valid prefix. The sample is a text-detection heuristic, not a guarantee about every byte in the file. Files that fail the rule are excluded from both indexed search and the unindexed-tail scan. No filename-extension list is used, so extensionless files such as `README` and `Makefile` are evaluated in the same way as named text formats.

Builders and queries must apply the same eligibility rule. Otherwise, a builder could advance its watermark past a file that a query considers searchable even though the file has no postings. The size cap and sampling rule are therefore part of the versioned search semantics, not independent deployment tunables.

Attributes already exist in the filesystem model. Using an attribute as a text/binary override remains deferred; current indexing does not treat an arbitrary resource-type hint as an eligibility override. Introducing one would require coordinated builder, query, and rebuild behavior.

## Postings

The tokenizer extracts every overlapping three-byte window after folding ASCII letters to lowercase. Grams are byte sequences rather than Unicode characters. This supports common ASCII case-insensitive searches without storing separate postings for each case, but it does not implement general Unicode case folding.

Each posting identifies `(inode_id, revision_no)`. It does not contain a path. Renames can therefore change result paths without requiring posting rewrites; paths are resolved from metadata at query time.

A posting row has this structure:

```text
key:      gram-{six lowercase hex characters}-{first inode id:020}
payload:  a packed batch of (inode_id, revision_no) pairs
```

The target is approximately 256 postings per row. The batch stores its count, the first inode ID and revision number, then subsequent inode deltas and absolute revision numbers as unsigned varints. The pairs are ordered, and rows for a gram are read together.

Batching reduces per-row overhead while retaining the segment and iteration machinery used for other sorted rows. Readers union posting batches from different runs. Reorganization does not need to combine every posting for a gram into one monolithic payload to preserve correctness.

Inode identity alone does not make an index reusable across namespaces. A fork begins without grep state or copied posting segments and is indexed independently, even though its inherited metadata contains the same inode IDs.

## Segments, manifests, and discovery

Grep segments use the layout in [metadata block storage](metadata-block-storage.md): prefix-compressed row keys, independently compressed data blocks, a bloom filter, an index, and per-section checksums. The payload is grep-specific. The filter key is the gram prefix, so a negative result excludes a segment for that gram.

```text
namespaces/{namespace_id}/extensions/grep/
├── hint.json
├── manifests/{manifest_no:020}.json
└── segments/{segment_id}.sst.zst
```

The hint records a manifest number from which discovery starts. A reader loads that manifest and probes consecutive numbers until not-found. The current manifest contains lifecycle, visible segments, the run allocator, and pending reorganization. Core manifests contain none of this state.

Enablement writes the hint naming manifest 1 before creating manifest 1. Later publications write completed segments, then the next manifest with put-if-absent. A competing publisher reloads the winner and re-plans. Successful publication raises the hint by CAS; a failed raise does not undo publication.

Queries check for a successor to their cached manifest on every request. A present successor requires discovery again. The hint can lag and does not replace this freshness check. The [grep format](../specs/format.md#appendix-d-grep-extension-format) defines identity and checksum validation.

## Index lifecycle

The lifecycle has three states:

| State | Position and behavior |
| --- | --- |
| `disabled` | No query-visible segment set and no pending reorganization. |
| `backfilling` | A checkpoint ID, captured `target_seq`, and optional last-consumed `cursor_inode_id`. Queries are unavailable. |
| `active` | An incremental cursor consisting of `built_through_seq` and `next_event_index`. Queries can use the completed index plus the unindexed tail. |

A zero `next_event_index` denotes a completed commit boundary. A nonzero value identifies the next event to process within `built_through_seq`; that commit is only partly indexed. Consumers must not interpret the sequence alone as complete coverage of that commit. HTTP status serialization may omit a zero event index; the durable encoding has its own field-presence rules.

A single request operation can produce multiple change events. The event index is an offset within the commit's ordered events, not a count of request operations.

Disabling the index publishes a `disabled` manifest with no segment references. The operation does not synchronously delete the previous objects. An in-progress worker that encounters the disabled publication stops rather than republishing its earlier state.

## Building the index

### Initial backfill

Enablement creates an expiring user checkpoint at the namespace head, then publishes a backfilling manifest with that checkpoint ID, target sequence, and no inode cursor.

Each bounded build step enumerates files from the checkpoint in ascending inode order. It reads one current revision per visible file, applies the eligibility rule, extracts grams, and writes new delta segments. The segment set and last-consumed inode are published together in the next numbered grep manifest.

The final backfill step changes the lifecycle to `active` at the captured sequence and releases the checkpoint. Writes committed after that sequence are then processed through the change feed. They do not change the fixed target of the backfill already in progress.

If the checkpoint or necessary change history becomes unavailable, the worker abandons the incomplete build and begins another checkpointed backfill. It does not report the partial index as complete.

### Incremental builds

An active worker resumes the change feed from its sequence/event cursor. It indexes the eligible revisions published by those events and advances the cursor in the same numbered manifest publication that publishes their segment references.

Moves, deletes, and undeletes do not publish new content revisions in those events, so they do not require new postings. Their effect on visibility and paths is evaluated against metadata during queries. Operations that append a new revision are processed as revision events even when the underlying bytes were already stored.

The default build budget is 256 files or 64 MiB per step. Build batches are bounded, but the total cost of backfilling a namespace still depends on the eligible corpus. Failed attempts or a restarted backfill can read content again.

The index does not prevent the namespace's WAL retention floor from advancing. If incremental history required by the worker has been removed, the worker rebuilds from a new checkpoint.

## Maintenance scheduling

`GrepWorker` runs through the existing maintenance runner as a separate job from core metadata maintenance. Each invocation builds one bounded batch, or performs one reorganization step when there is no remaining build work for that invocation. The runner handles duplicate scheduling hints, concurrency, backoff, and periodic checks. A failure for one namespace does not require delaying unrelated namespaces.

The periodic probe discovers the current grep manifest. For an active index at a commit boundary, it also checks the change feed for another commit. Enabling the index schedules initial work; publications and queries that observe index lag can schedule more work.

A query-only server does not register the maintenance job and rejects index mutations. No grep operation enumerates all namespaces to discover work.

Embedded CLI profiles run a local maintenance scheduler and settle admitted work after mutations. `loonfs maintenance index enable` captures a target sequence and performs bounded passes until the index reaches it. Later namespace writes do not extend that target. `--no-wait` returns after enablement; `--max-steps` and `--deadline-ms` bound the wait and report incomplete progress as an error. Repeating the command can advance an existing index that has fallen behind.

For namespaces that may remain inactive, assign maintenance explicitly with `loonfs maintenance loop --namespaces <id>`. `--job grep-index` selects index maintenance, and `--drain` processes the current assignment and exits, subject to its step and deadline limits. A namespace without an enabled index returns `not_enabled` after the status read.

These assignments are separate from grep garbage collection. Build and reorganization do not automatically perform the explicit GC operation described below.

## Reorganization

The index uses three tiers: delta, mid, and base. Delta runs are merged into mid runs; accumulated mid runs are merged with the base into a new base.

The thresholds count logical runs, not physical segments. Every build publication allocates one `run_no` for its batch from `next_run_no`. A run can contain several segments without changing the reorganization count. Separate backfill batches remain separate runs even though they share the same captured target sequence.

With eight delta runs per mid run and eight mid runs per base rewrite, a simplified sequence of full batches produces a base rewrite about once per 64 build runs. A two-tier scheme rewriting the base after every eight delta runs would rewrite it eight times as often in that example. This is a constant-factor reduction; a fixed three-tier structure does not establish logarithmic cumulative write amplification as the corpus grows.

### Processing a reorganization in steps

A reorganization records the exact input segment set it will consume. It then scans the gram keyspace in bounded row-count steps, writing output segments at the selected tier and publishing progress after each step.

Until the scan completes, both inputs and completed outputs remain referenced and query-visible. This is safe for add-only postings because readers union the batches: repeated postings do not create missing candidates. It is not a general rule for replacing metadata rows with different retention semantics.

Segments published after the input snapshot was selected are excluded from that reorganization and remain referenced at completion. The final step removes the selected inputs and retains the completed outputs and unrelated segments.

The row budget is soft for equal keys. All rows with the same key are processed together before the cursor advances beyond that key. Splitting that group while recording an exclusive last-key cursor could skip its remaining rows on resume.

The manifest records the segment `level` (`0`, `1`, or `2`), `run_no`, and `next_run_no`. The in-progress reorganization records its `output_level`, fixed output `run_no`, selected inputs, outputs, and cursor. A replacement worker resumes from the last successfully published manifest.

### Old postings

The initial design does not remove postings solely because their revisions later become unobservable. Queries filter those candidates against current metadata before reporting results. Removing the postings requires a separate, correct liveness policy; it is not implied by routine segment merging.

This means the index can grow with historical revisions, not only the current visible files. The impact depends on the workload and should be measured rather than hidden by an index-size estimate based only on the current corpus.

## Queries

The serving deployment must advertise the query capability, and the namespace must have a usable grep index. A capability declaration is not evidence that backfill is complete for every namespace.

The endpoint is `GET /v0/namespaces/{ns}/grep`. Requests specify the pattern, case sensitivity, optional path-prefix scope, and optional cursor. `allow_scan` permits bounded scanning for a pattern that cannot be narrowed through grams. `allow_stale` permits indexed-only results when the unindexed tail cannot be scanned within the applicable budget.

Patterns use the Rust `regex` dialect, without backreferences or lookaround. Pattern syntax and the planner's interpretation must agree with the final verifier.

### Planning

The planner derives constraints that every match must satisfy. Its implementation represents the plan as an AND of sets of alternative grams: a candidate must satisfy every set by containing at least one of that set's grams.

For concatenated literal text, the required trigrams can be intersected directly. For alternatives, the constraints must preserve every matching branch. When a constraint cannot be established safely, the planner omits it. Fewer constraints increase the candidate set but do not omit valid matches.

ASCII-case-folded grams are useful for common case-insensitive patterns. A non-ASCII case alternative may not map to the same indexed bytes. The planner must weaken that constraint rather than assume the index implements Unicode case folding.

A valid pattern with no useful required grams, such as `.*` or a single character, is rejected as unindexable by default. `allow_scan` permits an explicitly bounded scan instead. Syntax errors and valid-but-unindexable patterns are different outcomes.

### Candidate selection and verification

Within a page, execution uses one pinned metadata view. For each required gram, it excludes disjoint segment ranges, checks bloom filters, and reads posting batches. Sorted intersections and unions produce candidate inode/revision pairs.

Candidates are then resolved in batches against the pinned metadata: visibility, current revision, and current path. A posting for an older revision is not a match against a file's current revision. Path-prefix filtering applies to the resolved path, not a path cached when the posting was created.

For each remaining candidate, the server reads the referenced content, verifies it, and runs the original pattern. Content reads use limited concurrency. The response contains line-oriented matches rather than a streaming file response.

A match identifies the inode, revision, derived absolute path, one-based line number, byte offset, and matching line. Long lines can be truncated to the configured cap, with `line_truncated` indicating that truncation. Matches are ordered by inode ID and byte offset.

### Pagination

A page is bounded by both its match limit and its verified-candidate budget. A pattern with many false-positive candidates must not trigger an unlimited content scan merely because it produces few matches.

The continuation records scan progress, including candidates that produced no matches, rather than only the last emitted match. It is bound to the result-selecting request fields: pattern, case flag, path scope, `allow_scan`, and `allow_stale`. Reusing it with different request semantics is rejected.

Each page reports the namespace `head_seq` used for that page. All metadata phases within the page share that view, but later pages can observe a newer head. The current grep request has no snapshot selector for keeping an entire multipage search at one durable snapshot. Namespace snapshot support elsewhere in the API does not imply that grep pagination already has that contract.

## Freshness and the unindexed tail

At a completed watermark, the index contains the postings needed for eligible revisions through that boundary. Revisions after it, up to the query's pinned head, are enumerated from the change feed and checked exhaustively with the same eligibility rule and verifier. A partly indexed commit must be treated according to its event cursor rather than assumed complete from its sequence alone.

For example, an index completed through sequence 100 can still return a current result at head 103 by considering the new revisions from commits 101 through 103. A metadata-only rename in that interval changes a result's path through the pinned metadata view without requiring a new content posting.

If the tail exceeds the query's budget, the default is a typed `index_lagging` error. With `allow_stale`, the server can return indexed-only results and report `tail_scanned: false`, together with the index and head positions. Stale results remain subject to visibility and content verification; they may omit eligible revisions that are not yet indexed.

Index maintenance reduces this gap when it runs. Core WAL-tail backpressure does not, by itself, bound grep lag: metadata can be flushed while grep maintenance remains behind. The query's tail budget and explicit stale-result option define the behavior when the gap is too large.

## Grep garbage collection

Grep GC is explicit and namespace-scoped. It is invoked through `loonfs maintenance index gc` or `POST /v0/maintenance/namespaces/{ns}/grep/index/gc`. Index building and reorganization do not run it implicitly.

Each call loads the current manifest, builds its live segment set, and scans the manifest and segment collections from beginning to end. It uses a fixed call clock and stores no progress cursor. Invalid or unreadable roots fail before deletion.

Manifest numbers at or above the observed hint remain for discovery. Earlier manifests require both their own and their immediate successor’s provider age to meet ordinary grace; an absent successor does not prevent deletion. Only the current manifest retains its listed segments, including pending reorganization inputs and outputs. An unreferenced segment must be strictly older than 24 hours.

Every output-producing step checks the bounded metadata publication budget before creating its next manifest. That budget and the collection age gates protect concurrent builds. A failed or interrupted step can leave segments for later collection.

For a missing or deleted core namespace, an explicit call can reap the entire grep prefix after ordinary grace. Core and grep collectors operate on separate object families. Neither collector resumes a durable GC run from a previous call.

## Costs and defaults

The main costs are reading content to build the index, reading and rewriting posting segments during reorganization, and reading candidate content during queries. Selectivity determines how much of the last cost can be avoided.

An eligible revision is read and tokenized during its build attempt. Retries and restarted backfills can repeat that work. Fixed build budgets limit an invocation, not the total work required to index an arbitrarily large namespace.

The following distinctions matter for compatibility and tuning:

| Category | Rules or settings |
| --- | --- |
| Versioned search semantics | ASCII-folded byte trigrams, eligibility cap and sample rule, posting keys, packed posting encoding. |
| Build defaults | 256 files or 64 MiB per step. |
| Reorganization defaults | Eight delta runs and eight mid runs per trigger, with a bounded step row target. |
| Posting writer target | Approximately 256 postings per row. |
| Serving limits | Match count, candidate verification, matching-line length, content-read concurrency, and unindexed-tail budgets. |

Writer targets and serving budgets are not alternate interpretations of stored postings. Changes to tokenization or eligibility require a compatible version and rebuild strategy after release. Rebuildability permits replacement of derived state; it does not make a large rebuild inexpensive.

## Deferred work

Resource-type attributes could provide explicit text/binary eligibility overrides, but that behavior is not implemented by the current rule. Dead-posting reclamation also remains separate from ordinary compaction and requires a valid revision-liveness policy.

Dynamic size-tiered leveling is an option if large-corpus measurements show that the fixed three-tier layout still causes excessive rewriting. The presence of a numeric `level` field is not sufficient evidence that arbitrary future levels are compatible with existing validation and publication rules. Compatibility must be established before describing that change as writer policy alone.

Variable-length grams could improve selectivity by choosing boundaries using corpus frequency, but would require new tokenization semantics and supporting statistics. A richer full-text index could reuse the extension keyspace, segment machinery, and change-feed-driven maintenance without being presented as an existing grep feature.

A stable snapshot across all pages of a grep query is also deferred. The current per-page snapshot contract should remain explicit until the request and retention protocols support a longer-lived query view.
