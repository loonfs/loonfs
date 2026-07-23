# Content Search Index

LoonFS can answer grep-style regular-expression queries over file
content from a derived index that event-driven per-namespace maintenance
builds and folds independently of metadata. This document describes what
the index stores, how it is built and reclaimed, how a query runs,
and why the pieces are shaped that way. The index is derived work
in the sense of `docs/specs/format.md` section 6.6: rebuildable
from authoritative state, contained in the grep-owned extension keyspace,
and invisible to any reader that does not understand it.

## The approach

A regular expression cannot be served from an inverted index
directly, but almost every useful pattern requires some literal
bytes to appear in any matching file. The classic code-search
design (Russ Cox, "Regular Expression Matching with a Trigram
Index", 2012) exploits that: index every three-byte substring — a
gram — of every file, translate the query into grams a match must
contain, intersect those posting lists into a candidate set, and
run the real regular expression over each candidate's content.

```
pattern -> required grams -> posting intersection -> candidates
        -> read content, verify with the real pattern -> matches
```

Verification carries half the correctness burden: it removes
false positives, so a candidate set may be arbitrarily loose
without changing answers. The other half is a completeness
invariant that every builder must uphold: every eligible revision
at or below the index watermark has complete postings, under the
same eligibility rule queries apply. Verification cannot repair a
missing candidate; a stale tail is exact only because revisions
past the watermark are scanned exhaustively. Every choice below
leans on those two properties together.

## What gets indexed

Eligibility is decided when a revision is indexed, not when it is
uploaded, so the write path and the wire are untouched. A revision
is eligible when:

- its content is at or below the version's size cap (8 MiB), and
- a sample of its first 8 KiB contains no NUL byte and is valid
  UTF-8 — the binary-detection heuristic grep-family tools use.

The whole rule — cap and sniff — is a format constant of feature
version 1, not a tunable: queries decide the exhaustive-scan and
verification cut with the same rule the builder used, and a
divergence would turn into silent false negatives once the
watermark passes a file one side considers eligible and the other
does not. Loosening the rule is a feature-version bump and a
rebuild.

Ineligible files are simply absent from the index, and the query
path applies the same rule to unindexed data, so the contract is
uniform: search matches text files. There is no extension list;
sniffing covers extensionless text (README, Makefile, LICENSE)
and misnamed binaries alike.

Format section 8 reserves optional resource properties, including
a resource-type hint that lives on the inode and moves with
renames. When that lands, the hint becomes an override — a
resource marked text is indexed past the sniff, one marked binary
is skipped — but the index deliberately does not wait for it.

## Postings

The tokenizer for feature version 1 is fixed: every overlapping
three-byte window of the content, after folding ASCII letters to
lower case. Grams are bytes, not characters, so the index is
encoding-agnostic; case folding buys cheap case-insensitive
search for the common case (see "Queries" for the non-ASCII
caveat).

Postings are ordinary sorted rows in ordinary segments:

```
row key:  gram-{gram as hex}-{first inode id, zero padded}
payload:  a packed batch of (inode id, revision number) pairs,
          delta-coded, about 256 postings per row
```

A posting names an inode id and revision number, never a path:
inode identity is durable across renames, and forks preserve it,
so postings survive both. Paths are derived at query time from
the child-binding index, the same way other inode-first reads
present results.

Batching postings into one row per key keeps the index within a
small factor of a dedicated posting-list format while staying
plain rows: the merge machinery, block builder, and iteration
model the metadata families already use apply unchanged. Rows for
one gram sort together, and batches from different runs for the
same gram are unioned by readers, so merges never need to combine
payloads to stay correct.

## Segments, manifests, and the grep root pointer

Index segments reuse the metadata segment layout described in
`metadata-block-storage.md` — prefix-compressed keys,
independently compressed and checksummed blocks, an index block,
and a bloom filter block — with one twist: the per-row filter key
is the gram prefix of the row key. The existing filter machinery
therefore answers "this segment definitely contains no postings
for this gram", which is exactly the pruning a query wants. The
block builder is generalized over the row payload to make this
possible; metadata families keep byte-identical output.

Segments live under
`namespaces/{namespace}/extensions/grep/segments/`. The small mutable
`extensions/grep/root.json` pointer names an immutable, content-derived
manifest under `extensions/grep/manifests/`; that manifest records the
query-visible segments, `built_through_seq`, lifecycle, run-ordinal
allocation, and any in-progress fold snapshot, outputs, and cursor. The
pointer and manifests are independent of the namespace manifest, so core
never has to understand grep state to read the filesystem.

Publication writes the immutable manifest first and installs the pointer by
one etag CAS. A CAS loser's manifest and segments are unreachable derived
garbage for grep GC. Maintenance is created by namespace events, never by
store discovery, and no grep path enumerates namespaces.

Disabling writes a manifest with lifecycle `disabled` and no segment
references, then CAS-publishes its pointer. The old objects become candidates for grep-owned collection;
the disable call never deletes them synchronously. Grep GC retains every
object named by a verified live root and deletes unreferenced grep objects
only after its grace window. A fork does not copy a grep root, so it begins
unmaterialized and can be enabled independently.

## Building

`GrepWorker` is driven through explicit bounded steps, independently of
core metadata maintenance. Enablement first creates an expiring user
checkpoint at the namespace head, then CAS-publishes a backfilling root
that records the checkpoint id, target sequence, and empty cursor. Build
steps walk that checkpoint's immutable manifest, read eligible revision
content, write delta segments, and publish the cursor and segment set in
one root CAS. The completing step changes the lifecycle to `steady` and
releases the checkpoint.

One per-namespace driver owns those steps. Starting a driver immediately
runs backfill and incremental catch-up continuously, yielding between bounded
steps. Once the root is steady and its watermark reaches the namespace head,
the driver parks without a timer. A nudge received while active coalesces into
one more catch-up run; a nudge received while parked wakes it. Step failures
back off exponentially inside only that namespace's driver, capped at one
second, so a poisoned root cannot delay a sibling namespace.

In server `embedded` mode, enable and re-enable start the namespace driver,
disable stops it, and successful runtime publications send a non-blocking
nudge only to an already-running driver. A grep query also starts or nudges a
driver when the root it inspects is backfilling or its watermark trails the
head. That first-touch path resumes durable work after a restart without a
scan. `serve_only` serves the same queries and administration operations but
starts no drivers.

Detached maintenance is explicitly assigned with repeatable
`loonfs-grep --namespace <id>` flags. `--once` catches up only those namespaces
and exits. The long-running form polls each assigned namespace's own head at
`poll_interval_ms` (default 1000, zero rejected), the manifest-poll analog for
this derived index. It never lists a namespace prefix.

Steady-state build steps replay the validated change feed after
`built_through_seq`, collect the file revisions that appeared, read each
eligible revision's content, extract grams, and write new delta-level
segments. Work per step is budgeted by files and bytes (defaults 256 files
or 64 MiB), and every watermark advance shares one root CAS with the
segment set that implements it.

Two rules keep the cycle honest:

- **Index building never rides core maintenance.** Metadata ticks flush and
  reorganize core state only. Grep build and fold steps are scheduled by the
  grep worker, and freshness between worker steps is the query path's job.
- **Retention may outrun the index.** The independent worker does not hold
  the core WAL floor behind `built_through_seq`. If the change feed reports
  that retention removed required history, the worker starts a fresh
  checkpointed backfill instead of guessing across the gap.

When delta runs accumulate past the same threshold shape the
metadata families use, a fold consumes them. A gram base can grow
far past what any metadata family reaches, so index folds differ
from reorganization in two ways.

They are tiered: delta runs fold into a mid run, and only
accumulated mid runs fold — together with the base — into a
fresh base, so the whole-index rewrite happens once per
`max_l0_runs x max_mid_runs` build runs (about 64 with the
defaults) instead of once per delta threshold. That is a
constant-factor amortization of base rewrites, not logarithmic
cumulative write amplification: the level count is fixed at
three, and logarithmic amplification needs a level count that
grows with corpus size. The constant factor is expected to
suffice at the corpus sizes v1 targets; if large-corpus evidence
says otherwise, the documented next step is dynamic (size-tiered)
leveling, where levels are added as the corpus grows (see
"Deferred, with intent").

Fold triggers count logical runs, never physical segments. Every
publish that creates gram segments — a WAL or backfill build
unit, a delta fold's outputs, a base fold's outputs — stamps one
run ordinal on the whole batch, allocated from a counter in the
grep manifest and incremented in the same pointer publication,
so allocation is atomic with the root swap. The per-segment row
cap can therefore split a run into any number of segments without
changing fold cadence, and backfill units — which all carry the
unchanged enable-time watermark as their `run_seq` — still count
as distinct runs.

And they are partitioned rather than whole-family: a fold
snapshots the segment set it will consume, then walks the gram
keyspace in bounded row-count steps, each step merging one key
range from the snapshot into fresh segments at the fold's output
tier and CAS-publishing a root that records the outputs and resume
cursor.
Until the walk completes, snapshot inputs and outputs are
both referenced and both served — postings are add-only, so
readers that union them see duplicates, never gaps — and segments
that arrive during the fold stay out of the snapshot and survive
it. The completing step swaps the snapshot out for the outputs; a
fold interrupted anywhere resumes from the cursor the last
published manifest carries. The step's row budget is soft: rows
with equal keys are consumed as one atomic group, because the
resume cursor is the last merged key plus a terminator and
splitting the group would strand its tail behind the cursor.

The tiering and run identity are durable writer-side bookkeeping,
invisible to reads:

- Descriptor `level` in the grep manifest's segment list: `0` for the
  delta segments build units write, `1` for a delta fold's mid
  runs, and `2` for the base.
- Descriptor `run_ordinal`: the batch-wide run identity described
  above.
- Root index-state `next_run_ordinal`: the allocation counter.
- Fold-state `output_level`: the tier the in-flight fold's outputs
  are stamped with (`1` or `2`).
- Fold-state `run_ordinal`: the ordinal stamped on every output
  segment of the fold, fixed when the fold starts so a resumed
  fold keeps its identity.

Enabling the index on a namespace that already has data starts a
backfill: the worker creates an expiring user checkpoint and a root cursor
walks the checkpoint manifest's revisions family in key order across steps,
indexing as it goes. While lifecycle is `backfilling` the index is not yet
materialized and queries are refused with the feature named; when the walk
completes, lifecycle becomes `steady`, the checkpoint is released, and the
watermark takes over. If the checkpoint expires or vanishes, the worker
starts again from a fresh checkpoint.

Grep garbage collection is also explicit and per namespace. The standalone
binary runs it only when passed `--gc`; the server exposes
`POST /v0/admin/namespaces/{ns}/index/grams/gc`, a sibling of core's explicit
per-namespace GC endpoint. Pointing that operation at an absent or tombstoned
namespace reaps its aged `extensions/grep/` state. The core namespace head's
deleted tombstone is already the absorbing gate: enable refuses it, so pointer
deletion under a reverified tombstone needs no grep-specific condemned state.
Driver activity never runs GC.

Grep manifests are content-addressed, so two identical builders derive the
same manifest id. A worker that observes its create-if-absent as already
present can race GC deleting that unreachable candidate before the worker's
pointer CAS. Every successful pointer advance therefore HEADs its manifest
after the CAS and, if absent, re-puts the still-buffered bytes with
create-if-absent before returning. This verify-and-heal step closes the race
without adding mutable state to an immutable family.

Postings for revisions that later become unobservable are not
dropped in the first version. They are harmless — verification
and the visibility filter already ignore them — and dropping them
needs the same liveness reasoning as tombstone collapse, which is
documented future work for the metadata families too. The two
should land together.

## Queries

Search is the first resident of the reserved `query/v0` profile
(`docs/specs/api.md`), and a query needs both halves of the
capability contract: the deployment advertises the serving
feature, and the namespace's verified grep root shows the index
materialized for the data being served. Missing either half is
the existing `not_supported` response with the feature named.

The endpoint is `POST /v0/namespaces/{ns}/query/grep`. The
request carries the pattern, a case-insensitivity flag, an
optional path prefix to scope the search, a page cursor, and
limits. The pattern dialect is the Rust `regex` crate's — no
backreferences, no lookaround — which is what makes gram planning
sound.

Planning follows the classic analysis: walk the parsed pattern,
track the exact strings, prefixes, and suffixes each
subexpression can match, and emit an AND/OR tree of grams any
match must contain. A pattern that yields no required grams
(`.*`, single characters, very wide alternations) is rejected
with a typed error rather than silently scanning everything; a
capped scan mode behind an explicit request flag is the escape
hatch for small namespaces. Case-folded grams make
case-insensitive queries cheap, with one caveat: for required
literals whose case pairs fold outside ASCII, the planner must
weaken the requirement (fewer required grams, more candidates)
rather than emit grams the index will not contain — slower,
still correct.

Execution, in the read order segments are built for:

1. Prune index segments per gram by descriptor key range, then
   by bloom filter.
2. Stream posting batches for each required gram; postings
   within a gram arrive in inode order, so AND and OR nodes are
   sorted stream intersections and unions.
3. Filter candidates to the pinned snapshot: the newest visible
   revision of each live inode (point lookups on the
   revisions-by-inode index plus the standard visibility walk),
   and the path-prefix scope via child-binding ancestry.
4. Read each surviving candidate's content — server-side,
   verified whole-object reads, a small fixed fan-out — and run
   the real pattern, emitting line-oriented matches.

A match reports inode id, revision number, derived absolute
path, line number, byte offset, and the matching line (truncated
to a cap). Results return as pages in the existing pagination
idiom. Two budgets bound a page: the match limit, and a
verified-candidate budget, so a selective-looking pattern with
many false positives cannot read the whole corpus for one page.
The cursor therefore resumes strictly after the last candidate
the previous page finished scanning — not only after emitted
matches — and it carries a fingerprint of the request (pattern,
flags, scope) so a cursor replayed under a different request is
rejected instead of silently skipping results. Each page is
evaluated against the namespace head at page time and reports
that head in the response; pinning every page of one search to a
single snapshot needs read anchors that outlive a request and is
deliberately future work. There is no streaming response; pages
keep the wire boring and compose with the client helpers that
already exist.

**Freshness.** The index trails the head by design, so a query at
a pinned sequence is answered in two parts: the index covers
commits at or below `built_through_seq`, and the revisions that
landed in between — enumerable from the same WAL-tail replay
every read already performs — are scanned exhaustively with the
same eligibility rule and the same verifier. Steady-state
maintenance keeps that tail small, and write backpressure bounds
it outright. If maintenance has been off long enough that the gap
exceeds the query's tail budget, the query fails with a typed
"index lagging" error; an explicit request flag accepts
indexed-only results instead, with the staleness reported in the
response. Exact by default, stale only by consent.

## Costs

The numbers that matter, to be validated in the lab before any
default-on decision:

- **Index size.** Unique grams per file run roughly a third of
  file bytes for text; delta-coded batches under zstd land the
  index around 15-40% of eligible content bytes. Comparable
  published systems sit near the low end of that range.
- **Build cost.** Each eligible revision is read once, linearly
  tokenized, and sorted; the object reads dominate. Budgets make
  this a per-step constant, and tiered folds amortize the
  whole-index rewrite by a constant factor — about 64x rarer
  than the delta threshold with the defaults (see "Building").
- **Query cost.** Posting reads touch a handful of blocks per
  gram after pruning. Candidate content reads dominate end-to-end
  latency, which is why the planner works to keep candidate sets
  small and page limits cap the fan-out per request.

## Constants and tunables

Following the segment-format convention, two different contracts:

- **Format constants.** The tokenizer (ASCII-case-folded byte
  trigrams), the eligibility rule (the 8 MiB cap and the text
  sniff), the posting row key shape, and the batch encoding are
  pinned by the grep pointer/manifest and segment codec versions. Changing any
  of them is a format-version bump and a rebuild — cheap by
  construction, since the index is derived work.
- **Writer-side defaults.** The per-step build budgets (256
  files or 64 MiB), the fold run thresholds (eight delta runs,
  eight mid runs), the fold step's row budget, the posting
  batch target (about 256), page limits, the verified-candidate
  budget, and the query tail budget are writer- or server-side
  tunables; readers take what the grep manifest and descriptors
  describe.

## Deferred, with intent

- **Resource-type hints** (format section 8) as an eligibility
  override, once resource properties exist at all.
- **Dead-posting reclamation**, together with tombstone collapse.
- **Dynamic (size-tiered) leveling.** The fixed delta/mid/base
  tiers amortize base rewrites by a constant factor; if lab
  evidence at large corpora shows cumulative write amplification
  still dominating, the next step is a level count that grows
  with corpus size — true logarithmic amplification. The level
  and run-ordinal fields are already per-segment, so adding
  levels is writer-side policy, not a format change.
- **Variable-length grams.** Choosing gram boundaries by corpus
  rarity instead of a fixed width shrinks posting lists for
  common substrings and sharpens selectivity; it needs a
  frequency table derived from real corpora, and it is exactly
  the kind of change the feature version exists for.
- **Richer text queries.** A tokenized full-text index would be a
  sibling feature, but every seam here — the query profile, the
  extension keyspace, the derived segment family, the WAL-driven build
  tick — is the seam it would reuse.
