# Content collection

Garbage collection deletes a content object in an active namespace once no retained view refers to it, and the object is older than the reclamation grace `T`. The ordinary collection pass does this, with one clock value for the whole pass and no durable progress. [Content roots](../specs/format.md#119-content-roots) in the storage format defines the rule, and this note explains its design.

## Why content needs a rule

A base compaction keeps each file's revisions above the retention floor, and its newest revision at or below the floor (see [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction)). The objects that the dropped revisions referred to stay in the store. No current view reads them, and nothing else would delete them.

## The rule

A content object owned by namespace `N` is kept while a layout refers to that object, or a tail or session root refers to its chain:

1. **Layout views.** An extent owned by `N`, in a layout row of a rooted view. A rooted view is `N`'s current manifest, a pinned manifest, or a manifest whose immediate successor is younger than `T`, according to the successor's modification time in the listing. A successor listed without a time counts as young. A missing successor doesn't make the older manifest a root. The pass scans each layout segment that a rooted view lists once, however many views list it.
2. **WAL tail.** A revision delta in `N`'s unfolded WAL tail, or an extent owned by `N` in the layouts of the tail's row state.
3. **Upload sessions.** An upload session record in `N`, whatever its status.

The pass considers every other object under `namespaces/N/content/` for deletion once its provider age is at least `T`. It keeps objects that are younger than that, keys that don't parse as content object keys, and false positives from the shared base filter.

The same rule covers every way that content becomes garbage:

- **Completed session.** Cleanup removes the session's record, but not its object, after the content grace. A later pass collects the object if nothing published it.
- **Aborted session.** Cleanup deletes the session's object right away, and the pass collects anything that a late write leaves after the record is gone.
- **Retired namespace.** The same rule applies with no roots and no age check, so the retirement sweep deletes the namespace's whole content prefix.

A deleted namespace that hasn't been retired keeps its content.

Within each shard, layout roots protect exact object keys. A layout can refer to a shared base in another ID shard, so a Bloom filter over shared object keys protects those bases for the whole pass. A false positive keeps an unreferenced object for that pass, and every inserted key stays protected. The `now_ms` supplied to the pass seeds the filter, so a pass with a different clock uses a different seed. Tail and session IDs protect both the whole object and every span of their chains. An old span that no layout, tail, or session refers to can be collected even while its chain is still live.

## Why the roots are enough

A reader or writer works from a manifest that's current, pinned, or superseded for less than `T` (see [Reference roots](../specs/format.md#112-reference-roots)), together with the WAL tail. So the first two roots cover every view that a read or a copy can start from. The minimum grace is also longer than the lifetime of a direct download capability issued from such a view (see [Clock and operation assumptions](../specs/format.md#114-clock-and-operation-assumptions)).

A fork's source pin protects the fork's inherited revisions and layouts in the owner's namespace.

The pass lists upload sessions before it reads the WAL tail, and it scans layout rows after it loads the roots. A session record is removed only after any admission evidence it could issue has expired. A session that the pass doesn't list was either created after the listing or removed before it:

- The content of a session created later is younger than `T`.
- Every commit that refers to the content of a removed session succeeded before the tail was read, so the tail or a rooted manifest refers to that content.

Why list sessions before reading the tail? In the other order, a commit that uses an upload's content could succeed after the pass reads the tail, and a concurrent collector could then remove the session record before the pass lists sessions.

Why scan segments instead of views? A view's layout rows are the rows of its layout segments, so scanning each distinct segment reads every row of every view, and a segment that many views list is read once. The rows include a superseded layout row (a shorter layout of a chain that a longer row replaces), so an object that only a superseded row refers to stays until a compaction drops that row and no rooted view lists the old segment.

## Costs

Content roots add these costs to a collection pass:

- Two scans of the rows of each distinct layout segment that a rooted view lists, so the scan work grows with the distinct layout rows and not with the number of views. The first scan inserts shared base object keys into the Bloom filter, and the second scans disjoint ID ranges, one shard at a time. A fork's scans also read inherited segments.
- One content listing for each shard. The row counts of the current manifest's layout segments, and `content_shard_rows`, determine the shard count. The hex prefix width is the smallest `k` from 0 through 4 for which `rows / 16^k <= content_shard_rows`, and the pass visits the `16^k` shards in order. The default target is 65,536 rows per shard. An uneven distribution of IDs can put more rows in one shard.
- One listing of the uploads prefix, before the tail is read. Each session has a session record, and an ended session also has an outcome record whose key sorts directly after the session record's key, so the listing shows which sessions have ended. An ended session has two keys, and a completed session stays listed for the content reclamation grace (about seven days), so the listing can have up to twice as many keys as there are sessions. The pass reads the session record of each open session, and both records of each aborted session (up to 16 reads at a time). An outcome record can't change, so LoonFS reads a completed session's outcome record once per process. The runtime keeps each completed session's content ID and completion time between passes, in a cache with its own byte limit (32 MiB by default). If the cache drops a session, the next pass reads its outcome record again. The session sweep lists the uploads prefix again, but it reads only the records of sessions that weren't completed at the first read.
- No HEAD requests during the sweep, except one each time the pass aborts an expired upload session, to check that the session record still exists (see [Upload sessions](../specs/format.md#51-upload-sessions)). (Discovery still sends its HEAD probe for the current manifest's successor.) Candidate ages and successor ages come from listing timestamps, and the pass keeps an object whose timestamp is missing.
- Read working memory for the manifest and segment keys that the rooted manifests protect, the rooted manifests (the pass loads up to 16 at a time), the list of distinct layout segments, one shard's exact object keys, the tail and session IDs, and the blocks of up to 16 open segments while they load. If a set exceeds that budget, the pass fails. The pass releases a shard's set before it moves to the next shard.
- A shared base Bloom filter, with its own 64 MiB byte cap that's separate from read working memory. The filter follows these rules:
  - It uses the same xxh64 double hashing and seven probes as layout compaction.
  - Its initial expectation is the current manifest's layout row count, with a minimum of one. When an insertion would exceed a level's expectation, the filter adds a level with twice that expectation and a seed derived from the pass seed and the level index. A key is protected when any level matches.
  - Each level is sized at ten bits per expected entry. A filter with `n` levels has roughly `n` times the false-positive rate of one level.
  - The byte cap applies to the sum of all levels. If the initial level or a new level would exceed the cap, the pass logs the limit and skips the active namespace's content sweep, and no content listing runs. (An initial allocation over the cap skips the shared base scan, and growth over the cap stops it.) Other families are still swept.

A deleted namespace has none of these content-root costs.
