# Content collection

Garbage collection deletes a content object once no retained view in its fork family refers to it, and the object is older than the reclamation grace `T`. The ordinary collection pass on the family root does this, with one clock value for the whole pass and no durable progress. [Content roots](../specs/format.md#119-content-roots) in the storage format defines the rule, and this note explains its design. [Fork family content](fork-family-content.md) explains why a family's content is stored under one prefix.

## Why content needs a rule

A base compaction keeps each file's revisions above the retention floor, and its newest revision at or below the floor (see [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction)). The objects that the dropped revisions referred to stay in the store. No current view reads them, and nothing else would delete them.

## The rule

Every member of a fork family writes its content under the family root's prefix, so the pass on the family root `R` keeps a content object while a layout in any member refers to that object, or a tail or session root of any member refers to its chain:

1. **Layout views.** An extent in a layout row of a member's rooted view. A rooted view is the member's current manifest (active or deleted), a pinned manifest, or a manifest whose immediate successor is younger than `T`, according to the successor's modification time in the listing. A successor listed without a time counts as young. A missing successor doesn't make the older manifest a root. The pass scans each layout segment that a rooted view lists once, however many views and members list it.
2. **WAL tail.** A revision delta in a member's unfolded WAL tail, or an extent in the layouts of the tail's row state.
3. **Upload sessions.** An upload session record under `R`'s prefix, whatever its status.

The pass considers every other object under `namespaces/R/content/` for deletion once its provider age is at least `T`. It keeps objects that are younger than that, keys that don't parse as content object keys, and false positives from the shared base filter.

The members are the root and every namespace forked from a member. The pass finds them through fork pins, because each fork pin under a member's prefix identifies the fork that reads through it, and it enumerates them before it reads any view. A pass on a fork sweeps no content.

The same rule covers every way that content becomes garbage:

- **Completed session.** Cleanup removes the session's record, but not its object, after the content grace. A later pass collects the object if nothing published it.
- **Aborted session.** Cleanup deletes the session's object right away, and the pass collects anything that a late write leaves after the record is gone.
- **Retired fork.** A fork stores nothing under its own content prefix, so its retirement deletes no content. The objects it wrote stay while another member's view refers to them, and the rest become unreferenced candidates.
- **Retired family root.** Every fork of the root is retired first, because each fork pins its source. The same rule then applies with no roots and no age check, so the retirement sweep deletes the family's whole content prefix.

A deleted namespace that hasn't been retired keeps its tombstone as a rooted view, so the content it refers to stays until it's retired.

Within each shard, layout roots protect exact object keys. A layout can refer to a shared base in another ID shard, so a Bloom filter over shared object keys protects those bases for the whole pass. A false positive keeps an unreferenced object for that pass, and every inserted key stays protected. The `now_ms` supplied to the pass seeds the filter, so a pass with a different clock uses a different seed. Tail and session IDs protect both the whole object and every span of their chains. An old span that no layout, tail, or session refers to can be collected even while its chain is still live.

## Why the roots are enough

A reader or writer in any member works from a manifest that's current, pinned, or superseded for less than `T` (see [Reference roots](../specs/format.md#112-reference-roots)), together with that member's WAL tail. So the first two roots cover every view that a read or a copy can start from. The minimum grace is also longer than the lifetime of a direct download capability issued from such a view (see [Clock and operation assumptions](../specs/format.md#114-clock-and-operation-assumptions)).

A fork's source pin keeps the pinned source manifest as a rooted view of the source, so the fork's inherited revisions and layouts are protected through that view.

The pass enumerates the members before it lists sessions, lists sessions before it reads any tail, and scans layout rows after it loads the roots. A session record is removed only after any admission evidence it could issue has expired. A session that the pass doesn't list was either created after the listing or removed before it:

- The content of a session created later is younger than `T`.
- Every commit that refers to the content of a removed session succeeded before the tail was read, so the tail or a rooted manifest refers to that content.

A member that's retired after the enumeration has a missing manifest when the pass reads it, and the pass skips it. Retirement deletes the member's pin, so the member leaves the family at that point, and every object it wrote is referenced by a view the pass reads later, or by nothing. A member whose manifest is invalid stops the family's content sweep.

Why list sessions before reading the tail? In the other order, a commit that uses an upload's content could succeed after the pass reads the tail, and a concurrent collector could then remove the session record before the pass lists sessions.

Why scan segments instead of views? A view's layout rows are the rows of its layout segments, so scanning each distinct segment reads every row of every view, and a segment that many views list is read once. The rows include a superseded layout row (a shorter layout of a chain that a longer row replaces), so an object that only a superseded row refers to stays until a compaction drops that row and no rooted view lists the old segment.

Why does one pass gather every member's roots, instead of each member sweeping its own content? Every member can reference every object under `R`'s prefix, so a member alone can't tell whether an object it doesn't reference is garbage. The root's pass sees every root of every object it can delete.

## Costs

Content roots add these costs to a collection pass on a family root:

- One manifest discovery, one pin listing, one manifest listing, and one WAL tail read for each member of the family. A fork leaves the family when it's retired.
- Two scans of the rows of each distinct layout segment that a rooted view of any member lists, so the scan work grows with the distinct layout rows and not with the number of views. The first scan inserts shared base object keys into the Bloom filter, and the second scans disjoint ID ranges, one shard at a time. A fork's inherited segments are its source's, so each member adds the scan of its own runs.
- One content listing for each shard. The row counts of the members' current manifests' layout segments, and `content_shard_rows`, determine the shard count. The hex prefix width is the smallest `k` from 0 through 4 for which `rows / 16^k <= content_shard_rows`, and the pass visits the `16^k` shards in order. The default target is 65,536 rows per shard. An uneven distribution of IDs can put more rows in one shard.
- One listing of the root's uploads prefix, before any tail is read. The prefix contains every member's sessions. Each session has a session record, and an ended session also has an outcome record whose key sorts directly after the session record's key, so the listing shows which sessions have ended. An ended session has two keys, and a completed session stays listed for the content reclamation grace (about seven days), so the listing can have up to twice as many keys as there are sessions. The pass reads the session record of each open session, and both records of each aborted session (up to 16 reads at a time). An outcome record can't change, so LoonFS reads a completed session's outcome record once per process. The runtime keeps each completed session's content ID and completion time between passes, in a cache with its own byte limit (32 MiB by default). If the cache drops a session, the next pass reads its outcome record again. The session sweep lists the uploads prefix again, but it reads only the records of sessions that weren't completed at the first read.
- No HEAD requests during the sweep, except one each time the pass aborts an expired upload session, to check that the session record still exists (see [Upload sessions](../specs/format.md#51-upload-sessions)). (Discovery still sends its HEAD probe for each member's current manifest's successor.) Candidate ages and successor ages come from listing timestamps, and the pass keeps an object whose timestamp is missing.
- Read working memory for the manifest and segment keys that the rooted manifests protect, the rooted manifests (the pass loads up to 16 at a time), the list of distinct layout segments, one shard's exact object keys, the tail and session IDs, and the blocks of up to 16 open segments while they load. If a set exceeds that budget, the pass fails. The pass releases a shard's set before it moves to the next shard.
- A shared base Bloom filter, with its own 64 MiB byte cap that's separate from read working memory. The filter follows these rules:
  - It uses the same xxh64 double hashing and seven probes as layout compaction.
  - Its initial expectation is the sum of the members' current manifests' layout row counts, with a minimum of one. When an insertion would exceed a level's expectation, the filter adds a level with twice that expectation and a seed derived from the pass seed and the level index. A key is protected when any level matches.
  - Each level is sized at ten bits per expected entry. A filter with `n` levels has roughly `n` times the false-positive rate of one level.
  - The byte cap applies to the sum of all levels. If the initial level or a new level would exceed the cap, the pass logs the limit and skips the family's content sweep, and no content listing runs. (An initial allocation over the cap skips the shared base scan, and growth over the cap stops it.) Other families are still swept.

A pass on a fork has none of these content-root costs. A deleted family root's pass keeps them while forks of it exist, because those forks keep writing under its prefix.
