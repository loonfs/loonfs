# Content collection

Collection deletes a content object in an active namespace once no retained view names it and the object is older than the reclamation grace `T`. The ordinary collection pass does this, with its one fixed clock and no durable progress. The storage format defines the rule in [section 11.9](../specs/format.md#119-content-roots). This note explains its shape.

## Why content needs a rule

A base compaction keeps each file's revisions above the retention floor and its newest revision at or below it ([format section 10.3](../specs/format.md#103-row-retention-during-a-base-compaction)). The objects that the dropped revisions named stay in the store. No current view reads them, and nothing else would delete them.

## The rule

A content object owned by namespace `N` stays while a layout names that object or a tail or session root names its chain:

1. An extent owned by `N` in a layout row of a rooted view: `N`'s current manifest, a pinned manifest, or a manifest whose immediate successor is younger than `T` by its listed modification time. A successor listed without a time counts as young. An absent successor does not make the older manifest a root. A view whose layout segments are already covered adds no scan.
2. A revision delta in `N`'s unfolded WAL tail, or an extent owned by `N` in the tail row state's layouts.
3. An upload session record in `N`, whatever its status.

The pass deletes every other object under `namespaces/N/content/` whose provider age is at least `T`. It keeps younger objects and keys the layout does not recognize.

One rule covers every way content becomes garbage. A completed session's cleanup removes only its record, after the content grace, and a later pass collects the object if nothing published it. An aborted session's cleanup deletes its object at once, and the pass collects anything a late write leaves after the record is gone. A retired namespace is the same rule with no roots and no age gate: the retirement sweep deletes its whole content prefix. A deleted namespace that has not retired keeps its content.

Layout roots protect exact object keys. A layout can name a shared base in another id shard; a separate shared-base set protects that key throughout the pass. Tail and session ids protect both the whole object and every span of their chains. An old span that no layout, tail, or session names can be collected even while its chain remains live.

## Why the roots are enough

A reader or writer works from a manifest that is current, pinned, or superseded for less than `T` ([format section 11.2](../specs/format.md#112-reference-roots)), together with the WAL tail. The first two roots therefore cover every view that a read or a copy can start from. The minimum grace also outlasts a direct download capability issued from such a view ([format section 11.4](../specs/format.md#114-clock-and-operation-assumptions)).

A fork's source pin roots its inherited revisions and layouts in the owner's namespace.

The pass lists upload sessions before it reads the WAL tail and scans layout rows after loading the roots. A session record is removed only after any admission evidence it could issue has expired. A session the pass does not list was either created after the listing or removed before it. Content of a session created later is younger than `T`. Every commit that names content of a removed session landed before the tail was read, so the tail or a rooted manifest names that content. In the other order, a commit of an upload's content could land after the tail read, and a concurrent collector could then remove the session record before the listing.

## Costs

- Two scans of the layout family per distinct layout view. The first collects shared bases. The second scans disjoint id ranges, one shard at a time. A fork's scans also read inherited segments.
- One content listing per shard. The current manifest's layout segment row counts and `content_shard_rows` determine the shard count. The hex prefix width is the smallest `k` from 0 through 4 for which `rows / 16^k <= content_shard_rows`. There are `16^k` shards, visited in order. The default target is 65,536 rows per shard. Uneven id distribution can put more rows in one shard.
- One listing of upload sessions and one read of each record before the tail, besides the session sweep's own listing and reads.
- No HEAD requests during the sweep. Discovery keeps its HEAD probe for the current manifest's successor. Candidate ages and successor ages come from listing timestamps. Missing timestamps retain objects.
- Memory for one shard's object keys, the shared-base keys, and the tail and session ids. All three sets are charged to the read working memory alongside scan blocks. A set that exceeds that budget fails the pass. The shard set is released before the next shard.

A deleted namespace pays none of these content-root costs.
