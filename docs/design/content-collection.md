# Content collection

Garbage collection deletes a content object once no retained view in its fork family refers to it, and the object is older than the reclamation grace `T`. The ordinary collection pass on the family root does this, with one clock value for the whole pass and no durable progress. [Content roots](../specs/format.md#119-content-roots) in the storage format defines the rule, and this note explains its design. [Fork family content](fork-family-content.md) explains why a family's content is stored under one prefix.

## Why content needs a rule

A base compaction keeps each file's revisions above the retention floor, and its newest revision at or below the floor (see [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction)). The objects that the dropped revisions referred to stay in the store. No current view reads them, and nothing else would delete them.

## The rule

Every member of a fork family writes its content under the family root's prefix, so the pass on the family root `R` keeps a content object while a layout in any member refers to that object, or a tail or session root of any member refers to its chain:

1. **Layout views.** A name that the extent filter of a layout segment in a member's rooted view may contain. Each `content_layouts` segment carries an extent filter over the names of the content objects that its rows' extents reference, written with the segment (see [Extent filter](../specs/format.md#extent-filter)). A rooted view is the member's current manifest (active or deleted), a pinned manifest, or a manifest whose immediate successor is younger than `T`, according to the successor's modification time in the listing. A successor listed without a time counts as young. A missing successor doesn't make the older manifest a root. The pass reads the extent filter of each layout segment that a rooted view lists once, however many views and members list it.
2. **WAL tail.** A revision delta in a member's unfolded WAL tail, or an extent in the layouts of the tail's row state.
3. **Upload sessions.** An upload session record under `R`'s prefix, whatever its status.

The pass considers every other object under `namespaces/R/content/` for deletion once its provider age is at least `T`. It keeps objects that are younger than that, keys that don't parse as content object keys, and objects that an extent filter matches by chance.

The members are the family root and every namespace forked from a member. The pass finds them by walking fork pins. It lists each member's pins once, and each fork pin in a listing adds the pin's target as a member (see [Fork family content](fork-family-content.md#collection)). A pass on a fork sweeps no content.

The same rule covers every way that content becomes garbage:

- **Completed session.** Cleanup removes the session's record, but not its object, after the content grace. A later pass collects the object if nothing published it.
- **Aborted session.** Cleanup deletes the session's object right away, and the pass collects anything that a late write leaves after the record is gone.
- **Retired fork.** A fork stores nothing under its own content prefix, so its retirement deletes no content. The objects it wrote stay while another member's view refers to them, and the rest become unreferenced candidates.
- **Retired family root.** Every fork of the family root is retired first, because each fork pins its source. The same rule then applies with no roots and no age check, so the retirement sweep deletes the family's whole content prefix.

A deleted namespace that hasn't been retired keeps its tombstone as a rooted view, so the content that the tombstone refers to stays until the namespace is retired.

A name is an object's key with the family root's content prefix removed, so it's the same in a segment that a fork wrote and in the family root's content listing. The pass lists the content prefix once and checks each listed name against every extent filter. A filter has no false negatives, so every object that a rooted layout row references stays. A false positive keeps an unreferenced object for as long as a rooted view lists the filter's segment, and a later pass reclaims the object once no rooted view does. Tail and session IDs protect both the whole object and every span of their chains. An old span that no layout, tail, or session refers to can be collected even while its chain is still live.

## Why the roots are enough

A reader or writer in any member works from a manifest that's current, pinned, or superseded for less than `T` (see [Reference roots](../specs/format.md#112-reference-roots)), together with that member's WAL tail. So the first two roots cover every view that a read or a copy can start from. The minimum grace is also longer than the lifetime of a direct download capability issued from such a view (see [Clock and operation assumptions](../specs/format.md#114-clock-and-operation-assumptions)).

A fork's source pin keeps the pinned source manifest as a rooted view of the source, so the fork's inherited revisions and layouts are protected through that view.

The pass lists sessions before it reads any member's tail, and it reads the extent filters after it loads the roots. A session record is removed only after any admission evidence it could issue has expired. A session that the pass doesn't list was either created after the listing or removed before it:

- The content of a session created later is younger than `T`.
- Every commit that refers to the content of a removed session succeeded before the tail was read, so the tail or a rooted manifest refers to that content.

A member's references count as roots until the member is retired. Only retirement deletes a member's source pin, so the walk finds a deleted member until the member is retired. Retirement comes at least the retirement grace after the deletion. The pass reads every member's roots within the retirement grace less the clock allowance, measured from the start of the pass, or it skips the family's content sweep. So if a reference moves from one member to another after the pass has read the second member, the pass still sees it through the first member's views, even if the first member is then deleted (see [Content roots](../specs/format.md#119-content-roots)). A member whose manifest is invalid stops the family's content sweep.

Why list sessions before reading the tail? In the other order, a commit that uses an upload's content could succeed after the pass reads the tail, and a concurrent collector could then remove the session record before the pass lists sessions.

Why read the filters of segments instead of views? A view's layout rows are the rows of its layout segments, so the filters of the distinct segments cover every extent of every view, and a segment that many views list is read once. The filters cover a superseded layout row (a shorter layout of a chain that a longer row replaces), so an object that only a superseded row refers to stays until a compaction drops that row and no rooted view lists the old segment.

Why does a segment carry its extents' names instead of the pass reading its rows? What the pass needs from a segment, the objects that its extents name, is fixed when the segment is written. A scan of the rows would read every layout row of every rooted view on every pass, and exact sets of extent keys grow with the namespace, past the read working memory at a million files.

Why does one pass gather every member's roots, instead of each member sweeping its own content? Every member can reference every object under `R`'s prefix, so one member's roots don't show whether an object that the member doesn't reference is garbage. The family root's pass sees every root of every object it can delete.

## Costs

Content roots add these costs to a collection pass on a family root:

- One manifest discovery, one pin listing, one manifest listing, and one WAL tail read for each member of the family, including each deleted member until it's retired. A fork leaves the family when it's retired.
- One ranged GET of the extent filter of each distinct layout segment that a rooted view of any member lists (up to 16 at a time), so the reads grow with the distinct layout segments and not with the number of views or rows. The segment cache keeps each filter like any other block, so a later pass sends no GET for a filter that's still in the cache. A fork's inherited segments are its source's, so each member adds the filters of its own runs.
- One listing of the family root's content prefix.
- One listing of the family root's uploads prefix, before any tail is read. The prefix contains every member's sessions. Each session has a session record, and an ended session also has an outcome record whose key sorts directly after the session record's key, so the listing shows which sessions have ended. An ended session has two keys, and a completed session stays listed for the content reclamation grace (about seven days), so the listing can have up to twice as many keys as there are sessions. The pass reads the session record of each open session, and both records of each aborted session (up to 16 reads at a time). An outcome record can't change, so LoonFS reads a completed session's outcome record once per process. The runtime keeps each completed session's content ID and completion time between passes, in a cache with its own byte limit (32 MiB by default). If the cache drops a session, the next pass reads its outcome record again. The session sweep lists the uploads prefix again, but it reads only the records of sessions that weren't completed at the first read.
- No HEAD requests during the sweep, except one each time the pass aborts an expired upload session, to check that the session record still exists (see [Upload sessions](../specs/format.md#51-upload-sessions)). (Discovery still sends its HEAD probe for each member's current manifest's successor.) Candidate ages and successor ages come from listing timestamps, and the pass keeps an object whose timestamp is missing.
- Read working memory for the manifest and segment keys that the rooted manifests protect, the rooted manifests (the pass loads up to 16 at a time), the list of distinct layout segments, the extent filters, and the tail and session IDs. If a set exceeds that budget, the pass fails. An extent filter has 20 bits per extent name, so the filters of a namespace of one million files take about 2.5 MB.

A pass on a fork has none of these content-root costs. A deleted family root's pass keeps them while forks of it exist, because those forks keep writing under its prefix.
