# Content collection

Garbage collection deletes a content object once no retained view in its [pool](../specs/glossary.md#pool) refers to the object, and the object is older than the grace window `T`. The collection of the pool does this, with one clock value for the whole collection and no durable progress. [Content roots](../specs/format.md#119-content-roots) in the storage format defines the rule, and this note explains its design. [Pools](pools.md) explains why a pool's content is stored under the pool's own prefix.

## Why content needs a rule

A base compaction keeps each file's revisions above the retention floor, and its newest revision at or below the floor (see [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction)). The objects that the dropped revisions referred to stay in the store. No current view reads them, and nothing else would delete them.

## The rule

Every [member](../specs/glossary.md#pool) of a pool writes its content under the pool's prefix, so the collection of the pool `P` keeps a content object while a layout in any member refers to that object, or a tail or session root of any member refers to its chain:

1. **Layout views.** A name that the extent filter of a layout segment in a member's rooted view may contain. Each `content_layouts` segment carries an extent filter over the names of the content objects that its rows' extents reference, written with the segment (see [Extent filter](../specs/format.md#extent-filter)). A rooted view is the member's current manifest (active or deleted) or a manifest that a snapshot keeps. The collection reads the extent filter of each layout segment that a rooted view lists once, however many views and members list it.
2. **WAL tail.** A revision delta in a member's unfolded WAL tail, or an extent in the layouts of the tail's row state.
3. **Upload sessions.** An upload session record under `P`'s prefix, whatever its status.

The collection considers every other object under `P`'s content prefix (`pools/{pool_id}/content/`) for deletion once its provider age is at least `T`. It keeps objects that are younger than that, keys that don't parse as content object keys, and objects that an extent filter matches by chance.

The members are the namespaces that have a [member entry](../specs/glossary.md#member-entry) in the pool: the namespace that was created with the pool, and every namespace forked from a member. The collection lists the pool's member entries once, and reads the roots of each listed namespace whose newest manifest records the pool (see [The pool's collection](pools.md#the-pools-collection)). A namespace's own collection lists no content.

The same rule covers every way that content becomes garbage:

- **Completed session.** Cleanup removes the session's record, but not its object, after the completed-content grace. A later collection deletes the object if nothing published it.
- **Aborted session.** Cleanup deletes the session's object right away, and the collection deletes anything that a late write leaves after the record is gone.
- **Retired fork.** A fork's retirement deletes the fork snapshot on the fork's source and the fork's member entry, raises the fork's manifest boundary to retired, and deletes no content. The objects that the fork wrote stay while another member's view refers to those objects, and the rest become unreferenced candidates.
- **Retired last member.** Each fork keeps a fork snapshot on its source, so the namespace that was created with the pool is retired after every fork of it. Once every member is retired, the pool has no member entries and no roots, so the same rule applies with no roots, and later collections of the pool delete the pool's content under the age rule.

A deleted namespace that hasn't been retired keeps its tombstone as a rooted view, so the content that the tombstone refers to stays until the namespace's own collection retires the namespace. The pool's collection doesn't decide for itself that a tombstone is eligible for retirement.

A name is an object's key with the pool's content prefix removed, so it's the same in a segment that any member wrote and in the pool's content listing. The collection lists the content prefix once and checks each listed name against every extent filter. A filter has no false negatives, so every object that a rooted layout row references stays. A false positive keeps an unreferenced object for as long as a rooted view lists the filter's segment, and a later collection reclaims the object once no rooted view does. Tail and session IDs protect both the whole object and every span of their chains. An old span that no layout, tail, or session refers to can be collected even while its chain is still live.

## Why the roots are enough

A reader or writer in any member works from a manifest that's current or that a snapshot keeps, together with that member's WAL tail. A read or a write on a manifest that a newer manifest replaced runs again on the current manifest when it finds a segment missing (see [Reference roots](../specs/format.md#112-reference-roots)). So the first two roots cover every view that a read or a copy can start from. A presigned download URL for a revision that's current in the view that issued it works for the URL's whole lifetime, because a retention advance passes the commit that replaces the revision only once that commit is at least the [grace window](../specs/glossary.md#grace-window) old (see [Content roots](../specs/format.md#119-content-roots)).

A fork snapshot keeps the source manifest that its fork reads through as a rooted view of the source, so the fork's inherited revisions and layouts are protected through that view.

The collection lists sessions before it reads any member's tail, and it reads the extent filters after it loads the roots. A session record is removed only after any admission evidence it could issue has expired. A session that the collection doesn't list was either created after the listing or removed before it:

- The content of a session created later is younger than `T`.
- Every commit that refers to the content of a removed session succeeded before the tail was read, so the tail or a rooted manifest refers to that content.

A member's references count as roots until the member is retired. Only retirement deletes a member's member entry, so the collection finds a deleted member until the member is retired. Retirement comes no earlier than the grace window (24 hours) after the deletion. The collection reads every member's roots within 12 hours, measured from the start of the collection, or it skips the pool's content objects. So if a reference moves from one member to another after the collection has read the second member, the collection still sees it through the views of the member that the reference came from, even if that member is then deleted (see [Content roots](../specs/format.md#119-content-roots)). A member whose manifest is invalid stops the collection of the pool's content.

Why list sessions before reading the tail? In the other order, a commit that uses an upload's content could succeed after the collection reads the tail, and a concurrent collector could then remove the session record before the collection lists sessions.

Why read the filters of segments instead of views? A view's layout rows are the rows of its layout segments, so the filters of the distinct segments cover every extent of every view, and a segment that many views list is read once. The filters cover a superseded layout row (a shorter layout of a chain that a longer row replaces), so an object that only a superseded row refers to stays until a compaction drops that row and no rooted view lists the old segment.

Why does a segment carry its extents' names instead of the collection reading its rows? What the collection needs from a segment, the objects that its extents name, is fixed when the segment is written. A scan of the rows would read every layout row of every rooted view on every collection, and exact sets of extent keys grow with the namespace, past the read working memory at a million files.

Why does one collection gather every member's roots, instead of each member collecting its own content? Every member can reference every object under `P`'s prefix, so one member's roots don't show whether an object that the member doesn't reference is garbage. The pool's collection sees every root of every object it can delete.

## Costs

Content roots add these costs to a collection of a pool:

- One listing of the pool's member list.
- One manifest discovery, one snapshot listing, and one WAL tail read for each member of the pool, including each deleted member until it's retired. A fork stops being a member when it's retired.
- One ranged GET of the extent filter of each distinct layout segment that a rooted view of any member lists (up to 16 at a time), so the reads grow with the distinct layout segments and not with the number of views or rows. The segment cache keeps each filter like any other block, so a later collection sends no GET for a filter that's still in the cache. A fork's inherited segments are its source's, so each member adds the filters of its own runs.
- One listing of the pool's content prefix.
- One listing of the pool's uploads prefix, before any tail is read. The prefix contains every member's sessions. Each session has a session record, and an ended session also has an outcome record whose key sorts directly after the session record's key, so the listing shows which sessions have ended. An ended session has two keys, and a completed session stays listed for the completed-content grace (about seven days), so the listing can have up to twice as many keys as there are sessions. The collection reads the session record of each open session, and both records of each aborted session (up to 16 reads at a time). An outcome record can't change, so LoonFS reads a completed session's outcome record once per process. The runtime keeps each completed session's content ID and completion time between collections, in a cache with its own byte limit (32 MiB by default). If the cache drops a session, the next collection reads its outcome record again. Session cleanup decides from the same listing, and it reads the records again for the sessions that weren't completed at the first read.
- No HEAD requests during the collection, except one each time the collection aborts an expired upload session, to check that the session record still exists (see [Upload sessions](../specs/format.md#51-upload-sessions)). (Discovery still sends its HEAD probe for each member's current manifest's successor.) Candidate ages come from listing timestamps, and the collection keeps an object whose timestamp is missing.
- Read working memory for the manifest and segment keys that the rooted manifests protect, the rooted manifests (the collection loads up to 16 at a time), the list of distinct layout segments, the extent filters, and the tail and session IDs. If a set exceeds that budget, the collection fails. An extent filter has 20 bits per extent name, so the filters of a namespace of one million files take about 2.5 MB.

A namespace's own collection has none of these costs, because it makes no request under `pools/`.
