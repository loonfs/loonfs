# Content collection

Collection deletes a content object in an active namespace once no retained view names it and the object is older than the reclamation grace `T`. The ordinary collection pass does this, with its one fixed clock and no durable progress. The storage format defines the rule in [section 11.9](../specs/format.md#119-content-roots). This note explains its shape.

## Why content needs a rule

A base compaction keeps each file's revisions above the retention floor and its newest revision at or below it ([format section 10.3](../specs/format.md#103-row-retention-during-a-base-compaction)). The objects that the dropped revisions named stay in the store. No current view reads them, and nothing else would delete them.

## The rule

A content object owned by namespace `N` stays while one of these names its content ID:

1. A revision row in a manifest the pass already roots for its segments: `N`'s current manifest, a manifest a listed pin holds, or a manifest whose immediate successor is younger than `T`. The revision names a chain. The layout rows of the same views name the objects of the chains those revisions name.
2. A revision delta or a piece's base in `N`'s unfolded WAL tail.
3. An upload session record in `N`, whatever its status.

The pass deletes every other object under `namespaces/N/content/` whose provider age is at least `T`. It keeps younger objects and keys the layout does not recognize.

One rule covers every way content becomes garbage. A completed session's cleanup removes only its record, after the content grace, and a later pass collects the object if nothing published it. An aborted session's cleanup deletes its object at once, and the pass collects anything a late write leaves after the record is gone. A retired namespace is the same rule with no roots and no age gate: the retirement sweep deletes its whole content prefix. A deleted namespace that has not retired keeps its content.

The id set protects both the whole object and every span of a rooted chain. It also protects chains named only by another chain's layout. Superseded spans of a live chain remain until the sorted merge replaces the in-memory id set. An unreferenced layout does not root its objects.

## Why the roots are enough

A reader or writer works from a manifest that is current, pinned, or superseded for less than `T` ([format section 11.2](../specs/format.md#112-reference-roots)), together with the WAL tail. The first two roots therefore cover every view that a read or a copy can start from. The minimum grace also outlasts a direct download capability issued from such a view ([format section 11.4](../specs/format.md#114-clock-and-operation-assumptions)).

A fork's source pin roots its inherited revisions and layouts in the owner's namespace.

The pass lists upload sessions before it reads the WAL tail. A session record is removed only after any admission evidence it could issue has expired. A session the pass does not list was either created after the listing or removed before it. Content of a session created later is younger than `T`. Every commit that names content of a removed session landed before the tail was read, so the tail or a rooted manifest names that content. In the other order, a commit of an upload's content could land after the tail read, and a concurrent collector could then remove the session record before the listing.

## Costs

- One scan of each of the `revisions` and `content_layouts` families for each distinct rooted manifest. A fork's scan also reads the segments it inherited.
- One listing of the content prefix.
- One listing of the upload sessions and one read of each record before the tail, besides the session sweep's own listing and reads.
- One HEAD per unreferenced candidate, because the listing carries no timestamp.
- The referenced content IDs, held in memory for the pass. Their table is charged to the shared read working memory, beside the blocks the scan reads through the shared segment cache, so a pass never holds more than the configured budget. A namespace whose live IDs do not fit fails the pass with an error that names the bytes and the limit, and is not collected until the limit is raised.

A deleted namespace pays none of these.
