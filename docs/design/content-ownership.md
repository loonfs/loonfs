# Content ownership

This note describes where LoonFS stores content objects, how a merge keeps the content of the fork it merged, and how garbage collection decides when that content can be deleted. Every namespace stores the content it writes under its own prefix and collects that content from its own roots. A namespace that merges a fork pins the fork's manifest, and every manifest that the fork's own merge pins keep, until the namespace no longer depends on them. Maintenance copies the merged content into the merging namespace's prefix so that those pins can be released.

## The problem

This section describes how content is stored and collected today, and what that costs.

A [content reference](../specs/glossary.md#content-reference) contains a content ID, a size, and a checksum, and nothing that says which namespace wrote the bytes. Today every member of a [fork family](../specs/glossary.md#fork-family) writes its content objects and upload records under the prefix of the [family root](../specs/glossary.md#family-root), the namespace that was created without a source. Every manifest records the family root (`family_root_id`), and a reader derives each object's key from the family root and the object's name (see [File contents and ownership](../specs/format.md#15-file-contents-and-ownership) and [Storage layout](../specs/format.md#21-storage-layout)). A fork inherits its source's references and reads the same objects, and a [merge](../specs/glossary.md#merge) brings a fork's references into the namespace it was forked from without copying any bytes.

Because any member can refer to any object under the family root's prefix, one member's roots can't show whether an object is garbage. So the collection on the family root reads the roots of every member (see [Content roots](../specs/format.md#119-content-roots)). It finds the members by walking fork pins, and for each member it discovers the current manifest, lists the pins, and reads the WAL tail. A family with 200 live and recently deleted forks pays 200 manifest discoveries, 200 pin listings, and 200 tail reads on every collection of the family root, and a collection on a fork does none of this work.

The walk also has a time bound. A reference can move from one member to another while the collection runs, and the collection sees that reference through the first member's tombstone only until the first member is retired. So the collection has to read every member's roots within the retirement grace less the clock allowance (38 minutes and 30 seconds), measured from its start, or it skips the family's content. Nothing breaks when that happens, but nothing in the family's content prefix is reclaimed until a collection finishes in time. The family root also has to be the last member retired, because its prefix stores the whole family's content (see [Retirement](../specs/format.md#95-retirement)).

The obvious way out is for each namespace to store its own content and for a merge to copy the fork's content into the destination before the commit. That makes a merge cost one copy request for every object the fork wrote, so a merge of 10,000 new files makes 10,000 copies before it can commit, and every byte that a sandbox wrote is copied whether or not the destination keeps it for long. In the design below, a merge makes no copies. Maintenance makes them later, outside the merge's latency, and each copy is optional.

## The design

This section describes the rules of the design, each with the reason for it and an example.

In this note, "main" is a namespace, "sandbox" is a fork of main, and a merge of sandbox into main makes main the "destination". A rebase's destination is the new fork that it creates (see [Rebasing](merge.md#rebasing)).

### Content owners

Every content object is stored under the prefix of the namespace that wrote it, and that namespace is the object's "owner". The keys of a chain's objects are:

```text
namespaces/{owner}/content/{content_id}
namespaces/{owner}/content/{content_id}-{start:020}-{end:020}
```

An upload's object, the objects that a fold writes for WAL pieces, and the extents of new chains are all owned by the namespace that writes them. Upload session records are stored under the uploading namespace's prefix, and the collection on that namespace cleans them up.

Each [extent](../specs/glossary.md#extent) of a [layout](../specs/glossary.md#layout) records its owner (`owner_namespace_id`), and a reader derives the extent's key from the owner and the object's name. A layout row leaves the owner out when the owner is the layout's [creator](../specs/glossary.md#chain-creator), so a chain's own extents don't repeat the creator's ID. The owner appears on shared base extents that another namespace wrote, and on extents that a namespace copied into its own prefix (see [Localizing merged content](#localizing-merged-content)). Leaving it out matters because a namespace ID can be 128 bytes long and a layout can have 32 own extents, so writing the owner on every extent would add up to 4 KiB to a layout row.

For example, main creates `/model.bin`, and its fold writes the chain's whole object at `namespaces/main/content/c_7f2a`. Sandbox, a fork of main, appends to the file. Sandbox isn't the chain's creator, so the append starts a new chain whose layout lists main's object as a shared base extent with owner main, followed by an own extent that sandbox's fold writes at `namespaces/sandbox/content/c_91be-...`.

A read already locates a chain's objects through the chain's longest layout row whenever any of its bytes have been folded (see [Content verification](../specs/format.md#45-content-verification)), so the read path reads nothing new. The change is in how the reader derives a key. The reader puts each extent under the prefix of the extent's owner instead of the family root's prefix. The keys that LoonFS derives from a reference alone, without a layout row, all refer to the reading namespace's own content: an upload's object before the commit that publishes it, and the whole object that a fold writes for a chain that the namespace's own WAL started. Those keys use the reading namespace's prefix. A download of an empty file sends a HEAD for the chain's object, and that key comes from the layout's owner too, because an empty file that a fork inherited is stored under the ancestor's prefix.

The [chain creator](../specs/glossary.md#chain-creator) rule doesn't change. Only the creator extends a chain in place, and any other namespace's append starts a new chain. Separate prefixes would keep two namespaces' span objects at different keys, but the rule is still needed because a merge brings the fork's layout rows into the destination. If main and sandbox both extended one chain, the merge would bring a second layout for the same content ID with different bytes, and the longest layout rule would pick one namespace's bytes for a reference that the other namespace wrote.

The [extent filter](../specs/format.md#extent-filter) doesn't change. Its keys are object names without a prefix, and a name is the same under every owner.

### Ancestors

A namespace's "ancestors" are its source, its source's source, and so on, up to the namespace that was created without a source. The manifest records them in a list (`ancestors`), farthest first. A plain create records an empty list, and a fork records its source's list followed by the source. The list is set when the namespace is created and can't be changed. A rebase's new fork is a fork of main, so its ancestors are main's ancestors followed by main.

A fork's source pin keeps the source's manifest at the fork point (the fork's basis), the source's own source pin keeps the source's basis, and so on up to the namespace that was created without a source. This sequence of pinned manifests is the namespace's "basis chain", and it protects the objects that the namespace inherited from its ancestors.

For example, if sandbox is a fork of main and trial is a fork of sandbox, trial's list is `[main, sandbox]`. Farthest first makes each fork's list its source's list with one entry appended, so the list reads in the order the namespaces were created.

Why record the list instead of walking fork bases? A replay commit and a publication that writes layout rows both need to tell an ancestor's object from any other namespace's object (see [Replaying content in a rebase](#replaying-content-in-a-rebase) and [Localizing merged content](#localizing-merged-content)). A walk would read each fork basis manifest up the chain through its pin, which costs a request for each generation and adds rules for a manifest that's missing. A list in the manifest costs up to 128 bytes per generation.

### External owners and retained views

An owner is "external" to a namespace when it's neither the namespace nor one of its ancestors. Content with an external owner reaches a namespace through a merge or a rebase, either one that the namespace made or one that an ancestor made before the namespace was forked. For example, after main merges sandbox, main's rows refer to objects owned by sandbox, and sandbox is external to main. If trial forks main before main copies those objects into its own prefix, trial's inherited rows refer to sandbox's objects too, and sandbox is external to trial.

A namespace's "retained views" are its current manifest (active or deleted), every manifest that a pin on the namespace keeps, and its unfolded WAL tail. These are the reference roots of [Reference roots](../specs/format.md#112-reference-roots) together with the tail.

### Pinning a merged fork

A merge reads the fork through a pin on the fork's folded manifest, and the destination's rows then refer to the fork's objects. The fork's collection keeps an object while one of the fork's retained views refers to it, so the pin has to stay for as long as the destination refers to the fork's content.

A "merge pin" is a pin on a merged fork's manifest whose pin owner is `merge`, with the destination's namespace ID (`destination_namespace_id`). It's stored in the pinned namespace's `pins/` prefix like every other pin. A "destination record" is stored under the destination's prefix and records that the destination depends on that merge pin. Its key is:

```text
namespaces/{destination}/merge-pins/{pinned_namespace}/{pin_id}.json
```

A namespace ID never contains `/`, so LoonFS can parse the key. The record contains the destination's namespace ID, the pinned namespace's ID, the pin ID, a [manifest reference](../specs/format.md#25-manifest-references) to the pinned manifest, and the record's creation time (`created_at_ms`).

The fork's rows can also refer to objects that the fork itself merged from another namespace and hasn't copied into its own prefix yet. The fork keeps those objects through its own merge pins. A destination never relies on that. It creates a merge pin of its own on every manifest that one of the fork's merge pins keeps, so every dependency is "first order". A namespace depends on pins that it created itself, and never on another namespace keeping a pin.

To merge sandbox into main, LoonFS:

1. Reads the request clock as the call's start (`created_at_ms`). The prepared content of every plan in the call expires `DIRECT_TRANSFER_URL_TTL_MS` after this time.
2. Folds sandbox and selects its folded manifest, as [Merging](merge.md#merging) step 7 does.
3. Creates a merge pin on that manifest, with `created_at_ms` from step 1, and verifies it as any pin from the current head is verified (see [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin)). If the verification fails, LoonFS deletes the pin, best effort, and starts again from a fresh basis.
4. Lists sandbox's destination records. For each record, it creates a merge pin of main's own on the manifest that the record's pin keeps, and then reads sandbox's pin again. If sandbox's pin still exists, main's pin is protected. If sandbox's pin is gone, LoonFS deletes main's new pin, because sandbox released its pin, which means that the manifest selected in step 2 doesn't depend on it.
5. Writes a destination record under main's prefix for each pin that steps 3 and 4 kept, with put-if-absent and the same `created_at_ms`.
6. Lists main's own destination records.
7. Plans and publishes the merge commit, reading sandbox through the pin from step 3. The merge record lists every merge pin of main's that protects an extent with an external owner that the commit admits: the pins that steps 3 and 4 kept, and main's existing pins (from step 6) on the owners of the admitted extents.

The merge commit is published only after steps 5 and 6 succeed, and it MUST start its WAL put within `DIRECT_TRANSFER_URL_TTL_MS` of `created_at_ms`. The writer enforces that bound with the prepared content's deadline. A merge admits each of the fork's references with a proof that expires at that time, and the writer refuses the batch if a proof has expired when it checks the proofs again just before the numbered WAL put. A merge that admits no reference (one that only moves or deletes) has no proof, but it admits no extent either, so its merge record lists no pin. Nothing deletes the pins or the records after the commit.

A route call creates one set of pins. A plan made again after a stale commit reads the same folded manifest through the same pin, so it reuses the first plan's pins, records, `created_at_ms`, and deadline. A retry of the same commit ID in a new call, after an unknown outcome, creates a new set. A rebase creates merge pins in the same way, with the rebase's new fork as the destination, for each call. A merge creates no user pin on the fork.

For example, main merges sandbox at main's sequence 300. Sandbox has no merge pins of its own, so step 4 adds nothing. Sandbox's pins include `pin_00000000000000000042-3f...` with owner `merge {destination_namespace_id: main}`, main's prefix contains `merge-pins/sandbox/pin_00000000000000000042-3f....json`, and the merge record at sequence 300 lists that pin ID.

Why is reading sandbox's pin again after main's put enough? Pins are never recreated. If sandbox's pin exists after main's pin is written, it existed the whole time, so every collection of the pinned namespace that started before main's put listed sandbox's pin, and every later collection lists main's. This is the argument that [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin) makes for a fork of a user pin. A pin of sandbox's that step 4's listing misses belongs to a merge into sandbox that came after the manifest selected in step 2, so that manifest doesn't depend on it.

Why does the destination copy the fork's pins instead of depending on them through the fork? If main depended on sandbox's pins, main's release of its pin on sandbox would have to wait until no namespace that sandbox pins was still referenced by main. That rule would track owners, and an owner can reach main both through main's own earlier merge and through the fork. For example, main merges a fork called scratch, and before main copies scratch's objects into its own prefix, sandbox forks main. Sandbox's fork pin keeps a manifest of main that refers to scratch, and sandbox's own manifest inherits that segment. Main later merges sandbox and copies every object its current rows refer to. A rule that tracked owners would see scratch in the manifest that sandbox's fork pin keeps and in sandbox's manifest, so main would never release its pin on sandbox. Sandbox could then never be retired, and its fork pin on main would never be deleted, even after all three namespaces were deleted. Narrower versions of that rule fail in the merge loop, where one owner can reach a namespace through two different manifests (see the second example in [Releasing a merge pin](#releasing-a-merge-pin)). First-order pins avoid the question, because each destination record is released by its own namespace's views alone.

Why does the merge record list main's existing pins as well as the new ones? A fold attributes each external extent that a merge admits to the merge record's pins on the extent's owner (see [Recording which pins a segment depends on](#recording-which-pins-a-segment-depends-on)), and only the route knows which of main's pins protect an extent that came back through the fork. For example, main merges scratch with pin `p_ms`, sandbox forks main, and then main localizes scratch's objects. Main's current manifest no longer depends on `p_ms`, but main keeps it, because the manifest that sandbox's fork pin keeps depends on it. Sandbox's rows still refer to scratch's objects, and sandbox has no pin on scratch, so when main merges sandbox, those extents come back into main's rows. If the fold left them unattributed, sandbox could be retired, its fork pin's manifest would leave main's retained views, main would release `p_ms`, scratch could be retired, and main's current rows would refer to deleted objects. With `p_ms` in the merge record, the new segment depends on it until main localizes those extents again. An extent with an external owner that no pin in the record covers came from main's fork basis, and the basis chain protects it.

Why does a refused merge keep its pins and records? A commit whose outcome is unknown might still be applied, and deleting a pin would leave the destination referring to objects that the pinned namespace's collection can delete. The release rule deletes them once the creation grace has passed and the destination doesn't depend on them.

A merge pin and its destination record are kept for at least the "merge pin creation grace" (`MERGE_PIN_CREATION_GRACE_MS`, 1,335,000 ms, or 22 minutes 15 seconds) after their `created_at_ms`:

```text
MERGE_PIN_CREATION_GRACE_MS
    = DIRECT_TRANSFER_URL_TTL_MS
      + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
      + GC_SAFETY_MARGIN_MS
```

Why is the grace that long? The merge starts its WAL put within `DIRECT_TRANSFER_URL_TTL_MS` of `created_at_ms`, as described above. The provider applies that put within `PROVIDER_PUBLICATION_REQUEST_BOUND_MS` of its start, and the collector's clock can be ahead of the merging host's clock by up to the clock allowance. So once the grace has passed by the collector's clock, every WAL put that the plan could make has been applied, and a collection that reads the destination's tail afterwards finds the merge record if the merge committed.

### Recording which pins a segment depends on

A collection has to find out, without reading rows, which of its namespace's merge pins its retained views still depend on. So each `content_layouts` segment descriptor records the segment's "pin dependencies" (`pin_dependencies`). Each entry is a pair of an owner (`owner_namespace_id`) and the ID of a merge pin that the namespace that wrote the segment created on that owner (`pin_id`). A pair always identifies a pin of the segment's writer, and a namespace acts only on the pairs of segments it wrote. The set lists the merge pins whose manifests keep objects that the segment's rows refer to. It's usually empty, and it rarely has more than a few entries.

A fold computes a segment's pin dependencies from the merge records in the WAL objects it folds. An extent in a merge's rows that's owned by X is attributed to the merge record's pins on X. An extent with an external owner that has no pin in the merge record came from the destination's fork basis, and it stays unattributed, because the basis chain protects it, as it protects an ancestor's object. A compaction records, for its output, the union of the pin dependencies of its input segments that the compacting namespace wrote, less the pairs whose owner has no extent left in the output rows. It drops the pairs of inherited input segments.

A segment that a fork inherits keeps the pin dependencies that its writer recorded, and those pairs identify the writer's pins, not the fork's. So a pair in an inherited segment never causes the fork to keep or release a pin of its own. The pair still matters to the writer, because the fork's pin on the writer keeps a manifest that lists the segment, and that manifest is one of the writer's retained views.

A namespace "depends on" one of its merge pins when the pin's ID appears in the pin dependencies of a `content_layouts` segment that one of its retained views lists, or in a merge record in its unfolded WAL tail. A collection reads both from the manifests it already loads and the tail it already reads, so it reads no rows and sends no extra request.

For example, main merges sandbox with pin `pin_...42-3f...`, and the fold after the merge writes a segment whose pin dependencies are `{sandbox, pin_...42-3f...}`. Trial forks main before main copies sandbox's objects. Trial's manifest lists that segment, but the pair identifies main's pin, so trial has nothing to release. Main depends on the pin while its current manifest or the manifest that trial's fork pin keeps lists the segment.

Why does a compaction drop an inherited segment's pairs? The pairs identify the writer's pins, so they're no use to the fork, and the writer loses nothing, because the writer's retained views still list the writer's original segments. Dropping them also keeps each pair self-contained, so a publication can tell which owners it may localize from its own inputs, without a request (see [Localizing merged content](#localizing-merged-content)).

### Replaying content in a rebase

A rebase's new fork starts from main's current head, not from the replayed fork's fork point, so the replayed fork's rows can refer to objects that the new fork's view doesn't protect. So a replay commit admits an extent only in two cases. The first is when the new fork's own view (its basis plus its own history) already has a layout for that chain that covers the extent. The second is when the replay commit's merge pins (the pin on the replayed fork and the pins that step 4 copied) include a pin on the extent's owner. Otherwise the replay commit doesn't apply that change, and the conflict report lists the change as a `visibility` conflict, the kind that a rebase already reports for a file that main deleted after the fork point (see [Rebasing](merge.md#rebasing)). The planner already has the chain's layout row from the replayed fork, and it looks the chain up in the new fork's view with one row lookup for each chain it admits that no merge pin covers.

A merge into the fork's source needs no such check, because every extent in the fork's rows is owned by the fork, is protected by one of the fork's own merge pins (which the merge copies), or came from the source's rows at the fork point, which the source's basis chain and the source's own merge pins protect for as long as the fork's pin keeps that manifest.

For example, main has `/data.bin`, whose object main owns. Sandbox forks main at sequence 100 and copies `/data.bin` to `/copy.bin`, which reuses main's chain. Main then deletes `/data.bin`, its retention floor passes the deletion, and a compaction drops the rows. Main's collection still keeps the object, because sandbox's fork pin keeps main's manifest at sequence 100. Sandbox's merge conflicts on another file, so the merge loop rebases sandbox into sandbox-2, a fork of main's current head. Without the rule, the replay commit would create `/copy.bin` in sandbox-2 with an extent owned by main, which sandbox-2's basis doesn't list. Main is an ancestor of sandbox-2, so no merge pin of sandbox-2's covers the object. Sandbox-2 would release its merge pin on sandbox once it stopped depending on it, the merge loop would delete sandbox, sandbox's retirement would delete its fork pin on main, and main's next collection would delete the object that `/copy.bin` refers to. With the rule, the conflict report lists `/copy.bin`, and the resolver recreates it from sandbox's bytes, as it does for any file whose base main no longer has.

The rule covers objects that a third namespace owns in the same way. Main merges scratch, with main's pin on scratch's manifest, and sandbox forks main before main localizes scratch's objects, so sandbox's inherited rows refer to scratch's objects. Main then localizes them, and main keeps its pin on scratch because the manifest that sandbox's fork pin keeps depends on it. Sandbox's replay into sandbox-2 has no pin on scratch, because sandbox has no merge pin of its own, and sandbox-2's basis (main's current head) no longer refers to scratch. Without the rule, sandbox-2 would refer to scratch's objects, and once sandbox was retired, its fork pin on main would go, main would release its pin on scratch, and scratch's retirement would delete the objects. With the rule, the conflict report lists the files that refer to them.

### Localizing merged content

A merge pin keeps the whole pinned manifest, so the pinned namespace can't be retired while the destination depends on the pin. So that the pinned namespace can be retired, maintenance copies the pinned namespace's objects that the destination refers to into the destination's own prefix.

A publication that writes layout rows (a fold, a compaction, or a commit's validation when it writes pending pieces) can "localize" an extent. The publication copies the extent's object to the same name under the publishing namespace's prefix with one server-side copy, and it writes the row with the publishing namespace as that extent's owner. The copy is an [assembly](../specs/glossary.md#assembly) with one source, so the provider's stored checksum is checked. A key that's already occupied with the same stored checksum counts as written, and a different or missing checksum is corruption, as for every chain extent that a fold writes (see [Publishing a manifest](../specs/format.md#72-publishing-a-manifest)).

A publication localizes an extent only when the extent's owner is a namespace on which the publishing namespace has a merge pin. It finds those owners in the pin dependencies of its own input segments (the ones it wrote) and, for a fold, in the merge records it folds, so it sends no listing. It never localizes an object that an ancestor owns, and it never localizes an object whose owner it has no merge pin on, because that object reached the namespace through its basis chain (it was inherited), and copying it would copy the data that the fork was created to share. For example, trial, forked from main before main localized sandbox's objects, has no merge pin on sandbox, so trial's compactions don't copy sandbox's objects. When a compaction rewrites inherited rows together with merged rows that have the same owner, and the namespace has a merge pin on that owner, it localizes both. The inherited ones are the few objects that the source hadn't localized when the fork was taken.

Once a segment has no extent left with a given owner, its pin dependencies drop every pair on that owner, and the destination record is released at the next collection, unless an older retained view still depends on the pin.

The copies happen before the publication writes its segments, within the publication's budget. A publication localizes at most `max_localized_objects_per_publication` objects (a maintenance setting, 1,000 by default, which is about a second of copies at 64 in flight). A publication that reaches the limit writes the remaining extents with their owners and keeps their pairs, and a later publication continues. Each copy is idempotent, and each row records which of its extents are already local, so the work resumes after a crash without a progress record.

Which publications localize is maintenance policy, not format. The reference implementation localizes first in the fold that follows a merge (see [Merging](merge.md#merging) step 12), so a small merge is local after one fold. Compaction localizes the rest. A segment whose pin dependencies identify one of the namespace's own merge pins counts as a compaction candidate, so a base run that nothing else would compact doesn't keep a merged fork from being retired.

Localization is optional for correctness. Without it, nothing is copied, and a merged fork stays unretired while any retained view of the destination depends on its pin. The release rule, the pin dependencies, and the owners on extents are required either way.

### Releasing a merge pin

A destination's collection releases each of the destination's merge pins once the destination no longer needs it, because the destination's views are the ones that depend on the pins.

A collection on main releases a destination record (merge pin P on sandbox) when both of these conditions hold by the collection's clock:

1. The record is older than `MERGE_PIN_CREATION_GRACE_MS`.
2. Main doesn't depend on P, which means that P's ID appears in no pin dependencies of a `content_layouts` segment that one of main's retained views lists, and in no merge record in main's unfolded WAL tail.

To release the record, the collection deletes P from sandbox's prefix first and then deletes the record. Deleting a pin that's already gone succeeds, so a collection that stopped between the two deletes finishes the release on the next collection.

The rule has no transitive condition. Every object that main's rows refer to is main's own, covered by main's basis chain, or kept by one of main's own merge pins, so main's own views determine every release.

The first example is the one that a rule tracking owners couldn't release. Main merges scratch with pin `p_ms`, and sandbox forks main while main's rows still refer to scratch. Main localizes scratch's objects and then merges sandbox with pin `p_mb`. The manifest of main that sandbox's fork pin keeps lists the pair `{scratch, p_ms}` and not `p_mb`. So main releases `p_mb` once its current manifest, its tail, and its other pinned manifests stop depending on it. Sandbox can then be retired, its retirement deletes its fork pin on main, that manifest leaves main's retained views, main releases `p_ms`, and scratch can be retired.

The second example is the merge loop, in which one owner reaches a namespace through two manifests. Sandbox merges part of its range into main, with main's pin `p_ms1` on sandbox's manifest `M1`. Its next merge conflicts, so the loop rebases sandbox into sandbox-2, which pins sandbox's later manifest `M2` with `p_s2`. When main merges sandbox-2, step 4 lists sandbox-2's destination records and creates main's own pin `p_ms2` on `M2`, next to main's pin `p_ms2b` on sandbox-2 from step 3. Main's rows then record the pairs `{sandbox-2, p_ms2b}` and `{sandbox, p_ms2}`, in addition to the older `{sandbox, p_ms1}`. Main localizes sandbox-2's and sandbox's objects and releases each of its three pins as its views stop depending on it. Sandbox-2's retirement deletes `p_s2`, and sandbox can be retired. A fork of main taken in between inherits `{sandbox, p_ms1}`, but the pair identifies main's pin, so the fork never acts on it, and main keeps `p_ms1` while the manifest that the fork's pin keeps is retained, which is exactly as long as a read through that fork pin could need sandbox's objects.

A deleted namespace's current manifest stops counting as a retained view for the release rule once the collection's clock has passed the tombstone's retirement deadline (see [Retirement](../specs/format.md#95-retirement)). Until then it counts, so a cached reader within the revalidation bound still finds the objects it refers to. After the deadline, the manifests that pins on the deleted namespace keep still count.

Why does a tombstone stop counting at its deadline? Without that, a deleted destination and the deleted fork it merged would never be retired. Suppose main merges sandbox, and then both are deleted. Sandbox's fork pin on main blocks main's retirement, and main's merge pin on sandbox blocks sandbox's retirement. Main's tombstone depends on the merge pin, and a tombstone has no successor, so nothing ever localizes sandbox's objects in main. Once main's retirement deadline passes, main's tombstone stops counting, the only manifest left in main's views is the one sandbox's fork pin keeps, and that manifest was published before the merge. So main releases its merge pin, sandbox is retired, sandbox's retirement deletes its fork pin on main, and main is retired.

A merged fork can also stay pinned through a fork of the destination. If trial forks main while main still depends on its pin on sandbox, trial's fork pin keeps a manifest of main that depends on it, and trial reads sandbox's objects through that manifest. Main keeps its merge pin on sandbox until trial is retired. Localizing in the fold right after a merge makes this rare, because few forks are taken in the time between a merge and its fold.

### Cleaning up a merge pin

A pinned namespace's collection checks whether to keep each merge pin on the namespace, in the same way that it checks a fork pin (see [Pin cleanup](../specs/format.md#117-pin-cleanup)). Releasing a merge pin is the destination's job, so the pinned namespace's collection deletes a merge pin only when the destination has finished with it or never needed it.

| Condition | Decision |
| --- | --- |
| The pin is inside its creation grace | Keep the pin. |
| A read of the destination record at its key finds it with the same manifest reference | Keep the pin. |
| The read fails | Keep the pin. |
| The record is absent | Delete the pin. The destination released the pin, never wrote the record, or was retired. |
| The record exists with a different manifest reference | Fail with `namespace_corrupt`. |

This costs one GET for each merge pin, compared with two for a fork pin (the target's hint and its manifest), and it reads no rows and no WAL tail of the destination.

### Collecting a namespace's content

A collection on a namespace establishes its roots as [One complete collection](../specs/format.md#111-one-complete-collection) does, without the fork family steps. It discovers the current manifest, lists the namespace's upload sessions, reads the WAL tail, lists all pins, and lists the namespace's destination records. A content object under the namespace's own prefix is kept while the extent filter of a `content_layouts` segment in one of its retained views may contain the object's name, or the tail or an upload session refers to its chain, as [Content roots](../specs/format.md#119-content-roots) describes for one member of a fork family. The collection then cleans up pins, including merge pins, releases destination records, and retires the namespace if it's eligible.

For example, a collection on sandbox lists `namespaces/sandbox/content/` and reads the extent filters of the layout segments that sandbox's retained views list: its current manifest, and the manifests that its snapshots, its forks' fork pins, and main's merge pins keep. It deletes every object older than the grace window that none of those filters may contain and that no chain in sandbox's tail or upload sessions refers to.

Why is that safe without reading any other namespace's roots? Every other namespace that reads an object under sandbox's prefix reads it through a manifest of sandbox that a pin keeps. A fork of sandbox reads through its fork pin, and a fork of that fork reads through the same pinned manifest, which the chain of fork pins keeps. A namespace whose rows refer to sandbox's objects through a merge has its own merge pin on sandbox, because merges copy the fork's pins, and a fork of that namespace reads through a manifest that depends on that pin. Sandbox's own pin listing includes every one of those pins, so sandbox's collection sees every manifest that anyone reads sandbox's objects through. A rebase is the one way a namespace could refer to an object without such a pin or its basis chain, and the replay rule (see [Replaying content in a rebase](#replaying-content-in-a-rebase)) refuses that case.

### Retiring a namespace

A deleted namespace is eligible for retirement when its deadline has passed and its complete pin listing contains no pin on it (see [Retirement](../specs/format.md#95-retirement)). Merge pins are pins, so a namespace whose content a destination still reads can't be retired. To retire an eligible namespace, garbage collection:

1. Lists the namespace's content prefix and deletes every key.
2. Deletes each merge pin that the namespace's destination records refer to, and then the record.
3. Deletes the source pin that the tombstone's fork basis refers to, if there is one.

Step 2 doesn't apply the release rule, because nothing that the namespace refers to is read any more. Every step is idempotent, so a retirement that stops can start over. Retirement doesn't delete the namespace's hint or its tombstone.

Every namespace's retirement deletes its own content, so no namespace waits for another namespace's retirement before its own content can be deleted, and no namespace collects another namespace's content.

### Invariants

The design keeps these properties, and a bounded model checks them:

1. Every content object exists while a retained view of a namespace that isn't retired refers to it, including an object that the view reaches through a chain of pins.
2. If every namespace in a set that refers to each other's content is deleted, every one of them is eventually retired. In particular, a deleted destination and the deleted fork it merged are both retired.
3. A merge pin is deleted only by its destination's collection once the destination no longer depends on it, by the pinned namespace's collection when the destination record is absent after the creation grace, or by the destination's retirement.
4. A content object is deleted only by its owner's collection or its owner's retirement.
5. Every extent that a namespace refers to and that an ancestor owns is covered by its basis chain, which means that the pinned basis manifest of its source refers to the same object, directly or through the source's own basis.
6. Every object that a namespace's rows refer to is owned by the namespace, is covered by its basis chain, or is listed by a manifest that one of the namespace's own merge pins keeps.

## Costs

This section lists what the design costs, compared with one content prefix per fork family.

- A merge lists the fork's destination records and its own once per route call, writes one destination record for each merge pin it creates, and deletes nothing after the commit (with one prefix per fork family, a merge deletes its merge snapshot). A sandbox usually has no merge pins of its own, so a merge usually creates one pin and one record. A rebase does the same for each replay commit.
- In the merge loop, a merge or a replay commit also creates one or two more pins, each with a GET of the fork's pin and a destination record, because the fork has merge pins from earlier rebases.
- A collection on a namespace sends one more LIST (its destination records, usually empty) and one GET for each merge pin on the namespace (the destination record). A collection on a fork also lists the fork's own content prefix, which is empty with one prefix per fork family.
- A collection sends no per-member manifest discovery, pin listing, or tail read, and it has no time bound. A template with many forks still lists and judges one fork pin for each fork, at two GETs each, as it does with one prefix per fork family.
- Localization sends no listing, and each localized object costs one server-side copy (or one HEAD when the key is already occupied), bounded per publication.
- A merged fork's bytes are stored twice from the time they're localized until the fork is retired. Without localization, they're stored once, and the fork stays unretired while the destination depends on its pin.
- A merged fork stays unretired while a fork of the destination, taken before localization, keeps a manifest of the destination that depends on its pin, and while a snapshot of the destination does.
- A replay commit refuses a change that refers to an object that neither the new fork's view nor one of its merge pins covers, so the resolver re-uploads that file's bytes.
- Each manifest stores the ancestor list, up to 128 bytes per generation. Each layout segment descriptor stores its pin dependencies, and each pair is an owner (up to 128 bytes) and a pin ID (41 bytes). Each merge record lists its merge pins' IDs.
- Each namespace's writes go to its own prefix from the first request, so a busy fork family no longer concentrates its writes in one prefix that the provider has to partition.

## Changes to the specs and glossary

This section lists the sections of the storage format, the API spec, and the glossary that the design changes.

The storage format changes in these sections:

- [Namespaces and identity](../specs/format.md#11-namespaces-and-identity) drops the fork family, its members, and the family root, and defines a namespace's ancestors and the manifest's ancestor list.
- [File contents and ownership](../specs/format.md#15-file-contents-and-ownership) says that a content object is stored under its owner's prefix and that each extent records its owner, replaces the paragraphs about one prefix per family with the rule that a namespace writes its content under its own prefix, and bases the creator rule's reason on the merge of layout rows.
- [Storage layout](../specs/format.md#21-storage-layout) removes the exception for a fork's content and upload records, and adds the `merge-pins/` prefix.
- [Manifest references](../specs/format.md#25-manifest-references) says that a destination record refers to a manifest in another namespace.
- [Content verification](../specs/format.md#45-content-verification) says that a reader derives each extent's key from the extent's owner.
- [Upload sessions](../specs/format.md#51-upload-sessions) stores the session's object and records under the uploading namespace's prefix.
- [Merges](../specs/format.md#merges) says that a merge creates its merge pins and destination records before its commit, including its own pins on the manifests that the fork's merge pins keep.
- [Commit identity and retries](../specs/format.md#65-commit-identity-and-retries) defines a merge as a commit into the namespace that the fork was forked from or into a rebase's new fork, without the fork family.
- [Operations and WAL deltas](../specs/format.md#66-operations-and-wal-deltas) restates why only a chain's creator extends it in place, and says that a new chain's shared base extents keep their owners.
- [Publishing a manifest](../specs/format.md#72-publishing-a-manifest) replaces the family root row of the first-manifest table with the ancestor list, writes a fold's extents under the folding namespace's prefix, adds localization and its bound, requires every successor to keep the ancestor list, and defines how a fold computes a segment's pin dependencies from merge records.
- [Records and owners](../specs/format.md#81-records-and-owners) adds the `merge` owner with `destination_namespace_id`, removed by the destination's release, the pinned namespace's cleanup, or the destination's retirement.
- [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin) says how a merge creates a merge pin on a manifest that another merge pin keeps, and why reading that pin again after the put protects the manifest.
- [Reads, deletion, and expiry](../specs/format.md#83-reads-deletion-and-expiry) says that LoonFS reads a fork through a merge pin while it plans, that the API doesn't accept a merge pin's ID, and that a merge pin can't be deleted explicitly.
- [Creating a namespace](../specs/format.md#91-creating-a-namespace) records an empty ancestor list.
- [Forking a namespace](../specs/format.md#92-forking-a-namespace) copies the source's ancestor list followed by the source, in place of the family root.
- [Retirement](../specs/format.md#95-retirement) deletes every eligible namespace's content and its merge pins, and drops the rule that the family root is retired last.
- [Fork dependencies after deletion](../specs/format.md#96-fork-dependencies-after-deletion) says that a fork finds inherited bytes under each extent's owner, and adds merge pins to the dependencies that block retirement.
- [Cross-namespace copies and moves](../specs/format.md#97-cross-namespace-copies-and-moves) says that content moves between namespaces by a merge, a rebase, or an upload, replaces the 15-minute merge pin with merge pins, and adds the replay rule for objects that the new fork's view doesn't cover.
- [Compaction windows](../specs/format.md#102-compaction-windows) makes a layout segment that depends on one of the namespace's own merge pins a compaction candidate.
- [Streaming compaction](../specs/format.md#104-streaming-compaction) adds localization within a compaction's budget and the rule for its output's pin dependencies.
- The introduction to [Garbage collection](../specs/format.md#11-garbage-collection) says that each namespace collects its own content.
- [One complete collection](../specs/format.md#111-one-complete-collection) drops the steps for other members and adds the listing of destination records, the release step, and merge pin cleanup.
- [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules) applies the content rule to every namespace's own prefix and replaces the family root's retired content with every retired namespace's.
- [Upload-session cleanup](../specs/format.md#116-upload-session-cleanup) cleans up each namespace's own sessions under its own prefix.
- [Pin cleanup](../specs/format.md#117-pin-cleanup) adds the table for judging a merge pin and drops the paragraph about finding members.
- [Deleting a retired owner's content](../specs/format.md#118-deleting-a-retired-owners-content) applies step 1 to every namespace and adds the deletion of its merge pins and destination records.
- [Content roots](../specs/format.md#119-content-roots) protects a namespace's content with its own retained views, and drops the walk over fork pins, the time bound, and the family root's collection of other members.
- A new subsection after Content roots defines when a namespace depends on a merge pin and the release rule, including the tombstone deadline.
- [Alternatives considered](../specs/format.md#13-alternatives-considered) replaces the entries for content under the writing namespace and an owner on every content reference with entries for one prefix per fork family and copying at merge time, and links this note in place of the fork family content note.
- [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads) replaces `family_root_id` with `ancestors`, adds `pin_dependencies` to the `content_layouts` segment descriptor and the destination record payload, and moves the upload records under the uploading namespace.
- [WAL records](../specs/format.md#a5-wal-records) adds to the merge record the IDs of the destination's merge pins that protect the extents it admits, and says that a revision delta's layout includes each extent's owner when it isn't the creator, and that a copy or restore takes the owners from the planning view.
- [Metadata rows and row keys](../specs/format.md#a6-metadata-rows-and-row-keys) adds the optional `owner_namespace_id` to an extent, absent when it equals `creator_namespace_id`.
- [Extent filter](../specs/format.md#extent-filter) defines a name as the extent's key with its owner's content prefix removed.
- [Object keys](../specs/format.md#a8-object-keys) puts content objects under the owner, upload records under the uploading namespace, and adds the destination record key.
- [Merge commits](../specs/format.md#b5-merge-commits) leaves the merge record's pin IDs out of the fingerprint, because a retry of the same commit ID in a new route call creates new pins and would otherwise conflict with the original commit.
- [Publication and collection timing](../specs/format.md#c1-publication-and-collection-timing) adds `MERGE_PIN_CREATION_GRACE_MS` and its formula.
- [The `texts` and `content_revisions` families](../specs/format.md#d4-the-texts-and-content_revisions-families) and the grep sections that read a source's content say that a content ID identifies the same bytes in every namespace that refers to it, without the fork family.

The API changes in these sections:

- [Minimal upload, commit, and change-feed model](../specs/api.md#5-minimal-upload-commit-and-change-feed-model) drops the question about a prepared value from the destination's fork family.
- [Maintenance jobs](../specs/api.md#maintenance-jobs) says that every collection lists and collects its own content, drops the skipped content sweep, reports `retired_content_objects` for every retired namespace, and reports the merge pins that a collection released.
- [Snapshots](../specs/api.md#snapshots) no longer mentions a merge's snapshot, and [Checkpoint inventory](../specs/api.md#checkpoint-inventory) lists merge pins with `owner.kind` `merge` and `destination_namespace_id`.
- [Content references and tokens](../specs/api.md#content-references-and-tokens) says that a reference's bytes are found through the layout's extent owners, not the family root.
- [`GET /v0/namespaces/{ns}`](../specs/api.md#62-get-v0namespacesns) replaces `family_root_id` with `ancestors`.
- [`POST /commits`](../specs/api.md#68-post-commits) words the append rule's new-chain case for a chain that another namespace created, without the fork family.
- [`POST /forks`](../specs/api.md#613-post-forks) says that the fork reads inherited content under each extent's owner and records the ancestor list.
- [`POST /merges`](../specs/api.md#614-post-merges) step 6 creates the merge pins and destination records in place of the `merge <commit_id>` snapshot, including pins on the manifests that the fork's merge pins keep, and the pins aren't deleted after the commit.
- [`POST /rebases`](../specs/api.md#616-post-rebases) creates merge pins for each replay commit in the same way, and lists a change that refers to an object that neither the new fork's view nor one of its merge pins covers as a `visibility` conflict.
- [Server requirements](../specs/api.md#71-server-requirements) resolves content through the extent's owner and the content ID, in place of the family root.

The glossary changes in these entries:

- New entries define "ancestor", "content owner", "external owner", "merge pin", "destination record", "pin dependencies", and "localize".
- "Fork family" and "Family root" are removed.
- "Content object", "Content reference", "Chain creator", "Extent", "Layout", "Upload session", and "Content root" describe ownership by the writing namespace in place of the family root.
- "Snapshot" lists the merge pin as a third owner and drops the merge's user pin.
- "Fork", "Merge", and "Rebase" describe their relationships without the fork family.
- "Collection" and "Namespace retirement" drop the family root's collection of other members and say that every retired namespace's content is deleted.

## Other systems

Several storage systems share data between a clone and its source, and each one handles the source's collection differently. A Delta Lake shallow clone refers to the source table's data files in place, and running VACUUM on the source can delete files that the clone still reads. A merge pin exists to prevent that case. Ceph RBD layering lets a clone refer to a protected snapshot of its parent, and its `flatten` operation copies the snapshot's data into the clone so that the parent can be unprotected and deleted. Localization is a flatten that runs inside ordinary maintenance, bounded per publication, and only for content that the namespace's own merges brought in. Neon's `detach_ancestor` copies an ancestor's layers below a branch point into the child's own prefix with server-side copies and then re-parents the child, which keeps the parent free to collect and compact. ZFS `promote` swaps the roles of a clone and its origin so that the origin can be destroyed without copying anything. Apache Iceberg removes the data files that no retained branch or tag reaches when it expires snapshots, and it computes that from metadata rather than from a scan of the data, which is the shape of the release rule here.

## Alternatives considered

- **One content prefix per fork family.** Every member writes under the family root's prefix, and the family root's collection reads every member's roots. A merge copies nothing and needs no second kind of pin. But every collection of the family root pays a manifest discovery, a pin listing, and a tail read for each member, it has to finish within the retirement grace less the clock allowance, the family's writes all go to one prefix, and the family root is retired last.
- **Copying at merge time.** The destination copies every object the merge refers to before the commit. There are no merge pins, no localization, and no release rule, but a merge of 10,000 new files makes 10,000 copies before it commits, and every byte a sandbox wrote is copied, including bytes that the destination replaces soon after.
- **Tracking dependencies by owner, with a transitive release condition.** Each segment would record the external owners of its rows, and a destination would release its pin on a fork only when none of the owners that the fork's manifest lists was still referenced. One owner can reach a namespace through its fork basis and through a merge, through two different pinned manifests, and an owner set can't tell them apart. The rule then never releases in the case described in [Pinning a merged fork](#pinning-a-merged-fork), and the narrower versions that do release are unsafe in the merge loop.
- **An owner on every content reference.** A reference would locate its own bytes, so a read could find an object without a layout row. Every reference in every row and every delta would grow by a namespace ID, and a reference would become a locator that a client could point at any namespace's prefix.
- **Walking fork bases instead of recording ancestors.** The planner and the maintainer would read each pinned basis manifest up the chain to decide whether an owner is an ancestor, which costs a request for each generation.
- **Recording merge pins in the manifest.** A fold would add an entry for each merge record in the tail, and the destination would need a manifest publication to remove an entry, or every later publication would check each entry's pin, which costs a request for each unreleased pin on every fold.
- **No localization.** Nothing is ever copied, which is correct and the simplest design, but a long-lived destination that still depends on a merge pin never lets the pinned namespace be retired.
- **Localizing every external owner.** A fork of the destination taken before localization would copy the merged content it inherited, which is the data the fork was created to share, and the copies wouldn't let any pin be released, because the pair in the inherited segment identifies the destination's pin.
- **Copying uncovered objects during a rebase.** A replay commit could copy an object that the new fork's view doesn't cover into the new fork's prefix before the commit, bounded by the replay commit's size. The replay already reports the changes that depend on state main no longer has, and the resolver already recreates such files from the fork's bytes, so the copy would be a second path for the same case.
- **Adopting the fork's prefix.** The destination could take over the merged fork's content prefix instead of copying from it. A prefix is part of a namespace's identity, and the fork can stay in use after the merge, so the destination can't take it over.
