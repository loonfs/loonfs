# The manifest boundary

This note describes how LoonFS finds a [namespace](../specs/glossary.md#namespace)'s newest manifest, cleans up old manifests, and records that a namespace ID was used. One small object for each namespace stores one manifest number, the "manifest boundary", and every manifest at or below that number is dead. Cleanup raises the boundary before it deletes a manifest, a publisher reads the boundary after its put, and a reader lists the manifests above the boundary. Retiring a deleted namespace raises the boundary to a "retired" state, and the retired namespace keeps nothing else under its prefix. The boundary replaces the hint, a small object that pointed readers at a recent manifest (see [The problem](#the-problem)).

## The problem

This section describes how LoonFS finds the current manifest and records a namespace ID, and what that costs.

Each namespace has a hint (`namespaces/{namespace_id}/hint.json`), a [control object](../specs/glossary.md#control-object) that records a recent manifest number. After each manifest publication, the runtime raises the hint with one [compare-and-swap](../specs/glossary.md#compare-and-swap-cas) (see [The current manifest and WAL tip](../specs/format.md#24-the-current-manifest-and-wal-tip)). A cold read loads the hinted manifest and probes the next numbers until one is missing (see [Resolving the metadata basis](../specs/format.md#41-resolving-the-metadata-basis)), so a [collection](../specs/glossary.md#collection) deletes only manifests below the hint value it observed (see [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules)).

The hint is there for speed, but correctness rules depend on the hint. A missing hinted manifest with an unchanged hint means a corrupt store. A process that already has a manifest can't tell a successor that was never published from one that was published and deleted, so it trusts a missing successor only within `READ_REVALIDATION_BOUND_MS` of its probe (see [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal)). A manifest publication puts at its basis's number plus one only when its basis was confirmed less than that bound ago, and otherwise discovers the current manifest through the hint (see [Publishing a manifest](../specs/format.md#72-publishing-a-manifest)). And every publication writes the hint once more, although some providers accept about one write per second to one object (see the [provider reference](../specs/object-storage-providers.md#1-frequently-used-data-points)).

The hint also records that an ID was used. A create or fork writes the hint with put-if-absent before manifest 1, and the hint put determines whether the ID is new, because a collection can delete manifest 1 once a newer manifest replaces it (see [Creating a namespace](../specs/format.md#91-creating-a-namespace)).

A namespace ID identifies one namespace from its creation to its deletion, and it can't be reused (see [Namespaces and identity](../specs/format.md#11-namespaces-and-identity)). So a deleted namespace's hint and [tombstone](../specs/glossary.md#namespace-tombstone) stay as long as the store does, and the tombstone, which stays the current manifest, keeps every segment that it lists and that the namespace wrote. The reference server's [sweep](../specs/glossary.md#sweep) runs a collection of every namespace under `namespaces/`, so the work of every sweep grows with the number of namespaces that were ever deleted. An application that creates, merges, and deletes a fork for each task never stores more live data, but each sweep takes longer than the one before.

LoonFS can't delete those objects without some record of the ID, because three parts of LoonFS depend on an ID not being used twice: the source's [merge index](../specs/glossary.md#merge-index) keys its rows by the fork's namespace ID, a chain's layout records the [chain's creator](../specs/glossary.md#chain-creator) by namespace ID, and LoonFS caches a namespace's head under the namespace ID and the manifest and WAL numbers.

## The design

This section describes the boundary object, the rules for each process that reads or writes the boundary, the pool's collection, and the time rules. The design doesn't change the WAL rules.

### The boundary object

The "manifest boundary" of a namespace is a manifest number `B` such that every manifest of the namespace numbered `B` or lower is dead. No process may publish a manifest at such a number, and no process may treat such a manifest as the namespace's newest. A dead manifest can still exist, because a [snapshot](../specs/glossary.md#snapshot) keeps the manifest or cleanup hasn't deleted that manifest yet. The boundary of a retired namespace is in a second state, "retired", which compares as above every manifest number.

The boundary is stored in one control object for each namespace, inside the namespace's prefix:

```text
namespaces/{namespace_id}/boundary.json
```

The object is uncompressed JSON of envelope kind `boundary`, like a pool record. Its payload is the namespace ID (`namespace_id`), which a reader checks against the key, and the boundary (`boundary`), a tagged value like a manifest's `status`: `{"kind":"manifest","manifest_no":44}` for a number, or `{"kind":"retired"}`. The object follows four rules:

1. A missing object reads as `B = 0`.
2. `B` never decreases. A write of the object is a put-if-absent when the object is missing, or a compare-and-swap that raises the number.
3. Cleanup writes the object, and no publisher does.
4. Nothing deletes the object, including the cleanup of a retired namespace.

Every comparison `n <= B` below is true when the boundary is retired. (The retired state is an enum variant of its own, not a sentinel number.)

For example, `main` has manifests 1 to 45, a user snapshot on manifest 7, and a boundary of 30. Manifests 1 to 30 are dead. Manifest 7 still exists, and a read through the snapshot uses manifest 7, but no process treats manifest 7 as `main`'s newest manifest.

### Publishing a manifest

To publish a manifest at the next number `n`, including manifest 1 and a tombstone, a publisher:

1. Puts the manifest at `n` with put-if-absent.
2. If the put loses, finds the manifest that won and tries again, as in [Publishing a manifest](../specs/format.md#72-publishing-a-manifest).
3. If the put succeeds, reads the boundary. If `n` is above `B`, the publication succeeded. If `n` is at or below `B`, the put is treated as lost, like a put that found the number taken.

The publisher makes no assumption about how long it waited before step 1, or between step 1 and step 3. For example, a fold that has had manifest 40 for two days puts manifest 41. If other processes have since published manifests 41 to 45, and cleanup has deleted 41 to 44 and raised the boundary to 44, the put succeeds, but the boundary read returns 44, so the fold treats its put as lost.

This rule has one consequence. If a call's put is good (its manifest was the namespace's newest), and the call then stalls between its put and its boundary read while another process publishes a later manifest and a cleanup raises the boundary over the call's manifest, the call is told that it lost. For a create, that's `namespace_exists` for a namespace that this call created. Cleanup raises the boundary only over manifests older than the [grace window](../specs/glossary.md#grace-window), so this takes a wait longer than the grace window (24 hours) between two adjacent steps of one call.

Why read the boundary after the put? A put-if-absent at a number that cleanup emptied succeeds. If two creates of `sandbox` both found no namespace, and the second put manifest 1 after a request published manifest 2 and a collection deleted the first manifest 1, both creates would report that they created `sandbox` without the read.

### Cleaning up manifests

To delete the manifests that nothing needs, a namespace's collection:

1. Finds the namespace's newest manifest `N` (see [Finding the newest manifest](#finding-the-newest-manifest)). The collection never deletes `N`.
2. Lists the namespace's snapshots, after step 1.
3. Chooses the manifests to delete: every manifest below `N` that's older than the grace window and that no snapshot in the listing keeps.
4. If it chose any, raises the stored boundary to at least the highest chosen number, with a compare-and-swap that it retries until the stored boundary is that high.
5. Deletes exactly the manifests that it chose in step 3.

If the chosen manifests don't fit the collection's memory, the collection works through them in number order, a batch at a time, and raises the boundary to a batch's highest number before it deletes that batch.

Cleanup never deletes a manifest above the boundary, and raises the boundary to `N` or higher only to retire the namespace.

For example, `main`'s boundary is 30, its newest manifest is 45, and its snapshots keep manifests 7 and 38. Manifests 31 to 44 are older than the grace window. The collection chooses 31 to 37 and 39 to 44, raises the boundary to 44, and deletes those 13 manifests. Manifests 7 and 38 stay, and both are dead.

Why is the snapshot listing taken after the newest manifest is found? A snapshot is created on the manifest that's newest at that moment, so a snapshot that the listing misses keeps manifest `N` or a higher one. If the listing came first, a snapshot could be created on manifest 1 after the listing, a request could publish manifest 2, and the collection would find manifest 2 and delete manifest 1, which the snapshot keeps.

Why does a collection delete only the manifests that it chose, and not every manifest at or below the stored boundary? Suppose collection A finds manifest 1 as the newest and lists no snapshots. A snapshot is then created on manifest 1, a request publishes manifests 2 and 3, and collection B chooses manifest 2 and raises the boundary to 2. If A deleted every unlisted manifest at or below the stored boundary, it would delete manifest 1.

Why raise the boundary before the delete? If the collection deleted manifest 1 first, a create that put manifest 1 at the empty number before the raise would read `B = 0` and report success.

Why is the boundary never raised to the newest manifest? The next reader would find no manifest above the boundary of a live namespace and report a corrupt store.

Why is every write a compare-and-swap that raises the number? If one collection raised the boundary to 2 and another then wrote 1 with a plain put, the boundary would fall, and a publisher's put at the emptied number 2 would pass its boundary read.

Why does cleanup write the boundary, and no publisher? The boundary has to rise before a manifest is deleted, and cleanup deletes manifests. A reader lists the manifests above the boundary, so the boundary doesn't have to follow publications.

### Finding the newest manifest

A process that has nothing in memory for a namespace (a cold read, a collection, or a create) finds the newest manifest with a listing. The process:

1. Reads the boundary, `b`.
2. If `b` is retired, stops. The namespace is retired.
3. Lists the manifests numbered above `b` (a listing that starts after the key of manifest `b`).
4. If the listing is empty, reads the boundary again. If the boundary changed, starts again. If it's unchanged and `b = 0`, no namespace has the ID. If it's unchanged and `b` is above 0, the store is corrupt, because cleanup never leaves a live namespace without a manifest above its boundary.
5. Loads the highest-numbered manifest in the listing, `n`. If that manifest is gone, starts again.
6. Reads the boundary again. If `n` is at or below the boundary, starts again. Otherwise `n` is the newest manifest, and a tombstone at `n` means that the namespace is deleted.

For example, a cold read of `main` reads the boundary 44, lists manifests 45 and 46, loads 46, and reads the boundary again, which is still 44, so manifest 46 is the newest.

Why is the boundary read again after the highest manifest is loaded? Suppose a request reads the boundary 0 and lists manifest 1. A tombstone is then published as manifest 2, and the namespace is retired. If the request loaded manifest 1 and stopped there, it would take an active manifest as the newest manifest of a retired namespace. The last read finds the retired boundary, and the request starts again. (The second read after an empty listing covers the same race.)

### Checking a manifest that a process already has

A process that has a manifest in memory (a cached view or a [writer position](../specs/glossary.md#writer-position)) checks for a newer one (see [Read freshness](read-freshness.md)). The process:

1. Probes the next numbers after its manifest (the successor probe). If newer manifests exist, it loads the highest one found.
2. Reads the boundary. If the manifest in hand is at or below the boundary, it drops the manifest and finds the newest manifest from the start (see [Finding the newest manifest](#finding-the-newest-manifest)). Otherwise the manifest in hand is the newest.

A process that keeps a reference to its manifest, and has to read the manifest from the store again, does step 2 before that read. If the read finds the manifest missing, the process reads the boundary again. A manifest at or below the boundary sends the process to the newest manifest, and a missing manifest above the boundary is a corrupt store.

Why read the boundary after the probe? Suppose a process has manifest 1, another process publishes manifests 2 and 3, and a collection raises the boundary to 2 and deletes manifest 2. The probe finds no manifest 2, and without the boundary read, the process would take manifest 1 as the newest. With the read, the answer doesn't depend on how long ago the process last checked.

### Creating and forking a namespace

To create or fork a namespace, a call:

1. Finds the newest manifest for the new ID. A retired ID returns `namespace_deleted`. A namespace that's found returns `namespace_exists`, or `namespace_deleted` for a tombstone, unless the namespace is still being created. The call writes nothing in these cases.
2. Otherwise publishes manifest 1 as in [Publishing a manifest](#publishing-a-manifest). After a lost put, including a put at or below the boundary, the call starts again at step 1.

[Creating a namespace and the pool's member list](namespace-creation.md) describes the other objects that a create or fork writes, the order of those writes, and a namespace that's still being created.

A create or fork that's refused or fails can leave a manifest 1 at or below the boundary. No reader takes that manifest as the newest, and cleanup deletes the manifest.

For example, `main` has manifests 45 and 46 and a boundary of 44, and a create of `main` that started before `main` existed puts manifest 1 late. The put succeeds, because manifest 1 was deleted long ago, but the boundary read returns 44. So the put is lost, and the create finds manifest 46 and returns `namespace_exists`.

Why does the call look for the namespace before its first write? Publishing manifest 1 already determines whether the call created the namespace, but the first step keeps a call for a taken ID from writing anything, such as a fork's temporary snapshot.

### Retiring a namespace

"Retirement" is the point at which a deleted namespace stops being a member of its [pool](../specs/glossary.md#pool) (see [Retirement](../specs/format.md#95-retirement)). A tombstone is eligible for retirement when its retirement deadline has passed and no snapshot is on the namespace. To retire an eligible namespace, a collection:

1. If the namespace is a fork, deletes its fork snapshot on its source.
2. Deletes the namespace's member entry in its pool (see [Retiring a namespace](namespace-creation.md#retiring-a-namespace)).
3. Raises the boundary to retired.

After step 3, the namespace has no roots, so every manifest (the tombstone included), WAL object, segment, and snapshot record under its prefix is a candidate. Later collections delete each candidate under the ordinary age rules, in any order, including an object that a provider writes late (see [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules)). A collection keeps `boundary.json`, and it keeps an unrecognized key, as every collection does. A collection that stops before step 3 starts again at step 1, because deleting a missing object succeeds.

A request to a retired namespace returns `namespace_deleted`, as before the retirement, and so does a create or fork that uses a retired ID.

For example, `sandbox`, a fork of `main`, writes its last segments at 09:50 on Monday, is deleted at 10:00, and with the 24-hour grace window is eligible at 10:00 on Tuesday. The sweep at 03:00 on Wednesday deletes `sandbox`'s fork snapshot on `main` and its member entry, raises the boundary to retired, and deletes the manifests, the WAL objects, and the segments, which are all more than 24 hours old. Later sweeps list `sandbox`, read its boundary, and find nothing else under the prefix.

Why is the fork snapshot deleted before the boundary is raised? The tombstone's fork basis is the record of which snapshot to delete, and the tombstone stays until the boundary is retired. If the boundary were raised first and the collection stopped, a later collection could delete the tombstone under the age rules. The fork snapshot would then stay on the source until the source's snapshot cleanup found that the snapshot's target has no manifest (see [Snapshot cleanup](../specs/format.md#117-snapshot-cleanup)).

Why is the boundary raised to retired, and not to the tombstone's number? A boundary at the tombstone's number would leave no manifest above the boundary, and a reader would report a corrupt store.

Why is the boundary object kept? It's the record that the ID was used, which a create or a late publication finds when it reads the boundary.

### The pool's collection

A [pool](../specs/glossary.md#pool)'s collection lists the pool's member entries and finds the newest manifest of each listed namespace (see [The pool's collection](namespace-creation.md#the-pools-collection)). A member's content references count as roots until its retirement deletes its member entry. A listed namespace whose boundary is retired has no roots, and the collection deletes its member entry.

When a fork snapshot's target is retired, the target has no roots. The source's snapshot cleanup deletes the snapshot, as it does for a target that has no manifest.

### Time rules

The design has no rule that a manifest publication's basis was confirmed within `READ_REVALIDATION_BOUND_MS` before its put, and a process's check of whether its manifest is the newest doesn't depend on that bound. These time rules stay:

- A WAL writer that confirmed its tip within `WAL_PUBLISH_BUDGET_MS` puts the next WAL object without a check.
- The retirement deadline is computed as in [Retirement](../specs/format.md#95-retirement): the grace window (24 hours) after the deletion, which covers a tombstone publication that's applied late and the readers after the tombstone. A manifest put that a provider applies after the retirement doesn't depend on the deadline. Every manifest number is at or below a retired boundary, so the put's publisher counts the put as lost, and no reader takes that manifest as the newest.
- The age rules for segments, content objects, and upload records.

## Costs

This section lists the design's costs.

- Each manifest publication sends one conditional read of the boundary after its put (a 304 response when the boundary is unchanged), in place of the hint's raise.
- Finding the newest manifest with nothing in memory sends four requests, as with the hint: a boundary read, one listing, one manifest load, and a conditional boundary read. The listing returns the manifests published since cleanup last raised the boundary, however many older manifests snapshots and forks keep.
- A create or fork for a free ID makes those reads before its first write.
- Each successor probe sends one conditional read of the boundary. A cached view sends the probe at most once per manifest check interval (1 second by default) for each namespace in each process.
- The prefix of a retired namespace keeps its boundary object. So the reference server's sweep lists every retired namespace and runs the namespace's steps, and each retired namespace costs every sweep 9 requests when the namespace is its pool's first member: one read of the boundary for each of the metadata call, the retention advance, the namespace's collection, and the pool's collection, and the five listings of the namespace's collection. A retired fork costs 8, because its pool's collection doesn't read it, and the grep index call adds one boundary read when the server maintains grep. That cost grows with the number of retired namespaces. A retired namespace stores no tombstone and no segments.

## Changes to the specs and glossary

The storage format changes in these sections:

- [Namespaces and identity](../specs/format.md#11-namespaces-and-identity) says that the boundary object records a used ID, and that a retired ID returns `namespace_deleted`.
- [Storage layout](../specs/format.md#21-storage-layout), [Object roles and mutability](../specs/format.md#22-object-roles-and-mutability), [Family versions](../specs/format.md#a1-family-versions), [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads), and [Object keys](../specs/format.md#a8-object-keys) replace the hint with the boundary object.
- [The current manifest and WAL tip](../specs/format.md#24-the-current-manifest-and-wal-tip), [A summary of the log protocol](../specs/format.md#26-a-summary-of-the-log-protocol), and [Resolving the metadata basis](../specs/format.md#41-resolving-the-metadata-basis) replace the hint, its raise, and the corrupt-store rule for a missing hinted manifest with [Finding the newest manifest](#finding-the-newest-manifest).
- [Required operations](../specs/format.md#31-required-operations) says that a prefix listing can start after a given key.
- [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) adds the boundary read to the successor probe.
- [Publication and group commit](../specs/format.md#63-publication-and-group-commit) and [Publishing a manifest](../specs/format.md#72-publishing-a-manifest) replace the hint raise and the basis rule under `READ_REVALIDATION_BOUND_MS` with the boundary read after the put, and [Publication and collection timing](../specs/format.md#c1-publication-and-collection-timing) removes a publication's basis from that constant's meaning.
- [Creating a namespace](../specs/format.md#91-creating-a-namespace), [Forking a namespace](../specs/format.md#92-forking-a-namespace), and [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations) replace the hint step with the lookup before the first write.
- [Retirement](../specs/format.md#95-retirement), [One complete collection](../specs/format.md#111-one-complete-collection), [Reference roots](../specs/format.md#112-reference-roots), [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules), and [Retiring an eligible tombstone](../specs/format.md#118-retiring-an-eligible-tombstone) describe the manifest cleanup, the raise to retired, and the cleanup of a retired prefix.
- [Snapshot cleanup](../specs/format.md#117-snapshot-cleanup) finds a fork's target without a hint and treats a retired target as a target with no manifest, and [Content roots](../specs/format.md#119-content-roots) describes a retired member.
- [Alternatives considered](../specs/format.md#13-alternatives-considered) removes the hint raised at most once per second.

The API spec's [error table](../specs/api.md#3-standard-error-contract), [Creating and forking namespaces](../specs/api.md#creating-and-forking-namespaces), [`GET /v0/namespaces/{ns}`](../specs/api.md#62-get-v0namespacesns), and [Conformance requirements](../specs/api.md#7-conformance-requirements) change. So do [Read freshness](read-freshness.md), [Pools](pools.md), [Maintenance hosting](maintenance-hosting.md), [The WAL implementation](wal-boundary.md), the [architecture](../specs/architecture.md) overview, and the [self-hosting guide](../../crates/loonfs-server/docs/self-hosting.md#background-maintenance).

In the glossary, "Hint" is removed, an entry defines "Manifest boundary", and "Manifest", "Control object", "Namespace tombstone", "Namespace retirement", "Revalidation bound", and "Sweep" change.

## Alternatives considered

- **A pointer object that each publication raises.** This is the hint. It costs a write for each publication, on an object that some providers let a client write about once per second. The larger cost is that correctness rules depend on a helper for speed: cleanup keeps every manifest at or above the pointer's value, a missing pointed-to manifest is a corrupt store, and a publisher or reader has to act within a time bound of its last check. A retirement built on the pointer has to delete manifest 1 before the pointer, because a new pointer to a manifest 1 that's still present would make a retired namespace look active.
- **A retirement record outside the namespace's prefix.** A record such as `retirements/{namespace_id}.json` records the retired ID, and the prefix empties. But the hint stays, retirement needs four steps in a set order, and a create or fork has to read the record after its hint step. A request to a retired namespace returns `namespace_not_found`, because telling a retired namespace from a missing one would cost every request another read.
- **The boundary object outside the namespace's prefix.** With a key such as `boundaries/{namespace_id}.json`, the prefix of a retired namespace empties, and the sweep stops listing the namespace. But the store would then record a namespace under two prefixes, and a listing of `namespaces/` would no longer show every ID that was used.
- **Listing every manifest of the namespace to find the newest.** A namespace keeps one manifest for each snapshot and each fork, so the folder can contain tens of thousands of manifests, and a cold read would list every manifest.
- **Deleting a retired namespace's boundary so that the ID can be used again.** The prefix empties, and a later create can use the ID. Such a design has to prevent four failures. A create that put manifest 1 before the retirement reads the missing boundary as 0 and reports success for a namespace that's gone. A create that looked for the ID before the namespace was first created puts manifest 1 after the deletion and creates an active namespace under the retired ID, and nothing in the store tells that call from a new create. A request that loaded manifest 1 during the retirement reads the missing boundary as 0, takes manifest 1 as the newest, and puts an active manifest 2. A request that already had manifest 1 puts manifest 2 with no read at all, because no rule bounds how long a process keeps a manifest. The merge index, the chain creators, and the cached heads would also refer to two namespaces by one ID.
- **Letting publishers raise the boundary too.** A publisher could raise the boundary to its predecessor's number after each publication. That's a write for each publication, the same cost as the hint, and no reader needs that write, because the listing above the boundary finds the newer manifests. It would also make the consequence in [Publishing a manifest](#publishing-a-manifest) common: a publisher whose put was good would be told that it lost whenever the next publication raised the boundary before the publisher's read.
