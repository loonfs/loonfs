# Retiring a namespace

This note describes how LoonFS retires a deleted [namespace](../specs/glossary.md#namespace), whether a [fork](../specs/glossary.md#fork) or a namespace created without a source. A deleted namespace is retired once its retirement grace has passed and no pin is on the namespace. To retire a namespace, a collection writes a small record of the namespace's ID and its pool's ID outside the namespace's prefix (`namespaces/{namespace_id}/`), deletes the namespace's manifest 1, and then deletes the namespace's hint. Later collections delete every object under the namespace's prefix under the ordinary age rules. So a retired namespace stores nothing under its own prefix, a sweep stops listing the namespace, and the namespace's ID still can't be used again.

## The problem

This section describes what a deleted namespace would cost if its hint and its tombstone stayed in place.

A namespace ID identifies one namespace from its creation to its deletion, and it can't be reused (see [Namespaces and identity](../specs/format.md#11-namespaces-and-identity)). A create or fork writes the [hint](../specs/glossary.md#hint) with put-if-absent, and when the hint exists, the call reads the current manifest through the hint and returns `namespace_exists`, or `namespace_deleted` for a [namespace tombstone](../specs/glossary.md#namespace-tombstone) (see [Creating a namespace](../specs/format.md#91-creating-a-namespace)).

If a deleted namespace's hint and tombstone recorded its ID, both objects would have to stay as long as the store does. The tombstone is the namespace's current manifest, so the tombstone would stay a root and keep every metadata segment that it lists and that the namespace wrote.

The reference server's [sweep](../specs/glossary.md#sweep) runs a [collection](../specs/glossary.md#collection) of every namespace under `namespaces/`. A collection of such a namespace would read the hint and the tombstone, list each of the five candidate families (manifests, WAL objects, metadata segments, extension segments, and pins), and delete nothing. So the work of every sweep would grow with the number of namespaces that were ever deleted. An application that creates a fork for each task, merges the fork into its source, and deletes the fork never stores more live data, but each sweep would take longer than the one before.

LoonFS can't delete those objects without a record of the ID, because three parts of LoonFS depend on a namespace ID not being used twice. The source's [merge index](../specs/glossary.md#merge-index) keys its rows by the fork's namespace ID (see [Merges](../specs/format.md#merges)). A chain's layout records the [chain's creator](../specs/glossary.md#chain-creator) by namespace ID, and the creator alone extends the chain in place (see [File contents and ownership](../specs/format.md#15-file-contents-and-ownership)). LoonFS caches a namespace's head and its WAL-tail projections under the namespace ID and the manifest and WAL numbers, which is correct only while one ID and one number identify one object.

## The design

This section describes the retirement record, the collections that retire a namespace and empty its prefix, creates that reuse a retired ID, pools, and the API.

### The retirement record

A "retirement record" records that a namespace ID belonged to a [namespace](../specs/glossary.md#namespace) that has been [retired](../specs/glossary.md#namespace-retirement). Its key is `retirements/{namespace_id}.json`, for example `retirements/sandbox.json`.

The record is a [control object](../specs/glossary.md#control-object) of envelope kind `retirement`, in uncompressed JSON like a pin record or a pool record. Its payload is the namespace ID (`namespace_id`) and the ID of the namespace's [pool](../specs/glossary.md#pool) (`pool_id`), which is the pool that the namespace's manifests record. A reader checks the namespace ID against the key.

The collection that retires the namespace writes the record once, with put-if-absent, and every collector builds the same bytes for one namespace. Nothing changes or deletes the record. The key is under neither `namespaces/` nor `pools/`, so no [sweep](../specs/glossary.md#sweep) and no [collection](../specs/glossary.md#collection) lists the record. Three requests read the record: a create or fork, after its hint step, a collection of a namespace that has no current manifest, and a pool's collection when the pool's first member has no current manifest.

Why isn't the record stored under the namespace's own prefix? A sweep lists every prefix under `namespaces/` that contains an object, so a record there would keep the retired namespace in every sweep.

Why isn't the record stored under the pool's prefix? A namespace ID is unique in the store, not in a pool, so a create or fork that used the same ID in another pool wouldn't know which pool's prefix to read.

Why does the payload include the two IDs and nothing more? No rule reads anything else about a retired namespace. The pool's collection reads the pool ID (see [A pool whose members are all retired](#a-pool-whose-members-are-all-retired)), and no request needs the namespace's deletion time after the retirement.

### Retiring an eligible namespace

A namespace is eligible for retirement when the collection's clock has reached the tombstone's retirement deadline (`deleted_at_ms` plus the larger of the configured [grace window](../specs/glossary.md#grace-window) and `NAMESPACE_RETIREMENT_GRACE_WINDOW_MS`), and the complete pin listing has no pin on the namespace (see [Retirement](../specs/format.md#95-retirement)). With the default grace window of one hour, a namespace is eligible one hour after it's deleted. Each [fork](../specs/glossary.md#fork) pins its source, so a pool's first member becomes eligible only after every fork in the pool is retired.

A [collection](../specs/glossary.md#collection) reads its roots first: the current manifest through the [hint](../specs/glossary.md#hint), and then the complete pin listing. When the namespace's [tombstone](../specs/glossary.md#namespace-tombstone) is eligible, the collection retires the namespace before it lists any candidate family. The collection:

1. If the namespace is a fork, deletes the source pin that the tombstone's fork basis refers to. (A namespace created without a source has no source pin.)
2. Writes the namespace's retirement record with put-if-absent. If the record already exists, an earlier collection wrote the record.
3. Deletes the namespace's manifest 1. A delete of a missing object succeeds, so this step is the same whether or not an earlier collection already deleted manifest 1.
4. Deletes the namespace's hint.

The namespace stops being a member of its [pool](../specs/glossary.md#pool) when the namespace is retired, and no retirement deletes content. After step 4 the namespace has no hint, so a request finds no namespace, and the collection continues as a collection of a retired namespace (see [Collecting a retired namespace](#collecting-a-retired-namespace)).

Every step can run again, and no progress is stored. After a stop at step 1 or step 2, the next collection finds the hint and the tombstone and starts again at step 1. After a stop at step 3, the hint is still there. If the hint points to a manifest above 1, the next collection finds the tombstone through the hint and starts again at step 1, and the delete of manifest 1 then finds nothing. If the hint points to manifest 1, the next collection finds no current manifest, reads the record, and continues as a collection of a retired namespace, which deletes the hint. After a stop at step 4, the next collection finds a record and no hint. A put of the record, or a delete of manifest 1, that fails or has an unknown outcome ends the collection with that error before the hint is deleted, and the next collection starts again.

Between step 3 and step 4, a hint that points to manifest 1 leads to no manifest, so a request returns `namespace_not_found`. A request to a retired namespace returns the same error, and the record already exists. No pin is on an eligible namespace, so no request reads manifest 1 through a pin at that point. The tombstone is never manifest 1, because a creation writes manifest 1 and a deletion publishes a later manifest.

Why is the record written before the hint is deleted? If the collection deleted the hint first and stopped there, the prefix would have no hint and no record, like the prefix of a namespace that was never created. A create could then use the ID again, and no collection would delete the objects left under the prefix. For a pool's first member, the pool's collection would also treat the pool as a pool whose create call failed, and the collection would delete the pool record while content was still under the pool's prefix.

Why does the collection delete manifest 1 before the hint? A create or fork with a retired ID writes a hint that points to manifest 1 before it reads the record, and that hint stays after the call returns `namespace_deleted`. A request finds the current manifest by loading the hinted manifest and checking the next numbers until one is missing (see [Resolving the metadata basis](../specs/format.md#41-resolving-the-metadata-basis)). A deleted namespace can have manifest 1 and no manifest 2. For example, a pin on `sandbox`'s manifest 1 keeps manifest 1 after a collection deletes manifest 2, and manifest 1 is still under the prefix when the pin expires and `sandbox` becomes eligible. If the collection deleted the hint while manifest 1 was under the prefix, a request that read the new hint would load manifest 1, find no manifest 2, and take manifest 1 as the current manifest of an active namespace. A second create would return `namespace_exists`, a writer could commit, and the next collection would find a current manifest and never read the record. With manifest 1 deleted first, the new hint leads to no manifest. No call writes manifest 1 after that. A create or fork that passes its hint step once the record exists reads the record before it writes manifest 1. A call that passed its hint step when the namespace was created stopped writing within its metadata publication budget, and the retirement grace is longer than that budget (see [Clock and operation assumptions](../specs/format.md#114-clock-and-operation-assumptions)). The order is creation's in reverse: a create writes the hint and then manifest 1, and retirement deletes manifest 1 and then the hint.

Why does the collection retire the namespace before it lists the candidate families? After step 4 nothing under the prefix is a root, so the same collection can delete the namespace's objects that are already old enough.

### Collecting a retired namespace

When a [collection](../specs/glossary.md#collection) finds no current manifest (the prefix has no [hint](../specs/glossary.md#hint), or its hint's manifest 1 is missing), the collection reads the retirement record of the namespace ID:

- **No record.** The namespace was never created, or its creation didn't finish. The collection deletes nothing and returns `namespace_not_found`.
- **A record.** The namespace is retired. The collection deletes the hint if the prefix has one, and lists the five candidate families with no roots.

So every manifest (the tombstone included), WAL object, metadata segment, extension segment, and pin under the prefix is a candidate. The collection deletes a manifest, a WAL object, or a pin at a provider age of at least `T`, and a segment at a provider age strictly greater than 24 hours (see [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules)). It keeps an unrecognized key, as every collection does.

The collection that retires a namespace continues in this way after step 4. Once the prefix is empty, a [sweep](../specs/glossary.md#sweep)'s listing of `namespaces/` doesn't include the namespace, and no collection of the namespace runs. If a request that a provider applies late writes an object under the prefix, the next sweep lists the prefix again, and its collection deletes the object under its age rule.

For example, `sandbox` is a [fork](../specs/glossary.md#fork) of `main`. `sandbox` writes its last metadata segments at 09:50 on Monday, is merged into `main`, and is deleted at 10:00. With the default grace window, `sandbox` is eligible for retirement at 11:00. The collection in the sweep at 03:00 on Tuesday deletes `sandbox`'s pin on `main`, writes `retirements/sandbox.json`, deletes manifest 1 and then the hint, and deletes the other manifests and the WAL objects, which are older than one hour. The segments are 17 hours old, so the collection keeps the segments. The sweep at 03:00 on Wednesday lists `sandbox` because the segments are still there, and its collection finds no hint, reads the record, and deletes the segments, which are now older than 24 hours. Later sweeps don't list `sandbox`.

Why does the collection wait for the age rules instead of deleting a retired namespace's objects at once? The age rules already delete every object under the prefix, so deleting the objects at retirement would be a second way to delete the same objects.

Why does the collection read the record only when it finds no current manifest? A retired namespace has no hint, or a hint without manifest 1. So a namespace that has a current manifest isn't retired, and the collection of that namespace makes no extra request.

### Creating a namespace with a retired ID

A create and a fork write the [hint](../specs/glossary.md#hint) first, with put-if-absent (see [Creating a namespace](../specs/format.md#91-creating-a-namespace) and [Forking a namespace](../specs/format.md#92-forking-a-namespace)). After the hint step, and before the call writes a pool record or manifest 1, the call reads the retirement record of the ID. If the record exists, the call returns `namespace_deleted`, and a fork deletes its source pin, as after any refused installation (see [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations)).

The check is safe because a hint put succeeds only when the prefix has no hint, and a collection deletes a retired namespace's hint only after writing the namespace's retirement record. So a call whose hint put succeeds for a retired ID finds the record afterwards. A call that finds the namespace's own hint reads the [tombstone](../specs/glossary.md#namespace-tombstone) and returns `namespace_deleted`, as for any deleted namespace.

A call for a retired ID can leave a hint in two ways: the call stops after its hint step, or the record refuses the call. Retirement deleted manifest 1 before it deleted the namespace's own hint, so neither hint has manifest 1 behind it. A request that reads the prefix through that hint finds no namespace and returns `namespace_not_found`. A later create or fork that finds that hint still reads the record and returns `namespace_deleted`. The next collection finds no current manifest, reads the record, and deletes the hint.

For example, after `sandbox` is retired, a fork of `main` with the new namespace ID `sandbox` writes its [fork pin](../specs/glossary.md#snapshot) on `main` and the hint `namespaces/sandbox/hint.json`. The fork reads `retirements/sandbox.json`, returns `namespace_deleted`, and deletes its fork pin. Until the next collection of `sandbox` deletes the hint, a request to `sandbox` returns `namespace_not_found`.

Why does the call read the record after the hint step and not before? If a call found no record while `sandbox` was deleted but not yet retired, and a collection then retired `sandbox`, the call's hint put would succeed, and the call would create a namespace with the retired ID. A read after the hint step also covers both ways that a call reaches manifest 1: a hint put that succeeded, and an existing hint without manifest 1.

### A pool whose members are all retired

A [pool](../specs/glossary.md#pool)'s collection reads the pool record and then the current manifest of the pool's first member (see [Content roots](../specs/format.md#119-content-roots)). When the first member has no current manifest, the collection reads the first member's retirement record:

- **A record whose pool ID is this pool's ID.** Every member of the pool is retired. Each fork pins its source, and a namespace can't be retired while a pin is on it, so the first member is retired last. The collection reads no member roots, deletes the pool's content and temporary objects under the age rule, applies the rules in [Upload-session cleanup](../specs/format.md#116-upload-session-cleanup), and keeps the pool record.
- **No record, or a record whose pool ID is another pool's ID.** The pool's create call failed, or lost to another create call for the same namespace ID. After the pool record's creation grace, the collection deletes the pool record and lists nothing.

A first member that is eligible for retirement but not yet retired still has its [hint](../specs/glossary.md#hint) and its [tombstone](../specs/glossary.md#namespace-tombstone), and the pool's collection takes no roots from that tombstone.

For example, `main` was created with `pool_3s0drmj8em84yb4mnetqkz2j0x`, `sandbox` is a fork of `main`, and `trial` is a fork of `sandbox`. All three are deleted, and `trial`, `sandbox`, and `main` are retired in that order. A collection of the pool then finds no current manifest for `main`, finds the pool's own ID in `retirements/main.json`, and deletes the pool's remaining content objects under the age rule. Once the three prefixes are empty, the store contains `pools/pool_3s0drmj8em84yb4mnetqkz2j0x/pool.json` and the three retirement records, and nothing under `namespaces/main/`, `namespaces/sandbox/`, or `namespaces/trial/`.

Why does the record include the pool's ID? Two create calls for one namespace ID each generate a pool ID, and one call fails or loses (see [Creating a namespace](../specs/format.md#91-creating-a-namespace)). The losing call's pool record identifies the same first member, and once the other call's namespace is retired, a retirement record for that ID exists. Without the pool ID, the losing call's pool would look like a pool whose members are all retired, and its collection would keep the pool record instead of deleting the record after its creation grace.

Why does the pool keep its pool record? A later collection of the pool needs the record to find that the pool has no members, for the reason in [Content roots](../specs/format.md#119-content-roots).

### The API

A request to a retired [namespace](../specs/glossary.md#namespace) finds no [hint](../specs/glossary.md#hint), or a hint without manifest 1, so the request returns `namespace_not_found` (404), like a request to a namespace that was never created. Before a deleted namespace's retirement, a request to the namespace returns `namespace_deleted` (410). A create or fork that uses a retired ID returns `namespace_deleted`. A `gc` job for a retired namespace runs the collection in [Collecting a retired namespace](#collecting-a-retired-namespace), and every other job returns `namespace_not_found`. The design adds no field to any request or response.

Why doesn't a request to a retired namespace return `namespace_deleted`? Every request that finds no hint would have to read the retirement record to tell a retired namespace from a namespace that was never created. The difference matters for a create, which could use the ID again, and a create reads the record.

## Costs

This section lists the costs, compared with a deleted namespace that keeps its hint and its tombstone.

- Each retired namespace has a retirement record of under 350 bytes (about 215 bytes plus the namespace ID, which is at most 128 bytes). The number of records grows with the number of retired namespaces, so a store that retires 30,000 namespaces a day adds about 11 million records a year, under 4 GB. No listing includes the records, so they add no request to a sweep.
- Each create and each fork sends one more GET, for the retirement record.
- The collection that retires a namespace sends one more PUT and two more DELETEs.
- Until a retired namespace's prefix is empty, each collection of the prefix sends a GET for the hint, a GET for the record, and one listing for each of the five families. In the `sandbox` example, that's one collection after the retirement.
- A pool's collection sends one more GET when the pool's first member has no current manifest.
- A pool whose members are all retired stores its pool record and its members' retirement records.
- A request to a retired namespace returns `namespace_not_found`, not `namespace_deleted`.
- A pool's collection that read a member's tombstone before the member was retired can find that one of the member's layout segments was deleted when the collection reads that segment's extent filter. (A retired namespace's segments are deleted once they're older than 24 hours.) That collection ends with an error and deletes no content, and the next collection of the pool takes no roots from the retired namespace.

## Changes to the specs and glossary

These sections of the storage format change:

- [Namespaces and identity](../specs/format.md#11-namespaces-and-identity) says that a namespace tombstone stays the current manifest until the namespace is retired, and that a retired ID can't be used again.
- [Family versions](../specs/format.md#a1-family-versions), [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads), and [Object keys](../specs/format.md#a8-object-keys) add the `retirement` kind, its payload, and its key.
- [Creating a namespace](../specs/format.md#91-creating-a-namespace) and [Forking a namespace](../specs/format.md#92-forking-a-namespace) add the read of the retirement record after the hint step.
- [Deleting a namespace](../specs/format.md#94-deleting-a-namespace), [Retirement](../specs/format.md#95-retirement), [Fork dependencies after deletion](../specs/format.md#96-fork-dependencies-after-deletion), and [Retiring an eligible tombstone](../specs/format.md#118-retiring-an-eligible-tombstone) describe the four steps and what a retired namespace stores.
- [One complete collection](../specs/format.md#111-one-complete-collection), [Reference roots](../specs/format.md#112-reference-roots), and [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules) describe the collection of a retired namespace, which has no roots.
- [Content roots](../specs/format.md#119-content-roots) says that the first member's retirement record tells a retired first member from a failed create when the first member has no current manifest, and that a fork pin's target without a manifest can be a retired fork.

In the glossary, "Namespace retirement", "Namespace tombstone", "Sweep", and "Control object" change, and an entry defines "Retirement record". In the API spec, the [error table](../specs/api.md#3-standard-error-contract) rows for `namespace_not_found` and `namespace_deleted`, [`GET /v0/namespaces/{ns}`](../specs/api.md#62-get-v0namespacesns), [`POST /forks`](../specs/api.md#613-post-forks), and [Garbage collection jobs](../specs/api.md#garbage-collection-jobs) change for a retired namespace. [Background maintenance](../../crates/loonfs-server/docs/self-hosting.md#background-maintenance) in the self-hosting guide says that a sweep lists a deleted namespace until the namespace's prefix is empty.

## Alternatives considered

- **Keeping the hint and the tombstone, and deleting the segments.** A retirement stops protecting the tombstone's segments and leaves two objects for each retired namespace. The sweep still lists and collects every retired namespace, so the work of a sweep still grows with the number of namespaces that were ever deleted.
- **Using a retired namespace's ID again.** With no record, a retired namespace leaves nothing, and a later create or fork can use its ID. A second fork with that ID would continue the first fork's range in the source's merge index. A second chain creator with that ID would extend chains whose other layouts the second namespace never read, so one content ID could describe two byte sequences. A process could return the first namespace's cached head for the second namespace. Each of the three would have to identify a namespace by an ID that LoonFS generates.
- **Removing the forks whose ID LoonFS generated, and no others.** A generated ID isn't generated twice, so a retired fork with a generated ID needs no record. A fork whose ID the caller chose keeps its hint and its tombstone. A fork then ends in one of two ways, depending on how its ID was chosen, and an application that chooses its fork IDs keeps the sweep cost.
- **Keeping the hint and the tombstone of a pool's first member.** The pool's collection reads the first member's tombstone to tell a pool whose members are all retired from a pool whose create call failed, so the record needs no pool ID and the pool's collection reads no retirement record. A deleted namespace then ends in one of two ways, depending on whether it was created with a source. A store that creates and deletes many pools (for example, one pool for each customer project) keeps a hint, a tombstone, and the tombstone's segments for each deleted first member, and every sweep collects each of those namespaces.
- **Deleting the hint in the call that the record refuses.** A call that stops between its hint put and that delete still leaves the hint, so retirement would still delete manifest 1 first, and the call's delete would remove the hint one collection earlier and no more.
- **Reading the retirement record whenever a request resolves a namespace through its hint.** No hint for a retired ID could then lead to a manifest, but every resolution of every namespace would send one more GET.
