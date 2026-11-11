# Retiring a fork

This note describes how LoonFS retires a deleted [fork](../specs/glossary.md#fork). A deleted fork is retired once its retirement grace has passed and no pin is on the fork. Retiring a fork writes a small record of the fork's namespace ID outside the fork's prefix (`namespaces/{namespace_id}/`), and deletes the fork's hint. Later collections delete every object under the fork's prefix under the ordinary age rules. So a retired fork stores nothing under its own prefix, a sweep stops listing the fork, and the fork's ID still can't be used again.

## The problem

This section describes what a deleted fork would cost if its hint and its tombstone stayed in place.

A namespace ID identifies one namespace from its creation to its deletion, and the ID can't be reused (see [Namespaces and identity](../specs/format.md#11-namespaces-and-identity)). A create or fork writes the [hint](../specs/glossary.md#hint) with put-if-absent, and when the hint exists, the call reads the current manifest through the hint and returns `namespace_exists`, or `namespace_deleted` for a [namespace tombstone](../specs/glossary.md#namespace-tombstone) (see [Creating a namespace](../specs/format.md#91-creating-a-namespace)).

If a deleted fork's hint and tombstone recorded its ID, both objects would have to stay as long as the store does. The tombstone is the fork's current manifest, so it would stay a root and keep every metadata segment that it lists and that the fork wrote.

The reference server's [sweep](../specs/glossary.md#sweep) runs a [collection](../specs/glossary.md#collection) of every namespace under `namespaces/`. A collection of such a fork would read the hint and the tombstone, list the fork's pins and each of the five candidate families (manifests, WAL objects, metadata segments, extension segments, and pins), and delete nothing. So the work of every sweep would grow with the number of forks that were ever deleted. An application that creates a fork for each task, merges the fork into its source, and deletes the fork never stores more live data, but each sweep would take longer than the one before.

LoonFS can't delete those objects without a record of the ID, because three parts of LoonFS depend on a namespace ID not being used twice. The source's [merge index](../specs/glossary.md#merge-index) keys its rows by the fork's namespace ID (see [Merges](../specs/format.md#merges)). A chain's layout records the [chain's creator](../specs/glossary.md#chain-creator) by namespace ID, and the creator alone extends the chain in place (see [File contents and ownership](../specs/format.md#15-file-contents-and-ownership)). LoonFS caches a namespace's head and its WAL-tail projections under the namespace ID and the manifest and WAL numbers, which is correct only while one ID and one number identify one object.

## The design

This section describes the retirement record, the collections that retire a fork and empty its prefix, creates that reuse a retired ID, a pool's first member, and the API.

### The retirement record

A "retirement record" records that a namespace ID belonged to a [fork](../specs/glossary.md#fork) that has been [retired](../specs/glossary.md#namespace-retirement). Its key is `retirements/{namespace_id}.json`, for example `retirements/sandbox.json`.

The record is a [control object](../specs/glossary.md#control-object) of envelope kind `retirement_record`, in uncompressed JSON like a pin record or a pool record. Its payload is the namespace ID (`namespace_id`) and nothing else, and a reader checks that the payload's ID matches the key.

The collection that retires the fork writes the record once, with put-if-absent, and every collector builds the same bytes for one fork. Nothing changes or deletes the record. The key is under neither `namespaces/` nor `pools/`, so no [sweep](../specs/glossary.md#sweep) and no [collection](../specs/glossary.md#collection) lists the record. Two requests read the record: a create or fork, after its hint step, and a collection of a namespace that has no current manifest.

Why isn't the record stored under the fork's own prefix? A sweep lists every prefix under `namespaces/` that contains an object, so a record there would keep the retired fork in every sweep.

Why isn't the record stored under the pool's prefix? A namespace ID is unique in the store, not in a [pool](../specs/glossary.md#pool). A create without a source, or a fork in another pool, that used the same ID wouldn't know which pool's prefix to read.

Why does the payload include the ID and nothing more? No rule reads anything else about a retired fork, and no request needs the fork's deletion time after the retirement.

### Retiring an eligible fork

A fork is eligible under the rule in [Retirement](../specs/format.md#95-retirement): the collection's clock has reached the tombstone's retirement deadline (`deleted_at_ms` plus the larger of the configured [grace window](../specs/glossary.md#grace-window) and `NAMESPACE_RETIREMENT_GRACE_WINDOW_MS`), and the complete pin listing has no pin on the fork. With the default grace window of one hour, a fork is eligible one hour after it's deleted.

A [collection](../specs/glossary.md#collection) reads its roots first: the current manifest through the [hint](../specs/glossary.md#hint), and then the complete pin listing. When the namespace is a [fork](../specs/glossary.md#fork) whose [tombstone](../specs/glossary.md#namespace-tombstone) is eligible, the collection retires the fork before it lists any candidate family. The collection:

1. Deletes the source pin that the tombstone's fork basis refers to.
2. Writes the fork's retirement record with put-if-absent. If the record already exists, an earlier collection wrote the record.
3. Deletes the fork's hint.

The fork stops being a member of its [pool](../specs/glossary.md#pool) at step 1, and no retirement deletes content. After step 3 the fork has no hint, so a request finds no namespace, and the collection continues as a collection of a retired fork (see [Collecting a retired fork](#collecting-a-retired-fork)).

Every step can run again, and no progress is stored. After a stop at step 1 or step 2, the hint and the tombstone are still there, so the next collection starts again at step 1, where the pin is already gone. After a stop at step 3, the next collection finds a record and no hint (see [Collecting a retired fork](#collecting-a-retired-fork)). A put of the record with an unknown outcome ends the collection with that error before the hint is deleted, and the next collection writes or finds the record.

Why is the record written before the hint is deleted? If the collection deleted the hint first and stopped there, the prefix would have no hint and no record, like the prefix of a namespace that was never created. A create could then use the ID again, and no collection would delete the objects left under the prefix.

Why does the collection retire the fork before it lists the candidate families? After step 3 nothing under the prefix is a root, so the same collection can delete the fork's objects that are already old enough.

### Collecting a retired fork

When a [collection](../specs/glossary.md#collection) finds no current manifest (the prefix has no [hint](../specs/glossary.md#hint), or its hint's manifest 1 is missing), the collection reads the retirement record of the namespace ID:

- **No record.** The namespace was never created, or its creation didn't finish. The collection deletes nothing and returns `namespace_not_found`.
- **A record.** The namespace is a retired [fork](../specs/glossary.md#fork). The collection deletes the hint if the prefix has one, and lists the five candidate families with no roots.

So every manifest (the tombstone included), WAL object, metadata segment, extension segment, and pin under the prefix is a candidate. The collection deletes a manifest, a WAL object, or a pin once its provider age is at least `T`, and a segment once its provider age is strictly greater than 24 hours (see [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules)). It keeps an unrecognized key, as every collection does. (The prefix contains a hint only when a create or fork used the retired ID and stopped after its hint step.)

The collection that retires a fork continues in this way after step 3. Once the prefix contains no object, a [sweep](../specs/glossary.md#sweep)'s listing of `namespaces/` doesn't include the fork, and no collection of the fork runs. If an object is written under the prefix later (for example, by a request that a provider applies late), the next sweep lists the prefix again, and the collection deletes the object under its age rule.

For example, `sandbox` is a fork of `main`. `sandbox` writes its last metadata segments at 09:50 on Monday, is merged into `main`, and is deleted at 10:00. With the default grace window, `sandbox` is eligible for retirement at 11:00. The sweep at 03:00 on Tuesday collects `sandbox`. The collection deletes `sandbox`'s pin on `main`, writes `retirements/sandbox.json`, deletes the hint, and deletes the manifests and WAL objects, which are older than one hour. The segments are 17 hours old, so the collection keeps the segments. The sweep at 03:00 on Wednesday lists `sandbox` because the segments are still there, and its collection finds no hint, reads the record, and deletes the segments, which are now older than 24 hours. Later sweeps don't list `sandbox`.

Why does the collection wait for the age rules instead of deleting a retired fork's objects at once? The age rules already delete every object under the prefix, so deleting the objects at retirement would be a second way to delete the same objects.

Why does the collection read the record only when it finds no current manifest? A namespace that has a current manifest isn't a retired fork, so its collection makes no extra request.

### Creating a namespace with a retired fork's ID

A create and a fork write the [hint](../specs/glossary.md#hint) first, with put-if-absent (see [Creating a namespace](../specs/format.md#91-creating-a-namespace) and [Forking a namespace](../specs/format.md#92-forking-a-namespace)). After the hint step, and before the call writes a pool record or manifest 1, the call reads the retirement record of the ID. If the record exists, the call returns `namespace_deleted`, and a fork deletes its source pin, as after any refused installation (see [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations)).

The check is safe because a hint put succeeds only when the prefix has no hint, and a collection deletes a retired fork's hint only after writing the fork's retirement record. So a call whose hint put succeeds for a retired fork's ID finds the record when it reads the record afterwards. A call that finds the fork's own hint reads the [tombstone](../specs/glossary.md#namespace-tombstone) and returns `namespace_deleted`, as for any deleted namespace.

A call that stops between its hint step and its read of the record leaves a hint without manifest 1, which the next collection of the prefix deletes. A later call that finds that hint still reads the record and returns `namespace_deleted`.

For example, after `sandbox` is retired, a fork of `main` with the new namespace ID `sandbox` writes its [fork pin](../specs/glossary.md#snapshot) on `main` and the hint `namespaces/sandbox/hint.json`. The fork reads `retirements/sandbox.json`, returns `namespace_deleted`, and deletes its fork pin. The next collection of `sandbox` deletes the hint.

Why does the call read the record after the hint step and not before? A read before the hint step could miss a retirement. If a call found no record while `sandbox` was deleted but not yet retired, and a collection then retired `sandbox`, the call's hint put would succeed, and the call would create a namespace with the retired ID. A read after the hint step also covers both ways that a call reaches manifest 1: a hint put that succeeded, and an existing hint without manifest 1.

### A pool's first member

A namespace that was created without a source (a [pool](../specs/glossary.md#pool)'s first member) keeps its [hint](../specs/glossary.md#hint) and its [tombstone](../specs/glossary.md#namespace-tombstone) after its retirement. Its retirement writes no retirement record and deletes nothing, as in [Retirement](../specs/format.md#95-retirement).

Each fork pins its source, and a tombstone can't be retired while a pin is on it, so a pool's first member is retired after every fork in the pool. A pool with no members stores its pool record, its first member's hint and tombstone, and the tombstone's segments.

Why does a pool's first member keep its hint and its tombstone? A pool's collection reads the first member's current manifest to tell a pool whose members are all retired from a pool whose create call failed (see [Content roots](../specs/format.md#119-content-roots)). For the first, the collection deletes the pool's content under the age rule and keeps the pool record. For the second, the collection deletes the pool record and lists nothing. Without the tombstone, a pool whose members are all retired would look like a pool whose create call failed, and the collection would delete the pool record while content was still under the pool's prefix.

### The API

A request to a retired [fork](../specs/glossary.md#fork) finds no [hint](../specs/glossary.md#hint), so the request returns `namespace_not_found` (404), like a request to a namespace that was never created. Between a fork's deletion and its retirement, a request to the fork returns `namespace_deleted` (410). A create or fork that uses a retired fork's ID returns `namespace_deleted`. A `gc` job for a retired fork runs the collection in [Collecting a retired fork](#collecting-a-retired-fork), and every other job returns `namespace_not_found`. The design adds no field to any request or response.

Why doesn't a request to a retired fork return `namespace_deleted`? Every request that finds no hint would have to read the retirement record to tell a retired fork from a namespace that was never created. The difference matters for a create, because a create is the request that could use the ID again, and a create reads the record.

## Costs

This section lists what the design costs, compared with a deleted fork that keeps its hint and its tombstone.

- Each retired fork has a retirement record of under 300 bytes (about 170 bytes plus the namespace ID, which is at most 128 bytes). The number of records grows with the number of forks that were ever retired. For example, a store that retires 30,000 forks a day adds about 11 million records a year, under 3.3 GB. No listing includes the records, so they add no request to a sweep.
- Each create and each fork sends one more GET, for the retirement record.
- The collection that retires a fork sends one more PUT and one more DELETE.
- Until a retired fork's prefix is empty, each collection of the prefix sends a GET for the hint, a GET for the record, and one listing for each of the five families. In the `sandbox` example, that's one collection after the retirement.
- A request to a retired fork returns `namespace_not_found`, where a deleted fork that isn't retired returns `namespace_deleted`.
- A pool's collection that read a fork's tombstone before the fork was retired can find one of the fork's layout segments deleted when the collection reads that segment's extent filter. (A fork's segments are deleted only after its retirement, once they're older than 24 hours.) That collection ends with an error at the first unreadable extent filter and deletes no content, and the next collection of the pool doesn't find the fork among the members.

## Changes to the specs and glossary

These sections of the storage format change:

- [Namespaces and identity](../specs/format.md#11-namespaces-and-identity) says that a fork's tombstone stays the current manifest until the fork is retired, and that a create or fork that uses a retired fork's ID returns `namespace_deleted`.
- [Family versions](../specs/format.md#a1-family-versions), [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads), and [Object keys](../specs/format.md#a8-object-keys) add the `retirement_record` kind, its payload, and the key pattern `retirements/{namespace_id}.json`.
- [Creating a namespace](../specs/format.md#91-creating-a-namespace) and [Forking a namespace](../specs/format.md#92-forking-a-namespace) add the read of the retirement record after the hint step.
- [Deleting a namespace](../specs/format.md#94-deleting-a-namespace), [Retirement](../specs/format.md#95-retirement), and [Retiring an eligible tombstone](../specs/format.md#118-retiring-an-eligible-tombstone) describe the three steps, which run before the candidate families are listed, and what a retired fork stores. [Fork dependencies after deletion](../specs/format.md#96-fork-dependencies-after-deletion) states its rule for a fork whose hint is deleted.
- [One complete collection](../specs/format.md#111-one-complete-collection) says that a collection of a namespace without a current manifest reads the retirement record.
- [Reference roots](../specs/format.md#112-reference-roots) and [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules) say that a retired fork has no roots, and that the hint and the current manifest are kept for every namespace except a retired fork.
- [Content roots](../specs/format.md#119-content-roots) says that a fork pin's target that has no manifest can be a retired fork.

In the glossary, the "Namespace retirement", "Namespace tombstone", "Sweep", and "Control object" entries describe the retirement record, and an entry defines "Retirement record". In the API spec, the [error table](../specs/api.md#3-standard-error-contract) rows for `namespace_not_found` and `namespace_deleted`, [`GET /v0/namespaces/{ns}`](../specs/api.md#62-get-v0namespacesns), [`POST /forks`](../specs/api.md#613-post-forks), and [Garbage collection jobs](../specs/api.md#garbage-collection-jobs) describe a retired fork. [Background maintenance](../../crates/loonfs-server/docs/self-hosting.md#background-maintenance) in the self-hosting guide says that a sweep lists a deleted fork until the fork's prefix is empty.

## Alternatives considered

- **Keeping the hint and the tombstone, and deleting the segments.** A retirement stops protecting the tombstone's segments and leaves two objects for each retired fork. The sweep still lists every retired fork and runs a collection of each one, so the work of a sweep still grows with the number of forks that were ever deleted.
- **Using a retired fork's ID again.** With no record, a retired fork leaves nothing, and a later create or fork can use its ID. A second fork with that ID would continue the first fork's range in the source's merge index. As a chain's creator, the second fork would extend a chain whose other layouts the second fork never read, so one content ID could describe two byte sequences. A process that cached the first namespace's head and WAL-tail projections could return those entries for the second namespace. Each of the three would have to identify a namespace by an ID that LoonFS generates instead of by the namespace ID.
- **Removing the forks whose ID LoonFS generated, and no others.** A generated ID isn't generated twice, so a retired fork with a generated ID needs no record. A fork whose ID the caller chose keeps its hint and its tombstone. A fork then ends in one of two ways, depending on how its ID was chosen, and an application that chooses its fork IDs keeps the sweep cost.
- **Retiring a pool's first member in the same way.** The pool's collection reads the first member's retirement record to tell a pool whose members are all retired from a pool whose create call failed. The record then has to include the pool's ID, because two create calls for one namespace ID generate two pools and one call fails. A pool has one first member, so those two rules would remove one hint and one tombstone for each pool, and the pool record would stay.
