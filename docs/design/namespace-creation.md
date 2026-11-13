# Creating a namespace and the pool's member list

This note describes the order of writes that creates or forks a [namespace](../specs/glossary.md#namespace), and how the collection of a [pool](../specs/glossary.md#pool) finds the pool's members. A new namespace's manifest 1 has a third status, `initializing`, and LoonFS refuses reads and writes to a namespace with that status. The create or fork call then writes the objects that the namespace needs outside its own prefix, and publishes manifest 2 with the status `active`. A later create or fork call for the same ID can finish a namespace that's initializing, and a delete call or a collection can delete one. Each pool has a member list, with one small object for each member under the pool's prefix, and the pool's collection lists those objects to find the members. The note builds on [The manifest boundary](manifest-boundary.md).

## The problem

This section describes what LoonFS writes when it creates or forks a namespace, and the time rules and records that those writes need.

To create a namespace without a source, LoonFS generates a pool ID and writes the [pool record](../specs/glossary.md#pool-record) (`pools/{pool_id}/pool.json`) before manifest 1 (see [Creating a namespace](../specs/format.md#91-creating-a-namespace)). To fork a namespace, LoonFS writes and verifies a fork snapshot on the source before the fork's manifest 1 (see [Forking a namespace](../specs/format.md#92-forking-a-namespace)). In both cases, the call writes an object outside the new namespace's prefix before the namespace has a manifest. If the call stops after that write, no manifest records the object, and LoonFS can't tell that object from the object of a call that's still running.

So LoonFS judges those objects by age. The "creation grace" of a pool record or a fork snapshot is the [grace window](../specs/glossary.md#grace-window) after the object's `created_at_ms`. The pool's collection keeps the pool record of a pool with no members until its creation grace has passed, and then deletes the record (see [Content roots](../specs/format.md#119-content-roots)). The source's snapshot cleanup keeps a fork snapshot during its creation grace, and after that deletes the snapshot if the target has no manifest or the target's fork basis doesn't refer to the snapshot (see [Snapshot cleanup](../specs/format.md#117-snapshot-cleanup)). A create or fork call's metadata publication budget starts before its first write, so the call ends inside the creation grace. For example, if a fork of `main` into `sandbox` writes its fork snapshot and the process then stops, `main` keeps that snapshot for at least the grace window, and a retry of the fork writes a second fork snapshot.

The pool record has a second job. It identifies the pool's first member, and the pool's collection finds every other member by walking fork snapshots from the first member (see [Content roots](../specs/format.md#119-content-roots)). Two create calls for one namespace ID each write a pool record, and the call whose manifest 1 put loses leaves a pool that has no members. The collection treats that pool like the pool of a failed create, and with the manifest boundary, a losing pool record that no collection deleted before the namespace was retired stays (see [The pool's collection](manifest-boundary.md#the-pools-collection)).

Finally, a call can't tell whether its own manifest 1 put succeeded after the put has an unknown outcome, unless manifest 1 refers to an object or an ID that only this attempt created: the pool ID that the call generated, or the call's fork snapshot (see [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations)). Every first manifest has to be built that way.

## The design

This section describes the `initializing` status, the member list, the order of writes for a create and a fork, how a namespace that's initializing is finished or deleted, retirement, the pool's collection, and the time rules. The design uses [Publishing a manifest](manifest-boundary.md#publishing-a-manifest) and [Finding the newest manifest](manifest-boundary.md#finding-the-newest-manifest) from the manifest boundary design, unchanged.

### The `initializing` status

A manifest's status (`status`) has three values: `active`, `deleted`, and `initializing`. Manifest 1 of every new namespace has the status `initializing`, and manifest 2 is the same manifest with the status `active`. A namespace is "initializing" while its newest manifest has that status, which means that no call has finished writing the objects that the namespace needs outside its prefix.

LoonFS treats a namespace that's initializing as missing, except for a create, a fork, or a delete with its ID:

- A read, a commit, a snapshot, a fork from the namespace, a merge, and a writer claim return `namespace_not_found`.
- A create or fork call with the namespace's ID can finish the namespace (see [Finishing a namespace that's initializing](#finishing-a-namespace-thats-initializing)).
- A delete call with the namespace's ID publishes a tombstone (see [Deleting a namespace that's initializing](#deleting-a-namespace-thats-initializing)).

For example, a create of `sandbox` publishes manifest 1 with the status `initializing`. Until manifest 2 is published, `GET /v0/namespaces/sandbox` returns `namespace_not_found`.

Why is this a third status value, and not a separate flag? A namespace is in exactly one of the three states, and [docs/STYLE.md](../STYLE.md#state-and-types) says to represent a lifecycle as one enum, because a set of flags allows combinations that can't exist (such as deleted and initializing at once). A new variant also makes the compiler list every place that reads the status.

### The pool's member list

A pool's "member list" is a set of control objects under the pool's prefix, one for each member of the pool:

```text
pools/{pool_id}/members/{namespace_id}.json
```

Each object is a "member entry". A create or fork call writes the entry once, with put-if-absent, before the namespace becomes active, and the namespace's retirement deletes the entry. A member of a pool is still a namespace whose manifest records the pool, until the namespace is [retired](../specs/glossary.md#namespace-retirement). The entry is how the pool's collection finds the member.

The entry is uncompressed JSON of envelope kind `pool_member`, like the other [control objects](../specs/glossary.md#control-object). Its payload is the pool ID (`pool_id`) and the namespace ID (`namespace_id`), and both MUST match the key. The pool's collection uses the entry's key and doesn't read the payload.

A pool ID is generated by the create call of a namespace without a source, and a pool has no pool record. A pool with no member entries and no other object under its prefix doesn't exist.

For example, creating `main` with `pool_3s0drmj8em84yb4mnetqkz2j0x` writes `pools/pool_3s0drmj8em84yb4mnetqkz2j0x/members/main.json`, and forking `main` into `sandbox` writes `pools/pool_3s0drmj8em84yb4mnetqkz2j0x/members/sandbox.json`.

Why does every member have an entry, a fork included? The pool's collection lists the entries once, instead of reading a pool record and then each member's fork snapshots in turn. Each entry's key is set by manifest 1 (the pool ID that manifest 1 records, and the namespace ID), so every call that finishes the namespace writes the same key.

### Creating a namespace without a source

To create a namespace without a source, a call:

1. Finds the newest manifest for the ID (see [Finding the newest manifest](manifest-boundary.md#finding-the-newest-manifest)). A retired ID returns `namespace_deleted`. An active namespace returns `namespace_exists` (or the namespace's summary, when the request allows an existing namespace), and a tombstone returns `namespace_deleted`. A namespace that's initializing is finished as described in [Finishing a namespace that's initializing](#finishing-a-namespace-thats-initializing). If no namespace has the ID, the call continues.
2. Generates a pool ID, and publishes manifest 1 with the status `initializing` and that pool ID (see [Publishing a manifest](manifest-boundary.md#publishing-a-manifest)). After a lost put, the call starts again at step 1.
3. Writes the member entry with put-if-absent.
4. Publishes manifest 2, the same manifest with the status `active`.

For example, a create of `main` publishes manifest 1 with the status `initializing` and `pool_3s0drmj8em84yb4mnetqkz2j0x`, writes `pools/pool_3s0drmj8em84yb4mnetqkz2j0x/members/main.json`, and publishes manifest 2 with the status `active`.

Why does a namespace without a source need two manifests? The namespace has to have its member entry before it can be used, because the pool's collection finds members only through their entries. If the call wrote the entry before manifest 1, a call that then lost the manifest 1 put to another call would leave an entry in a pool that no manifest records, and two calls with the same request would write entries in two pools. With manifest 1 first, the call whose manifest 1 put succeeds chooses the pool ID, and every later write uses a key that manifest 1 sets.

### Forking a namespace

To fork a namespace, a call:

1. Finds the newest manifest for the new ID, as in step 1 of [Creating a namespace without a source](#creating-a-namespace-without-a-source).
2. Chooses the snapshot that the fork is built from. If the caller named a user snapshot, the call uses that snapshot, which must exist, not be deleted, and not have expired. Otherwise the call creates a "temporary snapshot" of the source's head: a user snapshot with no name, which expires `OPERATION_SNAPSHOT_TTL_MS` (5 minutes) after the call takes it (see [Creating and verifying a snapshot](../specs/format.md#82-creating-and-verifying-a-snapshot)). It's the same kind of snapshot that a merge takes of its fork (see [Cross-namespace copies and moves](../specs/format.md#97-cross-namespace-copies-and-moves)). A fork that a rebase creates checks here that its fork point isn't below the retention floor of that snapshot's manifest, and returns `below_retention_floor` before it publishes a manifest (see [Rebasing](merge.md#rebasing)).
3. Generates the fork snapshot's ID, and publishes the fork's manifest 1 with the status `initializing`, built from the source manifest that the snapshot of step 2 keeps. The fork basis records that manifest, the fork snapshot's ID, and the ID of the snapshot of step 2. After a lost put, the call starts again at step 1.
4. Checks that the source is active and that the snapshot of step 2 exists, isn't deleted, and hasn't expired. Then it writes the fork snapshot on the source with put-if-absent, with that ID and no expiry, on the source manifest that the snapshot of step 2 keeps, and makes the same check again, as a fork of a user snapshot does (see [Creating and verifying a snapshot](../specs/format.md#82-creating-and-verifying-a-snapshot)). The call also writes the member entry with put-if-absent. The two writes don't depend on each other. A call never deletes the fork snapshot, even when the write or the check fails (see [Finishing a namespace that's initializing](#finishing-a-namespace-thats-initializing)).
5. Publishes manifest 2, the same manifest with the status `active`.

A temporary snapshot isn't deleted. It expires, and garbage collection deletes its record after the grace window, as for any expired snapshot.

For example, a fork of `main` into `sandbox` creates a temporary snapshot of `main`'s manifest 46, publishes `sandbox`'s manifest 1 built from manifest 46, writes the fork snapshot on `main` and `pools/pool_3s0drmj8em84yb4mnetqkz2j0x/members/sandbox.json`, and publishes `sandbox`'s manifest 2.

Why is the fork snapshot written after manifest 1? Manifest 1 records the fork snapshot's ID before the snapshot exists, so every fork snapshot has a namespace whose manifest records the snapshot. The source's snapshot cleanup keeps a fork snapshot while the target's newest manifest records it, and deletes it otherwise, without a creation grace.

Why does a fork need the snapshot of step 2? Manifest 1 lists segments of a source manifest, and the fork snapshot that keeps that manifest is written only after manifest 1. Until the fork snapshot is verified, the snapshot of step 2 keeps the source manifest and its segments, so a collection of the source can't delete them.

Why does manifest 1 record the snapshot of step 2? A call that finishes the fork makes the fork snapshot from that snapshot, and manifest 1 is the only place where that call can find its ID.

### Finishing a namespace that's initializing

A create or fork call that finds the ID initializing compares its request with manifest 1: the same source namespace (or no source in both), the same snapshot if the request names one, and the same create options. A request that names no snapshot matches whichever snapshot manifest 1 records, so the call finishes the fork from the snapshot that the first call chose. The fields that record who created the namespace, and when, aren't compared. If the request and manifest 1 differ, the call returns `namespace_exists`. If they match, the call continues from the step after manifest 1, with the values that manifest 1 records (the pool ID, and for a fork, the fork basis). Each later write is a put-if-absent of a key that manifest 1 sets: the member entry, the fork snapshot, and manifest 2. So two calls that finish the same namespace at once write the same objects, and both return success. A call that loses the put of manifest 2 finds the newest manifest again, and returns success if the namespace is active, or `namespace_deleted` if it finds a tombstone or a retired ID.

A check before or after the fork snapshot's write fails if the snapshot that manifest 1 records has ended (it expired, or a caller deleted it), or if the source was deleted. A call whose write or check fails doesn't delete the fork snapshot. Two calls can finish the same fork with the same fork snapshot ID, and the other call's check may have passed, so the fork snapshot may be the one that an active fork reads through. The call finds the newest manifest again instead. If the namespace is active, another call finished it, and the call returns success. Otherwise the call returns an error, the namespace can't become active, and it stays initializing until it's deleted (see [Deleting a namespace that's initializing](#deleting-a-namespace-thats-initializing)). Its fork snapshot stays until the namespace is retired.

For example, a fork of `main` into `sandbox` stops after `sandbox`'s manifest 1. If the client sends the same fork request again within 5 minutes of the first call's temporary snapshot, the second call writes the fork snapshot and the member entry, publishes manifest 2, and returns success. If the client sends it 10 minutes later, the temporary snapshot has expired, and the call returns an error.

Why can any call finish the namespace? Every write after manifest 1 uses a key that manifest 1 sets, so a call doesn't need to know which call wrote manifest 1. A call whose manifest 1 put had an unknown outcome finds the newest manifest and finishes the namespace like any other call, so manifest 1 doesn't have to refer to an object or an ID that only one attempt created. A client that retries after a lost response gets success if the first call stopped before manifest 2, and `namespace_exists` if the namespace is already active.

### Deleting a namespace that's initializing

A delete call for a namespace that's initializing publishes the tombstone as the next manifest, as for any namespace (see [Deleting a namespace](../specs/format.md#94-deleting-a-namespace)). The tombstone and the active manifest 2 compete for the same number, so the namespace either becomes deleted or becomes active, and never both. A namespace that's initializing has no writer and no WAL objects, so its tombstone is a manifest publication without a writer session.

A call can also be abandoned before manifest 2, by a process that stops or by a fork whose snapshot ended. LoonFS generates a namespace ID when the caller doesn't send one, and a caller that never learned the generated ID can't finish or delete the namespace. So a namespace's [collection](../specs/glossary.md#collection) deletes an abandoned attempt: when the namespace's newest manifest is initializing, and the manifest's `created_at_ms` is more than the grace window before the collection's clock, the collection publishes a tombstone in the same way as a delete call. From then on, the namespace is an ordinary deleted namespace, and a later collection retires it once the tombstone is eligible. This adds a case to the namespace's existing collection, which the reference server's [sweep](../specs/glossary.md#sweep) already runs for every namespace that it lists, and no new job or listing.

For example, a create call with no namespace ID generates `ns_5gk2a8m3qy9b7d0f4h6j8n2p1r`, publishes manifest 1, and stops. The next sweep finds manifest 1 initializing and older than the grace window, and publishes a tombstone as manifest 2. A later sweep retires the namespace.

Why does the collection wait for the grace window? The wait keeps the collection from deleting the namespace of a call that's still running. The wait isn't needed for safety, because the tombstone and the active manifest 2 compete for one number.

### Retiring a namespace

Retirement deletes the objects that the namespace's tombstone records outside the namespace's prefix before it raises the boundary. To retire an eligible namespace, a collection:

1. If the namespace is a fork, deletes its fork snapshot on its source.
2. Deletes its member entry.
3. Raises the boundary to retired.

The tombstone records the pool ID and the fork basis, which can't change after manifest 1, so the collection finds both keys in the tombstone. Deleting a missing object succeeds, so a collection that stops before step 3 starts again at step 1. [Retiring a namespace](manifest-boundary.md#retiring-a-namespace) describes the rest of retirement, and why the boundary is raised last.

A slow call can write a member entry or a fork snapshot after the namespace is retired, for example a call that found the namespace initializing just before a delete. The pool's collection deletes such an entry (see [The pool's collection](#the-pools-collection)), and the source's snapshot cleanup deletes such a fork snapshot, because the target's newest manifest doesn't record it. Neither rule depends on the retired boundary.

Why can the member entry be deleted before the boundary is raised? The collection deletes the entry only once the tombstone is eligible for retirement, so the namespace leaves the member list no earlier than its retirement deadline (see [Retirement](../specs/format.md#95-retirement)). That's the bound that the pool's time limit for reading every member depends on (see [Content roots](../specs/format.md#119-content-roots)).

### The pool's collection

A collection of a pool:

1. Lists the pool's upload sessions, before it reads any member's WAL tail.
2. Lists `pools/{pool_id}/members/`.
3. For each listed namespace, finds the newest manifest, and only then lists the namespace's snapshots. Then, depending on what it finds:
   - **Active or deleted, and recording this pool.** The collection reads the member's roots, as in [Content roots](../specs/format.md#119-content-roots).
   - **Initializing.** The namespace has no roots.
   - **Retired, no namespace, or a manifest that records another pool.** The namespace has no roots, and the collection deletes the member entry.
   - **Unreadable.** The collection deletes no content objects in this pass.
4. Reads the extent filters, lists the content prefix once, and deletes unreferenced content objects, temporary objects, and upload records under the rules in [Content roots](../specs/format.md#119-content-roots).

The collection still has to read every member's roots within `POOL_ROOTS_READ_BUDGET_MS`, or it deletes no content objects. It writes no object. A pool that has no member entries has no roots, so the age rules delete its content, temporary objects, and upload records. A collection of a pool ID with no object under the pool's prefix returns `pool_not_found`.

Why does a namespace that's initializing have no roots? LoonFS refuses commits to it, so a namespace without a source refers to no content. A fork refers only to the content of the source manifest that the snapshot of step 2 or the fork snapshot keeps, and that manifest is one of the source's roots while the snapshot exists.

Why is a fork safe when its member entry is written after the collection lists the entries? The collection doesn't read that fork. The content that the fork starts with is the content of the source manifest that the snapshot of step 2 keeps. The collection lists the source's snapshots after it finds the source's newest manifest. If the listing includes the snapshot of step 2, that snapshot's manifest is one of the source's roots, and a snapshot's record stays for the grace window after the snapshot ends (see [Snapshot cleanup](../specs/format.md#117-snapshot-cleanup)), so the listing includes a snapshot that has already expired. If the listing doesn't include it, the snapshot was created later, on the manifest that the collection found or on a newer one. A newer manifest of the source differs from the one that the collection found only by commits made after the collection read the source. Content that those commits wrote is younger than the grace window or kept by an upload record, and [Content roots](../specs/format.md#119-content-roots) covers a reference that a merge moves between members after the collection starts. The same holds for content that the fork itself writes or receives.

### Time rules

The design has no creation grace for a pool record or a fork snapshot, because no create or fork writes an object that no manifest records. It has these time rules:

- A fork call has to verify its fork snapshot before the snapshot of step 2 ends. For a temporary snapshot, that's `OPERATION_SNAPSHOT_TTL_MS` (5 minutes) after the call takes it, which is shorter than the 15 minutes (`METADATA_PUBLICATION_BUDGET_MS`) that a manifest publication can take. A call that's later can't finish the fork.
- A namespace's collection deletes an abandoned attempt once its manifest 1 is older than the grace window. A shorter wait would be safe too, because the tombstone and the active manifest 2 compete for one number.
- The pool's collection reads every member's roots within `POOL_ROOTS_READ_BUDGET_MS`, or deletes no content objects.

The time rules in [The manifest boundary](manifest-boundary.md#time-rules) also apply.

## Costs

This section lists the design's costs.

| Call | Object writes in LoonFS | Object writes in this design |
| --- | --- | --- |
| Create without a source | 2: pool record, manifest 1 | 3: manifest 1, member entry, manifest 2 |
| Fork | 2: fork snapshot, manifest 1 | 5: temporary snapshot, manifest 1, fork snapshot, member entry, manifest 2, in 4 steps |

- With the [manifest boundary](manifest-boundary.md), each manifest publication also reads the boundary, and each snapshot creation makes the checks in [Creating and verifying a snapshot](../specs/format.md#82-creating-and-verifying-a-snapshot).
- A fork call has 5 minutes from its temporary snapshot to its verified fork snapshot.
- A fork from the source's head leaves the record of its temporary snapshot on the source until garbage collection deletes the record, the grace window after the snapshot expires. Until then, the source's snapshot listing has two records for the fork: the temporary snapshot and the fork snapshot.
- A call that's abandoned before manifest 2 uses up its namespace ID, including an ID that the caller chose. A caller that comes back after the collection deleted the namespace gets `namespace_deleted` for a namespace that it never used. A fork whose snapshot ended uses up its ID in the same way.
- Every namespace keeps a member entry under its pool's prefix until it's retired, and the pool's collection lists those entries.

## Changes to the specs, the glossary, and other design notes

The storage format changes in these sections:

- [Namespaces and identity](../specs/format.md#11-namespaces-and-identity) adds the `initializing` status, defines the members of a pool by their member entries, and removes the first member.
- [Storage layout](../specs/format.md#21-storage-layout), [Object roles and mutability](../specs/format.md#22-object-roles-and-mutability), [Immutable writes and retries](../specs/format.md#32-immutable-writes-and-retries), [Family versions](../specs/format.md#a1-family-versions), [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads), and [Object keys](../specs/format.md#a8-object-keys) replace the pool record with the member entry, add the status value, and add the snapshot of step 2 to the fork basis. [Publication and collection timing](../specs/format.md#c1-publication-and-collection-timing) adds the temporary snapshot of a fork to the meaning of `OPERATION_SNAPSHOT_TTL_MS`.
- [Creating and verifying a snapshot](../specs/format.md#82-creating-and-verifying-a-snapshot) and [Records and owners](../specs/format.md#81-records-and-owners) describe the temporary snapshot of a fork.
- [Creating a namespace](../specs/format.md#91-creating-a-namespace), [Forking a namespace](../specs/format.md#92-forking-a-namespace), and [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations) describe the order of writes and finishing a namespace that's initializing, and remove the rule that a first manifest refers to an object or an ID that only one attempt created.
- [Deleting a namespace](../specs/format.md#94-deleting-a-namespace) describes the tombstone of a namespace that's initializing, and the collection of an abandoned attempt.
- [Retirement](../specs/format.md#95-retirement), [One complete collection](../specs/format.md#111-one-complete-collection), [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules), and [Retiring an eligible tombstone](../specs/format.md#118-retiring-an-eligible-tombstone) add the deletion of the member entry and the collection of an abandoned attempt, and remove the pool record.
- [Snapshot cleanup](../specs/format.md#117-snapshot-cleanup) removes the creation grace of a fork snapshot, and [Content roots](../specs/format.md#119-content-roots) replaces the pool record, the first member, the walk along fork snapshots, and the failed create with the member list.

The API spec's [Maintenance jobs](../specs/api.md#maintenance-jobs), [Creating and forking namespaces](../specs/api.md#creating-and-forking-namespaces), [`GET /v0/namespaces/{ns}`](../specs/api.md#62-get-v0namespacesns), and [error table](../specs/api.md#3-standard-error-contract) change. So do [Pools](pools.md), [Content collection](content-collection.md), and [The manifest boundary](manifest-boundary.md), whose sections on creating a namespace and on the pool's collection already point to this note.

In the glossary, "Pool record" is removed, and "Pool", "Manifest", "Control object", "Snapshot", "Fork", and "Namespace retirement" change.

## Alternatives considered

- **Writing the fork snapshot first.** A fork writes and verifies its fork snapshot, publishes manifest 1 with the status `initializing`, writes the member entry, and publishes manifest 2. That's four writes instead of five, and no temporary snapshot. But a call can stop between the fork snapshot and manifest 1, so the source's snapshot cleanup keeps the creation grace of a fork snapshot, and a fork call has to finish inside that grace.
- **A pool record and a walk along fork snapshots.** A create writes a pool record that identifies the first member, and the pool's collection finds every other member through the fork snapshot on the member's source. The pool record is written before manifest 1, so the collection needs a creation grace to tell a failed create from a running one, and a case for a pool whose create failed or lost. With the manifest boundary, a losing create's pool record can stay after the namespace is retired.
