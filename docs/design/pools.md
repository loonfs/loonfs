# Pools

This note describes where LoonFS stores content objects and upload records, and how they're collected. A "pool" is a set of namespaces that share one set of content objects, together with the key prefix that stores those objects. Every namespace belongs to exactly one pool, and every manifest records that pool's ID. A fork joins its source's pool, so a fork and a merge share content without copying bytes. A pool's content is stored under the pool's own prefix, and a collection of the pool, which is a job of its own, deletes the pool's unreferenced content. So no namespace's prefix stores another namespace's content, and no namespace's retirement deletes content.

## The problem

This section describes how fork families store content, and what that costs.

Every namespace belongs to a [fork family](../specs/glossary.md#fork-family): one namespace that was created without a source (the [family root](../specs/glossary.md#family-root)), and every namespace forked from it, directly or through other forks. Every member of the family writes its content objects and upload records under the family root's prefix (`namespaces/{family_root_id}/content/`), so one [content reference](../specs/glossary.md#content-reference) identifies the same object in every member (see [File contents and ownership](../specs/format.md#15-file-contents-and-ownership)). The family root's [collection](../specs/glossary.md#collection) deletes the family's unreferenced content, with roots that it reads from every member (see [Content roots](../specs/format.md#119-content-roots)).

So one namespace's prefix stores the content of every member, and that namespace's lifecycle depends on data that other namespaces wrote. Each fork pins its source, and a tombstone can't be [retired](../specs/glossary.md#namespace-retirement) while a pin is on it, so the family root is retired after every fork of it and every fork of those forks (see [Retirement](../specs/format.md#95-retirement)). After the family root is deleted, its collections keep running for as long as any fork exists, because no other collection deletes the family's content. When the family root is retired, its retirement deletes every object under its content prefix in one step, with no age check (see [Deleting a retired owner's content](../specs/format.md#118-deleting-a-retired-owners-content)).

The family has no ID of its own, because its content prefix is identified by a namespace ID. A `gc` job on the family root reports the whole family's content in its `deleted` fields and its `listed_bytes`, together with the family root's own metadata, and the rules of a collection have one case for a family root and another for a fork (see [One complete collection](../specs/format.md#111-one-complete-collection)).

## The design

This section describes pools and pool records, how a namespace is created, the pool's collection, and the API.

### Pools and pool records

A pool's ID (`pool_id`) is `pool_` followed by the 26-character body that every generated LoonFS ID has (see [Numbered objects and generated IDs](../specs/format.md#23-numbered-objects-and-generated-ids)), for example `pool_3s0drmj8em84yb4mnetqkz2j0x`. Every manifest records the namespace's pool (`pool_id`). A [namespace](../specs/glossary.md#namespace) created without a source records a pool ID that its create call generates, and a [fork](../specs/glossary.md#fork) copies the value from the pinned source manifest. The value can't change. A "member" of a pool is a namespace whose manifest records that pool, until the namespace is [retired](../specs/glossary.md#namespace-retirement).

A pool stores these objects under its own prefix:

```text
pools/{pool_id}/pool.json
pools/{pool_id}/content/{content_id}
pools/{pool_id}/content/{content_id}-{start:020}-{end:020}
pools/{pool_id}/uploads/{upload_id}.json
pools/{pool_id}/uploads/{upload_id}.outcome.json
pools/{pool_id}/temporary/{temporary_id}
```

A "pool record" (`pool.json`) is written once, when the pool is created. It records the pool's ID, the namespace that was created with the pool, and the creation time (`created_at_ms`), which the create call takes from its request clock. That namespace is the pool's first member. The rest of the prefix stores the pool's [content objects](../specs/glossary.md#content-object) and span objects, the session records and outcome records of its [upload sessions](../specs/glossary.md#upload-session), and the [temporary objects](../specs/glossary.md#temporary-object) of content assemblies. Everything else stays under the namespace's own prefix: WAL objects, manifests, the hint, pins, metadata segments, extension segments, and the temporary objects of the namespace's own assemblies.

A content reference contains a content ID, a size, and a checksum, and no pool, namespace, or path. A reader builds the object key from its own manifest's `pool_id` and the content ID, so one reference identifies the same object in every member of a pool. An [extent filter](../specs/format.md#extent-filter) records object keys with the pool's content prefix removed, so a segment that any member wrote matches the keys in the pool's content listing.

Inside a pool, a fork and a [merge](../specs/glossary.md#merge) use references and copy no file bytes. No operation adds a reference to another pool's content, and content moves between pools by an upload (see [Cross-namespace copies and moves](../specs/format.md#97-cross-namespace-copies-and-moves)).

For example, creating `main` generates `pool_3s0drmj8em84yb4mnetqkz2j0x`. Forking `main` into `sandbox`, and `sandbox` into `trial`, writes no pool key, and all three manifests record that pool ID. A file that `sandbox` uploads is stored at `pools/pool_3s0drmj8em84yb4mnetqkz2j0x/content/con_014d2pf2dbsqqg28t5cy4tqkff`, and after `sandbox` is merged into `main`, a revision in `main` refers to the same object.

Why does a pool have its own ID and prefix instead of using a namespace's? If one namespace's prefix stored the content of every member, that namespace would have to be retired last for a reason that has nothing to do with its own data. Its collections would have to keep running after it's deleted, and its retirement would delete every member's content. With a pool prefix, no namespace stores another namespace's content, and no namespace's collection or retirement acts on that content.

### Creating a namespace

To create a namespace without a source, LoonFS:

1. Generates a pool ID and builds the first manifest, which records that pool ID (see [Publishing a manifest](../specs/format.md#72-publishing-a-manifest)).
2. Writes the pool record with put-if-absent.
3. Writes the hint with put-if-absent, and then manifest 1 with put-if-absent, as [Creating a namespace](../specs/format.md#91-creating-a-namespace) describes.
4. If the hint already exists, follows the rules in [Creating a namespace](../specs/format.md#91-creating-a-namespace). An active namespace returns `namespace_exists`, and a tombstone returns `namespace_deleted`. If the hint's manifest 1 is missing, the call writes its own manifest 1.
5. If the call finds that the namespace's current manifest records another pool, deletes its own pool record. That deletion is best effort.

The pool record is written before the hint, so every pool that a manifest records has a pool record. A pool ID is generated by one create call, so no other call writes that pool's record. A namespace writes under a pool's prefix after a manifest records the pool, so if the create call fails, its pool record is the only object under the pool's prefix.

A call whose manifest put has an unknown outcome leaves its pool record in place, because the manifest might have been written. If the manifest wasn't written, the pool's collection deletes the pool record after its creation grace (see [The pool's collection](#the-pools-collection)). The call takes the pool record's `created_at_ms` from its request clock, and its metadata publication budget starts no later than that reading, as a fork call's budget starts at the beginning of the fork call. So a create call ends inside the pool record's creation grace.

A fork writes nothing under the pool's prefix when it's created. Its first manifest copies the source's `pool_id`, and its installation makes the requests in [Forking a namespace](../specs/format.md#92-forking-a-namespace). The fork's fork pin on its source is the only record of the fork.

Why does a pool ID change how a create resolves an unknown outcome? After a manifest put with an unknown transport outcome, the publisher reads the manifest at that number. Identical bytes confirm the attempt's own success when the manifest refers to an object that's unique to that attempt. A first manifest without a pool ID doesn't refer to such an object, because two plain creates with the same request build the same bytes (see [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations)). A plain create's first manifest contains a pool ID that only this attempt generated, so identical bytes confirm the create, in the same way that a fork's source pin confirms a fork.

Why isn't the pool record written by a fork? The fork pin already records the fork, and the pin is unique to one installation attempt. A second record for each fork would have to be kept in step with the pin by every installation and every retirement.

### The pool's collection

A collection of a pool deletes the pool's unreferenced content objects, upload records, and temporary objects. It's a job of its own, separate from the collection of any namespace. The pool's collection:

1. Reads the pool record. If the pool has no pool record, the collection deletes nothing.
2. Lists the pool's upload sessions.
3. Reads the roots of the pool's first member: its current manifest (found by discovery from its hint), its WAL tail, and its pin listing.
4. Finds the other members by walking fork pins, and reads the same roots for each member. Each fork pin in a member's pin listing adds the pin's target as a member.
5. Reads the extent filter of each distinct layout segment in the members' layout views, and lists the pool's content prefix once.
6. Deletes each content object that no root protects, once its provider age is at least the [grace window](../specs/glossary.md#grace-window).
7. Applies the rules in [Upload-session cleanup](../specs/format.md#116-upload-session-cleanup) to the pool's session records, and deletes the pool's temporary objects that are older than the grace window.

These rules are the same as in [Content roots](../specs/format.md#119-content-roots), applied to the members of a pool:

- The sessions are listed before any member's WAL tail is read, for the reason in [Content collection](content-collection.md#why-the-roots-are-enough).
- The collection has to read every member's roots within the retirement grace less the clock allowance (38 minutes and 30 seconds), or it deletes no content objects. The API reports this as skipped work.
- A member whose manifest fails verification stops the collection of the pool's content, and other pools are unaffected.
- A member's content references are roots until the member is [retired](../specs/glossary.md#namespace-retirement). A deleted member's tombstone keeps its runs.
- A [fork pin](../specs/glossary.md#snapshot) whose target has no manifest is skipped.

The pool's first member has no source pin, so its retirement deletes nothing. The first member is retired when its tombstone's retirement deadline has passed and no pin is on it (see [Retirement](../specs/format.md#95-retirement)). Each fork pins its source, so the first member can be retired once every other member is retired. From then on the pool has no members, and no root protects any of its content. The ordinary age rule deletes every content object and every temporary object, and the rules in [Upload-session cleanup](../specs/format.md#116-upload-session-cleanup) delete every upload record. No rule deletes a pool's content in one step.

A pool also has no members when its create call failed, or when another create call for the same namespace ID succeeded first. The collection finds this case when the pool record is older than its creation grace, and the namespace that the pool record identifies has no current manifest, or has a current manifest that records another pool. The creation grace is the grace window after the record's `created_at_ms`, the same rule as a fork pin's creation grace (see [Forking a namespace](../specs/format.md#92-forking-a-namespace) and [Pin cleanup](../specs/format.md#117-pin-cleanup)), and it covers a create call that's still running. Inside the creation grace, the collection deletes nothing.

The collection deletes the pool record in the second case, and not in the first. After a failed create, the pool record is the only object under the pool's prefix, and the first collection after the creation grace deletes the record. A pool whose first member is retired keeps its pool record, and each later collection of the pool finds no members and applies the same rules.

For example, suppose that `main`, `sandbox`, and `trial` are all deleted. `trial` is retired first, which deletes its pin on `sandbox`, and then `sandbox` is retired, which deletes its pin on `main`. Once `main` is retired, a collection of `pool_3s0drmj8em84yb4mnetqkz2j0x` finds no members. It deletes every content object and temporary object whose provider age is at least the grace window, it deletes the pool's upload records under the rules in [Upload-session cleanup](../specs/format.md#116-upload-session-cleanup), and it keeps the pool record.

A namespace's own collection covers its manifests, WAL objects, metadata and extension segments, pins, and its own temporary objects. It never lists content objects or upload records. Retiring a fork deletes the fork's source pin and nothing else, and no namespace's retirement deletes content.

Why does the collection delete nothing when the pool record is missing? The pool record is how the collection finds the members. Without it, the collection can't tell a pool with no members from a pool whose members it can't find, and deleting content in the second case would lose data.

Why doesn't retirement delete the pool's content in one step? Once the pool has no members, the ordinary rules already delete the pool's content, upload records, and temporary objects, so a separate rule would be a second way to delete content.

Why does a pool keep its pool record after its last member is retired? A delayed abort or completion can create an outcome record after garbage collection deleted the session's records (see [Upload-session cleanup](../specs/format.md#116-upload-session-cleanup)), and by then every other object under the pool's prefix might be deleted. A later collection deletes that outcome record under its age rule, and the collection reads the pool record to find that the pool has no members.

### The API

`NamespaceMetadata` includes the namespace's `pool_id` (see [`GET /v0/namespaces/{ns}`](../specs/api.md#62-get-v0namespacesns)). The create and fork requests have no pool field.

A maintenance job can run on a pool as well as on a namespace. `POST /v0/maintenance/pools/{pool_id}/runs` with `kind` `gc` and the optional `grace_window_ms` runs one collection of the pool, as `POST /v0/maintenance/namespaces/{ns}/runs` runs a job on a namespace. Its result includes `deleted.content_objects`, `deleted.upload_sessions`, `deleted.temporary_objects`, `listed_bytes` for the objects under the pool's prefix, `next_reclamation_at_ms`, and `skipped`. A namespace's `gc` job reports no content objects or upload sessions, its result has no `retired_content_objects`, and its `listed_bytes` covers the namespace's own prefix.

A pool's collection reports `content_sweep_pool_budget` in `skipped` when it couldn't read every member's roots in time, and `content_sweep_unreadable_member` when it couldn't read one member's roots. The reference server's [sweep](../specs/glossary.md#sweep) lists the pools and runs a collection of each pool when it collects.

An application that meters storage reads a pool's stored bytes from the pool job's `listed_bytes`, and a namespace's metadata bytes from the namespace job's `listed_bytes`.

Why doesn't a create request include a pool? A pool ID is generated by one create call, so the pool record identifies the pool's one member without a source, and the collection finds every other member through fork pins. If a create request could add a namespace to an existing pool, each namespace created that way would need its own record (see [Alternatives considered](#alternatives-considered)).

## Costs

This section lists what the design costs, compared with fork families.

- Creating a namespace without a source makes one more PUT request, for the pool record. A fork makes no more requests.
- There are two kinds of collection. An operator who runs `gc` jobs by hand runs one for each namespace and one for each pool. A namespace without forks is read by both collections: the pool's collection discovers the namespace's manifest, reads its WAL tail, and lists its pins, as the namespace's own collection does.
- The pool's collection makes the same requests for each member as the family root's collection: one manifest discovery, one pin listing, and one WAL tail read, including for each deleted member until it's retired. The time limit and the effect of one unreadable member are the same.
- A pool's content is deleted under the ordinary age rule by the first collection of the pool after its last member is retired. A family root's retirement deletes the family's content in the collection that retires the family root.
- Storage is measured for a pool. As with a fork family, a member's share of a pool's content isn't visible from object keys.
- A namespace can't move to another pool, because `pool_id` is set when the namespace is created.
- A failed create can leave a pool record, which stays until a collection of that pool finds the record older than its creation grace. The sweep finds such a pool by listing the pools.
- A pool whose members are all retired keeps its pool record, and a sweep that collects still runs a collection of that pool, in the same way that a deleted namespace stays in the sweep's listing.

## Changes to the specs and glossary

This section lists the sections of the storage format, the API spec, the design notes, and the glossary that the design changes.

The storage format changes in these sections:

- [Namespaces and identity](../specs/format.md#11-namespaces-and-identity) replaces the paragraph that defines a fork family with one that defines a pool and a member, and says that every manifest records the namespace's pool (`pool_id`).
- [File contents and ownership](../specs/format.md#15-file-contents-and-ownership) says that a namespace's content is stored in its pool, under `pools/{pool_id}/content/`, and that the reading namespace's `pool_id` and the content ID determine the key. The paragraphs about one prefix for a fork family, the chain creator, reading inherited content, and the "why" paragraph say the same for a pool and its members.
- [Storage layout](../specs/format.md#21-storage-layout) adds the pool prefix to the layout, and removes `uploads/` and `content/` from a namespace's prefix.
- [Upload sessions](../specs/format.md#51-upload-sessions) says that a session's records are stored under the pool's prefix, and that each record identifies the namespace that created the session.
- [Commit identity and retries](../specs/format.md#65-commit-identity-and-retries) says that a merge applies a fork's commits to another namespace in the fork's pool, and [Operations and WAL deltas](../specs/format.md#66-operations-and-wal-deltas) says that every member of a pool writes under the same prefix.
- [Publishing a manifest](../specs/format.md#72-publishing-a-manifest) replaces the "Family root" row of the first manifest's field table with a "Pool" row: a generated pool ID for a plain create, and the pinned source manifest's pool for a fork. A fold writes each chain's extent under the pool's prefix, and every successor manifest keeps the pool.
- [Creating a namespace](../specs/format.md#91-creating-a-namespace) has the five steps in [Creating a namespace](#creating-a-namespace), and says why the pool record is written before the hint.
- [Forking a namespace](../specs/format.md#92-forking-a-namespace) says that step 3 copies the pinned manifest's pool.
- [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations) moves a plain create's first manifest to the list of manifests that identical bytes confirm, because the manifest contains a pool ID that only one attempt generated. The example of two plain creates in its "why" paragraph is replaced by a manifest from the list that identical bytes don't confirm.
- [Retirement](../specs/format.md#95-retirement) says that retirement deletes a fork's source pin and no content, and that the pool's first member has no source pin, so its retirement deletes nothing. The paragraph about the family root being retired last, and the "why" paragraph about no member taking over, are replaced by the rule in [The pool's collection](#the-pools-collection) for a pool with no members.
- [Fork dependencies after deletion](../specs/format.md#96-fork-dependencies-after-deletion) says that every member finds inherited file bytes under the pool's prefix.
- [Cross-namespace copies and moves](../specs/format.md#97-cross-namespace-copies-and-moves) says "pool" in place of "fork family".
- [Garbage collection](../specs/format.md#11-garbage-collection) says that a pool's content stays while a retained view of any member refers to it, and removes the sentence about deleting the family's prefix when the family root is retired.
- [One complete collection](../specs/format.md#111-one-complete-collection) describes the two collections: a namespace's collection, which reads the namespace's roots and covers the objects under its prefix, and a pool's collection, with the steps in [The pool's collection](#the-pools-collection).
- [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules) says that the content object row applies to a pool's prefix and the roots of its members, and removes the row for an eligible family root's content and the exception for its content prefix.
- [Upload-session cleanup](../specs/format.md#116-upload-session-cleanup) says that the pool's collection covers the sessions of every member, and that cleanup derives every content key and provider cleanup target from the pool ID and the content ID in the session record. The row for a completed session in an eligible deleted namespace refers to [Content roots](../specs/format.md#119-content-roots) alone.
- [Pin cleanup](../specs/format.md#117-pin-cleanup) says that the pool's collection finds each member through the fork pin on the member's source.
- [Deleting a retired owner's content](../specs/format.md#118-deleting-a-retired-owners-content) removes step 1, so retirement deletes the source pin that the tombstone's fork basis refers to, when the namespace is a fork. The paragraph about the family root's retirement deleting the content of the whole family is removed.
- [Content roots](../specs/format.md#119-content-roots) describes the pool's collection: the pool record, the members (the pool's first member and every namespace forked from a member), the roots that it reads from each member, the two cases in which a pool has no members, and the deletion of a failed create's pool record. Its "why" paragraphs use the pool's collection and its members.
- [Alternatives considered](../specs/format.md#13-alternatives-considered) links to this note in place of fork family content. "Content stored under the namespace that writes it" says that with one prefix for each pool, a reference identifies the same object in every member, and "An owner namespace on every content reference" says that the reader's manifest records the pool.
- [Family versions](../specs/format.md#a1-family-versions) lists the pool record as a fifth kind of control object, with its envelope kind, encoding, and version.
- [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads) replaces the `family_root_id` row with `pool_id`, adds the pool record's fields (the pool ID, the namespace that was created with the pool, and `created_at_ms`), and says that both upload records are stored under the pool's prefix.
- [WAL records](../specs/format.md#a5-wal-records) says that a delta layout's creator can be another member of the pool.
- [Extent filter](../specs/format.md#extent-filter) says that a name is the extent's object key with the pool's content prefix (`pools/{pool_id}/content/`) removed.
- [Object keys](../specs/format.md#a8-object-keys) lists the pool keys in [Pools and pool records](#pools-and-pool-records), and says that content objects, upload records, and the temporary objects of content assemblies are stored under the pool's prefix.
- [The `texts` and `content_revisions` families](../specs/format.md#d4-the-texts-and-content_revisions-families) says that two references with the same content ID and size refer to the same bytes in every member of a pool.

The API changes in these sections:

- [Minimal upload, commit, and change-feed model](../specs/api.md#5-minimal-upload-commit-and-change-feed-model) says "the destination's pool" in its paragraph about prepared values.
- [Jobs and outcomes](../specs/api.md#jobs-and-outcomes) adds the pool route, `POST /v0/maintenance/pools/{pool_id}/runs`, which accepts a `gc` job, and [Operations and retry classes](../specs/api.md#operations-and-retry-classes) lists the route.
- [Garbage collection jobs](../specs/api.md#garbage-collection-jobs) splits the result fields. A pool's result has the fields in [The API](#the-api), and a namespace's result has no `content_objects`, `upload_sessions`, or `retired_content_objects`, and its `listed_bytes` covers the namespace's own prefix. `content_sweep_family_budget` becomes `content_sweep_pool_budget`, and `content_sweep_unreadable_member` describes a member of the pool.
- [Deleting, retaining, and reclaiming](../specs/api.md#deleting-retaining-and-reclaiming) says that the sweep lists the pools and runs a collection of each pool.
- [Content references and tokens](../specs/api.md#content-references-and-tokens) says that the reading namespace's pool and the content ID determine where the bytes are stored, and that a token is rejected in every other namespace, including another member of the same pool.
- [Creating and forking namespaces](../specs/api.md#creating-and-forking-namespaces) says that a plain create writes the pool record before the hint, and that an exact read-back of the manifest after an unknown transport outcome confirms a plain create, as it confirms a fork.
- [`GET /v0/namespaces/{ns}`](../specs/api.md#62-get-v0namespacesns) replaces `family_root_id` with `pool_id` in the example and in the `NamespaceMetadata` table.
- [`POST /commits`](../specs/api.md#68-post-commits) says that the chain of an append's base can be created by another member of the pool.
- [`POST /forks`](../specs/api.md#613-post-forks) says that the fork is a member of its source's pool and reads inherited content under the pool's prefix, and the example replaces `family_root_id` with `pool_id`.
- [`POST /rebases`](../specs/api.md#616-post-rebases) replaces `family_root_id` with `pool_id` in the example of the new fork.
- [Server requirements](../specs/api.md#71-server-requirements) says that a server resolves content through the reading namespace's pool and the reference's content ID.

In the design notes and guides, this note replaces [Fork family content](fork-family-content.md). [Content collection](content-collection.md), [Grep across a fork family](grep-fork-family.md), [Small-file bytes family](small-file-bytes-family.md), [Merging a fork](merge.md), and [Inline small content](inline-small-content.md) use pool and member in place of fork family and family root, and the small-file bytes note says that no retirement deletes content. [Session steps and sweeps](maintenance-hosting.md#session-steps-and-sweeps) and [Store request costs](maintenance-hosting.md#store-request-costs) in [Maintenance hosting](maintenance-hosting.md), and the self-hosting guide's [Background maintenance](../../crates/loonfs-server/docs/self-hosting.md#background-maintenance), describe the sweep's collection of pools. Background maintenance also says that a pool's content is deleted under the age rule after its last member is retired, and drops `deleted.retired_content_objects`. The [architecture overview](../specs/architecture.md) describes a pool in its paragraphs about content and forks.

In the glossary, two entries are added, "Pool" and "Pool record". The "Fork family" and "Family root" entries are removed, and the "Namespace", "Fork", "Content object", "Content reference", "Chain creator", "Upload session", "Content root", "Merge", "Collection", and "Namespace retirement" entries describe the pool instead. "Sweep" says that a sweep also lists the pools and runs a collection of each pool. After the change, "family" in the glossary means a metadata row family and nothing else.

## Alternatives considered

- **A pool stored under its first member's prefix.** This is the fork family design. The content of every member is stored under the prefix of the namespace that was created without a source, and every manifest records that namespace. So that namespace is retired last because of other members' data, its collections keep running after it's deleted, its retirement deletes every member's content, and the pool has no ID of its own.
- **A member record for every member.** Every namespace writes a record under its pool, and the collection lists the records once instead of walking fork pins. A fork then has two records of its creation, the fork pin and the member record. A member record's key isn't unique to one installation attempt, so the collection can't delete the record of a failed installation without risking the record of a later attempt for the same namespace ID, unless the manifest records which member record is its own, as a fork basis does for a fork pin.
- **Every pin of a pool stored under the pool's prefix.** One listing finds every member and every pin, which saves one pin listing for each member in each collection. But a pin protects a namespace's manifest and segments, so a namespace's own collection would list keys outside its prefix, and looking up a fork pin by its ID needs a listing.
- **Content stored under the namespace that writes it.** Each namespace stores its uploads and folded pieces under its own prefix. Content that one namespace wrote can be used by another namespace only by copying the bytes, or by a pin from the reader onto the writer for as long as the reference exists. A merge then makes the destination depend on the fork, which is usually the shorter-lived of the two, so the dependency has to be ended while the destination is still in use, by copying the merged bytes and proving that no reference to the fork's objects remains.
- **One pool for every namespace in a store.** Unrelated namespaces share one collection and one content listing. One unreadable namespace stops content collection for all of them, and the collection has to read every namespace in the store within its time limit.
- **A pool that the caller chooses when it creates a namespace.** A create request identifies an existing pool, so that namespaces created separately share content. A pool ID is then not generated by one create call, so the pool record alone doesn't identify every member that has no source, and each such member needs its own record with a key that's unique to one creation attempt. The design also needs a rule for which callers may add a namespace to a pool.
- **A pool that the caller creates before its first namespace.** Every create request identifies a pool, and a caller with one namespace manages a second resource that it never uses.
- **Content keyed by a hash of its bytes.** Identical uploads share one object. But a direct multipart upload has no SHA-256 of the whole object, so the key needs a second pass over the bytes or a copy after the upload. A chain's content ID stays the same while appends extend the chain, which a hash of the bytes can't do. The collection's safety also depends on each content ID being written once, and with a hash key, an upload can write a key again while a collection is deleting the object at that key.
