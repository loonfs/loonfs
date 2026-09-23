# Namespace generations

A namespace id can be deleted and created again right away. The new namespace is a new generation of the same id. It starts empty for a plain create or with the source tree for a fork, shares the id's object prefix with every earlier generation, and continues the id's counters from where the previous generation stopped. Nothing an earlier generation published is renamed, rewritten, or overwritten. Earlier generations stay in place until nothing needs them, and forks of them keep working.

This note describes the design. The [storage format specification](../specs/format.md) defines the durable objects it builds on.

## Why counters continue

Two things make a delete-and-recreate expensive in a naive design: cleaning up before the create, and telling old references from new ones after it. LoonFS avoids both by never restarting a counter.

- Manifests, WAL objects, and pins are numbered immutable objects created with put-if-absent. A number names one object forever. If a new generation restarted at number 1, a cached reader or a fork basis could not tell the first generation's manifest 3 from the second's.
- A fork basis names one manifest by owner, number, and checksum, and a pin under the owner protects it. Resolving the basis is one read. It does not pass through the owner's history and does not depend on how many generations the owner has had since.
- Readers and writers cache by manifest number and WAL number, and revalidate by probing forward. Monotone object numbers keep every cached entry either valid or detectably stale. Sequences and inode ids are logical values inside those objects and belong to one generation.

A generation boundary is therefore a lifecycle transition on the existing manifest chain, not a new address space. Recreation publishes the next manifest number, exactly as deletion, writer acquisition, and floor advancement do.

## What continues and what resets

| Manifest field | At a generation boundary |
| --- | --- |
| `manifest_no` | Continues. |
| `folded_wal_no` | Copied from the tombstone, which records the WAL tip at deletion, so no earlier WAL object replays into the new tree. |
| `head_seq`, `base_seq`, `retention_floor_seq` | Zero for a plain recreation. For a fork, the head and floor are the captured source sequence and the base is the source's, exactly as for a fresh fork. |
| `next_inode_id`, `next_run_no` | Restart at 2 and 0 for a plain recreation; a fork takes the source's allocators. Inode ids are unique within a generation. |
| `writer_epoch`, `compactor_epoch` | Increment. Sessions and compactors that captured the previous generation are fenced. |
| `generation` | Increments. A newly created namespace is generation 1. |
| `generation_first_manifest_no` | Becomes the new manifest's own number. |
| `content_store_id` | A fresh domain for a plain recreation; the source's domain for a fork. |
| `created_at_ms`, `created_by` | Taken from the create or fork request. |
| `access` | Taken from the create request for a plain recreation; copied from the source for a fork. |
| `fork_basis` | Absent for a plain recreation; the fork's basis for a fork. |
| `status`, `writer` | Active, absent. |
| `runs` | Empty for a plain recreation; copied from the source for a fork. |
| `head_commit_id` | The genesis commit id for a plain recreation; the source's head commit id for a fork. |

The successor rule allows these fields to change only when `generation` increments, and allows `generation` to increment only from a deleted manifest to an active one whose `generation_first_manifest_no` equals its own number. Within a generation the identity fields are immutable and a deleted manifest has no active successor. An empty active manifest describes a generation's genesis: equal head, base, and floor sequences, the genesis commit id, and no runs. Its allocators are not constrained, because they continue from the previous generation. Its root inode and any root access grants are synthesized at the generation's first sequence with the generation's creation time.

Sequences and inode ids repeat across generations, as row ids do when a database drops and recreates a table under one name. A change-feed cursor or an inode reference taken in an earlier generation is not distinguishable by its value. The feed refuses a cursor above the current head, but once the new generation passes that sequence the cursor is accepted and the consumer applies new events to an old tree. A consumer that can span a recreation compares `generation` on the namespace object and rebootstraps when it changes. The same holds for `expected_head_seq` and inode-addressed preconditions; `expected_generation` is the guard that makes them exact. The server-side grep index records the generation it indexed and rebuilds on a change.

## Recreating a namespace

Creating a namespace whose current manifest is deleted recreates it. A fork into a deleted id recreates it the same way, with the source's runs and content domain. There is no separate operation and no flag. The create response and the namespace object carry `generation`.

1. Load the current manifest. Active status answers `namespace_exists`, or the current summary with `allow_existing`. Deleted status continues below. An absent namespace takes the ordinary creation path.
2. Take the WAL tip from the tombstone's folded WAL number. Deletion stamps the discovered tip there, and publishing the tombstone acquired the writer epoch, so no further WAL object can be published under it. Recreation reads no WAL object, which matters because the deleted generation's WAL objects are unprotected and may already be collected.
3. Write a retired pin over the tombstone with put-if-absent. Its id is `pin_{tombstone_no:020}-` followed by sixteen hex characters derived from the namespace id and the tombstone number, so repeated attempts land on one record. The next section describes the pin.
4. Write the content-store descriptor with put-if-absent, for a fresh domain on a plain recreation or the source's domain on a fork.
5. Build the new manifest from the table above and publish it at the tombstone's number plus one with put-if-absent, within the metadata publication budget measured from step 1. A fork also stays within its installation budget, measured before creating the source pin.
6. Publication raises the hint to the new manifest number. A failed raise does not fail the creation.

A losing manifest put reads the winner. An active winner is a concurrent recreation and answers `namespace_exists`, or the winner with `allow_existing`. A newer tombstone means another generation was created and deleted, so recreation retries over that tombstone. A put with an unknown transport outcome confirms its own success only by reading back the exact proposed manifest.

A lost attempt can leave its retired pin and its descriptor behind. The pin is over a real tombstone and describes real reclamation work, and its derived id means concurrent attempts wrote one record. The descriptor is an unused domain, as a lost ordinary creation can leave, and is never collected.

## Reclaiming a prior generation

A deleted generation still owns content under its owner prefix in its content domain, and if it was a fork it still holds a pin under its source. Reclaiming it sweeps that prefix and deletes that pin. While a deleted manifest is current, the collector finds it through the manifest itself. After recreation the current manifest is active, so the collector needs another way to find prior generations, and there is no mutable object in which to record a retirement deadline.

### Retired pins

A retired pin is a pin record with owner `{"kind": "retired"}` over a tombstone manifest. Recreation writes it before publishing the new generation. It is the collector's index of prior generations: the collector already lists the pin prefix on every pass, and the pin's manifest reference names the tombstone that holds every fact reclamation needs. The pin protects its tombstone like any other pin, so the ordinary manifest sweep cannot remove the tombstone while reclamation is pending. Reclamation deletes the pin last, after which the tombstone is an ordinary old manifest and ages out with the rest.

In the owner table of format section 8.1, a retired pin stores no owner fields and lives until its generation is reclaimed. The checkpoint API never lists, reads, creates, or deletes one.

### Stateless retirement

A tombstone records `deleted_at_ms`, the call clock of the deletion, in its deleted status. The deleted status carries no retirement deadline. The deadline is derived:

```text
reclaim_after_ms = deleted_at_ms + max(configured_grace, NAMESPACE_RETIREMENT_GRACE_MS)
```

A generation is reclaimable in a collection pass when the pass's clock is at or past that deadline and the pass's complete pin listing holds no pin over the generation's manifest range other than the retired pin itself. The range runs from the tombstone's `generation_first_manifest_no` through the tombstone's own number. A pin's manifest number is part of its id, so this check reads no pin bodies.

The grace constant covers a publication budget, the provider operation deadline and attempt timeout, the direct transfer capability lifetime, and the clock safety margin. Those terms bound how far the deleter's clock can lag the tombstone it publishes. A fork whose installation was in flight when the deletion published writes its pin within the fork installation budget, which the grace also covers, so a listing taken after the deadline either sees that pin or the fork failed.

Nothing creates a new pin over a prior generation after the deadline. The deleted manifest refuses checkpoint and fork creation while it is current, and after recreation the checkpoint surface refuses a basis below the current generation.

There is no retirement publication. A collector never publishes a manifest to retire a generation, no deadline is stored, and there is no rule about a deadline regressing because there is no deadline to regress.

### One procedure, two discovery paths

Reclaiming a tombstone `T`:

1. Load `T` through the retired pin's manifest reference and verify its checksum, or use the current manifest when it is itself deleted.
2. Confirm the deadline and the pin range. Otherwise report the derived deadline and stop.
3. Confirm that the retired pin still exists, or that the current manifest is still `T`. Then sweep `content-stores/{T.content_store_id}/objects/{namespace_id}/{T.generation}/` under the rules of format section 11.8.
4. If `T` has a fork basis, delete the source pin it names.
5. Delete the retired pin, if there is one.

Steps 3 and 4 are idempotent, and a pass that stops early repeats them on its next visit. Step 5 comes last so that a pass which stops early never loses the index entry. A deleted namespace that has not been recreated has no retired pin; the collector reaches its tombstone through the current manifest and runs the same procedure without step 5.

Everything else a prior generation left behind is collected by the existing rules. Its manifests are below the hint and unpinned once the retired pin is gone. Its segments are unreferenced once no pinned manifest lists them. Its WAL objects are below the new generation's folded number and floor.

## Checkpoints across generations

Pins share one prefix. A pin over a manifest below the current manifest's `generation_first_manifest_no` belongs to a prior generation.

- The checkpoint API lists, reads, and deletes only pins at or above that number. Pin keys sort by manifest number and the listing already starts after a durable key, so it starts at the current generation's first manifest number and never lists or loads a prior-generation record. Prior-generation user checkpoints and snapshots are hidden, and a checkpoint id that names one answers as a deleted record does.
- Prior-generation user and snapshot pins are collected under the rule for pins on a deleted namespace: creation plus the ordinary grace. A user pin without expiry does not survive the deletion of its generation.
- Forking from a snapshot or reading through a checkpoint requires a basis at or above that number. A fork from the head always satisfies this.

### Fork pins whose target was recreated

A fork pin under source `S` is owned by target `T`. The source's collector retains it while `T`'s current manifest names it, and deletes it when `T` names another pin or none, treating the pin as an abandoned installation attempt. A recreated `T` has a different fork basis or none, yet `T`'s earlier generation and any forks of that generation can still reference `S`'s segments through that pin.

When `T`'s current manifest does not name the pin and `T`'s generation is above 1, `S`'s collector lists `T`'s retired pins and loads each tombstone. A tombstone whose fork basis names the pin retains it, and `T`'s own collector deletes it when it reclaims that generation. If no tombstone names it, the pin is an abandoned attempt and is deleted. A target at generation 1 needs no retired-pin reads.

## Content

Content keys carry the owner namespace id and owner generation. Two generations never share an owner prefix because the prefix carries the generation, whichever content domain they use. The retired owner's sweep deletes only that generation's prefix. Content references carry both owner fields; the domain comes from the manifest a reader resolves them through.

Completed-upload receipts and content tokens are bound to the namespace and owner generation. An upload session opened under one generation cannot be published in the next: its receipt names the prior generation and admission refuses it. Direct transfer capabilities issued under the old generation expire on their own inside the retirement grace.

A cross-namespace import that reads content from a deleted owner resolves the content-store id from the pinned manifest it imports through, not from the owner's current manifest. The owner's current manifest may belong to a later generation with a different domain.

## Writers, retries, and the API

- **Writer sessions.** The recreated manifest carries the next writer epoch. The session that published the deletion sees `writer_fenced` on its next publication and is terminal, as any fenced session is. A server that cached an engine for the id recovers exactly as it does when another server takes over a namespace.
- **Commit retries.** Commit receipts are metadata rows and do not cross the boundary. A retry of a commit id from an earlier generation executes as a new commit in the current one. Any name-addressed system has this property. A caller that must not write into a recreated namespace passes `expected_generation` as a request-level precondition, which fails when the generation differs.
- **Namespace object.** `generation` is present on the namespace object, the create response, and diagnostics.
- **Inode ids.** Inode 1 is the root and allocation starts again at 2 in every generation. An inode id identifies an item within one generation.

## Cost under churn

Each delete-and-recreate cycle adds a fixed number of objects and grows nothing that the hot path reads.

| Object | Per cycle | Lifetime |
| --- | --- | --- |
| Tombstone manifest | 1 | Until its generation is reclaimed and it ages out as an old manifest |
| Retired pin | 1 | Until its generation is reclaimed |
| Content-store descriptor | 1 for a plain recreation; shared for a fork | Never collected, like every descriptor |
| New generation's manifest | 1 | An ordinary manifest |

The current manifest carries two integers for generations, whatever their count. Reads and commits load the hint, the current manifest, and the WAL tail, and never learn how many generations exist. A fork basis is one read regardless of the source's history.

A collection pass costs one tombstone read per unreclaimed generation on top of the pin listing it already performs. When a collector runs regularly, that is the number of generations deleted within one grace window. When no collector runs, the backlog waits at no cost to anyone else and the first pass clears it. A generation held by a long-lived fork or checkpoint costs one tombstone read per pass until it is released. It blocks nothing else: every generation is reclaimed on its own evidence, in any order.

## Alternatives

**A name-to-id indirection.** Generating a hidden namespace id per creation and mapping the public name to it isolates generations completely. It also adds a lookup to every commit and read, or a cache of that mapping that every server must invalidate on delete. The hot path is the constraint this design serves, so the indirection is not used.

**A key prefix per generation.** Placing each generation under its own key prefix leaves earlier generations exactly as they are. It also means every reference to a manifest or segment must carry the owner's generation. Continuing the counters isolates those objects without adding generation fields to their references. Content keys carry the owner generation because content is reclaimed by owner prefix.

**A ledger in the manifest.** Recording every unreclaimed generation in the current manifest puts the collector's work list on the hot path. It grows with every cycle until a collector runs, and no collector runs by default.

**A chain of tombstones.** Pointing each manifest at the previous generation's tombstone keeps the current manifest small, but reclamation can only unlink at the head of the chain. A generation held by a fork keeps every older generation linked and revisited on every pass, and the collector has to walk the chain to find its work. Retired pins give each generation its own record, found by a listing the collector already performs and reclaimed independently.

**Sweeping before recreation.** Deleting a namespace's objects before allowing the id again makes creation linear in the namespace's size and breaks every fork that shares those objects.
