# Namespace generations

A namespace id can be deleted and created again right away. The new namespace is a new generation of the same id. It starts empty, shares the id's object prefix with every earlier generation, and continues the id's counters from where the previous generation stopped. Nothing an earlier generation published is renamed, rewritten, or overwritten. Earlier generations stay in place until nothing needs them, and forks of them keep working.

This note describes the design. The [storage format specification](../specs/format.md) defines the durable objects it builds on.

## Why counters continue

Two things make a delete-and-recreate expensive in a naive design: cleaning up before the create, and telling old references from new ones after it. LoonFS avoids both by never restarting a counter.

- Manifests, WAL objects, and pins are numbered immutable objects created with put-if-absent. A number names one object forever. If a new generation restarted at number 1, a cached reader or a fork basis could not tell the first generation's manifest 3 from the second's.
- A fork basis names one manifest by owner, number, and checksum, and a pin under the owner protects it. Resolving the basis is one read. It does not pass through the owner's history and does not depend on how many generations the owner has had since.
- Readers cache by manifest number, WAL number, and sequence, and revalidate by probing forward. Monotone counters keep every cached entry either valid or detectably stale.

A generation boundary is therefore a lifecycle transition on the existing manifest chain, not a new address space. Recreation publishes the next manifest number, exactly as deletion, writer acquisition, and floor advancement do.

## What continues and what resets

| Manifest field | At a generation boundary |
| --- | --- |
| `manifest_no` | Continues. |
| `last_folded_wal_no` | Becomes the discovered WAL tip, so no earlier WAL object replays into the new tree. |
| `head_seq`, `base_seq`, `retention_floor_seq` | Become the tombstone's `head_seq` plus one. Recreation consumes one sequence. |
| `next_inode_id`, `next_run_no` | Continue. Every inode id other than the root is allocated once per namespace id. |
| `writer_epoch`, `compactor_epoch` | Increment. Sessions and compactors that captured the previous generation are fenced. |
| `generation` | Increments. A newly created namespace is generation 1. |
| `generation_first_manifest_no` | Becomes the new manifest's own number. |
| `content_store_id` | A fresh content domain. |
| `created_at_ms`, `created_by`, `access` | Taken from the create request. |
| `fork_basis` | Absent. A recreated namespace is not a fork. |
| `status`, `writer`, `runs` | Active, absent, empty. |
| `head_commit_id` | The genesis commit id. |

The successor rule allows these fields to change only when `generation` increments, and allows `generation` to increment only from a deleted manifest to an active one whose `generation_first_manifest_no` equals its own number. Within a generation the identity fields are immutable and a deleted manifest has no active successor, as today. An empty active manifest describes a generation's genesis: equal head, base, and floor sequences, the genesis commit id, and no runs. Its allocators are not constrained, because they continue from the previous generation. Its root inode and any root access grants are synthesized at the generation's first sequence with the generation's creation time.

Consuming a sequence is what makes the boundary visible to every consumer that resumes by sequence. The change feed already answers `rebootstrap_required` for a cursor below the retention floor, and the grep index already rebuilds on that answer. A cursor left at the tombstone's final sequence is below the new floor, so no consumer can apply the new generation's commits on top of a tree from the old one.

## Recreating a namespace

Creating a namespace whose current manifest is deleted recreates it. There is no separate operation and no flag. The create response and the namespace object carry `generation`.

1. Load the current manifest. Active status answers `namespace_exists`, or the current summary with `allow_existing`. Deleted status continues below. An absent namespace takes the ordinary creation path.
2. Discover the WAL tip by probing forward from the greater of the hint's WAL number and the tombstone's folded number. Publishing the tombstone acquired the writer epoch, so no further WAL object can be published under it.
3. Write a retired pin over the tombstone with put-if-absent. Its id is `pin_{tombstone_no:020}-` followed by sixteen hex characters derived from the namespace id and the tombstone number, so repeated attempts land on one record. The next section describes the pin.
4. Write the content-store descriptor for the fresh domain with put-if-absent.
5. Build the new manifest from the table above and publish it at the tombstone's number plus one with put-if-absent, within the metadata publication budget measured from step 1.
6. Raise the hint to the new manifest number. A failed raise does not fail the creation.

A losing manifest put reads the winner. An active winner is a concurrent recreation and answers `namespace_exists`, or the winner with `allow_existing`. Nothing else publishes a successor to a tombstone, so any other winner is corruption. A put with an unknown transport outcome confirms its own success only by reading back the exact proposed manifest.

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

The grace constant is unchanged. It already covers a publication budget, the provider operation deadline and attempt timeout, the direct transfer capability lifetime, and the clock safety margin. Those terms bound how far a collector's clock can lag the retirement it publishes today. Here they bound how far the deleter's clock can lag the tombstone it publishes, which is the same budget. A fork whose installation was in flight when the deletion published writes its pin within the fork installation budget, which the grace also covers, so a listing taken after the deadline either sees that pin or the fork failed.

Nothing creates a new pin over a prior generation after the deadline. The deleted manifest refuses checkpoint and fork creation while it is current, and after recreation the checkpoint surface refuses a basis below the current generation.

There is no retirement publication. A collector never publishes a manifest to retire a generation, no deadline is stored, and there is no rule about a deadline regressing because there is no deadline to regress.

### One procedure, two discovery paths

Reclaiming a tombstone `T`:

1. Load `T` through the retired pin's manifest reference and verify its checksum, or use the current manifest when it is itself deleted.
2. Confirm the deadline and the pin range. Otherwise report the derived deadline and stop.
3. Confirm that the retired pin still exists, or that the current manifest is still `T`. Then sweep `content-stores/{T.content_store_id}/objects/{namespace_id}/` under the rules of format section 11.8.
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

A fork pin under source `S` is owned by target `T`. The source's collector retains it while `T`'s current manifest names it, and deletes it when `T` names another pin or none, treating the pin as an abandoned installation attempt. A recreated `T` has no fork basis, yet `T`'s earlier generation and any forks of that generation can still reference `S`'s segments through that pin.

The decision table gains one condition. When `T`'s current manifest does not name the pin and `T`'s generation is above 1, `S`'s collector lists `T`'s retired pins and loads each tombstone. A tombstone whose fork basis names the pin retains it, and `T`'s own collector deletes it when it reclaims that generation. If no tombstone names it, the pin is an abandoned attempt and is deleted. A target at generation 1 keeps the existing rule with no extra reads.

## Content

Each generation has its own content domain. The retired owner's sweep deletes every object under one owner prefix in one domain, so two generations sharing a domain would let one generation's sweep delete the other's bytes. A fresh domain per generation keeps the sweep a prefix delete with no further checks and changes no key grammar. Content references still carry the owner namespace id; the domain comes from the manifest a reader resolves them through.

Completed-upload receipts and content tokens are bound to the namespace and its content-store id. An upload session opened under one generation cannot be published in the next: its receipt names the old domain and admission refuses it. Direct transfer capabilities issued under the old generation expire on their own inside the retirement grace.

A cross-namespace import that reads content from a deleted owner resolves the content-store id from the pinned manifest it imports through, not from the owner's current manifest. The owner's current manifest may belong to a later generation with a different domain.

## Writers, retries, and the API

- **Writer sessions.** The recreated manifest carries the next writer epoch. The session that published the deletion sees `writer_fenced` on its next publication and is terminal, as any fenced session is. A server that cached an engine for the id recovers exactly as it does when another server takes over a namespace.
- **Commit retries.** Commit receipts are metadata rows and do not cross the boundary. A retry of a commit id from an earlier generation executes as a new commit in the current one. Any name-addressed system has this property. A caller that must not write into a recreated namespace passes `expected_generation` as a request-level precondition, which fails when the generation differs.
- **Namespace object.** `generation` is present on the namespace object, the create response, and diagnostics. A fork's `fork_basis` also reports `source_generation`.
- **Root inode.** Inode 1 is the root in every generation. Every other inode id is allocated once per namespace id because the allocator continues.

## Cost under churn

Each delete-and-recreate cycle adds a fixed number of objects and grows nothing that the hot path reads.

| Object | Per cycle | Lifetime |
| --- | --- | --- |
| Tombstone manifest | 1 | Until its generation is reclaimed and it ages out as an old manifest |
| Retired pin | 1 | Until its generation is reclaimed |
| Content-store descriptor | 1 | Never collected, like every descriptor |
| New generation's manifest | 1 | An ordinary manifest |

The current manifest carries two integers for generations, whatever their count. Reads and commits load the hint, the current manifest, and the WAL tail, and never learn how many generations exist. A fork basis is one read regardless of the source's history.

A collection pass costs one tombstone read per unreclaimed generation on top of the pin listing it already performs. When a collector runs regularly, that is the number of generations deleted within one grace window. When no collector runs, the backlog waits at no cost to anyone else and the first pass clears it. A generation held by a long-lived fork or checkpoint costs one tombstone read per pass until it is released. It blocks nothing else: every generation is reclaimed on its own evidence, in any order.

## Alternatives

**A name-to-id indirection.** Generating a hidden namespace id per creation and mapping the public name to it isolates generations completely. It also adds a lookup to every commit and read, or a cache of that mapping that every server must invalidate on delete. The hot path is the constraint this design serves, so the indirection is not used.

**A key prefix per generation.** Placing each generation under its own key prefix leaves earlier generations exactly as they are. It also means every reference to a manifest, segment, or content object must carry the owner's generation, including the content reference on the public API. Continuing the counters gives the same isolation with no reference change.

**A ledger in the manifest.** Recording every unreclaimed generation in the current manifest puts the collector's work list on the hot path. It grows with every cycle until a collector runs, and no collector runs by default.

**A chain of tombstones.** Pointing each manifest at the previous generation's tombstone keeps the current manifest small, but reclamation can only unlink at the head of the chain. A generation held by a fork keeps every older generation linked and revisited on every pass, and the collector has to walk the chain to find its work. Retired pins give each generation its own record, found by a listing the collector already performs and reclaimed independently.

**Sweeping before recreation.** Deleting a namespace's objects before allowing the id again makes creation linear in the namespace's size and breaks every fork that shares those objects.
