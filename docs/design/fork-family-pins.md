# Fork family pins

This note explains where LoonFS stores the pins of a [fork family](../specs/glossary.md#fork-family), and how the family root's collection uses them to find the family's members and to decide which pins are still needed. [Records and owners](../specs/format.md#81-records-and-owners) in the storage format defines the keys, [Content roots](../specs/format.md#119-content-roots) defines the collection, [Pin cleanup](../specs/format.md#117-pin-cleanup) defines when a pin is dead, and [Retirement](../specs/format.md#95-retirement) defines when a deleted member is retired.

## The problem

A [fork](../specs/glossary.md#fork) reads its source's metadata through a fork pin, which is a pin on the source's manifest whose owner is the fork. The fork pin is also the only durable record that the fork exists, because nothing else in the source records which namespaces were forked from it. The family root's collection protects the family's content with roots from every member (see [Fork family content](fork-family-content.md#collection)), so before it can delete anything, it has to find every member, and every fork pin is one edge of the family's tree.

If each pin were stored under the prefix of the namespace whose manifest it keeps, the collection would find the members by walking the tree. It would list the family root's pins, then the pins of each fork that listing found, and so on until a listing found no new member. To decide whether a fork pin is still needed, the collection would also read the pin's record and its target's state, which is 4 requests for each fork pin (the pin record, the target's hint, one HEAD request for the next manifest, and the target's manifest). On a flat family of 2,500 forks of one namespace, each collection would make about 2,500 listings and about 10,000 probe requests. Both numbers grow with the number of forks, and a family whose forks are nested needs one round of listings for each level.

## The design

### Keys

Every pin of a fork family is stored under the family root's prefix, in a sub-prefix for the member whose manifest the pin keeps. A fork pin's key also includes the namespace that the fork pin was created for (its "target"):

```text
namespaces/{family_root_id}/pins/{member_namespace_id}/{pin_id}.json
namespaces/{family_root_id}/pins/{member_namespace_id}/{pin_id}-{target_namespace_id}.json
```

The first form is a user pin's key, and the second is a fork pin's key.

The prefix `namespaces/{family_root_id}/pins/` is the family's "family pin prefix". A namespace created without a source is its own family root, so its own pins are stored under `namespaces/{namespace_id}/pins/{namespace_id}/`.

A user pin's ID has the form `pin_{manifest_no:020}-{16 lowercase hex}`, as a snapshot ID does in the API. A fork pin's ID has the same form with its kind in front, `fork_{manifest_no:020}-{16 lowercase hex}`, so it's always 42 characters. The target is in the key and not in the ID, because a namespace ID is chosen by the caller, can be up to 128 bytes long, and can contain `-`.

For example, suppose `sandbox` is a fork of `main` at manifest 9, `trial` is a fork of `sandbox` at manifest 4, and a caller took a snapshot of `sandbox` at manifest 4. The family pin prefix of `main` then contains three keys:

```text
namespaces/main/pins/main/fork_00000000000000000009-3f9c2a7d41e05b68-sandbox.json
namespaces/main/pins/sandbox/fork_00000000000000000004-5c2e8f1a9b7d4e60-trial.json
namespaces/main/pins/sandbox/pin_00000000000000000004-0000000000000009.json
```

A fork pin's ID always has the same length, so the target is the text after the `-` that follows the ID, up to `.json`, even when the target's ID contains `-`. The target in the key and the record's `owner.target_namespace_id` are equal, and a pin whose key and record disagree is corrupt. The pin record has the same fields under both key forms.

A fork pin's ID keeps its random suffix because each attempt to install a fork needs a pin that's unique to that attempt. A fork's first manifest refers to its source pin's ID, and identical manifest bytes show that an installation succeeded only when the manifest refers to something that the attempt created (see [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations)).

A read through a pin loads the namespace's current manifest, and the manifest's `family_root_id` identifies the family pin prefix. A fork knows its own fork pin's key, because the fork is the target. A lookup of a fork pin by its ID alone lists the prefix `namespaces/{family_root_id}/pins/{member_namespace_id}/{pin_id}-`, which returns one key or none in one request. Creating a pin, and extending or deleting a user pin, follow [Pins](../specs/format.md#8-pins) with these keys.

A listing of a namespace's pins lists the namespace's sub-prefix in key order, and `fork_` keys sort before `pin_` keys, so a checkpoint listing returns a member's fork pins before its snapshots. The forks of a namespace are the targets in the `fork_` keys under its sub-prefix, so a listing of the forks needs no record reads.

### The family root's collection

A member's own collection lists its own sub-prefix for its reference roots and judges its user pins, as a collection on a namespace without forks does. The family root's collection judges every pin in the family and finds every member. It:

1. Captures the family root's state: its hint, and its current manifest or tombstone.
2. Lists the family pin prefix completely (the "first listing"). The members are the family root and the target of every fork pin key in the listing. Every fork pin is under the family root's prefix, so one listing includes every edge of the tree, however deep it is.
3. Captures each other member's state: its hint, its current manifest or tombstone, and its WAL tail. A member whose hint is absent isn't captured, because its installation hasn't written the hint.
4. Lists the family pin prefix again, after every capture (the "second listing"). The roots of each captured member are its captured current manifest and the manifests above it, and the manifest of every pin under its sub-prefix in the second listing. The content roots come from the captured members' views as in [Fork family content](fork-family-content.md#collection).
5. Judges every pin in the second listing.
6. Retires the eligible tombstones in the order it found them eligible, so a member is retired before its source, then deletes the other dead pins, one DELETE request each, and then collects unreferenced objects in the usual order.

A pin in the first listing that's missing from the second was deleted in between, and it protects nothing. A fork pin in the second listing whose target isn't in the first belongs to a fork installed in between. The collection doesn't capture that fork and deletes nothing under its sub-prefix, and the fork pin protects the source manifest that the fork reads through.

### Judging pins

A user pin is dead at its expiry plus the grace window, or at its creation plus the grace window if its member is a tombstone, as in a member's own collection.

A pin of either kind under the sub-prefix of a namespace that is neither the family root nor the target of any fork pin in the first listing is dead after its creation grace. Such a namespace was retired, or its installation never finished, and a pin can still land there when its creator stalled before the put or the put arrived late. Without this rule one such pin would keep the family root from ever being retired.

A fork pin on member `m` whose target is `t` is judged from the key and the captured states. Each fork's manifest records the manifest reference and the ID of its source pin (the "fork basis"), and that record shows which fork pin the fork reads through:

- If `t` was captured and its fork basis refers to this pin, the pin is live, unless `t` is a tombstone that's eligible for retirement in this collection (see [Retirement](#retirement)).
- If `t` was captured and its fork basis refers to another pin, or `t` has no manifest, the pin belongs to an abandoned or losing installation, and it's dead after its creation grace (the grace window after the pin's creation time).
- If `t` wasn't captured, the collection sends one HEAD request for `t`'s hint. If the hint exists, `t`'s installation finished after the first listing or after the collection tried to capture it, and the pin is live. If it doesn't, the pin is dead after its creation grace.
- If a read of `t`'s state failed, the pin is live.

A target whose installation wrote its hint and then stopped is kept by the HEAD request in the first collection that sees it. The next collection captures it, finds no manifest, and judges the pin dead after its creation grace.

Before the collection judges a fork pin dead, it reads the pin's record, to check the creation time (`created_at_ms`) and that the record's target matches the key. A pin inside its creation grace is kept, whichever rule applies. The collection reads no other fork pin record.

### Retirement

A captured tombstone is eligible for retirement when its retirement deadline has passed and every pin under its sub-prefix is dead. A fork pin whose target is an eligible tombstone is dead, so the collection judges the pins again after it finds each eligible tombstone, until no judgment changes. That takes at most one round for each member. The family root is eligible when its deadline has passed and every pin under the whole family pin prefix is dead, which means that every other member is retired in this collection or was retired earlier.

For example, suppose `trial` is a fork of `sandbox`, `sandbox` is a fork of `main`, and all three are deleted with their deadlines passed. `trial` has no pins, so it's eligible, which makes its fork pin on `sandbox` dead. `sandbox` then has no live pins, so it's eligible, and its fork pin on `main` is dead, so `main` is eligible too. One collection retires all three.

The collection retires eligible members before it deletes any other dead pin, in the order it found them eligible, so a member is retired before its source. Retiring a member deletes every pin under its sub-prefix and then its fork pin on its source, in that order. That fork pin is the key the first listing finds the member by, so a retirement that stops part way leaves a member that the next collection still finds, where deleting the key first and stopping would leave a member that no listing finds again. Retiring the family root also deletes every object under the family's content prefix (see [Deleting a retired owner's content](../specs/format.md#118-deleting-a-retired-owners-content)). The collection stores no record of its judgments. A collection that stops part way leaves nothing inconsistent, because the next collection judges every pin again from its own listings and captures.

### Why two listings, and why in that order

Why does the collection list the family pin prefix twice? A pin protects its manifest in one of two ways. If the pin is in the second listing, the collection loads the pin's manifest directly. If it isn't, the pin's put completed after the second listing began, because the format requires strongly consistent writes and reliable listings (see [Required operations](../specs/format.md#31-required-operations)). The pin's creator loads the member's current manifest after the put and accepts the pin only if the pinned manifest is still current (see [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin)). So the pinned manifest was current after the collection captured the member, and its number is at or above the captured current manifest, which the collection protects along with every manifest above it (see [Reference roots](../specs/format.md#112-reference-roots)).

Why does the first listing come before the captures, and the second after them? The collection has to know the members before it can capture them, so the first listing comes first. The argument above needs a listing that starts after every capture, so the second listing comes last. A single listing before the captures would be unsafe. A pin written after that listing, and verified before the collection captured its member, can keep a manifest below the captured current one, and the collection wouldn't protect that manifest. A single listing after the captures can't show which members to capture.

## Costs

- A collection of a family of 2,500 forks of one namespace makes 2 listings of about 3 pages each, and no probe requests for each pin. It still reads each member's hint and current manifest, about 2 requests for each member, because the content roots need every member's state.
- Creating a fork makes 15 requests, and storing its pin under the family root's prefix adds none. Forks of an idle namespace share one pinned manifest.
- A fork pin's ID starts with `fork_` and a snapshot's ID with `pin_`, so a client that parses `Checkpoint.checkpoint_id` or `fork_basis.source_pin_id` sees both forms, and a checkpoint listing returns a member's fork pins before its snapshots.
- A lookup of a fork pin by its ID alone costs one listing request instead of one GET request.
- A fork's pins aren't under the fork's own prefix, so a listing of that prefix doesn't show them.
- Each fork pin that a collection deletes costs one record read as well as the DELETE request, and each fork pin whose target wasn't captured costs one HEAD request.

Storing pins under the family root changes nothing in the protocol for creating and verifying a pin, in reads through a pin, extension, deletion, expiry, forks of snapshots, the fork basis, the pin record, or the API's routes.

## Alternatives considered

- **Pins stored under the namespace they protect.** Each pin could be stored under its member's own prefix. Finding the members would then need a walk with one listing for each member, and judging each fork pin would need 4 requests, so a family of 2,500 forks would cost about 2,500 listings and about 10,000 probe requests on every collection. A deep family would need one round of listings for each level, and a chain of deleted members would retire one level in each collection, because a member's own collection would delete its pin on its source.
- **Pins listed in the manifest.** Each manifest could list the pins on it, so that creating a pin would publish a manifest and need no verification or listing. But each live fork adds about 288 bytes to its source's manifest, about 2.9 MB at 10,000 forks, which every publication writes and every cold open reads. Concurrent forks of one source would contend for its next manifest number, and on S3, 100 concurrent forks took 14.2 s instead of 10.2 s and made 46 to 62 percent more requests (see [Alternatives considered](../specs/format.md#13-alternatives-considered)).
- **Pins as rows that a fold adds.** A pin could be a row that a fold adds, with the manifest that the fold publishes as the target, so the publication itself would show that the target is current. But a fork's first manifest lists its source's runs, so forks would inherit the rows unless they were in a separate segment family. A manifest reference includes the target's payload checksum, so a row in a segment that the target lists can't contain that reference. The design would need separate target rules for a fork of a snapshot and for a fork of an idle namespace, and without the second rule, 2,500 forks of an idle template would keep 2,500 pinned manifests where pin objects share one. The collection would still find the members one at a time, by reading each member's rows, where one listing of the family pin prefix finds them all.
- **A manifest field that points to a separate pin-list object.** The manifest wouldn't grow with the number of pins, but every pin would still publish a manifest of its member, so concurrent forks of one source would still contend for its next manifest number.
- **Keeping replaced manifests for the grace window.** A pin on a manifest that was just replaced would then be safe without loading the manifest again after the put. But every collection would have to load each recently replaced manifest to protect its segments.
- **A single listing of the family pin prefix.** A listing before the captures can miss a pin whose manifest is below the captured current one, and a listing after the captures can't show which members to capture (see [Why two listings, and why in that order](#why-two-listings-and-why-in-that-order)).
