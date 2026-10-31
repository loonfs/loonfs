# Fork family content

This note explains where LoonFS stores content objects and upload records, and why every member of a fork family writes them under one prefix. [File contents and ownership](../specs/format.md#15-file-contents-and-ownership) in the storage format defines the rule, [Content roots](../specs/format.md#119-content-roots) defines how the family's content is collected, and [Retirement](../specs/format.md#95-retirement) defines when it's deleted.

## The problem

A content reference identifies a chain by a random content ID, and a reader finds the chain's bytes by deriving object keys from the reference. Nothing in the reference says which namespace wrote the bytes, and nothing in the read path needs to know. A fork inherits its source's references and reads the same objects, and a fork's revisions and the source's revisions can refer to one object for as long as both namespaces exist.

So the question for the format is which prefix stores an object, because the prefix decides which garbage collection pass can delete the object, and which namespace's retirement deletes it. If each namespace stored the content it wrote under its own prefix, an object that two namespaces reference would be protected by one collector and read by two. A fork pin keeps that safe in one direction, from a fork to its source. Every other case, such as a commit in one namespace that references content a related namespace wrote, would need a pin in that direction too, or a copy of the bytes under a new content ID. A copy costs the bytes, a second content ID for the same data, and a cache miss for every reader that already had the first object.

## The design

A "fork family" is a namespace that was created without a source and every namespace forked from it, directly or through other forks. The namespace that was created without a source is the "family root". Every manifest records its family root (`family_root_id`). A plain create records the namespace itself, a fork copies the value from the pinned source manifest, and the value can't change.

Every member of a family writes new content under the family root's prefix: the object of each upload, the objects a fold writes for inline pieces, and the extents of new chains. Upload session records are stored under the same prefix, and each one records the namespace that created it. A content reference contains a content ID, a size, and a checksum, and the reader's own manifest supplies the family root, so a reference identifies the same object in every member. A fork reads inherited content and its own content by one rule, and content is never copied between the members of a family. An import from another family copies the bytes under a new content ID in the destination's family, as every import between unrelated namespaces does.

Metadata stays with the namespace that wrote it. Manifests, WAL objects, segments, pins, and temporary objects are stored under each member's own prefix, a fork reads its inherited segments through its fork pin, and compaction output is written under the compacting namespace's prefix. The two rules follow one principle: data keyed by content identity is stored per family and collected by reachability, and data keyed by namespace state is stored per namespace and protected by pins.

### One creator per chain

A chain is appended to in place by one namespace, its "creator", which is the namespace whose commit started the chain. The layout row records the creator. When any other member of the family appends to the chain, LoonFS starts a new chain whose layout lists the first chain's extents as shared base extents, followed by the appended bytes. The new chain's creator is the appending namespace, so its later appends extend the new chain in place.

Why does the rule exist? Every member writes under the same prefix, so two members that appended to one chain from the same length would write a span object at the same key with different bytes, and their layout rows would describe one content ID as two different byte sequences. The longest layout rule, which picks the newest row among the longest layouts of a chain, assumes that every layout of a chain is a prefix of the longest one. One creator per chain keeps that true. The rule costs a content ID and a layout row for each other member that appends to a chain, and no bytes, because the new chain shares the first chain's extents.

For example, main creates `/build.log` and appends to it three times, which extends one chain under main's prefix. A sandbox forked from main appends to the file. The sandbox isn't the chain's creator, so its append starts a new chain whose layout lists main's extents and the appended bytes, under the same prefix. The sandbox's next append extends its own chain. Main's later appends extend main's chain, and neither namespace writes a key that the other could write.

### Collection

The family root's garbage collection pass collects the family's content and upload records, with roots gathered from every member. The members are found through fork pins: each fork pin under a member's prefix identifies the fork that reads through it, so the pass lists the root's pins, then each fork's pins, until no new member appears. A deleted member stays in the family until it's retired, because retirement deletes its pin.

The pass enumerates the members first, then lists the family's upload sessions, and then loads each member's roots: its current manifest, the manifests its pins keep, the manifests whose successor is younger than the grace, and its WAL tail. Those are the same roots a single namespace's pass loads today. The layout scan visits each distinct segment once, and a fork's inherited segments are its source's, so the extra work for each member is a scan of its own runs. One listing of the root's content prefix then finds the candidates.

A member whose manifest is missing when the pass reads it was retired after the enumeration, and the pass skips it. Every object that member wrote is referenced by a view that the pass reads after the enumeration, or by nothing, so the skip deletes nothing a member needs. A member whose manifest fails verification stops the family's content sweep, and other families are unaffected. A pass on a fork sweeps no content, because nothing is stored under its content prefix.

A fork's retirement releases its source pin and deletes nothing else. Content the fork wrote and no member references becomes unreferenced and is collected by the family pass under the ordinary age rule. A family root can't be retired while any fork of it exists, because each fork pins its source, so when the root is retired its content prefix is the whole family's content, and the retirement sweep deletes all of it.

## Costs

- The family pass loads one set of roots for each live member. A family with many short-lived forks pays one manifest discovery, one pin listing, and one WAL tail read for each of them on every pass, and a fork leaves the family once it's retired.
- Storage is attributed to the family by prefix. A listing of the root's content prefix measures the family's content, and a member's share isn't visible from object keys. Each member's upload admission limits are the quota inside a family.
- One member's garbage stays in the root's prefix until the next family pass, and a member whose manifest fails verification stops content reclamation for the whole family until the member is repaired or deleted.
- The shared base filter is sized from the sum of the members' current manifests' layout row counts, under the same byte cap as a single namespace's filter.

## Alternatives considered

- **Content stored under the namespace that writes it.** Each namespace could store its uploads and folded pieces under its own prefix, with a fork reading its source's objects through the fork pin. A reference would then identify an object in one namespace's prefix. Content that one member wrote could be used by another member only by copying the bytes under a new content ID, or by a pin from the reader onto the writer for as long as the reference exists, which puts a second kind of pin and a second direction of dependency into retirement.
- **Copying at retirement.** A member could reference another member's objects without a copy, under a pin in the reverse direction, and the bytes could be copied into the reader's prefix when the writer is retired, with the reader's layouts rewritten to point at the copies. That needs pins in both directions, a publication in one namespace triggered by another's retirement, and it still copies every shared byte eventually.
- **An owner namespace on every content reference.** Each reference could record the namespace whose prefix stores its bytes. Within a family that namespace is always the family root, which the manifest records, so the field would repeat one value in every reference and every extent. It would also make a reference into another family's prefix representable, and such a reference is never safe, because that family's collector knows nothing about the reader.
- **Any member extends a chain in place.** Two members that appended to one chain from the same length would write span objects at one key with different bytes, and a chain would stop being one sequence of bytes. Adding a unique component to span keys avoids the key collision but not the second problem, because the longest layout rule would then pick one member's bytes for a reference that the other member wrote.
- **Only the family root extends chains in place.** A fork would start a new chain on every append, and each new chain's layout would list every earlier extent as a shared base extent. Shared base extents are never merged and don't count toward the extent limit, so a file that a fork appended to a thousand times would have a thousand extents.
- **One content prefix for every namespace in a deployment.** Unrelated namespaces would share one collector and one content listing, and a namespace's retirement could delete nothing on its own. The family is the smallest set of namespaces that can share a reference, so it's the smallest collection domain that works.
