# Commit history

The `commits` metadata family stores one row per retained logical commit, keyed by sequence. The change feed reads a range of these rows, and a retried commit reads the row at its receipt's sequence. The storage format defines the row, its key, its compaction group, and its retention ([Appendix A.6](../specs/format.md#a6-metadata-rows-and-row-keys), [section 10.3](../specs/format.md#103-row-retention-during-a-base-rebuild)). This note explains why the family exists and how reads use it.

## Why a commits family

Every other metadata family reshapes WAL deltas into one query order: bindings by parent and name, revisions by inode, receipts by commit ID. None of them is ordered by sequence, and none keeps the grouping of deltas into commits. Without the `commits` family, a request for the commit at sequence 42 has nowhere to look except the WAL, so every feed page and every replay costs work in proportion to the retained history rather than to the answer. Measured on that WAL-search path, one replay on a namespace with 1,038 retained WAL objects read 57.6 MiB of WAL to return one commit.

A commit row is the WAL commit record without its inline content, in the same encoding. The feed maps a row to events with the same mapping it uses for a WAL record.

The family shares a compaction group with `commit_receipts`, the way the two bind families share the bindings group. Both hold one row per commit, both are written by the same flush, and both are removed by the same rule at the same floor. A receipt therefore always has its commit row, and a replay always has its events.

## Writing

A commit is one conditional put of the next numbered WAL object, and no row is written before it. WAL replay adds one commit row per record to the projected tail, next to the rows that the record's deltas produce. A flush writes the tail's commit rows into its new run with the other families, and compaction merges them under the group's retention rule. The folded WAL objects then become collectable.

## Reading

A change-feed page reads through the same pinned view as every other read: the basis manifest, its segments, and the replayed tail. The durable side is one range scan over the `commits` family from the cursor. The tail side is the projected tail's commit rows above the cursor. Every manifest row is at or below the basis head and every tail row is above it, so the page is the durable rows followed by the tail rows, cut at the limit.

A retried commit finds its receipt by commit ID, reads the commit row at the receipt's sequence, and compares that row's fingerprint. The response is rebuilt from that row. Because the two rows share a run and a retention rule, a receipt whose commit row is missing is corruption, not a retired record.

A snapshot feed reads its pinned manifest's `commits` family and nothing later, so the page ends at the captured sequence without reading live history.

A page costs the blocks it returns, and a replay costs two point reads, on top of the pinned view that every read shares. Neither reads retained history. The only WAL that a cold view replays is the unfolded tail, and the flush trigger bounds it.

## Costs

- A commit row repeats the deltas that the other families already hold in other orders. An attribute or access delta carries a whole map, so those rows are the largest. Rows below the floor are removed at the next base rebuild.
- The projected tail holds commit rows in memory, within the existing tail budgets. The flush trigger bounds a tail.
- A flush writes one more family. A base rebuild of the commits group merges two families instead of one.
- The delta-to-event mapping must stay total over every retained row, for both metadata rows and WAL records.

## Alternatives considered

**Search the retained WAL.** Binary or exponential probing over WAL objects by their sequence ranges. Every probe fetches and decompresses a whole object to learn its range, and the reader must handle fences and batches on the way. It keeps the WAL as the history store.

**Index WAL locations in a metadata family.** A family mapping sequence ranges to WAL numbers finds the object without probing. Two things rule it out. A pinned manifest would then list rows that point at WAL objects that collection is allowed to delete, which would be the format's first reference to an object that need not exist. And every page would still fetch and decode a whole WAL object.

**Encode the WAL as block-indexed segments.** Each WAL object would carry a sequence index so a reader could fetch one block. It needs self-describing framing, since segments keep their handles in the manifest, and it does not locate the object. turbopuffer writes its WAL entries as indexed tables because its queries search the unindexed tail by key. LoonFS replays a bounded tail into memory and does not need to. WAL objects here average tens of kilobytes.

**Materialize events rather than deltas.** Storing the API event shape would tie the durable row to the wire format. Deltas are already the durable vocabulary, and one mapping serves both the row and the record.
