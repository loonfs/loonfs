# Commit history

The `commits` metadata family stores one row for each retained logical commit, keyed by sequence. The change feed reads a range of these rows, and a retried commit reads the row at its receipt's sequence. The storage format defines the row, its key, its compaction group, and its retention (see [Metadata rows and row keys](../specs/format.md#a6-metadata-rows-and-row-keys) and [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction)). This note explains why the family exists and how reads use it.

## Why a commits family

Every other metadata family reorganizes WAL deltas into one query order: bindings by parent and name, revisions by inode, and receipts by commit ID. None of them is ordered by sequence, and none of them keeps the deltas grouped into commits. Without the `commits` family, the only place to look for the commit at sequence 42 would be the WAL. Every feed page and every replay would then cost work in proportion to the retained history, instead of to the size of the result.

For example, in a measurement of that WAL-search path, one replay in a namespace with 1,038 retained WAL objects read 57.6 MiB of WAL to return one commit.

A commit row is the WAL commit record without its inline content, in the same encoding. The change feed maps a row to events the same way it maps a WAL record.

The family shares a compaction group with `commit_receipts`, the same way the two binding families share the bindings group. Both families contain one row per commit, the same fold writes both, and the same rule removes both at the same floor. So a receipt always has its commit row, and a replay always has its events.

## Writing

A commit is one conditional put of the next numbered WAL object, and no row is written before that put. WAL replay adds one commit row for each record to the projected tail, next to the rows that the record's deltas produce. A fold writes the tail's commit rows into its new run, along with the other families, and compaction merges them under the group's retention rule. The folded WAL objects can then be collected.

## Reading

This section describes how a change-feed page and a retried commit read commit rows.

A change-feed page reads through the same read view as every other read, which is the basis manifest, its segments, and the replayed tail. A page has two parts:

- The durable part is one range scan over the `commits` family, starting from the cursor.
- The tail part is the projected tail's commit rows above the cursor.

Every manifest row is at or below the basis head, and every tail row is above it, so the page is the durable rows followed by the tail rows, cut off at the limit.

To handle a retried commit, LoonFS:

1. Finds the receipt by commit ID.
2. Reads the commit row at the receipt's sequence.
3. Compares that row's fingerprint with the retried request's fingerprint.
4. Rebuilds the response from that row.

The two rows share a run and a retention rule, so a receipt whose commit row is missing is corruption, not a record that retention removed.

A snapshot's change feed reads its pinned manifest's `commits` family and nothing later, so the page ends at the captured sequence, without reading live history.

A page costs the blocks it returns, and a replay costs two point reads, in addition to the read view that every read shares. Neither one reads retained history. The only WAL that a cold view replays is the unfolded tail, and the fold trigger limits the size of that tail.

## Costs

The `commits` family has these costs:

- A commit row repeats the deltas that the other families already store in other orders. An attribute or access delta includes a whole map, so those rows are the largest. Rows below the floor are removed at the next base compaction.
- The projected tail keeps commit rows in memory, within the existing tail budgets. The fold trigger limits the size of a tail.
- A fold writes one more family. A base compaction of the commits group merges two families instead of one.
- The mapping from deltas to events must keep covering every retained row, for both metadata rows and WAL records.

## Alternatives considered

This section describes other designs for reading commit history, and why LoonFS doesn't use them.

- **Search the retained WAL.** A reader probes WAL objects by their sequence ranges, with a binary or exponential search. Every probe fetches and decompresses a whole object to learn its range, and the reader has to handle fences and batches along the way. This design keeps the WAL as the history store.
- **Index WAL locations in a metadata family.** A family that maps sequence ranges to WAL numbers finds the object without probing. Two problems rule it out. A pinned manifest would list rows that point to WAL objects that garbage collection is allowed to delete, and no other reference in the format points to an object that doesn't have to exist. And every page would still fetch and decode a whole WAL object.
- **Encode the WAL as block-indexed segments.** Each WAL object would include a sequence index, so a reader could fetch one block. This design needs self-describing framing, because segments keep their handles in the manifest, and it doesn't help a reader find the object. In turbopuffer, WAL entries are written as indexed tables, because its queries search the unindexed tail by key. LoonFS replays a bounded tail into memory, so it doesn't need an index. LoonFS WAL objects average tens of kilobytes.
- **Materialize events rather than deltas.** Storing the API's event shape would tie the durable row to the wire format. Deltas are already the durable vocabulary, and one mapping works for both the row and the WAL record.
