# Commit history

**Status: implemented.**

The `commits` metadata family stores one row per logical commit, keyed by sequence. The change feed reads a range of these rows. A retry finds the receipt by commit ID and reads the commit row at its sequence. The WAL supplies the unfolded tail and is collectible after folding.

Every other metadata family re-shapes WAL deltas into one query order: bindings by parent and name, revisions by inode, receipts by commit ID. None of them is ordered by sequence, and none keeps the grouping of deltas into commits. Without the `commits` family, a request for the commit at sequence 42 has nowhere to look except the WAL, which makes every feed page and every replay cost proportional to the retained history rather than to the answer. On a namespace with 1,038 retained WAL objects, one such replay read 57.6 MiB of WAL to return one commit.

## The commits family

A commit row is the WAL commit record of format specification Appendix A.5 with its inline content left out: `seq`, `commit_id`, `committed_by`, `semantic_commit_fingerprint`, `committed_at_ms`, optional `message`, and `deltas`. The row uses the same encoding as the WAL record, so the feed's delta-to-event mapping reads a row with the same mapping used for a WAL record. A row that carries inline content is invalid.

| | Value |
| --- | --- |
| Family | `commits` |
| Row key | `commit-{seq:020}` |
| Filter key | The complete row key |
| Group | `commits`, shared with `commit_receipts` |
| Retention | Remove rows strictly below the retention floor |

Row keys ascend with sequence, so a range scan from the key after `commit-{after_seq}` reads the next commits in feed order, and a point read at `commit-{seq}` finds one commit. Each delta run covers a disjoint sequence interval, so range planning selects one run per lookup from the descriptors' key ranges.

The family shares a group with `commit_receipts`, the way the two bind families share the bindings group. Both hold one row per commit, both are written by the same fold, and both are removed by the same rule at the same floor. Manifest validation checks that every run holds equal counts of commit rows and receipt rows. A receipt therefore always has its commit row, and a replay always has its events.

## Writing

A commit is one conditional put of the next numbered WAL object, and no row is written before it is acknowledged.

WAL replay pushes one commit row per record into the projected tail, next to the rows its deltas produce. The row carries the record's deltas and none of its inline bytes. A fold writes the tail's commit rows into the new delta run with the other families, and compaction merges them under the group's retention rule.

## Reading

**The change feed.** A page is read through the same pinned view every other read uses: the basis manifest, its segments, and the replayed tail. The durable side is one range scan over the `commits` family from the cursor. The tail side is the projected tail's commit rows above the cursor. Every manifest row is at or below the basis head and every tail row is above it, so the page is the durable rows followed by the tail rows, cut at the limit. The page obeys the `through_seq` bound and returns `rebootstrap_required` below the retention floor.

**A replayed commit.** Admission finds the receipt by commit ID, compares the fingerprint, and reads the commit row at the receipt's sequence. The response is rebuilt from that row. Because the two rows share a run and a retention rule, a receipt whose commit row is missing is corruption, not a retired record.

**A snapshot feed.** A snapshot reads through its pinned manifest. Its feed reads that manifest's `commits` family and nothing later, so the page ends at the captured sequence without reading the live history.

A page costs the blocks it returns, and a replay costs two point reads, on top of the pinned view that every read shares. Neither reads retained history. The only WAL a cold view replays is the unfolded tail, and the fold trigger bounds it.

## WAL retention

Readers need only WAL objects above `folded_wal_no`. Collection deletes older WAL objects once they meet the age rule. `retention_floor_seq` governs rows: incremental replay, receipts, superseded bindings, old attribute and access states, and commits. Advancing it is explicit.

Inline bytes remain in the WAL until the fold writes their content objects. The folded WAL objects then become collectible.

## Costs

- A commit row repeats the deltas that the other families already hold in other orders. An attribute or access delta carries a whole map, so those rows are the largest. Rows below the floor are removed at the next base rebuild.
- The projected tail holds commit rows in memory, within the existing tail budgets. A tail is bounded by the fold trigger.
- A fold writes one more family. A base rebuild of the commits group merges two families instead of one.
- The delta-to-event mapping must stay total over every retained row, for both metadata rows and WAL records.

## Alternatives considered

**Search the retained WAL.** Binary or exponential probing over WAL objects by their sequence ranges. Every probe fetches and decompresses a whole object to learn its range, and the reader must handle fences and batches on the way. It keeps the WAL as the history store.

**Index WAL locations in a metadata family.** A family mapping sequence ranges to WAL numbers finds the object without probing. Two things rule it out. A pinned manifest would then list rows that point at WAL objects collection is allowed to delete, which would be the format's first reference to an object that need not exist. And every page would still fetch and decode a whole WAL object.

**Encode the WAL as block-indexed segments.** Each WAL object would carry a sequence index so a reader could fetch one block. It needs self-describing framing, since segments keep their handles in the manifest, and it does not locate the object. turbopuffer writes its WAL entries as indexed tables because queries search the unindexed tail by key; LoonFS replays a bounded tail into memory and does not need to. WAL objects here average tens of kilobytes. A separately ranged inline payload section remains the natural change if inline bytes make them large.

**Materialize events rather than deltas.** Storing the API event shape would tie the durable row to the wire format. Deltas are already the durable vocabulary, and one mapping serves both the row and the record.

## Verification

- A feed page over a folded and compacted history reads the same number of metadata blocks whether the namespace holds eight commits or sixty-four, and reads no WAL object.
- A replay after a fold and a base rebuild returns the original commit with its events and reads no WAL object.
- A page that crosses the fold boundary returns the folded commits and then the tail commits in one ascending sequence, with the cursor contract unchanged.
- A manifest whose run has unequal commit and receipt row counts does not load.
- A commit row carrying inline content does not decode.
- A base rebuild removes commit rows and receipts below the floor together and keeps both above it.
- Collection deletes a folded WAL object after grace and keeps an unfolded one, whatever the sequence floor.
