# Metadata block storage

LoonFS stores filesystem metadata in immutable segments arranged as a log-structured merge tree. Each namespace manifest describes the complete set of segments needed to reconstruct its materialized state. Recent changes are read from the write-ahead log, or WAL, until a flush writes them into new segments.

The main storage tradeoff is between object size and read amplification. Larger objects reduce object counts and can be efficient to write, but a metadata lookup should not require downloading an entire object. LoonFS therefore divides each segment into independently readable sections.

This document explains that layout, the read path, and the maintenance cycle. The [storage format](../specs/format.md) defines the durable encodings and required behavior. [Streaming compaction](metadata-streaming-compaction.md) describes the merge implementation and its resource limits in more detail.

## Segment objects

A segment contains sorted rows from one metadata family. The object contains data blocks, a bloom filter, and an index, in that order:

```text
+------------+------------+-----+------------+--------+-------+
| data block | data block | ... | data block | filter | index |
+------------+------------+-----+------------+--------+-------+
```

Data blocks target approximately 64 KiB before compression. Keys within a family often share long prefixes, so most entries store a shared-prefix length and the remaining key suffix rather than another complete key. Entries at restart positions store the full key.

Each data block is compressed independently with zstd. The index block is also compressed; the bloom-filter block is stored without compression. Each section has a CRC32C over its stored bytes. A reader can fetch a section by byte range and check its checksum before decoding it.

The segment has no self-describing header or footer. Its manifest descriptor contains the index and filter block handles. Each handle records an offset, stored length, decoded length, and checksum. Index entries then describe the locations and checksums of the data blocks.

```text
manifest descriptor
    |-- filter handle -> bloom-filter bytes
    `-- index handle  -> index entries -> data-block handles -> rows
```

The descriptor is the entry point for a read. Keeping these handles in the manifest avoids a separate footer lookup and another copy of the segment's structural metadata. The tradeoff is that an isolated segment is not a self-contained recovery description: its manifest descriptor is required to interpret it.

The descriptor also records a SHA-256 checksum of the complete segment for whole-object verification and related checks. Normal ranged reads use the section CRCs. Section checksums detect corruption in the ranges read. They do not authenticate individual ranges through the complete-object SHA-256 digest.

## Reads

A point lookup first compares its key with the segment's minimum and maximum keys in the manifest. A disjoint range requires no object read. For a segment whose range overlaps the lookup, the reader checks the bloom filter, locates candidate blocks in the index, and reads the necessary data ranges.

The bloom filter is constructed over each row's family-specific lookup key. It can establish that no row with that lookup key exists in the segment, or indicate that the reader must inspect the data. A positive filter result is not a matching row.

Small filters are also included in the manifest descriptor as `filter_inline`. This avoids an additional object read when excluding a small delta-run segment. It is particularly useful for parent-keyed families: several delta runs can have overlapping key ranges even when only one contains the requested name. The inline bytes are checked against the same length and CRC as the stored filter; they are not an independent filter with different contents.

The index contains the last row key and block handle for each data block. A binary search identifies the blocks whose ranges may contain the requested rows. Adjacent required blocks can be fetched in one ranged GET.

For example, a directory lookup may overlap the descriptor ranges of several delta runs. The inline filters can exclude some of those segments without a fetch. The remaining segment indexes identify the relevant blocks; the read need not download every segment whose broad key range overlapped the directory.

## Caching and combined reads

Decoded index, filter, and data blocks are cached within a byte budget. Cache contents affect read cost, not the metadata result.

The implementation also combines reads when transferring some additional bytes is cheaper than another object-store round trip. Scans and clustered lookups can prefetch nearby blocks. On a cold read, the filter and adjacent index can be fetched together. A sufficiently small segment can be read once and decoded section by section.

These are read-path optimizations. They do not change block boundaries, checksum coverage, or the visibility rules applied to the rows.

## Flushes and reorganization

A flush and a reorganization both publish manifests, but they perform different work.

A flush materializes the visible WAL tail into a new delta run. The new manifest retains the previous runs and adds the newly written segments. The metadata rows encoded by the flush are proportional to the changes since the previous flush. This does not make every part of the operation independent of namespace size: the complete manifest still describes the retained file set.

Reorganization merges complete runs for one family group. The bindings group includes the parent-and-name bindings, child-binding index, and unbind records because their retention decisions must remain consistent. Other groups, such as revisions, can be processed independently.

Each ordinary maintenance step limits its input by run count, decoded row count, and decoded data-block bytes. It selects a contiguous merge window. Runs outside that window remain referenced without being rewritten.

## When rows can be removed

A merge can remove obsolete rows only when its window starts at the oldest run in the family group. This is a bottom-anchored merge, and its output is a base run. The input then includes the older records required to apply the retention rules to that window.

A window above the oldest run produces a delta run and preserves every input row. The excluded older run may contain the other records needed for a deletion decision. Merging only the newer runs can reduce run count, but it cannot safely apply the same retention rules.

For example:

```text
Before:                       Merge the two newest runs:

  delta 3                       delta 4  <- rows from delta 2 and 3
  delta 2                       delta 1
  delta 1                       base
  base
```

This merge reduces the number of delta runs without rewriting the base. No rows are removed from the merged inputs. The resulting delta run is placed at the sequence of its newest input, not automatically at the manifest's current head sequence.

A bottom-anchored output is instead stamped at the manifest head sequence and ordered as the group's base. A group has at most one base run. The storage format defines the per-family retention rules; file revision rows are not removed by advancing the replay floor.

## Larger family groups

When an eligible merge window exceeds an ordinary step's row or byte budget, streaming compaction processes it using the same merge engine. It writes output directly under the namespace’s segment prefix and publishes the completed file set in the next numbered manifest. The compactor epoch, publication time bound, and segment minimum age protect this process under the collection rules.

Both execution paths select at most eight input runs. A large backlog can therefore require several publications. The size-tiered policy can also defer a rewrite when too little newer data has accumulated relative to the oldest selected run. A maintenance budget does not guarantee that every invocation reduces the run count or removes all delta runs.

The [streaming-compaction design](metadata-streaming-compaction.md) specifies window eligibility, placement, resource bounds, and explicit compaction behavior.

## Publication and restart behavior

Each completed merge is published through the next numbered manifest put-if-absent. Until that conditional update succeeds, readers use the earlier manifest. A partially written output set is never a published file set.

After a bounded merge is interrupted, the next invocation plans from the current manifest. There is no separate row-level continuation record for that merge. A background compaction also restarts from the current manifest after process failure rather than resuming partial output segments.

Concurrent publications are reconciled at finalization. A compaction removes only its selected inputs, after confirming that their descriptors are unchanged, and preserves newer or unrelated runs.

The merge validates the parent-and-name binding rows and child-index rows selected for output. It also rejects duplicate logical input keys, including duplicates that would otherwise be removed by retention. Matching index digests alone are insufficient to detect the same duplicate in both families.

## Constants and tunables

The encoding and the writer's target sizes have different compatibility requirements.

Bloom hashing and the block grammar are durable-format rules. The filter uses two fixed-seed 64-bit hashes, with seven probes and approximately ten bits per inserted key. A change that alters interpretation of stored filters requires the appropriate format-version change after release.

Block sizes, segment targets, and ordinary maintenance budgets are implementation settings:

| Setting | Current default | Meaning |
| --- | --- | --- |
| Data-block target | 64 KiB decoded | A block is closed after its target is reached. |
| Segment byte target | 8 MiB decoded | A segment can exceed the target by its final row. |
| Segment row target | 65,536 rows | Segments are also split by row count. |
| Merge input limit | 8 complete runs | Both bounded and streaming merges limit their fan-in. |
| Bounded-step row budget | 131,072 rows | Larger eligible windows require streaming execution. |
| Bounded-step data budget | 64 MiB decoded SST data | Separate from stored-byte size-tiering decisions. |

Retuning a writer target is different from changing the stored encoding. Readers interpret the actual handles and lengths in each descriptor. These targets are not a substitute for decoder validation or reader-enforced resource limits.
