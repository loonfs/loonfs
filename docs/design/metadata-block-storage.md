# Metadata block storage

LoonFS stores filesystem metadata in immutable segments arranged as a log-structured merge tree. Each namespace manifest describes the complete set of segments needed to reconstruct its materialized state. Recent changes are read from the write-ahead log, or WAL, until a flush writes them into new segments.

The main storage tradeoff is between object size and read amplification. Larger objects reduce object counts and can be efficient to write, but a metadata lookup should not require downloading an entire object. LoonFS therefore divides each segment into independently readable sections.

This document explains that layout and the read path. The [storage format](../specs/format.md#a7-block-segment-encoding) defines the durable encodings and required behavior. [Streaming compaction](metadata-streaming-compaction.md) describes the merge implementation and its resource limits.

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

The descriptor also records a SHA-256 checksum of the complete segment, which the block cache uses as the segment's identity. Reads verify the CRC of each section they fetch. Section checksums detect corruption in the ranges read; they do not authenticate a range against the complete-object digest.

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

## Flushes and compaction

A flush and a compaction both publish manifests, but they perform different work. A flush materializes the visible WAL tail into a new run, which is a delta run except for the first flush over an empty manifest. The rows it encodes are proportional to the changes since the previous flush, but the complete manifest still describes the retained file set. A compaction merges complete runs for one family group; runs outside its window remain referenced without being rewritten.

Each published file set replaces the previous one through the next numbered manifest put-if-absent, and readers use the earlier manifest until that put succeeds. The [storage format](../specs/format.md#10-retention-and-compaction) defines where a merged run is placed and which rows it can remove. [Streaming compaction](metadata-streaming-compaction.md) describes window selection, resource bounds, and restart behavior.

## Constants and tunables

The encoding and the writer's target sizes have different compatibility requirements.

Bloom hashing and the block grammar are durable-format rules. The filter uses two fixed-seed 64-bit hashes, with seven probes and approximately ten bits per inserted key. A change that alters the interpretation of stored filters requires a new format version.

Block sizes, segment targets, and ordinary maintenance budgets are implementation settings:

| Setting | Default | Meaning |
| --- | --- | --- |
| Data-block target | 64 KiB decoded | A block is closed after its target is reached. |
| Segment byte target | 8 MiB decoded | A segment can exceed the target by its final row. |
| Segment row target | 65,536 rows | Segments are also split by row count. |
| Merge input limit | 8 complete runs | Both bounded and streaming merges limit their fan-in. |
| Bounded-step row budget | 131,072 rows | Larger eligible windows require streaming execution. |
| Bounded-step data budget | 64 MiB decoded SST data | Separate from stored-byte size-tiering decisions. |

Retuning a writer target is different from changing the stored encoding. Readers interpret the actual handles and lengths in each descriptor. These targets are not a substitute for decoder validation or reader-enforced resource limits.
