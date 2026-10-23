# Metadata block storage

LoonFS stores filesystem metadata in immutable segments, arranged as a log-structured merge tree. Each namespace manifest describes the complete set of segments needed to reconstruct its materialized state. Recent changes are read from the write-ahead log (WAL) until a fold writes them into new segments.

The main storage tradeoff is between object size and read amplification. Larger objects mean fewer objects, and they can be efficient to write, but a metadata lookup shouldn't have to download a whole object. So LoonFS divides each segment into sections that can be read independently.

This note explains that layout and the read path. [Block-segment encoding](../specs/format.md#a7-block-segment-encoding) in the storage format defines the durable encodings and the required behavior, and [Metadata streaming compaction](metadata-streaming-compaction.md) describes the merge implementation and its resource limits.

## Segment objects

A segment contains sorted rows from one metadata family. The object contains data blocks, a Bloom filter, and an index, in that order:

```text
+------------+------------+-----+------------+--------+-------+
| data block | data block | ... | data block | filter | index |
+------------+------------+-----+------------+--------+-------+
```

The target size of a data block is about 64 KiB before compression. Keys within a family often share long prefixes, so most entries store a shared-prefix length and the rest of the key, instead of another complete key. Entries at restart positions store the full key.

Each data block is compressed separately with zstd. The index block is also compressed, but the Bloom filter block is stored uncompressed. Each section has a CRC32C of its stored bytes. A reader can fetch a section by byte range and check its checksum before it decodes the section.

The segment has no header or footer that describes it. Instead, its descriptor in the manifest contains the handles of the index and filter blocks. Each handle records an offset, a stored length, a decoded length, and a checksum. The index entries then describe the locations and checksums of the data blocks.

```text
manifest descriptor
    |-- filter handle -> bloom-filter bytes
    `-- index handle  -> index entries -> data-block handles -> rows
```

The descriptor is where a read starts. A reader verifies the CRC of each section it fetches, and the section checksums detect corruption in the ranges that are read.

Why keep the handles in the manifest instead of in a footer? Keeping them in the manifest avoids a separate footer lookup, and a second copy of the segment's structural metadata. The cost is that a segment on its own isn't a complete description for recovery, because a reader needs the segment's manifest descriptor to interpret it.

## Reads

This section describes how a reader finds rows in a segment.

A point lookup first compares its key with the segment's minimum and maximum keys, which are in the manifest. If the segment's range doesn't overlap the lookup, no object read is needed. For a segment whose range overlaps the lookup, the reader:

1. Checks the Bloom filter.
2. Finds candidate blocks in the index.
3. Reads the data ranges it needs.

The Bloom filter is built from each row's lookup key, which depends on the family. The filter can show that no row with the requested lookup key exists in the segment, or that the reader has to inspect the data. A positive result from the filter doesn't mean that a row matches.

A small filter is also included in the manifest descriptor, as `filter_inline`, so a reader can rule out a small delta-run segment without another object read. This is especially useful for families keyed by parent, because several delta runs can have overlapping key ranges even when only one of them contains the requested name. The inline bytes are checked against the same length and CRC as the stored filter. They're a copy of the stored filter, not a separate filter with different contents.

The index contains the last row key and the block handle of each data block. A binary search over the index finds the blocks whose ranges might contain the requested rows. Adjacent blocks that are all needed can be fetched in one ranged GET.

For example, a directory lookup might overlap the descriptor ranges of several delta runs. The inline filters can rule out some of those segments without a fetch, and the indexes of the remaining segments identify the relevant blocks. So the read doesn't have to download every segment whose broad key range overlaps the directory.

A key can also have rows in several runs. For example, a file that's written once between each of seven folds has a revision in each of seven runs, and the filters of all seven runs admit the file's key. A lookup that wants the newest fact for a key (a file's latest revision, the newest version of a name's [directory binding](../specs/glossary.md#directory-binding-direntry), or an inode's newest attributes or access row) needs only the newest of those runs. Each run covers its own range of commits, and a compaction keeps its output where the runs it merged were, so a newer run's rows for a key are newer than every older run's rows for it. ([Compaction windows](../specs/format.md#102-compaction-windows) in the storage format defines that order.)

So after it checks the filters of every run, LoonFS reads the admitted runs newest first, four at a time, and stops at the first run that has a row for the key. A run whose filter admitted the key by a false positive has no rows for it, and the lookup goes on to the next run. The visibility rules then apply to those rows as they would to the rows of every run. In the example, a cold lookup of the file's latest revision sends four GETs instead of seven, and a warm lookup decodes the index and data blocks of one run instead of seven. (The window of four runs is an implementation default, not a format requirement.)

A lookup that needs every fact for a key, such as a file's revision listing or a tombstone check, still merges the rows of every run.

Why read four runs at once instead of one? If the newest admitted run turns out to be a false positive, the reads of the next runs have already been sent, so the false positive doesn't add a round trip. The cost is the reads of the other runs in the window when the newest run has the row, which it does unless its filter gave a false positive.

## Caching and combined reads

LoonFS caches decoded index, filter, and data blocks in memory, up to a byte budget. A server can also keep every block that the cache inserts on its local SSD, as a second tier of the same cache with a byte budget of its own. The SSD tier keeps each block in its stored form, which is the block's section exactly as the segment stores it. So LoonFS checks a block that it reads back from disk against the block's stored length and CRC32C, and decodes it, with the same code as a block read from the store. The cache affects how much a read costs, but not the metadata it returns.

The implementation also combines reads when transferring some extra bytes is cheaper than another object-store round trip:

- A read that misses a data block also fetches the blocks after it in the same ranged GET, up to 1 MiB of stored bytes, so a scan or a run of nearby lookups doesn't send a GET for each block. The read keeps those blocks as stored bytes and decodes each one when it reaches it, as [Reading ahead](block-cache.md#reading-ahead) describes.
- On a cold read, the filter and the index next to it can be fetched together.
- A small enough segment can be read once and decoded section by section.

These optimizations are limited to the read path. They don't change block boundaries, checksum coverage, or the visibility rules applied to the rows.

## Folds and compaction

A fold and a compaction both publish manifests, but they do different work. A fold materializes the visible WAL tail into a new run, which is a delta run, except for the first fold over an empty manifest. The number of rows a fold encodes is proportional to the changes since the previous fold, but the complete manifest still describes the whole retained file set. A compaction merges complete runs of one family group, and the runs outside its window stay referenced without being rewritten.

Each published file set replaces the previous one when the put-if-absent of the next numbered manifest succeeds, and readers keep using the earlier manifest until then. [Retention and compaction](../specs/format.md#10-retention-and-compaction) in the storage format defines where a merged run is placed and which rows it can remove. [Metadata streaming compaction](metadata-streaming-compaction.md) describes window selection, resource bounds, and restart behavior.

## Constants and tunables

The encoding and the writer's target sizes have different compatibility requirements.

Bloom hashing and the block grammar are rules of the durable format. The filter uses two 64-bit hashes with constant seeds, seven probes, and about ten bits per inserted key. Any change to how stored filters are interpreted requires a new format version.

Block sizes, segment targets, the read-ahead window, and ordinary maintenance budgets are implementation settings:

| Setting | Default | Meaning |
| --- | --- | --- |
| Data-block target | 64 KiB decoded | A block is closed once it reaches its target. |
| Read-ahead window | 1 MiB stored | A read that misses a data block fetches the blocks after it, up to this size, in the same GET. |
| Segment byte target | 8 MiB decoded | A segment can exceed the target by its final row. |
| Segment row target | 65,536 rows | Segments are also split by row count. |
| Merge input limit | 8 complete runs | Both bounded and streaming merges limit their fan-in. |
| Bounded-step row budget | 131,072 rows | Larger eligible windows need streaming execution. |
| Bounded-step data budget | 64 MiB decoded SST data | This budget is separate from the size-tiering decisions, which use stored bytes. |

Changing a writer target is different from changing the stored encoding. Readers interpret the actual handles and lengths in each descriptor. The targets don't replace decoder validation or the resource limits that readers enforce.
