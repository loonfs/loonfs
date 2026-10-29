# Block cache

This note describes how a LoonFS server caches the blocks of [metadata segments](../specs/glossary.md#metadata-segment). Every block that a read loads goes through one cache with one interface. By default the cache keeps blocks in memory, and a server can also keep every block in the cache on its local SSD, as a second tier of the same cache.

## The cost of a miss

This section describes what a read pays for a block that isn't cached, and why that cost grows on a server that serves many namespaces.

A LoonFS server keeps no durable state of its own. Every [metadata segment](../specs/glossary.md#metadata-segment) is an immutable object in the bucket, and the server reads a segment's data, index, and filter blocks with ranged GETs, as [Metadata block storage](metadata-block-storage.md) describes. The server keeps decoded blocks in memory under one byte budget, `metadata_cache.max_segment_bytes`. The budget is 256 MiB on a server without a memory limit, and 30% of managed memory on a sized server (40% when the server doesn't serve grep). A block that isn't in memory costs a GET, and a GET from a server next to its bucket takes tens of milliseconds.

Three cases make those misses most of what a read costs:

- A server that serves many [namespaces](../specs/glossary.md#namespace) has more working sets than its budget can keep, so reads of one namespace evict the blocks of others.
- A server that restarts starts with an empty cache, so the first reads of every namespace miss.
- When a namespace's metadata is larger than the budget, reading the namespace evicts its own blocks. A pass over its metadata misses on every page, because a least-recently-used cache evicts each block of a pass that's larger than the cache before the pass comes back to that block.

When one server serves many tenants' namespaces, these misses set the latency of its reads and its cost per tenant, because every miss is a request that the bucket's provider bills.

## The cache interface

This section describes the one interface that every block read goes through, and the two implementations of that interface.

If a server kept blocks in two caches, each with rules of its own, one cache could keep a block that the other evicted, and an operator would have two budgets, two families of counters, and two eviction policies to size and read. So LoonFS keeps every block that a metadata read loads (data blocks, index blocks, and filter blocks) in one "block cache", and no cache under the block cache keeps blocks by rules of its own.

The block cache has three operations:

- Get a block by its key and its handle.
- Insert a block with its decoded form and its stored bytes.
- Get or load a block. If the block is missing, the cache runs one load for all the concurrent callers of its key, and inserts the loaded block with its stored bytes.

A block's key is its object store, its segment's object key (which includes the owner namespace ID and the generated segment ID, as [Object keys](../specs/format.md#a8-object-keys) shows), the block's kind, and the block's offset in the segment. [Keeping blocks on local SSD](#keeping-blocks-on-local-ssd) describes how each tier identifies the object store. A block's handle records the block's offset, its stored and decoded lengths, and a CRC32C of its stored bytes (see [Block-segment encoding](../specs/format.md#a7-block-segment-encoding)). A caller always has the handle before it asks for a block, because the manifest's segment descriptor includes the handles of the filter and index blocks, and the index block includes the handles of the data blocks. The stored bytes of an insert are the block's bytes as the segment stores them, and the memory implementation ignores the stored bytes.

For example, a lookup of one name in a directory compares the name with each segment's key range in the manifest, checks the filter of each segment whose range covers the name, reads the index block of each segment that its filter doesn't exclude, and reads the data block that the index points to. A read keeps the blocks it has used in a memo of its own (see [Reading ahead](#reading-ahead)) and looks there before the block cache, so a second use of a block within one read isn't a get. The lookup's first use of each filter, index, and data block is a get or load from the block cache, and a miss costs a GET from the store.

The interface has two implementations:

- The memory implementation keeps decoded blocks in memory under `metadata_cache.max_segment_bytes`. It's the cache engine (`DecodedBlockCache`, in [`block_cache.rs`](../../crates/loonfs-core/src/block_cache.rs)) that every LoonFS cache is built from, as [One cache type](cache-ownership.md#one-cache-type) describes, and the segment block cache (`SharedSegmentBlocks`, in [`segment/cache.rs`](../../crates/loonfs-core/src/segment/cache.rs)) is one instance of that engine. A server uses this implementation by default.
- The tiered implementation keeps blocks in memory and on the server's local SSD, as one cache with one rule for which blocks go to disk. [Keeping blocks on local SSD](#keeping-blocks-on-local-ssd) describes this implementation.

Why does a get include the handle? A block read back from disk has to be checked before a read uses the block, and the handle records what the bytes have to match. A block in memory doesn't need the check again, because LoonFS checked it when the block was loaded.

Why does the cache load a missing block itself? Concurrent callers of one key would otherwise each send a GET for the same block, so the cache runs one load for all of them. That load has to run inside the cache, because an eviction of the key while the load runs determines whether the loaded block is inserted at all. With that check in one place, the cache's counters record what happened to each block.

Why is there no cache under the block cache? A second cache under the block cache, with its own budget and rules, could keep blocks that the block cache evicted and skip blocks that reads use, so the two caches would keep different sets of blocks under different rules. With one cache, one admission rule and the least-recently-used eviction of each tier determine which blocks the server keeps, and each tier reports one family of counters.

## Keeping blocks on local SSD

This section describes the tiered implementation, which extends the block cache from memory onto the server's local SSD.

The host sets how much memory a server has, and memory is empty after a restart. A local SSD is larger, costs about an order of magnitude less per byte, and keeps its contents while the server is stopped. But a block on disk has to be read back, checked, and decoded before a read can use the block.

The tiered implementation has two parts. The "memory tier" keeps decoded blocks under the memory budget, as the memory implementation does, and the "SSD tier" keeps blocks in a directory on the local SSD under a byte budget of its own. The two tiers form one cache with one admission rule: every block that's inserted goes into both tiers. To get a block, the tiered implementation:

1. If the block is in memory, marks it as the most recently used block on disk and returns the block.
2. If the block is on disk, checks the length of the block's file against the stored length that the SSD tier recorded for the block, before it reads any of the file's bytes. Then it reads the block, checks its length and checksum against the caller's handle, decodes it, marks it as the most recently used block on disk, inserts the decoded block into memory, and returns the block.
3. If the block is in neither tier, returns a miss.

After a miss, a get or load reads the block from the store and inserts it. An insert puts the decoded block into memory and queues the stored bytes for the SSD tier's writer, which writes them to disk, so a read doesn't wait for a disk write. An insert of a block that the disk already keeps writes nothing, and it refreshes the block's place on disk. LoonFS inserts a block in three cases:

- After it loads the block from the store, with the stored bytes from the GET.
- After a [fold](../specs/glossary.md#fold) or a [compaction](../specs/glossary.md#compaction) writes a segment, with the bytes that it just wrote. A fold inserts every block of the segment. A compaction inserts the segment's index and filter blocks, and it inserts the segment's data blocks only when the segment's stored size is at most 256 KiB.
- When a manifest's segment descriptor includes an inline copy of the filter (`filter_inline`), with the copy's decoded bytes. The filter is stored uncompressed, so those bytes match the stored filter, with the same length and checksum.

Each tier evicts its least recently used block when it's over its budget. A hit in either tier refreshes the block's place on disk, so the disk evicts blocks that neither tier has used recently. A block that leaves memory needs no write, because it went to disk when it was inserted.

The SSD tier keeps each block in its stored form, the compressed bytes of the block's section exactly as the segment stores them, so the disk keeps the same bytes as the store. A block read from disk is therefore checked and decoded by the same code as a block read from the store.

The SSD tier keeps blocks from every [namespace](../specs/glossary.md#namespace) that the server serves, and it evicts the least recently used block across all of them. For example, if a server serves 500 namespaces and 20 of them are busy, the disk fills with the blocks of those 20, and the blocks of an idle namespace leave the disk as the busy namespaces read.

The SSD tier keeps blocks and no other entries. Decoded manifests stay in memory alone, under a budget of their own (see [Budgets and counters](#budgets-and-counters)), because a manifest is small and a read loads its namespace's manifest once per revalidation.

In memory, the cache identifies a block's object store by the store's scope, a number that the process assigns to each object store that it opens. On disk, the SSD tier identifies the object store by the store's identity, which is its provider, its location, and its key prefix. The location is the bucket, with the endpoint when the provider has one, or a local store's absolute root directory. A disk key is a hash of that identity, the segment's object key, the block's kind, and the block's offset. A store without an identity, such as a store in memory, has no objects outside the process, so the cache keeps that store's blocks in memory alone.

The blocks on disk stay readable across a restart. When the server shuts down, it closes the SSD tier after reads have stopped, and closing waits for the writer to write every block that's still queued, so the blocks that reads loaded last are on disk for the next start. (If the process is killed before it closes the tier, the queued blocks aren't written, and the next read of each one goes to the store.) When the server starts, the SSD tier rebuilds its index from the names of the files in its directory. Each file's name records the block's disk key and stored length, so the scan doesn't read any block's bytes, and a block that was on disk when the server stopped is a disk hit after the restart. A file name doesn't record when its block was last used, so after a restart the SSD tier's recency order is the order in which the scan found the files, not the order before the server stopped.

The tiered implementation and the SSD tier are part of the `loonfs` runtime crate, not `loonfs-core`, because the SSD tier runs a writer task and has to be closed when the server shuts down, and `loonfs-core` doesn't spawn tasks or manage their lifecycle. The SSD tier's file reads, writes, and deletes run on the runtime's blocking pool, so waiting for the disk doesn't block one of the runtime's worker threads.

Why does the SSD tier keep stored bytes and not decoded blocks? The handle's checksum covers the stored bytes, so a block read from disk passes the same check as a block read from the store. The stored form is also smaller, because data and index blocks are compressed.

Why does the disk key use the store's identity and not its scope number? A process assigns scope numbers in the order that it opens object stores, so after a restart the same store can have a different number, and another store can have the number that the first store had. A disk key with the number would miss the server's blocks after a restart, or find another store's blocks. The store's identity is the same in every process, so a restarted server finds its blocks, and a server that serves two stores keeps their blocks apart.

Why does the SSD tier rebuild its index from file names, and not store the index in a file? A separate index file would need writes of its own, and after a crash it could list blocks that aren't on disk, or miss blocks that are. An index rebuilt from the file names lists exactly the blocks in the directory.

Why does a fold insert every block of the segments it writes? The writer that just folded is the process most likely to read the folded rows next. For example, after a fold of a batch of new files, the writer's next stats and reads of those files look up the rows that the fold just wrote. With every block of the fold's segments in memory (and on disk, when the SSD tier is on), those lookups hit the cache instead of the store. The blocks are ordinary entries, charged to `metadata_cache.max_segment_bytes` and evicted like any other block. A fold of 4,500 new files inserts about 13 MiB of decoded blocks, which is about 5% of the default 256 MiB budget.

Why doesn't a compaction insert every block too? A compaction rewrites rows that are already in segments, and no read asked for those rows. So a compaction inserts only the blocks that the first read of a new segment would load: the index and filter blocks, which every lookup in the segment reads, and the data blocks of a segment small enough that a read fetches the whole segment in one GET (at most 256 KiB stored).

Why write a block to disk when it's inserted, and not when it leaves memory? When a block is inserted, the caller has its stored bytes, which the handle's checksum covers. Writing them then keeps the memory tier to decoded blocks, as in the memory implementation, and keeps the disk to exactly the bytes that the store has. A write when a block leaves memory would need either the block's stored bytes kept in memory beside its decoded form, or a second encoding of the decoded block on disk with a checksum of its own.

Why does a memory hit refresh the block's place on disk? Without the refresh, the disk could evict a block that memory still keeps, and the first read after memory evicts that block would cost a GET that the disk was meant to save.

## Reading ahead

This section describes where LoonFS keeps the extra blocks that a read fetches ahead of what it needs.

When a listing or a lookup misses on a data block (neither the read's memo nor the block cache has the block), LoonFS reads ahead. In one ranged GET, it fetches the missed block and the blocks after it in the segment, as long as their stored bytes stay within 1 MiB. These blocks are the read's "read-ahead window". For example, in a segment whose blocks are stored in about 4 KiB each, a read that misses block 40 fetches a window of blocks 40 through about 295. In a segment whose blocks are stored in about 64 KiB each, the window has 16 blocks, and a block larger than 1 MiB is fetched alone. A window stops at the segment's last block. (Some lookups that match one row, such as a lookup of one name in a directory, don't read ahead.)

The read's memo records the window's other blocks as stored bytes, without decoding them, whether the read is a scan or a lookup. When the read reaches one of those blocks and the block cache doesn't have it, LoonFS decodes the block from its stored bytes and inserts it into the block cache (into both tiers, when the SSD tier is on). A block that the read doesn't reach isn't decoded, so a damaged block in a window fails only a read that reaches it. For example, a listing's scan of a directory's entries decodes the blocks that contain the page's entries, and a lookup of a file's latest revision decodes the one block that contains the revision.

A read's memo, which the read looks in before the block cache, is charged to the server's read working memory (`max_read_working_bytes`), and the charge includes the stored bytes of each window. If the read working memory doesn't have room for a block that the memo records, the memo evicts its own oldest blocks to make room, so a read doesn't fail when the read working memory is full. A window is kept only in the memo, so it also stops at the read working memory that's free when the read fetches it. For example, if 200 KiB is free, a read fetches a window of at most 200 KiB, and if none is free, it fetches the missed block alone. LoonFS drops the memo when the read ends. For example, a page of a listing looks up the latest revision of each file it lists, and those lookups share the page's memo. The next page is a separate read, so its scan and its lookups fetch their windows again, even if the previous page fetched the same windows. (A scan that visits one range for a caller, such as garbage collection's scan of content layouts, stops each window at the end of its range, and drops the decoded blocks from its memo before it hands their rows to the caller, so the caller's own sets can use the rest of the read working memory.) Because the SSD tier keeps blocks as stored bytes, a later change could let it keep a window's other blocks without decoding them, but LoonFS doesn't do this yet.

Why is a window 1 MiB? The tail latency of a GET from an object store is nearly flat from tens of kilobytes to about 1 MiB, and the store charges the same for each GET. So a window of 1 MiB costs about as much as a GET of the missed block alone, and each later block of the window that the read uses needs no GET of its own. (1 MiB is an implementation default, `READAHEAD_WINDOW_BYTES`. A runtime can set another size on its metadata cache, up to the 4 MiB that one ranged GET of blocks reads.)

Why does a window start at the block that the read missed, and not at a fixed boundary in the segment? LoonFS reads ahead only when a read misses a block. A scan asks for one block at a time, so after it reads a window, its next blocks are in the memo, and its next window starts at the first block that it misses. If LoonFS read ahead from every block that a scan asked for, each window would end one block after the window before it, and LoonFS would fetch that one block in a GET of its own.

Why doesn't LoonFS decode a window when it fetches the window, and insert all of its blocks into the block cache? A read reaches the blocks of its key range one at a time, and its key range can end before the window does, so decoding the whole window would decode blocks that the read never reaches. Decoded blocks are also much larger than stored ones. For example, a window of 1 MiB of blocks stored in about 4 KiB each and decoded to 64 KiB each is about 16 MiB decoded, and a scan with 16 segments open would hold 16 such windows at once. And if the block cache couldn't keep a decoded window's blocks until the read reached them, LoonFS would fetch and decode the rest of the window again at each later block, so the read would decode most of a window for each block it reads. In a test where the block cache keeps four blocks, a lookup that reads 97 blocks decodes each of them once, because its windows are kept as stored bytes. In a listing of a million files, lookups that decoded each window when they fetched it spent an hour decoding. The cost of keeping a window in the memo is that a later read, such as the next page of a listing, can't find the window's other blocks in the block cache and fetches them again.

## Budgets and counters

This section describes the budget of each tier, the memory that the SSD tier adds to the server's sizing, and the counters that each tier reports.

LoonFS keeps each kind of cache entry under a budget of its own, so that one kind can't evict another, and every cache keeps the same family of counters (hits, misses, evictions, and rejections), as [Cache ownership](cache-ownership.md) describes. The memory tier is the segment block cache, with its own counters and its own budget, the segment budget (`metadata_cache.max_segment_bytes`). The segment budget is separate from the budgets for [manifests](../specs/glossary.md#manifest), namespace [heads](../specs/glossary.md#head), [WAL tails](../specs/glossary.md#wal-tail), and content objects, so a burst of block reads doesn't evict a namespace's manifest or a reader's WAL tail.

LoonFS keeps decoded manifests in the manifest cache, under the manifest budget (`metadata_cache.max_manifest_bytes`), which is 32 MiB with or without a memory limit. A read of a namespace starts from that namespace's manifest, so a manifest that isn't in memory costs a GET before the read loads any block. A manifest is charged about 10 to 60 KB for a namespace of 10,000 files (more when the namespace's writes leave more [runs](../specs/glossary.md#run)), and about 240 KB for a namespace of 1,000,000 files written in large commits. So the default budget keeps the manifests of about 600 to 3,000 namespaces of 10,000 files, or about 140 namespaces of 1,000,000 files.

For example, in one measurement a server read 64 namespaces of 10,000 files in rotation. With manifests under the segment budget, the server sent a GET to load a namespace's manifest again each time it came back to that namespace, at every segment budget from 64 MiB to 160 MiB. With the manifest budget, the server sent none of those GETs. The stats that had waited for those GETs were among the slowest 2%, so the 99th percentile latency of a stat fell by about 25 ms.

The SSD tier has two settings, in a `[metadata_cache.ssd]` table under the server's `[metadata_cache]` table ([`config.rs`](../../crates/loonfs-server/src/config.rs)). Both settings are required, and without the table, the server has no SSD tier.

- `directory` is the directory on the local SSD that the SSD tier writes its blocks to. The server creates it if it's missing.
- `max_segment_bytes` is the budget for blocks on disk, shared by every namespace that the server serves. The SSD tier charges each block its stored length, or 4 KiB for a smaller block, so the budget limits the number of blocks on disk as well as their bytes.

Each read or write of a block on disk opens and closes the block's file, so the SSD tier keeps no files open between reads, and the server's sizing has no rule for open files.

The SSD tier also keeps its index and its write queue in memory. The index records the hashed disk key and the stored length of each block on disk, and the write queue keeps the stored bytes of the blocks that are waiting for the writer, up to 64 MiB. If the queue is full when a block is inserted, the SSD tier doesn't write the block, and the block stays in memory. The [sizing formula](../../crates/loonfs-server/docs/self-hosting.md#set-one-number) includes both in `fixed` (the part of the formula that covers memory outside the pools), so the pools are sized from what's left after that memory. The SSD tier's term is its 64 MiB write queue plus its index of the blocks on disk, at 168 bytes for each block (the index's measured cost per entry):

```text
64 MiB + metadata_cache.ssd.max_segment_bytes / 4 KiB x 168 B
```

Because the SSD tier charges each block at least 4 KiB, it keeps at most one block for each 4 KiB of its budget, so the term is the most memory that the write queue and the index use. For a 10 GiB tier, the term is about 484 MiB, and for a 100 GiB tier, about 4.2 GiB. (For comparison, the mean stored block in a compacted namespace of 20,000 files was 4.1 KiB.)

Why does the SSD tier have one budget for every namespace, and not one per namespace? Tenants' working sets differ by orders of magnitude, and they change over the day. For example, one tenant might read 2 GB of metadata all morning and nothing in the evening, while another reads 5 MB once an hour. A shared least-recently-used budget follows those working sets as they change, and a budget per namespace would need an operator to set and adjust a number for each tenant.

Why does the SSD tier have one budget when memory has one for each kind of entry? The SSD tier extends one cache, the segment block cache, so it keeps one kind of entry. Manifests, namespace heads, WAL tails, and content objects stay in memory under their own budgets.

Why isn't the manifest budget a share of managed memory? The memory that manifests need depends on the namespaces that a server reads and on their sizes, not on the host, so the budget is a setting of its own, like the head budget. The sizing formula subtracts the manifest budget from the memory limit, along with `fixed`, the headroom, and the head budget, and divides what's left (managed memory) among the pools.

Why is the default 32 MiB? A 32 MiB budget keeps the manifests of hundreds of namespaces, and a larger default would shrink every pool on a sized server. For example, with a 64 MiB default, a server with a 2 GiB memory limit would have less than the 256 MiB of managed memory that the sizing check requires.

Why does the SSD tier charge each block at least 4 KiB? The index keeps an entry in memory for each block on disk, and its term in `fixed` covers one entry for each 4 KiB of the budget. If the SSD tier charged each block only its stored length, a tier of small blocks could keep many more entries than the term covers (a tier of 1 KiB blocks would keep more than three times as many), and the server could use more memory than its limit. The 4 KiB minimum costs disk instead, because a tier of blocks smaller than 4 KiB keeps fewer bytes on disk than its budget.

Why does the SSD tier write blocks from a queue, and skip the write when the queue is full? Without the queue, a read that inserts a block would wait for a disk write before it returns. Without the cap, a disk that's slower than the reads would grow the queue past its term in `fixed`. A block that the SSD tier doesn't write stays in memory, and if memory evicts it, the next read of the block costs a GET, as it would on a server without the tier.

## Stale and corrupted blocks

This section describes why a cached block is never out of date, and what LoonFS does with a block on disk that's damaged.

A cache of data that changes needs invalidation, and a cache on disk can return bytes that differ from the bytes that were written, for example after a crash during a write or on a failing disk.

A metadata segment is immutable, and its key includes a generated segment ID, so the bytes under a segment's key don't change (see [Numbered objects and generated IDs](../specs/format.md#23-numbered-objects-and-generated-ids)). A cached block is a copy of bytes that the next GET would return unchanged, so the block cache needs no invalidation. A [fold](../specs/glossary.md#fold) or a [compaction](../specs/glossary.md#compaction) writes new segments under new IDs, and the blocks of the segments that a compaction merged leave the cache when the eviction policy removes them. Blocks of a segment that [garbage collection](../specs/glossary.md#garbage-collection-gc) deletes leave the cache in the same way.

LoonFS checks a block's stored length and CRC32C when it reads the block from the store, and the SSD tier checks them again, against the caller's handle, when it reads the block from disk. If a block on disk fails the check, or the disk read fails, the SSD tier deletes the entry and returns a miss, and the caller fetches the block from the store again.

The SSD tier's directory contains nothing that the server depends on, so an operator can delete its contents at any time, and the reads that follow miss and go to the store. A server without an SSD tier keeps blocks in memory alone, and every block that isn't in memory costs a GET.

Why check a block from disk if it passed the check when it was loaded from the store? The disk can return different bytes than the SSD tier wrote, and a damaged block could still decode into rows, so without the check a read could return wrong metadata. The check costs a CRC32C over the block's stored bytes, which the decode reads anyway.

## Running the SSD tier

This section describes what an operator configures and sees, and what a client sees.

An operator chooses whether a server keeps blocks on local disk, and how much disk to use, and needs to see whether the disk saves requests. The SSD tier is optional and off by default. With it off, the server uses the memory implementation. With it on, `loonfs-server --check-config` creates the SSD tier's directory if it's missing, and checks that the server can write a file in it, so a directory that the server can't use is reported before the server starts. On a server with the tier, the first read of a block that the server hasn't inserted costs a GET, as it does without the tier, and later reads of that block come from memory or from disk instead of the store, including reads after a restart. A tenant whose blocks aren't cached pays for its first pass over its metadata, and its later passes read from memory or disk, even when other tenants' reads have pushed its blocks out of memory.

A client gets the same results with or without the tier, because the tier changes what a read costs and not what it returns. A read that misses memory and finds its block on disk waits for a read from local disk instead of a GET.

The block cache's metrics keep one cache name, `segment`, in the grammar of the other cache metrics (`loonfs_<cache>_cache_*`), and they report each tier under a `tier` label with the value `memory` or `ssd`. The object cache's metrics use a `key_class` label in the same way (see [Probes and metrics](../../crates/loonfs-server/docs/self-hosting.md#probes-and-metrics)). For example, `loonfs_segment_cache_gets_total{tier="ssd",result="hit"}` records the gets that the SSD tier served from disk, and `loonfs_segment_cache_gets_total{tier="ssd",result="miss"}` records the gets that went to the store. Manifests have a budget of their own, so the manifest cache reports a family of metrics of its own, `loonfs_manifest_cache_*`.

The SSD tier records each block that it finds in its directory at startup as an insert, in `loonfs_segment_cache_inserts_total{tier="ssd"}`. The rejections family, `loonfs_segment_cache_rejections_total`, has the same `tier` label and records the blocks that a tier didn't keep. The SSD tier records a rejection when the write queue is full, when the write fails, when the tier is already closed, or when the block is larger than the whole budget, and in each case the block stays in memory. The SSD tier's gauge, `loonfs_segment_cache_retained_stored_bytes`, reports the bytes charged to the blocks on disk. Each block's charge is the larger of its stored length and 4 KiB, plus the index's own allocation for the block, and `metadata_cache.ssd.max_segment_bytes` limits that same charge. So the gauge can report more bytes than the disk keeps, for example on a tier of blocks smaller than 4 KiB. The SSD tier has a gauge of its own because the disk keeps stored bytes, and the memory gauge, `loonfs_segment_cache_retained_decoded_bytes`, reports decoded bytes.

The [self-hosting guide](../../crates/loonfs-server/docs/self-hosting.md#keeping-metadata-blocks-on-local-ssd) describes the two settings, and its [Set one number](../../crates/loonfs-server/docs/self-hosting.md#set-one-number) section states the term that the SSD tier adds to `fixed`.

## Costs

This section lists what the block cache and its SSD tier cost.

| | Cost |
| --- | --- |
| Memory | The memory tier keeps decoded blocks, as the memory implementation does. The SSD tier's 64 MiB write queue and its index, at 168 bytes for each block on disk, are a term in `fixed` |
| Disk | Up to the SSD tier's budget (less on a tier of blocks smaller than 4 KiB), and a write for each inserted block that isn't on disk yet, including the blocks of the segments that folds and compactions write |
| CPU | A disk hit is checked and decoded again. A read decodes only the blocks of its read-ahead window that it reaches |
| Requests | The first read of a block that the server hasn't inserted costs a GET, with or without the tier. A disk hit costs no request |
| Code | One interface with two implementations, and no cache under the block cache |

## Acceptance measurements

This section describes the three measured cases that the SSD tier has to pass before LoonFS adopts the tier.

The SSD tier costs memory, disk, and CPU, so it's worth adopting if it removes requests and latency in the cases it's built for. Each case runs the same reads on a server with the SSD tier and on a server without it, next to the bucket:

| Case | Namespaces | Reads measured |
| --- | --- | --- |
| Restart | A built namespace of 100,000 files, and a restart of the server | The first 100 stats and first-page listings after the restart |
| Working set | A namespace of 1,000,000 files, with a memory budget smaller than its metadata | Stats and listings across the namespace |
| Many namespaces | More namespaces than the memory budget can keep | Reads of each namespace in rotation |

Each case compares the two servers' store requests per call and their latency distributions (p50, p90, and p99), and reads the peak memory of the server with the SSD tier against its memory limit. LoonFS adopts the SSD tier if, in all three cases, the server with the tier sends fewer requests per call and has lower latency, and its peak memory stays within its memory limit.

## What the format and API specs say

- The format's requirements don't cover caches, because caches belong to the runtime. [Block-segment encoding](../specs/format.md#a7-block-segment-encoding) defines the stored length and CRC32C that a block is checked against when it's read from the store and again when it's read from disk, and [Numbered objects and generated IDs](../specs/format.md#23-numbered-objects-and-generated-ids) defines the generated segment IDs that make each segment's key refer to one immutable object.
- The API specification leaves caches to the implementation, and a read returns the same metadata with or without the SSD tier.
- [Caching and combined reads](metadata-block-storage.md#caching-and-combined-reads) describes the block cache and read-ahead.
- The [self-hosting guide](../../crates/loonfs-server/docs/self-hosting.md) describes the SSD tier's settings, its term in the sizing formula, and its metrics.

## Alternatives considered

Keep manifests under the segment budget. Blocks that reads of other namespaces load can then evict a manifest while its namespace goes unread, and the next read of the namespace sends a GET for the manifest before it reads any block.

Keep each namespace's current manifest with its cached head, outside any budget. The default head budget keeps about 18,000 heads, and their manifests could take about 1 GB for namespaces of 10,000 files, or about 4 GB for namespaces of 1,000,000 files. Charging the manifests to the head budget instead would let the manifest of one large namespace evict hundreds of heads.

Keep index and filter blocks under a budget of their own as well. In the measurement that read 64 namespaces in rotation, such a budget saved no GET of an index or filter block at any segment budget from 64 MiB to 160 MiB, so it would add a setting and a family of counters without saving requests.

Put a disk tier under the decoded cache, with its own budget and its own rules, that keeps the stored bytes of some block kinds and not others (for example, index and filter blocks but not the data blocks that scans read). The server then has two caches that keep different sets of blocks under different rules. A block that one cache evicts can stay in the other, a block that a read uses can be one that the disk tier skips, and each cache needs its own counters and its own term in the sizing formula.

Write a block to disk when it leaves memory, instead of when it's loaded from the store. The disk then keeps the blocks that memory evicted, and a block isn't kept in both tiers. But a block in memory is decoded, and the handle's checksum covers its stored bytes. So either the memory tier keeps each block's stored bytes beside its decoded form, which takes memory budget away from decoded blocks, or the disk keeps a second encoding of each decoded block with a checksum of its own, which a disk hit checks and decodes with different code than a block read from the store.

Cache byte ranges at the object-store layer, under the block cache, in aligned chunks of each object (for example, 1 MiB). Chunk boundaries don't fall on block boundaries, so a block that crosses a boundary needs two chunks, and a read of one 20 KB block fetches and keeps a whole chunk, most of which no read asked for. The block cache above it already keeps the blocks that reads use, so the range cache spends its budget on the bytes around them.

Set a separate SSD budget for each namespace. An operator then sets one number for each tenant, and the working sets move away from those numbers. A tenant that grows misses while other tenants' budgets sit unused, and a tenant that goes idle keeps disk that a busy tenant could use.

Keep whole segment objects on disk instead of blocks. A segment's target size is 8 MiB decoded and a data block's is 64 KiB, so a page that needs a few blocks of a segment brings in about 128 of them. The disk fills with bytes that no read uses.

Raise the memory budget instead of adding a disk tier. Memory costs about an order of magnitude more per byte than local SSD, so keeping the same working sets costs much more, and memory is empty after a restart, so every namespace still starts cold.
