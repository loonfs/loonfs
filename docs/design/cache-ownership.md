# Cache ownership

This note describes which in-memory state LoonFS keeps in its caches, and which state it keeps with the process or the session that uses it. A process caches namespace [heads](../specs/glossary.md#head) under a budget of their own, so that a WAL tail can't evict a head, a [writer session](../specs/glossary.md#writer-session) keeps its own projected WAL tail, and LoonFS builds every cache from one cache type, with one family of counters and a separate budget for each kind of entry.

## The problem

A runtime reads through caches of decoded state: metadata segment blocks and manifests, namespace heads, projections of WAL tails, small content objects, and the completed upload sessions that garbage collection reuses between passes. An entry in any of them can be evicted and loaded again, but three cases need more than eviction and reloading.

- **Heads and tails differ in size.** A cached head is charged about 0.6 to 0.9 KB. (The largest access control list that LoonFS accepts, 1,000 grants with 64 KiB of principal IDs, makes a head about 116 KB.) A tail can be tens of MiB. If heads and tails shared one budget, one large tail could evict tens of thousands of heads.
- **A writer session reads its own tail.** A writer session plans each commit against its [writer position](../specs/glossary.md#writer-position), and it reads the projection of the [WAL tail](../specs/glossary.md#wal-tail) above the position's manifest on every publish, fold, and receipt check. If that tail were a cache entry, the cache could evict it while the session still needs it, and each of those paths would need a way to read the tail back from the store. For a publish, that's discovery: it reads the hint twice, the hinted manifest, and the next manifest number, reads every WAL object in the tail (up to 128) plus the missing numbers at the end of its last read window, and sends a HEAD for a successor manifest.
- **A head can change while a load of it is in flight.** LoonFS can refresh a cached head, evict it, or drop it (after a change whose effect on the head LoonFS can't identify) while a load of that head runs. Without a rule for this, a slow load that started before a newer head was installed, and finished after the cache evicted that newer head, would install the older head, and a later read would return state older than an earlier read did. A load that started before a drop would install state from before the change.

A cache with a structure of its own also needs its own eviction rule, its own counters, and its own check for a zero budget.

## A budget for heads

LoonFS keeps heads and tails in two instances of one cache type, the block cache engine (`DecodedBlockCache`), each with its own budget, so a tail can't evict a head. The head budget (`max_head_bytes`) is 16 MiB by default, which fits about 18,000 namespaces at 0.9 KB each, and the tail budget (`max_wal_tail_bytes`) is 64 MiB by default.

The engine keeps a map of the loads and checks in flight, with one entry per key while a caller loads or checks that key. The entry goes when its last caller leaves, so the map contains nothing for an idle namespace. Each entry is the key's join (a `SharedCheck`), and the entry covers the races above:

- **Sharing a refresh.** A load uses the entry so that concurrent callers load a missing key once (`get_or_load`), and a refresh of a cached head uses the entry for its check, so the reads that need a refresh of one head share that refresh, as [Read freshness](read-freshness.md) describes.
- **A drop during a load.** The entry records how many times an invalidation or an eviction has dropped its key. A load or a check that finds a drop since it started installs nothing. So a load that started before a drop doesn't install state from before the change, and a slow load doesn't install a head older than one that was installed and evicted while it ran.
- **Order.** Every install of a head (from a load, a refresh, or the end of a writer position) replaces the cached head only with a head that's newer in the durable order (head sequence, then manifest number), through `replace_head`. The engine's lock covers both the comparison and the insert, so a head doesn't replace a newer one, whatever order the installs run in.

A change whose effect the process can't identify drops the cached view, and the next read loads it. [Read freshness](read-freshness.md) describes the same rule. In the writer's process, the position's head stays out of the cache while the session is open.

Why do heads get a budget of their own? A reader process opens no session for the namespaces it reads, so it has no point at which it's done with a head, and its heads are a cache. A head is about 0.9 KB and a tail can be tens of MiB, so with one budget, one large tail could evict tens of thousands of heads.

Why doesn't a head need a record outside the cache? A record for each namespace, kept outside the cache's budget while a read or a session references it, could keep a refresh join, a generation that moves on each drop, and a floor (the order of the newest head that the record installed or saw, kept after the cache evicts that head). Each of those matters only while a load or a check of that head is in flight. With nothing in flight, there's no refresh for a read to share, and no load that a drop could make out of date. A head that was installed before a load or a check started is already durable, so the load or the check finds that head or a newer one. A floor therefore matters only for a head that's installed and then evicted while a load or a check is in flight, and the eviction then makes that load or check install nothing. The engine keeps an in-flight entry for each key for exactly the time that a join, a generation, and a floor matter.

## The writer session's tail

The session's tail belongs to its position. The position (`PublishTailPosition`) keeps the projected tail as an `Arc`, and reads that start from the position take the tail from the position. The shared cache keeps the tails of cached views, and not the tail of an open position. Whenever the position ends (the session closes, or LoonFS drops the position), LoonFS hands the read cache the position's head and its tail, with the tail under the key of that head (its manifest number and head sequence). The head is installed under the same rule as every other install of a head, as described in [A budget for heads](#a-budget-for-heads). When the session closes, the next read starts from the head and the tail without a replay. After another writer's claim or a fence, LoonFS then drops the cached head, because the next read has to load the new writer's state. The tail stays in the cache under its key, unused unless the next load finds the same head. (A tail under its key can't be wrong, because the namespace ID, manifest number, and head sequence identify one immutable tail.)

Because the position keeps its tail, the session doesn't look its tail up in the shared cache, so a publish, a fold, a receipt check, or a read in the writer's process has no evicted tail to handle.

The session bounds its tail itself. The write stop makes new commits wait for a fold at 128 unfolded WAL objects, and once about 32 MiB of inline bytes are unfolded, LoonFS stages new inline content as content objects instead (an append's bytes stay in the WAL past that limit). The fold that follows the fold trigger (32 WAL objects or 2 MiB of inline bytes) moves the tail into segments. The cost is memory. A session keeps its tail in memory until a fold moves the tail into segments, and during a large install the tail can be larger than 64 MiB, so a host pays that memory for each open session.

Why does the tail belong to the position and not to the cache? The session reads the tail on every publish, fold, and receipt check, so if the cache evicted the tail, the next publish would read the tail back through discovery, and each path that reads the tail would have to handle the miss. With the tail in the position, none of those paths needs a miss case, and the write stop and the fold trigger bound the memory.

## One cache type

LoonFS builds every cache from `DecodedBlockCache`:

| Cache | Default budget | Entries shared by | Contents |
| --- | ---: | --- | --- |
| Segment data blocks | 256 MiB | Runtimes over one object store | Decoded data blocks of metadata segments |
| Segment index and filter blocks | 32 MiB | Runtimes over one object store | Decoded index blocks, filter blocks, and manifests, which a lookup reads before it reads a data block |
| Heads | 16 MiB | One runtime | Namespace heads |
| WAL tails | 64 MiB | One runtime | Projections of the WAL tails of cached views |
| Content objects | 96 MiB | Runtimes over one object store | Content objects of up to 1 MiB |
| Completed upload sessions | 32 MiB | One runtime | The content ID and completion time of each completed upload session |
| Segment blocks on local SSD | None, because a server has no SSD tier unless its config sets one | Runtimes whose object stores have an identity | The disk key and stored length of each segment block on local SSD, charged at the block's stored length or 4 KiB, whichever is larger, so the budget bounds both the bytes on disk and the number of entries |

Each budget applies across every runtime that shares the metadata cache, and every cache evicts the least recently used entry first. The completed-session budget is 32 MiB, which keeps about 110,000 sessions. (The engine charges each session its recency position and table slot as well as its record, so 16 MiB would keep about half as many.) Segment block loads and tail replays go through `get_or_load`, so concurrent callers of one key load it once, and every load or refresh of a cached head goes through the head's in-flight entry. Every cache keeps the same family of counters (hits, misses, evictions, and rejections), and the engine applies the zero-budget rule (a cache with a zero budget keeps nothing), so no caller checks for a zero budget itself. The budget for segment index and filter blocks is the one setting whose zero doesn't mean an empty cache. At zero, LoonFS keeps index blocks, filter blocks, and manifests under the data blocks' budget, so a host can keep the segment cache under one budget. The block memo and the read working memory pool don't use the engine, because they're reservations against a pool, not caches.

Why one cache type? All seven caches keep immutable or rebuildable values under a byte budget, and nothing that they store needs a structure, an eviction rule, or a counter family of its own.

Why a budget for each kind instead of one budget? With one budget, a scan that loads segment blocks could evict a reader's tail, and the reader would then pay a replay. In the same way, a scan that loads data blocks could evict the index blocks, filter blocks, and manifests that every lookup reads first, and the next lookup in each namespace would pay a GET for each of them.

## Costs

| | Cost |
| --- | --- |
| Memory for heads | About 0.6 to 0.9 KB per head (about 116 KB with the largest access control list), in a 16 MiB budget of their own that fits about 18,000 heads at 0.9 KB. The lab's 100k sandbox workload uses 17 namespaces (about 15 KB). 10,000 namespaces need about 9 MB, or about 1.2 GB if each has the largest access control list, more than the budget fits |
| Memory for a writer session's tail | Kept by the session up to the write stop (128 WAL objects, with about 32 MiB of inline bytes plus appends), for each open session, and larger than 64 MiB during a large install |
| Requests after an eviction | None for a writer's tail, because the cache doesn't keep it. A reader whose tail is evicted replays the tail from the store |
| Code for heads | No record outside the cache. Each in-flight entry is its key's join, and an invalidation or an eviction of the key during the entry's load or check makes that load or check install nothing |
| Code for the writer's tail | No path for an evicted tail |
| Code for the caches | One structure, one counter family, and the zero-budget rule in the engine |

## What the format and API specs say

The format's requirements don't cover caches, because the caches belong to the runtime. [Publication and group commit](../specs/format.md#63-publication-and-group-commit) describes the reference runtime: after a confirmation, the writer keeps its position along with the position's tail.

The API specification leaves caches to the implementation. The documentation that describes them says:

- The Rust API's documentation of `max_head_bytes` and `max_wal_tail_bytes`, and the server's [self-hosting guide](../../crates/loonfs-server/docs/self-hosting.md), describe the head budget (16 MiB by default) and the tail budget (64 MiB by default in the Rust API) as two settings. The guide says that a session keeps its own tail in memory until a fold moves it into segments.
- The server reports evictions of heads in `loonfs_head_cache_evictions_total`, evictions of tails in `loonfs_wal_tail_cache_evictions_total`, and evictions of segment index blocks, filter blocks, and manifests in `loonfs_segment_index_cache_evictions_total`, each in its cache's family of counters.
- The `loonfs.publisher.tail_replays` counter records a publish that reads the WAL tail from the store, and an eviction isn't one of its causes.
- The documentation of `max_completed_upload_session_bytes` describes LRU eviction, and the self-hosting guide's table of caches lists the completed-session budget as 32 MiB.

## Alternatives considered

Keep a record of each namespace's head outside the cache, with a floor, a generation, and a refresh join, in a map of weak references that drops unreferenced records as the map grows. The record, its lock, and the map's compaction would then exist to keep state that matters only while a load or a check is in flight, which is how long the engine keeps an in-flight entry.

Keep every head until its namespace closes, with no budget. A reader process opens no session for the namespaces it reads, so there's no close, and its heads would grow with every namespace it reads.

Pin the writer's current tail in the shared cache. This uses the same memory as keeping the tail in the position, but the session would still look the tail up by key, so every path that reads the tail would still need a case for a missing tail.

Use one budget across all kinds of entry. Segment blocks could then evict a reader's tail, which costs a replay.

Evict nothing, and keep every tail, including the tails of readers. A process without the session can read any number of namespaces, so its tails would grow with each namespace it reads.
