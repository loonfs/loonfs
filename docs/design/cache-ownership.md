# Cache ownership

This note describes which in-memory state LoonFS keeps in its caches, and which state it keeps with the process or the session that uses it. A process caches namespace [heads](../specs/glossary.md#head) under a budget of their own, so that a WAL tail can't evict a head, a [writer session](../specs/glossary.md#writer-session) keeps its own projected WAL tail, and LoonFS builds every cache from one cache type, with one family of counters and a separate budget for each kind of entry.

## The problem

Today, each runtime reads through four caches:

| Cache | Default budget | Shared by | Contents | Eviction |
| --- | ---: | --- | --- | --- |
| Segment blocks | 256 MiB | Runtimes over one object store | Decoded metadata segment blocks and manifests | Least recently used |
| Head state | 64 MiB | One runtime | Namespace heads, and projections of WAL tails | Least recently used |
| Objects | 128 MiB | Runtimes over one object store | Decoded WAL objects, and content objects of up to 1 MiB | Least recently used |
| Completed upload sessions | 16 MiB | One runtime | The content ID and completion time of each completed upload session | Every entry is dropped when an insert takes the cache over its budget |

The first three are instances of one cache type (`DecodedBlockCache`), a hash map with a lazy LRU queue. It charges each entry the weight of its value plus the heap of its key, evicts on inserts and on hits, rejects an entry whose charge is more than the whole budget, and loads a missing key once for concurrent callers (`get_or_load`). The completed-session cache has a structure of its own. Beyond that, segment loads go through `get_or_load` and the other loads don't, each cache reports its own family of counters, and the object, head-state, and session code each check for a zero budget outside the engine.

Two mechanisms exist because the head-state cache can evict an entry that its user still needs, and because a head can be dropped or refreshed while a load of that head is in flight.

The first is the "head record". A runtime keeps one record per namespace outside the cache's budget, for as long as a read or a writer session references it, in a map of weak references that drops unreferenced records as the map grows. The record stores a floor, a generation, and a refresh join, and the record's lock covers the lookup and the insert of a new head. The floor is the order (head sequence, then manifest number) of the newest head that the record installed or saw, and it stays after the cache evicts that head. Without the floor, a slow load that started before a newer head was installed, and finished after the cache evicted that newer head, would install the older head, and a later read would return state older than an earlier read did.

The generation moves each time the runtime drops the cached head after a change whose effect on the head the runtime can't identify. A refresh or a load installs its head only if the record's generation hasn't moved since the refresh or the load started, so a load that started before a drop installs nothing after the drop. The refresh join (`SharedCheck`) makes the reads that need a refresh share one. A read waits for the refresh in flight and then shares the next one, so a wave of concurrent strong reads costs at most two refreshes. An eventual read takes the head that the refresh in flight confirmed when that head is within the read's maximum staleness, so a wave of concurrent eventual reads costs at most one refresh. A cached head is charged about 0.6 to 0.9 KB. The largest access control list that LoonFS accepts (1,000 grants with 64 KiB of principal IDs) makes a head about 116 KB.

The second is the set of paths that recover a writer's evicted tail. A writer session plans each commit against its [writer position](../specs/glossary.md#writer-position), and it stores the projection of the [WAL tail](../specs/glossary.md#wal-tail) above the position's manifest in the head-state cache, keyed by namespace, manifest number, and head sequence. When the cache evicts the tail:

- A publish runs a "cold load". It reads the hint twice, the hinted manifest, and the next manifest number, reads every WAL object in the tail (up to 128) plus the missing numbers at the end of its last read window, and sends a HEAD for a successor manifest.
- A fold reads its input from the store.
- A read in the writer's process replays the tail from the store.
- A receipt check before staging inline content falls back to a `strong` read.
- A manifest that the runtime publishes drops the position instead of moving it.

Each publish also removes its tail from the cache and inserts it again around its update. After a fold or a published manifest, the session removes the tail under the old key and inserts it under the new key.

In the lab's two S3 runs of the 100k workloads, tail misses occurred in the app-drive setup (6) and user operations (32) phases, and in the sandbox install (33 and 14), repo (1), and reinstall (7 and 13) phases. In one install run, the cache rejected 15 tails because each was charged more than the 64 MiB budget.

## A budget for heads

LoonFS keeps heads and tails in two instances of `DecodedBlockCache`, each with its own budget, so a tail can't evict a head. The head budget is 16 MiB by default, which fits about 18,000 namespaces at 0.9 KB each, and the tail budget stays at 64 MiB. Both are settings.

The head record goes, and the engine's map of loads in flight takes over the record's jobs. The engine (`DecodedBlockCache`) already keeps this map, with one entry per key, so that concurrent callers of `get_or_load` load a missing key once, and the map contains a key only while a load of that key is in flight. Each in-flight entry becomes the key's join. A load uses the entry the way `get_or_load` does today, and a refresh uses the entry for its check, so the reads that need a refresh of one head share that refresh. `SharedCheck` moves into the engine as the entry's type. An invalidation or an eviction of a key during a load or a check of that key makes the load or the check install nothing. The rule for an invalidation does the job of today's generation, and the rule for an eviction does the job of today's floor, but the engine applies both per key and only while a load or a check is in flight. Every install of a head (from a load, a refresh, or the end of a writer position) still replaces the cached head only with a head that's newer in the durable order (head sequence, then manifest number), as `replace_head` does today. The engine's lock covers both the comparison and the insert. Together, these changes replace the record type, its lock, its floor, its generation, the map of weak records, and the map's compaction.

A change whose effect the process can't identify drops the cached view, and the next read loads it. [Read freshness](read-freshness.md) describes the same rule. In the writer's process, the position's head stays out of the cache while the session is open.

Why do heads get a budget of their own? A reader process opens no session for the namespaces it reads, so it has no point at which it's done with a head, and its heads are a cache. A head is about 0.9 KB and a writer's tail can be above 64 MiB, so with one budget, one large tail could evict tens of thousands of heads.

Why can the record go? The record keeps a refresh join, a generation, and a floor for a namespace's head, and each matters only while a load or a check of that head is in flight. With nothing in flight, there's no refresh for a read to share, and no load that a drop could make out of date. A head that was installed before a load or a check started is already durable, so the load or the check finds that head or a newer one. The floor therefore matters only for a head that's installed and then evicted while a load or a check is in flight, and the eviction then makes that load or check install nothing. The engine's in-flight map keeps an entry for each key while a load or a check of that key is in flight, which is the time that the join, the generation, and the floor matter, and the map keeps nothing for an idle namespace.

## The writer session's tail

The session's tail belongs to its position. The position (`PublishTailPosition`) keeps the projected tail as an `Arc`, and reads that start from the position take the tail from the position. The shared cache keeps the tails of cached views in processes without the session, and not the writer's tail. Whenever the position ends (the session closes, or LoonFS drops the position), LoonFS hands the read cache the position's head and its tail, with the tail under the key of that head (its manifest number and head sequence). The head is installed under the same rule as every other install of a head, as described in [A budget for heads](#a-budget-for-heads). When the session closes, the next read starts from the head and the tail without a replay. After another writer's claim or a fence, LoonFS then drops the cached head, because the next read has to load the new writer's state. The tail stays in the cache under its key, unused unless the next load finds the same head. (A tail under its key can't be wrong, because the namespace ID, manifest number, and head sequence identify one immutable tail.)

Every path that handles an evicted writer tail goes: the session's lookups, inserts, and invalidations of its tail in the shared cache, the misses described above, and the removal and reinsertion around each publish.

The session bounds its tail itself. The write stop refuses commits at 128 unfolded WAL objects, and once about 32 MiB of inline bytes are unfolded, LoonFS stages new inline content as content objects instead (an append's bytes stay in the WAL past that limit). The fold that follows the fold trigger (32 WAL objects or 2 MiB of inline bytes) moves the tail into segments. The cost is that during a large install, a tail above 64 MiB stays resident instead of being read again on every publish and fold, and a host pays that memory for each open session.

Why does the tail belong to the position and not to the cache? The session reads the tail on every publish, fold, and receipt check, so an evicted tail comes back on the next publish through a cold load, and each path that reads the tail has to handle the miss. With the tail in the position, those paths go, and the write stop and the fold trigger bound the memory.

## One cache type

LoonFS builds every cache from `DecodedBlockCache`. The completed-session cache becomes an instance too, with LRU eviction and a 32 MiB budget, double today's. The engine charges each session its recency position and table slot, so 16 MiB would keep about half as many sessions (about 57,000 instead of 110,000). Every loader that can race with another goes through `get_or_load`, and a refresh of a cached head goes through the key's in-flight entry, the same entry that a load of that key uses. Each cache reports the same family of counters (hits, misses, evictions, and rejections), and the engine applies the zero-budget rule, so no caller checks for a zero budget itself. The block memo and the read working memory pool stay as they are, because they're reservations against a pool, not caches.

The budgets stay separate for each kind of entry: segment blocks, heads, tails, objects, and completed sessions.

Why one cache type? All five caches keep immutable or rebuildable values under a byte budget, and nothing that they store needs a structure, an eviction rule, or a counter family of its own.

Why a budget for each kind instead of one budget? With one budget, a scan that loads segment blocks could evict a reader's tail, and the reader would then pay a replay.

## Costs

| | Today | With this design |
| --- | --- | --- |
| Memory for heads | About 0.6 to 0.9 KB per head (about 116 KB with the largest access control list), in the 64 MiB head-state budget that tails share | The same per head, in a 16 MiB budget of their own that fits about 18,000 heads at 0.9 KB. The lab's 100k sandbox workload uses 17 namespaces (about 15 KB). 10,000 namespaces need about 9 MB, or about 1.2 GB if each has the largest access control list, more than the budget fits |
| Memory for a writer session's tail | Within the head-state budget, evicted under pressure, and not kept above 64 MiB | Kept by the session up to the write stop (128 WAL objects, with about 32 MiB of inline bytes plus appends), above 64 MiB in the lab's 100k sandbox install, for each open session |
| Requests after the cache evicts a writer's tail | A publish runs a cold load (one GET per WAL object in the tail, up to 128, plus four control-object GETs, the GETs past the tip, and a HEAD). A fold reads the tail from the store, and a read replays it | None, because the cache doesn't keep the writer's tail |
| Code for heads | The head record outside the cache, with its lock, a floor, a generation, and a refresh join, in a map of weak records with compaction | The record type goes, and the engine's in-flight entries replace it. Each entry is its key's join, and an invalidation or an eviction of the key during the entry's load or check makes that load or check install nothing |
| Code for the writer's tail | The paths that handle an evicted tail | Those paths go |
| Code for the caches | Two cache structures, a counter family for each cache, and zero-budget checks in the object, head-state, and session code | One structure, one counter family, and the zero-budget rule in the engine |

## What the format and API specs change

The format's requirements don't change, because the caches belong to the runtime. In [Publication and group commit](../specs/format.md#63-publication-and-group-commit), the description of the reference runtime changes: after a confirmation, the writer keeps its position along with the position's tail, and the case where the tail was evicted goes.

The API specification leaves caches to the implementation. The documentation that describes them changes:

- The Rust API's documentation of `max_head_state_bytes`, and the server's [self-hosting guide](../../crates/loonfs-server/docs/self-hosting.md), describe the head budget (16 MiB by default) and the tail budget (64 MiB by default) as two settings. The guide's sentence that a session's tail is stored in the head-state cache between publishes says that the session keeps the tail.
- The `loonfs_head_state_cache_evictions_total` counter splits into a head counter and a tail counter, one in each cache's family.
- The `loonfs.publisher.tail_replays` counter keeps its meaning (a publish that reads the WAL tail from the store), and its list of causes loses the tail eviction.
- The documentation of `max_completed_upload_session_bytes` describes LRU eviction, and the self-hosting guide's table of caches lists the completed-session budget as 32 MiB.

## Alternatives considered

Keep the head record outside the cache, with its floor, its generation, and its refresh join. The record type, its lock, the map of weak records, and the map's compaction then stay, to keep state that matters only while a load or a check is in flight, which is how long the engine keeps an in-flight entry.

Keep every head until its namespace closes, with no budget. A reader process opens no session for the namespaces it reads, so there's no close, and its heads would grow with every namespace it reads.

Pin the writer's current tail in the shared cache. This uses the same memory as keeping the tail in the position, but the session still looks the tail up by key, so every path that handles a missing tail stays.

Use one budget across all kinds of entry. Segment blocks could then evict a reader's tail, which costs a replay.

Evict nothing, and keep every tail, including the tails of readers. A process without the session can read any number of namespaces, so its tails would grow with each namespace it reads.
