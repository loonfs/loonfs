# Small-file bytes family

This note proposes a change to where LoonFS stores the bytes of small files once a fold has covered them. Today a fold writes every small file that a commit included inline to a content object of its own. With this change, a fold stores the complete bytes of a small whole file as a row in a new metadata row family, so a fold writes a few metadata segments where it wrote thousands of content objects, and a read of a small file is served from a segment block without a content request. Files that are appended to keep today's path, and a writer setting turns the family off, so a deployment can keep every file in its own object.

[Inline small content](inline-small-content.md) describes the inline path that this design extends. The design would change these parts of the storage format:

- [File contents and ownership](../specs/format.md#15-file-contents-and-ownership) and [Content verification](../specs/format.md#45-content-verification): where a reader finds a chain's bytes.
- [Publishing a materialized file set](../specs/format.md#72-publishing-a-materialized-file-set): which chains a fold stores as rows.
- [Operations and WAL deltas](../specs/format.md#66-operations-and-wal-deltas) and [WAL records](../specs/format.md#a5-wal-records): what a copy, a restore, and a new chain include in their commits.
- [Metadata rows and row keys](../specs/format.md#a6-metadata-rows-and-row-keys) and [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction): the new family, its group, and its retention.
- [Sweeping a retired owner's content](../specs/format.md#118-sweeping-a-retired-owners-content): what retirement deletes.

In the API specification, [Download transport](../specs/api.md#610-download-transport) would change for small files, and the self-hosting guide's [Resource sizing](../../crates/loonfs-server/docs/self-hosting.md#resource-sizing) would list one more inline setting.

## What small files cost a fold today

This section describes the requests a fold makes for small files, and what they cost in the lab.

A WAL object can't be deleted until a fold has written its pieces somewhere else, so every small file that a commit includes inline is written a second time at its fold. The fold writes each chain in its tail to a content object of its own, 16 chains at a time, and then it writes the metadata segments and publishes the manifest. A content write that merges no earlier extents (which includes every new small file) is one conditional PUT below 8 MiB. So a fold of 4,000 small files makes about 4,000 PUTs, followed by a few more for its segments and its manifest. Each of those objects also adds a layout row, an object that every garbage collection pass lists, and a GET to a cold read of the file.

The lab's 100k workloads, run on a host in the bucket's region (run `20261020T014302Z` in the lab repository, loonfs `8e4c5c3a7`), show how much of a write-heavy phase that is:

| Phase | PUTs | WAL objects | Segments | Manifests | Content objects and upload records |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sandbox install: 50,000 files, 99% of them at most 64 KiB | 64,862 | 441 | 356 | 95 | 63,970 (99%) |
| Agent turns: 2,000 sessions, 236,250 calls | 146,251 | 7,825 | 2,153 | 603 | 135,670 (93%) |

The install's 12 writer folds wrote about 5,000 content objects each, and the phase ended with a 9.5 s wait for its last fold. In the agent phase, 110,000 of the calls replace a whole small file (a 1 KiB state file on every turn, and a 6 KiB memory file on every tenth turn). Each replacement starts a new chain, so each one became a content object at its fold. The rest of the phase's content writes are for transcripts and subagent logs that are appended to. This design leaves those on today's path, along with the phase's 20,939 HEAD requests (about 118 per fold), which a fold sends for the earlier extents that it merges into an appended chain's new extent.

A log-structured tree on object storage usually keeps small values in the data blocks of its sorted tables, so a flush or a merge writes a few large objects, however many values they contain. This design applies that idea to small whole files.

Why not raise the fold's write concurrency instead? More writes in flight make a fold shorter, but each file still costs a request, an object that garbage collection lists, and a GET on a cold read.

## Bytes rows

This section defines the new row family and the chains that a fold stores in it.

A "bytes row" stores the complete bytes of one [chain](../specs/glossary.md#chain), from offset 0. Bytes rows make up a new metadata row family, the "small-file bytes family" (`small_file_bytes`), with at most one row for each content ID in a run:

| Field | Meaning |
| --- | --- |
| `owner_namespace_id` | The chain's owner, as recorded in its content reference. |
| `content_id` | The chain's content ID. |
| `committed_seq` | The commit that included the bytes. |
| `bytes` | The chain's bytes. |

The row key is `small-file-bytes-{content_id}`, and the filter key is the complete row key. A row stores no size or checksum, because a reader checks the bytes against the reference it's reading, as it does for a content object.

A fold stores a chain as a bytes row when all of these are true:

- The tail's pieces for the chain start at offset 0, so the tail includes the chain's whole value.
- Every reference to the chain in the tail has the same size, so nothing in the tail appends to it.
- That size is at most the folding runtime's row threshold, which is 64 KiB by default (see [The row threshold](#the-row-threshold)). The format's limit is the piece limit, 256 KiB.
- The chain has no layout.

Every other chain is written to content objects, as today. A zero-byte value qualifies too, so a fold writes an empty row instead of an empty object.

For example, suppose one fold covers four commits: a put of `a.js` (900 bytes), a put of `b.js` (40 KiB), an upload of `c.png` (200 KiB), and an append of 10 bytes to `a.js`. The fold stores `b.js` as a bytes row. It writes `a.js` to a content object, because the tail appends to its chain, and `c.png` already has its object.

The family joins the `content_layouts` [family group](../specs/glossary.md#family-group), so one compaction keeps or drops a chain's layout rows and its bytes row together. The group's retention rule becomes:

| Family | Rows kept or removed |
| --- | --- |
| `content_layouts`, `small_file_bytes` | Keep the first layout row for each content ID, ordered by longest layout and then newest commit. Keep one bytes row for each content ID, unless a kept layout row covers its size. In a base compaction, drop both families' rows for chains that aren't in the basis's revision filter. |

Two bytes rows for one content ID always store the same bytes (a restore can write a second one, as [Copies, restores, and new chains](#copies-restores-and-new-chains) describes), so a compaction that finds two different values reports corruption.

Why key a bytes row by content ID, and not by the revision that wrote it? A reference identifies content, and several revisions can refer to one chain. A copy reuses its source's reference on another inode, and a restore reuses an older revision's reference. A row found from one revision's key would be found from no other revision, and a base compaction of that revision's file would remove it while the copy still refers to the chain. Some reads also start from a reference with no revision at all: the administrator's bare reference read, an import with `prepare_content_ref`, and the check of whether a namespace has published a reference. Today these find a chain through its layout row, which is keyed by content ID, so a bytes row keyed the same way is found the same way.

Why the layouts group? A bytes row does the job of a chain's layout row (it records where the chain's bytes are), it has the same key, and it needs the same retention rule, which already exists. Every compaction of the group scans the basis's revisions once, and a base compaction drops the chains that none of them refer to. In one group, a compaction that keeps a chain's layout row can drop the bytes row that the layout covers. Garbage collection's content scan reads only `content_layouts` segments, so bytes rows add nothing to a pass.

## Folding

A fold works as [Publishing a materialized file set](../specs/format.md#72-publishing-a-materialized-file-set) describes, except that it writes no content object for a chain that it stores as a bytes row. It checks each whole value's checksum against its reference before it writes the row, as it does before it writes an object today, and it adds the bytes rows to its delta run along with its other rows. Segments roll at the usual targets (8 MiB or 65,536 rows), so a fold of 4,000 files of 4 KiB writes two or three bytes segments where it wrote 4,000 content objects.

A same-chain append to a chain stored as a bytes row works as it does today. The commit includes the appended bytes as a piece, and validation writes nothing. At the next fold, the chain has a bytes row in the basis and pieces in the tail. The fold writes the row's bytes followed by the pieces as the chain's first content object (a span from offset 0 to the newest reference's size), and it writes a layout row that lists that object. From then on, the doubling rule applies as it does to any appended chain. This fold sends one PUT for the chain and no HEAD, because the earlier bytes come from the row, not from an object. The bytes row stays while a revision refers to the chain, until a compaction whose window includes both rows drops it.

Why does an append move the chain to objects, instead of writing a longer bytes row? An append can add up to 256 KiB, and a chain can grow without limit, so a growing chain kept in rows would write its whole value again at every fold.

## The row threshold

This section describes the setting that controls which small files a fold stores as bytes rows, and what happens when it changes.

A fold stores a qualifying chain as a bytes row when the chain's size is at most the row threshold of the runtime that runs the fold. The threshold is writer policy, like the inline threshold, and it isn't stored in the namespace. It has the same shape and the same home as the other inline settings. It's a field of `InlineContentPolicy`, which an embedded host sets with `LoonFsBuilder::inline_content`, and a key in the server's `[inline_content]` table, which [Resource sizing](../../crates/loonfs-server/docs/self-hosting.md#resource-sizing) documents. Every fold reads it from its own runtime, as maintenance folds already read `inline_content_fold_at_bytes`.

| Setting | Default | Meaning |
| --- | --- | --- |
| `inline_content_row_threshold_bytes` | The inline threshold, 64 KiB | A fold stores a whole value at or under this size as a bytes row. `None` in the embedded policy, `false` in the server TOML, or 0 turns bytes rows off, and a fold then writes a content object for every file, as it does today. |

The threshold can't exceed 256 KiB, the piece limit. `InlineContentPolicy::default()` sets both thresholds to 64 KiB. In the server TOML, an unset value takes the configured `inline_content_threshold_bytes`, so a server with inline writes off has bytes rows off too. A deployment that keeps every file in its own object turns the row threshold off, and it can keep inline writes on. Commits then still include small files' bytes, and every fold writes them to content objects.

Changing the threshold doesn't move any file's bytes:

- **Lowering it.** Files that earlier folds stored as bytes rows stay rows. Compaction keeps each row while a revision refers to its chain, because retention follows revisions, not the setting. A direct download of such a file still writes its object when the grant is requested (see [Direct downloads](#direct-downloads)). Later folds write content objects for the files that the lower threshold excludes.
- **Raising it.** Existing content objects and their layouts stay as they are, because no fold or compaction rewrites a chain that's already in objects. Files that later folds cover can become bytes rows.

With the threshold off, a fold writes a bytes row in one case: a whole value of another owner's chain, which a fold stores as a row whatever its threshold, because it can't write that owner's objects. A writer whose threshold is off doesn't create such a value, because it restores an inherited bytes row under a new content ID (see [Copies, restores, and new chains](#copies-restores-and-new-chains)). So when every process that writes or folds a namespace has the threshold off, folds write a content object for every file, as they do today, and the rows that earlier folds wrote stay readable.

Why is the threshold writer policy, and not a namespace property? Like the inline threshold, it changes how much a fold writes and how a read is served, not what a read returns. Each process that folds a namespace can have a different setting, and the setting can change between two folds, so one namespace can have small files in both forms, and a pin or a fork can keep both forms readable long after the setting changed. A namespace property would have to be changed with a publication and checked by every fold, and it still couldn't move the files that earlier folds wrote. That's why each folded chain's own record (a layout row or a bytes row) says where its bytes are, and a read follows that record, never the setting.

## Reading

This section describes where a reader finds a small file's bytes.

Every folded chain has its own record in the layouts group, and a read follows that record, not the row threshold or the file's size. A reader looks up the chain by its content ID. A layout row lists the chain's content objects, and a bytes row contains its bytes. The tail's pieces come after either one, as they do today. The table in [Content verification](../specs/format.md#45-content-verification) changes like this:

| Condition | Where the reader reads the bytes |
| --- | --- |
| The tail has pieces at or below the reference's size | The chain's layout, or else its bytes row, up to the lowest tail offset, and then the pieces, up to the reference's size. |
| No pieces cover the reference | The chain's layout, or else its bytes row, up to the reference's size. |
| Zero bytes, and no pieces | Nowhere. The reader verifies the empty value without a request. |
| A nonzero folded prefix with neither a layout nor a bytes row | Nowhere. The reader reports corruption, identifying the content ID. |

A chain can have both records for a while (after an append, until compaction drops the bytes row), and the reader then uses the layout. Both store the same bytes for the row's length, so either is correct, and preferring the layout means that every reader makes the same choice.

A cold read of a folded small file reads one data block of the bytes family, without a read-ahead window. Today it reads a block of the layouts family and then the content object, so a cold read sends one request where it sent up to two. A warm read sends none in either design.

Why doesn't the reader choose by the file's size or by the threshold? A small file can be in a content object: an upload is never stored as a bytes row, a writer with a lower threshold or with bytes rows off writes small files to objects, an appended chain has a layout, and a fork inherits whatever its source stored. The threshold can also differ between processes and change over time, as [The row threshold](#the-row-threshold) describes. The chain's own record is the reliable signal, and finding it costs a filter probe in each of the two families, which usually needs no request.

### Caching bytes rows

The memory tier of the [block cache](block-cache.md) keeps the bytes family's blocks under a budget of their own, separate from the segment budget (256 MiB by default), so a burst of small-file reads can't evict the blocks that path resolution and listings read. For example, 100,000 files of 1 KiB make about 105 MiB of bytes rows, which would be 41% of the segment budget. Small files stored as rows don't go through the content-object cache, so the new budget takes its memory from that cache's 96 MiB. The SSD tier keeps the family's blocks under its one budget, like any other block.

Why a separate budget? [Cache ownership](cache-ownership.md) keeps each kind of entry under a budget of its own so that one kind can't evict another, and a small file's bytes are content that a path lookup or a listing never reads.

## Copies, restores, and new chains

This section describes how a commit that reuses a bytes row's reference, or builds on one, gets the row's bytes.

Today such a commit includes a layout for the bytes it needs, and validation first writes any pending pieces to content objects. A bytes row has no object for a layout to list, so each case changes:

- **Copies.** A copy reuses its source's reference, and its delta includes neither a layout nor pieces. Validation still looks the chain up, as it does today to build the copy's layout. A copy always takes the source file's current revision, and that revision stays the file's head in every manifest until the copy is folded. (A later replacement or deletion of the source comes after the copy in the WAL, so any fold that covers it covers the copy too.) So retention keeps the source's revision and the bytes row until the copy is folded, and after that the copy's own revision keeps the row. The decoder can't see the bytes family, so the rule that a delta without pieces needs a layout becomes a check in validation for this case.
- **Restores.** A restore includes the restored revision's bytes as a piece at offset 0 of the same chain, and the fold that covers it writes a second bytes row for the chain, or the chain's whole object when its runtime's threshold excludes the value. Unlike a copy's source, a restored revision can be old. A floor advance and a base compaction of the revisions group can remove it before the restore is folded, and a base compaction of the layouts group would then drop the bytes row that the restore refers to. In a fork, the restored chain can belong to the source namespace, so [WAL records](../specs/format.md#a5-wal-records) would accept a whole value of at most 256 KiB for another owner's chain, and a fold stores such a value only as a bytes row. If the commit has no inline budget left, or the writer's row threshold is off, a restore of the namespace's own chain falls back to today's path (validation writes the bytes to the chain's whole object, and the restore includes that layout), and a restore of an inherited chain uses a new content ID owned by the fork, as an append in a fork does.
- **New chains.** An append that starts a new chain from a bytes-row base includes the base's bytes as the new chain's first piece, at offset 0, followed by the appended bytes. That's an ordinary new chain with a whole value, so validation writes nothing, and no WAL rule changes. Validation reads the base's bytes from its row, which can cost one block read. The fold stores the result as a bytes row if it's at or under the threshold, and as a content object otherwise. If the base's bytes don't fit in the commit's WAL object, validation writes them as the new chain's first extent, and the delta includes that layout.
- **Imports.** An import with `prepare_content_ref` reads the owner's bytes row where the owner's view has one, and it still writes the bytes under a new content ID owned by the destination, as [Cross-namespace copies and moves](../specs/format.md#97-cross-namespace-copies-and-moves) describes.

Why doesn't a copy include the bytes, the way a restore does? A copy of a directory tree is one request, so including every file's bytes would fill the commit's inline budget (1 MiB by default) after a few hundred files, and the rest would fall back to one content write per file in validation, where today a copy of folded files writes none. A copy's source is always a file's head, which retention keeps, so the copy needs nothing in its commit.

## Direct downloads

A direct download grant signs a URL for an immutable object, and a bytes row isn't an object. For a reference whose chain has no layout (a bytes row, or a whole value that's still in the tail), the grant writes the bytes to a new [temporary object](../specs/glossary.md#temporary-object) under the namespace's `temporary/` prefix, with the fold's writer and one content-write permit, and signs that object. Garbage collection deletes temporary objects once they're older than the reclamation grace (at least 41.5 minutes), and a URL expires after 15 minutes, so no URL outlives its object. A chain with a layout keeps today's path. A deployment that can't write objects returns `content_not_materialized`, and the client reads the file through the proxied route, which serves any small file in one request.

Why not write the object at the reference's own key, as the inline design does before a fold? When a fold stores the chain as a bytes row, no layout lists that object, so nothing protects it after the fold, and garbage collection deletes it once it's older than the grace. A later grant that found the object and signed it could issue a URL that stops working before it expires. The granting process can't tell which form a fold will choose, because the fold can run in another process with another threshold, so the grant uses a temporary object whatever its own threshold is. In a fork, the reference's key is also under the source namespace's prefix, which the fork doesn't write to. A new temporary object for each grant avoids both problems, for one PUT per grant of a small file.

## Collection and retirement

A bytes row is metadata, so garbage collection has nothing of its own to delete for it. The layouts group's retention keeps the row while a revision refers to its chain, and garbage collection deletes the row's segments under the segment rules in [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules). Pins and fork pins keep bytes rows the same way they keep every other row of the manifests they protect. A small file stored as a bytes row has no layout row and no content object, so each pass lists fewer objects and scans fewer layout rows.

Retirement needs one new step. A namespace tombstone keeps its runs as a root, so garbage collection never deletes their segments, and [Sweeping a retired owner's content](../specs/format.md#118-sweeping-a-retired-owners-content) deletes the content prefix. Without a change, a retired namespace's large files would be deleted and its small files would stay in storage. So the sweep also deletes every bytes-family segment that the tombstone's runs list and that the namespace owns, after it deletes the content prefix. No read needs those rows after retirement. An import from a deleted owner can read its bytes until then, and afterward a missing bytes segment returns `namespace_deleted`, as a missing content object does. A fork pin keeps its source from retiring, so no fork that's still active reads a deleted segment.

## The reasons against values in segments

[Inline small content](inline-small-content.md#alternatives-considered) lists four reasons for writing small values out at the fold instead of keeping them in segments. This design answers each one:

- **Compaction rewrites the bytes.** It does, in the layouts group, not the revisions group, so a compaction of the revisions that stats and listings read doesn't touch them. The layouts group rewrites each byte about once each time its run grows four times, and [Costs](#costs) estimates what that adds up to.
- **Metadata blocks fill with payloads.** Bytes rows have their own family, so their segments, blocks, and filters are separate. Path resolution, stats, and listings read other families, so none of them reads a bytes row. The block cache is where the two meet, and the family's own budget keeps them apart.
- **Direct downloads still need an object.** They do. A grant writes a temporary object, which costs one PUT per grant of a small file, and proxied reads (the usual path for small files) need none.
- **File bytes stay out of the metadata tree.** This design doesn't keep that property for small whole files. Metadata stored bytes grow by those files' bytes, retirement has to delete the family's segments, and a fork copies inherited small files when it compacts its own layouts group. A deployment that keeps file bytes out of its metadata turns the row threshold off, and it can keep inline writes on.

The same note rejects packing a fold's values into one object, because retention can leave a pack partly dead, and reclaiming its space needs a rewrite. A segment is such a pack, and compaction is the rewrite that already exists.

## Costs

This design has these costs:

- **Compaction writes more bytes.** A base compaction of the layouts group rewrites every bytes row it keeps, and size tiers rewrite each byte about once each time the run that contains it grows four times (see [Compaction windows](../specs/format.md#102-compaction-windows)). The table below assumes files of 1 KiB that don't compress (like the lab's generated files) and fold runs of 2 MiB (the inline fold trigger). The figures for today's layouts group use the 43 stored bytes per layout row measured in the lab's local sandbox smoke run (`20261014T042848Z`).

  | Files of 1 KiB | Layouts group today | Layouts group with bytes rows | Largest compaction (the base) | Compaction writes as the namespace grows |
  | ---: | ---: | ---: | ---: | ---: |
  | 100,000 | about 4 MB | about 105 MiB | about 105 MiB | up to about 3 rewrites, about 300 MiB |
  | 1,000,000 | about 43 MB | about 1 GiB | about 1 GiB | up to about 5 rewrites, about 5 GiB |

  A fold that covers a larger tail writes a larger run, which leaves fewer rewrites (the install's folds wrote about 17 MiB of small files each). In requests, rewriting 1 GiB takes about 130 segment PUTs, where today's folds of the same files made a million content PUTs. In bytes written to the store, a small file costs about four to six times its size over the namespace's growth, where today it costs one.
- **Metadata stored bytes grow.** A namespace's `metadata_stored_bytes` includes its bytes rows, for every retained revision of a small file. 100,000 files of 1 KiB add about 105 MiB, about five times what the other families store for those files, and the sandbox's 50,000-file install adds about 200 MiB, because its small files average 4.1 KiB. Content storage falls by about the same amount, but the statistics observation doesn't include content, so an application that meters `metadata_stored_bytes` sees the growth and not the saving. `segments_by_family` lists the new family.
- **The block cache keeps whole blocks.** A read of one small file loads a whole data block (64 KiB by default), so when reads have no locality, a budget keeps far fewer small files than the content-object cache does. 96 MiB keeps about 24,000 files of 4 KiB as content objects, or about 1,500 blocks of 64 KiB. Content IDs are random, so files written together don't share blocks. Smaller blocks for the family make this better, at the cost of a larger index block in each segment.
- **Forks copy small files when they compact.** A fork shares its source's bytes segments until a base compaction of its own layouts group writes the inherited bytes rows into the fork's segments, as a base compaction does with every inherited row. A fork never copies a content object today. A fork that writes little rarely merges its inherited base, because its small runs don't make a size tier with the base, but an explicit compaction does merge it.
- **Some commits include earlier bytes.** Restores and new chains include up to 256 KiB of earlier bytes in their commits (64 KiB at the default threshold).
- **Direct downloads of small files write an object.** A grant for a chain without a layout costs one PUT, where a folded file costs none today. This is true with the row threshold off too, so with bytes rows off, a grant for a whole value in the tail writes a temporary object, and the fold writes the chain's own object afterward.
- **Some format checks move.** The decoder accepts a copy's delta without a layout or pieces, so validation checks it. WAL records accept a whole value for another owner's chain. Retirement deletes segments that a root lists.

These stay the same:

- **Cold replay of the WAL tail.** The tail includes the same pieces until a fold covers it, and the fold triggers don't change. A faster fold leaves a shorter tail.
- **The fingerprint contract.** A commit's fingerprint is built from its request, and the bytes that a restore or a new chain includes are part of its WAL record, not its request.
- **One reference kind.** Every revision keeps an ordinary `blob_v1` reference, and the reference doesn't record where the bytes are.
- **Activity counters.** They count what a revision adds, so a restore counts its full length, and an append counts the bytes it appends, even when its commit includes the base's bytes.

## Measurements

This section describes what the lab measures to decide whether LoonFS ships this design, and the results that would stop it.

The lab first checks the premise on today's main, before any code is written. It traces each writer fold in the sandbox install and the agent turns phases at 100k, and records the fold's wall time, its requests by kind, and the share of its wall time that its content writes take. If content writes take less than half of the median fold's wall time in both phases, removing them can't make folds twice as fast, and the design stops there.

If the premise is true, the lab runs the 100k workloads on its host in the bucket's region with one prototype build, once with `inline_content_row_threshold_bytes` at its default and once with it off, and compares both with run `20261020T014302Z`:

| Area | What the lab records | Expected with bytes rows on |
| --- | --- | --- |
| Folds | Each writer fold's wall time and requests by kind, in the install and the agent turns | Install PUTs fall from 64,862 to a few thousand, mostly WAL objects and the uploads of large files. Agent-turn PUTs fall by about 110,000, and HEADs stay near 21,000. |
| Throughput | Install files per second, commit p99, and the final fold wait, and agent turns at a higher offered rate (the phase isn't fold-bound at today's rate) | The install runs faster than 725 files per second, and its final fold wait falls well below 9.5 s. |
| Compaction | Each layouts-group compaction's wall time, bytes read and written, and peak RSS, an explicit compaction after the install, and the 1m sandbox install (200,000 files, about 800 MiB of small files) for a large base | Compactions take much less time in total than the folds save. |
| Reads | The matrix's `stat`, `read_small`, `read_revision`, `list_first_page`, and `list_revisions`, the edit loop's `read`, and the restore's cold reads from forks, at p50 and p95, with requests per call, for 64 KiB and 16 KiB bytes-family blocks and two splits of the content budget | No row gets slower, and a cold small read sends at most one request. |
| Collection | GC pass time and requests after the reinstall and the sessions expiry, and stored bytes by family | Passes get faster and list fewer objects. |
| Copies and restores | A copy of the 2,000-file repository, and a batch of restores | The copy writes no content objects. |

LoonFS doesn't ship bytes rows if any of these is true:

- With the row threshold off, a fold sends different requests than main's fold sends for the same tail. The matrix's `explicit_fold` row folds a tail that the lab builds the same way in every run, and it sends 97 requests on main in all three 100k datasets. With the threshold off, it has to send exactly 97, with the same count of each kind, and the install's writer folds have to send the same requests for each chain they write, by kind. (Direct download grants are the one path that differs with the threshold off, as [Costs](#costs) describes, so the comparison uses folds that no grant preceded.) Any difference stops the design until it's explained.
- With bytes rows on, the median writer fold in the install isn't at least twice as fast, or the install's files per second rises by less than half (from 483 to under 725).
- The layouts group's compactions during and after the install take longer, in total, than the install's folds save.
- A read row above gets more than 10% slower at p50 or p95, or sends more requests per call.
- Peak RSS rises by more than 64 MiB in any phase, or a GC pass gets slower.

## Alternatives considered

- **Bytes rows in the revisions group, keyed by revision.** A key of `(inode, revision number, committed sequence)` sorts a file's bytes next to the bytes of the files created with it, so a cold read of a directory of small files reads one block for many files, and a compaction of the revisions group keeps or drops each bytes row together with its revision row, as the commits group does for commits and receipts. But a copy reuses its source's reference on another inode, so the copy's revision has no row under its own key, and once the source file's revisions are removed, the copy can't be read. A bare reference read, an import, and the published check start from a content ID, and they couldn't find the row at all.
- **Bytes inside the `file_revision` row.** A stat and a read would share one lookup. But listings and revision histories read revision rows for sizes and references, so a listing of 1,000 small files would read up to 64 MiB of rows instead of about 60 KB, and the cache's revision blocks would be mostly file bytes. This design also has the copy problem above.
- **Bytes inside the layout row, as a new kind of extent.** No new family is needed. But each garbage collection pass scans every layout row of every retained view, so it would read every small file's bytes on every pass.
- **A layout row with an extent that points to a bytes row.** The layout would stay the one record that a reader looks up. But every cold read would need two lookups, and every small file two rows. With bytes rows keyed by revision, the layout's pointer could also refer to a row that a compaction already removed.
- **Choosing the form by the file's size.** Reads would need no record for small files. But uploads, writers with a lower threshold or with bytes rows off, appended chains, and forks all put small files in objects, and the threshold changes over time, so a size can't say where a file's bytes are.
- **A namespace property for the threshold.** Every process would fold the namespace the same way. But changing it would need a publication that every fold checks, and it still couldn't move the files that earlier folds wrote, so readers would need the per-chain record anyway.
- **A family group of its own.** A compaction of the layouts group wouldn't rewrite bytes. But both groups receive a run from every fold and reach their size tiers at the same folds, so the bytes would be rewritten as often. The group would also need its own scan of the basis's revisions, and its compactions couldn't drop a bytes row when the chain's layout row appears.
- **Copies and restores that write an object in validation,** as they do today for pending pieces. No WAL rule would change. But a copy of a tree of folded small files would make one content write per file on the commit path, where today it makes none, and in a fork, validation can't write under the source's prefix.
- **Restores under a new content ID.** No WAL rule would change, but a restored file would have a different reference from the revision it restores, so one file's history would show two references for the same bytes. [Inline small content](inline-small-content.md#a-reference-names-content-not-a-location) keeps one identity for the same content over its whole life.
- **Time-ordered content IDs for small values.** Files written together would sort together, so they'd share blocks. But the format draws content IDs at random, and garbage collection divides content into shards by ID prefix, so IDs with a common time prefix would put most new content into one shard.
