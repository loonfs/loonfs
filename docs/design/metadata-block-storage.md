# Metadata Block Storage

LoonFS keeps a namespace's filesystem metadata — bindings, revisions,
inodes, and the other per-kind tables, called families — in immutable
segment objects arranged as a log-structured merge tree and referenced
by the namespace manifest. This document describes the segment layout,
the read path, and the maintenance cycle, and why they are shaped that
way. The binding rules live in `docs/specs/format.md` (section 4.2.1
for the object layout, section 6 for maintenance).

## Segment objects

A segment holds sorted rows for one family. The object is a sequence of
independently readable sections:

```
+-------+-------+     +-------+--------+-------+
| data  | data  | ... | data  | filter | index |
| block | block |     | block | block  | block |
+-------+-------+     +-------+--------+-------+
  ~64 KiB of rows each            |        |
                                  |        +- last key and byte range
                                  |           of every data block
                                  +- can answer "that key is
                                     definitely not in this segment"
```

Keys within a family share long prefixes, so each entry in a data block
stores only the part of its key that differs from the previous entry.
Every block is compressed and checksummed on its own, so a reader can
fetch and verify exactly the byte ranges it needs.

There is no footer or header. The manifest's segment descriptor records
where the index and filter blocks live, and is the only way into the
object:

```
manifest ──> descriptor ──> index block ──> data blocks
             (offset, length, and checksum at every arrow)
```

A segment is only reachable through a manifest, so a self-describing
footer would be a second copy of the truth. The checksums chain
instead: the manifest pins the index, the index pins each data block,
and every ranged read is verified before it is decoded — a manifest
therefore pins the exact bytes of every segment it references.

## Reads

A lookup touches a segment in this order, stopping as early as it can:

1. Key range in the descriptor — free, already in the manifest.
2. Filter block — a bloom filter over each row's lookup key. It answers
   "definitely not here" (skip the whole segment) or "maybe" (continue).
   Small delta-run segments inline the filter's bytes in their manifest
   descriptor, making this step free for exactly the segments an
   unfolded delta backlog multiplies: delta-run key ranges overlap on
   parent-keyed families, so without the inline copy every run would
   cost one filter fetch just to be ruled out.
3. Index block — one binary search names the data blocks that can hold
   the key.
4. Data blocks — one ranged GET per contiguous stretch of needed
   blocks.

Index, filter, and data blocks are cached decoded, under a byte budget.
Scans and clustered lookups read some blocks ahead of what they
strictly need: on an object store a request costs more than a few dozen
extra kilobytes, so trading bytes for fewer round trips is the right
side of the exchange. Cold lookups make the same trade across sections:
a filter fetch pulls the adjacent index block in the same ranged GET,
and a segment whose whole object costs less to transfer than a second
round trip is fetched once and decoded section by section.

## Checkpoints and reorganization

The mutation log (WAL) is absorbed into the tree by two separate
operations, each with its own cost bound.

**Checkpoints append.** A checkpoint folds the WAL tail into one new
delta run and publishes a manifest referencing every prior run
unchanged. Its cost is proportional to what changed since the last
checkpoint, never to namespace size.

**Reorganization folds.** As delta runs accumulate, maintenance merges an
oldest-first subset of complete runs for one family group, each fold ending
in its own manifest publish. The subset is capped by logical-run count,
decoded row count, and decoded SST data-block bytes. Rows that no retained
history can observe are dropped during the merge; runs no longer referenced
by any retained manifest become garbage.

```
checkpoints append:          reorganization folds, one group per step:

  delta run 3 (newest)         bindings:  oldest bounded subset -> base
  delta run 2                  revisions: oldest bounded subset -> base
  delta run 1                  inodes:    oldest bounded subset -> base
  base run    (oldest)         ...repeated until no delta rows remain
```

Because every fold publishes a manifest, the manifest doubles as the
resume point: reorganization interrupted at any point simply continues
from whatever the live manifest says still has delta rows. There is no
separate progress record to maintain, repair, or mistrust. Readers
never observe an intermediate state — every step lands through the
normal manifest publication path.

A bounded output carries the manifest head sequence. While an older or
same-sequence delta run remains, that base/delta ordering tells the next step
that a triggered reorganization is still in progress even if the remaining
delta count fell below the normal trigger. Once the batch is drained, later
delta runs are strictly newer than the base and the ordinary trigger applies
again.

The two secondary-index families fold together with their canonical family
({bindings, child bindings} and {revisions, revisions-by-inode}). Every
selected input is a complete logical run, so row-level parity is validated
over exactly the subset being replaced without materializing or cloning the
unselected family.

## Constants and tunables

Two kinds of numbers appear above, with different contracts:

- **Filter parameters are format constants.** Every segment carries one
  bloom filter, about 10 bits per key with 7 probes derived from two
  fixed-seed 64-bit hashes. Filters are durable bytes, so the hashing
  evolves in place at version 1 before the first stable release; afterward,
  changing it requires a new owning format version.
- **Sizes are writer-side defaults.** Data blocks target 64 KiB before
  compression and base segments target 65,536 rows; readers take
  whatever the descriptor and index describe, so both can be retuned
  without a format change.
- **Reorganization budgets are writer-side defaults.** One step inspects at
  most 8 complete runs and decodes at most 131,072 row payloads or 64 MiB of
  SST data blocks. A manifest publish is the only progress record, so later
  steps continue with the runs the prior publish left referenced.
